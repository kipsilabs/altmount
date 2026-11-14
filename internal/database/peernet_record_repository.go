package database

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Record kinds stored in peernet_records.kind.
const (
	PeernetRecordKindRelease     = 1
	PeernetRecordKindAttestation = 2
)

// Record sources stored in peernet_records.source. Own records are never
// evicted to make room for peer records.
const (
	PeernetRecordSourceLocal = 0
	PeernetRecordSourcePeer  = 1
)

// Hard limits of the record store.
const (
	// PeernetRecordMaxPayload is the largest opaque record, in bytes.
	PeernetRecordMaxPayload = 2048
	// PeernetRecordMaxTitle bounds the indexed title, in bytes.
	PeernetRecordMaxTitle = 1024
	// DefaultPeernetMaxPerPublisher is the default per-publisher quota of
	// peer-sourced records per network. Own records are exempt.
	DefaultPeernetMaxPerPublisher = 5000
	// DefaultPeernetMaxPerRelayer is the default quota of peer-sourced records
	// one relayer (the authenticated node a record came from) may hold per
	// network. A relayer at its quota only ever displaces its own oldest rows.
	// Publisher keys cost nothing to fabricate on an open network, so the
	// relayer is the identity the share is accounted to.
	DefaultPeernetMaxPerRelayer = 20000
	// DefaultPeernetMaxPerNetwork is the default storage cap per network. A
	// row is at most ~2.4 KB of payload plus index columns (typically well
	// under 1 KB), so the cap keeps the worst case near 100 MB per network,
	// which a home server tolerates; 200,000 rows were ~4x that and made the
	// per-network housekeeping slow. Own records are not subject to it.
	DefaultPeernetMaxPerNetwork = 50000
	// PeernetInsertBusyWait bounds how long one peer insert may wait for the
	// database write lock (and run) before it gives up with ErrPeernetBusy, so
	// the peer feature backs off instead of starving the main application.
	PeernetInsertBusyWait = 5 * time.Second
	// peernetDeleteBatch is the most rows one DeleteNetwork or relayer purge
	// transaction removes before releasing the write lock.
	peernetDeleteBatch = 1000
)

// ErrPeernetBusy is returned when the database write lock could not be
// obtained within PeernetInsertBusyWait.
var ErrPeernetBusy = errors.New("peernet record: database busy")

// ErrPeernetRecordInvalid wraps every validation failure of a record.
var ErrPeernetRecordInvalid = errors.New("peernet record: invalid")

// PeernetRecordOutcome is the result class of PeernetRecordRepository.Insert.
type PeernetRecordOutcome int

const (
	// PeernetRecordInserted: a new row was stored.
	PeernetRecordInserted PeernetRecordOutcome = iota + 1
	// PeernetRecordSuperseded: an older record of the same
	// (publisher, kind, release, file) was replaced; the new row got a new seq.
	PeernetRecordSuperseded
	// PeernetRecordDuplicate: an equal or newer record is already stored;
	// nothing changed and no seq was consumed.
	PeernetRecordDuplicate
	// PeernetRecordRejectedQuota: a quota or the storage cap refused the
	// record; nothing changed.
	PeernetRecordRejectedQuota
)

// PeernetRecordQuotaScope says which limit refused a record.
type PeernetRecordQuotaScope string

const (
	PeernetRecordQuotaPublisher PeernetRecordQuotaScope = "publisher"
	PeernetRecordQuotaStorage   PeernetRecordQuotaScope = "storage"
)

// PeernetInsertResult is returned by Insert. Seq is set for Inserted and
// Superseded. QuotaScope is set for RejectedQuota.
type PeernetInsertResult struct {
	Outcome    PeernetRecordOutcome
	Seq        int64
	QuotaScope PeernetRecordQuotaScope
}

// PeernetRecordLimits are the per-network limits applied inside Insert. A zero
// field selects the default (unlike phase-1 caps, zero never means unlimited).
type PeernetRecordLimits struct {
	MaxPerPublisher int
	MaxPerRelayer   int
	MaxPerNetwork   int
}

func (l PeernetRecordLimits) withDefaults() PeernetRecordLimits {
	if l.MaxPerPublisher <= 0 {
		l.MaxPerPublisher = DefaultPeernetMaxPerPublisher
	}
	if l.MaxPerRelayer <= 0 {
		l.MaxPerRelayer = DefaultPeernetMaxPerRelayer
	}
	if l.MaxPerNetwork <= 0 {
		l.MaxPerNetwork = DefaultPeernetMaxPerNetwork
	}
	return l
}

// PeernetRecord is one signed record's opaque bytes plus its index columns.
// The caller (which has verified the signature) extracts the index columns;
// this layer never parses Payload. IDs are 64 lowercase hex characters.
type PeernetRecord struct {
	NetworkID       string
	Kind            int
	ContentHash     string // SHA-256 of Payload, unique per network
	ReleaseID       string
	FileID          string // attestations only; empty for release records
	PublisherNodeID string
	CreatedAt       time.Time
	ExpiresAt       time.Time
	Source          int
	// RelayerNodeID is the authenticated node a peer-sourced record came from
	// (lower-case hex). Empty for own records. It is local bookkeeping, never
	// part of the signed record.
	RelayerNodeID string

	// Attestations only.
	Healthy   *bool
	Backbones []string  // canonicalised on write (trimmed, lower-cased, sorted, unique)
	CheckedAt time.Time // zero means CreatedAt; clamped to the receive time
	// SegmentsChecked and TotalSegments are the attestation's signed coverage.
	SegmentsChecked int
	TotalSegments   int

	// Release records only.
	IMDbID    string
	Season    *int
	Episode   *int
	Title     string
	TotalSize int64

	// ReleaseScheme says how ReleaseID was derived: PeernetSchemeNZB (the
	// default, also for 0) or PeernetSchemeFiles. It comes from the signed
	// record.
	ReleaseScheme int
	// BundleID is the Bundle ID the signed release record announces (lower-case
	// hex), empty when the publisher serves no bundle.
	BundleID string

	Payload []byte
}

// PeernetRecordRepository stores signed peer-network records (table
// peernet_records).
//
// Sequence guarantee: seq is allocated from a per-network counter inside the
// insert transaction, which first takes a per-network lock that is held until
// commit (a transaction-scoped advisory lock on PostgreSQL, the database write
// lock on SQLite). Inserts into one network are therefore fully serialised, so
// seq order equals commit order and no gaps appear from aborted inserts (the
// counter update rolls back with the transaction). A reader that has seen
// seq N has seen every row of that network with a lower seq that still exists:
// it can page by "seq > cursor" and never skip a row. A global auto-increment
// column cannot give this on PostgreSQL, where a sequence value can commit
// after a higher one. Seqs are strictly increasing for the life of a network
// membership and never reused, even when the highest row is deleted; they
// restart only after DeleteNetwork, which is why peers need ResetPullCursor.
type PeernetRecordRepository struct {
	db  *dialectAwareDB
	now func() time.Time
	// peerGate admits one peer-sourced insert at a time, so peer ingest is a
	// single writer even when the pool is shared with the application.
	peerGate chan struct{}
	// deleteBatch is the rows per transaction of DeleteNetwork (a field so
	// tests can use a small batch).
	deleteBatch int
	// ownReleaseBatch is the rows one DeleteOwnRelease transaction removes (a
	// field so tests can use a small batch).
	ownReleaseBatch int
	// busyWait bounds a peer insert (PeernetInsertBusyWait; a field for tests).
	busyWait time.Duration
}

// NewPeernetRecordRepository creates a repository over conn.
func NewPeernetRecordRepository(conn *sql.DB, d Dialect) *PeernetRecordRepository {
	return NewPeernetRecordRepositoryWithClock(conn, d, time.Now)
}

// NewPeernetRecordRepositoryWithClock is NewPeernetRecordRepository with an
// injectable clock (used for expiry decisions and received_at).
func NewPeernetRecordRepositoryWithClock(conn *sql.DB, d Dialect, now func() time.Time) *PeernetRecordRepository {
	return &PeernetRecordRepository{db: newDialectAwareDB(conn, d), now: now, peerGate: make(chan struct{}, 1), deleteBatch: peernetDeleteBatch, ownReleaseBatch: peernetOwnReleaseMax, busyWait: PeernetInsertBusyWait}
}

func isLowerHexID(s string) bool { return isHexID(s) && s == strings.ToLower(s) }

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrPeernetRecordInvalid, fmt.Sprintf(format, a...))
}

// canonicalBackbones returns the stored form: trimmed, lower-cased, unique,
// sorted, comma-joined.
func canonicalBackbones(labels []string) string {
	seen := make(map[string]struct{}, len(labels))
	out := make([]string, 0, len(labels))
	for _, l := range labels {
		l = strings.ToLower(strings.TrimSpace(l))
		if l == "" {
			continue
		}
		if _, dup := seen[l]; dup {
			continue
		}
		seen[l] = struct{}{}
		out = append(out, l)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func splitBackbones(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func (r PeernetRecord) validate(now time.Time) error {
	switch {
	case !isLowerHexID(r.NetworkID):
		return invalid("network id")
	case !isLowerHexID(r.ContentHash):
		return invalid("content hash")
	case !isLowerHexID(r.ReleaseID):
		return invalid("release id")
	case !isLowerHexID(r.PublisherNodeID):
		return invalid("publisher node id")
	case r.Source != PeernetRecordSourceLocal && r.Source != PeernetRecordSourcePeer:
		return invalid("source %d", r.Source)
	case r.RelayerNodeID != "" && !isLowerHexID(r.RelayerNodeID):
		return invalid("relayer node id")
	case r.Source == PeernetRecordSourceLocal && r.RelayerNodeID != "":
		return invalid("own record with a relayer")
	case r.ReleaseScheme < 0 || r.ReleaseScheme > PeernetSchemeFiles:
		return invalid("release scheme %d", r.ReleaseScheme)
	case r.BundleID != "" && !isLowerHexID(r.BundleID):
		return invalid("bundle id")
	case r.BundleID != "" && r.Kind != PeernetRecordKindRelease:
		return invalid("bundle id on a non-release record")
	case r.SegmentsChecked < 0 || r.TotalSegments < 0:
		return invalid("negative segment count")
	case len(r.Payload) == 0:
		return invalid("empty payload")
	case len(r.Payload) > PeernetRecordMaxPayload:
		return invalid("payload %d bytes exceeds %d", len(r.Payload), PeernetRecordMaxPayload)
	case !r.ExpiresAt.After(r.CreatedAt):
		return invalid("expiry not after creation")
	case !r.ExpiresAt.After(now):
		return invalid("already expired")
	case len(r.Title) > PeernetRecordMaxTitle:
		return invalid("title longer than %d bytes", PeernetRecordMaxTitle)
	case len(r.IMDbID) > 32:
		return invalid("imdb id too long")
	case (r.Season != nil && *r.Season < 0) || (r.Episode != nil && *r.Episode < 0):
		return invalid("negative season or episode")
	case r.TotalSize < 0:
		return invalid("negative total size")
	}
	for _, b := range r.Backbones {
		if strings.Contains(b, ",") {
			return invalid("backbone label contains a comma")
		}
	}
	switch r.Kind {
	case PeernetRecordKindRelease:
		if r.FileID != "" || r.Healthy != nil || len(r.Backbones) > 0 {
			return invalid("release record carries attestation fields")
		}
	case PeernetRecordKindAttestation:
		if !isLowerHexID(r.FileID) {
			return invalid("attestation file id")
		}
		if r.Healthy == nil {
			return invalid("attestation without a verdict")
		}
		if r.IMDbID != "" || r.Season != nil || r.Episode != nil || r.Title != "" || r.TotalSize != 0 {
			return invalid("attestation carries release fields")
		}
	default:
		return invalid("kind %d", r.Kind)
	}
	return nil
}

// Insert stores rec following the supersede rule: for one (network, publisher,
// kind, release, file) only the newest created_at survives. A newer record
// replaces the older one and gets a new seq; an older or equal one is a
// Duplicate and changes nothing. Replacing a record never changes the counts
// and is exempt from the limits.
//
// Limits (see PeernetRecordLimits), enforced in the insert transaction from
// maintained counters, never by scanning: a peer-sourced record is refused at
// the per-publisher quota; at the per-relayer quota it displaces that relayer's
// own oldest rows; at the network cap expired rows go first, then the oldest
// rows of the relayer holding the most. Own (source 0) rows are exempt from
// the publisher and relayer quotas, are never evicted, and are inserted even
// when the cap is full of own rows. Peer-sourced inserts run one at a time and
// give up with ErrPeernetBusy after PeernetInsertBusyWait.
func (r *PeernetRecordRepository) Insert(ctx context.Context, rec PeernetRecord, limits PeernetRecordLimits) (PeernetInsertResult, error) {
	now := r.now()
	if err := rec.validate(now); err != nil {
		return PeernetInsertResult{}, err
	}
	limits = limits.withDefaults()

	if rec.Source == PeernetRecordSourcePeer {
		busy, cancel := context.WithTimeout(ctx, r.busyWait)
		defer cancel()
		select {
		case r.peerGate <- struct{}{}:
			defer func() { <-r.peerGate }()
		case <-busy.Done():
			return PeernetInsertResult{}, busyError(ctx, busy)
		}
		res, err := r.insert(busy, rec, limits, now)
		if err != nil {
			err = busyOr(ctx, busy, err)
		}
		return res, err
	}
	return r.insert(ctx, rec, limits, now)
}

func busyError(parent, child context.Context) error {
	if parent.Err() != nil {
		return parent.Err()
	}
	return fmt.Errorf("%w: %v", ErrPeernetBusy, child.Err())
}

// busyOr turns a deadline of the busy-wait context (not of the caller) into
// ErrPeernetBusy.
func busyOr(parent, child context.Context, err error) error {
	if parent.Err() == nil && child.Err() != nil {
		return fmt.Errorf("%w: %v", ErrPeernetBusy, err)
	}
	return err
}

func (r *PeernetRecordRepository) insert(ctx context.Context, rec PeernetRecord, limits PeernetRecordLimits, now time.Time) (PeernetInsertResult, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return PeernetInsertResult{}, fmt.Errorf("insert peernet record: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	if err := r.lockNetwork(ctx, tx, rec.NetworkID); err != nil {
		return PeernetInsertResult{}, err
	}

	// Same hash anywhere in the network: already stored (the hash covers the
	// publisher and creation time, so this is the identical record).
	var one int
	switch err := tx.QueryRowContext(ctx, `SELECT 1 FROM peernet_records WHERE network_id = ? AND content_hash = ?`,
		rec.NetworkID, rec.ContentHash).Scan(&one); {
	case err == nil:
		return PeernetInsertResult{Outcome: PeernetRecordDuplicate}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return PeernetInsertResult{}, fmt.Errorf("insert peernet record: hash lookup: %w", err)
	}

	superseded, keepAttribution := false, false
	old := recordRef{}
	var oldCreated int64
	switch err := tx.QueryRowContext(ctx, `SELECT seq, created_at, source, relayer_node_id FROM peernet_records
		WHERE network_id = ? AND publisher_node_id = ? AND kind = ? AND release_id = ? AND file_id = ?`,
		rec.NetworkID, rec.PublisherNodeID, rec.Kind, rec.ReleaseID, rec.FileID).Scan(&old.seq, &oldCreated, &old.source, &old.relayer); {
	case err == nil:
		if rec.CreatedAt.Unix() <= oldCreated {
			return PeernetInsertResult{Outcome: PeernetRecordDuplicate}, nil
		}
		old.publisher = rec.PublisherNodeID
		if err := deleteRefs(ctx, tx, rec.NetworkID, []recordRef{old}); err != nil {
			return PeernetInsertResult{}, err
		}
		superseded = true
		// The row stays with the relayer that delivered the record first, so a
		// later relayer cannot take over (and later purge or evict) rows it
		// did not deliver. Only an unattributed row changes hands, and then
		// the new relayer's share applies.
		if rec.Source == PeernetRecordSourcePeer && old.source == PeernetRecordSourcePeer && old.relayer != "" {
			rec.RelayerNodeID = old.relayer
			keepAttribution = true
		}
	case !errors.Is(err, sql.ErrNoRows):
		return PeernetInsertResult{}, fmt.Errorf("insert peernet record: key lookup: %w", err)
	}

	// A replacement changes no count, so it is exempt from the limits unless a
	// peer row changes relayer.
	if !superseded || (rec.Source == PeernetRecordSourcePeer && !keepAttribution) {
		scope, err := r.enforceLimits(ctx, tx, rec, limits, now.Unix())
		if err != nil {
			return PeernetInsertResult{}, err
		}
		if scope != "" {
			return PeernetInsertResult{Outcome: PeernetRecordRejectedQuota, QuotaScope: scope}, nil
		}
	}

	seq, err := nextSeq(ctx, tx, rec.NetworkID)
	if err != nil {
		return PeernetInsertResult{}, err
	}
	checked := rec.CheckedAt
	if rec.Kind == PeernetRecordKindAttestation {
		if checked.IsZero() {
			checked = rec.CreatedAt
		}
		// A publisher-chosen time in the future must not float its records to
		// the top of the newest-check-first ordering.
		if checked.After(now) {
			checked = now
		}
	}
	var healthy any
	if rec.Healthy != nil {
		healthy = boolInt(*rec.Healthy)
	}
	scheme := rec.ReleaseScheme
	if scheme == PeernetSchemeNone {
		scheme = PeernetSchemeNZB
	}
	var season, episode any
	if rec.Season != nil {
		season = *rec.Season
	}
	if rec.Episode != nil {
		episode = *rec.Episode
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO peernet_records
		(network_id, seq, kind, content_hash, release_id, file_id, publisher_node_id, created_at, expires_at,
		 received_at, source, relayer_node_id, healthy, backbones, checked_at, segments_checked, total_segments,
		 imdb_id, season, episode, title, total_size, payload, release_scheme, bundle_id)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.NetworkID, seq, rec.Kind, rec.ContentHash, rec.ReleaseID, rec.FileID, rec.PublisherNodeID,
		rec.CreatedAt.Unix(), rec.ExpiresAt.Unix(), now.Unix(), rec.Source, rec.RelayerNodeID, healthy,
		canonicalBackbones(rec.Backbones), unixOrZero(checked), rec.SegmentsChecked, rec.TotalSegments,
		rec.IMDbID, season, episode, rec.Title, rec.TotalSize, rec.Payload, scheme, rec.BundleID); err != nil {
		return PeernetInsertResult{}, fmt.Errorf("insert peernet record: %w", err)
	}
	if err := addRowCounts(ctx, tx, rec.NetworkID, recordRef{publisher: rec.PublisherNodeID, source: rec.Source, relayer: rec.RelayerNodeID}, 1); err != nil {
		return PeernetInsertResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeernetInsertResult{}, fmt.Errorf("insert peernet record: commit: %w", err)
	}
	if rec.BundleID != "" {
		bundleOfferGeneration.Add(1)
	}
	if superseded {
		return PeernetInsertResult{Outcome: PeernetRecordSuperseded, Seq: seq}, nil
	}
	return PeernetInsertResult{Outcome: PeernetRecordInserted, Seq: seq}, nil
}

// HasContentHash reports whether a record with this content hash is stored in
// the network. It is a single indexed read outside any write transaction, so
// replayed duplicates cost one lookup and no lock.
func (r *PeernetRecordRepository) HasContentHash(ctx context.Context, networkID, contentHash string) (bool, error) {
	if !isLowerHexID(networkID) || !isLowerHexID(contentHash) {
		return false, errors.New("peernet records: invalid network or content hash")
	}
	var one int
	switch err := r.db.QueryRowContext(ctx, `SELECT 1 FROM peernet_records WHERE network_id = ? AND content_hash = ?`,
		networkID, contentHash).Scan(&one); {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("peernet records: hash lookup: %w", err)
	}
}

// lockNetwork serialises the transaction against other writers of the same
// network's records, until commit or rollback.
func (r *PeernetRecordRepository) lockNetwork(ctx context.Context, tx *dialectAwareTx, network string) error {
	if r.db.dialect.IsPostgres() {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(?)`, recordLockKey(network)); err != nil {
			return fmt.Errorf("peernet records: lock: %w", err)
		}
		return nil
	}
	// Take the SQLite write lock before the first read so the transaction is
	// never an upgrade that fails with "database is locked".
	if _, err := tx.ExecContext(ctx, `DELETE FROM peernet_records WHERE 1 = 0`); err != nil {
		return fmt.Errorf("peernet records: lock: %w", err)
	}
	return nil
}

func recordLockKey(network string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("peernet_records:" + network))
	return int64(h.Sum64())
}

// nextSeq allocates the next sequence number. The caller holds the network
// lock, so read-then-write is race free.
func nextSeq(ctx context.Context, tx *dialectAwareTx, network string) (int64, error) {
	var seq int64
	switch err := tx.QueryRowContext(ctx, `SELECT next_seq FROM peernet_record_counters WHERE network_id = ?`,
		network).Scan(&seq); {
	case errors.Is(err, sql.ErrNoRows):
		seq = 1
		if _, err := tx.ExecContext(ctx, `INSERT INTO peernet_record_counters (network_id, next_seq) VALUES (?, ?)`,
			network, seq+1); err != nil {
			return 0, fmt.Errorf("peernet records: init counter: %w", err)
		}
		return seq, nil
	case err != nil:
		return 0, fmt.Errorf("peernet records: read counter: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE peernet_record_counters SET next_seq = ? WHERE network_id = ?`,
		seq+1, network); err != nil {
		return 0, fmt.Errorf("peernet records: bump counter: %w", err)
	}
	return seq, nil
}

// enforceLimits applies the publisher quota, the relayer share and the
// network cap for a new row, from the maintained counters. It returns the
// refusing scope, or "" when rec may be inserted. Rows deleted on the way are
// rolled back by the caller when it then refuses.
func (r *PeernetRecordRepository) enforceLimits(ctx context.Context, tx *dialectAwareTx, rec PeernetRecord,
	limits PeernetRecordLimits, now int64) (PeernetRecordQuotaScope, error) {
	own := rec.Source == PeernetRecordSourceLocal
	if !own {
		n, err := getCount(ctx, tx, rec.NetworkID, countScopePublisher, rec.PublisherNodeID)
		if err != nil {
			return "", err
		}
		if n >= int64(limits.MaxPerPublisher) {
			// Expired rows of the publisher do not count against the quota.
			freed, err := r.evictWhere(ctx, tx, rec.NetworkID, `publisher_node_id = ? AND expires_at <= ?`,
				[]any{rec.PublisherNodeID, now}, `expires_at, seq`, int(n)-limits.MaxPerPublisher+1)
			if err != nil {
				return "", err
			}
			if int(n)-freed >= limits.MaxPerPublisher {
				return PeernetRecordQuotaPublisher, nil
			}
		}
		share, err := getCount(ctx, tx, rec.NetworkID, countScopeRelayer, rec.RelayerNodeID)
		if err != nil {
			return "", err
		}
		// Within its own share a relayer replaces its own oldest rows.
		if over := int(share) - limits.MaxPerRelayer + 1; over > 0 {
			if _, err := r.evictRelayerOldest(ctx, tx, rec.NetworkID, rec.RelayerNodeID, over); err != nil {
				return "", err
			}
		}
	}

	total, err := getCount(ctx, tx, rec.NetworkID, countScopeNetwork, "")
	if err != nil {
		return "", err
	}
	need := int(total) - limits.MaxPerNetwork + 1
	if need <= 0 {
		return "", nil
	}
	got, err := r.evictWhere(ctx, tx, rec.NetworkID, `expires_at <= ?`, []any{now}, `expires_at, seq`, need)
	if err != nil {
		return "", err
	}
	// Then the relayer holding the most rows gives up its oldest.
	for need -= got; need > 0; {
		relayer, ok, err := largestRelayer(ctx, tx, rec.NetworkID)
		if err != nil {
			return "", err
		}
		if !ok {
			break
		}
		n, err := r.evictRelayerOldest(ctx, tx, rec.NetworkID, relayer, 1)
		if err != nil {
			return "", err
		}
		if n == 0 {
			return "", fmt.Errorf("peernet records: counters out of step for relayer share")
		}
		need -= n
	}
	if need > 0 && !own {
		return PeernetRecordQuotaStorage, nil
	}
	return "", nil
}

// evictRelayerOldest deletes up to limit of a relayer's oldest peer rows.
func (r *PeernetRecordRepository) evictRelayerOldest(ctx context.Context, tx *dialectAwareTx, network, relayer string, limit int) (int, error) {
	return r.evictWhere(ctx, tx, network, `source = ? AND relayer_node_id = ?`, []any{PeernetRecordSourcePeer, relayer}, `seq`, limit)
}

// evictWhere deletes up to limit rows of the network matching cond, in order.
func (r *PeernetRecordRepository) evictWhere(ctx context.Context, tx *dialectAwareTx, network, cond string, args []any, order string, limit int) (int, error) {
	refs, err := selectRefs(ctx, tx, network, cond, args, order, limit)
	if err != nil {
		return 0, err
	}
	if err := deleteRefs(ctx, tx, network, refs); err != nil {
		return 0, err
	}
	return len(refs), nil
}

// PurgeExpired deletes up to limit rows whose expiry has passed (oldest
// expiry first) and returns how many it deleted. Call it repeatedly while it
// returns limit. The counters follow in the same transactions, one per
// network so each holds only that network's lock.
func (r *PeernetRecordRepository) PurgeExpired(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		return 0, errors.New("peernet records: purge limit must be positive")
	}
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT network_id FROM (
		SELECT network_id FROM peernet_records WHERE expires_at <= ? ORDER BY expires_at LIMIT ?) AS due`,
		r.now().Unix(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge expired peernet records: %w", err)
	}
	var networks []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("purge expired peernet records: %w", err)
		}
		networks = append(networks, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("purge expired peernet records: %w", err)
	}
	rows.Close()
	sort.Strings(networks)
	total := 0
	for _, nw := range networks {
		if total >= limit {
			break
		}
		n, err := r.purgeExpiredIn(ctx, nw, limit-total)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (r *PeernetRecordRepository) purgeExpiredIn(ctx context.Context, network string, limit int) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("purge expired peernet records: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	if err := r.lockNetwork(ctx, tx, network); err != nil {
		return 0, err
	}
	n, err := r.evictWhere(ctx, tx, network, `expires_at <= ?`, []any{r.now().Unix()}, `expires_at, seq`, limit)
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

// DeleteOwnRelease deletes this node's own (source 0) records of one release
// in a network, release record and attestations alike, and returns how many
// went. Peer-sourced rows are untouched. It is used to retract what was
// published before an exclusion was added.
func (r *PeernetRecordRepository) DeleteOwnRelease(ctx context.Context, networkID, releaseID string) (int, error) {
	if !isLowerHexID(networkID) || !isLowerHexID(releaseID) {
		return 0, errors.New("peernet records: invalid network or release id")
	}
	total := 0
	for {
		n, err := r.deleteOwnReleaseBatch(ctx, networkID, releaseID)
		total += n
		if err != nil || n < r.ownReleaseBatch {
			return total, err
		}
	}
}

// deleteOwnReleaseBatch removes up to ownReleaseBatch rows in one short
// transaction, so the write lock is released between batches.
func (r *PeernetRecordRepository) deleteOwnReleaseBatch(ctx context.Context, networkID, releaseID string) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("delete own peernet release: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	if err := r.lockNetwork(ctx, tx, networkID); err != nil {
		return 0, err
	}
	n, err := r.evictWhere(ctx, tx, networkID, `source = ? AND release_id = ?`,
		[]any{PeernetRecordSourceLocal, releaseID}, `seq`, r.ownReleaseBatch)
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

// peernetOwnReleaseMax is the rows one DeleteOwnRelease transaction removes;
// a release with more rows takes several transactions.
const peernetOwnReleaseMax = 5000

// DeleteNetwork removes every record, the sequence counter and every sync
// cursor of a network. Phase-1 PeernetNetworkRepository.DeleteNetwork removes
// only invites and members (peers are cleaned separately), so callers leaving
// a network call both. A later rejoin starts a fresh sequence at 1.
//
// Records go in batches of at most 1000 rows, each in its own short
// transaction, so the write lock is released between batches. The counters
// follow each batch, so the store stays consistent at every point. The
// bookkeeping rows (counter, epoch, cursors) go in a last step that runs only
// when no record is left; a record that arrives meanwhile just makes the loop
// run another batch, and a caller interrupted midway finds the network listed
// by NetworkIDs again, so the orphan cleanup finishes it. The call is
// idempotent.
func (r *PeernetRecordRepository) DeleteNetwork(ctx context.Context, networkID string) error {
	if !isLowerHexID(networkID) {
		return errors.New("peernet records: invalid network id")
	}
	for {
		done, err := r.deleteNetworkStep(ctx, networkID)
		if err != nil || done {
			return err
		}
	}
}

// deleteNetworkStep deletes one batch, or the bookkeeping when no record is
// left (done is then true).
func (r *PeernetRecordRepository) deleteNetworkStep(ctx context.Context, network string) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("delete peernet records: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	if err := r.lockNetwork(ctx, tx, network); err != nil {
		return false, err
	}
	refs, err := selectRefs(ctx, tx, network, `1 = 1`, nil, `seq`, r.deleteBatch)
	if err != nil {
		return false, err
	}
	if len(refs) > 0 {
		if err := deleteRefs(ctx, tx, network, refs); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	for _, q := range []string{
		`DELETE FROM peernet_record_counts WHERE network_id = ?`,
		`DELETE FROM peernet_record_counters WHERE network_id = ?`,
		`DELETE FROM peernet_record_epochs WHERE network_id = ?`,
		`DELETE FROM peernet_sync_cursors WHERE network_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, network); err != nil {
			return false, fmt.Errorf("delete peernet network data: %w", err)
		}
	}
	return true, tx.Commit()
}

// Epoch returns the random value identifying this node's record sequence space
// for a network, creating it on first use. It stays the same while the
// network is joined and changes after DeleteNetwork (or a database reset), so
// a peer that pulled from this node can tell its stored cursor no longer
// applies. It is stored separately from the sequence counter because the
// counter row exists only once a record has been stored.
func (r *PeernetRecordRepository) Epoch(ctx context.Context, networkID string) (string, error) {
	if !isLowerHexID(networkID) {
		return "", errors.New("peernet records: invalid network id")
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("peernet record epoch: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, `INSERT INTO peernet_record_epochs (network_id, epoch) VALUES (?, ?)
		ON CONFLICT (network_id) DO NOTHING`, networkID, hex.EncodeToString(buf[:])); err != nil {
		return "", fmt.Errorf("peernet record epoch: create: %w", err)
	}
	var epoch string
	if err := r.db.QueryRowContext(ctx, `SELECT epoch FROM peernet_record_epochs WHERE network_id = ?`,
		networkID).Scan(&epoch); err != nil {
		return "", fmt.Errorf("peernet record epoch: read: %w", err)
	}
	return epoch, nil
}

// NetworkIDs returns every network that has stored records, a sequence counter
// or an epoch, so the caller can find data left behind by networks that no
// longer exist.
func (r *PeernetRecordRepository) NetworkIDs(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT network_id FROM peernet_record_counters
		UNION SELECT network_id FROM peernet_record_epochs
		UNION SELECT network_id FROM peernet_record_counts
		UNION SELECT DISTINCT network_id FROM peernet_records`)
	if err != nil {
		return nil, fmt.Errorf("list peernet record networks: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan peernet record network: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// bundleOfferGeneration counts the records stored with a Bundle ID in this
// process. A cached "this node holds a bundle offer" answer is stale exactly
// when the number changed.
var bundleOfferGeneration atomic.Uint64

// BundleOfferGeneration returns the number of bundle-announcing records stored
// by this process so far (see AnyBundleOffer).
func BundleOfferGeneration() uint64 { return bundleOfferGeneration.Load() }

// AnyBundleOffer reports whether any stored record announces a bundle.
func (r *PeernetRecordRepository) AnyBundleOffer(ctx context.Context) (bool, error) {
	var one int
	err := r.db.QueryRowContext(ctx, `SELECT 1 FROM peernet_records WHERE bundle_id <> '' LIMIT 1`).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("peernet records: any bundle offer: %w", err)
	}
	return true, nil
}
