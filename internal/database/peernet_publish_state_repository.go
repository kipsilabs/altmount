package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PeernetPublishState is what was last published for one (network, kind,
// release, file). Kind uses the PeernetRecordKind* values; FileID is empty for
// release records. CheckedAt and Verdict are set for attestations only.
type PeernetPublishState struct {
	NetworkID   string
	Kind        int
	ReleaseID   string
	FileID      string
	PublishedAt time.Time
	CheckedAt   time.Time
	Verdict     int
	// OriginCategory and OriginIndexer are a private, never-transmitted tag
	// (lower-cased, trimmed) of the release's origin at publish time.
	OriginCategory string
	OriginIndexer  string
}

// PeernetPublishStateRepository stores last-published state (table
// peernet_publish_state).
type PeernetPublishStateRepository struct {
	db *dialectAwareDB
}

// NewPeernetPublishStateRepository creates a repository over conn.
func NewPeernetPublishStateRepository(conn *sql.DB, d Dialect) *PeernetPublishStateRepository {
	return &PeernetPublishStateRepository{db: newDialectAwareDB(conn, d)}
}

func validStateKey(network string, kind int, release, file string) error {
	switch {
	case !isLowerHexID(network):
		return errors.New("peernet publish state: invalid network id")
	case kind != PeernetRecordKindRelease && kind != PeernetRecordKindAttestation:
		return errors.New("peernet publish state: invalid kind")
	case !isLowerHexID(release):
		return errors.New("peernet publish state: invalid release id")
	case file != "" && !isLowerHexID(file):
		return errors.New("peernet publish state: invalid file id")
	}
	return nil
}

// Get returns the stored state; ok is false when nothing was published.
func (r *PeernetPublishStateRepository) Get(ctx context.Context, network string, kind int, release, file string) (PeernetPublishState, bool, error) {
	if err := validStateKey(network, kind, release, file); err != nil {
		return PeernetPublishState{}, false, err
	}
	s := PeernetPublishState{NetworkID: network, Kind: kind, ReleaseID: release, FileID: file}
	var published, checked int64
	err := r.db.QueryRowContext(ctx,
		`SELECT published_at, checked_at, verdict, origin_category, origin_indexer FROM peernet_publish_state
		 WHERE network_id = ? AND kind = ? AND release_id = ? AND file_id = ?`,
		network, kind, release, file).Scan(&published, &checked, &s.Verdict, &s.OriginCategory, &s.OriginIndexer)
	if errors.Is(err, sql.ErrNoRows) {
		return PeernetPublishState{}, false, nil
	}
	if err != nil {
		return PeernetPublishState{}, false, fmt.Errorf("get peernet publish state: %w", err)
	}
	s.PublishedAt, s.CheckedAt = timeOrZero(published), timeOrZero(checked)
	return s, true, nil
}

// Put inserts or replaces the state of one key.
func (r *PeernetPublishStateRepository) Put(ctx context.Context, s PeernetPublishState) error {
	if err := validStateKey(s.NetworkID, s.Kind, s.ReleaseID, s.FileID); err != nil {
		return err
	}
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO peernet_publish_state (network_id, kind, release_id, file_id, published_at, checked_at, verdict,
			origin_category, origin_indexer)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (network_id, kind, release_id, file_id) DO UPDATE SET
			published_at = excluded.published_at, checked_at = excluded.checked_at, verdict = excluded.verdict,
			origin_category = excluded.origin_category, origin_indexer = excluded.origin_indexer`,
		s.NetworkID, s.Kind, s.ReleaseID, s.FileID, unixOrZero(s.PublishedAt), unixOrZero(s.CheckedAt), s.Verdict,
		normTag(s.OriginCategory), normTag(s.OriginIndexer))
	if err != nil {
		return fmt.Errorf("put peernet publish state: %w", err)
	}
	return nil
}

// ListReleasesPublishedBefore returns up to limit release-record states whose
// last publication is older than before, oldest first. Attestation rows are
// never listed.
func (r *PeernetPublishStateRepository) ListReleasesPublishedBefore(ctx context.Context, before time.Time, limit int) ([]PeernetPublishState, error) {
	if limit < 1 {
		return nil, errors.New("peernet publish state: limit must be positive")
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT network_id, release_id, published_at FROM peernet_publish_state
		 WHERE kind = ? AND published_at < ? ORDER BY published_at, network_id, release_id LIMIT ?`,
		PeernetRecordKindRelease, before.Unix(), limit)
	if err != nil {
		return nil, fmt.Errorf("list peernet publish state: %w", err)
	}
	defer rows.Close()
	var out []PeernetPublishState
	for rows.Next() {
		s := PeernetPublishState{Kind: PeernetRecordKindRelease}
		var published int64
		if err := rows.Scan(&s.NetworkID, &s.ReleaseID, &published); err != nil {
			return nil, fmt.Errorf("scan peernet publish state: %w", err)
		}
		s.PublishedAt = time.Unix(published, 0)
		out = append(out, s)
	}
	return out, rows.Err()
}

// DeleteNetwork removes every row of a network.
func (r *PeernetPublishStateRepository) DeleteNetwork(ctx context.Context, network string) error {
	if !isLowerHexID(network) {
		return errors.New("peernet publish state: invalid network id")
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM peernet_publish_state WHERE network_id = ?`, network); err != nil {
		return fmt.Errorf("delete peernet publish state of network: %w", err)
	}
	return nil
}

func normTag(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// DeleteOrphanNetworks removes the rows of networks that no longer exist and
// returns how many rows went. known is called after the stored network IDs
// have been read: a network created in between is then in the live set (if it
// was created before the call) or absent from the snapshot (if after), so it
// can never be mistaken for an orphan.
func (r *PeernetPublishStateRepository) DeleteOrphanNetworks(ctx context.Context, known func(context.Context) (map[string]bool, error)) (int64, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT network_id FROM peernet_publish_state`)
	if err != nil {
		return 0, fmt.Errorf("list peernet publish state networks: %w", err)
	}
	var stored []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan peernet publish state network: %w", err)
		}
		stored = append(stored, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	live, err := known(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, id := range stored {
		if live[id] {
			continue
		}
		res, err := r.db.ExecContext(ctx, `DELETE FROM peernet_publish_state WHERE network_id = ?`, id)
		if err != nil {
			return total, fmt.Errorf("delete orphan peernet publish state: %w", err)
		}
		n, err := rowsAffected(res, "delete orphan peernet publish state")
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// PeernetPublishCursor is a position in the primary-key order of the publish
// state table. The zero value is the start.
type PeernetPublishCursor struct {
	NetworkID string
	Kind      int
	ReleaseID string
	FileID    string
}

// PruneUnindexed scans one bounded window of at most window rows after cursor
// in primary-key order and deletes the rows of that window whose release is no
// longer in the content index. It returns how many went and the cursor for the
// next call; the zero cursor means the scan reached the end and starts over.
// Called once per few upkeep passes it keeps the table in step with the
// library without ever scanning the whole table under the write lock.
func (r *PeernetPublishStateRepository) PruneUnindexed(ctx context.Context, cursor PeernetPublishCursor, window int) (int64, PeernetPublishCursor, error) {
	if window < 1 {
		return 0, cursor, errors.New("peernet publish state: window must be positive")
	}
	const after = `(network_id, kind, release_id, file_id) > (?, ?, ?, ?)`
	args := []any{cursor.NetworkID, cursor.Kind, cursor.ReleaseID, cursor.FileID}
	// The last key of the window, if the table has that many rows left.
	var last PeernetPublishCursor
	err := r.db.QueryRowContext(ctx, `SELECT network_id, kind, release_id, file_id FROM peernet_publish_state
		WHERE `+after+` ORDER BY network_id, kind, release_id, file_id LIMIT 1 OFFSET ?`, append(args, window-1)...).
		Scan(&last.NetworkID, &last.Kind, &last.ReleaseID, &last.FileID)
	bounded := true
	switch {
	case errors.Is(err, sql.ErrNoRows):
		bounded = false // fewer than a window left: scan to the end
	case err != nil:
		return 0, cursor, fmt.Errorf("prune peernet publish state: %w", err)
	}
	q := `DELETE FROM peernet_publish_state WHERE ` + after
	if bounded {
		q += ` AND (network_id, kind, release_id, file_id) <= (?, ?, ?, ?)`
		args = append(args, last.NetworkID, last.Kind, last.ReleaseID, last.FileID)
	}
	q += ` AND NOT EXISTS (SELECT 1 FROM peernet_content_index c WHERE c.release_id = peernet_publish_state.release_id)`
	res, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, cursor, fmt.Errorf("prune peernet publish state: %w", err)
	}
	n, err := rowsAffected(res, "prune peernet publish state")
	if err != nil {
		return 0, cursor, err
	}
	if !bounded {
		return n, PeernetPublishCursor{}, nil
	}
	return n, last, nil
}

// PeernetPublishedRelease names a release published to a network.
type PeernetPublishedRelease struct {
	NetworkID string
	ReleaseID string
}

// ListExcluded returns up to limit distinct (network, release) pairs whose
// origin tag matches one of the excluded categories or indexers (compared
// lower-cased and trimmed). Releases published before tags were kept have
// empty tags and never match.
func (r *PeernetPublishStateRepository) ListExcluded(ctx context.Context, categories, indexers []string, limit int) ([]PeernetPublishedRelease, error) {
	if limit < 1 {
		return nil, errors.New("peernet publish state: limit must be positive")
	}
	var conds []string
	var args []any
	add := func(col string, vals []string) {
		var in []string
		for _, v := range vals {
			// "unknown" is how an unknown indexer is spelled; the publisher
			// never matches exclusions against it, so neither may this.
			if v = normTag(v); v != "" && (col != "origin_indexer" || v != "unknown") {
				in = append(in, v)
			}
		}
		if len(in) == 0 {
			return
		}
		conds = append(conds, col+` IN (`+strings.TrimSuffix(strings.Repeat("?,", len(in)), ",")+`)`)
		for _, v := range in {
			args = append(args, v)
		}
	}
	add("origin_category", categories)
	add("origin_indexer", indexers)
	if len(conds) == 0 {
		return nil, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT network_id, release_id FROM peernet_publish_state WHERE `+
		strings.Join(conds, " OR ")+` ORDER BY network_id, release_id LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("list excluded peernet publish state: %w", err)
	}
	defer rows.Close()
	var out []PeernetPublishedRelease
	for rows.Next() {
		var p PeernetPublishedRelease
		if err := rows.Scan(&p.NetworkID, &p.ReleaseID); err != nil {
			return nil, fmt.Errorf("scan excluded peernet publish state: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeleteRelease removes every row (release record and attestations) of one
// release in one network.
func (r *PeernetPublishStateRepository) DeleteRelease(ctx context.Context, network, release string) error {
	if !isLowerHexID(network) || !isLowerHexID(release) {
		return errors.New("peernet publish state: invalid network or release id")
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM peernet_publish_state WHERE network_id = ? AND release_id = ?`,
		network, release); err != nil {
		return fmt.Errorf("delete peernet publish state of release: %w", err)
	}
	return nil
}
