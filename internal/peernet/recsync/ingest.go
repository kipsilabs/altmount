package recsync

import (
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// Origin says where a record came from.
type Origin uint8

const (
	// OriginPeer: received from another node.
	OriginPeer Origin = iota + 1
	// OriginLocal: built by this node; it must be signed by this node's key.
	OriginLocal
)

// Result is the class of an ingest outcome.
type Result uint8

const (
	// ResultStored: a new or newer record was stored (it got a new seq).
	ResultStored Result = iota + 1
	// ResultDuplicate: an equal or newer record was already stored.
	ResultDuplicate
	// ResultRejected: the record was refused; Reason says why.
	ResultRejected
)

// Outcome is the typed result of Ingest. It never carries record content.
type Outcome struct {
	Result     Result
	Reason     Reason // set for ResultRejected
	RecordKind records.Kind
	Seq        int64 // set for ResultStored
}

// ErrIngestRateLimited is returned by Ingest when the node-wide ingest rate
// limit is exhausted. The record was not looked at: a puller stops its round
// without moving its cursor, a push is answered 429.
var ErrIngestRateLimited = errors.New("recsync: ingest rate limit reached")

// RecordStore is the part of the record repository ingest needs.
type RecordStore interface {
	Insert(ctx context.Context, rec database.PeernetRecord, limits database.PeernetRecordLimits) (database.PeernetInsertResult, error)
	// HasContentHash is a read-only existence check done before any signature
	// work, so a replayed duplicate costs one indexed read.
	HasContentHash(ctx context.Context, networkID, contentHash string) (bool, error)
}

// IngestConfig configures an Ingester.
type IngestConfig struct {
	// Self is this node's ID. Required.
	Self identity.NodeID
	// Store persists accepted records. Required.
	Store RecordStore
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Limits are passed to every insert (zero fields use the store defaults).
	Limits database.PeernetRecordLimits
	// RatePerSecond and Burst bound, node-wide, how many peer records per
	// second get past the duplicate check to signature verification and a
	// database write. Zero values use DefaultIngestRate and DefaultIngestBurst.
	RatePerSecond float64
	Burst         int
}

// Ingester is the single entry point for records, from peers and from this
// node. It holds no per-record state and logs nothing per record. Safe for
// concurrent use.
type Ingester struct {
	cfg IngestConfig
	now func() time.Time

	rate     *tokenBucket
	perRelay *relayerBuckets

	mu   sync.Mutex
	nets map[string]*netCounters
}

// NewIngester returns an Ingester.
func NewIngester(cfg IngestConfig) *Ingester {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	rate, burst := cfg.RatePerSecond, cfg.Burst
	if rate <= 0 {
		rate = DefaultIngestRate
	}
	if burst <= 0 {
		burst = DefaultIngestBurst
	}
	return &Ingester{cfg: cfg, now: now, rate: newTokenBucket(rate, burst),
		perRelay: newRelayerBuckets(rate, burst, 4096), nets: map[string]*netCounters{}}
}

type netCounters struct {
	stored, duplicates int64
	backoffs           int64
	rejected           [reasonCount]int64
}

// Counters is a snapshot of one network's ingest counters since start.
type Counters struct {
	Stored     int64
	Duplicates int64
	// Backoffs counts how often a peer was put in back-off.
	Backoffs int64
	// Rejected counts rejections by reason name (only non-zero reasons).
	Rejected map[string]int64
}

// Counters returns the counters of a network.
func (i *Ingester) Counters(nw netid.NetworkID) Counters {
	i.mu.Lock()
	defer i.mu.Unlock()
	c := i.nets[nw.String()]
	out := Counters{Rejected: map[string]int64{}}
	if c == nil {
		return out
	}
	out.Stored, out.Duplicates, out.Backoffs = c.stored, c.duplicates, c.backoffs
	for r := reasonNone + 1; r < reasonCount; r++ {
		if c.rejected[r] > 0 {
			out.Rejected[r.String()] = c.rejected[r]
		}
	}
	return out
}

// Forget drops a network's counters (it was left or deleted).
func (i *Ingester) Forget(nw netid.NetworkID) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.nets, nw.String())
}

func (i *Ingester) counters(nw netid.NetworkID) *netCounters {
	c := i.nets[nw.String()]
	if c == nil {
		c = &netCounters{}
		i.nets[nw.String()] = c
	}
	return c
}

// noteBackoff counts a peer put in back-off.
func (i *Ingester) noteBackoff(nw netid.NetworkID) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.counters(nw).backoffs++
}

func (i *Ingester) count(nw netid.NetworkID, out Outcome) {
	i.mu.Lock()
	defer i.mu.Unlock()
	c := i.counters(nw)
	switch out.Result {
	case ResultStored:
		c.stored++
	case ResultDuplicate:
		c.duplicates++
	case ResultRejected:
		c.rejected[out.Reason]++
	}
}

// Ingest runs the pipeline for one encoded record that arrived in (or is
// published into) network nw:
//
//	size cap -> Peek -> network and publisher checks -> strict decode ->
//	Verify(now, nw) -> row mapping -> limited insert.
//
// A rejected record is a nil error with ResultRejected. A non-nil error means
// the store failed and the record's fate is unknown (it is not counted). The
// index columns of the stored row come only from the verified, decoded
// record. A record from a peer that names this node as publisher is dropped
// before any signature work; a local record must be signed by this node.
func (i *Ingester) Ingest(ctx context.Context, nw netid.NetworkID, raw []byte, origin Origin) (Outcome, error) {
	return i.IngestFrom(ctx, nw, raw, origin, identity.NodeID{})
}

// IngestFrom is Ingest for a record relayed by the authenticated node relayer
// (the peer it was pulled from or pushed by). The store attributes the row to
// the relayer, which is the identity its share is accounted to. For a local
// record relayer is ignored.
func (i *Ingester) IngestFrom(ctx context.Context, nw netid.NetworkID, raw []byte, origin Origin, relayer identity.NodeID) (Outcome, error) {
	out, err := i.run(ctx, nw, raw, origin, relayer)
	if err != nil {
		return Outcome{}, err
	}
	i.count(nw, out)
	return out, nil
}

func reject(kind records.Kind, r Reason) Outcome {
	return Outcome{Result: ResultRejected, Reason: r, RecordKind: kind}
}

func (i *Ingester) run(ctx context.Context, nw netid.NetworkID, raw []byte, origin Origin, relayer identity.NodeID) (Outcome, error) {
	if len(raw) > records.MaxEncodedSize {
		return reject(0, ReasonOversize), nil
	}
	env, err := records.Peek(raw)
	if err != nil {
		return reject(0, reasonOf(err)), nil
	}
	if !env.NetworkID.Equal(nw) {
		return reject(env.Kind, ReasonWrongNetwork), nil
	}
	self := env.PublisherID.Equal(i.cfg.Self)
	switch {
	case origin == OriginPeer && self:
		return reject(env.Kind, ReasonSelfPublisher), nil
	case origin == OriginLocal && !self:
		return reject(env.Kind, ReasonNotOwn), nil
	case origin != OriginPeer && origin != OriginLocal:
		return reject(env.Kind, ReasonInvalid), nil
	}
	// Replayed duplicates cost one indexed read: no signature check, no write
	// lock, no rate-limit token.
	have, err := i.cfg.Store.HasContentHash(ctx, nw.String(), hex.EncodeToString(env.ContentHash[:]))
	if err != nil {
		return Outcome{}, err
	}
	if have {
		return Outcome{Result: ResultDuplicate, RecordKind: env.Kind}, nil
	}
	if origin == OriginPeer {
		// Each relayer first spends from its own share, so a flooding peer
		// runs dry long before it can drain the node-wide bucket.
		now := i.now()
		if !relayer.IsZero() && !i.perRelay.take(relayer.String(), now) {
			return Outcome{}, ErrIngestRateLimited
		}
		if !i.rate.take(now) {
			return Outcome{}, ErrIngestRateLimited
		}
	}
	row, err := i.verifiedRow(nw, raw, env, origin)
	if err != nil {
		return reject(env.Kind, reasonOf(err)), nil
	}
	if origin == OriginPeer && !relayer.IsZero() {
		row.RelayerNodeID = relayer.String()
	}
	res, err := i.cfg.Store.Insert(ctx, row, i.cfg.Limits)
	if err != nil {
		if errors.Is(err, database.ErrPeernetRecordInvalid) {
			return reject(env.Kind, ReasonInvalid), nil
		}
		return Outcome{}, err
	}
	switch res.Outcome {
	case database.PeernetRecordInserted, database.PeernetRecordSuperseded:
		return Outcome{Result: ResultStored, RecordKind: env.Kind, Seq: res.Seq}, nil
	case database.PeernetRecordDuplicate:
		return Outcome{Result: ResultDuplicate, RecordKind: env.Kind}, nil
	case database.PeernetRecordRejectedQuota:
		if res.QuotaScope == database.PeernetRecordQuotaPublisher {
			return reject(env.Kind, ReasonQuotaPublisher), nil
		}
		return reject(env.Kind, ReasonQuotaStorage), nil
	}
	return Outcome{}, errors.New("recsync: unexpected store outcome")
}

// verifiedRow decodes and verifies raw and maps it to a row. Every column
// comes from the decoded record, never from the caller or the transport.
func (i *Ingester) verifiedRow(nw netid.NetworkID, raw []byte, env records.Envelope, origin Origin) (database.PeernetRecord, error) {
	now := i.now()
	source := database.PeernetRecordSourcePeer
	if origin == OriginLocal {
		source = database.PeernetRecordSourceLocal
	}
	row := database.PeernetRecord{
		NetworkID: nw.String(), ContentHash: hex.EncodeToString(env.ContentHash[:]),
		ReleaseID: hex.EncodeToString(env.ReleaseID[:]), PublisherNodeID: env.PublisherID.String(),
		CreatedAt: env.CreatedAt, ExpiresAt: env.ExpiresAt, Source: source, Payload: raw,
		ReleaseScheme: int(env.ReleaseScheme),
	}
	switch env.Kind {
	case records.KindRelease:
		r, err := records.DecodeReleaseRecord(raw)
		if err != nil {
			return row, err
		}
		if err := r.Verify(now, nw); err != nil {
			return row, err
		}
		row.Kind = database.PeernetRecordKindRelease
		if r.BundleID != (records.BundleID{}) {
			row.BundleID = hex.EncodeToString(r.BundleID[:])
		}
		row.Title, row.TotalSize, row.IMDbID = r.Title, r.TotalSize, r.Media.IMDb
		row.Season, row.Episode = intPtr(r.Media.Season), intPtr(r.Media.Episode)
	case records.KindAttestation:
		a, err := records.DecodeHealthAttestation(raw)
		if err != nil {
			return row, err
		}
		if err := a.Verify(now, nw); err != nil {
			return row, err
		}
		healthy := a.Healthy()
		row.Kind = database.PeernetRecordKindAttestation
		row.FileID = hex.EncodeToString(a.FileID[:])
		row.Healthy, row.Backbones, row.CheckedAt = &healthy, a.Backbones, a.CheckedAt
		row.SegmentsChecked, row.TotalSegments = int(a.SegmentsChecked), int(a.TotalSegments)
	default:
		return row, records.ErrMalformed
	}
	return row, nil
}

func intPtr(v *uint32) *int {
	if v == nil {
		return nil
	}
	n := int(*v)
	return &n
}

// reasonOf maps a records package error to a rejection reason.
func reasonOf(err error) Reason {
	switch {
	case errors.Is(err, records.ErrTooLarge):
		return ReasonOversize
	case errors.Is(err, records.ErrUnsupportedVersion):
		return ReasonUnsupportedVersion
	case errors.Is(err, records.ErrInvalidField), errors.Is(err, records.ErrCheckedAfterCreated):
		return ReasonInvalid
	case errors.Is(err, records.ErrWrongNetwork):
		return ReasonWrongNetwork
	case errors.Is(err, records.ErrFromFuture):
		return ReasonFuture
	case errors.Is(err, records.ErrExpired):
		return ReasonExpired
	case errors.Is(err, records.ErrStale):
		return ReasonStale
	case errors.Is(err, records.ErrBadSignature), errors.Is(err, records.ErrNodeIDMismatch):
		return ReasonBadSignature
	}
	return ReasonMalformed
}
