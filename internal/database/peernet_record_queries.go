package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Query bounds.
const (
	// PeernetListMaxLimit caps the rows scanned by one ListSince/ListOwnSince.
	PeernetListMaxLimit = 500
	// PeernetCandidatesMaxLimit caps CandidatesByMedia results.
	PeernetCandidatesMaxLimit = 200
	// PeernetMaxReleaseIDsPerQuery bounds HealthSummaryForReleases input.
	PeernetMaxReleaseIDsPerQuery = 200
	// PeernetMaxNetworksPerQuery bounds CandidatesByMedia's network list.
	PeernetMaxNetworksPerQuery = 32
	// peernetAttestationsMax caps AttestationsFor results.
	peernetAttestationsMax = 1000
)

// ErrPeernetTooManyIDs is returned when a batch query gets more IDs than its bound.
var ErrPeernetTooManyIDs = errors.New("peernet records: too many ids in one query")

// PeernetRecordRow is one stored record as served to pullers.
type PeernetRecordRow struct {
	Seq     int64
	Payload []byte
}

// PeernetRecordPage is one page of ListSince/ListOwnSince. HighestSeq is the
// highest seq scanned (expired rows included), or the input cursor when
// nothing was scanned: store it as the next cursor. More reports that rows
// beyond this page exist.
type PeernetRecordPage struct {
	Rows       []PeernetRecordRow
	HighestSeq int64
	More       bool
}

func (r *PeernetRecordRepository) page(ctx context.Context, networkID string, afterSeq int64, limit int, ownOnly bool) (PeernetRecordPage, error) {
	if !isLowerHexID(networkID) {
		return PeernetRecordPage{}, errors.New("peernet records: invalid network id")
	}
	if limit < 1 {
		return PeernetRecordPage{}, errors.New("peernet records: page limit must be positive")
	}
	limit = min(limit, PeernetListMaxLimit)
	q := `SELECT seq, payload, expires_at FROM peernet_records WHERE network_id = ? AND seq > ?`
	args := []any{networkID, afterSeq}
	if ownOnly {
		q += ` AND source = ?`
		args = append(args, PeernetRecordSourceLocal)
	}
	q += ` ORDER BY seq LIMIT ?`
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return PeernetRecordPage{}, fmt.Errorf("list peernet records: %w", err)
	}
	defer rows.Close()
	now := r.now().Unix()
	out := PeernetRecordPage{HighestSeq: afterSeq}
	scanned := 0
	for rows.Next() {
		var seq, expires int64
		var payload []byte
		if err := rows.Scan(&seq, &payload, &expires); err != nil {
			return PeernetRecordPage{}, fmt.Errorf("scan peernet record: %w", err)
		}
		if scanned == limit {
			out.More = true
			break
		}
		scanned++
		out.HighestSeq = seq
		if expires > now {
			out.Rows = append(out.Rows, PeernetRecordRow{Seq: seq, Payload: payload})
		}
	}
	return out, rows.Err()
}

// ListSince returns the non-expired records of a network with seq above
// afterSeq, in seq order. At most min(limit, PeernetListMaxLimit) rows are
// scanned; expired rows are skipped but still advance HighestSeq so a page of
// expired rows cannot stall a puller.
//
// Why a pager never skips a row: inserts into a network are serialised until
// commit and seq is allocated under that lock (see PeernetRecordRepository),
// so a row that becomes visible later always has a higher seq than every row
// visible earlier. No bounded-lag window is needed.
func (r *PeernetRecordRepository) ListSince(ctx context.Context, networkID string, afterSeq int64, limit int) (PeernetRecordPage, error) {
	return r.page(ctx, networkID, afterSeq, limit, false)
}

// ListOwnSince is ListSince restricted to own (source 0) records, for pushing.
func (r *PeernetRecordRepository) ListOwnSince(ctx context.Context, networkID string, afterSeq int64, limit int) (PeernetRecordPage, error) {
	return r.page(ctx, networkID, afterSeq, limit, true)
}

// PeernetAttestation is one stored health attestation.
type PeernetAttestation struct {
	PublisherNodeID string
	FileID          string
	Healthy         bool
	Backbones       []string
	CheckedAt       time.Time
	CreatedAt       time.Time
	Source          int
	// SegmentsChecked and TotalSegments are the publisher's signed coverage:
	// how many of the file's segments its check covered. Both are 0 for rows
	// stored before coverage was kept (unknown), so a consumer requiring a
	// minimum coverage must treat 0/0 as not meeting it.
	SegmentsChecked int
	TotalSegments   int
}

// AttestationsFor returns the non-expired attestations of a release whose
// checked-at time is not before notBefore, newest check first. An empty fileID
// matches every file of the release. At most 1000 rows are returned.
//
// checked_at is clamped to the receive time on insert, so a publisher cannot
// push its attestations to the front of this ordering with a future check
// time. Choosing between publishers (for example preferring known ones) is the
// consumer's job.
func (r *PeernetRecordRepository) AttestationsFor(ctx context.Context, networkID, releaseID, fileID string, notBefore time.Time) ([]PeernetAttestation, error) {
	if !isLowerHexID(networkID) || !isLowerHexID(releaseID) {
		return nil, errors.New("peernet records: invalid network or release id")
	}
	q := `SELECT publisher_node_id, file_id, healthy, backbones, checked_at, created_at, source,
			segments_checked, total_segments
		FROM peernet_records
		WHERE network_id = ? AND release_id = ? AND kind = ? AND expires_at > ? AND checked_at >= ?`
	args := []any{networkID, releaseID, PeernetRecordKindAttestation, r.now().Unix(), unixOrZero(notBefore)}
	if fileID != "" {
		if !isLowerHexID(fileID) {
			return nil, errors.New("peernet records: invalid file id")
		}
		q += ` AND file_id = ?`
		args = append(args, fileID)
	}
	q += ` ORDER BY checked_at DESC, publisher_node_id, file_id LIMIT ?`
	args = append(args, peernetAttestationsMax)
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query peernet attestations: %w", err)
	}
	defer rows.Close()
	var out []PeernetAttestation
	for rows.Next() {
		var a PeernetAttestation
		var healthy sql.NullInt64
		var bb string
		var checked, created int64
		if err := rows.Scan(&a.PublisherNodeID, &a.FileID, &healthy, &bb, &checked, &created, &a.Source, &a.SegmentsChecked, &a.TotalSegments); err != nil {
			return nil, fmt.Errorf("scan peernet attestation: %w", err)
		}
		a.Healthy = healthy.Valid && healthy.Int64 != 0
		a.Backbones = splitBackbones(bb)
		a.CheckedAt, a.CreatedAt = timeOrZero(checked), timeOrZero(created)
		out = append(out, a)
	}
	return out, rows.Err()
}

// PeernetUnhealthyRef names the file an unhealthy attestation received from a
// peer is about, with the record's sequence number in its network.
type PeernetUnhealthyRef struct {
	Seq       int64
	ReleaseID string
	FileID    string
}

// PeernetUnhealthyPage is one page of UnhealthyAttestationsSince. HighestSeq
// is the highest sequence number returned, or the input cursor when there was
// none; More reports that rows beyond this page exist.
type PeernetUnhealthyPage struct {
	Rows       []PeernetUnhealthyRef
	HighestSeq int64
	More       bool
}

// UnhealthyAttestationsSince returns the non-expired unhealthy attestations
// received from peers (not this node's own) with seq above afterSeq, in seq
// order, at most min(limit, PeernetListMaxLimit) of them. Store HighestSeq as
// the next cursor and each attestation comes up once.
func (r *PeernetRecordRepository) UnhealthyAttestationsSince(ctx context.Context, networkID string, afterSeq int64, limit int) (PeernetUnhealthyPage, error) {
	if !isLowerHexID(networkID) {
		return PeernetUnhealthyPage{}, errors.New("peernet records: invalid network id")
	}
	if limit < 1 {
		return PeernetUnhealthyPage{}, errors.New("peernet records: page limit must be positive")
	}
	limit = min(limit, PeernetListMaxLimit)
	rows, err := r.db.QueryContext(ctx, `SELECT seq, release_id, file_id FROM peernet_records
		WHERE network_id = ? AND seq > ? AND kind = ? AND healthy = 0 AND source = ? AND expires_at > ?
		ORDER BY seq LIMIT ?`,
		networkID, afterSeq, PeernetRecordKindAttestation, PeernetRecordSourcePeer, r.now().Unix(), limit+1)
	if err != nil {
		return PeernetUnhealthyPage{}, fmt.Errorf("list unhealthy peernet attestations: %w", err)
	}
	defer rows.Close()
	out := PeernetUnhealthyPage{HighestSeq: afterSeq}
	for rows.Next() {
		var ref PeernetUnhealthyRef
		if err := rows.Scan(&ref.Seq, &ref.ReleaseID, &ref.FileID); err != nil {
			return PeernetUnhealthyPage{}, fmt.Errorf("scan unhealthy peernet attestation: %w", err)
		}
		if len(out.Rows) == limit {
			out.More = true
			break
		}
		out.Rows = append(out.Rows, ref)
		out.HighestSeq = ref.Seq
	}
	return out, rows.Err()
}

// PeernetHealthSummary aggregates the fresh attestations of one release. A
// publisher counts once per verdict however many files it attested; a
// publisher that attested some files healthy and others unhealthy counts in
// both. Newest* are the latest checked-at times of that verdict.
type PeernetHealthSummary struct {
	HealthyPublishers   int
	UnhealthyPublishers int
	NewestHealthy       time.Time
	NewestUnhealthy     time.Time
}

// HealthSummaryForReleases returns the summary per release for up to
// PeernetMaxReleaseIDsPerQuery releases (ErrPeernetTooManyIDs beyond that),
// counting non-expired attestations checked at or after notBefore. Releases
// without any such attestation are absent from the map.
func (r *PeernetRecordRepository) HealthSummaryForReleases(ctx context.Context, networkID string, releaseIDs []string, notBefore time.Time) (map[string]PeernetHealthSummary, error) {
	if !isLowerHexID(networkID) {
		return nil, errors.New("peernet records: invalid network id")
	}
	seen := make(map[string]struct{}, len(releaseIDs))
	ids := make([]string, 0, len(releaseIDs))
	for _, id := range releaseIDs {
		if !isLowerHexID(id) {
			return nil, errors.New("peernet records: invalid release id")
		}
		if _, dup := seen[id]; !dup {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if len(ids) > PeernetMaxReleaseIDsPerQuery {
		return nil, ErrPeernetTooManyIDs
	}
	out := make(map[string]PeernetHealthSummary, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	args := []any{networkID, PeernetRecordKindAttestation, r.now().Unix(), unixOrZero(notBefore)}
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := r.db.QueryContext(ctx, `SELECT release_id, healthy, COUNT(DISTINCT publisher_node_id), MAX(checked_at)
		FROM peernet_records
		WHERE network_id = ? AND kind = ? AND expires_at > ? AND checked_at >= ? AND healthy IS NOT NULL
		  AND release_id IN (?`+strings.Repeat(",?", len(ids)-1)+`)
		GROUP BY release_id, healthy`, args...)
	if err != nil {
		return nil, fmt.Errorf("query peernet health summary: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var rel string
		var healthy, count, newest int64
		if err := rows.Scan(&rel, &healthy, &count, &newest); err != nil {
			return nil, fmt.Errorf("scan peernet health summary: %w", err)
		}
		s := out[rel]
		if healthy != 0 {
			s.HealthyPublishers, s.NewestHealthy = int(count), timeOrZero(newest)
		} else {
			s.UnhealthyPublishers, s.NewestUnhealthy = int(count), timeOrZero(newest)
		}
		out[rel] = s
	}
	return out, rows.Err()
}

// PeernetCandidate is one stored release record found by media key.
type PeernetCandidate struct {
	Seq             int64
	NetworkID       string
	ReleaseID       string
	PublisherNodeID string
	IMDbID          string
	Season          *int
	Episode         *int
	Title           string
	TotalSize       int64
	CreatedAt       time.Time
	Source          int
}

// CandidatesByMedia returns non-expired release records for a media key across
// the given networks (at most PeernetMaxNetworksPerQuery), newest first, at
// most min(limit, PeernetCandidatesMaxLimit). Season and episode match
// exactly: nil matches only records without one (movies).
func (r *PeernetRecordRepository) CandidatesByMedia(ctx context.Context, networkIDs []string, imdbID string,
	season, episode *int, limit int) ([]PeernetCandidate, error) {
	if imdbID == "" || len(imdbID) > 32 {
		return nil, errors.New("peernet records: invalid media id")
	}
	if len(networkIDs) == 0 {
		return nil, errors.New("peernet records: no networks")
	}
	if len(networkIDs) > PeernetMaxNetworksPerQuery {
		return nil, ErrPeernetTooManyIDs
	}
	if limit < 1 {
		return nil, errors.New("peernet records: limit must be positive")
	}
	limit = min(limit, PeernetCandidatesMaxLimit)
	args := make([]any, 0, len(networkIDs)+6)
	for _, n := range networkIDs {
		if !isLowerHexID(n) {
			return nil, errors.New("peernet records: invalid network id")
		}
		args = append(args, n)
	}
	q := `SELECT seq, network_id, release_id, publisher_node_id, imdb_id, season, episode, title, total_size,
			created_at, source
		FROM peernet_records
		WHERE network_id IN (?` + strings.Repeat(",?", len(networkIDs)-1) + `)
		  AND kind = ? AND imdb_id = ? AND expires_at > ?`
	args = append(args, PeernetRecordKindRelease, imdbID, r.now().Unix())
	for _, f := range []struct {
		col string
		v   *int
	}{{"season", season}, {"episode", episode}} {
		if f.v == nil {
			q += ` AND ` + f.col + ` IS NULL` // col is a compile-time literal
		} else {
			q += ` AND ` + f.col + ` = ?`
			args = append(args, *f.v)
		}
	}
	q += ` ORDER BY created_at DESC, seq DESC, network_id LIMIT ?`
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query peernet candidates: %w", err)
	}
	defer rows.Close()
	var out []PeernetCandidate
	for rows.Next() {
		var c PeernetCandidate
		var s, e sql.NullInt64
		var created int64
		if err := rows.Scan(&c.Seq, &c.NetworkID, &c.ReleaseID, &c.PublisherNodeID, &c.IMDbID, &s, &e, &c.Title,
			&c.TotalSize, &created, &c.Source); err != nil {
			return nil, fmt.Errorf("scan peernet candidate: %w", err)
		}
		if s.Valid {
			v := int(s.Int64)
			c.Season = &v
		}
		if e.Valid {
			v := int(e.Int64)
			c.Episode = &v
		}
		c.CreatedAt = timeOrZero(created)
		out = append(out, c)
	}
	return out, rows.Err()
}

// PeernetRecordCount is the number of non-expired records of one kind and source.
type PeernetRecordCount struct {
	Kind   int
	Source int
	Count  int
}

// PeernetRecordStats summarises one network's non-expired records.
type PeernetRecordStats struct {
	Total      int
	Publishers int // distinct publisher node IDs
	Counts     []PeernetRecordCount
}

// Count returns the records of the given kind and source.
func (s PeernetRecordStats) Count(kind, source int) int {
	for _, c := range s.Counts {
		if c.Kind == kind && c.Source == source {
			return c.Count
		}
	}
	return 0
}

// Stats returns record counts by kind and source plus the number of distinct
// publishers, for a status page.
func (r *PeernetRecordRepository) Stats(ctx context.Context, networkID string) (PeernetRecordStats, error) {
	if !isLowerHexID(networkID) {
		return PeernetRecordStats{}, errors.New("peernet records: invalid network id")
	}
	now := r.now().Unix()
	var out PeernetRecordStats
	rows, err := r.db.QueryContext(ctx, `SELECT kind, source, COUNT(*) FROM peernet_records
		WHERE network_id = ? AND expires_at > ? GROUP BY kind, source ORDER BY kind, source`, networkID, now)
	if err != nil {
		return out, fmt.Errorf("count peernet records: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var c PeernetRecordCount
		if err := rows.Scan(&c.Kind, &c.Source, &c.Count); err != nil {
			return out, fmt.Errorf("scan peernet record count: %w", err)
		}
		out.Counts = append(out.Counts, c)
		out.Total += c.Count
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT publisher_node_id) FROM peernet_records
		WHERE network_id = ? AND expires_at > ?`, networkID, now).Scan(&out.Publishers); err != nil {
		return out, fmt.Errorf("count peernet publishers: %w", err)
	}
	return out, nil
}
