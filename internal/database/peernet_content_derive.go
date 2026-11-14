package database

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// peernetVideoExts are the file extensions the "healthy on the network" label
// treats as a release's main files.
var peernetVideoExts = []string{
	"mkv", "mp4", "m4v", "avi", "mov", "wmv", "mpg", "mpeg", "ts", "m2ts", "webm", "flv", "vob",
}

// GroupSummaries returns up to limit import groups after the given key, in key
// order, each with the number of its files that are not nzb-scheme. It is a
// keyset scan over the group index, so memory is bounded by limit and the
// caller can stop and resume at any key.
func (r *PeernetContentRepository) GroupSummaries(ctx context.Context, after string, limit int) ([]PeernetGroupSummary, error) {
	if limit < 1 {
		return nil, errors.New("peernet content: group limit must be positive")
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT group_key, COUNT(*) FROM peernet_content_index
		 WHERE group_key > ? AND group_key <> '' AND release_scheme <> 1
		 GROUP BY group_key ORDER BY group_key LIMIT ?`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list peernet groups: %w", err)
	}
	defer rows.Close()
	var out []PeernetGroupSummary
	for rows.Next() {
		var g PeernetGroupSummary
		if err := rows.Scan(&g.Key, &g.Files); err != nil {
			return nil, fmt.Errorf("scan peernet group: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// GroupMembers returns the non-nzb-scheme rows of the given groups, ordered by
// group key and then path. The caller bounds the total by the group sizes it
// asked for from GroupSummaries.
func (r *PeernetContentRepository) GroupMembers(ctx context.Context, keys []string) ([]PeernetContentEntry, error) {
	sorted := make([]string, 0, len(keys))
	for _, k := range keys {
		if !isHexID(k) {
			return nil, errors.New("peernet content: invalid group key")
		}
		sorted = append(sorted, strings.ToLower(k))
	}
	sort.Strings(sorted)
	var out []PeernetContentEntry
	for start := 0; start < len(sorted); start += peernetMissingChunk {
		chunk := sorted[start:min(start+peernetMissingChunk, len(sorted))]
		args := make([]any, len(chunk))
		for i, k := range chunk {
			args[i] = k
		}
		rows, err := r.db.QueryContext(ctx,
			`SELECT `+peernetContentCols+` FROM peernet_content_index
			 WHERE release_scheme <> 1 AND group_key IN (?`+strings.Repeat(",?", len(chunk)-1)+`)
			 ORDER BY group_key, file_path`, args...)
		if err != nil {
			return nil, fmt.Errorf("list peernet group members: %w", err)
		}
		for rows.Next() {
			e, err := scanContent(rows)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan peernet group member: %w", err)
			}
			out = append(out, e)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ApplyDerived writes derived release IDs in one short transaction and returns
// how many rows changed. A row is only touched while it is still the row the
// caller read (same file ID and group key) and is not nzb-scheme, so a
// concurrent re-index or a store-based ID always wins. An empty ReleaseID
// clears the row's derived ID.
func (r *PeernetContentRepository) ApplyDerived(ctx context.Context, updates []PeernetDerivedUpdate) (int, error) {
	if len(updates) == 0 {
		return 0, nil
	}
	for _, u := range updates {
		if !isHexID(u.FileID) || !isHexID(u.GroupKey) || (u.ReleaseID != "" && !isHexID(u.ReleaseID)) {
			return 0, errors.New("peernet content: invalid derived update")
		}
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("apply derived release ids: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	stmt, err := tx.PrepareContext(ctx,
		`UPDATE peernet_content_index SET release_id = ?, release_scheme = ?
		 WHERE file_path = ? AND file_id = ? AND group_key = ? AND release_scheme <> 1
		   AND (release_id <> ? OR release_scheme <> ?)`)
	if err != nil {
		return 0, fmt.Errorf("apply derived release ids: prepare: %w", err)
	}
	defer stmt.Close()
	changed := 0
	for _, u := range updates {
		rel, scheme := strings.ToLower(u.ReleaseID), PeernetSchemeNone
		if rel != "" {
			scheme = PeernetSchemeFiles
		}
		res, err := stmt.ExecContext(ctx, rel, scheme, normalizePeernetPath(u.FilePath),
			strings.ToLower(u.FileID), strings.ToLower(u.GroupKey), rel, scheme)
		if err != nil {
			return 0, fmt.Errorf("apply derived release id %q: %w", u.FilePath, err)
		}
		n, err := rowsAffected(res, "apply derived release id")
		if err != nil {
			return 0, err
		}
		changed += int(n)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("apply derived release ids: commit: %w", err)
	}
	return changed, nil
}

// StillIndexed reports which of the given release IDs are carried by at least
// one indexed file.
func (r *PeernetContentRepository) StillIndexed(ctx context.Context, releaseIDs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(releaseIDs))
	ids := make([]string, 0, len(releaseIDs))
	for _, id := range releaseIDs {
		if !isHexID(id) {
			return nil, errors.New("peernet content: invalid release id")
		}
		ids = append(ids, strings.ToLower(id))
	}
	for start := 0; start < len(ids); start += peernetMissingChunk {
		chunk := ids[start:min(start+peernetMissingChunk, len(ids))]
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := r.db.QueryContext(ctx,
			`SELECT DISTINCT release_id FROM peernet_content_index WHERE release_id IN (?`+strings.Repeat(",?", len(chunk)-1)+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("query indexed peernet releases: %w", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan indexed release: %w", err)
			}
			out[id] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// RefsByPrefix returns the group and release identity of dir and everything
// beneath it, at most limit rows. It refuses an empty directory.
func (r *PeernetContentRepository) RefsByPrefix(ctx context.Context, dir string, limit int) ([]PeernetContentRef, error) {
	d := normalizePeernetPath(dir)
	if d == "" {
		return nil, errors.New("peernet content: empty directory prefix")
	}
	if limit < 1 {
		return nil, errors.New("peernet content: limit must be positive")
	}
	pre := d + "/"
	rows, err := r.db.QueryContext(ctx,
		`SELECT group_key, release_id, release_scheme FROM peernet_content_index
		 WHERE file_path = ? OR substr(file_path, 1, ?) = ? LIMIT ?`,
		d, utf8.RuneCountInString(pre), pre, limit)
	if err != nil {
		return nil, fmt.Errorf("list peernet content under %q: %w", d, err)
	}
	defer rows.Close()
	var out []PeernetContentRef
	for rows.Next() {
		var ref PeernetContentRef
		if err := rows.Scan(&ref.GroupKey, &ref.ReleaseID, &ref.ReleaseScheme); err != nil {
			return nil, fmt.Errorf("scan peernet content ref: %w", err)
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

// RefByPath returns the group and release identity of one path.
func (r *PeernetContentRepository) RefByPath(ctx context.Context, filePath string) (PeernetContentRef, bool, error) {
	e, ok, err := r.GetByPath(ctx, filePath)
	if err != nil || !ok {
		return PeernetContentRef{}, false, err
	}
	return PeernetContentRef{GroupKey: e.GroupKey, ReleaseID: e.ReleaseID, ReleaseScheme: e.ReleaseScheme}, true, nil
}

// VideoFilesOfReleases returns, per release, the File IDs of its video files
// in path order, at most perRelease+1 each (the extra one tells the caller the
// release has more than it can label). It is one query for up to
// PeernetMaxPathsPerQuery releases. Releases without a video file are absent.
func (r *PeernetContentRepository) VideoFilesOfReleases(ctx context.Context, releaseIDs []string, perRelease int) (map[string][]string, error) {
	if perRelease < 1 {
		return nil, errors.New("peernet content: per-release limit must be positive")
	}
	seen := make(map[string]struct{}, len(releaseIDs))
	ids := make([]string, 0, len(releaseIDs))
	for _, id := range releaseIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if !isHexID(id) {
			return nil, errors.New("peernet content: invalid release id")
		}
		ids = append(ids, strings.ToLower(id))
	}
	if len(ids) > PeernetMaxPathsPerQuery {
		return nil, ErrPeernetTooManyIDs
	}
	out := make(map[string][]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(ids)+1)
	for _, id := range ids {
		args = append(args, id)
	}
	args = append(args, perRelease+1)
	conds := make([]string, len(peernetVideoExts))
	for i, e := range peernetVideoExts {
		conds[i] = `lower(file_path) LIKE '%.` + e + `'` // e is a compile-time literal
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT release_id, file_id FROM (
			SELECT release_id, file_id, ROW_NUMBER() OVER (PARTITION BY release_id ORDER BY file_path) AS rn
			FROM peernet_content_index
			WHERE release_id IN (?`+strings.Repeat(",?", len(ids)-1)+`) AND (`+strings.Join(conds, " OR ")+`)
		) ranked WHERE rn <= ? ORDER BY release_id, rn`, args...)
	if err != nil {
		return nil, fmt.Errorf("query peernet release video files: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var rel, file string
		if err := rows.Scan(&rel, &file); err != nil {
			return nil, fmt.Errorf("scan peernet release video file: %w", err)
		}
		out[rel] = append(out[rel], file)
	}
	return out, rows.Err()
}

// GroupCounts returns, for the given groups, how many files are not
// nzb-scheme. Empty groups are absent. It lets a caller refuse a huge group
// before reading its members.
func (r *PeernetContentRepository) GroupCounts(ctx context.Context, keys []string) (map[string]int, error) {
	out := make(map[string]int, len(keys))
	norm := make([]string, 0, len(keys))
	for _, k := range keys {
		if !isHexID(k) {
			return nil, errors.New("peernet content: invalid group key")
		}
		norm = append(norm, strings.ToLower(k))
	}
	for start := 0; start < len(norm); start += peernetMissingChunk {
		chunk := norm[start:min(start+peernetMissingChunk, len(norm))]
		args := make([]any, len(chunk))
		for i, k := range chunk {
			args[i] = k
		}
		rows, err := r.db.QueryContext(ctx,
			`SELECT group_key, COUNT(*) FROM peernet_content_index
			 WHERE release_scheme <> 1 AND group_key IN (?`+strings.Repeat(",?", len(chunk)-1)+`)
			 GROUP BY group_key`, args...)
		if err != nil {
			return nil, fmt.Errorf("count peernet groups: %w", err)
		}
		for rows.Next() {
			var k string
			var n int
			if err := rows.Scan(&k, &n); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan peernet group count: %w", err)
			}
			out[k] = n
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// peernetClearBatch is the most rows ClearDerivedGroup updates per transaction.
const peernetClearBatch = 1000

// ClearDerivedGroup removes the derived release ID from every row of one
// group, in short transactions of at most batch rows (default 1000), and
// returns the distinct IDs it removed. nzb-scheme rows are never touched.
func (r *PeernetContentRepository) ClearDerivedGroup(ctx context.Context, key string, batch int) ([]string, error) {
	if !isHexID(key) {
		return nil, errors.New("peernet content: invalid group key")
	}
	if batch < 1 {
		batch = peernetClearBatch
	}
	key = strings.ToLower(key)
	rows, err := r.db.QueryContext(ctx,
		`SELECT DISTINCT release_id FROM peernet_content_index WHERE group_key = ? AND release_scheme = 2`, key)
	if err != nil {
		return nil, fmt.Errorf("list derived release ids: %w", err)
	}
	var old []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan derived release id: %w", err)
		}
		old = append(old, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for {
		res, err := r.db.ExecContext(ctx,
			`UPDATE peernet_content_index SET release_id = '', release_scheme = 0
			 WHERE file_path IN (SELECT file_path FROM peernet_content_index
			                     WHERE group_key = ? AND release_scheme = 2 LIMIT ?)`, key, batch)
		if err != nil {
			return old, fmt.Errorf("clear derived release ids: %w", err)
		}
		n, err := rowsAffected(res, "clear derived release ids")
		if err != nil {
			return old, err
		}
		if int(n) < batch {
			return old, nil
		}
	}
}
