package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// peernetMissingChunk bounds the number of bind variables per MissingPaths query.
const peernetMissingChunk = 500

// PeernetContentEntry maps one virtual file path to its content identifiers.
// IDs are 64-character hex strings (see internal/peernet/contentid). ReleaseID
// is empty for files whose metadata has no shared NZB store.
type PeernetContentEntry struct {
	FilePath  string
	FileID    string
	ReleaseID string
	// ReleaseScheme says how ReleaseID was derived: PeernetSchemeNone (no
	// release ID), PeernetSchemeNZB or PeernetSchemeFiles. A ReleaseID given
	// with no scheme is stored as PeernetSchemeNZB.
	ReleaseScheme int
	// GroupKey is a local-only 64-hex key shared by the files of one import
	// (empty when the metadata names no source). It never leaves the node.
	GroupKey string
}

// Release ID schemes stored in peernet_content_index.release_scheme and in
// peernet_records.release_scheme.
const (
	PeernetSchemeNone  = 0
	PeernetSchemeNZB   = 1
	PeernetSchemeFiles = 2
)

// PeernetGroupSummary is one import group and how many of its files still
// need a derived release ID decision (files that are not nzb-scheme).
type PeernetGroupSummary struct {
	Key   string
	Files int
}

// PeernetDerivedUpdate sets (or, with an empty ReleaseID, clears) the derived
// release ID of one row. FileID and GroupKey guard against a row that changed
// since the group was read.
type PeernetDerivedUpdate struct {
	FilePath  string
	FileID    string
	GroupKey  string
	ReleaseID string
}

// PeernetContentRef is the group and release identity of one row.
type PeernetContentRef struct {
	GroupKey      string
	ReleaseID     string
	ReleaseScheme int
}

// PeernetContentRepository stores the local index from virtual file path to
// File ID and Release ID (table peernet_content_index).
type PeernetContentRepository struct {
	db      *dialectAwareDB
	dialect dialectHelper
}

// NewPeernetContentRepository creates a repository over conn.
func NewPeernetContentRepository(conn *sql.DB, d Dialect) *PeernetContentRepository {
	return &PeernetContentRepository{
		db:      newDialectAwareDB(conn, d),
		dialect: dialectHelper{d: d},
	}
}

// peernetContentCols is the column list every row read uses; scanContent reads it.
const peernetContentCols = `file_path, file_id, release_id, release_scheme, group_key`

const peernetUpsertSQL = `
	INSERT INTO peernet_content_index (file_path, file_id, release_id, release_scheme, group_key, indexed_at)
	VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	ON CONFLICT (file_path) DO UPDATE SET
		file_id = excluded.file_id,
		release_id = excluded.release_id,
		release_scheme = excluded.release_scheme,
		group_key = excluded.group_key,
		indexed_at = CURRENT_TIMESTAMP`

func scanContent(r rowScanner) (PeernetContentEntry, error) {
	var e PeernetContentEntry
	err := r.Scan(&e.FilePath, &e.FileID, &e.ReleaseID, &e.ReleaseScheme, &e.GroupKey)
	return e, err
}

// normalizePeernetPath folds a virtual path to the form used as the row key:
// forward slashes, cleaned, no leading slash.
func normalizePeernetPath(p string) string {
	v := path.Clean(strings.ReplaceAll(p, "\\", "/"))
	v = strings.TrimPrefix(v, "/")
	if v == "." {
		return ""
	}
	return v
}

// isHexID reports whether s is a 64-character hex ID (either case). The check
// itself lives in netid so every layer shares one implementation.
func isHexID(s string) bool { return netid.IsHexID(s, false) }

// prepare validates e and returns it with a normalised path and lower-cased IDs.
func (e PeernetContentEntry) prepare() (PeernetContentEntry, error) {
	e.FilePath = normalizePeernetPath(e.FilePath)
	if e.FilePath == "" {
		return e, errors.New("peernet content: empty file path")
	}
	if !isHexID(e.FileID) {
		return e, fmt.Errorf("peernet content: invalid file id for %q", e.FilePath)
	}
	if e.ReleaseID != "" && !isHexID(e.ReleaseID) {
		return e, fmt.Errorf("peernet content: invalid release id for %q", e.FilePath)
	}
	if e.ReleaseID != "" && e.ReleaseScheme == PeernetSchemeNone {
		e.ReleaseScheme = PeernetSchemeNZB // a release ID with no scheme is the original kind
	}
	switch {
	case e.ReleaseScheme < PeernetSchemeNone || e.ReleaseScheme > PeernetSchemeFiles:
		return e, fmt.Errorf("peernet content: invalid release scheme for %q", e.FilePath)
	case (e.ReleaseID == "") != (e.ReleaseScheme == PeernetSchemeNone):
		return e, fmt.Errorf("peernet content: release id and scheme disagree for %q", e.FilePath)
	case e.GroupKey != "" && !isHexID(e.GroupKey):
		return e, fmt.Errorf("peernet content: invalid group key for %q", e.FilePath)
	}
	e.FileID = strings.ToLower(e.FileID)
	e.ReleaseID = strings.ToLower(e.ReleaseID)
	e.GroupKey = strings.ToLower(e.GroupKey)
	return e, nil
}

// Upsert inserts or replaces the row for e.FilePath.
func (r *PeernetContentRepository) Upsert(ctx context.Context, e PeernetContentEntry) error {
	e, err := e.prepare()
	if err != nil {
		return err
	}
	if _, err := r.db.ExecContext(ctx, peernetUpsertSQL, e.FilePath, e.FileID, e.ReleaseID, e.ReleaseScheme, e.GroupKey); err != nil {
		return fmt.Errorf("upsert peernet content %q: %w", e.FilePath, err)
	}
	return nil
}

// UpsertBatch upserts all entries in one transaction. Nothing is written if
// any entry is invalid.
func (r *PeernetContentRepository) UpsertBatch(ctx context.Context, entries []PeernetContentEntry) error {
	if len(entries) == 0 {
		return nil
	}
	prepared := make([]PeernetContentEntry, len(entries))
	for i, e := range entries {
		p, err := e.prepare()
		if err != nil {
			return err
		}
		prepared[i] = p
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("upsert peernet content batch: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	stmt, err := tx.PrepareContext(ctx, peernetUpsertSQL)
	if err != nil {
		return fmt.Errorf("upsert peernet content batch: prepare: %w", err)
	}
	defer stmt.Close()
	for _, e := range prepared {
		if _, err := stmt.ExecContext(ctx, e.FilePath, e.FileID, e.ReleaseID, e.ReleaseScheme, e.GroupKey); err != nil {
			return fmt.Errorf("upsert peernet content %q: %w", e.FilePath, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("upsert peernet content batch: commit: %w", err)
	}
	return nil
}

// GetByPath returns the row of one virtual path; ok is false when the path is
// not indexed.
func (r *PeernetContentRepository) GetByPath(ctx context.Context, filePath string) (PeernetContentEntry, bool, error) {
	p := normalizePeernetPath(filePath)
	if p == "" {
		return PeernetContentEntry{}, false, errors.New("peernet content: empty file path")
	}
	e, err := scanContent(r.db.QueryRowContext(ctx,
		`SELECT `+peernetContentCols+` FROM peernet_content_index WHERE file_path = ?`, p))
	if errors.Is(err, sql.ErrNoRows) {
		return PeernetContentEntry{}, false, nil
	}
	if err != nil {
		return PeernetContentEntry{}, false, fmt.Errorf("get peernet content %q: %w", p, err)
	}
	return e, true, nil
}

// PeernetMaxPathsPerQuery bounds ReleaseIDsByPaths.
const PeernetMaxPathsPerQuery = 200

// ReleaseIDsByPaths returns the release ID of each indexed path that has one,
// keyed by the path exactly as given. It is one query for up to
// PeernetMaxPathsPerQuery distinct paths (ErrPeernetTooManyIDs beyond that).
func (r *PeernetContentRepository) ReleaseIDsByPaths(ctx context.Context, paths []string) (map[string]string, error) {
	byNorm := make(map[string][]string, len(paths))
	for _, p := range paths {
		if n := normalizePeernetPath(p); n != "" {
			byNorm[n] = append(byNorm[n], p)
		}
	}
	out := make(map[string]string, len(paths))
	if len(byNorm) == 0 {
		return out, nil
	}
	if len(byNorm) > PeernetMaxPathsPerQuery {
		return nil, ErrPeernetTooManyIDs
	}
	args := make([]any, 0, len(byNorm))
	for n := range byNorm {
		args = append(args, n)
	}
	rows, err := r.db.QueryContext(ctx, `SELECT file_path, release_id FROM peernet_content_index
		WHERE release_id <> '' AND file_path IN (?`+strings.Repeat(",?", len(args)-1)+`)`, args...)
	if err != nil {
		return nil, fmt.Errorf("query peernet release ids by path: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var path, rel string
		if err := rows.Scan(&path, &rel); err != nil {
			return nil, fmt.Errorf("scan peernet release id: %w", err)
		}
		for _, orig := range byNorm[path] {
			out[orig] = rel
		}
	}
	return out, rows.Err()
}

// FindByFileID returns every indexed path carrying fileID, ordered by path.
func (r *PeernetContentRepository) FindByFileID(ctx context.Context, fileID string) ([]PeernetContentEntry, error) {
	if !isHexID(fileID) {
		return nil, errors.New("peernet content: invalid file id")
	}
	return r.find(ctx, "file_id", strings.ToLower(fileID))
}

// FindByReleaseID returns every indexed path of releaseID, ordered by path.
func (r *PeernetContentRepository) FindByReleaseID(ctx context.Context, releaseID string) ([]PeernetContentEntry, error) {
	if !isHexID(releaseID) {
		return nil, errors.New("peernet content: invalid release id")
	}
	return r.find(ctx, "release_id", strings.ToLower(releaseID))
}

func (r *PeernetContentRepository) find(ctx context.Context, column, id string) ([]PeernetContentEntry, error) {
	// column is one of two compile-time constants, never caller input.
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+peernetContentCols+` FROM peernet_content_index WHERE `+column+` = ? ORDER BY file_path`, id)
	if err != nil {
		return nil, fmt.Errorf("query peernet content by %s: %w", column, err)
	}
	defer rows.Close()
	var out []PeernetContentEntry
	for rows.Next() {
		e, err := scanContent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan peernet content: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Delete removes the row for filePath. An absent row is not an error.
func (r *PeernetContentRepository) Delete(ctx context.Context, filePath string) error {
	p := normalizePeernetPath(filePath)
	if p == "" {
		return errors.New("peernet content: empty file path")
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM peernet_content_index WHERE file_path = ?`, p); err != nil {
		return fmt.Errorf("delete peernet content %q: %w", p, err)
	}
	return nil
}

// DeleteByPrefix removes dir itself and everything beneath it. It refuses an
// empty directory so a bad caller cannot wipe the whole index.
//
// The match is an exact, case-sensitive comparison of the leading characters
// (substr equality), not LIKE: LIKE is case-insensitive for ASCII on SQLite
// and case-sensitive on PostgreSQL, so "Movies" would also have removed
// "movies/..." on one backend only.
func (r *PeernetContentRepository) DeleteByPrefix(ctx context.Context, dir string) error {
	d := normalizePeernetPath(dir)
	if d == "" {
		return errors.New("peernet content: empty directory prefix")
	}
	pre := d + "/"
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM peernet_content_index WHERE file_path = ? OR substr(file_path, 1, ?) = ?`,
		d, utf8.RuneCountInString(pre), pre); err != nil {
		return fmt.Errorf("delete peernet content under %q: %w", d, err)
	}
	return nil
}

// RenamePrefix moves the rows of oldDir (itself and everything beneath it) to
// newDir in one transaction, keeping their identifiers: a rename changes no
// file's content. Rows already present under newDir are replaced.
func (r *PeernetContentRepository) RenamePrefix(ctx context.Context, oldDir, newDir string) error {
	o, n := normalizePeernetPath(oldDir), normalizePeernetPath(newDir)
	if o == "" || n == "" {
		return errors.New("peernet content: empty directory prefix")
	}
	if o == n {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("rename peernet content: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	nPre := n + "/"
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM peernet_content_index WHERE file_path = ? OR substr(file_path, 1, ?) = ?`,
		n, utf8.RuneCountInString(nPre), nPre); err != nil {
		return fmt.Errorf("rename peernet content: clear destination: %w", err)
	}
	oPre := o + "/"
	if _, err := tx.ExecContext(ctx,
		`UPDATE peernet_content_index SET file_path = CAST(? AS TEXT) || substr(file_path, ?)
		 WHERE file_path = ? OR substr(file_path, 1, ?) = ?`,
		n, utf8.RuneCountInString(o)+1, o, utf8.RuneCountInString(oPre), oPre); err != nil {
		return fmt.Errorf("rename peernet content %q: %w", o, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("rename peernet content: commit: %w", err)
	}
	return nil
}

// ListPage returns up to limit rows with file_path greater than after, in path
// order. Pass the last path of a page as after to read the next one; an empty
// after starts from the beginning. It is a keyset scan, so rows may be
// deleted or rewritten between pages.
func (r *PeernetContentRepository) ListPage(ctx context.Context, after string, limit int) ([]PeernetContentEntry, error) {
	if limit < 1 {
		return nil, errors.New("peernet content: page limit must be positive")
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+peernetContentCols+` FROM peernet_content_index
		 WHERE file_path > ? ORDER BY file_path LIMIT ?`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list peernet content page: %w", err)
	}
	defer rows.Close()
	var out []PeernetContentEntry
	for rows.Next() {
		e, err := scanContent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan peernet content: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MissingPaths returns the candidates (normalised, de-duplicated, in input
// order) that have no row in the index. It is the backfill's work list.
func (r *PeernetContentRepository) MissingPaths(ctx context.Context, candidates []string) ([]string, error) {
	seen := make(map[string]struct{}, len(candidates))
	norm := make([]string, 0, len(candidates))
	for _, c := range candidates {
		p := normalizePeernetPath(c)
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		norm = append(norm, p)
	}

	present := make(map[string]struct{})
	for start := 0; start < len(norm); start += peernetMissingChunk {
		chunk := norm[start:min(start+peernetMissingChunk, len(norm))]
		args := make([]any, len(chunk))
		for i, p := range chunk {
			args[i] = p
		}
		q := `SELECT file_path FROM peernet_content_index WHERE file_path IN (?` +
			strings.Repeat(",?", len(chunk)-1) + `)`
		rows, err := r.db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("query indexed peernet paths: %w", err)
		}
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan indexed path: %w", err)
			}
			present[p] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	var out []string
	for _, p := range norm {
		if _, ok := present[p]; !ok {
			out = append(out, p)
		}
	}
	return out, nil
}

// PeernetContentCounts summarises the index: every indexed file and how many
// of them have a Release ID, by scheme. NoSourceGroup counts files with no
// Release ID whose metadata names no source (they can never be grouped).
type PeernetContentCounts struct {
	Files, WithReleaseID                   int
	WithNZBReleaseID, WithDerivedReleaseID int
	NoSourceGroup                          int
}

// Counts returns the size of the index.
func (r *PeernetContentRepository) Counts(ctx context.Context) (PeernetContentCounts, error) {
	var c PeernetContentCounts
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*),
			COALESCE(SUM(CASE WHEN release_id <> '' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN release_scheme = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN release_scheme = 2 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN release_id = '' AND group_key = '' THEN 1 ELSE 0 END), 0)
		 FROM peernet_content_index`).
		Scan(&c.Files, &c.WithReleaseID, &c.WithNZBReleaseID, &c.WithDerivedReleaseID, &c.NoSourceGroup)
	if err != nil {
		return PeernetContentCounts{}, fmt.Errorf("count peernet content: %w", err)
	}
	return c, nil
}
