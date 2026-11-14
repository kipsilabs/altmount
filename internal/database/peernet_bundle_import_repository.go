package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Bundle import states.
const (
	PeernetBundleStatePending = "pending"
	PeernetBundleStatePassed  = "passed"
	PeernetBundleStateFailed  = "failed"
)

// PeernetBundleImport is one virtual file that came from a peer bundle.
type PeernetBundleImport struct {
	FilePath      string
	FileID        string
	ReleaseID     string
	NetworkID     string
	PublisherID   string
	QueueID       int64
	State         string
	ArrOwed       bool
	ArrNotified   bool
	Category      string
	TargetPath    string
	ResultingPath string
	// Committed is set once every file of the import was written; a row
	// that is not committed belongs to a write that never finished.
	Committed bool
}

// PeernetBundleImportRepository stores the peer-bundle import state.
type PeernetBundleImportRepository struct{ db *dialectAwareDB }

// NewPeernetBundleImportRepository creates a repository over conn.
func NewPeernetBundleImportRepository(conn *sql.DB, d Dialect) *PeernetBundleImportRepository {
	return &PeernetBundleImportRepository{db: newDialectAwareDB(conn, d)}
}

const bundleImportCols = `file_path, file_id, release_id, network_id, publisher_id, queue_id, state, arr_owed, arr_notified, category, target_path, resulting_path, committed`

// maxBundleImportBatch bounds an IN list.
const maxBundleImportBatch = 500

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func scanBundleImport(sc interface{ Scan(...any) error }) (PeernetBundleImport, error) {
	var b PeernetBundleImport
	var owed, notified, committed int
	err := sc.Scan(&b.FilePath, &b.FileID, &b.ReleaseID, &b.NetworkID, &b.PublisherID, &b.QueueID, &b.State,
		&owed, &notified, &b.Category, &b.TargetPath, &b.ResultingPath, &committed)
	b.ArrOwed, b.ArrNotified, b.Committed = owed != 0, notified != 0, committed != 0
	return b, err
}

// Upsert stores the rows as pending, replacing any row of the same path. The
// path is slash-normalised like every other virtual path.
func (r *PeernetBundleImportRepository) Upsert(ctx context.Context, rows []PeernetBundleImport) error {
	now := time.Now().Unix()
	for _, row := range rows {
		p := normalizeHealthPath(row.FilePath)
		if p == "" {
			return errors.New("peernet bundle import: empty file path")
		}
		_, err := r.db.ExecContext(ctx, `INSERT INTO peernet_bundle_imports
			(file_path, file_id, release_id, network_id, publisher_id, queue_id, state, arr_owed, arr_notified, category, target_path, resulting_path, committed, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, 0, ?, ?, ?, 0, ?, ?)
			ON CONFLICT(file_path) DO UPDATE SET file_id = excluded.file_id, release_id = excluded.release_id,
			  network_id = excluded.network_id, publisher_id = excluded.publisher_id, queue_id = excluded.queue_id,
			  state = 'pending', arr_owed = excluded.arr_owed, arr_notified = 0, category = excluded.category,
			  target_path = excluded.target_path, resulting_path = excluded.resulting_path, committed = 0, updated_at = excluded.updated_at`,
			p, row.FileID, row.ReleaseID, row.NetworkID, row.PublisherID, row.QueueID, b2i(row.ArrOwed),
			row.Category, row.TargetPath, row.ResultingPath, now, now)
		if err != nil {
			return fmt.Errorf("peernet bundle import: upsert: %w", err)
		}
	}
	return nil
}

// Get returns the row of one path.
func (r *PeernetBundleImportRepository) Get(ctx context.Context, filePath string) (PeernetBundleImport, bool, error) {
	row, err := scanBundleImport(r.db.QueryRowContext(ctx,
		`SELECT `+bundleImportCols+` FROM peernet_bundle_imports WHERE file_path = ?`, normalizeHealthPath(filePath)))
	if errors.Is(err, sql.ErrNoRows) {
		return PeernetBundleImport{}, false, nil
	}
	if err != nil {
		return PeernetBundleImport{}, false, fmt.Errorf("peernet bundle import: get: %w", err)
	}
	return row, true, nil
}

func inList(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }

// PendingAmong returns the rows among paths whose full check has not passed
// (state pending), keyed by normalised path.
func (r *PeernetBundleImportRepository) PendingAmong(ctx context.Context, paths []string) (map[string]PeernetBundleImport, error) {
	out := map[string]PeernetBundleImport{}
	for len(paths) > 0 {
		n := min(len(paths), maxBundleImportBatch)
		args := make([]any, n)
		for i, p := range paths[:n] {
			args[i] = normalizeHealthPath(p)
		}
		paths = paths[n:]
		rows, err := r.db.QueryContext(ctx, `SELECT `+bundleImportCols+` FROM peernet_bundle_imports
			WHERE state = 'pending' AND file_path IN (`+inList(n)+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("peernet bundle import: pending: %w", err)
		}
		for rows.Next() {
			b, err := scanBundleImport(rows)
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("peernet bundle import: scan: %w", err)
			}
			out[b.FilePath] = b
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	return out, nil
}

// QueueHasPending reports whether any file of the queue item still awaits its
// full check.
func (r *PeernetBundleImportRepository) QueueHasPending(ctx context.Context, queueID int64) (bool, error) {
	if queueID <= 0 {
		return false, nil
	}
	var n int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM peernet_bundle_imports WHERE queue_id = ? AND state = 'pending'`, queueID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("peernet bundle import: queue pending: %w", err)
	}
	return n > 0, nil
}

func (r *PeernetBundleImportRepository) transition(ctx context.Context, filePath, to string) (bool, error) {
	res, err := r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports SET state = ?, updated_at = ?
		WHERE file_path = ? AND state = 'pending'`, to, time.Now().Unix(), normalizeHealthPath(filePath))
	if err != nil {
		return false, fmt.Errorf("peernet bundle import: %s: %w", to, err)
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// MarkPassed moves a pending file to passed. It reports whether this call did
// it (a file that already passed or failed is left alone).
func (r *PeernetBundleImportRepository) MarkPassed(ctx context.Context, filePath string) (bool, error) {
	return r.transition(ctx, filePath, PeernetBundleStatePassed)
}

// MarkFailed moves a pending file to failed, with the same compare-and-set.
func (r *PeernetBundleImportRepository) MarkFailed(ctx context.Context, filePath string) (bool, error) {
	return r.transition(ctx, filePath, PeernetBundleStateFailed)
}

// ClaimAnnouncement is the single winner's ticket to tell the ARRs about a
// queue item: every file of it passed, the announcement is owed and nobody
// claimed it yet. Concurrent callers cannot both win. It returns one of the
// item's rows, which carries what the notification needs.
func (r *PeernetBundleImportRepository) ClaimAnnouncement(ctx context.Context, queueID int64) (PeernetBundleImport, bool, error) {
	if queueID <= 0 {
		return PeernetBundleImport{}, false, nil
	}
	res, err := r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports SET arr_notified = 1, updated_at = ?
		WHERE queue_id = ? AND arr_owed = 1 AND arr_notified = 0
		  AND NOT EXISTS (SELECT 1 FROM peernet_bundle_imports x WHERE x.queue_id = ? AND (x.state <> 'passed' OR x.committed = 0))`,
		time.Now().Unix(), queueID, queueID)
	if err != nil {
		return PeernetBundleImport{}, false, fmt.Errorf("peernet bundle import: claim: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return PeernetBundleImport{}, false, nil
	}
	row, err := scanBundleImport(r.db.QueryRowContext(ctx, `SELECT `+bundleImportCols+
		` FROM peernet_bundle_imports WHERE queue_id = ? ORDER BY file_path LIMIT 1`, queueID))
	if err != nil {
		return PeernetBundleImport{}, false, fmt.Errorf("peernet bundle import: claim read: %w", err)
	}
	return row, true, nil
}

// ReleaseAnnouncement undoes a claim whose notification could not be sent, so
// a later pass tries again.
func (r *PeernetBundleImportRepository) ReleaseAnnouncement(ctx context.Context, queueID int64) error {
	_, err := r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports SET arr_notified = 0 WHERE queue_id = ?`, queueID)
	return err
}

// QueuesAwaitingAnnouncement lists the queue items whose files all passed but
// whose announcement was never claimed (a restart between the two).
func (r *PeernetBundleImportRepository) QueuesAwaitingAnnouncement(ctx context.Context) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT queue_id FROM peernet_bundle_imports
		WHERE queue_id <> 0 AND arr_owed = 1 AND arr_notified = 0
		  AND NOT EXISTS (SELECT 1 FROM peernet_bundle_imports x WHERE x.queue_id = peernet_bundle_imports.queue_id AND (x.state <> 'passed' OR x.committed = 0))
		ORDER BY queue_id LIMIT 1000`)
	if err != nil {
		return nil, fmt.Errorf("peernet bundle import: awaiting: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// StatesForQueues maps queue ids that have bundle rows to "still pending".
func (r *PeernetBundleImportRepository) StatesForQueues(ctx context.Context, ids []int64) (map[int64]bool, error) {
	out := map[int64]bool{}
	for len(ids) > 0 {
		n := min(len(ids), maxBundleImportBatch)
		args := make([]any, n)
		for i, id := range ids[:n] {
			args[i] = id
		}
		ids = ids[n:]
		rows, err := r.db.QueryContext(ctx, `SELECT queue_id, MAX(CASE WHEN state = 'pending' THEN 1 ELSE 0 END)
			FROM peernet_bundle_imports WHERE queue_id IN (`+inList(n)+`) GROUP BY queue_id`, args...)
		if err != nil {
			return nil, fmt.Errorf("peernet bundle import: queue states: %w", err)
		}
		for rows.Next() {
			var id int64
			var pending int
			if err := rows.Scan(&id, &pending); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out[id] = pending != 0
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// StatesForPaths maps paths that have a bundle row to "still pending".
func (r *PeernetBundleImportRepository) StatesForPaths(ctx context.Context, paths []string) (map[string]bool, error) {
	out := map[string]bool{}
	for len(paths) > 0 {
		n := min(len(paths), maxBundleImportBatch)
		args := make([]any, n)
		for i, p := range paths[:n] {
			args[i] = normalizeHealthPath(p)
		}
		paths = paths[n:]
		rows, err := r.db.QueryContext(ctx, `SELECT file_path, state FROM peernet_bundle_imports
			WHERE file_path IN (`+inList(n)+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("peernet bundle import: path states: %w", err)
		}
		for rows.Next() {
			var p, state string
			if err := rows.Scan(&p, &state); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out[p] = state == PeernetBundleStatePending
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Delete removes the rows of the given paths.
func (r *PeernetBundleImportRepository) Delete(ctx context.Context, paths []string) error {
	for len(paths) > 0 {
		n := min(len(paths), maxBundleImportBatch)
		args := make([]any, n)
		for i, p := range paths[:n] {
			args[i] = normalizeHealthPath(p)
		}
		paths = paths[n:]
		if _, err := r.db.ExecContext(ctx, `DELETE FROM peernet_bundle_imports WHERE file_path IN (`+inList(n)+`)`, args...); err != nil {
			return fmt.Errorf("peernet bundle import: delete: %w", err)
		}
	}
	return nil
}

// SetQueueHold sets or clears the queue item's peer_hold mark. A held item
// shows in the SABnzbd-compatible queue as work in progress and never in its
// history; the user's own skip_arr_notification flag is not touched.
func (r *PeernetBundleImportRepository) SetQueueHold(ctx context.Context, queueID int64, held bool) error {
	_, err := r.db.ExecContext(ctx, `UPDATE import_queue SET peer_hold = ?, updated_at = ? WHERE id = ?`,
		b2i(held), time.Now().UTC(), queueID)
	if err != nil {
		return fmt.Errorf("peernet bundle import: set hold: %w", err)
	}
	return nil
}

// FailHeldQueueItem turns a held queue item into a failed one with a short
// message, so the SABnzbd-compatible history reports it as failed (the ARR then
// blocklists the release and searches again). Its provisional history rows go
// with it, so nothing can later resurface as a completed import. It reports
// whether an item was held and changed.
func (r *PeernetBundleImportRepository) FailHeldQueueItem(ctx context.Context, queueID int64, message string) (bool, error) {
	now := time.Now().UTC()
	res, err := r.db.ExecContext(ctx, `UPDATE import_queue SET status = 'failed', error_message = ?, peer_hold = 0,
		completed_at = ?, updated_at = ? WHERE id = ? AND peer_hold <> 0`, message, now, now, queueID)
	if err != nil {
		return false, fmt.Errorf("peernet bundle import: fail held item: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM import_history WHERE nzb_id = ?`, queueID); err != nil {
		return true, fmt.Errorf("peernet bundle import: drop history of a failed item: %w", err)
	}
	return true, nil
}

// RequeueHeldQueueItem puts a held queue item back as pending, with its
// provisional history gone, so it is imported again from its NZB. It reports
// whether an item was held and changed.
func (r *PeernetBundleImportRepository) RequeueHeldQueueItem(ctx context.Context, queueID int64) (bool, error) {
	now := time.Now().UTC()
	res, err := r.db.ExecContext(ctx, `UPDATE import_queue SET status = 'pending', error_message = NULL, peer_hold = 0,
		started_at = NULL, completed_at = NULL, updated_at = ? WHERE id = ? AND peer_hold <> 0`, now, queueID)
	if err != nil {
		return false, fmt.Errorf("peernet bundle import: requeue held item: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM import_history WHERE nzb_id = ?`, queueID); err != nil {
		return true, fmt.Errorf("peernet bundle import: drop history of a requeued item: %w", err)
	}
	return true, nil
}

// SetQueue links the rows of the given paths to a queue item.
func (r *PeernetBundleImportRepository) SetQueue(ctx context.Context, paths []string, queueID int64) error {
	for len(paths) > 0 {
		n := min(len(paths), maxBundleImportBatch)
		args := []any{queueID}
		for _, p := range paths[:n] {
			args = append(args, normalizeHealthPath(p))
		}
		paths = paths[n:]
		if _, err := r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports SET queue_id = ? WHERE file_path IN (`+inList(n)+`)`, args...); err != nil {
			return fmt.Errorf("peernet bundle import: set queue: %w", err)
		}
	}
	return nil
}

// MoveFile makes a row follow its file to a new virtual path. It reports
// whether a row moved.
func (r *PeernetBundleImportRepository) MoveFile(ctx context.Context, from, to string) (bool, error) {
	res, err := r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports SET file_path = ?, updated_at = ? WHERE file_path = ?`,
		normalizeHealthPath(to), time.Now().Unix(), normalizeHealthPath(from))
	if err != nil {
		return false, fmt.Errorf("peernet bundle import: move file: %w", err)
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// MoveDir makes the rows below a folder follow it to a new virtual path; the
// recorded resulting folder follows too.
func (r *PeernetBundleImportRepository) MoveDir(ctx context.Context, from, to string) error {
	from, to = normalizeHealthPath(from), normalizeHealthPath(to)
	if from == "" || to == "" {
		return errors.New("peernet bundle import: empty folder")
	}
	like := escapeLikePrefix(from) + `/%`
	_, err := r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports
		SET file_path = CAST(? AS TEXT) || substr(file_path, CAST(? AS INTEGER)), updated_at = ?
		WHERE file_path LIKE ? ESCAPE '\'`, to, len(from)+1, time.Now().Unix(), like)
	if err != nil {
		return fmt.Errorf("peernet bundle import: move folder: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports SET resulting_path = ? WHERE resulting_path = ?`, to, from)
	if err != nil {
		return fmt.Errorf("peernet bundle import: move folder: %w", err)
	}
	return nil
}

// DeleteDir removes the rows below a folder.
func (r *PeernetBundleImportRepository) DeleteDir(ctx context.Context, dir string) error {
	dir = normalizeHealthPath(dir)
	if dir == "" {
		return errors.New("peernet bundle import: empty folder")
	}
	_, err := r.db.ExecContext(ctx, `DELETE FROM peernet_bundle_imports WHERE file_path LIKE ? ESCAPE '\'`, escapeLikePrefix(dir)+`/%`)
	if err != nil {
		return fmt.Errorf("peernet bundle import: delete folder: %w", err)
	}
	return nil
}

// PendingCounts returns how many files still owe their full check and how many
// of those have waited since before staleBefore (zero time: none counted stale).
func (r *PeernetBundleImportRepository) PendingCounts(ctx context.Context, staleBefore time.Time) (pending, stale int, err error) {
	cutoff := int64(0)
	if !staleBefore.IsZero() {
		cutoff = staleBefore.Unix()
	}
	err = r.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(CASE WHEN created_at < ? THEN 1 ELSE 0 END), 0)
		FROM peernet_bundle_imports WHERE state = 'pending'`, cutoff).Scan(&pending, &stale)
	if err != nil {
		return 0, 0, fmt.Errorf("peernet bundle import: pending counts: %w", err)
	}
	return pending, stale, nil
}

// StalePending lists pending rows created before staleBefore, oldest first.
func (r *PeernetBundleImportRepository) StalePending(ctx context.Context, staleBefore time.Time, limit int) ([]PeernetBundleImport, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+bundleImportCols+` FROM peernet_bundle_imports
		WHERE state = 'pending' AND created_at < ? ORDER BY created_at LIMIT ?`, staleBefore.Unix(), limit)
	if err != nil {
		return nil, fmt.Errorf("peernet bundle import: stale pending: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []PeernetBundleImport
	for rows.Next() {
		row, err := scanBundleImport(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// CommitRows marks the rows of the given paths as completely written.
func (r *PeernetBundleImportRepository) CommitRows(ctx context.Context, paths []string) error {
	for len(paths) > 0 {
		n := min(len(paths), maxBundleImportBatch)
		args := []any{time.Now().Unix()}
		for _, p := range paths[:n] {
			args = append(args, normalizeHealthPath(p))
		}
		paths = paths[n:]
		if _, err := r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports SET committed = 1, updated_at = ? WHERE file_path IN (`+inList(n)+`)`, args...); err != nil {
			return fmt.Errorf("peernet bundle import: commit: %w", err)
		}
	}
	return nil
}

// ListUncommitted lists the rows of writes that never finished, by path.
func (r *PeernetBundleImportRepository) ListUncommitted(ctx context.Context, limit int) ([]PeernetBundleImport, error) {
	return r.listRows(ctx, `WHERE committed = 0 ORDER BY file_path LIMIT ?`, limit)
}

// ListByQueue lists the rows of one queue item, by path.
func (r *PeernetBundleImportRepository) ListByQueue(ctx context.Context, queueID int64) ([]PeernetBundleImport, error) {
	if queueID <= 0 {
		return nil, nil
	}
	return r.listRows(ctx, `WHERE queue_id = ? ORDER BY file_path`, queueID)
}

// ListAfter lists up to limit rows whose path sorts after the given one, for a
// full walk in pages.
func (r *PeernetBundleImportRepository) ListAfter(ctx context.Context, afterPath string, limit int) ([]PeernetBundleImport, error) {
	return r.listRows(ctx, `WHERE file_path > ? ORDER BY file_path LIMIT ?`, afterPath, limit)
}

func (r *PeernetBundleImportRepository) listRows(ctx context.Context, tail string, args ...any) ([]PeernetBundleImport, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+bundleImportCols+` FROM peernet_bundle_imports `+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("peernet bundle import: list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []PeernetBundleImport
	for rows.Next() {
		row, err := scanBundleImport(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// OrphanedHolds lists the held queue items that no bundle row refers to any
// more (their files were deleted, or the state was lost): nothing can ever
// release such a hold. Only rows carrying the mark are looked at.
func (r *PeernetBundleImportRepository) OrphanedHolds(ctx context.Context) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT q.id FROM import_queue q
		WHERE q.peer_hold <> 0
		  AND NOT EXISTS (SELECT 1 FROM peernet_bundle_imports b WHERE b.queue_id = q.id)
		ORDER BY q.id LIMIT 1000`)
	if err != nil {
		return nil, fmt.Errorf("peernet bundle import: orphaned holds: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SettledHolds lists the held queue items whose import is finished and owes
// nobody anything: every row is passed and committed and no ARR is to be told.
// Such a hold can only still be set because releasing it failed once.
func (r *PeernetBundleImportRepository) SettledHolds(ctx context.Context) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT q.id FROM import_queue q
		WHERE q.peer_hold <> 0
		  AND EXISTS (SELECT 1 FROM peernet_bundle_imports b WHERE b.queue_id = q.id)
		  AND NOT EXISTS (SELECT 1 FROM peernet_bundle_imports b WHERE b.queue_id = q.id
		                  AND (b.state <> 'passed' OR b.committed = 0 OR b.arr_owed <> 0))
		ORDER BY q.id LIMIT 1000`)
	if err != nil {
		return nil, fmt.Errorf("peernet bundle import: settled holds: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SetBundleID records the Bundle ID the rows of the given paths were accepted
// from.
func (r *PeernetBundleImportRepository) SetBundleID(ctx context.Context, paths []string, bundleID string) error {
	for len(paths) > 0 {
		n := min(len(paths), maxBundleImportBatch)
		args := []any{bundleID}
		for _, p := range paths[:n] {
			args = append(args, normalizeHealthPath(p))
		}
		paths = paths[n:]
		if _, err := r.db.ExecContext(ctx, `UPDATE peernet_bundle_imports SET bundle_id = ? WHERE file_path IN (`+inList(n)+`)`, args...); err != nil {
			return fmt.Errorf("peernet bundle import: set bundle id: %w", err)
		}
	}
	return nil
}

// BundleIDOf returns the Bundle ID a row was accepted from ("" if unknown).
func (r *PeernetBundleImportRepository) BundleIDOf(ctx context.Context, filePath string) (string, error) {
	var id string
	err := r.db.QueryRowContext(ctx, `SELECT bundle_id FROM peernet_bundle_imports WHERE file_path = ?`, normalizeHealthPath(filePath)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("peernet bundle import: bundle id: %w", err)
	}
	return id, nil
}

// maxFailedBundles bounds the negative cache: the oldest entries go first.
const maxFailedBundles = 10_000

// RecordFailedBundle puts a Bundle ID whose layout failed local verification on
// the negative cache (the same layout from another publisher is then skipped).
func (r *PeernetBundleImportRepository) RecordFailedBundle(ctx context.Context, bundleID, releaseID string, at time.Time) error {
	if bundleID == "" {
		return nil
	}
	if _, err := r.db.ExecContext(ctx, `INSERT INTO peernet_failed_bundles (bundle_id, release_id, failed_at) VALUES (?, ?, ?)
		ON CONFLICT (bundle_id) DO UPDATE SET failed_at = excluded.failed_at`, bundleID, releaseID, at.Unix()); err != nil {
		return fmt.Errorf("peernet bundle import: record failed bundle: %w", err)
	}
	// Bounded: keep the newest maxFailedBundles.
	if _, err := r.db.ExecContext(ctx, `DELETE FROM peernet_failed_bundles WHERE bundle_id IN (
		SELECT bundle_id FROM peernet_failed_bundles ORDER BY failed_at DESC LIMIT 1000000 OFFSET ?)`, maxFailedBundles); err != nil {
		return fmt.Errorf("peernet bundle import: trim failed bundles: %w", err)
	}
	return nil
}

// IsFailedBundle reports whether the Bundle ID failed local verification at or
// after notBefore.
func (r *PeernetBundleImportRepository) IsFailedBundle(ctx context.Context, bundleID string, notBefore time.Time) (bool, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM peernet_failed_bundles WHERE bundle_id = ? AND failed_at >= ?`,
		bundleID, notBefore.Unix()).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("peernet bundle import: failed bundle lookup: %w", err)
	}
	return n > 0, nil
}

// PruneFailedBundles removes entries older than before.
func (r *PeernetBundleImportRepository) PruneFailedBundles(ctx context.Context, before time.Time) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM peernet_failed_bundles WHERE failed_at < ?`, before.Unix())
	return err
}
