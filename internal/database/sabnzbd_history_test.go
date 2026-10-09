package database

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupImportHistorySchema creates the import_history table (production shape,
// no status column — this is the no-schema-change design).
func setupImportHistorySchema(t *testing.T, db *sql.DB) {
	t.Helper()
	schema := `
		CREATE TABLE import_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			download_id TEXT DEFAULT NULL,
			nzb_id INTEGER,
			nzb_name TEXT NOT NULL,
			file_name TEXT NOT NULL,
			file_size BIGINT,
			virtual_path TEXT NOT NULL,
			category TEXT,
			metadata TEXT DEFAULT NULL,
			indexer TEXT DEFAULT NULL,
			completed_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("Failed to create import_history schema: %v", err)
	}
}

func newSABHistoryRepo(t *testing.T) (*Repository, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	setupQueueSchema(t, db)
	setupImportHistorySchema(t, db)
	t.Cleanup(func() { _ = db.Close() })
	return NewRepository(db, DialectSQLite), db
}

// insertQ inserts a queue row with the fields the history union reads.
func insertQ(t *testing.T, db *sql.DB, id int64, status, category string, completedAt time.Time, skipArr bool) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority, category, file_size, storage_path,
			skip_arr_notification, created_at, updated_at, completed_at)
		VALUES (?, ?, ?, 1, ?, 100, ?, ?, ?, ?, ?)`,
		id, fmt.Sprintf("%d-item.nzb", id), status, category, fmt.Sprintf("%s/%d", category, id),
		skipArr, completedAt.UTC().Format("2006-01-02 15:04:05"),
		completedAt.UTC().Format("2006-01-02 15:04:05"), completedAt.UTC().Format("2006-01-02 15:04:05"))
	require.NoError(t, err)
}

// insertH inserts an import_history row (nzbID<=0 => NULL nzb_id).
func insertH(t *testing.T, db *sql.DB, id, nzbID int64, category string, completedAt time.Time) {
	t.Helper()
	var nzb any
	if nzbID > 0 {
		nzb = nzbID
	}
	_, err := db.Exec(`
		INSERT INTO import_history (id, nzb_id, nzb_name, file_name, file_size, virtual_path, category, completed_at)
		VALUES (?, ?, ?, ?, 100, ?, ?, ?)`,
		id, nzb, fmt.Sprintf("h%d.nzb", id), fmt.Sprintf("h%d.mkv", id),
		fmt.Sprintf("%s/h%d", category, id), category, completedAt.UTC().Format("2006-01-02 15:04:05"))
	require.NoError(t, err)
}

func TestSABnzbdHistory_DedupAndSources(t *testing.T) {
	repo, db := newSABHistoryRepo(t)
	ctx := context.Background()
	base := time.Now().UTC()

	// Completed item present in BOTH queue and history (history.nzb_id = queue.id).
	insertQ(t, db, 5, "completed", "tv", base.Add(5*time.Minute), false)
	insertH(t, db, 100, 5, "tv", base.Add(5*time.Minute))
	// History-only completed (its queue row was cleared): nzb_id has no live row.
	insertH(t, db, 101, 99, "tv", base.Add(3*time.Minute))
	// Failed queue item (never in history).
	insertQ(t, db, 6, "failed", "tv", base.Add(4*time.Minute), false)
	// skip_arr item must be excluded.
	insertQ(t, db, 7, "completed", "tv", base.Add(6*time.Minute), true)

	total, err := repo.CountSABnzbdHistory(ctx, "")
	require.NoError(t, err)
	// item 5 (once, deduped), history 101, failed 6 = 3. skip_arr 7 excluded.
	assert.Equal(t, 3, total)

	rows, err := repo.ListSABnzbdHistory(ctx, "", 100, 0)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	bySource := map[string]int{}
	for _, r := range rows {
		bySource[r.Source]++
	}
	assert.Equal(t, 1, bySource["completed_queue"], "item 5 comes from the live queue, history dup dropped")
	assert.Equal(t, 1, bySource["history"], "history-only item survives")
	assert.Equal(t, 1, bySource["failed_queue"], "failed item included")

	// Ordering: newest sort_time first (item5@+5, failed6@+4, history101@+3).
	assert.Equal(t, "completed_queue", rows[0].Source)
	assert.Equal(t, "failed_queue", rows[1].Source)
	assert.Equal(t, QueueStatusFailed, rows[1].Status)
	assert.Equal(t, "history", rows[2].Source)
}

func TestSABnzbdHistory_PaginationAndCategory(t *testing.T) {
	repo, db := newSABHistoryRepo(t)
	ctx := context.Background()
	base := time.Now().UTC()

	// 5 completed tv items (ids 1..5, increasing time), 2 movies items.
	for i := int64(1); i <= 5; i++ {
		insertQ(t, db, i, "completed", "tv", base.Add(time.Duration(i)*time.Minute), false)
	}
	insertQ(t, db, 10, "completed", "movies", base.Add(time.Minute), false)
	insertQ(t, db, 11, "completed", "movies", base.Add(2*time.Minute), false)

	tvTotal, err := repo.CountSABnzbdHistory(ctx, "tv")
	require.NoError(t, err)
	assert.Equal(t, 5, tvTotal)

	// Page through tv in 2s: expect ids 5,4,3,2,1 (newest first), no overlap.
	seen := map[int64]bool{}
	var order []int64
	for _, start := range []int{0, 2, 4} {
		page, err := repo.ListSABnzbdHistory(ctx, "tv", 2, start)
		require.NoError(t, err)
		for _, r := range page {
			assert.False(t, seen[r.ID], "id %d on two pages", r.ID)
			seen[r.ID] = true
			order = append(order, r.ID)
		}
	}
	assert.Equal(t, []int64{5, 4, 3, 2, 1}, order)

	// start beyond total → empty.
	empty, err := repo.ListSABnzbdHistory(ctx, "tv", 2, 999)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestSABnzbdFailedHistory_FiltersAndCap(t *testing.T) {
	repo, db := newSABHistoryRepo(t)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour)

	insertQ(t, db, 1, "failed", "tv", base, false)
	insertQ(t, db, 2, "failed", "tv", base.Add(time.Minute), false)
	insertQ(t, db, 3, "failed", "tv", base.Add(2*time.Minute), true) // skip_arr_notification: never shown
	insertQ(t, db, 4, "failed", "movies", base.Add(3*time.Minute), false)
	insertQ(t, db, 5, "completed", "tv", base.Add(4*time.Minute), false)
	insertH(t, db, 6, 0, "tv", base.Add(5*time.Minute))

	rows, err := repo.ListSABnzbdFailedHistory(ctx, "tv", 10)
	require.NoError(t, err)
	var ids []int64
	for _, r := range rows {
		assert.Equal(t, "failed_queue", r.Source)
		ids = append(ids, r.ID)
	}
	assert.Equal(t, []int64{2, 1}, ids, "newest first, tv only, no skip_arr rows")

	rows, err = repo.ListSABnzbdFailedHistory(ctx, "tv", 1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), rows[0].ID)

	rows, err = repo.ListSABnzbdFailedHistory(ctx, "", 10)
	require.NoError(t, err)
	assert.Len(t, rows, 3, "empty category means all categories")
}
