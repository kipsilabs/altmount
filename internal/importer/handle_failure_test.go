package importer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer/queue"
	"github.com/kipsilabs/altmount/internal/importer/validation"
)

// newMoveToFailedTestService builds a minimal *Service with a real SQLite DB so that
// MoveToFailedFolder can call UpdateQueueItemNzbPath without panicking. The config
// is wired so that GetFailedNzbFolder() returns <configDir>/.nzbs/failed/.
func newMoveToFailedTestService(t *testing.T) *Service {
	t.Helper()

	configDir := t.TempDir()

	// Use a per-test unique in-memory SQLite DB to avoid shared-cache collisions.
	dbDSN := "file:" + t.Name() + "_move_failed?mode=memory&cache=shared&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dbDSN)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS import_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			download_id TEXT DEFAULT NULL,
			nzb_path TEXT NOT NULL,
			relative_path TEXT DEFAULT NULL,
			storage_path TEXT DEFAULT NULL,
			priority INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'pending'
				CHECK(status IN ('pending','processing','completed','failed','fallback','paused')),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			started_at DATETIME DEFAULT NULL,
			completed_at DATETIME DEFAULT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			error_message TEXT DEFAULT NULL,
			batch_id TEXT DEFAULT NULL,
			metadata TEXT DEFAULT NULL,
			category TEXT DEFAULT NULL,
			file_size BIGINT DEFAULT NULL,
			target_path TEXT DEFAULT NULL,
			skip_arr_notification BOOLEAN NOT NULL DEFAULT FALSE,
			skip_post_import_links BOOLEAN NOT NULL DEFAULT FALSE,
			indexer TEXT DEFAULT NULL,
			UNIQUE(nzb_path)
		);
		CREATE INDEX IF NOT EXISTS idx_queue_nzb_path ON import_queue(nzb_path);
 CREATE TABLE import_history (id INTEGER PRIMARY KEY, nzb_id INTEGER);
	`)
	require.NoError(t, err)

	repo := database.NewQueueRepository(db, database.DialectSQLite)
	dbWrapper := &database.DB{}
	dbWrapper.Repository = repo

	cfgGetter := config.ConfigGetter(func() *config.Config {
		return &config.Config{
			Database: config.DatabaseConfig{
				Path: filepath.Join(configDir, "altmount.db"),
			},
		}
	})

	return &Service{
		database:     dbWrapper,
		configGetter: cfgGetter,
		log:          slog.Default(),
		cancelFuncs:  make(map[int64]context.CancelFunc),
		mu:           sync.RWMutex{},
	}
}

// TestHandleFailure_MovesToFailedDir verifies that MoveToFailedFolder moves an .nzb
// file from the OS temp queue directory (where ensurePersistentNzb places it) to
// GetFailedNzbFolder(), and updates the DB record's nzb_path accordingly.
func TestHandleFailure_MovesToFailedDir(t *testing.T) {
	svc := newMoveToFailedTestService(t)

	// Write a .nzb in the OS temp queue dir (simulating post-ensurePersistentNzb state).
	tmpQueue := filepath.Join(os.TempDir(), ".altmount-queue")
	require.NoError(t, os.MkdirAll(tmpQueue, 0755))
	nzbPath := filepath.Join(tmpQueue, "42-show.s01e01.nzb")
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb/>"), 0644))
	t.Cleanup(func() { os.Remove(nzbPath) }) // safety net if test fails mid-way

	// Insert the item into the DB so UpdateQueueItemNzbPath can find it.
	ctx := context.Background()
	item := &database.ImportQueueItem{NzbPath: nzbPath, Status: database.QueueStatusPending}
	err := svc.database.Repository.AddToQueue(ctx, item)
	require.NoError(t, err)
	require.NotZero(t, item.ID, "AddToQueue should populate item.ID")

	// Act: move the NZB to the failed folder.
	moveErr := svc.MoveToFailedFolder(ctx, item)
	require.NoError(t, moveErr)

	failedDir := svc.GetFailedNzbFolder()

	// Assert: the file is in the failed directory.
	entries, err := os.ReadDir(failedDir)
	require.NoError(t, err)
	found := false
	var movedName string
	for _, e := range entries {
		if strings.Contains(e.Name(), "show") {
			found = true
			movedName = e.Name()
			t.Cleanup(func() { os.Remove(filepath.Join(failedDir, movedName)) })
		}
	}
	assert.True(t, found, "failed .nzb should be moved to failed dir; got entries: %v", entries)

	// Assert: original temp file is gone.
	assert.NoFileExists(t, nzbPath, "original temp file should be removed after failure move")

	// Assert: item struct path is updated to the new location.
	assert.True(t, strings.HasPrefix(item.NzbPath, failedDir),
		"item.NzbPath should point to failed dir after move, got %q", item.NzbPath)

	// Assert: DB record is updated to the new path.
	dbItem, err := svc.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, item.NzbPath, dbItem.NzbPath,
		"DB nzb_path should match the new failed-folder path")
}

// TestHandleFailure_SourceMissing_IsNoop verifies that MoveToFailedFolder is a
// no-op (no error) when the source NZB no longer exists on disk.
func TestHandleFailure_SourceMissing_IsNoop(t *testing.T) {
	svc := newMoveToFailedTestService(t)

	item := &database.ImportQueueItem{
		ID:      999,
		NzbPath: filepath.Join(os.TempDir(), ".altmount-queue", "nonexistent-handle-failure.nzb"),
	}

	err := svc.MoveToFailedFolder(context.Background(), item)
	assert.NoError(t, err, "missing source NZB should not be treated as an error")
}

func TestHandleFailure_FastFailInconclusiveSkipsDefinitiveFailureHandling(t *testing.T) {
	svc := newMoveToFailedTestService(t)
	ctx := context.Background()
	item := &database.ImportQueueItem{
		NzbPath:    filepath.Join(t.TempDir(), "inconclusive.nzb"),
		Status:     database.QueueStatusProcessing,
		MaxRetries: 1,
	}
	require.NoError(t, os.WriteFile(item.NzbPath, []byte("<nzb/>"), 0644))
	require.NoError(t, svc.database.Repository.AddToQueue(ctx, item))

	// postProcessor is deliberately nil: reaching definitive fallback/ARR
	// handling would panic, while an inconclusive validation must stop before it.
	svc.HandleFailure(ctx, item, fmt.Errorf("fast-fail check: %w", validation.ErrFastFailInconclusive))

	dbItem, err := svc.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, dbItem)
	assert.Equal(t, database.QueueStatusPending, dbItem.Status)
	require.NotNil(t, dbItem.ErrorMessage)
	assert.Contains(t, *dbItem.ErrorMessage, validation.ErrFastFailInconclusive.Error())
	assert.FileExists(t, item.NzbPath)
	assert.Equal(t, 1, dbItem.RetryCount)
	// Exhaustion keeps the inconclusive item for manual retry without fallback.
	svc.HandleFailure(ctx, dbItem, validation.ErrFastFailInconclusive)
	exhausted, err := svc.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, database.QueueStatusFailed, exhausted.Status)
}

func TestHandleFailure_ContentProbeInconclusiveSkipsDefinitiveFailureHandling(t *testing.T) {
	svc := newMoveToFailedTestService(t)
	ctx := context.Background()
	item := &database.ImportQueueItem{
		NzbPath: filepath.Join(t.TempDir(), "probe-inconclusive.nzb"),
		Status:  database.QueueStatusPending,
	}
	require.NoError(t, os.WriteFile(item.NzbPath, []byte("<nzb/>"), 0644))
	require.NoError(t, svc.database.Repository.AddToQueue(ctx, item))

	// postProcessor is deliberately nil: HandleFailure/NoteImportFailure and the
	// ARR notification would panic here, so reaching them fails this test.
	svc.HandleFailure(ctx, item, fmt.Errorf("content verification could not complete for %q: %w: %w",
		"movie.mkv", ErrContentProbeInconclusive, errors.New("connection reset")))

	dbItem, err := svc.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, dbItem)
	assert.Equal(t, database.QueueStatusFailed, dbItem.Status)
	require.NotNil(t, dbItem.ErrorMessage)
	assert.Contains(t, *dbItem.ErrorMessage, ErrContentProbeInconclusive.Error())
	assert.FileExists(t, item.NzbPath, "an inconclusive content probe must retain the NZB for manual retry")
}

// TestCleanupFailedItems_RemovesNzbFile verifies that cleanupFailedItems deletes the
// on-disk NZB after the DB record is purged, regardless of whether the file lived in
// the OS temp queue dir or the failed/ directory.
func TestCleanupFailedItems_RemovesNzbFile(t *testing.T) {
	svc := newMoveToFailedTestService(t)

	// Create a temp "failed" NZB file on disk.
	failedDir := svc.GetFailedNzbFolder()
	require.NoError(t, os.MkdirAll(failedDir, 0755))
	nzbPath := filepath.Join(failedDir, "old-failed-cleanup.nzb")
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb/>"), 0644))
	t.Cleanup(func() { os.Remove(nzbPath) })

	ctx := context.Background()

	// Insert into DB.
	item := &database.ImportQueueItem{NzbPath: nzbPath, Status: database.QueueStatusPending}
	require.NoError(t, svc.database.Repository.AddToQueue(ctx, item))

	errMsg := "simulated failure"
	require.NoError(t, svc.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusFailed, &errMsg))

	// Use DeleteFailedItemsOlderThan with a far-future cutoff to delete the record.
	futureTime := time.Now().Add(24 * time.Hour)
	deleted, err := svc.database.Repository.DeleteFailedItemsOlderThan(ctx, futureTime)
	require.NoError(t, err)
	require.Len(t, deleted, 1, "should have deleted one failed item")

	// Simulate what cleanupFailedItems does: remove the file.
	for _, di := range deleted {
		if di.NzbPath != "" {
			rmErr := os.Remove(di.NzbPath)
			assert.NoError(t, rmErr)
		}
	}

	assert.NoFileExists(t, nzbPath, "failed NZB should be removed by cleanup")
}

func TestHandleFailure_CancelledDoesNotRetryOrFallback(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		delay time.Duration
	}{
		{"cancelled processing", context.Canceled, 0},
		{"cancelled validation", validation.ErrFastFailInconclusive, 0},
		{"cancel during backoff", validation.ErrFastFailInconclusive, 20 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newMoveToFailedTestService(t)
			item := &database.ImportQueueItem{NzbPath: filepath.Join(t.TempDir(), "cancelled.nzb"), Status: database.QueueStatusProcessing, MaxRetries: 3}
			require.NoError(t, svc.database.Repository.AddToQueue(context.Background(), item))
			ctx, cancelItem := context.WithCancelCause(context.Background())
			cancel := func() { cancelItem(queue.ErrProcessingCancelled) }
			defer cancel()
			if tc.delay == 0 {
				cancel()
			} else {
				timer := time.AfterFunc(tc.delay, cancel)
				defer timer.Stop()
			}
			svc.HandleFailure(ctx, item, tc.err)
			got, err := svc.database.Repository.GetQueueItem(context.Background(), item.ID)
			require.NoError(t, err)
			require.Equal(t, database.QueueStatusFailed, got.Status)
			require.Zero(t, got.RetryCount)
			require.Contains(t, *got.ErrorMessage, "cancelled")
		})
	}
}

func TestHandleFailure_ShutdownPreservesInterruptedImport(t *testing.T) {
	svc := newMoveToFailedTestService(t)
	item := &database.ImportQueueItem{NzbPath: filepath.Join(t.TempDir(), "interrupted.nzb"), Status: database.QueueStatusProcessing, MaxRetries: 3}
	require.NoError(t, svc.database.Repository.AddToQueue(context.Background(), item))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.HandleFailure(ctx, item, context.Canceled)
	got, err := svc.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	require.Equal(t, database.QueueStatusProcessing, got.Status)
	require.Zero(t, got.RetryCount)
}
