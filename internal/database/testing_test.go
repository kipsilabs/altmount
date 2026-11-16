package database

import (
	"database/sql"
	"testing"
	"time"
)

// setupQueueSchema creates the import_queue table for testing
// This matches the production schema from migrations/001_initial_schema.sql
func setupQueueSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	schema := `
		CREATE TABLE import_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			download_id TEXT DEFAULT NULL,
			nzb_path TEXT NOT NULL,
			relative_path TEXT DEFAULT NULL,
			storage_path TEXT DEFAULT NULL,
			priority INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'completed', 'failed', 'fallback', 'paused')),
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
			peer_hold INTEGER NOT NULL DEFAULT 0,
			indexer TEXT DEFAULT NULL,
			UNIQUE(nzb_path)
		);

		CREATE INDEX idx_queue_download_id ON import_queue(download_id);
		CREATE INDEX idx_queue_status_priority ON import_queue(status, priority, created_at);
		CREATE INDEX idx_queue_batch_id ON import_queue(batch_id);
		CREATE INDEX idx_queue_status ON import_queue(status);
		CREATE INDEX idx_queue_retry ON import_queue(status, retry_count, max_retries);
		CREATE INDEX idx_queue_nzb_path ON import_queue(nzb_path);
		CREATE INDEX idx_import_queue_category ON import_queue(category);
		CREATE INDEX idx_queue_file_size ON import_queue(file_size);
	`

	_, err := db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to create queue schema: %v", err)
	}
}

// insertQueueItem inserts a test queue item with minimal required fields
func insertQueueItem(t *testing.T, db *sql.DB, id int64, nzbPath, status string) {
	t.Helper()

	query := `
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (?, ?, ?, 1)
	`

	_, err := db.Exec(query, id, nzbPath, status)
	if err != nil {
		t.Fatalf("Failed to insert queue item: %v", err)
	}
}

// insertQueueItemWithTime inserts item with custom started_at time
// Useful for testing stale item cleanup
// Note: SQLite datetime('now') uses UTC, so we convert times to UTC
func insertQueueItemWithTime(t *testing.T, db *sql.DB, id int64, nzbPath, status string, startedAt time.Time) {
	t.Helper()

	query := `
		INSERT INTO import_queue (id, nzb_path, status, started_at, priority)
		VALUES (?, ?, ?, ?, 1)
	`

	// Format time as UTC to match SQLite's datetime('now') behavior
	_, err := db.Exec(query, id, nzbPath, status, startedAt.UTC().Format("2006-01-02 15:04:05"))
	if err != nil {
		t.Fatalf("Failed to insert queue item with time: %v", err)
	}
}

// getQueueItemStatus retrieves the status of a queue item by ID
func getQueueItemStatus(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()

	var status string
	err := db.QueryRow("SELECT status FROM import_queue WHERE id = ?", id).Scan(&status)
	if err != nil {
		t.Fatalf("Failed to get queue item status: %v", err)
	}
	return status
}

// countQueueItemsByStatus counts items with given status
func countQueueItemsByStatus(t *testing.T, db *sql.DB, status string) int {
	t.Helper()

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM import_queue WHERE status = ?", status).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count queue items: %v", err)
	}
	return count
}
