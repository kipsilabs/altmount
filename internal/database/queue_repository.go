package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// chunkSize is the maximum number of bound parameters per IN-clause batch.
// Stays well under SQLite's default SQLITE_MAX_VARIABLE_NUMBER (999) and
// PostgreSQL's 65535 parameter limit.
const bulkChunkSize = 500

// escapeLikePattern escapes LIKE wildcards so a literal string can be matched as a prefix.
func escapeLikePattern(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// inPlaceholders builds an IN-clause body of n "?" placeholders.
func inPlaceholders(n int) string {
	if n == 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

// QueueRepository handles queue-specific database operations
type QueueRepository struct {
	db      DBQuerier
	dialect dialectHelper
}

// NewQueueRepository creates a new queue repository
func NewQueueRepository(db *sql.DB, d Dialect) *QueueRepository {
	return &QueueRepository{
		db:      newDialectAwareDB(db, d),
		dialect: dialectHelper{d: d},
	}
}

// RemoveFromQueue removes an item from the queue together with its
// import_history copy.
//
// The two rows go together: the SABnzbd history view suppresses the history
// copy only while the live completed queue row exists, so deleting the queue
// row alone resurrects the job as a fresh "Completed" slot pointing at a path
// the ARR already imported and deleted (issue #586). They are deleted in one
// transaction so a half-applied delete cannot strand a ghost that no retry can
// reach.
func (r *QueueRepository) RemoveFromQueue(ctx context.Context, id int64) error {
	return r.withQueueTransaction(ctx, func(txRepo *QueueRepository) error {
		if _, err := txRepo.db.ExecContext(ctx, `DELETE FROM import_queue WHERE id = ?`, id); err != nil {
			return fmt.Errorf("failed to remove from queue: %w", err)
		}
		if _, err := txRepo.db.ExecContext(ctx, `DELETE FROM import_history WHERE nzb_id = ?`, id); err != nil {
			return fmt.Errorf("failed to remove import history for queue item %d: %w", id, err)
		}
		return nil
	})
}

// RemoveFromQueueBulk removes multiple items from the queue in bulk
func (r *QueueRepository) RemoveFromQueueBulk(ctx context.Context, ids []int64) (*BulkOperationResult, error) {
	if len(ids) == 0 {
		return &BulkOperationResult{}, nil
	}

	result := &BulkOperationResult{
		FailedIDs: []int64{},
	}

	err := r.withQueueTransaction(ctx, func(txRepo *QueueRepository) error {
		// Process in chunks to stay under SQL parameter limits.
		for start := 0; start < len(ids); start += bulkChunkSize {
			end := min(start+bulkChunkSize, len(ids))
			chunk := ids[start:end]

			args := make([]any, len(chunk))
			for i, id := range chunk {
				args[i] = id
			}

			// One query: fetch id, status, nzb_path for the chunk.
			selectQuery := fmt.Sprintf(
				`SELECT id, status, nzb_path FROM import_queue WHERE id IN (%s)`,
				inPlaceholders(len(chunk)),
			)
			rows, err := txRepo.db.QueryContext(ctx, selectQuery, args...)
			if err != nil {
				return fmt.Errorf("failed to fetch queue items for bulk remove: %w", err)
			}

			deleteIDs := make([]any, 0, len(chunk))
			deletePaths := make([]string, 0, len(chunk))
			func() {
				defer rows.Close()
				for rows.Next() {
					var id int64
					var status QueueStatus
					var nzbPath string
					if err = rows.Scan(&id, &status, &nzbPath); err != nil {
						return
					}
					if status == QueueStatusProcessing {
						result.ProcessingCount++
						result.FailedIDs = append(result.FailedIDs, id)
						continue
					}
					deleteIDs = append(deleteIDs, id)
					if nzbPath != "" {
						deletePaths = append(deletePaths, nzbPath)
					}
				}
				if err == nil {
					err = rows.Err()
				}
			}()
			if err != nil {
				return fmt.Errorf("failed to scan queue items for bulk remove: %w", err)
			}

			if len(deleteIDs) == 0 {
				continue
			}

			// Drop the import_history copies in the same transaction, or the
			// SABnzbd history view resurrects the deleted jobs (issue #586).
			historyQuery := fmt.Sprintf(
				`DELETE FROM import_history WHERE nzb_id IN (%s)`,
				inPlaceholders(len(deleteIDs)),
			)
			if _, err := txRepo.db.ExecContext(ctx, historyQuery, deleteIDs...); err != nil {
				return fmt.Errorf("failed to bulk delete import history: %w", err)
			}

			// One query: delete all eligible ids in the chunk.
			deleteQuery := fmt.Sprintf(
				`DELETE FROM import_queue WHERE id IN (%s)`,
				inPlaceholders(len(deleteIDs)),
			)
			if _, err := txRepo.db.ExecContext(ctx, deleteQuery, deleteIDs...); err != nil {
				return fmt.Errorf("failed to bulk delete queue items: %w", err)
			}
			result.DeletedCount += len(deleteIDs)
			result.DeletedPaths = append(result.DeletedPaths, deletePaths...)
		}
		return nil
	})

	if err != nil {
		return result, err
	}

	// If we couldn't delete some items because they were processing, return an error
	// so the API handler knows to return a conflict status
	if result.ProcessingCount > 0 {
		return result, fmt.Errorf("%d items were in processing status and could not be deleted", result.ProcessingCount)
	}

	return result, nil
}

// RestartQueueItemsBulk resets multiple queue items back to pending status
func (r *QueueRepository) RestartQueueItemsBulk(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	return r.withQueueTransaction(ctx, func(txRepo *QueueRepository) error {
		for start := 0; start < len(ids); start += bulkChunkSize {
			end := min(start+bulkChunkSize, len(ids))
			chunk := ids[start:end]

			args := make([]any, len(chunk))
			for i, id := range chunk {
				args[i] = id
			}

			query := fmt.Sprintf(`
				UPDATE import_queue
				SET status = 'pending', started_at = NULL, completed_at = NULL, error_message = NULL, updated_at = datetime('now')
				WHERE id IN (%s) AND status != 'processing'
			`, inPlaceholders(len(chunk)))

			if _, err := txRepo.db.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf("failed to bulk restart queue items: %w", err)
			}
		}
		return nil
	})
}

// AddToQueue adds a new NZB file to the import queue
func (r *QueueRepository) AddToQueue(ctx context.Context, item *ImportQueueItem) error {
	query := `
		INSERT INTO import_queue (download_id, nzb_path, relative_path, category, priority, status, retry_count, max_retries, batch_id, metadata, file_size, target_path, skip_arr_notification, skip_post_import_links, indexer, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
		ON CONFLICT(nzb_path) DO UPDATE SET
		download_id = COALESCE(excluded.download_id, import_queue.download_id),
		priority = CASE WHEN excluded.priority < priority THEN excluded.priority ELSE priority END,
		category = excluded.category,
		batch_id = excluded.batch_id,
		metadata = excluded.metadata,
		file_size = excluded.file_size,
		target_path = excluded.target_path,
		skip_arr_notification = excluded.skip_arr_notification,
		skip_post_import_links = excluded.skip_post_import_links,
		status = excluded.status,
		indexer = COALESCE(excluded.indexer, import_queue.indexer),
		retry_count = 0,
		started_at = NULL,
		updated_at = datetime('now'),
		relative_path = excluded.relative_path
		WHERE status NOT IN ('processing', 'pending')
	`

	args := []any{item.DownloadID, item.NzbPath, item.RelativePath, item.Category, item.Priority, item.Status,
		item.RetryCount, item.MaxRetries, item.BatchID, item.Metadata, item.FileSize, item.TargetPath, item.SkipArrNotification, item.SkipPostImportLinks, item.Indexer}

	if r.dialect.IsPostgres() {
		err := r.db.QueryRowContext(ctx, query+" RETURNING id", args...).Scan(&item.ID)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to add queue item: %w", err)
		}
	} else {
		result, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to add queue item: %w", err)
		}
		item.ID, err = result.LastInsertId()
		if err != nil {
			return fmt.Errorf("failed to get last insert ID: %w", err)
		}
	}

	item.CreatedAt = time.Now()
	item.UpdatedAt = time.Now()
	return nil
}

func (r *QueueRepository) AddStoragePath(ctx context.Context, itemID int64, storagePath string) error {
	query := `
		UPDATE import_queue
		SET storage_path = ?, updated_at = datetime('now')
		WHERE id = ?
	`

	_, err := r.db.ExecContext(ctx, query, storagePath, itemID)
	if err != nil {
		return fmt.Errorf("failed to add storage path: %w", err)
	}

	return nil
}

// IsFileInQueue checks if a file is already in the queue (pending, processing, or paused).
// It matches by exact nzb_path first, and also by filename suffix to handle the case where
// ensurePersistentNzb has already moved the file and updated nzb_path from the temp path to
// the persistent storage path (e.g. /tmp/altmount-uploads/x.nzb → /.nzbs/stremio/42-x.nzb).
// The LIKE pattern uses "%-filename" (dash wildcard) rather than "%/filename" so it also
// matches the "<id>-<base>.nzb" naming that ensurePersistentNzb applies on collision.
func (r *QueueRepository) IsFileInQueue(ctx context.Context, filePath string) (bool, error) {
	filename := filepath.Base(filePath)
	query := `SELECT 1 FROM import_queue WHERE (nzb_path = ? OR nzb_path LIKE ?) AND status IN ('pending', 'processing', 'paused') LIMIT 1`
	likePattern := "%-" + filename

	var exists int
	err := r.db.QueryRowContext(ctx, query, filePath, likePattern).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if file in queue: %w", err)
	}

	return true, nil
}

// ClaimNextQueueItem atomically claims and returns the next available queue item
func (r *QueueRepository) ClaimNextQueueItem(ctx context.Context) (*ImportQueueItem, error) {
	// Use immediate transaction to atomically claim an item
	var claimedItem *ImportQueueItem

	err := r.withQueueTransaction(ctx, func(txRepo *QueueRepository) error {
		// First, get the next available item ID within the transaction
		var itemID int64
		selectQuery := `
			SELECT id FROM import_queue
			WHERE status = 'pending'
			ORDER BY priority ASC, created_at ASC
			LIMIT 1
		`

		err := txRepo.db.QueryRowContext(ctx, selectQuery).Scan(&itemID)
		if err != nil {
			if err == sql.ErrNoRows {
				// No items available
				return nil
			}
			return fmt.Errorf("failed to select queue item: %w", err)
		}

		// Now atomically update that specific item and get all its data
		updateQuery := `
			UPDATE import_queue
			SET status = 'processing', started_at = datetime('now'), updated_at = datetime('now')
			WHERE id = ? AND status = 'pending'
		`

		result, err := txRepo.db.ExecContext(ctx, updateQuery, itemID)
		if err != nil {
			return fmt.Errorf("failed to claim queue item %d: %w", itemID, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			// Item was claimed by another worker between SELECT and UPDATE
			return nil
		}

		// Get the complete claimed item data
		getQuery := `
			SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
			       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, skip_arr_notification, skip_post_import_links, indexer
			FROM import_queue
			WHERE id = ?
		`

		var item ImportQueueItem
		err = txRepo.db.QueryRowContext(ctx, getQuery, itemID).Scan(
			&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
			&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
			&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.SkipArrNotification, &item.SkipPostImportLinks, &item.Indexer,
		)
		if err != nil {
			return fmt.Errorf("failed to get claimed item: %w", err)
		}

		claimedItem = &item
		return nil
	})

	if err != nil {
		return nil, err
	}

	return claimedItem, nil
}

// UpdateQueueItemStatus updates the status of a queue item
func (r *QueueRepository) UpdateQueueItemStatus(ctx context.Context, id int64, status QueueStatus, errorMessage *string) error {
	now := time.Now()
	var query string
	var args []any

	switch status {
	case QueueStatusProcessing:
		query = `UPDATE import_queue SET status = ?, started_at = ?, updated_at = ? WHERE id = ?`
		args = []any{status, now, now, id}
	case QueueStatusCompleted:
		query = `UPDATE import_queue SET status = ?, completed_at = ?, updated_at = ?, error_message = NULL WHERE id = ?`
		args = []any{status, now, now, id}
		// Track successful import
		_ = r.IncrementDailyStat(ctx, "completed")
	case QueueStatusFailed:
		query = `UPDATE import_queue SET status = ?, error_message = ?, updated_at = ? WHERE id = ?`
		args = []any{status, errorMessage, now, id}
		// Track failed import
		_ = r.IncrementDailyStat(ctx, "failed")
	default:
		query = `UPDATE import_queue SET status = ?, error_message = ?, updated_at = ? WHERE id = ?`
		args = []any{status, errorMessage, now, id}
	}

	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update queue item status: %w", err)
	}

	return nil
}

// IncrementDailyStat increments the completed or failed count for the current day
func (r *QueueRepository) IncrementDailyStat(ctx context.Context, statType string) error {
	if statType != "completed" && statType != "failed" {
		return fmt.Errorf("invalid stat type: %q (must be \"completed\" or \"failed\")", statType)
	}

	column := "completed_count"
	if statType == "failed" {
		column = "failed_count"
	}

	// Also increment hourly stat for rolling 24h calculation
	_ = r.IncrementHourlyStat(ctx, statType)

	// date('now') is rewritten to CURRENT_DATE for postgres by the dialectAwareDB wrapper.
	query := fmt.Sprintf(`
		INSERT INTO import_daily_stats (day, %s, updated_at)
		VALUES (date('now'), 1, datetime('now'))
		ON CONFLICT(day) DO UPDATE SET
		%s = %s + 1,
		updated_at = datetime('now')
	`, column, column, column)

	_, err := r.db.ExecContext(ctx, query)
	return err
}

// IncrementHourlyStat increments the completed or failed count for the current hour
func (r *QueueRepository) IncrementHourlyStat(ctx context.Context, statType string) error {
	if statType != "completed" && statType != "failed" {
		return fmt.Errorf("invalid stat type: %q (must be \"completed\" or \"failed\")", statType)
	}

	column := "completed_count"
	if statType == "failed" {
		column = "failed_count"
	}

	// Calculate start of current hour
	currentHour := time.Now().UTC().Truncate(time.Hour)

	query := fmt.Sprintf(`
		INSERT INTO import_hourly_stats (hour, %s, updated_at)
		VALUES (?, 1, datetime('now'))
		ON CONFLICT(hour) DO UPDATE SET
		%s = %s + 1,
		updated_at = datetime('now')
	`, column, column, column)

	_, err := r.db.ExecContext(ctx, query, currentHour)
	return err
}

// GetImportHourlyStats retrieves import statistics for the specified number of hours
func (r *QueueRepository) GetImportHourlyStats(ctx context.Context, hours int) ([]*ImportHourlyStat, error) {
	cutoff := time.Now().UTC().Add(-time.Duration(hours) * time.Hour)
	query := `
		SELECT hour, completed_count, failed_count, bytes_downloaded, updated_at
		FROM import_hourly_stats
		WHERE hour >= ?
		ORDER BY hour ASC
	`

	rows, err := r.db.QueryContext(ctx, query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to get import hourly stats: %w", err)
	}
	defer rows.Close()

	var stats []*ImportHourlyStat
	for rows.Next() {
		var s ImportHourlyStat
		err := rows.Scan(&s.Hour, &s.CompletedCount, &s.FailedCount, &s.BytesDownloaded, &s.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan import hourly stat: %w", err)
		}
		stats = append(stats, &s)
	}

	return stats, rows.Err()
}

// GetImportDailyStats retrieves historical import statistics for the last N days
func (r *QueueRepository) GetImportDailyStats(ctx context.Context, days int) ([]*ImportDailyStat, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02")
	query := `
		SELECT day, completed_count, failed_count, bytes_downloaded, updated_at
		FROM import_daily_stats
		WHERE day >= ?
		ORDER BY day ASC
	`

	rows, err := r.db.QueryContext(ctx, query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to get import history: %w", err)
	}
	defer rows.Close()

	var stats []*ImportDailyStat
	for rows.Next() {
		var s ImportDailyStat
		err := rows.Scan(&s.Day, &s.CompletedCount, &s.FailedCount, &s.BytesDownloaded, &s.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan import daily stat: %w", err)
		}

		stats = append(stats, &s)
	}

	return stats, rows.Err()
}

// GetImportHistory retrieves historical import statistics for the last N days (Alias for GetImportDailyStats)
func (r *QueueRepository) GetImportHistory(ctx context.Context, days int) ([]*ImportDailyStat, error) {
	return r.GetImportDailyStats(ctx, days)
}

// AddImportHistory records a successful file import in the persistent history table
func (r *QueueRepository) AddImportHistory(ctx context.Context, history *ImportHistory) error {
	virtualPath := normalizeHealthPath(history.VirtualPath)
	query := `
		INSERT INTO import_history (download_id, nzb_id, nzb_name, file_name, file_size, virtual_path, category, metadata, indexer, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
	`
	_, err := r.db.ExecContext(ctx, query,
		history.DownloadID, history.NzbID, history.NzbName, history.FileName, history.FileSize,
		virtualPath, history.Category, history.Metadata, history.Indexer)
	if err != nil {
		return fmt.Errorf("failed to add import history: %w", err)
	}
	return nil
}

// ListImportHistory retrieves the last N successful imports from the persistent history
func (r *QueueRepository) ListImportHistory(ctx context.Context, limit int) ([]*ImportHistory, error) {
	query := `
		SELECT h.id, h.download_id, h.nzb_id, h.nzb_name, h.file_name, h.file_size, h.virtual_path, f.library_path, h.category, h.metadata, h.indexer, h.completed_at
		FROM import_history h
		LEFT JOIN file_health f ON h.virtual_path = f.file_path
		ORDER BY h.completed_at DESC
		LIMIT ?
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list import history: %w", err)
	}
	defer rows.Close()

	var history []*ImportHistory
	for rows.Next() {
		var h ImportHistory
		err := rows.Scan(&h.ID, &h.DownloadID, &h.NzbID, &h.NzbName, &h.FileName, &h.FileSize, &h.VirtualPath, &h.LibraryPath, &h.Category, &h.Metadata, &h.Indexer, &h.CompletedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan import history: %w", err)
		}
		history = append(history, &h)
	}
	return history, nil
}

// IncrementRetryCountAndResetStatus increments the retry count and resets the status to pending
func (r *QueueRepository) IncrementRetryCountAndResetStatus(ctx context.Context, id int64, errorMessage *string) (bool, error) {
	query := `
		UPDATE import_queue 
		SET status = 'pending', retry_count = retry_count + 1, started_at = NULL, error_message = ?, updated_at = datetime('now')
		WHERE id = ? AND retry_count < max_retries
	`
	result, err := r.db.ExecContext(ctx, query, errorMessage, id)
	if err != nil {
		return false, fmt.Errorf("failed to increment retry count: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected > 0, nil
}

// UpdateQueueItemPriority updates the priority of a queue item
func (r *QueueRepository) UpdateQueueItemPriority(ctx context.Context, id int64, priority QueuePriority) error {
	query := `UPDATE import_queue SET priority = ?, updated_at = datetime('now') WHERE id = ?`
	_, err := r.db.ExecContext(ctx, query, priority, id)
	if err != nil {
		return fmt.Errorf("failed to update queue item priority: %w", err)
	}
	return nil
}

// UpdateQueueItemNzbPath updates the NZB path of a queue item
func (r *QueueRepository) UpdateQueueItemNzbPath(ctx context.Context, id int64, nzbPath string) error {
	query := `UPDATE import_queue SET nzb_path = ?, updated_at = datetime('now') WHERE id = ?`
	_, err := r.db.ExecContext(ctx, query, nzbPath, id)
	if err != nil {
		return fmt.Errorf("failed to update queue item nzb path: %w", err)
	}
	return nil
}

// UpdateQueueItemCategory updates the category and priority of a queue item.
func (r *QueueRepository) UpdateQueueItemCategory(ctx context.Context, id int64, category *string, priority QueuePriority) error {
	query := `UPDATE import_queue SET category = ?, priority = ?, updated_at = datetime('now') WHERE id = ?`
	if _, err := r.db.ExecContext(ctx, query, category, priority, id); err != nil {
		return fmt.Errorf("failed to update queue item category: %w", err)
	}
	return nil
}

// GetPendingQueueItemsByPathPrefix returns pending queue items whose nzb_path starts with prefix.
func (r *QueueRepository) GetPendingQueueItemsByPathPrefix(ctx context.Context, prefix string) ([]*ImportQueueItem, error) {
	query := `SELECT id, nzb_path FROM import_queue WHERE status = 'pending' AND nzb_path LIKE ? ESCAPE '\'`
	rows, err := r.db.QueryContext(ctx, query, escapeLikePattern(prefix)+"%")
	if err != nil {
		return nil, fmt.Errorf("failed to query pending queue items by prefix: %w", err)
	}
	defer rows.Close()

	var items []*ImportQueueItem
	for rows.Next() {
		var it ImportQueueItem
		if err := rows.Scan(&it.ID, &it.NzbPath); err != nil {
			return nil, fmt.Errorf("failed to scan pending queue item: %w", err)
		}
		items = append(items, &it)
	}
	return items, rows.Err()
}

// GetQueueItemByNzbPath returns the queue item with the given NZB path, or nil if not found.
func (r *QueueRepository) GetQueueItemByNzbPath(ctx context.Context, nzbPath string) (*ImportQueueItem, error) {
	query := `
		SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
		       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, skip_arr_notification, skip_post_import_links, indexer
		FROM import_queue WHERE nzb_path = ? LIMIT 1
	`

	var item ImportQueueItem
	err := r.db.QueryRowContext(ctx, query, nzbPath).Scan(
		&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
		&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
		&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.SkipArrNotification, &item.SkipPostImportLinks, &item.Indexer,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue item by nzb path: %w", err)
	}
	return &item, nil
}

// GetQueueStats returns current queue statistics
func (r *QueueRepository) GetQueueStats(ctx context.Context) (*QueueStats, error) {
	// Aggregate counts by status in a single index scan over idx_queue_status.
	const countsQuery = `
		SELECT status, COUNT(*)
		FROM import_queue
		WHERE status IN ('pending', 'processing', 'completed', 'failed', 'paused')
		GROUP BY status
	`

	stats := &QueueStats{}
	rows, err := r.db.QueryContext(ctx, countsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue status counts: %w", err)
	}
	defer rows.Close()

	var pendingCount, pausedCount int
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("failed to scan queue status count: %w", err)
		}
		switch status {
		case "pending":
			pendingCount = count
		case "paused":
			pausedCount = count
		case "processing":
			stats.TotalProcessing = count
		case "completed":
			stats.TotalCompleted = count
		case "failed":
			stats.TotalFailed = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue status counts: %w", err)
	}

	stats.TotalQueued = pendingCount + pausedCount // pending + paused

	// Calculate average processing time for completed items
	var avgProcessingTimeFloat sql.NullFloat64
	avgQuery := fmt.Sprintf(`
		SELECT AVG(%s)
		FROM import_queue
		WHERE status = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL
	`, r.dialect.AvgProcessingTimeMS("started_at", "completed_at"))
	if err := r.db.QueryRowContext(ctx, avgQuery).Scan(&avgProcessingTimeFloat); err != nil {
		return nil, fmt.Errorf("failed to calculate average processing time: %w", err)
	}

	// Convert float to int64 for storage
	if avgProcessingTimeFloat.Valid {
		avgTime := int(avgProcessingTimeFloat.Float64)
		stats.AvgProcessingTimeMs = &avgTime
	}

	stats.LastUpdated = time.Now()
	return stats, nil
}

// AddBatchToQueue adds multiple items to the queue in a single transaction
func (r *QueueRepository) AddBatchToQueue(ctx context.Context, items []*ImportQueueItem) error {
	if len(items) == 0 {
		return nil
	}

	return r.withQueueTransaction(ctx, func(txRepo *QueueRepository) error {
		// Prepare batch insert statement
		query := `
			INSERT INTO import_queue (download_id, nzb_path, relative_path, category, priority, status, retry_count, max_retries, batch_id, metadata, file_size, skip_arr_notification, skip_post_import_links, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
			ON CONFLICT(nzb_path) DO UPDATE SET
			download_id = COALESCE(excluded.download_id, import_queue.download_id),
			priority = CASE WHEN excluded.priority < priority THEN excluded.priority ELSE priority END,
			category = excluded.category,
			batch_id = excluded.batch_id,
			metadata = excluded.metadata,
			file_size = excluded.file_size,
			updated_at = datetime('now')
			WHERE status NOT IN ('processing', 'completed')
		`

		// The statement is identical for every item, so prepare it once and reuse
		// it across the batch instead of re-parsing (and re-rewriting the dialect)
		// on every row.
		txTx, ok := txRepo.db.(*dialectAwareTx)
		if !ok {
			return fmt.Errorf("batch queue insert requires a transaction")
		}

		isPostgres := txRepo.dialect.IsPostgres()
		stmtQuery := query
		if isPostgres {
			stmtQuery = query + " RETURNING id"
		}

		stmt, err := txTx.PrepareContext(ctx, stmtQuery)
		if err != nil {
			return fmt.Errorf("failed to prepare batch queue insert: %w", err)
		}
		defer stmt.Close()

		now := time.Now()
		for _, item := range items {
			args := []any{item.DownloadID, item.NzbPath, item.RelativePath, item.Category, item.Priority, item.Status,
				item.RetryCount, item.MaxRetries, item.BatchID, item.Metadata, item.FileSize, item.SkipArrNotification, item.SkipPostImportLinks}

			if isPostgres {
				if err := stmt.QueryRowContext(ctx, args...).Scan(&item.ID); err != nil && err != sql.ErrNoRows {
					return fmt.Errorf("failed to insert queue item %s: %w", item.NzbPath, err)
				}
			} else {
				result, err := stmt.ExecContext(ctx, args...)
				if err != nil {
					return fmt.Errorf("failed to insert queue item %s: %w", item.NzbPath, err)
				}
				item.ID, err = result.LastInsertId()
				if err != nil {
					return fmt.Errorf("failed to get last insert ID for %s: %w", item.NzbPath, err)
				}
			}
			item.CreatedAt = now
			item.UpdatedAt = now
		}

		return nil
	})
}

// GetQueueItem retrieves a specific queue item by ID
func (r *QueueRepository) GetQueueItem(ctx context.Context, id int64) (*ImportQueueItem, error) {
	query := `
		SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
		       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, skip_arr_notification, skip_post_import_links, indexer
		FROM import_queue WHERE id = ?
	`

	var item ImportQueueItem
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
		&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
		&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.SkipArrNotification, &item.SkipPostImportLinks, &item.Indexer,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Item not found
		}
		return nil, fmt.Errorf("failed to get queue item: %w", err)
	}

	return &item, nil
}

// GetQueueItemByDownloadID retrieves a queue item by its DownloadID
func (r *QueueRepository) GetQueueItemByDownloadID(ctx context.Context, downloadID string) (*ImportQueueItem, error) {
	query := `
		SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
		       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, skip_arr_notification, skip_post_import_links, indexer
		FROM import_queue WHERE download_id = ?
	`

	var item ImportQueueItem
	err := r.db.QueryRowContext(ctx, query, downloadID).Scan(
		&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
		&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
		&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.SkipArrNotification, &item.SkipPostImportLinks, &item.Indexer,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Item not found
		}
		return nil, fmt.Errorf("failed to get queue item by download_id: %w", err)
	}

	return &item, nil
}

// withQueueTransaction executes a function within a queue database transaction
func (r *QueueRepository) withQueueTransaction(ctx context.Context, fn func(*QueueRepository) error) error {
	ddb, ok := r.db.(*dialectAwareDB)
	if !ok {
		return fmt.Errorf("repository not connected to dialectAwareDB")
	}

	tx, err := ddb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin queue transaction: %w", err)
	}

	// Create a repository that uses the transaction
	txRepo := &QueueRepository{db: tx, dialect: r.dialect}

	err = fn(txRepo)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("failed to rollback queue transaction (original error: %w): %v", err, rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit queue transaction: %w", err)
	}

	return nil
}

// DeleteFailedItemsOlderThan deletes failed queue items older than the given time.
// Returns the deleted items so the caller can clean up associated NZB files.
func (r *QueueRepository) DeleteFailedItemsOlderThan(ctx context.Context, olderThan time.Time) ([]*ImportQueueItem, error) {
	var deletedItems []*ImportQueueItem

	err := r.withQueueTransaction(ctx, func(txRepo *QueueRepository) error {
		// Select failed items older than the threshold
		selectQuery := `SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
			started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, skip_arr_notification, skip_post_import_links, indexer
			FROM import_queue WHERE status = 'failed' AND updated_at < ?`

		rows, err := txRepo.db.QueryContext(ctx, selectQuery, olderThan)
		if err != nil {
			return fmt.Errorf("failed to select old failed items: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var item ImportQueueItem
			if err := rows.Scan(
				&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
				&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
				&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.SkipArrNotification, &item.SkipPostImportLinks, &item.Indexer,
			); err != nil {
				return fmt.Errorf("failed to scan failed queue item: %w", err)
			}
			deletedItems = append(deletedItems, &item)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to iterate failed queue items: %w", err)
		}

		if len(deletedItems) == 0 {
			return nil
		}

		// Single ranged DELETE — uses idx_queue_status_updated.
		const deleteQuery = `DELETE FROM import_queue WHERE status = 'failed' AND updated_at < ?`
		if _, err := txRepo.db.ExecContext(ctx, deleteQuery, olderThan); err != nil {
			return fmt.Errorf("failed to delete failed queue items: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return deletedItems, nil
}

// DeleteImportHistoryOlderThan deletes import_history records completed before olderThan.
func (r *QueueRepository) DeleteImportHistoryOlderThan(ctx context.Context, olderThan time.Time) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM import_history WHERE completed_at < ?`, olderThan)
	if err != nil {
		return fmt.Errorf("failed to delete old import history: %w", err)
	}
	return nil
}

// ResetStaleItems resets processing items back to pending on service startup
func (r *QueueRepository) ResetStaleItems(ctx context.Context) error {
	// Reset all items that are in 'processing' status
	// Since the service is just starting up, any item marked as processing is from a previous interrupted run
	query := `
		UPDATE import_queue
		SET status = 'pending', started_at = NULL, updated_at = datetime('now')
		WHERE status = 'processing'`

	result, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to reset stale queue items: %w", err)
	}

	_, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	return nil
}

// ClearImportHistory deletes all records from the import_history and import_daily_stats tables
func (r *QueueRepository) ClearImportHistory(ctx context.Context) error {
	// Clear history records
	queryHistory := `DELETE FROM import_history`
	if _, err := r.db.ExecContext(ctx, queryHistory); err != nil {
		return fmt.Errorf("failed to clear import history: %w", err)
	}

	// Also clear daily stats
	return r.ClearDailyStats(ctx)
}

// ClearDailyStats deletes all records from the import_daily_stats, import_hourly_stats, and provider_hourly_stats tables
func (r *QueueRepository) ClearDailyStats(ctx context.Context) error {
	queryDaily := `DELETE FROM import_daily_stats`
	if _, err := r.db.ExecContext(ctx, queryDaily); err != nil {
		return fmt.Errorf("failed to clear daily stats: %w", err)
	}

	if err := r.ClearHourlyStats(ctx); err != nil {
		return err
	}

	return r.ClearProviderHourlyStats(ctx)
}

// ClearHourlyStats deletes all records from the import_hourly_stats table
func (r *QueueRepository) ClearHourlyStats(ctx context.Context) error {
	queryHourly := `DELETE FROM import_hourly_stats`
	if _, err := r.db.ExecContext(ctx, queryHourly); err != nil {
		return fmt.Errorf("failed to clear hourly stats: %w", err)
	}
	return nil
}

// ClearProviderHourlyStats deletes all records from the provider_hourly_stats table
func (r *QueueRepository) ClearProviderHourlyStats(ctx context.Context) error {
	queryProviderHourly := `DELETE FROM provider_hourly_stats`
	if _, err := r.db.ExecContext(ctx, queryProviderHourly); err != nil {
		return fmt.Errorf("failed to clear provider hourly stats: %w", err)
	}
	return nil
}

// ClearImportHistorySince deletes records from the import_history and import_queue tables
// since the specified time, and adjusts the import_daily_stats and import_hourly_stats accordingly.
func (r *QueueRepository) ClearImportHistorySince(ctx context.Context, since time.Time) error {
	// Clear from persistent history
	queryHistory := `DELETE FROM import_history WHERE completed_at >= ?`
	if _, err := r.db.ExecContext(ctx, queryHistory, since); err != nil {
		return fmt.Errorf("failed to clear import history: %w", err)
	}

	// Also clear from queue (completed/failed)
	queryQueue := `DELETE FROM import_queue WHERE status IN ('completed', 'failed') AND updated_at >= ?`
	if _, err := r.db.ExecContext(ctx, queryQueue, since); err != nil {
		return fmt.Errorf("failed to clear queue history: %w", err)
	}

	// Note: We don't decrement daily/hourly counts here for simplicity,
	// as strict rolling 24h will naturally age out the data.
	return nil
}
