package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// DBQuerier defines the interface for database query operations
// Both *sql.DB and *sql.Tx implement this interface
type DBQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Repository provides database operations for NZB and file management
type Repository struct {
	db      DBQuerier
	dialect dialectHelper
}

// NewRepository creates a new repository instance
func NewRepository(db *sql.DB, d Dialect) *Repository {
	return &Repository{
		db:      newDialectAwareDB(db, d),
		dialect: dialectHelper{d: d},
	}
}

// Transaction support

// WithTransaction executes a function within a database transaction
func (r *Repository) WithTransaction(ctx context.Context, fn func(*Repository) error) error {
	return r.withTransactionMode(ctx, "", fn)
}

// WithImmediateTransaction executes a function within an immediate database transaction
// This reduces lock contention for queue operations by acquiring write locks immediately
// Uses SQLite's IMMEDIATE transaction mode via BeginTx with Serializable isolation
func (r *Repository) WithImmediateTransaction(ctx context.Context, fn func(*Repository) error) error {
	ddb, ok := r.db.(*dialectAwareDB)
	if !ok {
		return fmt.Errorf("repository not connected to dialectAwareDB")
	}

	tx, err := ddb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	txRepo := &Repository{db: tx, dialect: r.dialect}

	err = fn(txRepo)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("failed to rollback immediate transaction (original error: %w): %v", err, rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit immediate transaction: %w", err)
	}

	return nil
}

// withTransactionMode executes a function within a database transaction with specified mode
func (r *Repository) withTransactionMode(ctx context.Context, _ string, fn func(*Repository) error) error {
	ddb, ok := r.db.(*dialectAwareDB)
	if !ok {
		return fmt.Errorf("repository not connected to dialectAwareDB")
	}

	tx, err := ddb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	txRepo := &Repository{db: tx, dialect: r.dialect}

	err = fn(txRepo)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("failed to rollback transaction (original error: %w): %w", err, rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Queue operations

// AddToQueue adds an NZB file to the import queue with optimized concurrency
func (r *Repository) AddToQueue(ctx context.Context, item *ImportQueueItem) error {
	// Use UPSERT with immediate lock to prevent conflicts during concurrent inserts
	query := `
		INSERT INTO import_queue (download_id, nzb_path, relative_path, category, priority, status, retry_count, max_retries, batch_id, metadata, file_size, target_path, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
		ON CONFLICT(nzb_path) DO UPDATE SET
		download_id = COALESCE(excluded.download_id, import_queue.download_id),
		priority = CASE WHEN excluded.priority < priority THEN excluded.priority ELSE priority END,
		category = excluded.category,
		batch_id = excluded.batch_id,
		metadata = excluded.metadata,
		file_size = excluded.file_size,
		target_path = excluded.target_path,
		updated_at = datetime('now')
		WHERE status NOT IN ('processing', 'completed')
	`

	args := []any{item.DownloadID, item.NzbPath, item.RelativePath, item.Category, item.Priority, item.Status,
		item.RetryCount, item.MaxRetries, item.BatchID, item.Metadata, item.FileSize, item.TargetPath}

	if r.dialect.IsPostgres() {
		err := r.db.QueryRowContext(ctx, query+" RETURNING id", args...).Scan(&item.ID)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to add to queue: %w", err)
		}
	} else {
		result, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to add to queue: %w", err)
		}
		insertID, insertIDErr := result.LastInsertId()
		if insertIDErr == nil && insertID != 0 {
			item.ID = insertID
		} else {
			// SQLite returns no useful insert ID for an UPSERT that updates an
			// existing row. Resolve the exact row instead of attaching a zero or
			// unrelated ID to the caller's item.
			existing, lookupErr := r.getQueueItemByNzbPath(ctx, item.NzbPath)
			switch {
			case lookupErr != nil:
				return fmt.Errorf("failed to resolve queue item after upsert (last insert id: %v): %w",
					insertIDErr, lookupErr)
			case existing == nil:
				return fmt.Errorf("queue item %q not found after upsert (last insert id: %v)",
					item.NzbPath, insertIDErr)
			}
			item.ID = existing.ID
		}
	}

	item.CreatedAt = time.Now()
	item.UpdatedAt = time.Now()
	return nil
}

func (r *Repository) getQueueItemByNzbPath(ctx context.Context, nzbPath string) (*ImportQueueItem, error) {
	query := `SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
		started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, indexer
		FROM import_queue WHERE nzb_path = ? LIMIT 1`
	var item ImportQueueItem
	err := r.db.QueryRowContext(ctx, query, nzbPath).Scan(
		&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
		&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt, &item.RetryCount, &item.MaxRetries,
		&item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.Indexer,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// ClaimNextQueueItem atomically claims and returns the next available queue item
// This prevents multiple workers from processing the same item
// Uses a single atomic UPDATE...RETURNING query to eliminate race conditions
func (r *Repository) ClaimNextQueueItem(ctx context.Context) (*ImportQueueItem, error) {
	// Use immediate transaction to atomically claim an item
	var claimedItem *ImportQueueItem

	err := r.WithImmediateTransaction(ctx, func(txRepo *Repository) error {
		// Single atomic operation: update and return in one query
		// This eliminates the race condition window between SELECT and UPDATE
		updateQuery := fmt.Sprintf(`
			UPDATE import_queue
			SET status = 'processing',
			    started_at = datetime('now'),
			    updated_at = datetime('now')
			WHERE id = (
				SELECT id FROM import_queue
				WHERE status = 'pending'
				  AND (started_at IS NULL OR %s < datetime('now'))
				ORDER BY priority ASC, created_at ASC
				LIMIT 1
			) AND status = 'pending'
			RETURNING id, download_id, nzb_path, relative_path, category, priority, status,
			          created_at, updated_at, started_at, completed_at,
			          retry_count, max_retries, error_message, batch_id, metadata, file_size, target_path
		`, r.dialect.ColumnPlusMinutes("started_at", 10))

		var item ImportQueueItem
		err := txRepo.db.QueryRowContext(ctx, updateQuery).Scan(
			&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category,
			&item.Priority, &item.Status, &item.CreatedAt, &item.UpdatedAt,
			&item.StartedAt, &item.CompletedAt, &item.RetryCount,
			&item.MaxRetries, &item.ErrorMessage, &item.BatchID,
			&item.Metadata, &item.FileSize, &item.TargetPath,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				// No items available to claim
				return nil
			}
			return fmt.Errorf("failed to claim queue item: %w", err)
		}

		claimedItem = &item
		return nil
	})

	if err != nil {
		return nil, err
	}

	return claimedItem, nil
}

// AddBatchToQueue adds multiple items to the queue in a single transaction for better performance
func (r *Repository) AddBatchToQueue(ctx context.Context, items []*ImportQueueItem) error {
	if len(items) == 0 {
		return nil
	}

	// Use immediate transaction for batch operations to reduce lock contention
	return r.WithImmediateTransaction(ctx, func(txRepo *Repository) error {
		// Prepare batch insert statement
		query := `
			INSERT INTO import_queue (download_id, nzb_path, relative_path, category, priority, status, retry_count, max_retries, batch_id, metadata, file_size, target_path, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
			ON CONFLICT(nzb_path) DO UPDATE SET
			download_id = COALESCE(excluded.download_id, import_queue.download_id),
			priority = CASE WHEN excluded.priority < priority THEN excluded.priority ELSE priority END,
			category = excluded.category,
			batch_id = excluded.batch_id,
			metadata = excluded.metadata,
			file_size = excluded.file_size,
			target_path = excluded.target_path,
			updated_at = datetime('now')
			WHERE status NOT IN ('processing', 'completed')
		`

		now := time.Now()
		for _, item := range items {
			args := []any{item.DownloadID, item.NzbPath, item.RelativePath, item.Category, item.Priority, item.Status,
				item.RetryCount, item.MaxRetries, item.BatchID, item.Metadata, item.FileSize, item.TargetPath}

			if txRepo.dialect.IsPostgres() {
				err := txRepo.db.QueryRowContext(ctx, query+" RETURNING id", args...).Scan(&item.ID)
				if err != nil && err != sql.ErrNoRows {
					return fmt.Errorf("failed to insert queue item %s: %w", item.NzbPath, err)
				}
			} else {
				result, err := txRepo.db.ExecContext(ctx, query, args...)
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

// UpdateQueueItemStatus updates the status of a queue item
func (r *Repository) UpdateQueueItemStatus(ctx context.Context, id int64, status QueueStatus, errorMessage *string) error {
	now := time.Now()
	var query string
	var args []any

	switch status {
	case QueueStatusProcessing:
		query = `UPDATE import_queue SET status = ?, started_at = ?, updated_at = ? WHERE id = ?`
		args = []any{status, now, now, id}
	case QueueStatusPending:
		// Reset lifecycle columns so the worker can claim the item immediately.
		// ClaimNextQueueItem skips pending rows whose started_at is within the
		// last 10 minutes (orphan-recovery gate), so we must clear started_at
		// when transitioning back to pending via retry.
		query = `UPDATE import_queue SET status = ?, started_at = NULL, completed_at = NULL, error_message = NULL, retry_count = 0, updated_at = ? WHERE id = ?`
		args = []any{status, now, id}
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
func (r *Repository) IncrementDailyStat(ctx context.Context, statType string) error {
	column := "completed_count"
	if statType == "failed" {
		column = "failed_count"
	}

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

// GetQueueItem retrieves a specific queue item by ID
func (r *Repository) GetQueueItem(ctx context.Context, id int64) (*ImportQueueItem, error) {
	query := `
		SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
		       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, indexer
		FROM import_queue WHERE id = ?
	`

	var item ImportQueueItem
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
		&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
		&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.Indexer,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get queue item: %w", err)
	}

	return &item, nil
}

// GetQueueItemByDownloadID retrieves a queue item by its DownloadID
func (r *Repository) GetQueueItemByDownloadID(ctx context.Context, downloadID string) (*ImportQueueItem, error) {
	query := `
		SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
		       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, indexer
		FROM import_queue WHERE download_id = ?
	`

	var item ImportQueueItem
	err := r.db.QueryRowContext(ctx, query, downloadID).Scan(
		&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
		&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
		&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.Indexer,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get queue item by download_id: %w", err)
	}

	return &item, nil
}

// UpdateQueueItemIndexerByDownloadID updates the indexer for a queue item by its DownloadID
func (r *Repository) UpdateQueueItemIndexerByDownloadID(ctx context.Context, downloadID string, indexer string) error {
	query := `UPDATE import_queue SET indexer = ?, updated_at = datetime('now') WHERE download_id = ?`
	_, err := r.db.ExecContext(ctx, query, indexer, downloadID)
	if err != nil {
		return fmt.Errorf("failed to update queue item indexer: %w", err)
	}
	return nil
}

// UpdateQueueItemIndexer updates the indexer for a queue item by its ID
func (r *Repository) UpdateQueueItemIndexer(ctx context.Context, id int64, indexer string) error {
	query := `UPDATE import_queue SET indexer = ?, updated_at = datetime('now') WHERE id = ?`
	_, err := r.db.ExecContext(ctx, query, indexer, id)
	if err != nil {
		return fmt.Errorf("failed to update queue item indexer by id: %w", err)
	}
	return nil
}

// RemoveFromQueueByDownloadID removes an item from the queue by its DownloadID
func (r *Repository) RemoveFromQueueByDownloadID(ctx context.Context, downloadID string) error {
	query := `DELETE FROM import_queue WHERE download_id = ?`

	result, err := r.db.ExecContext(ctx, query, downloadID)
	if err != nil {
		return fmt.Errorf("failed to remove from queue by download_id: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// HasActiveOrRecentQueueItemForStoragePath reports whether any import_queue
// row references a storage_path equal to (or nested under) the given relative
// path and is either still active (pending/processing/paused/retrying) or was
// completed within the supplied grace window. It is used to guard against arr
// webhook directory deletions racing a sibling import that just wrote into
// the same release folder.
//
// relPath is matched as a path prefix; both "<relPath>" and "/<relPath>" are
// considered (storage_path is typically absolute, e.g. "/moviesHQ/release/...",
// while webhook-normalized paths are relative).
func (r *Repository) HasActiveOrRecentQueueItemForStoragePath(
	ctx context.Context,
	relPath string,
	completedGrace time.Duration,
) (bool, error) {
	relPath = strings.Trim(relPath, "/")
	if relPath == "" {
		return false, nil
	}

	cutoff := time.Now().Add(-completedGrace)
	withSlash := "/" + relPath
	prefixWithSlash := withSlash + "/"
	prefixNoSlash := relPath + "/"

	query := `
		SELECT 1 FROM import_queue
		WHERE storage_path IS NOT NULL
		  AND (
		        storage_path = ? OR storage_path LIKE ?
		     OR storage_path = ? OR storage_path LIKE ?
		      )
		  AND (
		        status IN ('pending', 'processing', 'paused')
		     OR (status = 'completed' AND completed_at IS NOT NULL AND completed_at > ?)
		      )
		LIMIT 1
	`

	var exists int
	err := r.db.QueryRowContext(ctx, query,
		withSlash, prefixWithSlash+"%",
		relPath, prefixNoSlash+"%",
		cutoff,
	).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check active queue items for storage path: %w", err)
	}
	return true, nil
}

// DeleteQueueItemsByPath removes items from the queue matching the given path
func (r *Repository) DeleteQueueItemsByPath(ctx context.Context, path string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM import_queue WHERE storage_path = ? OR nzb_path = ?`, path, path)
	if err != nil {
		return fmt.Errorf("failed to delete queue items by path: %w", err)
	}
	return nil
}

// RemoveFromQueue removes an item from the queue
func (r *Repository) RemoveFromQueue(ctx context.Context, id int64) error {
	// The queue row and its import_history copy go together: the SABnzbd
	// history view suppresses the history copy only while the live completed
	// queue row exists, so a half-applied delete resurrects the job as a fresh
	// "Completed" slot pointing at a path the ARR already imported and deleted
	// (issue #586). Deleting the queue row alone would also be unrecoverable —
	// a retry can no longer find the item to clean up after.
	return r.WithTransaction(ctx, func(tx *Repository) error {
		result, err := tx.db.ExecContext(ctx, `DELETE FROM import_queue WHERE id = ?`, id)
		if err != nil {
			return fmt.Errorf("failed to remove from queue: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			return sql.ErrNoRows
		}

		if _, err := tx.db.ExecContext(ctx, `DELETE FROM import_history WHERE nzb_id = ?`, id); err != nil {
			return fmt.Errorf("failed to remove import history for queue item %d: %w", id, err)
		}

		return nil
	})
}

// RemoveFromHistoryByDownloadID removes a record from import_history by its DownloadID
func (r *Repository) RemoveFromHistoryByDownloadID(ctx context.Context, downloadID string) (int64, error) {
	query := `DELETE FROM import_history WHERE download_id = ?`
	result, err := r.db.ExecContext(ctx, query, downloadID)
	if err != nil {
		return 0, fmt.Errorf("failed to remove history record by download_id: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// RemoveFromHistory removes a record from import_history by its own ID
func (r *Repository) RemoveFromHistory(ctx context.Context, id int64) (int64, error) {
	query := `DELETE FROM import_history WHERE id = ?`
	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return 0, fmt.Errorf("failed to remove history record: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// RemoveFromHistoryByNzbID removes a record from import_history by its original NZB ID
func (r *Repository) RemoveFromHistoryByNzbID(ctx context.Context, nzbID int64) (int64, error) {
	query := `DELETE FROM import_history WHERE nzb_id = ?`
	result, err := r.db.ExecContext(ctx, query, nzbID)
	if err != nil {
		return 0, fmt.Errorf("failed to remove from history: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// RemoveFromQueueBulk removes multiple items from the queue, excluding those currently being processed
func (r *Repository) RemoveFromQueueBulk(ctx context.Context, ids []int64) (*BulkOperationResult, error) {
	if len(ids) == 0 {
		return &BulkOperationResult{}, nil
	}

	// Build placeholders for the IN clause
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	// First, count how many items are currently processing
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM import_queue WHERE id IN (%s) AND status = ?`, strings.Join(placeholders, ","))
	countArgs := append(args, QueueStatusProcessing)

	var processingCount int64
	err := r.db.QueryRowContext(ctx, countQuery, countArgs...).Scan(&processingCount)
	if err != nil {
		return nil, fmt.Errorf("failed to count processing items: %w", err)
	}

	// If there are processing items, return error
	if processingCount > 0 {
		return &BulkOperationResult{
			DeletedCount:    0,
			ProcessingCount: int(processingCount),
		}, fmt.Errorf("cannot delete %d items that are currently being processed", processingCount)
	}

	// The history copies and the queue rows go together, in one transaction:
	// a half-applied delete either resurrects the job in the SABnzbd history
	// view or drops history with no queue cleanup to match (issue #586).
	historyQuery := fmt.Sprintf(
		`DELETE FROM import_history WHERE nzb_id IN (SELECT id FROM import_queue WHERE id IN (%s) AND status != ?)`,
		strings.Join(placeholders, ","))
	deleteQuery := fmt.Sprintf(`DELETE FROM import_queue WHERE id IN (%s) AND status != ?`, strings.Join(placeholders, ","))

	txArgs := make([]any, 0, len(args)+1)
	txArgs = append(txArgs, args...)
	txArgs = append(txArgs, QueueStatusProcessing)

	var rowsAffected int64
	if err := r.WithTransaction(ctx, func(tx *Repository) error {
		if _, err := tx.db.ExecContext(ctx, historyQuery, txArgs...); err != nil {
			return fmt.Errorf("failed to remove import history for queue items: %w", err)
		}

		result, err := tx.db.ExecContext(ctx, deleteQuery, txArgs...)
		if err != nil {
			return fmt.Errorf("failed to remove items from queue: %w", err)
		}

		rowsAffected, err = result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &BulkOperationResult{
		DeletedCount:    int(rowsAffected),
		ProcessingCount: 0,
	}, nil
}

// RestartQueueItemsBulk resets multiple queue items to pending status for reprocessing
func (r *Repository) RestartQueueItemsBulk(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	// Build placeholders for the IN clause
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	// Reset items to pending status with cleared retry count and timestamps
	query := fmt.Sprintf(`
		UPDATE import_queue 
		SET status = 'pending',
		    retry_count = 0,
		    error_message = NULL,
		    started_at = NULL,
		    completed_at = NULL,
		    updated_at = datetime('now')
		WHERE id IN (%s)
	`, strings.Join(placeholders, ","))

	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to restart queue items: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no queue items found to restart")
	}

	return nil
}

// GetQueueStats retrieves current queue statistics
func (r *Repository) GetQueueStats(ctx context.Context) (*QueueStats, error) {
	// Update stats from actual queue data
	err := r.UpdateQueueStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to update queue stats: %w", err)
	}

	query := `
		SELECT id, total_queued, total_processing, total_completed, total_failed, 
		       avg_processing_time_ms, last_updated
		FROM queue_stats ORDER BY id DESC LIMIT 1
	`

	var stats QueueStats
	err = r.db.QueryRowContext(ctx, query).Scan(
		&stats.ID, &stats.TotalQueued, &stats.TotalProcessing, &stats.TotalCompleted,
		&stats.TotalFailed, &stats.AvgProcessingTimeMs, &stats.LastUpdated,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			// Initialize default stats if none exist
			defaultStats := &QueueStats{
				TotalQueued:     0,
				TotalProcessing: 0,
				TotalCompleted:  0,
				TotalFailed:     0,
				LastUpdated:     time.Now(),
			}
			return defaultStats, nil
		}
		return nil, fmt.Errorf("failed to get queue stats: %w", err)
	}

	return &stats, nil
}

// UpdateQueueStats updates queue statistics based on current queue state
func (r *Repository) UpdateQueueStats(ctx context.Context) error {
	// Get current counts
	countQueries := []string{
		`SELECT COUNT(*) FROM import_queue WHERE status = 'pending'`,
		`SELECT COUNT(*) FROM import_queue WHERE status = 'processing'`,
		`SELECT COUNT(*) FROM import_queue WHERE status = 'completed'`,
		`SELECT COUNT(*) FROM import_queue WHERE status = 'failed'`,
	}

	var counts [4]int
	for i, query := range countQueries {
		err := r.db.QueryRowContext(ctx, query).Scan(&counts[i])
		if err != nil {
			return fmt.Errorf("failed to get count for query %d: %w", i, err)
		}
	}

	// Calculate average processing time for completed items
	var avgProcessingTimeFloat sql.NullFloat64
	avgQuery := fmt.Sprintf(`
		SELECT AVG(%s)
		FROM import_queue
		WHERE status = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL
	`, r.dialect.AvgProcessingTimeMS("started_at", "completed_at"))
	err := r.db.QueryRowContext(ctx, avgQuery).Scan(&avgProcessingTimeFloat)
	if err != nil {
		return fmt.Errorf("failed to calculate average processing time: %w", err)
	}

	// Convert float to int64 for storage
	var avgProcessingTime sql.NullInt64
	if avgProcessingTimeFloat.Valid {
		avgProcessingTime = sql.NullInt64{
			Int64: int64(avgProcessingTimeFloat.Float64),
			Valid: true,
		}
	}

	// Update or insert stats
	updateQuery := `
		UPDATE queue_stats SET 
		total_queued = ?, total_processing = ?, total_completed = ?, total_failed = ?,
		avg_processing_time_ms = ?, last_updated = ?
		WHERE id = (SELECT MAX(id) FROM queue_stats)
	`

	var avgTime any
	if avgProcessingTime.Valid {
		avgTime = avgProcessingTime.Int64
	} else {
		avgTime = nil
	}

	_, err = r.db.ExecContext(ctx, updateQuery, counts[0], counts[1], counts[2], counts[3], avgTime, time.Now())
	if err != nil {
		return fmt.Errorf("failed to update queue stats: %w", err)
	}

	return nil
}

// ListQueueItems retrieves queue items with optional filtering
func (r *Repository) ListQueueItems(ctx context.Context, status *QueueStatus, search string, category string, limit, offset int, sortBy, sortOrder string) ([]*ImportQueueItem, error) {
	var query string
	var args []any

	baseSelect := `SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
	               started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, indexer
	               FROM import_queue`

	var conditions []string
	var conditionArgs []any

	if status != nil {
		conditions = append(conditions, "status = ?")
		conditionArgs = append(conditionArgs, *status)
	}

	if search != "" {
		conditions = append(conditions, "(nzb_path LIKE ? OR relative_path LIKE ?)")
		searchPattern := "%" + search + "%"
		conditionArgs = append(conditionArgs, searchPattern, searchPattern)
	}

	if category != "" {
		conditions = append(conditions, "LOWER(category) = LOWER(?)")
		conditionArgs = append(conditionArgs, category)
	}

	if len(conditions) > 0 {
		query = baseSelect + " WHERE " + strings.Join(conditions, " AND ")
	} else {
		query = baseSelect
	}

	// Build ORDER BY clause with validation
	var orderByColumn string
	switch sortBy {
	case "created_at":
		orderByColumn = "created_at"
	case "updated_at":
		orderByColumn = "updated_at"
	case "status":
		orderByColumn = "status"
	case "nzb_path":
		orderByColumn = "nzb_path"
	default:
		orderByColumn = "updated_at"
	}

	sortDirection := "DESC"
	if sortOrder == "asc" {
		sortDirection = "ASC"
	}

	query += fmt.Sprintf(" ORDER BY %s %s LIMIT ? OFFSET ?", orderByColumn, sortDirection)
	args = append(conditionArgs, limit, offset)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list queue items: %w", err)
	}
	defer rows.Close()

	var items []*ImportQueueItem
	for rows.Next() {
		var item ImportQueueItem
		err := rows.Scan(
			&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
			&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
			&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.Indexer,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan queue item: %w", err)
		}
		items = append(items, &item)
	}

	return items, rows.Err()
}

// ListActiveQueueItems retrieves pending and processing queue items
func (r *Repository) ListActiveQueueItems(ctx context.Context, search string, category string, limit, offset int, sortBy, sortOrder string) ([]*ImportQueueItem, error) {
	var query string
	var args []any

	baseSelect := `SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
	               started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path, indexer
	               FROM import_queue`

	conditions := []string{"(status = 'pending' OR status = 'processing' OR status = 'paused')"}
	var conditionArgs []any

	if search != "" {
		conditions = append(conditions, "(nzb_path LIKE ? OR relative_path LIKE ?)")
		searchPattern := "%" + search + "%"
		conditionArgs = append(conditionArgs, searchPattern, searchPattern)
	}

	if category != "" {
		conditions = append(conditions, "LOWER(category) = LOWER(?)")
		conditionArgs = append(conditionArgs, category)
	}

	query = baseSelect + " WHERE " + strings.Join(conditions, " AND ")

	// Build ORDER BY clause with validation
	var orderByColumn string
	switch sortBy {
	case "created_at":
		orderByColumn = "created_at"
	case "updated_at":
		orderByColumn = "updated_at"
	case "status":
		orderByColumn = "status"
	case "nzb_path":
		orderByColumn = "nzb_path"
	default:
		orderByColumn = "updated_at"
	}

	sortDirection := "DESC"
	if sortOrder == "asc" {
		sortDirection = "ASC"
	}

	query += fmt.Sprintf(" ORDER BY %s %s LIMIT ? OFFSET ?", orderByColumn, sortDirection)
	args = append(conditionArgs, limit, offset)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list active queue items: %w", err)
	}
	defer rows.Close()

	var items []*ImportQueueItem
	for rows.Next() {
		var item ImportQueueItem
		err := rows.Scan(
			&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
			&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
			&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath, &item.Indexer,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan queue item: %w", err)
		}
		items = append(items, &item)
	}

	return items, rows.Err()
}

// CountQueueItems counts the total number of queue items matching the given filters
func (r *Repository) CountQueueItems(ctx context.Context, status *QueueStatus, search string, category string) (int, error) {
	var query string
	var args []any

	baseQuery := `SELECT COUNT(*) FROM import_queue`

	var conditions []string
	var conditionArgs []any

	if status != nil {
		conditions = append(conditions, "status = ?")
		conditionArgs = append(conditionArgs, *status)
	}

	if search != "" {
		conditions = append(conditions, "(nzb_path LIKE ? OR relative_path LIKE ?)")
		searchPattern := "%" + search + "%"
		conditionArgs = append(conditionArgs, searchPattern, searchPattern)
	}

	if category != "" {
		conditions = append(conditions, "LOWER(category) = LOWER(?)")
		conditionArgs = append(conditionArgs, category)
	}

	if len(conditions) > 0 {
		query = baseQuery + " WHERE " + strings.Join(conditions, " AND ")
	} else {
		query = baseQuery
	}

	args = conditionArgs

	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count queue items: %w", err)
	}

	return count, nil
}

// CountActiveQueueItems counts the total number of pending and processing queue items
func (r *Repository) CountActiveQueueItems(ctx context.Context, search string, category string) (int, error) {
	var query string
	var args []any

	baseQuery := `SELECT COUNT(*) FROM import_queue WHERE (status = 'pending' OR status = 'processing' OR status = 'paused')`

	var conditions []string
	var conditionArgs []any

	if search != "" {
		conditions = append(conditions, "(nzb_path LIKE ? OR relative_path LIKE ?)")
		searchPattern := "%" + search + "%"
		conditionArgs = append(conditionArgs, searchPattern, searchPattern)
	}

	if category != "" {
		conditions = append(conditions, "LOWER(category) = LOWER(?)")
		conditionArgs = append(conditionArgs, category)
	}

	if len(conditions) > 0 {
		query = baseQuery + " AND " + strings.Join(conditions, " AND ")
	} else {
		query = baseQuery
	}

	args = conditionArgs

	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count active queue items: %w", err)
	}

	return count, nil
}

// clearQueueItemsByStatus removes queue rows matching the given statuses and
// returns the nzb_path values of every deleted row so the caller can clean up
// files on disk.
func (r *Repository) clearQueueItemsByStatus(ctx context.Context, statuses ...QueueStatus) ([]string, int, error) {
	if len(statuses) == 0 {
		return nil, 0, nil
	}
	placeholders := strings.Repeat("?,", len(statuses))
	placeholders = placeholders[:len(placeholders)-1]
	args := make([]any, 0, len(statuses))
	for _, s := range statuses {
		args = append(args, s)
	}

	// The path listing and both deletes run in one transaction. Reading the
	// paths outside it let a row that entered the target status in between be
	// deleted without its nzb_path ever reaching the caller for on-disk
	// cleanup, and splitting the two deletes could leave history dropped with
	// the queue rows still present (issue #586).
	//
	// The history delete goes FIRST so the transaction opens with a write.
	// SQLite transactions are DEFERRED: leading with the SELECT would take a
	// read snapshot and only then try to upgrade to a write, and under WAL an
	// upgrade whose snapshot a concurrent commit has invalidated returns
	// SQLITE_BUSY_SNAPSHOT — which busy_timeout does not retry. A "clear
	// completed" fired while the importer commits would simply fail with
	// "database is locked". Writing first takes the write lock up front, which
	// busy_timeout does wait for.
	historyQuery := fmt.Sprintf(
		`DELETE FROM import_history WHERE nzb_id IN (SELECT id FROM import_queue WHERE status IN (%s))`, placeholders)
	selectQuery := fmt.Sprintf(`SELECT nzb_path FROM import_queue WHERE status IN (%s)`, placeholders)
	deleteQuery := fmt.Sprintf(`DELETE FROM import_queue WHERE status IN (%s)`, placeholders)

	paths := []string{}
	var rowsAffected int64
	if err := r.WithTransaction(ctx, func(tx *Repository) error {
		if _, err := tx.db.ExecContext(ctx, historyQuery, args...); err != nil {
			return fmt.Errorf("failed to clear import history for queue items: %w", err)
		}

		rows, err := tx.db.QueryContext(ctx, selectQuery, args...)
		if err != nil {
			return fmt.Errorf("failed to list queue paths: %w", err)
		}
		func() {
			defer rows.Close()
			for rows.Next() {
				var p string
				if err = rows.Scan(&p); err != nil {
					return
				}
				if p != "" {
					paths = append(paths, p)
				}
			}
			if err == nil {
				err = rows.Err()
			}
		}()
		if err != nil {
			return fmt.Errorf("failed to scan queue paths: %w", err)
		}

		result, err := tx.db.ExecContext(ctx, deleteQuery, args...)
		if err != nil {
			return fmt.Errorf("failed to clear queue items: %w", err)
		}
		rowsAffected, err = result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	return paths, int(rowsAffected), nil
}

// ClearCompletedQueueItems removes completed items from the queue and returns
// the paths of deleted rows.
func (r *Repository) ClearCompletedQueueItems(ctx context.Context) ([]string, int, error) {
	return r.clearQueueItemsByStatus(ctx, QueueStatusCompleted)
}

// ClearFailedQueueItems removes failed items from the queue and returns the
// paths of deleted rows.
func (r *Repository) ClearFailedQueueItems(ctx context.Context) ([]string, int, error) {
	return r.clearQueueItemsByStatus(ctx, QueueStatusFailed)
}

// ClearPendingQueueItems removes pending items from the queue and returns the
// paths of deleted rows.
func (r *Repository) ClearPendingQueueItems(ctx context.Context) ([]string, int, error) {
	return r.clearQueueItemsByStatus(ctx, QueueStatusPending)
}

// IsFileInQueue checks if a file is already in the queue (pending or processing)
func (r *Repository) IsFileInQueue(ctx context.Context, filePath string) (bool, error) {
	query := `SELECT 1 FROM import_queue WHERE nzb_path = ? AND (status = 'pending' OR status = 'processing' OR status = 'paused') LIMIT 1`

	var exists int
	err := r.db.QueryRowContext(ctx, query, filePath).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if file in queue: %w", err)
	}

	return true, nil
}

// UpdateQueueItemPriority updates the priority of a queue item
func (r *Repository) UpdateQueueItemPriority(ctx context.Context, id int64, priority QueuePriority) error {
	query := `UPDATE import_queue SET priority = ?, updated_at = datetime('now') WHERE id = ?`
	_, err := r.db.ExecContext(ctx, query, priority, id)
	if err != nil {
		return fmt.Errorf("failed to update queue item priority: %w", err)
	}
	return nil
}

// UpdateQueueItemsPriorityBulk updates the priority of multiple queue items, skipping any that are currently processing.
// Returns the number of items updated and the number skipped.
func (r *Repository) UpdateQueueItemsPriorityBulk(ctx context.Context, ids []int64, priority QueuePriority) (updated int, skipped int, err error) {
	for _, id := range ids {
		var status string
		scanErr := r.db.QueryRowContext(ctx, `SELECT status FROM import_queue WHERE id = ?`, id).Scan(&status)
		if scanErr == sql.ErrNoRows {
			continue
		}
		if scanErr != nil {
			return updated, skipped, fmt.Errorf("failed to check queue item status: %w", scanErr)
		}
		if status == string(QueueStatusProcessing) {
			skipped++
			continue
		}
		_, err = r.db.ExecContext(ctx, `UPDATE import_queue SET priority = ?, updated_at = datetime('now') WHERE id = ?`, priority, id)
		if err != nil {
			return updated, skipped, fmt.Errorf("failed to update queue item priority: %w", err)
		}
		updated++
	}
	return updated, skipped, nil
}

// AddImportHistory records a successful file import in the persistent history table
func (r *Repository) AddImportHistory(ctx context.Context, history *ImportHistory) error {
	virtualPath := normalizeHealthPath(history.VirtualPath)
	query := `
		INSERT INTO import_history (download_id, nzb_id, nzb_name, file_name, file_size, virtual_path, category, indexer, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
	`
	_, err := r.db.ExecContext(ctx, query,
		history.DownloadID, history.NzbID, history.NzbName, history.FileName, history.FileSize,
		virtualPath, history.Category, history.Indexer)
	if err != nil {
		return fmt.Errorf("failed to add import history: %w", err)
	}
	return nil
}

// GetImportHistoryByDownloadID retrieves an import history item by its DownloadID
func (r *Repository) GetImportHistoryByDownloadID(ctx context.Context, downloadID string) (*ImportHistory, error) {
	query := `
		SELECT h.id, h.download_id, h.nzb_id, h.nzb_name, h.file_name, h.file_size, h.virtual_path, f.library_path, h.category, h.metadata, h.indexer, h.completed_at
		FROM import_history h
		LEFT JOIN file_health f ON h.virtual_path = f.file_path
		WHERE h.download_id = ?
		LIMIT 1
	`

	var h ImportHistory
	err := r.db.QueryRowContext(ctx, query, downloadID).Scan(&h.ID, &h.DownloadID, &h.NzbID, &h.NzbName, &h.FileName, &h.FileSize, &h.VirtualPath, &h.LibraryPath, &h.Category, &h.Metadata, &h.Indexer, &h.CompletedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get import history by download_id: %w", err)
	}

	return &h, nil
}

// GetImportHistoryByNzbID retrieves an import history item by its original NZB ID
// (the integer ID of the queue row that produced this history entry). Returns
// (nil, nil) when no matching row exists.
func (r *Repository) GetImportHistoryByNzbID(ctx context.Context, nzbID int64) (*ImportHistory, error) {
	query := `
		SELECT h.id, h.download_id, h.nzb_id, h.nzb_name, h.file_name, h.file_size, h.virtual_path, f.library_path, h.category, h.metadata, h.indexer, h.completed_at
		FROM import_history h
		LEFT JOIN file_health f ON h.virtual_path = f.file_path
		WHERE h.nzb_id = ?
		LIMIT 1
	`

	var h ImportHistory
	err := r.db.QueryRowContext(ctx, query, nzbID).Scan(&h.ID, &h.DownloadID, &h.NzbID, &h.NzbName, &h.FileName, &h.FileSize, &h.VirtualPath, &h.LibraryPath, &h.Category, &h.Metadata, &h.Indexer, &h.CompletedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get import history by nzb_id: %w", err)
	}

	return &h, nil
}

// GetImportHistoryByPath retrieves an import history item by its virtual path
func (r *Repository) GetImportHistoryByPath(ctx context.Context, virtualPath string) (*ImportHistory, error) {
	virtualPath = normalizeHealthPath(virtualPath)
	query := `
		SELECT h.id, h.download_id, h.nzb_id, h.nzb_name, h.file_name, h.file_size, h.virtual_path, f.library_path, h.category, h.metadata, h.indexer, h.completed_at
		FROM import_history h
		LEFT JOIN file_health f ON h.virtual_path = f.file_path
		WHERE h.virtual_path = ?
		LIMIT 1
	`

	var h ImportHistory
	err := r.db.QueryRowContext(ctx, query, virtualPath).Scan(&h.ID, &h.DownloadID, &h.NzbID, &h.NzbName, &h.FileName, &h.FileSize, &h.VirtualPath, &h.LibraryPath, &h.Category, &h.Metadata, &h.Indexer, &h.CompletedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get import history by path: %w", err)
	}

	return &h, nil
}

// ListImportHistory retrieves import history items with optional filtering and pagination
func (r *Repository) ListImportHistory(ctx context.Context, limit, offset int, search string, category string) ([]*ImportHistory, error) {
	query := `
		SELECT h.id, h.download_id, h.nzb_id, h.nzb_name, h.file_name, h.file_size, h.virtual_path, f.library_path, h.category, h.metadata, h.indexer, h.completed_at
		FROM import_history h
		LEFT JOIN file_health f ON h.virtual_path = f.file_path
		WHERE (? = '' OR h.nzb_name LIKE ? OR h.file_name LIKE ? OR h.virtual_path LIKE ?)
		  AND (? = '' OR LOWER(h.category) = LOWER(?))
		ORDER BY h.completed_at DESC
		LIMIT ? OFFSET ?
	`

	searchPattern := "%" + search + "%"
	rows, err := r.db.QueryContext(ctx, query, search, searchPattern, searchPattern, searchPattern, category, category, limit, offset)
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

	return history, rows.Err()
}

// ListRecentImportHistory retrieves import history items completed within the last N minutes.
func (r *Repository) ListRecentImportHistory(ctx context.Context, minutes int, category string) ([]*ImportHistory, error) {
	var cutoffExpr string
	if r.dialect.IsPostgres() {
		cutoffExpr = fmt.Sprintf("NOW() - INTERVAL '%d minutes'", minutes)
	} else {
		cutoffExpr = fmt.Sprintf("datetime('now', '-%d minutes')", minutes)
	}

	query := fmt.Sprintf(`
		SELECT h.id, h.download_id, h.nzb_id, h.nzb_name, h.file_name, h.file_size, h.virtual_path, '' AS library_path, h.category, h.metadata, h.indexer, h.completed_at
		FROM import_history h
		WHERE h.completed_at >= %s
		  AND (? = '' OR LOWER(h.category) = LOWER(?))
		ORDER BY h.completed_at DESC
	`, cutoffExpr)

	rows, err := r.db.QueryContext(ctx, query, category, category)
	if err != nil {
		return nil, fmt.Errorf("failed to list recent import history: %w", err)
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

	return history, rows.Err()
}

// SABnzbdHistoryRow is one flattened row of the unified SABnzbd history view
// (completed queue + persistent history + failed queue). Source lets the caller
// reconstruct the "#596 missing-path -> Failed" behavior, which applies only to
// rows still present in the live completed queue.
type SABnzbdHistoryRow struct {
	Source       string // "completed_queue" | "history" | "failed_queue"
	ID           int64
	DownloadID   *string
	Name         string // nzb_path for queue rows, nzb_name for history rows
	Status       QueueStatus
	FileSize     *int64
	CompletedAt  *time.Time
	Category     *string
	StoragePath  *string
	Metadata     *string
	ErrorMessage *string
}

// sabnzbdHistoryUnion builds the deduplicated UNION-ALL body shared by
// ListSABnzbdHistory and CountSABnzbdHistory, plus its bound args (the category
// filter, repeated once per arm). A completed import can exist in both
// import_queue (status=completed, never auto-pruned) and import_history; the
// anti-join drops the history copy when the live completed queue row is present,
// so each import is counted once. Failed items live only in import_queue and are
// unaffected. Portable across SQLite + Postgres (no window functions, no
// basename/regexp); ? placeholders and FALSE are rewritten by the dialect layer.
func sabnzbdHistoryUnion(category string) (string, []any) {
	body := `
		SELECT 'completed_queue' AS source, q.id, q.download_id, q.nzb_path AS name, q.status,
		       q.file_size, q.completed_at, q.category, q.storage_path, q.metadata, q.error_message,
		       COALESCE(q.completed_at, q.updated_at, q.created_at) AS sort_time
		  FROM import_queue q
		 WHERE q.status = 'completed' AND q.skip_arr_notification = FALSE
		   AND (? = '' OR LOWER(q.category) = LOWER(?))
		UNION ALL
		SELECT 'history', COALESCE(h.nzb_id, h.id), h.download_id, h.nzb_name, 'completed',
		       h.file_size, h.completed_at, h.category, h.virtual_path, h.metadata, NULL,
		       h.completed_at
		  FROM import_history h
		  LEFT JOIN import_queue q
		    ON q.id = h.nzb_id AND q.status = 'completed' AND q.skip_arr_notification = FALSE
		 WHERE q.id IS NULL
		   AND (? = '' OR LOWER(h.category) = LOWER(?))
		UNION ALL
		SELECT 'failed_queue', q.id, q.download_id, q.nzb_path, q.status,
		       q.file_size, q.completed_at, q.category, q.storage_path, q.metadata, q.error_message,
		       COALESCE(q.completed_at, q.updated_at, q.created_at)
		  FROM import_queue q
		 WHERE q.status = 'failed' AND q.skip_arr_notification = FALSE
		   AND (? = '' OR LOWER(q.category) = LOWER(?))`
	args := []any{category, category, category, category, category, category}
	return body, args
}

// ListSABnzbdHistory returns one time-ordered, deduplicated page of the SABnzbd
// history view using real SQL LIMIT/OFFSET.
func (r *Repository) ListSABnzbdHistory(ctx context.Context, category string, limit, offset int) ([]*SABnzbdHistoryRow, error) {
	body, args := sabnzbdHistoryUnion(category)
	query := `SELECT source, id, download_id, name, status, file_size, completed_at, category, storage_path, metadata, error_message
		FROM (` + body + `) t
		ORDER BY sort_time DESC, id DESC
		LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list sabnzbd history: %w", err)
	}
	defer rows.Close()

	var out []*SABnzbdHistoryRow
	for rows.Next() {
		var row SABnzbdHistoryRow
		if err := rows.Scan(&row.Source, &row.ID, &row.DownloadID, &row.Name, &row.Status,
			&row.FileSize, &row.CompletedAt, &row.Category, &row.StoragePath, &row.Metadata, &row.ErrorMessage); err != nil {
			return nil, fmt.Errorf("failed to scan sabnzbd history row: %w", err)
		}
		out = append(out, &row)
	}
	return out, rows.Err()
}

// ListSABnzbdFailedHistory returns the failed-queue rows of the SABnzbd history
// view, newest first, at most max rows. It reads the same union as
// ListSABnzbdHistory, so the category and skip_arr_notification filters match
// the main page exactly. Failed rows stay in import_queue only until the
// FailedItemRetentionHours cleanup deletes them; when that cleanup is off they
// are kept forever, which is why max is required.
func (r *Repository) ListSABnzbdFailedHistory(ctx context.Context, category string, max int) ([]*SABnzbdHistoryRow, error) {
	body, args := sabnzbdHistoryUnion(category)
	query := `SELECT source, id, download_id, name, status, file_size, completed_at, category, storage_path, metadata, error_message
		FROM (` + body + `) t
		WHERE source = 'failed_queue'
		ORDER BY sort_time DESC, id DESC
		LIMIT ?`
	args = append(args, max)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list failed sabnzbd history: %w", err)
	}
	defer rows.Close()

	var out []*SABnzbdHistoryRow
	for rows.Next() {
		var row SABnzbdHistoryRow
		if err := rows.Scan(&row.Source, &row.ID, &row.DownloadID, &row.Name, &row.Status,
			&row.FileSize, &row.CompletedAt, &row.Category, &row.StoragePath, &row.Metadata, &row.ErrorMessage); err != nil {
			return nil, fmt.Errorf("failed to scan failed sabnzbd history row: %w", err)
		}
		out = append(out, &row)
	}
	return out, rows.Err()
}

// CountSABnzbdHistory returns the exact pre-pagination total for the SABnzbd
// history view (for noofslots).
func (r *Repository) CountSABnzbdHistory(ctx context.Context, category string) (int, error) {
	body, args := sabnzbdHistoryUnion(category)
	query := `SELECT COUNT(*) FROM (` + body + `) t`
	var count int
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count sabnzbd history: %w", err)
	}
	return count, nil
}

// GetImportDailyStats retrieves import statistics for the specified number of days
func (r *Repository) GetImportDailyStats(ctx context.Context, days int) ([]*ImportDailyStat, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02")
	query := `
		SELECT day, completed_count, failed_count, bytes_downloaded, updated_at
		FROM import_daily_stats
		WHERE day >= ?
		ORDER BY day ASC
	`

	rows, err := r.db.QueryContext(ctx, query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to get import daily stats: %w", err)
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

// GetImportHourlyStats retrieves import statistics for the specified number of hours
func (r *Repository) GetImportHourlyStats(ctx context.Context, hours int) ([]*ImportHourlyStat, error) {
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

// AddBytesDownloadedToDailyStat increments the bytes_downloaded counter for the current day
func (r *Repository) AddBytesDownloadedToDailyStat(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}

	// Also add to hourly stat for rolling 24h calculation
	_ = r.AddBytesDownloadedToHourlyStat(ctx, bytes)

	query := `
		INSERT INTO import_daily_stats (day, bytes_downloaded, updated_at)
		VALUES (date('now'), ?, datetime('now'))
		ON CONFLICT(day) DO UPDATE SET
		bytes_downloaded = bytes_downloaded + excluded.bytes_downloaded,
		updated_at = datetime('now')
	`

	_, err := r.db.ExecContext(ctx, query, bytes)
	return err
}

// AddProviderBytesToHourlyStat adds bytes downloaded to the current hour's stat for a specific provider
func (r *Repository) AddProviderBytesToHourlyStat(ctx context.Context, providerID string, bytes int64) error {
	// Current hour start
	hour := time.Now().UTC().Truncate(time.Hour)

	query := `
		INSERT INTO provider_hourly_stats (hour, provider_id, bytes_downloaded, updated_at)
		VALUES (?, ?, ?, datetime('now'))
		ON CONFLICT(hour, provider_id) DO UPDATE SET
			bytes_downloaded = bytes_downloaded + excluded.bytes_downloaded,
			updated_at = datetime('now')
	`

	_, err := r.db.ExecContext(ctx, query, hour, providerID, bytes)
	if err != nil {
		return fmt.Errorf("failed to update provider hourly stats: %w", err)
	}

	return nil
}

// GetProviderHourlyStats retrieves provider download statistics for the last N hours
func (r *Repository) GetProviderHourlyStats(ctx context.Context, hours int) (map[string]int64, error) {
	var cutoffExpr string
	if r.dialect.IsPostgres() {
		cutoffExpr = fmt.Sprintf("NOW() - INTERVAL '%d hours'", hours)
	} else {
		cutoffExpr = fmt.Sprintf("datetime('now', '-%d hours')", hours)
	}

	query := fmt.Sprintf(`
		SELECT provider_id, SUM(bytes_downloaded)
		FROM provider_hourly_stats
		WHERE hour >= %s
		GROUP BY provider_id
	`, cutoffExpr)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query provider hourly stats: %w", err)
	}
	defer rows.Close()

	results := make(map[string]int64)
	for rows.Next() {
		var providerID string
		var bytes int64
		if err := rows.Scan(&providerID, &bytes); err != nil {
			return nil, fmt.Errorf("failed to scan provider hourly stat: %w", err)
		}
		results[providerID] = bytes
	}

	return results, rows.Err()
}

// GetProviderHistoricalStats retrieves aggregated data usage per provider over the given number of days.
// The data is grouped by the specified interval.
func (r *Repository) GetProviderHistoricalStats(ctx context.Context, days int, interval string) ([]*ProviderHistoricalStat, error) {
	var cutoffExpr string
	if r.dialect.IsPostgres() {
		cutoffExpr = fmt.Sprintf("NOW() - INTERVAL '%d days'", days)
	} else {
		cutoffExpr = fmt.Sprintf("datetime('now', '-%d days')", days)
	}

	var timeCol string
	switch interval {
	case "yearly":
		if r.dialect.IsPostgres() {
			timeCol = "date_trunc('year', hour)"
		} else {
			timeCol = "strftime('%Y-01-01 00:00:00', hour)"
		}
	case "monthly":
		if r.dialect.IsPostgres() {
			timeCol = "date_trunc('month', hour)"
		} else {
			timeCol = "strftime('%Y-%m-01 00:00:00', hour)"
		}
	case "weekly":
		if r.dialect.IsPostgres() {
			timeCol = "date_trunc('week', hour)"
		} else {
			// SQLite: Adjust to start of the week (Sunday)
			timeCol = "date(hour, 'weekday 0', '-6 days')"
		}
	default: // daily
		if r.dialect.IsPostgres() {
			timeCol = "date_trunc('day', hour)"
		} else {
			timeCol = "date(hour)"
		}
	}

	query := fmt.Sprintf(`
		SELECT %[1]s as ts, provider_id, SUM(bytes_downloaded)
		FROM provider_hourly_stats
		WHERE hour >= %[2]s
		GROUP BY ts, provider_id
		ORDER BY ts ASC
	`, timeCol, cutoffExpr)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query provider historical stats: %w", err)
	}
	defer rows.Close()

	var stats []*ProviderHistoricalStat
	for rows.Next() {
		var stat ProviderHistoricalStat
		var tsRaw interface{}
		if err := rows.Scan(&tsRaw, &stat.ProviderID, &stat.BytesDownloaded); err != nil {
			return nil, fmt.Errorf("failed to scan provider historical stat: %w", err)
		}

		switch v := tsRaw.(type) {
		case time.Time:
			stat.Timestamp = v
		case []byte:
			t, err := time.Parse("2006-01-02 15:04:05", string(v))
			if err == nil {
				stat.Timestamp = t
			} else if t, err := time.Parse("2006-01-02", string(v)); err == nil {
				stat.Timestamp = t
			} else {
				t, _ = time.Parse(time.RFC3339, string(v))
				stat.Timestamp = t
			}
		case string:
			t, err := time.Parse("2006-01-02 15:04:05", v)
			if err == nil {
				stat.Timestamp = t
			} else if t, err := time.Parse("2006-01-02", v); err == nil {
				stat.Timestamp = t
			} else {
				t, _ = time.Parse(time.RFC3339, v)
				stat.Timestamp = t
			}
		}

		stats = append(stats, &stat)
	}

	return stats, rows.Err()
}

// RecordProviderSpeedTest saves a speed test result for a provider
func (r *Repository) RecordProviderSpeedTest(ctx context.Context, providerID string, speedMbps float64) error {
	query := `
		INSERT INTO provider_speed_tests_history (provider_id, speed_mbps, created_at)
		VALUES (?, ?, datetime('now'))
	`
	if r.dialect.IsPostgres() {
		query = `
			INSERT INTO provider_speed_tests_history (provider_id, speed_mbps, created_at)
			VALUES ($1, $2, NOW())
		`
	}

	_, err := r.db.ExecContext(ctx, query, providerID, speedMbps)
	if err != nil {
		return fmt.Errorf("failed to record provider speed test: %w", err)
	}

	return nil
}

// GetProviderSpeedTestHistory retrieves the speed test history over the given number of days
func (r *Repository) GetProviderSpeedTestHistory(ctx context.Context, days int) ([]*ProviderSpeedTestStat, error) {
	var query string
	if r.dialect.IsPostgres() {
		var format string
		if days <= 7 {
			format = "date_trunc('hour', created_at)"
		} else if days <= 60 {
			format = "date_trunc('day', created_at)"
		} else {
			format = "date_trunc('week', created_at)"
		}
		query = fmt.Sprintf(`
			SELECT MIN(id) as id, provider_id, AVG(speed_mbps) as speed_mbps, %s as created_at
			FROM provider_speed_tests_history
			WHERE created_at >= NOW() - INTERVAL '%d days'
			GROUP BY provider_id, %s
			ORDER BY created_at ASC
		`, format, days, format)
	} else {
		var format string
		if days <= 7 {
			format = "strftime('%Y-%m-%d %H:00:00', created_at)"
		} else if days <= 60 {
			format = "strftime('%Y-%m-%d 00:00:00', created_at)"
		} else {
			format = "strftime('%Y-%m-%d 00:00:00', created_at, 'weekday 0', '-6 days')"
		}
		query = fmt.Sprintf(`
			SELECT MIN(id) as id, provider_id, AVG(speed_mbps) as speed_mbps, %s as created_at
			FROM provider_speed_tests_history
			WHERE created_at >= datetime('now', '-%d days')
			GROUP BY provider_id, %s
			ORDER BY created_at ASC
		`, format, days, format)
	}

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query provider speed test history: %w", err)
	}
	defer rows.Close()

	var stats []*ProviderSpeedTestStat
	for rows.Next() {
		var stat ProviderSpeedTestStat
		var tsRaw interface{}
		if err := rows.Scan(&stat.ID, &stat.ProviderID, &stat.SpeedMbps, &tsRaw); err != nil {
			return nil, fmt.Errorf("failed to scan provider speed test history: %w", err)
		}

		switch v := tsRaw.(type) {
		case time.Time:
			stat.CreatedAt = v
		case []byte:
			t, err := time.Parse("2006-01-02 15:04:05", string(v))
			if err == nil {
				stat.CreatedAt = t
			} else {
				t, _ = time.Parse(time.RFC3339, string(v))
				stat.CreatedAt = t
			}
		case string:
			t, err := time.Parse("2006-01-02 15:04:05", v)
			if err == nil {
				stat.CreatedAt = t
			} else {
				t, _ = time.Parse(time.RFC3339, v)
				stat.CreatedAt = t
			}
		}

		stats = append(stats, &stat)
	}

	return stats, rows.Err()
}

// AddBytesDownloadedToHourlyStat increments the bytes_downloaded counter for the current hour
func (r *Repository) AddBytesDownloadedToHourlyStat(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}

	// Calculate start of current hour: YYYY-MM-DD HH:00:00
	currentHour := time.Now().UTC().Truncate(time.Hour)

	query := `
		INSERT INTO import_hourly_stats (hour, bytes_downloaded, updated_at)
		VALUES (?, ?, datetime('now'))
		ON CONFLICT(hour) DO UPDATE SET
		bytes_downloaded = bytes_downloaded + excluded.bytes_downloaded,
		updated_at = datetime('now')
	`

	_, err := r.db.ExecContext(ctx, query, currentHour, bytes)
	return err
}

// IncrementHourlyStat increments the completed or failed count for the current hour
func (r *Repository) IncrementHourlyStat(ctx context.Context, statType string) error {
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

// GetImportHistory retrieves historical import statistics for the last N days (Alias for GetImportDailyStats)
func (r *Repository) GetImportHistory(ctx context.Context, days int) ([]*ImportDailyStat, error) {
	return r.GetImportDailyStats(ctx, days)
}

// GetImportHistoryItem retrieves a specific import history item by ID
func (r *Repository) GetImportHistoryItem(ctx context.Context, id int64) (*ImportHistory, error) {
	query := `
		SELECT h.id, h.download_id, h.nzb_id, h.nzb_name, h.file_name, h.file_size, h.virtual_path, f.library_path, h.category, h.completed_at
		FROM import_history h
		LEFT JOIN file_health f ON h.virtual_path = f.file_path
		WHERE h.id = ?
	`

	var h ImportHistory
	err := r.db.QueryRowContext(ctx, query, id).Scan(&h.ID, &h.DownloadID, &h.NzbID, &h.NzbName, &h.FileName, &h.FileSize, &h.VirtualPath, &h.LibraryPath, &h.Category, &h.CompletedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get import history item: %w", err)
	}

	return &h, nil
}

// GetSystemStats retrieves all system statistics as a map
func (r *Repository) GetSystemStats(ctx context.Context) (map[string]int64, error) {
	query := `SELECT key, value FROM system_stats`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get system stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]int64)
	for rows.Next() {
		var key string
		var value int64
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("failed to scan system stat: %w", err)
		}
		stats[key] = value
	}

	return stats, nil
}

// parseSQLiteDate attempts to parse a date string from SQLite in various formats
func parseSQLiteDate(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}

	layouts := []string{
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}

	// Try with a space before the offset if it looks like +HH:MM
	if len(raw) > 6 && (raw[len(raw)-6] == '+' || raw[len(raw)-6] == '-') {
		modified := raw[:len(raw)-6] + " " + raw[len(raw)-6:]
		for _, layout := range layouts {
			if t, err := time.Parse(layout, modified); err == nil {
				return t, true
			}
		}
	}

	return time.Time{}, false
}

// GetOldestStatDate returns the date of the oldest record in import_daily_stats or import_history
func (r *Repository) GetOldestStatDate(ctx context.Context) (time.Time, error) {
	// Check daily stats first (this tracks from the very beginning of the system stats feature)
	queryDaily := `SELECT MIN(day) FROM import_daily_stats`
	var oldestDaily string
	_ = r.db.QueryRowContext(ctx, queryDaily).Scan(&oldestDaily)

	// Check history (this tracks specific imported files)
	queryHistory := `SELECT MIN(completed_at) FROM import_history`
	var oldestHistory string
	_ = r.db.QueryRowContext(ctx, queryHistory).Scan(&oldestHistory)

	// Check system_stats itself (oldest entry updated_at)
	queryStats := `SELECT MIN(updated_at) FROM system_stats`
	var oldestStats string
	_ = r.db.QueryRowContext(ctx, queryStats).Scan(&oldestStats)

	now := time.Now()
	var oldest time.Time

	for _, raw := range []string{oldestDaily, oldestHistory, oldestStats} {
		if t, ok := parseSQLiteDate(raw); ok {
			if oldest.IsZero() || t.Before(oldest) {
				oldest = t
			}
		}
	}

	if oldest.IsZero() {
		return now, nil
	}

	return oldest, nil
}

// GetOldestProviderStatDates returns the date of the oldest record per provider in provider_hourly_stats
func (r *Repository) GetOldestProviderStatDates(ctx context.Context) (map[string]time.Time, error) {
	query := `SELECT provider_id, MIN(hour) FROM provider_hourly_stats GROUP BY provider_id`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get oldest provider stats: %w", err)
	}
	defer rows.Close()

	dates := make(map[string]time.Time)
	for rows.Next() {
		var providerID string
		var oldestHourStr string
		if err := rows.Scan(&providerID, &oldestHourStr); err != nil {
			return nil, fmt.Errorf("failed to scan oldest provider date: %w", err)
		}

		if t, ok := parseSQLiteDate(oldestHourStr); ok {
			dates[providerID] = t
		}
	}

	return dates, nil
}

// UpdateSystemStat updates or inserts a single system statistic
func (r *Repository) UpdateSystemStat(ctx context.Context, key string, value int64) error {
	query := `
		INSERT INTO system_stats (key, value, updated_at)
		VALUES (?, ?, datetime('now'))
		ON CONFLICT(key) DO UPDATE SET
		value = excluded.value,
		updated_at = datetime('now')
	`
	_, err := r.db.ExecContext(ctx, query, key, value)
	if err != nil {
		return fmt.Errorf("failed to update system stat %s: %w", key, err)
	}
	return nil
}

// MigrateSystemStats atomically writes replacement system-stat keys and removes
// a bounded set of legacy keys. Callers must treat returned errors as sensitive
// and avoid logging their raw values.
func (r *Repository) MigrateSystemStats(ctx context.Context, stats map[string]int64, deleteKeys []string) error {
	if len(stats) == 0 && len(deleteKeys) == 0 {
		return nil
	}

	return r.WithTransaction(ctx, func(txRepo *Repository) error {
		for key, value := range stats {
			query := `
				INSERT INTO system_stats (key, value, updated_at)
				VALUES (?, ?, CURRENT_TIMESTAMP)
				ON CONFLICT(key) DO UPDATE SET
				value = excluded.value,
				updated_at = CURRENT_TIMESTAMP
			`
			if _, err := txRepo.db.ExecContext(ctx, query, key, value); err != nil {
				return fmt.Errorf("failed to write migrated system stat: %w", err)
			}
		}
		for _, key := range deleteKeys {
			if _, err := txRepo.db.ExecContext(ctx, `DELETE FROM system_stats WHERE key = ?`, key); err != nil {
				return fmt.Errorf("failed to remove migrated system stat: %w", err)
			}
		}
		return nil
	})
}

// BatchUpdateSystemStats updates multiple system statistics in a single transaction
func (r *Repository) BatchUpdateSystemStats(ctx context.Context, stats map[string]int64) error {
	if len(stats) == 0 {
		return nil
	}

	ddb, ok := r.db.(*dialectAwareDB)
	if !ok {
		return fmt.Errorf("repository not connected to dialectAwareDB, cannot begin transaction")
	}

	tx, err := ddb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		INSERT INTO system_stats (key, value, updated_at)
		VALUES (?, ?, datetime('now'))
		ON CONFLICT(key) DO UPDATE SET
		value = excluded.value,
		updated_at = datetime('now')
	`

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for key, value := range stats {
		if _, err := stmt.ExecContext(ctx, key, value); err != nil {
			return fmt.Errorf("failed to execute statement for key %s: %w", key, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// UpdateSystemState updates a system state string (JSON) by key
func (r *Repository) UpdateSystemState(ctx context.Context, key string, value string) error {
	query := `
		INSERT INTO system_state (key, value, updated_at)
		VALUES (?, ?, datetime('now'))
		ON CONFLICT(key) DO UPDATE SET
		value = excluded.value,
		updated_at = datetime('now')
	`
	_, err := r.db.ExecContext(ctx, query, key, value)
	if err != nil {
		return fmt.Errorf("failed to update system state %s: %w", key, err)
	}
	return nil
}

// GetSystemState retrieves a system state string by key
func (r *Repository) GetSystemState(ctx context.Context, key string) (string, error) {
	query := `SELECT value FROM system_state WHERE key = ?`
	var value string
	err := r.db.QueryRowContext(ctx, query, key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("failed to get system state %s: %w", key, err)
	}
	return value, nil
}

// ClearImportHistory deletes all records from the import_history and import_daily_stats tables
func (r *Repository) ClearImportHistory(ctx context.Context) error {
	// Clear history records
	queryHistory := `DELETE FROM import_history`
	if _, err := r.db.ExecContext(ctx, queryHistory); err != nil {
		return fmt.Errorf("failed to clear import history: %w", err)
	}

	// Also clear daily stats
	return r.ClearDailyStats(ctx)
}

// ClearImportHistorySince deletes records from the import_history and import_queue tables
// since the specified time, and adjusts the import_daily_stats accordingly.
func (r *Repository) ClearImportHistorySince(ctx context.Context, since time.Time) error {
	return r.WithTransaction(ctx, func(txRepo *Repository) error {
		// 1. Count completed items in history per day
		queryHistoryCounts := `
			SELECT date(completed_at) as day, COUNT(*) as count
			FROM import_history
			WHERE completed_at >= ?
			GROUP BY day
		`
		rows, err := txRepo.db.QueryContext(ctx, queryHistoryCounts, since)
		if err != nil {
			return fmt.Errorf("failed to query history counts: %w", err)
		}
		defer rows.Close()

		type dayCount struct {
			day   string
			count int
		}
		var historyCounts []dayCount
		for rows.Next() {
			var dc dayCount
			if err := rows.Scan(&dc.day, &dc.count); err != nil {
				return err
			}
			historyCounts = append(historyCounts, dc)
		}
		rows.Close()

		// 2. Count failed items in queue per day
		queryFailedCounts := `
			SELECT date(updated_at) as day, COUNT(*) as count
			FROM import_queue
			WHERE status = 'failed' AND updated_at >= ?
			GROUP BY day
		`
		rows, err = txRepo.db.QueryContext(ctx, queryFailedCounts, since)
		if err != nil {
			return fmt.Errorf("failed to query failed counts: %w", err)
		}
		defer rows.Close()

		var failedCounts []dayCount
		for rows.Next() {
			var dc dayCount
			if err := rows.Scan(&dc.day, &dc.count); err != nil {
				return err
			}
			failedCounts = append(failedCounts, dc)
		}
		rows.Close()

		// 3. Decrement daily stats for completed items
		for _, hc := range historyCounts {
			queryUpdate := `
				UPDATE import_daily_stats 
				SET completed_count = MAX(0, completed_count - ?), updated_at = datetime('now')
				WHERE day = ?
			`
			if _, err := txRepo.db.ExecContext(ctx, queryUpdate, hc.count, hc.day); err != nil {
				return fmt.Errorf("failed to decrement completed daily stats: %w", err)
			}
		}

		// 4. Decrement daily stats for failed items
		for _, fc := range failedCounts {
			queryUpdate := `
				UPDATE import_daily_stats 
				SET failed_count = MAX(0, failed_count - ?), updated_at = datetime('now')
				WHERE day = ?
			`
			if _, err := txRepo.db.ExecContext(ctx, queryUpdate, fc.count, fc.day); err != nil {
				return fmt.Errorf("failed to decrement failed daily stats: %w", err)
			}
		}

		// 5. Delete from import_history
		queryDeleteHistory := `DELETE FROM import_history WHERE completed_at >= ?`
		if _, err := txRepo.db.ExecContext(ctx, queryDeleteHistory, since); err != nil {
			return fmt.Errorf("failed to delete from import history: %w", err)
		}

		// 6. Delete from import_queue (completed and failed items)
		queryDeleteQueue := `
			DELETE FROM import_queue 
			WHERE status IN ('completed', 'failed') 
			  AND (
				(status = 'completed' AND completed_at >= ?) OR 
				(status = 'failed' AND updated_at >= ?)
			  )
		`
		if _, err := txRepo.db.ExecContext(ctx, queryDeleteQueue, since, since); err != nil {
			return fmt.Errorf("failed to delete from import queue: %w", err)
		}

		return nil
	})
}

// ClearDailyStats deletes all records from the import_daily_stats, import_hourly_stats, and provider_hourly_stats tables
func (r *Repository) ClearDailyStats(ctx context.Context) error {
	queryDaily := `DELETE FROM import_daily_stats`
	if _, err := r.db.ExecContext(ctx, queryDaily); err != nil {
		return fmt.Errorf("failed to clear daily stats: %w", err)
	}

	if err := r.ClearHourlyStats(ctx); err != nil {
		return err
	}

	return r.ClearProviderHourlyStats(ctx)
}

// ClearDailyStatsSince deletes records from the import_daily_stats table since the specified time
func (r *Repository) ClearDailyStatsSince(ctx context.Context, since time.Time) error {
	day := since.Format("2006-01-02")
	query := `DELETE FROM import_daily_stats WHERE day >= ?`
	if _, err := r.db.ExecContext(ctx, query, day); err != nil {
		return fmt.Errorf("failed to clear daily stats since: %w", err)
	}
	return r.ClearHourlyStatsSince(ctx, since)
}

// ClearHourlyStats deletes all records from the import_hourly_stats table
func (r *Repository) ClearHourlyStats(ctx context.Context) error {
	queryHourly := `DELETE FROM import_hourly_stats`
	if _, err := r.db.ExecContext(ctx, queryHourly); err != nil {
		return fmt.Errorf("failed to clear hourly stats: %w", err)
	}
	return nil
}

// ClearProviderHourlyStats deletes all records from the provider_hourly_stats table
func (r *Repository) ClearProviderHourlyStats(ctx context.Context) error {
	queryProviderHourly := `DELETE FROM provider_hourly_stats`
	if _, err := r.db.ExecContext(ctx, queryProviderHourly); err != nil {
		return fmt.Errorf("failed to clear provider hourly stats: %w", err)
	}
	return nil
}

// ClearHourlyStatsSince deletes records from the import_hourly_stats table since the specified time
func (r *Repository) ClearHourlyStatsSince(ctx context.Context, since time.Time) error {
	query := `DELETE FROM import_hourly_stats WHERE hour >= ?`
	if _, err := r.db.ExecContext(ctx, query, since); err != nil {
		return fmt.Errorf("failed to clear hourly stats since: %w", err)
	}
	return nil
}

// GetExpiredStremioQueueItems returns completed Stremio queue items whose completed_at
// is older than ttlHours. Items are identified as Stremio-originated by their download_id
// having the "stremio:" prefix set when the addon enqueues an import.
func (r *Repository) GetExpiredStremioQueueItems(ctx context.Context, ttlHours int) ([]*ImportQueueItem, error) {
	var cutoffExpr string
	if r.dialect.IsPostgres() {
		cutoffExpr = fmt.Sprintf("NOW() - INTERVAL '%d hours'", ttlHours)
	} else {
		cutoffExpr = fmt.Sprintf("datetime('now', '-%d hours')", ttlHours)
	}

	query := fmt.Sprintf(`
		SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
		       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path
		FROM import_queue
		WHERE status = 'completed'
		  AND completed_at < %s
		  AND download_id LIKE 'stremio:%%'
		ORDER BY completed_at ASC
		LIMIT 100
	`, cutoffExpr)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query expired stremio queue items: %w", err)
	}
	defer rows.Close()

	var items []*ImportQueueItem
	for rows.Next() {
		var item ImportQueueItem
		err := rows.Scan(
			&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
			&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
			&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan expired stremio queue item: %w", err)
		}
		items = append(items, &item)
	}

	return items, rows.Err()
}

// GetFailedStremioQueueItems returns Stremio-originated queue items whose import
// failed. Used by the Stremio stream handler to exclude releases that are known not
// to work, so they stop being offered (and re-picked) on every search.
//
// Ordered by updated_at because UpdateQueueItemStatus does not set completed_at on
// failure. Age filtering (Stremio.FailedReleaseTTLHours) is applied by the caller,
// mirroring GetCachedStremioQueueItems.
func (r *Repository) GetFailedStremioQueueItems(ctx context.Context) ([]*ImportQueueItem, error) {
	query := `
		SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
		       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path
		FROM import_queue
		WHERE status = 'failed'
		  AND download_id LIKE 'stremio:%'
		ORDER BY updated_at DESC
		LIMIT 500
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query failed stremio queue items: %w", err)
	}
	defer rows.Close()

	var items []*ImportQueueItem
	for rows.Next() {
		var item ImportQueueItem
		err := rows.Scan(
			&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
			&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
			&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan failed stremio queue item: %w", err)
		}
		items = append(items, &item)
	}

	return items, rows.Err()
}

// GetCachedStremioQueueItems returns completed queue items that have a storage path
// (i.e. are streamable). When reuseLibrary is true, it returns completed imports from
// all sources (Sonarr, Radarr, SABnzbd, Stremio) across the entire library.
//
// ttlHours, when positive, restricts results to items that completed within that
// window. The caller applies the same TTL when building its match set; pushing
// it down here keeps the row count bounded by recency rather than by library
// size, which matters because this runs on every Stremio stream request.
func (r *Repository) GetCachedStremioQueueItems(ctx context.Context, reuseLibrary bool, ttlHours int) ([]*ImportQueueItem, error) {
	const columns = `SELECT id, download_id, nzb_path, relative_path, category, priority, status, created_at, updated_at,
			       started_at, completed_at, retry_count, max_retries, error_message, batch_id, metadata, file_size, storage_path, target_path
			FROM import_queue
			WHERE status = 'completed'
			  AND storage_path IS NOT NULL
			  AND storage_path != ''`

	query := columns
	limit := 1000
	if !reuseLibrary {
		query += ` AND download_id LIKE 'stremio:%'`
		limit = 500
	}

	var args []interface{}
	if ttlHours > 0 {
		query += ` AND completed_at IS NOT NULL AND completed_at >= ?`
		args = append(args, time.Now().Add(-time.Duration(ttlHours)*time.Hour))
	}

	query += fmt.Sprintf(" ORDER BY completed_at DESC LIMIT %d", limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query cached stremio queue items: %w", err)
	}
	defer rows.Close()

	var items []*ImportQueueItem
	for rows.Next() {
		var item ImportQueueItem
		err := rows.Scan(
			&item.ID, &item.DownloadID, &item.NzbPath, &item.RelativePath, &item.Category, &item.Priority, &item.Status,
			&item.CreatedAt, &item.UpdatedAt, &item.StartedAt, &item.CompletedAt,
			&item.RetryCount, &item.MaxRetries, &item.ErrorMessage, &item.BatchID, &item.Metadata, &item.FileSize, &item.StoragePath, &item.TargetPath,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan cached stremio queue item: %w", err)
		}
		items = append(items, &item)
	}

	return items, rows.Err()
}
