package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/model"
)

// HealthRepository handles file health database operations
type HealthRepository struct {
	db      *dialectAwareDB
	dialect dialectHelper
}

// NewHealthRepository creates a new health repository
func NewHealthRepository(db *sql.DB, d Dialect) *HealthRepository {
	return &HealthRepository{
		db:      newDialectAwareDB(db, d),
		dialect: dialectHelper{d: d},
	}
}

// normalizeHealthPath canonicalizes a virtual file path before it is written to or
// matched against file_health. file_path carries a UNIQUE constraint, so a leading
// slash ("/tv/x" vs "tv/x") from one caller would otherwise split a single virtual
// file across two rows and silently defeat the repair_retry_count budget that the
// repair state machine relies on. Every method that writes or matches on file_path
// funnels through here.
//
// Trim removes all leading/trailing separators: imports can yield doubled
// prefixes or a trailing separator ("/tv/x/"). Keeping either variant would
// split one virtual file across keys and defeat history joins and repair state.
func normalizeHealthPath(p string) string {
	p = strings.ReplaceAll(p, `\`, "/")
	p = strings.Trim(p, "/")
	return p
}

// escapeLikePrefix escapes the LIKE metacharacters in a literal prefix so it can be
// safely concatenated into a "prefix/%" pattern. Release/folder names routinely
// contain '_' (matches any single char) and occasionally '%' (matches any run), which
// would otherwise over-match unrelated siblings. Callers must pair the result with an
// explicit `ESCAPE '\'` clause. The backslash is escaped first so it cannot double-escape
// a subsequently-inserted escape character.
func escapeLikePrefix(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

// UpdateFileHealth updates or inserts a file health record
func (r *HealthRepository) UpdateFileHealth(ctx context.Context, filePath string, status HealthStatus, errorMessage *string, sourceNzbPath *string, errorDetails *string, noRetry bool) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		INSERT INTO file_health (file_path, status, last_checked, last_error, source_nzb_path, error_details, retry_count, max_retries, repair_retry_count, created_at, updated_at, scheduled_check_at, priority)
		VALUES (?, ?, datetime('now'), ?, ?, ?, CASE WHEN ? THEN 1 ELSE 0 END, 2, 0, datetime('now'), datetime('now'), datetime('now'), CASE WHEN ? THEN 2 ELSE 0 END)
		ON CONFLICT(file_path) DO UPDATE SET
		status = excluded.status,
		last_checked = datetime('now'),
		last_error = excluded.last_error,
		source_nzb_path = COALESCE(excluded.source_nzb_path, source_nzb_path),
		error_details = excluded.error_details,
		retry_count = CASE WHEN ? THEN max_retries - 1 ELSE retry_count END,
		max_retries = excluded.max_retries,
		updated_at = datetime('now'),
		scheduled_check_at = datetime('now'),
		priority = CASE WHEN ? THEN 2 ELSE priority END
	`

	_, err := r.db.ExecContext(ctx, query, filePath, status, errorMessage, sourceNzbPath, errorDetails, noRetry, noRetry, noRetry, noRetry)
	if err != nil {
		return fmt.Errorf("failed to update file health: %w", err)
	}

	return nil
}

// RecordPar2RepairFailure records the repair error without condemning degraded
// files or cancelling an existing ARR repair. Retry budgets, health-check
// schedules, and library metadata remain intact for those files.
func (r *HealthRepository) RecordPar2RepairFailure(ctx context.Context, filePath, reason string) error {
	filePath = normalizeHealthPath(filePath)
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, last_checked, last_error, scheduled_check_at)
		VALUES (?, 'corrupted', datetime('now'), ?, NULL)
		ON CONFLICT(file_path) DO UPDATE SET
		    status = CASE
		        WHEN file_health.status IN ('degraded', 'repair_triggered') THEN file_health.status
		        ELSE 'corrupted' END,
		    scheduled_check_at = CASE
		        WHEN file_health.status IN ('degraded', 'repair_triggered') THEN file_health.scheduled_check_at
		        ELSE NULL END,
		    last_error = excluded.last_error,
		    last_checked = datetime('now'),
		    updated_at = datetime('now')
	`, filePath, reason)
	if err != nil {
		return fmt.Errorf("failed to record PAR2 repair failure: %w", err)
	}
	return nil
}

// UpdateFileHealthScheduled is like UpdateFileHealth but uses an explicit scheduledAt time
// instead of datetime('now') for the scheduled_check_at column.
func (r *HealthRepository) UpdateFileHealthScheduled(ctx context.Context, filePath string, status HealthStatus, errorMessage *string, sourceNzbPath *string, errorDetails *string, noRetry bool, scheduledAt time.Time) error {
	filePath = normalizeHealthPath(filePath)
	scheduledAtStr := scheduledAt.UTC().Format("2006-01-02 15:04:05")
	query := `
		INSERT INTO file_health (file_path, status, last_checked, last_error, source_nzb_path, error_details, retry_count, max_retries, repair_retry_count, created_at, updated_at, scheduled_check_at, priority)
		VALUES (?, ?, datetime('now'), ?, ?, ?, CASE WHEN ? THEN 1 ELSE 0 END, 2, 0, datetime('now'), datetime('now'), ?, CASE WHEN ? THEN 2 ELSE 0 END)
		ON CONFLICT(file_path) DO UPDATE SET
		status = excluded.status,
		last_checked = datetime('now'),
		last_error = excluded.last_error,
		source_nzb_path = COALESCE(excluded.source_nzb_path, source_nzb_path),
		error_details = excluded.error_details,
		retry_count = CASE WHEN ? THEN max_retries - 1 ELSE retry_count END,
		max_retries = excluded.max_retries,
		updated_at = datetime('now'),
		scheduled_check_at = ?,
		priority = CASE WHEN ? THEN 2 ELSE priority END
	`

	_, err := r.db.ExecContext(ctx, query, filePath, status, errorMessage, sourceNzbPath, errorDetails, noRetry, scheduledAtStr, noRetry, noRetry, scheduledAtStr, noRetry)
	if err != nil {
		return fmt.Errorf("failed to update file health: %w", err)
	}

	return nil
}

// fileHealthSelectColumns is the canonical SELECT … FROM file_health prefix shared
// by the point-lookup queries below. Append a WHERE clause to it. Keeping a single
// source of truth for the column list (and the matching scanFileHealth order) avoids
// drift when a column is added.
const fileHealthSelectColumns = `
	SELECT id, file_path, library_path, status, last_checked, last_error, retry_count, max_retries,
	       repair_retry_count, max_repair_retries, source_nzb_path,
	       error_details, created_at, updated_at, release_date, priority,
		   streaming_failure_count, is_masked
	, metadata, indexer, download_id
	FROM file_health
	`

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanFileHealth scans one row selected via fileHealthSelectColumns into a FileHealth.
// The caller is responsible for sql.ErrNoRows handling (so point lookups can map it to
// (nil, nil) while row-iterating callers treat it normally).
func scanFileHealth(s rowScanner) (*FileHealth, error) {
	var health FileHealth
	err := s.Scan(
		&health.ID, &health.FilePath, &health.LibraryPath, &health.Status, &health.LastChecked,
		&health.LastError, &health.RetryCount, &health.MaxRetries,
		&health.RepairRetryCount, &health.MaxRepairRetries,
		&health.SourceNzbPath, &health.ErrorDetails,
		&health.CreatedAt, &health.UpdatedAt, &health.ReleaseDate, &health.Priority,
		&health.StreamingFailureCount, &health.IsMasked,
		&health.Metadata, &health.Indexer, &health.DownloadID,
	)
	if err != nil {
		return nil, err
	}
	return &health, nil
}

// GetFileHealth retrieves health record for a specific file
func (r *HealthRepository) GetFileHealth(ctx context.Context, filePath string) (*FileHealth, error) {
	filePath = normalizeHealthPath(filePath)
	health, err := scanFileHealth(r.db.QueryRowContext(ctx, fileHealthSelectColumns+"WHERE file_path = ?", filePath))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get file health: %w", err)
	}
	return health, nil
}

// GetFileHealthByLibraryPath retrieves health record for a specific file by its library path
func (r *HealthRepository) GetFileHealthByLibraryPath(ctx context.Context, libraryPath string) (*FileHealth, error) {
	health, err := scanFileHealth(r.db.QueryRowContext(ctx, fileHealthSelectColumns+"WHERE library_path = ?", libraryPath))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get file health by library path: %w", err)
	}
	return health, nil
}

func (r *HealthRepository) GetFileHealthByID(ctx context.Context, id int64) (*FileHealth, error) {
	health, err := scanFileHealth(r.db.QueryRowContext(ctx, fileHealthSelectColumns+"WHERE id = ?", id))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get file health by ID: %w", err)
	}
	return health, nil
}

// IncrementStreamingFailureCount increments the streaming failure count and returns whether masking/repair threshold was reached
func (r *HealthRepository) IncrementStreamingFailureCount(ctx context.Context, filePath string, threshold int) (bool, bool, error) {
	filePath = normalizeHealthPath(filePath)
	query := `
		UPDATE file_health
		SET streaming_failure_count = streaming_failure_count + 1,
		    is_masked = CASE WHEN streaming_failure_count + 1 >= ? THEN TRUE ELSE is_masked END,
		    updated_at = datetime('now')
		WHERE file_path = ?
		RETURNING is_masked, (streaming_failure_count >= ?)
	`

	var isMasked bool
	var shouldRepair bool
	err := r.db.QueryRowContext(ctx, query, threshold, filePath, threshold).Scan(&isMasked, &shouldRepair)
	if err != nil {
		// File not yet in file_health (health system just enabled, or first occurrence
		// before post-import scheduler runs): treat as first failure, proceed with repair.
		if err == sql.ErrNoRows {
			return false, true, nil
		}
		return false, false, fmt.Errorf("failed to increment streaming failure count: %w", err)
	}

	return isMasked, shouldRepair, nil
}

// UnmaskFile removes the mask from a file and resets the failure count
func (r *HealthRepository) UnmaskFile(ctx context.Context, filePath string) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		UPDATE file_health
		SET is_masked = FALSE,
		    streaming_failure_count = 0,
		    updated_at = datetime('now')
		WHERE file_path = ?
	`

	_, err := r.db.ExecContext(ctx, query, filePath)
	if err != nil {
		return fmt.Errorf("failed to unmask file: %w", err)
	}

	return nil
}

// GetUnhealthyFiles returns files that need health checks
func (r *HealthRepository) GetUnhealthyFiles(ctx context.Context, limit int, strategy string, libraryDir string, maxRetries int) ([]*FileHealth, error) {
	query := `
		SELECT id, file_path, status, last_checked, last_error, retry_count, max_retries,
		       repair_retry_count, max_repair_retries, source_nzb_path,
		       error_details, created_at, updated_at, release_date, scheduled_check_at,
			   library_path, priority, streaming_failure_count, is_masked
		, metadata, indexer, download_id
		FROM file_health
		WHERE scheduled_check_at IS NOT NULL
		  AND scheduled_check_at <= datetime('now')
		  AND retry_count < ?
		  -- 'corrupted' is terminal: enforce it at the query level so no re-arm vector
		  -- (e.g. an unconditional release-date backfill writing scheduled_check_at) can
		  -- pull a finalized record back into the check queue. 'repair_triggered' and
		  -- 'checking' are owned by other queries / an in-flight cycle.
		  AND status NOT IN ('repair_triggered', 'checking', 'corrupted')
		  AND (
			  ? = 'NONE' 
			  OR status = 'pending'
			  OR (metadata IS NOT NULL AND metadata != '')
			  OR (library_path IS NOT NULL AND (library_path LIKE ? ESCAPE '!' OR library_path LIKE ? ESCAPE '!'))
			  OR (last_error LIKE '%failed to unmarshal metadata%')
			  OR (last_error LIKE '%failed to read file metadata%')
			  OR (last_error LIKE '%no ARR instance found%')
			  OR (last_error LIKE '%missing % checked segments%')
		  )
		ORDER BY priority DESC, 
		         (CASE WHEN status = 'pending' THEN 0 ELSE 1 END) ASC, 
		         scheduled_check_at ASC
		LIMIT ?
	`

	// Build library directory prefix filters. Windows may store library_path with
	// backslashes even when config paths are normalized with slashes.
	// Normalize the base to forward slashes first so each pattern is internally
	// consistent regardless of whether libraryDir uses forward or backslashes.
	libraryBase := strings.TrimRight(strings.ReplaceAll(libraryDir, `\`, "/"), "/")
	libraryPrefix := libraryBase + "/%"
	libraryPrefixAlt := strings.ReplaceAll(libraryBase, "/", `\`) + `\%`
	rows, err := r.db.QueryContext(ctx, query, maxRetries, strategy, libraryPrefix, libraryPrefixAlt, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query files due for check: %w", err)
	}
	defer rows.Close()

	var files []*FileHealth
	for rows.Next() {
		var health FileHealth
		err := rows.Scan(
			&health.ID, &health.FilePath, &health.Status, &health.LastChecked,
			&health.LastError, &health.RetryCount, &health.MaxRetries,
			&health.RepairRetryCount, &health.MaxRepairRetries,
			&health.SourceNzbPath, &health.ErrorDetails,
			&health.CreatedAt, &health.UpdatedAt, &health.ReleaseDate,
			&health.ScheduledCheckAt,
			&health.LibraryPath,
			&health.Priority,
			&health.StreamingFailureCount,
			&health.IsMasked,
			&health.Metadata, &health.Indexer, &health.DownloadID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file health: %w", err)
		}
		files = append(files, &health)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate unhealthy files: %w", err)
	}

	return files, nil
}

// SetPriority sets the priority for a file health record
func (r *HealthRepository) SetPriority(ctx context.Context, id int64, priority HealthPriority) error {
	query := `
		UPDATE file_health
		SET priority = ?,
		    updated_at = datetime('now')
		WHERE id = ?
	`

	_, err := r.db.ExecContext(ctx, query, priority, id)
	if err != nil {
		return fmt.Errorf("failed to set priority: %w", err)
	}

	return nil
}

// GetFilesForRepairNotification returns files that need repair notification (repair_triggered status).
// Records whose repair_retry_count has reached max_repair_retries are returned too: the worker
// finalizes them as corrupted (prepareRepairNotificationUpdate). Filtering them out here would
// leave them permanently stuck in repair_triggered — no other query ever selects that status.
func (r *HealthRepository) GetFilesForRepairNotification(ctx context.Context, limit int) ([]*FileHealth, error) {
	query := `
		SELECT id, file_path, status, last_checked, last_error, retry_count, max_retries,
		       repair_retry_count, max_repair_retries, source_nzb_path,
		       error_details, created_at, updated_at
		, metadata
		FROM file_health
		WHERE status = 'repair_triggered'
		  AND (scheduled_check_at IS NULL OR scheduled_check_at <= datetime('now'))
		ORDER BY last_checked ASC
		LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query files for repair notification: %w", err)
	}
	defer rows.Close()

	var files []*FileHealth
	for rows.Next() {
		var health FileHealth
		err := rows.Scan(
			&health.ID, &health.FilePath, &health.Status, &health.LastChecked,
			&health.LastError, &health.RetryCount, &health.MaxRetries,
			&health.RepairRetryCount, &health.MaxRepairRetries,
			&health.SourceNzbPath, &health.ErrorDetails,
			&health.CreatedAt, &health.UpdatedAt, &health.Metadata,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file health for repair notification: %w", err)
		}
		files = append(files, &health)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate files for repair notification: %w", err)
	}

	return files, nil
}

// IncrementRetryCount increments the retry count and schedules next check
func (r *HealthRepository) IncrementRetryCount(ctx context.Context, filePath string, errorMessage *string, errorDetails *string, nextCheck time.Time) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		UPDATE file_health
		SET retry_count = retry_count + 1,
		    last_error = ?,
		    error_details = ?,
			status = 'pending',
			scheduled_check_at = ?,
		    updated_at = datetime('now')
		WHERE file_path = ?
	`

	_, err := r.db.ExecContext(ctx, query, errorMessage, errorDetails, nextCheck.UTC().Format("2006-01-02 15:04:05"), filePath)
	if err != nil {
		return fmt.Errorf("failed to increment retry count: %w", err)
	}

	return nil
}

// SetRepairTriggered sets a file's status to repair_triggered
func (r *HealthRepository) SetRepairTriggered(ctx context.Context, filePath string, errorMessage *string, errorDetails *string) error {
	filePath = normalizeHealthPath(filePath)
	query := fmt.Sprintf(`
		UPDATE file_health
		SET status = 'repair_triggered',
		    last_error = ?,
		    error_details = ?,
			scheduled_check_at = %s,
		    updated_at = datetime('now')
		WHERE file_path = ?
	`, r.dialect.DatetimePlusHour())

	result, err := r.db.ExecContext(ctx, query, errorMessage, errorDetails, filePath)
	if err != nil {
		return fmt.Errorf("failed to update file status to repair_triggered: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no file found to update status: %s", filePath)
	}

	return nil
}

// GetHealthStats returns statistics about file health
func (r *HealthRepository) GetHealthStats(ctx context.Context) (map[HealthStatus]int, error) {
	query := `
		SELECT status, COUNT(*) 
		FROM file_health 
		GROUP BY status
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get health stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[HealthStatus]int)
	for rows.Next() {
		var status HealthStatus
		var count int
		err := rows.Scan(&status, &count)
		if err != nil {
			return nil, fmt.Errorf("failed to scan health stats: %w", err)
		}
		stats[status] = count
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate health stats: %w", err)
	}

	return stats, nil
}

// SetRepairTriggeredByID sets a file's status to repair_triggered by ID
func (r *HealthRepository) SetRepairTriggeredByID(ctx context.Context, id int64, errorMessage *string, errorDetails *string) error {
	query := fmt.Sprintf(`
		UPDATE file_health
		SET status = 'repair_triggered',
		    last_error = ?,
		    error_details = ?,
			scheduled_check_at = %s,
		    updated_at = datetime('now')
		WHERE id = ?
	`, r.dialect.DatetimePlusHour())

	result, err := r.db.ExecContext(ctx, query, errorMessage, errorDetails, id)
	if err != nil {
		return fmt.Errorf("failed to update file status to repair_triggered by ID: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no file found to update status with ID: %d", id)
	}

	return nil
}

// SetFileCheckingByID sets a file's status to 'checking' by ID
func (r *HealthRepository) SetFileCheckingByID(ctx context.Context, id int64) error {
	query := `
		UPDATE file_health 
		SET status = ?,
		    updated_at = datetime('now')
		WHERE id = ?
	`

	_, err := r.db.ExecContext(ctx, query, HealthStatusChecking, id)
	if err != nil {
		return fmt.Errorf("failed to set file status to checking by ID: %w", err)
	}

	return nil
}

// DeleteHealthRecordByID removes a specific health record from the database by ID
func (r *HealthRepository) DeleteHealthRecordByID(ctx context.Context, id int64) error {
	query := `DELETE FROM file_health WHERE id = ?`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete health record by ID: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no health record found to delete with ID: %d", id)
	}

	return nil
}

// DeleteHealthRecord removes a specific health record from the database
func (r *HealthRepository) DeleteHealthRecord(ctx context.Context, filePath string) error {
	filePath = normalizeHealthPath(filePath)
	query := `DELETE FROM file_health WHERE file_path = ?`

	result, err := r.db.ExecContext(ctx, query, filePath)
	if err != nil {
		return fmt.Errorf("failed to delete health record: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no health record found to delete: %s", filePath)
	}

	return nil
}

// DeleteHealthRecordByLibraryPath deletes the health record matching the given absolute library path.
// Returns the file_path of the deleted record so the caller can use it for metadata cleanup.
func (r *HealthRepository) DeleteHealthRecordByLibraryPath(ctx context.Context, libraryPath string) (string, error) {
	var filePath string
	selectQuery := `SELECT file_path FROM file_health WHERE library_path = ? LIMIT 1`
	err := r.db.QueryRowContext(ctx, selectQuery, libraryPath).Scan(&filePath)
	if err != nil {
		return "", fmt.Errorf("no health record found for library_path %s: %w", libraryPath, err)
	}

	deleteQuery := `DELETE FROM file_health WHERE library_path = ?`
	if _, err := r.db.ExecContext(ctx, deleteQuery, libraryPath); err != nil {
		return "", fmt.Errorf("failed to delete health record by library_path: %w", err)
	}

	return filePath, nil
}

// DeleteHealthRecordsByLibraryPathPrefix deletes health records where library_path matches the given prefix.
// Returns the file_paths of deleted records for metadata cleanup, plus the count.
func (r *HealthRepository) DeleteHealthRecordsByLibraryPathPrefix(ctx context.Context, libraryPathPrefix string) ([]string, int64, error) {
	if libraryPathPrefix == "" {
		return nil, 0, nil
	}

	likePattern := escapeLikePrefix(libraryPathPrefix) + "/%"
	query := `DELETE FROM file_health WHERE library_path = ? OR library_path LIKE ? ESCAPE '\' RETURNING file_path`
	rows, err := r.db.QueryContext(ctx, query, libraryPathPrefix, likePattern)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to delete health records by library_path prefix %s: %w", libraryPathPrefix, err)
	}
	defer rows.Close()

	var filePaths []string
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			return nil, 0, fmt.Errorf("failed to scan file_path: %w", err)
		}
		filePaths = append(filePaths, fp)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error iterating rows: %w", err)
	}

	return filePaths, int64(len(filePaths)), nil
}

// DeleteHealthRecordsByPrefix removes ALL health records at or under the given virtual
// path prefix. Used by the webhook directory-delete handler, where the whole subtree is
// genuinely gone and every record (including healthy/relinked ones) must be removed.
// For failed-import rollback use DeleteUnvalidatedHealthRecordsByPrefix instead.
func (r *HealthRepository) DeleteHealthRecordsByPrefix(ctx context.Context, prefix string) (int64, error) {
	prefix = normalizeHealthPath(prefix)
	if prefix == "" {
		return 0, nil
	}

	// LIKE metacharacters in the prefix must be escaped: a release folder containing '_'
	// or '%' would otherwise over-match and delete unrelated siblings' records.
	query := `DELETE FROM file_health WHERE file_path = ? OR file_path LIKE ? ESCAPE '\'`
	likePattern := escapeLikePrefix(prefix) + "/%"

	result, err := r.db.ExecContext(ctx, query, prefix, likePattern)
	if err != nil {
		return 0, fmt.Errorf("failed to delete health records by prefix %s: %w", prefix, err)
	}

	return result.RowsAffected()
}

// DeleteUnvalidatedHealthRecordsByPrefix removes only the still-unvalidated placeholder
// records at or under the prefix — those an ARR webhook has not yet relinked to a real
// library path (library_path NULL or still equal to the virtual file_path) and that are
// not in a terminal/repair state. This is the failed-import rollback path: the nzbFolder
// is deterministic per release (not unique per queue item), so a failed re-import of a
// release that previously imported successfully shares the subtree. Scoping to unvalidated
// records protects the prior successful import's healthy/relinked/repair_triggered/corrupted
// records (and the repair budget they carry) from being wiped by an unrelated failed attempt.
func (r *HealthRepository) DeleteUnvalidatedHealthRecordsByPrefix(ctx context.Context, prefix string) (int64, error) {
	prefix = normalizeHealthPath(prefix)
	if prefix == "" {
		return 0, nil
	}

	query := `
		DELETE FROM file_health
		WHERE (file_path = ? OR file_path LIKE ? ESCAPE '\')
		  AND status IN ('pending', 'checking')
		  AND (library_path IS NULL OR library_path = file_path)
	`
	likePattern := escapeLikePrefix(prefix) + "/%"

	result, err := r.db.ExecContext(ctx, query, prefix, likePattern)
	if err != nil {
		return 0, fmt.Errorf("failed to delete unvalidated health records by prefix %s: %w", prefix, err)
	}

	return result.RowsAffected()
}

// RegisterCorruptedFile adds or updates a file as corrupted and schedules it for immediate check/repair
func (r *HealthRepository) RegisterCorruptedFile(ctx context.Context, filePath string, libraryPath *string, errorMessage string) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		INSERT INTO file_health (
			file_path, library_path, status, last_error, error_details,
			retry_count, max_retries, repair_retry_count, max_repair_retries,
			created_at, updated_at, scheduled_check_at, last_checked, priority
		)
		VALUES (?, ?, 'pending', ?, ?, 1, 2, 0, 3, datetime('now'), datetime('now'), datetime('now'), datetime('now'), 2)
		ON CONFLICT(file_path) DO UPDATE SET
			library_path = COALESCE(excluded.library_path, library_path),
			status = 'pending',
			last_error = excluded.last_error,
			error_details = excluded.error_details,
			retry_count = 0,
			scheduled_check_at = datetime('now'),
			last_checked = datetime('now'),
			updated_at = datetime('now'),
			priority = 2
	`

	_, err := r.db.ExecContext(ctx, query, filePath, libraryPath, errorMessage, errorMessage)
	if err != nil {
		return fmt.Errorf("failed to register corrupted file: %w", err)
	}

	return nil
}

// AddFileToHealthCheck adds a file to the health database for checking
func (r *HealthRepository) AddFileToHealthCheck(ctx context.Context, filePath string, libraryPath *string, maxRetries int, maxRepairRetries int, sourceNzbPath *string, priority HealthPriority) error {
	return r.AddFileToHealthCheckWithMetadata(ctx, filePath, libraryPath, maxRetries, maxRepairRetries, sourceNzbPath, priority, nil, nil, nil, nil)
}

// AddFileToHealthCheckWithMetadata adds a file to the health database for checking with metadata.
// On conflict (re-import over an existing record) the record is reset to pending for
// re-validation, but repair_retry_count is intentionally preserved: it is the per-title
// repair budget, so a re-download of a broken release cannot reset its own escalation
// counter. A successful health check resets it to 0.
func (r *HealthRepository) AddFileToHealthCheckWithMetadata(ctx context.Context, filePath string, libraryPath *string, maxRetries int, maxRepairRetries int, sourceNzbPath *string, priority HealthPriority, releaseDate *time.Time, metadata *string, indexer *string, downloadID *string) error {
	filePath = normalizeHealthPath(filePath)
	var releaseDateStr any = nil
	if releaseDate != nil {
		releaseDateStr = releaseDate.UTC().Format("2006-01-02 15:04:05")
	}

	query := `
		INSERT INTO file_health (file_path, library_path, status, last_checked, retry_count, max_retries, repair_retry_count, max_repair_retries, source_nzb_path, priority, release_date, metadata, indexer, download_id, created_at, updated_at, scheduled_check_at)
		VALUES (?, ?, ?, datetime('now'), 0, ?, 0, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), datetime('now'))
		ON CONFLICT(file_path) DO UPDATE SET

		library_path = COALESCE(excluded.library_path, library_path),
		status = excluded.status,
		retry_count = 0,
		last_error = NULL,
		error_details = NULL,
		max_retries = excluded.max_retries,
		max_repair_retries = excluded.max_repair_retries,
		source_nzb_path = COALESCE(excluded.source_nzb_path, source_nzb_path),
		priority = excluded.priority,
		release_date = COALESCE(excluded.release_date, release_date),
		metadata = COALESCE(excluded.metadata, metadata),
		indexer = COALESCE(excluded.indexer, indexer),
		download_id = COALESCE(excluded.download_id, download_id),
		updated_at = datetime('now'),
		scheduled_check_at = datetime('now')
	`

	_, err := r.db.ExecContext(ctx, query, filePath, libraryPath, HealthStatusPending, maxRetries, maxRepairRetries, sourceNzbPath, priority, releaseDateStr, metadata, indexer, downloadID)

	if err != nil {
		return fmt.Errorf("failed to add file to health check: %w", err)
	}

	return nil
}

// HealthCheckUpsert is one record for BatchAddFileToHealthCheck.
type HealthCheckUpsert struct {
	FilePath         string
	LibraryPath      *string
	SourceNzbPath    *string
	Indexer          *string
	Priority         HealthPriority
	MaxRetries       int
	MaxRepairRetries int
	ReleaseDate      *time.Time
	Metadata         *string
	DownloadID       *string
}

// BatchAddFileToHealthCheck upserts many health records in a few multi-row statements
// instead of one transaction per file. It has the SAME conflict semantics as
// AddFileToHealthCheckWithMetadata (reset to pending for re-validation, repair_retry_count
// preserved as the per-title budget). Used by the import post-processor, where a single
// archive/season-pack import can expand to hundreds of per-file checks.
func (r *HealthRepository) BatchAddFileToHealthCheck(ctx context.Context, records []HealthCheckUpsert) error {
	if len(records) == 0 {
		return nil
	}

	// 10 bound params per row; keep batches under SQLite's ~999 parameter limit.
	const batchSize = 95

	for i := 0; i < len(records); i += batchSize {
		end := min(i+batchSize, len(records))
		if err := r.batchUpsertFileHealthCheck(ctx, records[i:end]); err != nil {
			return fmt.Errorf("failed to upsert health-check batch at index %d: %w", i, err)
		}
	}

	return nil
}

// batchUpsertFileHealthCheck performs a single multi-row upsert.
func (r *HealthRepository) batchUpsertFileHealthCheck(ctx context.Context, records []HealthCheckUpsert) error {
	valueStrings := make([]string, len(records))
	args := make([]any, 0, len(records)*10)

	for i, rec := range records {
		// status, retry_count and repair_retry_count are literals so excluded.status is
		// always 'pending' (matching the single-row upsert's bound HealthStatusPending).
		valueStrings[i] = "(?, ?, 'pending', datetime('now'), 0, ?, 0, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), datetime('now'))"

		var releaseDateStr any = nil
		if rec.ReleaseDate != nil {
			releaseDateStr = rec.ReleaseDate.UTC().Format("2006-01-02 15:04:05")
		}

		args = append(args,
			normalizeHealthPath(rec.FilePath), rec.LibraryPath,
			rec.MaxRetries, rec.MaxRepairRetries,
			rec.SourceNzbPath, rec.Priority, releaseDateStr, rec.Metadata, rec.Indexer, rec.DownloadID)
	}

	query := fmt.Sprintf(`
		INSERT INTO file_health (file_path, library_path, status, last_checked, retry_count, max_retries, repair_retry_count, max_repair_retries, source_nzb_path, priority, release_date, metadata, indexer, download_id, created_at, updated_at, scheduled_check_at)
		VALUES %s
		ON CONFLICT(file_path) DO UPDATE SET
			library_path = COALESCE(excluded.library_path, library_path),
			status = excluded.status,
			retry_count = 0,
			last_error = NULL,
			error_details = NULL,
			max_retries = excluded.max_retries,
			max_repair_retries = excluded.max_repair_retries,
			source_nzb_path = COALESCE(excluded.source_nzb_path, source_nzb_path),
			priority = excluded.priority,
			release_date = COALESCE(excluded.release_date, release_date),
			metadata = COALESCE(excluded.metadata, metadata),
			indexer = COALESCE(excluded.indexer, indexer),
			download_id = COALESCE(excluded.download_id, download_id),
			updated_at = datetime('now'),
			scheduled_check_at = datetime('now')
	`, strings.Join(valueStrings, ","))

	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to batch upsert file health checks: %w", err)
	}

	return nil
}

// ListHealthItems returns all health records with optional filtering, sorting and pagination
func (r *HealthRepository) ListHealthItems(ctx context.Context, statusFilter *HealthStatus, limit, offset int, sinceFilter *time.Time, search string, sortBy string, sortOrder string) ([]*FileHealth, error) {
	// Validate and prepare ORDER BY clause
	orderClause := "created_at DESC"
	if sortBy != "" {
		// Whitelist of allowed sort fields to prevent SQL injection
		allowedFields := map[string]string{
			"file_path":          "file_path",
			"created_at":         "created_at",
			"status":             "status",
			"priority":           "priority",
			"last_checked":       "last_checked",
			"scheduled_check_at": "scheduled_check_at",
		}

		if field, ok := allowedFields[sortBy]; ok {
			orderDirection := "ASC"
			if sortOrder == "desc" || sortOrder == "DESC" {
				orderDirection = "DESC"
			}
			orderClause = fmt.Sprintf("%s %s", field, orderDirection)
		}
	}

	query := fmt.Sprintf(`
		SELECT id, file_path, status, last_checked, last_error, retry_count, max_retries,
		       repair_retry_count, max_repair_retries, source_nzb_path,
		       error_details, created_at, updated_at, scheduled_check_at,
			   library_path, streaming_failure_count, is_masked
		, metadata, indexer
		FROM file_health
		WHERE (? IS NULL OR status = ?)
		  AND (? IS NULL OR created_at >= ?)
		  AND (? = '' OR file_path LIKE ? OR (source_nzb_path IS NOT NULL AND source_nzb_path LIKE ?))
		ORDER BY %s
		LIMIT ? OFFSET ?
	`, orderClause)

	// Prepare arguments for the query
	var statusParam any = nil
	if statusFilter != nil {
		statusParam = string(*statusFilter)
	}

	var sinceParam any = nil
	if sinceFilter != nil {
		sinceParam = sinceFilter.Format("2006-01-02 15:04:05")
	}

	// Prepare search parameter with wildcards
	searchPattern := "%" + search + "%"

	args := []any{
		statusParam, statusParam, // status filter (checked twice in WHERE clause)
		sinceParam, sinceParam, // since filter (checked twice in WHERE clause)
		search, searchPattern, searchPattern, // search filter (file_path and source_nzb_path)
		limit, offset,
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query health items: %w", err)
	}
	defer rows.Close()

	var files []*FileHealth
	for rows.Next() {
		var health FileHealth
		err := rows.Scan(
			&health.ID, &health.FilePath, &health.Status, &health.LastChecked,
			&health.LastError, &health.RetryCount, &health.MaxRetries,
			&health.RepairRetryCount, &health.MaxRepairRetries,
			&health.SourceNzbPath, &health.ErrorDetails,
			&health.CreatedAt, &health.UpdatedAt, &health.ScheduledCheckAt,
			&health.LibraryPath, &health.StreamingFailureCount, &health.IsMasked,
			&health.Metadata, &health.Indexer,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan health item: %w", err)
		}
		files = append(files, &health)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate health items: %w", err)
	}

	return files, nil
}

// CountHealthItems returns the total count of health records with optional filtering
func (r *HealthRepository) CountHealthItems(ctx context.Context, statusFilter *HealthStatus, sinceFilter *time.Time, search string) (int, error) {
	query := `
		SELECT COUNT(*) 
		FROM file_health
		WHERE (? IS NULL OR status = ?)
		  AND (? IS NULL OR created_at >= ?)
		  AND (? = '' OR file_path LIKE ? OR (source_nzb_path IS NOT NULL AND source_nzb_path LIKE ?))
	`

	// Prepare arguments for the query
	var statusParam any = nil
	if statusFilter != nil {
		statusParam = string(*statusFilter)
	}

	var sinceParam any = nil
	if sinceFilter != nil {
		sinceParam = sinceFilter.Format("2006-01-02 15:04:05")
	}

	// Prepare search parameter with wildcards
	searchPattern := "%" + search + "%"

	args := []any{
		statusParam, statusParam, // status filter (checked twice in WHERE clause)
		sinceParam, sinceParam, // since filter (checked twice in WHERE clause)
		search, searchPattern, searchPattern, // search filter (file_path and source_nzb_path)
	}

	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count health items: %w", err)
	}

	return count, nil
}

// SetFileChecking sets a file's status to 'checking'
func (r *HealthRepository) SetFileChecking(ctx context.Context, filePath string) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		UPDATE file_health 
		SET status = ?,
		    updated_at = datetime('now')
		WHERE file_path = ?
	`

	_, err := r.db.ExecContext(ctx, query, HealthStatusChecking, filePath)
	if err != nil {
		return fmt.Errorf("failed to set file status to checking: %w", err)
	}

	return nil
}

// SetFilesCheckingBulk marks many files 'checking' in as few writes as possible. The
// health cycle calls this once for the whole batch instead of issuing one UPDATE per
// file, which under SQLite's single writer would serialize N transactions against each
// other and the final bulk status write. Crash recovery is unchanged: ResetFileAllChecking
// at worker startup re-arms any record stranded in 'checking'.
func (r *HealthRepository) SetFilesCheckingBulk(ctx context.Context, filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	// SQLite parameter limit is typically 999; leave room for the status arg.
	const batchSize = 500

	for i := 0; i < len(filePaths); i += batchSize {
		end := min(i+batchSize, len(filePaths))
		chunk := filePaths[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)+1)
		args = append(args, HealthStatusChecking)
		for j, p := range chunk {
			placeholders[j] = "?"
			args = append(args, normalizeHealthPath(p))
		}

		query := fmt.Sprintf(`
			UPDATE file_health
			SET status = ?, updated_at = datetime('now')
			WHERE file_path IN (%s)
		`, strings.Join(placeholders, ","))

		if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("failed to bulk set files checking (batch at %d): %w", i, err)
		}
	}

	return nil
}

func (r *HealthRepository) ResetFileAllChecking(ctx context.Context) error {
	query := `
		UPDATE file_health
		SET status = ?,
		    updated_at = datetime('now'),
			scheduled_check_at = datetime('now')
		WHERE status = ?
	`

	_, err := r.db.ExecContext(ctx, query, HealthStatusPending, HealthStatusChecking)
	if err != nil {
		return fmt.Errorf("failed to reset all file statuses: %w", err)
	}

	return nil
}

// ResetStalePendingFiles resets pending files that have exhausted retries back to retry_count=0
// so they can be re-checked in the next health cycle. Called during worker startup.
func (r *HealthRepository) ResetStalePendingFiles(ctx context.Context) error {
	query := `UPDATE file_health
	          SET retry_count = 0,
	              updated_at = datetime('now'),
	              scheduled_check_at = datetime('now')
	          WHERE status = ? AND retry_count >= max_retries`
	result, err := r.db.ExecContext(ctx, query, HealthStatusPending)
	if err != nil {
		return fmt.Errorf("failed to reset stale pending files: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows > 0 {
		slog.InfoContext(ctx, "Reset stale pending files", "count", rows)
	}

	// Fix files stuck in pending status with scheduled_check_at set to the distant future
	// (usually by the backfill release dates worker when status check was not restricted).
	// We only reset them if they have retry_count = 0 (never checked/no legitimate retry delay)
	// and scheduled_check_at is more than 24 hours in the future relative to now.
	var stuckQuery string
	if r.dialect.IsPostgres() {
		stuckQuery = `UPDATE file_health
		               SET scheduled_check_at = NOW(),
		                   updated_at = NOW()
		               WHERE status = ? 
		                 AND retry_count = 0 
		                 AND scheduled_check_at > NOW() + INTERVAL '24 hours'`
	} else {
		stuckQuery = `UPDATE file_health
		               SET scheduled_check_at = datetime('now'),
		                   updated_at = datetime('now')
		               WHERE status = ? 
		                 AND retry_count = 0 
		                 AND scheduled_check_at > datetime('now', '+24 hours')`
	}
	stuckResult, err := r.db.ExecContext(ctx, stuckQuery, HealthStatusPending)
	if err != nil {
		return fmt.Errorf("failed to reset stuck pending files: %w", err)
	}
	stuckRows, _ := stuckResult.RowsAffected()
	if stuckRows > 0 {
		slog.InfoContext(ctx, "Reset stuck pending files (scheduled in distant future)", "count", stuckRows)
	}

	return nil
}

// DeleteHealthRecordsBulk removes multiple health records from the database
func (r *HealthRepository) DeleteHealthRecordsBulk(ctx context.Context, filePaths []string) (int64, error) {
	if len(filePaths) == 0 {
		return 0, nil
	}

	// SQLite parameter limit typically is 999. Batch delete in chunks of 250 (500 placeholders).
	const batchSize = 250
	var totalRowsAffected int64

	for i := 0; i < len(filePaths); i += batchSize {
		end := min(i+batchSize, len(filePaths))
		chunk := filePaths[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for j, path := range chunk {
			placeholders[j] = "?"
			args[j] = normalizeHealthPath(path)
		}

		query := fmt.Sprintf(`DELETE FROM file_health WHERE file_path IN (%s)`, strings.Join(placeholders, ","))

		result, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			return totalRowsAffected, fmt.Errorf("failed to delete health records batch starting at %d: %w", i, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return totalRowsAffected, fmt.Errorf("failed to get rows affected for batch starting at %d: %w", i, err)
		}
		totalRowsAffected += rowsAffected
	}

	// Zero deletions is not an error; callers report the actual count.
	return totalRowsAffected, nil
}

// ResetHealthChecksBulk resets multiple health records to pending status
func (r *HealthRepository) ResetHealthChecksBulk(ctx context.Context, filePaths []string) (int, error) {
	if len(filePaths) == 0 {
		return 0, nil
	}

	// SQLite parameter limit typically is 999. Batch reset in chunks of 500.
	const batchSize = 500
	var totalRowsAffected int64

	for i := 0; i < len(filePaths); i += batchSize {
		end := min(i+batchSize, len(filePaths))
		chunk := filePaths[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)+1)
		args = append(args, string(HealthStatusPending))
		for j, path := range chunk {
			placeholders[j] = "?"
			args = append(args, normalizeHealthPath(path))
		}

		query := fmt.Sprintf(`
			UPDATE file_health
			SET status = ?,
			    retry_count = 0,
			    repair_retry_count = 0,
			    last_error = NULL,
			    error_details = NULL,
			    updated_at = datetime('now'),
				scheduled_check_at = datetime('now')
			WHERE file_path IN (%s)
		`, strings.Join(placeholders, ","))

		result, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			return 0, fmt.Errorf("failed to reset health records batch starting at %d: %w", i, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("failed to get rows affected for batch starting at %d: %w", i, err)
		}
		totalRowsAffected += rowsAffected
	}

	return int(totalRowsAffected), nil
}

// ResetAllHealthChecks resets all health records to pending status
func (r *HealthRepository) ResetAllHealthChecks(ctx context.Context) (int, error) {
	query := `
		UPDATE file_health
		SET status = 'pending',
		    retry_count = 0,
		    repair_retry_count = 0,
		    last_error = NULL,
		    error_details = NULL,
		    updated_at = datetime('now'),
			scheduled_check_at = datetime('now')
	`

	result, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to reset all health records: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// DeleteHealthRecordsByDate deletes health records older than the specified date with optional status filter
func (r *HealthRepository) DeleteHealthRecordsByDate(ctx context.Context, olderThan time.Time, statusFilter *HealthStatus) (int, error) {
	query := `
		DELETE FROM file_health
		WHERE created_at < ?
		  AND (? IS NULL OR status = ?)
	`

	// Prepare arguments for the query
	var statusParam any = nil
	if statusFilter != nil {
		statusParam = string(*statusFilter)
	}

	args := []any{
		olderThan.Format("2006-01-02 15:04:05"),
		statusParam, statusParam, // status filter (checked twice in WHERE clause)
	}

	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to delete health records by date: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// AddHealthCheck adds or updates a health check record
func (r *HealthRepository) AddHealthCheck(
	ctx context.Context,
	filePath string,
	releaseDate time.Time,
	scheduledCheckAt time.Time,
	sourceNzbPath *string,
) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		INSERT INTO file_health (
			file_path, status, last_checked, retry_count, max_retries,
			repair_retry_count, max_repair_retries, source_nzb_path,
			release_date, scheduled_check_at,
			created_at, updated_at
		)
		VALUES (?, ?, datetime('now'), 0, 2, 0, 3, ?, ?, ?, datetime('now'), datetime('now'))
		ON CONFLICT(file_path) DO UPDATE SET
			release_date = excluded.release_date,
			scheduled_check_at = excluded.scheduled_check_at,
			status = excluded.status,
			updated_at = datetime('now')
	`

	_, err := r.db.ExecContext(ctx, query, filePath, HealthStatusHealthy, sourceNzbPath, releaseDate.UTC(), scheduledCheckAt.UTC())
	if err != nil {
		return fmt.Errorf("failed to add health check: %w", err)
	}

	return nil
}

// UpdateScheduledCheckTime updates the scheduled check time for a file
func (r *HealthRepository) UpdateScheduledCheckTime(ctx context.Context, filePath string, nextCheckTime time.Time) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		UPDATE file_health
		SET status = ?,
		    scheduled_check_at = ?,
		    updated_at = datetime('now')
		WHERE file_path = ?
	`

	result, err := r.db.ExecContext(ctx, query, HealthStatusHealthy, nextCheckTime.UTC().Format("2006-01-02 15:04:05"), filePath)
	if err != nil {
		return fmt.Errorf("failed to update scheduled check time: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no automatic health check found for file: %s", filePath)
	}

	return nil
}

// MarkAsHealthy marks a file as healthy and clears all retry/error state
func (r *HealthRepository) MarkAsHealthy(ctx context.Context, filePath string, nextCheckTime time.Time) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		UPDATE file_health
		SET status = ?,
		    scheduled_check_at = ?,
		    retry_count = 0,
		    repair_retry_count = 0,
		    last_error = NULL,
		    error_details = NULL,
		    updated_at = datetime('now')
		WHERE file_path = ?
	`

	result, err := r.db.ExecContext(ctx, query, HealthStatusHealthy, nextCheckTime.UTC().Format("2006-01-02 15:04:05"), filePath)
	if err != nil {
		return fmt.Errorf("failed to mark file as healthy: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no health check found for file: %s", filePath)
	}

	return nil
}

// UpdateHealthStatusBulk updates multiple health records in a single transaction
func (r *HealthRepository) UpdateHealthStatusBulk(ctx context.Context, updates []HealthStatusUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare common statements. Each carries an optional status guard:
	// `AND (status = ? OR ? = '')` — when the caller binds a non-empty expected status the
	// write only lands if the record is still in that status (closing the TOCTOU window);
	// when it binds the empty string the guard is a no-op and the write matches by path alone.
	stmtHealthy, err := tx.PrepareContext(ctx, `
		UPDATE file_health
		SET status = 'healthy', scheduled_check_at = ?, retry_count = 0,
		    repair_retry_count = 0, last_error = NULL, error_details = ?,
		    updated_at = datetime('now'), last_checked = datetime('now')
		WHERE file_path = ? AND (status = ? OR ? = '')
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare healthy statement: %w", err)
	}
	defer stmtHealthy.Close()

	stmtRetry, err := tx.PrepareContext(ctx, `
		UPDATE file_health
		SET retry_count = retry_count + 1, last_error = ?, error_details = ?,
		    status = 'pending', scheduled_check_at = ?,
		    updated_at = datetime('now'), last_checked = datetime('now')
		WHERE file_path = ? AND (status = ? OR ? = '')
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare retry statement: %w", err)
	}
	defer stmtRetry.Close()

	stmtRepair, err := tx.PrepareContext(ctx, `
		UPDATE file_health
		SET repair_retry_count = repair_retry_count + 1, last_error = ?,
		    error_details = ?, status = 'repair_triggered',
		    updated_at = datetime('now'), last_checked = datetime('now'),
			scheduled_check_at = ?
		WHERE file_path = ? AND (status = ? OR ? = '')
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare repair statement: %w", err)
	}
	defer stmtRepair.Close()

	// stmtRepairTrigger is for the first-time repair trigger — does NOT increment repair_retry_count.
	stmtRepairTrigger, err := tx.PrepareContext(ctx, `
		UPDATE file_health
		SET last_error = ?, error_details = ?, status = 'repair_triggered',
		    updated_at = datetime('now'), last_checked = datetime('now'),
		    scheduled_check_at = ?
		WHERE file_path = ? AND (status = ? OR ? = '')
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare repair trigger statement: %w", err)
	}
	defer stmtRepairTrigger.Close()

	stmtCorrupted, err := tx.PrepareContext(ctx, `
		UPDATE file_health
		SET status = 'corrupted', last_error = ?, error_details = ?,
		    scheduled_check_at = NULL,
		    updated_at = datetime('now'), last_checked = datetime('now')
		WHERE file_path = ? AND (status = ? OR ? = '')
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare corrupted statement: %w", err)
	}
	defer stmtCorrupted.Close()

	// stmtDegraded records a playable-with-glitches verdict: no repair fields
	// change, the file stays scheduled for periodic re-checks.
	stmtDegraded, err := tx.PrepareContext(ctx, `
		UPDATE file_health
		SET status = 'degraded', last_error = ?, error_details = ?,
		    scheduled_check_at = ?,
		    updated_at = datetime('now'), last_checked = datetime('now')
		WHERE file_path = ? AND (status = ? OR ? = '')
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare degraded statement: %w", err)
	}
	defer stmtDegraded.Close()

	// stmtInconclusive re-arms a check that produced no evidence: the record
	// keeps its previous status and both retry counters, and is simply due
	// again later. Counting an outage as a failed check burned the retry
	// budget and eventually escalated healthy files into repair (#861).
	stmtInconclusive, err := tx.PrepareContext(ctx, `
		UPDATE file_health
		SET status = ?, last_error = ?, error_details = ?, scheduled_check_at = ?,
		    updated_at = datetime('now'), last_checked = datetime('now')
		WHERE file_path = ? AND (status = ? OR ? = '')
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare inconclusive statement: %w", err)
	}
	defer stmtInconclusive.Close()

	for _, update := range updates {
		if update.Skip {
			continue
		}
		filePath := normalizeHealthPath(update.FilePath)
		// expected is the empty string for an unguarded write, or the status the record
		// must still hold for a guarded write to land (see HealthStatusUpdate.ExpectedStatus).
		expected := ""
		if update.ExpectedStatus != nil {
			expected = string(*update.ExpectedStatus)
		}
		switch update.Type {
		case UpdateTypeHealthy:
			// error_details is normally nil here (clearing the column as a
			// healthy result always did); a content-verification outcome is
			// the one payload a healthy record keeps.
			_, err = stmtHealthy.ExecContext(ctx, update.ScheduledCheckAt, update.ErrorDetails, filePath, expected, expected)
		case UpdateTypeRetry:
			_, err = stmtRetry.ExecContext(ctx, update.ErrorMessage, update.ErrorDetails, update.ScheduledCheckAt, filePath, expected, expected)
		case UpdateTypeRepairTrigger:
			_, err = stmtRepairTrigger.ExecContext(ctx, update.ErrorMessage, update.ErrorDetails, update.ScheduledCheckAt, filePath, expected, expected)
		case UpdateTypeRepairRetry:
			_, err = stmtRepair.ExecContext(ctx, update.ErrorMessage, update.ErrorDetails, update.ScheduledCheckAt, filePath, expected, expected)
		case UpdateTypeCorrupted:
			_, err = stmtCorrupted.ExecContext(ctx, update.ErrorMessage, update.ErrorDetails, filePath, expected, expected)
		case UpdateTypeDegraded:
			_, err = stmtDegraded.ExecContext(ctx, update.ErrorMessage, update.ErrorDetails, update.ScheduledCheckAt, filePath, expected, expected)
		case UpdateTypeInconclusive:
			_, err = stmtInconclusive.ExecContext(ctx, string(update.Status), update.ErrorMessage, update.ErrorDetails, update.ScheduledCheckAt, filePath, expected, expected)
		}

		if err != nil {
			return fmt.Errorf("failed to execute update for %s: %w", update.FilePath, err)
		}
	}

	return tx.Commit()
}

// UpdateType represents the type of health update
type UpdateType int

const (
	UpdateTypeHealthy       UpdateType = 1
	UpdateTypeRetry         UpdateType = 2
	UpdateTypeRepairRetry   UpdateType = 3 // re-check of an already-triggered repair; increments repair_retry_count
	UpdateTypeCorrupted     UpdateType = 4
	UpdateTypeRepairTrigger UpdateType = 5 // first-time trigger; does not increment repair_retry_count
	UpdateTypeDegraded      UpdateType = 6 // playable with glitches; no repair, periodic re-check
	// UpdateTypeInconclusive records a check that reached no verdict (provider
	// outage, timeouts, truncated sweep). It only moves scheduled_check_at and
	// the error text: neither retry counter advances, and Status carries the
	// status the record must be restored to (it was set to 'checking' for the
	// duration of the cycle).
	UpdateTypeInconclusive UpdateType = 7
)

// HealthStatusUpdate represents a single update request for batch processing
type HealthStatusUpdate struct {
	Type             UpdateType
	FilePath         string
	Status           HealthStatus
	ErrorMessage     *string
	ErrorDetails     *string
	ScheduledCheckAt time.Time
	Skip             bool // if true, skip this record in the bulk update (e.g. record already deleted)
	// ExpectedStatus, when non-nil, makes the write conditional on the record still being
	// in that status (the status the worker based its decision on). It closes the TOCTOU
	// window where a concurrent webhook relink, re-import upsert or manual recheck lands
	// between the cycle's read and its write: if the status changed underneath us the
	// guarded UPDATE matches no rows and the concurrent actor's decision wins instead of
	// being silently clobbered (last-writer-wins re-entering the repair loop).
	ExpectedStatus *HealthStatus
}

// BackfillRecord represents a record used for metadata backfilling
type BackfillRecord struct {
	ID       int64
	FilePath string
	Metadata *string
}

// BackfillUpdate represents an update for release date backfilling
type BackfillUpdate struct {
	ID               int64
	ReleaseDate      time.Time
	ScheduledCheckAt time.Time
}

// GetAllHealthCheckPaths returns all health check file paths (memory optimized)
func (r *HealthRepository) GetAllHealthCheckPaths(ctx context.Context) ([]string, error) {
	query := `
		SELECT file_path
		FROM file_health
		ORDER BY file_path ASC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query health check paths: %w", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("failed to scan file path: %w", err)
		}
		paths = append(paths, path)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate health check paths: %w", err)
	}

	return paths, nil
}

// GetAllHealthCheckRecords returns all health check records tracked in health system
func (r *HealthRepository) GetAllHealthCheckRecords(ctx context.Context) ([]AutomaticHealthCheckRecord, error) {
	query := `
		SELECT file_path, library_path, 
			   release_date, scheduled_check_at,
			   source_nzb_path, status
		FROM file_health
		ORDER BY file_path ASC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query health check paths: %w", err)
	}
	defer rows.Close()

	var records []AutomaticHealthCheckRecord
	for rows.Next() {
		var (
			path               string
			libraryPath        *string
			releaseDate        *time.Time
			scheduledCheckAtNT sql.NullTime
			sourceNzbPath      *string
			status             HealthStatus
		)

		if err := rows.Scan(&path, &libraryPath, &releaseDate, &scheduledCheckAtNT, &sourceNzbPath, &status); err != nil {
			return nil, fmt.Errorf("failed to scan file path: %w", err)
		}
		var scheduledCheckAt time.Time
		if scheduledCheckAtNT.Valid {
			scheduledCheckAt = scheduledCheckAtNT.Time
		}
		records = append(records, AutomaticHealthCheckRecord{
			FilePath:         path,
			LibraryPath:      libraryPath,
			ReleaseDate:      releaseDate,
			ScheduledCheckAt: &scheduledCheckAt,
			SourceNzbPath:    sourceNzbPath,
			Status:           status,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate health check paths: %w", err)
	}

	return records, nil
}

// GetFilesMissingReleaseDate returns a list of files that don't have a release date cached
func (r *HealthRepository) GetFilesMissingReleaseDate(ctx context.Context, limit int) ([]BackfillRecord, error) {
	query := `
		SELECT id, file_path
		, metadata
		FROM file_health
		WHERE release_date IS NULL
		LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []BackfillRecord
	for rows.Next() {
		var rec BackfillRecord
		if err := rows.Scan(&rec.ID, &rec.FilePath, &rec.Metadata); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, nil
}

// BackfillReleaseDates updates multiple health records with their release dates and next check times
func (r *HealthRepository) BackfillReleaseDates(ctx context.Context, updates []BackfillUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Backfill release_date for every selected record, but never re-arm a terminal or
	// in-flight record's schedule: a 'corrupted' record's only terminal guard is a NULL
	// scheduled_check_at, and a 'repair_triggered' record's schedule is the repair
	// back-off. Only pending/healthy/checking records get their next check rescheduled
	// from the freshly-derived release date.
	stmt, err := tx.PrepareContext(ctx, `
		UPDATE file_health
		SET release_date = ?,
		    scheduled_check_at = CASE WHEN status = 'healthy' THEN ? ELSE scheduled_check_at END,
		    updated_at = datetime('now')
		WHERE id = ?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, up := range updates {
		_, err = stmt.ExecContext(ctx, up.ReleaseDate.UTC().Format("2006-01-02 15:04:05"), up.ScheduledCheckAt.UTC().Format("2006-01-02 15:04:05"), up.ID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// AutomaticHealthCheckRecord represents a batch insert record
type AutomaticHealthCheckRecord struct {
	FilePath         string
	LibraryPath      *string
	ReleaseDate      *time.Time
	ScheduledCheckAt *time.Time
	SourceNzbPath    *string
	Status           HealthStatus
	MaxRetries       int
	MaxRepairRetries int
}

// BatchAddAutomaticHealthChecks inserts multiple automatic health checks efficiently
func (r *HealthRepository) BatchAddAutomaticHealthChecks(ctx context.Context, records []AutomaticHealthCheckRecord) error {
	if len(records) == 0 {
		return nil
	}

	// SQLite has a limit on the number of parameters (typically 999)
	// Process in batches of 150 records (6 params each = 900 params per batch)
	const batchSize = 150

	for i := 0; i < len(records); i += batchSize {
		end := min(i+batchSize, len(records))

		batch := records[i:end]
		if err := r.batchInsertAutomaticHealthChecks(ctx, batch); err != nil {
			return fmt.Errorf("failed to insert batch starting at index %d: %w", i, err)
		}
	}

	return nil
}

// batchInsertAutomaticHealthChecks performs a single batch insert
func (r *HealthRepository) batchInsertAutomaticHealthChecks(ctx context.Context, records []AutomaticHealthCheckRecord) error {
	if len(records) == 0 {
		return nil
	}

	// Build the INSERT query with multiple value sets
	valueStrings := make([]string, len(records))
	args := make([]any, 0, len(records)*8)

	for i, record := range records {
		valueStrings[i] = "(?, ?, ?, datetime('now'), 0, ?, 0, ?, ?, ?, ?, datetime('now'), datetime('now'))"
		var releaseDateStr, scheduledCheckAtStr any = nil, nil
		if record.ReleaseDate != nil {
			releaseDateStr = record.ReleaseDate.UTC().Format("2006-01-02 15:04:05")
		}
		if record.ScheduledCheckAt != nil {
			scheduledCheckAtStr = record.ScheduledCheckAt.UTC().Format("2006-01-02 15:04:05")
		}

		args = append(args,
			normalizeHealthPath(record.FilePath), record.LibraryPath, HealthStatusHealthy,
			record.MaxRetries, record.MaxRepairRetries,
			record.SourceNzbPath, releaseDateStr, scheduledCheckAtStr)
	}

	query := fmt.Sprintf(`
		INSERT INTO file_health (
			file_path, library_path, status, last_checked, retry_count, max_retries,
			repair_retry_count, max_repair_retries, source_nzb_path,
			release_date, scheduled_check_at,
			created_at, updated_at
		)
		VALUES %s
		ON CONFLICT(file_path) DO UPDATE SET
			library_path = COALESCE(excluded.library_path, library_path),
			status = CASE 
				WHEN source_nzb_path != excluded.source_nzb_path OR release_date != excluded.release_date THEN excluded.status 
				ELSE status 
			END,
			scheduled_check_at = CASE 
				WHEN source_nzb_path != excluded.source_nzb_path OR release_date != excluded.release_date THEN excluded.scheduled_check_at 
				ELSE scheduled_check_at 
			END,
			retry_count = CASE 
				WHEN source_nzb_path != excluded.source_nzb_path OR release_date != excluded.release_date THEN 0 
				ELSE retry_count 
			END,
			source_nzb_path = excluded.source_nzb_path,
			release_date = excluded.release_date,
			max_retries = excluded.max_retries,
			max_repair_retries = excluded.max_repair_retries,
			updated_at = datetime('now')
	`, strings.Join(valueStrings, ","))

	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to batch insert health checks: %w", err)
	}

	return nil
}

// ResolvePendingRepairsInDirectory removes health records with repair_triggered or corrupted status
// that exist in the specified directory. This is used when a new file is imported
// into a directory, implying it is a replacement for the broken file.
func (r *HealthRepository) ResolvePendingRepairsInDirectory(ctx context.Context, dirPath string) (int64, error) {
	dirPath = normalizeHealthPath(dirPath)
	if dirPath == "" {
		return 0, nil
	}
	// Ensure directory path ends with separator to match files inside it
	if !strings.HasSuffix(dirPath, "/") {
		dirPath = dirPath + "/"
	}

	query := `
		DELETE FROM file_health
		WHERE file_path LIKE ? ESCAPE '\'
		AND status IN ('repair_triggered', 'corrupted', 'degraded')
	`

	// Match paths starting with the directory
	likePattern := escapeLikePrefix(dirPath) + "%"

	result, err := r.db.ExecContext(ctx, query, likePattern)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve pending repairs in %s: %w", dirPath, err)
	}

	return result.RowsAffected()
}

// UpdateLibraryPath updates the library_path for a specific file
func (r *HealthRepository) UpdateLibraryPath(ctx context.Context, filePath string, libraryPath string) error {
	filePath = normalizeHealthPath(filePath)
	query := `
		UPDATE file_health
		SET library_path = ?,
		    updated_at = datetime('now')
		WHERE file_path = ?
	`

	result, err := r.db.ExecContext(ctx, query, libraryPath, filePath)
	if err != nil {
		return fmt.Errorf("failed to update library path: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no health record found to update: %s", filePath)
	}

	return nil
}

// RenameHealthRecord updates the file_path of a health record or records under a directory after a MOVE operation
func (r *HealthRepository) RenameHealthRecord(ctx context.Context, oldPath, newPath string) error {
	oldPath = normalizeHealthPath(oldPath)
	newPath = normalizeHealthPath(newPath)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Rename exact match
	_, err = tx.ExecContext(ctx, "UPDATE file_health SET file_path = ?, updated_at = datetime('now') WHERE file_path = ?", newPath, oldPath)
	if err != nil {
		return err
	}

	// 2. Rename children if it's a directory
	oldPrefix := oldPath + "/"
	newPrefix := newPath + "/"
	_, err = tx.ExecContext(ctx, `
		UPDATE file_health 
		SET file_path = ? || substr(file_path, ?),
		    updated_at = datetime('now')
		WHERE file_path LIKE ?`,
		newPrefix, len(oldPrefix)+1, oldPrefix+"%")
	if err != nil {
		return err
	}

	return tx.Commit()
}

// RelinkFileByFilename updates the file_path and library_path for a record that matches by filename.
// This is typically called by webhooks during renames or downloads to provide a definitive library path.
//
// revalidate controls what happens to records in repair_triggered/corrupted state:
//   - true (Download events — a re-downloaded copy was just imported): reset the record to
//     pending with an immediate check so the fresh copy is validated instead of being
//     destroyed by the next repair re-trigger. retry_count restarts for the new copy, but
//     repair_retry_count is preserved as the per-title repair budget so repeatedly broken
//     re-downloads still escalate to corrupted instead of looping forever.
//   - false (Rename events — no new content): preserve repair/corrupted state so a library
//     reorganization cannot wipe repair progress.
func (r *HealthRepository) RelinkFileByFilename(ctx context.Context, filename, filePath, libraryPath string, metadataStr *string, revalidate bool) (bool, error) {
	filePath = normalizeHealthPath(filePath)
	likePattern := "%/" + escapeLikePrefix(filename)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// Query to find all matched records
	rows, err := tx.QueryContext(ctx, `
		SELECT id, file_path, library_path FROM file_health
		WHERE (file_path LIKE ? ESCAPE '\' OR file_path = ? OR library_path LIKE ? ESCAPE '\' OR library_path = ?)
	`, likePattern, filename, likePattern, libraryPath)
	if err != nil {
		return false, fmt.Errorf("failed to query records for filename relink: %w", err)
	}

	type candidate struct {
		id          int64
		filePath    string
		libraryPath string
	}
	var allCandidates []candidate
	for rows.Next() {
		var c candidate
		var lp sql.NullString
		if err := rows.Scan(&c.id, &c.filePath, &lp); err != nil {
			rows.Close()
			return false, err
		}
		if lp.Valid {
			c.libraryPath = lp.String
		}
		allCandidates = append(allCandidates, c)
	}
	rows.Close()

	if len(allCandidates) == 0 {
		return false, nil
	}

	var matchedIDs []int64
	for _, cand := range allCandidates {
		// Keep if it is the target path/library path precisely
		if cand.filePath == filePath || cand.libraryPath == libraryPath {
			matchedIDs = append(matchedIDs, cand.id)
			continue
		}
		// Keep if it is a downloader path or shares show folder with the target path
		if isDownloaderPath(cand.filePath) || shareShowFolder(cand.filePath, filePath) {
			matchedIDs = append(matchedIDs, cand.id)
			continue
		}
	}

	if len(matchedIDs) == 0 {
		return false, nil
	}

	var targetID int64
	var sourceID int64
	for _, id := range matchedIDs {
		var isExact bool
		for _, cand := range allCandidates {
			if cand.id == id && (cand.filePath == filePath || cand.libraryPath == libraryPath) {
				isExact = true
				break
			}
		}
		if isExact {
			targetID = id
		} else {
			sourceID = id
		}
	}

	// If there are multiple other source IDs (e.g. len(matchedIDs) > 2), it is ambiguous collision
	if len(matchedIDs) > 2 {
		slog.WarnContext(ctx, "Refusing ambiguous relink — filename matches multiple source records",
			"filename", filename, "matches", len(matchedIDs))
		return false, nil
	}

	var actID int64
	if targetID != 0 && sourceID != 0 {
		actID = sourceID
	} else if targetID != 0 {
		actID = targetID
	} else if sourceID != 0 {
		actID = sourceID
	} else {
		return false, nil
	}

	if err := r.relinkOrMergeRecordTx(ctx, tx, actID, filePath, libraryPath, metadataStr, revalidate); err != nil {
		return false, fmt.Errorf("failed to merge/relink record %d: %w", actID, err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}

func isDownloaderPath(p string) bool {
	low := strings.ToLower(p)
	return strings.Contains(low, "complete") ||
		strings.Contains(low, "download") ||
		strings.Contains(low, "nzb") ||
		strings.Contains(low, "temp") ||
		strings.Contains(low, "tmp") ||
		strings.Contains(low, "incoming")
}

func shareShowFolder(p1, p2 string) bool {
	s1 := strings.Split(filepath.ToSlash(p1), "/")
	s2 := strings.Split(filepath.ToSlash(p2), "/")
	if len(s1) < 2 || len(s2) < 2 {
		return false
	}
	for i := len(s1) - 2; i >= 0 && i >= len(s1)-3; i-- {
		seg := s1[i]
		low := strings.ToLower(seg)
		if strings.HasPrefix(low, "season") ||
			low == "specials" || low == "tv" || low == "movies" || low == "downloads" {
			continue
		}
		for j := len(s2) - 2; j >= 0 && j >= len(s2)-3; j-- {
			if s2[j] == seg {
				return true
			}
		}
	}
	return false
}

// GetSystemState retrieves a persistent state value
func (r *HealthRepository) GetSystemState(ctx context.Context, key string) (string, error) {
	query := `SELECT value FROM system_state WHERE key = ?`
	var value string
	err := r.db.QueryRowContext(ctx, query, key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("failed to get system state: %w", err)
	}
	return value, nil
}

// UpdateSystemState updates or inserts a persistent state value
func (r *HealthRepository) UpdateSystemState(ctx context.Context, key string, value string) error {
	query := `
		INSERT INTO system_state (key, value, updated_at)
		VALUES (?, ?, datetime('now'))
		ON CONFLICT(key) DO UPDATE SET
		value = excluded.value,
		updated_at = datetime('now')
	`
	_, err := r.db.ExecContext(ctx, query, key, value)
	if err != nil {
		return fmt.Errorf("failed to update system state: %w", err)
	}
	return nil
}

// GetFilesByPaths returns health records for the specified file paths
func (r *HealthRepository) GetFilesByPaths(ctx context.Context, filePaths []string) ([]*FileHealth, error) {
	if len(filePaths) == 0 {
		return nil, nil
	}

	// Build placeholders for the IN clause
	placeholders := make([]string, len(filePaths))
	args := make([]any, len(filePaths))
	for i, path := range filePaths {
		placeholders[i] = "?"
		args[i] = normalizeHealthPath(path)
	}

	query := fmt.Sprintf(`
		SELECT id, file_path, library_path, status, last_checked, last_error, retry_count, max_retries,
		       repair_retry_count, max_repair_retries, source_nzb_path,
		       error_details, created_at, updated_at, release_date, priority
		, metadata
		FROM file_health
		WHERE file_path IN (%s)
		ORDER BY file_path ASC
	`, strings.Join(placeholders, ","))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query files by paths: %w", err)
	}
	defer rows.Close()

	var files []*FileHealth
	for rows.Next() {
		var health FileHealth
		err := rows.Scan(
			&health.ID, &health.FilePath, &health.LibraryPath, &health.Status, &health.LastChecked,
			&health.LastError, &health.RetryCount, &health.MaxRetries,
			&health.RepairRetryCount, &health.MaxRepairRetries, &health.SourceNzbPath,
			&health.ErrorDetails, &health.CreatedAt, &health.UpdatedAt, &health.ReleaseDate, &health.Priority,
			&health.Metadata,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file health: %w", err)
		}
		files = append(files, &health)
	}

	return files, nil
}

// GetFilesForLibrarySync returns all health records to verify their physical presence in the library
func (r *HealthRepository) GetFilesForLibrarySync(ctx context.Context) ([]*FileHealth, error) {
	query := `
		SELECT id, file_path, library_path, status, last_checked, last_error, retry_count, max_retries,
		       repair_retry_count, max_repair_retries, source_nzb_path,
		       error_details, created_at, updated_at, release_date, priority
		, metadata
		FROM file_health
		ORDER BY file_path ASC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query files for library sync: %w", err)
	}
	defer rows.Close()

	var files []*FileHealth
	for rows.Next() {
		var health FileHealth
		err := rows.Scan(
			&health.ID, &health.FilePath, &health.LibraryPath, &health.Status, &health.LastChecked,
			&health.LastError, &health.RetryCount, &health.MaxRetries,
			&health.RepairRetryCount, &health.MaxRepairRetries,
			&health.SourceNzbPath, &health.ErrorDetails,
			&health.CreatedAt, &health.UpdatedAt, &health.ReleaseDate, &health.Priority,
			&health.Metadata,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file health: %w", err)
		}
		files = append(files, &health)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate files for library sync: %w", err)
	}

	return files, nil
}

// HasImportHistoryForPath checks if any import history record exists for the
// given virtual path. Used to protect symlinks from deletion when an import
// has been recorded by AltMount, regardless of current metadata state.
func (r *HealthRepository) HasImportHistoryForPath(ctx context.Context, virtualPath string) (bool, error) {
	virtualPath = normalizeHealthPath(virtualPath)
	query := `SELECT 1 FROM import_history WHERE virtual_path = ? LIMIT 1`
	var exists int
	err := r.db.QueryRowContext(ctx, query, virtualPath).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check import history for path: %w", err)
	}
	return true, nil
}

// UpdateFileMetadata updates the metadata column for a health record
func (r *HealthRepository) UpdateFileMetadata(ctx context.Context, id int64, metadata []byte) error {
	query := `
		UPDATE file_health
		SET metadata = ?,
		    updated_at = datetime('now')
		WHERE id = ?
	`
	_, err := r.db.ExecContext(ctx, query, metadata, id)
	return err
}

// LogIndexerImport records a success or failure for an indexer persistently.
func (r *HealthRepository) LogIndexerImport(ctx context.Context, indexer string, status string, errMsg string, downloadID string) error {
	return logIndexerImport(ctx, r.db, indexer, status, errMsg, downloadID)
}

// relinkOrMergeRecordTx merges details or updates a matched health record under a transaction,
// resolving any UNIQUE constraint violations on file_path.
func (r *HealthRepository) relinkOrMergeRecordTx(ctx context.Context, tx *dialectAwareTx, id int64, filePath, libraryPath string, metadataStr *string, revalidate bool) error {
	filePath = normalizeHealthPath(filePath)

	// 1. Fetch source/matched record info
	var sourceStatus string
	var sourceUpdatedAt time.Time
	var sourceRepairRetry int
	var sourceSourceNzb *string
	var sourceIndexer *string
	var sourceReleaseDate *time.Time
	var sourceMetadata *string
	err := tx.QueryRowContext(ctx, `
		SELECT status, updated_at, repair_retry_count, source_nzb_path, indexer, release_date, metadata
		FROM file_health
		WHERE id = ?
	`, id).Scan(&sourceStatus, &sourceUpdatedAt, &sourceRepairRetry, &sourceSourceNzb, &sourceIndexer, &sourceReleaseDate, &sourceMetadata)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil // Source record no longer exists, nothing to do
		}
		return err
	}

	// 2. Check if a record with the new file_path already exists
	var conflictingID int64
	var conflictingStatus string
	var conflictingUpdatedAt time.Time
	var conflictingRepairRetry int
	var conflictingSourceNzb *string
	var conflictingIndexer *string
	var conflictingReleaseDate *time.Time
	var conflictingMetadata *string
	conflictExists := true

	err = tx.QueryRowContext(ctx, `
		SELECT id, status, updated_at, repair_retry_count, source_nzb_path, indexer, release_date, metadata
		FROM file_health
		WHERE file_path = ?
	`, filePath).Scan(&conflictingID, &conflictingStatus, &conflictingUpdatedAt, &conflictingRepairRetry, &conflictingSourceNzb, &conflictingIndexer, &conflictingReleaseDate, &conflictingMetadata)
	if err != nil {
		if err == sql.ErrNoRows {
			conflictExists = false
		} else {
			return err
		}
	}

	// If the conflicting record is the exact same record as the source, just update it normally
	if conflictExists && conflictingID == id {
		conflictExists = false
	}

	// Fast-Fail Revalidate Guard: If the target record (the one surviving the merge) recently
	// had a repair triggered, DO NOT reset it to pending. This prevents Webhooks that fire immediately
	// after an import's streaming failure from wiping out the repair trigger.
	targetStatus := sourceStatus
	targetUpdatedAt := sourceUpdatedAt
	if conflictExists {
		targetStatus = conflictingStatus
		targetUpdatedAt = conflictingUpdatedAt
	}

	if revalidate && (targetStatus == "repair_triggered" || targetStatus == "corrupted") && time.Since(targetUpdatedAt) < 60*time.Second {
		revalidate = false
	}

	if conflictExists {
		// Merge details into the conflicting record, then delete the source record
		mergedRepairRetry := sourceRepairRetry
		if conflictingRepairRetry > mergedRepairRetry {
			mergedRepairRetry = conflictingRepairRetry
		}

		var mergedSourceNzb *string
		if conflictingSourceNzb != nil {
			mergedSourceNzb = conflictingSourceNzb
		} else {
			mergedSourceNzb = sourceSourceNzb
		}

		var mergedIndexer *string
		if conflictingIndexer != nil {
			mergedIndexer = conflictingIndexer
		} else {
			mergedIndexer = sourceIndexer
		}

		var mergedReleaseDate *time.Time
		if conflictingReleaseDate != nil {
			mergedReleaseDate = conflictingReleaseDate
		} else {
			mergedReleaseDate = sourceReleaseDate
		}

		var mergedMetadata *string
		if metadataStr != nil {
			mergedMetadata = metadataStr
		} else if conflictingMetadata != nil {
			mergedMetadata = conflictingMetadata
		} else {
			mergedMetadata = sourceMetadata
		}

		var query string
		var args []any
		if revalidate {
			query = `
				UPDATE file_health
				SET library_path = ?,
				    status = 'pending',
				    retry_count = 0,
				    last_error = NULL,
				    error_details = NULL,
				    metadata = ?,
				    repair_retry_count = ?,
				    source_nzb_path = ?,
				    indexer = ?,
				    release_date = ?,
				    updated_at = datetime('now'),
				    scheduled_check_at = datetime('now')
				WHERE id = ?
			`
			args = []any{libraryPath, mergedMetadata, mergedRepairRetry, mergedSourceNzb, mergedIndexer, mergedReleaseDate, conflictingID}
		} else {
			query = `
				UPDATE file_health
				SET library_path = ?,
				    metadata = ?,
				    repair_retry_count = ?,
				    source_nzb_path = ?,
				    indexer = ?,
				    release_date = ?,
				    updated_at = datetime('now'),
				    scheduled_check_at = CASE WHEN status IN ('repair_triggered', 'corrupted', 'degraded') THEN scheduled_check_at ELSE datetime('now') END
				WHERE id = ?
			`
			args = []any{libraryPath, mergedMetadata, mergedRepairRetry, mergedSourceNzb, mergedIndexer, mergedReleaseDate, conflictingID}
		}

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}

		// Delete the source record (since its path was obsolete and its history is merged)
		_, err = tx.ExecContext(ctx, "DELETE FROM file_health WHERE id = ?", id)
		if err != nil {
			return err
		}
	} else {
		// No conflict: just rename the source record
		var query string
		var args []any
		if revalidate {
			query = `
				UPDATE file_health
				SET file_path = ?,
				    library_path = ?,
				    status = 'pending',
				    retry_count = 0,
				    last_error = NULL,
				    error_details = NULL,
				    metadata = COALESCE(?, metadata),
				    updated_at = datetime('now'),
				    scheduled_check_at = datetime('now')
				WHERE id = ?
			`
			args = []any{filePath, libraryPath, metadataStr, id}
		} else {
			query = `
				UPDATE file_health
				SET file_path = ?,
				    library_path = ?,
				    metadata = COALESCE(?, metadata),
				    updated_at = datetime('now'),
				    scheduled_check_at = CASE WHEN status IN ('repair_triggered', 'corrupted', 'degraded') THEN scheduled_check_at ELSE datetime('now') END
				WHERE id = ?
			`
			args = []any{filePath, libraryPath, metadataStr, id}
		}

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
	}

	return nil
}

// RelinkFileByMetadata matches repair_triggered or corrupted records by metadata and updates them to pending.
//
// revalidate controls what happens to records in repair_triggered/corrupted state:
//   - true (Download events — a re-downloaded copy was just imported): reset the record to
//     pending with an immediate check so the fresh copy is validated instead of being
//     destroyed by the next repair re-trigger. retry_count restarts for the new copy, but
//     repair_retry_count is preserved as the per-title repair budget so repeatedly broken
//     re-downloads still escalate to corrupted instead of looping forever.
//   - false (Rename events — no new content): preserve repair/corrupted state so a library
//     reorganization cannot wipe repair progress.
func (r *HealthRepository) RelinkFileByMetadata(ctx context.Context, webMeta *model.WebhookMetadata, filePath, libraryPath string, metadataStr *string, revalidate bool) (bool, error) {
	filePath = normalizeHealthPath(filePath)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, file_path, library_path, status, metadata
		FROM file_health
		WHERE status IN ('pending', 'repair_triggered', 'corrupted', 'degraded')
		  AND metadata IS NOT NULL
	`)
	if err != nil {
		return false, fmt.Errorf("failed to query non-healthy records in transaction: %w", err)
	}
	defer rows.Close()

	var matchedIDs []int64
	for rows.Next() {
		var id int64
		var dbFP, dbLP, dbStatus string
		var dbMetaStr string
		if err := rows.Scan(&id, &dbFP, &dbLP, &dbStatus, &dbMetaStr); err != nil {
			continue
		}

		var dbMeta model.WebhookMetadata
		if err := json.Unmarshal([]byte(dbMetaStr), &dbMeta); err != nil {
			continue
		}

		if r.matchMetadata(&dbMeta, webMeta) {
			matchedIDs = append(matchedIDs, id)
		}
	}
	rows.Close() // Explicitly close rows to free connection before modifications

	if len(matchedIDs) == 0 {
		return false, nil
	}

	// Update each matched record
	for _, id := range matchedIDs {
		if err := r.relinkOrMergeRecordTx(ctx, tx, id, filePath, libraryPath, metadataStr, revalidate); err != nil {
			return false, fmt.Errorf("failed to relink/merge matched record %d: %w", id, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}

// matchMetadata matches two WebhookMetadata objects by their IDs.
func (r *HealthRepository) matchMetadata(dbMeta, webMeta *model.WebhookMetadata) bool {
	if dbMeta == nil || webMeta == nil {
		return false
	}

	// 1. Check if both are Movie/Radarr
	if dbMeta.Movie != nil && webMeta.Movie != nil {
		if dbMeta.Movie.Id > 0 && webMeta.Movie.Id > 0 && dbMeta.Movie.Id == webMeta.Movie.Id {
			return true
		}
		if dbMeta.Movie.TmdbId > 0 && webMeta.Movie.TmdbId > 0 && dbMeta.Movie.TmdbId == webMeta.Movie.TmdbId {
			return true
		}
	}

	// 2. Check if both are Series/Sonarr
	if dbMeta.Series != nil && webMeta.Series != nil {
		seriesMatch := false
		if dbMeta.Series.Id > 0 && webMeta.Series.Id > 0 && dbMeta.Series.Id == webMeta.Series.Id {
			seriesMatch = true
		} else if dbMeta.Series.TvdbId > 0 && webMeta.Series.TvdbId > 0 && dbMeta.Series.TvdbId == webMeta.Series.TvdbId {
			seriesMatch = true
		}

		if seriesMatch {
			// Check if EpisodeFile matches
			if dbMeta.EpisodeFile != nil && webMeta.EpisodeFile != nil {
				if dbMeta.EpisodeFile.Id > 0 && webMeta.EpisodeFile.Id > 0 && dbMeta.EpisodeFile.Id == webMeta.EpisodeFile.Id {
					return true
				}
			}
			// Check if any Episode ID matches
			if len(dbMeta.Episodes) > 0 && len(webMeta.Episodes) > 0 {
				for _, dbEp := range dbMeta.Episodes {
					for _, webEp := range webMeta.Episodes {
						if dbEp.Id > 0 && webEp.Id > 0 && dbEp.Id == webEp.Id {
							return true
						}
					}
				}
			}
		}
	}

	// 3. Check if both are Album/Lidarr
	if dbMeta.Album != nil && webMeta.Album != nil {
		if dbMeta.Album.Id > 0 && webMeta.Album.Id > 0 && dbMeta.Album.Id == webMeta.Album.Id {
			return true
		}
	}

	// 4. Check if both are Book/Readarr
	if dbMeta.Book != nil && webMeta.Book != nil {
		if dbMeta.Book.Id > 0 && webMeta.Book.Id > 0 && dbMeta.Book.Id == webMeta.Book.Id {
			return true
		}
	}

	return false
}

// FindHealthyFilesForMovie returns healthy library files matching a movie by TMDB ID or title/year.
// It first searches metadata by TMDB ID; if no matches are found, it falls back to title and year search.
func (r *HealthRepository) FindHealthyFilesForMovie(ctx context.Context, title string, year string, tmdbID int) ([]*FileHealth, error) {
	// 1. Try TMDB ID if available
	if tmdbID > 0 {
		query := fileHealthSelectColumns + " WHERE status = 'healthy' AND metadata LIKE ? ORDER BY id DESC LIMIT 50"
		rows, err := r.db.QueryContext(ctx, query, fmt.Sprintf(`%%"tmdbId":%d%%`, tmdbID))
		if err == nil {
			var results []*FileHealth
			for rows.Next() {
				if h, scanErr := scanFileHealth(rows); scanErr == nil && h != nil {
					results = append(results, h)
				} else if scanErr != nil {
					rows.Close()
					return nil, fmt.Errorf("failed to scan movie health row: %w", scanErr)
				}
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to read movie health rows: %w", rowsErr)
			}
			rows.Close()
			if len(results) > 0 {
				return results, nil
			}
		}
	}

	// 2. Fallback to Title + Year search across file_path and library_path
	cleanTitle := strings.TrimSpace(title)
	if cleanTitle == "" {
		return nil, nil
	}

	titlePattern := buildTitleLikePattern(cleanTitle)
	var query string
	var args []interface{}
	if year != "" {
		yearPattern := "%" + escapeLikePrefix(year) + "%"
		query = fileHealthSelectColumns + " WHERE status = 'healthy' AND (file_path LIKE ? ESCAPE '\\' OR library_path LIKE ? ESCAPE '\\') AND (file_path LIKE ? ESCAPE '\\' OR library_path LIKE ? ESCAPE '\\') ORDER BY id DESC LIMIT 50"
		args = append(args, titlePattern, titlePattern, yearPattern, yearPattern)
	} else {
		query = fileHealthSelectColumns + " WHERE status = 'healthy' AND (file_path LIKE ? ESCAPE '\\' OR library_path LIKE ? ESCAPE '\\') ORDER BY id DESC LIMIT 50"
		args = append(args, titlePattern, titlePattern)
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*FileHealth
	for rows.Next() {
		if h, scanErr := scanFileHealth(rows); scanErr == nil && h != nil {
			results = append(results, h)
		} else if scanErr != nil {
			return nil, fmt.Errorf("failed to scan movie health row: %w", scanErr)
		}
	}
	return results, rows.Err()
}

// buildTitleLikePattern converts space-separated title words into a
// %-separated SQL LIKE wildcard pattern for robust matching.
//
// Note: the pattern is word-order sensitive — "Movie, The" will not match
// release names ordered as "The.Movie". Callers must guard against empty
// titles; an empty input yields an empty pattern (matches nothing) rather
// than a bare "%" (which would match every row).
func buildTitleLikePattern(title string) string {
	clean := strings.TrimSpace(title)
	if clean == "" {
		return ""
	}
	words := strings.Fields(clean)
	escapedWords := make([]string, 0, len(words))
	for _, w := range words {
		wClean := strings.Trim(w, ":;,!?\"'`~")
		if wClean != "" {
			escapedWords = append(escapedWords, escapeLikePrefix(wClean))
		}
	}
	if len(escapedWords) == 0 {
		return ""
	}
	return "%" + strings.Join(escapedWords, "%") + "%"
}

// FindHealthyFilesForSeries returns healthy library files matching a TV series by TVDB ID or series title.
// It first searches metadata by TVDB ID; if no matches are found, it falls back to series title search.
func (r *HealthRepository) FindHealthyFilesForSeries(ctx context.Context, seriesTitle string, tvdbID int) ([]*FileHealth, error) {
	// 1. Try TVDB ID if available
	if tvdbID > 0 {
		query := fileHealthSelectColumns + " WHERE status = 'healthy' AND metadata LIKE ? ORDER BY id DESC LIMIT 100"
		rows, err := r.db.QueryContext(ctx, query, fmt.Sprintf(`%%"tvdbId":%d%%`, tvdbID))
		if err == nil {
			var results []*FileHealth
			for rows.Next() {
				if h, scanErr := scanFileHealth(rows); scanErr == nil && h != nil {
					results = append(results, h)
				} else if scanErr != nil {
					rows.Close()
					return nil, fmt.Errorf("failed to scan series health row: %w", scanErr)
				}
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to read series health rows: %w", rowsErr)
			}
			rows.Close()
			if len(results) > 0 {
				return results, nil
			}
		}
	}

	// 2. Fallback to Series Title search across file_path and library_path
	cleanTitle := strings.TrimSpace(seriesTitle)
	if cleanTitle == "" {
		return nil, nil
	}

	titlePattern := buildTitleLikePattern(cleanTitle)
	query := fileHealthSelectColumns + " WHERE status = 'healthy' AND (file_path LIKE ? ESCAPE '\\' OR library_path LIKE ? ESCAPE '\\') ORDER BY id DESC LIMIT 100"
	rows, err := r.db.QueryContext(ctx, query, titlePattern, titlePattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*FileHealth
	for rows.Next() {
		if h, scanErr := scanFileHealth(rows); scanErr == nil && h != nil {
			results = append(results, h)
		} else if scanErr != nil {
			return nil, fmt.Errorf("failed to scan series health row: %w", scanErr)
		}
	}
	return results, rows.Err()
}
