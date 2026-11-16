package database

import (
	"path/filepath"
	"strings"
	"time"
)

const IndexerUnknown = "Unknown"

// QueueStatus represents the status of a queued import
type QueueStatus string

const (
	QueueStatusPending    QueueStatus = "pending"
	QueueStatusProcessing QueueStatus = "processing"
	QueueStatusCompleted  QueueStatus = "completed"
	QueueStatusFailed     QueueStatus = "failed"
	QueueStatusPaused     QueueStatus = "paused"
	QueueStatusFallback   QueueStatus = "fallback" // Sent to external SABnzbd as fallback
	// QueueStatusWaitingRepair marks an import parked while a PAR2 repair
	// rebuilds its missing articles. The item holds no worker slot; the repair
	// service returns it to pending on success or fails it as unrepairable.
	QueueStatusWaitingRepair QueueStatus = "waiting_repair"
)

// QueuePriority represents the priority level of a queued import
type QueuePriority int

const (
	QueuePriorityHigh   QueuePriority = 1
	QueuePriorityNormal QueuePriority = 2
	QueuePriorityLow    QueuePriority = 3
)

// ImportQueueItem represents a queued NZB file waiting for import
type ImportQueueItem struct {
	ID                  int64         `db:"id"`
	DownloadID          *string       `db:"download_id"` // GUID/String ID for external tracking (e.g. Sonarr/Radarr)
	NzbPath             string        `db:"nzb_path"`
	RelativePath        *string       `db:"relative_path"`
	StoragePath         *string       `db:"storage_path"`
	Category            *string       `db:"category"` // SABnzbd-compatible category
	Priority            QueuePriority `db:"priority"`
	Status              QueueStatus   `db:"status"`
	CreatedAt           time.Time     `db:"created_at"`
	UpdatedAt           time.Time     `db:"updated_at"`
	StartedAt           *time.Time    `db:"started_at"`
	CompletedAt         *time.Time    `db:"completed_at"`
	RetryCount          int           `db:"retry_count"`
	MaxRetries          int           `db:"max_retries"`
	ErrorMessage        *string       `db:"error_message"`
	BatchID             *string       `db:"batch_id"`
	Metadata            *string       `db:"metadata"`    // JSON metadata
	FileSize            *int64        `db:"file_size"`   // Total size in bytes calculated from segments
	TargetPath          *string       `db:"target_path"` // Optional forced symlink destination path
	SkipArrNotification bool          `db:"skip_arr_notification"`
	SkipPostImportLinks bool          `db:"skip_post_import_links"`
	Indexer             *string       `db:"indexer"`
	// PeerHold is set while the item's files (from a peer bundle) owe their
	// full health check. Only the active-queue list and the single-item lookups
	// fill it; every other read leaves it false.
	PeerHold bool `db:"-"`
}

// PeerImportNzbPrefix starts the nzb_path of the queue row of a direct peer
// import. Such a release came from a peer's bundle, not from an NZB: there is
// no file behind the path, and the row cannot be retried, requeued or downloaded.
const PeerImportNzbPrefix = "peernet-bundle/"

// IsPeerImport reports whether the queue item records a direct peer import.
func (i *ImportQueueItem) IsPeerImport() bool {
	return i != nil && strings.HasPrefix(filepath.ToSlash(i.NzbPath), PeerImportNzbPrefix)
}

// BulkOperationResult represents the result of a bulk queue operation
type BulkOperationResult struct {
	DeletedCount    int
	ProcessingCount int
	FailedIDs       []int64
	// DeletedPaths holds the nzb_path values for rows that were actually removed,
	// so callers can clean up the corresponding files on disk.
	DeletedPaths []string
}

// QueueStats represents statistics about the import queue
type QueueStats struct {
	ID              int64 `db:"id"`
	TotalQueued     int   `db:"total_queued"`
	TotalProcessing int   `db:"total_processing"`
	TotalCompleted  int   `db:"total_completed"`
	TotalFailed     int   `db:"total_failed"`
	// TotalWaitingRepair counts imports parked for a PAR2 repair. Counted
	// live; queue_stats has no column for it.
	TotalWaitingRepair  int       `db:"-"`
	AvgProcessingTimeMs *int      `db:"avg_processing_time_ms"`
	LastUpdated         time.Time `db:"last_updated"`
}

// HealthStatus represents the health status of a file
type HealthStatus string

const (
	HealthStatusPending         HealthStatus = "pending"          // File has not been checked yet
	HealthStatusChecking        HealthStatus = "checking"         // File is currently being checked
	HealthStatusHealthy         HealthStatus = "healthy"          // File passed health check
	HealthStatusRepairTriggered HealthStatus = "repair_triggered" // File repair has been triggered in Arrs
	HealthStatusCorrupted       HealthStatus = "corrupted"        // File has missing segments or is corrupted
	HealthStatusDegraded        HealthStatus = "degraded"         // Missing segments only hit media payload: still playable, no repair
)

// HealthPriority represents the priority level of a health check
type HealthPriority int

const (
	HealthPriorityNormal HealthPriority = 0
	HealthPriorityHigh   HealthPriority = 1
	HealthPriorityNext   HealthPriority = 2
)

// FileHealth represents the health tracking of files in the filesystem
type FileHealth struct {
	ID               int64        `db:"id"`
	FilePath         string       `db:"file_path"`
	LibraryPath      *string      `db:"library_path"` // Path to file in library directory (symlink or .strm file)
	Status           HealthStatus `db:"status"`
	LastChecked      *time.Time   `db:"last_checked"`
	LastError        *string      `db:"last_error"`
	RetryCount       int          `db:"retry_count"`        // Health check retry count
	MaxRetries       int          `db:"max_retries"`        // Max health check retries
	RepairRetryCount int          `db:"repair_retry_count"` // Repair retry count
	MaxRepairRetries int          `db:"max_repair_retries"` // Max repair retries
	SourceNzbPath    *string      `db:"source_nzb_path"`
	ErrorDetails     *string      `db:"error_details"` // JSON error details
	Metadata         *string      `db:"metadata"`      // JSON metadata
	CreatedAt        time.Time    `db:"created_at"`
	UpdatedAt        time.Time    `db:"updated_at"`
	// Health check scheduling fields
	ReleaseDate      *time.Time     `db:"release_date"`       // Cached from metadata for scheduling
	ScheduledCheckAt *time.Time     `db:"scheduled_check_at"` // Next check time
	Priority         HealthPriority `db:"priority"`           // Priority level for health checks
	// Failure masking fields
	StreamingFailureCount int     `db:"streaming_failure_count"`
	IsMasked              bool    `db:"is_masked"`
	Indexer               *string `db:"indexer"`
	DownloadID            *string `db:"download_id"`
}

// IsImported reports whether library_path points to a real, ARR-relinked library
// location rather than the import-time placeholder. The placeholder is stored equal
// to file_path (optionally with a leading slash from path normalization); a real
// filesystem path can never equal file_path or "/"+file_path, so this is unambiguous
// on every platform.
func (f *FileHealth) IsImported() bool {
	if f.LibraryPath == nil || *f.LibraryPath == "" {
		return false
	}
	lp := *f.LibraryPath
	return lp != f.FilePath && lp != "/"+f.FilePath
}

// EffectiveLibraryPath returns the real library path and true when the record has
// been relinked by an ARR; otherwise ("", false).
func (f *FileHealth) EffectiveLibraryPath() (string, bool) {
	if !f.IsImported() {
		return "", false
	}
	return *f.LibraryPath, true
}

// User represents a user account in the system
type User struct {
	ID           int64      `db:"id"`
	UserID       string     `db:"user_id"`       // Unique identifier from auth provider
	Email        *string    `db:"email"`         // User email address (nullable)
	Name         *string    `db:"name"`          // User display name (nullable)
	AvatarURL    *string    `db:"avatar_url"`    // User avatar image URL (nullable)
	Provider     string     `db:"provider"`      // Auth provider (direct, github, google, dev, etc.)
	ProviderID   *string    `db:"provider_id"`   // Provider-specific user ID (nullable)
	PasswordHash *string    `db:"password_hash"` // Bcrypt password hash for direct auth (nullable)
	APIKey       *string    `db:"api_key"`       // API key for user authentication (nullable)
	IsAdmin      bool       `db:"is_admin"`      // Admin privileges flag
	CreatedAt    time.Time  `db:"created_at"`    // Account creation timestamp
	UpdatedAt    time.Time  `db:"updated_at"`    // Last profile update timestamp
	LastLogin    *time.Time `db:"last_login"`    // Last login timestamp (nullable)
}

// ImportDailyStat represents historical import statistics for a specific day
type ImportDailyStat struct {
	Day             time.Time `db:"day"`
	CompletedCount  int       `db:"completed_count"`
	FailedCount     int       `db:"failed_count"`
	BytesDownloaded int64     `db:"bytes_downloaded"`
	UpdatedAt       time.Time `db:"updated_at"`
}

// ImportHourlyStat represents historical import statistics for a specific hour
type ImportHourlyStat struct {
	Hour            time.Time `db:"hour"`
	CompletedCount  int       `db:"completed_count"`
	FailedCount     int       `db:"failed_count"`
	BytesDownloaded int64     `db:"bytes_downloaded"`
	UpdatedAt       time.Time `db:"updated_at"`
}

// ImportHistory represents a persistent record of a single imported file
type ImportHistory struct {
	ID          int64     `db:"id"`
	DownloadID  *string   `db:"download_id"`
	NzbID       *int64    `db:"nzb_id"` // Nullable if queue item deleted
	NzbName     string    `db:"nzb_name"`
	FileName    string    `db:"file_name"`
	FileSize    int64     `db:"file_size"`
	VirtualPath string    `db:"virtual_path"`
	LibraryPath *string   `db:"library_path"` // Added to show final location from file_health
	Category    *string   `db:"category"`
	Metadata    *string   `db:"metadata"`
	Indexer     *string   `db:"indexer"`
	CompletedAt time.Time `db:"completed_at"`
}

// ImportMigrationStatus represents the status of a migration item
type ImportMigrationStatus string

const (
	ImportMigrationStatusPending          ImportMigrationStatus = "pending"
	ImportMigrationStatusImported         ImportMigrationStatus = "imported"
	ImportMigrationStatusFailed           ImportMigrationStatus = "failed"
	ImportMigrationStatusSymlinksMigrated ImportMigrationStatus = "symlinks_migrated"
)

// ImportMigration tracks progress of two-phase migrations (e.g. nzbdav → altmount)
type ImportMigration struct {
	ID           int64                 `db:"id"`
	Source       string                `db:"source"`        // e.g. "nzbdav"
	ExternalID   string                `db:"external_id"`   // source-specific ID (nzbdav GUID)
	QueueItemID  *int64                `db:"queue_item_id"` // FK → import_queue.id (nullable)
	RelativePath string                `db:"relative_path"` // virtual path when enqueued
	FinalPath    *string               `db:"final_path"`    // storage_path after import
	Status       ImportMigrationStatus `db:"status"`
	Error        *string               `db:"error"`
	CreatedAt    time.Time             `db:"created_at"`
	UpdatedAt    time.Time             `db:"updated_at"`
}

// ImportMigrationStats holds aggregate counts for a source
type ImportMigrationStats struct {
	Pending          int
	Imported         int
	Failed           int
	SymlinksMigrated int
	Total            int
}

// ProviderHistoricalStat represents aggregated data usage for a provider over a time period
type ProviderHistoricalStat struct {
	Timestamp       time.Time `db:"timestamp"`
	ProviderID      string    `db:"provider_id"`
	BytesDownloaded int64     `db:"bytes_downloaded"`
}

// ProviderSpeedTestStat represents a historical speed test result for a provider
type ProviderSpeedTestStat struct {
	ID         int64     `db:"id"`
	ProviderID string    `db:"provider_id"`
	SpeedMbps  float64   `db:"speed_mbps"`
	CreatedAt  time.Time `db:"created_at"`
}
