package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/kipsilabs/altmount/pkg/rclonecli"
	"github.com/sourcegraph/conc/pool"
)

// SyncProgress tracks the progress of an ongoing library sync
type SyncProgress struct {
	TotalFiles     int       `json:"total_files"`
	ProcessedFiles int       `json:"processed_files"`
	StartTime      time.Time `json:"start_time"`
}

// internalSyncProgress is the internal representation using atomic for thread safety
type internalSyncProgress struct {
	TotalFiles     int
	ProcessedFiles atomic.Int32
	StartTime      time.Time
}

// SyncResult stores the results of a completed library sync
type SyncResult struct {
	FilesAdded          int           `json:"files_added"`
	FilesDeleted        int           `json:"files_deleted"`
	MetadataDeleted     int           `json:"metadata_deleted"`
	LibraryFilesDeleted int           `json:"library_files_deleted"`
	LibraryDirsDeleted  int           `json:"library_dirs_deleted"`
	Duration            time.Duration `json:"duration"`
	CompletedAt         time.Time     `json:"completed_at"`
}

// LibrarySyncStatus represents the current status of the library sync worker
type LibrarySyncStatus struct {
	IsRunning      bool          `json:"is_running"`
	Progress       *SyncProgress `json:"progress,omitempty"`
	LastSyncResult *SyncResult   `json:"last_sync_result,omitempty"`
}

// UsedFiles holds both symlinks and STRM files found in directories
type UsedFiles struct {
	Symlinks  map[string]string // Map of mount target path -> library symlink path
	StrmFiles map[string]string // Map of virtual path (without .strm) -> library .strm file path
}

// LibrarySyncWorker manages automatic health check library synchronization
type LibrarySyncWorker struct {
	metadataService *metadata.MetadataService
	healthRepo      *database.HealthRepository
	configGetter    config.ConfigGetter
	configManager   *config.Manager
	cancelFunc      context.CancelFunc
	mu              sync.Mutex
	running         bool
	progressMu      sync.RWMutex
	progress        *internalSyncProgress
	lastSyncResult  *SyncResult
	manualTrigger   chan struct{}
	rcloneClient    rclonecli.RcloneRcClient
}

// NewLibrarySyncWorker creates a new library sync worker
func NewLibrarySyncWorker(
	metadataService *metadata.MetadataService,
	healthRepo *database.HealthRepository,
	configGetter config.ConfigGetter,
	configManager *config.Manager,
	rcloneClient rclonecli.RcloneRcClient,
) *LibrarySyncWorker {
	worker := &LibrarySyncWorker{
		metadataService: metadataService,
		healthRepo:      healthRepo,
		configGetter:    configGetter,
		configManager:   configManager,
		rcloneClient:    rcloneClient,
		manualTrigger:   make(chan struct{}, 1), // Buffered channel for non-blocking sends
	}

	// Load last result from database if available
	_ = worker.LoadLastResult(context.Background())

	return worker
}

const lastLibrarySyncResultKey = "last_library_sync_result"

// recentSymlinkImportGuard is how long a freshly created symlink is protected
// from orphan cleanup on the strength of its own age alone.
//
// Orphan cleanup also skips symlinks that still have an import_history row, but
// that anchor is not permanent: deleting a queue item drops its history copy
// (issue #586), and history is pruned wholesale at import.history_retention_days.
// A user clearing the completed queue must not be able to strip the protection
// off a symlink whose ARR import is still in flight, so the link's own mtime —
// which no database cleanup can reach — backs the guard up.
const recentSymlinkImportGuard = 24 * time.Hour

// symlinkWithinImportGuard reports whether info describes a symlink young
// enough that an ARR import may still be running for it. A mtime in the future
// (clock skew, a restored backup) counts as recent: the guard fails safe
// towards keeping the file.
func symlinkWithinImportGuard(info os.FileInfo, now time.Time) bool {
	if info == nil || info.Mode()&os.ModeSymlink == 0 {
		return false
	}
	return info.ModTime().After(now.Add(-recentSymlinkImportGuard))
}

// LoadLastResult loads the last sync result from the database
func (lsw *LibrarySyncWorker) LoadLastResult(ctx context.Context) error {
	data, err := lsw.healthRepo.GetSystemState(ctx, lastLibrarySyncResultKey)
	if err != nil {
		return err
	}
	if data == "" {
		return nil
	}

	var result SyncResult
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return err
	}

	lsw.progressMu.Lock()
	lsw.lastSyncResult = &result
	lsw.progressMu.Unlock()

	return nil
}

// StartLibrarySync starts the library sync worker in a background goroutine
func (lsw *LibrarySyncWorker) StartLibrarySync(ctx context.Context) {
	lsw.mu.Lock()
	defer lsw.mu.Unlock()

	if lsw.running {
		slog.WarnContext(ctx, "Library sync worker already running")
		return
	}

	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)
	lsw.cancelFunc = cancel
	lsw.running = true

	go lsw.run(ctx)
}

// Stop stops the library sync worker
func (lsw *LibrarySyncWorker) Stop(ctx context.Context) {
	lsw.mu.Lock()
	defer lsw.mu.Unlock()

	if !lsw.running {
		slog.WarnContext(ctx, "Library sync worker not running")
		return
	}

	if lsw.cancelFunc != nil {
		lsw.cancelFunc()
		lsw.cancelFunc = nil
	}
	lsw.running = false
	slog.InfoContext(ctx, "Library sync worker stopped")
}

// IsRunning returns whether the library sync worker is currently running
func (lsw *LibrarySyncWorker) IsRunning() bool {
	lsw.mu.Lock()
	defer lsw.mu.Unlock()
	return lsw.running
}

// GetStatus returns the current status of the library sync worker
func (lsw *LibrarySyncWorker) GetStatus() LibrarySyncStatus {
	lsw.progressMu.RLock()
	defer lsw.progressMu.RUnlock()

	status := LibrarySyncStatus{
		IsRunning: lsw.progress != nil,
	}

	// Copy progress if available
	if lsw.progress != nil {
		processedFiles := lsw.progress.ProcessedFiles.Load()
		progressCopy := SyncProgress{
			TotalFiles:     lsw.progress.TotalFiles,
			ProcessedFiles: int(processedFiles),
			StartTime:      lsw.progress.StartTime,
		}
		status.Progress = &progressCopy
	}

	// Copy last sync result if available
	if lsw.lastSyncResult != nil {
		resultCopy := *lsw.lastSyncResult
		status.LastSyncResult = &resultCopy
	}

	return status
}

// TriggerManualSync triggers a manual library sync
func (lsw *LibrarySyncWorker) TriggerManualSync(ctx context.Context) error {
	lsw.mu.Lock()
	running := lsw.running
	lsw.mu.Unlock()

	if !running {
		return fmt.Errorf("library sync worker is not running")
	}

	// Non-blocking send to trigger channel
	select {
	case lsw.manualTrigger <- struct{}{}:
		slog.InfoContext(ctx, "Manual library sync triggered")
		return nil
	default:
		// Channel already has a pending trigger
		return fmt.Errorf("library sync already triggered or in progress")
	}
}

// run is the main library sync loop
func (lsw *LibrarySyncWorker) run(ctx context.Context) {
	defer func() {
		lsw.mu.Lock()
		lsw.running = false
		lsw.mu.Unlock()
	}()

	cfg := lsw.configGetter()

	// Only run if health system is enabled
	if cfg.Health.Enabled == nil || !*cfg.Health.Enabled {
		slog.InfoContext(ctx, "Library sync disabled (health system is disabled)")
		return
	}

	if cfg.Health.LibrarySyncIntervalMinutes <= 0 {
		slog.InfoContext(ctx, "Library sync disabled (interval is 0)")
		return
	}

	interval := time.Duration(cfg.Health.LibrarySyncIntervalMinutes) * time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.InfoContext(ctx, "Library sync worker started",
		"interval_minutes", cfg.Health.LibrarySyncIntervalMinutes)

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Library sync worker stopped by context")
			return
		case <-ticker.C:
			lsw.safeSyncLibrary(ctx, false)
		case <-lsw.manualTrigger:
			slog.InfoContext(ctx, "Manual library sync trigger received")
			lsw.safeSyncLibrary(ctx, false)
		}
	}
}

// safeSyncLibrary executes SyncLibrary with panic recovery
func (lsw *LibrarySyncWorker) safeSyncLibrary(ctx context.Context, dryRun bool) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Panic in library sync", "panic", r)
		}
	}()
	lsw.SyncLibrary(ctx, dryRun)
}

// syncMaps holds the metadata and database record maps used during synchronization
type syncMaps struct {
	metaFileSet map[string]string                              // mount relative path -> metadata file path
	dbPathSet   map[string]database.AutomaticHealthCheckRecord // mount relative path -> health check record
}

// syncCounts holds the results of database synchronization operations
type syncCounts struct {
	added   int
	deleted int
}

// cleanupCounts holds the results of cleanup operations
type cleanupCounts struct {
	metadataDeleted     int
	libraryFilesDeleted int
	libraryDirsDeleted  int
}

// findFilesToDelete identifies database records that no longer have corresponding metadata files
// or library files. filesInUse can be nil for metadata-only sync.
func (lsw *LibrarySyncWorker) findFilesToDelete(
	ctx context.Context,
	dbRecords []database.AutomaticHealthCheckRecord,
	metaFileSet map[string]string,
	filesInLibrary map[string]bool,
) []string {
	var filesToDelete []string

	for _, dbRecord := range dbRecords {
		select {
		case <-ctx.Done():
			return filesToDelete
		default:
		}

		// Check if metadata file exists. Normalize to forward slashes so the
		// lookup matches the canonical keys built in buildSyncMaps.
		key := filepath.ToSlash(dbRecord.FilePath)
		if _, exists := metaFileSet[key]; !exists {
			filesToDelete = append(filesToDelete, dbRecord.FilePath)
			continue
		}

		// Check if file is in the official library (only for full sync, not metadata-only)
		if filesInLibrary != nil {
			// If repair is triggered, skip file existence check as it might be temporarily missing during repair
			if dbRecord.Status == database.HealthStatusRepairTriggered {
				continue
			}

			if !filesInLibrary[key] {
				filesToDelete = append(filesToDelete, dbRecord.FilePath)
			}
		}
	}

	return filesToDelete
}

// recordSyncResult stores the sync result and logs completion information
func (lsw *LibrarySyncWorker) recordSyncResult(
	ctx context.Context,
	startTime time.Time,
	dbCounts syncCounts,
	cleanup cleanupCounts,
	totalMetadataFiles int,
	totalDbRecords int,
) {
	duration := time.Since(startTime)
	result := &SyncResult{
		FilesAdded:          dbCounts.added,
		FilesDeleted:        dbCounts.deleted,
		MetadataDeleted:     cleanup.metadataDeleted,
		LibraryFilesDeleted: cleanup.libraryFilesDeleted,
		LibraryDirsDeleted:  cleanup.libraryDirsDeleted,
		Duration:            duration,
		CompletedAt:         time.Now(),
	}

	// Store sync result in memory
	lsw.progressMu.Lock()
	lsw.lastSyncResult = result
	lsw.progressMu.Unlock()

	// Persist sync result to database
	if resultData, err := json.Marshal(result); err == nil {
		if err := lsw.healthRepo.UpdateSystemState(ctx, lastLibrarySyncResultKey, string(resultData)); err != nil {
			slog.ErrorContext(ctx, "Failed to persist library sync result", "error", err)
		}
	}

	// Log completion
	slog.InfoContext(ctx, "Library sync completed",
		"total_metadata_files", totalMetadataFiles,
		"total_db_records", totalDbRecords,
		"added", dbCounts.added,
		"deleted", dbCounts.deleted,
		"metadata_deleted", cleanup.metadataDeleted,
		"library_files_deleted", cleanup.libraryFilesDeleted,
		"library_dirs_deleted", cleanup.libraryDirsDeleted,
		"duration", duration)
}

// syncDatabaseRecords performs batch add and delete operations on the health check database
// Returns the number of records added and deleted. If dryRun is true, it only counts without
// performing actual database operations.
func (lsw *LibrarySyncWorker) syncDatabaseRecords(
	ctx context.Context,
	filesToAdd []database.AutomaticHealthCheckRecord,
	filesToDelete []string,
	dryRun bool,
) syncCounts {
	counts := syncCounts{}

	// Batch add new files
	if len(filesToAdd) > 0 {
		if !dryRun {
			if err := lsw.healthRepo.BatchAddAutomaticHealthChecks(ctx, filesToAdd); err != nil {
				slog.ErrorContext(ctx, "Failed to batch add automatic health checks",
					"count", len(filesToAdd),
					"error", err)
			} else {
				counts.added = len(filesToAdd)
				slog.InfoContext(ctx, "Added new files to automatic health checks",
					"count", counts.added)
			}
		} else {
			counts.added = len(filesToAdd)
		}
	}

	// Batch delete orphaned files
	if len(filesToDelete) > 0 {
		if !dryRun {
			if _, err := lsw.healthRepo.DeleteHealthRecordsBulk(ctx, filesToDelete); err != nil {
				slog.ErrorContext(ctx, "Failed to delete orphaned health records",
					"count", len(filesToDelete),
					"error", err)
			} else {
				counts.deleted = len(filesToDelete)
				slog.InfoContext(ctx, "Deleted orphaned health records",
					"count", counts.deleted)
			}
		} else {
			counts.deleted = len(filesToDelete)
		}
	}

	return counts
}

// buildSyncMaps constructs the lookup maps for metadata files and database records
func (lsw *LibrarySyncWorker) buildSyncMaps(metadataFiles []string, dbRecords []database.AutomaticHealthCheckRecord) syncMaps {
	// Convert metadata files to map for efficient lookup
	metaFileSet := make(map[string]string, len(metadataFiles))
	for _, path := range metadataFiles {
		mountRelativePath := lsw.metaPathToMountRelativePath(path)
		metaFileSet[mountRelativePath] = path
	}

	// Convert database records to map for efficient lookup
	dbPathSet := make(map[string]database.AutomaticHealthCheckRecord, len(dbRecords))
	for _, record := range dbRecords {
		// Normalize to forward slashes so Windows-stored paths match the
		// canonical mount-relative keys produced above.
		dbPathSet[filepath.ToSlash(record.FilePath)] = record
	}

	return syncMaps{
		metaFileSet: metaFileSet,
		dbPathSet:   dbPathSet,
	}
}

// initializeProgressTracking initializes progress tracking for a sync operation
// and returns a cleanup function to be called when sync completes
func (lsw *LibrarySyncWorker) initializeProgressTracking(startTime time.Time) func() {
	lsw.progressMu.Lock()
	lsw.progress = &internalSyncProgress{
		TotalFiles:     0, // Will be updated once we know the total
		ProcessedFiles: atomic.Int32{},
		StartTime:      startTime,
	}
	lsw.progressMu.Unlock()

	// Return cleanup function
	return func() {
		lsw.progressMu.Lock()
		lsw.progress = nil
		lsw.progressMu.Unlock()
	}
}

// SyncLibrary performs a full library synchronization. If dryRun is true,
// it will count what would be deleted without actually deleting anything,
// and return a DryRunResult. If dryRun is false, it performs the sync normally
// and returns nil.
func (lsw *LibrarySyncWorker) SyncLibrary(ctx context.Context, dryRun bool) *DryRunResult {
	startTime := time.Now()
	cfg := lsw.configGetter()
	slog.InfoContext(ctx, "Starting library sync")

	// Determine mount paths for symlink updates
	var oldMountPath, newMountPath string
	if lsw.configManager != nil && lsw.configManager.NeedsLibrarySync() && !dryRun {
		oldMountPath = lsw.configManager.GetPreviousMountPath()
		newMountPath = cfg.MountPath
		slog.InfoContext(ctx, "Will update symlinks during filesystem walk",
			"old_mount", oldMountPath,
			"new_mount", newMountPath)
	}

	// Check import strategy - if NONE and no library dir, only sync DB with metadata files
	if cfg.Import.ImportStrategy == config.ImportStrategyNone && (cfg.Health.LibraryDir == nil || *cfg.Health.LibraryDir == "") {
		slog.InfoContext(ctx, "Import strategy is NONE and no library directory configured, performing metadata-only sync")
		return lsw.syncMetadataOnly(ctx, startTime, dryRun)
	}

	// Initialize progress tracking
	defer lsw.initializeProgressTracking(startTime)()

	// Parallelize filesystem walks for better performance
	var metadataFiles []string
	var libraryFiles *UsedFiles
	var importDirFiles *UsedFiles
	var librarySymlinksUpdated, importSymlinksUpdated int
	var libraryWalkErrors, importWalkErrors int

	fsWalkPool := pool.New().WithErrors().WithMaxGoroutines(3)

	// Get all metadata files from filesystem
	fsWalkPool.Go(func() error {
		files, err := lsw.getAllMetadataFiles(ctx)
		if err != nil {
			return fmt.Errorf("failed to get metadata files: %w", err)
		}
		metadataFiles = files
		return nil
	})

	// Get all library files (symlinks and .strm) to capture library paths
	// Also updates symlinks inline if mount path has changed
	fsWalkPool.Go(func() error {
		files, updated, walkErrs, err := lsw.getAllLibraryFiles(ctx, oldMountPath, newMountPath)
		if err != nil {
			return fmt.Errorf("failed to get library files: %w", err)
		}
		libraryFiles = files
		librarySymlinksUpdated = updated
		libraryWalkErrors = walkErrs
		return nil
	})

	// Get all import directory files
	// Also updates symlinks inline if mount path has changed
	fsWalkPool.Go(func() error {
		files, updated, walkErrs, err := lsw.getAllImportDirFiles(ctx, oldMountPath, newMountPath)
		if err != nil {
			return fmt.Errorf("failed to get import directory files: %w", err)
		}
		importDirFiles = files
		importSymlinksUpdated = updated
		importWalkErrors = walkErrs
		return nil
	})

	if err := fsWalkPool.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "Failed to walk filesystem", "error", err)
		}
		return nil
	}

	// Log and clear mount path change flag if symlinks were updated
	totalSymlinksUpdated := librarySymlinksUpdated + importSymlinksUpdated
	if totalSymlinksUpdated > 0 && lsw.configManager != nil {
		lsw.configManager.ClearLibrarySyncFlag()
		slog.InfoContext(ctx, "Completed mount path symlink updates during filesystem walk",
			"library_symlinks", librarySymlinksUpdated,
			"import_symlinks", importSymlinksUpdated,
			"total_updated", totalSymlinksUpdated)
	}

	// SAFETY: If the library scan returned zero files but we have metadata files,
	// it's almost certainly a mount failure or network glitch.
	// Abort cleanup operations to prevent mass data loss.
	totalFilesFound := len(libraryFiles.Symlinks) + len(libraryFiles.StrmFiles) +
		len(importDirFiles.Symlinks) + len(importDirFiles.StrmFiles)

	shouldCleanup := cfg.Health.CleanupOrphanedMetadata != nil && *cfg.Health.CleanupOrphanedMetadata
	if shouldCleanup && totalFilesFound == 0 && len(metadataFiles) > 0 {
		slog.WarnContext(ctx, "Library scan returned zero files while metadata exists. Possible mount failure? Aborting cleanup for safety.",
			"metadata_count", len(metadataFiles))
		shouldCleanup = false
	}

	// Guard: Walk errors detected — skip cleanup to prevent accidental deletion
	totalWalkErrors := libraryWalkErrors + importWalkErrors
	if shouldCleanup && totalWalkErrors > 0 {
		slog.WarnContext(ctx, "Walk errors detected during library scan, skipping cleanup",
			"walk_errors", totalWalkErrors,
			"library_walk_errors", libraryWalkErrors,
			"import_walk_errors", importWalkErrors)
		shouldCleanup = false
	}

	// Update total files count
	lsw.progressMu.Lock()
	lsw.progress.TotalFiles = len(metadataFiles)
	lsw.progressMu.Unlock()

	// Build a reverse map: mount relative path -> library path for quick lookup
	filesInUse := make(map[string]string)
	// Track which files are actually in the official library (to protect them from deletion)
	filesInLibrary := make(map[string]bool)

	// Helper to normalize keys to mount relative paths
	normalizeKeys := func(m map[string]string, isLibrary bool) {
		for target, libPath := range m {
			// Extract relative path within the mount
			rel := strings.TrimPrefix(filepath.ToSlash(target), filepath.ToSlash(cfg.MountPath))
			rel = strings.TrimPrefix(rel, "/")
			filesInUse[rel] = libPath
			if isLibrary {
				filesInLibrary[rel] = true
			}
		}
	}

	normalizeKeys(libraryFiles.Symlinks, true)
	normalizeKeys(libraryFiles.StrmFiles, true)
	normalizeKeys(importDirFiles.Symlinks, false)
	normalizeKeys(importDirFiles.StrmFiles, false)

	// Get all health check paths from database
	dbRecords, err := lsw.healthRepo.GetAllHealthCheckRecords(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get automatic health check paths from database", "error", err)
		return nil
	}

	// Build lookup maps for efficient searching
	syncMaps := lsw.buildSyncMaps(metadataFiles, dbRecords)
	metaFileSet := syncMaps.metaFileSet
	dbPathSet := syncMaps.dbPathSet

	// Find files to add (in filesystem but not in database)
	// Collect results via channel to avoid large slice allocations and lock contention
	filesToAddChan := make(chan database.AutomaticHealthCheckRecord, 100)
	var totalAdded int

	// Start a goroutine to process results from the channel in batches to save RAM
	done := make(chan struct{})
	go func() {
		const batchSize = 1000
		batch := make([]database.AutomaticHealthCheckRecord, 0, batchSize)

		flushBatch := func() {
			if len(batch) == 0 {
				return
			}
			if !dryRun {
				if err := lsw.healthRepo.BatchAddAutomaticHealthChecks(ctx, batch); err != nil {
					slog.ErrorContext(ctx, "Failed to batch add automatic health checks",
						"count", len(batch),
						"error", err)
				}
			}
			totalAdded += len(batch)
			batch = batch[:0]
		}

		for record := range filesToAddChan {
			batch = append(batch, record)
			if len(batch) >= batchSize {
				flushBatch()
			}
		}
		flushBatch()
		close(done)
	}()

	concurrency := cfg.GetLibrarySyncConcurrency()

	// Categories excluded from health checking must never be registered by the
	// discovery pass. Resolve their mount-relative prefixes once up front.
	excludedPrefixes := buildExcludedCategoryPrefixes(cfg)

	// Create a worker pool for parallel metadata reading
	p := pool.New().WithMaxGoroutines(concurrency)

	for mountRelativePath := range metaFileSet {
		select {
		case <-ctx.Done():
			p.Wait()
			close(filesToAddChan)
			return nil
		default:
		}

		// Capture loop variable for goroutine
		path := mountRelativePath

		// Skip files under health-excluded categories: never (re)register them.
		if pathHasExcludedPrefix(path, excludedPrefixes) {
			continue
		}

		p.Go(func() {
			// Check if needs to be added or repaired
			var existingRecord *database.AutomaticHealthCheckRecord
			if it, exists := dbPathSet[path]; exists {
				existingRecord = &it
			}

			// Look up library path from our map (found via symlinks/strm scanning)
			libraryPath := lsw.getLibraryPath(path, filesInUse)

			// Path Recovery: If record exists but the physical path is broken,
			// try to see if it's at the expected location even if not in filesInUse
			if existingRecord != nil && libraryPath == nil {
				lp := existingRecord.LibraryPath
				// A path needs recovery if it's nil, missing from disk, or is a relative path (buggy)
				needsRecovery := lp == nil

				if !needsRecovery {
					// Check if path is absolute and file exists
					if !filepath.IsAbs(*lp) {
						needsRecovery = true
					} else if _, err := os.Stat(*lp); os.IsNotExist(err) {
						needsRecovery = true
					}
				}

				if needsRecovery {
					// Use the configured mount path to build an absolute expected path
					expectedPath := utils.JoinAbsPath(cfg.MountPath, path)
					if _, err := os.Stat(expectedPath); err == nil {
						// Found it! Use this recovered path ONLY if it is absolute and NOT equal to the local mount path
						// (since repairs MUST use the library path Sonarr/Radarr expects).
						if filepath.IsAbs(expectedPath) && (lp == nil || *lp != expectedPath) {
							// Check if the expected path is actually a library path (must contain libraryDir or be different from mountPath)
							isLibraryPath := false
							if cfg.Health.LibraryDir != nil && strings.HasPrefix(expectedPath, *cfg.Health.LibraryDir) {
								isLibraryPath = true
							}

							if isLibraryPath {
								libStr := expectedPath
								libraryPath = &libStr
								slog.InfoContext(ctx, "Recovered broken library path",
									"path", path, "new_location", expectedPath)

								// Update DB immediately for this recovered path
								if err := lsw.healthRepo.UpdateLibraryPath(ctx, path, expectedPath); err != nil {
									slog.ErrorContext(ctx, "Failed to update recovered library path",
										"path", path, "error", err)
								}
							}
						}
					}
				}
			}

			// Determine if we need to update/add the record
			needsProcess := existingRecord == nil
			if !needsProcess {
				// Update if library path changed (e.g. renamed or recovered)
				oldLP := existingRecord.LibraryPath
				if libraryPath != nil && (oldLP == nil || *oldLP != *libraryPath) {
					needsProcess = true
				}
			}

			if needsProcess {
				record, err := lsw.processMetadataForSync(ctx, path, libraryPath)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to read metadata during sync, registering as corrupted",
						"mount_relative_path", path,
						"error", err)

					// Register as corrupted so HealthWorker can pick it up and trigger repair
					// Even if libraryPath is nil, we want to track this metadata corruption
					regErr := lsw.healthRepo.RegisterCorruptedFile(ctx, path, libraryPath, err.Error())
					if regErr != nil {
						slog.ErrorContext(ctx, "Failed to register corrupted file", "path", path, "error", regErr)
					}
					return
				}

				if record != nil {
					filesToAddChan <- *record
				}
			}

			if lsw.progress != nil {
				current := lsw.progress.ProcessedFiles.Add(1)
				if current%1000 == 0 {
					slog.InfoContext(ctx, "Processing metadata progress", "count", current)
				}
			}
		})
	}

	// Wait for all workers to complete and close results channel
	p.Wait()
	close(filesToAddChan)
	<-done

	// Two-pass soft delete for orphaned metadata files
	// Only delete metadata if it was orphaned in BOTH the current AND previous sync run.
	const pendingMetaDeletionKey = "pending_metadata_deletions"
	const pendingLibraryDeletionKey = "pending_library_deletions"

	metadataDeletedCount := 0
	if shouldCleanup {
		// Build current orphan set
		currentMetaOrphans := make(map[string]string) // mount_relative_path -> meta_path
		for relativeMountPath, metaPath := range metaFileSet {
			libraryPath := lsw.getLibraryPath(relativeMountPath, filesInUse)
			if libraryPath == nil {
				currentMetaOrphans[relativeMountPath] = metaPath
			}
		}

		// Ratio guard: if >20% of metadata files would be orphaned, something is wrong
		if len(metaFileSet) > 0 {
			orphanRatio := float64(len(currentMetaOrphans)) / float64(len(metaFileSet))
			if orphanRatio > 0.20 {
				slog.WarnContext(ctx, "Suspiciously high orphan ratio, skipping cleanup",
					"orphan_ratio", orphanRatio,
					"potential_orphans", len(currentMetaOrphans),
					"total_metadata", len(metaFileSet))
				shouldCleanup = false
			}
		}

		if shouldCleanup {
			// Load previous run's pending set from DB
			var previousPending map[string]bool
			if raw, sErr := lsw.healthRepo.GetSystemState(ctx, pendingMetaDeletionKey); sErr == nil && raw != "" {
				_ = json.Unmarshal([]byte(raw), &previousPending)
			}
			if previousPending == nil {
				previousPending = make(map[string]bool)
			}

			deletedDirs := make(map[string]bool)

			for relativeMountPath := range currentMetaOrphans {
				select {
				case <-ctx.Done():
					return nil
				default:
				}

				if previousPending[relativeMountPath] {
					// Confirmed orphan: missing in two consecutive runs → safe to delete
					if !dryRun {
						if err := lsw.metadataService.DeleteFileMetadata(ctx, relativeMountPath); err != nil {
							if !os.IsNotExist(err) {
								slog.ErrorContext(ctx, "Failed to delete confirmed orphaned metadata",
									"path", relativeMountPath, "error", err)
							}
						} else {
							slog.InfoContext(ctx, "Deleted confirmed orphaned metadata file (not found in library for 2 consecutive syncs)",
								"path", relativeMountPath)
							metadataDeletedCount++
							// Virtual path: keep it forward-slash so it matches a
							// VFS node on Windows too (see NotifyRcloneVFS).
							deletedDirs[path.Dir(rclonecli.ToVFSPath(relativeMountPath))] = true
						}
					} else {
						metadataDeletedCount++
					}
				}
			}

			if !dryRun && lsw.rcloneClient != nil && len(deletedDirs) > 0 {
				cfg := lsw.configGetter()
				switch cfg.MountType {
				case config.MountTypeRClone, config.MountTypeRCloneExternal:
					dirs := make([]string, 0, len(deletedDirs))
					for d := range deletedDirs {
						dirs = append(dirs, d)
					}
					vfsName := cfg.RClone.VFSName
					if vfsName == "" {
						vfsName = config.MountProvider
					}
					go func() {
						c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()
						// Deletion only: forget the directories so rclone drops
						// stale cache entries; skip the eager refresh since
						// nothing remains to list and the next access re-lists
						// lazily.
						_ = lsw.rcloneClient.ForgetDir(c, vfsName, dirs)
					}()
				}
			}

			// Persist current first-time orphans as pending for next run
			pendingPaths := make(map[string]bool, len(currentMetaOrphans))
			for path := range currentMetaOrphans {
				if !previousPending[path] {
					pendingPaths[path] = true
				}
			}
			if data, mErr := json.Marshal(pendingPaths); mErr == nil {
				_ = lsw.healthRepo.UpdateSystemState(ctx, pendingMetaDeletionKey, string(data))
			}
		}
	}

	// If cleanup was disabled (e.g. by ratio guard), clear the pending metadata set
	if !shouldCleanup {
		_ = lsw.healthRepo.UpdateSystemState(ctx, pendingMetaDeletionKey, "")
	}

	// Two-pass soft delete for orphaned library files (symlinks and STRM files without metadata)
	libraryFilesDeletedCount := 0
	libraryDirsDeletedCount := 0

	if shouldCleanup {
		// Build current orphan set for library files
		currentLibOrphans := make(map[string]string) // metaPath -> library file path
		for metaPath, file := range filesInUse {
			if _, exists := metaFileSet[metaPath]; !exists {
				currentLibOrphans[metaPath] = file
			}
		}

		// Load previous run's pending set from DB
		var previousLibPending map[string]bool
		if raw, sErr := lsw.healthRepo.GetSystemState(ctx, pendingLibraryDeletionKey); sErr == nil && raw != "" {
			_ = json.Unmarshal([]byte(raw), &previousLibPending)
		}
		if previousLibPending == nil {
			previousLibPending = make(map[string]bool)
		}

		for metaPath, file := range currentLibOrphans {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			if previousLibPending[metaPath] {
				// Confirmed orphan: missing in two consecutive runs → safe to delete
				if !dryRun {
					// SAFETY: Never delete physical files in NONE strategy.
					if cfg.Import.ImportStrategy == config.ImportStrategyNone {
						slog.DebugContext(ctx, "Skipped library file deletion (NONE strategy safety)", "path", file)
						continue
					}

					// Extra safety for other strategies: verify file type before deletion
					info, err := os.Lstat(file)
					if err != nil {
						continue
					}

					if cfg.Import.ImportStrategy == config.ImportStrategySYMLINK {
						// Only delete if it's actually a symlink
						if info.Mode()&os.ModeSymlink == 0 {
							slog.WarnContext(ctx, "Skipped orphaned file deletion: not a symlink", "path", file)
							continue
						}

						// Protect symlinks young enough that an ARR import may
						// still be in flight, whatever the database says.
						if symlinkWithinImportGuard(info, time.Now()) {
							slog.InfoContext(ctx, "Skipping orphaned symlink deletion: created too recently for an ARR import to have finished",
								"path", file, "created", info.ModTime())
							continue
						}

						// Protect symlinks that have import history (AltMount imported this file)
						target, readlinkErr := os.Readlink(file)
						if readlinkErr == nil {
							mountRelPath := strings.TrimPrefix(filepath.ToSlash(target), filepath.ToSlash(cfg.MountPath))
							mountRelPath = strings.TrimPrefix(mountRelPath, "/")
							if mountRelPath != "" {
								hasHistory, checkErr := lsw.healthRepo.HasImportHistoryForPath(ctx, mountRelPath)
								if checkErr == nil && hasHistory {
									slog.InfoContext(ctx, "Skipping orphaned symlink deletion: import history exists for this file",
										"path", file, "virtual_path", mountRelPath)
									continue
								}
							}
						}
					} else if cfg.Import.ImportStrategy == config.ImportStrategySTRM {
						// Only delete if it's actually a .strm file
						if !strings.HasSuffix(strings.ToLower(file), ".strm") {
							slog.WarnContext(ctx, "Skipped orphaned file deletion: not a .strm file", "path", file)
							continue
						}
					}

					// HARD SAFETY: Never delete the entire mount root or library root
					cleanFile := filepath.Clean(file)
					cleanMount := ""
					if cfg.MountPath != "" {
						cleanMount = filepath.Clean(cfg.MountPath)
					}
					cleanLibDir := ""
					if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
						cleanLibDir = filepath.Clean(*cfg.Health.LibraryDir)
					}

					if cleanFile == cleanMount || cleanFile == cleanLibDir || cleanFile == "/" || cleanFile == "." {
						slog.WarnContext(ctx, "Nuclear Guard: Blocked attempt to delete protected path in sync", "path", cleanFile)
						continue
					}

					err = os.Remove(file)
					if err != nil {
						if !os.IsNotExist(err) {
							slog.ErrorContext(ctx, "Failed to delete library file", "path", file, "error", err)
						}
						continue
					}
					slog.InfoContext(ctx, "Deleted confirmed orphaned library file", "path", file, "target", metaPath)
				}
				libraryFilesDeletedCount++
			}
		}

		// Persist current first-time orphans as pending for next run
		pendingLibPaths := make(map[string]bool, len(currentLibOrphans))
		for path := range currentLibOrphans {
			if !previousLibPending[path] {
				pendingLibPaths[path] = true
			}
		}
		if data, mErr := json.Marshal(pendingLibPaths); mErr == nil {
			_ = lsw.healthRepo.UpdateSystemState(ctx, pendingLibraryDeletionKey, string(data))
		}

		// Remove empty directories after file cleanup (only if not dry run and cleanup is safe)
		if !dryRun {
			var err error
			libraryDirsDeletedCount, err = lsw.removeEmptyDirectories(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.ErrorContext(ctx, "Failed to remove empty directories", "error", err)
				}
			}
		}
	}

	// If cleanup was disabled, clear the pending library set
	if !shouldCleanup {
		_ = lsw.healthRepo.UpdateSystemState(ctx, pendingLibraryDeletionKey, "")
	}

	// Find files to delete (in database but not in filesystem or not in the official library)
	// SAFETY: If mount protection is triggered (shouldCleanup is false),
	// we pass nil for filesInLibrary to skip the 'in-use' check and prevent mass record deletion.
	effectiveFilesInLibrary := filesInLibrary
	if !shouldCleanup {
		effectiveFilesInLibrary = nil
	}
	filesToDelete := lsw.findFilesToDelete(ctx, dbRecords, metaFileSet, effectiveFilesInLibrary)

	// Perform batch operations for deletions (additions were already streamed)
	dbCounts := lsw.syncDatabaseRecords(ctx, nil, filesToDelete, dryRun)
	dbCounts.added = totalAdded

	// Cleanup orphaned .ids/ symlinks after all other cleanup phases
	if shouldCleanup && !dryRun {
		removed, idsErr := lsw.metadataService.CleanupOrphanedIDSymlinks(ctx)
		if idsErr != nil && !errors.Is(idsErr, context.Canceled) {
			slog.ErrorContext(ctx, "Failed to cleanup orphaned ID symlinks", "error", idsErr)
		} else if removed > 0 {
			slog.InfoContext(ctx, "Cleaned up orphaned ID symlinks", "count", removed)
		}
	}

	// Return dry run results or record sync results
	if dryRun {
		wouldCleanup := cfg.Health.CleanupOrphanedMetadata != nil && *cfg.Health.CleanupOrphanedMetadata
		return &DryRunResult{
			OrphanedMetadataCount:  metadataDeletedCount,
			OrphanedLibraryFiles:   libraryFilesDeletedCount,
			DatabaseRecordsToClean: dbCounts.deleted,
			WouldCleanup:           wouldCleanup,
		}
	}

	// Record sync results
	cleanup := cleanupCounts{
		metadataDeleted:     metadataDeletedCount,
		libraryFilesDeleted: libraryFilesDeletedCount,
		libraryDirsDeleted:  libraryDirsDeletedCount,
	}
	lsw.recordSyncResult(ctx, startTime, dbCounts, cleanup, len(metadataFiles), len(dbRecords))
	return nil
}

// DryRunResult holds the results of a dry run sync
type DryRunResult struct {
	OrphanedMetadataCount  int
	OrphanedLibraryFiles   int
	DatabaseRecordsToClean int
	WouldCleanup           bool
}

// getAllMetadataFiles collects all .meta files from the filesystem
func (lsw *LibrarySyncWorker) getAllMetadataFiles(ctx context.Context) ([]string, error) {
	cfg := lsw.configGetter()
	rootPath := cfg.Metadata.RootPath

	var metaFiles []string
	count := 0
	err := filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return nil // Skip errors
		}

		// Skip the corrupted_metadata directory
		if d.IsDir() && d.Name() == "corrupted_metadata" {
			return filepath.SkipDir
		}

		// Only include .meta files
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".meta") {
			metaFiles = append(metaFiles, path)
			count++
			if count%1000 == 0 {
				slog.InfoContext(ctx, "Scanning metadata files progress", "count", count)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "Finished scanning metadata files", "total", count)
	return metaFiles, nil
}

// metaPathToMountRelativePath converts a metadata file path to a mount relative file path.
// Uses filepath.Rel + filepath.ToSlash so the result is canonical (forward slashes) and
// tolerant to differences in how the metadata root is spelled in config (./prefix,
// trailing separator, mixed separators on Windows, etc).
func (lsw *LibrarySyncWorker) metaPathToMountRelativePath(metaPath string) string {
	rootPath := lsw.configGetter().Metadata.RootPath

	rel, err := filepath.Rel(rootPath, metaPath)
	if err != nil {
		rel = strings.TrimPrefix(metaPath, rootPath)
		rel = strings.TrimPrefix(rel, string(filepath.Separator))
	}
	rel = strings.TrimSuffix(rel, ".meta")
	return filepath.ToSlash(rel)
}

// processMetadataForSync reads metadata and creates an AutomaticHealthCheckRecord.
// Returns nil record if metadata is invalid or file should be skipped.
// Returns an error only if metadata read failed (caller should handle registration).
func (lsw *LibrarySyncWorker) processMetadataForSync(
	ctx context.Context,
	path string,
	libraryPath *string,
) (*database.AutomaticHealthCheckRecord, error) {
	// Read metadata to get release date
	fileMeta, err := lsw.metadataService.ReadFileMetadata(path)
	if err != nil {
		return nil, err
	}
	if fileMeta == nil {
		return nil, nil
	}

	// Use CreatedAt if ReleaseDate is missing
	releaseDate := fileMeta.ReleaseDate
	if releaseDate == 0 {
		releaseDate = fileMeta.CreatedAt
		// Update metadata file with the CreatedAt as release date
		fileMeta.ReleaseDate = releaseDate
		if writeErr := lsw.metadataService.WriteFileMetadata(path, fileMeta); writeErr != nil {
			slog.ErrorContext(ctx, "Failed to update metadata with release date",
				"path", path,
				"error", writeErr)
		} else {
			slog.InfoContext(ctx, "Set release date from CreatedAt",
				"path", path,
				"release_date", time.Unix(releaseDate, 0))
		}
	}

	// Convert Unix timestamp to time.Time
	releaseDateAsTime := time.Unix(releaseDate, 0)

	// Calculate initial check time
	scheduledCheckAt := calculateInitialCheck()

	cfg := lsw.configGetter()

	return &database.AutomaticHealthCheckRecord{
		FilePath:         path,
		LibraryPath:      libraryPath,
		ReleaseDate:      &releaseDateAsTime,
		ScheduledCheckAt: &scheduledCheckAt,
		SourceNzbPath:    &fileMeta.SourceNzbPath,
		MaxRetries:       cfg.GetMaxRetries(),
		MaxRepairRetries: cfg.GetMaxRepairRetries(),
	}, nil
}

// updateSymlinkForMountChange updates a symlink when mount path changes.
// Returns the new target path, whether the symlink was updated, and any error.
func updateSymlinkForMountChange(
	ctx context.Context,
	symlinkPath string,
	currentTarget string,
	oldMountPath string,
	newMountPath string,
) (string, bool, error) {
	// Extract relative path within the mount
	relativePath := strings.TrimPrefix(currentTarget, oldMountPath)
	relativePath = strings.TrimPrefix(relativePath, string(filepath.Separator))

	// Create new target path
	newTarget := filepath.Join(newMountPath, relativePath)

	// HARD SAFETY: Never delete protected paths
	cleanSymlink := filepath.Clean(symlinkPath)
	if cleanSymlink == "/" || cleanSymlink == "." {
		return currentTarget, false, fmt.Errorf("safety block: refusing to remove protected symlink path: %s", cleanSymlink)
	}

	// Remove old symlink
	if err := os.Remove(symlinkPath); err != nil {
		slog.WarnContext(ctx, "Failed to remove old symlink during mount path update",
			"path", symlinkPath,
			"error", err)
		return currentTarget, false, err
	}

	// Create new symlink
	if err := os.Symlink(newTarget, symlinkPath); err != nil {
		slog.ErrorContext(ctx, "Failed to create updated symlink",
			"path", symlinkPath,
			"old_target", currentTarget,
			"new_target", newTarget,
			"error", err)
		return currentTarget, false, err
	}

	slog.InfoContext(ctx, "Updated symlink to new mount path",
		"path", symlinkPath,
		"old_target", currentTarget,
		"new_target", newTarget)

	return newTarget, true, nil
}

// getAllLibraryFiles collects both symlinks and .strm files from library directory in a single pass.
// If oldMountPath and newMountPath are provided, it also updates symlinks pointing to the old path.
// Returns the used files, symlink update count, walk error count, and any fatal error.
func (lsw *LibrarySyncWorker) getAllLibraryFiles(ctx context.Context, oldMountPath, newMountPath string) (*UsedFiles, int, int, error) {
	cfg := lsw.configGetter()

	// Get library directory
	if cfg.Health.LibraryDir == nil || *cfg.Health.LibraryDir == "" {
		return nil, 0, 0, fmt.Errorf("library directory is not configured")
	}

	libraryDir := *cfg.Health.LibraryDir
	mountDir := cfg.MountPath
	cleanMountDir := filepath.Clean(mountDir)

	result := &UsedFiles{
		Symlinks:  make(map[string]string),
		StrmFiles: make(map[string]string),
	}

	symlinkUpdates := 0
	walkErrors := 0
	count := 0
	shouldUpdateSymlinks := oldMountPath != "" && newMountPath != "" && oldMountPath != newMountPath
	oldMountPathClean := filepath.Clean(oldMountPath)
	newMountPathClean := filepath.Clean(newMountPath)

	// Walk the library directory recursively once
	err := filepath.WalkDir(libraryDir, func(path string, d os.DirEntry, err error) error {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			walkErrors++
			slog.WarnContext(ctx, "Error walking library directory", "path", path, "error", err)
			return nil // Continue walking despite errors
		}

		// Check if it's a symlink
		if d.Type()&os.ModeSymlink != 0 {
			// Read the symlink target
			target, err := os.Readlink(path)
			if err != nil {
				slog.WarnContext(ctx, "Failed to read symlink", "path", path, "error", err)
				return nil
			}

			// Make target absolute if it's relative
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(path), target)
			}

			// Clean the paths for comparison
			cleanTarget := filepath.Clean(target)

			// Update symlink if it points to the old mount path
			if shouldUpdateSymlinks && strings.HasPrefix(cleanTarget, oldMountPathClean) {
				newTarget, updated, err := updateSymlinkForMountChange(ctx, path, cleanTarget, oldMountPathClean, newMountPathClean)
				if err != nil {
					return nil
				}
				if updated {
					symlinkUpdates++
					cleanTarget = newTarget
				}
			}

			// Check if this symlink points inside the mount directory
			if strings.HasPrefix(cleanTarget, cleanMountDir) {
				// Store mapping of mount target path -> library symlink path
				result.Symlinks[cleanTarget] = path
				count++
			}
		} else if !d.IsDir() {
			// Check if it's a .strm file
			if strings.HasSuffix(d.Name(), ".strm") {
				// Read the STRM file content to extract the URL
				content, err := os.ReadFile(path)
				if err != nil {
					slog.WarnContext(ctx, "Failed to read STRM file",
						"path", path,
						"error", err)
					return nil
				}

				// Parse the URL from the file content (trim whitespace)
				urlStr := strings.TrimSpace(string(content))
				parsedURL, err := url.Parse(urlStr)
				if err != nil {
					slog.WarnContext(ctx, "Failed to parse URL from STRM file",
						"path", path,
						"url", urlStr,
						"error", err)
					return nil
				}

				// Extract the 'path' query parameter
				virtualPath := parsedURL.Query().Get("path")
				if virtualPath == "" {
					slog.WarnContext(ctx, "STRM file URL missing 'path' query parameter",
						"path", path,
						"url", urlStr)
					return nil
				}

				// Normalize path separators
				virtualPath = filepath.ToSlash(virtualPath)

				// Store mapping of virtual path -> library .strm file path
				result.StrmFiles[virtualPath] = path
				count++
			} else if cfg.Import.ImportStrategy == config.ImportStrategyNone {
				// For NONE strategy, also count regular files if they are in the mount directory
				cleanPath := filepath.Clean(path)

				if strings.HasPrefix(cleanPath, cleanMountDir) {
					// In NONE strategy, the library file IS the mount file
					result.Symlinks[cleanPath] = path
					count++
				}
			}
		}

		if count > 0 && count%1000 == 0 {
			slog.InfoContext(ctx, "Scanning library files progress", "count", count)
		}

		return nil
	})

	if err != nil {
		slog.ErrorContext(ctx, "Error during library file scan", "error", err)
		return nil, 0, walkErrors, err
	}

	slog.InfoContext(ctx, "Finished scanning library files", "total", count, "walk_errors", walkErrors)
	return result, symlinkUpdates, walkErrors, nil
}

// getAllImportDirFiles collects both regular files and .strm files from import directory in a single pass.
// Returns the used files, symlink update count, walk error count, and any fatal error.
func (lsw *LibrarySyncWorker) getAllImportDirFiles(ctx context.Context, oldMountPath, newMountPath string) (*UsedFiles, int, int, error) {
	cfg := lsw.configGetter()

	// Get import directory
	if cfg.Import.ImportDir == nil || *cfg.Import.ImportDir == "" {
		// No import directory configured - return empty result
		return &UsedFiles{
			Symlinks:  make(map[string]string),
			StrmFiles: make(map[string]string),
		}, 0, 0, nil
	}

	importDir := *cfg.Import.ImportDir

	// Check if directory exists
	if _, err := os.Stat(importDir); os.IsNotExist(err) {
		slog.WarnContext(ctx, "Import directory does not exist", "import_dir", importDir)
		return &UsedFiles{
			Symlinks:  make(map[string]string),
			StrmFiles: make(map[string]string),
		}, 0, 0, nil
	}

	result := &UsedFiles{
		Symlinks:  make(map[string]string),
		StrmFiles: make(map[string]string),
	}

	symlinkUpdates := 0
	walkErrors := 0
	shouldUpdateSymlinks := oldMountPath != "" && newMountPath != "" && oldMountPath != newMountPath
	oldMountPathClean := filepath.Clean(oldMountPath)
	newMountPathClean := filepath.Clean(newMountPath)
	mountDir := cfg.MountPath

	// Walk the import directory recursively once
	err := filepath.WalkDir(importDir, func(path string, d os.DirEntry, err error) error {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			walkErrors++
			slog.WarnContext(ctx, "Error walking import directory", "path", path, "error", err)
			return nil // Continue walking despite errors
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Get relative path from import_dir
		relativePath, err := filepath.Rel(importDir, path)
		if err != nil {
			slog.WarnContext(ctx, "Failed to get relative path for import file",
				"path", path,
				"error", err)
			return nil
		}

		// Normalize path separators
		virtualPath := filepath.ToSlash(relativePath)

		// Check if it's a symlink
		if d.Type()&os.ModeSymlink != 0 {
			// Read the symlink target
			target, err := os.Readlink(path)
			if err != nil {
				slog.WarnContext(ctx, "Failed to read symlink in import directory",
					"path", path,
					"error", err)
				return nil
			}

			// Make target absolute if it's relative
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(path), target)
			}

			// Clean the paths for comparison
			cleanTarget := filepath.Clean(target)
			cleanMountDir := filepath.Clean(mountDir)

			// Update symlink if it points to the old mount path
			if shouldUpdateSymlinks && strings.HasPrefix(cleanTarget, oldMountPathClean) {
				newTarget, updated, _ := updateSymlinkForMountChange(ctx, path, cleanTarget, oldMountPathClean, newMountPathClean)
				if updated {
					symlinkUpdates++
					cleanTarget = newTarget
				}
			}

			// Validate that the symlink points to mount directory
			if !strings.HasPrefix(cleanTarget, cleanMountDir) {
				slog.WarnContext(ctx, "Symlink in import directory does not point to mount directory",
					"path", path,
					"target", cleanTarget,
					"mount_dir", cleanMountDir)
				return nil
			}

			// Store mapping of mount target path -> library symlink path
			result.Symlinks[cleanTarget] = path
		} else if strings.HasSuffix(d.Name(), ".strm") {
			// Read the STRM file content to extract the URL
			content, err := os.ReadFile(path)
			if err != nil {
				slog.WarnContext(ctx, "Failed to read STRM file in import directory",
					"path", path,
					"error", err)
				return nil
			}

			// Parse the URL from the file content (trim whitespace)
			urlStr := strings.TrimSpace(string(content))
			parsedURL, err := url.Parse(urlStr)
			if err != nil {
				slog.WarnContext(ctx, "Failed to parse URL from STRM file in import directory",
					"path", path,
					"url", urlStr,
					"error", err)
				return nil
			}

			// Extract the 'path' query parameter
			virtualPath := parsedURL.Query().Get("path")
			if virtualPath == "" {
				slog.WarnContext(ctx, "STRM file URL in import directory missing 'path' query parameter",
					"path", path,
					"url", urlStr)
				return nil
			}

			// Normalize path separators
			virtualPath = filepath.ToSlash(virtualPath)

			// Store mapping of virtual path -> library .strm file path
			result.StrmFiles[virtualPath] = path
		} else if cfg.Import.ImportStrategy == config.ImportStrategyNone {
			// For NONE strategy, also count regular files
			result.Symlinks[virtualPath] = path
		}

		return nil
	})

	if err != nil {
		slog.ErrorContext(ctx, "Error during import directory file scan", "error", err)
		return nil, 0, walkErrors, err
	}

	return result, symlinkUpdates, walkErrors, nil
}

// getLibraryPath looks up the library path for a given mount relative path
// It checks both the full mount path and the relative path (for STRM files)
func (lsw *LibrarySyncWorker) getLibraryPath(metaPath string, filesInUse map[string]string) *string {
	cfg := lsw.configGetter()
	mountPath := utils.JoinAbsPath(cfg.MountPath, metaPath)

	if libPath, ok := filesInUse[mountPath]; ok {
		return &libPath
	}

	if libPath, ok := filesInUse[metaPath]; ok {
		// Try with virtual path (for STRM files)
		return &libPath
	}

	return nil
}

// resolveCategoryDir resolves a SABnzbd category name to its configured
// directory, mirroring importer.Service.resolveCategoryPath semantics: an
// explicit Dir wins; otherwise the Default category maps to DefaultCategoryDir
// and any other category maps to its own name.
func resolveCategoryDir(cfg *config.Config, category string) string {
	if len(cfg.SABnzbd.Categories) == 0 {
		if strings.EqualFold(category, config.DefaultCategoryName) {
			return config.DefaultCategoryDir
		}
		return category
	}

	for _, cat := range cfg.SABnzbd.Categories {
		if strings.EqualFold(cat.Name, category) {
			if cat.Dir != "" {
				return cat.Dir
			}
			if strings.EqualFold(category, config.DefaultCategoryName) {
				return config.DefaultCategoryDir
			}
			return category
		}
	}

	return category
}

// buildExcludedCategoryPrefixes returns the set of mount-relative, lower-cased,
// forward-slash path prefixes (<CompleteDir>/<categoryDir>) for every category
// listed in health.excluded_categories. Files whose mount-relative path falls
// under one of these prefixes are skipped by the library-sync discovery pass.
func buildExcludedCategoryPrefixes(cfg *config.Config) []string {
	if cfg == nil {
		return nil
	}

	names := cfg.GetHealthExcludedCategories()
	if len(names) == 0 {
		return nil
	}

	completeDir := strings.Trim(filepath.ToSlash(cfg.SABnzbd.CompleteDir), "/")

	prefixes := make([]string, 0, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		dir := strings.Trim(filepath.ToSlash(resolveCategoryDir(cfg, name)), "/")
		if dir == "" {
			continue
		}
		prefix := dir
		if completeDir != "" {
			prefix = completeDir + "/" + dir
		}
		prefixes = append(prefixes, strings.ToLower(prefix))
	}
	return prefixes
}

// pathHasExcludedPrefix reports whether mountRelPath equals, or is nested under,
// any of the given (already lower-cased) prefixes. Matching is on full path
// segments so "complete/tv" does not match "complete/tv-extras/...".
func pathHasExcludedPrefix(mountRelPath string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return false
	}
	p := strings.ToLower(strings.Trim(filepath.ToSlash(mountRelPath), "/"))
	for _, prefix := range prefixes {
		if p == prefix || strings.HasPrefix(p, prefix+"/") {
			return true
		}
	}
	return false
}

// buildProtectedImportDirs returns the set of clean absolute paths under
// ImportDir that must never be deleted by removeEmptyDirectories. This
// covers ImportDir itself, the optional CompleteDir level, and every
// configured SABnzbd category dir (plus the default category when no
// categories are configured). Without this protection, transiently-empty
// category folders get pruned between imports and arrs (Radarr v6+) raise
// a permanent RemotePathMappingCheck health error.
func buildProtectedImportDirs(cfg *config.Config) map[string]struct{} {
	protected := map[string]struct{}{}
	if cfg == nil || cfg.Import.ImportDir == nil || *cfg.Import.ImportDir == "" {
		return protected
	}

	importDir := filepath.Clean(*cfg.Import.ImportDir)
	protected[importDir] = struct{}{}

	base := importDir
	if cfg.SABnzbd.CompleteDir != "" {
		base = filepath.Join(importDir, cfg.SABnzbd.CompleteDir)
		protected[filepath.Clean(base)] = struct{}{}
	}

	addCategory := func(dir string) {
		if dir == "" {
			return
		}
		full := filepath.Clean(filepath.Join(base, dir))
		protected[full] = struct{}{}
		// Also protect every parent up to (but not including) the base level,
		// so nested category Dirs like "media/movies" keep their "media"
		// ancestor as well.
		for parent := filepath.Dir(full); parent != filepath.Clean(base) && parent != "." && parent != string(filepath.Separator); parent = filepath.Dir(parent) {
			protected[parent] = struct{}{}
		}
	}

	if len(cfg.SABnzbd.Categories) == 0 {
		addCategory(config.DefaultCategoryDir)
		return protected
	}

	for _, cat := range cfg.SABnzbd.Categories {
		dir := cat.Dir
		if dir == "" {
			if cat.Name == config.DefaultCategoryName {
				dir = config.DefaultCategoryDir
			} else {
				dir = cat.Name
			}
		}
		addCategory(dir)
	}
	return protected
}

// removeEmptyDirectories removes empty directories from the library, import, and metadata directories
func (lsw *LibrarySyncWorker) removeEmptyDirectories(ctx context.Context) (int, error) {
	cfg := lsw.configGetter()

	// Paths to scan for empty directories
	var scanPaths []string
	if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
		scanPaths = append(scanPaths, *cfg.Health.LibraryDir)
	}
	if cfg.Import.ImportDir != nil && *cfg.Import.ImportDir != "" {
		scanPaths = append(scanPaths, *cfg.Import.ImportDir)
	}
	if cfg.Metadata.RootPath != "" {
		scanPaths = append(scanPaths, cfg.Metadata.RootPath)
	}

	if len(scanPaths) == 0 {
		return 0, nil
	}

	slog.InfoContext(ctx, "Starting empty directory cleanup", "paths", scanPaths)

	// Helper function to get directory depth
	getDepth := func(path string) int {
		return strings.Count(path, string(filepath.Separator))
	}

	// Collect all subdirectories from all scan paths
	var dirs []string
	for _, root := range scanPaths {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err != nil {
				return nil // Continue on errors
			}

			// Add to list if it's a subdirectory (not the root itself)
			if d.IsDir() && path != root {
				dirs = append(dirs, path)
			}

			return nil
		})

		if err != nil {
			slog.ErrorContext(ctx, "Error during directory scan", "root", root, "error", err)
		}
	}

	if len(dirs) == 0 {
		return 0, nil
	}

	// Sort by depth (deepest first) to ensure we can remove nested empty folders
	sort.Slice(dirs, func(i, j int) bool {
		// If depths are equal, sort alphabetically for stability
		di, dj := getDepth(dirs[i]), getDepth(dirs[j])
		if di == dj {
			return dirs[i] > dirs[j]
		}
		return di > dj
	})

	// Iteratively remove empty directories
	deletedCount := 0
	maxIterations := 5 // Reduced iterations, sorting by depth usually handles it in 1-2
	protectedImportDirs := buildProtectedImportDirs(cfg)
	for range maxIterations {
		removedThisIteration := 0

		for _, dir := range dirs {
			select {
			case <-ctx.Done():
				return deletedCount, ctx.Err()
			default:
			}

			// HARD SAFETY: Never delete protected paths
			cleanDir := filepath.Clean(dir)
			cleanMount := ""
			if cfg.MountPath != "" {
				cleanMount = filepath.Clean(cfg.MountPath)
			}
			cleanLibDir := ""
			if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
				cleanLibDir = filepath.Clean(*cfg.Health.LibraryDir)
			}

			if cleanDir == cleanMount || cleanDir == cleanLibDir || cleanDir == "/" || cleanDir == "." {
				continue
			}
			if _, ok := protectedImportDirs[cleanDir]; ok {
				continue
			}

			// Try to remove the directory (will fail if not empty)
			if err := os.Remove(dir); err != nil {
				continue
			}

			slog.DebugContext(ctx, "Removed empty directory", "path", dir)
			removedThisIteration++
			deletedCount++
		}

		// If no directories were removed in this pass, no more empty ones exist
		if removedThisIteration == 0 {
			break
		}
	}

	slog.InfoContext(ctx, "Empty directory cleanup completed",
		"deleted_count", deletedCount)

	return deletedCount, nil
}

// syncMetadataOnly performs a simplified sync for NONE import strategy
// It only synchronizes database records with metadata files, skipping all
// library directory scanning and cleanup operations. If dryRun is true,
// it will count what would be changed without actually modifying the database,
// and return a DryRunResult. If dryRun is false, it performs the sync normally
// and returns nil.
func (lsw *LibrarySyncWorker) syncMetadataOnly(ctx context.Context, startTime time.Time, dryRun bool) *DryRunResult {
	cfg := lsw.configGetter()
	slog.InfoContext(ctx, "Starting metadata-only sync")

	// Initialize progress tracking
	defer lsw.initializeProgressTracking(startTime)()

	// Get all metadata files from filesystem
	// OPTIMIZATION: This still loads all metadata paths into memory.
	// Ideally we would stream this too, but for now let's optimize the DB side.
	metadataFiles, err := lsw.getAllMetadataFiles(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "Failed to get metadata files", "error", err)
		}
		return nil
	}

	// Update total files count
	lsw.progressMu.Lock()
	lsw.progress.TotalFiles = len(metadataFiles)
	lsw.progressMu.Unlock()

	// Get all health check paths from database
	dbRecords, err := lsw.healthRepo.GetAllHealthCheckRecords(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get automatic health check paths from database", "error", err)
		return nil
	}

	// Build lookup maps for efficient searching
	syncMaps := lsw.buildSyncMaps(metadataFiles, dbRecords)
	metaFileSet := syncMaps.metaFileSet
	dbPathSet := syncMaps.dbPathSet

	// Find files to add (in filesystem but not in database)
	var filesToAdd []database.AutomaticHealthCheckRecord
	var filesToAddMu sync.Mutex

	concurrency := cfg.GetLibrarySyncConcurrency()

	// Create a worker pool for parallel metadata reading
	p := pool.New().WithMaxGoroutines(concurrency)

	for mountRelativePath := range metaFileSet {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Capture loop variable for goroutine
		path := mountRelativePath

		p.Go(func() {
			// Check if needs to be added
			if _, exists := dbPathSet[path]; !exists {
				// For NONE strategy, library path is always nil
				// since files are accessed directly via mount
				record, err := lsw.processMetadataForSync(ctx, path, nil)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to read metadata",
						"mount_relative_path", path,
						"error", err)

					// Register as corrupted so HealthWorker can pick it up and trigger repair
					regErr := lsw.healthRepo.RegisterCorruptedFile(ctx, path, nil, err.Error())
					if regErr != nil {
						slog.ErrorContext(ctx, "Failed to register corrupted file", "path", path, "error", regErr)
					}
					return
				}

				if record != nil {
					filesToAddMu.Lock()
					filesToAdd = append(filesToAdd, *record)
					filesToAddMu.Unlock()
				}
			}

			if lsw.progress != nil {
				lsw.progress.ProcessedFiles.Add(1)
			}
		})
	}

	// Wait for all workers to complete
	p.Wait()

	// Find files to delete (in database but not in filesystem)
	// Pass nil for filesInUse since metadata-only sync doesn't check library usage
	filesToDelete := lsw.findFilesToDelete(ctx, dbRecords, metaFileSet, nil)

	// Perform batch operations
	dbCounts := lsw.syncDatabaseRecords(ctx, filesToAdd, filesToDelete, dryRun)

	// Return dry run results or record sync results
	if dryRun {
		wouldCleanup := cfg.Health.CleanupOrphanedMetadata != nil && *cfg.Health.CleanupOrphanedMetadata
		return &DryRunResult{
			OrphanedMetadataCount:  0, // No orphaned metadata in NONE strategy
			OrphanedLibraryFiles:   0, // No library files in NONE strategy
			DatabaseRecordsToClean: dbCounts.deleted,
			WouldCleanup:           wouldCleanup,
		}
	}

	// Record sync results (no cleanup operations for metadata-only sync)
	cleanup := cleanupCounts{
		metadataDeleted:     0,
		libraryFilesDeleted: 0,
		libraryDirsDeleted:  0,
	}
	lsw.recordSyncResult(ctx, startTime, dbCounts, cleanup, len(metadataFiles), len(dbRecords))
	return nil
}
