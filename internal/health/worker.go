package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/sourcegraph/conc/pool"
	"golang.org/x/sync/singleflight"
)

// ARRsRepairService abstracts the ARR repair operations needed by HealthWorker.
type ARRsRepairService interface {
	TriggerFileRescan(ctx context.Context, pathForRescan string, relativePath string, metadataStr *string) error
	DiscoverFileMetadata(ctx context.Context, filePath, relativePath, nzbName, libraryPath string) (*model.WebhookMetadata, error)
}

// WorkerStatus represents the current status of the health worker
type WorkerStatus string

const (
	WorkerStatusStopped  WorkerStatus = "stopped"
	WorkerStatusStarting WorkerStatus = "starting"
	WorkerStatusRunning  WorkerStatus = "running"
	WorkerStatusStopping WorkerStatus = "stopping"
)

// WorkerStats represents statistics about the health worker
type WorkerStats struct {
	Status                 WorkerStatus `json:"status"`
	LastRunTime            *time.Time   `json:"last_run_time,omitempty"`
	NextRunTime            *time.Time   `json:"next_run_time,omitempty"`
	TotalRunsCompleted     int64        `json:"total_runs_completed"`
	TotalFilesChecked      int64        `json:"total_files_checked"`
	TotalFilesHealthy      int64        `json:"total_files_healthy"`
	TotalFilesCorrupted    int64        `json:"total_files_corrupted"`
	CurrentRunStartTime    *time.Time   `json:"current_run_start_time,omitempty"`
	CurrentRunFilesChecked int          `json:"current_run_files_checked"`
	LastError              *string      `json:"last_error,omitempty"`
	ErrorCount             int64        `json:"error_count"`
}

// activeCheck is one in-flight health check. Registrations are compared by pointer so a
// releasing owner never removes an entry another owner has since registered for the same path.
type activeCheck struct {
	cancel   context.CancelFunc
	progress *CheckProgress
}

// HealthWorker manages continuous health monitoring and manual check requests
// Par2RepairEnqueuer queues a file for background PAR2 repair (implemented by
// par2repair.Service).
type Par2RepairEnqueuer interface {
	Enqueue(ctx context.Context, filePath string, failingSegmentID string)
}

// SetPar2RepairEnqueuer wires the PAR2 repair queue into the degraded-verdict
// path. Call during boot, before the worker starts.
func (hw *HealthWorker) SetPar2RepairEnqueuer(re Par2RepairEnqueuer) {
	hw.par2Repair = re
}

type HealthWorker struct {
	healthChecker       *HealthChecker
	healthRepo          *database.HealthRepository
	metadataService     *metadata.MetadataService
	arrsService         ARRsRepairService
	importerService     importer.ImportService
	configGetter        config.ConfigGetter
	progressBroadcaster *progress.ProgressBroadcaster // optional, may be nil
	par2Repair          Par2RepairEnqueuer            // optional, may be nil

	// Worker state
	status   WorkerStatus
	running  bool
	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex

	// Kept off mu so the cycle can clear it while Stop is blocked in wg.Wait().
	cycleRunning atomic.Bool

	// Active checks tracking for cancellation
	activeChecks   map[string]*activeCheck // filePath -> in-flight check
	activeChecksMu sync.RWMutex

	// directCheckTimeout bounds a single manual recheck. Overridable so tests
	// can exercise the timeout path without waiting out the real budget.
	directCheckTimeout time.Duration

	// Statistics
	stats   WorkerStats
	statsMu sync.RWMutex

	// Singleflight for metadata discovery
	discoverySF singleflight.Group
}

// NewHealthWorker creates a new health worker
func NewHealthWorker(
	healthChecker *HealthChecker,
	healthRepo *database.HealthRepository,
	metadataService *metadata.MetadataService,
	arrsService ARRsRepairService,
	importerService importer.ImportService,
	configGetter config.ConfigGetter,
	broadcaster *progress.ProgressBroadcaster,
) *HealthWorker {
	return &HealthWorker{
		healthChecker:       healthChecker,
		healthRepo:          healthRepo,
		metadataService:     metadataService,
		arrsService:         arrsService,
		importerService:     importerService,
		configGetter:        configGetter,
		progressBroadcaster: broadcaster,
		status:              WorkerStatusStopped,
		stopChan:            make(chan struct{}),
		activeChecks:        make(map[string]*activeCheck),
		directCheckTimeout:  defaultDirectCheckTimeout,
		// par2Repair is wired post-construction via SetPar2RepairEnqueuer.
		stats: WorkerStats{
			Status: WorkerStatusStopped,
		},
	}
}

// broadcastHealthChanged notifies SSE subscribers that health state has changed.
func (hw *HealthWorker) broadcastHealthChanged() {
	if hw.progressBroadcaster != nil {
		hw.progressBroadcaster.BroadcastHealthChanged()
	}
}

// Start begins the health worker service
func (hw *HealthWorker) Start(ctx context.Context) error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if hw.running {
		return fmt.Errorf("health worker already running")
	}

	if !hw.configGetter().GetHealthEnabled() {
		slog.WarnContext(ctx, "Health worker is disabled via configuration, not starting")
		return nil
	}

	hw.running = true
	hw.status = WorkerStatusStarting
	hw.updateStats(func(s *WorkerStats) {
		s.Status = WorkerStatusStarting
		s.LastError = nil
	})

	// Initialize health system - reset any files stuck in 'checking' status
	if err := hw.healthRepo.ResetFileAllChecking(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to reset checking files during initialization", "error", err)
		// Don't fail startup for this - just log and continue
	}

	// Reset pending files that exhausted retries so they can be rechecked
	if err := hw.healthRepo.ResetStalePendingFiles(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to reset stale pending files during initialization", "error", err)
		// Don't fail startup for this - just log and continue
	}

	// Start the main worker goroutine
	hw.stopChan = make(chan struct{})
	hw.wg.Go(func() {
		hw.run(ctx)
	})

	hw.status = WorkerStatusRunning
	hw.updateStats(func(s *WorkerStats) {
		s.Status = WorkerStatusRunning
	})

	slog.InfoContext(ctx, "Health worker started successfully", "check_interval", hw.getCheckInterval(), "max_concurrent_jobs", hw.getMaxConcurrentJobs())
	return nil
}

// Stop gracefully stops the health worker
func (hw *HealthWorker) Stop(ctx context.Context) error {
	hw.mu.Lock()
	if !hw.running {
		hw.mu.Unlock()
		return fmt.Errorf("health worker not running")
	}

	hw.running = false
	hw.status = WorkerStatusStopping
	stopCh := hw.stopChan
	hw.mu.Unlock()

	hw.updateStats(func(s *WorkerStats) {
		s.Status = WorkerStatusStopping
	})

	slog.InfoContext(ctx, "Stopping health worker...")
	close(stopCh)

	// mu must not be held here: the worker goroutine can still need it to finish,
	// which would deadlock Stop against the very goroutine it is waiting on.
	hw.wg.Wait()

	hw.mu.Lock()
	hw.status = WorkerStatusStopped
	hw.mu.Unlock()

	hw.updateStats(func(s *WorkerStats) {
		s.Status = WorkerStatusStopped
		s.CurrentRunStartTime = nil
		s.CurrentRunFilesChecked = 0
	})

	slog.InfoContext(ctx, "Health worker stopped")
	return nil
}

// IsRunning returns whether the health worker is currently running
func (hw *HealthWorker) IsRunning() bool {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	return hw.running
}

// GetStats returns current worker statistics
func (hw *HealthWorker) GetStats() WorkerStats {
	hw.statsMu.RLock()
	defer hw.statsMu.RUnlock()

	return hw.stats
}

// GetCheckProgress returns an independent snapshot of an active check.
func (hw *HealthWorker) GetCheckProgress(filePath string) *CheckProgress {
	hw.activeChecksMu.RLock()
	defer hw.activeChecksMu.RUnlock()
	entry := hw.activeChecks[filePath]
	if entry == nil || entry.progress == nil {
		return nil
	}
	snapshot := *entry.progress
	return &snapshot
}

// progressReporter captures owners so a cancelled or replaced check cannot
// update a newer registration for the same file.
func (hw *HealthWorker) progressReporter(entries map[string]*activeCheck) func(string, CheckProgress) {
	return func(path string, progress CheckProgress) {
		hw.activeChecksMu.Lock()
		defer hw.activeChecksMu.Unlock()
		if entry := entries[path]; entry != nil && hw.activeChecks[path] == entry {
			entry.progress = &progress
		}
	}
}

// CancelHealthCheck cancels an active health check for the specified file
func (hw *HealthWorker) CancelHealthCheck(ctx context.Context, filePath string) error {
	hw.activeChecksMu.Lock()
	defer hw.activeChecksMu.Unlock()

	check, exists := hw.activeChecks[filePath]
	if !exists {
		return fmt.Errorf("no active health check found for file: %s", filePath)
	}

	// Cancel the context
	check.cancel()

	// Remove from active checks
	delete(hw.activeChecks, filePath)

	// Update file status to pending to allow retry
	err := hw.healthRepo.UpdateFileHealth(ctx, filePath, database.HealthStatusPending, nil, nil, nil, false)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to update file status after cancellation", "file_path", filePath, "error", err)
		return fmt.Errorf("failed to update file status after cancellation: %w", err)
	}

	hw.broadcastHealthChanged()
	slog.InfoContext(ctx, "Health check cancelled", "file_path", filePath)
	return nil
}

// IsCheckActive returns whether a health check is currently active for the specified file
func (hw *HealthWorker) IsCheckActive(filePath string) bool {
	hw.activeChecksMu.RLock()
	defer hw.activeChecksMu.RUnlock()

	_, exists := hw.activeChecks[filePath]
	return exists
}

// run is the main worker loop
func (hw *HealthWorker) run(ctx context.Context) {
	ticker := time.NewTicker(hw.getCheckInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Health worker stopped by context")
			return
		case <-hw.stopChan:
			slog.InfoContext(ctx, "Health worker stopped by stop signal")
			return
		case <-ticker.C:
			// Check if a cycle is already running
			if hw.cycleRunning.Load() {
				slog.DebugContext(ctx, "Skipping health check cycle - previous cycle still running")
				continue
			}

			if err := hw.safeRunHealthCheckCycle(ctx); err != nil {
				slog.ErrorContext(ctx, "Health check cycle failed", "error", err)
				hw.updateStats(func(s *WorkerStats) {
					s.ErrorCount++
					errMsg := err.Error()
					s.LastError = &errMsg
				})
			}
		}
	}
}

// safeRunHealthCheckCycle runs a health check cycle with panic recovery
func (hw *HealthWorker) safeRunHealthCheckCycle(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in health check cycle: %v", r)
			slog.ErrorContext(ctx, "Panic in health check cycle", "panic", r)
		}
	}()
	return hw.runHealthCheckCycle(ctx)
}

// AddToHealthCheck adds a file to the health check list with pending status
func (hw *HealthWorker) AddToHealthCheck(ctx context.Context, filePath string, sourceNzb *string) error {
	// Check if file already exists in health database
	existingHealth, err := hw.healthRepo.GetFileHealth(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to check existing health record: %w", err)
	}

	// If file doesn't exist in health database, add it with a short jitter (0–5 min) so
	// newly imported files are checked soon without all firing at the exact same instant.
	if existingHealth == nil {
		scheduledAt := calculateInitialCheckForNewFile()
		err = hw.healthRepo.UpdateFileHealthScheduled(ctx,
			filePath,
			database.HealthStatusPending,
			nil,
			sourceNzb,
			nil,
			false,
			scheduledAt,
		)
		if err != nil {
			return fmt.Errorf("failed to add file to health database: %w", err)
		}

		slog.InfoContext(ctx, "Added file to health check list", "file_path", filePath, "scheduled_at", scheduledAt)
	} else {
		// File already exists, just reset to pending status if not already pending
		if existingHealth.Status != database.HealthStatusPending {
			err = hw.healthRepo.UpdateFileHealth(ctx,
				filePath,
				database.HealthStatusPending,
				nil,
				sourceNzb,
				nil,
				false,
			)
			if err != nil {
				return fmt.Errorf("failed to update file status to pending: %w", err)
			}
			slog.InfoContext(ctx, "Reset file status to pending for health check", "file_path", filePath)
		}
	}

	return nil
}

// defaultDirectCheckTimeout bounds a single manual recheck.
const defaultDirectCheckTimeout = 10 * time.Minute

// PerformBackgroundCheck starts a manual recheck of filePath in the
// background. currentStatus must be the file's HealthStatus as it was BEFORE
// the caller transitioned the row to Checking (e.g. via SetFileCheckingByID);
// it is threaded straight into CheckOptions.CurrentStatus so the automatic
// Pending-only content-verification gate (see shouldVerifyContent) still
// works on this path — re-reading the row inside performDirectCheck would
// always observe Checking and make the gate unreachable. verifyContentOverride,
// when non-nil, forces (true) or disables (false) content verification for
// this check regardless of currentStatus or the configured default.
func (hw *HealthWorker) PerformBackgroundCheck(ctx context.Context, filePath string, currentStatus database.HealthStatus, verifyContentOverride *bool) error {
	if !hw.IsRunning() {
		return fmt.Errorf("health worker is not running")
	}

	timeout := hw.directCheckTimeout
	if timeout <= 0 {
		timeout = defaultDirectCheckTimeout
	}

	// Start health check in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		checkErr := hw.performDirectCheck(ctx, filePath, currentStatus, verifyContentOverride)
		if checkErr != nil {
			if errors.Is(checkErr, context.Canceled) {
				// CancelHealthCheck already parked the record on Pending on the
				// user's behalf; re-writing it here would undo that decision.
				slog.InfoContext(ctx, "Background health check cancelled", "file_path", filePath)
				return
			}

			if errors.Is(checkErr, context.DeadlineExceeded) {
				slog.ErrorContext(ctx, "Background health check timed out", "file_path", filePath, "timeout", timeout)
			} else {
				slog.ErrorContext(ctx, "Background health check failed", "file_path", filePath, "error", checkErr)
			}

			// ctx is already expired on the timeout path, so the recovery write
			// needs a live context of its own.
			recoverCtx, recoverCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer recoverCancel()

			// Get current health record to preserve source NZB path
			fileHealth, getErr := hw.healthRepo.GetFileHealth(recoverCtx, filePath)
			var sourceNzb *string
			if getErr == nil && fileHealth != nil {
				sourceNzb = fileHealth.SourceNzbPath
			}

			// A check that never reached a verdict says nothing about the file:
			// restore the status captured before the row was set to 'checking'
			// rather than demoting a degraded/repair_triggered record to pending.
			restored := currentStatus
			if restored == database.HealthStatusChecking || restored == "" {
				restored = database.HealthStatusPending
			}

			errorMsg := checkErr.Error()
			updateErr := hw.healthRepo.UpdateFileHealth(recoverCtx, filePath, restored, &errorMsg, sourceNzb, nil, false)
			if updateErr != nil {
				slog.ErrorContext(ctx, "Failed to update status after failed check", "file_path", filePath, "error", updateErr)
			}
		}
	}()

	return nil
}

// inconclusiveRecheckDelay is how long a file waits after a health check that
// reached no verdict — a provider outage, timeouts, or a sweep truncated
// before every segment answered. Such a check produces no evidence, so the
// record is left exactly as it was and only its next check is pushed out; the
// delay keeps a provider-wide outage from turning into a tight recheck loop
// across the whole library.
const inconclusiveRecheckDelay = 30 * time.Minute

// prepareUpdateForResult decides what DB update and side effects are needed based on the check result.
func (hw *HealthWorker) prepareUpdateForResult(ctx context.Context, fh *database.FileHealth, event HealthEvent) (*database.HealthStatusUpdate, func() error) {
	update := &database.HealthStatusUpdate{
		FilePath: fh.FilePath,
	}

	var sideEffect func() error

	if event.Type == EventTypeFileRemoved {
		update.Skip = true
		sideEffect = func() error {
			slog.InfoContext(ctx, "File removed — health record already deleted, skipping bulk update", "file_path", fh.FilePath)
			return nil
		}
		return update, sideEffect
	}

	if event.Type == EventTypeFileHealthy {
		// File is now healthy
		releaseDate := fh.ReleaseDate
		if releaseDate == nil {
			releaseDate = &fh.CreatedAt
		}

		nextCheck := CalculateNextCheck(releaseDate.UTC(), time.Now().UTC())
		update.Type = database.UpdateTypeHealthy
		update.Status = database.HealthStatusHealthy
		update.ScheduledCheckAt = nextCheck
		// Normally nil, which clears the column as before. A content-verification
		// outcome is the one thing a healthy record still carries.
		update.ErrorDetails = event.Details

		sideEffect = func() error {
			slog.InfoContext(ctx, "File is healthy", "file_path", fh.FilePath)

			return hw.metadataService.UpdateFileStatus(fh.FilePath, metapb.FileStatus_FILE_STATUS_HEALTHY)
		}

		return update, sideEffect
	}

	// An inconclusive check says nothing about the file: the providers never
	// answered for at least one segment. Restore the status the record held
	// before the cycle set it to 'checking', push the next check out, and
	// leave both retry counters alone — an outage must not consume the retry
	// budget or escalate the file into repair (#861).
	if event.Type == EventTypeCheckInconclusive {
		status := fh.Status
		if status == database.HealthStatusChecking || status == "" {
			status = database.HealthStatusPending
		}
		nextCheck := time.Now().UTC().Add(inconclusiveRecheckDelay)

		update.Type = database.UpdateTypeInconclusive
		update.Status = status
		update.ScheduledCheckAt = nextCheck
		if event.Error != nil {
			text := event.Error.Error()
			update.ErrorMessage = &text
		}
		update.ErrorDetails = event.Details

		sideEffect = func() error {
			slog.WarnContext(ctx, "Health check inconclusive, rescheduling without counting a retry",
				"file_path", fh.FilePath,
				"status", status,
				"next_check", nextCheck,
				"error", event.Error)
			return nil
		}
		return update, sideEffect
	}

	// Handle Corrupted or CheckFailed
	var errorMsg *string
	if event.Error != nil {
		text := event.Error.Error()
		errorMsg = &text
	}
	update.ErrorMessage = errorMsg
	update.ErrorDetails = event.Details

	// Degraded verdict: the confirmed holes are within the padding caps, so
	// the file still plays (streaming zero-fills the gaps). Record it as
	// degraded — no repair trigger, no safety-folder move, periodic re-check —
	// unless a repair is already in flight, in which case the repair flow wins.
	if event.Classification != nil &&
		event.Classification.Verdict == holes.VerdictDegraded &&
		fh.Status != database.HealthStatusRepairTriggered {
		releaseDate := fh.ReleaseDate
		if releaseDate == nil {
			releaseDate = &fh.CreatedAt
		}
		nextCheck := CalculateNextCheck(releaseDate.UTC(), time.Now().UTC())

		update.Type = database.UpdateTypeDegraded
		update.Status = database.HealthStatusDegraded
		update.ScheduledCheckAt = nextCheck

		sideEffect = func() error {
			slog.InfoContext(ctx, "File degraded: missing segments are within padding caps, skipping ARR repair",
				"file_path", fh.FilePath,
				"total_missing", event.Classification.TotalMissing,
				"longest_run", event.Classification.LongestRun,
				"next_check", nextCheck)
			// Degraded damage is exactly what background PAR2 repair exists
			// for: attempt to restore the zero-filled bytes byte-exact. The
			// repair queue dedups; ARR replacement stays out of the picture.
			if hw.par2Repair != nil {
				hw.par2Repair.Enqueue(ctx, fh.FilePath, "")
			}
			return hw.metadataService.UpdateFileStatus(fh.FilePath, metapb.FileStatus_FILE_STATUS_DEGRADED)
		}
		return update, sideEffect
	}

	// When automatic repair is disabled, never trigger or re-trigger an Arr rescan: a file
	// that has exhausted its health-check retries (or is already in repair_triggered from a
	// time when repair was enabled) is finalized as corrupted for visibility, with no
	// metadata safety-folder move. Regular health-check retries below are left intact.
	// When delete-on-corruption is enabled, confirmed corruption is handled by removing
	// the file entirely instead of ever entering the repair flow below (this covers both
	// freshly-exhausted checks and files already sitting in repair_triggered).
	if hw.configGetter().GetHealthDeleteOnCorruption() {
		return hw.deleteCorruptedFile(ctx, fh)
	}

	repairEnabled := hw.configGetter().GetRepairEnabled()
	// par2Fallback: whether the file may fall back to PAR2 repair after the ARR
	// path yields nothing. Only valid while the file's metadata is still in
	// place — files already in repair_triggered may have had their metadata
	// moved to the safety folder, which a PAR2 repair cannot read.
	markCorruptedNoRepair := func(par2Fallback bool) (*database.HealthStatusUpdate, func() error) {
		update.Type = database.UpdateTypeCorrupted
		update.Status = database.HealthStatusCorrupted
		return update, func() error {
			slog.InfoContext(ctx, "File corrupted but ARR repair is disabled; marking corrupted without triggering repair",
				"file_path", fh.FilePath, "status", fh.Status)
			if par2Fallback {
				hw.enqueuePar2Fallback(ctx, fh.FilePath)
			}
			return nil
		}
	}

	switch fh.Status {
	case database.HealthStatusRepairTriggered:
		if !repairEnabled {
			return markCorruptedNoRepair(false)
		}
		if fh.RepairRetryCount >= hw.configGetter().GetMaxRepairRetries() {
			sideEffect = hw.markCorruptedRepairExhausted(ctx, fh, update, errorMsg)
		} else {
			// Calculate repair back-off
			interval := hw.configGetter().GetRepairInterval()
			if hw.configGetter().GetRepairExponentialBackoff() {
				// Exponential backoff: interval * 2^retry_count
				// e.g. 1h, 2h, 4h...
				multiplier := 1 << fh.RepairRetryCount
				interval = interval * time.Duration(multiplier)

				// Cap at max cooldown
				maxCoolDown := hw.configGetter().GetRepairMaxCoolDown()
				if interval > maxCoolDown {
					interval = maxCoolDown
				}
			}
			nextCheck := time.Now().UTC().Add(interval)

			update.Type = database.UpdateTypeRepairRetry
			update.Status = database.HealthStatusRepairTriggered
			update.ScheduledCheckAt = nextCheck

			sideEffect = func() error {
				slog.InfoContext(ctx, "Repair retry scheduled",
					"file_path", fh.FilePath,
					"repair_retry_count", fh.RepairRetryCount+1,
					"next_check", nextCheck)
				return nil
			}
		}

	default:
		// Regular health check phase
		if fh.RetryCount >= hw.configGetter().GetMaxRetries()-1 {
			// Repair budget exhausted: this title was already re-downloaded
			// max_repair_retries times (the counter survives webhook relinks and
			// re-import upserts by design). Triggering yet another rescan would
			// re-download the same broken release forever, so finalize as corrupted.
			if fh.RepairRetryCount >= hw.configGetter().GetMaxRepairRetries() {
				sideEffect = hw.markCorruptedRepairExhausted(ctx, fh, update, errorMsg)
				return update, sideEffect
			}

			// Repair disabled: finalize as corrupted instead of triggering an Arr
			// rescan — PAR2 repair, when enabled, is the only remaining option.
			if !repairEnabled {
				return markCorruptedNoRepair(true)
			}

			update.Type = database.UpdateTypeRepairTrigger
			update.Status = database.HealthStatusRepairTriggered

			update.ScheduledCheckAt = time.Now().UTC().Add(hw.configGetter().GetRepairInterval())

			sideEffect = func() error {
				slog.InfoContext(ctx, "Health check retries exhausted, triggering repair", "file_path", fh.FilePath)

				// Log failure against the indexer if known
				if fh.Indexer != nil && *fh.Indexer != "" && *fh.Indexer != database.IndexerUnknown {
					errMsg := "Retries exhausted"
					if errorMsg != nil {
						errMsg = *errorMsg
					}
					_ = hw.healthRepo.LogIndexerImport(ctx, *fh.Indexer, "failed", fmt.Sprintf("Health check failed (repair triggered): %s", errMsg), "")
				}

				outcome, err := hw.triggerFileRepair(ctx, fh, errorMsg, event.Details)
				applyRepairOutcome(update, outcome, err)
				if outcome == repairOutcomeCorrupted {
					// Nothing found in the ARRs (no instance configured, path
					// unmatched, or the trigger failed): the metadata was not
					// moved, so PAR2 repair gets its turn. A successful repair
					// flips the record back to healthy; a failed one confirms
					// the corrupted verdict written above.
					hw.enqueuePar2Fallback(ctx, fh.FilePath)
				}
				return nil
			}
		} else {
			// Increment health check retry count
			backoffMinutes := 15 * (1 << fh.RetryCount)
			nextCheck := time.Now().UTC().Add(time.Duration(backoffMinutes) * time.Minute)

			update.Type = database.UpdateTypeRetry
			update.Status = database.HealthStatusPending
			update.ScheduledCheckAt = nextCheck

			sideEffect = func() error {
				slog.InfoContext(ctx, "Health check retry scheduled",
					"file_path", fh.FilePath,
					"retry_count", fh.RetryCount+1,
					"next_check", nextCheck)
				return nil
			}
		}
	}

	return update, sideEffect
}

// markCorruptedRepairExhausted fills update with the terminal corrupted state for a file
// whose repair budget is spent after a failed health check, and returns the side effect
// (hide metadata in the safety folder, log the failure against the indexer).
func (hw *HealthWorker) markCorruptedRepairExhausted(ctx context.Context, fh *database.FileHealth, update *database.HealthStatusUpdate, errorMsg *string) func() error {
	update.Type = database.UpdateTypeCorrupted
	update.Status = database.HealthStatusCorrupted

	return func() error {
		slog.ErrorContext(ctx, "File permanently marked as corrupted after repair retries exhausted", "file_path", fh.FilePath)

		// Ensure metadata is hidden in the safety folder
		hw.moveMetadataToSafetyFolder(ctx, fh)

		// Log failure against the indexer if known
		if fh.Indexer != nil && *fh.Indexer != "" && *fh.Indexer != database.IndexerUnknown {
			errMsg := "Permanently corrupted"
			if errorMsg != nil {
				errMsg = *errorMsg
			}
			_ = hw.healthRepo.LogIndexerImport(ctx, *fh.Indexer, "failed", fmt.Sprintf("Health check permanently corrupted: %s", errMsg), "")
		}

		return nil
	}
}

// deleteCorruptedFile removes a confirmed-corrupted file's metadata (and optional source
// NZB), cleans up now-empty parent directories in both the metadata store and the physical
// library tree, and deletes the health record — used instead of triggering an Arr repair
// when health.corruption_action is set to "delete".
func (hw *HealthWorker) deleteCorruptedFile(ctx context.Context, fh *database.FileHealth) (*database.HealthStatusUpdate, func() error) {
	update := &database.HealthStatusUpdate{FilePath: fh.FilePath, Skip: true}
	return update, func() error {
		cfg := hw.configGetter()
		var physicalPath, rootPath string
		if fh.LibraryPath != nil {
			physicalPath = *fh.LibraryPath
			rootPath = cfg.MountPath
			if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
				rootPath = *cfg.Health.LibraryDir
			}
		}
		if err := hw.metadataService.DeleteCorruptedFile(ctx, fh.FilePath, physicalPath, rootPath); err != nil {
			slog.ErrorContext(ctx, "Failed to delete corrupted file", "file_path", fh.FilePath, "error", err)
			return err
		}
		if err := hw.healthRepo.DeleteHealthRecord(ctx, fh.FilePath); err != nil {
			slog.ErrorContext(ctx, "Failed to delete health record for deleted corrupted file", "file_path", fh.FilePath, "error", err)
		}
		slog.WarnContext(ctx, "Deleted corrupted file instead of triggering repair", "file_path", fh.FilePath)
		return nil
	}
}

// prepareRepairNotificationUpdate builds the update and side effect for a file already in
// repair_triggered state. It re-triggers ARR directly without calling CheckFile, since the
// metadata has already been moved to the corrupted folder.
func (hw *HealthWorker) prepareRepairNotificationUpdate(ctx context.Context, fh *database.FileHealth) (*database.HealthStatusUpdate, func() error) {
	if hw.configGetter().GetHealthDeleteOnCorruption() {
		return hw.deleteCorruptedFile(ctx, fh)
	}

	// Default to the existing diagnosis so a successful re-trigger (which never
	// sets these itself) doesn't null out the reason the file was flagged in
	// the first place; applyRepairOutcome combines onto this if repair fails.
	update := &database.HealthStatusUpdate{
		FilePath:     fh.FilePath,
		ErrorMessage: fh.LastError,
		ErrorDetails: fh.ErrorDetails,
	}

	if fh.RepairRetryCount >= hw.configGetter().GetMaxRepairRetries() {
		// Retries exhausted — give up and mark corrupted. Deliberately no metadata
		// move here: unlike the failed-check path, this sweep has not re-validated
		// the file's content, and a re-import may have just landed a good copy. The
		// user can recheck from the Health page; a failed check will then hide it.
		update.Type = database.UpdateTypeCorrupted
		update.Status = database.HealthStatusCorrupted
		sideEffect := func() error {
			slog.ErrorContext(ctx, "File permanently marked as corrupted after repair retries exhausted",
				"file_path", fh.FilePath,
				"repair_retry_count", fh.RepairRetryCount)
			return nil
		}
		return update, sideEffect
	}

	// Calculate repair back-off
	interval := hw.configGetter().GetRepairInterval()
	if hw.configGetter().GetRepairExponentialBackoff() {
		// Exponential backoff
		multiplier := 1 << fh.RepairRetryCount
		interval = interval * time.Duration(multiplier)

		// Cap at max cooldown
		maxCoolDown := hw.configGetter().GetRepairMaxCoolDown()
		if interval > maxCoolDown {
			interval = maxCoolDown
		}
	}
	nextCheck := time.Now().UTC().Add(interval)

	// Re-trigger ARR and increment repair_retry_count.
	update.Type = database.UpdateTypeRepairRetry
	update.Status = database.HealthStatusRepairTriggered
	update.ScheduledCheckAt = nextCheck

	sideEffect := func() error {
		outcome, err := hw.retriggerFileRepair(ctx, fh)
		applyRepairOutcome(update, outcome, err)
		return nil
	}

	return update, sideEffect
}

// beginBatchChecks registers a cancellable context per file of a cycle batch, so a file the
// scheduled cycle is checking can be cancelled through CancelHealthCheck exactly like a manual
// check-now. Each context is an independent child: cancelling one file neither aborts the shared
// cross-file sweep nor disturbs the other files — the cancelled file's result is discarded
// instead. Returns the per-file contexts, a release func, and an owner-bound progress callback.
func (hw *HealthWorker) beginBatchChecks(ctx context.Context, filePaths []string) (map[string]context.Context, func(), func(string, CheckProgress)) {
	fileCtxs := make(map[string]context.Context, len(filePaths))
	entries := make(map[string]*activeCheck, len(filePaths))

	hw.activeChecksMu.Lock()
	for _, filePath := range filePaths {
		if _, exists := hw.activeChecks[filePath]; exists {
			continue
		}
		fileCtx, cancel := context.WithCancel(ctx)
		entry := &activeCheck{cancel: cancel}
		fileCtxs[filePath] = fileCtx
		entries[filePath] = entry
		hw.activeChecks[filePath] = entry
	}
	hw.activeChecksMu.Unlock()

	return fileCtxs, func() {
		hw.activeChecksMu.Lock()
		for filePath, entry := range entries {
			if hw.activeChecks[filePath] == entry {
				delete(hw.activeChecks, filePath)
			}
		}
		hw.activeChecksMu.Unlock()

		for _, entry := range entries {
			entry.cancel()
		}
	}, hw.progressReporter(entries)
}

// performDirectCheck performs a health check on a single file using the HealthChecker.
// currentStatus is the file's HealthStatus as observed by the caller before the row was
// transitioned to Checking; see PerformBackgroundCheck for why this cannot be re-derived
// by re-reading the row here.
func (hw *HealthWorker) performDirectCheck(ctx context.Context, filePath string, currentStatus database.HealthStatus, verifyContentOverride *bool) error {
	// Create cancellable context for this check
	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Track active check
	entry := &activeCheck{cancel: cancel}
	hw.activeChecksMu.Lock()
	hw.activeChecks[filePath] = entry
	hw.activeChecksMu.Unlock()

	// Ensure cleanup on exit
	defer func() {
		hw.activeChecksMu.Lock()
		if hw.activeChecks[filePath] == entry {
			delete(hw.activeChecks, filePath)
		}
		hw.activeChecksMu.Unlock()
	}()

	// Check if already cancelled
	select {
	case <-checkCtx.Done():
		return checkCtx.Err()
	default:
	}

	// Get current file state first to determine check options
	fh, err := hw.healthRepo.GetFileHealth(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to get file health state: %w", err)
	}
	if fh == nil {
		return fmt.Errorf("file health record not found: %s", filePath)
	}

	opts := CheckOptions{CurrentStatus: currentStatus, VerifyContentOverride: verifyContentOverride, OnProgress: hw.progressReporter(map[string]*activeCheck{filePath: entry})}
	// Delegate to HealthChecker
	event := hw.healthChecker.CheckFile(checkCtx, filePath, opts)

	// Check if cancelled during check
	select {
	case <-checkCtx.Done():
		return checkCtx.Err()
	default:
	}

	// The row is deliberately 'checking' while a manual recheck runs, but an
	// inconclusive result says nothing about the file and must restore the state
	// captured before that transition. Keep definitive-result handling unchanged:
	// those paths can perform repair/delete side effects and need a broader
	// ownership protocol before they can safely use a guarded write.
	decisionHealth := *fh
	if event.Type == EventTypeCheckInconclusive {
		decisionHealth.Status = currentStatus
	}
	updatePtr, sideEffect := hw.prepareUpdateForResult(ctx, &decisionHealth, event)

	if event.Type == EventTypeCheckInconclusive {
		// A webhook or re-import may have changed the row while the NNTP sweep
		// was running. Only restore the prior state if this check still owns the
		// transient 'checking' row; otherwise the concurrent decision wins.
		checkingStatus := database.HealthStatusChecking
		updatePtr.ExpectedStatus = &checkingStatus
	}
	if sideEffect != nil {
		if err := sideEffect(); err != nil {
			slog.ErrorContext(ctx, "Side effect failed in direct check", "file_path", filePath, "error", err)
		}
	}

	if !updatePtr.Skip {
		if err := hw.healthRepo.UpdateHealthStatusBulk(ctx, []database.HealthStatusUpdate{*updatePtr}); err != nil {
			return fmt.Errorf("failed to update health status: %w", err)
		}
		hw.broadcastHealthChanged()
	}

	// Notify rclone VFS about the status change
	hw.healthChecker.notifyRcloneVFS(filePath, event)

	// Update stats
	hw.updateStats(func(s *WorkerStats) {
		s.TotalFilesChecked++
		switch event.Type {
		case EventTypeFileHealthy:
			s.TotalFilesHealthy++
		case EventTypeFileCorrupted:
			s.TotalFilesCorrupted++
		}
	})

	return nil
}

// updateStats safely updates worker statistics
// runHealthCheckCycle runs a single cycle of health checks
func (hw *HealthWorker) runHealthCheckCycle(ctx context.Context) error {
	// Set the cycle running flag
	hw.cycleRunning.Store(true)

	// Ensure we clear the flag when done
	defer hw.cycleRunning.Store(false)

	now := time.Now().UTC()
	hw.updateStats(func(s *WorkerStats) {
		s.CurrentRunStartTime = &now
		s.CurrentRunFilesChecked = 0
	})

	maxJobs := hw.getMaxConcurrentJobs()
	cfg := hw.configGetter()
	strategy := string(cfg.Import.ImportStrategy)
	libraryDir := ""
	if cfg.Health.LibraryDir != nil {
		libraryDir = *cfg.Health.LibraryDir
	}

	// Get files due for checking (ordered by scheduled_check_at)
	// New logic: Only check files with library_path (imported) unless strategy is NONE
	// The fetch limit is intentionally larger than maxJobs: segment availability
	// for the whole batch is verified in one cross-file StatMany sweep
	// (CheckFilesBatch), so NNTP throughput no longer depends on per-file job
	// concurrency. maxJobs still bounds the per-file result handling below
	// (repair side effects, ARR API calls).
	unhealthyFiles, err := hw.healthRepo.GetUnhealthyFiles(ctx, cfg.GetCheckBatchSize(), strategy, libraryDir, hw.configGetter().GetMaxRetries())
	if err != nil {
		return fmt.Errorf("failed to get unhealthy files: %w", err)
	}

	// Get files that need repair notifications. Only when automatic repair is enabled —
	// when it is disabled, corrupt files are left in the corrupted state and we never
	// re-trigger an Arr rescan.
	var repairFiles []*database.FileHealth
	if cfg.GetRepairEnabled() {
		repairFiles, err = hw.healthRepo.GetFilesForRepairNotification(ctx, maxJobs)
		if err != nil {
			return fmt.Errorf("failed to get files for repair notification: %w", err)
		}
	}

	totalFiles := len(unhealthyFiles) + len(repairFiles)
	if totalFiles == 0 {
		hw.updateStats(func(s *WorkerStats) {
			s.CurrentRunStartTime = nil
			s.CurrentRunFilesChecked = 0
			s.TotalRunsCompleted++
			s.LastRunTime = &now
			nextRun := now.Add(hw.getCheckInterval())
			s.NextRunTime = &nextRun
		})
		return nil
	}

	slog.InfoContext(ctx, "Found files to process",
		"health_check_files", len(unhealthyFiles),
		"repair_notification_files", len(repairFiles),
		"total", totalFiles,
		"max_concurrent_jobs", maxJobs)

	// Transition the whole batch to 'checking' in one write instead of one UPDATE per
	// file: under SQLite's single writer N per-file transitions would serialize against
	// each other and the final bulk status write. Crash recovery is unchanged
	// (ResetFileAllChecking at startup re-arms stranded 'checking' rows).
	checkingPaths := make([]string, len(unhealthyFiles))
	for i, fh := range unhealthyFiles {
		checkingPaths[i] = fh.FilePath
	}
	if err := hw.healthRepo.SetFilesCheckingBulk(ctx, checkingPaths); err != nil {
		slog.ErrorContext(ctx, "Failed to bulk-set files to checking", "count", len(checkingPaths), "error", err)
	}

	// Make every file of this batch cancellable for as long as it is 'checking'.
	fileCtxs, releaseChecks, reportProgress := hw.beginBatchChecks(ctx, checkingPaths)
	defer releaseChecks()
	// Let an already-open Health page discover checking rows and start polling.
	hw.broadcastHealthChanged()

	// Process files in parallel with bounded concurrency
	p := pool.New().WithMaxGoroutines(maxJobs)
	var results []database.HealthStatusUpdate
	var resultsMu sync.Mutex

	// The regular-check writes are based on the record being 'checking' (set just above);
	// guard them on that status so a concurrent webhook relink / re-import / manual
	// recheck that lands mid-check is not silently clobbered by a stale check result.
	checkingStatus := database.HealthStatusChecking

	// Phase A: proactive metadata discovery (may hit ARR APIs — bounded by
	// maxJobs), then verify segment availability for the whole batch in one
	// cross-file StatMany sweep. events is index-aligned with unhealthyFiles.
	discover := pool.New().WithMaxGoroutines(maxJobs)
	for _, fileHealth := range unhealthyFiles {
		fh := fileHealth // Capture for closure
		discover.Go(func() {
			slog.InfoContext(ctx, "Checking unhealthy file", "file_path", fh.FilePath)
			fh.Metadata = hw.ensureMetadata(ctx, fh)
		})
	}
	discover.Wait()

	paths := make([]string, len(unhealthyFiles))
	statuses := make([]database.HealthStatus, len(unhealthyFiles))
	for i, fh := range unhealthyFiles {
		paths[i] = fh.FilePath
		statuses[i] = fh.Status
	}
	events := hw.healthChecker.CheckFilesBatch(ctx, paths, statuses, CheckOptions{OnProgress: reportProgress})

	// Phase B: per-file result handling (repair side effects, ARR API calls,
	// VFS notifications), bounded by maxJobs.
	for i, fileHealth := range unhealthyFiles {
		fh := fileHealth // Capture for closure
		event := events[i]
		fileCtx := fileCtxs[fh.FilePath]
		p.Go(func() {
			// A cancelled check must not land its result: CancelHealthCheck already put the
			// record back to pending, and the repair side effects below would undo that.
			if fileCtx != nil && fileCtx.Err() != nil {
				slog.InfoContext(ctx, "Discarding result of cancelled health check", "file_path", fh.FilePath)
				return
			}

			updatePtr, sideEffect := hw.prepareUpdateForResult(ctx, fh, event)
			updatePtr.ExpectedStatus = &checkingStatus
			if sideEffect != nil {
				if err := sideEffect(); err != nil {
					slog.ErrorContext(ctx, "Failed to execute side effect for health result", "file_path", fh.FilePath, "error", err)
				}
			}

			resultsMu.Lock()
			results = append(results, *updatePtr)
			resultsMu.Unlock()

			// Notify VFS
			hw.healthChecker.notifyRcloneVFS(fh.FilePath, event)

			// Update cycle progress stats
			hw.updateStats(func(s *WorkerStats) {
				s.CurrentRunFilesChecked++
				s.TotalFilesChecked++
				switch event.Type {
				case EventTypeFileHealthy:
					s.TotalFilesHealthy++
				case EventTypeFileCorrupted:
					s.TotalFilesCorrupted++
				}
			})
		})
	}

	repairStatus := database.HealthStatusRepairTriggered
	for _, fileHealth := range repairFiles {
		fh := fileHealth // Capture for closure
		p.Go(func() {
			// Re-fetch the record to ensure we have the absolute latest library_path
			// (in case a webhook updated it while we were waiting in the cycle)
			latest, err := hw.healthRepo.GetFileHealth(ctx, fh.FilePath)
			if err == nil && latest != nil {
				fh = latest
			}

			// If a concurrent actor moved the record out of repair_triggered between the
			// cycle's read and now (e.g. a Download webhook relinked the fresh copy to
			// pending, or a manual recheck/delete fired), leave it alone. Re-triggering
			// would clobber that rescue and re-enter the repair loop — and fire the ARR
			// re-trigger / metadata-move side effects against a record that no longer needs them.
			if fh.Status != database.HealthStatusRepairTriggered {
				slog.InfoContext(ctx, "Skipping repair notification — record left repair_triggered concurrently",
					"file_path", fh.FilePath, "status", fh.Status)
				return
			}

			slog.InfoContext(ctx, "Re-triggering repair for file", "file_path", fh.FilePath)

			updatePtr, sideEffect := hw.prepareRepairNotificationUpdate(ctx, fh)
			updatePtr.ExpectedStatus = &repairStatus

			if sideEffect != nil {
				if err := sideEffect(); err != nil {
					slog.ErrorContext(ctx, "Failed to execute side effect for repair notification", "file_path", fh.FilePath, "error", err)
				}
			}

			resultsMu.Lock()
			results = append(results, *updatePtr)
			resultsMu.Unlock()

			// Update cycle progress stats
			hw.updateStats(func(s *WorkerStats) {
				s.CurrentRunFilesChecked++
				s.TotalFilesChecked++
			})
		})
	}

	// Wait for all files to complete processing
	p.Wait()

	// Build list of protected directories (categories and complete dir)
	cfg = hw.configGetter()
	protected := []string{"complete", "corrupted_metadata"} // Protect 'complete' and safety folder
	if cfg.SABnzbd.CompleteDir != "" {
		protected = append(protected, filepath.Base(cfg.SABnzbd.CompleteDir))
	}
	for _, cat := range cfg.SABnzbd.Categories {
		protected = append(protected, cat.Name)
		if cat.Dir != "" {
			protected = append(protected, cat.Dir)
		}
	}

	// Clean up empty directories in metadata (e.g. from moved/imported files)
	if err := hw.metadataService.CleanupEmptyDirectories("", protected); err != nil {
		slog.WarnContext(ctx, "Failed to cleanup empty directories in metadata", "error", err)
	}

	if days := cfg.Health.CorruptedRetentionDays; days != nil && *days > 0 {
		if _, err := hw.metadataService.PruneCorrupted(ctx, time.Duration(*days)*24*time.Hour); err != nil {
			slog.WarnContext(ctx, "Failed to prune corrupted metadata safety copies", "error", err)
		}
	}

	// Perform bulk database update
	if len(results) > 0 {
		if err := hw.healthRepo.UpdateHealthStatusBulk(ctx, results); err != nil {
			slog.ErrorContext(ctx, "Failed to perform bulk health status update", "error", err)
		}
		hw.broadcastHealthChanged()
	}

	// Update final stats
	hw.updateStats(func(s *WorkerStats) {
		s.CurrentRunStartTime = nil
		s.CurrentRunFilesChecked = 0
		s.TotalRunsCompleted++
		s.LastRunTime = &now
		nextRun := now.Add(hw.getCheckInterval())
		s.NextRunTime = &nextRun
	})

	slog.InfoContext(ctx, "Health check cycle completed",
		"health_check_files", len(unhealthyFiles),
		"repair_notification_files", len(repairFiles),
		"total_files", totalFiles,
		"duration", time.Since(now))

	return nil
}

// updateStats safely updates worker statistics
func (hw *HealthWorker) updateStats(updateFunc func(*WorkerStats)) {
	hw.statsMu.Lock()
	defer hw.statsMu.Unlock()
	updateFunc(&hw.stats)
}

// Helper methods to get dynamic health config values
func (hw *HealthWorker) getCheckInterval() time.Duration {
	return hw.configGetter().GetCheckInterval()
}

func (hw *HealthWorker) getMaxConcurrentJobs() int {
	return hw.configGetter().GetMaxConcurrentJobs()
}

// repairOutcome describes the result of a repair trigger attempt.
type repairOutcome int

const (
	repairOutcomeTriggered   repairOutcome = iota // ARR accepted the repair; metadata moved to corrupted folder
	repairOutcomeCorrupted                        // ARR failed with a generic error; mark file corrupted
	repairOutcomeDeleted                          // Health record and/or metadata were deleted (zombie)
	repairOutcomeRegenerated                      // Metadata was successfully regenerated from NZB
	repairOutcomeDeferred                         // ARR temporarily unreachable; keep repair-pending, do not condemn
)

// applyRepairOutcome maps a repairOutcome to the corresponding fields on the HealthStatusUpdate.
func applyRepairOutcome(update *database.HealthStatusUpdate, outcome repairOutcome, err error) {
	switch outcome {
	case repairOutcomeTriggered:
		update.Type = database.UpdateTypeRepairRetry
	case repairOutcomeDeleted:
		update.Skip = true
	case repairOutcomeRegenerated:
		update.Type = database.UpdateTypeHealthy
		update.Status = database.HealthStatusHealthy
		update.ScheduledCheckAt = time.Now().UTC().Add(24 * time.Hour) // Re-check tomorrow
	case repairOutcomeCorrupted:
		update.Type = database.UpdateTypeCorrupted
		update.Status = database.HealthStatusCorrupted
		if err != nil {
			// Preserve the original health-check diagnosis (e.g. "missing N
			// segments") instead of replacing it with the repair-trigger
			// failure — losing the root cause makes the UI show only an
			// unrelated ARR error with no indication of why the file was
			// flagged corrupted in the first place.
			repairErr := err.Error()
			if update.ErrorMessage != nil && *update.ErrorMessage != "" {
				combined := fmt.Sprintf("%s (repair failed: %s)", *update.ErrorMessage, repairErr)
				update.ErrorMessage = &combined
			} else {
				update.ErrorMessage = &repairErr
			}
		}
	case repairOutcomeDeferred:
		// The ARR was only temporarily unreachable. Keep the file in repair_triggered
		// WITHOUT incrementing repair_retry_count (UpdateTypeRepairTrigger does not bump
		// the budget) and without condemning it, so the next repair cycle retries and the
		// file self-heals once the ARR returns. The caller's pre-set ScheduledCheckAt
		// (repair back-off) is preserved.
		update.Type = database.UpdateTypeRepairTrigger
		update.Status = database.HealthStatusRepairTriggered
	}
}

// resolvePathForRescan determines the absolute path that ARR should rescan for a given file.
// It checks LibraryPath first, then LibraryDir, then ImportDir, and falls back to MountPath.
func (hw *HealthWorker) resolvePathForRescan(item *database.FileHealth) string {
	if p, ok := item.EffectiveLibraryPath(); ok {
		return p
	}
	cfg := hw.configGetter()
	if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
		return utils.JoinAbsPath(*cfg.Health.LibraryDir, item.FilePath)
	}
	if cfg.Import.ImportDir != nil && *cfg.Import.ImportDir != "" {
		return utils.JoinAbsPath(*cfg.Import.ImportDir, item.FilePath)
	}
	return utils.JoinAbsPath(cfg.MountPath, item.FilePath)
}

// cleanupZombieRecord deletes the health record and associated metadata for a file that is
// no longer tracked by ARR (zombie or orphan). Errors are logged but not returned because
// cleanup is best-effort.
func (hw *HealthWorker) cleanupZombieRecord(ctx context.Context, item *database.FileHealth) {
	// Delete library symlink/STRM if it exists (only for ARR-relinked records; an
	// import-time placeholder points at the virtual mount, not a real library file).
	if p, ok := item.EffectiveLibraryPath(); ok {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			slog.ErrorContext(ctx, "Failed to delete library file during zombie cleanup",
				"path", p, "error", err)
		}
	}

	if delErr := hw.healthRepo.DeleteHealthRecord(ctx, item.FilePath); delErr != nil {
		slog.ErrorContext(ctx, "Failed to delete health record during cleanup", "file_path", item.FilePath, "error", delErr)
	}

	cfg := hw.configGetter()
	relativePath := strings.TrimPrefix(item.FilePath, cfg.MountPath)
	relativePath = strings.TrimPrefix(relativePath, "/")

	if delMetaErr := hw.metadataService.DeleteFileMetadata(ctx, relativePath); delMetaErr != nil {
		slog.ErrorContext(ctx, "Failed to delete metadata during cleanup", "file_path", item.FilePath, "error", delMetaErr)
	} else {
		hw.NotifyRcloneVFS(item.FilePath)
	}
}

// triggerFileRepair handles the business logic for triggering repair of a corrupted file.
// It contacts ARR APIs and moves metadata, but does NOT write health status to the DB directly.
// Callers must apply the returned outcome to the HealthStatusUpdate before the bulk DB write.
func (hw *HealthWorker) triggerFileRepair(ctx context.Context, item *database.FileHealth, errorMsg *string, errorDetails *string) (repairOutcome, error) {
	filePath := item.FilePath

	// Check if file metadata still exists. If not, the file is gone (likely upgraded/deleted by Sonarr already)
	// and this health record is a zombie.
	var metadataErr error
	{
		meta, err := hw.metadataService.ReadFileMetadata(filePath)
		if err != nil {
			slog.WarnContext(ctx, "Metadata file unreadable during repair trigger (likely physical corruption) — proceeding with repair anyway",
				"file_path", filePath, "error", err)
			metadataErr = err
			// Proceed with repair attempt: physical corruption is why we're here
		} else if meta == nil {
			slog.WarnContext(ctx, "File metadata missing during repair trigger - file likely deleted/upgraded externally. Cleaning up zombie record.",
				"file_path", filePath)

			if delErr := hw.healthRepo.DeleteHealthRecord(ctx, filePath); delErr != nil {
				slog.ErrorContext(ctx, "Failed to delete zombie health record", "error", delErr)
				return repairOutcomeDeleted, delErr
			}
			return repairOutcomeDeleted, nil
		}
	}

	// SPECIAL CASE: If metadata is corrupted AND we don't have a library path,
	// we try to regenerate the metadata first before triggering a full ARR repair.
	if metadataErr != nil && !item.IsImported() {
		slog.InfoContext(ctx, "Metadata corrupted and no library path found - attempting regeneration from NZB", "file_path", filePath)
		if regenErr := hw.importerService.RegenerateMetadata(ctx, filePath); regenErr == nil {
			slog.InfoContext(ctx, "Successfully regenerated metadata for corrupted item", "file_path", filePath)
			return repairOutcomeRegenerated, nil
		} else {
			slog.WarnContext(ctx, "Regeneration attempt failed, proceeding with normal repair", "file_path", filePath, "error", regenErr)
		}
	}

	slog.InfoContext(ctx, "Triggering file repair using direct ARR API approach", "file_path", filePath)

	pathForRescan := hw.resolvePathForRescan(item)
	metadataStr := hw.ensureMetadata(ctx, item)

	err := hw.arrsService.TriggerFileRescan(ctx, pathForRescan, filePath, metadataStr)
	if err != nil {
		// ErrEpisodeAlreadySatisfied is an ID-based confirmation from the ARR (Smart Repair
		// Guard) that this title was upgraded/replaced by a *different* file, so the AltMount
		// copy is genuinely redundant and safe to remove.
		if errors.Is(err, arrs.ErrEpisodeAlreadySatisfied) {
			slog.WarnContext(ctx, "File replaced by a different file in ARR, removing redundant copy from AltMount",
				"file_path", filePath, "arr_error", err)
			hw.cleanupZombieRecord(ctx, item)
			return repairOutcomeDeleted, nil
		}

		// ErrPathMatchFailed only means AltMount could not match its rescan path against the
		// ARR library/queue. The ARR routinely renames and reorganizes imported files (symlink
		// libraries, custom naming), so a path miss is NOT a reliable orphan signal: treating
		// it as one deletes the user's library symlink and the underlying virtual file. Leave
		// the file in place — genuine orphans are removed safely by the library-sync orphan
		// pass (two consecutive misses + ratio guard + import-history check). Mark corrupted so
		// it follows the normal repair retry/back-off instead of being destroyed.
		if errors.Is(err, arrs.ErrPathMatchFailed) {
			slog.WarnContext(ctx, "ARR rescan path did not match library; leaving file in place (library-sync handles real orphans)",
				"file_path", filePath, "path_for_rescan", pathForRescan, "arr_error", err)
			return repairOutcomeCorrupted, err
		}

		// A temporarily unreachable ARR (network/transport error or 5xx) must NOT condemn
		// the file. Defer: keep it repair-pending (no retry-count bump, no metadata move)
		// so it self-heals on the next cycle once the ARR returns.
		if arrs.IsTemporarilyUnreachable(err) {
			slog.WarnContext(ctx, "ARR temporarily unreachable during repair trigger; deferring (file kept repair-pending, not condemned)",
				"file_path", filePath, "path_for_rescan", pathForRescan, "arr_error", err)
			return repairOutcomeDeferred, err
		}

		slog.ErrorContext(ctx, "Failed to trigger ARR rescan",
			"file_path", filePath,
			"path_for_rescan", pathForRescan,
			"error", err)
		return repairOutcomeCorrupted, err
	}

	// ARR rescan was triggered successfully.
	slog.InfoContext(ctx, "Successfully triggered ARR rescan for file repair",
		"file_path", filePath,
		"path_for_rescan", pathForRescan)

	// Move the metadata file to the corrupted folder so FUSE/WebDAV stops showing it.
	// CRITICAL: We only do this if the file has already been imported (has a LibraryPath).
	// If it hasn't been imported yet, we keep it visible so ARR can see the "Missing File"
	// or "Empty Folder" and report its own warning, which helps the repair cycle.
	if item.IsImported() {
		hw.moveMetadataToSafetyFolder(ctx, item)
	} else {
		slog.InfoContext(ctx, "Skipping metadata move for corrupted item - file not yet imported by ARR", "file_path", filePath)
	}

	return repairOutcomeTriggered, nil
}

// retriggerFileRepair re-triggers the ARR rescan for a file already in repair_triggered state.
// Unlike triggerFileRepair it does NOT write to the DB.
// Callers must apply the returned outcome to the HealthStatusUpdate before the bulk DB write.
func (hw *HealthWorker) retriggerFileRepair(ctx context.Context, item *database.FileHealth) (repairOutcome, error) {
	filePath := item.FilePath

	pathForRescan := hw.resolvePathForRescan(item)
	metadataStr := hw.ensureMetadata(ctx, item)

	slog.InfoContext(ctx, "Re-triggering ARR rescan for file in repair", "file_path", filePath, "path_for_rescan", pathForRescan)

	err := hw.arrsService.TriggerFileRescan(ctx, pathForRescan, filePath, metadataStr)
	if err != nil {
		// See triggerFileRepair: only an ID-confirmed replacement (ErrEpisodeAlreadySatisfied)
		// justifies deleting the AltMount copy. ErrPathMatchFailed is an ambiguous path miss
		// (e.g. an ARR-renamed library) and must not delete the user's library file.
		if errors.Is(err, arrs.ErrEpisodeAlreadySatisfied) {
			slog.WarnContext(ctx, "File replaced by a different file in ARR, removing redundant copy from AltMount",
				"file_path", filePath, "arr_error", err)
			hw.cleanupZombieRecord(ctx, item)
			return repairOutcomeDeleted, nil
		}

		if errors.Is(err, arrs.ErrPathMatchFailed) {
			slog.WarnContext(ctx, "ARR rescan path did not match library on re-trigger; leaving file in place (library-sync handles real orphans)",
				"file_path", filePath, "path_for_rescan", pathForRescan, "arr_error", err)
			return repairOutcomeCorrupted, err
		}

		// Temporarily unreachable ARR: defer instead of condemning. Note the metadata move
		// happens only on the success path below, so a deferred outcome leaves the file
		// visible and untouched until the ARR comes back.
		if arrs.IsTemporarilyUnreachable(err) {
			slog.WarnContext(ctx, "ARR temporarily unreachable during repair re-trigger; deferring (file kept repair-pending, not condemned)",
				"file_path", filePath, "path_for_rescan", pathForRescan, "arr_error", err)
			return repairOutcomeDeferred, err
		}

		slog.ErrorContext(ctx, "Failed to re-trigger ARR rescan", "file_path", filePath, "error", err)
		return repairOutcomeCorrupted, err
	}

	// ARR rescan re-triggered successfully — only now move the metadata to the safety
	// folder (if the file has been imported) so a deferred/failed outcome above never
	// hides a file that was not actually condemned.
	hw.moveMetadataToSafetyFolder(ctx, item)

	slog.InfoContext(ctx, "Successfully re-triggered ARR rescan", "file_path", filePath)
	return repairOutcomeTriggered, nil
}

// enqueuePar2Fallback queues a PAR2 repair for a file whose ARR repair came up
// empty — nothing found in the ARRs (no instance tracks the file, or none is
// configured) or ARR repair is disabled. Gated by par2_repair.arr_first
// (default on) so the fallback can be switched off; a no-op while PAR2 repair
// itself is disabled. Only called on paths where the file's metadata is still
// in place — a repair cannot read metadata that an earlier successful ARR
// trigger moved to the safety folder.
func (hw *HealthWorker) enqueuePar2Fallback(ctx context.Context, filePath string) {
	cfg := hw.configGetter().Par2Repair
	if hw.par2Repair == nil || cfg.Enabled == nil || !*cfg.Enabled || !cfg.EffectiveArrFirst() {
		return
	}
	slog.InfoContext(ctx, "Nothing found in ARR repair, falling back to PAR2 repair", "file_path", filePath)
	hw.par2Repair.Enqueue(ctx, filePath, "")
}

func (hw *HealthWorker) ensureMetadata(ctx context.Context, item *database.FileHealth) *string {
	needsDiscovery := false
	if item.Metadata == nil || *item.Metadata == "" {
		needsDiscovery = true
	} else {
		var dbMeta model.WebhookMetadata
		if err := json.Unmarshal([]byte(*item.Metadata), &dbMeta); err == nil {
			if dbMeta.Series != nil && len(dbMeta.Episodes) == 0 {
				needsDiscovery = true
			}
		}
	}

	if !needsDiscovery {
		return item.Metadata
	}

	// Build a singleflight key.
	// If NZB name is known, use that. Otherwise use parent directory of the file path.
	nzbName := ""
	if item.SourceNzbPath != nil {
		nzbName = filepath.Base(*item.SourceNzbPath)
	}
	sfKey := nzbName
	if sfKey == "" {
		sfKey = filepath.Dir(item.FilePath)
	}
	if sfKey == "" || sfKey == "." {
		sfKey = item.FilePath
	}

	res, err, _ := hw.discoverySF.Do(sfKey, func() (interface{}, error) {
		// Re-read from DB to verify if another concurrent worker finished discovery first
		latest, err := hw.healthRepo.GetFileHealth(ctx, item.FilePath)
		if err == nil && latest != nil && latest.Metadata != nil && *latest.Metadata != "" {
			var dbMeta model.WebhookMetadata
			if err := json.Unmarshal([]byte(*latest.Metadata), &dbMeta); err == nil {
				if dbMeta.Series == nil || len(dbMeta.Episodes) > 0 {
					return latest.Metadata, nil
				}
			}
		}

		slog.DebugContext(ctx, "Missing metadata or episode IDs, attempting discovery during health check", "file_path", item.FilePath, "sf_key", sfKey)
		relativePath := strings.TrimPrefix(item.FilePath, "complete/")
		libPath, _ := item.EffectiveLibraryPath()

		metadata, err := hw.arrsService.DiscoverFileMetadata(ctx, item.FilePath, relativePath, nzbName, libPath)
		if err == nil && metadata != nil {
			metaBytes, err := json.Marshal(metadata)
			if err == nil {
				str := string(metaBytes)
				slog.InfoContext(ctx, "Successfully discovered metadata during health check",
					"file_path", item.FilePath,
					"instance", metadata.InstanceName)
				if err := hw.healthRepo.UpdateFileMetadata(ctx, item.ID, metaBytes); err != nil {
					slog.ErrorContext(ctx, "Failed to save discovered metadata", "error", err)
				}
				return &str, nil
			}
		}
		return (*string)(nil), err
	})

	if err == nil && res != nil {
		if strPtr, ok := res.(*string); ok {
			return strPtr
		}
	}

	return nil
}

func (hw *HealthWorker) moveMetadataToSafetyFolder(ctx context.Context, item *database.FileHealth) {
	if !item.IsImported() {
		return
	}
	cfg := hw.configGetter()
	relativePath := strings.TrimPrefix(item.FilePath, cfg.MountPath)
	relativePath = strings.TrimPrefix(relativePath, "/")
	slog.InfoContext(ctx, "Moving metadata file for corrupted item to safety folder to trigger replacement", "file_path", item.FilePath)
	if moveErr := hw.metadataService.MoveToCorrupted(ctx, relativePath); moveErr != nil {
		slog.WarnContext(ctx, "Failed to move corrupted metadata file", "error", moveErr)
	} else {
		hw.NotifyRcloneVFS(item.FilePath)
	}
}

// NotifyRcloneVFS notifies rclone VFS to forget and refresh the directory containing filePath.
func (hw *HealthWorker) NotifyRcloneVFS(filePath string) {
	if hw != nil && hw.healthChecker != nil {
		hw.healthChecker.NotifyRcloneVFS(filePath)
	}
}

// NotifyRcloneVFSDirs notifies rclone VFS to forget and refresh the specified directories.
func (hw *HealthWorker) NotifyRcloneVFSDirs(dirs []string) {
	if hw != nil && hw.healthChecker != nil {
		hw.healthChecker.NotifyRcloneVFSDirs(dirs)
	}
}
