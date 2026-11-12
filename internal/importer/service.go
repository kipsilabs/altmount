package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/arrs"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/contentverify"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/importer/filesystem"
	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
	"github.com/kipsilabs/altmount/internal/importer/postprocessor"
	"github.com/kipsilabs/altmount/internal/importer/queue"
	"github.com/kipsilabs/altmount/internal/importer/scanner"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
	"github.com/kipsilabs/altmount/internal/importer/validation"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/nzbfile"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/kipsilabs/altmount/internal/sabnzbd"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/kipsilabs/altmount/pkg/rclonecli"
)

// ServiceConfig holds configuration for the NZB import service
type ServiceConfig struct {
	Workers int // Number of parallel queue workers (default: 2)
}

// Type aliases from scanner package for backward compatibility
type (
	ScanStatus      = scanner.ScanStatus
	ScanInfo        = scanner.ScanInfo
	ImportJobStatus = scanner.ImportJobStatus
	ImportInfo      = scanner.ImportInfo
)

// Re-export scanner status constants for backward compatibility
const (
	ScanStatusIdle      = scanner.ScanStatusIdle
	ScanStatusScanning  = scanner.ScanStatusScanning
	ScanStatusCanceling = scanner.ScanStatusCanceling
	ImportStatusIdle    = scanner.ImportStatusIdle
	ImportStatusRunning = scanner.ImportStatusRunning
)

// queueAdapterForScanner adapts database repository for scanner.QueueAdder interface
type queueAdapterForScanner struct {
	repo            *database.QueueRepository
	metadataService *metadata.MetadataService
	calcFileSize    func(string) (int64, error)
}

func (a *queueAdapterForScanner) AddToQueue(ctx context.Context, filePath string, relativePath *string, metadata *string) error {
	// Calculate file size before adding to queue
	var fileSize *int64
	if size, err := a.calcFileSize(filePath); err == nil {
		fileSize = &size
	}

	item := &database.ImportQueueItem{
		DownloadID:   nil, // Generated later in service if needed
		NzbPath:      filePath,
		RelativePath: relativePath,
		Priority:     database.QueuePriorityNormal,
		Status:       database.QueueStatusPending,
		RetryCount:   0,
		MaxRetries:   3,
		FileSize:     fileSize,
		Metadata:     metadata,
		CreatedAt:    time.Now(),
	}

	return a.repo.AddToQueue(ctx, item)
}

func (a *queueAdapterForScanner) IsFileInQueue(ctx context.Context, filePath string) bool {
	inQueue, _ := a.repo.IsFileInQueue(ctx, filePath)
	return inQueue
}

func (a *queueAdapterForScanner) IsFileProcessed(filePath string, scanRoot string) bool {
	return isFileAlreadyProcessed(a.metadataService, filePath, scanRoot)
}

// batchQueueAdapterForImporter adapts database repository for scanner.BatchQueueAdder and
// scanner.MigrationRecorder interfaces.
type batchQueueAdapterForImporter struct {
	repo          *database.QueueRepository
	migrationRepo *database.ImportMigrationRepository
}

func (a *batchQueueAdapterForImporter) AddBatchToQueue(ctx context.Context, items []*database.ImportQueueItem) error {
	return a.repo.AddBatchToQueue(ctx, items)
}

func (a *batchQueueAdapterForImporter) UpsertMigration(ctx context.Context, source, externalID, relativePath string) (int64, error) {
	return a.migrationRepo.Upsert(ctx, &database.ImportMigration{
		Source:       source,
		ExternalID:   externalID,
		RelativePath: relativePath,
		Status:       database.ImportMigrationStatusPending,
	})
}

func (a *batchQueueAdapterForImporter) IsMigrationCompleted(ctx context.Context, source, externalID string) (bool, error) {
	row, err := a.migrationRepo.LookupByExternalID(ctx, source, externalID)
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, nil
	}
	return row.Status == database.ImportMigrationStatusImported || row.Status == database.ImportMigrationStatusSymlinksMigrated, nil
}

func (a *batchQueueAdapterForImporter) LinkQueueItemID(ctx context.Context, source string, externalIDs []string, queueItemID int64) error {
	return a.migrationRepo.LinkQueueItemID(ctx, source, externalIDs, queueItemID)
}

// isFileAlreadyProcessed checks if a file has already been processed by checking metadata
func isFileAlreadyProcessed(metadataService *metadata.MetadataService, filePath string, scanRoot string) bool {
	// Calculate virtual path
	virtualPath := filepath.Dir(filePath)
	if scanRoot != "" {
		rel, err := filepath.Rel(scanRoot, filePath)
		if err == nil {
			virtualPath = filepath.Dir(rel)
		}
	}

	// Normalize filename (remove .nzb extension)
	fileName := filepath.Base(filePath)
	baseName := nzbtrim.TrimNzbExtension(fileName)

	// Check if a directory exists with the release name
	releaseDir := filepath.Join(virtualPath, baseName)
	if metadataService.DirectoryExists(releaseDir) {
		return true
	}

	// Also check if any file exists that starts with the release name in that directory
	if files, err := metadataService.ListDirectory(virtualPath); err == nil {
		for _, f := range files {
			if strings.HasPrefix(f, baseName) {
				return true
			}
		}
	}

	return false
}

// SetRepairEnqueuer wires the PAR2 repair queue into the import processor so
// degraded imports can queue a repair. Call during boot.
func (s *Service) SetRepairEnqueuer(re RepairEnqueuer) {
	if s.processor != nil {
		s.processor.SetRepairEnqueuer(re)
	}
}

// ResumeWaitingRepair returns an import parked pending a PAR2 repair to the
// pending queue, so the normal worker retries it — this time with the repaired
// articles available from the local patch store.
func (s *Service) ResumeWaitingRepair(ctx context.Context, nzbPath string) error {
	item, err := s.database.Repository.GetQueueItemByNzbPath(ctx, nzbPath)
	if err != nil {
		return err
	}
	if item == nil || item.Status != database.QueueStatusWaitingRepair {
		return nil
	}
	if err := s.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusPending, nil); err != nil {
		return err
	}
	if s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
	return nil
}

// FailWaitingRepair fails a parked import whose repair proved impossible,
// which is the outcome it would have had before deferral existed.
func (s *Service) FailWaitingRepair(ctx context.Context, nzbPath string, reason string) error {
	item, err := s.database.Repository.GetQueueItemByNzbPath(ctx, nzbPath)
	if err != nil {
		return err
	}
	if item == nil || item.Status != database.QueueStatusWaitingRepair {
		return nil
	}
	msg := "PAR2 repair could not rebuild the missing articles: " + reason
	if err := s.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusFailed, &msg); err != nil {
		return err
	}
	if s.broadcaster != nil {
		s.broadcaster.NotifyComplete(int(item.ID), string(database.QueueStatusFailed))
		s.broadcaster.BroadcastQueueChanged()
	}
	return nil
}

// SetPatchIndex wires the PAR2 patch store into the import availability sweep.
func (s *Service) SetPatchIndex(idx validation.PatchIndex) {
	if s.processor != nil {
		s.processor.SetPatchIndex(idx)
	}
}

// SetSegmentStore wires the streaming segment store into the importer so
// articles fetched at import are already cached when playback starts.
func (s *Service) SetSegmentStore(resolve func() usenet.SegmentStore) {
	if s.processor != nil {
		s.processor.SetSegmentStore(resolve)
	}
}

// GetPostProcessor returns the post-processor coordinator
func (s *Service) GetPostProcessor() *postprocessor.Coordinator {
	return s.postProcessor
}

// Service provides NZB import functionality with manual directory scanning and queue-based processing
type Service struct {
	config          ServiceConfig
	database        *database.DB              // Database for processing queue
	metadataService *metadata.MetadataService // Metadata service for file processing
	processor       *Processor
	postProcessor   *postprocessor.Coordinator    // Post-import processing coordinator
	queueManager    *queue.Manager                // Queue worker management
	dirScanner      *scanner.DirectoryScanner     // Manual directory scanning
	watcher         *scanner.Watcher              // Directory watcher for automated imports
	nzbdavImporter  *scanner.NzbDavImporter       // NZBDav database imports
	rcloneClient    rclonecli.RcloneRcClient      // Optional rclone client for VFS notifications
	configGetter    config.ConfigGetter           // Config getter for dynamic configuration access
	sabnzbdClient   *sabnzbd.SABnzbdClient        // SABnzbd client for fallback
	arrsService     *arrs.Service                 // ARRs service for triggering scans
	healthRepo      *database.HealthRepository    // Health repository for updating health status
	broadcaster     *progress.ProgressBroadcaster // WebSocket progress broadcaster
	userRepo        *database.UserRepository      // User repository for API key lookup
	poolManager     pool.Manager                  // Pool manager — used to push admission caps on config change
	contentVerifyFS contentverify.Opener          // Optional: real NzbFilesystem, set post-construction to break the init-order cycle with nzbfilesystem
	log             *slog.Logger

	// Runtime state
	mu      sync.RWMutex
	running bool
	paused  bool
	ctx     context.Context
	cancel  context.CancelFunc

	// Cancellation tracking for processing items
	cancelFuncs map[int64]context.CancelFunc
	cancelMu    sync.RWMutex

	// categoryPathCache memoizes buildCategoryPath results; cleared on config reload.
	categoryPathCache sync.Map

	// writtenPathsCache stores the metadata paths written during ProcessItem so that
	// HandleFailure can clean them up without changing the ItemProcessor interface.
	// Keys are item.ID (int64), values are []string.
	writtenPathsCache sync.Map
	grabbedIndexers   sync.Map
}

// NewService creates a new NZB import service with manual scanning and queue processing capabilities
func NewService(config ServiceConfig, metadataService *metadata.MetadataService, database *database.DB, poolManager pool.Manager, rcloneClient rclonecli.RcloneRcClient, configGetter config.ConfigGetter, healthRepo *database.HealthRepository, broadcaster *progress.ProgressBroadcaster, userRepo *database.UserRepository) (*Service, error) {
	// Set defaults
	if config.Workers == 0 {
		config.Workers = 2
	}

	// Wire store ref counter so metadata delete/create paths maintain reference counts.
	if database != nil && database.StoreRefRepo != nil {
		metadataService.SetStoreRefCounter(database.StoreRefRepo)
	}

	// Create processor with poolManager for dynamic pool access
	processor := NewProcessor(metadataService, poolManager, broadcaster, configGetter, nil)

	ctx, cancel := context.WithCancel(context.Background())

	// Create post-processor coordinator
	postProc := postprocessor.NewCoordinator(postprocessor.Config{
		ConfigGetter:    configGetter,
		MetadataService: metadataService,
		RcloneClient:    rcloneClient,
		HealthRepo:      healthRepo,
		UserRepo:        userRepo,
	})

	service := &Service{
		config:          config,
		metadataService: metadataService,
		database:        database,
		processor:       processor,
		postProcessor:   postProc,
		rcloneClient:    rcloneClient,
		configGetter:    configGetter,
		healthRepo:      healthRepo,
		sabnzbdClient:   sabnzbd.NewSABnzbdClient(httpclient.NewForExternal(configGetter().Network, httpclient.LongTimeout)),
		broadcaster:     broadcaster,
		userRepo:        userRepo,
		poolManager:     poolManager,
		log:             slog.Default().With("component", "importer-service"),
		ctx:             ctx,
		cancel:          cancel,
		cancelFuncs:     make(map[int64]context.CancelFunc),
		paused:          false,
	}

	// Push the initial admission cap to the pool so imports are gated from the
	// start. Zero keeps the controller disabled, matching prior behaviour.
	if poolManager != nil && configGetter != nil {
		if cfg := configGetter(); cfg != nil {
			poolManager.SetAdmissionCap(cfg.GetMaxConcurrentImports())
		}
	}

	// Set recorder for processor
	processor.SetRecorder(service)

	// Create scanner adapter for directory scanning
	scannerAdapter := &queueAdapterForScanner{
		repo:            database.Repository,
		metadataService: metadataService,
		calcFileSize:    service.CalculateFileSizeOnly,
	}
	service.dirScanner = scanner.NewDirectoryScanner(scannerAdapter)

	// Create adapter for NZBDav imports
	importerAdapter := &batchQueueAdapterForImporter{
		repo:          database.Repository,
		migrationRepo: database.MigrationRepo,
	}
	service.nzbdavImporter = scanner.NewNzbDavImporter(importerAdapter, importerAdapter)

	// Create directory watcher (Service implements WatchQueueAdder)
	service.watcher = scanner.NewWatcher(service, configGetter)

	// Create queue manager (Service implements queue.ItemProcessor interface)
	service.queueManager = queue.NewManager(
		queue.ManagerConfig{
			Workers:      config.Workers,
			ConfigGetter: configGetter,
		},
		database.Repository,
		service, // ItemProcessor
		service, // QueueEventListener
	)

	return service, nil
}

// AddImportHistory records a successful file import in persistent history
func (s *Service) AddImportHistory(ctx context.Context, history *database.ImportHistory) error {
	return s.database.Repository.AddImportHistory(ctx, history)
}

// Start starts the NZB import service (queue workers only, manual scanning available via API)
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("service is already started")
	}

	// Update database connection pool to match worker count
	// This prevents connection starvation when multiple workers try to claim items
	s.database.UpdateConnectionPool(s.config.Workers)
	s.log.InfoContext(ctx, "Updated database connection pool",
		"workers", s.config.Workers,
		"max_connections", s.config.Workers+4)

	// Reset any stale queue items from processing back to pending
	if err := s.database.Repository.ResetStaleItems(ctx); err != nil {
		s.log.ErrorContext(ctx, "Failed to reset stale queue items", "error", err)
		return fmt.Errorf("failed to reset stale queue items: %w", err)
	}

	// Delegate worker management to queue manager
	if err := s.queueManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start queue manager: %w", err)
	}

	// Start directory watcher if configured
	if err := s.watcher.Start(ctx); err != nil {
		s.log.ErrorContext(ctx, "Failed to start directory watcher", "error", err)
		// Don't fail service start if watcher fails
	}

	// Start background cleanup of stale failed queue items
	go s.runFailedItemCleanup(ctx)

	// Start background sweeper for grabbed indexers cache
	go s.runGrabbedIndexerSweeper(s.ctx)

	s.running = true
	s.log.InfoContext(ctx, fmt.Sprintf("NZB import service started successfully with %d workers", s.config.Workers))

	return nil
}

// ProcessItem implements queue.ItemProcessor - processes a single queue item
func (s *Service) ProcessItem(ctx context.Context, item *database.ImportQueueItem) (string, error) {
	resultPath, writtenPaths, err := s.processNzbItem(ctx, item)
	// Always store written paths so HandleFailure can clean them up on error.
	s.writtenPathsCache.Store(item.ID, writtenPaths)
	return resultPath, err
}

// HandleSuccess implements queue.ItemProcessor - handles successful processing
func (s *Service) HandleSuccess(ctx context.Context, item *database.ImportQueueItem, resultingPath string) error {
	// The written virtual paths are carried over from ProcessItem so multi-file
	// imports (season packs) can schedule a per-episode post-import health check.
	var writtenPaths []string
	if paths, ok := s.writtenPathsCache.LoadAndDelete(item.ID); ok {
		writtenPaths, _ = paths.([]string)
	}
	return s.handleProcessingSuccess(ctx, item, resultingPath, writtenPaths)
}

// HandleFailure implements queue.ItemProcessor - handles failed processing
func (s *Service) HandleFailure(ctx context.Context, item *database.ImportQueueItem, err error) {
	if paths, ok := s.writtenPathsCache.LoadAndDelete(item.ID); ok {
		s.cleanupWrittenPaths(ctx, item.ID, paths.([]string))
	}
	s.handleProcessingFailure(ctx, item, err)
}

// Pause pauses the queue processing
func (s *Service) Pause() {
	s.queueManager.Pause()
	s.mu.Lock()
	s.paused = true
	s.mu.Unlock()
	s.log.InfoContext(s.ctx, "Import service paused")
}

// Resume resumes the queue processing
func (s *Service) Resume() {
	s.queueManager.Resume()
	s.mu.Lock()
	s.paused = false
	s.mu.Unlock()
	s.log.InfoContext(s.ctx, "Import service resumed")
}

// IsPaused returns whether the service is paused
func (s *Service) IsPaused() bool {
	return s.queueManager.IsPaused()
}

func (s *Service) RegisterConfigChangeHandler(configManager any) {
	mgr, ok := configManager.(*config.Manager)
	if !ok {
		return
	}
	mgr.OnConfigChange(func(oldConfig, newConfig *config.Config) {
		// Update rclone client reference
		s.mu.Lock()
		if s.postProcessor != nil {
			s.postProcessor.SetRcloneClient(s.rcloneClient)
		}
		s.mu.Unlock()

		// Dynamically resize queue workers if count changed
		oldWorkers := oldConfig.Import.MaxProcessorWorkers
		newWorkers := newConfig.Import.MaxProcessorWorkers
		if newWorkers > 0 && oldWorkers != newWorkers {
			ctx := context.Background()
			if err := s.queueManager.Resize(ctx, newWorkers); err != nil {
				s.log.ErrorContext(ctx, "Failed to resize queue workers", "error", err)
			} else {
				s.database.UpdateConnectionPool(newWorkers)
				s.log.InfoContext(ctx, "Queue workers resized dynamically",
					"old_workers", oldWorkers, "new_workers", newWorkers)
			}
		}

		// Push the updated import-admission cap to the pool. Zero keeps
		// the admission gate disabled (unlimited).
		if s.poolManager != nil {
			cap := newConfig.GetMaxConcurrentImports()
			s.poolManager.SetAdmissionCap(cap)
			s.log.InfoContext(s.ctx, "Import admission cap updated",
				"max_concurrent_imports", cap)
		}
	})
}

// Stop stops the NZB import service and all queue workers
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()

	if !s.running {
		s.mu.Unlock()
		return nil
	}

	s.log.InfoContext(ctx, "Stopping NZB import service")
	s.running = false
	s.mu.Unlock()

	// Delegate worker shutdown to queue manager
	if err := s.queueManager.Stop(ctx); err != nil {
		s.log.WarnContext(ctx, "Error stopping queue manager", "error", err)
	}

	// Stop directory watcher
	s.watcher.Stop()

	// Cancel service context
	s.cancel()

	// Re-acquire lock to recreate context for potential restart
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.log.InfoContext(ctx, "NZB import service stopped")

	return nil
}

// Close closes the NZB import service and releases all resources
func (s *Service) Close() error {
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()

	if running {
		return s.Stop(context.Background())
	}

	return nil
}

// IsRunning returns whether the service is running
func (s *Service) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// SetRcloneClient sets or updates the RClone client for VFS notifications
func (s *Service) SetRcloneClient(client any) {
	var rc rclonecli.RcloneRcClient
	if client != nil {
		var ok bool
		rc, ok = client.(rclonecli.RcloneRcClient)
		if !ok {
			s.log.ErrorContext(s.ctx, "SetRcloneClient: unexpected client type", "type", fmt.Sprintf("%T", client))
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rcloneClient = rc
	if s.postProcessor != nil {
		s.postProcessor.SetRcloneClient(rc)
	}
	if rc != nil {
		s.log.InfoContext(s.ctx, "RClone client updated for VFS notifications")
	} else {
		s.log.InfoContext(s.ctx, "RClone client disabled")
	}
}

// SetContentVerifyFilesystem wires the real serving-stack filesystem used to
// probe imported media files' content signatures. Called once after the
// NzbFilesystem singleton is constructed (see cmd/altmount/cmd/serve.go),
// mirroring SetArrsService's late-binding to avoid an import-time dependency
// cycle between importer and nzbfilesystem.
func (s *Service) SetContentVerifyFilesystem(fs contentverify.Opener) {
	s.mu.Lock()
	s.contentVerifyFS = fs
	s.mu.Unlock()
}

// SetArrsService sets or updates the ARRs service
func (s *Service) SetArrsService(service any) {
	var as *arrs.Service
	if service != nil {
		var ok bool
		as, ok = service.(*arrs.Service)
		if !ok {
			s.log.ErrorContext(s.ctx, "SetArrsService: unexpected service type", "type", fmt.Sprintf("%T", service))
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.arrsService = as
	if s.postProcessor != nil {
		s.postProcessor.SetArrsService(as)
	}
}

// GetQueueStats returns current queue statistics from database
func (s *Service) GetQueueStats(ctx context.Context) (*database.QueueStats, error) {
	return s.database.Repository.GetQueueStats(ctx)
}

// StartManualScan starts a manual scan of the specified directory
func (s *Service) StartManualScan(scanPath string) error {
	return s.dirScanner.Start(scanPath)
}

// GetScanStatus returns the current scan status
func (s *Service) GetScanStatus() ScanInfo {
	return s.dirScanner.GetStatus()
}

// CancelScan cancels the current scan operation
func (s *Service) CancelScan() error {
	return s.dirScanner.Cancel()
}

// StartNzbdavImport starts an asynchronous import from an NZBDav database
func (s *Service) StartNzbdavImport(dbPath string, blobsPath string, cleanupFile bool) error {
	return s.nzbdavImporter.Start(dbPath, blobsPath, cleanupFile)
}

// GetImportStatus returns the current import status
func (s *Service) GetImportStatus() ImportInfo {
	return s.nzbdavImporter.GetStatus()
}

// ResetNzbdavImportStatus resets the import status to Idle
func (s *Service) ResetNzbdavImportStatus() {
	s.nzbdavImporter.Reset()
}

// CancelImport cancels the current import operation
func (s *Service) CancelImport() error {
	return s.nzbdavImporter.Cancel()
}

// IsFileInQueue checks if a file is already in the queue (pending or processing)
func (s *Service) IsFileInQueue(ctx context.Context, filePath string) (bool, error) {
	return s.database.Repository.IsFileInQueue(ctx, filePath)
}

// GetNzbFolder returns the path to the persistent NZB storage directory
func (s *Service) GetNzbFolder() string {
	cfg := s.configGetter()
	configDir := filepath.Dir(cfg.Database.Path)
	return filepath.Join(configDir, ".nzbs")
}

// GetFailedNzbFolder returns the path to the directory for failed NZB files
func (s *Service) GetFailedNzbFolder() string {
	return filepath.Join(s.GetNzbFolder(), "failed")
}

// MoveToFailedFolder moves a failed NZB file to the failed directory
func (s *Service) MoveToFailedFolder(ctx context.Context, item *database.ImportQueueItem) error {
	failedDir := s.GetFailedNzbFolder()

	// Add category subfolder if present to keep failed items organized
	if item.Category != nil && *item.Category != "" {
		failedDir = filepath.Join(failedDir, *item.Category)
	}

	if err := os.MkdirAll(failedDir, 0755); err != nil {
		return fmt.Errorf("failed to create failed directory: %w", err)
	}

	// import_queue.nzb_path is UNIQUE. Two failed items can share a basename (e.g. a re-acquired
	// release reuses the same NZB filename), so a prior failed item may already occupy the plain
	// destination -- without disambiguation the DB update below fails with a UNIQUE constraint
	// violation. uniqueFailedNzbPath namespaces the copy by the queue item ID on collision.
	newPath := uniqueFailedNzbPath(failedDir, item.NzbPath, item.ID)

	// Check if source exists
	if _, err := os.Stat(item.NzbPath); os.IsNotExist(err) {
		// If source doesn't exist, maybe it was already moved?
		return nil
	}

	// Avoid moving if already in failed folder (e.g. retry of failed item)
	if filepath.Dir(item.NzbPath) == failedDir {
		return nil
	}

	// Move file
	if err := utils.MoveFile(item.NzbPath, newPath); err != nil {
		return fmt.Errorf("failed to move NZB to failed folder: %w", err)
	}

	// Update DB
	if err := s.database.Repository.UpdateQueueItemNzbPath(ctx, item.ID, newPath); err != nil {
		return fmt.Errorf("failed to update DB with new NZB path: %w", err)
	}

	// Update struct
	item.NzbPath = newPath
	s.log.InfoContext(ctx, "Moved failed NZB to failed directory", "new_path", newPath)
	return nil
}

// uniqueFailedNzbPath returns the destination path for a failed NZB inside failedDir. It keeps the
// original basename when that destination is free, and namespaces it with the queue item ID when
// the plain path is already taken. import_queue.nzb_path is UNIQUE and two failed items can share a
// basename (e.g. a re-acquired release reuses the NZB filename), so reusing the destination would
// make MoveToFailedFolder's DB update fail with a UNIQUE constraint violation.
func uniqueFailedNzbPath(failedDir, srcNzbPath string, itemID int64) string {
	fileName := filepath.Base(srcNzbPath)
	newPath := filepath.Join(failedDir, fileName)
	if _, err := os.Stat(newPath); err == nil {
		newPath = filepath.Join(failedDir, fmt.Sprintf("%d-%s", itemID, fileName))
	}
	return newPath
}

// persistentNzbPath returns the destination path for a successfully imported NZB inside
// nzbDir. It prefers the clean filename directly in nzbDir and only namespaces it with the
// queue item ID as a filename prefix (nzbDir/<id>-<name><ext>) when the plain destination is
// reported taken by isTaken, guaranteeing a unique path for the UNIQUE import_queue.nzb_path
// column. Mirrors uniqueFailedNzbPath.
func persistentNzbPath(nzbDir, baseName, ext string, itemID int64, isTaken func(string) bool) string {
	plain := filepath.Join(nzbDir, baseName+ext)
	if !isTaken(plain) {
		return plain
	}
	return filepath.Join(nzbDir, fmt.Sprintf("%d-%s%s", itemID, baseName, ext))
}

// sanitizeFilename replaces invalid characters in filenames
func sanitizeFilename(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}

// AddToQueue adds a new NZB file to the import queue.
func (s *Service) AddToQueue(ctx context.Context, filePath string, relativePath *string, category *string, priority *database.QueuePriority, metadata *string, downloadID *string, indexer *string) (*database.ImportQueueItem, error) {
	// Check context before proceeding
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Calculate file size before adding to queue
	var fileSize *int64
	if size, err := s.CalculateFileSizeOnly(filePath); err == nil {
		fileSize = &size
	} else {
		s.log.WarnContext(ctx, "Failed to calculate file size", "file", filePath, "error", err)
		// Continue with NULL file size - don't fail the queue addition
		fileSize = nil
	}

	// Use default priority if not specified
	itemPriority := database.QueuePriorityNormal
	if priority != nil {
		itemPriority = *priority
	}

	// Lookup indexer from grabbedIndexers (Webhook-provided)
	var indexerName *string = nil
	if downloadID != nil && *downloadID != "" {
		if name, ok := s.GetGrabbedIndexer(*downloadID, filepath.Base(filePath)); ok {
			indexerName = &name
		}
	}
	// Prefer the explicit source indexer when webhook lookup misses.
	if indexerName == nil && indexer != nil && *indexer != "" {
		name := *indexer
		indexerName = &name
	}

	item := &database.ImportQueueItem{
		DownloadID:   downloadID,
		NzbPath:      filePath,
		RelativePath: relativePath,
		Category:     category,
		Priority:     itemPriority,
		Status:       database.QueueStatusPending,
		RetryCount:   0,
		MaxRetries:   3,
		FileSize:     fileSize,
		Metadata:     metadata,
		Indexer:      indexerName,
		CreatedAt:    time.Now(),
	}

	// Insert the DB row first so item.ID is available for the persistent filename
	// (which uses the queue ID as a uniqueness suffix). If persisting the NZB to
	// disk fails afterwards, roll back the row so the queue doesn't leak an orphan.
	if err := s.database.Repository.AddToQueue(ctx, item); err != nil {
		s.log.ErrorContext(ctx, "Failed to add file to queue", "file", item.NzbPath, "error", err)
		return nil, err
	}

	if err := s.ensurePersistentNzb(ctx, item); err != nil {
		s.log.ErrorContext(ctx, "Failed to ensure persistent NZB during queue addition", "file", filePath, "error", err)
		if rmErr := s.database.Repository.RemoveFromQueue(ctx, item.ID); rmErr != nil {
			s.log.WarnContext(ctx, "Failed to roll back queue row after persistence failure",
				"queue_id", item.ID, "error", rmErr)
		}
		return nil, fmt.Errorf("failed to make NZB persistent: %w", err)
	}

	if s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}

	if s.queueManager != nil {
		s.queueManager.NotifyNewItem()
	}

	if fileSize != nil {
		s.log.InfoContext(ctx, "Added NZB file to queue", "file", item.NzbPath, "queue_id", item.ID, "file_size", *fileSize)
	} else {
		s.log.InfoContext(ctx, "Added NZB file to queue", "file", item.NzbPath, "queue_id", item.ID, "file_size", "unknown")
	}

	return item, nil
}

// FindAndUpdatePendingUpload de-duplicates manual NZB re-uploads. UNIQUE(nzb_path) can't catch them
// because ensurePersistentNzb rewrites the path after insert, so this matches a still-pending item by
// its category-independent base filename and updates its category/priority in place. Returns nil if none.
func (s *Service) FindAndUpdatePendingUpload(ctx context.Context, filename string, category *string, priority *database.QueuePriority) (*database.ImportQueueItem, error) {
	// NZBs uploaded via the API are persisted into the OS temp queue dir.
	queueDir := filepath.Join(os.TempDir(), ".altmount-queue")

	base := nzbtrim.TrimNzbExtension(sanitizeFilename(filepath.Base(filename)))
	if base == "" {
		return nil, nil
	}

	items, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(ctx, queueDir+string(filepath.Separator))
	if err != nil {
		return nil, err
	}
	// Backward compatibility: also search the old configDir/.nzbs/ location for items queued
	// before the OS temp queue dir migration.
	oldNzbsDir := s.GetNzbFolder()
	oldItems, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(ctx, oldNzbsDir+string(filepath.Separator))
	if err != nil {
		return nil, err
	}
	items = append(items, oldItems...)
	// Also search the Stremio upload staging directory: handleNzbStreams writes there
	// before calling AddToQueue, so without this scan Stremio-submitted NZBs are never
	// found by FindAndUpdatePendingUpload and always create a new queue entry.
	stremioUploadDir := filepath.Join(os.TempDir(), "altmount-uploads")
	stremioItems, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(ctx, stremioUploadDir+string(filepath.Separator))
	if err != nil {
		return nil, err
	}
	items = append(items, stremioItems...)

	for _, it := range items {
		// Persisted name is "<base><ext>" or, on collision, "<id>-<base><ext>".
		cand := nzbtrim.TrimNzbExtension(filepath.Base(it.NzbPath))
		if cand != base && cand != fmt.Sprintf("%d-%s", it.ID, base) {
			continue
		}

		prio := it.Priority
		if priority != nil {
			prio = *priority
		}
		if err := s.database.Repository.UpdateQueueItemCategory(ctx, it.ID, category, prio); err != nil {
			return nil, err
		}

		updated, err := s.database.Repository.GetQueueItem(ctx, it.ID)
		if err != nil {
			return nil, err
		}

		if s.broadcaster != nil {
			s.broadcaster.BroadcastQueueChanged()
		}
		s.log.InfoContext(ctx, "Updated existing pending upload instead of creating a duplicate", "queue_id", it.ID)
		return updated, nil
	}

	return nil, nil
}

// processNzbItem processes the NZB file for a queue item
func (s *Service) processNzbItem(ctx context.Context, item *database.ImportQueueItem) (string, []string, error) {
	// Determine the base path
	basePath := ""
	if item.RelativePath != nil {
		basePath = *item.RelativePath
	}

	// Calculate the virtual directory for metadata storage
	virtualDir := s.calculateProcessVirtualDir(item, &basePath)

	// Ensure NZB is in a persistent location to prevent data loss if /tmp is cleaned
	if err := s.ensurePersistentNzb(ctx, item); err != nil {
		return "", nil, fmt.Errorf("failed to ensure persistent NZB: %w", err)
	}

	// Determine if allowed extensions override is needed
	var allowedExtensionsOverride *[]string
	if item.Category != nil && strings.ToLower(*item.Category) == "test" {
		emptySlice := []string{}
		allowedExtensionsOverride = &emptySlice // Allow all extensions for test files
	}

	// Parse metadata for extracted files (optimization for already extracted content)
	var extractedFiles []parser.ExtractedFileInfo
	if item.Metadata != nil && *item.Metadata != "" {
		type metaStruct struct {
			ExtractedFiles []parser.ExtractedFileInfo `json:"extracted_files"`
		}
		var meta metaStruct
		if err := json.Unmarshal([]byte(*item.Metadata), &meta); err == nil {
			extractedFiles = meta.ExtractedFiles
		}
	}

	resultPath, writtenPaths, err := s.processor.ProcessNzbFile(ctx, item.NzbPath, basePath, int(item.ID), allowedExtensionsOverride, &virtualDir, extractedFiles, item.Category, item.Metadata, item.DownloadID)
	var expansion *nestedNzbJobsError
	if errors.As(err, &expansion) {
		if err := s.enqueueNestedNzbs(ctx, item, expansion); err != nil {
			return "", nil, err
		}
		return s.processNzbItem(ctx, item)
	}
	if err != nil {
		return resultPath, writtenPaths, err
	}

	if verifyErr := s.verifyWrittenContent(ctx, writtenPaths); verifyErr != nil {
		return resultPath, writtenPaths, verifyErr
	}

	return resultPath, writtenPaths, nil
}

// verifyWrittenContent probes every eligible video/audio file among
// writtenPaths for a recognized media container signature, when
// import.verify_content is enabled. A definitive failure (bad signature or
// confirmed-missing head article) is returned as an error so the caller
// routes the item to HandleFailure exactly like any other import error,
// letting the Arr app blocklist the release. A transient probe error is
// wrapped with ErrContentProbeInconclusive instead, so handleProcessingFailure
// retains the item for retry without reporting a bad release.
func (s *Service) verifyWrittenContent(ctx context.Context, writtenPaths []string) error {
	s.mu.Lock()
	contentVerifyFS := s.contentVerifyFS
	s.mu.Unlock()

	cfg := s.configGetter()
	if cfg == nil || !cfg.GetImportVerifyContent() || contentVerifyFS == nil {
		return nil
	}

	timeout := cfg.GetImportVerifyContentTimeout()
	for _, path := range writtenPaths {
		if !fileinfo.IsVerifiableMediaFile(path) {
			continue
		}

		result := contentverify.Probe(ctx, contentVerifyFS, path, timeout)
		switch result.Result {
		case contentverify.ContentValid:
			continue
		case contentverify.ContentInvalid:
			return fmt.Errorf("content verification failed for %q: no recognized media container signature: %w", path, result.Err)
		case contentverify.ContentSegmentMissing:
			return fmt.Errorf("content verification failed for %q: head article missing: %w", path, result.Err)
		case contentverify.ContentProbeError:
			return fmt.Errorf("content verification could not complete for %q: %w: %w", path, ErrContentProbeInconclusive, result.Err)
		}
	}
	return nil
}

func (s *Service) calculateProcessVirtualDir(item *database.ImportQueueItem, basePath *string) string {
	// Calculate initial virtual directory from physical/relative path
	virtualDir := filesystem.CalculateVirtualDirectory(item.NzbPath, *basePath)

	// NZB is in the OS temp queue dir — the temp dir has no meaningful directory
	// structure to derive a virtual path from, so start from basePath. We must NOT
	// return early here: the category resolution and CompleteDir prepend below still
	// need to run, otherwise category-tagged uploads (SABnzbd/manual/Stremio) would
	// land at the mount root instead of inside their category folder.
	tempQueueDir := filepath.Join(os.TempDir(), ".altmount-queue")
	inTempQueue := strings.HasPrefix(item.NzbPath, tempQueueDir+string(filepath.Separator)) || item.NzbPath == tempQueueDir

	// Fix for issue where files moved to persistent .nzbs directory end up with exposed paths (like /config) in virtual directory
	// This happens when NzbPath is inside .nzbs and CalculateVirtualDirectory sees the physical parent folder.
	nzbFolder := s.GetNzbFolder()
	if inTempQueue {
		virtualDir = *basePath
	} else if strings.HasPrefix(item.NzbPath, nzbFolder) {
		// Calculate path relative to the persistent NZB folder
		if relPath, err := filepath.Rel(nzbFolder, item.NzbPath); err == nil {
			// If file is directly in root of .nzbs (e.g. "file.nzb"), relDir is "."
			relDir := filepath.Dir(relPath)

			if relDir == "." {
				// Use the original basePath if the file is in the root of .nzbs
				virtualDir = *basePath
			} else {
				// Recalculate virtualDir relative to the nzbFolder to discard physical parent paths like /config
				// We use the subdirectory structure found inside .nzbs if it exists

				// Strip 'failed' subdirectory if present (added when items fail and are moved to .nzbs/failed)
				// We want to avoid including 'failed' in the virtual directory path during retries.
				cleanRel := filepath.ToSlash(relDir)
				if after, ok := strings.CutPrefix(cleanRel, "failed/"); ok {
					cleanRel = after
				} else if cleanRel == "failed" {
					cleanRel = ""
				}

				// Strip the queue_id subfolder added by ensurePersistentNzb.
				// Storage structure: .nzbs/{category}/{queue_id}/filename.nzb.gz
				// The numeric ID must not leak into the virtual destination path.
				if item.ID != 0 {
					queueIDStr := strconv.FormatInt(item.ID, 10)
					if cleanRel == queueIDStr {
						cleanRel = ""
					} else if after, ok := strings.CutSuffix(cleanRel, "/"+queueIDStr); ok {
						cleanRel = after
					}
				}

				cleanBase := filepath.ToSlash(*basePath)
				// Avoid duplication if basePath already starts with relDir (common with Watcher or manual imports)
				// We only apply this reconstruction if basePath is empty or root, otherwise we trust basePath
				if cleanBase != "" && cleanBase != "/" && cleanBase != "." {
					virtualDir = *basePath
				} else if *basePath != "" && (cleanBase == cleanRel || strings.HasPrefix(cleanBase, cleanRel+"/")) {
					virtualDir = *basePath
				} else {
					virtualDir = filepath.Join(*basePath, cleanRel)
				}
			}

			// Ensure proper formatting
			if !strings.HasPrefix(virtualDir, "/") {
				virtualDir = "/" + virtualDir
			}
			virtualDir = filepath.ToSlash(virtualDir)
		}
	}

	// If category is specified, resolve to configured directory path
	if item.Category != nil && *item.Category != "" {
		categoryPath := s.buildCategoryPath(*item.Category)
		if categoryPath != "" {
			// Check if virtual path already contains the category path
			cleanVirtual := strings.Trim(filepath.ToSlash(virtualDir), "/")
			cleanCategory := strings.Trim(filepath.ToSlash(categoryPath), "/")

			virtualParts := strings.Split(cleanVirtual, "/")
			categoryParts := strings.Split(cleanCategory, "/")

			match := false
			if len(virtualParts) >= len(categoryParts) {
				// Check if categoryParts exists as a sub-sequence in virtualParts
				for i := 0; i <= len(virtualParts)-len(categoryParts); i++ {
					subMatch := true
					for j := range categoryParts {
						if !strings.EqualFold(virtualParts[i+j], categoryParts[j]) {
							subMatch = false
							break
						}
					}
					if subMatch {
						match = true
						break
					}
				}
			}

			// If the category is NOT present in the virtual path (e.g. NZBDav import),
			// we must append it to ensure the file ends up in the correct category folder.
			if !match {
				*basePath = joinPathsMergingOverlap(*basePath, categoryPath)
				virtualDir = joinPathsMergingOverlap(virtualDir, categoryPath)
			}
		}
	}

	// Ensure absolute virtual path
	if !strings.HasPrefix(virtualDir, "/") {
		virtualDir = "/" + virtualDir
	}
	virtualDir = filepath.ToSlash(virtualDir)

	cfg := s.configGetter()
	// ALWAYS prepend CompleteDir to isolate completed downloads from final library.
	if cfg != nil && cfg.SABnzbd.CompleteDir != "" {
		completeDir := strings.TrimRight(filepath.ToSlash(cfg.SABnzbd.CompleteDir), "/")
		if !strings.HasPrefix(completeDir, "/") {
			completeDir = "/" + completeDir
		}

		if completeDir != "/" && !strings.HasPrefix(virtualDir, completeDir+"/") && virtualDir != completeDir {
			virtualDir = filepath.Join(completeDir, virtualDir)
		}
	}

	return sanitizeVirtualPath(virtualDir)
}

// driveLetterRe matches a "<letter>:" segment anywhere in a forward-slash path.
var driveLetterRe = regexp.MustCompile(`(?i)(^|/)[a-z]:(?:/|$)`)

// sanitizeVirtualPath canonicalizes a virtual path to forward slashes and strips
// Windows drive-letter segments and stray colons — these would otherwise leak
// into metadata directory creation and fail with mkdir on Windows.
func sanitizeVirtualPath(p string) string {
	p = strings.ReplaceAll(p, `\`, "/")
	p = driveLetterRe.ReplaceAllString(p, "$1")
	p = strings.ReplaceAll(p, ":", "")
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// ensurePersistentNzb moves the NZB file to a persistent location in the OS
// temporary queue directory. Using the OS temp dir (instead of configDir/.nzbs/)
// keeps raw staging files separate from the permanent .nzbz store and prevents
// stremio's defer os.RemoveAll(stageDir) from deleting the file before the
// worker can process it.
func (s *Service) ensurePersistentNzb(ctx context.Context, item *database.ImportQueueItem) error {
	// Use OS temp queue dir; itemID ensures uniqueness so no category subfolder needed.
	nzbDir := filepath.Join(os.TempDir(), ".altmount-queue")

	// Check if current path is already in the persistent directory
	absNzbPath, _ := filepath.Abs(item.NzbPath)
	absNzbDir, _ := filepath.Abs(nzbDir)

	// Simple check: if path starts with persistent dir (with separator) or equals it, assume it's fine.
	// The trailing separator prevents a false match like /tmp/.altmount-queue-other/ matching /tmp/.altmount-queue.
	if strings.HasPrefix(absNzbPath, absNzbDir+string(os.PathSeparator)) || absNzbPath == absNzbDir {
		if _, err := os.Stat(item.NzbPath); err == nil {
			return nil
		}
		// The raw NZB was deleted after a prior successful import (handleProcessingSuccess
		// removes it on purpose once the .nzbz store exists). A retry/reprocess reaches this
		// point with a stale nzb_path, so regenerate the raw file from the store instead of
		// failing to open a file that no longer exists.
		return s.regenerateNzbFile(ctx, item)
	}

	if item.ID == 0 {
		return fmt.Errorf("cannot persist NZB without queue ID (row must be inserted first)")
	}

	if err := os.MkdirAll(nzbDir, 0755); err != nil {
		return fmt.Errorf("failed to create persistent NZB directory: %w", err)
	}

	ext := nzbfile.PlainExtension

	base := nzbtrim.TrimNzbExtension(sanitizeFilename(filepath.Base(item.NzbPath)))

	// Save directly into the category folder using the clean filename. Only when that
	// destination is already taken do we namespace it with the queue item ID as a filename
	// prefix (<id>-<name>) — this drops the numeric folder in the common case while still
	// guaranteeing a unique path on collision for the UNIQUE import_queue.nzb_path column.
	taken := func(p string) bool {
		if _, err := os.Stat(p); err == nil {
			return true
		}
		// Also treat a path owned by a different queue item as taken (UNIQUE nzb_path constraint).
		if existing, err := s.database.Repository.GetQueueItemByNzbPath(ctx, p); err == nil && existing != nil && existing.ID != item.ID {
			return true
		}
		return false
	}
	newPath := persistentNzbPath(nzbDir, base, ext, item.ID, taken)

	s.log.DebugContext(ctx, "Moving NZB to persistent storage", "old_path", item.NzbPath, "new_path", newPath)

	// Move the NZB to persistent storage.
	if err := utils.MoveFile(item.NzbPath, newPath); err != nil {
		s.log.ErrorContext(ctx, "Failed to move NZB to persistent storage", "error", err, "src", item.NzbPath, "dst", newPath)
		return fmt.Errorf("failed to move NZB to persistent storage: %w", err)
	}

	// Update DB
	oldPath := item.NzbPath
	item.NzbPath = newPath
	if err := s.database.Repository.UpdateQueueItemNzbPath(ctx, item.ID, newPath); err != nil {
		// If DB update fails, we are in a weird state (file moved but DB points to old).
		// We should probably try to move it back or just fail.
		// But failing here aborts the import.
		// The file is at newPath.
		// If we fail, the item stays 'processing' in DB with old path.
		// Next retry will fail to find file at old path.
		return fmt.Errorf("failed to update DB with new NZB path: %w", err)
	}

	s.log.InfoContext(ctx, "Moved NZB to persistent storage", "old_path", oldPath, "new_path", newPath)
	return nil
}

// regenerateNzbFile rebuilds the raw .nzb file for item from its .nzbz metadata store when the
// raw file has been deleted (handleProcessingSuccess removes it after a successful import) but
// the item is being reprocessed. It reconstructs the store path the same way processor.go writes
// it and handleDownloadNZB reads it (configDir/.nzbs/{category}/{queueID}-{nzbBase}.nzbz), writes
// the regenerated XML back into the persistent temp queue dir, and updates nzb_path in the DB.
func (s *Service) regenerateNzbFile(ctx context.Context, item *database.ImportQueueItem) error {
	if s.metadataService == nil {
		return fmt.Errorf("raw NZB file is missing and no metadata service is available to regenerate it")
	}

	cfg := s.configGetter()
	configDir := filepath.Dir(cfg.Database.Path)
	if !filepath.IsAbs(configDir) {
		if abs, err := filepath.Abs(configDir); err == nil {
			configDir = abs
		}
	}

	var categoryStr string
	if item.Category != nil && *item.Category != "" {
		categoryStr = strings.ReplaceAll(*item.Category, `\`, "/")
		categoryStr = strings.Trim(categoryStr, "/")
		for _, part := range strings.Split(categoryStr, "/") {
			if part == ".." || part == "." {
				categoryStr = ""
				break
			}
		}
	}

	nzbBase := nzbtrim.TrimNzbExtension(filepath.Base(item.NzbPath))
	storeRef := filepath.Join(configDir, ".nzbs", categoryStr, fmt.Sprintf("%d-%s.nzbz", item.ID, nzbBase))

	nzbXML, err := s.metadataService.Store().RegenerateNZB(storeRef)
	if err != nil {
		return fmt.Errorf("failed to regenerate NZB from store: %w", err)
	}
	if nzbXML == nil {
		return fmt.Errorf("raw NZB file is missing and no store was found at %q to regenerate it", storeRef)
	}

	nzbDir := filepath.Join(os.TempDir(), ".altmount-queue")
	if err := os.MkdirAll(nzbDir, 0755); err != nil {
		return fmt.Errorf("failed to create persistent NZB directory: %w", err)
	}

	ext := nzbfile.PlainExtension
	base := nzbtrim.TrimNzbExtension(sanitizeFilename(nzbBase))
	taken := func(p string) bool {
		if _, err := os.Stat(p); err == nil {
			return true
		}
		if existing, err := s.database.Repository.GetQueueItemByNzbPath(ctx, p); err == nil && existing != nil && existing.ID != item.ID {
			return true
		}
		return false
	}
	newPath := persistentNzbPath(nzbDir, base, ext, item.ID, taken)

	if err := os.WriteFile(newPath, nzbXML, 0644); err != nil {
		return fmt.Errorf("failed to write regenerated NZB to %q: %w", newPath, err)
	}

	oldPath := item.NzbPath
	item.NzbPath = newPath
	if err := s.database.Repository.UpdateQueueItemNzbPath(ctx, item.ID, newPath); err != nil {
		return fmt.Errorf("failed to update DB with regenerated NZB path: %w", err)
	}

	s.log.InfoContext(ctx, "Regenerated raw NZB from metadata store", "old_path", oldPath, "new_path", newPath, "store_ref", storeRef)
	return nil
}

// buildCategoryPath resolves a category name to its configured directory path (memoized).
func (s *Service) buildCategoryPath(category string) string {
	if category == "" {
		category = config.DefaultCategoryName
	}

	if cached, ok := s.categoryPathCache.Load(category); ok {
		return cached.(string)
	}

	result := s.resolveCategoryPath(category)
	s.categoryPathCache.Store(category, result)
	return result
}

// resolveCategoryPath performs the actual category-to-directory resolution.
func (s *Service) resolveCategoryPath(category string) string {
	cfg := s.configGetter()
	if cfg == nil || len(cfg.SABnzbd.Categories) == 0 {
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

// resolveIndexerFromArrs asks the ARRs service to resolve the indexer for a
// download ID on demand. This is the reliable path for Sportarr, which can't
// push its indexer via webhook (it's not starr-compatible and never fires an
// OnGrab event); the lookup runs at import time so the indexer stat is recorded
// correctly instead of as "Unknown". Returns ("", false) when the ARRs service
// is unset or has no match.
func (s *Service) resolveIndexerFromArrs(ctx context.Context, downloadID string) (string, bool) {
	s.mu.Lock()
	as := s.arrsService
	s.mu.Unlock()
	if as == nil {
		return "", false
	}
	return as.ResolveIndexerByDownloadID(ctx, downloadID)
}

// handleProcessingSuccess handles all steps after successful NZB processing.
// writtenPaths lists every virtual file the import wrote (may be nil for legacy
// callers); multi-file imports use it to health-check each file individually.
func (s *Service) handleProcessingSuccess(ctx context.Context, item *database.ImportQueueItem, resultingPath string, writtenPaths []string) error {
	// Log persistent indexer statistic
	indexerName := database.IndexerUnknown
	if item.Indexer != nil && *item.Indexer != "" {
		indexerName = *item.Indexer
	}
	if (indexerName == database.IndexerUnknown || indexerName == "") && item.DownloadID != nil && *item.DownloadID != "" {
		if latestItem, err := s.database.Repository.GetQueueItemByDownloadID(ctx, *item.DownloadID); err == nil && latestItem != nil && latestItem.Indexer != nil && *latestItem.Indexer != "" {
			indexerName = *latestItem.Indexer
		} else if name, ok := s.GetGrabbedIndexer(*item.DownloadID, filepath.Base(item.NzbPath)); ok {
			indexerName = name
		}
	}
	// Final fallback: query the ARRs directly. Covers Sportarr, whose indexer is
	// only available from its native queue (no webhook), resolved on demand here
	// so the stat is attributed correctly rather than logged as "Unknown".
	if (indexerName == database.IndexerUnknown || indexerName == "") && item.DownloadID != nil && *item.DownloadID != "" {
		if name, ok := s.resolveIndexerFromArrs(ctx, *item.DownloadID); ok {
			indexerName = name
		}
	}
	downloadID := ""
	if item.DownloadID != nil {
		downloadID = *item.DownloadID
	}
	if err := s.database.Repository.LogIndexerImport(ctx, indexerName, "success", "", downloadID); err != nil {
		s.log.WarnContext(ctx, "Failed to log indexer success statistic", "indexer", indexerName, "error", err)
	}

	// Add storage path to database
	if err := s.database.Repository.AddStoragePath(ctx, item.ID, resultingPath); err != nil {
		s.log.ErrorContext(ctx, "Failed to add storage path", "queue_id", item.ID, "error", err)
		return err
	}

	// Signal streamable as early as possible — files are accessible via VFS now.
	// Stremio waiters listening on the broadcaster can return stream URLs without
	// waiting for post-processing (symlinks, STRM, health scheduling) to complete.
	if s.broadcaster != nil {
		s.broadcaster.NotifyStreamable(int(item.ID), resultingPath)
	}

	// Refresh mount path if needed before post-processing
	s.postProcessor.RefreshMountPathIfNeeded(ctx, resultingPath, item.ID)

	// Delegate all post-processing to the coordinator
	// This handles: VFS notification, symlinks, ID links, STRM files, health checks, ARR notifications
	result, err := s.postProcessor.HandleSuccess(ctx, item, resultingPath, writtenPaths)
	if err != nil {
		s.log.ErrorContext(ctx, "Post-processing failed", "queue_id", item.ID, "error", err)
		return err
	}

	// Log any non-fatal errors from post-processing
	if len(result.Errors) > 0 {
		for _, postErr := range result.Errors {
			s.log.WarnContext(ctx, "Post-processing warning",
				"queue_id", item.ID,
				"error", postErr)
		}
	}

	// Mark as completed in queue database
	if err := s.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusCompleted, nil); err != nil {
		s.log.ErrorContext(ctx, "Failed to mark item as completed", "queue_id", item.ID, "error", err)
		return err
	}

	// Delete the on-disk NZB — the .nzbz store can regenerate it on demand.
	if item.NzbPath != "" {
		if err := os.Remove(item.NzbPath); err != nil && !os.IsNotExist(err) {
			s.log.WarnContext(ctx, "Failed to delete NZB after successful import",
				"queue_id", item.ID, "nzb_path", item.NzbPath, "error", err)
		}
	}

	// Update import_migrations row if this was a nzbdav migration import
	if s.database.MigrationRepo != nil {
		if err := s.database.MigrationRepo.MarkImported(ctx, item.ID, resultingPath); err != nil {
			// Non-fatal: log but don't fail
			s.log.WarnContext(ctx, "Failed to mark import_migration as imported",
				"queue_id", item.ID, "error", err)
		}
	}

	// Notify completion and clear progress tracking
	if s.broadcaster != nil {
		s.broadcaster.NotifyComplete(int(item.ID), "completed")
		s.broadcaster.BroadcastQueueChanged()
	}

	s.log.InfoContext(ctx, "Successfully processed queue item", "queue_id", item.ID, "file", item.NzbPath)

	return nil
}

// OnItemClaimed implements queue.QueueEventListener. It broadcasts a queue-changed
// notification whenever a worker claims a pending item (pending → processing transition).
func (s *Service) OnItemClaimed(ctx context.Context, item *database.ImportQueueItem) {
	if s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
}

// cleanupWrittenPaths deletes metadata files/directories written during a failed import,
// along with any health records already scheduled for them.
// Paths prefixed with "DIR:" indicate a directory that should be removed if empty; others are individual files.
func (s *Service) cleanupWrittenPaths(ctx context.Context, itemID int64, paths []string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	// Files first: a release folder is only ever removed once this job's own
	// files are gone, so what remains in it belongs to another import.
	var dirPaths []string
	for _, p := range paths {
		if after, ok := strings.CutPrefix(p, "DIR:"); ok {
			dirPaths = append(dirPaths, after)
			continue
		}

		if delErr := s.metadataService.DeleteFileMetadata(ctx, p); delErr != nil {
			s.log.WarnContext(ctx, "Failed to clean up metadata file after import failure",
				"queue_id", itemID,
				"path", p,
				"error", delErr)
		} else {
			s.log.DebugContext(ctx, "Cleaned up metadata file after import failure",
				"queue_id", itemID,
				"path", p)
		}
		s.cleanupHealthRecords(ctx, itemID, p)
	}

	for _, dirPath := range dirPaths {
		delErr := s.metadataService.DeleteDirectoryIfEmpty(dirPath)
		switch {
		case errors.Is(delErr, metadata.ErrDirectoryNotEmpty):
			// A concurrent import still owns files here. Its health records live
			// under the same prefix, so leave them alone too.
			s.log.DebugContext(ctx, "Preserved metadata directory after import failure (contains files from another import)",
				"queue_id", itemID,
				"dir", dirPath)
			continue
		case delErr != nil:
			s.log.WarnContext(ctx, "Failed to clean up metadata directory after import failure",
				"queue_id", itemID,
				"dir", dirPath,
				"error", delErr)
			continue
		}

		s.log.DebugContext(ctx, "Cleaned up empty metadata directory after import failure",
			"queue_id", itemID,
			"dir", dirPath)
		s.cleanupHealthRecords(ctx, itemID, dirPath)
	}
}

// cleanupHealthRecords removes the still-unvalidated health records under the given virtual
// path (a file or a directory) whose metadata was just rolled back by a failed import.
// Without this, records scheduled mid-import — or by an earlier attempt of the same release —
// linger in pending forever under SYMLINK/STRM strategies: their placeholder library_path
// never matches the worker's library filter, and no ARR webhook will ever relink a
// rolled-back import.
//
// It deletes only unvalidated placeholder records (pending/checking, not yet relinked to a
// real library path). The nzbFolder is deterministic per release rather than unique per
// queue item, so a failed re-import shares the subtree of a prior successful import; scoping
// to unvalidated records keeps that prior import's healthy/relinked/repair records intact.
func (s *Service) cleanupHealthRecords(ctx context.Context, itemID int64, virtualPath string) {
	if s.healthRepo == nil {
		return
	}

	deleted, err := s.healthRepo.DeleteUnvalidatedHealthRecordsByPrefix(ctx, virtualPath)
	if err != nil {
		s.log.WarnContext(ctx, "Failed to clean up health records after import failure",
			"queue_id", itemID,
			"path", virtualPath,
			"error", err)
		return
	}
	if deleted > 0 {
		s.log.InfoContext(ctx, "Removed health records for rolled-back import",
			"queue_id", itemID,
			"path", virtualPath,
			"records", deleted)
	}
}

// handleProcessingFailure handles when processing fails
func (s *Service) handleProcessingFailure(ctx context.Context, item *database.ImportQueueItem, processingErr error) {
	// Processor history is provisional until verification and post-processing
	// succeed. Roll it back for every failure/deferral, including cancellation,
	// before a fallback or retention cleanup can remove the owning queue row.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := s.database.Repository.DeleteImportHistoryByNzbID(cleanupCtx, item.ID); err != nil {
		s.log.ErrorContext(ctx, "Failed to roll back import history", "queue_id", item.ID, "error", err)
	}

	if ctx.Err() != nil {
		if errors.Is(context.Cause(ctx), queue.ErrProcessingCancelled) {
			s.markImportCancelled(cleanupCtx, item)
		}
		// Shutdown leaves the row processing for ResetStaleItems on restart.
		return
	}

	// A deferral is not a failure: the release is parked while a PAR2 repair
	// rebuilds its missing articles. The repair service returns it to pending
	// on success, or fails it when the damage proves unrepairable.
	if errors.Is(processingErr, ErrDeferredForRepair) {
		msg := "waiting for PAR2 repair to rebuild missing articles"
		if err := s.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusWaitingRepair, &msg); err != nil {
			s.log.ErrorContext(ctx, "Failed to park item pending repair", "queue_id", item.ID, "error", err)
			return
		}
		// Tell the UI: without this the row keeps showing its last progress
		// stage until the user reloads the page.
		if s.broadcaster != nil {
			s.broadcaster.NotifyComplete(int(item.ID), string(database.QueueStatusWaitingRepair))
			s.broadcaster.BroadcastQueueChanged()
		}
		s.log.InfoContext(ctx, "Import parked pending PAR2 repair",
			"queue_id", item.ID, "file", item.NzbPath)
		return
	}

	errorMessage := processingErr.Error()
	if errors.Is(processingErr, validation.ErrFastFailInconclusive) && item.RetryCount < item.MaxRetries {
		// Keep the item claimed during backoff so another worker cannot retry
		// immediately. Cancellation ends automatic retries.
		delay := time.Second << min(max(item.RetryCount, 0), 5)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
		retryCtx, cancelRetry := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelRetry()
		if ctx.Err() != nil {
			if errors.Is(context.Cause(ctx), queue.ErrProcessingCancelled) {
				s.markImportCancelled(retryCtx, item)
			}
			return
		} else {
			retried, err := s.database.Repository.IncrementRetryCountAndResetStatus(retryCtx, item.ID, &errorMessage)
			if err != nil {
				s.log.ErrorContext(ctx, "Failed to requeue inconclusive import", "queue_id", item.ID, "error", err)
				return
			}
			if retried {
				s.log.InfoContext(ctx, "Requeued inconclusive import", "queue_id", item.ID, "backoff", delay)
				if s.broadcaster != nil {
					s.broadcaster.NotifyComplete(int(item.ID), string(database.QueueStatusPending))
					s.broadcaster.BroadcastQueueChanged()
				}
				return
			}
		}
	}
	if errors.Is(processingErr, validation.ErrFastFailInconclusive) || errors.Is(processingErr, ErrContentProbeInconclusive) {
		// The provider never produced a conclusive answer within the bounded
		// retry budget of segment validation or of the content probe. Keep this
		// distinct from a bad release: do not log an indexer failure,
		// notify/blocklist in ARR, invoke fallback, or move the NZB away. The
		// retained failed item can be retried manually once the provider is
		// healthy again.
		s.log.WarnContext(ctx, "Import stopped because verification was inconclusive",
			"queue_id", item.ID,
			"file", item.NzbPath,
			"error", processingErr)
		if err := s.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusFailed, &errorMessage); err != nil {
			s.log.ErrorContext(ctx, "Failed to mark inconclusive import as failed", "queue_id", item.ID, "error", err)
		}
		if s.database.MigrationRepo != nil {
			if err := s.database.MigrationRepo.MarkFailed(ctx, item.ID, errorMessage); err != nil {
				s.log.WarnContext(ctx, "Failed to mark inconclusive import migration as failed",
					"queue_id", item.ID, "error", err)
			}
		}
		if s.broadcaster != nil {
			s.broadcaster.NotifyComplete(int(item.ID), "failed")
			s.broadcaster.BroadcastQueueChanged()
		}
		return
	}

	// Log persistent indexer statistic
	indexerName := database.IndexerUnknown
	if item.Indexer != nil && *item.Indexer != "" {
		indexerName = *item.Indexer
	}
	if (indexerName == database.IndexerUnknown || indexerName == "") && item.DownloadID != nil && *item.DownloadID != "" {
		if latestItem, err := s.database.Repository.GetQueueItemByDownloadID(ctx, *item.DownloadID); err == nil && latestItem != nil && latestItem.Indexer != nil && *latestItem.Indexer != "" {
			indexerName = *latestItem.Indexer
		} else if name, ok := s.GetGrabbedIndexer(*item.DownloadID, filepath.Base(item.NzbPath)); ok {
			indexerName = name
		}
	}
	// Final fallback: query the ARRs directly (covers Sportarr, which has no webhook).
	if (indexerName == database.IndexerUnknown || indexerName == "") && item.DownloadID != nil && *item.DownloadID != "" {
		if name, ok := s.resolveIndexerFromArrs(ctx, *item.DownloadID); ok {
			indexerName = name
		}
	}
	// Don't log if it was just cancelled by the user
	if !strings.Contains(errorMessage, "context canceled") && !strings.Contains(errorMessage, "processing cancelled") {
		downloadID := ""
		if item.DownloadID != nil {
			downloadID = *item.DownloadID
		}
		if err := s.database.Repository.LogIndexerImport(ctx, indexerName, "failed", errorMessage, downloadID); err != nil {
			s.log.WarnContext(ctx, "Failed to log indexer failure statistic", "indexer", indexerName, "error", err)
		}
	}

	// Check if the error was due to cancellation
	if strings.Contains(errorMessage, "context canceled") || strings.Contains(errorMessage, "processing cancelled") {
		errorMessage = "Processing cancelled by user request"
		s.log.InfoContext(ctx, "Processing cancelled by user",
			"queue_id", item.ID,
			"file", item.NzbPath)
	} else {
		s.log.WarnContext(ctx, "Processing failed",
			"queue_id", item.ID,
			"file", item.NzbPath,
			"error", processingErr)
	}

	// Mark as failed in queue database (no automatic retry)
	if err := s.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusFailed, &errorMessage); err != nil {
		s.log.ErrorContext(ctx, "Failed to mark item as failed", "queue_id", item.ID, "error", err)
	} else {
		s.log.ErrorContext(ctx, "Item failed",
			"queue_id", item.ID,
			"file", item.NzbPath)
	}

	// Update import_migrations row if this was a nzbdav migration import
	if s.database.MigrationRepo != nil {
		if err := s.database.MigrationRepo.MarkFailed(ctx, item.ID, errorMessage); err != nil {
			s.log.WarnContext(ctx, "Failed to mark import_migration as failed",
				"queue_id", item.ID, "error", err)
		}
	}

	// Notify failure and clear progress tracking
	if s.broadcaster != nil {
		s.broadcaster.NotifyComplete(int(item.ID), "failed")
		s.broadcaster.BroadcastQueueChanged()
	}

	// Delegate fallback handling to post-processor
	if err := s.postProcessor.HandleFailure(ctx, item, processingErr); err == nil {
		// Fallback succeeded - remove item from queue since ownership transfers to external SABnzbd
		if err := s.database.Repository.RemoveFromQueue(ctx, item.ID); err != nil {
			s.log.ErrorContext(ctx, "Failed to remove fallback item from queue", "queue_id", item.ID, "error", err)
		} else {
			s.log.InfoContext(ctx, "Item removed from queue after successful SABnzbd fallback transfer",
				"queue_id", item.ID,
				"file", item.NzbPath,
				"fallback_host", s.configGetter().SABnzbd.FallbackHost)
		}
		// Remove the local NZB file since ownership transfers to the external SABnzbd instance
		if rmErr := os.Remove(item.NzbPath); rmErr != nil && !os.IsNotExist(rmErr) {
			s.log.WarnContext(ctx, "Failed to remove NZB file after fallback transfer", "file", item.NzbPath, "error", rmErr)
		}
	} else if IsNonRetryable(err) && strings.Contains(err.Error(), "SABnzbd fallback not configured") {
		s.log.DebugContext(ctx, "SABnzbd fallback skipped (not configured)",
			"queue_id", item.ID,
			"file", item.NzbPath)

		// Always move failed NZB to failed folder for potential retry
		if moveErr := s.MoveToFailedFolder(ctx, item); moveErr != nil {
			s.log.ErrorContext(ctx, "Failed to move NZB to failed folder", "error", moveErr)
		}
	} else {
		s.log.ErrorContext(ctx, "Fallback handling failed",
			"queue_id", item.ID,
			"file", item.NzbPath,
			"error", err)

		// Always move failed NZB to failed folder for potential retry
		if moveErr := s.MoveToFailedFolder(ctx, item); moveErr != nil {
			s.log.ErrorContext(ctx, "Failed to move NZB to failed folder", "error", moveErr)
		}
	}
}

// markImportCancelled persists cancellation without starting fallback or ARR
// failure handling. ctx must remain live long enough for the status writes.
func (s *Service) markImportCancelled(ctx context.Context, item *database.ImportQueueItem) {
	message := "Processing cancelled by user request"
	if err := s.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusFailed, &message); err != nil {
		s.log.ErrorContext(ctx, "Failed to persist import cancellation", "queue_id", item.ID, "error", err)
	}
	if s.database.MigrationRepo != nil {
		if err := s.database.MigrationRepo.MarkFailed(ctx, item.ID, message); err != nil {
			s.log.WarnContext(ctx, "Failed to persist migration cancellation", "queue_id", item.ID, "error", err)
		}
	}
	if s.broadcaster != nil {
		s.broadcaster.NotifyComplete(int(item.ID), "failed")
		s.broadcaster.BroadcastQueueChanged()
	}
}

// runFailedItemCleanup periodically removes stale failed queue items and their NZB files.
func (s *Service) runFailedItemCleanup(ctx context.Context) {
	// Run once at startup
	s.cleanupFailedItems(ctx)
	s.cleanupOldHistory(ctx)

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanupFailedItems(ctx)
			s.cleanupOldHistory(ctx)
		}
	}
}

// cleanupFailedItems deletes failed queue items older than the configured retention period
// and removes their associated NZB files.
func (s *Service) cleanupFailedItems(ctx context.Context) {
	cfg := s.configGetter()
	retentionHours := 0
	if cfg.Import.FailedItemRetentionHours != nil {
		retentionHours = *cfg.Import.FailedItemRetentionHours
	}

	if retentionHours <= 0 {
		return // disabled
	}

	cutoff := time.Now().Add(-time.Duration(retentionHours) * time.Hour)
	deletedItems, err := s.database.Repository.DeleteFailedItemsOlderThan(ctx, cutoff)
	if err != nil {
		s.log.ErrorContext(ctx, "Failed to cleanup old failed queue items", "error", err)
		return
	}

	if len(deletedItems) == 0 {
		return
	}

	// Remove NZB files for deleted items
	for _, item := range deletedItems {
		if item.NzbPath != "" {
			if rmErr := os.Remove(item.NzbPath); rmErr != nil && !os.IsNotExist(rmErr) {
				s.log.WarnContext(ctx, "Failed to remove NZB file during cleanup", "file", item.NzbPath, "error", rmErr)
			}
		}
	}

	if s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
	s.log.InfoContext(ctx, "Cleaned up stale failed queue items",
		"count", len(deletedItems),
		"retention_hours", retentionHours)
}

// cleanupOldHistory deletes import_history records older than the configured retention period.
func (s *Service) cleanupOldHistory(ctx context.Context) {
	cfg := s.configGetter()
	if cfg.Import.HistoryRetentionDays == nil || *cfg.Import.HistoryRetentionDays <= 0 {
		return // disabled
	}

	cutoff := time.Now().Add(-time.Duration(*cfg.Import.HistoryRetentionDays) * 24 * time.Hour)
	if err := s.database.Repository.DeleteImportHistoryOlderThan(ctx, cutoff); err != nil {
		s.log.ErrorContext(ctx, "Failed to clean up old import history", "error", err)
	}
}

// CancelProcessing cancels a processing queue item by cancelling its context
func (s *Service) CancelProcessing(itemID int64) error {
	return s.queueManager.CancelProcessing(itemID)
}

// ExecuteItem manually triggers processing for a specific queue item, bypassing concurrency limits.
func (s *Service) ExecuteItem(ctx context.Context, itemID int64) error {
	s.ProcessItemInBackground(ctx, itemID)
	return nil
}

// ProcessItemInBackground processes a specific queue item in the background.
// NOTE: This intentionally runs outside the worker pool — it is used for manual retries
// of specific items and should not compete with the normal import queue workers.
func (s *Service) ProcessItemInBackground(ctx context.Context, itemID int64) {
	go func() {
		s.log.DebugContext(ctx, "Starting background processing of queue item", "item_id", itemID, "background", true)

		// Get the queue item
		item, err := s.database.Repository.GetQueueItem(ctx, itemID)
		if err != nil {
			s.log.ErrorContext(ctx, "Failed to get queue item for background processing", "item_id", itemID, "error", err)
			return
		}

		if item == nil {
			s.log.WarnContext(ctx, "Queue item not found for background processing", "item_id", itemID)
			return
		}

		// Update status to processing
		if err := s.database.Repository.UpdateQueueItemStatus(ctx, itemID, database.QueueStatusProcessing, nil); err != nil {
			s.log.ErrorContext(ctx, "Failed to update item status to processing", "item_id", itemID, "error", err)
			return
		}

		if s.broadcaster != nil {
			s.broadcaster.BroadcastQueueChanged()
		}

		// Create cancellable context for this item
		itemCtx, cancelItem := context.WithCancelCause(ctx)
		defer cancelItem(nil)
		cancel := func() { cancelItem(queue.ErrProcessingCancelled) }

		// Register cancel function
		s.cancelMu.Lock()
		s.cancelFuncs[item.ID] = cancel
		s.cancelMu.Unlock()

		// Clean up after processing
		defer func() {
			s.cancelMu.Lock()
			delete(s.cancelFuncs, item.ID)
			s.cancelMu.Unlock()
		}()

		// Process the NZB file using cancellable context
		resultingPath, writtenPaths, processingErr := s.processNzbItem(itemCtx, item)

		// Update queue database with results
		if processingErr != nil {
			// Clean up any metadata files written before the failure
			s.cleanupWrittenPaths(ctx, item.ID, writtenPaths)
			s.handleProcessingFailure(itemCtx, item, processingErr)
		} else {
			// Handle success (storage path, VFS notification, symlinks, status update)
			s.handleProcessingSuccess(ctx, item, resultingPath, writtenPaths)
		}
	}()
}

// CalculateFileSizeOnly calculates the total file size from NZB/STRM segments
// This is a lightweight parser that only extracts size information without full processing
func (s *Service) CalculateFileSizeOnly(filePath string) (int64, error) {
	if strings.HasSuffix(strings.ToLower(filePath), ".strm") {
		file, err := os.Open(filePath)
		if err != nil {
			return 0, NewNonRetryableError("failed to open file for size calculation", err)
		}
		defer file.Close()
		return s.calculateStrmFileSize(file)
	}

	file, err := nzbfile.Open(filePath)
	if err != nil {
		return 0, NewNonRetryableError("failed to open file for size calculation", err)
	}
	defer file.Close()
	return s.calculateNzbFileSize(file)
}

// calculateNzbFileSize calculates the total size from NZB file segments
func (s *Service) calculateNzbFileSize(r io.Reader) (int64, error) {
	n, err := nzbparser.Parse(r)
	if err != nil {
		return 0, NewNonRetryableError("failed to parse NZB XML for size calculation", err)
	}

	if len(n.Files) == 0 {
		return 0, NewNonRetryableError("NZB file contains no files", nil)
	}

	parser.SanitizeNzbFilenames(n)

	var totalSize int64
	par2Pattern := regexp.MustCompile(`(?i)\.par2$|\.p\d+$|\.vol\d+\+\d+\.par2$`)

	for _, file := range n.Files {
		// Skip PAR2 files (same logic as existing parser)
		if par2Pattern.MatchString(file.Filename) {
			continue
		}

		// Sum all segment bytes directly
		for _, segment := range file.Segments {
			totalSize += int64(segment.Bytes)
		}
	}

	return totalSize, nil
}

// calculateStrmFileSize extracts file size from STRM file NXG link
func (s *Service) calculateStrmFileSize(r io.Reader) (int64, error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && strings.HasPrefix(line, "nxglnk://") {
			u, err := url.Parse(line)
			if err != nil {
				return 0, NewNonRetryableError("invalid NXG URL in STRM file", err)
			}

			fileSizeStr := u.Query().Get("file_size")
			if fileSizeStr == "" {
				return 0, NewNonRetryableError("missing file_size parameter in NXG link", nil)
			}

			fileSize, err := strconv.ParseInt(fileSizeStr, 10, 64)
			if err != nil {
				return 0, NewNonRetryableError("invalid file_size parameter in NXG link", err)
			}

			return fileSize, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, NewNonRetryableError("failed to read STRM file for size calculation", err)
	}

	return 0, NewNonRetryableError("no valid NXG link found in STRM file", nil)
}

// RegenerateMetadata attempts to find the original NZB for a file and re-process it to fix corrupted metadata.
func (s *Service) RegenerateMetadata(ctx context.Context, mountRelativePath string) error {
	nzbFolder := s.GetNzbFolder()
	if nzbFolder == "" {
		return fmt.Errorf("NZB storage folder not configured")
	}

	// The mountRelativePath typically looks like:
	// "complete/tv/Show.Name.S01E01.1080p.WEB-DL/Show.Name.S01E01.1080p.WEB-DL.mkv"
	// or "tv/Show Name/Season 01/Show Name - S01E01.mkv"

	// Try to extract the release name. For "complete/" files, it's the folder name.
	releaseName := ""
	if strings.HasPrefix(mountRelativePath, "complete/") {
		parts := strings.Split(mountRelativePath, "/")
		if len(parts) >= 3 {
			releaseName = parts[2]
		}
	}

	if releaseName == "" {
		// Fallback: use the filename without extension
		releaseName = filepath.Base(mountRelativePath)
		releaseName = nzbtrim.TrimNzbExtension(releaseName)
	}

	s.log.InfoContext(ctx, "Attempting to regenerate metadata", "path", mountRelativePath, "release_name", releaseName)

	// Search for the NZB file in the .nzbs directory
	var foundNzbPath string
	err := filepath.WalkDir(nzbFolder, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}

		filename := d.Name()
		if !nzbtrim.HasNzbExtension(filename) {
			return nil
		}
		cleanName := nzbtrim.TrimNzbExtension(filename)
		if _, after, ok := strings.Cut(cleanName, "_"); ok {
			// Check both with and without queue ID prefix
			if after == releaseName || cleanName == releaseName {
				foundNzbPath = path
				return filepath.SkipAll
			}
		} else if cleanName == releaseName {
			foundNzbPath = path
			return filepath.SkipAll
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to search for NZB: %w", err)
	}

	if foundNzbPath == "" {
		return fmt.Errorf("could not find original NZB for release: %s", releaseName)
	}

	s.log.InfoContext(ctx, "Found original NZB, rebuilding metadata", "nzb_path", foundNzbPath)

	// Determine virtual directory based on where the file was originally (e.g. "complete/tv")
	virtualDir := filepath.ToSlash(filepath.Dir(mountRelativePath))

	// Re-process the NZB file. We use a dummy queue ID.
	// This will overwrite the existing .meta file.
	_, _, err = s.processor.ProcessNzbFile(ctx, foundNzbPath, "", 0, nil, &virtualDir, nil, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to re-process NZB: %w", err)
	}

	s.log.InfoContext(ctx, "Successfully regenerated metadata", "path", mountRelativePath)
	return nil
}

type grabbedIndexerInfo struct {
	indexer   string
	timestamp time.Time
}

// StoreGrabbedIndexer stores a downloadID to indexer mapping in-memory
func (s *Service) StoreGrabbedIndexer(downloadID string, releaseTitle string, indexer string) {
	info := grabbedIndexerInfo{
		indexer:   indexer,
		timestamp: time.Now(),
	}

	if downloadID != "" {
		s.grabbedIndexers.Store(downloadID, info)
	}
	// Sanitize and use release title as an alternative fallback key
	if releaseTitle != "" {
		cleanTitle := strings.TrimSpace(strings.ToLower(releaseTitle))
		cleanTitle = strings.TrimSuffix(cleanTitle, ".gz")
		cleanTitle = strings.TrimSuffix(cleanTitle, ".nzb")
		s.grabbedIndexers.Store(cleanTitle, info)
	}
}

// GetGrabbedIndexer retrieves a grabbed indexer by download ID or release title
// It only returns a match if it was stored within the last 24 hours.
func (s *Service) GetGrabbedIndexer(downloadID string, releaseTitle string) (string, bool) {
	now := time.Now()

	check := func(key string) (string, bool) {
		if val, ok := s.grabbedIndexers.Load(key); ok {
			if info, isInfo := val.(grabbedIndexerInfo); isInfo {
				if now.Sub(info.timestamp) < 24*time.Hour {
					return info.indexer, true
				}
				// Cleanup expired entry
				s.grabbedIndexers.Delete(key)
			}
		}
		return "", false
	}

	if downloadID != "" {
		if name, ok := check(downloadID); ok {
			return name, true
		}
	}

	if releaseTitle != "" {
		cleanTitle := strings.TrimSpace(strings.ToLower(releaseTitle))
		cleanTitle = strings.TrimSuffix(cleanTitle, ".gz")
		cleanTitle = strings.TrimSuffix(cleanTitle, ".nzb")
		if name, ok := check(cleanTitle); ok {
			return name, true
		}
	}

	return "", false
}

// runGrabbedIndexerSweeper runs in the background to clean up stale grabbed indexer mappings older than 24h
func (s *Service) runGrabbedIndexerSweeper(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pruneGrabbedIndexers()
		}
	}
}

func (s *Service) pruneGrabbedIndexers() {
	now := time.Now()
	s.grabbedIndexers.Range(func(key, val any) bool {
		if info, isInfo := val.(grabbedIndexerInfo); isInfo {
			if now.Sub(info.timestamp) >= 24*time.Hour {
				s.grabbedIndexers.Delete(key)
			}
		} else {
			s.grabbedIndexers.Delete(key)
		}
		return true
	})
}

func joinPathsMergingOverlap(parent, child string) string {
	parent = filepath.ToSlash(parent)
	child = filepath.ToSlash(child)

	parentParts := strings.Split(strings.Trim(parent, "/"), "/")
	childParts := strings.Split(strings.Trim(child, "/"), "/")

	if len(parentParts) == 1 && parentParts[0] == "" {
		if strings.HasPrefix(parent, "/") && !strings.HasPrefix(child, "/") {
			return "/" + child
		}
		return child
	}
	if len(childParts) == 1 && childParts[0] == "" {
		return parent
	}

	overlapLen := 0
	maxOverlap := len(parentParts)
	if len(childParts) < maxOverlap {
		maxOverlap = len(childParts)
	}

	for l := maxOverlap; l > 0; l-- {
		match := true
		for i := 0; i < l; i++ {
			parentIdx := len(parentParts) - l + i
			if !strings.EqualFold(parentParts[parentIdx], childParts[i]) {
				match = false
				break
			}
		}
		if match {
			overlapLen = l
			break
		}
	}

	resultParts := append(parentParts, childParts[overlapLen:]...)
	result := strings.Join(resultParts, "/")
	if strings.HasPrefix(parent, "/") {
		return "/" + result
	}
	return result
}
