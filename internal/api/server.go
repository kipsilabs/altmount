package api

import (
	"context"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/kipsilabs/altmount/internal/arrs"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem/segcache"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/kipsilabs/altmount/internal/rclone"
	"github.com/kipsilabs/altmount/internal/updater"
	"github.com/kipsilabs/altmount/internal/version"
	"golang.org/x/sync/singleflight"
)

// Config represents API server configuration
type Config struct {
	Prefix string // API path prefix (default: "/api")
}

// DefaultConfig returns default API configuration
func DefaultConfig() *Config {
	return &Config{
		Prefix: "/api",
	}
}

// Server represents the API server
type Server struct {
	config            *Config
	queueRepo         *database.Repository
	healthRepo        *database.HealthRepository
	authService       *auth.Service
	userRepo          *database.UserRepository
	configManager     ConfigManager
	metadataReader    *metadata.MetadataReader
	metadataService   *metadata.MetadataService
	nzbFilesystem     *nzbfilesystem.NzbFilesystem
	healthWorker      *health.HealthWorker
	librarySyncWorker *health.LibrarySyncWorker

	par2Repair              Par2RepairEnqueuer             // optional; nil = PAR2 repair unavailable
	par2RepairRepo          *database.Par2RepairRepository // optional; nil = repair job listing unavailable
	metadataMigrationWorker *metadata.MigrationWorker
	importerService         *importer.Service
	poolManager             pool.Manager
	arrsService             *arrs.Service
	mountService            *rclone.MountService
	startTime               time.Time
	progressBroadcaster     *progress.ProgressBroadcaster
	streamTracker           *StreamTracker
	fuseManager             *FuseManager
	cacheSource             *segcache.Source
	logFilePath             string
	migrationRepo           *database.ImportMigrationRepository
	updater                 updater.Updater
	ready                   atomic.Bool

	speedtest           *speedtestCoordinator
	speedtestOnce       sync.Once
	duplicateLookup     duplicateLookupService
	duplicateLookupOnce sync.Once

	// stremioPlayGroup coalesces concurrent Stremio plays of the same release (download once).
	stremioPlayGroup singleflight.Group
	// stremioContentGroup closes the check-then-enqueue race for the same release
	// of the same movie or episode.
	stremioContentGroup singleflight.Group
	stremioReleaseMu    sync.Mutex
	stremioReleaseRefs  map[string]stremioReleaseRef
	// stremioFailures records releases that failed without leaving a 'failed' queue row,
	// so they stop being offered in the stream list.
	stremioFailures *stremioFailureCache
}

// NewServer creates a new API server that can optionally register routes on the provided mux (for backwards compatibility)
func NewServer(
	config *Config,
	queueRepo *database.Repository,
	healthRepo *database.HealthRepository,
	authService *auth.Service,
	userRepo *database.UserRepository,
	configManager ConfigManager,
	metadataReader *metadata.MetadataReader,
	metadataService *metadata.MetadataService,
	nzbFilesystem *nzbfilesystem.NzbFilesystem,
	poolManager pool.Manager,
	importService *importer.Service,
	arrsService *arrs.Service,
	mountService *rclone.MountService,
	progressBroadcaster *progress.ProgressBroadcaster,
	streamTracker *StreamTracker,
	cacheSource *segcache.Source,
) *Server {
	if config == nil {
		config = DefaultConfig()
	}

	server := &Server{
		config:              config,
		stremioFailures:     newStremioFailureCache(),
		stremioReleaseRefs:  make(map[string]stremioReleaseRef),
		queueRepo:           queueRepo,
		healthRepo:          healthRepo,
		authService:         authService,
		userRepo:            userRepo,
		configManager:       configManager,
		metadataReader:      metadataReader,
		metadataService:     metadataService,
		nzbFilesystem:       nzbFilesystem,
		importerService:     importService, // Will be set later via SetImporterService
		poolManager:         poolManager,
		arrsService:         arrsService,
		mountService:        mountService,
		startTime:           time.Now(),
		progressBroadcaster: progressBroadcaster,
		streamTracker:       streamTracker,
		cacheSource:         cacheSource,
		speedtest:           newSpeedtestCoordinator(),
		fuseManager:         NewFuseManager(newMountFactory(nzbFilesystem, configManager, streamTracker)),
		updater:             updater.Default(),
	}

	// Wire stream-activity ↔ pool admission. Streams notify the pool when they
	// start/stop; the pool reads the active stream count to pick its
	// adaptive import cap.
	if poolManager != nil && streamTracker != nil {
		streamTracker.SetChangeNotifier(poolManager)
		poolManager.SetStreamSource(streamTracker)
	}

	return server
}

// SetHealthWorker sets the health worker reference for the server
func (s *Server) SetHealthWorker(healthWorker *health.HealthWorker) {
	s.healthWorker = healthWorker
}

// SetLibrarySyncWorker sets the library sync worker reference for the server
func (s *Server) SetLibrarySyncWorker(librarySyncWorker *health.LibrarySyncWorker) {
	s.librarySyncWorker = librarySyncWorker
}

// SetMetadataMigrationWorker sets the metadata migration worker for the server.
func (s *Server) SetMetadataMigrationWorker(worker *metadata.MigrationWorker) {
	s.metadataMigrationWorker = worker
}

// SetLogFilePath sets the path to the JSON log file used by the logs endpoints.
func (s *Server) SetLogFilePath(path string) {
	s.logFilePath = path
}

// SetMigrationRepo sets the migration repository used by the migrate-symlinks endpoint.
func (s *Server) SetMigrationRepo(repo *database.ImportMigrationRepository) {
	s.migrationRepo = repo
}

// SetReady sets the server as ready to accept requests
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// IsReady returns true if the server is ready to accept requests
func (s *Server) IsReady() bool {
	return s.ready.Load()
}

// SetupFiberRoutes configures API routes directly on the Fiber app
func (s *Server) SetupRoutes(app *fiber.App) {
	app.Use("/sabnzbd", s.handleSABnzbd)

	// Stremio addon endpoints — key-based auth, no JWT required.
	// CORS must be open (*) so Stremio can install the addon from any origin.
	stremioGroup := app.Group("/stremio", cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept",
		AllowMethods: "GET, OPTIONS",
	}))
	stremioGroup.Get("/:key/manifest.json", s.handleStremioManifest)
	stremioGroup.Get("/:key/stream/:type/:id.json", s.handleStremioAddonStream)
	stremioGroup.Get("/:key/play", s.handleStremioAddonPlay)

	api := app.Group(s.config.Prefix)

	// Public endpoints (authentication handled inside or not required)
	api.Post("/import/file", s.handleManualImportFile)
	api.Post("/arrs/webhook", s.handleArrsWebhook)
	api.Post("/nzb/streams", s.handleNzbStreams)

	cfg := s.configManager.GetConfig()

	// Apply global middleware — derive allowed CORS origins from COOKIE_DOMAIN env var,
	// explicit api.allowed_origins config, or fall back to wildcard.
	corsOrigins := "*"

	if cookieDomain := os.Getenv("COOKIE_DOMAIN"); cookieDomain != "" {
		// Allow both http and https on the configured domain
		corsOrigins = "http://" + cookieDomain + ",https://" + cookieDomain
	}

	if cfg != nil && len(cfg.API.AllowedOrigins) > 0 {
		// Explicit config overrides the env-derived value
		corsOrigins = strings.Join(cfg.API.AllowedOrigins, ",")
	}

	api.Use(cors.New(cors.Config{
		AllowOrigins:     corsOrigins,
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization",
		AllowMethods:     "GET, POST, PUT, PATCH, DELETE, OPTIONS",
		AllowCredentials: true,
	}))
	api.Use(recover.New())

	// This machine-to-machine endpoint authenticates its own API key, including
	// when interactive login is disabled. Register before the JWT middleware.
	api.Post("/files/duplicate-lookup", s.handleDuplicateLookup)

	// Apply JWT authentication middleware globally except for public auth routes
	// Only apply if login is required (default: true)
	loginRequired := true // Default to true if not set
	if cfg != nil && cfg.Auth.LoginRequired != nil {
		loginRequired = *cfg.Auth.LoginRequired
	}

	if loginRequired && s.authService != nil && s.userRepo != nil {
		tokenService := s.authService.TokenService()
		if tokenService != nil {
			// Define paths that should skip authentication
			skipPaths := []string{
				s.config.Prefix + "/auth/login",
				s.config.Prefix + "/auth/register",
				s.config.Prefix + "/auth/registration-status",
				s.config.Prefix + "/auth/config",
			}

			// Apply authentication middleware with skip paths
			api.Use(auth.RequireAuthWithSkip(tokenService, s.userRepo, skipPaths))
		}
	}

	// NZBDav Imports (now protected by JWT auth)
	api.Post("/import/nzbdav", s.handleImportNzbdav)
	api.Post("/import/nzbdav/reset", s.handleResetNzbdavImportStatus)
	api.Get("/import/nzbdav/status", s.handleGetNzbdavImportStatus)
	api.Delete("/import/nzbdav", s.handleCancelNzbdavImport)
	api.Delete("/import/nzbdav/pending-migrations", s.handleClearPendingNzbdavMigrations)
	api.Delete("/import/nzbdav/migrations", s.handleClearAllNzbdavMigrations)
	api.Post("/import/nzbdav/migrate-symlinks", s.handleMigrateNzbdavSymlinks)

	// Queue endpoints
	api.Get("/queue", s.handleListQueue)
	api.Get("/queue/stats", s.handleGetQueueStats)
	api.Get("/queue/stats/history", s.handleGetQueueHistoricalStats)
	// Note: /queue/stream and /health/stream are served by ServeQueueSSE/ServeHealthSSE
	// at the HTTP server level (setup.go) — bypasses adaptor.FiberApp for correct SSE streaming.
	api.Delete("/queue/completed", s.handleClearCompletedQueue)
	api.Delete("/queue/failed", s.handleClearFailedQueue)
	api.Delete("/queue/pending", s.handleClearPendingQueue)
	api.Delete("/queue/bulk", s.handleDeleteQueueBulk)
	api.Post("/queue/bulk/restart", s.handleRestartQueueBulk)
	api.Post("/queue/bulk/cancel", s.handleCancelQueueBulk)
	api.Patch("/queue/bulk/priority", s.handleBulkUpdateQueuePriority)
	api.Post("/queue/upload", s.handleUploadToQueue)
	api.Post("/queue/upload-nzblnk", s.handleUploadNZBLnk)
	api.Post("/queue/upload-by-name", s.handleSearchNZBByName)
	api.Post("/queue/test", s.handleAddTestQueueItem)
	api.Get("/queue/:id", s.handleGetQueue)
	api.Delete("/queue/:id", s.handleDeleteQueue)
	api.Post("/queue/:id/retry", s.handleRetryQueue)
	api.Post("/queue/:id/cancel", s.handleCancelQueue)
	api.Patch("/queue/:id/priority", s.handleUpdateQueueItemPriority)
	api.Get("/queue/:id/download", s.handleDownloadNZB)

	// Health endpoints
	api.Get("/health", s.handleListHealth)
	api.Post("/par2repair", s.handlePar2Repair)
	api.Get("/par2repair", s.handleListPar2Repair)
	api.Delete("/par2repair", s.handleCancelAllPar2Repair)
	api.Delete("/par2repair/:id", s.handleCancelPar2Repair)
	api.Post("/health/bulk/delete", s.handleDeleteHealthBulk)
	api.Post("/health/bulk/restart", s.handleRestartHealthChecksBulk)
	api.Post("/health/bulk/repair", s.handleRepairHealthBulk)
	api.Get("/health/corrupted", s.handleListCorrupted)
	api.Get("/health/stats", s.handleGetHealthStats)
	api.Delete("/health/cleanup", s.handleCleanupHealth)
	api.Post("/health/reset-all", s.handleResetAllHealthChecks)
	api.Post("/health/regenerate-symlinks", s.handleRegenerateLibraryFiles)
	api.Post("/health/check", s.handleAddHealthCheck)
	api.Get("/health/worker/status", s.handleGetHealthWorkerStatus)
	api.Post("/health/:id/repair", s.handleRepairHealth)
	api.Post("/health/:id/unmask", s.handleUnmaskHealth)
	api.Post("/health/:id/check-now", s.handleDirectHealthCheck)
	api.Post("/health/:id/priority", s.handleSetHealthPriority)
	api.Post("/health/:id/cancel", s.handleCancelHealthCheck)
	api.Get("/health/:id", s.handleGetHealth)
	api.Delete("/health/:id", s.handleDeleteHealth)

	// Library sync endpoints
	api.Get("/health/library-sync/status", s.handleGetLibrarySyncStatus)
	api.Get("/health/library-sync/needed", s.handleGetSyncNeeded)
	api.Post("/health/library-sync/start", s.handleStartLibrarySync)
	api.Post("/health/library-sync/cancel", s.handleCancelLibrarySync)
	api.Post("/health/library-sync/dry-run", s.handleDryRunLibrarySync)
	api.Post("/health/library-audit/scan", s.handleLibraryAuditScan)
	api.Post("/health/library-audit/cleanup", s.handleLibraryAuditCleanup)

	api.Get("/metadata/corrupted", s.handleGetCorruptedMetadata)
	api.Delete("/metadata/corrupted", s.handlePurgeCorruptedMetadata)
	api.Get("/metadata/migration/status", s.handleGetMetadataMigrationStatus)
	api.Post("/metadata/migration/dry-run", s.handleDryRunMetadataMigration)
	api.Post("/metadata/migration/start", s.handleStartMetadataMigration)
	api.Post("/metadata/migration/cancel", s.handleCancelMetadataMigration)

	api.Get("/files/info", s.handleGetFileMetadata)
	api.Get("/files/active-streams", s.handleGetActiveStreams)
	api.Delete("/files/active-streams/:id", s.handleKillStream)
	api.Get("/files/streams/history", s.handleGetStreamHistory)
	api.Get("/files/export-nzb", s.handleExportMetadataToNZB)
	api.Post("/files/export-batch", s.handleBatchExportNZB)
	// Note: /files/stream is handled by StreamHandler at HTTP server level

	api.Post("/import/scan", s.handleStartManualScan)
	api.Get("/logs", s.handleGetLogs)
	// Note: /logs/stream is handled by ServeLogsSSE at HTTP server level (bypasses adaptor)

	api.Get("/import/scan/status", s.handleGetScanStatus)
	api.Delete("/import/scan", s.handleCancelScan)
	api.Get("/import/history", s.handleGetImportHistory)
	api.Delete("/import/history", s.handleClearImportHistory)
	// System endpoints
	api.Get("/system/stats", s.handleGetSystemStats)
	api.Get("/system/health", s.handleGetSystemHealth)
	api.Get("/system/browse", s.handleSystemBrowse)
	api.Get("/system/pool/metrics", s.handleGetPoolMetrics)
	api.Get("/system/provider-stats", s.handleGetProviderHistoricalStats)
	api.Get("/system/provider-speed-history", s.handleGetProviderSpeedHistory)
	api.Get("/system/indexer-stats", s.handleGetIndexerStats)
	api.Delete("/system/indexer-stats/cleanup", s.handleCleanupIndexerStats)
	api.Post("/system/stats/reset", s.handleResetSystemStats)
	api.Post("/system/cleanup", s.handleSystemCleanup)
	api.Post("/system/restart", s.handleSystemRestart)

	// Update endpoints
	api.Get("/system/update/status", s.handleGetUpdateStatus)
	api.Post("/system/update/apply", s.handleApplyUpdate)

	api.Get("/config", s.handleGetConfig)
	api.Put("/config", s.handleUpdateConfig)
	api.Patch("/config/:section", s.handlePatchConfigSection)
	api.Post("/config/reload", s.handleReloadConfig)
	api.Post("/config/validate", s.handleValidateConfig)

	// Stremio helper endpoints
	api.Post("/prowlarr/indexers", s.handleListProwlarrIndexers)
	api.Post("/stremio/indexers/newsnab/test", s.handleTestNewsnabIndexer)
	api.Get("/stremio/useragents", s.handleGetStremioUserAgents)
	api.Post("/stremio/useragents/refresh", s.handleRefreshStremioUserAgents)
	api.Post("/stremio/search/inspect", s.handleInspectStremioSearch)
	api.Get("/stremio/display/presets", s.handleGetStremioDisplayPresets)
	api.Post("/stremio/display/preview", s.handleStremioDisplayPreview)
	api.Post("/stremio/scoring/evaluate", s.handleEvaluateStremioRelease)

	// FUSE endpoints
	api.Post("/fuse/start", s.handleStartFuseMount)
	api.Post("/fuse/stop", s.handleStopFuseMount)
	api.Post("/fuse/force-stop", s.handleForceStopFuseMount)
	api.Get("/fuse/status", s.handleGetFuseStatus)

	// Provider management endpoints
	api.Get("/providers/backbones", s.handleProviderBackbones)
	api.Post("/providers/test", s.handleTestProvider)
	api.Post("/providers/:id/speedtest", s.handleTestProviderSpeed)
	api.Post("/providers/:id/tune-pipeline", s.handleTunePipeline)
	api.Post("/providers", s.handleCreateProvider)
	api.Put("/providers/reorder", s.handleReorderProviders)
	api.Put("/providers/:id", s.handleUpdateProvider)
	api.Post("/providers/:id/reset-quota", s.handleResetProviderQuota)
	api.Delete("/providers/:id", s.handleDeleteProvider)

	// Configuration-based instance endpoints
	api.Get("/arrs/instances", s.handleListArrsInstances)
	api.Get("/arrs/instances/:type/:name", s.handleGetArrsInstance)
	api.Post("/arrs/instances/test", s.handleTestArrsConnection)
	api.Get("/arrs/stats", s.handleGetArrsStats)
	api.Get("/arrs/health", s.handleGetArrsHealth)
	api.Post("/arrs/webhook/register", s.handleRegisterArrsWebhooks)
	api.Post("/arrs/download-client/register", s.handleRegisterArrsDownloadClients)
	api.Post("/arrs/download-client/test", s.handleTestArrsDownloadClients)

	// Direct authentication endpoints — rate-limited to prevent brute-force attacks
	authLimiter := limiter.New(limiter.Config{
		Max:        10,
		Expiration: 1 * time.Minute,
		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP()
		},
		LimitReached: func(c *fiber.Ctx) error {
			return RespondError(c, fiber.StatusTooManyRequests, "TOO_MANY_REQUESTS",
				"Too many login attempts. Please wait a minute before trying again.", "")
		},
	})
	api.Post("/auth/login", authLimiter, s.handleDirectLogin)
	api.Post("/auth/register", authLimiter, s.handleRegister)
	api.Post("/auth/reset-admin-password", authLimiter, s.handleResetAdminPassword)
	api.Get("/auth/registration-status", s.handleCheckRegistration)
	api.Get("/auth/config", s.handleGetAuthConfig)

	// Protected API endpoints for user management (authentication already handled globally)
	api.Get("/user", s.handleAuthUser)
	api.Post("/user/refresh", s.handleAuthRefresh)
	api.Post("/user/logout", s.handleAuthLogout)
	api.Post("/user/api-key/regenerate", s.handleRegenerateAPIKey)
	api.Put("/user/password", s.handleChangeOwnPassword)
}

// Shutdown shuts down the API server and its managed resources
func (s *Server) Shutdown(ctx context.Context) {
	if s.fuseManager != nil {
		s.fuseManager.Stop()
	}
	if s.speedtest != nil {
		s.speedtest.shutdown()
	}
}

// handleGetActiveStreams handles GET /api/files/active-streams
//
//	@Summary		List active streams
//	@Description	Returns all currently active NZB file streams. Optionally filter by type=file.
//	@Tags			Files
//	@Produce		json
//	@Param			type	query		string	false	"Filter by source type (e.g. file)"
//	@Success		200		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/files/active-streams [get]
func (s *Server) handleGetActiveStreams(c *fiber.Ctx) error {
	if s.streamTracker == nil {
		return RespondSuccess(c, []nzbfilesystem.ActiveStream{})
	}

	streams := s.streamTracker.GetAll()

	// Check for filter parameter
	filterType := c.Query("type") // e.g., type=file

	if filterType == "file" {
		filteredStreams := make([]nzbfilesystem.ActiveStream, 0)
		for _, stream := range streams {
			// Assuming "API" and "WebDAV" are the desired "file being streams"
			if stream.Source == "API" || stream.Source == "WebDAV" {
				filteredStreams = append(filteredStreams, stream)
			}
		}
		streams = filteredStreams
	}

	return RespondSuccess(c, streams)
}

// getSystemInfo returns current system information
func (s *Server) getSystemInfo() SystemInfoResponse {
	uptime := time.Since(s.startTime)
	return SystemInfoResponse{
		Version:   version.Version,
		GitCommit: version.GitCommit,
		StartTime: s.startTime,
		Uptime:    uptime.String(),
		GoVersion: runtime.Version(),
	}
}

// checkSystemHealth performs a basic health check
func (s *Server) checkSystemHealth(ctx context.Context) SystemHealthResponse {
	components := make(map[string]ComponentHealth)
	overallStatus := "healthy"

	// Check database connectivity
	if _, err := s.queueRepo.GetQueueStats(ctx); err != nil {
		components["database"] = ComponentHealth{
			Status:  "unhealthy",
			Message: "Database connection failed",
			Details: err.Error(),
		}
		overallStatus = "unhealthy"
	} else {
		components["database"] = ComponentHealth{
			Status:  "healthy",
			Message: "Database connection OK",
		}
	}

	// Check health repository
	if _, err := s.healthRepo.GetHealthStats(ctx); err != nil {
		components["health_repository"] = ComponentHealth{
			Status:  "unhealthy",
			Message: "Health repository failed",
			Details: err.Error(),
		}
		if overallStatus != "unhealthy" {
			overallStatus = "degraded"
		}
	} else {
		components["health_repository"] = ComponentHealth{
			Status:  "healthy",
			Message: "Health repository OK",
		}
	}

	return SystemHealthResponse{
		Status:     overallStatus,
		Timestamp:  time.Now(),
		Components: components,
	}
}

// Library sync handler methods

// handleGetLibrarySyncStatus handles GET /api/health/library-sync/status
//
//	@Summary		Get library sync status
//	@Description	Returns the current status of the library sync worker.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/library-sync/status [get]
func (s *Server) handleGetLibrarySyncStatus(c *fiber.Ctx) error {
	if s.librarySyncWorker == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Library sync worker not available",
		})
	}

	handlers := NewLibrarySyncHandlers(s.librarySyncWorker, s.configManager)
	return handlers.handleGetLibrarySyncStatus(c)
}

// handleStartLibrarySync handles POST /api/health/library-sync/start
//
//	@Summary		Start library sync
//	@Description	Triggers a library sync operation to reconcile the file system with the database.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/library-sync/start [post]
func (s *Server) handleStartLibrarySync(c *fiber.Ctx) error {
	if s.librarySyncWorker == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Library sync worker not available",
		})
	}

	handlers := NewLibrarySyncHandlers(s.librarySyncWorker, s.configManager)
	return handlers.handleStartLibrarySync(c)
}

// handleCancelLibrarySync handles POST /api/health/library-sync/cancel
//
//	@Summary		Cancel library sync
//	@Description	Cancels an in-progress library sync operation.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/library-sync/cancel [post]
func (s *Server) handleCancelLibrarySync(c *fiber.Ctx) error {
	if s.librarySyncWorker == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Library sync worker not available",
		})
	}

	handlers := NewLibrarySyncHandlers(s.librarySyncWorker, s.configManager)
	return handlers.handleCancelLibrarySync(c)
}

// handleLibraryAuditScan handles POST /api/health/library-audit/scan
func (s *Server) handleLibraryAuditScan(c *fiber.Ctx) error {
	if s.librarySyncWorker == nil {
		return RespondServiceUnavailable(c, "Library sync is not available", "")
	}
	return NewLibrarySyncHandlers(s.librarySyncWorker, s.configManager).handleLibraryAuditScan(c)
}

// handleLibraryAuditCleanup handles POST /api/health/library-audit/cleanup
func (s *Server) handleLibraryAuditCleanup(c *fiber.Ctx) error {
	if s.librarySyncWorker == nil {
		return RespondServiceUnavailable(c, "Library sync is not available", "")
	}
	return NewLibrarySyncHandlers(s.librarySyncWorker, s.configManager).handleLibraryAuditCleanup(c)
}

// handleDryRunLibrarySync handles POST /api/health/library-sync/dry-run
//
//	@Summary		Dry-run library sync
//	@Description	Simulates a library sync and returns what would be changed without applying it.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	APIResponse{data=DryRunSyncResult}
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/library-sync/dry-run [post]
func (s *Server) handleDryRunLibrarySync(c *fiber.Ctx) error {
	if s.librarySyncWorker == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Library sync worker not available",
		})
	}

	handlers := NewLibrarySyncHandlers(s.librarySyncWorker, s.configManager)
	return handlers.handleDryRunLibrarySync(c)
}

// handleGetSyncNeeded handles GET /api/health/library-sync/needed
//
//	@Summary		Check if library sync is needed
//	@Description	Returns whether a library sync is needed based on current configuration state.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/library-sync/needed [get]
func (s *Server) handleGetSyncNeeded(c *fiber.Ctx) error {
	handlers := NewLibrarySyncHandlers(s.librarySyncWorker, s.configManager)
	return handlers.handleGetSyncNeeded(c)
}

// handleKillStream handles DELETE /api/files/active-streams/:id
//
//	@Summary		Kill active stream
//	@Description	Terminates an active NZB file stream by ID.
//	@Tags			Files
//	@Produce		json
//	@Param			id	path		string	true	"Stream ID"
//	@Success		200	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/files/active-streams/{id} [delete]
func (s *Server) handleKillStream(c *fiber.Ctx) error {
	id := c.Params("id")
	if s.streamTracker == nil {
		return c.Status(404).JSON(fiber.Map{
			"success": false,
			"message": "Stream tracker not available",
		})
	}

	if s.streamTracker.KillStream(id) {
		return c.JSON(fiber.Map{
			"success": true,
			"message": "Stream termination requested",
		})
	}

	return c.Status(404).JSON(fiber.Map{
		"success": false,
		"message": "Stream not found or cannot be killed",
	})
}

// handleGetStreamHistory handles GET /api/files/streams/history
//
//	@Summary		Get stream history
//	@Description	Returns a history of recently completed NZB file streams.
//	@Tags			Files
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/files/streams/history [get]
func (s *Server) handleGetStreamHistory(c *fiber.Ctx) error {
	if s.streamTracker == nil {
		return c.JSON(fiber.Map{
			"success": true,
			"data":    []nzbfilesystem.ActiveStream{},
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"data":    s.streamTracker.GetHistory(),
	})
}

// Metadata migration handler methods

// handleGetMetadataMigrationStatus handles GET /api/metadata/migration/status
//
//	@Summary		Get metadata migration status
//	@Description	Returns the status of the legacy metadata → v3 migration worker.
//	@Tags			Metadata
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/metadata/migration/status [get]
func (s *Server) handleGetMetadataMigrationStatus(c *fiber.Ctx) error {
	if s.metadataMigrationWorker == nil {
		return RespondServiceUnavailable(c, "Metadata migration worker not available", "")
	}
	return NewMetadataMigrationHandlers(s.metadataMigrationWorker).handleGetStatus(c)
}

// handleDryRunMetadataMigration handles POST /api/metadata/migration/dry-run
//
//	@Summary		Dry-run the metadata migration
//	@Description	Converts against an isolated temporary root and reports measured savings without touching the library.
//	@Tags			Metadata
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		409	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/metadata/migration/dry-run [post]
func (s *Server) handleDryRunMetadataMigration(c *fiber.Ctx) error {
	if s.metadataMigrationWorker == nil {
		return RespondServiceUnavailable(c, "Metadata migration worker not available", "")
	}
	return NewMetadataMigrationHandlers(s.metadataMigrationWorker).handleDryRun(c)
}

// handleStartMetadataMigration handles POST /api/metadata/migration/start
//
//	@Summary		Start the metadata migration
//	@Description	Rewrites legacy .meta files to the v3 store-backed format in place.
//	@Tags			Metadata
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		409	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/metadata/migration/start [post]
func (s *Server) handleStartMetadataMigration(c *fiber.Ctx) error {
	if s.metadataMigrationWorker == nil {
		return RespondServiceUnavailable(c, "Metadata migration worker not available", "")
	}
	return NewMetadataMigrationHandlers(s.metadataMigrationWorker).handleStart(c)
}

// handleCancelMetadataMigration handles POST /api/metadata/migration/cancel
//
//	@Summary		Cancel the metadata migration
//	@Description	Stops an in-flight migration after the current file.
//	@Tags			Metadata
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/metadata/migration/cancel [post]
func (s *Server) handleCancelMetadataMigration(c *fiber.Ctx) error {
	if s.metadataMigrationWorker == nil {
		return RespondServiceUnavailable(c, "Metadata migration worker not available", "")
	}
	return NewMetadataMigrationHandlers(s.metadataMigrationWorker).handleCancel(c)
}
