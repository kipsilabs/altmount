package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"github.com/kipsilabs/altmount/frontend"
	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/arrs"
	"github.com/kipsilabs/altmount/internal/arrs/registrar"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem/segcache"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/kipsilabs/altmount/internal/rclone"
	"github.com/kipsilabs/altmount/internal/slogutil"
	"github.com/kipsilabs/altmount/internal/stremio"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/kipsilabs/altmount/internal/webdav"
	"github.com/spf13/cobra"
)

// For development, serve static files from disk
// In production, these would be embedded
var frontendBuildPath = "/app/frontend/dist"

func init() {
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the AltMount WebDAV server",
		Long:  `Start the AltMount WebDAV server using configuration from YAML file.`,
		RunE:  runServe,
	}

	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	// 1. Load and validate configuration
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		return err
	}

	if err := cfg.ValidateDirectories(); err != nil {
		slog.Error("directory validation failed", "err", err)
		return err
	}

	// Setup logging
	logger, dynamicLeveler := slogutil.SetupLogRotationWithFallback(cfg.Log, cfg.Log.Level)
	slog.SetDefault(logger)

	// 2. Create context and managers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configManager := config.NewManager(cfg, configFile)

	// 3. Initialize core services
	db, err := initializeDatabase(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		logger.Info("Closing database")
		if err := db.Close(); err != nil {
			logger.Error("failed to close database", "err", err)
		}
	}()

	db.StartCheckpointLoop(ctx, 5*time.Minute)

	repos := setupRepositories(ctx, db)
	poolManager := pool.NewManager(ctx, repos.MainRepo)

	metadataService, metadataReader := initializeMetadata(cfg)

	// 4. Setup network services
	if err := setupNNTPPool(ctx, cfg, poolManager); err != nil {
		return err
	}
	defer func() {
		logger.Info("Clearing NNTP pool")
		if err := poolManager.ClearPool(); err != nil {
			logger.Error("failed to clear NNTP pool", "err", err)
		}
	}()

	mountService := rclone.NewMountService(configManager)

	var rcloneRCClient = setupRCloneClient(ctx, cfg, configManager)
	if cfg.RClone.MountEnabled != nil && *cfg.RClone.MountEnabled {
		rcloneRCClient = mountService.GetManager()
	}

	// 5. Initialize importer and filesystem

	arrsService := arrs.NewService(configManager.GetConfigGetter(), configManager, repos.UserRepo, repos.MainRepo)

	// Create progress broadcaster for WebSocket progress updates
	progressBroadcaster := progress.NewProgressBroadcaster()
	defer progressBroadcaster.Close()

	// Create stream tracker for monitoring active streams
	streamTracker := api.NewStreamTracker(poolManager)
	defer streamTracker.Stop()

	importerService, err := initializeImporter(ctx, cfg, metadataService, db, poolManager, rcloneRCClient, configManager.GetConfigGetter(), progressBroadcaster, repos.UserRepo, repos.HealthRepo)
	if err != nil {
		return err
	}
	// Wire ARRs service into importer for instant import triggers
	importerService.SetArrsService(arrsService)
	importerService.RegisterConfigChangeHandler(configManager)
	defer func() {
		logger.Info("Closing importer service")
		if err := importerService.Close(); err != nil {
			logger.Error("failed to close importer service", "err", err)
		}
	}()

	// Stop RSS requests before closing the importer or database, including on
	// early startup errors. The parent context also stops it during shutdown.
	rssCtx, stopRSS := context.WithCancel(ctx)
	rssDone := make(chan struct{})
	go func() {
		defer close(rssDone)
		importer.NewRSSPoller(importerService, configManager.GetConfigGetter()).Run(rssCtx)
	}()
	defer func() { stopRSS(); <-rssDone }()

	// Initialize segment cache source — encapsulates atomic manager swap and enabled-flag check.
	cacheSource := segcache.NewSource(configManager.GetConfigGetter())
	if initialCache := initializeSegmentCache(ctx, cfg, cacheSource); initialCache != nil {
		defer initialCache.Stop()
	}
	applySoftMemoryLimit(ctx, cfg)
	// Keep the memory tier from pushing the live heap over the soft limit:
	// under GC pressure the governor shrinks it, then restores it when calm.
	go cacheSource.RunPressureGovernor(ctx)
	importerService.SetSegmentStore(cacheSource.Store)

	// Background PAR2 repair: repairs missing articles and serves the patched
	// payloads on the read path's hole branch.
	par2RepairService := startPar2RepairService(ctx, cfg, repos.Par2RepairRepo, repos.HealthRepo, metadataService, poolManager, configManager.GetConfigGetter(), func() bool {
		return streamTracker.ActiveStreams() > 0
	})

	// Let degraded imports queue a PAR2 repair (opt-in via repair_on_import),
	// count locally repaired articles as available during the availability
	// sweep, and serve repaired payloads to every reader that has no hooks of
	// its own (the import path builds readers deep inside the parser).
	importerService.SetRepairEnqueuer(par2RepairService)
	importerService.SetPatchIndex(par2RepairService.PatchStore())
	par2RepairService.SetImportResumer(importerService)
	usenet.SetDefaultPatchLookup(func(segID string) []byte {
		p, ok := par2RepairService.PatchStore().Get(strings.Trim(segID, "<>"))
		if !ok {
			return nil
		}
		return p
	})

	fs := initializeFilesystem(ctx, metadataService, repos.HealthRepo, arrsService, rcloneRCClient, poolManager, configManager.GetConfigGetter(), streamTracker, cacheSource, par2RepairService)
	importerService.SetContentVerifyFilesystem(fs)

	// 6. Setup web services
	app, debugMode := createFiberApp(ctx, cfg)
	loginRequired := cfg.Auth.LoginRequired != nil && *cfg.Auth.LoginRequired
	authService, err := setupAuthService(ctx, cfg, repos.UserRepo, loginRequired)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize authentication service", "err", err)
		return err
	}

	streamTracker.StartCleanup(ctx) // Periodic cleanup of stale streams

	stremioCleanup := stremio.NewStremioCleanupService(repos.MainRepo, metadataService, configManager.GetConfigGetter())
	stremioCleanup.StartCleanup(ctx)

	apiServer := setupAPIServer(app, repos, authService, configManager, metadataReader, metadataService, fs, poolManager, importerService, arrsService, mountService, progressBroadcaster, streamTracker, cacheSource)
	apiServer.SetLogFilePath(slogutil.GetLogFilePath(cfg.Log))
	apiServer.SetMigrationRepo(db.MigrationRepo)

	webdavHandler, err := setupWebDAV(cfg, fs, authService, repos.UserRepo, configManager, streamTracker)
	if err != nil {
		return err
	}

	// Create stream handler for file streaming
	streamHandler := setupStreamHandler(fs, repos.UserRepo, streamTracker)

	// Setup SPA routes
	setupSPARoutes(app)

	// 7. Register config change handlers
	pool.RegisterConfigHandlers(ctx, configManager, poolManager)
	webdav.RegisterConfigHandlers(ctx, configManager, webdavHandler)
	api.RegisterLogLevelHandler(ctx, configManager, debugMode, dynamicLeveler)
	apiServer.RegisterFuseConfigChangeHandler(configManager)

	// Register segment cache config change handler for dynamic path/size/expiry changes.
	// Enable/disable toggles take effect automatically via cacheSource.Store() at file-open time.
	configManager.OnConfigChange(func(oldConfig, newConfig *config.Config) {
		if oldConfig.SoftMemoryLimit("") != newConfig.SoftMemoryLimit("") {
			applySoftMemoryLimit(ctx, newConfig)
		}
		structuralChange := oldConfig.SegmentCache.CachePath != newConfig.SegmentCache.CachePath ||
			oldConfig.SegmentCache.MaxSizeGB != newConfig.SegmentCache.MaxSizeGB ||
			intPtrValue(oldConfig.SegmentCache.ExpiryHours) != intPtrValue(newConfig.SegmentCache.ExpiryHours)

		if !structuralChange {
			return
		}

		// Stop old manager and swap in a new one for path/size/expiry changes.
		if oldMgr := cacheSource.Manager(); oldMgr != nil {
			oldMgr.Stop()
			cacheSource.Swap(nil)
		}

		newMgr := initializeSegmentCache(context.Background(), newConfig, cacheSource)
		if newMgr != nil {
			logger.InfoContext(ctx, "Segment cache reinitialized dynamically")
		}
	})

	healthWorker, librarySyncWorker, err := startHealthWorker(ctx, cfg, metadataService, repos.HealthRepo, poolManager, configManager, rcloneRCClient, arrsService, importerService, progressBroadcaster, par2RepairService, fs)
	if err != nil {
		logger.Warn("Health worker initialization failed", "err", err)
	}
	if healthWorker != nil {
		apiServer.SetHealthWorker(healthWorker)
	}
	if par2RepairService != nil {
		apiServer.SetPar2RepairEnqueuer(par2RepairService)
	}
	if repos.Par2RepairRepo != nil {
		apiServer.SetPar2RepairRepo(repos.Par2RepairRepo)
	}
	if librarySyncWorker != nil {
		apiServer.SetLibrarySyncWorker(librarySyncWorker)
	}

	// Legacy metadata → v3 store migration. Manually triggered from the panel;
	// creating the worker starts nothing.
	apiServer.SetMetadataMigrationWorker(
		metadata.NewMigrationWorker(metadataService, configManager.GetConfigGetter()),
	)

	// Register health system config change handler for dynamic enable/disable
	if healthWorker != nil && librarySyncWorker != nil {
		healthController := health.NewHealthSystemController(healthWorker, librarySyncWorker)
		healthController.RegisterConfigChangeHandler(ctx, configManager)

		// Trigger initial metadata date sync if health is enabled
		if cfg.Health.Enabled != nil && *cfg.Health.Enabled {
			healthController.SyncMetadataDates(ctx)
		}
	}

	// Start ARRs queue cleanup worker
	if err := arrsService.StartWorker(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to start ARR queue cleanup worker", "error", err)
	}
	arrsService.RegisterConfigChangeHandler(ctx, configManager)

	// Start metadata backup worker
	metadataBackupWorker := metadata.NewBackupWorker(configManager.GetConfigGetter())
	if err := metadataBackupWorker.Start(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to start metadata backup worker", "error", err)
	}

	// ARRs service status logging
	if cfg.Arrs.Enabled != nil && *cfg.Arrs.Enabled {
		logger.InfoContext(ctx, "Arrs service ready for health monitoring and repair")
	} else {
		logger.InfoContext(ctx, "Arrs service is disabled in configuration")
	}

	// 9. Create HTTP server
	customServer := createHTTPServer(apiServer, app, webdavHandler, streamHandler, cfg.WebDAV.Port, configManager.GetConfigGetter())

	logger.Info("AltMount server started",
		"port", cfg.WebDAV.Port,
		"webdav_path", "/webdav",
		"api_path", "/api",
		"providers", len(cfg.Providers),
		"processor_workers", cfg.Import.MaxProcessorWorkers)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)

	// Start custom server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		if err := customServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ErrorContext(ctx, "Custom server error", "error", err)
			serverErr <- err
		}
	}()

	// Trigger automatic webhook registration after server is up
	go func() {
		// Wait for server to be fully ready
		time.Sleep(5 * time.Second)

		// Use a fresh background context
		bgCtx := context.Background()

		// Find an API key to use for the webhook
		// If we don't have one, we can't register securely
		apiKey := ""
		// Internal helper to get first admin key
		// We'll use the one we just added to arrsService
		if key := arrsService.GetFirstAdminAPIKey(bgCtx); key != "" {
			apiKey = key
		}

		if apiKey != "" {
			logger.InfoContext(bgCtx, "Triggering automatic ARR webhook registration", "webhook_url", registrar.RedactWebhookURLForLog(cfg.GetWebhookBaseURL()))
			if err := arrsService.EnsureWebhookRegistration(bgCtx, cfg.GetWebhookBaseURL(), apiKey); err != nil {
				logger.ErrorContext(bgCtx, "Failed to register ARR webhooks on startup", "error", err)
			}
		} else {
			logger.WarnContext(bgCtx, "No admin API key found, skipping automatic webhook registration")
		}
	}()

	// Start mount service after HTTP server is running
	// This ensures the WebDAV server is ready to accept connections
	go func() {
		// Wait for HTTP server to be fully ready by polling the liveness endpoint
		if err := waitForHTTPServer(ctx, cfg.WebDAV.Port); err != nil {
			logger.WarnContext(ctx, "HTTP server did not become ready, starting mount service anyway", "err", err)
		} else {
			logger.InfoContext(ctx, "HTTP server is ready, starting mount service")
		}

		if err := startMountService(ctx, cfg, mountService, logger); err != nil {
			logger.WarnContext(ctx, "Mount service failed to start", "err", err)
		}

		// Auto-start FUSE mount if enabled
		apiServer.AutoStartFuse()
	}()

	// Signal that the server is ready
	apiServer.SetReady(true)

	// Wait for shutdown signal or server error
	select {
	case sig := <-sigChan:
		logger.InfoContext(ctx, "Received shutdown signal", "signal", sig.String())
		cancel() // Cancel context to signal all services to stop
	case err := <-serverErr:
		logger.ErrorContext(ctx, "Server error, shutting down", "error", err)
		cancel()
	case <-ctx.Done():
		logger.InfoContext(ctx, "Context cancelled, shutting down")
	}

	<-rssDone

	// Start graceful shutdown sequence
	logger.InfoContext(ctx, "Starting graceful shutdown sequence")

	// Stop the importer first so it stops claiming and processing new queue
	// items immediately. Without this, the importer keeps running (it was
	// only ever closed via a deferred Close() after runServe returns) for
	// the full duration of the remaining shutdown steps below, including
	// the HTTP server's graceful-shutdown timeout.
	if err := importerService.Stop(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to stop importer service", "error", err)
	} else {
		logger.InfoContext(ctx, "Importer service stopped")
	}

	// Shutdown API server and its managed resources (like FUSE)
	apiServer.Shutdown(ctx)

	// Stop health worker if running
	if healthWorker != nil {
		if err := healthWorker.Stop(ctx); err != nil {
			logger.ErrorContext(ctx, "Failed to stop health worker", "error", err)
		} else {
			logger.InfoContext(ctx, "Health worker stopped")
		}
	}

	// Stop ARRs queue cleanup worker
	arrsService.StopWorker(ctx)

	// Stop metadata backup worker
	metadataBackupWorker.Stop(ctx)

	// Stop RClone mount service if running
	if cfg.RClone.MountEnabled != nil && *cfg.RClone.MountEnabled {
		if err := mountService.Stop(ctx); err != nil {
			logger.ErrorContext(ctx, "Failed to stop mount service", "error", err)
		} else {
			logger.InfoContext(ctx, "RClone mount service stopped")
		}
	}

	// Shutdown custom server with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	logger.InfoContext(ctx, "Shutting down server...")
	if err := customServer.Shutdown(shutdownCtx); err != nil {
		logger.ErrorContext(ctx, "Error shutting down server", "error", err)
		return err
	}
	logger.InfoContext(ctx, "Server shutdown completed")

	logger.InfoContext(ctx, "AltMount server shutdown completed successfully")
	return nil
}

// handleFiberHealth provides a lightweight liveness check endpoint for Docker using Fiber
func handleFiberHealth(c *fiber.Ctx) error {
	response := map[string]any{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	return c.JSON(response)
}

// setupSPARoutes configures Fiber SPA routing for the frontend
func setupSPARoutes(app *fiber.App) {
	// Determine frontend build path
	frontendPath := frontendBuildPath
	if _, err := os.Stat(frontendBuildPath); err != nil {
		// Development mode - serve from disk
		frontendPath = "./frontend/dist"
	}

	// Cli mode - use embedded filesystem
	//nolint:staticcheck
	buildFS, err := frontend.GetBuildFS()
	if err != nil { //nolint:staticcheck
		// Docker or development - serve static files with SPA fallback
		app.All("/*", filesystem.New(filesystem.Config{
			Root:         http.Dir(frontendPath),
			NotFoundFile: "index.html",
			Index:        "index.html",
		}))
	} else {
		// For embedded filesystem, we'll handle it differently below
		app.All("/*", filesystem.New(filesystem.Config{
			Root:         http.FS(buildFS),
			NotFoundFile: "index.html",
			Index:        "index.html",
		}))

		return
	}
}

func waitForHTTPServer(ctx context.Context, port int) error {
	client := &http.Client{
		Timeout: 500 * time.Millisecond,
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/live", port)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Wait up to 30 seconds
	timeout := time.After(30 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for HTTP server on port %d to start", port)
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				continue
			}
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return nil
				}
			}
		}
	}
}

// intPtrValue returns the value pointed to by p, or 0 when p is nil. It is used
// to compare optional integer config fields (e.g. segment cache ExpiryHours) by
// value rather than by pointer identity.
func intPtrValue(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}
