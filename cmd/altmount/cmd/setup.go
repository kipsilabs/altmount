package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-pkgz/auth/v2/token"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	fLogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/arrs"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/contentverify"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem/segcache"
	"github.com/kipsilabs/altmount/internal/par2repair"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/kipsilabs/altmount/internal/rclone"
	"github.com/kipsilabs/altmount/internal/webdav"
	"github.com/kipsilabs/altmount/pkg/rclonecli"
)

// repositorySet holds all database repositories
type repositorySet struct {
	MainRepo       *database.Repository
	HealthRepo     *database.HealthRepository
	UserRepo       *database.UserRepository
	Par2RepairRepo *database.Par2RepairRepository
}

// initializeDatabase creates and initializes the database
func initializeDatabase(ctx context.Context, cfg *config.Config) (*database.DB, error) {
	dbConfig := database.Config{
		Type:         cfg.Database.Type,
		DatabasePath: cfg.Database.Path,
		DSN:          cfg.Database.DSN,
	}

	db, err := database.NewDB(dbConfig)
	if err != nil {
		slog.ErrorContext(ctx, "failed to initialize database", "err", err)
		return nil, err
	}

	return db, nil
}

// initializeMetadata creates metadata service and reader
func initializeMetadata(cfg *config.Config) (*metadata.MetadataService, *metadata.MetadataReader) {
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	metadataReader := metadata.NewMetadataReader(metadataService)
	return metadataService, metadataReader
}

// initializeImporter creates and starts the importer service
func initializeImporter(
	ctx context.Context,
	cfg *config.Config,
	metadataService *metadata.MetadataService,
	db *database.DB,
	poolManager pool.Manager,
	rcloneClient rclonecli.RcloneRcClient,
	configGetter config.ConfigGetter,
	broadcaster *progress.ProgressBroadcaster,
	userRepo *database.UserRepository,
	healthRepo *database.HealthRepository,
) (*importer.Service, error) {
	// Set defaults for workers if not configured
	maxProcessorWorkers := cfg.Import.MaxProcessorWorkers
	if maxProcessorWorkers <= 0 {
		maxProcessorWorkers = 2 // Default: 2 parallel workers
	}

	serviceConfig := importer.ServiceConfig{
		Workers: maxProcessorWorkers,
	}

	importerService, err := importer.NewService(serviceConfig, metadataService, db, poolManager, rcloneClient, configGetter, healthRepo, broadcaster, userRepo)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create importer service", "err", err)
		return nil, err
	}

	// Start importer service
	if err := importerService.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "failed to start importer service", "err", err)
		return nil, err
	}

	return importerService, nil
}

// initializeFilesystem creates the NZB filesystem with health tracking
func initializeFilesystem(
	ctx context.Context,
	metadataService *metadata.MetadataService,
	healthRepo *database.HealthRepository,
	arrsService *arrs.Service,
	rcloneClient rclonecli.RcloneRcClient,
	poolManager pool.Manager,
	configGetter config.ConfigGetter,
	streamTracker nzbfilesystem.StreamTracker,
	cacheSource *segcache.Source,
	par2RepairService *par2repair.Service,
) *nzbfilesystem.NzbFilesystem {
	// Reset all in-progress file health checks on start up
	if err := healthRepo.ResetFileAllChecking(ctx); err != nil {
		slog.ErrorContext(ctx, "failed to reset in progress file health", "err", err)
	}

	// Create metadata-based remote file handler
	metadataRemoteFile := nzbfilesystem.NewMetadataRemoteFile(
		metadataService,
		healthRepo,
		arrsService,
		rcloneClient,
		poolManager,
		configGetter,
		streamTracker,
		cacheSource,
	)

	// Serve PAR2-repaired article payloads on the hole read path, and queue
	// repairs when playback hits missing articles.
	if par2RepairService != nil {
		metadataRemoteFile.SetPatchSource(par2RepairService.PatchStore())
		metadataRemoteFile.SetRepairEnqueuer(par2RepairService)
	}

	// Create filesystem backed by metadata
	return nzbfilesystem.NewNzbFilesystem(metadataRemoteFile)
}

// setupNNTPPool initializes the NNTP connection pool
func setupNNTPPool(ctx context.Context, cfg *config.Config, poolManager pool.Manager) error {
	if len(cfg.Providers) > 0 {
		providers := cfg.ToNNTPProviders()
		if err := poolManager.SetProviders(providers); err != nil {
			slog.ErrorContext(ctx, "failed to create initial NNTP pool", "err", err)
			return err
		}
		slog.InfoContext(ctx, "NNTP connection pool initialized", "provider_count", len(cfg.Providers))
	} else {
		slog.InfoContext(ctx, "Starting server without NNTP providers - configure via API to enable downloads")
	}

	return nil
}

// setupRCloneClient creates an RClone client if enabled
func setupRCloneClient(ctx context.Context, cfg *config.Config, configManager *config.Manager) rclonecli.RcloneRcClient {
	if cfg.RClone.RCEnabled != nil && *cfg.RClone.RCEnabled {
		httpClient := httpclient.NewDefault()
		rcloneClient := rclonecli.NewRcloneRcClient(configManager, httpClient)

		if cfg.RClone.RCUrl != "" {
			slog.InfoContext(ctx, "RClone RC client initialized for external server",
				"rc_url", cfg.RClone.RCUrl)
		} else {
			slog.InfoContext(ctx, "RClone RC client initialized for internal server",
				"rc_port", cfg.RClone.RCPort)
		}
		return rcloneClient
	}

	slog.InfoContext(ctx, "RClone RC notifications disabled")
	return nil
}

// createFiberApp creates and configures the Fiber application
func createFiberApp(ctx context.Context, cfg *config.Config) (*fiber.App, *bool) {
	app := fiber.New(fiber.Config{
		RequestMethods: append(
			fiber.DefaultMethods, "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK",
		),
		BodyLimit: 100 * 1024 * 1024, // 100MB limit for uploads (e.g. nzbdav DBs)
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			slog.ErrorContext(ctx, "Fiber error", "path", c.Path(), "method", c.Method(), "error", err)
			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})

	// Conditional Fiber request logging - only in debug mode
	debugMode := cfg.Log.Level == "debug"

	// Create the logger middleware but wrap it to check debug mode
	fiberLogger := fLogger.New()
	app.Use(func(c *fiber.Ctx) error {
		if debugMode {
			return fiberLogger(c)
		}
		return c.Next()
	})

	return app, &debugMode
}

// setupRepositories creates all database repositories
func setupRepositories(ctx context.Context, db *database.DB) *repositorySet {
	dbConn := db.Connection()
	d := db.Dialect()

	return &repositorySet{
		MainRepo:       database.NewRepository(dbConn, d),
		HealthRepo:     database.NewHealthRepository(dbConn, d),
		UserRepo:       database.NewUserRepository(dbConn, d),
		Par2RepairRepo: database.NewPar2RepairRepository(dbConn, d),
	}
}

// setupAuthService creates and initializes the authentication service.
// When loginRequired is true, JWT_SECRET must be set or an error is returned.
// When loginRequired is false, a missing JWT_SECRET is logged as a warning and nil is returned.
func setupAuthService(ctx context.Context, cfg *config.Config, userRepo *database.UserRepository, loginRequired bool) (*auth.Service, error) {
	authConfig, err := auth.LoadConfigFromEnv()
	if err != nil {
		if loginRequired {
			return nil, fmt.Errorf("failed to load auth configuration: %w", err)
		}
		slog.WarnContext(ctx, "Auth configuration not loaded (login is disabled)", "err", err)
		return nil, nil
	}

	// Override with values from config file
	authConfig.Host = cfg.WebDAV.Host
	authConfig.Port = cfg.WebDAV.Port

	authService, err := auth.NewService(authConfig, userRepo)
	if err != nil {
		return nil, fmt.Errorf("failed to create authentication service: %w", err)
	}

	// Setup OAuth providers
	if err := authService.SetupProviders(authConfig); err != nil {
		return nil, fmt.Errorf("failed to setup auth providers: %w", err)
	}

	slog.InfoContext(ctx, "Authentication service initialized")
	return authService, nil
}

// setupStreamHandler creates the HTTP stream handler for file streaming
func setupStreamHandler(
	nzbFilesystem *nzbfilesystem.NzbFilesystem,
	userRepo *database.UserRepository,
	streamTracker *api.StreamTracker,
) *api.StreamHandler {
	return api.NewStreamHandler(nzbFilesystem, userRepo, streamTracker)
}

// setupAPIServer creates and configures the API server
func setupAPIServer(
	app *fiber.App,
	repos *repositorySet,
	authService *auth.Service,
	configManager *config.Manager,
	metadataReader *metadata.MetadataReader,
	metadataService *metadata.MetadataService,
	nzbFilesystem *nzbfilesystem.NzbFilesystem,
	poolManager pool.Manager,
	importerService *importer.Service,
	arrsService *arrs.Service,
	mountService *rclone.MountService,
	progressBroadcaster *progress.ProgressBroadcaster,
	streamTracker *api.StreamTracker,
	cacheSource *segcache.Source,
) *api.Server {
	apiConfig := &api.Config{
		Prefix: "/api",
	}

	apiServer := api.NewServer(
		apiConfig,
		repos.MainRepo,
		repos.HealthRepo,
		authService,
		repos.UserRepo,
		configManager,
		metadataReader,
		metadataService,
		nzbFilesystem,
		poolManager,
		importerService,
		arrsService,
		mountService,
		progressBroadcaster,
		streamTracker,
		cacheSource,
	)

	apiServer.SetupRoutes(app)

	// Register RClone handlers
	rcloneHandlers := api.NewRCloneHandlers(mountService, configManager.GetConfigGetter())
	api.RegisterRCloneRoutes(app.Group("/api"), rcloneHandlers)

	// Add simple liveness endpoint for Docker health checks
	app.Get("/live", handleFiberHealth)

	return apiServer
}

// initializeSegmentCache creates and starts the segment cache manager, loading it into
// source. Returns the manager so the caller can defer Stop(). Returns nil if CachePath
// is not configured (enabled/disabled is checked at read-time via source.Store()).
func initializeSegmentCache(ctx context.Context, cfg *config.Config, source *segcache.Source) *segcache.Manager {
	if cfg.SegmentCache.CachePath == "" {
		slog.InfoContext(ctx, "Segment cache not configured (no cache_path set)")
		return nil
	}

	// ExpiryHours is normalized in config.Validate (nil -> 24h); guard against
	// nil defensively in case the cache is initialized outside the load path. A
	// zero value is preserved and means "cache forever".
	expiryHours := 24
	if cfg.SegmentCache.ExpiryHours != nil {
		expiryHours = *cfg.SegmentCache.ExpiryHours
	}

	mgrCfg := segcache.ManagerConfig{
		CachePath:      cfg.SegmentCache.CachePath,
		MaxSizeBytes:   int64(cfg.SegmentCache.MaxSizeGB) * 1024 * 1024 * 1024,
		ExpiryDuration: time.Duration(expiryHours) * time.Hour,
	}.WithDefaults()

	mgr, err := segcache.NewManager(mgrCfg, slog.Default().With("component", "segcache"))
	if err != nil {
		slog.WarnContext(ctx, "Failed to create segment cache manager, running without segment cache", "error", err)
		return nil
	}

	mgr.Start(ctx)
	source.Swap(mgr)
	slog.InfoContext(ctx, "Segment cache started (catalog loads in background)",
		"cache_path", mgrCfg.CachePath,
		"max_size_bytes", mgrCfg.MaxSizeBytes,
		"expiry_duration", mgrCfg.ExpiryDuration)

	return mgr
}

// setupWebDAV creates and configures the WebDAV handler
func setupWebDAV(
	cfg *config.Config,
	fs *nzbfilesystem.NzbFilesystem,
	authService *auth.Service,
	userRepo *database.UserRepository,
	configManager *config.Manager,
	streamTracker *api.StreamTracker,
) (*webdav.Handler, error) {
	var tokenService *token.Service
	var webdavUserRepo *database.UserRepository

	// Pass authentication services if available
	if authService != nil {
		tokenService = authService.TokenService()
		webdavUserRepo = userRepo
	}

	webdavHandler, err := webdav.NewHandler(&webdav.Config{
		Port:   cfg.WebDAV.Port,
		User:   cfg.WebDAV.User,
		Pass:   cfg.WebDAV.Password,
		Prefix: "/webdav",
	}, fs, tokenService, webdavUserRepo, configManager.GetConfigGetter(), streamTracker)

	if err != nil {
		return nil, err
	}

	return webdavHandler, nil
}

// startPar2RepairService wires and starts the background PAR2 repair service.
// Always constructed (triggers no-op while disabled, and enable/disable is a
// hot config change); the worker loop itself starts here.
func startPar2RepairService(
	ctx context.Context,
	cfg *config.Config,
	repo *database.Par2RepairRepository,
	healthRepo *database.HealthRepository,
	metadataService *metadata.MetadataService,
	poolManager pool.Manager,
	configGetter config.ConfigGetter,
	streamsActive func() bool,
) *par2repair.Service {
	// Repair fetches ride the pool's background lane, which the pool keeps
	// behind playback and imports on its own. The repair's cap only bounds how
	// much of an idle pool one repair may fill.
	fetcher := par2repair.NewPoolFetcher(func() (par2repair.BodyClient, error) {
		return poolManager.GetPool()
	}, par2repair.NewConnLimiter(func() int {
		return configGetter().Par2Repair.EffectiveMaxConnections()
	}))
	// Stream-aware sweep width (conservative while anything plays, bounded
	// widening when idle), further capped by the repair's own connection
	// budget so a 10-connection repair never floods every connection's STAT
	// pipeline. See par2repair.SweepStatConcurrency.
	fetcher.StatConcurrency = func() int {
		c := configGetter()
		return par2repair.SweepStatConcurrency(
			poolManager.StatSweepConcurrency(c.StatConcurrency()),
			c.Par2Repair.EffectiveMaxConnections(),
		)
	}
	patchStore := par2repair.NewPatchStore(cfg.Par2Repair.EffectivePatchDir(cfg.Metadata.RootPath))
	service := par2repair.NewService(
		repo,
		par2repair.NewMetadataSource(metadataService),
		fetcher,
		patchStore,
		func() par2repair.Config {
			c := configGetter()
			return par2repair.Config{
				Enabled:           c.Par2Repair.Enabled != nil && *c.Par2Repair.Enabled,
				MaxRepairRatio:    c.Par2Repair.MaxRepairRatio,
				MaxMemoryMB:       c.Par2Repair.MaxMemoryMB,
				MaxConcurrentJobs: c.Par2Repair.MaxConcurrentJobs,
				MaxConnections:    c.Par2Repair.EffectiveMaxConnections(),
				MinReleaseSizeMB:  c.Par2Repair.MinReleaseSizeMB,
				MaxReleaseSizeMB:  c.Par2Repair.MaxReleaseSizeMB,
				MaxPatchStoreMB:   c.Par2Repair.MaxPatchStoreMB,
			}
		},
		slog.Default(),
	)
	if healthRepo != nil {
		service.SetHealthStore(healthRepo)
	}
	// Jobs yield fetch depth and solver fold width to active playback streams.
	service.SetStreamsActive(streamsActive)
	go service.Start(ctx)
	slog.InfoContext(ctx, "PAR2 repair service started",
		"enabled", cfg.Par2Repair.Enabled != nil && *cfg.Par2Repair.Enabled)
	return service
}

// startHealthWorker creates and starts the health monitoring worker
func startHealthWorker(
	ctx context.Context,
	cfg *config.Config,
	metadataService *metadata.MetadataService,
	healthRepo *database.HealthRepository,
	poolManager pool.Manager,
	configManager *config.Manager,
	rcloneClient rclonecli.RcloneRcClient,
	arrsService *arrs.Service,
	importerService importer.ImportService,
	broadcaster *progress.ProgressBroadcaster,
	par2RepairService *par2repair.Service,
	contentVerifyFS contentverify.Opener,
) (*health.HealthWorker, *health.LibrarySyncWorker, error) {
	// The health and library-sync workers share the process-wide metadata service so
	// their deletions go through the same store reference counter the importer wires
	// up. A second instance here silently skipped every DecStoreRef, leaking .nzbz
	// stores and letting the two lite caches serve each other stale entries.

	// Create health checker
	healthChecker := health.NewHealthChecker(
		healthRepo,
		metadataService,
		poolManager,
		configManager.GetConfigGetter(),
		rcloneClient,
		contentVerifyFS,
	)

	healthWorker := health.NewHealthWorker(
		healthChecker,
		healthRepo,
		metadataService,
		arrsService,
		importerService,
		configManager.GetConfigGetter(),
		broadcaster,
	)

	// Degraded verdicts attempt PAR2 repair before anything else; with
	// arr_first (default on) it also picks up corrupted files the ARRs
	// could not repair.
	if par2RepairService != nil {
		healthChecker.SetPatchIndex(par2RepairService.PatchStore())
		healthWorker.SetPar2RepairEnqueuer(par2RepairService)
	}

	// Create library sync worker (always create, but only start if enabled)
	librarySyncWorker := health.NewLibrarySyncWorker(
		metadataService,
		healthRepo,
		configManager.GetConfigGetter(),
		configManager,
		rcloneClient,
	)

	// Only start health system if enabled
	if cfg.Health.Enabled != nil && *cfg.Health.Enabled {
		// Start health worker with the main context
		if err := healthWorker.Start(ctx); err != nil {
			slog.ErrorContext(ctx, "Failed to start health worker", "error", err)
			return nil, nil, err
		}

		// Start library sync worker
		librarySyncWorker.StartLibrarySync(ctx)

		slog.InfoContext(ctx, "Health system started")
	} else {
		slog.InfoContext(ctx, "Health system disabled - no health monitoring or repairs will occur")
	}

	return healthWorker, librarySyncWorker, nil
}

// startMountService starts the RClone mount service if enabled
func startMountService(ctx context.Context, cfg *config.Config, mountService *rclone.MountService, logger *slog.Logger) error {
	if cfg.RClone.MountEnabled == nil || !*cfg.RClone.MountEnabled {
		slog.InfoContext(ctx, "RClone mount service is disabled in configuration")
		return nil
	}

	if err := mountService.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to start mount service", "error", err)
		return err
	}

	slog.InfoContext(ctx, "RClone mount service started", "mount_point", cfg.MountPath)
	return nil
}

// createHTTPServer creates the HTTP server with routing
func createHTTPServer(apiServer *api.Server, app *fiber.App, webdavHandler *webdav.Handler, streamHandler *api.StreamHandler, port int, configGetter config.ConfigGetter) *http.Server {
	// Mount WebDAV handler directly (no Fiber adapter needed)
	webdavHTTPHandler := webdavHandler.GetHTTPHandler()

	// Mount stream handler directly (no Fiber adapter needed)
	streamHTTPHandler := streamHandler.GetHTTPHandler()

	// Convert Fiber app to HTTP handler for all other routes
	fiberHTTPHandler := adaptor.FiberApp(app)

	// Create a handler that routes between WebDAV, Stream, and Fiber
	mainHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Check if server is ready, but allow /live and /api/system/health
		if !apiServer.IsReady() && path != "/live" && path != "/api/system/health" && !strings.HasPrefix(path, "/api/auth/config") {
			w.Header().Set("Retry-After", "10")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("503 Service Unavailable: Server is initializing"))
			return
		}

		// Route profiler requests if enabled
		if configGetter().ProfilerEnabled && strings.HasPrefix(path, "/debug/pprof") {
			http.DefaultServeMux.ServeHTTP(w, r)
			return
		}

		// Long-lived streaming responses must not inherit the server-wide
		// WriteTimeout safety net: it hard-kills every media transfer at
		// exactly 30 minutes regardless of activity, forcing clients into a
		// mid-playback reconnect. Clear the write deadline for media streams,
		// WebDAV reads, and SSE endpoints before dispatching.
		if isStreamingRoute(path) {
			clearWriteDeadline(w)
		}

		// Route stream requests directly to stream handler
		if strings.HasPrefix(path, "/api/files/stream") {
			streamHTTPHandler.ServeHTTP(w, r)
			return
		}

		// Route SSE log stream directly — bypasses adaptor.FiberApp which
		// blocks forever on streaming responses (calls Response.Body() which
		// reads the SSE pipe until EOF that never comes).
		if path == "/api/logs/stream" {
			apiServer.ServeLogsSSE(w, r)
			return
		}
		if path == "/api/queue/stream" {
			apiServer.ServeQueueSSE(w, r)
			return
		}
		if path == "/api/health/stream" {
			apiServer.ServeHealthSSE(w, r)
			return
		}

		// Route WebDAV requests directly to WebDAV handler
		if len(path) >= 7 && path[:7] == "/webdav" {
			webdavHTTPHandler.ServeHTTP(w, r)
			return
		}

		// Route all other requests to Fiber handler
		fiberHTTPHandler.ServeHTTP(w, r)
	})

	// Create and configure the HTTP server
	return &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mainHandler,
		IdleTimeout:  time.Minute * 5,
		WriteTimeout: time.Minute * 30,
		ReadTimeout:  time.Minute * 5,
	}
}

// isStreamingRoute reports whether the request path serves a long-lived
// response (media transfer, WebDAV read, or SSE feed) that must outlive the
// server-wide WriteTimeout safety net.
func isStreamingRoute(path string) bool {
	return strings.HasPrefix(path, "/api/files/stream") ||
		strings.HasPrefix(path, "/webdav") ||
		path == "/api/logs/stream" ||
		path == "/api/queue/stream" ||
		path == "/api/health/stream"
}

// clearWriteDeadline removes the connection write deadline so streaming
// responses are never hard-killed mid-transfer by the http.Server timeout.
func clearWriteDeadline(w http.ResponseWriter) {
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
}
