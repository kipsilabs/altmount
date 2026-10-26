package arrs

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/data"
	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/kipsilabs/altmount/internal/arrs/instances"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/arrs/registrar"
	"github.com/kipsilabs/altmount/internal/arrs/scanner"
	"github.com/kipsilabs/altmount/internal/arrs/worker"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"golift.io/starr"
)

// Re-export types for backward compatibility
type ConfigInstance = model.ConfigInstance
type ConfigManager = model.ConfigManager

var (
	ErrPathMatchFailed         = model.ErrPathMatchFailed
	ErrEpisodeAlreadySatisfied = model.ErrEpisodeAlreadySatisfied
	ErrInstanceNotFound        = model.ErrInstanceNotFound
)

// IsTemporarilyUnreachable reports whether err indicates the *arr was only
// temporarily unreachable — a network/transport failure (timeout, connection
// refused, DNS, TLS, etc.) or a 5xx server response — rather than a definitive
// answer about the release. Such errors must DEFER a repair (keep the file
// repair-pending so it self-heals when the *arr returns) instead of condemning
// the file as corrupted. starr surfaces non-2xx responses as a *starr.ReqError
// whose Code carries the HTTP status; transport failures surface as net.Error
// (often wrapped in *url.Error) or a context deadline.
func IsTemporarilyUnreachable(err error) bool {
	if err == nil {
		return false
	}

	// Typed 5xx response from the starr app (server-side, almost always transient).
	var reqErr *starr.ReqError
	if errors.As(err, &reqErr) && reqErr.Code >= 500 && reqErr.Code <= 599 {
		return true
	}

	// Network/transport failure: timeouts, connection refused, DNS, TLS handshake,
	// etc. *url.Error (returned by the HTTP client) also satisfies net.Error.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// A context deadline tripping mid-request is an unreachable/too-slow *arr, not
	// a corrupt file.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	return false
}

// Service manages Radarr, Sonarr, Lidarr, Readarr, and Whisparr instances for health monitoring and file repair
type Service struct {
	configGetter  config.ConfigGetter
	configManager model.ConfigManager
	userRepo      *database.UserRepository

	instances *instances.Manager
	clients   *clients.Manager
	data      *data.Manager
	scanner   *scanner.Manager
	worker    *worker.Worker
	registrar *registrar.Manager
}

// NewService creates a new arrs service for health monitoring and file repair
func NewService(configGetter config.ConfigGetter, configManager model.ConfigManager, userRepo *database.UserRepository, queueRepo *database.Repository) *Service {
	instManager := instances.NewManager(configGetter, configManager)
	clientManager := clients.NewManager(httpclient.NewForExternal(configGetter().Network, 30*time.Second))
	dataManager := data.NewManager()
	// One failure tracker shared by every AltMount→arr re-acquire producer: the
	// queue-cleanup worker and the scanner (repair re-triggers) count the same
	// per-target keys.
	failureTracker := failures.NewTracker()
	scannerManager := scanner.NewManager(configGetter, instManager, clientManager, dataManager, queueRepo, failureTracker)
	workerManager := worker.NewWorker(configGetter, instManager, clientManager, queueRepo, failureTracker)
	scannerManager.SetSonarrQueueRepair(workerManager.RepairSonarrDownload)
	registrarManager := registrar.NewManager(instManager, clientManager)

	return &Service{
		configGetter:  configGetter,
		configManager: configManager,
		userRepo:      userRepo,
		instances:     instManager,
		clients:       clientManager,
		data:          dataManager,
		scanner:       scannerManager,
		worker:        workerManager,
		registrar:     registrarManager,
	}
}

// GetAllInstances returns all arrs instances from configuration
func (s *Service) GetAllInstances() []*model.ConfigInstance {
	return s.instances.GetAllInstances()
}

// ResolveIndexerByDownloadID looks up which indexer a download came from by
// querying the native queue of each configured Sportarr instance for the given
// download-client ID. Sportarr is not starr-compatible and never fires an OnGrab
// webhook, so this on-demand lookup (used at import time) is how its imports get
// attributed instead of falling back to "Unknown" in indexer health. Only Sportarr
// instances are queried — the other *arrs supply the indexer via webhook. Returns
// ("", false) when no Sportarr instance knows the ID.
func (s *Service) ResolveIndexerByDownloadID(ctx context.Context, downloadID string) (string, bool) {
	if downloadID == "" {
		return "", false
	}
	for _, instance := range s.instances.GetAllInstances() {
		if instance == nil || !instance.Enabled || instance.Type != "sportarr" {
			continue
		}
		client, err := s.clients.GetOrCreateSportarrClient(instance.Name, instance.URL, instance.APIKey)
		if err != nil {
			continue
		}
		queue, err := client.GetQueue(ctx)
		if err != nil {
			slog.DebugContext(ctx, "Sportarr indexer lookup: failed to read queue",
				"instance", instance.Name, "download_id", downloadID, "error", err)
			continue
		}
		for _, q := range queue {
			if q.DownloadID == downloadID && q.Indexer != "" {
				return q.Indexer, true
			}
		}
	}
	return "", false
}

// GetInstance returns a specific instance by type and name
func (s *Service) GetInstance(instanceType, instanceName string) *model.ConfigInstance {
	return s.instances.GetInstance(instanceType, instanceName)
}

// RegisterInstance attempts to automatically register an ARR instance
func (s *Service) RegisterInstance(ctx context.Context, arrURL, apiKey string) error {
	registered, err := s.instances.RegisterInstance(ctx, arrURL, apiKey)
	if err != nil {
		return err
	}

	if registered {
		// Automatically register webhook for the new instance
		go func() {
			// Small delay to ensure DB/Config is fully settled
			time.Sleep(2 * time.Second)
			bgCtx := context.Background()

			key := s.GetFirstAdminAPIKey(bgCtx)
			if key != "" {
				cfg := s.configGetter()
				baseURL := cfg.GetWebhookBaseURL()
				_ = s.registrar.EnsureWebhookRegistration(bgCtx, baseURL, key)
			}
		}()
	}

	return nil
}

// GetFirstAdminAPIKey retrieves the API key of the first admin user
func (s *Service) GetFirstAdminAPIKey(ctx context.Context) string {
	if s.userRepo == nil {
		return ""
	}
	users, err := s.userRepo.GetAllUsers(ctx)

	// If no users exist and auth is disabled, bootstrap a default admin
	if (err == nil && len(users) == 0) || (err != nil && strings.Contains(err.Error(), "no such table")) {
		cfg := s.configGetter()
		loginRequired := true
		if cfg.Auth.LoginRequired != nil {
			loginRequired = *cfg.Auth.LoginRequired
		}

		if !loginRequired {
			slog.InfoContext(ctx, "Bootstrapping default admin user for automatic background tasks")
			user := &database.User{
				UserID:   "admin",
				Provider: "direct",
				IsAdmin:  true,
			}
			if err := s.userRepo.CreateUser(ctx, user); err == nil {
				if key, err := s.userRepo.RegenerateAPIKey(ctx, user.UserID); err == nil {
					return key
				}
			}
		}
	}

	if err != nil || len(users) == 0 {
		return ""
	}

	for _, user := range users {
		if user.IsAdmin && user.APIKey != nil {
			return *user.APIKey
		}
	}

	// Fallback: if we have an admin but no key, generate one automatically
	for _, user := range users {
		if user.IsAdmin {
			if key, err := s.userRepo.RegenerateAPIKey(ctx, user.UserID); err == nil {
				return key
			}
		}
	}

	// Fallback to first user with a key
	if len(users) > 0 && users[0].APIKey != nil {
		return *users[0].APIKey
	}

	return ""
}

// NoteImportFailure runs the importer-side failure breaker for a permanently
// failed *arr-originated download: counts the failure per target against the
// shared tracker and, at the queue_cleanup_max_failures threshold, unmonitors
// the target and removes the *arr queue record with blocklist-without-re-search
// (before the *arr's own failed-download handling can auto-re-search). See
// worker.HandleImportFailure.
func (s *Service) NoteImportFailure(ctx context.Context, downloadID, category string) {
	s.worker.HandleImportFailure(ctx, downloadID, category)
}

// StartWorker starts the queue cleanup worker
func (s *Service) StartWorker(ctx context.Context) error {
	return s.worker.Start(ctx)
}

// StopWorker stops the queue cleanup worker
func (s *Service) StopWorker(ctx context.Context) {
	s.worker.Stop(ctx)
}

// RegisterConfigChangeHandler subscribes to config changes and starts/stops
// the recovery worker when arrs.enabled changes. Queue rules are gated per tick.
func (s *Service) RegisterConfigChangeHandler(ctx context.Context, configManager *config.Manager) {
	configManager.OnConfigChange(func(oldConfig, newConfig *config.Config) {
		oldOn := worker.ShouldRunWorker(oldConfig)
		newOn := worker.ShouldRunWorker(newConfig)
		if oldOn == newOn {
			return
		}
		if newOn {
			slog.InfoContext(ctx, "ARR worker enabled via config change, starting")
			if err := s.worker.Start(ctx); err != nil {
				slog.ErrorContext(ctx, "Failed to start ARR worker", "error", err)
			}
			return
		}
		slog.InfoContext(ctx, "ARR worker disabled via config change, stopping")
		s.worker.Stop(ctx)
	})
}

// TriggerFileRescan triggers a rescan for a specific file path through the appropriate ARR instance
func (s *Service) TriggerFileRescan(ctx context.Context, pathForRescan string, relativePath string, metadataStr *string) error {
	return s.scanner.TriggerFileRescan(ctx, pathForRescan, relativePath, metadataStr)
}

// ClearInstanceCache clears all movie and series caches for a specific instance
func (s *Service) ClearInstanceCache(ctx context.Context, instanceName string) {
	if instanceName == "" || s.data == nil {
		return
	}
	slog.DebugContext(ctx, "Clearing ARR cache for instance", "instance", instanceName)
	s.data.ClearMoviesCache(instanceName)
	s.data.ClearSeriesCache(instanceName)
}

// DiscoverFileMetadata attempts to discover the rich metadata for a file through the appropriate ARR instance
func (s *Service) DiscoverFileMetadata(ctx context.Context, filePath, relativePath, nzbName, libraryPath string) (*model.WebhookMetadata, error) {
	return s.scanner.DiscoverFileMetadata(ctx, filePath, relativePath, nzbName, libraryPath)
}

// TriggerScanForFile finds the ARR instance managing the file and triggers a download scan on it.
func (s *Service) TriggerScanForFile(ctx context.Context, filePath string) error {
	return s.scanner.TriggerScanForFile(ctx, filePath)
}

// TriggerDownloadScan triggers the "Check For Finished Downloads" task in ARR instances
func (s *Service) TriggerDownloadScan(ctx context.Context, instanceType string) {
	s.scanner.TriggerDownloadScan(ctx, instanceType)
}

// EnsureWebhookRegistration ensures that the AltMount webhook is registered in all enabled ARR instances
func (s *Service) EnsureWebhookRegistration(ctx context.Context, altmountURL string, apiKey string) error {
	return s.registrar.EnsureWebhookRegistration(ctx, altmountURL, apiKey)
}

// EnsureDownloadClientRegistration ensures that AltMount is registered as a SABnzbd download client in all enabled ARR instances
func (s *Service) EnsureDownloadClientRegistration(ctx context.Context, altmountHost string, altmountPort int, urlBase string, apiKey string) error {
	return s.registrar.EnsureDownloadClientRegistration(ctx, altmountHost, altmountPort, urlBase, apiKey)
}

// TestDownloadClientRegistration tests the connection from ARR instances back to AltMount
func (s *Service) TestDownloadClientRegistration(ctx context.Context, altmountHost string, altmountPort int, urlBase string, apiKey string) (map[string]string, error) {
	return s.registrar.TestDownloadClientRegistration(ctx, altmountHost, altmountPort, urlBase, apiKey)
}

// TestConnection tests the connection to an arrs instance
func (s *Service) TestConnection(ctx context.Context, instanceType, url, apiKey string, apiVersion ...string) error {
	return s.clients.TestConnection(ctx, instanceType, url, apiKey, apiVersion...)
}

// GetHealth retrieves health checks from all enabled ARR instances
func (s *Service) GetHealth(ctx context.Context) (map[string]any, error) {
	instances := s.instances.GetAllInstances()
	results := make(map[string]any)

	for _, instance := range instances {
		if !instance.Enabled {
			continue
		}

		var health any

		switch instance.Type {
		case "radarr":
			client, err := s.clients.GetOrCreateRadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err == nil {
				_ = client.GetInto(ctx, starr.Request{URI: "/health"}, &health)
			}
		case "sonarr":
			client, err := s.clients.GetOrCreateSonarrClientContext(ctx, instance.Name, instance.URL, instance.APIKey, instance.APIVersion)
			if err == nil {
				_ = client.GetInto(ctx, starr.Request{URI: client.APIVersion() + "/health"}, &health)
			}
		case "lidarr":
			client, err := s.clients.GetOrCreateLidarrClient(instance.Name, instance.URL, instance.APIKey)
			if err == nil {
				_ = client.GetInto(ctx, starr.Request{URI: "/health"}, &health)
			}
		case "readarr":
			client, err := s.clients.GetOrCreateReadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err == nil {
				_ = client.GetInto(ctx, starr.Request{URI: "/health"}, &health)
			}
		case "whisparr":
			client, err := s.clients.GetOrCreateWhisparrClient(instance.Name, instance.URL, instance.APIKey)
			if err == nil {
				_ = client.GetInto(ctx, starr.Request{URI: "/health"}, &health)
			}
		case "sportarr":
			// Sportarr is not starr-compatible; report reachability via its native
			// status endpoint rather than a starr /health call.
			client, err := s.clients.GetOrCreateSportarrClient(instance.Name, instance.URL, instance.APIKey)
			if err == nil {
				if hErr := client.Health(ctx); hErr == nil {
					health = []any{} // healthy: no issues reported
				}
			}
		}
		if health != nil {
			results[instance.Name] = health
		}
	}

	return results, nil
}
