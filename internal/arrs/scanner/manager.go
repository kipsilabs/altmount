package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/data"
	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/kipsilabs/altmount/internal/arrs/instances"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/arrs/registrar"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"golang.org/x/sync/singleflight"
	"golift.io/starr"
	"golift.io/starr/lidarr"
	"golift.io/starr/radarr"
	"golift.io/starr/readarr"
	"golift.io/starr/sonarr"
)

var errNoSonarrQueueMatch = errors.New("no matching Sonarr queue download")

type Manager struct {
	sonarrStagesMu sync.Mutex
	sonarrStages   map[string]*sonarrRepairStage
	configGetter   config.ConfigGetter
	instances      *instances.Manager
	clients        *clients.Manager
	data           *data.Manager
	repo           *database.Repository
	// failures is the shared per-target circuit breaker (see arrs/failures). The
	// scanner bumps it on every targeted re-search it issues; at the configured
	// queue_cleanup_max_failures threshold it stops searching and unmonitors the
	// target instead, so a dead release can't drive an endless re-grab storm.
	failures          *failures.Tracker
	sf                singleflight.Group
	sonarrQueueRepair func(context.Context, string, clients.SonarrClient, model.SonarrDownloadGroup) error
}

// SetSonarrQueueRepair routes queue fallback through the worker's grouped,
// resumable executor. Install before starting scanner/worker goroutines.
func (m *Manager) SetSonarrQueueRepair(repair func(context.Context, string, clients.SonarrClient, model.SonarrDownloadGroup) error) {
	m.sonarrQueueRepair = repair
}

func NewManager(configGetter config.ConfigGetter, instances *instances.Manager, clients *clients.Manager, data *data.Manager, repo *database.Repository, failureTracker *failures.Tracker) *Manager {
	return &Manager{
		configGetter: configGetter,
		instances:    instances,
		clients:      clients,
		data:         data,
		repo:         repo,
		failures:     failureTracker,
	}
}

// findInstanceForFilePath finds which ARR instance manages the given file path
func (m *Manager) findInstanceForFilePath(ctx context.Context, filePath string, relativePath string) (instanceType string, instanceName string, err error) {
	slog.DebugContext(ctx, "Finding instance for file path", "file_path", filePath, "relative_path", relativePath)

	allInstances := m.instances.GetAllInstances()

	// Strategy 1: Fast Path - Check Root Folders.
	//
	// Every instance is scored and the most specific (longest) matching root
	// folder wins. First-match-wins sent files under "/media/tv-4k" to the
	// instance rooted at "/media/tv" whenever that one happened to be listed
	// first, and the same applies to a nested root such as "/media/tv/anime"
	// (issue #749).
	bestScore, bestType, bestName := 0, "", ""
	for _, instance := range allInstances {
		if !instance.Enabled {
			continue
		}

		if client, err := m.clients.GetOrCreateClient(instance); err == nil {
			if score := m.managesFile(ctx, instance.Type, client, filePath); score > bestScore {
				bestScore, bestType, bestName = score, instance.Type, instance.Name
			}
		}
	}
	if bestScore > 0 {
		slog.DebugContext(ctx, "Found managing instance by root folder",
			"instance", bestName, "type", bestType, "root_len", bestScore)
		return bestType, bestName, nil
	}

	// Strategy 2: Category Match - Check if file is in the staging/complete folder
	cfg := m.configGetter()
	if cfg.SABnzbd.CompleteDir != "" {
		completeDir := strings.Trim(filepath.ToSlash(cfg.SABnzbd.CompleteDir), "/")
		completeSegment := "/" + completeDir + "/"
		normalizedPath := filepath.ToSlash(filePath)

		// Check if path contains the complete directory as a segment
		if _, after, found := strings.Cut(normalizedPath, completeSegment); found {
			// Extract everything after the complete directory segment (e.g., "tv/show/file.mkv")
			afterPrefix := after
			parts := strings.Split(afterPrefix, "/")
			if len(parts) > 0 {
				category := parts[0]
				slog.DebugContext(ctx, "File is in complete_dir, matching by category", "category", category)

				for _, instance := range allInstances {
					if !instance.Enabled {
						continue
					}

					if strings.EqualFold(instance.Category, category) {
						slog.InfoContext(ctx, "Found managing instance by category in complete_dir", "instance", instance.Name, "category", category)
						return instance.Type, instance.Name, nil
					}
				}
			}
		}
	}

	// Strategy 3: Slow Path - Search Cache by Relative Path
	if relativePath != "" {
		slog.InfoContext(ctx, "Root folder match failed, attempting relative path search", "relative_path", relativePath)

		for _, instance := range allInstances {
			if !instance.Enabled {
				continue
			}

			if client, err := m.clients.GetOrCreateClient(instance); err == nil {
				if m.hasFile(ctx, instance.Type, client, instance.Name, relativePath) {
					slog.InfoContext(ctx, "Found managing instance by relative path", "instance", instance.Name, "type", instance.Type)
					return instance.Type, instance.Name, nil
				}
			}
		}
	}

	return "", "", fmt.Errorf("no ARR instance found managing file path: %s", filePath)
}

// rootFolderScore reports how specifically rootPath claims filePath: the
// length of the normalized root folder when filePath is that folder or lives
// inside it, else 0. Bigger means more specific.
//
// Matching is on path-segment boundaries. A raw strings.HasPrefix made
// "/media/tv" claim "/media/tv-4k/..." — sending repairs to an ARR that owns
// no such series (issue #749).
func rootFolderScore(filePath, rootPath string) int {
	root := strings.TrimRight(filepath.ToSlash(rootPath), "/")
	file := strings.TrimRight(filepath.ToSlash(filePath), "/")
	if root == "" || file == "" {
		return 0
	}
	if file == root || strings.HasPrefix(file, root+"/") {
		return len(root)
	}
	return 0
}

// managesFile reports how specifically instanceType's root folders claim
// filePath: the length of the longest matching root folder, or 0 when none
// matches. Callers compare scores across instances so the most specific root
// wins (see findInstanceForFilePath).
func (m *Manager) managesFile(ctx context.Context, instanceType string, client any, filePath string) int {
	switch instanceType {
	case "radarr":
		rc, ok := client.(*radarr.Radarr)
		if !ok {
			return 0
		}
		return m.radarrManagesFile(ctx, rc, filePath)
	case "sonarr":
		sc, ok := client.(clients.SonarrClient)
		if !ok {
			return 0
		}
		return m.sonarrManagesFile(ctx, sc, filePath)
	case "lidarr":
		lc, ok := client.(*lidarr.Lidarr)
		if !ok {
			return 0
		}
		return m.lidarrManagesFile(ctx, lc, filePath)
	case "readarr":
		rc, ok := client.(*readarr.Readarr)
		if !ok {
			return 0
		}
		return m.readarrManagesFile(ctx, rc, filePath)
	case "whisparr":
		wc, ok := client.(clients.SonarrClient)
		if !ok {
			return 0
		}
		return m.sonarrManagesFile(ctx, wc, filePath)
	default:
		return 0
	}
}

func (m *Manager) hasFile(ctx context.Context, instanceType string, client any, instanceName, relativePath string) bool {
	switch instanceType {
	case "radarr":
		rc, ok := client.(*radarr.Radarr)
		if !ok {
			return false
		}
		return m.radarrHasFile(ctx, rc, instanceName, relativePath)
	case "sonarr":
		sc, ok := client.(clients.SonarrClient)
		if !ok {
			return false
		}
		return m.sonarrHasFile(ctx, sc, instanceName, relativePath)
	case "lidarr", "readarr", "whisparr":
		// For now, these don't have a slow path search implementation
		// They rely on the Root Folder (Strategy 1) or Category (Strategy 2)
		return false
	default:
		return false
	}
}

// radarrManagesFile scores how specifically Radarr's root folders claim
// filePath (checkrr approach). See rootFolderScore for the match rule.
func (m *Manager) radarrManagesFile(ctx context.Context, client *radarr.Radarr, filePath string) int {
	slog.DebugContext(ctx, "Checking Radarr root folders for file ownership",
		"file_path", filePath)

	// Get root folders from Radarr (much faster than GetMovie)
	rootFolders, err := client.GetRootFoldersContext(ctx)
	if err != nil {
		slog.DebugContext(ctx, "Failed to get root folders from Radarr for file check", "error", err)
		return 0
	}

	best := 0
	for _, folder := range rootFolders {
		slog.DebugContext(ctx, "Checking Radarr root folder", "folder_path", folder.Path, "file_path", filePath)
		if score := rootFolderScore(filePath, folder.Path); score > best {
			slog.DebugContext(ctx, "File matches Radarr root folder", "folder_path", folder.Path)
			best = score
		}
	}

	if best == 0 {
		slog.DebugContext(ctx, "File does not match any Radarr root folders")
	}
	return best
}

// sonarrManagesFile scores how specifically Sonarr's root folders claim
// filePath (checkrr approach). See rootFolderScore for the match rule.
func (m *Manager) sonarrManagesFile(ctx context.Context, client clients.SonarrClient, filePath string) int {
	slog.DebugContext(ctx, "Checking Sonarr root folders for file ownership",
		"file_path", filePath)

	// Get root folders from Sonarr (much faster than GetAllSeries)
	rootFolders, err := client.GetRootFoldersContext(ctx)
	if err != nil {
		slog.DebugContext(ctx, "Failed to get root folders from Sonarr for file check", "error", err)
		return 0
	}

	best := 0
	for _, folder := range rootFolders {
		slog.DebugContext(ctx, "Checking Sonarr root folder", "folder_path", folder.Path, "file_path", filePath)
		if score := rootFolderScore(filePath, folder.Path); score > best {
			slog.DebugContext(ctx, "File matches Sonarr root folder", "folder_path", folder.Path)
			best = score
		}
	}

	if best == 0 {
		slog.DebugContext(ctx, "File does not match any Sonarr root folders")
	}
	return best
}

// lidarrManagesFile scores how specifically Lidarr's root folders claim filePath.
func (m *Manager) lidarrManagesFile(ctx context.Context, client *lidarr.Lidarr, filePath string) int {
	slog.DebugContext(ctx, "Checking Lidarr root folders for file ownership", "file_path", filePath)
	rootFolders, err := client.GetRootFoldersContext(ctx)
	if err != nil {
		slog.DebugContext(ctx, "Failed to get root folders from Lidarr", "error", err)
		return 0
	}
	best := 0
	for _, folder := range rootFolders {
		if score := rootFolderScore(filePath, folder.Path); score > best {
			best = score
		}
	}
	return best
}

// readarrManagesFile scores how specifically Readarr's root folders claim filePath.
func (m *Manager) readarrManagesFile(ctx context.Context, client *readarr.Readarr, filePath string) int {
	slog.DebugContext(ctx, "Checking Readarr root folders for file ownership", "file_path", filePath)
	rootFolders, err := client.GetRootFoldersContext(ctx)
	if err != nil {
		slog.DebugContext(ctx, "Failed to get root folders from Readarr", "error", err)
		return 0
	}
	best := 0
	for _, folder := range rootFolders {
		if score := rootFolderScore(filePath, folder.Path); score > best {
			best = score
		}
	}
	return best
}

// radarrHasFile checks if any movie in the instance contains the given relative path
func (m *Manager) radarrHasFile(ctx context.Context, client *radarr.Radarr, instanceName, relativePath string) bool {
	movies, err := m.data.GetMovies(ctx, client, instanceName)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get movies for relative path check", "instance", instanceName, "error", err)
		return false
	}

	strippedRelative := strings.TrimSuffix(relativePath, ".strm")

	for _, movie := range movies {
		if movie.HasFile && movie.MovieFile != nil {
			if strings.HasSuffix(movie.MovieFile.Path, relativePath) ||
				strings.HasSuffix(strings.TrimSuffix(movie.MovieFile.Path, filepath.Ext(movie.MovieFile.Path)), strippedRelative) {
				return true
			}
		}
	}
	return false
}

// sonarrHasFile checks if any series in the instance contains the given relative path
func (m *Manager) sonarrHasFile(ctx context.Context, client clients.SonarrClient, instanceName, relativePath string) bool {
	seriesList, err := m.data.GetSeries(ctx, client, instanceName)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get series for relative path check", "instance", instanceName, "error", err)
		return false
	}

	// Normalize relative path for comparison
	relativePath = filepath.ToSlash(relativePath)
	strippedRelative := strings.TrimSuffix(relativePath, ".strm")

	for _, series := range seriesList {
		// Check if the series folder name is part of the relative path
		folderName := filepath.Base(series.Path)
		if strings.Contains(relativePath, folderName) || strings.Contains(strippedRelative, folderName) {
			return true
		}
	}
	return false
}

// TriggerFileRescan triggers a rescan for a specific file path through the appropriate ARR instance
func (m *Manager) TriggerFileRescan(ctx context.Context, pathForRescan string, relativePath string, metadataStr *string) error {
	hasMeta := "false"
	if metadataStr != nil && *metadataStr != "" {
		hasMeta = "true"
	}
	key := fmt.Sprintf("rescan:%s:%s", pathForRescan, hasMeta)

	res, err, _ := m.sf.Do(key, func() (interface{}, error) {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		slog.InfoContext(bgCtx, "Triggering ARR rescan (singleflight)", "path", pathForRescan, "relative_path", relativePath, "has_metadata", hasMeta)

		var metadata *model.WebhookMetadata
		if metadataStr != nil && *metadataStr != "" {
			var parsedMetadata model.WebhookMetadata
			if err := json.Unmarshal([]byte(*metadataStr), &parsedMetadata); err == nil {
				metadata = &parsedMetadata
			} else {
				slog.WarnContext(bgCtx, "Failed to parse metadata string, falling back to path-based repair", "error", err, "path", pathForRescan)
			}
		}

		var instanceType, instanceName string
		var err error

		// Try fast path: use instance from metadata
		if metadata != nil && metadata.InstanceName != "" {
			// Find if the instance exists in config
			instances := m.instances.GetAllInstances()
			for _, inst := range instances {
				if inst.Name == metadata.InstanceName {
					instanceType = inst.Type
					instanceName = inst.Name
					slog.InfoContext(bgCtx, "Fast path: Found instance from metadata", "instance", instanceName, "type", instanceType)
					break
				}
			}
		}

		// Fallback to path-based logic if instance not found from metadata
		if instanceName == "" {
			slog.DebugContext(bgCtx, "Instance not found from metadata, falling back to path-based detection", "path", pathForRescan)
			instanceType, instanceName, err = m.findInstanceForFilePath(bgCtx, pathForRescan, relativePath)
			if err != nil {
				return nil, fmt.Errorf("failed to find ARR instance for file path %s: %w", pathForRescan, err)
			}
		}

		// Find the instance configuration
		instanceConfig, err := m.instances.FindConfigInstance(instanceType, instanceName)
		if err != nil {
			return nil, fmt.Errorf("failed to find instance config: %w", err)
		}

		// Check if instance is enabled
		if !instanceConfig.Enabled {
			return nil, fmt.Errorf("instance %s/%s is disabled", instanceType, instanceName)
		}

		// Trigger rescan based on instance type
		switch instanceType {
		case "radarr":
			client, err := m.clients.GetOrCreateRadarrClient(instanceName, instanceConfig.URL, instanceConfig.APIKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create Radarr client: %w", err)
			}
			return nil, m.triggerRadarrRescanByPath(bgCtx, client, pathForRescan, relativePath, instanceName, metadata)

		case "sonarr":
			client, err := m.clients.GetOrCreateSonarrClientContext(bgCtx, instanceName, instanceConfig.URL, instanceConfig.APIKey, instanceConfig.APIVersion)
			if err != nil {
				return nil, fmt.Errorf("failed to create Sonarr client: %w", err)
			}
			return nil, m.triggerSonarrRescanByPath(bgCtx, client, pathForRescan, relativePath, instanceName, metadata)

		case "lidarr":
			client, err := m.clients.GetOrCreateLidarrClient(instanceName, instanceConfig.URL, instanceConfig.APIKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create Lidarr client: %w", err)
			}
			return nil, m.triggerLidarrRescanByPath(bgCtx, client, pathForRescan, relativePath, instanceName, metadata)

		case "readarr":
			client, err := m.clients.GetOrCreateReadarrClient(instanceName, instanceConfig.URL, instanceConfig.APIKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create Readarr client: %w", err)
			}
			return nil, m.triggerReadarrRescanByPath(bgCtx, client, pathForRescan, relativePath, instanceName, metadata)

		case "whisparr":
			client, err := m.clients.GetOrCreateWhisparrClient(instanceName, instanceConfig.URL, instanceConfig.APIKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create Whisparr client: %w", err)
			}
			return nil, m.triggerSonarrRescanByPath(bgCtx, client, pathForRescan, relativePath, instanceName, metadata)

		default:
			return nil, fmt.Errorf("unsupported instance type: %s", instanceType)
		}
	})

	if err != nil {
		return err
	}
	if res != nil {
		return res.(error)
	}
	return nil
}

// TriggerScanForFile finds the ARR instance managing the file and triggers a download scan on it.
func (m *Manager) TriggerScanForFile(ctx context.Context, filePath string) error {
	// Try to find which instance manages this file path
	instanceType, instanceName, err := m.findInstanceForFilePath(ctx, filePath, "")
	if err != nil {
		return err
	}

	instance, err := m.instances.FindConfigInstance(instanceType, instanceName)
	if err != nil {
		return err
	}

	if !instance.Enabled {
		return fmt.Errorf("instance %s is disabled", instanceName)
	}

	slog.InfoContext(ctx, "Triggering download scan for specific instance", "instance", instanceName, "type", instanceType)

	// Launch scan in background to not block caller
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		switch instance.Type {
		case "radarr":
			client, err := m.clients.GetOrCreateRadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(bgCtx, "Failed to create Radarr client for scan trigger", "instance", instance.Name, "error", err)
				return
			}
			// Trigger RefreshMonitoredDownloads
			_, err = client.SendCommandContext(bgCtx, &radarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
			if err != nil {
				slog.ErrorContext(bgCtx, "Failed to trigger RefreshMonitoredDownloads", "instance", instance.Name, "error", err)
			} else {
				slog.InfoContext(bgCtx, "Triggered RefreshMonitoredDownloads", "instance", instance.Name)
			}

		case "sonarr":
			client, err := m.clients.GetOrCreateSonarrClientContext(bgCtx, instance.Name, instance.URL, instance.APIKey, instance.APIVersion)
			if err != nil {
				slog.ErrorContext(bgCtx, "Failed to create Sonarr client for scan trigger", "instance", instance.Name, "error", err)
				return
			}
			// Trigger RefreshMonitoredDownloads
			_, err = client.SendCommandContext(bgCtx, &sonarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
			if err != nil {
				slog.ErrorContext(bgCtx, "Failed to trigger RefreshMonitoredDownloads", "instance", instance.Name, "error", err)
			} else {
				slog.InfoContext(bgCtx, "Triggered RefreshMonitoredDownloads", "instance", instance.Name)
			}
		case "lidarr":
			client, err := m.clients.GetOrCreateLidarrClient(instance.Name, instance.URL, instance.APIKey)
			if err == nil {
				_, _ = client.SendCommandContext(bgCtx, &lidarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
			}
		case "readarr":
			client, err := m.clients.GetOrCreateReadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err == nil {
				_, _ = client.SendCommandContext(bgCtx, &readarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
			}
		case "whisparr":
			client, err := m.clients.GetOrCreateWhisparrClient(instance.Name, instance.URL, instance.APIKey)
			if err == nil {
				_, _ = client.SendCommandContext(bgCtx, &sonarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
			}
		}
	}()

	return nil
}

// TriggerDownloadScan triggers the "Check For Finished Downloads" task in ARR instances
func (m *Manager) TriggerDownloadScan(ctx context.Context, instanceType string) {
	instances := m.instances.GetAllInstances()
	for _, instance := range instances {
		if !instance.Enabled || instance.Type != instanceType {
			continue
		}

		slog.DebugContext(ctx, "Triggering download client scan", "instance", instance.Name, "type", instance.Type)

		go func(inst *model.ConfigInstance) {
			_, _, _ = m.sf.Do(fmt.Sprintf("scan:%s", inst.Name), func() (interface{}, error) {
				bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				switch inst.Type {
				case "radarr":
					client, err := m.clients.GetOrCreateRadarrClient(inst.Name, inst.URL, inst.APIKey)
					if err != nil {
						slog.ErrorContext(bgCtx, "Failed to create Radarr client for scan trigger", "instance", inst.Name, "error", err)
						return nil, err
					}
					// Trigger RefreshMonitoredDownloads
					_, err = client.SendCommandContext(bgCtx, &radarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
					if err != nil {
						slog.ErrorContext(bgCtx, "Failed to trigger RefreshMonitoredDownloads", "instance", inst.Name, "error", err)
					} else {
						slog.InfoContext(bgCtx, "Triggered RefreshMonitoredDownloads", "instance", inst.Name)
					}

				case "sonarr":
					client, err := m.clients.GetOrCreateSonarrClientContext(bgCtx, inst.Name, inst.URL, inst.APIKey, inst.APIVersion)
					if err != nil {
						slog.ErrorContext(bgCtx, "Failed to create Sonarr client for scan trigger", "instance", inst.Name, "error", err)
						return nil, err
					}
					// Trigger RefreshMonitoredDownloads
					_, err = client.SendCommandContext(bgCtx, &sonarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
					if err != nil {
						slog.ErrorContext(bgCtx, "Failed to trigger RefreshMonitoredDownloads", "instance", inst.Name, "error", err)
					} else {
						slog.InfoContext(bgCtx, "Triggered RefreshMonitoredDownloads", "instance", inst.Name)
					}
				case "lidarr":
					client, err := m.clients.GetOrCreateLidarrClient(inst.Name, inst.URL, inst.APIKey)
					if err == nil {
						_, _ = client.SendCommandContext(bgCtx, &lidarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
					}
				case "readarr":
					client, err := m.clients.GetOrCreateReadarrClient(inst.Name, inst.URL, inst.APIKey)
					if err == nil {
						_, _ = client.SendCommandContext(bgCtx, &readarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
					}
				case "whisparr":
					client, err := m.clients.GetOrCreateWhisparrClient(inst.Name, inst.URL, inst.APIKey)
					if err == nil {
						_, _ = client.SendCommandContext(bgCtx, &sonarr.CommandRequest{Name: "RefreshMonitoredDownloads"})
					}
				}
				return nil, nil
			})
		}(instance)
	}
}

// radarrFileMatchesTarget reports whether the movie's current file is the corrupt
// target identified by filePath/relativePath. It is the single source of truth for
// the Radarr delete-safety decision: both the library path scan and the TMDB-id
// fallback route through it so the check cannot drift apart. A movie with no file
// never matches. Match forms: exact path, basename, .strm-stripped, relative-suffix.
func radarrFileMatchesTarget(movie *radarr.Movie, filePath, relativePath string) bool {
	if movie == nil || !movie.HasFile || movie.MovieFile == nil {
		return false
	}
	moviePath := movie.MovieFile.Path

	// Exact path match
	if moviePath == filePath {
		return true
	}

	// Basename match (robust when only the parent directory differs)
	if filepath.Base(moviePath) == filepath.Base(filePath) {
		return true
	}

	// .strm-stripped match: filePath is a .strm pointing at the real file
	if before, ok := strings.CutSuffix(filePath, ".strm"); ok {
		if strings.TrimSuffix(moviePath, filepath.Ext(moviePath)) == before {
			return true
		}
	}

	// Relative-suffix match
	if relativePath != "" {
		strippedRelative := strings.TrimSuffix(relativePath, ".strm")
		if strings.HasSuffix(moviePath, relativePath) ||
			strings.HasSuffix(strings.TrimSuffix(moviePath, filepath.Ext(moviePath)), strippedRelative) {
			return true
		}
	}

	return false
}

// triggerRadarrRescanByPath triggers a rescan in Radarr for the given file path
func (m *Manager) triggerRadarrRescanByPath(ctx context.Context, client *radarr.Radarr, filePath, relativePath, instanceName string, metadata *model.WebhookMetadata) error {
	slog.InfoContext(ctx, "Searching Radarr for matching movie",
		"instance", instanceName,
		"file_path", filePath,
		"relative_path", relativePath)

	var targetMovie *radarr.Movie
	var targetMovieFileID int64
	var sceneName string
	var err error

	// ID-Based Precision: If we have the movie ID from metadata, use it
	if metadata != nil && metadata.Movie != nil && metadata.Movie.Id > 0 {
		// Targeted lookup by internal Radarr movie ID instead of fetching the ENTIRE
		// movie library and scanning it. On large libraries the full GetMovies call can
		// exceed the HTTP client timeout, error out, and wrongly condemn the file.
		movie, lookupErr := client.GetMovieByIDContext(ctx, metadata.Movie.Id)
		if lookupErr != nil {
			slog.WarnContext(ctx, "Targeted Radarr movie lookup by ID failed, falling back to full library scan",
				"instance", instanceName, "movie_id", metadata.Movie.Id, "error", lookupErr)
		}

		// Fall back to the OLD method when the targeted lookup did not resolve the movie:
		// fetch the library and match by internal id. This recovers from a transient
		// targeted-endpoint failure and keeps the Smart Repair Guard working whenever the
		// metadata ids are present. If the id is genuinely gone (movie removed/re-added
		// with a new internal id) neither resolves it and we fall through to path matching.
		if movie == nil || movie.ID != metadata.Movie.Id {
			movies, listErr := m.data.GetMovies(ctx, client, instanceName)
			if listErr != nil {
				slog.WarnContext(ctx, "Radarr full library scan fallback failed, falling back to path matching",
					"instance", instanceName, "movie_id", metadata.Movie.Id, "error", listErr)
			} else {
				for _, candidate := range movies {
					if candidate.ID == metadata.Movie.Id {
						movie = candidate
						break
					}
				}
			}
		}

		if movie != nil && movie.ID == metadata.Movie.Id {
			targetMovie = movie

			if metadata.MovieFile != nil && metadata.MovieFile.Id > 0 {
				slog.InfoContext(ctx, "ID-Based Precision: Using metadata IDs for Radarr repair",
					"movie_id", metadata.Movie.Id,
					"movie_file_id", metadata.MovieFile.Id)
				sceneName = metadata.MovieFile.SceneName

				// Smart Repair Guard: Check if the movie already has a newer/different healthy file
				if movie.HasFile && movie.MovieFile != nil {
					if movie.MovieFile.ID != metadata.MovieFile.Id {
						slog.InfoContext(ctx, "Smart Repair Guard: Movie already has a different healthy file (likely upgraded). Skipping repair.",
							"movie", movie.Title,
							"old_file_id", metadata.MovieFile.Id,
							"new_file_id", movie.MovieFile.ID)
						return model.ErrEpisodeAlreadySatisfied
					}
					targetMovieFileID = movie.MovieFile.ID
				}
			} else {
				slog.InfoContext(ctx, "ID-Based Precision: Using metadata movie ID for Radarr repair",
					"movie_id", metadata.Movie.Id)
				if movie.HasFile && movie.MovieFile != nil {
					targetMovieFileID = movie.MovieFile.ID
				}
			}
		}
	}

	// Fallback to path-based guessing if ID-based precision failed or metadata was missing
	if targetMovie == nil {
		slog.DebugContext(ctx, "Falling back to path-based guessing for Radarr", "file_path", filePath)
		// Get all movies to find the one with matching file path
		movies, err := m.data.GetMovies(ctx, client, instanceName)
		if err != nil {
			return fmt.Errorf("failed to get movies from Radarr: %w", err)
		}

		for _, movie := range movies {
			// Route through the shared matcher so the library scan and the TMDB-id
			// fallback below apply identical path-matching (and thus identical
			// delete-safety) rules.
			if radarrFileMatchesTarget(movie, filePath, relativePath) {
				slog.InfoContext(ctx, "Found Radarr movie match by path",
					"movie", movie.Title,
					"path", movie.MovieFile.Path,
					"file_path", filePath)
				targetMovie = movie
				targetMovieFileID = movie.MovieFile.ID
				sceneName = movie.MovieFile.SceneName
				break
			}
		}
	}

	// TMDB-id fallback: a removed-and-re-added movie gets a NEW internal Radarr id, so
	// both the metadata-id lookup and the library path scan miss it and the file would
	// be wrongly condemned. Resolve by the stable TMDB id instead.
	//
	// CRITICAL DELETE SAFETY: the resolved movie's file is deleted only when it actually
	// matches the corrupt target, checked through the SAME radarrFileMatchesTarget matcher
	// used by the path scan above (so the two cannot drift apart):
	//   - file present AND matches   -> blocklist + delete + search (targetMovieFileID set)
	//   - file present but DIFFERENT -> ErrEpisodeAlreadySatisfied, no delete
	//   - no file yet                -> search only, no delete (targetMovieFileID stays 0)
	if targetMovie == nil && metadata != nil && metadata.Movie != nil && metadata.Movie.TmdbId > 0 {
		matches, lookupErr := client.GetMovieContext(ctx, &radarr.GetMovie{TMDBID: metadata.Movie.TmdbId})
		if lookupErr != nil {
			slog.WarnContext(ctx, "Radarr TMDB-id fallback lookup failed, falling through to queue/path-match handling",
				"instance", instanceName, "tmdb_id", metadata.Movie.TmdbId, "error", lookupErr)
		} else {
			for _, movie := range matches {
				if movie == nil || movie.TmdbID != metadata.Movie.TmdbId {
					continue
				}

				if movie.HasFile && movie.MovieFile != nil {
					if radarrFileMatchesTarget(movie, filePath, relativePath) {
						slog.InfoContext(ctx, "Radarr TMDB-id fallback matched the corrupt target file",
							"instance", instanceName, "tmdb_id", metadata.Movie.TmdbId,
							"movie", movie.Title, "movie_path", movie.MovieFile.Path)
						targetMovie = movie
						targetMovieFileID = movie.MovieFile.ID
						sceneName = movie.MovieFile.SceneName
					} else {
						// Re-added movie holds a DIFFERENT healthy file. Deleting it would
						// destroy a good file; report satisfied so the redundant AltMount copy
						// is cleaned up without any delete against Radarr.
						slog.InfoContext(ctx, "Radarr TMDB-id fallback found a different healthy file; not deleting",
							"instance", instanceName, "tmdb_id", metadata.Movie.TmdbId,
							"movie", movie.Title, "movie_path", movie.MovieFile.Path, "file_path", filePath)
						return model.ErrEpisodeAlreadySatisfied
					}
				} else {
					// Movie re-added but has no file yet — search only, never delete.
					slog.InfoContext(ctx, "Radarr TMDB-id fallback resolved movie with no file; search only",
						"instance", instanceName, "tmdb_id", metadata.Movie.TmdbId, "movie", movie.Title)
					targetMovie = movie
				}
			}

		}
	}

	if targetMovie == nil {
		slog.WarnContext(ctx, "No movie found with matching file path or ID in Radarr library, attempting queue-based failure",
			"instance", instanceName,
			"file_path", filePath)

		// Fallback: search in Radarr download queue for active/stuck imports
		if err := m.failRadarrQueueItemByPath(ctx, client, filePath); err == nil {
			return nil
		}

		return fmt.Errorf("no movie found with file path %s in library or queue: %w", filePath, model.ErrPathMatchFailed)
	}

	slog.InfoContext(ctx, "Found matching movie for file",
		"instance", instanceName,
		"movie_id", targetMovie.ID,
		"movie_title", targetMovie.Title,
		"movie_path", targetMovie.Path,
		"file_path", filePath)

	failAt := time.Now()
	blocklisted := false

	// If we found the movie and have a file ID, try to blocklist and delete the file
	if targetMovieFileID > 0 {
		// Try to blocklist the release associated with this file
		if err := m.blocklistRadarrMovieFile(ctx, client, targetMovie.ID, targetMovieFileID, relativePath, sceneName); err != nil {
			slog.WarnContext(ctx, "Failed to blocklist Radarr release", "error", err)
		}
		blocklisted = true
	} else {
		slog.InfoContext(ctx, "Movie has no specific file ID linked in Radarr, attempting release blocklist using metadata",
			"movie", targetMovie.Title)
		if metadata != nil && metadata.Movie != nil && metadata.Movie.Id > 0 {
			if err := m.blocklistRadarrMovieFile(ctx, client, targetMovie.ID, 0, relativePath, sceneName); err != nil {
				slog.WarnContext(ctx, "Failed to blocklist Radarr release using metadata fallback", "error", err)
			}
			blocklisted = true
		}
	}

	var autoSearch *arrCommand
	if blocklisted {
		auto, state, settleErr := m.settleRadarrAutoRedownload(ctx, client, instanceName, radarrRedownloadTarget{
			movieID:     targetMovie.ID,
			movieFileID: targetMovieFileID,
		}, failAt)
		if settleErr != nil {
			return settleErr
		}
		switch state {
		case redownloadHandled:
			return nil
		case redownloadSatisfied:
			return model.ErrEpisodeAlreadySatisfied
		}
		autoSearch = auto
	}

	// Failure breaker: every targeted re-search counts one failure-driven action
	// against the movie. At the threshold the movie is unmonitored instead of
	// re-searched so a dead release can't drive an endless re-grab storm.
	if m.movieBreakerTripped(instanceName, targetMovie.ID) {
		unmonitorRadarrMovie(ctx, client, instanceName, targetMovie.ID)
		return nil
	}

	// Step 3: Trigger targeted search for the missing movie
	response, err := m.sendRadarrSearch(ctx, client, instanceName, targetMovie.ID, autoSearch)
	if err != nil {
		return err
	}

	slog.InfoContext(ctx, "Successfully triggered Radarr targeted search for re-download",
		"instance", instanceName,
		"movie_id", targetMovie.ID,
		"command_id", response.ID)

	return nil
}

// triggerSonarrRescanByPath triggers a rescan in Sonarr for the given file path
func (m *Manager) triggerSonarrRescanByPath(ctx context.Context, client clients.SonarrClient, filePath, relativePath, instanceName string, metadata *model.WebhookMetadata) (resultErr error) {
	slog.InfoContext(ctx, "Searching Sonarr for matching series",
		"instance", instanceName,
		"file_path", filePath,
		"relative_path", relativePath)

	var targetSeriesID int64
	var targetSeriesTitle string
	var targetEpisodeFileID int64
	var sceneName string
	var err error

	// ID-Based Precision: If we have the series ID from metadata, use it
	if metadata != nil && metadata.Series != nil && metadata.Series.Id > 0 {
		targetSeriesID = metadata.Series.Id
		targetSeriesTitle = "Known Series (ID Based)"

		if metadata.EpisodeFile != nil && metadata.EpisodeFile.Id > 0 {
			slog.InfoContext(ctx, "ID-Based Precision: Using metadata IDs for Sonarr repair",
				"series_id", metadata.Series.Id,
				"episode_file_id", metadata.EpisodeFile.Id)
			targetEpisodeFileID = metadata.EpisodeFile.Id
			sceneName = metadata.EpisodeFile.SceneName
		} else {
			slog.InfoContext(ctx, "ID-Based Precision: Using metadata series ID for Sonarr repair",
				"series_id", metadata.Series.Id)
		}
	}

	// Fallback to path-based guessing if ID-based precision failed or metadata was missing
	if targetSeriesID == 0 {
		slog.DebugContext(ctx, "Falling back to path-based guessing for Sonarr", "file_path", filePath)

		// Get library directory from health config
		libraryDir := m.configGetter().MountPath
		cfg := m.configGetter()
		if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
			libraryDir = *cfg.Health.LibraryDir
		}

		slog.DebugContext(ctx, "Searching Sonarr for matching series by path",
			"library_dir", libraryDir)

		// Get all series to find the one that contains this file path
		series, err := m.data.GetSeries(ctx, client, instanceName)
		if err != nil {
			return fmt.Errorf("failed to get series from Sonarr: %w", err)
		}

		// Find the series that contains this file path
		var targetSeries *sonarr.Series
		for _, show := range series {
			if strings.Contains(filePath, show.Path) {
				targetSeries = show
				break
			}
		}

		if targetSeries == nil {
			// Fallback search for series using relative path
			for _, show := range series {
				showFolderName := filepath.Base(show.Path)
				if strings.Contains(relativePath, showFolderName) {
					slog.InfoContext(ctx, "Found series match by folder name", "series", show.Title, "folder", showFolderName)
					targetSeries = show
					break
				}
			}
		}

		if targetSeries == nil {
			slog.WarnContext(ctx, "No series found in Sonarr matching file path in library, attempting queue-based failure",
				"instance", instanceName,
				"file_path", filePath)

			// Fallback: search in Sonarr download queue for active/stuck imports
			if err := m.failSonarrQueueItemByPath(ctx, client, instanceName, filePath); err == nil {
				return nil
			} else if !errors.Is(err, errNoSonarrQueueMatch) {
				return err
			}

			return fmt.Errorf("no series found containing file path in library or queue: %s: %w", filePath, model.ErrPathMatchFailed)
		}

		targetSeriesID = targetSeries.ID
		targetSeriesTitle = targetSeries.Title

		slog.InfoContext(ctx, "Found matching series, searching for episode file",
			"series_title", targetSeriesTitle,
			"series_path", targetSeries.Path)

		// Get episode files for this series to find the matching file
		episodeFiles, err := m.data.GetEpisodeFiles(ctx, client, instanceName, targetSeriesID)
		if err != nil {
			return fmt.Errorf("failed to get episode files for series %s: %w", targetSeriesTitle, err)
		}

		// Find the episode file with matching path
		var targetEpisodeFile *sonarr.EpisodeFile
		for _, episodeFile := range episodeFiles {
			if episodeFile.Path == filePath {
				targetEpisodeFile = episodeFile
				break
			}

			// Try match by filename
			if filepath.Base(episodeFile.Path) == filepath.Base(filePath) {
				slog.InfoContext(ctx, "Found Sonarr episode match by filename", "path", episodeFile.Path)
				targetEpisodeFile = episodeFile
				break
			}

			// Try match without .strm extension
			if before, ok := strings.CutSuffix(filePath, ".strm"); ok {
				strippedPath := before
				if strings.TrimSuffix(episodeFile.Path, filepath.Ext(episodeFile.Path)) == strippedPath {
					targetEpisodeFile = episodeFile
					break
				}
			}

			// Try match with relative path
			if relativePath != "" {
				strippedRelative := strings.TrimSuffix(relativePath, ".strm")
				if strings.HasSuffix(episodeFile.Path, relativePath) ||
					strings.HasSuffix(strings.TrimSuffix(episodeFile.Path, filepath.Ext(episodeFile.Path)), strippedRelative) {
					slog.InfoContext(ctx, "Found Sonarr episode match by relative path suffix",
						"sonarr_path", episodeFile.Path,
						"relative_path", relativePath)
					targetEpisodeFile = episodeFile
					break
				}
			}
		}

		if targetEpisodeFile != nil {
			targetEpisodeFileID = targetEpisodeFile.ID
			sceneName = targetEpisodeFile.SceneName
		}
	}

	if targetEpisodeFileID == 0 {
		if err := m.failSonarrQueueItemByPath(ctx, client, instanceName, filePath); err == nil {
			return nil
		} else if !errors.Is(err, errNoSonarrQueueMatch) {
			return err
		}
	}
	event := m.sonarrRepairEvent(ctx, instanceName, relativePath, targetEpisodeFileID)
	if metadata != nil && strings.TrimSpace(metadata.DownloadID) != "" {
		event = failures.SonarrDownloadKey(instanceName, metadata.DownloadID)
	}
	unlock, lockErr := m.failures.LockAction(ctx, event)
	if lockErr != nil {
		return lockErr
	}
	defer unlock()
	completionKey := fmt.Sprintf("%s|repair:%d:%s", event, targetEpisodeFileID, relativePath)
	finish, acquired := m.failures.BeginAction(completionKey)
	if !acquired {
		return nil
	}
	stage, finishStage := m.startSonarrRepairStage(completionKey)
	defer func() {
		success := resultErr == nil || errors.Is(resultErr, model.ErrEpisodeAlreadySatisfied)
		finish(success)
		finishStage(success)
	}()

	failedDownloadID := m.sonarrSourceDownloadID(ctx, relativePath, metadata)
	var episodeIDs []int64
	var autoSearch *arrCommand

	// Get all episodes for this specific series
	episodes, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{
		SeriesID: targetSeriesID,
	})
	if err != nil {
		return fmt.Errorf("failed to get episodes for series %s: %w", targetSeriesTitle, err)
	}

	if targetEpisodeFileID > 0 {
		// Found the file record - get episodes linked to it
		for _, episode := range episodes {
			if episode.EpisodeFileID == targetEpisodeFileID {
				episodeIDs = append(episodeIDs, episode.ID)
			}
		}

		if len(episodeIDs) == 0 {
			// Recover intended targets after an earlier deletion, or from the
			// original webhook. File-cache entries cannot establish satisfaction.
			episodeIDs = append(episodeIDs, stage.EpisodeIDs...)
			if len(episodeIDs) == 0 && metadata != nil {
				for _, ep := range metadata.Episodes {
					if ep.Id > 0 {
						episodeIDs = append(episodeIDs, ep.Id)
					}
				}
			}
		}
		if len(episodeIDs) > 0 {
			stage.EpisodeIDs = append([]int64(nil), episodeIDs...)
		}

		if len(episodeIDs) > 0 {
			slog.DebugContext(ctx, "Found matching episodes by file ID",
				"episode_count", len(episodeIDs),
				"episode_file_id", targetEpisodeFileID)

			stage.EpisodeIDs = append([]int64(nil), episodeIDs...)
			failAt := stage.Created

			eligibleIDs, refreshErr := unresolvedSonarrRepairEpisodes(ctx, client, episodeIDs, targetEpisodeFileID, failedDownloadID)
			if refreshErr != nil {
				return refreshErr
			}
			if len(eligibleIDs) == 0 && !stage.CleanupStarted {
				return nil
			}
			stage.CleanupStarted = true
			_, exhausted := m.splitEpisodesByBreaker(instanceName, eligibleIDs, event)
			if err := unmonitorSonarrEpisodes(ctx, client, instanceName, exhausted); err != nil {
				return err
			}
			// Try to blocklist the release associated with this file
			if err := m.blocklistSonarrRepairOnce(ctx, client, event, targetSeriesID, targetEpisodeFileID, relativePath, episodeIDs, sceneName, failedDownloadID); err != nil {
				return err
			}

			auto, state, err := m.settleSonarrAutoRedownload(ctx, client, instanceName, sonarrRedownloadTarget{
				seriesID:      targetSeriesID,
				episodeIDs:    episodeIDs,
				episodeFileID: targetEpisodeFileID,
				downloadID:    failedDownloadID,
			}, failAt)
			if err != nil {
				return err
			}
			switch state {
			case redownloadHandled:
				return nil
			case redownloadSatisfied:
				return model.ErrEpisodeAlreadySatisfied
			}
			autoSearch = auto
		}
	} else {
		// Queue fallback was attempted before taking the file repair lock.
		// Fallback 2: If we have episode IDs from metadata, use them to trigger replacement search
		if metadata != nil && len(metadata.Episodes) > 0 {
			slog.InfoContext(ctx, "Using episode IDs from metadata to trigger replacement search", "series", targetSeriesTitle)
			for _, ep := range metadata.Episodes {
				episodeIDs = append(episodeIDs, ep.Id)
			}

			stage.EpisodeIDs = append([]int64(nil), episodeIDs...)
			failAt := stage.Created

			eligibleIDs, refreshErr := unresolvedSonarrRepairEpisodes(ctx, client, episodeIDs, targetEpisodeFileID, failedDownloadID)
			if refreshErr != nil {
				return refreshErr
			}
			if len(eligibleIDs) == 0 && !stage.CleanupStarted {
				return nil
			}
			stage.CleanupStarted = true
			_, exhausted := m.splitEpisodesByBreaker(instanceName, eligibleIDs, event)
			if err := unmonitorSonarrEpisodes(ctx, client, instanceName, exhausted); err != nil {
				return err
			}
			// Try to blocklist the release associated with these episodes
			if err := m.blocklistSonarrRepairOnce(ctx, client, event, targetSeriesID, 0, relativePath, episodeIDs, sceneName, failedDownloadID); err != nil {
				return err
			}

			auto, state, err := m.settleSonarrAutoRedownload(ctx, client, instanceName, sonarrRedownloadTarget{
				seriesID:   targetSeriesID,
				episodeIDs: episodeIDs,
				downloadID: failedDownloadID,
			}, failAt)
			if err != nil {
				return err
			}
			switch state {
			case redownloadHandled:
				return nil
			case redownloadSatisfied:
				return model.ErrEpisodeAlreadySatisfied
			}
			autoSearch = auto
		}
	}

	if len(episodeIDs) == 0 {
		return fmt.Errorf("no episodes found for file in library or queue: %s: %w", filePath, model.ErrPathMatchFailed)
	}

	// Automatic searches can cover only part of a pack. Refresh and search the
	// unresolved monitored siblings rather than repeating the full episode set.
	episodeIDs, err = unresolvedSonarrRepairEpisodes(ctx, client, episodeIDs, targetEpisodeFileID, failedDownloadID)
	if err != nil {
		return err
	}
	if len(episodeIDs) == 0 {
		return nil
	}

	// Failure breaker: every targeted re-search counts one failure-driven action
	// against each episode. Episodes at the threshold are unmonitored instead of
	// re-searched so a dead release can't drive an endless re-grab storm.
	searchIDs, giveUpIDs := m.splitEpisodesByBreaker(instanceName, episodeIDs, event)
	if err := unmonitorSonarrEpisodes(ctx, client, instanceName, giveUpIDs); err != nil {
		return err
	}
	if len(searchIDs) == 0 {
		return nil
	}

	searchIDs, err = reconcileSonarrSearch(ctx, client, stage, searchIDs)
	if err != nil {
		return err
	}
	if len(searchIDs) == 0 {
		return nil
	}
	stage.CommandAttempted = time.Now()
	// Trigger targeted episode search for the remaining episodes in this file
	response, err := m.sendSonarrSearch(ctx, client, instanceName, searchIDs, autoSearch)
	if err != nil {
		return err
	}

	slog.InfoContext(ctx, "Successfully triggered Sonarr targeted episode search for re-download",
		"instance", instanceName,
		"series_title", targetSeriesTitle,
		"episode_ids", searchIDs,
		"command_id", response.ID)

	return nil
}

// failRadarrQueueItemByPath searches for an item in the active Radarr queue by path and marks it as failed
func (m *Manager) failRadarrQueueItemByPath(ctx context.Context, client *radarr.Radarr, path string) error {
	queue, err := client.GetQueueContext(ctx, 0, 500)
	if err != nil {
		return fmt.Errorf("failed to get Radarr queue: %w", err)
	}

	for _, q := range queue.Records {
		// Try exact match, prefix match (if queue item is parent dir), or filename match
		if q.OutputPath == path ||
			(q.OutputPath != "" && strings.HasPrefix(filepath.ToSlash(path), filepath.ToSlash(q.OutputPath))) ||
			(q.OutputPath != "" && filepath.Base(q.OutputPath) == filepath.Base(path)) {
			slog.InfoContext(ctx, "Found matching item in Radarr download queue, marking as failed",
				"queue_id", q.ID, "path", path, "output_path", q.OutputPath)

			removeFromClient := true
			opts := &starr.QueueDeleteOpts{
				RemoveFromClient: &removeFromClient,
				BlockList:        true,
				SkipRedownload:   false,
			}
			return client.DeleteQueueContext(ctx, q.ID, opts)
		}
	}

	return fmt.Errorf("no matching item found in Radarr queue for path: %s", path)
}

// failSonarrQueueItemByPath searches for an item in the active Sonarr queue by path and marks it as failed
func (m *Manager) failSonarrQueueItemByPath(ctx context.Context, client clients.SonarrClient, instanceName, path string) error {
	groups, err := client.QueueGroups(ctx)
	if err != nil {
		return fmt.Errorf("read Sonarr queue: %w", err)
	}
	for _, group := range groups {
		q := group.Record
		if q == nil || !registrar.IsAltmountDownloadClient(q.DownloadClient) || q.OutputPath == "" {
			continue
		}
		source := strings.TrimRight(filepath.ToSlash(q.OutputPath), "/")
		target := filepath.ToSlash(path)
		if target != source && !strings.HasPrefix(target, source+"/") && filepath.Base(q.OutputPath) != filepath.Base(path) {
			continue
		}
		if m.sonarrQueueRepair == nil {
			return fmt.Errorf("grouped Sonarr queue executor unavailable; deferring repair")
		}
		return m.sonarrQueueRepair(ctx, instanceName, client, group)
	}
	return fmt.Errorf("%w for path: %s", errNoSonarrQueueMatch, path)
}

// blocklistRadarrMovieFile finds the history event for the given file and marks it as failed (blocklisting the release)
func (m *Manager) blocklistRadarrMovieFile(ctx context.Context, client *radarr.Radarr, movieID int64, fileID int64, relativePath string, sceneName string) error {
	slog.DebugContext(ctx, "Attempting to find and blocklist release for movie file", "movie_id", movieID, "file_id", fileID, "scene_name", sceneName)

	var downloadID string
	if m.repo != nil && relativePath != "" {
		if hist, err := m.repo.GetImportHistoryByPath(ctx, relativePath); err == nil && hist != nil {
			if hist.DownloadID != nil {
				downloadID = *hist.DownloadID
			}
		}
	}

	// Fetch history for this specific movie
	req := &starr.PageReq{PageSize: 100, SortKey: "date", SortDir: starr.SortDescend}
	if downloadID != "" {
		req.Set("downloadId", downloadID)
	} else {
		req.Set("movieId", strconv.FormatInt(movieID, 10))
	}

	history, err := client.GetHistoryPageContext(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to fetch Radarr history: %w", err)
	}

	targetFileID := strconv.FormatInt(fileID, 10)

	// 1. Find the import event to get the downloadId
	for _, record := range history.Records {
		if record.EventType == "movieFileImported" || record.EventType == "downloadFolderImported" {
			if record.Data.FileID == targetFileID {
				downloadID = record.DownloadID
				break
			}
		}
	}

	if downloadID == "" {
		slog.WarnContext(ctx, "Could not find import event in Radarr history for file, attempting to find latest grabbed event directly", "movie_id", movieID, "file_id", fileID)

		// Precision Fallback: Find the grabbed event matching the SceneName or MovieID
		for _, record := range history.Records {
			if record.EventType == "grabbed" && record.MovieID == movieID {
				if sceneName != "" && strings.Contains(strings.ToLower(record.SourceTitle), strings.ToLower(sceneName)) {
					slog.InfoContext(ctx, "Found grabbed history record using SceneName precision fallback, marking as failed to blocklist release",
						"history_id", record.ID, "source_title", record.SourceTitle)
					if failErr := client.FailContext(ctx, record.ID); failErr != nil {
						return fmt.Errorf("failed to fail Radarr grab event %d: %w", record.ID, failErr)
					}
					return nil
				}

				// Without an exact SceneName match, only fallback to the latest grab if it was recent (within 24 hours)
				if time.Since(record.Date) < 24*time.Hour {
					slog.InfoContext(ctx, "Found recent grabbed history record using MovieID fallback, marking as failed to blocklist release",
						"history_id", record.ID, "movie_id", movieID, "grabbed_at", record.Date)
					if failErr := client.FailContext(ctx, record.ID); failErr != nil {
						return fmt.Errorf("failed to fail Radarr grab event %d: %w", record.ID, failErr)
					}
					return nil
				}
			}
		}

		slog.WarnContext(ctx, "Could not find any matching or recent grab event in Radarr history to blocklist", "movie_id", movieID)
		return nil
	}

	// 2. Find the original grab event using the downloadId
	for _, record := range history.Records {
		if record.DownloadID == downloadID && record.EventType == "grabbed" {
			slog.InfoContext(ctx, "Found grabbed history record, marking as failed to blocklist release",
				"history_id", record.ID, "download_id", downloadID)
			if failErr := client.FailContext(ctx, record.ID); failErr != nil {
				return fmt.Errorf("failed to fail Radarr grab event %d: %w", record.ID, failErr)
			}
			return nil
		}
	}

	slog.WarnContext(ctx, "Could not find grab event in Radarr history for download", "download_id", downloadID)
	return nil
}

// blocklistSonarrEpisodeFile finds the grabbed history event for the given file and marks it as failed (blocklisting the release)
func (m *Manager) blocklistSonarrEpisodeFile(ctx context.Context, client clients.SonarrClient, seriesID int64, fileID int64, relativePath string, episodeIDs []int64, sceneName string, attempted *time.Time, downloadIDs ...string) error {
	slog.DebugContext(ctx, "Attempting to find and blocklist release for episode file", "series_id", seriesID, "file_id", fileID, "scene_name", sceneName)

	var downloadID string
	if len(downloadIDs) > 0 {
		downloadID = downloadIDs[0]
	}
	if downloadID == "" && m.repo != nil && relativePath != "" {
		if hist, err := m.repo.GetImportHistoryByPath(ctx, relativePath); err == nil && hist != nil {
			if hist.DownloadID != nil {
				downloadID = *hist.DownloadID
			}
		}
	}

	// Fetch history for this specific series
	req := &starr.PageReq{PageSize: 100, SortKey: "date", SortDir: starr.SortDescend}
	if downloadID != "" {
		req.Set("downloadId", downloadID)
	} else {
		req.Set("seriesId", strconv.FormatInt(seriesID, 10))
		if len(episodeIDs) > 0 {
			req.Set("episodeId", strconv.FormatInt(episodeIDs[0], 10))
		}
	}

	history, err := client.GetHistoryPageContext(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to fetch Sonarr history: %w", err)
	}

	// A response can be lost after Sonarr accepted a history failure. Its
	// failed event is definitive acknowledgement; never fail the grabbed event twice.
	if downloadID != "" {
		for _, record := range history.Records {
			if record == nil {
				continue
			}
			if record != nil && strings.EqualFold(record.DownloadID, downloadID) && record.EventType == "downloadFailed" {
				return nil
			}
		}
	}

	failGrab := func(grab *sonarr.HistoryRecord) error {
		if grab.DownloadID != "" {
			for _, record := range history.Records {
				if record != nil && record.EventType == "downloadFailed" && strings.EqualFold(record.DownloadID, grab.DownloadID) {
					return nil
				}
			}
		}
		if attempted != nil && !attempted.IsZero() && time.Since(*attempted) < 30*time.Second {
			return fmt.Errorf("waiting to reconcile uncertain Sonarr history failure")
		}
		if attempted != nil {
			*attempted = time.Now()
		}
		err := client.FailContext(ctx, grab.ID)
		var request *starr.ReqError
		if err != nil && attempted != nil && errors.As(err, &request) && request.Code >= 400 && request.Code < 500 {
			*attempted = time.Time{}
		}
		return err
	}

	targetFileID := strconv.FormatInt(fileID, 10)

	// 1. Find the import event to get the downloadId
	for _, record := range history.Records {
		if record == nil {
			continue
		}
		if downloadID == "" && record.EventType == "downloadFolderImported" {
			if record.Data.FileID == targetFileID {
				downloadID = record.DownloadID
				break
			}

			// Fallback: Sonarr history might not expose fileId in data consistently. Match by EpisodeID.
			for _, epID := range episodeIDs {
				if record.EpisodeID == epID {
					slog.DebugContext(ctx, "Found import event using EpisodeID fallback", "episode_id", epID, "download_id", record.DownloadID)
					downloadID = record.DownloadID
					break
				}
			}
			if downloadID != "" {
				break
			}
		}
	}

	if downloadID == "" {
		slog.WarnContext(ctx, "Could not find import event in Sonarr history for file, attempting to find grabbed event directly", "series_id", seriesID, "file_id", fileID)

		// Precision Fallback: Find the grabbed event matching the SceneName
		for _, record := range history.Records {
			if record == nil {
				continue
			}
			if record.EventType == "grabbed" {
				if sceneName != "" && strings.Contains(strings.ToLower(record.SourceTitle), strings.ToLower(sceneName)) {
					slog.InfoContext(ctx, "Found grabbed history record using SceneName precision fallback, marking as failed to blocklist release",
						"history_id", record.ID, "source_title", record.SourceTitle)
					if failErr := failGrab(record); failErr != nil {
						return fmt.Errorf("failed to fail Sonarr grab event %d: %w", record.ID, failErr)
					}
					return nil
				}

				for _, epID := range episodeIDs {
					if record.EpisodeID == epID {
						// Only fallback if the grabbed event was recent (within 24 hours)
						if time.Since(record.Date) < 24*time.Hour {
							slog.InfoContext(ctx, "Found recent grabbed history record using EpisodeID fallback, marking as failed to blocklist release",
								"history_id", record.ID, "episode_id", epID, "grabbed_at", record.Date)
							if failErr := failGrab(record); failErr != nil {
								return fmt.Errorf("failed to fail Sonarr grab event %d: %w", record.ID, failErr)
							}
							return nil
						}
					}
				}
			}
		}

		slog.WarnContext(ctx, "Could not find any matching or recent grab event in Sonarr history to blocklist", "series_id", seriesID)
		return nil
	}

	// 2. Find the original grab event using the downloadId
	for _, record := range history.Records {
		if record == nil {
			continue
		}
		if strings.EqualFold(record.DownloadID, downloadID) && record.EventType == "grabbed" {
			slog.InfoContext(ctx, "Found grabbed history record, marking as failed to blocklist release",
				"history_id", record.ID, "download_id", downloadID)
			if failErr := failGrab(record); failErr != nil {
				return fmt.Errorf("failed to fail Sonarr grab event %d: %w", record.ID, failErr)
			}
			return nil
		}
	}
	slog.WarnContext(ctx, "Could not find grab event in Sonarr history for download", "download_id", downloadID)
	return nil
}

// triggerLidarrRescanByPath triggers a rescan in Lidarr
func (m *Manager) triggerLidarrRescanByPath(ctx context.Context, client *lidarr.Lidarr, filePath, relativePath, instanceName string, metadata *model.WebhookMetadata) error {
	slog.InfoContext(ctx, "Searching Lidarr for matching track", "instance", instanceName, "file_path", filePath)

	var targetAlbumID int64
	var targetTrackFileID int64

	if metadata != nil && metadata.Album != nil && metadata.TrackFile != nil && metadata.Album.Id > 0 && metadata.TrackFile.Id > 0 {
		targetAlbumID = metadata.Album.Id
		targetTrackFileID = metadata.TrackFile.Id
	}

	if targetAlbumID == 0 {
		m.TriggerScanForFile(ctx, filePath)
		return nil
	}

	if targetTrackFileID > 0 {
		if err := m.blocklistLidarrTrackFile(ctx, client, targetAlbumID, targetTrackFileID); err != nil {
			slog.WarnContext(ctx, "Failed to blocklist Lidarr release", "error", err)
		}
		_ = client.DeleteTrackFileContext(ctx, targetTrackFileID)
	}

	searchCmd := &lidarr.CommandRequest{
		Name:     "AlbumSearch",
		AlbumIDs: []int64{targetAlbumID},
	}
	_, err := client.SendCommandContext(ctx, searchCmd)
	if err != nil {
		return fmt.Errorf("failed to trigger Lidarr search: %w", err)
	}

	return nil
}

// blocklistLidarrTrackFile marks a track file as failed
func (m *Manager) blocklistLidarrTrackFile(ctx context.Context, client *lidarr.Lidarr, albumID int64, fileID int64) error {
	req := &starr.PageReq{PageSize: 100, SortKey: "date", SortDir: starr.SortDescend}
	req.Set("albumId", strconv.FormatInt(albumID, 10))

	history, err := client.GetHistoryPageContext(ctx, req)
	if err != nil {
		return err
	}

	var downloadID string

	for _, record := range history.Records {
		// Attempting Data.TrackFileId or fallback if needed
		if record.EventType == "trackFileImported" {
			// Without knowing exact starr mapping, checking DownloadID is safer if it's there
			downloadID = record.DownloadID
			break
		}
	}

	if downloadID == "" {
		return nil
	}

	for _, record := range history.Records {
		if record.DownloadID == downloadID && record.EventType == "grabbed" {
			return client.FailContext(ctx, record.ID)
		}
	}
	return nil
}

// triggerReadarrRescanByPath triggers a rescan in Readarr
func (m *Manager) triggerReadarrRescanByPath(ctx context.Context, client *readarr.Readarr, filePath, relativePath, instanceName string, metadata *model.WebhookMetadata) error {
	slog.InfoContext(ctx, "Searching Readarr for matching book", "instance", instanceName, "file_path", filePath)

	var targetBookID int64
	var targetBookFileID int64

	if metadata != nil && metadata.Book != nil && metadata.BookFile != nil && metadata.Book.Id > 0 && metadata.BookFile.Id > 0 {
		targetBookID = metadata.Book.Id
		targetBookFileID = metadata.BookFile.Id
	}

	if targetBookID == 0 {
		m.TriggerScanForFile(ctx, filePath)
		return nil
	}

	if targetBookFileID > 0 {
		if err := m.blocklistReadarrBookFile(ctx, client, targetBookID, targetBookFileID); err != nil {
			slog.WarnContext(ctx, "Failed to blocklist Readarr release", "error", err)
		}
		_ = client.DeleteBookFileContext(ctx, targetBookFileID)
	}

	searchCmd := &readarr.CommandRequest{
		Name:    "BookSearch",
		BookIDs: []int64{targetBookID},
	}
	_, err := client.SendCommandContext(ctx, searchCmd)
	if err != nil {
		return fmt.Errorf("failed to trigger Readarr search: %w", err)
	}

	return nil
}

// blocklistReadarrBookFile marks a book file as failed
func (m *Manager) blocklistReadarrBookFile(ctx context.Context, client *readarr.Readarr, bookID int64, fileID int64) error {
	req := &starr.PageReq{PageSize: 100, SortKey: "date", SortDir: starr.SortDescend}
	req.Set("bookId", strconv.FormatInt(bookID, 10))

	history, err := client.GetHistoryPageContext(ctx, req)
	if err != nil {
		return err
	}

	var downloadID string

	for _, record := range history.Records {
		if record.EventType == "bookFileImported" {
			downloadID = record.DownloadID
			break
		}
	}

	if downloadID == "" {
		return nil
	}

	for _, record := range history.Records {
		if record.DownloadID == downloadID && record.EventType == "grabbed" {
			return client.FailContext(ctx, record.ID)
		}
	}
	return nil
}
