package api

import (
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/javi11/altmount/internal/auth"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
)

// nzbJobName returns the display name for an NZB job by stripping the .nzb or .nzb.gz
// extension from the base filename, accounting for compressed storage.
func nzbJobName(nzbPath string) string {
	name := filepath.Base(nzbPath)
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".nzb.gz"):
		name = name[:len(name)-len(".nzb.gz")]
	case strings.HasSuffix(lower, ".nzb"):
		name = name[:len(name)-len(".nzb")]
	}
	return name
}

// API Response Wrappers for sensitive data masking

// ConfigAPIResponse wraps config.Config with sensitive data handling
type ConfigAPIResponse struct {
	*config.Config
	WebDAV          WebDAVAPIResponse     `json:"webdav"`
	Import          ImportAPIResponse     `json:"import"`
	RClone          RCloneAPIResponse     `json:"rclone"`
	SABnzbd         SABnzbdAPIResponse    `json:"sabnzbd"`
	Providers       []ProviderAPIResponse `json:"providers"`
	Arrs            ArrsAPIResponse       `json:"arrs"`
	Stremio         StremioAPIResponse    `json:"stremio"`
	APIKey          string                `json:"api_key,omitempty"`      // User's API key for authentication
	DownloadKey     string                `json:"download_key,omitempty"` // SHA256 of the API key, used for download/stream URLs
	ProfilerEnabled bool                  `json:"profiler_enabled"`
}

// WebDAVAPIResponse sanitizes WebDAV config for API responses
type WebDAVAPIResponse struct {
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"` // Masked if set
	Host     string `json:"host"`
}

// RCloneAPIResponse sanitizes RClone config for API responses
type RCloneAPIResponse struct {
	// Encryption
	PasswordSet bool `json:"password_set"`
	SaltSet     bool `json:"salt_set"`

	// RC (Remote Control) Configuration

	RCEnabled bool `json:"rc_enabled"`

	RCUrl string `json:"rc_url"`

	VFSName string `json:"vfs_name"`

	RCPort int `json:"rc_port"`

	RCUser    string            `json:"rc_user"`
	RCPassSet bool              `json:"rc_pass_set"`
	RCOptions map[string]string `json:"rc_options"`

	// Mount Configuration
	MountEnabled bool              `json:"mount_enabled"`
	MountOptions map[string]string `json:"mount_options"`

	// Mount-Specific Settings
	AllowOther    bool   `json:"allow_other"`
	AllowNonEmpty bool   `json:"allow_non_empty"`
	Links         bool   `json:"links"`
	ReadOnly      bool   `json:"read_only"`
	Timeout       string `json:"timeout"`
	Syslog        bool   `json:"syslog"`

	// System and filesystem options
	LogLevel    string `json:"log_level"`
	UID         int    `json:"uid"`
	GID         int    `json:"gid"`
	Umask       string `json:"umask"`
	BufferSize  string `json:"buffer_size"`
	AttrTimeout string `json:"attr_timeout"`
	Transfers   int    `json:"transfers"`

	// VFS Cache Settings
	CacheDir              string `json:"cache_dir"`
	VFSCacheMode          string `json:"vfs_cache_mode"`
	VFSCachePollInterval  string `json:"vfs_cache_poll_interval"`
	VFSReadChunkSize      string `json:"vfs_read_chunk_size"`
	VFSReadChunkSizeLimit string `json:"vfs_read_chunk_size_limit"`
	VFSCacheMaxSize       string `json:"vfs_cache_max_size"`
	VFSCacheMaxAge        string `json:"vfs_cache_max_age"`
	VFSReadAhead          string `json:"vfs_read_ahead"`
	DirCacheTime          string `json:"dir_cache_time"`
	VFSCacheMinFreeSpace  string `json:"vfs_cache_min_free_space"`
	VFSDiskSpaceTotal     string `json:"vfs_disk_space_total"`
	VFSReadChunkStreams   int    `json:"vfs_read_chunk_streams"`

	// Advanced Settings
	NoModTime          bool `json:"no_mod_time"`
	NoChecksum         bool `json:"no_checksum"`
	AsyncRead          bool `json:"async_read"`
	VFSFastFingerprint bool `json:"vfs_fast_fingerprint"`
	UseMmap            bool `json:"use_mmap"`
}

// ProviderAPIResponse sanitizes Provider config for API responses
type ProviderAPIResponse struct {
	// ID is a stable public provider identifier; it is not an authentication field.
	ID                       string     `json:"id"`
	Host                     string     `json:"host"`
	Port                     int        `json:"port"`
	Username                 string     `json:"username"`
	MaxConnections           int        `json:"max_connections"`
	TLS                      bool       `json:"tls"`
	InsecureTLS              bool       `json:"insecure_tls"`
	ProxyURL                 string     `json:"proxy_url,omitempty"`
	PasswordSet              bool       `json:"password_set"`
	Enabled                  bool       `json:"enabled"`
	IsBackupProvider         bool       `json:"is_backup_provider"`
	InflightRequests         int        `json:"inflight_requests"`
	LastRTTMs                int64      `json:"last_rtt_ms"`
	LastSpeedTestMbps        float64    `json:"last_speed_test_mbps"`
	LastSpeedTestTime        *time.Time `json:"last_speed_test_time,omitempty"`
	SkipPing                 bool       `json:"skip_ping"`
	KeepaliveIntervalSeconds int        `json:"keepalive_interval_seconds"`
	KeepaliveCommand         string     `json:"keepalive_command,omitempty"`
	UserAgent                string     `json:"user_agent,omitempty"`
	QuotaBytes               int64      `json:"quota_bytes"`
	QuotaPeriodHours         int        `json:"quota_period_hours"`
}

// ImportAPIResponse handles Import config for API responses
type ImportAPIResponse struct {
	MaxProcessorWorkers            int                   `json:"max_processor_workers"`
	QueueProcessingIntervalSeconds int                   `json:"queue_processing_interval_seconds"` // Interval in seconds
	AllowedFileExtensions          []string              `json:"allowed_file_extensions"`
	MaxImportConnections           int                   `json:"max_import_connections"`
	MaxDownloadPrefetch            int                   `json:"max_download_prefetch"`
	ReadTimeoutSeconds             int                   `json:"read_timeout_seconds"`
	SegmentSamplePercentage        int                   `json:"segment_sample_percentage"` // Percentage of segments to check (1-100)
	ImportStrategy                 config.ImportStrategy `json:"import_strategy"`
	ImportDir                      *string               `json:"import_dir"`
	WatchDir                       *string               `json:"watch_dir"`

	WatchIntervalSeconds     *int  `json:"watch_interval_seconds,omitempty"`
	AllowNestedRarExtraction *bool `json:"allow_nested_rar_extraction,omitempty"`
	RenameToNzbName          *bool `json:"rename_to_nzb_name,omitempty"`
	FilterSampleFiles        *bool `json:"filter_sample_files,omitempty"`
	CompressNzb              *bool `json:"compress_nzb,omitempty"`
}

// SABnzbdAPIResponse sanitizes SABnzbd config for API responses
type SABnzbdAPIResponse struct {
	Enabled                 bool                     `json:"enabled"`
	CompleteDir             string                   `json:"complete_dir"`
	DownloadClientBaseURL   string                   `json:"download_client_base_url"`
	Categories              []config.SABnzbdCategory `json:"categories"`
	HistoryRetentionMinutes int                      `json:"history_retention_minutes"`
	FallbackHost            string                   `json:"fallback_host"`
	FallbackAPIKey          string                   `json:"fallback_api_key"`     // Obfuscated if set
	FallbackAPIKeySet       bool                     `json:"fallback_api_key_set"` // Indicates if API key is set
}

// ArrsAPIResponse sanitizes Arrs config for API responses
type ArrsAPIResponse struct {
	Enabled                        bool                      `json:"enabled"`
	MaxWorkers                     int                       `json:"max_workers,omitempty"`
	WebhookBaseURL                 string                    `json:"webhook_base_url,omitempty"`
	RadarrInstances                []ArrsInstanceAPIResponse `json:"radarr_instances"`
	SonarrInstances                []ArrsInstanceAPIResponse `json:"sonarr_instances"`
	LidarrInstances                []ArrsInstanceAPIResponse `json:"lidarr_instances"`
	ReadarrInstances               []ArrsInstanceAPIResponse `json:"readarr_instances"`
	WhisparrInstances              []ArrsInstanceAPIResponse `json:"whisparr_instances"`
	SportarrInstances              []ArrsInstanceAPIResponse `json:"sportarr_instances"`
	QueueCleanupEnabled            bool                      `json:"queue_cleanup_enabled,omitempty"`
	QueueCleanupIntervalSeconds    int                       `json:"queue_cleanup_interval_seconds,omitempty"`
	QueueCleanupGracePeriodMinutes int                       `json:"queue_cleanup_grace_period_minutes,omitempty"`
	QueueCleanupMaxFailures        int                       `json:"queue_cleanup_max_failures,omitempty"`
	QueueCleanupRules              []config.StuckCleanupRule `json:"queue_cleanup_rules,omitempty"`
}

// ArrsInstanceAPIResponse sanitizes ArrsInstance config for API responses
type ArrsInstanceAPIResponse struct {
	Name              string `json:"name"`
	URL               string `json:"url"`
	APIKey            string `json:"api_key"`
	APIKeySet         bool   `json:"api_key_set"`
	Category          string `json:"category,omitempty"`
	Enabled           bool   `json:"enabled,omitempty"`
	SyncIntervalHours *int   `json:"sync_interval_hours,omitempty"`
}

// StremioAPIResponse sanitizes Stremio config for API responses
type StremioAPIResponse struct {
	Enabled     bool                `json:"enabled"`
	NzbTTLHours int                 `json:"nzb_ttl_hours,omitempty"`
	BaseURL     string              `json:"base_url,omitempty"`
	Prowlarr    ProwlarrAPIResponse `json:"prowlarr"`
}

// ProwlarrAPIResponse sanitizes Prowlarr config for API responses
type ProwlarrAPIResponse struct {
	Enabled    bool     `json:"enabled"`
	Host       string   `json:"host,omitempty"`
	APIKey     string   `json:"api_key"`
	APIKeySet  bool     `json:"api_key_set"`
	Categories []int    `json:"categories,omitempty"`
	Languages  []string `json:"languages,omitempty"`
	Qualities  []string `json:"qualities,omitempty"`
}

// Helper functions to create API responses from core config types

// ToConfigAPIResponse converts config.Config to ConfigAPIResponse with sensitive data masked
func ToConfigAPIResponse(cfg *config.Config, apiKey string) *ConfigAPIResponse {
	if cfg == nil {
		return nil
	}

	// Convert providers with password masking
	providers := make([]ProviderAPIResponse, len(cfg.Providers))
	for i, p := range cfg.Providers {
		providers[i] = ProviderAPIResponse{
			ID:                       p.ID,
			Host:                     p.Host,
			Port:                     p.Port,
			Username:                 p.Username,
			MaxConnections:           p.MaxConnections,
			TLS:                      p.TLS,
			InsecureTLS:              p.InsecureTLS,
			ProxyURL:                 p.ProxyURL,
			PasswordSet:              p.Password != "",
			Enabled:                  p.Enabled != nil && *p.Enabled,
			IsBackupProvider:         p.IsBackupProvider != nil && *p.IsBackupProvider,
			InflightRequests:         p.InflightRequests,
			LastRTTMs:                p.LastRTTMs,
			LastSpeedTestMbps:        p.LastSpeedTestMbps,
			LastSpeedTestTime:        p.LastSpeedTestTime,
			SkipPing:                 p.SkipPing,
			KeepaliveIntervalSeconds: p.KeepaliveIntervalSeconds,
			KeepaliveCommand:         p.KeepaliveCommand,
			QuotaBytes:               p.QuotaBytes,
			QuotaPeriodHours:         p.QuotaPeriodHours,
		}
	}

	// Create RClone response with all configuration fields
	rcloneResp := RCloneAPIResponse{
		PasswordSet:  cfg.RClone.Password != "",
		SaltSet:      cfg.RClone.Salt != "",
		RCEnabled:    cfg.RClone.RCEnabled != nil && *cfg.RClone.RCEnabled,
		RCUrl:        cfg.RClone.RCUrl,
		VFSName:      cfg.RClone.VFSName,
		RCPort:       cfg.RClone.RCPort,
		RCUser:       cfg.RClone.RCUser,
		RCPassSet:    cfg.RClone.RCPass != "",
		RCOptions:    cfg.RClone.RCOptions,
		MountEnabled: cfg.RClone.MountEnabled != nil && *cfg.RClone.MountEnabled,
		MountOptions: cfg.RClone.MountOptions,

		// Mount-Specific Settings
		AllowOther:    cfg.RClone.AllowOther,
		AllowNonEmpty: cfg.RClone.AllowNonEmpty,
		Links:         cfg.RClone.Links,
		ReadOnly:      cfg.RClone.ReadOnly,
		Timeout:       cfg.RClone.Timeout,
		Syslog:        cfg.RClone.Syslog,

		// System and filesystem options
		LogLevel:    cfg.RClone.LogLevel,
		UID:         cfg.RClone.UID,
		GID:         cfg.RClone.GID,
		Umask:       cfg.RClone.Umask,
		BufferSize:  cfg.RClone.BufferSize,
		AttrTimeout: cfg.RClone.AttrTimeout,
		Transfers:   cfg.RClone.Transfers,

		// VFS Cache Settings
		CacheDir:              cfg.RClone.CacheDir,
		VFSCacheMode:          cfg.RClone.VFSCacheMode,
		VFSCachePollInterval:  cfg.RClone.VFSCachePollInterval,
		VFSReadChunkSize:      cfg.RClone.VFSReadChunkSize,
		VFSReadChunkSizeLimit: cfg.RClone.VFSReadChunkSizeLimit,
		VFSCacheMaxSize:       cfg.RClone.VFSCacheMaxSize,
		VFSCacheMaxAge:        cfg.RClone.VFSCacheMaxAge,
		VFSReadAhead:          cfg.RClone.VFSReadAhead,
		DirCacheTime:          cfg.RClone.DirCacheTime,
		VFSCacheMinFreeSpace:  cfg.RClone.VFSCacheMinFreeSpace,
		VFSDiskSpaceTotal:     cfg.RClone.VFSDiskSpaceTotal,
		VFSReadChunkStreams:   cfg.RClone.VFSReadChunkStreams,

		// Advanced Settings
		NoModTime:          cfg.RClone.NoModTime,
		NoChecksum:         cfg.RClone.NoChecksum,
		AsyncRead:          cfg.RClone.AsyncRead,
		VFSFastFingerprint: cfg.RClone.VFSFastFingerprint,
		UseMmap:            cfg.RClone.UseMmap,
	}

	// Create SABnzbd response with API key obfuscated
	fallbackAPIKey := ""
	if cfg.SABnzbd.FallbackAPIKey != "" {
		fallbackAPIKey = "********" // Obfuscate the actual key
	}

	sabnzbdResp := SABnzbdAPIResponse{
		Enabled:                 cfg.SABnzbd.Enabled != nil && *cfg.SABnzbd.Enabled,
		CompleteDir:             cfg.SABnzbd.CompleteDir,
		DownloadClientBaseURL:   cfg.SABnzbd.DownloadClientBaseURL,
		Categories:              cfg.SABnzbd.Categories,
		HistoryRetentionMinutes: cfg.SABnzbd.HistoryRetentionMinutes,
		FallbackHost:            cfg.SABnzbd.FallbackHost,
		FallbackAPIKey:          fallbackAPIKey,
		FallbackAPIKeySet:       cfg.SABnzbd.FallbackAPIKey != "",
	}

	webdavResp := WebDAVAPIResponse{
		Port:     cfg.WebDAV.Port,
		User:     cfg.WebDAV.User,
		Password: "********", // Mask password
		Host:     cfg.WebDAV.Host,
	}

	downloadKey := ""
	if apiKey != "" {
		downloadKey = auth.HashAPIKey(apiKey)
	}

	toArrsInstances := func(instances []config.ArrsInstanceConfig) []ArrsInstanceAPIResponse {
		var resp []ArrsInstanceAPIResponse
		for _, inst := range instances {
			resp = append(resp, ArrsInstanceAPIResponse{
				Name:              inst.Name,
				URL:               inst.URL,
				APIKey:            inst.APIKey,
				APIKeySet:         inst.APIKey != "",
				Category:          inst.Category,
				Enabled:           inst.Enabled != nil && *inst.Enabled,
				SyncIntervalHours: inst.SyncIntervalHours,
			})
		}
		if resp == nil {
			resp = []ArrsInstanceAPIResponse{}
		}
		return resp
	}

	arrsResp := ArrsAPIResponse{
		Enabled:                        cfg.Arrs.Enabled != nil && *cfg.Arrs.Enabled,
		MaxWorkers:                     cfg.Arrs.MaxWorkers,
		WebhookBaseURL:                 cfg.Arrs.WebhookBaseURL,
		RadarrInstances:                toArrsInstances(cfg.Arrs.RadarrInstances),
		SonarrInstances:                toArrsInstances(cfg.Arrs.SonarrInstances),
		LidarrInstances:                toArrsInstances(cfg.Arrs.LidarrInstances),
		ReadarrInstances:               toArrsInstances(cfg.Arrs.ReadarrInstances),
		WhisparrInstances:              toArrsInstances(cfg.Arrs.WhisparrInstances),
		SportarrInstances:              toArrsInstances(cfg.Arrs.SportarrInstances),
		QueueCleanupEnabled:            cfg.Arrs.QueueCleanupEnabled != nil && *cfg.Arrs.QueueCleanupEnabled,
		QueueCleanupIntervalSeconds:    cfg.Arrs.QueueCleanupIntervalSeconds,
		QueueCleanupGracePeriodMinutes: cfg.Arrs.QueueCleanupGracePeriodMinutes,
		QueueCleanupMaxFailures:        cfg.Arrs.QueueCleanupMaxFailures,
		QueueCleanupRules:              cfg.Arrs.QueueCleanupRules,
	}

	stremioResp := StremioAPIResponse{
		Enabled:     cfg.Stremio.Enabled != nil && *cfg.Stremio.Enabled,
		NzbTTLHours: cfg.Stremio.NzbTTLHours,
		BaseURL:     cfg.Stremio.BaseURL,
		Prowlarr: ProwlarrAPIResponse{
			Enabled:    cfg.Stremio.Prowlarr.Enabled != nil && *cfg.Stremio.Prowlarr.Enabled,
			Host:       cfg.Stremio.Prowlarr.Host,
			APIKey:     cfg.Stremio.Prowlarr.APIKey,
			APIKeySet:  cfg.Stremio.Prowlarr.APIKey != "",
			Categories: cfg.Stremio.Prowlarr.Categories,
			Languages:  cfg.Stremio.Prowlarr.Languages,
			Qualities:  cfg.Stremio.Prowlarr.Qualities,
		},
	}

	return &ConfigAPIResponse{
		Config:          cfg,
		WebDAV:          webdavResp,
		Import:          ToImportAPIResponse(cfg.Import),
		RClone:          rcloneResp,
		SABnzbd:         sabnzbdResp,
		Providers:       providers,
		Arrs:            arrsResp,
		Stremio:         stremioResp,
		APIKey:          apiKey,
		DownloadKey:     downloadKey,
		ProfilerEnabled: cfg.ProfilerEnabled,
	}
}

func ToImportAPIResponse(importConfig config.ImportConfig) ImportAPIResponse {
	return ImportAPIResponse{
		MaxProcessorWorkers:            importConfig.MaxProcessorWorkers,
		QueueProcessingIntervalSeconds: importConfig.QueueProcessingIntervalSeconds,
		AllowedFileExtensions:          importConfig.AllowedFileExtensions,
		MaxImportConnections:           importConfig.MaxImportConnections,
		MaxDownloadPrefetch:            importConfig.MaxDownloadPrefetch,
		ReadTimeoutSeconds:             importConfig.ReadTimeoutSeconds,
		SegmentSamplePercentage:        importConfig.SegmentSamplePercentage,
		ImportStrategy:                 importConfig.ImportStrategy,
		ImportDir:                      importConfig.ImportDir,
		WatchDir:                       importConfig.WatchDir,

		WatchIntervalSeconds:     importConfig.WatchIntervalSeconds,
		AllowNestedRarExtraction: importConfig.AllowNestedRarExtraction,
		RenameToNzbName:          importConfig.RenameToNzbName,
		FilterSampleFiles:        importConfig.FilterSampleFiles,
		CompressNzb:              importConfig.CompressNzb,
	}
}

// Common API response structures

// APIResponse represents a standard API response wrapper
type APIResponse struct {
	Success bool      `json:"success"`
	Data    any       `json:"data,omitempty"`
	Error   *APIError `json:"error,omitempty"`
	Meta    *APIMeta  `json:"meta,omitempty"`
}

// APIError represents an error response
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// APIMeta represents metadata for paginated responses
type APIMeta struct {
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	Count  int `json:"count"`
}

// Pagination represents pagination parameters
type Pagination struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// DefaultPagination returns default pagination settings
func DefaultPagination() Pagination {
	return Pagination{
		Limit:  50,
		Offset: 0,
	}
}

// Queue API Types

// QueueItemResponse represents a queue item in API responses
type QueueItemResponse struct {
	ID             int64                  `json:"id"`
	NzbPath        string                 `json:"nzb_path"`
	NzbDisplayName string                 `json:"nzb_display_name"`
	TargetPath     string                 `json:"target_path"`
	Category     *string                `json:"category"`
	Priority     database.QueuePriority `json:"priority"`
	Status       database.QueueStatus   `json:"status"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	StartedAt    *time.Time             `json:"started_at"`
	CompletedAt  *time.Time             `json:"completed_at"`
	RetryCount   int                    `json:"retry_count"`
	MaxRetries   int                    `json:"max_retries"`
	ErrorMessage *string                `json:"error_message"`
	BatchID      *string                `json:"batch_id"`
	Metadata     *string                `json:"metadata"`
	FileSize     *int64                 `json:"file_size"`
	Indexer      *string                `json:"indexer,omitempty"`      // Indexer name
	Percentage   *int                   `json:"percentage,omitempty"`    // Progress percentage (0-100), only for items being processed
	Stage        string                 `json:"stage,omitempty"`         // Progress stage (e.g. "Validating segments")
	StoragePath  *string                `json:"storage_path,omitempty"` // Internal FUSE mount path (populated after completion)
}

// QueueStatsResponse represents queue statistics in API responses
type QueueStatsResponse struct {
	TotalQueued         int       `json:"total_queued"`
	TotalProcessing     int       `json:"total_processing"`
	TotalCompleted      int       `json:"total_completed"`
	TotalFailed         int       `json:"total_failed"`
	AvgProcessingTimeMs *int      `json:"avg_processing_time_ms"`
	LastUpdated         time.Time `json:"last_updated"`
}

// QueueHistoryRange represents statistics for a specific time range
type QueueHistoryRange struct {
	Completed  int     `json:"completed"`
	Failed     int     `json:"failed"`
	Percentage float64 `json:"percentage"`
}

// ImportHistoryResponse represents a persistent import record in API responses
type ImportHistoryResponse struct {
	ID          int64     `json:"id"`
	NzbID       *int64    `json:"nzb_id"`
	NzbName     string    `json:"nzb_name"`
	FileName    string    `json:"file_name"`
	FileSize    int64     `json:"file_size"`
	VirtualPath string    `json:"virtual_path"`
	LibraryPath *string   `json:"library_path,omitempty"`
	Category    *string   `json:"category"`
	Indexer     *string   `json:"indexer,omitempty"` // Added indexer
	Metadata    *string   `json:"metadata,omitempty"`
	CompletedAt time.Time `json:"completed_at"`
}

// DailyStat represents statistics for a single day
type DailyStat struct {
	Day       string `json:"day"`
	Completed int    `json:"completed"`
	Failed    int    `json:"failed"`
}

// QueueHistoricalStatsResponse represents historical queue statistics
type QueueHistoricalStatsResponse struct {
	Last24Hours QueueHistoryRange `json:"last_24_hours"`
	Last7Days   QueueHistoryRange `json:"last_7_days"`
	Last30Days  QueueHistoryRange `json:"last_30_days"`
	Last365Days QueueHistoryRange `json:"last_365_days"`
	Daily       []DailyStat       `json:"daily"`
}

// Health API Types

// HealthItemResponse represents a health record in API responses
type HealthItemResponse struct {
	ID               int64                   `json:"id"`
	FilePath         string                  `json:"file_path"`
	LibraryPath      *string                 `json:"library_path,omitempty"`
	Status           database.HealthStatus   `json:"status"`
	LastChecked      *time.Time              `json:"last_checked"`
	LastError        *string                 `json:"last_error"`
	RetryCount       int                     `json:"retry_count"`
	MaxRetries       int                     `json:"max_retries"`
	SourceNzbPath    *string                 `json:"source_nzb_path"`
	ErrorDetails     *string                 `json:"error_details"`
	RepairRetryCount int                     `json:"repair_retry_count"`
	MaxRepairRetries int                     `json:"max_repair_retries"`
	Indexer          *string                 `json:"indexer,omitempty"` // Added indexer
	CreatedAt        time.Time               `json:"created_at"`
	UpdatedAt        time.Time               `json:"updated_at"`
	ScheduledCheckAt *time.Time              `json:"scheduled_check_at,omitempty"`
	Priority         database.HealthPriority `json:"priority"`
	// Failure masking fields
	StreamingFailureCount int  `json:"streaming_failure_count"`
	IsMasked              bool `json:"is_masked"`
}

// HealthStatsResponse represents health statistics in API responses
type HealthStatsResponse struct {
	Total           int `json:"total"`
	Pending         int `json:"pending"`
	Corrupted       int `json:"corrupted"`
	Healthy         int `json:"healthy"`
	RepairTriggered int `json:"repair_triggered"`
	Checking        int `json:"checking"`
}

// HealthRepairRequest represents request to trigger repair for a corrupted file
type HealthRepairRequest struct {
	ResetRepairRetryCount bool `json:"reset_repair_retry_count,omitempty"`
}

// HealthCleanupRequest represents request to cleanup health records
type HealthCleanupRequest struct {
	OlderThan   *time.Time             `json:"older_than"`
	Status      *database.HealthStatus `json:"status"`
	DeleteFiles bool                   `json:"delete_files"` // Whether to also delete the physical files
}

// HealthCheckRequest represents request to add file for manual health checking
type HealthCheckRequest struct {
	FilePath    string                  `json:"file_path"`
	LibraryPath *string                 `json:"library_path,omitempty"`
	MaxRetries  *int                    `json:"max_retries,omitempty"`
	SourceNzb   *string                 `json:"source_nzb_path,omitempty"`
	Priority    database.HealthPriority `json:"priority,omitempty"`
}

// HealthWorkerStatusResponse represents the current status of the health worker
type HealthWorkerStatusResponse struct {
	Status                 string     `json:"status"`
	LastRunTime            *time.Time `json:"last_run_time,omitempty"`
	NextRunTime            *time.Time `json:"next_run_time,omitempty"`
	TotalRunsCompleted     int64      `json:"total_runs_completed"`
	TotalFilesChecked      int64      `json:"total_files_checked"`
	TotalFilesHealthy      int64      `json:"total_files_healthy"`
	TotalFilesCorrupted    int64      `json:"total_files_corrupted"`
	CurrentRunStartTime    *time.Time `json:"current_run_start_time,omitempty"`
	CurrentRunFilesChecked int        `json:"current_run_files_checked"`
	LastError              *string    `json:"last_error,omitempty"`
	ErrorCount             int64      `json:"error_count"`
}

// System API Types

// SystemStatsResponse represents combined system statistics
type SystemStatsResponse struct {
	Queue  QueueStatsResponse  `json:"queue"`
	Health HealthStatsResponse `json:"health"`
	System SystemInfoResponse  `json:"system"`
}

// SystemInfoResponse represents system information
type SystemInfoResponse struct {
	Version   string    `json:"version,omitempty"`
	GitCommit string    `json:"git_commit,omitempty"`
	StartTime time.Time `json:"start_time"`
	Uptime    string    `json:"uptime"`
	GoVersion string    `json:"go_version,omitempty"`
}

// Update API Types

// UpdateChannel represents the Docker image channel for updates.
type UpdateChannel string

const (
	UpdateChannelLatest UpdateChannel = "latest"
	UpdateChannelDev    UpdateChannel = "dev"
)

// UpdateStatusResponse represents the current update status.
type UpdateStatusResponse struct {
	CurrentVersion         string        `json:"current_version"`
	GitCommit              string        `json:"git_commit,omitempty"`
	Channel                UpdateChannel `json:"channel"`
	LatestVersion          string        `json:"latest_version,omitempty"`
	UpdateAvailable        bool          `json:"update_available"`
	ReleaseURL             string        `json:"release_url,omitempty"`
	DockerAvailable        bool          `json:"docker_available"`
	BinaryUpdateAvailable  bool          `json:"binary_update_available"`
}

// SystemHealthResponse represents system health check result
type SystemHealthResponse struct {
	Status     string                     `json:"status"` // "healthy", "degraded", "unhealthy"
	Timestamp  time.Time                  `json:"timestamp"`
	Components map[string]ComponentHealth `json:"components"`
}

// ComponentHealth represents health of a system component
type ComponentHealth struct {
	Status  string `json:"status"` // "healthy", "degraded", "unhealthy"
	Message string `json:"message,omitempty"`
	Details string `json:"details,omitempty"`
}

// SystemCleanupRequest represents request for system cleanup
type SystemCleanupRequest struct {
	QueueOlderThan  *time.Time `json:"queue_older_than"`
	HealthOlderThan *time.Time `json:"health_older_than"`
	DryRun          bool       `json:"dry_run,omitempty"`
}

// SystemCleanupResponse represents cleanup operation results
type SystemCleanupResponse struct {
	QueueItemsRemoved    int  `json:"queue_items_removed"`
	HealthRecordsRemoved int  `json:"health_records_removed"`
	DryRun               bool `json:"dry_run"`
}

// SystemRestartRequest represents request for system restart
type SystemRestartRequest struct {
	Force bool `json:"force,omitempty"` // Force restart even if unsafe
}

// SystemRestartResponse represents restart operation result
type SystemRestartResponse struct {
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

// Configuration API Types - Now using core config types directly with minimal wrappers above

// Converter functions

// ToQueueItemResponse converts database.ImportQueueItem to QueueItemResponse
func ToQueueItemResponse(item *database.ImportQueueItem) *QueueItemResponse {
	if item == nil {
		return nil
	}

	// Generate target_path by removing .nzb/.nzb.gz extension and ID prefix if present
	targetPath := nzbJobName(item.NzbPath)

	// Remove ID prefix (e.g. "123_filename")
	parts := strings.SplitN(targetPath, "_", 2)
	if len(parts) == 2 {
		// Only strip prefix if first part is actually numeric (the queue ID)
		if _, err := strconv.Atoi(parts[0]); err == nil {
			targetPath = parts[1]
		}
	}

	nzbDisplayName := filepath.Base(item.NzbPath)
	if strings.HasSuffix(strings.ToLower(nzbDisplayName), ".gz") {
		nzbDisplayName = nzbDisplayName[:len(nzbDisplayName)-3]
	}

	// Transform error message for better user understanding
	errorMessage := transformQueueError(item.ErrorMessage)

	return &QueueItemResponse{
		ID:             item.ID,
		NzbPath:        item.NzbPath,
		NzbDisplayName: nzbDisplayName,
		TargetPath:     targetPath,
		Category:     item.Category,
		Priority:     item.Priority,
		Status:       item.Status,
		CreatedAt:    item.CreatedAt,
		UpdatedAt:    item.UpdatedAt,
		StartedAt:    item.StartedAt,
		CompletedAt:  item.CompletedAt,
		RetryCount:   item.RetryCount,
		MaxRetries:   item.MaxRetries,
		ErrorMessage: &errorMessage,
		BatchID:      item.BatchID,
		Metadata:     item.Metadata,
		FileSize:     item.FileSize,
		StoragePath:  item.StoragePath,
		Indexer:      item.Indexer,
	}
}

// ToQueueStatsResponse converts database.QueueStats to QueueStatsResponse
func ToQueueStatsResponse(stats *database.QueueStats) *QueueStatsResponse {
	if stats == nil {
		return nil
	}
	return &QueueStatsResponse{
		TotalQueued:         stats.TotalQueued,
		TotalProcessing:     stats.TotalProcessing,
		TotalCompleted:      stats.TotalCompleted,
		TotalFailed:         stats.TotalFailed,
		AvgProcessingTimeMs: stats.AvgProcessingTimeMs,
		LastUpdated:         stats.LastUpdated,
	}
}

// ToQueueHistoricalStatsResponse converts database.ImportDailyStat slice to QueueHistoricalStatsResponse
func ToQueueHistoricalStatsResponse(stats []*database.ImportDailyStat, hourlyStats []*database.ImportHourlyStat) *QueueHistoricalStatsResponse {
	response := &QueueHistoricalStatsResponse{
		Daily: make([]DailyStat, 0, len(stats)),
	}

	now := time.Now().UTC()
	day24h := now.Add(-24 * time.Hour)
	day7d := now.Add(-7 * 24 * time.Hour)
	day30d := now.Add(-30 * 24 * time.Hour)
	day365d := now.Add(-365 * 24 * time.Hour)

	// If hourly stats provided, use them for strict rolling 24h
	if len(hourlyStats) > 0 {
		for _, hs := range hourlyStats {
			if hs.Hour.After(day24h) || hs.Hour.Equal(day24h) {
				response.Last24Hours.Completed += hs.CompletedCount
				response.Last24Hours.Failed += hs.FailedCount
			}
		}
	}

	for _, s := range stats {
		// Daily list
		response.Daily = append(response.Daily, DailyStat{
			Day:       s.Day.Format("2006-01-02"),
			Completed: s.CompletedCount,
			Failed:    s.FailedCount,
		})

		// Aggregates
		if s.Day.After(day365d) || s.Day.Format("2006-01-02") == day365d.Format("2006-01-02") {
			response.Last365Days.Completed += s.CompletedCount
			response.Last365Days.Failed += s.FailedCount
		}
		if s.Day.After(day30d) || s.Day.Format("2006-01-02") == day30d.Format("2006-01-02") {
			response.Last30Days.Completed += s.CompletedCount
			response.Last30Days.Failed += s.FailedCount
		}
		if s.Day.After(day7d) || s.Day.Format("2006-01-02") == day7d.Format("2006-01-02") {
			response.Last7Days.Completed += s.CompletedCount
			response.Last7Days.Failed += s.FailedCount
		}

		// Fallback for 24h if no hourly stats provided
		if len(hourlyStats) == 0 {
			if s.Day.After(day24h) || s.Day.Format("2006-01-02") == day24h.Format("2006-01-02") {
				response.Last24Hours.Completed += s.CompletedCount
				response.Last24Hours.Failed += s.FailedCount
			}
		}
	}

	// Calculate percentages
	calcPercentage := func(r *QueueHistoryRange) {
		total := r.Completed + r.Failed
		if total > 0 {
			r.Percentage = (float64(r.Completed) / float64(total)) * 100
		}
	}

	calcPercentage(&response.Last24Hours)
	calcPercentage(&response.Last7Days)
	calcPercentage(&response.Last30Days)
	calcPercentage(&response.Last365Days)

	return response
}

// ToImportHistoryResponse converts database.ImportHistory to ImportHistoryResponse
func ToImportHistoryResponse(h *database.ImportHistory) *ImportHistoryResponse {
	if h == nil {
		return nil
	}
	return &ImportHistoryResponse{
		ID:          h.ID,
		NzbID:       h.NzbID,
		NzbName:     h.NzbName,
		FileName:    h.FileName,
		FileSize:    h.FileSize,
		VirtualPath: h.VirtualPath,
		LibraryPath: h.LibraryPath,
		Category:    h.Category,
		Indexer:     h.Indexer, // Fixed: use pointer directly
		Metadata:    h.Metadata,
		CompletedAt: h.CompletedAt,
	}
}

// ToHealthItemResponse converts database.FileHealth to HealthItemResponse
func ToHealthItemResponse(item *database.FileHealth) *HealthItemResponse {
	if item == nil {
		return nil
	}
	return &HealthItemResponse{
		ID:                    item.ID,
		FilePath:              item.FilePath,
		LibraryPath:           item.LibraryPath,
		Status:                item.Status,
		LastChecked:           item.LastChecked,
		LastError:             item.LastError,
		RetryCount:            item.RetryCount,
		MaxRetries:            item.MaxRetries,
		SourceNzbPath:         item.SourceNzbPath,
		ErrorDetails:          item.ErrorDetails,
		RepairRetryCount:      item.RepairRetryCount,
		MaxRepairRetries:      item.MaxRepairRetries,
		Indexer:               item.Indexer, // Fixed: use pointer directly
		CreatedAt:             item.CreatedAt,
		UpdatedAt:             item.UpdatedAt,
		ScheduledCheckAt:      item.ScheduledCheckAt,
		Priority:              item.Priority,
		StreamingFailureCount: item.StreamingFailureCount,
		IsMasked:              item.IsMasked,
	}
}

// ToHealthStatsResponse converts health stats map to HealthStatsResponse
func ToHealthStatsResponse(stats map[database.HealthStatus]int) *HealthStatsResponse {
	pending := stats[database.HealthStatusPending]
	corrupted := stats[database.HealthStatusCorrupted]
	healthy := stats[database.HealthStatusHealthy]
	repairTriggered := stats[database.HealthStatusRepairTriggered]
	checking := stats[database.HealthStatusChecking]

	// Calculate total from all tracked statuses
	total := 0
	for _, count := range stats {
		total += count
	}

	return &HealthStatsResponse{
		Total:           total,
		Pending:         pending,
		Corrupted:       corrupted,
		Healthy:         healthy,
		RepairTriggered: repairTriggered,
		Checking:        checking,
	}
}

// File Metadata API Types

// FileMetadataResponse represents file metadata information in API responses
type FileMetadataResponse struct {
	FileSize          int64                  `json:"file_size"`
	SourceNzbPath     string                 `json:"source_nzb_path"`
	Status            string                 `json:"status"`
	SegmentCount      int                    `json:"segment_count"`
	AvailableSegments *int                   `json:"available_segments"`
	Encryption        string                 `json:"encryption"`
	CreatedAt         string                 `json:"created_at"`
	ModifiedAt        string                 `json:"modified_at"`
	PasswordProtected bool                   `json:"password_protected"`
	Segments          []SegmentInfoResponse  `json:"segments"`
	NestedSources     []NestedSourceResponse `json:"nested_sources,omitempty"`
}

// SegmentInfoResponse represents segment information in API responses
type SegmentInfoResponse struct {
	SegmentSize int64  `json:"segment_size"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	MessageID   string `json:"message_id"`
	Available   bool   `json:"available"`
}

// NestedSegmentResponse represents one segment within a nested source volume
type NestedSegmentResponse struct {
	SegmentSize int64  `json:"segment_size"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	MessageID   string `json:"message_id"`
}

// NestedSourceResponse represents one inner-RAR volume in a nested archive
type NestedSourceResponse struct {
	VolumeIndex     int                     `json:"volume_index"`
	InnerLength     int64                   `json:"inner_length"`
	InnerVolumeSize int64                   `json:"inner_volume_size"`
	Encrypted       bool                    `json:"encrypted"`
	SegmentCount    int                     `json:"segment_count"`
	Segments        []NestedSegmentResponse `json:"segments"`
}

// Provider Management API Types

// ProviderTestRequest represents a request to test provider connectivity
type ProviderTestRequest struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	TLS         bool   `json:"tls"`
	InsecureTLS bool   `json:"insecure_tls"`
	ProxyURL    string `json:"proxy_url,omitempty"`
}

// ProviderCreateRequest represents a request to create a new provider
type ProviderCreateRequest struct {
	Host             string `json:"host"`
	Port             int    `json:"port"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	MaxConnections   int    `json:"max_connections"`
	TLS              bool   `json:"tls"`
	InsecureTLS      bool   `json:"insecure_tls"`
	ProxyURL         string `json:"proxy_url,omitempty"`
	Enabled          bool   `json:"enabled"`
	IsBackupProvider bool   `json:"is_backup_provider"`
}

// ProviderUpdateRequest represents a request to update an existing provider
type ProviderUpdateRequest struct {
	Host             *string `json:"host,omitempty"`
	Port             *int    `json:"port,omitempty"`
	Username         *string `json:"username,omitempty"`
	Password         *string `json:"password,omitempty"`
	MaxConnections   *int    `json:"max_connections,omitempty"`
	TLS              *bool   `json:"tls,omitempty"`
	InsecureTLS      *bool   `json:"insecure_tls,omitempty"`
	ProxyURL         *string `json:"proxy_url,omitempty"`
	Enabled          *bool   `json:"enabled,omitempty"`
	IsBackupProvider *bool   `json:"is_backup_provider,omitempty"`
}

// ProviderReorderRequest represents a request to reorder providers
type ProviderReorderRequest struct {
	ProviderIDs []string `json:"provider_ids"`
}

// Import API Types

// ManualScanRequest represents a request to start a manual directory scan
type ManualScanRequest struct {
	Path string `json:"path"`
}

// ScanStatusResponse represents the current status of a manual scan operation
type ScanStatusResponse struct {
	Status      string     `json:"status"`
	Path        string     `json:"path,omitempty"`
	StartTime   *time.Time `json:"start_time,omitempty"`
	FilesFound  int        `json:"files_found"`
	FilesAdded  int        `json:"files_added"`
	CurrentFile string     `json:"current_file,omitempty"`
	LastError   *string    `json:"last_error,omitempty"`
}

// ManualImportRequest represents a request to manually import a file by path
type ManualImportRequest struct {
	FilePath            string  `json:"file_path"`
	RelativePath        *string `json:"relative_path,omitempty"`
	SkipArrNotification bool    `json:"skip_arr_notification,omitempty"`
}

// ManualImportResponse represents the response from manually importing a file
type ManualImportResponse struct {
	QueueID int64  `json:"queue_id"`
	Message string `json:"message"`
}

// ProviderStatusResponse represents NNTP provider connection status in API responses
type ProviderStatusResponse struct {
	ID                      string     `json:"id"`
	Host                    string     `json:"host"`
	UsedConnections         int        `json:"used_connections"`
	MaxConnections          int        `json:"max_connections"`
	State                   string     `json:"state"`
	ErrorCount              int64      `json:"error_count"`
	LastConnectionAttempt   time.Time  `json:"last_connection_attempt"`
	LastSuccessfulConnect   time.Time  `json:"last_successful_connect"`
	FailureReason           string     `json:"failure_reason"`
	LastSpeedTestMbps       float64    `json:"last_speed_test_mbps"`
	LastSpeedTestTime       *time.Time `json:"last_speed_test_time,omitempty"`
	CurrentSpeedBytesPerSec float64    `json:"current_speed_bytes_per_sec"`
	PingMs                  int64      `json:"ping_ms"`
	MissingCount            int64      `json:"missing_count"`
	MissingRatePerMinute    float64    `json:"missing_rate_per_minute"`
	MissingWarning          bool       `json:"missing_warning"`
	ByteCount               int64      `json:"byte_count"`
	ByteCount24h            int64      `json:"byte_count_24h"`
	StartedAt               time.Time  `json:"started_at"`
	QuotaBytes              int64      `json:"quota_bytes,omitempty"`
	QuotaUsed               int64      `json:"quota_used,omitempty"`
	QuotaResetAt            *time.Time `json:"quota_reset_at,omitempty"`
	QuotaExceeded           bool       `json:"quota_exceeded,omitempty"`
}

// PoolMetricsResponse represents NNTP pool metrics in API responses
type PoolMetricsResponse struct {
	BytesDownloaded             int64                    `json:"bytes_downloaded"`
	BytesDownloaded24h          int64                    `json:"bytes_downloaded_24h"`
	BytesUploaded               int64                    `json:"bytes_uploaded"`
	ArticlesDownloaded          int64                    `json:"articles_downloaded"`
	ArticlesPosted              int64                    `json:"articles_posted"`
	TotalErrors                 int64                    `json:"total_errors"`
	ProviderErrors              map[string]int64         `json:"provider_errors"`
	ProviderBytes               map[string]int64         `json:"provider_bytes"`
	DownloadSpeedBytesPerSec    float64                  `json:"download_speed_bytes_per_sec"`
	MaxDownloadSpeedBytesPerSec float64                  `json:"max_download_speed_bytes_per_sec"`
	UploadSpeedBytesPerSec      float64                  `json:"upload_speed_bytes_per_sec"`
	Timestamp                   time.Time                `json:"timestamp"`
	StartedAt                   time.Time                `json:"started_at"`
	Providers                   []ProviderStatusResponse `json:"providers"`
}

type TestProviderResponse struct {
	Success      bool   `json:"success"`
	ErrorMessage string `json:"error_message,omitempty"`
	RTTMs        int64  `json:"rtt_ms,omitempty"`
}

type ProviderHistoricalStatResponse struct {
	Timestamp       time.Time `json:"timestamp"`
	ProviderID      string    `json:"provider_id"`
	BytesDownloaded int64     `json:"bytes_downloaded"`
}

type ProviderHistoricalStatsResponse struct {
	Stats []ProviderHistoricalStatResponse `json:"stats"`
}

type ProviderSpeedTestHistoryStat struct {
	ID         int64     `json:"id"`
	ProviderID string    `json:"provider_id"`
	SpeedMbps  float64   `json:"speed_mbps"`
	CreatedAt  time.Time `json:"created_at"`
}

type ProviderSpeedTestHistoryResponse struct {
	History []ProviderSpeedTestHistoryStat `json:"history"`
}

// Library Sync API Types

// DryRunSyncResult represents the results of a library sync dry run
type DryRunSyncResult struct {
	OrphanedMetadataCount  int  `json:"orphaned_metadata_count"`   // Number of orphaned metadata files
	OrphanedLibraryFiles   int  `json:"orphaned_library_files"`    // Number of orphaned library files (symlinks/STRM)
	DatabaseRecordsToClean int  `json:"database_records_to_clean"` // Number of database records to clean
	WouldCleanup           bool `json:"would_cleanup"`             // Whether cleanup would occur based on config
}
