package config

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/go-viper/mapstructure/v2"
	"github.com/javi11/nntppool/v5"
	"github.com/jinzhu/copier"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

const MountProvider = "altmount"
const DefaultCategoryName = "Default"
const DefaultCategoryDir = "complete"

// MountType represents the active mount system
type MountType string

const (
	// defaultMinConnectionsAlive keeps a couple of sockets warm so a stream
	// started after an idle period does not pay TCP + TLS + AUTHINFO first.
	// A negative min_connections_alive opts out entirely.
	defaultMinConnectionsAlive = 2
	// providerIdleTimeout stays under the idle cut most providers apply while
	// keeping warm connections through short pauses.
	providerIdleTimeout = 2 * time.Minute
	// providerReconnectDelay lets a provider dropped after a 502 rejoin the
	// pool instead of staying out until restart.
	providerReconnectDelay = 30 * time.Second
)

const (
	MountTypeNone           MountType = "none"
	MountTypeRClone         MountType = "rclone"
	MountTypeFuse           MountType = "fuse"
	MountTypeRCloneExternal MountType = "rclone_external"
)

// Config represents the complete application configuration
type Config struct {
	WebDAV       WebDAVConfig       `yaml:"webdav" mapstructure:"webdav" json:"webdav"`
	API          APIConfig          `yaml:"api" mapstructure:"api" json:"api"`
	Auth         AuthConfig         `yaml:"auth" mapstructure:"auth" json:"auth"`
	Database     DatabaseConfig     `yaml:"database" mapstructure:"database" json:"database"`
	Metadata     MetadataConfig     `yaml:"metadata" mapstructure:"metadata" json:"metadata"`
	Streaming    StreamingConfig    `yaml:"streaming" mapstructure:"streaming" json:"streaming"`
	Health       HealthConfig       `yaml:"health" mapstructure:"health" json:"health"`
	RClone       RCloneConfig       `yaml:"rclone" mapstructure:"rclone" json:"rclone"`
	Import       ImportConfig       `yaml:"import" mapstructure:"import" json:"import"`
	Log          LogConfig          `yaml:"log" mapstructure:"log" json:"log"`
	SABnzbd      SABnzbdConfig      `yaml:"sabnzbd" mapstructure:"sabnzbd" json:"sabnzbd"`
	Arrs         ArrsConfig         `yaml:"arrs" mapstructure:"arrs" json:"arrs"`
	Stremio      StremioConfig      `yaml:"stremio" mapstructure:"stremio" json:"stremio"`
	Fuse         FuseConfig         `yaml:"fuse" mapstructure:"fuse" json:"fuse"`
	SegmentCache SegmentCacheConfig `yaml:"segment_cache" mapstructure:"segment_cache" json:"segment_cache"`
	Providers    []ProviderConfig   `yaml:"providers" mapstructure:"providers" json:"providers"`
	Nzblnk       NzblnkConfig       `yaml:"nzblnk,omitempty" mapstructure:"nzblnk" json:"-"`
	Network      NetworkConfig      `yaml:"network" mapstructure:"network" json:"network"`
	// UserAgent is the HTTP User-Agent sent on every outbound HTTP request that
	// identifies AltMount (indexer NZB downloads, metadata lookups, nzblnk://
	// resolution, GitHub release checks). Defaults to a browser-like string
	// because some public indexers reject non-browser agents. Leave empty to
	// use the default.
	UserAgent       string           `yaml:"user_agent" mapstructure:"user_agent" json:"user_agent"`
	Par2Repair      Par2RepairConfig `yaml:"par2_repair" mapstructure:"par2_repair" json:"par2_repair"`
	MountPath       string           `yaml:"mount_path" mapstructure:"mount_path" json:"mount_path"`
	MountType       MountType        `yaml:"mount_type" mapstructure:"mount_type" json:"mount_type"`
	ProfilerEnabled bool             `yaml:"profiler_enabled" mapstructure:"profiler_enabled" json:"profiler_enabled" default:"false"`
	// MemoryLimitMB is the Go soft memory limit. Unset or 0 derives it from
	// the budgets that allocate (see SoftMemoryLimit); a positive value pins
	// it; a negative value leaves the runtime default. GOMEMLIMIT in the
	// environment always takes precedence.
	MemoryLimitMB *int `yaml:"memory_limit_mb" mapstructure:"memory_limit_mb" json:"memory_limit_mb"`
}

// Par2RepairConfig configures background PAR2 repair of missing usenet
// articles. Repaired article payloads are persisted under
// <metadata_root>/patches and served on the read path's hole branch.
type Par2RepairConfig struct {
	Enabled *bool `yaml:"enabled" mapstructure:"enabled" json:"enabled,omitempty"`
	// MaxRepairRatio caps how large a fraction of a file's bytes a repair may
	// reconstruct. The release's PAR2 redundancy is always the hard ceiling.
	MaxRepairRatio float64 `yaml:"max_repair_ratio" mapstructure:"max_repair_ratio" json:"max_repair_ratio,omitempty"`
	// MaxMemoryMB bounds one job's in-heap solver memory; jobs needing more
	// still run, backed by memory-mapped scratch files next to the patch store.
	MaxMemoryMB int `yaml:"max_memory_mb" mapstructure:"max_memory_mb" json:"max_memory_mb,omitempty"`
	// MaxConcurrentJobs bounds simultaneously running repair jobs.
	MaxConcurrentJobs int `yaml:"max_concurrent_jobs" mapstructure:"max_concurrent_jobs" json:"max_concurrent_jobs,omitempty"`
	// MaxConnections bounds how many NNTP connections repair jobs use for
	// article fetches (shared across concurrent jobs). Repair streams the
	// whole release once, so this directly sets its download speed on an idle
	// pool. Fetches ride the pool's background lane: while anything streams
	// or imports, the pool holds repair to a quarter of each provider's
	// connections regardless of this value. 0 (default) means 10.
	MaxConnections int `yaml:"max_connections" mapstructure:"max_connections" json:"max_connections,omitempty"`
	// MinReleaseSizeMB / MaxReleaseSizeMB bound the size of releases repair
	// takes on (content bytes, PAR2 files excluded). A repair downloads the
	// whole release once, so these let users skip releases too large to be
	// worth the bandwidth or too small to bother with. 0 (default) means
	// unbounded on that side — all sizes are repaired.
	MinReleaseSizeMB int `yaml:"min_release_size_mb" mapstructure:"min_release_size_mb" json:"min_release_size_mb,omitempty"`
	MaxReleaseSizeMB int `yaml:"max_release_size_mb" mapstructure:"max_release_size_mb" json:"max_release_size_mb,omitempty"`
	// MaxPatchStoreMB bounds the on-disk patch store size; oldest patches are
	// evicted first when the cap is exceeded. 0 (default) means unlimited.
	MaxPatchStoreMB int `yaml:"max_patch_store_mb" mapstructure:"max_patch_store_mb" json:"max_patch_store_mb,omitempty"`
	// PatchDir is where repaired article payloads (and the solver's transient
	// scratch files) are stored. Empty (default) means <metadata_root>/patches.
	// Applied at startup; changing it does not move existing patches.
	PatchDir string `yaml:"patch_dir" mapstructure:"patch_dir" json:"patch_dir,omitempty"`
	// ArrFirst makes PAR2 repair the fallback for corrupted files: the health
	// worker triggers the ARR rescan first exactly as before, and enqueues a
	// PAR2 repair only when nothing is found in the ARRs (no instance
	// configured, none tracks the file) or ARR repair is disabled. On by
	// default; disable to keep PAR2 out of the corrupted-file flow (degraded
	// files, playback holes and repair-on-import still repair via PAR2
	// directly).
	ArrFirst *bool `yaml:"arr_first" mapstructure:"arr_first" json:"arr_first,omitempty"`
	// RepairOnImport queues a repair as soon as an import completes with
	// confirmed missing segments, instead of waiting for the first playback or
	// a health check. Off by default: every repair costs one full release
	// download, so a large damaged backlog would be expensive. Worth enabling
	// when your library outlives article retention — a release's PAR2 volumes
	// are most likely to still be retrievable close to the post date.
	RepairOnImport *bool `yaml:"repair_on_import" mapstructure:"repair_on_import" json:"repair_on_import,omitempty"`
}

// EffectiveMaxConnections resolves the repair fetch connection bound,
// defaulting to 10 when unset (configs written before the knob existed).
func (p Par2RepairConfig) EffectiveMaxConnections() int {
	if p.MaxConnections <= 0 {
		return 10
	}
	return p.MaxConnections
}

// EffectivePatchDir resolves where repaired article payloads live: the
// configured PatchDir, or "patches" under the metadata root when unset.
func (p Par2RepairConfig) EffectivePatchDir(metadataRoot string) string {
	if p.PatchDir != "" {
		return p.PatchDir
	}
	return filepath.Join(metadataRoot, "patches")
}

// EffectiveArrFirst reports whether corrupted files fall back to PAR2 repair
// after the ARR repair comes up empty. Defaults to true when unset (configs
// written before the knob existed).
func (p Par2RepairConfig) EffectiveArrFirst() bool {
	return p.ArrFirst == nil || *p.ArrFirst
}

// EffectiveRepairOnImport reports whether imports queue PAR2 repairs. Requires
// the feature itself to be enabled; defaults to false when unset.
func (p Par2RepairConfig) EffectiveRepairOnImport() bool {
	return p.Enabled != nil && *p.Enabled && p.RepairOnImport != nil && *p.RepairOnImport
}

// NzblnkConfig is the legacy scoped user-agent config, retained only so
// existing YAML files can be migrated into the global user_agent field.
type NzblnkConfig struct {
	UserAgent string `yaml:"user_agent,omitempty" mapstructure:"user_agent" json:"-"`
}

// DefaultUserAgent is the fallback for Config.UserAgent. Browser-like because
// some public indexers (e.g. nzbking.com) reject non-browser agents.
const DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

// GetUserAgent returns the configured global User-Agent, falling back to
// DefaultUserAgent when unset.
func (c *Config) GetUserAgent() string {
	if c.UserAgent != "" {
		return c.UserAgent
	}
	return DefaultUserAgent
}

// NetworkConfig holds outbound HTTP routing options applied to every external
// client (indexers, arrs, SABnzbd fallback, NZBLNK resolver). Internal
// endpoints (RC server, self-loopback) are unaffected.
//
// Semantics mirror Go's standard HTTP_PROXY/HTTPS_PROXY/NO_PROXY env vars.
// Empty strings disable proxying for that scheme.
type NetworkConfig struct {
	HTTPProxy  string `yaml:"http_proxy" mapstructure:"http_proxy" json:"http_proxy,omitempty"`
	HTTPSProxy string `yaml:"https_proxy" mapstructure:"https_proxy" json:"https_proxy,omitempty"`
	NoProxy    string `yaml:"no_proxy" mapstructure:"no_proxy" json:"no_proxy,omitempty"`
}

// GetHTTPProxy returns the configured HTTP proxy URL. Implements the
// httpclient.NetworkProxyConfig interface to avoid config↔httpclient import cycles.
func (n NetworkConfig) GetHTTPProxy() string { return n.HTTPProxy }

// GetHTTPSProxy returns the configured HTTPS proxy URL.
func (n NetworkConfig) GetHTTPSProxy() string { return n.HTTPSProxy }

// GetNoProxy returns the comma-separated bypass list.
func (n NetworkConfig) GetNoProxy() string { return n.NoProxy }

// SegmentCacheConfig configures the segment-aligned disk cache shared by FUSE and WebDAV.
// When enabled, this cache replaces the FUSE VFS disk cache and additionally benefits WebDAV.
// Cache key: Usenet message ID. Cache unit: ~750KB decoded segment (matches one NNTP article).
type SegmentCacheConfig struct {
	Enabled   *bool  `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	CachePath string `yaml:"cache_path" mapstructure:"cache_path" json:"cache_path"`
	MaxSizeGB int    `yaml:"max_size_gb" mapstructure:"max_size_gb" json:"max_size_gb"`
	// ExpiryHours controls how long cached segments are kept before automatic
	// eviction. Set to 0 to disable expiry (cache forever, bounded only by
	// MaxSizeGB via LRU eviction). Left unset (nil) it defaults to 24 hours.
	ExpiryHours *int `yaml:"expiry_hours" mapstructure:"expiry_hours" json:"expiry_hours"`
	// MemoryMB bounds the in-memory tier of decoded articles that sits in
	// front of the disk cache and is on even when the disk cache is off.
	// Left unset (nil) it defaults to 256; an explicit 0 disables it.
	MemoryMB *int `yaml:"memory_mb" mapstructure:"memory_mb" json:"memory_mb"`
}

// defaultSegmentCacheMemoryMB is the memory tier size when memory_mb is unset.
const defaultSegmentCacheMemoryMB = 256

// MemoryBytes is the memory tier budget in bytes (0 = disabled).
func (c SegmentCacheConfig) MemoryBytes() int64 {
	if c.MemoryMB == nil {
		return int64(defaultSegmentCacheMemoryMB) << 20
	}
	return int64(max(*c.MemoryMB, 0)) << 20
}

// StreamReadAheadBytesCap bounds one reader's read-ahead window in bytes.
// The usenet reader enforces it; it lives here so the soft memory limit can
// budget for the windows that are live while files stream.
const StreamReadAheadBytesCap int64 = 96 << 20

// Soft memory limit headroom: what the process holds live above the memory
// tier and the PAR2 solver. Every term scales with what actually allocates
// so the limit stays above the live set; a limit below it makes the
// collector run back to back and burn CPU without freeing anything.
const (
	// softMemoryBaseMB covers the runtime, metadata, HTTP and pool bookkeeping.
	softMemoryBaseMB = 128
	// softMemoryStreams is how many full read-ahead windows are budgeted:
	// a mount typically keeps two chunk readers open plus one being torn down.
	softMemoryStreams = 3
	// softMemoryPerConnectionBytes is the read and write buffers plus TLS
	// record state each pool connection pins while open.
	softMemoryPerConnectionBytes int64 = 256 << 10
)

// softMemoryHeadroom is the headroom for this config's read-ahead and
// connection footprint.
func (c *Config) softMemoryHeadroom() int64 {
	conns := int64(0)
	for _, p := range c.Providers {
		if p.Enabled != nil && !*p.Enabled {
			continue
		}
		conns += int64(max(p.MaxConnections, 0))
	}
	return int64(softMemoryBaseMB)<<20 +
		softMemoryStreams*StreamReadAheadBytesCap +
		conns*softMemoryPerConnectionBytes
}

// SoftMemoryLimit is the Go soft memory limit to apply, or 0 to leave the
// runtime alone. Without a limit the collector lets the heap reach twice the
// live set, so a 256 MB memory tier costs 600+ MB of RSS. The automatic value
// adds every budget that holds live heap (memory tier, PAR2 solver per
// concurrent job, read-ahead windows, connection buffers) plus a base, so
// the limit stays above the live set and the collector never has to run back
// to back. A soft limit is only useful while the memory tier is on: with it
// off the heap is small and bursty.
func (c *Config) SoftMemoryLimit(gomemlimit string) int64 {
	if gomemlimit != "" {
		return 0
	}
	if c.MemoryLimitMB != nil && *c.MemoryLimitMB != 0 {
		if *c.MemoryLimitMB < 0 {
			return 0
		}
		return int64(*c.MemoryLimitMB) << 20
	}
	cache := c.SegmentCache.MemoryBytes()
	if cache == 0 {
		return 0
	}
	// Repair solvers only allocate when the feature is on; a disabled repair
	// service must not raise the ceiling the collector works under.
	var par2 int64
	if c.Par2Repair.Enabled != nil && *c.Par2Repair.Enabled {
		par2 = int64(max(c.Par2Repair.MaxMemoryMB, 0)) * int64(max(c.Par2Repair.MaxConcurrentJobs, 1))
	}
	return cache + par2<<20 + c.softMemoryHeadroom()
}

// WebDAVConfig represents WebDAV server configuration
type WebDAVConfig struct {
	Port     int    `yaml:"port" mapstructure:"port" json:"port"`
	User     string `yaml:"user" mapstructure:"user" json:"user"`
	Password string `yaml:"password" mapstructure:"password" json:"password"`
	Host     string `yaml:"host" mapstructure:"host" json:"host,omitempty"`
}

// FuseConfig represents FUSE mount configuration
type FuseConfig struct {
	MountPath           string `yaml:"mount_path" mapstructure:"mount_path" json:"mount_path"`
	Enabled             *bool  `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	AllowOther          bool   `yaml:"allow_other" mapstructure:"allow_other" json:"allow_other"`
	Debug               bool   `yaml:"debug" mapstructure:"debug" json:"debug"`
	AttrTimeoutSeconds  int    `yaml:"attr_timeout_seconds" mapstructure:"attr_timeout_seconds" json:"attr_timeout_seconds"`
	EntryTimeoutSeconds int    `yaml:"entry_timeout_seconds" mapstructure:"entry_timeout_seconds" json:"entry_timeout_seconds"`
	MaxCacheSizeMB      int    `yaml:"max_cache_size_mb" mapstructure:"max_cache_size_mb" json:"max_cache_size_mb"`
	MaxReadAheadMB      int    `yaml:"max_read_ahead_mb" mapstructure:"max_read_ahead_mb" json:"max_read_ahead_mb"`
	// AsyncBufferSizeMB is the per-open-file read-ahead buffer size (MB). A
	// background goroutine fills it ahead of the player so reads are served
	// from memory instead of blocking on the network, mirroring the buffering
	// rclone's VFS provides over WebDAV. 0 disables read-ahead (every read is a
	// direct passthrough — the previous behavior).
	AsyncBufferSizeMB int `yaml:"async_buffer_size_mb" mapstructure:"async_buffer_size_mb" json:"async_buffer_size_mb"`
	// AsyncBufferMaxTotalMB caps total read-ahead memory across all open files
	// (MB). Buffers reserve their size only while actively streaming; over the
	// cap, additional streams run unbuffered. 0 = unlimited.
	AsyncBufferMaxTotalMB int    `yaml:"async_buffer_max_total_mb" mapstructure:"async_buffer_max_total_mb" json:"async_buffer_max_total_mb"`
	Backend               string `yaml:"backend" mapstructure:"backend" json:"backend"` // "hanwen" or "cgo" (empty = platform default)
	// NoModTime reports epoch for all timestamps (mtime, ctime, atime); prevents
	// media servers re-scanning on unstable VFS mtimes. Note: tools like find -atime
	// will not see meaningful access times on this mount.
	NoModTime bool `yaml:"no_mod_time" mapstructure:"no_mod_time" json:"no_mod_time"`
}

// APIConfig represents REST API configuration
type APIConfig struct {
	Prefix         string   `yaml:"prefix" mapstructure:"prefix" json:"prefix"`
	KeyOverride    string   `yaml:"key_override" mapstructure:"key_override" json:"key_override,omitempty"`
	AllowedOrigins []string `yaml:"allowed_origins" mapstructure:"allowed_origins" json:"allowed_origins,omitempty"`
}

// ProwlarrConfig configures the Prowlarr indexer integration for Stremio addon searches.
type ProwlarrConfig struct {
	// Enabled controls whether Prowlarr search is active for the Stremio addon.
	Enabled *bool `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	// Host is the Prowlarr base URL (e.g. "http://localhost:9696").
	Host string `yaml:"host" mapstructure:"host" json:"host,omitempty"`
	// APIKey is the Prowlarr API key.
	APIKey string `yaml:"api_key" mapstructure:"api_key" json:"api_key,omitempty"`
	// Categories filters search results by Newznab category IDs.
	// Defaults to 5000 (Movies), 5010 (Movies/Foreign), 5030 (TV), 5040 (TV/HD).
	Categories []int `yaml:"categories" mapstructure:"categories" json:"categories,omitempty"`
	// Indexers optionally restricts searches to specific Prowlarr indexer IDs.
	// Empty = search across all configured indexers.
	Indexers []int `yaml:"indexers" mapstructure:"indexers" json:"indexers,omitempty"`
	// PreferredIndexers lists Prowlarr indexer IDs to prioritize and rank above others.
	PreferredIndexers []int `yaml:"preferred_indexers" mapstructure:"preferred_indexers" json:"preferred_indexers,omitempty"`
	// PreferredIndexerNames lists Prowlarr indexer names/keywords to prioritize and rank above others.
	PreferredIndexerNames []string `yaml:"preferred_indexer_names" mapstructure:"preferred_indexer_names" json:"preferred_indexer_names,omitempty"`
	// Languages is an optional list of keywords; releases must contain at least one to pass.
	// Empty = no filtering. Examples: ["Esp", "🇪🇸", "Spanish", "DUAL"]
	Languages []string `yaml:"languages" mapstructure:"languages" json:"languages,omitempty"`
	// PreferredLanguages lists language keywords to prioritize and rank above others.
	PreferredLanguages []string `yaml:"preferred_languages" mapstructure:"preferred_languages" json:"preferred_languages,omitempty"`
	// Qualities is an optional list of keywords; releases must contain at least one to pass.
	// Empty = no filtering. Examples: ["1080p", "HD", "4K", "3D"]
	Qualities []string `yaml:"qualities" mapstructure:"qualities" json:"qualities,omitempty"`
	// ExcludeKeywords is an optional list of keywords/patterns; releases containing any are dropped.
	// Examples: ["DV", "DoVi", "CAM", "TS", "TeleSync"]
	ExcludeKeywords []string `yaml:"exclude_keywords" mapstructure:"exclude_keywords" json:"exclude_keywords,omitempty"`
	// CustomScores maps keyword/regex patterns to numeric scores (TRaSH guide style).
	// Positive scores promote releases; negative scores demote them.
	// Examples: {"2160p": 500, "REMUX": 300, "DV": -1000}
	CustomScores map[string]int `yaml:"custom_scores" mapstructure:"custom_scores" json:"custom_scores,omitempty"`
}

// NewsnabIndexerConfig holds configuration for a direct Newznab indexer.
type NewsnabIndexerConfig struct {
	ID             string `yaml:"id" mapstructure:"id" json:"id"`
	Name           string `yaml:"name" mapstructure:"name" json:"name"`
	URL            string `yaml:"url" mapstructure:"url" json:"url"`
	APIKey         string `yaml:"api_key" mapstructure:"api_key" json:"api_key"`
	Categories     []int  `yaml:"categories" mapstructure:"categories" json:"categories,omitempty"`
	Weight         int    `yaml:"weight" mapstructure:"weight" json:"weight"`
	TimeoutSeconds int    `yaml:"timeout_seconds" mapstructure:"timeout_seconds" json:"timeout_seconds"`
	Enabled        bool   `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
}

// StremioIndexersConfig configures search providers for the Stremio addon.
type StremioIndexersConfig struct {
	Provider        string                 `yaml:"provider" mapstructure:"provider" json:"provider"` // "prowlarr", "newsnab", "both"
	UserAgentMode   string                 `yaml:"user_agent_mode" mapstructure:"user_agent_mode" json:"user_agent_mode,omitempty"`
	CustomUserAgent string                 `yaml:"custom_user_agent" mapstructure:"custom_user_agent" json:"custom_user_agent,omitempty"`
	Prowlarr        ProwlarrConfig         `yaml:"prowlarr" mapstructure:"prowlarr" json:"prowlarr"`
	Newsnab         []NewsnabIndexerConfig `yaml:"newsnab" mapstructure:"newsnab" json:"newsnab,omitempty"`
}

// TrashCustomFormatConfig defines a format scoring rule in configuration.
type TrashCustomFormatConfig struct {
	ID          string `yaml:"id" mapstructure:"id" json:"id"`
	Name        string `yaml:"name" mapstructure:"name" json:"name"`
	Category    string `yaml:"category" mapstructure:"category" json:"category"`
	Pattern     string `yaml:"pattern" mapstructure:"pattern" json:"pattern"`
	PatternType string `yaml:"pattern_type" mapstructure:"pattern_type" json:"pattern_type"`
	Score       int    `yaml:"score" mapstructure:"score" json:"score"`
	Enabled     bool   `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	IsCustom    bool   `yaml:"is_custom" mapstructure:"is_custom" json:"is_custom"`
	Invert      bool   `yaml:"invert,omitempty" mapstructure:"invert" json:"invert,omitempty"`
}

// StreamScoringConfig holds TRaSH scoring configuration.
type StreamScoringConfig struct {
	Preset                   string                    `yaml:"preset" mapstructure:"preset" json:"preset"`
	CustomFormats            []TrashCustomFormatConfig `yaml:"custom_formats" mapstructure:"custom_formats" json:"custom_formats,omitempty"`
	ExcludeKeywords          []string                  `yaml:"exclude_keywords" mapstructure:"exclude_keywords" json:"exclude_keywords,omitempty"`
	ExcludeRegex             string                    `yaml:"exclude_regex,omitempty" mapstructure:"exclude_regex" json:"exclude_regex,omitempty"`
	PreferredLanguages       []string                  `yaml:"preferred_languages" mapstructure:"preferred_languages" json:"preferred_languages,omitempty"`
	RequirePreferredLanguage bool                      `yaml:"require_preferred_language" mapstructure:"require_preferred_language" json:"require_preferred_language"`
}

// StremioConfig configures the Stremio NZB stream endpoint (POST /api/nzb/streams).
type StremioConfig struct {
	// Enabled controls whether the endpoint is active. Disabled by default.
	// When false, the endpoint returns 404 Not Found.
	Enabled *bool `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	// AddonName is the display name shown in Stremio's addon catalog.
	AddonName string `yaml:"addon_name" mapstructure:"addon_name" json:"addon_name,omitempty"`
	// AddonDescription is the description shown in Stremio's addon catalog.
	AddonDescription string `yaml:"addon_description" mapstructure:"addon_description" json:"addon_description,omitempty"`
	// DirectStream when true streams NZB media segments directly.
	DirectStream *bool `yaml:"direct_stream" mapstructure:"direct_stream" json:"direct_stream,omitempty"`
	// ShowCachedIndicator when true shows cached badges in stream titles.
	ShowCachedIndicator *bool `yaml:"show_cached_indicator" mapstructure:"show_cached_indicator" json:"show_cached_indicator,omitempty"`
	// FallbackTimeoutMs is the timeout before attempting a fallback release.
	FallbackTimeoutMs int `yaml:"fallback_timeout_ms" mapstructure:"fallback_timeout_ms" json:"fallback_timeout_ms,omitempty"`
	// MaxRetries is the maximum number of retry attempts for stream fetching.
	MaxRetries int `yaml:"max_retries" mapstructure:"max_retries" json:"max_retries,omitempty"`
	// StreamTTLSeconds is the stream cache expiration time in seconds.
	StreamTTLSeconds int `yaml:"stream_ttl_seconds" mapstructure:"stream_ttl_seconds" json:"stream_ttl_seconds,omitempty"`
	// NzbTTLHours controls how long a completed NZB result is cached before
	// the same NZB is re-processed on the next request.
	// Set to 0 to disable expiry (cache forever). Defaults to 24 hours.
	NzbTTLHours int `yaml:"nzb_ttl_hours" mapstructure:"nzb_ttl_hours" json:"nzb_ttl_hours"`
	// FailedReleaseTTLHours controls how long a release that failed to import stays
	// excluded from the Stremio stream list. Mirrors NzbTTLHours semantics: it filters
	// failure records by age rather than deleting them, so it is bounded above by
	// Import.FailedItemRetentionHours. Set to 0 to exclude for as long as the record
	// survives. Defaults to 24 hours.
	FailedReleaseTTLHours int `yaml:"failed_release_ttl_hours" mapstructure:"failed_release_ttl_hours" json:"failed_release_ttl_hours"`
	// MaxFallbackReleases caps how many *extra* releases a single play request may try
	// after the first one fails. Set to 0 to disable fallback. Defaults to 2.
	MaxFallbackReleases int `yaml:"max_fallback_releases" mapstructure:"max_fallback_releases" json:"max_fallback_releases"`
	// IncludeLibraryStreams when true checks and presents matching completed files
	// from your Altmount library as instant 0s stream options in Stremio. Defaults to true.
	IncludeLibraryStreams *bool `yaml:"include_library_streams" mapstructure:"include_library_streams" json:"include_library_streams,omitempty"`
	// FastFailHeaderOnly when true causes Stremio-originated imports to fail fast
	// immediately on the release-level probe when missing articles are detected,
	// skipping the multi-part RAR per-file Stat sweep. Defaults to true.
	FastFailHeaderOnly *bool `yaml:"fast_fail_header_only" mapstructure:"fast_fail_header_only" json:"fast_fail_header_only"`
	// BaseURL is the public base URL used when building Stremio stream links
	// (e.g. "https://altmount.example.com"). Falls back to the auto-detected
	// request origin when not set.
	BaseURL string `yaml:"base_url" mapstructure:"base_url" json:"base_url,omitempty"`
	// Indexers configures Prowlarr and direct Newsnab search providers.
	Indexers StremioIndexersConfig `yaml:"indexers" mapstructure:"indexers" json:"indexers"`
	// Scoring configures TRaSH format scoring and exclusions.
	Scoring StreamScoringConfig `yaml:"scoring" mapstructure:"scoring" json:"scoring"`
	// Prowlarr configures the legacy Prowlarr indexer settings (retained for compatibility).
	Prowlarr ProwlarrConfig `yaml:"prowlarr" mapstructure:"prowlarr" json:"prowlarr"`
}

// maxStremioFallbackReleases bounds MaxFallbackReleases so a run of failures cannot
// fire an unbounded number of Prowlarr searches for a single play request.
const maxStremioFallbackReleases = 4

// EffectiveMaxFallbackReleases returns MaxFallbackReleases clamped to [0, 4].
func (s StremioConfig) EffectiveMaxFallbackReleases() int {
	if s.MaxFallbackReleases < 0 {
		return 0
	}
	if s.MaxFallbackReleases > maxStremioFallbackReleases {
		return maxStremioFallbackReleases
	}
	return s.MaxFallbackReleases
}

// EffectiveIncludeLibraryStreams reports whether Stremio stream responses should include
// matching completed releases from the local library. Defaults to true.
func (s StremioConfig) EffectiveIncludeLibraryStreams() bool {
	if s.IncludeLibraryStreams == nil {
		return true
	}
	return *s.IncludeLibraryStreams
}

// EffectiveFastFailHeaderOnly reports whether Stremio imports should fast-fail
// immediately on the release probe when missing articles are found. Defaults to true.
func (s StremioConfig) EffectiveFastFailHeaderOnly() bool {
	if s.FastFailHeaderOnly == nil {
		return true
	}
	return *s.FastFailHeaderOnly
}

// AuthConfig represents authentication configuration
type AuthConfig struct {
	LoginRequired *bool `yaml:"login_required" mapstructure:"login_required" json:"login_required"`
}

// DatabaseConfig represents database configuration
type DatabaseConfig struct {
	// Type selects the database backend: "sqlite" (default) or "postgres".
	Type string `yaml:"type" mapstructure:"type" json:"type"`
	// Path is the SQLite database file path (sqlite only).
	Path string `yaml:"path" mapstructure:"path" json:"path"`
	// DSN is the PostgreSQL connection string (postgres only).
	// Example: "postgres://user:password@localhost:5432/altmount?sslmode=disable"
	DSN string `yaml:"dsn" mapstructure:"dsn" json:"dsn,omitempty"`
}

// MetadataConfig represents metadata filesystem configuration
type MetadataConfig struct {
	RootPath  string                  `yaml:"root_path" mapstructure:"root_path" json:"root_path"`
	Backup    MetadataBackupConfig    `yaml:"backup" mapstructure:"backup" json:"backup"`
	Migration MetadataMigrationConfig `yaml:"migration" mapstructure:"migration" json:"migration"`
}

// MetadataMigrationConfig configures the legacy-metadata → v3 migration.
type MetadataMigrationConfig struct {
	// DefaultGroup is the newsgroup written into synthesized NzbStore entries.
	// Legacy metas do not retain the original groups, and nzb.BuildNZB renders an
	// empty <groups> element without this, which most NZB clients reject.
	DefaultGroup string `yaml:"default_group" mapstructure:"default_group" json:"default_group"`
}

// MetadataBackupConfig represents metadata backup configuration
type MetadataBackupConfig struct {
	Enabled     *bool  `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	Schedule    string `yaml:"schedule" mapstructure:"schedule" json:"schedule"` // cron expression (UTC)
	KeepBackups int    `yaml:"keep_backups" mapstructure:"keep_backups" json:"keep_backups"`
	Path        string `yaml:"path" mapstructure:"path" json:"path"`
}

// FailureMaskingConfig represents failure masking configuration
type FailureMaskingConfig struct {
	Enabled   *bool `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	Threshold int   `yaml:"threshold" mapstructure:"threshold" json:"threshold"`
}

// StreamingConfig represents streaming and chunking configuration
type StreamingConfig struct {
	MaxPrefetch int `yaml:"max_prefetch" mapstructure:"max_prefetch" json:"max_prefetch"`
	// ReadTimeoutSeconds bounds a single streaming read (WebDAV Read and FUSE
	// ReadAt alike). Nothing below this layer carries a deadline the caller can
	// observe: a segment fetch that stalls without erroring parks the read
	// indefinitely, and on FUSE that means an uninterruptible D-state for the
	// reading process. On expiry the in-flight reader is interrupted and the
	// read fails, so the caller gets an EIO it can act on. 0 uses the built-in
	// default; a negative value disables the timeout.
	ReadTimeoutSeconds int                  `yaml:"read_timeout_seconds" mapstructure:"read_timeout_seconds" json:"read_timeout_seconds"`
	FailureMasking     FailureMaskingConfig `yaml:"failure_masking" mapstructure:"failure_masking" json:"failure_masking"`
}

// RCloneConfig represents rclone configuration
type RCloneConfig struct {
	// RClone Path
	Path string `yaml:"path" mapstructure:"path" json:"path"`
	// Encryption
	Password string `yaml:"password" mapstructure:"password" json:"-"`
	Salt     string `yaml:"salt" mapstructure:"salt" json:"-"`

	// RC (Remote Control) Configuration
	RCEnabled *bool             `yaml:"rc_enabled" mapstructure:"rc_enabled" json:"rc_enabled"`
	RCUrl     string            `yaml:"rc_url" mapstructure:"rc_url" json:"rc_url"`
	RCPort    int               `yaml:"rc_port" mapstructure:"rc_port" json:"rc_port"`
	RCUser    string            `yaml:"rc_user" mapstructure:"rc_user" json:"rc_user"`
	RCPass    string            `yaml:"rc_pass" mapstructure:"rc_pass" json:"rc_pass,omitempty"`
	RCOptions map[string]string `yaml:"rc_options" mapstructure:"rc_options" json:"rc_options"`

	// Mount Configuration
	MountEnabled *bool             `yaml:"mount_enabled" mapstructure:"mount_enabled" json:"mount_enabled"`
	VFSName      string            `yaml:"vfs_name" mapstructure:"vfs_name" json:"vfs_name"`
	MountOptions map[string]string `yaml:"mount_options" mapstructure:"mount_options" json:"mount_options"`
	LogLevel     string            `yaml:"log_level" mapstructure:"log_level" json:"log_level"`
	UID          int               `yaml:"uid" mapstructure:"uid" json:"uid"`
	GID          int               `yaml:"gid" mapstructure:"gid" json:"gid"`
	Umask        string            `yaml:"umask" mapstructure:"umask" json:"umask"`
	BufferSize   string            `yaml:"buffer_size" mapstructure:"buffer_size" json:"buffer_size"`
	AttrTimeout  string            `yaml:"attr_timeout" mapstructure:"attr_timeout" json:"attr_timeout"`
	Transfers    int               `yaml:"transfers" mapstructure:"transfers" json:"transfers"`

	// VFS Cache Settings
	CacheDir              string `yaml:"cache_dir" mapstructure:"cache_dir" json:"cache_dir"`
	VFSCacheMode          string `yaml:"vfs_cache_mode" mapstructure:"vfs_cache_mode" json:"vfs_cache_mode"`
	VFSCachePollInterval  string `yaml:"vfs_cache_poll_interval" mapstructure:"vfs_cache_poll_interval" json:"vfs_cache_poll_interval"`
	VFSReadChunkSize      string `yaml:"vfs_read_chunk_size" mapstructure:"vfs_read_chunk_size" json:"vfs_read_chunk_size"`
	VFSReadChunkSizeLimit string `yaml:"vfs_read_chunk_size_limit" mapstructure:"vfs_read_chunk_size_limit" json:"vfs_read_chunk_size_limit"`
	VFSCacheMaxSize       string `yaml:"vfs_cache_max_size" mapstructure:"vfs_cache_max_size" json:"vfs_cache_max_size"`
	VFSCacheMaxAge        string `yaml:"vfs_cache_max_age" mapstructure:"vfs_cache_max_age" json:"vfs_cache_max_age"`
	VFSReadAhead          string `yaml:"vfs_read_ahead" mapstructure:"vfs_read_ahead" json:"vfs_read_ahead"`
	DirCacheTime          string `yaml:"dir_cache_time" mapstructure:"dir_cache_time" json:"dir_cache_time"`
	VFSCacheMinFreeSpace  string `yaml:"vfs_cache_min_free_space" mapstructure:"vfs_cache_min_free_space" json:"vfs_cache_min_free_space"`
	VFSDiskSpaceTotal     string `yaml:"vfs_disk_space_total" mapstructure:"vfs_disk_space_total" json:"vfs_disk_space_total"`
	VFSReadChunkStreams   int    `yaml:"vfs_read_chunk_streams" mapstructure:"vfs_read_chunk_streams" json:"vfs_read_chunk_streams"`

	// Mount-Specific Settings
	AllowOther    bool   `yaml:"allow_other" mapstructure:"allow_other" json:"allow_other"`
	AllowNonEmpty bool   `yaml:"allow_non_empty" mapstructure:"allow_non_empty" json:"allow_non_empty"`
	ReadOnly      bool   `yaml:"read_only" mapstructure:"read_only" json:"read_only"`
	Timeout       string `yaml:"timeout" mapstructure:"timeout" json:"timeout"`
	Syslog        bool   `yaml:"syslog" mapstructure:"syslog" json:"syslog"`

	// RcdRestartAfter is how long the rcd subprocess must stay unresponsive to
	// liveness probes before it is killed and restarted. Empty means the built-in
	// default. Restarting is disruptive, because re-establishing the mount
	// unmounts it out from under every process reading it, so an install whose
	// rcd goes briefly slow under load can raise this to ride the stall out.
	RcdRestartAfter string `yaml:"rcd_restart_after" mapstructure:"rcd_restart_after" json:"rcd_restart_after"`

	// Advanced Settings
	NoModTime          bool `yaml:"no_mod_time" mapstructure:"no_mod_time" json:"no_mod_time"`
	NoChecksum         bool `yaml:"no_checksum" mapstructure:"no_checksum" json:"no_checksum"`
	AsyncRead          bool `yaml:"async_read" mapstructure:"async_read" json:"async_read"`
	VFSFastFingerprint bool `yaml:"vfs_fast_fingerprint" mapstructure:"vfs_fast_fingerprint" json:"vfs_fast_fingerprint"`
	UseMmap            bool `yaml:"use_mmap" mapstructure:"use_mmap" json:"use_mmap"`
	Links              bool `yaml:"links" mapstructure:"links" json:"links"`
}

// ImportStrategy represents the import strategy type
type ImportStrategy string

const (
	ImportStrategyNone    ImportStrategy = "NONE"
	ImportStrategySYMLINK ImportStrategy = "SYMLINK"
	ImportStrategySTRM    ImportStrategy = "STRM"
)

// defaultVerifyContentTimeoutSeconds is the shared fallback for both
// Import.VerifyContentTimeoutSeconds and Health.VerifyContentTimeoutSeconds
// so the 15s default lives in one place instead of being duplicated across
// DefaultConfig and the accessor fallbacks.
const defaultVerifyContentTimeoutSeconds = 15

// ImportConfig represents import processing configuration
type ImportConfig struct {
	MaxProcessorWorkers            int      `yaml:"max_processor_workers" mapstructure:"max_processor_workers" json:"max_processor_workers"`
	QueueProcessingIntervalSeconds int      `yaml:"queue_processing_interval_seconds" mapstructure:"queue_processing_interval_seconds" json:"queue_processing_interval_seconds"`
	AllowedFileExtensions          []string `yaml:"allowed_file_extensions" mapstructure:"allowed_file_extensions" json:"allowed_file_extensions"`
	// MaxConcurrentImports caps the number of NZB imports that may run
	// end-to-end at the same time. 0 = unlimited. NNTP connection use is
	// balanced automatically: imports share the pool's full capacity and
	// yield to streams (priority lane + adaptive connection budget).
	MaxConcurrentImports int `yaml:"max_concurrent_imports" mapstructure:"max_concurrent_imports" json:"max_concurrent_imports"`
	// StreamHeadroomConnections is how many connections are held back from
	// import per active stream. On a saturated link those connections are slack
	// — they are not converting into bytes — so handing them to playback costs
	// little and measurably shortens stream latency. Past that point the pool
	// can no longer fill the link and import throughput really does fall.
	//
	// Measured at 100 connections behind a 400 MB/s link: 8 is free (import
	// 358 vs 359 MB/s) and takes stream p50 190ms -> 179ms; 32 costs ~13% import
	// and takes p50 to 146ms and p99 245ms -> 167ms. The right value depends on
	// the link rate and pool size, so it is a knob rather than a constant.
	// 0 disables the reservation entirely.
	// A nil pointer means "unset, use the default"; an explicit 0 disables the
	// reservation. A plain int cannot express that difference, since YAML omits
	// and YAML-zero both decode to 0.
	StreamHeadroomConnections *int `yaml:"stream_headroom_connections" mapstructure:"stream_headroom_connections" json:"stream_headroom_connections,omitempty"`
	MaxDownloadPrefetch       int  `yaml:"max_download_prefetch" mapstructure:"max_download_prefetch" json:"max_download_prefetch"`
	// SegmentSamplePercentage is the fraction (1-100) of segments randomly sampled
	// during import-phase validation (RAR extraction, archive integrity). Distinct from
	// Health.SegmentSamplePercentage, which samples during post-import health checks.
	// Lower defaults faster imports but skips error detection. Default 1%.
	SegmentSamplePercentage  int            `yaml:"segment_sample_percentage" mapstructure:"segment_sample_percentage" json:"segment_sample_percentage"`
	ReadTimeoutSeconds       int            `yaml:"read_timeout_seconds" mapstructure:"read_timeout_seconds" json:"read_timeout_seconds"`
	IsoAnalyzeTimeoutSeconds *int           `yaml:"iso_analyze_timeout_seconds" mapstructure:"iso_analyze_timeout_seconds" json:"iso_analyze_timeout_seconds,omitempty"`
	ImportStrategy           ImportStrategy `yaml:"import_strategy" mapstructure:"import_strategy" json:"import_strategy"`
	ImportDir                *string        `yaml:"import_dir" mapstructure:"import_dir" json:"import_dir,omitempty"`
	WatchDir                 *string        `yaml:"watch_dir" mapstructure:"watch_dir" json:"watch_dir,omitempty"`
	WatchIntervalSeconds     *int           `yaml:"watch_interval_seconds" mapstructure:"watch_interval_seconds" json:"watch_interval_seconds,omitempty"`
	AllowNestedRarExtraction *bool          `yaml:"allow_nested_rar_extraction" mapstructure:"allow_nested_rar_extraction" json:"allow_nested_rar_extraction,omitempty"`
	ExpandBlurayIso          *bool          `yaml:"expand_bluray_iso" mapstructure:"expand_bluray_iso" json:"expand_bluray_iso,omitempty"`
	RenameToNzbName          *bool          `yaml:"rename_to_nzb_name" mapstructure:"rename_to_nzb_name" json:"rename_to_nzb_name,omitempty"`
	FilterSampleFiles        *bool          `yaml:"filter_sample_files" mapstructure:"filter_sample_files" json:"filter_sample_files,omitempty"`
	FailedItemRetentionHours *int           `yaml:"failed_item_retention_hours" mapstructure:"failed_item_retention_hours" json:"failed_item_retention_hours,omitempty"`
	HistoryRetentionDays     *int           `yaml:"history_retention_days" mapstructure:"history_retention_days" json:"history_retention_days,omitempty"`
	// VerifyContent, when true, probes each eligible video/audio file's
	// first bytes through the serving stack after import and fails the
	// import if no recognized media container signature is found.
	VerifyContent               *bool `yaml:"verify_content" mapstructure:"verify_content" json:"verify_content,omitempty"`
	VerifyContentTimeoutSeconds *int  `yaml:"verify_content_timeout_seconds" mapstructure:"verify_content_timeout_seconds" json:"verify_content_timeout_seconds,omitempty"`
}

// LogConfig represents logging configuration with rotation support
type LogConfig struct {
	File       string `yaml:"file" mapstructure:"file" json:"file,omitempty"`                      // Log file path (empty = console only)
	Level      string `yaml:"level" mapstructure:"level" json:"level,omitempty"`                   // Log level (debug, info, warn, error)
	MaxSize    int    `yaml:"max_size" mapstructure:"max_size" json:"max_size,omitempty"`          // Max size in MB before rotation
	MaxAge     int    `yaml:"max_age" mapstructure:"max_age" json:"max_age,omitempty"`             // Max age in days to keep files
	MaxBackups int    `yaml:"max_backups" mapstructure:"max_backups" json:"max_backups,omitempty"` // Max number of old files to keep
	Compress   bool   `yaml:"compress" mapstructure:"compress" json:"compress,omitempty"`          // Compress old log files
}

// RepairConfig represents repair behavior configuration
type RepairConfig struct {
	Enabled          *bool `yaml:"enabled" mapstructure:"enabled" json:"enabled,omitempty"`
	IntervalMinutes  int   `yaml:"interval_minutes" mapstructure:"interval_minutes" json:"interval_minutes,omitempty"`
	MaxCoolDownHours int   `yaml:"max_cooldown_hours" mapstructure:"max_cooldown_hours" json:"max_cooldown_hours,omitempty"`
	MaxRepairRetries int   `yaml:"max_repair_retries" mapstructure:"max_repair_retries" json:"max_repair_retries"`

	ExponentialBackoff *bool `yaml:"exponential_backoff" mapstructure:"exponential_backoff" json:"exponential_backoff,omitempty"`

	// AutoSearchWaitSeconds bounds how long a repair waits for the ARR's own
	// automatic redownload search — queued by the ARR the moment a release is
	// blocklisted — to finish before the file record is deleted and AltMount
	// issues its own targeted search. 0 disables the wait and restores the
	// previous fire-and-forget ordering.
	AutoSearchWaitSeconds int `yaml:"auto_search_wait_seconds" mapstructure:"auto_search_wait_seconds" json:"auto_search_wait_seconds"`
	// FileDeleteConfirmSeconds bounds how long a repair waits for the ARR to
	// report the deleted file record as unlinked before issuing its search.
	FileDeleteConfirmSeconds int `yaml:"file_delete_confirm_seconds" mapstructure:"file_delete_confirm_seconds" json:"file_delete_confirm_seconds"`
}

// HealthConfig represents health checker configuration
type HealthConfig struct {
	Enabled                 *bool   `yaml:"enabled" mapstructure:"enabled" json:"enabled,omitempty"`
	LibraryDir              *string `yaml:"library_dir" mapstructure:"library_dir" json:"library_dir,omitempty"`
	CleanupOrphanedMetadata *bool   `yaml:"cleanup_orphaned_metadata" mapstructure:"cleanup_orphaned_metadata" json:"cleanup_orphaned_metadata,omitempty"`
	CheckIntervalSeconds    int     `yaml:"check_interval_seconds" mapstructure:"check_interval_seconds" json:"check_interval_seconds,omitempty"`
	// MaxConcurrentSegmentChecks bounds how many STAT checks a health sweep keeps
	// in flight. STAT is bodyless and pipelines many per connection, so connection
	// count is the wrong unit — the import path already sizes its sweep by STAT
	// pipeline depth (see importer/processor.go).
	//
	// nil or 0 means "adapt": the pool narrows the sweep to one connection's depth
	// while a stream is playing and widens it to the pool's aggregate STAT capacity
	// when idle, which measured 24x the sweep throughput on an idle pool.
	MaxConcurrentSegmentChecks *int `yaml:"max_concurrent_segment_checks" mapstructure:"max_concurrent_segment_checks" json:"max_concurrent_segment_checks,omitempty"`

	// Deprecated: superseded by MaxConcurrentSegmentChecks, which is named for
	// what it actually bounds. Read for one-time migration only (see
	// migrateHealthSweepConcurrency), then cleared. Do not use in new code.
	MaxConnectionsForHealthChecks int `yaml:"max_connections_for_health_checks,omitempty" mapstructure:"max_connections_for_health_checks" json:"max_connections_for_health_checks,omitempty"`
	CheckBatchSize                int `yaml:"check_batch_size" mapstructure:"check_batch_size" json:"check_batch_size,omitempty"`
	MaxConcurrentJobs             int `yaml:"max_concurrent_jobs" mapstructure:"max_concurrent_jobs" json:"max_concurrent_jobs,omitempty"`
	// SegmentSamplePercentage is the fraction (1-100) of segments randomly sampled
	// during health-check sweeps (post-import validation only). Distinct from
	// Import.SegmentSamplePercentage, which samples during archive extraction.
	// Higher sampling catches more corruption but adds overhead. Default 5%.
	SegmentSamplePercentage             int     `yaml:"segment_sample_percentage" mapstructure:"segment_sample_percentage" json:"segment_sample_percentage,omitempty"`
	MaxRetries                          int     `yaml:"max_retries" mapstructure:"max_retries" json:"max_retries"`
	LibrarySyncIntervalMinutes          int     `yaml:"library_sync_interval_minutes" mapstructure:"library_sync_interval_minutes" json:"library_sync_interval_minutes,omitempty"`
	LibrarySyncConcurrency              int     `yaml:"library_sync_concurrency" mapstructure:"library_sync_concurrency" json:"library_sync_concurrency,omitempty"`
	ResolveRepairOnImport               *bool   `yaml:"resolve_repair_on_import" mapstructure:"resolve_repair_on_import" json:"resolve_repair_on_import,omitempty"`
	VerifyData                          *bool   `yaml:"verify_data" mapstructure:"verify_data" json:"verify_data,omitempty"`
	CheckAllSegments                    *bool   `yaml:"check_all_segments" mapstructure:"check_all_segments" json:"check_all_segments,omitempty"`
	ReadTimeoutSeconds                  int     `yaml:"read_timeout_seconds" mapstructure:"read_timeout_seconds" json:"read_timeout_seconds,omitempty"`
	AcceptableMissingSegmentsPercentage float64 `yaml:"acceptable_missing_segments_percentage" mapstructure:"acceptable_missing_segments_percentage" json:"acceptable_missing_segments_percentage"`
	// ExcludedCategories lists SABnzbd category names whose files must never be
	// registered for health checking by the library-sync discovery pass. Matching
	// is by the category's configured directory under CompleteDir and is
	// case-insensitive. Empty means no categories are excluded.
	ExcludedCategories []string     `yaml:"excluded_categories" mapstructure:"excluded_categories" json:"excluded_categories,omitempty"`
	Repair             RepairConfig `yaml:"repair" mapstructure:"repair" json:"repair"`
	// CorruptionAction controls what happens when the health checker or a streaming read
	// confirms real (non-degraded) corruption: "repair" (default) triggers an Arr rescan;
	// "delete" removes the file's metadata/NZB/health record and cleans up now-empty
	// parent directories instead. Degraded files are never affected either way.
	CorruptionAction string `yaml:"corruption_action" mapstructure:"corruption_action" json:"corruption_action,omitempty"`
	// VerifyContent, when true, probes each eligible video/audio file's
	// first bytes through the serving stack during a health check and
	// marks the file corrupted if no recognized media container signature
	// is found. Distinct from the unrelated, unused VerifyData field above.
	VerifyContent               *bool `yaml:"verify_content" mapstructure:"verify_content" json:"verify_content,omitempty"`
	VerifyContentTimeoutSeconds *int  `yaml:"verify_content_timeout_seconds" mapstructure:"verify_content_timeout_seconds" json:"verify_content_timeout_seconds,omitempty"`
	// CorruptedRetentionDays bounds how long the corrupted_metadata safety copies
	// created by MoveToCorrupted are kept before the health cycle prunes them.
	// nil or 0 means keep forever, which is what every install did before this
	// setting existed.
	CorruptedRetentionDays *int `yaml:"corrupted_retention_days" mapstructure:"corrupted_retention_days" json:"corrupted_retention_days,omitempty"`
}

// Path validation functions have been moved to internal/utils/path.go

// ProviderConfig represents a single NNTP provider configuration
type ProviderConfig struct {
	// ID is a stable public identifier used in pool names, metrics, and errors.
	// It must never contain credentials or non-graphic characters.
	ID                  string `yaml:"id" mapstructure:"id" json:"id"`
	Name                string `yaml:"name" mapstructure:"name" json:"name,omitempty"`
	Host                string `yaml:"host" mapstructure:"host" json:"host"`
	Port                int    `yaml:"port" mapstructure:"port" json:"port"`
	Username            string `yaml:"username" mapstructure:"username" json:"username"`
	Password            string `yaml:"password" mapstructure:"password" json:"-"`
	MaxConnections      int    `yaml:"max_connections" mapstructure:"max_connections" json:"max_connections"`
	MinConnectionsAlive int    `yaml:"min_connections_alive" mapstructure:"min_connections_alive" json:"min_connections_alive,omitempty"`
	// InflightRequests caps concurrent decoded article bodies per connection (memory
	// ceiling). In practice, the pool-wide ImportBudget caps total in-flight bodies
	// to roughly the connection count, so each connection carries ~1 body. Peak observed
	// depth is 3-4 against this cap of 10. Raising it does not improve import throughput.
	InflightRequests int `yaml:"inflight_requests" mapstructure:"inflight_requests" json:"inflight_requests"`
	// StatInflightRequests caps the per-connection STAT pipeline depth (bodyless
	// existence checks). Its pool-wide aggregate (connections × depth, clamped to 4096)
	// determines how much an idle-pool health sweep can widen: measured 24x sweep
	// throughput improvement. While a stream is active the sweep is capped lower regardless.
	StatInflightRequests int `yaml:"stat_inflight_requests" mapstructure:"stat_inflight_requests" json:"stat_inflight_requests"`
	// StreamInflightRequests caps streaming (priority-lane) bodies in flight
	// per connection, so a playback read never queues behind a connection's
	// worth of read-ahead. 0 defaults to 4; values above inflight_requests
	// are capped to it.
	StreamInflightRequests   int    `yaml:"stream_inflight_requests" mapstructure:"stream_inflight_requests" json:"stream_inflight_requests,omitempty"`
	TLS                      bool   `yaml:"tls" mapstructure:"tls" json:"tls"`
	InsecureTLS              bool   `yaml:"insecure_tls" mapstructure:"insecure_tls" json:"insecure_tls"`
	ProxyURL                 string `yaml:"proxy_url" mapstructure:"proxy_url" json:"proxy_url,omitempty"`
	Enabled                  *bool  `yaml:"enabled" mapstructure:"enabled" json:"enabled,omitempty"`
	IsBackupProvider         *bool  `yaml:"is_backup_provider" mapstructure:"is_backup_provider" json:"is_backup_provider,omitempty"`
	StorageGroup             string `yaml:"storage_group" mapstructure:"storage_group" json:"storage_group,omitempty"`
	SkipPing                 bool   `yaml:"skip_ping" mapstructure:"skip_ping" json:"skip_ping,omitempty"`
	KeepaliveIntervalSeconds int    `yaml:"keepalive_interval_seconds" mapstructure:"keepalive_interval_seconds" json:"keepalive_interval_seconds,omitempty"`
	KeepaliveCommand         string `yaml:"keepalive_command" mapstructure:"keepalive_command" json:"keepalive_command,omitempty"`
	UserAgent                string `yaml:"user_agent" mapstructure:"user_agent" json:"user_agent,omitempty"`
	// MaxArticleAgeDays is how far back this provider's retention reaches.
	// Articles older than this are fetched from it only after every provider
	// that does reach back that far has been tried, so a short-retention
	// provider absorbs recent posts and leaves the deep-retention providers'
	// connections and quota for the articles that have nowhere else to go.
	// 0 (the default) declares no limit.
	MaxArticleAgeDays int `yaml:"max_article_age_days" mapstructure:"max_article_age_days" json:"max_article_age_days,omitempty"`
	// StrictMaxArticleAge makes MaxArticleAgeDays binding rather than an
	// ordering preference: the provider is not contacted at all for an
	// article older than its retention. For a metered account whose capacity
	// should not be spent on a request that will almost certainly 430.
	StrictMaxArticleAge   bool       `yaml:"strict_max_article_age" mapstructure:"strict_max_article_age" json:"strict_max_article_age,omitempty"`
	QuotaBytes            int64      `yaml:"quota_bytes" mapstructure:"quota_bytes" json:"quota_bytes,omitempty"`
	QuotaPeriodHours      int        `yaml:"quota_period_hours" mapstructure:"quota_period_hours" json:"quota_period_hours,omitempty"`
	LastRTTMs             int64      `yaml:"last_rtt_ms" mapstructure:"last_rtt_ms" json:"last_rtt_ms,omitempty"`
	LastSpeedTestMbps     float64    `yaml:"last_speed_test_mbps" mapstructure:"last_speed_test_mbps" json:"last_speed_test_mbps,omitempty"`
	LastSpeedTestTime     *time.Time `yaml:"last_speed_test_time" mapstructure:"last_speed_test_time" json:"last_speed_test_time,omitempty"`
	AccountExpirationDate string     `yaml:"account_expiration_date" mapstructure:"account_expiration_date" json:"account_expiration_date,omitempty"`
}

// SABnzbdConfig represents SABnzbd-compatible API configuration
type SABnzbdConfig struct {
	Enabled               *bool             `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	CompleteDir           string            `yaml:"complete_dir" mapstructure:"complete_dir" json:"complete_dir"`
	DownloadClientBaseURL string            `yaml:"download_client_base_url" mapstructure:"download_client_base_url" json:"download_client_base_url,omitempty"`
	Categories            []SABnzbdCategory `yaml:"categories" mapstructure:"categories" json:"categories"`
	// HistoryRetentionMinutes controls how far back the SABnzbd-emulating history
	// endpoint looks into import_history when *arr clients poll without a specific
	// nzo_id filter. Older entries are still returned when *arr asks for them by
	// nzo_id. Defaults to 10080 (7 days).
	HistoryRetentionMinutes int `yaml:"history_retention_minutes" mapstructure:"history_retention_minutes" json:"history_retention_minutes,omitempty"`
	// Fallback configuration for sending failed imports to external SABnzbd
	FallbackHost   string `yaml:"fallback_host" mapstructure:"fallback_host" json:"fallback_host"`
	FallbackAPIKey string `yaml:"fallback_api_key" mapstructure:"fallback_api_key" json:"fallback_api_key"` // Masked in API responses
}

// SABnzbdCategory represents a SABnzbd category configuration
type SABnzbdCategory struct {
	Name     string `yaml:"name" mapstructure:"name" json:"name"`
	Order    int    `yaml:"order" mapstructure:"order" json:"order"`
	Priority int    `yaml:"priority" mapstructure:"priority" json:"priority"`
	Dir      string `yaml:"dir" mapstructure:"dir" json:"dir"`
	Type     string `yaml:"type" mapstructure:"type" json:"type"` // "sonarr" or "radarr"
}

// IgnoredMessage represents an error message to ignore during queue cleanup
type IgnoredMessage struct {
	Message string `yaml:"message" mapstructure:"message" json:"message"`
	Enabled bool   `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
}

// ArrsConfig represents arrs configuration
type ArrsConfig struct {
	Enabled                        *bool                `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	MaxWorkers                     int                  `yaml:"max_workers" mapstructure:"max_workers" json:"max_workers,omitempty"`
	WebhookBaseURL                 string               `yaml:"webhook_base_url" mapstructure:"webhook_base_url" json:"webhook_base_url,omitempty"`
	RadarrInstances                []ArrsInstanceConfig `yaml:"radarr_instances" mapstructure:"radarr_instances" json:"radarr_instances"`
	SonarrInstances                []ArrsInstanceConfig `yaml:"sonarr_instances" mapstructure:"sonarr_instances" json:"sonarr_instances"`
	LidarrInstances                []ArrsInstanceConfig `yaml:"lidarr_instances" mapstructure:"lidarr_instances" json:"lidarr_instances"`
	ReadarrInstances               []ArrsInstanceConfig `yaml:"readarr_instances" mapstructure:"readarr_instances" json:"readarr_instances"`
	WhisparrInstances              []ArrsInstanceConfig `yaml:"whisparr_instances" mapstructure:"whisparr_instances" json:"whisparr_instances"`
	SportarrInstances              []ArrsInstanceConfig `yaml:"sportarr_instances" mapstructure:"sportarr_instances" json:"sportarr_instances"`
	QueueCleanupEnabled            *bool                `yaml:"queue_cleanup_enabled" mapstructure:"queue_cleanup_enabled" json:"queue_cleanup_enabled,omitempty"`
	QueueCleanupIntervalSeconds    int                  `yaml:"queue_cleanup_interval_seconds" mapstructure:"queue_cleanup_interval_seconds" json:"queue_cleanup_interval_seconds,omitempty"`
	QueueCleanupGracePeriodMinutes int                  `yaml:"queue_cleanup_grace_period_minutes" mapstructure:"queue_cleanup_grace_period_minutes" json:"queue_cleanup_grace_period_minutes,omitempty"`

	// QueueCleanupMaxFailures is the per-target failure circuit breaker. After
	// AltMount has acted on the same target (a Radarr movie or Sonarr/Whisparr
	// episode that keeps failing import) this many times — via queue cleanup,
	// health-repair re-searches or the partial-pack reconcile — it gives up:
	// it blocklists without re-searching and unmonitors the item in the *arr so it
	// stops being automatically re-grabbed. 0 (the default) disables the breaker.
	QueueCleanupMaxFailures int `yaml:"queue_cleanup_max_failures" mapstructure:"queue_cleanup_max_failures" json:"queue_cleanup_max_failures,omitempty"`

	// QueueCleanupRules matches an *arr status message for a stuck/failed import and
	// decides the action (remove / blocklist / blocklist+search). This is the single
	// message-rule list for queue cleanup; ghost/empty-folder detection runs alongside
	// it in the same pass. Only items owned by AltMount's download client are touched
	// (see issue #523).
	QueueCleanupRules []StuckCleanupRule `yaml:"queue_cleanup_rules,omitempty" mapstructure:"queue_cleanup_rules" json:"queue_cleanup_rules,omitempty"`

	// Deprecated: the fields below are read from existing config files for one-time
	// migration into the unified queue-cleanup model (see migrateArrsCleanup) and are
	// cleared afterwards. They are hidden from the API (json:"-") and dropped from
	// saved YAML once empty (yaml omitempty). Do not use in new code.
	QueueCleanupAllowlist          []IgnoredMessage   `yaml:"queue_cleanup_allowlist,omitempty" mapstructure:"queue_cleanup_allowlist" json:"-"`
	StuckCleanupEnabled            *bool              `yaml:"stuck_cleanup_enabled,omitempty" mapstructure:"stuck_cleanup_enabled" json:"-"`
	StuckCleanupGracePeriodMinutes int                `yaml:"stuck_cleanup_grace_period_minutes,omitempty" mapstructure:"stuck_cleanup_grace_period_minutes" json:"-"`
	StuckCleanupRules              []StuckCleanupRule `yaml:"stuck_cleanup_rules,omitempty" mapstructure:"stuck_cleanup_rules" json:"-"`
	// Deprecated: the hardcoded "automatic import is not possible" purge has been
	// folded into the unified QueueCleanupRules (see migrateArrsCleanup). Read for
	// one-time migration only, then cleared. Do not use in new code.
	CleanupAutomaticImportFailure *bool `yaml:"cleanup_automatic_import_failure,omitempty" mapstructure:"cleanup_automatic_import_failure" json:"-"`
}

// Stuck cleanup actions decide what happens to a matched stuck import.
const (
	// StuckActionRemove removes the item from the queue only (no blocklist, no
	// re-search) — for transient/environmental errors or already-satisfied files.
	StuckActionRemove = "remove"
	// StuckActionBlocklist removes the item and blocklists the release so the same
	// NZB is not grabbed again, but does NOT trigger a new search.
	StuckActionBlocklist = "blocklist"
	// StuckActionBlocklistSearch blocklists the release and triggers the *arr to
	// search for a replacement.
	StuckActionBlocklistSearch = "blocklist_search"
)

// StuckCleanupRule matches a stuck-import status message and decides the action
// (one of StuckActionRemove, StuckActionBlocklist, StuckActionBlocklistSearch).
type StuckCleanupRule struct {
	Message string `yaml:"message" mapstructure:"message" json:"message"`
	Enabled bool   `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	Action  string `yaml:"action" mapstructure:"action" json:"action"`
}

// migrateGlobalUserAgent adopts the legacy nzblnk.user_agent as the global
// user_agent when the global one was never customized, then clears the legacy
// field so it is dropped from saved YAML. Idempotent.
// migrateHealthSweepConcurrency carries the legacy
// health.max_connections_for_health_checks over to max_concurrent_segment_checks
// and clears it, so it drops out of saved YAML.
//
// The rename is not cosmetic: the old key was named for connections but bounded
// STAT concurrency, and because it defaulted to 100 with validation rejecting
// <= 0, the pool's stream-aware adaptive sizing was unreachable in every valid
// config. An operator who had set it explicitly keeps that value; everyone else
// gets the adaptive behaviour the code always intended.
//
// Idempotent: once the legacy field is zero this does nothing.
func migrateHealthSweepConcurrency(config *Config) {
	legacy := config.Health.MaxConnectionsForHealthChecks
	if legacy == 0 {
		return
	}
	if config.Health.MaxConcurrentSegmentChecks == nil {
		v := legacy
		config.Health.MaxConcurrentSegmentChecks = &v
	}
	config.Health.MaxConnectionsForHealthChecks = 0
}

func migrateGlobalUserAgent(config *Config) {
	if config.Nzblnk.UserAgent == "" {
		return
	}
	if config.UserAgent == "" || config.UserAgent == DefaultUserAgent {
		config.UserAgent = config.Nzblnk.UserAgent
	}
	config.Nzblnk = NzblnkConfig{}
}

// migrateProviderIDs assigns a stable "provider_N" id to any provider whose
// id is empty. ID was optional until Validate started requiring it: earlier
// docs told users it was fine to leave blank ("leave empty for
// auto-generation"), but nothing ever actually generated one for a
// hand-edited config.yaml — only the create-provider API did. Without this,
// such a config now fails validation and the process refuses to start.
//
// Existing non-empty ids are left untouched and never reused, so this never
// collides with an id a provider already has.
func migrateProviderIDs(config *Config) {
	used := make(map[string]struct{}, len(config.Providers))
	for _, p := range config.Providers {
		if id := strings.TrimSpace(p.ID); id != "" {
			used[id] = struct{}{}
		}
	}

	next := 1
	for i := range config.Providers {
		if strings.TrimSpace(config.Providers[i].ID) != "" {
			continue
		}
		var id string
		for {
			id = fmt.Sprintf("provider_%d", next)
			next++
			if _, exists := used[id]; !exists {
				break
			}
		}
		used[id] = struct{}{}
		config.Providers[i].ID = id
		slog.Warn("Assigned a stable id to a provider with a blank id",
			"index", i, "host", config.Providers[i].Host, "assigned_id", id)
	}
}

// migrateArrsCleanup folds the legacy split cleanup config (separate stuck-rules
// list, allowlist, enable flag and grace period) into the unified QueueCleanupRules
// model, then clears the legacy fields so they are dropped from saved YAML.
//
// A config predates the merge when any legacy field is present. In that case the
// unified rules are rebuilt from the legacy values (the user's actual settings),
// overriding the defaults DefaultConfig pre-populated — otherwise a legacy config
// loaded on top of fresh defaults would silently keep the defaults and discard the
// user's customizations. The function is idempotent: once the legacy fields are
// empty it does nothing.
func migrateArrsCleanup(config *Config) {
	a := &config.Arrs

	// The legacy split-cleanup model (separate stuck rules / allowlist / enable flag /
	// grace period) predated the unified queue_cleanup_rules and coexisted with no
	// queue_cleanup_rules at all. When present, rebuild the unified list from it so the
	// user's actual settings override the defaults DefaultConfig pre-populated.
	legacySplitPresent := len(a.StuckCleanupRules) > 0 ||
		len(a.QueueCleanupAllowlist) > 0 ||
		a.StuckCleanupEnabled != nil ||
		a.StuckCleanupGracePeriodMinutes > 0
	if legacySplitPresent {
		// Rebuild the unified rules from the legacy config: the stuck rules verbatim,
		// then allowlist entries as plain "remove" rules. Rule matching is substring-based
		// (see matchStuckRule), so skip any allowlist entry already covered by an existing
		// rule whose message is a substring of it — e.g. an allowlist "Sample file" is dead
		// next to a "Sample" rule, and would just be a confusing duplicate.
		rules := append([]StuckCleanupRule(nil), a.StuckCleanupRules...)
		for _, m := range a.QueueCleanupAllowlist {
			covered := false
			for _, r := range rules {
				if r.Message == m.Message || (r.Message != "" && strings.Contains(m.Message, r.Message)) {
					covered = true
					break
				}
			}
			if !covered {
				rules = append(rules, StuckCleanupRule{
					Message: m.Message,
					Enabled: m.Enabled,
					Action:  StuckActionRemove,
				})
			}
		}
		a.QueueCleanupRules = rules

		// Enable unified cleanup if only the legacy stuck toggle was on.
		if a.QueueCleanupEnabled == nil && a.StuckCleanupEnabled != nil && *a.StuckCleanupEnabled {
			enabled := true
			a.QueueCleanupEnabled = &enabled
		}

		// Prefer the legacy stuck grace period when no queue grace period is configured.
		if a.QueueCleanupGracePeriodMinutes == 0 && a.StuckCleanupGracePeriodMinutes > 0 {
			a.QueueCleanupGracePeriodMinutes = a.StuckCleanupGracePeriodMinutes
		}

		// Clear legacy fields so SaveConfig no longer emits them.
		a.QueueCleanupAllowlist = nil
		a.StuckCleanupEnabled = nil
		a.StuckCleanupGracePeriodMinutes = 0
		a.StuckCleanupRules = nil
	}

	// Fold the legacy "Import Failure Cleanup" toggle (cleanup_automatic_import_failure)
	// into the unified rules. Unlike the split-cleanup fields, this toggle coexisted with
	// queue_cleanup_rules, so operate on the already-loaded rules (never rebuild them) to
	// avoid wiping the user's customizations. When it was on, preserve the prior purge by
	// enabling an existing rule that matches "automatic import is not possible" (e.g. the
	// seeded default, which loads disabled) or appending one; matching mirrors
	// matchStuckRule (a rule's message is a case-insensitive substring of the phrase).
	if a.CleanupAutomaticImportFailure != nil {
		if *a.CleanupAutomaticImportFailure {
			const phrase = "automatic import is not possible"
			found := false
			for i := range a.QueueCleanupRules {
				r := &a.QueueCleanupRules[i]
				if r.Message != "" && strings.Contains(phrase, strings.ToLower(r.Message)) {
					r.Enabled = true
					found = true
					break
				}
			}
			if !found {
				a.QueueCleanupRules = append(a.QueueCleanupRules, StuckCleanupRule{
					Message: phrase,
					Enabled: true,
					Action:  StuckActionRemove,
				})
			}
		}
		// Clear the legacy flag so SaveConfig no longer emits it.
		a.CleanupAutomaticImportFailure = nil
	}
}

// ArrsInstanceConfig represents a single arrs instance configuration
type ArrsInstanceConfig struct {
	Name              string `yaml:"name" mapstructure:"name" json:"name"`
	URL               string `yaml:"url" mapstructure:"url" json:"url"`
	APIKey            string `yaml:"api_key" mapstructure:"api_key" json:"api_key"`
	Category          string `yaml:"category" mapstructure:"category" json:"category,omitempty"`
	Enabled           *bool  `yaml:"enabled" mapstructure:"enabled" json:"enabled,omitempty"`
	SyncIntervalHours *int   `yaml:"sync_interval_hours" mapstructure:"sync_interval_hours" json:"sync_interval_hours,omitempty"`
}

// DeepCopy returns a deep copy of the configuration using the copier library.
// This handles all pointer fields, slices, and maps automatically.
func (c *Config) DeepCopy() *Config {
	if c == nil {
		return nil
	}

	copyCfg := &Config{}
	if err := copier.CopyWithOption(copyCfg, c, copier.Option{DeepCopy: true}); err != nil {
		// Fallback to shallow copy if deep copy fails (should not happen)
		shallowCopy := *c
		return &shallowCopy
	}

	return copyCfg
}

// GetWebhookBaseURL returns the configured webhook base URL or a default one based on the current port.
func (c *Config) GetWebhookBaseURL() string {
	if c.Arrs.WebhookBaseURL != "" {
		return c.Arrs.WebhookBaseURL
	}
	host := c.WebDAV.Host
	if host == "" {
		host = "altmount"
	}
	return fmt.Sprintf("http://%s:%d", host, c.WebDAV.Port)
}

// GetDownloadClientBaseURL returns the configured download client base URL or a default one based on the current port.
func (c *Config) GetDownloadClientBaseURL() string {
	if c.SABnzbd.DownloadClientBaseURL != "" {
		return c.SABnzbd.DownloadClientBaseURL
	}
	host := c.WebDAV.Host
	if host == "" {
		host = "altmount"
	}
	return fmt.Sprintf("http://%s:%d/sabnzbd", host, c.WebDAV.Port)
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.WebDAV.Port <= 0 || c.WebDAV.Port > 65535 {
		return fmt.Errorf("webdav port must be between 1 and 65535")
	}

	if c.Streaming.MaxPrefetch <= 0 {
		c.Streaming.MaxPrefetch = 60 // Default to 60 segments prefetched ahead if not set
	}

	// Segment cache expiry: nil (unset) defaults to 24 hours; an explicit 0 is
	// preserved and means "cache forever" (bounded only by the size cap). A
	// pointer is used so unset and explicit-0 can be distinguished.
	if c.SegmentCache.ExpiryHours == nil {
		defaultExpiryHours := 24
		c.SegmentCache.ExpiryHours = &defaultExpiryHours
	}
	if c.SegmentCache.MemoryMB == nil {
		memoryMB := defaultSegmentCacheMemoryMB
		c.SegmentCache.MemoryMB = &memoryMB
	} else if *c.SegmentCache.MemoryMB < 0 {
		return fmt.Errorf("segment_cache memory_mb must be 0 or greater")
	}

	if c.Import.MaxProcessorWorkers <= 0 {
		return fmt.Errorf("import max_processor_workers must be greater than 0")
	}

	if c.Import.QueueProcessingIntervalSeconds < 1 {
		return fmt.Errorf("import queue_processing_interval_seconds must be at least 1 second")
	}

	if c.Import.QueueProcessingIntervalSeconds > 300 {
		return fmt.Errorf("import queue_processing_interval_seconds must not exceed 300 seconds")
	}

	if c.Import.MaxDownloadPrefetch <= 0 {
		return fmt.Errorf("import max_download_prefetch must be greater than 0")
	}

	if c.Import.SegmentSamplePercentage < 1 || c.Import.SegmentSamplePercentage > 100 {
		return fmt.Errorf("import segment_sample_percentage must be between 1 and 100")
	}

	if c.Import.ReadTimeoutSeconds <= 0 {
		c.Import.ReadTimeoutSeconds = 300
	}

	// Validate import strategy
	validStrategies := map[ImportStrategy]bool{
		ImportStrategyNone:    true,
		ImportStrategySYMLINK: true,
		ImportStrategySTRM:    true,
	}
	if !validStrategies[c.Import.ImportStrategy] {
		return fmt.Errorf("import_strategy must be one of: NONE, SYMLINK, STRM")
	}
	// Validate import directory when strategy requires it
	if c.Import.ImportStrategy == ImportStrategySYMLINK || c.Import.ImportStrategy == ImportStrategySTRM {
		if c.Import.ImportDir == nil || *c.Import.ImportDir == "" {
			return fmt.Errorf("import_dir cannot be empty when import strategy is %s", c.Import.ImportStrategy)
		}
		if !filepath.IsAbs(*c.Import.ImportDir) {
			return fmt.Errorf("import_dir must be an absolute path")
		}
	}

	// Validate watch directory if configured
	if c.Import.WatchDir != nil && *c.Import.WatchDir != "" {
		if !filepath.IsAbs(*c.Import.WatchDir) {
			return fmt.Errorf("import watch_dir must be an absolute path")
		}
		if c.Import.WatchIntervalSeconds != nil && *c.Import.WatchIntervalSeconds <= 0 {
			return fmt.Errorf("import watch_interval_seconds must be greater than 0")
		}
	}

	if c.Import.VerifyContentTimeoutSeconds != nil && *c.Import.VerifyContentTimeoutSeconds <= 0 {
		return fmt.Errorf("import verify_content_timeout_seconds must be greater than 0")
	}

	// Validate log level (both old and new config)
	if c.Log.Level != "" {
		validLevels := []string{"debug", "info", "warn", "error"}
		isValid := slices.Contains(validLevels, c.Log.Level)
		if !isValid {
			return fmt.Errorf("log_level must be one of: debug, info, warn, error")
		}
	}

	// Validate log configuration
	if c.Log.Level != "" {
		validLevels := []string{"debug", "info", "warn", "error"}
		isValid := slices.Contains(validLevels, c.Log.Level)
		if !isValid {
			return fmt.Errorf("log.level must be one of: debug, info, warn, error")
		}
	}

	if c.Log.MaxSize < 0 {
		return fmt.Errorf("log.max_size must be non-negative")
	}

	if c.Log.MaxAge < 0 {
		return fmt.Errorf("log.max_age must be non-negative")
	}

	if c.Log.MaxBackups < 0 {
		return fmt.Errorf("log.max_backups must be non-negative")
	}

	// Validate metadata configuration (now required)
	if c.Metadata.RootPath == "" {
		return fmt.Errorf("metadata root_path cannot be empty")
	}

	// Validate metadata backup configuration
	if c.Metadata.Backup.Enabled != nil && *c.Metadata.Backup.Enabled {
		if c.Metadata.Backup.Schedule == "" {
			return fmt.Errorf("metadata backup schedule cannot be empty")
		}
		if _, err := cron.ParseStandard(c.Metadata.Backup.Schedule); err != nil {
			return fmt.Errorf("metadata backup schedule is not a valid cron expression: %w", err)
		}
		if c.Metadata.Backup.KeepBackups <= 0 {
			return fmt.Errorf("metadata backup keep_backups must be greater than 0")
		}
		if c.Metadata.Backup.Path == "" {
			return fmt.Errorf("metadata backup path cannot be empty")
		}
		if !filepath.IsAbs(c.Metadata.Backup.Path) {
			return fmt.Errorf("metadata backup path must be an absolute path")
		}
	}

	// Validate streaming configuration

	// Validate health configuration (always active)
	if c.Health.CheckIntervalSeconds <= 0 {
		return fmt.Errorf("health check_interval_seconds must be greater than 0")
	}
	if c.Health.MaxConcurrentSegmentChecks != nil && *c.Health.MaxConcurrentSegmentChecks < 0 {
		return fmt.Errorf("health max_concurrent_segment_checks must be zero (adaptive) or greater")
	}
	if c.Health.MaxConcurrentJobs <= 0 {
		return fmt.Errorf("health max_concurrent_jobs must be greater than 0")
	}
	if c.Health.LibrarySyncIntervalMinutes < 0 {
		return fmt.Errorf("health library_sync_interval_minutes must be non-negative")
	}
	if c.Health.SegmentSamplePercentage < 1 || c.Health.SegmentSamplePercentage > 100 {
		return fmt.Errorf("health segment_sample_percentage must be between 1 and 100")
	}
	if c.Health.VerifyContentTimeoutSeconds != nil && *c.Health.VerifyContentTimeoutSeconds <= 0 {
		return fmt.Errorf("health verify_content_timeout_seconds must be greater than 0")
	}
	if c.Health.CorruptedRetentionDays != nil && *c.Health.CorruptedRetentionDays < 0 {
		return fmt.Errorf("health corrupted_retention_days must be zero (keep forever) or greater")
	}
	if c.Health.Repair.AutoSearchWaitSeconds < 0 {
		return fmt.Errorf("health repair auto_search_wait_seconds must be zero (no wait) or greater")
	}
	if c.Health.Repair.FileDeleteConfirmSeconds < 0 {
		return fmt.Errorf("health repair file_delete_confirm_seconds must be zero (no wait) or greater")
	}

	// Validate health configuration - requires library_dir when enabled and using a strategy other than NONE
	if c.Health.Enabled != nil && *c.Health.Enabled {
		if c.Import.ImportStrategy != ImportStrategyNone {
			if c.Health.LibraryDir == nil || *c.Health.LibraryDir == "" {
				return fmt.Errorf("health library_dir is required when health system is enabled with %s strategy", c.Import.ImportStrategy)
			}
			if !filepath.IsAbs(*c.Health.LibraryDir) {
				return fmt.Errorf("health library_dir must be an absolute path")
			}
		}
	}

	// Validate cleanup orphaned metadata - requires library_dir when enabled and using a strategy other than NONE
	if c.Health.CleanupOrphanedMetadata != nil && *c.Health.CleanupOrphanedMetadata {
		if c.Import.ImportStrategy != ImportStrategyNone {
			if c.Health.LibraryDir == nil || *c.Health.LibraryDir == "" {
				return fmt.Errorf("health library_dir is required when cleanup_orphaned_metadata is enabled with %s strategy", c.Import.ImportStrategy)
			}
			if !filepath.IsAbs(*c.Health.LibraryDir) {
				return fmt.Errorf("health library_dir must be an absolute path")
			}
		}
	}

	// Enforce mutual exclusion via MountType and sync legacy flags
	falseVal := false
	trueVal := true
	switch c.MountType {
	case MountTypeNone, "":
		c.RClone.MountEnabled = &falseVal
		c.RClone.RCEnabled = &falseVal
		c.Fuse.Enabled = &falseVal
	case MountTypeRClone:
		if c.MountPath == "" {
			return fmt.Errorf("mount_path cannot be empty when mount type is rclone")
		}
		if !filepath.IsAbs(c.MountPath) {
			return fmt.Errorf("mount_path must be an absolute path")
		}
		c.RClone.MountEnabled = &trueVal
		c.RClone.RCEnabled = &trueVal // mount requires RC
		c.Fuse.Enabled = &falseVal
	case MountTypeFuse:
		if c.MountPath == "" {
			return fmt.Errorf("mount_path cannot be empty when mount type is fuse")
		}
		if !filepath.IsAbs(c.MountPath) {
			return fmt.Errorf("mount_path must be an absolute path")
		}
		c.RClone.MountEnabled = &falseVal
		c.RClone.RCEnabled = &falseVal
		c.Fuse.Enabled = &trueVal
		c.Fuse.MountPath = c.MountPath
	case MountTypeRCloneExternal:
		c.RClone.MountEnabled = &falseVal
		c.RClone.RCEnabled = &trueVal
		c.Fuse.Enabled = &falseVal
	default:
		return fmt.Errorf("invalid mount_type: %s (must be none, rclone, fuse, or rclone_external)", c.MountType)
	}

	// Apply default history retention for older configs that pre-date the field.
	if c.SABnzbd.HistoryRetentionMinutes <= 0 {
		c.SABnzbd.HistoryRetentionMinutes = 10080
	}

	// Validate SABnzbd configuration
	if c.SABnzbd.Enabled != nil && *c.SABnzbd.Enabled {
		// CompleteDir is a virtual path relative to the mount point, not an absolute filesystem path
		// It defaults to "/" (root of mount) if not specified
		// Normalize: remove leading/trailing slashes for consistency, then ensure it starts with /
		if c.SABnzbd.CompleteDir == "" {
			c.SABnzbd.CompleteDir = "/"
		} else {
			// Normalize the path: ensure it starts with / and remove trailing /
			cleanDir := strings.Trim(c.SABnzbd.CompleteDir, "/")
			if cleanDir == "" {
				c.SABnzbd.CompleteDir = "/"
			} else {
				c.SABnzbd.CompleteDir = "/" + cleanDir
			}
		}

		// Validate categories if provided. Names are compared trimmed and
		// case-insensitively so " Movies" cannot shadow "movies", but the stored
		// values are left untouched — Validate must not mutate the config it is
		// handed.
		categoryNames := make(map[string]bool)
		for i, category := range c.SABnzbd.Categories {
			name := strings.TrimSpace(category.Name)
			if name == "" {
				return fmt.Errorf("sabnzbd category %d: name cannot be empty", i)
			}
			nameKey := strings.ToLower(name)
			if categoryNames[nameKey] {
				return fmt.Errorf("sabnzbd category %d: duplicate category name '%s'", i, category.Name)
			}
			categoryNames[nameKey] = true
		}

		// Validate fallback configuration if host is provided
		if c.SABnzbd.FallbackHost != "" {
			// Basic URL validation
			if !strings.HasPrefix(c.SABnzbd.FallbackHost, "http://") && !strings.HasPrefix(c.SABnzbd.FallbackHost, "https://") {
				return fmt.Errorf("sabnzbd fallback_host must start with http:// or https://")
			}
			// Warn if API key is missing (but don't fail validation)
			if c.SABnzbd.FallbackAPIKey == "" {
				slog.Warn("SABnzbd fallback_host is set but fallback_api_key is empty")
			}
		}
	}

	for pattern := range c.Stremio.Prowlarr.CustomScores {
		if _, err := regexp.Compile("(?i)" + pattern); err != nil {
			return fmt.Errorf("stremio prowlarr custom_scores pattern %q is invalid: %w", pattern, err)
		}
	}
	for i, format := range c.Stremio.Scoring.CustomFormats {
		if format.Enabled && format.PatternType != "token" && strings.TrimSpace(format.Pattern) != "" {
			if _, err := regexp.Compile(format.Pattern); err != nil {
				return fmt.Errorf("stremio scoring custom_formats[%d] pattern is invalid: %w", i, err)
			}
		}
	}
	if strings.TrimSpace(c.Stremio.Scoring.ExcludeRegex) != "" {
		if _, err := regexp.Compile(c.Stremio.Scoring.ExcludeRegex); err != nil {
			return fmt.Errorf("stremio scoring exclude_regex is invalid: %w", err)
		}
	}

	// Validate mount_path
	if c.MountPath != "" && !filepath.IsAbs(c.MountPath) {
		return fmt.Errorf("mount_path must be an absolute path")
	}

	// Validate scraper configuration
	if c.Arrs.Enabled != nil && *c.Arrs.Enabled {
		// Mount path is required when ARRs is enabled
		if c.MountPath == "" {
			return fmt.Errorf("mount_path is required when arrs is enabled")
		}
		if c.Arrs.MaxWorkers <= 0 {
			return fmt.Errorf("scraper max_workers must be greater than 0")
		}
	}

	// Validate each provider
	providerIDs := make(map[string]struct{}, len(c.Providers))
	for i, provider := range c.Providers {
		trimmedID := strings.TrimSpace(provider.ID)
		if trimmedID == "" {
			return fmt.Errorf("provider %d: id cannot be empty", i)
		}
		if trimmedID != provider.ID {
			return fmt.Errorf("provider %d: id cannot have leading or trailing whitespace", i)
		}
		if strings.IndexFunc(provider.ID, func(r rune) bool { return !unicode.IsGraphic(r) }) >= 0 {
			return fmt.Errorf("provider %d: id contains non-graphic characters", i)
		}
		if _, exists := providerIDs[provider.ID]; exists {
			return fmt.Errorf("provider %d: id %q is duplicated", i, provider.ID)
		}
		providerIDs[provider.ID] = struct{}{}
		if provider.Host == "" {
			return fmt.Errorf("provider %d: host cannot be empty", i)
		}
		if provider.Port <= 0 || provider.Port > 65535 {
			return fmt.Errorf("provider %d: port must be between 1 and 65535", i)
		}
		if provider.MaxConnections <= 0 {
			return fmt.Errorf("provider %d: max_connections must be greater than 0", i)
		}
		if provider.MinConnectionsAlive < 0 || provider.MinConnectionsAlive > provider.MaxConnections {
			return fmt.Errorf("provider %d: min_connections_alive must be between 0 and max_connections", i)
		}
		if provider.InflightRequests <= 0 {
			c.Providers[i].InflightRequests = 10
		}
		if provider.StatInflightRequests <= 0 {
			c.Providers[i].StatInflightRequests = 100
		}
	}

	// Validate Fuse configuration
	if c.Fuse.MaxCacheSizeMB <= 0 {
		c.Fuse.MaxCacheSizeMB = 32 // Default
	}
	if c.Fuse.MaxReadAheadMB <= 0 {
		c.Fuse.MaxReadAheadMB = 128 // Default 128MB
	}
	// AsyncBufferSizeMB: 0 means disabled (passthrough). Clamp negatives.
	if c.Fuse.AsyncBufferSizeMB < 0 {
		c.Fuse.AsyncBufferSizeMB = 0
	}
	// AsyncBufferMaxTotalMB: 0 means unlimited. Clamp negatives.
	if c.Fuse.AsyncBufferMaxTotalMB < 0 {
		c.Fuse.AsyncBufferMaxTotalMB = 0
	}

	// Validate arrs queue-cleanup rule actions: an explicitly set action must be a
	// known value. An empty action is allowed and treated as "remove" at runtime
	// (see starrDeleteOpts), so it is not rejected here.
	for i, rule := range c.Arrs.QueueCleanupRules {
		switch rule.Action {
		case "", StuckActionRemove, StuckActionBlocklist, StuckActionBlocklistSearch:
		default:
			return fmt.Errorf("arrs queue_cleanup_rules[%d]: invalid action %q (must be %q, %q, or %q)",
				i, rule.Action, StuckActionRemove, StuckActionBlocklist, StuckActionBlocklistSearch)
		}
	}

	// The failure circuit breaker counts up to a positive threshold; 0 disables it.
	if c.Arrs.QueueCleanupMaxFailures < 0 {
		return fmt.Errorf("arrs queue_cleanup_max_failures must be >= 0 (0 disables the breaker), got %d",
			c.Arrs.QueueCleanupMaxFailures)
	}

	return nil
}

// ValidateDirectories validates that all configured directories are writable
// This performs actual filesystem checks and may create directories if needed
func (c *Config) ValidateDirectories() error {
	// Check metadata directory
	if err := utils.CheckDirectoryWritable(c.Metadata.RootPath); err != nil {
		return fmt.Errorf("metadata directory validation failed: %w", err)
	}

	// Check database directory
	if err := utils.CheckFileDirectoryWritable(c.Database.Path, "database"); err != nil {
		return err
	}

	// Check log file directory (only if log file is configured)
	if err := utils.CheckFileDirectoryWritable(c.Log.File, "log"); err != nil {
		return err
	}

	return nil
}

// ProviderChangeType describes the kind of provider change detected by ProvidersDiff.
type ProviderChangeType int

const (
	ProviderAdded ProviderChangeType = iota
	ProviderRemoved
	ProviderModified
)

// ProviderChange describes a single provider change between two configurations.
type ProviderChange struct {
	Type        ProviderChangeType
	ProviderID  string
	OldProvider *ProviderConfig // nil for Added
	NewProvider *ProviderConfig // nil for Removed
}

// NNTPPoolName returns the stable ID nntppool uses to identify this provider.
func (p *ProviderConfig) NNTPPoolName() string {
	return p.ID
}

// ToNNTPProvider converts a single ProviderConfig to an nntppool.Provider.
// Does not check the Enabled flag — caller is responsible for that.
func (p *ProviderConfig) ToNNTPProvider() nntppool.Provider {
	isBackup := false
	if p.IsBackupProvider != nil {
		isBackup = *p.IsBackupProvider
	}

	host := fmt.Sprintf("%s:%d", p.Host, p.Port)

	var tlsCfg *tls.Config
	if p.TLS {
		tlsCfg = &tls.Config{
			InsecureSkipVerify: p.InsecureTLS,
			ServerName:         p.Host,
			// One cached session per connection the allowance permits, so every
			// reconnect after the first is a resumption handshake.
			ClientSessionCache: tls.NewLRUClientSessionCache(max(p.MaxConnections, 1)),
		}
	}

	minConns := p.MinConnectionsAlive
	switch {
	case minConns < 0:
		minConns = 0
	case minConns == 0:
		minConns = defaultMinConnectionsAlive
	}
	if minConns > p.MaxConnections {
		minConns = p.MaxConnections
	}

	inflight := p.InflightRequests
	if inflight <= 0 {
		inflight = 10
	}

	statInflight := p.StatInflightRequests
	if statInflight <= 0 {
		statInflight = 100
	}

	// Streaming bodies per connection default to the full pipeline depth.
	// A tighter cap shortens how long a seek waits behind read-ahead on its
	// connection, but providers that reward deep pipelines lose sequential
	// throughput to it: capped at 4 a 15-connection account measured about
	// 20% below the uncapped pipeline. Users who want the bounded wait set
	// stream_inflight_requests explicitly.
	streamInflight := p.StreamInflightRequests
	if streamInflight <= 0 || streamInflight > inflight {
		streamInflight = inflight
	}

	return nntppool.Provider{
		Host:              host,
		Name:              p.ID,
		TLSConfig:         tlsCfg,
		Auth:              nntppool.Auth{Username: p.Username, Password: p.Password},
		Connections:       p.MaxConnections,
		MinConnections:    minConns,
		Backup:            isBackup,
		StorageGroup:      p.StorageGroup,
		Inflight:          inflight,
		StatInflight:      statInflight,
		StreamInflight:    streamInflight,
		IdleTimeout:       providerIdleTimeout,
		ReconnectDelay:    providerReconnectDelay,
		SkipPing:          p.SkipPing,
		KeepaliveInterval: time.Duration(p.KeepaliveIntervalSeconds) * time.Second,
		KeepaliveCommand:  p.KeepaliveCommand,
		UserAgent:         p.UserAgent,
		QuotaBytes:        p.QuotaBytes,
		QuotaPeriod:       time.Duration(p.QuotaPeriodHours) * time.Hour,
		MaxArticleAge:     time.Duration(max(p.MaxArticleAgeDays, 0)) * 24 * time.Hour,
		StrictMaxAge:      p.StrictMaxArticleAge,
	}
}

// ProvidersDiff computes the set of provider changes between this config and another.
// Returns nil if providers are identical (same set and same field values).
func (c *Config) ProvidersDiff(other *Config) []ProviderChange {
	oldMap := make(map[string]ProviderConfig, len(c.Providers))
	for _, p := range c.Providers {
		oldMap[p.ID] = p
	}

	newMap := make(map[string]ProviderConfig, len(other.Providers))
	for _, p := range other.Providers {
		newMap[p.ID] = p
	}

	var changes []ProviderChange

	// Detect removed and modified providers
	for id, oldP := range oldMap {
		newP, exists := newMap[id]
		if !exists {
			oldCopy := oldP
			changes = append(changes, ProviderChange{
				Type:        ProviderRemoved,
				ProviderID:  id,
				OldProvider: &oldCopy,
			})
			continue
		}
		if !providersFieldsEqual(oldP, newP) {
			oldCopy := oldP
			newCopy := newP
			changes = append(changes, ProviderChange{
				Type:        ProviderModified,
				ProviderID:  id,
				OldProvider: &oldCopy,
				NewProvider: &newCopy,
			})
		}
	}

	// Detect added providers
	for id, newP := range newMap {
		if _, exists := oldMap[id]; !exists {
			newCopy := newP
			changes = append(changes, ProviderChange{
				Type:        ProviderAdded,
				ProviderID:  id,
				NewProvider: &newCopy,
			})
		}
	}

	if len(changes) == 0 {
		return nil
	}
	return changes
}

// providersFieldsEqual returns true if two ProviderConfig values are identical
// in all fields that affect pool behaviour.
func providersFieldsEqual(a, b ProviderConfig) bool {
	return a.Host == b.Host &&
		a.Port == b.Port &&
		a.Username == b.Username &&
		a.SkipPing == b.SkipPing &&
		a.Password == b.Password &&
		a.MaxConnections == b.MaxConnections &&
		a.MinConnectionsAlive == b.MinConnectionsAlive &&
		a.InflightRequests == b.InflightRequests &&
		a.StatInflightRequests == b.StatInflightRequests &&
		a.TLS == b.TLS &&
		a.InsecureTLS == b.InsecureTLS &&
		a.ProxyURL == b.ProxyURL &&
		a.KeepaliveIntervalSeconds == b.KeepaliveIntervalSeconds &&
		a.KeepaliveCommand == b.KeepaliveCommand &&
		a.UserAgent == b.UserAgent &&
		a.QuotaBytes == b.QuotaBytes &&
		a.QuotaPeriodHours == b.QuotaPeriodHours &&
		a.MaxArticleAgeDays == b.MaxArticleAgeDays &&
		a.StrictMaxArticleAge == b.StrictMaxArticleAge &&
		a.StorageGroup == b.StorageGroup &&
		boolPtrEqual(a.Enabled, b.Enabled) &&
		boolPtrEqual(a.IsBackupProvider, b.IsBackupProvider)
}

// boolPtrEqual safely compares two *bool values.
func boolPtrEqual(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// ProvidersOrderChanged returns true if the provider order differs between
// this config and another, even if the set of providers is unchanged.
func (c *Config) ProvidersOrderChanged(other *Config) bool {
	if len(c.Providers) != len(other.Providers) {
		return false // different lengths are handled by ProvidersDiff
	}
	for i := range c.Providers {
		if c.Providers[i].ID != other.Providers[i].ID {
			return true
		}
	}
	return false
}

// ProvidersEqual compares the providers in this config with another config for equality
func (c *Config) ProvidersEqual(other *Config) bool {
	if len(c.Providers) != len(other.Providers) {
		return false
	}

	// Create maps for comparison (using ID as key for proper matching)
	oldMap := make(map[string]ProviderConfig)
	newMap := make(map[string]ProviderConfig)

	for _, provider := range c.Providers {
		oldMap[provider.ID] = provider
	}

	for _, provider := range other.Providers {
		newMap[provider.ID] = provider
	}

	// Check if all old providers exist in new config and are identical
	for id, oldProvider := range oldMap {
		newProvider, exists := newMap[id]
		if !exists {
			return false // Provider removed
		}

		// Compare all fields
		if oldProvider.ID != newProvider.ID ||
			oldProvider.Host != newProvider.Host ||
			oldProvider.Port != newProvider.Port ||
			oldProvider.Username != newProvider.Username ||
			oldProvider.Password != newProvider.Password ||
			oldProvider.MaxConnections != newProvider.MaxConnections ||
			oldProvider.TLS != newProvider.TLS ||
			oldProvider.InsecureTLS != newProvider.InsecureTLS ||
			oldProvider.ProxyURL != newProvider.ProxyURL ||
			oldProvider.StorageGroup != newProvider.StorageGroup ||
			*oldProvider.Enabled != *newProvider.Enabled ||
			*oldProvider.IsBackupProvider != *newProvider.IsBackupProvider {
			return false // Provider modified
		}
	}

	// Check if any new providers were added
	for id := range newMap {
		if _, exists := oldMap[id]; !exists {
			return false // Provider added
		}
	}

	return true // All providers are identical
}

// ToNNTPProviders converts ProviderConfig slice to nntppool.Provider slice (enabled only)
func (c *Config) ToNNTPProviders() []nntppool.Provider {
	var providers []nntppool.Provider
	for _, p := range c.Providers {
		if p.Enabled != nil && *p.Enabled {
			providers = append(providers, p.ToNNTPProvider())
		}
	}
	return providers
}

// ChangeCallback represents a function called when configuration changes
type ChangeCallback func(oldConfig, newConfig *Config)

// ConfigGetter represents a function that returns the current configuration
type ConfigGetter func() *Config

// Manager manages configuration state and persistence
type Manager struct {
	current           *Config
	configFile        string
	mutex             sync.RWMutex
	callbacks         []ChangeCallback
	needsLibrarySync  bool
	previousMountPath string
	librarySyncMutex  sync.RWMutex
}

// NewManager creates a new configuration manager
func NewManager(config *Config, configFile string) *Manager {
	return &Manager{
		current:    config,
		configFile: configFile,
	}
}

// GetConfig returns the current configuration (thread-safe)
func (m *Manager) GetConfig() *Config {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.current
}

// GetConfigGetter returns a function that provides the current configuration
func (m *Manager) GetConfigGetter() ConfigGetter {
	return m.GetConfig
}

// UpdateConfig updates the current configuration (thread-safe)
func (m *Manager) UpdateConfig(config *Config) error {
	m.mutex.Lock()
	// Take a deep copy of the old config so callbacks get an immutable snapshot
	var oldConfig *Config
	if m.current != nil {
		oldConfig = m.current.DeepCopy()
	}

	// Detect mount_path changes
	if oldConfig != nil && oldConfig.MountPath != config.MountPath {
		m.librarySyncMutex.Lock()
		m.needsLibrarySync = true
		m.previousMountPath = oldConfig.MountPath
		m.librarySyncMutex.Unlock()
	}

	m.current = config
	callbacks := make([]ChangeCallback, len(m.callbacks))
	copy(callbacks, m.callbacks)
	m.mutex.Unlock()

	// Notify callbacks after releasing the lock
	for _, callback := range callbacks {
		callback(oldConfig, config)
	}
	return nil
}

// OnConfigChange registers a callback to be called when configuration changes
func (m *Manager) OnConfigChange(callback ChangeCallback) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.callbacks = append(m.callbacks, callback)
}

// ValidateConfigUpdate validates configuration updates with additional restrictions
func (m *Manager) ValidateConfigUpdate(newConfig *Config) error {
	// First run standard validation
	if err := newConfig.Validate(); err != nil {
		return err
	}

	// Get current config for comparison
	m.mutex.RLock()
	currentConfig := m.current
	m.mutex.RUnlock()

	if currentConfig != nil {
		// Protect WebDAV port from API changes
		if newConfig.WebDAV.Port != currentConfig.WebDAV.Port {
			return fmt.Errorf("webdav port cannot be changed via API - requires server restart")
		}

		// Protect database path from API changes
		if newConfig.Database.Path != currentConfig.Database.Path {
			return fmt.Errorf("database path cannot be changed via API - requires server restart")
		}

		// Protect metadata root path from API changes
		if newConfig.Metadata.RootPath != currentConfig.Metadata.RootPath {
			return fmt.Errorf("metadata root_path cannot be changed via API - requires server restart")
		}

	}

	return nil
}

// ValidateConfig validates the configuration using existing validation logic
func (m *Manager) ValidateConfig(config *Config) error {
	return config.Validate()
}

// ReloadConfig reloads configuration from file
func (m *Manager) ReloadConfig() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Read configuration file directly with yaml.Unmarshal to preserve map keys with dots (e.g. regex patterns in custom_scores)
	config := DefaultConfig()
	data, err := os.ReadFile(m.configFile)
	if err != nil {
		return fmt.Errorf("error reading config file %s: %w", m.configFile, err)
	}

	if err := yaml.Unmarshal(data, config); err != nil {
		return fmt.Errorf("error unmarshaling config %s: %w", m.configFile, err)
	}

	// Keep viper in sync for any components that query viper
	viper.SetConfigFile(m.configFile)
	_ = viper.ReadInConfig()
	warnUnknownConfigKeys()

	// Ensure *bool pointers are not nil after unmarshal (viper may leave them nil if not set in YAML)
	if config.Fuse.Enabled == nil {
		defaultEnabled := false
		config.Fuse.Enabled = &defaultEnabled
	}

	// Migrate: infer mount_type from legacy enabled flags if not set
	if config.MountType == "" {
		if config.RClone.MountEnabled != nil && *config.RClone.MountEnabled {
			config.MountType = MountTypeRClone
		} else if config.Fuse.Enabled != nil && *config.Fuse.Enabled {
			config.MountType = MountTypeFuse
		} else if config.RClone.RCEnabled != nil && *config.RClone.RCEnabled {
			config.MountType = MountTypeRCloneExternal
		} else {
			config.MountType = MountTypeNone
		}
	}

	// Migrate: fold legacy stuck/allowlist cleanup config into the unified rules.
	migrateArrsCleanup(config)
	migrateGlobalUserAgent(config)
	migrateHealthSweepConcurrency(config)
	migrateProviderIDs(config)

	// Validate configuration
	if err := config.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	m.current = config
	return nil
}

// SaveConfig saves the current configuration to file
func (m *Manager) SaveConfig() error {
	m.mutex.RLock()
	config := m.current
	m.mutex.RUnlock()

	if config == nil {
		return fmt.Errorf("no configuration to save")
	}

	return SaveToFile(config, m.configFile)
}

// NeedsLibrarySync returns whether a library sync is needed due to configuration changes
func (m *Manager) NeedsLibrarySync() bool {
	m.librarySyncMutex.RLock()
	defer m.librarySyncMutex.RUnlock()
	return m.needsLibrarySync
}

// GetPreviousMountPath returns the previous mount path before the last change
func (m *Manager) GetPreviousMountPath() string {
	m.librarySyncMutex.RLock()
	defer m.librarySyncMutex.RUnlock()
	return m.previousMountPath
}

// ClearLibrarySyncFlag clears the library sync needed flag
func (m *Manager) ClearLibrarySyncFlag() {
	m.librarySyncMutex.Lock()
	defer m.librarySyncMutex.Unlock()
	m.needsLibrarySync = false
	m.previousMountPath = ""
}

// isRunningInDocker detects if the application is running inside a Docker container
func isRunningInDocker() bool {
	// Check for the presence of /.dockerenv file (most reliable method)
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}

	// Fallback: check /proc/self/cgroup for container indicators
	if data, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		content := string(data)
		// Look for Docker container indicators in cgroup
		if strings.Contains(content, "/docker/") ||
			strings.Contains(content, "/docker-") ||
			strings.Contains(content, ".scope") {
			return true
		}
	}

	return false
}

// DefaultConfig returns a config with default values
// If configDir is provided, it will be used for database and log file paths
func DefaultConfig(configDir ...string) *Config {
	healthEnabled := false           // Health system disabled by default
	cleanupOrphanedMetadata := false // Cleanup orphaned metadata disabled by default
	resolveRepairOnImport := false   // Disable smart replacement detection by default
	vfsEnabled := false
	mountEnabled := false // Disabled by default
	sabnzbdEnabled := false
	scrapperEnabled := false
	fuseEnabled := false
	loginRequired := true   // Require login by default
	stremioEnabled := false // Stremio endpoint disabled by default
	stremioFastFailHeaderOnly := true
	prowlarrEnabled := false        // Prowlarr integration disabled by default
	watchIntervalSeconds := 10      // Default watch interval
	failedItemRetentionHours := 24  // Default: auto-remove failed items after 24 hours
	historyRetentionDays := 90      // Default: auto-remove import history after 90 days (3 months)
	isoAnalyzeTimeoutSeconds := 120 // Default: 120s hard cap per ISO analyse (prevents stuck NNTP from stalling import for 9+ minutes)
	metadataBackupEnabled := false
	failureMaskingEnabled := false
	repairEnabled := true
	repairExponentialBackoff := true
	par2RepairEnabled := false   // beta: opt-in until the feature settles
	par2RepairOnImport := false  // opt-in: each repair costs a full release download
	par2ArrFirst := true         // prefer ARR replacement; PAR2 only when the ARRs come up empty
	importVerifyContent := false // Content verification disabled by default (destructive if misfired)
	importVerifyContentTimeoutSeconds := defaultVerifyContentTimeoutSeconds
	healthVerifyContent := false // Content verification disabled by default (destructive if misfired)
	healthVerifyContentTimeoutSeconds := defaultVerifyContentTimeoutSeconds
	healthCorruptedRetentionDays := 0

	// Set paths based on whether we're running in Docker or have a specific config directory
	var dbPath, metadataPath, logPath, rclonePath, cachePath, backupPath string

	// If a config directory is provided, use it
	if len(configDir) > 0 && configDir[0] != "" {
		dbPath = filepath.Join(configDir[0], "altmount.db")
		metadataPath = filepath.Join(configDir[0], "metadata")
		logPath = filepath.Join(configDir[0], "altmount.log")
		rclonePath = configDir[0]
		cachePath = filepath.Join(configDir[0], "cache")
		backupPath = filepath.Join(configDir[0], "backups")
	} else if isRunningInDocker() {
		dbPath = "/config/altmount.db"
		metadataPath = "/metadata"
		logPath = "/config/altmount.log"
		rclonePath = "/config"
		cachePath = "/config/cache"
		backupPath = "/config/backups"
	} else {
		dbPath = "./altmount.db"
		metadataPath = "./metadata"
		logPath = "./altmount.log"
		rclonePath = "."
		cachePath = "./cache"
		backupPath = "./backups"
	}

	return &Config{
		WebDAV: WebDAVConfig{
			Port:     8080,
			User:     "usenet",
			Password: "usenet",
		},
		API: APIConfig{
			Prefix: "/api",
		},
		Stremio: StremioConfig{
			Enabled:               &stremioEnabled,
			NzbTTLHours:           24,
			FailedReleaseTTLHours: 24,
			MaxFallbackReleases:   2,
			FastFailHeaderOnly:    &stremioFastFailHeaderOnly,
			Prowlarr: ProwlarrConfig{
				Enabled:    &prowlarrEnabled,
				Host:       "http://localhost:9696",
				Categories: []int{2000, 2010, 2030, 2040, 2045, 2060, 5000, 5010, 5030, 5040},
			},
		},
		Auth: AuthConfig{
			LoginRequired: &loginRequired,
		},
		Database: DatabaseConfig{
			Type: "sqlite",
			Path: dbPath,
		},
		Metadata: MetadataConfig{
			RootPath: metadataPath,
			Backup: MetadataBackupConfig{
				Enabled:     &metadataBackupEnabled,
				Schedule:    "0 3 * * *", // daily at 3 AM UTC
				KeepBackups: 10,
				Path:        backupPath,
			},
			Migration: MetadataMigrationConfig{
				DefaultGroup: "alt.binaries.misc",
			},
		},
		Streaming: StreamingConfig{
			MaxPrefetch:        60,  // Default: 60 segments prefetched ahead
			ReadTimeoutSeconds: 120, // Default: bound a single read at 2 minutes
			FailureMasking: FailureMaskingConfig{
				Enabled:   &failureMaskingEnabled,
				Threshold: 3,
			},
		},
		RClone: RCloneConfig{
			Path:         rclonePath,
			Password:     "",
			Salt:         "",
			RCEnabled:    &vfsEnabled, // Using vfsEnabled var for backward compatibility
			RCUrl:        "",
			RCUser:       "admin",
			RCPass:       "admin",
			RCPort:       5573, // Changed from 5572 to match your command
			VFSName:      MountProvider,
			MountEnabled: &mountEnabled,
			MountOptions: map[string]string{},

			// Mount Configuration defaults - matching your command
			LogLevel:    "INFO",
			UID:         1000,
			GID:         1000,
			Umask:       "002", // Changed from 0022 to match --umask=002
			BufferSize:  "32M", // Changed from 10M to match --buffer-size=32M
			AttrTimeout: "1s",
			Transfers:   4,
			Timeout:     "10m", // New field matching --timeout=10m

			// Mount-Specific Settings - matching your command
			AllowOther:    true,  // --allow-other
			AllowNonEmpty: true,  // --allow-non-empty
			ReadOnly:      false, // Not specified in your command, so false
			Syslog:        true,  // --syslog

			// Matches the previous hard-coded behaviour: probes run every 30s and
			// three consecutive failures triggered a restart.
			RcdRestartAfter: "90s",

			// VFS Cache Settings - matching your command
			CacheDir:              cachePath, // VFS cache directory (defaults to <rclone_path>/cache)
			VFSCacheMode:          "full",    // --vfs-cache-mode=full
			VFSCacheMaxSize:       "50G",     // --vfs-cache-max-size=50G (changed from 100G)
			VFSCacheMaxAge:        "504h",    // --vfs-cache-max-age=504h (changed from 100h)
			VFSCachePollInterval:  "1m",      // --vfs-cache-poll-interval=1m
			VFSReadChunkSize:      "32M",     // --vfs-read-chunk-size=32M (changed from 128M)
			VFSReadChunkSizeLimit: "2G",      // --vfs-read-chunk-size-limit=2G
			VFSReadAhead:          "128M",    // --vfs-read-ahead=128M (changed from 128k)
			DirCacheTime:          "10m",     // --dir-cache-time=10m (changed from 5m)

			// Additional VFS Settings (not specified in your command, using sensible defaults)
			VFSCacheMinFreeSpace: "1G",
			VFSDiskSpaceTotal:    "1G",
			VFSReadChunkStreams:  4,
		},
		Import: ImportConfig{
			MaxProcessorWorkers:            2, // Default: 2 processor workers
			QueueProcessingIntervalSeconds: 5, // Default: check for work every 5 seconds
			AllowedFileExtensions: []string{ // Default: common media extensions
				".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".vob", ".m4v", ".mov", ".wmv", ".mpg", ".mpeg",
				".xvid", ".rm", ".rmvb", ".asf", ".asx", ".wtv", ".mk3d", ".dvr-ms",
				".mp3", ".flac", ".m4a", ".epub", ".pdf", ".cbz",
			},
			MaxDownloadPrefetch:         DefaultMaxDownloadPrefetch, // Segments prefetched ahead for archive analysis
			SegmentSamplePercentage:     1,                          // Default: 1% segment sampling
			ReadTimeoutSeconds:          300,                        // Default: 5 minutes read timeout
			IsoAnalyzeTimeoutSeconds:    &isoAnalyzeTimeoutSeconds,
			ImportStrategy:              ImportStrategyNone, // Default: no import strategy (direct import)
			ImportDir:                   nil,                // No default import directory
			WatchDir:                    nil,
			WatchIntervalSeconds:        &watchIntervalSeconds,
			FailedItemRetentionHours:    &failedItemRetentionHours,
			HistoryRetentionDays:        &historyRetentionDays,
			VerifyContent:               &importVerifyContent,               // Disabled by default
			VerifyContentTimeoutSeconds: &importVerifyContentTimeoutSeconds, // Default: 15s per-file content probe timeout
		},
		Log: LogConfig{
			File:       logPath, // Default log file path
			Level:      "info",  // Default log level
			MaxSize:    100,     // 100MB max size
			MaxAge:     30,      // Keep for 30 days
			MaxBackups: 10,      // Keep 10 old files
			Compress:   true,    // Compress old files
		},
		Health: HealthConfig{
			Enabled:                             &healthEnabled,           // Disabled by default
			CleanupOrphanedMetadata:             &cleanupOrphanedMetadata, // Disabled by default
			CheckIntervalSeconds:                5,
			CheckBatchSize:                      50,
			MaxConcurrentJobs:                   1,                                  // Default: 1 concurrent job
			SegmentSamplePercentage:             5,                                  // Default: 5% segment sampling
			LibrarySyncIntervalMinutes:          360,                                // Default: sync every 6 hours
			ResolveRepairOnImport:               &resolveRepairOnImport,             // Enabled by default
			AcceptableMissingSegmentsPercentage: 2,                                  // Default: tolerate up to 2% missing segments
			VerifyContent:                       &healthVerifyContent,               // Disabled by default
			VerifyContentTimeoutSeconds:         &healthVerifyContentTimeoutSeconds, // Default: 15s per-file content probe timeout
			CorruptedRetentionDays:              &healthCorruptedRetentionDays,      // Default: keep corrupted safety copies forever
			Repair: RepairConfig{
				Enabled:                  &repairEnabled,
				IntervalMinutes:          60,
				MaxCoolDownHours:         24,
				ExponentialBackoff:       &repairExponentialBackoff,
				AutoSearchWaitSeconds:    120,
				FileDeleteConfirmSeconds: 15,
			},
		},
		Par2Repair: Par2RepairConfig{
			Enabled:           &par2RepairEnabled,
			MaxRepairRatio:    0.02, // matches the holes padding byte-ratio cap
			MaxMemoryMB:       256,
			MaxConcurrentJobs: 1,
			MaxConnections:    10,
			MaxPatchStoreMB:   0, // unlimited by default
			ArrFirst:          &par2ArrFirst,
			RepairOnImport:    &par2RepairOnImport,
		},
		SABnzbd: SABnzbdConfig{
			Enabled:               &sabnzbdEnabled,
			CompleteDir:           "/complete",
			DownloadClientBaseURL: "",
			Categories: []SABnzbdCategory{
				{
					Name:     "Movies",
					Order:    1,
					Priority: 0,
				},
				{
					Name:     "TV",
					Order:    1,
					Priority: 1,
				},
				{
					Name:     "Music",
					Order:    1,
					Priority: 2,
				},
				{
					Name:     "Books",
					Order:    1,
					Priority: 3,
				},
				{
					Name:     "Adult",
					Order:    1,
					Priority: 4,
				},
			},
			FallbackHost:            "",
			FallbackAPIKey:          "",
			HistoryRetentionMinutes: 10080,
		},
		Providers: []ProviderConfig{},
		UserAgent: DefaultUserAgent,
		Arrs: ArrsConfig{
			Enabled:                        &scrapperEnabled, // Disabled by default
			MaxWorkers:                     5,                // Default to 5 concurrent workers
			WebhookBaseURL:                 "",
			RadarrInstances:                []ArrsInstanceConfig{},
			SonarrInstances:                []ArrsInstanceConfig{},
			LidarrInstances:                []ArrsInstanceConfig{},
			ReadarrInstances:               []ArrsInstanceConfig{},
			WhisparrInstances:              []ArrsInstanceConfig{},
			SportarrInstances:              []ArrsInstanceConfig{},
			QueueCleanupGracePeriodMinutes: 5, // Default to 5 minutes stuck before acting
			QueueCleanupMaxFailures:        0, // Failure circuit breaker disabled by default
			// Rule table modeled on wArrden's queue cleanup. Action decides what to do:
			// blocklist_search (bad release → block + re-search), blocklist (block but
			// don't search), or remove (just clear the queue: transient/environmental
			// errors or files that are already satisfied).
			QueueCleanupRules: []StuckCleanupRule{
				// Bad release — blocklist and search for a replacement.
				{Message: "Sample", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "Unable to determine if file is a sample", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "is not a valid video file", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "No files found are eligible", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "No audio tracks detected", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "Found archive file", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "Unable to parse file", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "Unexpected error processing file", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "unsupported extension", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "potentially dangerous file", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "Found executable file", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "was not found in the grabbed release", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "Invalid season or episode", Enabled: true, Action: StuckActionBlocklistSearch},
				{Message: "One or more episodes expected in this release were not imported or missing", Enabled: true, Action: StuckActionBlocklistSearch},
				// Already satisfied — remove from queue only (don't re-search).
				{Message: "Not a Custom Format upgrade", Enabled: true, Action: StuckActionRemove},
				{Message: "Not an upgrade for existing", Enabled: true, Action: StuckActionRemove},
				{Message: "Not a quality revision upgrade", Enabled: true, Action: StuckActionRemove},
				{Message: "Movie file already imported", Enabled: true, Action: StuckActionRemove},
				{Message: "Episode file already imported", Enabled: true, Action: StuckActionRemove},
				{Message: "Album already imported", Enabled: true, Action: StuckActionRemove},
				// Transient/environmental — disabled by default (enable if desired).
				{Message: "Not enough free space", Enabled: false, Action: StuckActionRemove},
				{Message: "File is still being unpacked", Enabled: false, Action: StuckActionRemove},
				{Message: "Locked file, try again later", Enabled: false, Action: StuckActionRemove},
				{Message: "is reporting an error", Enabled: false, Action: StuckActionRemove},
				{Message: "Import failed, path does not exist", Enabled: false, Action: StuckActionRemove},
				// Folded from the former queue-cleanup allowlist — remove from queue only.
				// ("Sample file" is intentionally omitted: the "Sample" rule above already
				// substring-matches it.)
				{Message: "No video files were found in the selected folder", Enabled: true, Action: StuckActionRemove},
				{Message: "Could not find file", Enabled: true, Action: StuckActionRemove},
				{Message: "Download doesn't contain intermediate path", Enabled: true, Action: StuckActionRemove},
				// Folded from the former "Import Failure Cleanup" toggle (cleanup_automatic_import_failure).
				// Seeded disabled to match the toggle's default-off behavior, but discoverable so
				// users can switch it on (and pick blocklist/blocklist_search if they prefer). A
				// migrated config that had the toggle enabled gets this rule enabled automatically
				// (see migrateArrsCleanup).
				{Message: "automatic import is not possible", Enabled: false, Action: StuckActionRemove},
			},
		},
		Fuse: FuseConfig{
			Enabled:               &fuseEnabled,
			MountPath:             "",
			AllowOther:            true,
			Debug:                 false,
			AttrTimeoutSeconds:    30,
			EntryTimeoutSeconds:   1,
			MaxCacheSizeMB:        128,
			MaxReadAheadMB:        128,
			AsyncBufferSizeMB:     16,  // 16MB per-stream read-ahead (rclone-VFS-like smoothing)
			AsyncBufferMaxTotalMB: 256, // cap total read-ahead memory across all streams
		},
		MountPath: "",            // Empty by default - required when ARRs is enabled
		MountType: MountTypeNone, // No mount system active by default
	}
}

// SaveToFile saves a configuration to a YAML file
func SaveToFile(config *Config, filename string) error {
	if filename == "" {
		return fmt.Errorf("no config file path provided")
	}

	// Ensure the directory exists
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Marshal config to YAML
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// LoadConfig loads configuration from file and merges with defaults
// warnUnknownConfigKeys logs config keys that do not map to any field on Config.
// These are almost always settings removed or renamed in a past release: viper
// ignores them silently, so a stale key looks live while having no effect.
//
// This warns rather than fails — rejecting unknown keys would break upgrades for
// anyone whose config still carries a retired setting.
func warnUnknownConfigKeys() {
	probe := DefaultConfig()
	err := viper.Unmarshal(probe, func(dc *mapstructure.DecoderConfig) {
		dc.ErrorUnused = true
	})
	if err != nil && !strings.Contains(err.Error(), "custom_scores") {
		slog.Warn("Configuration contains keys this version does not use; they have no effect and can be removed",
			"detail", err)
	}
}

func LoadConfig(configFile string) (*Config, error) {
	config := DefaultConfig()

	var targetConfigFile string
	if configFile != "" {
		viper.SetConfigFile(configFile)
		targetConfigFile = configFile
	} else {
		// Look for config file in common locations
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		targetConfigFile = "config.yaml"
	}

	// Read the configuration file
	if err := viper.ReadInConfig(); err != nil {
		// Check if it's a file not found error
		if os.IsNotExist(err) || strings.Contains(err.Error(), "no such file") {
			// Create default config file with paths relative to config directory
			configDir := filepath.Dir(targetConfigFile)
			configForSave := DefaultConfig(configDir)
			if err := SaveToFile(configForSave, targetConfigFile); err != nil {
				return nil, fmt.Errorf("failed to create default config file %s: %w", targetConfigFile, err)
			}

			// Log that we created a default config
			slog.Info("Created default configuration file — please review and modify as needed", "path", targetConfigFile)

			// Now try to read the newly created file
			viper.SetConfigFile(targetConfigFile)
			if err := viper.ReadInConfig(); err != nil {
				return nil, fmt.Errorf("error reading newly created config file %s: %w", targetConfigFile, err)
			}
		} else {
			// Other error (permissions, syntax, etc.)
			if configFile != "" {
				return nil, fmt.Errorf("error reading config file %s: %w", configFile, err)
			}
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	warnUnknownConfigKeys()

	// Read and unmarshal YAML directly into config to preserve arbitrary map keys containing dots (e.g. custom_scores regexes)
	if data, err := os.ReadFile(targetConfigFile); err == nil {
		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("error unmarshaling yaml config %s: %w", targetConfigFile, err)
		}
	} else if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Ensure *bool pointers are not nil after unmarshal (viper may leave them nil if not set in YAML)
	if config.Fuse.Enabled == nil {
		defaultEnabled := false
		config.Fuse.Enabled = &defaultEnabled
	}

	// Migrate: infer mount_type from legacy enabled flags if not set
	if config.MountType == "" {
		if config.RClone.MountEnabled != nil && *config.RClone.MountEnabled {
			config.MountType = MountTypeRClone
		} else if config.Fuse.Enabled != nil && *config.Fuse.Enabled {
			config.MountType = MountTypeFuse
		} else if config.RClone.RCEnabled != nil && *config.RClone.RCEnabled {
			config.MountType = MountTypeRCloneExternal
		} else {
			config.MountType = MountTypeNone
		}
	}

	// Migrate: fold legacy stuck/allowlist cleanup config into the unified rules.
	migrateArrsCleanup(config)
	migrateGlobalUserAgent(config)
	migrateHealthSweepConcurrency(config)
	migrateProviderIDs(config)

	// If log file was not explicitly set in the config file and we have a specific config file path,
	// derive log file path from config file location
	if configFile != "" && !viper.IsSet("log.file") {
		configDir := filepath.Dir(configFile)
		config.Log.File = filepath.Join(configDir, "altmount.log")
	}

	// If cache_dir was not explicitly set or is empty, derive it from config file location
	if configFile != "" && (!viper.IsSet("rclone.cache_dir") || config.RClone.CacheDir == "") {
		configDir := filepath.Dir(configFile)
		config.RClone.CacheDir = filepath.Join(configDir, "cache")
	}

	// Check for PORT environment variable override
	if portEnv := os.Getenv("PORT"); portEnv != "" {
		port := 0
		_, err := fmt.Sscanf(portEnv, "%d", &port)
		if err != nil {
			return nil, fmt.Errorf("invalid PORT environment variable '%s': must be a number", portEnv)
		}
		if port <= 0 || port > 65535 {
			return nil, fmt.Errorf("invalid PORT environment variable %d: must be between 1 and 65535", port)
		}
		config.WebDAV.Port = port
		slog.Info("Using PORT from environment variable", "port", port)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}
