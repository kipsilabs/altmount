package config

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Health config accessor methods with default fallbacks.
// These methods provide safe access to health configuration values
// with sensible defaults when values are not set or invalid.

// GetCheckInterval returns the health check interval with a default fallback.
func (c *Config) GetCheckInterval() time.Duration {
	if c.Health.CheckIntervalSeconds <= 0 {
		return 5 * time.Second // Default: 5 seconds
	}
	return time.Duration(c.Health.CheckIntervalSeconds) * time.Second
}

// GetMaxConcurrentJobs returns max concurrent health check jobs with a default fallback.
func (c *Config) GetMaxConcurrentJobs() int {
	if c.Health.MaxConcurrentJobs <= 0 {
		return 1 // Default: 1 job
	}
	return c.Health.MaxConcurrentJobs
}

// GetCheckBatchSize returns how many due files each health cycle fetches and
// sweeps together in one cross-file StatMany call, with a default fallback.
// Distinct from HealthStatConcurrency: this bounds how many FILES are batched
// into one sweep, while that bounds how many segment checks run concurrently
// within a sweep.
func (c *Config) GetCheckBatchSize() int {
	if c.Health.CheckBatchSize <= 0 {
		return 50 // Default: 50 files per cycle
	}
	return c.Health.CheckBatchSize
}

// GetSegmentSamplePercentage returns segment sample percentage with a default fallback.
// Returns a value between 1 and 100.
func (c *Config) GetSegmentSamplePercentage() int {
	if c.Health.SegmentSamplePercentage < 1 || c.Health.SegmentSamplePercentage > 100 {
		return 5 // Default: 5%
	}
	return c.Health.SegmentSamplePercentage
}

// GetLibrarySyncInterval returns the library sync interval with a default fallback.
func (c *Config) GetLibrarySyncInterval() time.Duration {
	if c.Health.LibrarySyncIntervalMinutes <= 0 {
		return 60 * time.Minute // Default: 60 minutes
	}
	return time.Duration(c.Health.LibrarySyncIntervalMinutes) * time.Minute
}

// GetLibrarySyncConcurrency returns the library sync concurrency with a default fallback.
func (c *Config) GetLibrarySyncConcurrency() int {
	if c.Health.LibrarySyncConcurrency <= 0 {
		return 5 // Default: 5 concurrent operations
	}
	return c.Health.LibrarySyncConcurrency
}

// GetVerifyData returns whether to verify data during health checks.
func (c *Config) GetVerifyData() bool {
	if c.Health.VerifyData == nil {
		return false // Default: false
	}
	return *c.Health.VerifyData
}

// GetCheckAllSegments returns whether to check all segments during health checks.
func (c *Config) GetCheckAllSegments() bool {
	if c.Health.CheckAllSegments == nil {
		return false // Default: false
	}
	return *c.Health.CheckAllSegments
}

// GetHealthReadTimeout returns the health check read timeout as a duration with a default fallback.
func (c *Config) GetHealthReadTimeout() time.Duration {
	if c.Health.ReadTimeoutSeconds <= 0 {
		return 30 * time.Second // Default: 30 seconds
	}
	return time.Duration(c.Health.ReadTimeoutSeconds) * time.Second
}

// DefaultStreamReadTimeout bounds a single streaming read when
// streaming.read_timeout_seconds is unset. A healthy read finishes in seconds;
// even a pathological one is bounded by the per-segment fetch budget (15s per
// attempt, 2 attempts). Anything past this is a stall, not slowness.
const DefaultStreamReadTimeout = 120 * time.Second

// GetStreamReadTimeout returns the per-read timeout for streaming reads.
// Zero configures the default; a negative value disables the timeout entirely
// and is returned as 0.
func (c *Config) GetStreamReadTimeout() time.Duration {
	if c.Streaming.ReadTimeoutSeconds < 0 {
		return 0 // Explicitly disabled
	}
	if c.Streaming.ReadTimeoutSeconds == 0 {
		return DefaultStreamReadTimeout
	}
	return time.Duration(c.Streaming.ReadTimeoutSeconds) * time.Second
}

// GetMaxRetries returns the maximum number of health check retries.
func (c *Config) GetMaxRetries() int {
	if c.Health.MaxRetries <= 0 {
		return 2 // Default: 2 retries
	}
	return c.Health.MaxRetries
}

// GetMaxRepairRetries returns the maximum number of repair notification retries.
func (c *Config) GetMaxRepairRetries() int {
	if c.Health.Repair.MaxRepairRetries <= 0 {
		return 3 // Default: 3 retries
	}
	return c.Health.Repair.MaxRepairRetries
}

// GetAcceptableMissingSegmentsPercentage returns the missing-segment
// percentage a health check tolerates before failing a file (and triggering
// repair) rather than leaving it degraded. Defaults to 2%; set to 0 to
// disable tolerance entirely (any missing segment fails immediately), or to
// 100 to disable failing on this basis altogether (always degraded, never
// repaired for missing segments alone). The value is clamped to [0, 100].
func (c *Config) GetAcceptableMissingSegmentsPercentage() float64 {
	if c.Health.AcceptableMissingSegmentsPercentage < 0 {
		return 0
	}
	if c.Health.AcceptableMissingSegmentsPercentage > 100 {
		return 100
	}
	return c.Health.AcceptableMissingSegmentsPercentage
}

// TotalProviderConnections returns the pool's total connection capacity: the
// sum of MaxConnections across enabled, non-backup providers. When no primary
// providers are configured it falls back to the enabled backup providers' sum
// so the capacity is not zero while a usable pool exists. Returns 0 when no
// providers are configured at all.
// ProviderFingerprint names the set of enabled providers: SHA-1 over the
// sorted "host:port" list, first 8 bytes hex. Hole records are trusted only
// under the fingerprint that recorded them, because "missing" is an answer
// about particular servers.
func (c *Config) ProviderFingerprint() string {
	hosts := make([]string, 0, len(c.Providers))
	for _, p := range c.Providers {
		if p.Enabled != nil && !*p.Enabled {
			continue
		}
		hosts = append(hosts, fmt.Sprintf("%s:%d", p.Host, p.Port))
	}
	sort.Strings(hosts)
	sum := sha1.Sum([]byte(strings.Join(hosts, "\n")))
	return hex.EncodeToString(sum[:8])
}

func (c *Config) TotalProviderConnections() int {
	primary := 0
	backup := 0
	for _, p := range c.Providers {
		if p.Enabled != nil && !*p.Enabled {
			continue
		}
		if p.IsBackupProvider != nil && *p.IsBackupProvider {
			backup += p.MaxConnections
		} else {
			primary += p.MaxConnections
		}
	}
	if primary > 0 {
		return primary
	}
	return backup
}

// defaultStatInflightRequests mirrors the per-provider default applied in
// (*Manager) config normalization, so StatConcurrency reports the depth the
// pool will actually use for providers that leave the field unset.
const defaultStatInflightRequests = 100

// maxStatConcurrency caps StatConcurrency. The chunked STAT sweeps use this
// number as their chunk size, and a chunk boundary is the only place a file
// can be dropped from the remaining work — so an unbounded value would erase
// early-termination granularity and make one chunk deadline cover the entire
// sweep.
const maxStatConcurrency = 500

// StatConcurrency returns how many STATs to keep in flight during an
// availability sweep. STAT carries no body, so nntppool pipelines it down a
// connection to Provider.StatInflight depth rather than spending one
// connection per check (see NNTPConnection.inflightSem) — connection count is
// the wrong ceiling.
//
// The value is the deepest stat_inflight_requests among enabled providers, not
// the sum of connections × depth. A single provider's pipeline depth is the
// conservative read: it is what one connection alone can sustain, so the sweep
// stays within reach even when the pool is busy serving streams.
func (c *Config) StatConcurrency() int {
	depth := 0
	for _, p := range c.Providers {
		if p.Enabled != nil && !*p.Enabled {
			continue
		}
		d := p.StatInflightRequests
		if d <= 0 {
			d = defaultStatInflightRequests
		}
		if d > depth {
			depth = d
		}
	}
	if depth <= 0 {
		depth = defaultStatInflightRequests
	}
	if depth > maxStatConcurrency {
		depth = maxStatConcurrency
	}
	return depth
}

// GetMaxConcurrentImports returns the global cap on concurrent NZB imports.
// 0 means unlimited (the default).
func (c *Config) GetMaxConcurrentImports() int {
	if c.Import.MaxConcurrentImports < 0 {
		return 0
	}
	return c.Import.MaxConcurrentImports
}

// GetMaxConcurrentSegmentChecks returns the operator's hard cap on in-flight
// STAT checks during a health sweep, or 0 meaning "let the pool adapt".
func (c *Config) GetMaxConcurrentSegmentChecks() int {
	if c.Health.MaxConcurrentSegmentChecks == nil {
		return 0
	}
	if n := *c.Health.MaxConcurrentSegmentChecks; n > 0 {
		return n
	}
	return 0
}

// GetStreamHeadroomConnections returns the per-stream import reservation:
// an explicit setting when present, otherwise a value derived from the pool size.
func (c *Config) GetStreamHeadroomConnections() int {
	if c.Import.StreamHeadroomConnections != nil {
		if n := *c.Import.StreamHeadroomConnections; n > 0 {
			return n
		}
		return 0 // explicitly disabled
	}
	return DefaultStreamHeadroom(c.TotalProviderConnections())
}

const (
	// streamHeadroomFraction makes the default reservation a fixed SHARE of the
	// pool rather than a fixed count. The cost of a reservation is the fraction
	// of the pool it removes, so an absolute default cannot be right at more
	// than one pool size: 8 connections is 8% of a 100-connection pool but 80%
	// of a 10-connection one.
	streamHeadroomFraction = 4

	// minStreamHeadroom keeps a small pool's reservation meaningful without
	// letting it dominate: at 8 connections or fewer the fraction would round
	// toward nothing.
	minStreamHeadroom = 2
)

// DefaultStreamHeadroom derives the per-stream import reservation from the
// pool's connection count.
//
// On a saturated link the pool holds slack — connections that are not
// converting into bytes because the wire, not the pool, is the limit — and
// handing that slack to playback is close to free. Measured at 100 connections
// behind a 400 MB/s ceiling: a reservation of 8 cost nothing (import 357 vs
// 359 MB/s) for stream p50 190ms -> 179ms, and 32 cost 7-13% import throughput
// during playback only for p50 147ms and p99 245ms -> 170ms.
//
// A quarter of the pool sits between those and, crucially, costs the same share
// at every pool size. How much slack actually exists depends on the link rate,
// which cannot be known statically — a small pool on a fast line has little, and
// will pay something real for this. Set stream_headroom_connections explicitly
// to override, or 0 to disable.
func DefaultStreamHeadroom(connections int) int {
	h := connections / streamHeadroomFraction
	if h < minStreamHeadroom {
		return minStreamHeadroom
	}
	return h
}

// DefaultMaxDownloadPrefetch is the default number of segments to prefetch ahead
// during archive analysis and expansion. Must match the value in DefaultConfig.
const DefaultMaxDownloadPrefetch = 10

// GetMaxDownloadPrefetch returns max download prefetch with a default fallback.
func (c *Config) GetMaxDownloadPrefetch() int {
	if c.Import.MaxDownloadPrefetch <= 0 {
		return DefaultMaxDownloadPrefetch
	}
	return c.Import.MaxDownloadPrefetch
}

// GetReadTimeoutSeconds returns read timeout in seconds with a default fallback.
func (c *Config) GetReadTimeoutSeconds() int {
	if c.Import.ReadTimeoutSeconds <= 0 {
		return 30 // Default: 30 seconds
	}
	return c.Import.ReadTimeoutSeconds
}

// GetImportVerifyContent returns whether imported media files should be
// probed for a valid container signature before the import is reported
// successful.
func (c *Config) GetImportVerifyContent() bool {
	if c.Import.VerifyContent == nil {
		return false
	}
	return *c.Import.VerifyContent
}

// GetImportVerifyContentTimeout returns the per-file content probe timeout
// for import verification, defaulting to 15 seconds.
func (c *Config) GetImportVerifyContentTimeout() time.Duration {
	if c.Import.VerifyContentTimeoutSeconds == nil || *c.Import.VerifyContentTimeoutSeconds <= 0 {
		return defaultVerifyContentTimeoutSeconds * time.Second
	}
	return time.Duration(*c.Import.VerifyContentTimeoutSeconds) * time.Second
}

// GetImportSegmentCheckTimeout returns how long the import availability check
// may run. 0 (unset) leaves the choice to the validator, whose default is 45s.
func (c *Config) GetImportSegmentCheckTimeout() time.Duration {
	if c.Import.SegmentCheckTimeoutSeconds == nil || *c.Import.SegmentCheckTimeoutSeconds <= 0 {
		return 0
	}
	return time.Duration(*c.Import.SegmentCheckTimeoutSeconds) * time.Second
}

// GetHealthVerifyContent returns whether health checks should probe media
// files for a valid container signature.
func (c *Config) GetHealthVerifyContent() bool {
	if c.Health.VerifyContent == nil {
		return false
	}
	return *c.Health.VerifyContent
}

// GetHealthVerifyContentTimeout returns the per-file content probe timeout
// for health check verification, defaulting to 15 seconds.
func (c *Config) GetHealthVerifyContentTimeout() time.Duration {
	if c.Health.VerifyContentTimeoutSeconds == nil || *c.Health.VerifyContentTimeoutSeconds <= 0 {
		return defaultVerifyContentTimeoutSeconds * time.Second
	}
	return time.Duration(*c.Health.VerifyContentTimeoutSeconds) * time.Second
}

// GetIsoAnalyzeTimeout returns the per-ISO analyse deadline with a 120s
// default fallback. This bounds the entire iso.AnalyzeISO walk so a
// degraded NNTP provider cannot stall the importer indefinitely.
//
// Sentinel handling:
//   - nil (config field unset)        → 120s default
//   - 0 or negative (explicit "none") → 120s default; users cannot disable
//     the cap — the whole purpose of this knob is to prevent unbounded
//     waits. To approximate "unlimited", set a very large value (e.g.
//     86400 for a one-day budget).
func (c *Config) GetIsoAnalyzeTimeout() time.Duration {
	if c.Import.IsoAnalyzeTimeoutSeconds == nil || *c.Import.IsoAnalyzeTimeoutSeconds <= 0 {
		return 120 * time.Second
	}
	return time.Duration(*c.Import.IsoAnalyzeTimeoutSeconds) * time.Second
}

// GetMetadataBackupKeep returns the number of metadata backups to keep with a default fallback.
func (c *Config) GetMetadataBackupKeep() int {
	if c.Metadata.Backup.KeepBackups <= 0 {
		return 10 // Default: 10 backups
	}
	return c.Metadata.Backup.KeepBackups
}

// GetFuseMountPath returns the FUSE mount path, falling back to the root mount_path if not set.
func (c *Config) GetFuseMountPath() string {
	if c.Fuse.MountPath != "" {
		return c.Fuse.MountPath
	}
	return c.MountPath
}

// GetHealthEnabled returns whether health checking is enabled (defaults to true)
func (c *Config) GetHealthEnabled() bool {
	if c.Health.Enabled == nil {
		return true
	}
	return *c.Health.Enabled
}

// GetHealthExcludedCategories returns the SABnzbd category names whose files
// must not be registered for health checking by the library-sync pass.
func (c *Config) GetHealthExcludedCategories() []string {
	return c.Health.ExcludedCategories
}

// GetHealthDeleteOnCorruption reports whether confirmed corruption should delete the
// file instead of triggering an Arr repair (health.corruption_action == "delete").
func (c *Config) GetHealthDeleteOnCorruption() bool {
	return c.Health.CorruptionAction == "delete"
}

// GetRepairEnabled returns whether automatic repair is enabled (defaults to true)
func (c *Config) GetRepairEnabled() bool {
	if c.Health.Repair.Enabled == nil {
		return true
	}
	return *c.Health.Repair.Enabled
}

// GetRepairInterval returns the repair check interval
func (c *Config) GetRepairInterval() time.Duration {
	if c.Health.Repair.IntervalMinutes <= 0 {
		return 60 * time.Minute // Default: 1 hour
	}
	return time.Duration(c.Health.Repair.IntervalMinutes) * time.Minute
}

// GetRepairMaxCoolDown returns the maximum cooldown for repairs
func (c *Config) GetRepairMaxCoolDown() time.Duration {
	if c.Health.Repair.MaxCoolDownHours <= 0 {
		return 24 * time.Hour // Default: 24 hours
	}
	return time.Duration(c.Health.Repair.MaxCoolDownHours) * time.Hour
}

// GetRepairAutoSearchWait returns how long a repair may wait for the ARR's own
// automatic redownload search to finish. Zero means "do not wait".
func (c *Config) GetRepairAutoSearchWait() time.Duration {
	if c.Health.Repair.AutoSearchWaitSeconds <= 0 {
		return 0
	}
	return time.Duration(c.Health.Repair.AutoSearchWaitSeconds) * time.Second
}

// GetRepairFileDeleteConfirm returns how long a repair may wait for the ARR to
// report a deleted file record as unlinked. Zero means "do not wait".
func (c *Config) GetRepairFileDeleteConfirm() time.Duration {
	if c.Health.Repair.FileDeleteConfirmSeconds <= 0 {
		return 0
	}
	return time.Duration(c.Health.Repair.FileDeleteConfirmSeconds) * time.Second
}

// GetRepairExponentialBackoff returns whether exponential backoff is enabled for repairs
func (c *Config) GetRepairExponentialBackoff() bool {
	if c.Health.Repair.ExponentialBackoff == nil {
		return true
	}
	return *c.Health.Repair.ExponentialBackoff
}
