package pool

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v5"
)

// MissingRateWarningThreshold is the missing articles per minute rate that triggers a warning.
const MissingRateWarningThreshold = 10.0

// providerBytes24hTTL is how long the 24h per-provider volume read from the
// database is reused before another query is issued.
const providerBytes24hTTL = 30 * time.Second

// ProviderQuotaSnapshot holds the quota state for a single provider.
type ProviderQuotaSnapshot struct {
	QuotaBytes    int64     `json:"quota_bytes"`
	QuotaUsed     int64     `json:"quota_used"`
	QuotaResetAt  time.Time `json:"quota_reset_at,omitempty"`
	QuotaExceeded bool      `json:"quota_exceeded"`
}

// MetricsSnapshot represents pool metrics at a point in time with calculated values
type MetricsSnapshot struct {
	BytesDownloaded             int64                                `json:"bytes_downloaded"`
	BytesUploaded               int64                                `json:"bytes_uploaded"`
	ArticlesDownloaded          int64                                `json:"articles_downloaded"`
	ArticlesPosted              int64                                `json:"articles_posted"`
	TotalErrors                 int64                                `json:"total_errors"`
	ProviderErrors              map[string]int64                     `json:"provider_errors"`
	ProviderBytes               map[string]int64                     `json:"provider_bytes"`
	ProviderBytes24h            map[string]int64                     `json:"provider_bytes_24h"`
	ProviderStartedAt           map[string]time.Time                 `json:"provider_started_at"`
	ProviderQuotas              map[string]ProviderQuotaSnapshot     `json:"provider_quotas,omitempty"`
	DownloadSpeedBytesPerSec    float64                              `json:"download_speed_bytes_per_sec"`
	MaxDownloadSpeedBytesPerSec float64                              `json:"max_download_speed_bytes_per_sec"`
	UploadSpeedBytesPerSec      float64                              `json:"upload_speed_bytes_per_sec"`
	Timestamp                   time.Time                            `json:"timestamp"`
	StartedAt                   time.Time                            `json:"started_at"`
	ProviderMissingRates        map[string]float64                   `json:"provider_missing_rates"`
	ProviderMissingWarning      map[string]bool                      `json:"provider_missing_warning"`
	ProviderSpeeds              map[string]float64                   `json:"provider_speeds"`
}

// MetricsTracker tracks pool metrics over time and calculates rates
type MetricsTracker struct {
	pool              *nntppool.Client
	repo              StatsRepository
	mu                sync.RWMutex
	startedAt         time.Time
	samples           []metricsample
	providerIDMap     map[string]string

	sampleInterval    time.Duration
	retentionPeriod   time.Duration
	calculationWindow time.Duration // Window for speed calculations (shorter than retention for accuracy)
	maxDownloadSpeed  float64
	// Live counters
	articlesDownloaded  atomic.Int64
	articlesPosted      atomic.Int64
	liveBytesDownloaded atomic.Int64
	// Persistent counters (loaded from DB on start)
	initialBytesDownloaded    int64
	initialArticlesDownloaded int64
	initialBytesUploaded      int64
	initialArticlesPosted     int64
	initialProviderErrors     map[string]int64
	initialProviderBytes      map[string]int64
	initialProviderStartedAt  map[string]time.Time
	lastSavedBytesDownloaded  int64
	lastSavedProviderBytes    map[string]int64
	persistenceThreshold      int64 // Bytes to download before forcing a save
	cancel                    context.CancelFunc
	wg                        sync.WaitGroup
	logger                    *slog.Logger

	// 24h provider volume cache, guarded by its own mutex so the database read
	// never happens under mu.
	bytes24hMu       sync.Mutex
	bytes24hCache    map[string]int64
	bytes24hFetched  time.Time
	bytes24hInflight bool

	// statsFn overrides the live pool counters; only set by tests.
	statsFn func() nntppool.ClientStats
}

// metricsample represents a single metrics sample at a point in time
type metricsample struct {
	avgSpeed        float64
	totalBytes      int64
	totalErrors     int64
	providerErrors  map[string]int64
	providerMissing map[string]int64
	timestamp       time.Time
}

// NewMetricsTracker creates a new metrics tracker
func NewMetricsTracker(pool *nntppool.Client, repo StatsRepository) *MetricsTracker {
	mt := &MetricsTracker{
		pool:                  pool,
		repo:                  repo,
		samples:               make([]metricsample, 0, 60), // Preallocate for 60 samples
		initialProviderErrors: make(map[string]int64),
		initialProviderBytes:  make(map[string]int64),
		initialProviderStartedAt: make(map[string]time.Time),
		lastSavedProviderBytes: make(map[string]int64),
		sampleInterval:        2 * time.Second, // Match playback sampling for "live" feel
		retentionPeriod:       60 * time.Second,
		calculationWindow:     10 * time.Second,   // Use 10s window for more accurate real-time speeds
		persistenceThreshold:  1024 * 1024 * 1024, // Save every 1GB downloaded
		startedAt:             time.Now(),
		logger:                slog.Default().With("component", "metrics-tracker"),
	}

	return mt
}

// poolStats reads the pool's cumulative counters. These are lifetime counters
// that the pool itself cannot reset, so every reset path offsets them instead.
func (mt *MetricsTracker) poolStats() nntppool.ClientStats {
	if mt.statsFn != nil {
		return mt.statsFn()
	}
	if mt.pool == nil {
		return nntppool.ClientStats{}
	}
	return mt.pool.Stats()
}

// providerBytes24h returns the last 24h of per-provider volume, refreshing from
// the database at most once per providerBytes24hTTL. It must be called without
// holding mu: a slow query here would otherwise stall sampling and every caller
// of GetSnapshot. While a refresh is in flight, other callers get the previous
// value rather than queueing behind the database.
func (mt *MetricsTracker) providerBytes24h(ctx context.Context) map[string]int64 {
	if mt.repo == nil {
		return make(map[string]int64)
	}

	mt.bytes24hMu.Lock()
	fresh := mt.bytes24hCache != nil && time.Since(mt.bytes24hFetched) < providerBytes24hTTL
	if fresh || mt.bytes24hInflight {
		cached := copyProviderErrors(mt.bytes24hCache)
		mt.bytes24hMu.Unlock()
		if cached == nil {
			return make(map[string]int64)
		}
		return cached
	}
	mt.bytes24hInflight = true
	mt.bytes24hMu.Unlock()

	stats24h, err := mt.repo.GetProviderHourlyStats(ctx, 24)

	mt.bytes24hMu.Lock()
	defer mt.bytes24hMu.Unlock()
	mt.bytes24hInflight = false
	if err != nil {
		mt.logger.ErrorContext(ctx, "Failed to load 24h provider stats", "error", err)
	} else {
		mt.bytes24hCache = stats24h
		mt.bytes24hFetched = time.Now()
	}

	cached := copyProviderErrors(mt.bytes24hCache)
	if cached == nil {
		return make(map[string]int64)
	}
	return cached
}

// Start begins collecting metrics samples
func (mt *MetricsTracker) Start(ctx context.Context) {
	childCtx, cancel := context.WithCancel(ctx)
	mt.cancel = cancel

	// Load initial stats from DB
	if mt.repo != nil {
		stats, err := mt.repo.GetSystemStats(ctx)
		if err != nil {
			mt.logger.ErrorContext(ctx, "Failed to load system stats from database", "error", err)
		} else {
			mt.mu.Lock()
			mt.initialBytesDownloaded = stats["bytes_downloaded"]
			mt.initialArticlesDownloaded = stats["articles_downloaded"]
			mt.initialBytesUploaded = stats["bytes_uploaded"]
			mt.initialArticlesPosted = stats["articles_posted"]
			mt.maxDownloadSpeed = float64(stats["max_download_speed"])
			mt.lastSavedBytesDownloaded = mt.initialBytesDownloaded

			// 1. Load provider stats (prefixed with provider_error: or provider_bytes: or provider_started_at:)
			for k, v := range stats {
				if after, ok := strings.CutPrefix(k, "provider_error:"); ok {
					providerID := after
					mt.initialProviderErrors[providerID] = v
				} else if after, ok := strings.CutPrefix(k, "provider_bytes:"); ok {
					providerID := after
					mt.initialProviderBytes[providerID] = v
					mt.lastSavedProviderBytes[providerID] = v
				} else if after, ok := strings.CutPrefix(k, "provider_started_at:"); ok {
					providerID := after
					mt.initialProviderStartedAt[providerID] = time.Unix(v, 0)
				}
			}

			// 2. Load and verify global started_at date
			oldest, _ := mt.repo.GetOldestStatDate(ctx)
			if startedAtUnix := stats["started_at"]; startedAtUnix > 0 {
				savedStartedAt := time.Unix(startedAtUnix, 0)
				// If our saved date is more recent than our actual history,
				// correct it to the oldest record found.
				if oldest.Before(savedStartedAt) {
					mt.startedAt = oldest
				} else {
					mt.startedAt = savedStartedAt
				}
			} else {
				// Fallback to the oldest record in daily stats
				mt.startedAt = oldest
			}

			// 3. Verify per-provider started_at dates against history (check provider_hourly_stats)
			providerOldest, err := mt.repo.GetOldestProviderStatDates(ctx)
			if err == nil {
				for pid, oldestDate := range providerOldest {
					savedDate, exists := mt.initialProviderStartedAt[pid]
					// If no date exists OR if the saved date is more recent than actual history
					if !exists || oldestDate.Before(savedDate) {
						mt.initialProviderStartedAt[pid] = oldestDate
					}
				}
			}

			// 4. Trigger a save if any date was initialized or corrected
			go mt.saveStats(ctx)

			mt.mu.Unlock()

			mt.logger.InfoContext(ctx, "Loaded persistent system stats",
				"articles", mt.initialArticlesDownloaded,
				"bytes", mt.initialBytesDownloaded,
				"provider_errors", len(mt.initialProviderErrors),
				"provider_bytes", len(mt.initialProviderBytes))
		}
	}

	// Take initial sample
	mt.takeSample()

	// Start sampling goroutine
	mt.wg.Add(1)
	go mt.samplingLoop(childCtx)

	mt.logger.InfoContext(ctx, "Metrics tracker started",
		"sample_interval", mt.sampleInterval,
		"retention_period", mt.retentionPeriod,
	)
}

// Stop stops collecting metrics samples
func (mt *MetricsTracker) Stop() {
	if mt.cancel != nil {
		mt.cancel()
		mt.wg.Wait()
		mt.logger.InfoContext(context.Background(), "Metrics tracker stopped")
	}
}

// GetSnapshot returns the current metrics with calculated speeds
func (mt *MetricsTracker) GetSnapshot() MetricsSnapshot {
	providerBytes24h := mt.providerBytes24h(context.Background())

	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.getSnapshot(time.Now(), mt.poolStats(), providerBytes24h)
}

func (mt *MetricsTracker) getSnapshot(now time.Time, stats nntppool.ClientStats, providerBytes24h map[string]int64) MetricsSnapshot {
	// Calculate total errors and provider errors/bytes from v4 stats
	var totalErrors int64
	providerErrors := make(map[string]int64)
	providerBytes := make(map[string]int64)
	for _, ps := range stats.Providers {
		totalErrors += ps.Errors
		providerErrors[ps.Name] = ps.Errors
		providerBytes[ps.Name] = ps.BytesConsumed
	}

	// Use live counter for speed and totals
	bytesDownloaded := mt.liveBytesDownloaded.Load()

	// Calculate windowed download speed
	downloadSpeed := 0.0
	if len(mt.samples) > 0 {
		// Find the sample closest to calculationWindow ago
		cutoff := now.Add(-mt.calculationWindow)
		var referenceSample *metricsample
		for i := len(mt.samples) - 1; i >= 0; i-- {
			if mt.samples[i].timestamp.Before(cutoff) {
				referenceSample = &mt.samples[i]
				break
			}
		}

		if referenceSample != nil {
			bytesDiff := bytesDownloaded - referenceSample.totalBytes
			duration := now.Sub(referenceSample.timestamp).Seconds()
			// Only calculate speed if we have a significant duration to avoid spikes
			if duration >= 2.0 && bytesDiff >= 0 {
				downloadSpeed = float64(bytesDiff) / duration
			}
		} else {
			// Fallback if we don't have enough samples yet
			// Use the oldest sample
			oldest := mt.samples[0]
			bytesDiff := bytesDownloaded - oldest.totalBytes
			duration := now.Sub(oldest.timestamp).Seconds()
			// Only calculate speed if we have a significant duration to avoid spikes
			if duration >= 2.0 && bytesDiff >= 0 {
				downloadSpeed = float64(bytesDiff) / duration
			}
		}
	}

	// Update max speed
	if downloadSpeed > mt.maxDownloadSpeed {
		mt.maxDownloadSpeed = downloadSpeed
	}

	// Merge provider errors and bytes
	mergedProviderErrors := make(map[string]int64)
	maps.Copy(mergedProviderErrors, mt.initialProviderErrors)
	for k, v := range providerErrors {
		mergedProviderErrors[k] += v
	}

	mergedProviderBytes := make(map[string]int64)
	maps.Copy(mergedProviderBytes, mt.initialProviderBytes)
	for k, v := range providerBytes {
		mergedProviderBytes[k] += v
	}

	mergedProviderStartedAt := make(map[string]time.Time)
	maps.Copy(mergedProviderStartedAt, mt.initialProviderStartedAt)
	for k := range mergedProviderBytes {
		if _, ok := mergedProviderStartedAt[k]; !ok {
			mergedProviderStartedAt[k] = now
		}
	}

	// Compute windowed missing article rates per provider
	missingRates := make(map[string]float64)
	missingWarning := make(map[string]bool)

	// Collect current missing counts from pool stats
	currentMissing := make(map[string]int64)
	for _, ps := range stats.Providers {
		currentMissing[ps.Name] = ps.Missing
	}

	// Find the oldest sample within the calculation window
	windowStart := now.Add(-mt.calculationWindow)
	var oldestSample *metricsample
	for i := range mt.samples {
		if mt.samples[i].timestamp.After(windowStart) || mt.samples[i].timestamp.Equal(windowStart) {
			oldestSample = &mt.samples[i]
			break
		}
	}

	if oldestSample != nil {
		elapsed := now.Sub(oldestSample.timestamp)
		if elapsed > 0 {
			elapsedMinutes := elapsed.Minutes()
			for name, current := range currentMissing {
				old := oldestSample.providerMissing[name]
				delta := current - old
				if delta > 0 && elapsedMinutes > 0 {
					rate := float64(delta) / elapsedMinutes
					missingRates[name] = rate
					missingWarning[name] = rate >= MissingRateWarningThreshold
				}
			}
		}
	}

	// Collect per-provider quota snapshots
	var providerQuotas map[string]ProviderQuotaSnapshot
	for _, ps := range stats.Providers {
		if ps.QuotaBytes > 0 {
			if providerQuotas == nil {
				providerQuotas = make(map[string]ProviderQuotaSnapshot)
			}
			providerQuotas[ps.Name] = ProviderQuotaSnapshot{
				QuotaBytes:    ps.QuotaBytes,
				QuotaUsed:     ps.QuotaUsed,
				QuotaResetAt:  ps.QuotaResetAt,
				QuotaExceeded: ps.QuotaExceeded,
			}
		}
	}

	if providerBytes24h == nil {
		providerBytes24h = make(map[string]int64)
	}

	// Compute per-provider speeds
	providerSpeeds := make(map[string]float64)
	var totalPoolAvgSpeed float64
	for _, ps := range stats.Providers {
		totalPoolAvgSpeed += ps.AvgSpeed
	}

	for _, ps := range stats.Providers {
		// Calculate proportional speed using our accurate global speed distributed by pool's relative speeds
		currentProviderSpeed := ps.AvgSpeed
		if totalPoolAvgSpeed > 0 && downloadSpeed > 0 {
			weight := ps.AvgSpeed / totalPoolAvgSpeed
			currentProviderSpeed = downloadSpeed * weight
		}
		providerSpeeds[ps.Name] = currentProviderSpeed
	}

	return MetricsSnapshot{
		BytesDownloaded:             bytesDownloaded + mt.initialBytesDownloaded,
		BytesUploaded:               mt.initialBytesUploaded,
		ArticlesDownloaded:          mt.articlesDownloaded.Load() + mt.initialArticlesDownloaded,
		ArticlesPosted:              mt.articlesPosted.Load() + mt.initialArticlesPosted,
		TotalErrors:                 totalErrors,
		ProviderErrors:              mergedProviderErrors,
		ProviderBytes:               mergedProviderBytes,
		ProviderBytes24h:            providerBytes24h,
		ProviderStartedAt:           mergedProviderStartedAt,
		ProviderQuotas:              providerQuotas,
		DownloadSpeedBytesPerSec:    downloadSpeed,
		MaxDownloadSpeedBytesPerSec: mt.maxDownloadSpeed,
		UploadSpeedBytesPerSec:      0, // v4 doesn't track uploads
		Timestamp:                   now,
		StartedAt:                   mt.startedAt,
		ProviderMissingRates:        missingRates,
		ProviderMissingWarning:      missingWarning,
		ProviderSpeeds:              providerSpeeds,
	}
}

// IncArticlesDownloaded increments the count of articles successfully downloaded
func (mt *MetricsTracker) IncArticlesDownloaded() {
	mt.articlesDownloaded.Add(1)
}

// UpdateDownloadProgress updates the live bytes downloaded counter
func (mt *MetricsTracker) UpdateDownloadProgress(id string, bytesDownloaded int64) {
	mt.liveBytesDownloaded.Add(bytesDownloaded)
}

// IncArticlesPosted increments the count of articles successfully posted
func (mt *MetricsTracker) IncArticlesPosted() {
	mt.articlesPosted.Add(1)
}

// samplingLoop periodically samples metrics
func (mt *MetricsTracker) samplingLoop(ctx context.Context) {
	defer mt.wg.Done()
	ticker := time.NewTicker(mt.sampleInterval)
	defer ticker.Stop()

	// Use a longer interval for DB updates to avoid excessive writes
	dbUpdateTicker := time.NewTicker(1 * time.Minute)
	defer dbUpdateTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final save on shutdown
			mt.saveStats(context.Background())
			return
		case <-ticker.C:
			mt.takeSample()
		case <-dbUpdateTicker.C:
			mt.saveStats(ctx)
		}
	}
}

// saveStats persists current totals to the database
func (mt *MetricsTracker) saveStats(ctx context.Context) {
	if mt.repo == nil {
		return
	}

	snapshot := mt.GetSnapshot()

	// Prepare batch update
	stats := map[string]int64{
		"bytes_downloaded":    snapshot.BytesDownloaded,
		"articles_downloaded": snapshot.ArticlesDownloaded,
		"bytes_uploaded":      snapshot.BytesUploaded,
		"articles_posted":     snapshot.ArticlesPosted,
		"max_download_speed":  int64(snapshot.MaxDownloadSpeedBytesPerSec),
		"started_at":          snapshot.StartedAt.Unix(),
	}

	// Add provider errors to batch
	for providerID, errorCount := range snapshot.ProviderErrors {
		stats["provider_error:"+providerID] = errorCount
	}

	// Add provider bytes to batch
	for providerID, byteCount := range snapshot.ProviderBytes {
		stats["provider_bytes:"+providerID] = byteCount
	}

	// Add provider started at to batch
	for providerID, startedAt := range snapshot.ProviderStartedAt {
		stats["provider_started_at:"+providerID] = startedAt.Unix()
	}

	// Persist per-provider quota state for restore across restarts
	poolStats := mt.poolStats()
	for _, ps := range poolStats.Providers {
		if ps.QuotaBytes > 0 {
			stats["quota_used:"+ps.Name] = ps.QuotaUsed
			if !ps.QuotaResetAt.IsZero() {
				stats["quota_reset_at:"+ps.Name] = ps.QuotaResetAt.UnixNano()
			}
		}
	}

	if err := mt.repo.BatchUpdateSystemStats(ctx, stats); err != nil {
		mt.logger.ErrorContext(ctx, "Failed to persist system stats", "error", err)
	} else {
		// Calculate delta for daily stats
		mt.mu.Lock()
		delta := snapshot.BytesDownloaded - mt.lastSavedBytesDownloaded
		mt.lastSavedBytesDownloaded = snapshot.BytesDownloaded

		// Calculate deltas per provider for hourly stats
		providerDeltas := make(map[string]int64)
		for providerID, currentTotal := range snapshot.ProviderBytes {
			lastTotal := mt.lastSavedProviderBytes[providerID]
			if currentTotal > lastTotal {
				providerDeltas[providerID] = currentTotal - lastTotal
				mt.lastSavedProviderBytes[providerID] = currentTotal
			}
		}
		mt.mu.Unlock()

		// Update daily stats with the delta
		if delta > 0 {
			if err := mt.repo.AddBytesDownloadedToDailyStat(ctx, delta); err != nil {
				mt.logger.ErrorContext(ctx, "Failed to update daily download volume", "error", err)
			}
		}

		// Update per-provider hourly stats
		for providerID, providerDelta := range providerDeltas {
			if err := mt.repo.AddProviderBytesToHourlyStat(ctx, providerID, providerDelta); err != nil {
				mt.logger.ErrorContext(ctx, "Failed to update provider hourly volume", "provider", providerID, "error", err)
			}
		}

		// Record current speed to history for charting (MBps)
		for poolName, speedBytes := range snapshot.ProviderSpeeds {
			if speedBytes > 1024*1024 { // Only record if speed > 1MB/s to show active performance
				speedMbps := speedBytes / (1024 * 1024)
				
				// Map pool name to config ID
				id := poolName
				mt.mu.RLock()
				if mappedID, ok := mt.providerIDMap[poolName]; ok {
					id = mappedID
				}
				mt.mu.RUnlock()

				if err := mt.repo.RecordProviderSpeedTest(ctx, id, speedMbps); err != nil {
					mt.logger.ErrorContext(ctx, "Failed to record provider speed history", "provider", id, "error", err)
				}
			}
		}
	}
}

// Reset resets cumulative metrics both in memory and in the database based on flags
func (mt *MetricsTracker) Reset(ctx context.Context, resetPeak bool, resetTotals bool) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if resetTotals {
		poolStats := mt.poolStats()

		mt.initialBytesDownloaded = 0
		mt.initialArticlesDownloaded = 0
		mt.initialBytesUploaded = 0
		mt.initialArticlesPosted = 0
		mt.articlesDownloaded.Store(0)
		mt.articlesPosted.Store(0)
		mt.liveBytesDownloaded.Store(0)
		mt.lastSavedBytesDownloaded = 0
		mt.startedAt = time.Now()
		mt.initialProviderErrors = make(map[string]int64)
		mt.initialProviderBytes = make(map[string]int64)
		mt.initialProviderStartedAt = make(map[string]time.Time)
		mt.lastSavedProviderBytes = make(map[string]int64)

		// The pool's per-provider counters are cumulative for its whole lifetime
		// and cannot be zeroed, so negate them: displayed = initial + live = 0.
		// The delta baseline has to follow that merged total, otherwise the next
		// save would charge the entire pool-lifetime volume to the current hour.
		for _, ps := range poolStats.Providers {
			mt.initialProviderBytes[ps.Name] = -ps.BytesConsumed
			mt.initialProviderErrors[ps.Name] = -ps.Errors
			mt.lastSavedProviderBytes[ps.Name] = 0
		}

		// Clear samples to reset speed calculation
		mt.samples = make([]metricsample, 0, 60)

		// Clear provider hourly stats if repository is available
		if mt.repo != nil {
			if err := mt.repo.ClearProviderHourlyStats(ctx); err != nil {
				mt.logger.ErrorContext(ctx, "Failed to clear provider hourly stats during reset", "error", err)
			}

			mt.bytes24hMu.Lock()
			mt.bytes24hCache = nil
			mt.bytes24hFetched = time.Time{}
			mt.bytes24hMu.Unlock()
		}
	}

	if resetPeak {
		mt.maxDownloadSpeed = 0
	}

	// Persist the reset state to database
	if mt.repo != nil {
		// We need to fetch all current keys to know what to reset (especially provider errors)
		currentStats, err := mt.repo.GetSystemStats(ctx)
		if err != nil {
			mt.logger.ErrorContext(ctx, "Failed to fetch stats for reset", "error", err)
		} else {
			resetMap := make(map[string]int64)

			if resetTotals {
				for k := range currentStats {
					resetMap[k] = 0
				}
				// Ensure core keys are present
				resetMap["bytes_downloaded"] = 0
				resetMap["articles_downloaded"] = 0
				resetMap["bytes_uploaded"] = 0
				resetMap["articles_posted"] = 0
			}

			if resetPeak {
				resetMap["max_download_speed"] = 0
			}

			if len(resetMap) > 0 {
				if err := mt.repo.BatchUpdateSystemStats(ctx, resetMap); err != nil {
					mt.logger.ErrorContext(ctx, "Failed to persist reset stats", "error", err)
				}
			}
		}
	}

	mt.logger.InfoContext(ctx, "Pool metrics have been reset", "reset_peak", resetPeak, "reset_totals", resetTotals)
	return nil
}

// ResetProviderErrors zeroes out all per-provider error counts by offsetting
// the live pool error counts. Bytes, speed, and history are untouched.
func (mt *MetricsTracker) ResetProviderErrors(ctx context.Context) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Negate the live error counts so that displayed = initial + live = 0.
	poolStats := mt.poolStats()
	for _, ps := range poolStats.Providers {
		mt.initialProviderErrors[ps.Name] = -ps.Errors
	}

	// Persist zeros for all provider_error:* keys in the database.
	if mt.repo != nil {
		currentStats, err := mt.repo.GetSystemStats(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch stats for provider error reset: %w", err)
		}

		resetMap := make(map[string]int64)
		for k := range currentStats {
			if strings.HasPrefix(k, "provider_error:") {
				resetMap[k] = 0
			}
		}

		if len(resetMap) > 0 {
			if err := mt.repo.BatchUpdateSystemStats(ctx, resetMap); err != nil {
				return fmt.Errorf("failed to persist provider error reset: %w", err)
			}
		}
	}

	mt.logger.InfoContext(ctx, "Provider error counts reset")
	return nil
}

// SetProviderIDs sets the mapping from pool names to config IDs
func (mt *MetricsTracker) SetProviderIDs(mapping map[string]string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.providerIDMap = mapping
}

// takeSample captures a metrics snapshot and stores it
func (mt *MetricsTracker) takeSample() {
	stats := mt.poolStats()

	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Calculate total errors, provider errors, and provider missing counts
	var totalErrors int64
	providerErrors := make(map[string]int64)
	providerMissing := make(map[string]int64)
	for _, ps := range stats.Providers {
		totalErrors += ps.Errors
		providerErrors[ps.Name] = ps.Errors
		providerMissing[ps.Name] = ps.Missing
	}

	bytesDownloaded := mt.liveBytesDownloaded.Load()

	// Create sample
	sample := metricsample{
		totalBytes:      bytesDownloaded,
		avgSpeed:        stats.AvgSpeed,
		totalErrors:     totalErrors,
		providerErrors:  copyProviderErrors(providerErrors),
		providerMissing: copyProviderErrors(providerMissing),
		timestamp:       time.Now(),
	}

	// Add sample
	mt.samples = append(mt.samples, sample)

	// Adaptive Persistence: Check if we should force a save due to high activity
	totalBytesDownloaded := bytesDownloaded + mt.initialBytesDownloaded
	if totalBytesDownloaded-mt.lastSavedBytesDownloaded >= mt.persistenceThreshold {
		// Use a non-blocking save or a shorter context?
		// For now, simple call is fine as it's a goroutine
		go mt.saveStats(context.Background())
	}

	// Clean up old samples
	mt.cleanupOldSamples()
}

// cleanupOldSamples removes samples older than the retention period
func (mt *MetricsTracker) cleanupOldSamples() {
	cutoff := time.Now().Add(-mt.retentionPeriod)

	// Find first sample to keep
	keepIndex := 0
	for i, sample := range mt.samples {
		if sample.timestamp.After(cutoff) {
			keepIndex = i
			break
		}
	}

	// Remove old samples
	if keepIndex > 0 {
		mt.samples = mt.samples[keepIndex:]
	}
}

// copyProviderErrors creates a copy of the provider errors map
func copyProviderErrors(original map[string]int64) map[string]int64 {
	if original == nil {
		return nil
	}

	copy := make(map[string]int64, len(original))
	maps.Copy(copy, original)
	return copy
}
