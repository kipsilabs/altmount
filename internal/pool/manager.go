package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/javi11/nntppool/v4"
)

// Manager provides centralized NNTP connection pool management.
type Manager interface {
	// GetPool returns the current connection pool or error if not available.
	// The returned client exposes the narrow NntpClient surface so tests can
	// substitute a fake (see internal/testsupport/fakepool). In production it
	// is backed by *nntppool.Client.
	GetPool() (NntpClient, error)

	// SetProviders creates/recreates the pool with new providers
	SetProviders(providers []nntppool.Provider) error

	// ClearPool shuts down and removes the current pool
	ClearPool() error

	// HasPool returns true if a pool is currently available
	HasPool() bool

	// GetMetrics returns the current pool metrics with calculated speeds
	GetMetrics() (MetricsSnapshot, error)

	// ResetMetrics resets specific cumulative metrics
	ResetMetrics(ctx context.Context, resetPeak bool, resetTotals bool) error

	// ResetProviderErrors zeroes all per-provider error counts without
	// affecting bytes downloaded, peak speed, or history.
	ResetProviderErrors(ctx context.Context) error

	// IncArticlesDownloaded increments the count of articles successfully downloaded
	IncArticlesDownloaded()

	// UpdateDownloadProgress updates the bytes downloaded for a specific stream
	UpdateDownloadProgress(id string, bytesDownloaded int64)

	// IncArticlesPosted increments the count of articles successfully posted
	IncArticlesPosted()

	// AddProvider adds a single provider to the running pool.
	// If no pool exists, a new one is created with this provider.
	AddProvider(provider nntppool.Provider) error

	// RemoveProvider removes a provider by its stable configuration ID.
	// If the last provider is removed, the pool is closed.
	RemoveProvider(name string) error

	// ResetProviderQuota resets the download quota counter for a provider,
	// clearing its consumed-bytes counter and exceeded flag in-place.
	ResetProviderQuota(ctx context.Context, poolName string) error

	// SetProviderIDs sets a mapping between pool names and configuration IDs.
	SetProviderIDs(mapping map[string]string)

	// AcquireImportSlot blocks until an admission slot is available for an
	// NZB import to start, or ctx is cancelled. The returned release function
	// must be called exactly once when the import has finished (success or
	// failure). When admission caps are unconfigured (both 0) it is a no-op.
	AcquireImportSlot(ctx context.Context) (release func(), err error)

	// SetAdmissionCaps configures the two import-concurrency caps:
	// capIdle applies when no stream is active; capWhileStreaming applies
	// while any stream is active. A cap of 0 means unlimited.
	SetAdmissionCaps(capIdle, capWhileStreaming int)

	// SetStreamSource wires the activity signal so admission can adapt to
	// whether any stream is currently active.
	SetStreamSource(src StreamActivitySource)

	// NotifyStreamChange must be called by the stream source whenever its
	// active stream count changes, so the admission gate can re-evaluate.
	NotifyStreamChange()
}

// StatsRepository defines the interface for persisting pool statistics
type StatsRepository interface {
	UpdateSystemStat(ctx context.Context, key string, value int64) error
	BatchUpdateSystemStats(ctx context.Context, stats map[string]int64) error
	MigrateSystemStats(ctx context.Context, stats map[string]int64, deleteKeys []string) error
	GetSystemStats(ctx context.Context) (map[string]int64, error)
	AddBytesDownloadedToDailyStat(ctx context.Context, bytes int64) error
	AddProviderBytesToHourlyStat(ctx context.Context, providerID string, bytes int64) error
	RecordProviderSpeedTest(ctx context.Context, providerID string, speedMbps float64) error
	GetProviderHourlyStats(ctx context.Context, hours int) (map[string]int64, error)
	ClearProviderHourlyStats(ctx context.Context) error
	GetOldestStatDate(ctx context.Context) (time.Time, error)
	GetOldestProviderStatDates(ctx context.Context) (map[string]time.Time, error)
}

// manager implements the Manager interface
type manager struct {
	mu               sync.RWMutex
	pool             *nntppool.Client
	metricsTracker   *MetricsTracker
	providerIDMap    map[string]string
	repo             StatsRepository
	ctx              context.Context
	logger           *slog.Logger
	quotaWatchCancel context.CancelFunc
	admission        *ImportAdmission
}

// NewManager creates a new pool manager
func NewManager(ctx context.Context, repo StatsRepository) Manager {
	return &manager{
		ctx:       ctx,
		repo:      repo,
		logger:    slog.Default().With("component", "pool"),
		admission: NewImportAdmission(),
	}
}

// providerPoolName returns the lookup key nntppool uses for a provider.
func providerPoolName(p nntppool.Provider) string {
	if p.Name != "" {
		return p.Name
	}
	return legacyProviderPoolName(p)
}

func legacyProviderPoolName(p nntppool.Provider) string {
	name := p.Host
	if p.Auth.Username != "" {
		name += "+" + p.Auth.Username
	}
	return name
}

// injectQuotaState loads persisted quota counters from the database and sets
// QuotaUsed / QuotaResetAt on each provider so nntppool can resume quota
// tracking across restarts.
func (m *manager) injectQuotaState(providers []nntppool.Provider) {
	if m.repo == nil {
		return
	}

	stats, err := m.repo.GetSystemStats(m.ctx)
	if err != nil {
		m.logger.ErrorContext(m.ctx, "Failed to load quota state from database", "error", err)
		return
	}

	// Build lookup maps from prefixed keys
	quotaUsed := make(map[string]int64)
	quotaResetAt := make(map[string]int64)
	for k, v := range stats {
		if after, ok := strings.CutPrefix(k, "quota_used:"); ok {
			quotaUsed[after] = v
		} else if after, ok := strings.CutPrefix(k, "quota_reset_at:"); ok {
			quotaResetAt[after] = v
		}
	}

	migrated := make(map[string]int64)
	legacyKeys := make(map[string]struct{})
	legacyProviderCounts := make(map[string]int)
	for i := range providers {
		name := providerPoolName(providers[i])
		legacyName := legacyProviderPoolName(providers[i])
		if name != legacyName {
			legacyProviderCounts[legacyName]++
		}
	}

	for i := range providers {
		name := providerPoolName(providers[i])
		legacyName := legacyProviderPoolName(providers[i])
		used, hasUsed := quotaUsed[name]
		resetNano, hasReset := quotaResetAt[name]
		if name != legacyName {
			if legacyProviderCounts[legacyName] == 1 {
				// Provider IDs replaced host+username keys. Import each legacy value
				// only when the new key does not already exist, so a restart cannot
				// overwrite state written after a partial migration.
				if !hasUsed {
					used, hasUsed = quotaUsed[legacyName]
					if hasUsed {
						migrated["quota_used:"+name] = used
					}
				}
				if !hasReset {
					resetNano, hasReset = quotaResetAt[legacyName]
					if hasReset {
						migrated["quota_reset_at:"+name] = resetNano
					}
				}
				if _, ok := quotaUsed[legacyName]; ok {
					legacyKeys["quota_used:"+legacyName] = struct{}{}
				}
				if _, ok := quotaResetAt[legacyName]; ok {
					legacyKeys["quota_reset_at:"+legacyName] = struct{}{}
				}
			}
		}

		hasResetTime := false
		var resetTime time.Time
		if hasReset && resetNano > 0 {
			resetTime = time.Unix(0, resetNano)
			hasResetTime = true
		}

		if hasResetTime {
			if resetTime.After(time.Now()) {
				// The quota period is still active: restore the used bytes and the reset time
				if hasUsed && used > 0 {
					providers[i].QuotaUsed = used
				}
				providers[i].QuotaResetAt = resetTime
			} else {
				// The quota period has already expired: discard the old usage (it resets to 0)
				providers[i].QuotaUsed = 0
				providers[i].QuotaResetAt = time.Time{}
			}
		} else {
			// No persisted reset time: restore whatever usage we have (fallback)
			if hasUsed && used > 0 {
				providers[i].QuotaUsed = used
			}
		}
	}

	if len(legacyKeys) > 0 {
		keys := make([]string, 0, len(legacyKeys))
		for key := range legacyKeys {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if err := m.repo.MigrateSystemStats(m.ctx, migrated, keys); err != nil {
			m.logger.ErrorContext(m.ctx, "Failed to migrate legacy provider quota state", "error_class", "repository")
		}
	} else if len(migrated) > 0 {
		if err := m.repo.MigrateSystemStats(m.ctx, migrated, nil); err != nil {
			m.logger.ErrorContext(m.ctx, "Failed to migrate legacy provider quota state", "error_class", "repository")
		}
	}
}

// GetPool returns the current connection pool or error if not available.
// The concrete return type is *nntppool.Client which satisfies NntpClient.
func (m *manager) GetPool() (NntpClient, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.pool == nil {
		return nil, fmt.Errorf("NNTP connection pool not available - no providers configured")
	}

	return m.pool, nil
}

// SetProviders creates/recreates the pool with new providers
func (m *manager) SetProviders(providers []nntppool.Provider) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Shut down existing pool and metrics tracker if present
	if m.pool != nil {
		m.logger.InfoContext(m.ctx, "Shutting down existing NNTP connection pool")
		if m.metricsTracker != nil {
			m.metricsTracker.Stop()
			m.metricsTracker = nil
		}
		m.pool.Close()
		m.pool = nil
	}

	// Return early if no providers (clear pool scenario)
	if len(providers) == 0 {
		m.logger.InfoContext(m.ctx, "No NNTP providers configured - pool cleared")
		return nil
	}

	// Restore quota state from DB before creating the pool
	m.injectQuotaState(providers)

	// Create new pool with providers
	m.logger.InfoContext(m.ctx, "Creating NNTP connection pool", "provider_count", len(providers))
	pool, err := nntppool.NewClient(m.ctx, providers)
	if err != nil {
		return fmt.Errorf("failed to create NNTP connection pool: %w", err)
	}

	m.pool = pool

	// Start metrics tracker
	m.metricsTracker = NewMetricsTracker(pool, m.repo)
	if m.providerIDMap != nil {
		m.metricsTracker.SetProviderIDs(m.providerIDMap)
	}
	m.metricsTracker.Start(m.ctx)

	m.startQuotaWatcher()

	m.logger.InfoContext(m.ctx, "NNTP connection pool created successfully")
	return nil
}

// ClearPool shuts down and removes the current pool
func (m *manager) ClearPool() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pool != nil {
		m.logger.InfoContext(m.ctx, "Clearing NNTP connection pool")
		m.stopQuotaWatcher()
		if m.metricsTracker != nil {
			m.metricsTracker.Stop()
			m.metricsTracker = nil
		}
		m.pool.Close()
		m.pool = nil
	}

	return nil
}

// HasPool returns true if a pool is currently available
func (m *manager) HasPool() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.pool != nil
}

// GetMetrics returns the current pool metrics with calculated speeds
func (m *manager) GetMetrics() (MetricsSnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.pool == nil {
		return MetricsSnapshot{}, fmt.Errorf("NNTP connection pool not available")
	}

	if m.metricsTracker == nil {
		return MetricsSnapshot{}, fmt.Errorf("metrics tracker not available")
	}

	return m.metricsTracker.GetSnapshot(), nil
}

// ResetMetrics resets specific cumulative metrics
func (m *manager) ResetMetrics(ctx context.Context, resetPeak bool, resetTotals bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.metricsTracker != nil {
		return m.metricsTracker.Reset(ctx, resetPeak, resetTotals)
	}

	// If tracker not available, still try to reset DB directly
	if m.repo != nil {
		currentStats, err := m.repo.GetSystemStats(ctx)
		if err == nil {
			resetMap := make(map[string]int64)
			for k := range currentStats {
				if resetTotals {
					resetMap[k] = 0
				}
			}

			if resetTotals {
				resetMap["bytes_downloaded"] = 0
				resetMap["articles_downloaded"] = 0
				resetMap["bytes_uploaded"] = 0
				resetMap["articles_posted"] = 0
			}

			if resetPeak {
				resetMap["max_download_speed"] = 0
			}

			if len(resetMap) > 0 {
				_ = m.repo.BatchUpdateSystemStats(ctx, resetMap)
			}
		}
	}

	return nil
}

// ResetProviderErrors zeroes per-provider error counts without affecting other metrics.
func (m *manager) ResetProviderErrors(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.metricsTracker == nil {
		return nil
	}

	return m.metricsTracker.ResetProviderErrors(ctx)
}

// IncArticlesDownloaded increments the count of articles successfully downloaded
func (m *manager) IncArticlesDownloaded() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.metricsTracker != nil {
		m.metricsTracker.IncArticlesDownloaded()
	}
}

// UpdateDownloadProgress updates the bytes downloaded for a specific stream
func (m *manager) UpdateDownloadProgress(id string, bytesDownloaded int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.metricsTracker != nil {
		m.metricsTracker.UpdateDownloadProgress(id, bytesDownloaded)
	}
}

// IncArticlesPosted increments the count of articles successfully posted
func (m *manager) IncArticlesPosted() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.metricsTracker != nil {
		m.metricsTracker.IncArticlesPosted()
	}
}

// AddProvider adds a single provider to the running pool.
// If no pool exists yet, a new one is created with this single provider.
func (m *manager) AddProvider(provider nntppool.Provider) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Restore quota state from DB
	providers := []nntppool.Provider{provider}
	m.injectQuotaState(providers)
	provider = providers[0]

	if m.pool == nil {
		// No pool yet — create one with this single provider
		m.logger.InfoContext(m.ctx, "Creating NNTP connection pool for first provider", "provider", provider.Host)
		pool, err := nntppool.NewClient(m.ctx, []nntppool.Provider{provider}, nntppool.WithDispatchStrategy(nntppool.DispatchRoundRobin))
		if err != nil {
			return fmt.Errorf("failed to create NNTP connection pool: %w", err)
		}
		m.pool = pool
		m.metricsTracker = NewMetricsTracker(pool, m.repo)
		if m.providerIDMap != nil {
			m.metricsTracker.SetProviderIDs(m.providerIDMap)
		}
		m.metricsTracker.Start(m.ctx)
	} else {
		m.logger.InfoContext(m.ctx, "Adding provider to NNTP connection pool", "provider", provider.Host)
		if err := m.pool.AddProvider(provider); err != nil {
			return err
		}
	}

	m.startQuotaWatcher()
	return nil
}

// RemoveProvider removes a provider by name from the running pool.
// If the last provider is removed, the pool and metrics tracker are shut down.
func (m *manager) RemoveProvider(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pool == nil {
		return fmt.Errorf("NNTP connection pool not available - cannot remove provider")
	}

	m.logger.InfoContext(m.ctx, "Removing provider from NNTP connection pool", "provider", name)
	if err := m.pool.RemoveProvider(name); err != nil {
		return err
	}

	// If no providers remain, tear down the pool entirely
	if m.pool.NumProviders() == 0 {
		m.logger.InfoContext(m.ctx, "Last provider removed - shutting down NNTP connection pool")
		m.stopQuotaWatcher()
		if m.metricsTracker != nil {
			m.metricsTracker.Stop()
			m.metricsTracker = nil
		}
		m.pool.Close()
		m.pool = nil
	}

	return nil
}

// resetProviderQuotaLocked performs the quota reset with m.mu already held.
func (m *manager) resetProviderQuotaLocked(ctx context.Context, poolName string) error {
	if m.pool == nil {
		return fmt.Errorf("NNTP connection pool not available")
	}

	m.logger.InfoContext(ctx, "Resetting provider quota", "provider", poolName)

	if err := m.pool.ResetProviderQuota(poolName); err != nil {
		return fmt.Errorf("failed to reset provider quota: %w", err)
	}

	if m.repo != nil {
		stats := map[string]int64{
			"quota_used:" + poolName:     0,
			"quota_reset_at:" + poolName: 0,
		}
		if err := m.repo.BatchUpdateSystemStats(ctx, stats); err != nil {
			m.logger.ErrorContext(ctx, "Failed to clear persisted quota state", "err", err, "provider", poolName)
		}
	}

	return nil
}

// ResetProviderQuota resets the download quota counter for a provider,
// clearing its consumed-bytes counter and exceeded flag in-place.
func (m *manager) ResetProviderQuota(ctx context.Context, poolName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resetProviderQuotaLocked(ctx, poolName)
}

// AcquireImportSlot blocks until an import admission slot is available or ctx
// is cancelled. See ImportAdmission.Acquire.
func (m *manager) AcquireImportSlot(ctx context.Context) (func(), error) {
	return m.admission.Acquire(ctx)
}

// SetAdmissionCaps configures the import-concurrency caps. capIdle applies
// when no stream is active; capWhileStreaming applies while any stream is
// active. A cap of 0 means unlimited.
func (m *manager) SetAdmissionCaps(capIdle, capWhileStreaming int) {
	m.admission.SetCaps(capIdle, capWhileStreaming)
}

// SetStreamSource wires the source used to determine whether streams are
// currently active.
func (m *manager) SetStreamSource(src StreamActivitySource) {
	m.admission.SetStreamSource(src)
}

// NotifyStreamChange forwards a stream-count change to the admission gate so
// it can wake or hold waiters according to the new effective cap.
func (m *manager) NotifyStreamChange() {
	m.admission.NotifyStreamChange()
}

// SetProviderIDs sets a mapping between pool names and configuration IDs
func (m *manager) SetProviderIDs(mapping map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.providerIDMap = mapping
	if m.metricsTracker != nil {
		m.metricsTracker.SetProviderIDs(mapping)
	}
}

// startQuotaWatcher starts the background quota watcher if not already running.
// Must be called with m.mu held.
func (m *manager) startQuotaWatcher() {
	if m.quotaWatchCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.quotaWatchCancel = cancel
	go m.quotaWatchLoop(ctx)
}

// stopQuotaWatcher stops the background quota watcher if running.
// Must be called with m.mu held.
func (m *manager) stopQuotaWatcher() {
	if m.quotaWatchCancel != nil {
		m.quotaWatchCancel()
		m.quotaWatchCancel = nil
	}
}

// quotaWatchLoop runs a periodic check for providers whose quota period has elapsed.
func (m *manager) quotaWatchLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkAndResetExpiredQuotas(ctx)
		}
	}
}

// checkAndResetExpiredQuotas resets any provider whose quota period has elapsed
// but whose quota counter was never cleared (because no new request arrived to
// trigger nntppool's on-demand reset path).
func (m *manager) checkAndResetExpiredQuotas(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pool == nil {
		return
	}

	now := time.Now()
	for _, ps := range m.pool.Stats().Providers {
		if ps.QuotaBytes == 0 || !ps.QuotaExceeded {
			continue
		}
		if ps.QuotaResetAt.IsZero() || !ps.QuotaResetAt.Before(now) {
			continue
		}

		m.logger.InfoContext(ctx, "Auto-resetting expired provider quota",
			"provider", ps.Name, "reset_at", ps.QuotaResetAt)
		if err := m.resetProviderQuotaLocked(ctx, ps.Name); err != nil {
			m.logger.ErrorContext(ctx, "Failed to auto-reset provider quota",
				"provider", ps.Name, "error", err)
		}
	}
}
