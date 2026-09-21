package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/javi11/nntppool/v5"
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
	// failure). When the admission cap is unconfigured (0) it is a no-op.
	AcquireImportSlot(ctx context.Context) (release func(), err error)

	// SetAdmissionCap configures the cap on concurrently running NZB imports.
	// A cap of 0 means unlimited.
	SetAdmissionCap(cap int)

	// AcquireImportConnection blocks until the global import connection
	// budget grants a token for one segment (body) fetch, or ctx is
	// cancelled. The returned release function must be called exactly once
	// when the fetch is done. No-op while the budget capacity is unset (0).
	AcquireImportConnection(ctx context.Context) (release func(), err error)

	// SetImportConnCapacity sets the import connection budget to the pool's
	// total connection count (sum of provider max connections).
	SetImportConnCapacity(total int)

	// SetStreamHeadroom sets how many connections the import budget holds back
	// per active stream. See config.Config.StreamHeadroomConnections.
	SetStreamHeadroom(perStream int)
	// SpeculativeBudget bounds read-ahead fetches across every stream; nil
	// (as returned by test fakes) means unlimited.
	SpeculativeBudget() *SpeculativeBudget

	// ImportConnCapacity returns the current budget capacity snapshot,
	// useful for sizing import worker pools.
	ImportConnCapacity() int

	// SetStreamSource wires the activity signal so the import connection
	// budget can shrink while streams are active.
	SetStreamSource(src StreamActivitySource)

	// NotifyStreamChange must be called by the stream source whenever its
	// active stream count changes, so the budget can re-evaluate.
	NotifyStreamChange()

	// StatSweepConcurrency returns how many STATs an availability sweep
	// (import fast-fail, health check) should keep in flight. While streams
	// are active it returns the caller's conservative bound — typically one
	// connection's STAT pipeline depth, so sweeps never crowd playback — and
	// on an idle pool it opens up to the pool's aggregate STAT pipeline
	// capacity (nntppool's StatCapacity), where STAT's one-line replies make
	// the extra width nearly free. Falls back to conservative when no pool
	// exists or its capacity is below the conservative bound.
	StatSweepConcurrency(conservative int) int
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
	budget           *ImportBudget
	specBudget       *SpeculativeBudget
}

// NewManager creates a new pool manager
func NewManager(ctx context.Context, repo StatsRepository) Manager {
	return &manager{
		ctx:        ctx,
		repo:       repo,
		logger:     slog.Default().With("component", "pool"),
		admission:  NewImportAdmission(),
		budget:     NewImportBudget(),
		specBudget: NewSpeculativeBudget(),
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

// currentPool snapshots the pool pointer under a short read lock so callers can
// use it without holding the manager lock across slow operations.
func (m *manager) currentPool() *nntppool.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.pool
}

// resetProviderQuotaOn resets poolName's quota on the given pool and clears the
// persisted quota state. Must be called without m.mu held: the repository write
// can block, and holding the manager lock across it would stall GetPool on the
// streaming/import hot path.
func (m *manager) resetProviderQuotaOn(ctx context.Context, pool *nntppool.Client, poolName string) error {
	m.logger.InfoContext(ctx, "Resetting provider quota", "provider", poolName)

	if err := pool.ResetProviderQuota(poolName); err != nil {
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
	pool := m.currentPool()
	if pool == nil {
		return fmt.Errorf("NNTP connection pool not available")
	}

	return m.resetProviderQuotaOn(ctx, pool, poolName)
}

// AcquireImportSlot blocks until an import admission slot is available or ctx
// is cancelled. See ImportAdmission.Acquire.
func (m *manager) AcquireImportSlot(ctx context.Context) (func(), error) {
	return m.admission.Acquire(ctx)
}

// SetAdmissionCap configures the cap on concurrently running NZB imports.
// A cap of 0 means unlimited.
func (m *manager) SetAdmissionCap(cap int) {
	m.admission.SetCap(cap)
}

// AcquireImportConnection blocks until the import connection budget grants a
// token or ctx is cancelled. See ImportBudget.Acquire.
func (m *manager) AcquireImportConnection(ctx context.Context) (func(), error) {
	return m.budget.Acquire(ctx)
}

// SetImportConnCapacity sets the import connection budget to the pool's total
// connection count.
func (m *manager) SetImportConnCapacity(total int) {
	m.budget.SetCapacity(total)
}

// SetStreamHeadroom sets the per-stream import connection reservation.
func (m *manager) SetStreamHeadroom(perStream int) {
	m.budget.SetHeadroom(perStream)
}

// SpeculativeBudget returns the pool-wide read-ahead budget.
func (m *manager) SpeculativeBudget() *SpeculativeBudget {
	return m.specBudget
}

// ImportConnCapacity returns the current budget capacity snapshot.
func (m *manager) ImportConnCapacity() int {
	return m.budget.Capacity()
}

// SetStreamSource wires the source used to determine whether streams are
// currently active.
func (m *manager) SetStreamSource(src StreamActivitySource) {
	m.budget.SetStreamSource(src)
}

// NotifyStreamChange forwards a stream-count change to the import connection
// budget so it can wake or hold waiters according to the new effective cap.
func (m *manager) NotifyStreamChange() {
	m.budget.NotifyStreamChange()
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

// idleStatSweepFactor bounds how far an idle-pool sweep widens past the
// conservative (one-connection-pipeline) bound. The pool's full StatCapacity
// is NOT safe here: nntppool stamps each STAT's per-attempt deadline (~2s
// adaptive) at dispatch, and capacity-width dispatch fills every connection's
// pipeline to StatInflight depth — the pipeline tail cannot drain before its
// deadline, the reader's read timeout then closes the whole connection
// (replies are FIFO), and every pipelined request on it fails with
// "connection died". Observed live: a 4096-wide sweep against a 50-connection
// pool left 3692 of 4147 STATs unresolved per pass and killed the pool for
// minutes, taking concurrent playback to 0 B/s. Twice the conservative bound
// keeps per-connection pipelines a few requests deep — drained well inside
// the attempt window at realistic RTTs — while still sweeping a full release
// in seconds.
const idleStatSweepFactor = 2

// StatSweepConcurrency picks the in-flight STAT bound for an availability
// sweep: conservative while streams are active (or with no pool); a bounded
// widening — min(StatCapacity, idleStatSweepFactor × conservative) — when
// idle. See idleStatSweepFactor for why the full capacity is never used.
func (m *manager) StatSweepConcurrency(conservative int) int {
	if m.budget.StreamsActive() {
		return conservative
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.pool == nil {
		return conservative
	}
	widened := min(m.pool.StatCapacity(), idleStatSweepFactor*conservative)
	return max(widened, conservative)
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
	// Snapshot the pool and do the rest unlocked: SetProviders/ClearPool may
	// swap or close it meanwhile, but nntppool's accessors are concurrency-safe
	// and resetting a quota on a replaced/closed pool is harmless (it either
	// finds the provider or reports it as not found).
	pool := m.currentPool()
	if pool == nil {
		return
	}

	now := time.Now()
	for _, ps := range pool.Stats().Providers {
		// Skip providers with no quota configured or no tracked usage to reset.
		// Without the QuotaUsed guard, providers that simply have a quota period
		// configured but have never downloaded anything would trigger a spurious
		// reset + DB write on every tick once their reset time expires.
		if ps.QuotaBytes == 0 || ps.QuotaUsed == 0 {
			continue
		}
		if ps.QuotaResetAt.IsZero() || !ps.QuotaResetAt.Before(now) {
			continue
		}

		m.logger.InfoContext(ctx, "Auto-resetting expired provider quota",
			"provider", ps.Name, "reset_at", ps.QuotaResetAt)
		if err := m.resetProviderQuotaOn(ctx, pool, ps.Name); err != nil {
			m.logger.ErrorContext(ctx, "Failed to auto-reset provider quota",
				"provider", ps.Name, "error", err)
		}
	}
}
