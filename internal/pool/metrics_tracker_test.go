package pool

import (
	"context"
	"log/slog"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
)

func TestMetricsTracker_WindowedSpeed(t *testing.T) {
	mt := &MetricsTracker{
		samples:           make([]metricsample, 0),
		calculationWindow: 10 * time.Second,
	}

	now := time.Now()

	// Case 1: No samples
	snapshot := mt.getSnapshot(now, nntppool.ClientStats{}, nil)
	assert.Equal(t, 0.0, snapshot.DownloadSpeedBytesPerSec)

	// Case 2: One sample (100MB at now-5s)
	mt.samples = append(mt.samples, metricsample{
		totalBytes: 100 * 1024 * 1024,
		timestamp:  now.Add(-5 * time.Second),
	})

	// Current state: 150MB
	mt.liveBytesDownloaded.Store(150 * 1024 * 1024)

	snapshot = mt.getSnapshot(now, nntppool.ClientStats{}, nil)
	// Speed = (150 - 100) / 5 = 10 MB/s
	assert.Equal(t, float64(50*1024*1024)/5.0, snapshot.DownloadSpeedBytesPerSec)

	// Case 3: Multiple samples, all newer than calculationWindow
	mt.samples = append(mt.samples, metricsample{
		totalBytes: 120 * 1024 * 1024,
		timestamp:  now.Add(-2 * time.Second),
	})
	// Sample 0: 100MB at now-5s
	// Sample 1: 120MB at now-2s
	// cutoff = now-10s. Both are after cutoff. Fallback to oldest (Sample 0).

	snapshot = mt.getSnapshot(now, nntppool.ClientStats{}, nil)
	assert.Equal(t, float64(50*1024*1024)/5.0, snapshot.DownloadSpeedBytesPerSec)

	// Case 4: Sample older than 10s
	mt.samples = append([]metricsample{{
		totalBytes: 50 * 1024 * 1024,
		timestamp:  now.Add(-15 * time.Second),
	}}, mt.samples...)
	// Sample 0: 50MB at now-15s (Reference! It's the newest sample BEFORE now-10s)
	// Sample 1: 100MB at now-5s
	// Sample 2: 120MB at now-2s

	snapshot = mt.getSnapshot(now, nntppool.ClientStats{}, nil)
	// Speed = (150 - 50) / 15 = 6.66 MB/s
	assert.InDelta(t, float64(100*1024*1024)/15.0, snapshot.DownloadSpeedBytesPerSec, 0.001)

	// Case 5: Sample too recent (under 2s)
	mt.samples = []metricsample{{
		totalBytes: 140 * 1024 * 1024,
		timestamp:  now.Add(-1 * time.Second),
	}}
	mt.liveBytesDownloaded.Store(150 * 1024 * 1024)
	snapshot = mt.getSnapshot(now, nntppool.ClientStats{}, nil)
	assert.Equal(t, 0.0, snapshot.DownloadSpeedBytesPerSec)
}

func TestMetricsTracker_Reset(t *testing.T) {
	mt := &MetricsTracker{
		maxDownloadSpeed: 500.0,
		samples: []metricsample{
			{totalBytes: 100, timestamp: time.Now()},
		},
		initialProviderErrors: make(map[string]int64),
		logger:                slog.Default(),
	}
	mt.liveBytesDownloaded.Store(1000)
	mt.articlesDownloaded.Store(10)

	// Case 1: Reset Peak only
	err := mt.Reset(context.Background(), true, false)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, mt.maxDownloadSpeed)
	assert.Equal(t, int64(1000), mt.liveBytesDownloaded.Load())
	assert.Equal(t, int64(10), mt.articlesDownloaded.Load())
	assert.Len(t, mt.samples, 1)

	// Case 2: Reset Totals only
	mt.maxDownloadSpeed = 500.0
	err = mt.Reset(context.Background(), false, true)
	assert.NoError(t, err)
	assert.Equal(t, 500.0, mt.maxDownloadSpeed)
	assert.Equal(t, int64(0), mt.liveBytesDownloaded.Load())
	assert.Equal(t, int64(0), mt.articlesDownloaded.Load())
	assert.Len(t, mt.samples, 0)

	// Case 3: Reset All
	mt.liveBytesDownloaded.Store(1000)
	mt.articlesDownloaded.Store(10)
	mt.samples = []metricsample{{totalBytes: 100, timestamp: time.Now()}}
	err = mt.Reset(context.Background(), true, true)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, mt.maxDownloadSpeed)
	assert.Equal(t, int64(0), mt.liveBytesDownloaded.Load())
	assert.Equal(t, int64(0), mt.articlesDownloaded.Load())
	assert.Len(t, mt.samples, 0)
}

// recordingStatsRepo captures what the tracker writes so tests can assert on
// the deltas it derives rather than on the totals it persists.
type recordingStatsRepo struct {
	noopStatsRepo
	mu            sync.Mutex
	hourlyDeltas  map[string]int64
	dailyDelta    int64
	hourlyCleared int
	hourlyQueries int
	hourlyStats   map[string]int64
}

func newRecordingStatsRepo() *recordingStatsRepo {
	return &recordingStatsRepo{
		hourlyDeltas: make(map[string]int64),
		hourlyStats:  make(map[string]int64),
	}
}

func (r *recordingStatsRepo) AddProviderBytesToHourlyStat(_ context.Context, providerID string, bytes int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hourlyDeltas[providerID] += bytes
	return nil
}

func (r *recordingStatsRepo) AddBytesDownloadedToDailyStat(_ context.Context, bytes int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dailyDelta += bytes
	return nil
}

func (r *recordingStatsRepo) ClearProviderHourlyStats(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hourlyCleared++
	return nil
}

func (r *recordingStatsRepo) GetProviderHourlyStats(context.Context, int) (map[string]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hourlyQueries++
	out := make(map[string]int64, len(r.hourlyStats))
	maps.Copy(out, r.hourlyStats)
	return out, nil
}

func TestMetricsTracker_ResetTotalsOffsetsLivePoolCounters(t *testing.T) {
	repo := newRecordingStatsRepo()

	// The pool has already consumed 1MB and hit 7 errors for this provider, and
	// those counters cannot be reset in the pool.
	live := nntppool.ClientStats{
		Providers: []nntppool.ProviderStats{
			{Name: "provider-a", BytesConsumed: 1024 * 1024, Errors: 7},
		},
	}

	mt := &MetricsTracker{
		repo:                     repo,
		samples:                  make([]metricsample, 0),
		calculationWindow:        10 * time.Second,
		initialProviderErrors:    map[string]int64{"provider-a": 100},
		initialProviderBytes:     map[string]int64{"provider-a": 5 * 1024 * 1024},
		initialProviderStartedAt: make(map[string]time.Time),
		lastSavedProviderBytes:   map[string]int64{"provider-a": 6 * 1024 * 1024},
		lastSavedBytesDownloaded: 6 * 1024 * 1024,
		logger:                   slog.Default(),
	}
	mt.statsFn = func() nntppool.ClientStats { return live }
	mt.liveBytesDownloaded.Store(1024 * 1024)
	mt.initialBytesDownloaded = 5 * 1024 * 1024

	before := mt.GetSnapshot()
	assert.Equal(t, int64(6*1024*1024), before.ProviderBytes["provider-a"])
	assert.Equal(t, int64(107), before.ProviderErrors["provider-a"])

	assert.NoError(t, mt.Reset(context.Background(), false, true))
	assert.Equal(t, 1, repo.hourlyCleared)

	// Displayed per-provider counters must be zero even though the pool's live
	// counters still hold the pre-reset volume.
	after := mt.GetSnapshot()
	assert.Equal(t, int64(0), after.ProviderBytes["provider-a"])
	assert.Equal(t, int64(0), after.ProviderErrors["provider-a"])
	assert.Equal(t, int64(0), after.BytesDownloaded)

	// New traffic after the reset: 500KB more consumed by the pool.
	live.Providers[0].BytesConsumed += 500 * 1024
	mt.liveBytesDownloaded.Store(500 * 1024)

	mt.saveStats(context.Background())

	// Only the post-reset bytes may be charged to the hourly/daily history.
	assert.Equal(t, int64(500*1024), repo.hourlyDeltas["provider-a"])
	assert.Equal(t, int64(500*1024), repo.dailyDelta)
}

func TestMetricsTracker_ProviderBytes24hIsCached(t *testing.T) {
	repo := newRecordingStatsRepo()
	repo.hourlyStats["provider-a"] = 4096

	mt := &MetricsTracker{
		repo:                     repo,
		samples:                  make([]metricsample, 0),
		calculationWindow:        10 * time.Second,
		initialProviderErrors:    make(map[string]int64),
		initialProviderBytes:     make(map[string]int64),
		initialProviderStartedAt: make(map[string]time.Time),
		lastSavedProviderBytes:   make(map[string]int64),
		logger:                   slog.Default(),
	}

	for range 5 {
		snapshot := mt.GetSnapshot()
		assert.Equal(t, int64(4096), snapshot.ProviderBytes24h["provider-a"])
	}

	assert.Equal(t, 1, repo.hourlyQueries)

	// An expired entry triggers exactly one more query.
	mt.bytes24hMu.Lock()
	mt.bytes24hFetched = time.Now().Add(-2 * providerBytes24hTTL)
	mt.bytes24hMu.Unlock()

	mt.GetSnapshot()
	mt.GetSnapshot()
	assert.Equal(t, 2, repo.hourlyQueries)
}

func TestMetricsTracker_ResetProviderErrors(t *testing.T) {
	mt := &MetricsTracker{
		samples:               make([]metricsample, 0),
		initialProviderErrors: map[string]int64{"provider-a": 50, "provider-b": 30},
		logger:                slog.Default(),
	}

	poolStats := nntppool.ClientStats{
		Providers: []nntppool.ProviderStats{
			{Name: "provider-a", Errors: 10},
			{Name: "provider-b", Errors: 5},
		},
	}

	// Before reset: provider-a = 50+10 = 60, provider-b = 30+5 = 35
	snapshot := mt.getSnapshot(time.Now(), poolStats, nil)
	assert.Equal(t, int64(60), snapshot.ProviderErrors["provider-a"])
	assert.Equal(t, int64(35), snapshot.ProviderErrors["provider-b"])

	// Simulate the offset that ResetProviderErrors applies:
	// initialProviderErrors[id] = -liveErrors[id], so merged = 0
	mt.initialProviderErrors["provider-a"] = -poolStats.Providers[0].Errors
	mt.initialProviderErrors["provider-b"] = -poolStats.Providers[1].Errors

	snapshot = mt.getSnapshot(time.Now(), poolStats, nil)
	assert.Equal(t, int64(0), snapshot.ProviderErrors["provider-a"])
	assert.Equal(t, int64(0), snapshot.ProviderErrors["provider-b"])
}
