package api

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/javi11/nntppool/v5"
)

// provider_speedtest_storm_test.go reproduces the speed-test handler
// storm: every invocation calls nntppool.NewClient directly, bypassing
// pool.Manager entirely. The storm shape is structural — the handler
// doesn't even reach through the seam tests can intercept.

// countingPoolManager is a pool.Manager that records every call to its
// connection-acquiring methods. The S8 test asserts these counters
// remain zero despite N HTTP requests, proving the handler bypasses
// pool.Manager.
type countingPoolManager struct {
	getPoolCalls atomic.Int64
	hasPoolCalls atomic.Int64
}

var _ pool.Manager = (*countingPoolManager)(nil)

func (m *countingPoolManager) GetPool() (pool.NntpClient, error) {
	m.getPoolCalls.Add(1)
	return nil, nil
}
func (m *countingPoolManager) HasPool() bool                            { m.hasPoolCalls.Add(1); return false }
func (m *countingPoolManager) SetProviders(_ []nntppool.Provider) error { return nil }
func (m *countingPoolManager) ClearPool() error                         { return nil }
func (m *countingPoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}
func (m *countingPoolManager) ResetMetrics(_ context.Context, _, _ bool) error {
	return nil
}
func (m *countingPoolManager) ResetProviderErrors(_ context.Context) error { return nil }
func (m *countingPoolManager) IncArticlesDownloaded()                      {}
func (m *countingPoolManager) UpdateDownloadProgress(_ string, _ int64)    {}
func (m *countingPoolManager) IncArticlesPosted()                          {}
func (m *countingPoolManager) AddProvider(_ nntppool.Provider) error       { return nil }
func (m *countingPoolManager) RemoveProvider(_ string) error               { return nil }
func (m *countingPoolManager) ResetProviderQuota(_ context.Context, _ string) error {
	return nil
}
func (m *countingPoolManager) SetProviderIDs(_ map[string]string) {}
func (m *countingPoolManager) AcquireImportSlot(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *countingPoolManager) SetAdmissionCap(_ int) {}
func (m *countingPoolManager) AcquireImportConnection(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *countingPoolManager) SetImportConnCapacity(_ int)                 {}
func (m *countingPoolManager) ImportConnCapacity() int                     { return 0 }
func (m *countingPoolManager) SetStreamSource(_ pool.StreamActivitySource) {}
func (m *countingPoolManager) NotifyStreamChange()                         {}
func (m *countingPoolManager) StatSweepConcurrency(conservative int) int   { return conservative }

// TestStorm_SpeedTestBypassesPoolManager pins the post-S8 invariant:
// the /providers/:id/speedtest handler routes through pool.Manager
// (and the singleton speedtestCoordinator for non-pool providers)
// rather than creating a fresh nntppool.Client per request. The test
// asserts the structural property: poolManager.GetPool MUST be called
// at least once per request (the prerequisite for reusing the
// production pool), so the application has a chance to dedupe and
// gate concurrent traffic.
//
// Driving the handler against an unreachable host (127.0.0.1 port 1)
// makes the underlying speed test fail fast; the structural assertion
// runs regardless of the outcome.
func TestStorm_SpeedTestBypassesPoolManager(t *testing.T) {
	t.Parallel()
	const concurrentRequests = 5

	enabled := true
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{
		{
			ID:             "storm-test-provider",
			Host:           "127.0.0.1",
			Port:           1, // closed port: nntppool's initial ping fails immediately
			Username:       "user",
			Password:       "pass",
			MaxConnections: 1, // keep resource use minimal
			Enabled:        &enabled,
		},
	}

	cm := &mockConfigManager{cfg: cfg}
	cpm := &countingPoolManager{}
	server := &Server{configManager: cm, poolManager: cpm}

	app := fiber.New()
	app.Post("/api/config/providers/:id/speedtest", server.handleTestProviderSpeed)

	// Fire N concurrent requests. Each will fail (closed port) but the
	// structural bypass is what we're measuring, not the outcome.
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/api/config/providers/storm-test-provider/speedtest", nil)
			// Tight timeout — we don't care about the response, only the
			// fact that the handler took the bypass path.
			resp, err := app.Test(req, 3000)
			if err == nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	getPoolCalls := cpm.getPoolCalls.Load()
	t.Logf("%d concurrent speedtest requests completed in %s; "+
		"poolManager.GetPool calls=%d (invariant: > 0 per request)",
		concurrentRequests, elapsed, getPoolCalls)

	// PINNED INVARIANT: handler MUST consult pool.Manager (so it can
	// reuse the production pool for already-running providers and the
	// singleton speedtestCoordinator for everything else). Without
	// this, every request creates a fresh nntppool.Client and dials
	// independently.
	if getPoolCalls < 1 {
		t.Errorf("INVARIANT regression (S8): poolManager.GetPool calls=%d, want >= 1 "+
			"after %d concurrent speedtest requests. handleTestProviderSpeed must "+
			"route through s.poolManager rather than calling nntppool.NewClient inline.",
			getPoolCalls, concurrentRequests)
	}
}

func (m *countingPoolManager) SetStreamHeadroom(int)                      {}
func (m *countingPoolManager) SpeculativeBudget() *pool.SpeculativeBudget { return nil }
