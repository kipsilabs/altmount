package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/javi11/nntppool/v5"
)

type ProviderSpeedTestResponse struct {
	SpeedMBps float64 `json:"speed_mbps"`
	Duration  float64 `json:"duration_seconds"`
}

// handleTestProviderSpeed tests the download speed of a specific provider
//
//	@Summary		Test provider download speed
//	@Description	Runs a speed test against the specified NNTP provider and saves the result to config.
//	@Tags			Providers
//	@Produce		json
//	@Param			id	path	string	true	"Provider ID"
//	@Success		200	{object}	APIResponse{data=ProviderSpeedTestResponse}
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/config/providers/{id}/speedtest [post]
func (s *Server) handleTestProviderSpeed(c *fiber.Ctx) error {
	providerID := c.Params("id")
	if providerID == "" {
		return RespondBadRequest(c, "Provider ID is required", "")
	}

	if s.configManager == nil {
		return RespondInternalError(c, "Configuration management not available", "")
	}

	cfg := s.configManager.GetConfig()
	var targetProvider *config.ProviderConfig
	for _, p := range cfg.Providers {
		if p.ID == providerID {
			targetProvider = &p
			break
		}
	}

	if targetProvider == nil {
		return RespondNotFound(c, "Provider", "")
	}

	testCtx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()

	// Use the stable provider ID used by the production pool and coordinator
	// to match the right per-provider stats entry in the result.
	providerName := targetProvider.NNTPPoolName()

	// Prefer the production pool when this provider is already part of
	// it; otherwise fall back to the singleton coordinator so we never
	// create a fresh nntppool.Client per request.
	result, err := s.runProviderSpeedTest(testCtx, targetProvider)
	if err != nil {
		return RespondInternalError(c, "Speed test failed", err.Error())
	}
	// Prefer the targeted provider's per-provider wire speed over the
	// client-wide aggregate. WireSpeedBps divides the whole-client
	// BytesConsumed delta by elapsed, so on the shared production pool
	// any concurrent stream or other provider's traffic during the test
	// window inflates it. result.Providers carries the delta-based,
	// per-provider speed for exactly the provider we targeted.
	//
	// Match by name to avoid picking a random provider when the result
	// slice contains multiple entries (map iteration order is undefined).
	wireBps := result.WireSpeedBps
	for _, ps := range result.Providers {
		if ps.Name == providerName && ps.AvgSpeed > 0 {
			wireBps = ps.AvgSpeed
			break
		}
	}
	speed := wireBps / 1024 / 1024 // bytes/sec → MB/s
	if speed < 0 {
		speed = 0
	}

	// Update provider config with speed test result
	now := time.Now()
	currentConfig := s.configManager.GetConfig()
	newConfig := currentConfig.DeepCopy()

	for i, p := range newConfig.Providers {
		if p.ID == providerID {
			newConfig.Providers[i].LastSpeedTestMbps = speed
			newConfig.Providers[i].LastSpeedTestTime = &now
			break
		}
	}

	if err := s.configManager.UpdateConfig(newConfig); err != nil {
		slog.ErrorContext(c.Context(), "Failed to update provider speed test result in config", "provider_id", providerID, "err", err)
		return RespondInternalError(c, "Failed to save speed test result", err.Error())
	}

	if err := s.configManager.SaveConfig(); err != nil {
		slog.ErrorContext(c.Context(), "Failed to persist config after speed test", "err", err)
	}

	// Record to database
	if err := s.queueRepo.RecordProviderSpeedTest(c.Context(), providerID, speed); err != nil {
		slog.ErrorContext(c.Context(), "Failed to record speed test history", "provider_id", providerID, "err", err)
	}

	return RespondSuccess(c, ProviderSpeedTestResponse{
		SpeedMBps: speed,
		Duration:  result.Elapsed.Seconds(),
	})
}

// runProviderSpeedTest executes the speed test against the given
// provider, preferring the production pool when the provider is part
// of it. Falls back to the per-request speedtest coordinator (cached
// nntppool client + singleflight dedupe) when the provider isn't in
// the active pool.
func (s *Server) runProviderSpeedTest(ctx context.Context, p *config.ProviderConfig) (*nntppool.SpeedTestResult, error) {
	// Always consult the pool manager so a provider already in the
	// running pool reuses its connections rather than dialing fresh.
	// pool.Manager is required wiring; in tests it may return nil/err.
	if s.poolManager != nil {
		if cp, err := s.poolManager.GetPool(); err == nil && cp != nil {
			if real, ok := cp.(*nntppool.Client); ok {
				// ToNNTPProvider registers the provider under its stable ID.
				providerName := p.NNTPPoolName()
				// Try the production pool first. If the provider isn't
				// in it, nntppool returns an error and we fall through
				// to the coordinator.
				if result, sterr := real.SpeedTest(ctx, nntppool.SpeedTestOptions{
					ProviderName: providerName,
				}); sterr == nil {
					return result, nil
				}
			}
		}
	}

	// Fall back to the dedicated speed-test client (cached + dedupe).
	// Lazy-construct the coordinator if Server was assembled outside
	// NewServer (e.g. in tests). speedtestOnce makes this safe under
	// concurrent calls.
	s.speedtestOnce.Do(func() {
		if s.speedtest == nil {
			s.speedtest = newSpeedtestCoordinator()
		}
	})
	v, err := s.speedtest.run(ctx, p, func(client *nntppool.Client, providerName string) (any, error) {
		return client.SpeedTest(ctx, nntppool.SpeedTestOptions{
			ProviderName: providerName,
		})
	})
	if err != nil {
		return nil, err
	}
	result, _ := v.(*nntppool.SpeedTestResult)
	return result, nil
}
