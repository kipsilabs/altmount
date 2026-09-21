package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/javi11/nntppool/v5"
)

const (
	pipelineTuneConns    = 4
	pipelineTuneSegments = 160
	pipelineLevelTimeout = 20 * time.Second
	pipelineWinFactor    = 1.10
	defaultStatInflight  = 100
)

// pipelineTuneDepths are the inflight depths compared against the depth-1 baseline.
var pipelineTuneDepths = []int{4, 8, 16}

type PipelineDepthSample struct {
	Depth int     `json:"depth"`
	Mbps  float64 `json:"mbps"`
}

type PipelineTuneResponse struct {
	RecommendedInflight     int                   `json:"recommended_inflight"`
	RecommendedStatInflight int                   `json:"recommended_stat_inflight"`
	BaselineMbps            float64               `json:"baseline_mbps"`
	BestMbps                float64               `json:"best_mbps"`
	Improvement             float64               `json:"improvement_pct"`
	Enabled                 bool                  `json:"enabled"`
	TestConnections         int                   `json:"test_connections"`
	Tested                  []PipelineDepthSample `json:"tested"`
	Warning                 string                `json:"warning,omitempty"`
}

// handleTunePipeline sweeps pipeline depths and recommends the best inflight (1 = off).
//
//	@Summary		Auto-tune provider pipeline depth
//	@Description	Sweeps inflight depths against the provider and returns the recommended inflight_requests value.
//	@Tags			Providers
//	@Produce		json
//	@Param			id	path	string	true	"Provider ID"
//	@Success		200	{object}	APIResponse{data=PipelineTuneResponse}
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/config/providers/{id}/tune-pipeline [post]
func (s *Server) handleTunePipeline(c *fiber.Ctx) error {
	providerID := c.Params("id")
	if providerID == "" {
		return RespondBadRequest(c, "Provider ID is required", "")
	}
	if s.configManager == nil {
		return RespondInternalError(c, "Configuration management not available", "")
	}

	var target *config.ProviderConfig
	for _, p := range s.configManager.GetConfig().Providers {
		if p.ID == providerID {
			pc := p
			target = &pc
			break
		}
	}
	if target == nil {
		return RespondNotFound(c, "Provider", "")
	}

	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()

	resp, err := s.runPipelineSweep(ctx, target)
	if err != nil {
		return RespondInternalError(c, "Pipeline tuning failed", err.Error())
	}
	return RespondSuccess(c, resp)
}

// runPipelineSweep picks the best depth, or leaves it unchanged with a warning if it can't measure.
func (s *Server) runPipelineSweep(ctx context.Context, p *config.ProviderConfig) (*PipelineTuneResponse, error) {
	conns := p.MaxConnections
	if conns > pipelineTuneConns {
		conns = pipelineTuneConns
	}
	if conns < 1 {
		conns = 1
	}

	current := p.InflightRequests
	if current < 1 {
		current = 1
	}

	currentStat := p.StatInflightRequests
	if currentStat <= 0 {
		currentStat = defaultStatInflight
	}

	resp := &PipelineTuneResponse{
		RecommendedInflight:     current,
		RecommendedStatInflight: currentStat,
		TestConnections:         conns,
	}

	nzbBytes, err := fetchSpeedTestNZB(ctx)
	if err != nil {
		resp.Warning = "Could not fetch the speed-test NZB: " + err.Error()
		return resp, nil
	}

	baseline := s.measurePipelineDepth(ctx, p, conns, 1, nzbBytes)
	resp.BaselineMbps = round2(baseline)
	resp.Tested = append(resp.Tested, PipelineDepthSample{Depth: 1, Mbps: resp.BaselineMbps})
	if baseline <= 0 {
		resp.Warning = "Provider was busy or at its connection limit; pipeline left unchanged."
		return resp, nil
	}

	for _, depth := range pipelineTuneDepths {
		mbps := s.measurePipelineDepth(ctx, p, conns, depth, nzbBytes)
		resp.Tested = append(resp.Tested, PipelineDepthSample{Depth: depth, Mbps: round2(mbps)})
	}

	resp.RecommendedInflight, resp.Enabled, resp.Improvement, resp.BestMbps =
		pickPipelineDepth(baseline, resp.Tested[1:])
	resp.RecommendedStatInflight = pickStatDepth(resp.RecommendedStatInflight, resp.Enabled)
	return resp, nil
}

// pickStatDepth keeps the configured STAT depth while bodies pipeline, and drops to 1
// otherwise: a server that refuses pipelined bodies cannot absorb 100 pipelined STATs.
func pickStatDepth(configured int, pipeliningEnabled bool) int {
	if !pipeliningEnabled {
		return 1
	}
	if configured <= 0 {
		return defaultStatInflight
	}
	return configured
}

// pickPipelineDepth returns the smallest depth beating the baseline by the win factor, else 1.
func pickPipelineDepth(baseline float64, samples []PipelineDepthSample) (inflight int, enabled bool, improvement, best float64) {
	bestDepth := 0
	for _, s := range samples {
		if s.Mbps > best {
			best, bestDepth = s.Mbps, s.Depth
		}
	}
	if baseline > 0 && best > baseline {
		improvement = round2((best - baseline) / baseline * 100)
	}
	if bestDepth > 0 && baseline > 0 && best >= baseline*pipelineWinFactor {
		return bestDepth, true, improvement, best
	}
	return 1, false, improvement, best
}

// measurePipelineDepth returns MB/s for one bounded speed test at the given depth (0 on failure).
func (s *Server) measurePipelineDepth(ctx context.Context, p *config.ProviderConfig, conns, inflight int, nzbBytes []byte) float64 {
	client, err := buildAdHocClient(ctx, p, conns, inflight)
	if err != nil {
		return 0
	}
	defer client.Close()

	levelCtx, cancel := context.WithTimeout(ctx, pipelineLevelTimeout)
	defer cancel()

	res, err := client.SpeedTest(levelCtx, nntppool.SpeedTestOptions{
		NZBReader:   bytes.NewReader(nzbBytes),
		MaxSegments: pipelineTuneSegments,
	})
	if err != nil || res == nil || res.WireSpeedBps <= 0 {
		return 0
	}
	return res.WireSpeedBps / 1024 / 1024
}

func fetchSpeedTestNZB(ctx context.Context) ([]byte, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, nntppool.DefaultSpeedTestNZBURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("NZB HTTP %d", res.StatusCode)
	}
	return io.ReadAll(io.LimitReader(res.Body, 32*1024*1024))
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
