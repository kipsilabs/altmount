package api

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
)

// lastMissingWarnTime tracks the last time a missing article warning was logged per provider.
var lastMissingWarnTime sync.Map

// handleGetSystemStats handles GET /api/system/stats
//
//	@Summary		Get system statistics
//	@Description	Returns combined queue, health, and system information statistics.
//	@Tags			System
//	@Produce		json
//	@Success		200	{object}	APIResponse{data=SystemStatsResponse}
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/system/stats [get]
func (s *Server) handleGetSystemStats(c *fiber.Ctx) error {
	// Get queue statistics
	queueStats, err := s.queueRepo.GetQueueStats(c.Context())
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve queue statistics", err.Error())
	}

	// Get health statistics
	healthStatsMap, err := s.healthRepo.GetHealthStats(c.Context())
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve health statistics", err.Error())
	}

	// Convert to response format
	response := SystemStatsResponse{
		Queue:  *ToQueueStatsResponse(queueStats),
		Health: *ToHealthStatsResponse(healthStatsMap),
		System: s.getSystemInfo(),
	}

	return RespondSuccess(c, response)
}

// handleGetSystemHealth handles GET /api/system/health
//
//	@Summary		Get system health
//	@Description	Returns health status of all system components (database, workers, etc.).
//	@Tags			System
//	@Produce		json
//	@Success		200	{object}	APIResponse{data=SystemHealthResponse}
//	@Security		BearerAuth
//	@Router			/system/health [get]
func (s *Server) handleGetSystemHealth(c *fiber.Ctx) error {
	// Perform health checks
	healthCheck := s.checkSystemHealth(c.Context())

	// Set appropriate HTTP status code based on health
	switch healthCheck.Status {
	case "healthy":
		return RespondSuccess(c, healthCheck)
	case "degraded":
		// Return 200 but indicate degraded status
		return RespondSuccess(c, healthCheck)
	case "unhealthy":
		// Return 503 Service Unavailable for unhealthy status
		c.Set("Retry-After", "10")
		return c.Status(503).JSON(fiber.Map{
			"success": false,
			"data":    healthCheck,
		})
	}

	// Default case (shouldn't reach here)
	return RespondInternalError(c, "Unknown health status", "")
}

// handleSystemCleanup handles POST /api/system/cleanup
//
//	@Summary		System cleanup
//	@Description	Removes old queue items and health records based on age. Supports dry-run mode.
//	@Tags			System
//	@Accept			json
//	@Produce		json
//	@Param			body	body		SystemCleanupRequest	false	"Cleanup criteria"
//	@Success		200		{object}	APIResponse{data=SystemCleanupResponse}
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/system/cleanup [post]
func (s *Server) handleSystemCleanup(c *fiber.Ctx) error {
	// Parse request body
	var req SystemCleanupRequest
	if len(c.Body()) > 0 {
		if err := c.BodyParser(&req); err != nil {
			return RespondBadRequest(c, "Invalid request body", err.Error())
		}
	}

	// Parse parameters from query string if not in body
	if req.QueueOlderThan == nil {
		if queueOlderThan, err := ParseTimeParamFiber(c, "queue_older_than"); err != nil {
			return RespondError(c, 422, "VALIDATION_ERROR", "Invalid queue_older_than parameter", err.Error())
		} else if queueOlderThan != nil {
			req.QueueOlderThan = queueOlderThan
		}
	}

	if req.HealthOlderThan == nil {
		if healthOlderThan, err := ParseTimeParamFiber(c, "health_older_than"); err != nil {
			return RespondError(c, 422, "VALIDATION_ERROR", "Invalid health_older_than parameter", err.Error())
		} else if healthOlderThan != nil {
			req.HealthOlderThan = healthOlderThan
		}
	}

	// Parse dry_run parameter
	if dryRunStr := c.Query("dry_run"); dryRunStr != "" {
		req.DryRun = dryRunStr == "true"
	}

	// Set default cleanup times if not specified
	if req.QueueOlderThan == nil {
		// Default: clean queue items older than 7 days
		defaultTime := time.Now().Add(-7 * 24 * time.Hour)
		req.QueueOlderThan = &defaultTime
	}

	if req.HealthOlderThan == nil {
		// Default: clean health records older than 30 days
		defaultTime := time.Now().Add(-30 * 24 * time.Hour)
		req.HealthOlderThan = &defaultTime
	}

	// Perform cleanup operations
	var queueItemsRemoved, healthRecordsRemoved int
	var err error

	// Clean up queue items
	if !req.DryRun {
		var paths []string
		paths, queueItemsRemoved, err = s.queueRepo.ClearCompletedQueueItems(c.Context())
		if err != nil {
			return RespondInternalError(c, "Failed to cleanup queue items", err.Error())
		}
		s.removeQueueNzbFiles(c, paths)
	} else {
		// For dry run, we could count what would be removed
		// For now, we'll just return 0
		queueItemsRemoved = 0
	}

	// Clean up health records
	if !req.DryRun {
		// The current health repository doesn't have a time-based cleanup method
		// We'll need to implement this or return 0 for now
		healthRecordsRemoved = 0
	} else {
		healthRecordsRemoved = 0
	}

	// Prepare response
	response := SystemCleanupResponse{
		QueueItemsRemoved:    queueItemsRemoved,
		HealthRecordsRemoved: healthRecordsRemoved,
		DryRun:               req.DryRun,
	}

	return RespondSuccess(c, response)
}

// handleSystemRestart handles POST /api/system/restart
//
//	@Summary		Restart system
//	@Description	Schedules a graceful system restart. Use force=true to skip safety checks.
//	@Tags			System
//	@Accept			json
//	@Produce		json
//	@Param			body	body		SystemRestartRequest	false	"Restart options"
//	@Success		200		{object}	APIResponse{data=SystemRestartResponse}
//	@Security		BearerAuth
//	@Router			/system/restart [post]
func (s *Server) handleSystemRestart(c *fiber.Ctx) error {
	// Parse request body if present
	var req SystemRestartRequest
	if len(c.Body()) > 0 {
		if err := c.BodyParser(&req); err != nil {
			return RespondBadRequest(c, "Invalid request body", err.Error())
		}
	}

	slog.InfoContext(c.Context(), "System restart requested", "force", req.Force, "user_agent", c.Get("User-Agent"))

	// Prepare response
	response := SystemRestartResponse{
		Message:   "Server restart initiated. The server will restart shortly.",
		Timestamp: time.Now(),
	}

	// Send response immediately before restart
	result := RespondSuccess(c, response)

	// Start restart process in a goroutine to allow response to be sent
	go s.performRestart(c.Context())

	return result
}

// handleResetSystemStats handles POST /api/system/stats/reset
//
//	@Summary		Reset system statistics
//	@Description	Resets accumulated system statistics counters (pool metrics, download counters).
//	@Tags			System
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/system/stats/reset [post]
func (s *Server) handleResetSystemStats(c *fiber.Ctx) error {
	ctx := c.Context()
	durationStr := c.Query("duration")
	resetPeak := c.Query("reset_peak") == "true"
	resetTotals := c.Query("reset_totals") == "true"
	resetHistory := c.Query("reset_history") == "true"
	resetQueue := c.Query("reset_queue") == "true"
	resetProviderErrors := c.Query("reset_provider_errors") == "true"

	// If no flags are provided, default to full reset
	if !resetPeak && !resetTotals && !resetHistory && !resetQueue && !resetProviderErrors && durationStr == "" {
		resetPeak = true
		resetTotals = true
		resetHistory = true
	}

	// If duration is provided and not "all", perform granular reset for history
	if durationStr != "" && durationStr != "all" {
		duration, err := ParseDuration(durationStr)
		if err != nil {
			return RespondBadRequest(c, "Invalid duration format", err.Error())
		}

		since := time.Now().Add(-duration)

		if s.queueRepo != nil && resetHistory {
			// Clear dashboard stats only, preserve Sonarr history records
			if err := s.queueRepo.ClearDailyStatsSince(ctx, since); err != nil {
				return RespondInternalError(c, "Failed to reset dashboard statistics", err.Error())
			}
		}

		return RespondMessage(c, fmt.Sprintf("Dashboard statistics for last %s reset successfully", durationStr))
	}

	// Full/Granular reset
	// Reset pool metrics (NNTP errors, totals, peak)
	if s.poolManager != nil && (resetTotals || resetPeak) {
		if err := s.poolManager.ResetMetrics(ctx, resetPeak, resetTotals); err != nil {
			return RespondInternalError(c, "Failed to reset pool metrics", err.Error())
		}
	}

	// Reset only provider error counts
	if s.poolManager != nil && resetProviderErrors {
		if err := s.poolManager.ResetProviderErrors(ctx); err != nil {
			return RespondInternalError(c, "Failed to reset provider error counts", err.Error())
		}
	}

	// Reset import history and daily stats
	if s.queueRepo != nil {
		if resetHistory {
			// Clear dashboard totals but KEEP the actual history records for Sonarr deduplication
			if err := s.queueRepo.ClearDailyStats(ctx); err != nil {
				return RespondInternalError(c, "Failed to reset dashboard statistics", err.Error())
			}
		}

		// Optional: Clear completed/failed queue items too if requested
		if resetQueue {
			completedPaths, _, err := s.queueRepo.ClearCompletedQueueItems(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to clear completed queue items during reset", "error", err)
			} else {
				s.removeQueueNzbFiles(c, completedPaths)
			}
			failedPaths, _, err := s.queueRepo.ClearFailedQueueItems(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to clear failed queue items during reset", "error", err)
			} else {
				s.removeQueueNzbFiles(c, failedPaths)
			}
		}
	}

	return RespondMessage(c, "System statistics reset successfully")
}

// performRestart performs the actual server restart
func (s *Server) performRestart(ctx context.Context) {
	slog.InfoContext(ctx, "Initiating server restart process")

	// Give a moment for the HTTP response to be sent
	time.Sleep(100 * time.Millisecond)

	// Stop the rclone mount service before re-execing so the child rclone
	// subprocess is terminated cleanly and releases its log file. This is
	// critical on Windows where syscall.Exec is unavailable: the fallback
	// path calls os.Exit, which skips the SIGINT shutdown sequence and would
	// otherwise orphan the rclone process (keeping rclone.log locked and
	// breaking the next startup). Use a fresh context with its own timeout
	// because the request context is already canceled by this point.
	if s.mountService != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := s.mountService.Stop(stopCtx); err != nil {
			slog.WarnContext(stopCtx, "Failed to stop mount service before restart", "error", err)
		} else {
			slog.InfoContext(stopCtx, "Mount service stopped for restart")
		}
		stopCancel()
	}

	// Get the current executable path
	executable, err := os.Executable()
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get executable path for restart", "error", err)
		return
	}

	slog.InfoContext(ctx, "Restarting server", "executable", executable, "args", os.Args)

	// Use syscall.Exec to replace the current process
	// This preserves the process ID and is the cleanest way to restart
	err = syscall.Exec(executable, os.Args, os.Environ())
	if err != nil {
		slog.ErrorContext(ctx, "Failed to restart using syscall.Exec, trying exec.Command", "error", err)

		// Fallback: use exec.Command (this creates a new process)
		cmd := exec.CommandContext(ctx, executable, os.Args[1:]...)
		cmd.Env = os.Environ()

		if err := cmd.Start(); err != nil {
			slog.ErrorContext(ctx, "Failed to restart server using exec.Command", "error", err)
			return
		}

		slog.InfoContext(ctx, "Server restart initiated with new process", "pid", cmd.Process.Pid)

		// Exit the current process
		os.Exit(0)
	}
}

// handleGetProviderHistoricalStats returns historical data usage per provider
//
//	@Summary		Get provider historical data usage stats
//	@Description	Returns aggregated data usage per provider over the given number of days.
//	@Tags			System
//	@Produce		json
//	@Param			days	query		int	false	"Number of days to fetch data for (default: 30)"
//	@Param			interval query		string false "Aggregation interval (daily, weekly, monthly, yearly)"
//	@Success		200		{object}	APIResponse{data=ProviderHistoricalStatsResponse}
//	@Security		BearerAuth
//	@Router			/system/provider-stats [get]
func (s *Server) handleGetProviderHistoricalStats(c *fiber.Ctx) error {
	daysStr := c.Query("days", "30")
	days, err := strconv.Atoi(daysStr)
	if err != nil || days <= 0 {
		days = 30
	}

	interval := c.Query("interval", "daily")
	switch interval {
	case "daily", "weekly", "monthly", "yearly":
		// Valid
	default:
		interval = "daily"
	}

	// Maximum 5 years to prevent excessive querying
	if days > 1825 {
		days = 1825
	}

	stats, err := s.queueRepo.GetProviderHistoricalStats(c.Context(), days, interval)
	if err != nil {
		return RespondInternalError(c, "Failed to get provider historical stats", err.Error())
	}

	var responseStats []ProviderHistoricalStatResponse
	for _, stat := range stats {
		responseStats = append(responseStats, ProviderHistoricalStatResponse{
			Timestamp:       stat.Timestamp,
			ProviderID:      stat.ProviderID,
			BytesDownloaded: stat.BytesDownloaded,
		})
	}

	if responseStats == nil {
		responseStats = make([]ProviderHistoricalStatResponse, 0)
	}

	return RespondSuccess(c, ProviderHistoricalStatsResponse{
		Stats: responseStats,
	})
}

// handleGetProviderSpeedHistory returns historical speed test results per provider
//
//	@Summary		Get provider speed test history
//	@Description	Returns speed test history per provider over the given number of days.
//	@Tags			System
//	@Produce		json
//	@Param			days	query		int	false	"Number of days to fetch data for (default: 30)"
//	@Success		200		{object}	APIResponse{data=ProviderSpeedTestHistoryResponse}
//	@Security		BearerAuth
//	@Router			/system/provider-speed-history [get]
func (s *Server) handleGetProviderSpeedHistory(c *fiber.Ctx) error {
	daysStr := c.Query("days", "30")
	days, err := strconv.Atoi(daysStr)
	if err != nil || days <= 0 {
		days = 30
	}

	if days > 1825 {
		days = 1825
	}

	stats, err := s.queueRepo.GetProviderSpeedTestHistory(c.Context(), days)
	if err != nil {
		return RespondInternalError(c, "Failed to get provider speed history", err.Error())
	}

	var history []ProviderSpeedTestHistoryStat
	for _, stat := range stats {
		history = append(history, ProviderSpeedTestHistoryStat{
			ID:         stat.ID,
			ProviderID: stat.ProviderID,
			SpeedMbps:  stat.SpeedMbps,
			CreatedAt:  stat.CreatedAt,
		})
	}

	if history == nil {
		history = make([]ProviderSpeedTestHistoryStat, 0)
	}

	return RespondSuccess(c, ProviderSpeedTestHistoryResponse{
		History: history,
	})
}

// handleGetPoolMetrics handles GET /api/system/pool/metrics
//
//	@Summary		Get NNTP pool metrics
//	@Description	Returns download/upload metrics, speed, and per-provider connection statistics.
//	@Tags			System
//	@Produce		json
//	@Success		200	{object}	APIResponse{data=PoolMetricsResponse}
//	@Security		BearerAuth
//	@Router			/system/pool/metrics [get]
func (s *Server) handleGetPoolMetrics(c *fiber.Ctx) error {
	// Check if pool manager is available
	if s.poolManager == nil {
		return RespondInternalError(c, "Pool manager not available", "NNTP pool manager not configured")
	}

	// Check if pool is available
	if !s.poolManager.HasPool() {
		response := PoolMetricsResponse{
			BytesDownloaded:          0,
			BytesUploaded:            0,
			ArticlesDownloaded:       0,
			ArticlesPosted:           0,
			TotalErrors:              0,
			ProviderErrors:           make(map[string]int64),
			DownloadSpeedBytesPerSec: 0.0,
			UploadSpeedBytesPerSec:   0.0,
			Timestamp:                time.Now(),
			StartedAt:                time.Now(),
			Providers:                []ProviderStatusResponse{},
		}
		return RespondSuccess(c, response)
	}

	// Get metrics from the pool manager (includes calculated speeds)
	metrics, err := s.poolManager.GetMetrics()
	if err != nil {
		return RespondInternalError(c, "Failed to get NNTP pool metrics", err.Error())
	}

	// Get the pool to fetch provider stats
	pool, err := s.poolManager.GetPool()
	if err != nil {
		return RespondInternalError(c, "Failed to get NNTP pool", err.Error())
	}

	// Get provider stats from pool (v4 API)
	poolStats := pool.Stats()

	// Get current configuration to access provider details and speed test results
	config := s.configManager.GetConfig()

	// Calculate total speed from all providers to use for proportional scaling
	var totalProviderSpeed float64
	for _, ps := range poolStats.Providers {
		totalProviderSpeed += ps.AvgSpeed
	}

	// Build provider response from pool stats + config
	providers := make([]ProviderStatusResponse, 0, len(poolStats.Providers))
	for _, ps := range poolStats.Providers {
		// Try to find matching provider in config for additional details
		var providerID string
		var host string
		var lastSpeedTestMbps float64
		var lastSpeedTestTime *time.Time

		if config != nil {
			for _, p := range config.Providers {
				// Match by the stable provider ID assigned to the nntppool entry.
				if ps.Name == p.NNTPPoolName() {
					providerID = p.ID
					host = p.Host
					lastSpeedTestMbps = p.LastSpeedTestMbps
					lastSpeedTestTime = p.LastSpeedTestTime
					break
				}
			}
		}

		// Fallback: use pool stats name if config match failed
		if host == "" {
			host = ps.Name
		}
		if providerID == "" {
			providerID = ps.Name
		}

		// Get error count from metrics (sum from both names if they differ)
		errorCount := int64(0)
		if metrics.ProviderErrors != nil {
			errorCount += metrics.ProviderErrors[ps.Name]
			if providerID != ps.Name {
				errorCount += metrics.ProviderErrors[providerID]
			}
		}

		// Get byte count from metrics (sum from both names if they differ)
		byteCount := int64(0)
		if metrics.ProviderBytes != nil {
			byteCount += metrics.ProviderBytes[ps.Name]
			if providerID != ps.Name {
				byteCount += metrics.ProviderBytes[providerID]
			}
		}

		// Get 24h byte count from metrics (sum from both names if they differ)
		byteCount24h := int64(0)
		if metrics.ProviderBytes24h != nil {
			byteCount24h += metrics.ProviderBytes24h[ps.Name]
			if providerID != ps.Name {
				byteCount24h += metrics.ProviderBytes24h[providerID]
			}
		}

		// Get the earliest started_at date between the two names
		startedAt := metrics.ProviderStartedAt[ps.Name]
		if providerID != ps.Name {
			if oldStartedAt, exists := metrics.ProviderStartedAt[providerID]; exists {
				if startedAt.IsZero() || (!oldStartedAt.IsZero() && oldStartedAt.Before(startedAt)) {
					startedAt = oldStartedAt
				}
			}
		}

		// Final fallback: if both are zero, use global startedAt
		if startedAt.IsZero() {
			startedAt = metrics.StartedAt
		}

		// Get missing rate and warning from metrics snapshot
		missingRate := metrics.ProviderMissingRates[ps.Name]
		missingWarning := metrics.ProviderMissingWarning[ps.Name]

		// Calculate proportional speed
		// We use our accurate global speed and distribute it based on pool's relative provider speeds
		currentProviderSpeed := ps.AvgSpeed
		if totalProviderSpeed > 0 && metrics.DownloadSpeedBytesPerSec > 0 {
			weight := ps.AvgSpeed / totalProviderSpeed
			currentProviderSpeed = metrics.DownloadSpeedBytesPerSec * weight
		}

		prov := ProviderStatusResponse{
			ID:                      providerID,
			Host:                    host,
			UsedConnections:         ps.ActiveConnections,
			MaxConnections:          ps.MaxConnections,
			State:                   "active",
			ErrorCount:              errorCount,
			ByteCount:               byteCount,
			ByteCount24h:            byteCount24h,
			StartedAt:               startedAt,
			CurrentSpeedBytesPerSec: currentProviderSpeed,
			PingMs:                  ps.Ping.RTT.Milliseconds(),
			LastSpeedTestMbps:       lastSpeedTestMbps,
			LastSpeedTestTime:       lastSpeedTestTime,
			MissingCount:            ps.Missing,
			MissingRatePerMinute:    missingRate,
			MissingWarning:          missingWarning,
		}

		if q, ok := metrics.ProviderQuotas[ps.Name]; ok {
			prov.QuotaBytes = q.QuotaBytes
			prov.QuotaUsed = q.QuotaUsed
			if !q.QuotaResetAt.IsZero() {
				t := q.QuotaResetAt
				prov.QuotaResetAt = &t
			}
			prov.QuotaExceeded = q.QuotaExceeded
		}

		providers = append(providers, prov)

		// Rate-limited warning logging (at most once per 60s per provider)
		if missingWarning {
			const warnCooldown = 60 * time.Second
			now := time.Now()
			if lastWarn, ok := lastMissingWarnTime.Load(ps.Name); !ok || now.Sub(lastWarn.(time.Time)) >= warnCooldown {
				lastMissingWarnTime.Store(ps.Name, now)
				slog.WarnContext(c.Context(), "NNTP provider has high missing article rate — consider using a backup provider",
					"provider", host,
					"missing_count", ps.Missing,
					"missing_rate_per_minute", fmt.Sprintf("%.1f", missingRate),
				)
			}
		}
	}

	// Get last 24h stats for download volume (strict rolling 24h)
	var bytesDownloaded24h int64
	if s.queueRepo != nil {
		hourlyStats, err := s.queueRepo.GetImportHourlyStats(c.Context(), 24)
		if err == nil {
			for _, hs := range hourlyStats {
				bytesDownloaded24h += hs.BytesDownloaded
			}
		}
	}

	// Map pool metrics to API response format
	response := PoolMetricsResponse{
		BytesDownloaded:             metrics.BytesDownloaded,
		BytesDownloaded24h:          bytesDownloaded24h,
		BytesUploaded:               metrics.BytesUploaded,
		ArticlesDownloaded:          metrics.ArticlesDownloaded,
		ArticlesPosted:              metrics.ArticlesPosted,
		TotalErrors:                 metrics.TotalErrors,
		ProviderErrors:              metrics.ProviderErrors,
		ProviderBytes:               metrics.ProviderBytes,
		DownloadSpeedBytesPerSec:    metrics.DownloadSpeedBytesPerSec,
		MaxDownloadSpeedBytesPerSec: metrics.MaxDownloadSpeedBytesPerSec,
		UploadSpeedBytesPerSec:      metrics.UploadSpeedBytesPerSec,
		Timestamp:                   metrics.Timestamp,
		StartedAt:                   metrics.StartedAt,
		Providers:                   providers,
	}

	return RespondSuccess(c, response)
}

// FileEntry represents a file or directory in the system browser
type FileEntry struct {
	Name    string    `json:"name"`
	Path    string    `json:"path"`
	IsDir   bool      `json:"is_dir"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

// handleSystemBrowse handles GET /api/system/browse
//
//	@Summary		Browse filesystem
//	@Description	Lists filesystem entries at a given path for directory/file picker UIs.
//	@Tags			System
//	@Produce		json
//	@Param			path	query		string	false	"Directory path to browse (defaults to root)"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/system/browse [get]
func (s *Server) handleSystemBrowse(c *fiber.Ctx) error {
	path := c.Query("path")
	if path == "" {
		// Default to root or current working directory
		var err error
		path, err = os.Getwd()
		if err != nil {
			path = "/"
		}
	}

	// Sanitize and validate the path to prevent path traversal
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return RespondBadRequest(c, "Path must be absolute", "")
	}

	// Read directory
	entries, err := os.ReadDir(path)
	if err != nil {
		return RespondInternalError(c, "Failed to read directory", err.Error())
	}

	var files []FileEntry
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Skip hidden files if desired, but for system browsing we might want them
		// For now, let's keep them

		files = append(files, FileEntry{
			Name:    entry.Name(),
			Path:    filepath.Join(path, entry.Name()),
			IsDir:   entry.IsDir(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		})
	}

	return RespondSuccess(c, fiber.Map{
		"current_path": path,
		"parent_path":  filepath.Dir(path),
		"files":        files,
	})
}
