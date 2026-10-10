package rclonecli

import (
	"context"
	"fmt"
	"time"
)

const (
	// healthCheckInterval is how often MonitorMounts probes mount/rcd health.
	healthCheckInterval = 30 * time.Second

	// startupGracePeriod is the window after the RC server first becomes ready
	// during which the health check will NOT kill+restart the rcd subprocess.
	// The rcd is busiest while it initializes FUSE mounts right after startup,
	// which is exactly when a liveness probe is most likely to be slow; killing
	// it then is the regression we are guarding against.
	startupGracePeriod = 90 * time.Second

	// maxConsecutiveProbeFailures is how many back-to-back failed liveness
	// probes are required before the rcd subprocess is considered wedged and
	// restarted. Prevents a single transient miss from nuking a healthy rcd.
	//
	// This is the fallback for rclone.rcd_restart_after; see
	// restartAfterProbeFailures, which derives the count from that setting.
	maxConsecutiveProbeFailures = 3

	// probeTimeout bounds a single liveness probe.
	probeTimeout = 5 * time.Second
)

// restartAfterProbeFailures returns how many consecutive failed probes must
// accumulate before the rcd is restarted.
//
// The knob is expressed as a duration rather than a probe count because a count
// is meaningless without healthCheckInterval, and exposing both invites
// combinations that mean nothing. One duration, divided by the interval, keeps
// the two in step.
func (m *Manager) restartAfterProbeFailures() int {
	if m.cfg == nil {
		return maxConsecutiveProbeFailures
	}

	configured := m.cfg.GetConfig().RClone.RcdRestartAfter
	if configured == "" {
		return maxConsecutiveProbeFailures
	}

	after, err := time.ParseDuration(configured)
	if err != nil || after <= 0 {
		m.logger.WarnContext(m.ctx, "Ignoring unusable rclone.rcd_restart_after",
			"value", configured, "using_probe_threshold", maxConsecutiveProbeFailures)
		return maxConsecutiveProbeFailures
	}

	// Round up by dividing first and adjusting, rather than the usual
	// (after + interval - 1) / interval. That form overflows for a duration near
	// the maximum time.Duration, wrapping negative, and the clamp below would
	// then turn the longest tolerance anyone can express into a threshold of 1:
	// a restart on every failed probe, the exact opposite of what was asked for.
	threshold := int(after / healthCheckInterval)
	if after%healthCheckInterval != 0 {
		threshold++
	}

	// A value shorter than one interval still means "restart on the first
	// sustained failure" rather than "restart immediately, every tick".
	if threshold < 1 {
		threshold = 1
	}

	return threshold
}

// checkMountHealth checks if a specific mount is healthy
func (m *Manager) checkMountHealth(provider string) error {
	// Try to list the root directory of the mount
	req := RCRequest{
		Command: "operations/list",
		Args: map[string]any{
			"fs":     fmt.Sprintf("%s:", provider),
			"remote": "",
		},
	}

	_, err := m.makeRequest(req, true)
	return err
}

// RecoverMount attempts to recover a failed mount
func (m *Manager) RecoverMount(ctx context.Context, provider string) error {
	return m.recoverMount(ctx, provider, nil)
}

// recoverMount optionally limits health-triggered recovery to the mount instance
// that failed its probe. Public recovery requests do not impose this condition.
func (m *Manager) recoverMount(ctx context.Context, provider string, expected *MountInfo) error {
	m.recoveryMu.Lock()
	if m.recovering == nil {
		m.recovering = make(map[string]struct{})
	}
	if _, exists := m.recovering[provider]; exists {
		m.recoveryMu.Unlock()
		return nil
	}
	m.recovering[provider] = struct{}{}
	m.recoveryMu.Unlock()
	defer func() {
		m.recoveryMu.Lock()
		delete(m.recovering, provider)
		m.recoveryMu.Unlock()
	}()

	m.mountMu.Lock()
	defer m.mountMu.Unlock()

	m.mountsMutex.RLock()
	mountInfo, exists := m.mounts[provider]
	if expected != nil && (!exists || mountInfo != expected || !mountInfo.Mounted || !mountInfo.desired) {
		m.mountsMutex.RUnlock()
		return nil
	}
	m.mountsMutex.RUnlock()

	if !exists {
		return fmt.Errorf("mount for provider %s does not exist", provider)
	}

	m.logger.WarnContext(ctx, "Attempting to recover mount", "provider", provider)

	// Pre-recovery rcd liveness probe. If the rcd subprocess has wedged,
	// every subsequent RPC (mount/unmount, config/create, mount/mount) will
	// hang on context deadline exceeded. Kill+respawn rcd before issuing
	// recovery RPCs to break out of that wedge.
	if !m.probe(ctx, probeTimeout) {
		m.logger.WarnContext(ctx, "rcd unresponsive during recovery, restarting subprocess", "provider", provider)
		if err := m.restart(ctx); err != nil {
			return fmt.Errorf("failed to restart wedged rcd: %w", err)
		}
		// After restart there is nothing to RC-unmount; skip straight to Mount,
		// which will recreate the rclone config and FUSE mount on the fresh rcd.
		if err := m.mountWithRetry(ctx, provider, mountInfo.LocalPath, mountInfo.WebDAVURL, 3); err != nil {
			return fmt.Errorf("failed to recover mount for %s after rcd restart: %w", provider, err)
		}
		m.logger.InfoContext(ctx, "Successfully recovered mount after rcd restart", "provider", provider)
		return nil
	}

	// First try to unmount cleanly. The health checker leaves Mounted=true until
	// this call so unmount can still reach rclone and reclaim its VFS before the
	// remount below.
	if err := m.unmount(ctx, provider, true); err != nil {
		return fmt.Errorf("failed to unmount during recovery for %s: %w", provider, err)
	}

	// Wait a moment
	time.Sleep(1 * time.Second)

	// Try to remount
	if err := m.mountWithRetry(ctx, provider, mountInfo.LocalPath, mountInfo.WebDAVURL, 3); err != nil {
		return fmt.Errorf("failed to recover mount for %s: %w", provider, err)
	}

	m.logger.InfoContext(ctx, "Successfully recovered mount", "provider", provider)
	return nil
}

// MonitorMounts continuously monitors mount health and attempts recovery
func (m *Manager) MonitorMounts(ctx context.Context) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.DebugContext(ctx, "Mount monitoring stopped")
			return
		case <-ticker.C:
			m.performMountHealthCheck()
		}
	}
}

// performMountHealthCheck checks and attempts to recover unhealthy mounts
func (m *Manager) performMountHealthCheck() {
	if !m.IsReady() {
		return
	}

	// IsReady() only reflects startup state. Probe the rcd subprocess with a
	// bounded timeout so a wedged rcd is detected even when no individual
	// mount has failed yet.
	if m.probe(m.ctx, probeTimeout) {
		// Healthy probe: clear the failure streak and continue to per-mount checks.
		m.consecutiveProbeFailures = 0
	} else {
		m.consecutiveProbeFailures++
		restartAfter := m.restartAfterProbeFailures()

		// Never kill the rcd during the startup grace period: right after
		// startup it is busy initializing FUSE mounts, so a slow probe is
		// expected rather than a wedge. Killing it then is the regression we
		// guard against. Also require sustained failure before restarting so a
		// single transient miss can't nuke a healthy rcd.
		m.mu.RLock()
		readyAt := m.readyAt
		m.mu.RUnlock()
		withinGrace := !readyAt.IsZero() && time.Since(readyAt) < startupGracePeriod

		if withinGrace || m.consecutiveProbeFailures < restartAfter {
			m.logger.WarnContext(m.ctx, "rcd liveness probe failed; not restarting yet",
				"consecutive_failures", m.consecutiveProbeFailures,
				"threshold", restartAfter,
				"within_startup_grace", withinGrace)
			// Skip per-mount recovery this tick: if rcd is slow, operations/list
			// would fail too and could trigger a restart that bypasses this guard.
			return
		}

		m.logger.WarnContext(m.ctx, "rcd unresponsive during health check, restarting subprocess",
			"consecutive_failures", m.consecutiveProbeFailures)
		m.consecutiveProbeFailures = 0
		if err := m.restart(m.ctx); err != nil {
			m.logger.ErrorContext(m.ctx, "Failed to restart wedged rcd", "err", err)
			return
		}
		// restartServer marked all mounts as unmounted. Re-establish each one
		// against the fresh rcd; each Mount call is independent.
		m.mountsMutex.RLock()
		toRemount := make([]*MountInfo, 0, len(m.mounts))
		for _, mount := range m.mounts {
			if mount.desired {
				copy := *mount
				toRemount = append(toRemount, &copy)
			}
		}
		m.mountsMutex.RUnlock()

		for _, mount := range toRemount {
			info := mount
			go func() {
				if err := m.Mount(m.ctx, info.Provider, info.LocalPath, info.WebDAVURL); err != nil {
					m.logger.ErrorContext(m.ctx, "Failed to remount after rcd restart", "err", err, "provider", info.Provider)
				}
			}()
		}
		// Don't fall through to per-mount recovery on this tick; remounts are
		// in flight and the next tick will assess health.
		return
	}

	m.mountsMutex.RLock()
	mounts := make([]*MountInfo, 0, len(m.mounts))
	for _, mount := range m.mounts {
		if mount.Mounted {
			mounts = append(mounts, mount)
		}
	}
	m.mountsMutex.RUnlock()

	restartAfter := m.restartAfterProbeFailures()
	for _, mount := range mounts {
		provider := mount.Provider
		err := m.checkMountHealth(provider)

		m.mountsMutex.Lock()
		// A mount may have been replaced or unmounted while the RPC was in flight.
		// Its result belongs only to the mount instance that was checked.
		if m.mounts[provider] != mount || !mount.Mounted {
			m.mountsMutex.Unlock()
			continue
		}
		if err == nil {
			mount.consecutiveHealthFailures = 0
			mount.Error = ""
			m.mountsMutex.Unlock()
			continue
		}

		mount.consecutiveHealthFailures++
		failures := mount.consecutiveHealthFailures
		// Keep Mounted=true so recovery can unmount and reclaim rclone's VFS.
		mount.Error = fmt.Sprintf("Health check failed: %v", err)
		if failures >= restartAfter {
			mount.consecutiveHealthFailures = 0
		}
		m.mountsMutex.Unlock()

		if failures < restartAfter {
			m.logger.WarnContext(m.ctx, "Mount health check failed; not recovering yet",
				"provider", provider, "err", err,
				"consecutive_failures", failures, "threshold", restartAfter)
			continue
		}

		m.logger.WarnContext(m.ctx, "Mount health check failed, attempting recovery",
			"provider", provider, "err", err,
			"consecutive_failures", failures, "threshold", restartAfter)
		go func(provider string, expected *MountInfo) {
			if err := m.recoverMount(m.ctx, provider, expected); err != nil {
				m.logger.ErrorContext(m.ctx, "Failed to recover mount", "err", err, "provider", provider)
			}
		}(provider, mount)
	}
}
