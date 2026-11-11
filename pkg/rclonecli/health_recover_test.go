package rclonecli

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// recoveryTestManager is a health test manager with a recoverable mount and a
// counting mount stub, so RecoverMount's decisions can be observed.
func recoveryTestManager(t *testing.T, probeOK bool) (*Manager, *int32, *int32) {
	t.Helper()
	m, restarts := newHealthTestManager(t, probeOK, afterGrace())
	var mounts int32
	m.mounts["altmount"] = &MountInfo{Provider: "altmount", LocalPath: "/mnt/test", Mounted: true, desired: true}
	m.mountFn = func(context.Context, string, string, string) error {
		atomic.AddInt32(&mounts, 1)
		return nil
	}
	m.forceUnmount = func(string) error { return nil }
	m.retryDelay = func(int) time.Duration { return time.Millisecond }
	return m, restarts, &mounts
}

// RecoverMount must not restart the rcd after a single failed probe, which
// would bypass both rclone.rcd_restart_after and the startup grace window. It
// must leave the restart decision to the health check, whose streak is
// measured over the configured duration, and must not touch the mount while
// the rcd is down.
func TestRecoverMount_UnresponsiveRcdIsLeftToTheHealthCheck(t *testing.T) {
	m, restarts, mounts := recoveryTestManager(t, false)

	if err := m.RecoverMount(context.Background(), "altmount"); err == nil {
		t.Fatal("expected an error when the rcd does not answer")
	}
	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("RecoverMount restarted the rcd itself, got %d restarts", got)
	}
	if got := atomic.LoadInt32(mounts); got != 0 {
		t.Fatalf("RecoverMount attempted %d mounts against an unresponsive rcd", got)
	}
}

// The restart still happens, through the health check, once the rcd has been
// unresponsive for the configured duration.
func TestRecoverMount_ThenHealthCheckRestartsAtConfiguredThreshold(t *testing.T) {
	m, restarts, _ := recoveryTestManager(t, false)
	// 150s at a 30s interval is 6 probes: the first failed probe at t=0 plus
	// one per interval (see restartAfterProbeFailures).
	withRcdRestartAfter(t, m, "150s")

	_ = m.RecoverMount(context.Background(), "altmount")
	for range 5 {
		m.performMountHealthCheck()
	}
	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("restarted after 5 failed ticks with a threshold of 6, got %d", got)
	}
	m.performMountHealthCheck()
	if got := atomic.LoadInt32(restarts); got != 1 {
		t.Fatalf("restarts = %d at the configured threshold, want 1", got)
	}
}

// With a responsive rcd, recovery proceeds as before: unmount, then remount,
// without restarting the rcd.
func TestRecoverMount_ResponsiveRcdRemountsWithoutRestart(t *testing.T) {
	m, restarts, mounts := recoveryTestManager(t, true)
	m.rcPort = "5572"
	m.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}")), Header: http.Header{}}, nil
	})}

	if err := m.RecoverMount(context.Background(), "altmount"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("restarts = %d for a responsive rcd, want 0", got)
	}
	if got := atomic.LoadInt32(mounts); got != 1 {
		t.Fatalf("mount attempts = %d, want 1", got)
	}
}
