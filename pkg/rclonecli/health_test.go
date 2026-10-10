package rclonecli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
)

// newHealthTestManager builds a minimal Manager wired only with the fields
// performMountHealthCheck touches, plus injectable probe/restart seams so the
// restart decision can be asserted without a live rcd subprocess.
func newHealthTestManager(t *testing.T, probeOK bool, readyAt time.Time) (*Manager, *int32) {
	t.Helper()

	ready := make(chan struct{})
	close(ready) // IsReady() == true

	var restartCalls int32
	m := &Manager{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ctx:           context.Background(),
		mounts:        make(map[string]*MountInfo),
		serverReady:   ready,
		serverStarted: true,
		readyAt:       readyAt,
		probe: func(context.Context, time.Duration) bool {
			return probeOK
		},
	}
	m.restart = func(context.Context) error {
		atomic.AddInt32(&restartCalls, 1)
		return nil
	}
	return m, &restartCalls
}

// afterGrace returns a readyAt timestamp old enough that the startup grace
// period has elapsed.
func afterGrace() time.Time {
	return time.Now().Add(-2 * startupGracePeriod)
}

func TestPerformMountHealthCheck_SuccessResetsFailureStreak(t *testing.T) {
	m, restarts := newHealthTestManager(t, true, afterGrace())
	m.consecutiveProbeFailures = 2

	m.performMountHealthCheck()

	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("healthy probe must not restart rcd, got %d restarts", got)
	}
	if m.consecutiveProbeFailures != 0 {
		t.Fatalf("healthy probe must reset failure streak, got %d", m.consecutiveProbeFailures)
	}
}

func TestPerformMountHealthCheck_WithinGraceNeverRestarts(t *testing.T) {
	// readyAt = now -> firmly inside the startup grace period.
	m, restarts := newHealthTestManager(t, false, time.Now())

	// Even well past the failure threshold, no restart may happen during grace.
	for range maxConsecutiveProbeFailures + 2 {
		m.performMountHealthCheck()
	}

	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("must not restart rcd during startup grace period, got %d restarts", got)
	}
}

func TestPerformMountHealthCheck_BelowThresholdDoesNotRestart(t *testing.T) {
	m, restarts := newHealthTestManager(t, false, afterGrace())

	for range maxConsecutiveProbeFailures - 1 {
		m.performMountHealthCheck()
	}

	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("must not restart below threshold, got %d restarts", got)
	}
	if m.consecutiveProbeFailures != maxConsecutiveProbeFailures-1 {
		t.Fatalf("failure streak = %d, want %d", m.consecutiveProbeFailures, maxConsecutiveProbeFailures-1)
	}
}

func TestPerformMountHealthCheck_AtThresholdRestartsOnceAndResets(t *testing.T) {
	m, restarts := newHealthTestManager(t, false, afterGrace())

	for range maxConsecutiveProbeFailures {
		m.performMountHealthCheck()
	}

	if got := atomic.LoadInt32(restarts); got != 1 {
		t.Fatalf("expected exactly 1 restart at threshold, got %d", got)
	}
	if m.consecutiveProbeFailures != 0 {
		t.Fatalf("failure streak must reset after restart, got %d", m.consecutiveProbeFailures)
	}
}

func TestPerformMountHealthCheck_NotReadyIsNoOp(t *testing.T) {
	m, restarts := newHealthTestManager(t, false, afterGrace())
	m.mu.Lock()
	m.serverReady = make(chan struct{}) // open -> IsReady() == false
	m.mu.Unlock()

	m.performMountHealthCheck()

	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("must not restart before server is ready, got %d restarts", got)
	}
	if m.consecutiveProbeFailures != 0 {
		t.Fatalf("must not touch failure streak before ready, got %d", m.consecutiveProbeFailures)
	}
}

func TestPerformMountHealthCheck_LeavesFailedMountMarkedMountedForRecovery(t *testing.T) {
	m, _ := newHealthTestManager(t, true, afterGrace())
	m.mounts["altmount"] = &MountInfo{Provider: "altmount", Mounted: true}
	m.forceUnmount = func(string) error { return nil }
	m.restart = func(context.Context) error { return errors.New("stop recovery") }
	m.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("health check failed")
	})}

	m.performMountHealthCheck()

	info, ok := m.GetMountInfo("altmount")
	if !ok {
		t.Fatal("mount info missing")
	}
	if !info.Mounted {
		t.Fatal("failed mount must remain marked mounted until RecoverMount can unmount and reclaim its VFS")
	}
	if !strings.Contains(info.Error, "health check failed") {
		t.Fatalf("unexpected mount error: %q", info.Error)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// withRcdRestartAfter wires a config into the test manager so the derived
// threshold can be asserted.
func withRcdRestartAfter(t *testing.T, m *Manager, value string) {
	t.Helper()

	cfg := &config.Config{}
	cfg.RClone.RcdRestartAfter = value
	m.cfg = config.NewManager(cfg, "")
}

func TestRestartAfterProbeFailures_DerivesCountFromDuration(t *testing.T) {
	for _, tc := range []struct {
		configured string
		want       int
		why        string
	}{
		{"90s", 3, "the default, and the previous hard-coded behaviour"},
		{"30s", 1, "exactly one interval"},
		{"5m", 10, "a tolerant install riding out a long stall"},
		{"45s", 2, "rounds up rather than truncating to one interval"},
		{"1s", 1, "shorter than an interval still means one sustained failure"},
		{"", 3, "unset falls back to the built-in default"},
		{"nonsense", 3, "unparseable falls back rather than disabling the guard"},
		{"-30s", 3, "negative falls back rather than restarting every tick"},
		// Near time.Duration's maximum. The obvious (x+interval-1)/interval form
		// overflows here and collapses to 1, turning the longest tolerance
		// expressible into a restart on every failed probe.
		{"2562047h47m16.854775807s", 307445735, "an absurd but valid duration must not invert into no tolerance"},
	} {
		m, _ := newHealthTestManager(t, false, time.Time{})
		withRcdRestartAfter(t, m, tc.configured)

		if got := m.restartAfterProbeFailures(); got != tc.want {
			t.Errorf("rcd_restart_after=%q gave threshold %d, want %d (%s)",
				tc.configured, got, tc.want, tc.why)
		}
	}
}

func TestRestartAfterProbeFailures_NoConfigUsesDefault(t *testing.T) {
	m, _ := newHealthTestManager(t, false, time.Time{})
	if got := m.restartAfterProbeFailures(); got != maxConsecutiveProbeFailures {
		t.Errorf("with no config wired, threshold = %d, want %d", got, maxConsecutiveProbeFailures)
	}
}

// TestPerformMountHealthCheck_HonoursConfiguredTolerance is the point of the
// change: an install that configures more tolerance rides out a stall that the
// default would have restarted the rcd for, tearing the mount out from under
// every reader.
func TestPerformMountHealthCheck_HonoursConfiguredTolerance(t *testing.T) {
	m, restarts := newHealthTestManager(t, false, time.Now().Add(-time.Hour))
	withRcdRestartAfter(t, m, "5m") // 10 probes

	for range maxConsecutiveProbeFailures + 2 {
		m.performMountHealthCheck()
	}
	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("restarted %d times past the default threshold; the configured tolerance was ignored", got)
	}

	for range 5 {
		m.performMountHealthCheck()
	}
	if got := atomic.LoadInt32(restarts); got != 1 {
		t.Fatalf("restarts = %d after reaching the configured threshold, want 1", got)
	}
}

// Recovery uses the real RecoverMount path, stopping at the external force
// unmount boundary so no FUSE mount or subprocess is needed.
func newMountHealthTestManager(t *testing.T) (*Manager, map[string]bool, chan string) {
	t.Helper()
	m, _ := newHealthTestManager(t, true, afterGrace())
	failures := map[string]bool{"altmount": true}
	recoveries := make(chan string, 10)
	m.mounts["altmount"] = &MountInfo{Provider: "altmount", LocalPath: "altmount", Mounted: true, desired: true}
	m.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/operations/list" {
			var args map[string]any
			if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
				return nil, err
			}
			provider := strings.TrimSuffix(args["fs"].(string), ":")
			if !failures[provider] {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}
			return nil, errors.New("WebDAV temporarily unavailable")
		}
		return nil, errors.New("stop RC unmount")
	})}
	m.forceUnmount = func(path string) error {
		recoveries <- path
		return errors.New("stop recovery before remount")
	}
	return m, failures, recoveries
}

func assertNoMountRecovery(t *testing.T, recoveries <-chan string) {
	t.Helper()
	select {
	case provider := <-recoveries:
		t.Fatalf("unexpected recovery of %s", provider)
	case <-time.After(25 * time.Millisecond):
	}
}

func awaitMountRecovery(t *testing.T, m *Manager, recoveries <-chan string, want string) {
	t.Helper()
	select {
	case got := <-recoveries:
		if got != want {
			t.Fatalf("recovered %s, want %s", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery did not run at threshold")
	}
	// Wait for RecoverMount to release its deduplication marker before the next
	// health tick, ensuring a later duplicate recovery cannot be hidden by it.
	deadline := time.Now().Add(time.Second)
	for {
		m.recoveryMu.Lock()
		active := len(m.recovering)
		m.recoveryMu.Unlock()
		if active == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recovery did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPerformMountHealthCheck_SingleMountFailureDoesNotRecover(t *testing.T) {
	m, _, recoveries := newMountHealthTestManager(t)
	var logs bytes.Buffer
	m.logger = slog.New(slog.NewTextHandler(&logs, nil))
	m.performMountHealthCheck()
	assertNoMountRecovery(t, recoveries)
	if !strings.Contains(logs.String(), "err=") || !strings.Contains(logs.String(), "WebDAV temporarily unavailable") {
		t.Fatalf("health warning lost underlying failure: %s", logs.String())
	}
	info, _ := m.GetMountInfo("altmount")
	if !strings.Contains(info.Error, "WebDAV temporarily unavailable") {
		t.Fatalf("mount error lost underlying failure: %q", info.Error)
	}
}

func TestPerformMountHealthCheck_MountThresholdRecoversOnce(t *testing.T) {
	m, _, recoveries := newMountHealthTestManager(t)
	withRcdRestartAfter(t, m, "2m") // Four probes, using existing rcd tolerance.
	for range 3 {
		m.performMountHealthCheck()
	}
	assertNoMountRecovery(t, recoveries)
	m.performMountHealthCheck()
	awaitMountRecovery(t, m, recoveries, "altmount")
	m.performMountHealthCheck()
	assertNoMountRecovery(t, recoveries)
}

func TestPerformMountHealthCheck_MountSuccessResetsStreak(t *testing.T) {
	m, failures, recoveries := newMountHealthTestManager(t)
	for range 2 {
		m.performMountHealthCheck()
	}
	failures["altmount"] = false
	m.performMountHealthCheck()
	info, _ := m.GetMountInfo("altmount")
	if info.Error != "" {
		t.Fatalf("healthy mount retains error: %q", info.Error)
	}
	failures["altmount"] = true
	for range 2 {
		m.performMountHealthCheck()
	}
	assertNoMountRecovery(t, recoveries)
	m.performMountHealthCheck()
	awaitMountRecovery(t, m, recoveries, "altmount")
}

func TestPerformMountHealthCheck_MountStreaksAreIndependent(t *testing.T) {
	m, failures, recoveries := newMountHealthTestManager(t)
	m.mounts["other"] = &MountInfo{Provider: "other", LocalPath: "other", Mounted: true, desired: true}
	for range 2 {
		m.performMountHealthCheck()
	}
	failures["other"] = true
	m.performMountHealthCheck()
	awaitMountRecovery(t, m, recoveries, "altmount")
	m.performMountHealthCheck()
	assertNoMountRecovery(t, recoveries)
	m.performMountHealthCheck()
	awaitMountRecovery(t, m, recoveries, "other")
}

func TestPerformMountHealthCheck_NewMountStartsFreshStreak(t *testing.T) {
	m, _, recoveries := newMountHealthTestManager(t)
	for range 2 {
		m.performMountHealthCheck()
	}
	m.mountsMutex.Lock()
	m.mounts["altmount"] = &MountInfo{Provider: "altmount", LocalPath: "altmount", Mounted: true, desired: true}
	m.mountsMutex.Unlock()
	for range 2 {
		m.performMountHealthCheck()
	}
	assertNoMountRecovery(t, recoveries)
	m.performMountHealthCheck()
	awaitMountRecovery(t, m, recoveries, "altmount")
}

func TestCheckMountHealth_ReturnsUnderlyingError(t *testing.T) {
	m, _ := newHealthTestManager(t, true, afterGrace())
	failure := errors.New("WebDAV timeout")
	m.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, failure
	})}
	if err := m.checkMountHealth("altmount"); !errors.Is(err, failure) {
		t.Fatalf("health check error = %v, want underlying timeout", err)
	}
}

func TestPerformMountHealthCheck_IgnoresResultForReplacedMount(t *testing.T) {
	m, _, recoveries := newMountHealthTestManager(t)
	for range 2 {
		m.performMountHealthCheck()
	}
	transport := m.httpClient.Transport
	m.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		// Simulate replacement while operations/list is in flight.
		m.mountsMutex.Lock()
		m.mounts["altmount"] = &MountInfo{Provider: "altmount", LocalPath: "altmount", Mounted: true, desired: true}
		m.mountsMutex.Unlock()
		return transport.RoundTrip(req)
	})
	m.performMountHealthCheck()
	m.httpClient.Transport = transport
	for range 2 {
		m.performMountHealthCheck()
	}
	assertNoMountRecovery(t, recoveries)
	m.performMountHealthCheck()
	awaitMountRecovery(t, m, recoveries, "altmount")
}

func TestPerformMountHealthCheck_UnmountResetsStreak(t *testing.T) {
	m, _, recoveries := newMountHealthTestManager(t)
	for range 2 {
		m.performMountHealthCheck()
	}
	m.forceUnmount = func(string) error { return nil }
	if err := m.Unmount(context.Background(), "altmount"); err != nil {
		t.Fatal(err)
	}
	m.mountsMutex.Lock()
	m.mounts["altmount"].Mounted = true
	m.mounts["altmount"].desired = true
	m.mountsMutex.Unlock()
	m.forceUnmount = func(path string) error {
		recoveries <- path
		return errors.New("stop recovery before remount")
	}
	for range 2 {
		m.performMountHealthCheck()
	}
	assertNoMountRecovery(t, recoveries)
	m.performMountHealthCheck()
	awaitMountRecovery(t, m, recoveries, "altmount")
}

func TestPerformMountHealthCheck_QueuedRecoveryIgnoresLifecycleChange(t *testing.T) {
	for _, action := range []string{"replace", "unmount"} {
		t.Run(action, func(t *testing.T) {
			m, _, recoveries := newMountHealthTestManager(t)
			for range 2 {
				m.performMountHealthCheck()
			}
			m.mountMu.Lock()
			m.performMountHealthCheck()
			deadline := time.Now().Add(time.Second)
			for {
				m.recoveryMu.Lock()
				_, queued := m.recovering["altmount"]
				m.recoveryMu.Unlock()
				if queued {
					break
				}
				if time.Now().After(deadline) {
					m.mountMu.Unlock()
					t.Fatal("recovery was not queued")
				}
				time.Sleep(time.Millisecond)
			}
			m.mountsMutex.Lock()
			if action == "replace" {
				m.mounts["altmount"] = &MountInfo{Provider: "altmount", LocalPath: "replacement", Mounted: true, desired: true}
			} else {
				m.mounts["altmount"].Mounted = false
				m.mounts["altmount"].desired = false
			}
			m.mountsMutex.Unlock()
			m.mountMu.Unlock()
			assertNoMountRecovery(t, recoveries)
			// Ensure the queued recovery exited rather than merely failing to start.
			deadline = time.Now().Add(time.Second)
			for {
				m.recoveryMu.Lock()
				active := len(m.recovering)
				m.recoveryMu.Unlock()
				if active == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("stale recovery did not exit")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}
