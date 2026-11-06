package rclonecli

import (
	"sync/atomic"
	"testing"
	"time"
)

// Failures seen inside the startup grace window must not count toward the
// restart threshold. Otherwise a slow-starting rcd leaves the window with its
// streak already at the threshold and is killed on the first failed probe
// after it, so the window delays the restart by one tick instead of
// preventing it.
func TestPerformMountHealthCheck_GraceFailuresDoNotCountTowardThreshold(t *testing.T) {
	m, restarts := newHealthTestManager(t, false, time.Now())

	for range maxConsecutiveProbeFailures + 2 {
		m.performMountHealthCheck()
	}
	if m.consecutiveProbeFailures != 0 {
		t.Fatalf("failures inside grace were counted: streak = %d, want 0", m.consecutiveProbeFailures)
	}

	// The window expires while the rcd is still failing.
	m.mu.Lock()
	m.readyAt = afterGrace()
	m.mu.Unlock()

	m.performMountHealthCheck()
	if got := atomic.LoadInt32(restarts); got != 0 {
		t.Fatalf("restarted on the first failed probe after grace, got %d restarts", got)
	}

	for range maxConsecutiveProbeFailures - 1 {
		m.performMountHealthCheck()
	}
	if got := atomic.LoadInt32(restarts); got != 1 {
		t.Fatalf("restarts = %d after a full post-grace streak, want 1", got)
	}
}
