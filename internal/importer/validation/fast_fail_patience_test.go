package validation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
)

// transientFor builds a STAT script that fails n times with a retryable error
// before answering "exists".
func transientFor(n int) []error {
	seq := make([]error, 0, n+1)
	for range n {
		seq = append(seq, nntppool.ErrConnectionDied)
	}
	return append(seq, nil)
}

// convergingScript scripts ids so that each retry clears one more tenth of the
// sweep: ids 0-9 answer on the first attempt, 10-19 need two, and so on.
func convergingScript(count, maxFailures int) map[string][]error {
	outcomes := make(map[string][]error, count)
	for i := range count {
		outcomes[fmt.Sprintf("seg-%d", i)] = transientFor(min(i/10, maxFailures))
	}
	return outcomes
}

func TestFastFailReleaseProbeKeepsRetryingWhileSweepConverges(t *testing.T) {
	// 40 ids, the last ten needing four attempts: every attempt reports more
	// than the one before, so the probe should see it through rather than give
	// up at the old hard cap of three.
	client := newScriptedStatClient(convergingScript(40, 4))

	missing, err := FastFailReleaseProbe(context.Background(), probeFile(40), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	if err != nil {
		t.Fatalf("FastFailReleaseProbe error = %v, want nil: the sweep was converging", err)
	}
	if missing {
		t.Fatal("missing = true, want false")
	}
	if got := client.callCount("seg-39"); got != 4 {
		t.Fatalf("slowest id STATs = %d, want 4", got)
	}
}

func TestFastFailReleaseProbeStopsOnceSweepStallsAfterMinimumAttempts(t *testing.T) {
	outcomes := convergingScript(20, 1)
	// ids 10-19 clear on attempt 2; these never answer.
	for i := 20; i < 30; i++ {
		outcomes[fmt.Sprintf("seg-%d", i)] = []error{nntppool.ErrConnectionDied}
	}
	client := newScriptedStatClient(outcomes)

	_, err := FastFailReleaseProbe(context.Background(), probeFile(30), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	if !errors.Is(err, ErrFastFailInconclusive) {
		t.Fatalf("FastFailReleaseProbe error = %v, want ErrFastFailInconclusive once progress stops", err)
	}
	if got := client.callCount("seg-25"); got != fastFailStatMaxAttempts {
		t.Fatalf("stuck id STATs = %d, want %d: one non-converging attempt past the minimum ends the sweep", got, fastFailStatMaxAttempts)
	}
}

func TestFastFailReleaseProbeConvergenceIsBoundedByTotalBudget(t *testing.T) {
	prev := fastFailStatBudget
	fastFailStatBudget = 150 * time.Millisecond
	t.Cleanup(func() { fastFailStatBudget = prev })

	client := newScriptedStatClient(convergingScript(60, 5))

	start := time.Now()
	_, err := FastFailReleaseProbe(context.Background(), probeFile(60), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	elapsed := time.Since(start)
	if !errors.Is(err, ErrFastFailInconclusive) {
		t.Fatalf("FastFailReleaseProbe error = %v, want ErrFastFailInconclusive when the budget runs out", err)
	}
	if elapsed > time.Second {
		t.Fatalf("probe took %s, want it to stop near the %s budget", elapsed, fastFailStatBudget)
	}
	// Attempt 1 at t=0, 100 ms backoff, attempt 2 at ~100 ms; the next 200 ms
	// backoff would overrun the 150 ms budget, so the sweep ends there.
	if got := client.callCount("seg-59"); got != 2 {
		t.Fatalf("slowest id STATs = %d, want 2: the budget must cut the sweep short", got)
	}
}
