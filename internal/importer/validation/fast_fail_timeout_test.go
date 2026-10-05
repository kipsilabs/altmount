package validation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
)

// A provider miss followed by a retained provider's answer can take longer
// than the cheap probe deadline, on every attempt, even on an idle pool.
func TestFastFailReleaseProbeRetriesSlowProviderFallback(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			client := newDelayedStatClient(make(map[string][]error), nil)
			client.alwaysDelay = make(map[string]time.Duration)
			for i := range 8 {
				id := fmt.Sprintf("seg-%d", i)
				client.alwaysDelay[id] = 2200 * time.Millisecond
				if missing {
					client.outcomes[id] = []error{nntppool.ErrArticleNotFound}
				}
			}

			verdict, err := FastFailReleaseProbeVerdict(context.Background(), probeFile(8), fastFailPoolManager{client: client}, 100, 8, 5*time.Second, nil, time.Time{})
			if err != nil {
				t.Fatalf("probe error = %v, want a conclusive provider answer", err)
			}
			if verdict.Missing != missing || verdict.Dead != missing {
				t.Fatalf("verdict = %+v, want Missing and Dead = %t", verdict, missing)
			}
		})
	}
}

func TestFastFailReleaseProbeBudgetCancelsInFlightAttempt(t *testing.T) {
	prev := fastFailStatBudget
	fastFailStatBudget = 150 * time.Millisecond
	t.Cleanup(func() { fastFailStatBudget = prev })
	client := newDelayedStatClient(nil, nil)
	client.alwaysDelay = map[string]time.Duration{"seg-0": time.Second}

	start := time.Now()
	verdict, err := FastFailReleaseProbeVerdict(context.Background(), probeFile(1), fastFailPoolManager{client: client}, 100, 8, 30*time.Second, nil, time.Time{})
	if !errors.Is(err, ErrFastFailInconclusive) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("probe error = %v, want an inconclusive deadline", err)
	}
	if verdict.Missing || verdict.Dead {
		t.Fatalf("verdict = %+v, want unknown availability", verdict)
	}
	if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
		t.Fatalf("probe took %s, want the budget to cancel the in-flight attempt", elapsed)
	}
}

func TestFastFailReleaseProbeBudgetSpansBothWaves(t *testing.T) {
	prev := fastFailStatBudget
	fastFailStatBudget = 300 * time.Millisecond
	t.Cleanup(func() { fastFailStatBudget = prev })
	client := newDelayedStatClient(nil, nil)
	client.alwaysDelay = make(map[string]time.Duration)
	for i := range 9 {
		client.alwaysDelay[fmt.Sprintf("seg-%d", i)] = 200 * time.Millisecond
	}

	_, err := FastFailReleaseProbeVerdict(context.Background(), probeFile(9), fastFailPoolManager{client: client}, 100, 8, 30*time.Second, nil, time.Time{})
	if !errors.Is(err, ErrFastFailInconclusive) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("probe error = %v, want both waves bounded by one budget", err)
	}
}
