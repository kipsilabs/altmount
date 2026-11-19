package validation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
)

// The release probe honours the configured percentage and is bounded by its
// time budget.

func keepSampleOrder(t *testing.T) {
	t.Helper()
	prev := shuffleSample
	shuffleSample = func(int, func(i, j int)) {}
	t.Cleanup(func() { shuffleSample = prev })
}

func withProbeBudget(t *testing.T, d time.Duration) {
	t.Helper()
	prev := releaseProbeBudget
	releaseProbeBudget = d
	t.Cleanup(func() { releaseProbeBudget = prev })
}

func statedIDs(c *scriptedStatClient) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, calls := range c.calls {
		if calls > 0 {
			n++
		}
	}
	return n
}

func TestReleaseProbeHonoursSamplePercentage(t *testing.T) {
	client := newScriptedStatClient(nil)

	missing, err := FastFailReleaseProbe(context.Background(), probeFile(2000), fastFailPoolManager{client: client}, 80, 64, 30*time.Second, nil, time.Time{})
	if err != nil || missing {
		t.Fatalf("probe = (%t, %v), want a clean pass", missing, err)
	}
	if got := statedIDs(client); got != 1600 {
		t.Fatalf("STAT-ed %d distinct articles, want 1600 (80%% of 2000)", got)
	}
}

func TestReleaseProbeFullSampleLeadsWithEdgesThenShuffles(t *testing.T) {
	segs := makeTestSegments("seg", 500)
	got := edgesThenShuffled(segs)
	if len(got) != 500 {
		t.Fatalf("len = %d, want 500", len(got))
	}
	want := []string{"seg-0", "seg-1", "seg-2", "seg-498", "seg-499"}
	for i, id := range want {
		if got[i].Id != id {
			t.Fatalf("position %d = %s, want %s", i, got[i].Id, id)
		}
	}
	seen := make(map[string]struct{}, len(got))
	inOrder := true
	for i, s := range got {
		if _, dup := seen[s.Id]; dup {
			t.Fatalf("duplicate %s", s.Id)
		}
		seen[s.Id] = struct{}{}
		if i >= 5 && s.Id != fmt.Sprintf("seg-%d", i-2) {
			inOrder = false
		}
	}
	if inOrder {
		t.Fatal("middle is still in file order, want it shuffled")
	}
}

// A large clean sample that runs out of time passes on what it covered.
func TestReleaseProbePassesAtTimeLimitWithLargeCleanSample(t *testing.T) {
	keepSampleOrder(t)
	withProbeBudget(t, 400*time.Millisecond)
	client := newDelayedStatClient(nil, nil)
	client.alwaysDelay = make(map[string]time.Duration)
	for i := 400; i < 498; i++ { // the tail of the middle never answers in time
		client.alwaysDelay[fmt.Sprintf("seg-%d", i)] = 10 * time.Second
	}

	start := time.Now()
	missing, err := FastFailReleaseProbe(context.Background(), probeFile(500), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	if err != nil || missing {
		t.Fatalf("probe = (%t, %v), want a pass on the 402 answered articles", missing, err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("probe took %s, want it bounded by the 400ms budget", elapsed)
	}
}

// The budget passed by the caller (import.segment_check_timeout_seconds)
// bounds the probe instead of the default.
func TestReleaseProbeHonoursCallerBudget(t *testing.T) {
	keepSampleOrder(t)
	withProbeBudget(t, time.Minute) // the default must not be what applies
	client := newDelayedStatClient(nil, nil)
	client.alwaysDelay = make(map[string]time.Duration)
	for i := 400; i < 498; i++ {
		client.alwaysDelay[fmt.Sprintf("seg-%d", i)] = 10 * time.Second
	}

	start := time.Now()
	v, err := FastFailReleaseProbeVerdict(context.Background(), probeFile(500), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{}, 400*time.Millisecond)
	if err != nil || v.Missing {
		t.Fatalf("probe = (%+v, %v), want a pass on the 402 answered articles", v, err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("probe took %s, want it bounded by the 400ms budget passed in", elapsed)
	}
}

// Too few answers when time runs out is still inconclusive (import fails).
func TestReleaseProbeInconclusiveAtTimeLimitWithTooFewAnswers(t *testing.T) {
	keepSampleOrder(t)
	withProbeBudget(t, 400*time.Millisecond)
	client := newDelayedStatClient(nil, nil)
	client.alwaysDelay = make(map[string]time.Duration)
	for i := 40; i < 198; i++ {
		client.alwaysDelay[fmt.Sprintf("seg-%d", i)] = 10 * time.Second
	}

	_, err := FastFailReleaseProbe(context.Background(), probeFile(200), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	if !errors.Is(err, ErrFastFailInconclusive) {
		t.Fatalf("probe error = %v, want ErrFastFailInconclusive with only 42 answers", err)
	}
}

// A miss found before the time limit still fails the probe.
func TestReleaseProbeCatchesMissDeepInLargeSample(t *testing.T) {
	keepSampleOrder(t)
	client := newScriptedStatClient(map[string][]error{"seg-1500": {nntppool.ErrArticleNotFound}})

	missing, err := FastFailReleaseProbe(context.Background(), probeFile(2000), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	if err != nil || !missing {
		t.Fatalf("probe = (%t, %v), want missing: article 1500 of 2000 is gone", missing, err)
	}
}
