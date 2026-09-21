package usenet

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
)

// usenet_reader_storm_test.go documents connection-storm conditions the
// production code currently exhibits. Each test reproduces the storm with
// concrete assertions, so:
//
//  1. CI runs them today and proves the storm exists.
//  2. When a fix lands, the assertion at the bottom of each test fails
//     (because the bad behavior no longer happens). The fix author then
//     INVERTS the assertion (changes "current bad" → "new invariant") in
//     the same PR. The test then guards the fix.
//  3. The CURRENT-BEHAVIOR and TARGET-INVARIANT bands in each test
//     constitute the contract: a future contributor sees both numbers
//     side-by-side and knows what's allowed.

// TestStorm_RetryAmplifiesPerMessageCallCount pins the post-S1
// invariant: a permanently failing (non-ArticleNotFound) segment must
// produce at most 2 wire calls — one initial attempt plus one bounded
// retry. With the previous retry.Attempts(5) policy each failing
// segment caused 5x wire amplification under a flaky provider.
func TestStorm_RetryAmplifiesPerMessageCallCount(t *testing.T) {
	t.Parallel()
	const segSize = 16
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fp := fakepool.New()
	failingID := segments.MessageID(0)
	fp.SetBehavior(failingID, fakepool.SegmentBehavior{
		Latency: 5 * time.Millisecond,
		Err:     errors.New("synthetic transient error"),
	})

	rg := buildEagerRange(ctx, t, 1, segSize)
	ur := newReaderForTest(t, ctx, fp, rg, 1)
	ur.Start()
	_, _ = io.ReadAll(ur)

	calls := fp.PerMessageCalls(failingID)

	// PINNED INVARIANT: at most 2 wire calls per failing segment.
	const maxCallsPerFailure = 2
	if calls > maxCallsPerFailure {
		t.Errorf("INVARIANT regression (S1): PerMessageCalls=%d, want <= %d. "+
			"retry.Attempts must stay at 2 for non-ArticleNotFound errors.",
			calls, maxCallsPerFailure)
	}
}

// TestStorm_RetryUsesFixedDelayInsteadOfExponentialBackoff pins the
// post-S3 invariant: inter-attempt delays carry jitter, so multiple
// readers retrying simultaneously desynchronize. With Attempts=2 each
// failing segment contributes exactly one inter-attempt delta; we run
// many failing segments (each in its own reader, independent random
// draws) and assert the coefficient of variation across those deltas
// is meaningfully non-zero.
func TestStorm_RetryUsesFixedDelayInsteadOfExponentialBackoff(t *testing.T) {
	t.Parallel()
	const (
		segSize = 16
		samples = 30 // independent failing segments
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		arrivals = make(map[string][]time.Time, samples)
	)
	fp := fakepool.New()
	rec := &multiRecordingClient{Client: fp, arrivals: arrivals, mu: &mu}

	for i := 0; i < samples; i++ {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{
			Latency: 1 * time.Millisecond,
			Err:     errors.New("synthetic transient error"),
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < samples; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// One segment per reader, distinct message-ID per reader,
			// so each contributes one independent inter-attempt delta.
			segs := []*segment{newSegment(segments.MessageID(idx), 0, int64(segSize-1), int64(segSize), nil, 0)}
			rg := &segmentRange{segments: segs, start: 0, end: int64(segSize - 1), ctx: ctx}
			ur := newReaderForTestWithClient(t, ctx, rec, rg, 1)
			ur.Start()
			_, _ = io.ReadAll(ur)
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	deltas := make([]float64, 0, samples)
	for _, times := range arrivals {
		if len(times) < 2 {
			continue
		}
		// One delta per segment (Attempts=2 → exactly 2 timestamps).
		deltas = append(deltas, float64(times[1].Sub(times[0])/time.Millisecond))
	}
	if len(deltas) < samples/2 {
		t.Fatalf("recorded only %d usable deltas across %d segments", len(deltas), samples)
	}

	mean, stdev := meanStdev(deltas)
	cv := 0.0
	if mean > 0 {
		cv = stdev / mean
	}
	t.Logf("retry deltas across %d segments: mean=%.2fms stdev=%.2fms cv=%.3f",
		len(deltas), mean, stdev, cv)

	// PINNED INVARIANT: cv > 0.1 means deltas vary meaningfully across
	// independent retriers — the jitter is doing its job. The lower
	// bound on the floor is loose to absorb scheduler noise on shared CI.
	const minCV = 0.1
	if cv < minCV {
		t.Errorf("INVARIANT regression (S3): retry-delay cv=%.3f, want > %.3f. "+
			"retry.MaxJitter and retry.RandomDelay must remain in the DelayType.",
			cv, minCV)
	}
}

// --- helpers ---

// multiRecordingClient wraps a *fakepool.Client and timestamps every
// BodyPriority call, bucketed by message-ID. Lets the jitter test
// observe inter-attempt timing per segment without extending the
// fakepool public API.
type multiRecordingClient struct {
	*fakepool.Client
	arrivals map[string][]time.Time
	mu       *sync.Mutex
}

func (r *multiRecordingClient) Fetch(ctx context.Context, req nntppool.Req) (*nntppool.ArticleBody, error) {
	r.mu.Lock()
	r.arrivals[req.MessageID] = append(r.arrivals[req.MessageID], time.Now())
	r.mu.Unlock()
	return r.Client.Fetch(ctx, req)
}

func meanStdev(xs []float64) (float64, float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	var sq float64
	for _, x := range xs {
		d := x - mean
		sq += d * d
	}
	stdev := math.Sqrt(sq / float64(len(xs)))
	return mean, stdev
}
