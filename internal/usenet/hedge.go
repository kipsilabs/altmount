package usenet

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/pool"
)

// Hedged demand fetches. Delivery to the caller is in order, so one article
// with an abnormally slow fetch at the read position stalls the stream while
// the read-ahead window sits complete behind it; measured on a real provider
// the tail (p90 300-870 ms, occasional 2-4 s against a 120-240 ms median)
// repeatedly cut consumer throughput by three quarters. Widening the window
// does not help. Instead, once a demand-position fetch has run well past the
// recent median, the same article is requested a second time and the first
// result wins; the other request is cancelled so its connection drains.
const (
	// hedgeFloor is the least a fetch must have run before it is hedged,
	// whatever the median says: below it a second request is pure cost.
	hedgeFloor = 400 * time.Millisecond
	// hedgeFactor times the rolling median is the threshold once enough
	// fetches have been observed to know what normal looks like.
	hedgeFactor = 2
	// hedgeWindow is how many recent successful fetch durations the median
	// is taken over; hedgeMinSamples is how many are needed before the median
	// replaces the floor.
	hedgeWindow     = 16
	hedgeMinSamples = 4
	// maxHedgesInFlight caps concurrent hedges per reader: one per demand
	// window slot, so a struggling provider is never asked for more than the
	// reader is actually waiting on. The pool exposes no free-request
	// capacity, so this constant is the only guard on the extra load.
	maxHedgesInFlight = demandDepth
	// hedgePollInterval is how often a slow speculative fetch re-checks
	// whether the reader has caught up to it and it now qualifies.
	hedgePollInterval = 100 * time.Millisecond
)

// hedgePolicy decides when a running fetch is slow enough to hedge and bounds
// how many hedges a reader has out at once. The zero value is ready to use.
type hedgePolicy struct {
	mu       sync.Mutex
	samples  [hedgeWindow]time.Duration
	count    int
	next     int
	inFlight atomic.Int32
}

// record adds a successful fetch's duration to the rolling window.
func (h *hedgePolicy) record(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples[h.next] = d
	h.next = (h.next + 1) % hedgeWindow
	if h.count < hedgeWindow {
		h.count++
	}
}

// threshold is how long a fetch may run before it is hedged.
func (h *hedgePolicy) threshold() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count < hedgeMinSamples {
		return hedgeFloor
	}
	sorted := slices.Clone(h.samples[:h.count])
	slices.Sort(sorted)
	return max(hedgeFloor, hedgeFactor*sorted[len(sorted)/2])
}

// tryAcquire takes a hedge slot without blocking. release is idempotent.
func (h *hedgePolicy) tryAcquire() (release func(), ok bool) {
	if h.inFlight.Add(1) > maxHedgesInFlight {
		h.inFlight.Add(-1)
		return nil, false
	}
	var once sync.Once
	return func() { once.Do(func() { h.inFlight.Add(-1) }) }, true
}

// isDemand reports whether the range-local segment is at the read position or
// within demandDepth of it, so the caller is (about to be) waiting on it.
func (b *UsenetReader) isDemand(segIdx int) bool {
	b.mu.Lock()
	rg := b.rg
	b.mu.Unlock()
	if rg == nil {
		return false
	}
	ahead := segIdx - rg.GetCurrentIndex()
	return ahead >= 0 && ahead < demandDepth
}

type streamResult struct {
	w    *articleWriter
	body *nntppool.ArticleBody
	err  error
	dur  time.Duration
}

// streamArticle runs one priority-lane fetch of seg into art, hedging it with
// a second request once it has run past the policy threshold at a demand
// position without receiving a byte. The returned writer is the one whose bytes stand: the
// winner's on success, the last failure's otherwise. ErrArticleNotFound from
// either request ends the fetch at once — a 430 is never retried, and the
// caller's miss re-check owns what happens next.
func (b *UsenetReader) streamArticle(ctx context.Context, cp pool.NntpClient, seg *segment, segIdx int, art *articleBuf) (*articleWriter, *nntppool.ArticleBody, error) {
	// Cancelling the parent on return is what stops the loser: nntppool
	// switches its body to discard and drains or drops the connection.
	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	results := make(chan streamResult, 2)
	run := func(w *articleWriter) {
		go func() {
			start := time.Now()
			body, err := cp.Fetch(ctx, nntppool.Req{
				MessageID:   seg.Id,
				Lane:        nntppool.LanePriority,
				Writer:      w,
				ArticleDate: b.articleDate,
			})
			results <- streamResult{w: w, body: body, err: err, dur: time.Since(start)}
		}()
	}

	start := time.Now()
	primary := art.attemptWriter()
	run(primary)
	pending := 1

	hedged := false
	wait := time.NewTimer(b.hedger.threshold())
	defer wait.Stop()

	var lastFailed streamResult
	for pending > 0 {
		select {
		case r := <-results:
			pending--
			switch {
			case r.err == nil:
				b.hedger.record(r.dur)
				if hedged {
					winner := "hedge"
					if r.w == primary {
						winner = "original"
					}
					b.log.DebugContext(ctx, "hedged article fetch resolved",
						"segment_id", seg.Id,
						"winner", winner,
						"winner_dur", r.dur,
						"elapsed", time.Since(start))
				}
				return r.w, r.body, nil
			case errors.Is(r.err, nntppool.ErrArticleNotFound):
				return r.w, r.body, r.err
			default:
				lastFailed = r
			}
		case <-wait.C:
			// Once bytes are flowing the fetch is slow, not stuck, and will
			// finish in about its remaining time; the stragglers measured were
			// requests queued behind another body with nothing received yet.
			// Hedging a streaming 4 MiB article would only double its bytes.
			if hedged || primary.received.Load() > 0 {
				continue
			}
			if !b.isDemand(segIdx) {
				wait.Reset(hedgePollInterval)
				continue
			}
			release, ok := b.hedger.tryAcquire()
			if !ok {
				wait.Reset(hedgePollInterval)
				continue
			}
			defer release()
			hedged = true
			b.log.DebugContext(ctx, "hedging slow demand article fetch",
				"segment_id", seg.Id,
				"elapsed", time.Since(start))
			run(art.hedgeWriter(primary))
			pending++
		}
	}
	return lastFailed.w, lastFailed.body, lastFailed.err
}
