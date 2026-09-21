package usenet

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
)

// hedge_test.go pins the hedged demand fetch: a demand-position article whose
// fetch runs abnormally long is requested a second time and the first result
// wins. Measured on a real provider, one 2-3 s article at the read position
// stalls playback while the whole read-ahead window sits complete behind it.

// hedgeProbe wraps the fake so each call to a message-ID can be held for a
// different extra delay before it reaches the fake. fakepool's Latency is
// per-ID, so on its own it cannot make the second request for an article
// faster than the first. Calls whose context ends inside the delay are
// counted as cancelled and never reach the fake.
type hedgeProbe struct {
	*fakepool.Client
	mu        sync.Mutex
	delays    map[string][]time.Duration
	calls     map[string]int
	cancelled atomic.Int32
}

func newHedgeProbe(fp *fakepool.Client) *hedgeProbe {
	return &hedgeProbe{Client: fp, delays: map[string][]time.Duration{}, calls: map[string]int{}}
}

// delayCalls holds the n-th call to id for delays[n]; calls past the list run
// undelayed.
func (p *hedgeProbe) delayCalls(id string, delays ...time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.delays[id] = delays
}

func (p *hedgeProbe) callsFor(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[id]
}

// Fetch delays the configured number of times per message-ID before handing
// off to the embedded fake, so the hedge policy can be driven deterministically.
func (p *hedgeProbe) Fetch(ctx context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	id := r.MessageID
	p.mu.Lock()
	n := p.calls[id]
	p.calls[id]++
	var d time.Duration
	if ds := p.delays[id]; n < len(ds) {
		d = ds[n]
	}
	p.mu.Unlock()
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			p.cancelled.Add(1)
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	return p.Client.Fetch(ctx, r)
}

type countingMetrics struct{ downloaded atomic.Int64 }

func (m *countingMetrics) IncArticlesDownloaded()                   { m.downloaded.Add(1) }
func (m *countingMetrics) IncArticlesPosted()                       {}
func (m *countingMetrics) UpdateDownloadProgress(_ string, _ int64) {}

func newHedgeReader(t *testing.T, ctx context.Context, cp pool.NntpClient, rg *segmentRange, maxPrefetch int, metrics MetricsTracker) *UsenetReader {
	t.Helper()
	getter := func() (pool.NntpClient, error) { return cp, nil }
	ur, err := NewUsenetReader(ctx, getter, rg, maxPrefetch, metrics, "hedge-test", nil, withFlightMap(newFlightMap()))
	if err != nil {
		t.Fatalf("NewUsenetReader: %v", err)
	}
	t.Cleanup(func() { _ = ur.Close() })
	return ur
}

func fastSegments(fp *fakepool.Client, n, segSize int) []byte {
	var want []byte
	for i := range n {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize)})
		want = append(want, segments.Payload(i, segSize)...)
	}
	return want
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// A slow demand article is hedged once the floor elapses and the read
// completes in about the hedge's time, not the straggler's. The straggler's
// context is cancelled so its connection can drain.
func TestHedge_SlowDemandArticleIsHedgedAndLoserCancelled(t *testing.T) {
	t.Parallel()
	const nSegs, segSize = 4, 64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	want := fastSegments(fp, nSegs, segSize)
	probe := newHedgeProbe(fp)
	probe.delayCalls(segments.MessageID(1), 3*time.Second)

	rg := buildEagerRange(ctx, t, nSegs, segSize)
	ur := newHedgeReader(t, ctx, probe, rg, nSegs, noopMetrics{})

	start := time.Now()
	got, err := io.ReadAll(ur)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(want))
	}
	if elapsed < hedgeFloor || elapsed > 1500*time.Millisecond {
		t.Errorf("read took %v, want about the hedge floor (%v) rather than the 3s straggler", elapsed, hedgeFloor)
	}
	if n := probe.callsFor(segments.MessageID(1)); n != 2 {
		t.Errorf("slow article issued %d body calls, want 2 (original + one hedge)", n)
	}
	if !waitFor(t, time.Second, func() bool { return probe.cancelled.Load() == 1 }) {
		t.Errorf("straggler context cancelled %d times, want 1", probe.cancelled.Load())
	}
}

// Speculative articles are never hedged, however slow: only the read position
// and the segment after it qualify.
func TestHedge_SpeculativeArticlesAreNeverHedged(t *testing.T) {
	t.Parallel()
	const nSegs, segSize = 12, 64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fastSegments(fp, nSegs, segSize)
	probe := newHedgeProbe(fp)
	slow := segments.MessageID(demandDepth + 3)
	probe.delayCalls(slow, 3*time.Second)

	rg := buildEagerRange(ctx, t, nSegs, segSize)
	ur := newHedgeReader(t, ctx, probe, rg, nSegs, noopMetrics{})
	ur.Start() // schedule the window without consuming: the read position stays at 0

	time.Sleep(hedgeFloor + 4*hedgePollInterval)
	if n := probe.callsFor(slow); n != 1 {
		t.Errorf("speculative article issued %d body calls, want 1 (no hedge)", n)
	}
}

// A demand article whose bytes are already arriving is slow, not stuck: it is
// left to finish rather than fetched twice, however long it takes.
func TestHedge_StreamingArticleIsNotHedged(t *testing.T) {
	t.Parallel()
	const nSegs, segSize = 3, 64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	want := fastSegments(fp, nSegs, segSize)
	tail := make(chan struct{})
	fp.SetBehavior(segments.MessageID(1), fakepool.SegmentBehavior{
		Bytes: segments.Payload(1, segSize), ChunkSize: segSize / 2, TailGate: tail,
	})
	probe := newHedgeProbe(fp)

	rg := buildEagerRange(ctx, t, nSegs, segSize)
	ur := newHedgeReader(t, ctx, probe, rg, nSegs, noopMetrics{})

	go func() {
		time.Sleep(hedgeFloor + 4*hedgePollInterval)
		close(tail)
	}()
	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(want))
	}
	if n := probe.callsFor(segments.MessageID(1)); n != 1 {
		t.Errorf("streaming article issued %d body calls, want 1 (no hedge once bytes flow)", n)
	}
}

// An article is hedged at most once: a hedge that is itself slow is waited
// for, never hedged again.
func TestHedge_AtMostOneHedgePerArticle(t *testing.T) {
	t.Parallel()
	const nSegs, segSize = 3, 64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fastSegments(fp, nSegs, segSize)
	probe := newHedgeProbe(fp)
	probe.delayCalls(segments.MessageID(1), 4*time.Second, 800*time.Millisecond)

	rg := buildEagerRange(ctx, t, nSegs, segSize)
	ur := newHedgeReader(t, ctx, probe, rg, nSegs, noopMetrics{})

	start := time.Now()
	if _, err := io.ReadAll(ur); err != nil {
		t.Fatalf("read: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < hedgeFloor+800*time.Millisecond || elapsed > 3*time.Second {
		t.Errorf("read took %v, want the hedge's own time (~%v)", elapsed, hedgeFloor+800*time.Millisecond)
	}
	time.Sleep(3 * hedgePollInterval)
	if n := probe.callsFor(segments.MessageID(1)); n != 2 {
		t.Errorf("article issued %d body calls, want exactly 2", n)
	}
}

// A 430 from the hedge is authoritative: the straggler is cancelled and the
// existing miss path (one re-check, then a named DataCorruptionError) runs
// unchanged.
func TestHedge_NotFoundFromEitherSideKeepsMissSemantics(t *testing.T) {
	t.Parallel()
	const nSegs, segSize = 3, 64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fastSegments(fp, nSegs, segSize)
	fp.SetBehavior(segments.MessageID(1), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	probe := newHedgeProbe(fp)
	probe.delayCalls(segments.MessageID(1), 3*time.Second)

	rg := buildEagerRange(ctx, t, nSegs, segSize)
	ur := newHedgeReader(t, ctx, probe, rg, nSegs, noopMetrics{})

	start := time.Now()
	_, err := io.ReadAll(ur)
	elapsed := time.Since(start)
	var dce *DataCorruptionError
	if !errors.As(err, &dce) {
		t.Fatalf("want DataCorruptionError, got %v", err)
	}
	if dce.SegmentID != segments.MessageID(1) || dce.MissVerdict != MissConfirmed {
		t.Errorf("miss reported as segment %q verdict %v, want %q / MissConfirmed", dce.SegmentID, dce.MissVerdict, segments.MessageID(1))
	}
	if elapsed > 1500*time.Millisecond {
		t.Errorf("miss surfaced after %v, want about the hedge floor", elapsed)
	}
	if n := probe.callsFor(segments.MessageID(1)); n != 2 {
		t.Errorf("missing article issued %d body calls, want 2 (no retry of a 430)", n)
	}
	if n := fp.StatPriorityCalls(); n != 1 {
		t.Errorf("re-check issued %d priority STATs, want 1", n)
	}
	if !waitFor(t, time.Second, func() bool { return probe.cancelled.Load() == 1 }) {
		t.Errorf("straggler cancelled %d times, want 1", probe.cancelled.Load())
	}
}

// The articles-downloaded metric counts a hedged article once.
func TestHedge_MetricsCountArticleOnce(t *testing.T) {
	t.Parallel()
	const nSegs, segSize = 4, 64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fastSegments(fp, nSegs, segSize)
	probe := newHedgeProbe(fp)
	probe.delayCalls(segments.MessageID(1), 3*time.Second)

	metrics := &countingMetrics{}
	rg := buildEagerRange(ctx, t, nSegs, segSize)
	ur := newHedgeReader(t, ctx, probe, rg, nSegs, metrics)

	if _, err := io.ReadAll(ur); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !waitFor(t, time.Second, func() bool { return metrics.downloaded.Load() >= nSegs }) {
		t.Fatalf("articles downloaded = %d, want %d", metrics.downloaded.Load(), nSegs)
	}
	time.Sleep(200 * time.Millisecond)
	if n := metrics.downloaded.Load(); n != nSegs {
		t.Errorf("articles downloaded = %d, want exactly %d (hedged article counted once)", n, nSegs)
	}
}

// The threshold is the floor until enough fetches have been seen, then a
// multiple of the rolling median, which one outlier does not move.
func TestHedgePolicy_Threshold(t *testing.T) {
	t.Parallel()
	var h hedgePolicy
	if got := h.threshold(); got != hedgeFloor {
		t.Fatalf("empty threshold = %v, want floor %v", got, hedgeFloor)
	}
	for range hedgeMinSamples {
		h.record(100 * time.Millisecond)
	}
	if got := h.threshold(); got != hedgeFloor {
		t.Fatalf("fast-provider threshold = %v, want floor %v", got, hedgeFloor)
	}
	for range hedgeWindow {
		h.record(500 * time.Millisecond)
	}
	if got := h.threshold(); got != hedgeFactor*500*time.Millisecond {
		t.Fatalf("slow-provider threshold = %v, want %v", got, hedgeFactor*500*time.Millisecond)
	}
	h.record(10 * time.Second)
	if got := h.threshold(); got != hedgeFactor*500*time.Millisecond {
		t.Fatalf("one straggler moved the threshold to %v", got)
	}
}

// Hedges in flight per reader are capped; a released slot can be reused.
func TestHedgePolicy_InFlightCap(t *testing.T) {
	t.Parallel()
	var h hedgePolicy
	var releases []func()
	for range maxHedgesInFlight {
		release, ok := h.tryAcquire()
		if !ok {
			t.Fatal("slot refused below the cap")
		}
		releases = append(releases, release)
	}
	if _, ok := h.tryAcquire(); ok {
		t.Fatal("slot granted above the cap")
	}
	releases[0]()
	releases[0]() // idempotent
	if _, ok := h.tryAcquire(); !ok {
		t.Fatal("released slot not reusable")
	}
	if _, ok := h.tryAcquire(); ok {
		t.Fatal("double release freed two slots")
	}
}
