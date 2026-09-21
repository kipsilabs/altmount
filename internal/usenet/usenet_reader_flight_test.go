package usenet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
)

func newFlightReader(t *testing.T, ctx context.Context, fp *fakepool.Client, fm *flightMap, n, segSize int, opts ...ReaderOption) *UsenetReader {
	t.Helper()
	rg := buildEagerRange(ctx, t, n, segSize)
	getter := func() (pool.NntpClient, error) { return fp, nil }
	ur, err := NewUsenetReader(ctx, getter, rg, 8, noopMetrics{}, "flight-test", nil, append([]ReaderOption{withFlightMap(fm)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ur.Close() })
	return ur
}

// Two readers over the same file download every article once.
func TestTwoReadersShareOneDownload(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 20, 512
	fp := fakepool.New()
	for i := 0; i < n; i++ {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize), Latency: 20 * time.Millisecond, ChunkSize: 128})
	}
	fm := newFlightMap()
	a := newFlightReader(t, ctx, fp, fm, n, segSize)
	b := newFlightReader(t, ctx, fp, fm, n, segSize)
	a.Start()
	b.Start()

	var wg sync.WaitGroup
	results := make([][]byte, 2)
	errs := make([]error, 2)
	for i, r := range []*UsenetReader{a, b} {
		wg.Add(1)
		go func(i int, r *UsenetReader) {
			defer wg.Done()
			results[i], errs[i] = io.ReadAll(r)
		}(i, r)
	}
	wg.Wait()
	want := segments.FileBytes(n, segSize)
	for i := range results {
		if errs[i] != nil || !bytes.Equal(results[i], want) {
			t.Fatalf("reader %d: err=%v bytes ok=%v", i, errs[i], bytes.Equal(results[i], want))
		}
	}
	if got := fp.BodyStreamPriorityCalls(); got != n {
		t.Fatalf("stream fetches = %d, want %d (one per article)", got, n)
	}
	_ = a.Close()
	_ = b.Close()
	if fm.len() != 0 {
		t.Fatalf("flight map still tracks %d articles after both readers closed", fm.len())
	}
}

// A leader closed mid-article keeps the demand-window fetch running to
// completion, so a follower reads exact bytes from the shared buffer
// without a second fetch.
func TestFollowerTakesOverWhenLeaderCloses(t *testing.T) {
	ctx := context.Background()
	const segSize = 4096
	fp := fakepool.New()
	gate := make(chan struct{})
	fp.SetBehavior(segments.MessageID(0), fakepool.SegmentBehavior{
		Bytes: segments.Payload(0, segSize), ChunkSize: 1024, TailGate: gate,
	})
	fm := newFlightMap()
	leader := newFlightReader(t, ctx, fp, fm, 1, segSize)
	leader.Start()
	// Wait until the leader's fetch has started (first chunk written).
	deadline := time.Now().Add(2 * time.Second)
	for fp.BodyStreamPriorityCalls() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	follower := newFlightReader(t, ctx, fp, fm, 1, segSize)
	follower.Start()
	time.Sleep(50 * time.Millisecond) // follower is now waiting on the shared article

	leader.Interrupt()
	_ = leader.Close()
	// Releasing the gate lets any leftover leader attempt finish harmlessly;
	// the follower's own attempt is not gated (gate only holds once per call
	// while the channel is open).
	close(gate)

	got, err := io.ReadAll(follower)
	if err != nil {
		t.Fatalf("follower read: %v", err)
	}
	if !bytes.Equal(got, segments.Payload(0, segSize)) {
		t.Fatalf("follower got %d bytes, want exact payload", len(got))
	}
	if calls := fp.BodyStreamPriorityCalls(); calls != 1 {
		t.Fatalf("stream fetches = %d, want 1 (closed leader finished the article for the follower)", calls)
	}
}

// A hole padded by one reader's policy is not padded for another reader of
// the same article.
func TestHolePolicyIsPerReader(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 2, 64
	fp := fakepool.New()
	fp.SetBehavior(segments.MessageID(0), fakepool.SegmentBehavior{Bytes: segments.Payload(0, segSize)})
	fp.SetBehavior(segments.MessageID(1), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	fm := newFlightMap()
	pad := &HoleHooks{OnHole: func(int, string) holes.Decision { return holes.DecisionPad }}
	fail := &HoleHooks{OnHole: func(int, string) holes.Decision { return holes.DecisionFail }}
	padder := newFlightReader(t, ctx, fp, fm, n, segSize, WithHoleHooks(pad))
	failer := newFlightReader(t, ctx, fp, fm, n, segSize, WithHoleHooks(fail))
	padder.Start()
	failer.Start()

	got, err := io.ReadAll(padder)
	if err != nil || len(got) != n*segSize || !bytes.Equal(got[segSize:], make([]byte, segSize)) {
		t.Fatalf("padder: err=%v len=%d", err, len(got))
	}
	_, err = io.ReadAll(failer)
	if !errors.Is(err, nntppool.ErrArticleNotFound) {
		t.Fatalf("failer must see the missing article, got %v", err)
	}
}

// Import readers do not join the flight map.
func TestImportReadersBypassFlights(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 4, 64
	fp := fakepool.New()
	for i := 0; i < n; i++ {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize)})
	}
	fm := newFlightMap()
	a := newFlightReader(t, ctx, fp, fm, n, segSize, WithImportProfile(nil))
	b := newFlightReader(t, ctx, fp, fm, n, segSize, WithImportProfile(nil))
	a.Start()
	b.Start()
	if _, err := io.ReadAll(a); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(b); err != nil {
		t.Fatal(err)
	}
	if fp.BodyCalls() != 2*n || fm.len() != 0 {
		t.Fatalf("import must fetch independently: body=%d flights=%d", fp.BodyCalls(), fm.len())
	}
}
