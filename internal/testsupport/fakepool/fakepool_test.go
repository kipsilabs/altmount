package fakepool

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
)

// TestFake_BasicBodyReturnsPayload pins the simplest contract: a configured
// payload comes back as ArticleBody.Bytes with BytesDecoded set. Without this
// the fake can't substitute for nntppool.Client in any downstream test.
func TestFake_BasicBodyReturnsPayload(t *testing.T) {
	t.Parallel()
	c := New()
	c.SetBehavior("seg-1", SegmentBehavior{Bytes: []byte("hello")})

	body, err := c.Fetch(context.Background(), nntppool.Req{MessageID: "seg-1", Lane: nntppool.LanePriority})
	if err != nil {
		t.Fatalf("BodyPriority error: %v", err)
	}
	if string(body.Bytes) != "hello" {
		t.Errorf("Bytes = %q, want %q", body.Bytes, "hello")
	}
	if body.BytesDecoded != 5 {
		t.Errorf("BytesDecoded = %d, want 5", body.BytesDecoded)
	}
	if c.BodyPriorityCalls() != 1 {
		t.Errorf("BodyPriorityCalls = %d, want 1", c.BodyPriorityCalls())
	}
}

// TestFake_DefaultBehaviorAppliesWhenNoOverride confirms message-IDs without
// an explicit behavior fall through to the default.
func TestFake_DefaultBehaviorAppliesWhenNoOverride(t *testing.T) {
	t.Parallel()
	c := New()
	c.SetDefaultBehavior(SegmentBehavior{Bytes: []byte("default")})
	body, err := c.Fetch(context.Background(), nntppool.Req{MessageID: "any-id"})
	if err != nil {
		t.Fatalf("Body error: %v", err)
	}
	if string(body.Bytes) != "default" {
		t.Errorf("Bytes = %q, want %q", body.Bytes, "default")
	}
}

// TestFake_ErrorPropagates verifies that injected errors are returned
// verbatim — the downstream retry tests depend on this for things like
// nntppool.ErrArticleNotFound.
func TestFake_ErrorPropagates(t *testing.T) {
	t.Parallel()
	c := New()
	c.SetBehavior("missing", SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	_, err := c.Fetch(context.Background(), nntppool.Req{MessageID: "missing", Lane: nntppool.LanePriority})
	if !errors.Is(err, nntppool.ErrArticleNotFound) {
		t.Errorf("err = %v, want ErrArticleNotFound", err)
	}
}

// TestFake_ContextCancellationDuringLatencyShortCircuits proves the fake
// honors ctx — without this, slow-latency tests could not assert that
// cancelled requests stop spending wall time.
func TestFake_ContextCancellationDuringLatencyShortCircuits(t *testing.T) {
	t.Parallel()
	c := New()
	c.SetDefaultBehavior(SegmentBehavior{Latency: 5 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := c.Fetch(ctx, nntppool.Req{MessageID: "x", Lane: nntppool.LanePriority})
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed > time.Second {
		t.Errorf("call took %v, expected to short-circuit", elapsed)
	}
}

// TestFake_InFlightCounterTracksConcurrency is the core observability test:
// if we fire N parallel calls and pin them behind a gate, MaxInFlight should
// equal N exactly. Every storm-detection test in the project relies on this
// counter being accurate.
func TestFake_InFlightCounterTracksConcurrency(t *testing.T) {
	t.Parallel()
	c := New()
	c.SetDefaultBehavior(SegmentBehavior{Bytes: []byte("x")})

	release := make(chan struct{})
	c.BlockUntil(release)

	const N = 10
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_, _ = c.Fetch(context.Background(), nntppool.Req{MessageID: "seg", Lane: nntppool.LanePriority})
		}()
	}

	// Wait for all goroutines to reach the gate. Polling is fine; the
	// timeout protects against deadlock.
	deadline := time.Now().Add(2 * time.Second)
	for c.InFlight() < N && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := c.InFlight(); got != N {
		t.Fatalf("InFlight at gate = %d, want %d", got, N)
	}

	close(release)
	wg.Wait()

	if got := c.MaxInFlight(); got != N {
		t.Errorf("MaxInFlight = %d, want %d", got, N)
	}
	if got := c.InFlight(); got != 0 {
		t.Errorf("InFlight after drain = %d, want 0", got)
	}
}

// TestFake_BodyAsyncWritesToWriter mirrors the importer's BodyAsync use:
// the decoded payload must arrive on the writer, and the channel must yield
// exactly one BodyResult.
func TestFake_BodyAsyncWritesToWriter(t *testing.T) {
	t.Parallel()
	c := New()
	c.SetBehavior("a", SegmentBehavior{Bytes: []byte("payload")})
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	ch := c.FetchAsync(context.Background(), nntppool.Req{MessageID: "a", Writer: pw})

	buf := make([]byte, 7)
	if _, err := io.ReadFull(pr, buf); err != nil {
		t.Fatalf("pipe read: %v", err)
	}
	if string(buf) != "payload" {
		t.Errorf("read %q, want %q", buf, "payload")
	}
	select {
	case res := <-ch:
		if res.Err != nil {
			t.Errorf("BodyAsync result err = %v", res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("BodyAsync did not yield a result")
	}
}
