package nzbfilesystem

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
)

// fakeRcloneClient counts RefreshDir calls and records every directory list it
// receives, with safe concurrent access.
type fakeRcloneClient struct {
	mu    sync.Mutex
	calls int32
	dirs  []string
}

func (f *fakeRcloneClient) RefreshDir(_ context.Context, _ string, dirs []string) error {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	f.dirs = append(f.dirs, dirs...)
	f.mu.Unlock()
	return nil
}

func (f *fakeRcloneClient) ForgetDir(_ context.Context, _ string, dirs []string) error {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	f.dirs = append(f.dirs, dirs...)
	f.mu.Unlock()
	return nil
}

func (f *fakeRcloneClient) callCount() int {
	return int(atomic.LoadInt32(&f.calls))
}

func (f *fakeRcloneClient) collectedDirs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.dirs))
	copy(out, f.dirs)
	return out
}

func newTestCoalescer(t *testing.T, client *fakeRcloneClient) *RepairCoalescer {
	t.Helper()
	cfg := &config.Config{}
	cfg.RClone.VFSName = "test"
	c := NewRepairCoalescer(client, func() *config.Config { return cfg })
	// Tighten the flush window for tests so they stay snappy.
	c.flushDelay = 20 * time.Millisecond
	c.debounceTTL = 100 * time.Millisecond
	t.Cleanup(c.Close)
	return c
}

func TestRepairCoalescer_ShouldTrigger_DebouncesByPath(t *testing.T) {
	c := newTestCoalescer(t, &fakeRcloneClient{})

	if !c.ShouldTrigger("/movies/a.mkv") {
		t.Fatal("first call must return true")
	}
	if c.ShouldTrigger("/movies/a.mkv") {
		t.Fatal("repeat call within debounce window must return false")
	}
	if !c.ShouldTrigger("/movies/b.mkv") {
		t.Fatal("different path must return true")
	}

	// After the debounce TTL elapses, the path becomes triggerable again.
	time.Sleep(c.debounceTTL + 50*time.Millisecond)
	if !c.ShouldTrigger("/movies/a.mkv") {
		t.Fatal("path must be triggerable again after debounce TTL")
	}
}

func TestRepairCoalescer_ShouldTrigger_NilReceiverIsSafe(t *testing.T) {
	var c *RepairCoalescer
	if !c.ShouldTrigger("/foo") {
		t.Fatal("nil coalescer must allow trigger (no-op fallback)")
	}
}

func TestRepairCoalescer_BurstCoalescesIntoSingleRefresh(t *testing.T) {
	client := &fakeRcloneClient{}
	c := newTestCoalescer(t, client)

	// Simulate the issue #539 scenario: 12 concurrent corrupted files in the
	// same directory all fire EnqueueRefresh in rapid succession. Without
	// coalescing this would produce 12 RC POSTs.
	const burst = 12
	var wg sync.WaitGroup
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.EnqueueRefresh("/movies/sonarr")
		}()
	}
	wg.Wait()

	// Wait for the worker to flush.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if client.callCount() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := client.callCount(); got != 1 {
		t.Fatalf("expected exactly 1 coalesced RefreshDir call, got %d", got)
	}
	dirs := client.collectedDirs()
	if len(dirs) != 1 || dirs[0] != "/movies/sonarr" {
		t.Fatalf("expected single dir entry [/movies/sonarr], got %v", dirs)
	}
}

func TestRepairCoalescer_DistinctDirsBatchTogether(t *testing.T) {
	client := &fakeRcloneClient{}
	c := newTestCoalescer(t, client)

	c.EnqueueRefresh("/a")
	c.EnqueueRefresh("/b")
	c.EnqueueRefresh("/c")
	c.EnqueueRefresh("/a") // duplicate inside window

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if client.callCount() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := client.callCount(); got != 1 {
		t.Fatalf("expected 1 batched call, got %d", got)
	}
	dirs := client.collectedDirs()
	if len(dirs) != 3 {
		t.Fatalf("expected 3 deduped dirs, got %d (%v)", len(dirs), dirs)
	}
	seen := map[string]bool{}
	for _, d := range dirs {
		seen[d] = true
	}
	for _, want := range []string{"/a", "/b", "/c"} {
		if !seen[want] {
			t.Errorf("missing dir %q in batch (got %v)", want, dirs)
		}
	}
}

func TestRepairCoalescer_NilClient_NoPanic(t *testing.T) {
	cfg := &config.Config{}
	c := NewRepairCoalescer(nil, func() *config.Config { return cfg })
	t.Cleanup(c.Close)

	// Must not panic, and must not enqueue anything.
	c.EnqueueRefresh("/foo")
	time.Sleep(50 * time.Millisecond)
}

func TestRepairCoalescer_RemovalIsVisibleToOtherHandles(t *testing.T) {
	c := newTestCoalescer(t, &fakeRcloneClient{})
	const p = "/tv/Show.S01E01/ep.mkv"

	if c.WasRemoved(p) {
		t.Fatal("nothing removed yet")
	}

	// First handle wins the debounce and removes the file.
	if !c.ShouldTrigger(p) {
		t.Fatal("first trigger must win")
	}
	c.MarkRemoved(p)

	// Second handle on the same path fails inside the debounce window: it gets
	// no trigger of its own, but must still be able to see that the file is gone
	// so it can latch itself closed.
	if c.ShouldTrigger(p) {
		t.Fatal("second trigger must be debounced")
	}
	if !c.WasRemoved(p) {
		t.Fatal("debounced handle must observe the removal")
	}

	// An unrelated path is unaffected.
	if c.WasRemoved("/tv/Other.S01E01/ep.mkv") {
		t.Fatal("removal must be scoped to the path")
	}
}

func TestRepairCoalescer_RemovalExpiresWithDebounceWindow(t *testing.T) {
	c := newTestCoalescer(t, &fakeRcloneClient{}) // debounceTTL = 100ms
	const p = "/movies/a.mkv"

	c.MarkRemoved(p)
	if !c.WasRemoved(p) {
		t.Fatal("removal must be visible immediately")
	}

	// Once the window lapses a handle takes the full repair path again, where
	// MoveToCorrupted no-ops on an already-moved source and sets the latch, so
	// the recorded removal is no longer needed.
	time.Sleep(150 * time.Millisecond)
	if c.WasRemoved(p) {
		t.Fatal("removal must expire with the debounce window")
	}
}

func TestRepairCoalescer_RemovalNilReceiverIsSafe(t *testing.T) {
	var c *RepairCoalescer
	c.MarkRemoved("/x.mkv") // must not panic
	if c.WasRemoved("/x.mkv") {
		t.Fatal("nil coalescer must report nothing removed")
	}
}
