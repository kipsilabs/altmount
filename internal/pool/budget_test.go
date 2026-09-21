package pool

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
)

func TestImportBudget_ZeroCapacityIsNoOp(t *testing.T) {
	b := NewImportBudget()
	for i := range 50 {
		release, err := b.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Acquire %d failed: %v", i, err)
		}
		release()
	}
	b.sem.mu.Lock()
	if b.sem.inFlight != 0 {
		t.Fatalf("disabled budget leaked inFlight=%d", b.sem.inFlight)
	}
	b.sem.mu.Unlock()
}

func TestImportBudget_BlocksAtCapacityAndWakesOnRelease(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(2)

	r1, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	r2, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err != nil {
			t.Errorf("acquire 3: %v", err)
			return
		}
		close(acquired)
		r()
	}()

	select {
	case <-acquired:
		t.Fatal("third Acquire should block at capacity 2")
	case <-time.After(50 * time.Millisecond):
	}

	r1()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("third Acquire did not unblock after release")
	}
	r2()
}

func TestImportBudget_StreamsShrinkEffectiveCap(t *testing.T) {
	src := &stubStreamSource{}
	b := NewImportBudget()
	b.SetStreamSource(src)

	// Derive the sizes from defaultStreamHeadroom rather than hardcoding them, so the
	// constant stays tunable: a capacity of 4x headroom keeps the reserve well
	// clear of the capacity-1 clamp for the 2 streams below.
	const streams = 2
	capacity := 4 * defaultStreamHeadroom
	wantCap := capacity - streams*defaultStreamHeadroom
	b.SetCapacity(capacity)

	src.set(streams)
	b.NotifyStreamChange()

	var releases []func()
	for i := range wantCap {
		r, err := b.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases = append(releases, r)
	}

	blocked := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err == nil {
			close(blocked)
			r()
		}
	}()
	select {
	case <-blocked:
		t.Fatalf("acquire %d should block at effective cap %d (capacity %d, %d streams)",
			wantCap+1, wantCap, capacity, streams)
	case <-time.After(50 * time.Millisecond):
	}

	// Streams stop -> effective cap returns to capacity, waiter granted.
	src.set(0)
	b.NotifyStreamChange()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("waiter not granted after streams ended")
	}
	for _, r := range releases {
		r()
	}
}

func TestImportBudget_FloorOfOneUnderManyStreams(t *testing.T) {
	src := &stubStreamSource{}
	b := NewImportBudget()
	b.SetStreamSource(src)
	b.SetCapacity(4)

	// Reservation would exceed capacity — cap must floor at 1, not 0.
	src.set(100)
	b.NotifyStreamChange()

	r1, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire under floor: %v", err)
	}

	blocked := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err == nil {
			close(blocked)
			r()
		}
	}()
	select {
	case <-blocked:
		t.Fatal("second Acquire should block at floored cap 1")
	case <-time.After(50 * time.Millisecond):
	}

	r1()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("waiter not granted after release at floored cap")
	}
}

func TestImportBudget_SetCapacityGrowWakesWaiters(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(1)

	hold, _ := b.Acquire(context.Background())

	granted := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err == nil {
			close(granted)
			r()
		}
	}()

	if !waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return len(b.sem.waiters) == 1
	}) {
		t.Fatal("waiter never enqueued")
	}

	b.SetCapacity(2)
	select {
	case <-granted:
	case <-time.After(time.Second):
		t.Fatal("waiter not granted after capacity grew")
	}
	hold()
}

func TestImportBudget_ShrinkBelowInFlightBlocksNewGrants(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(3)

	var releases []func()
	for i := range 3 {
		r, err := b.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases = append(releases, r)
	}

	// Shrink to 1 while 3 are in flight — existing tokens drain naturally.
	b.SetCapacity(1)

	granted := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err == nil {
			close(granted)
			r()
		}
	}()

	// Releasing two still leaves inFlight=1 == cap, so no grant yet.
	releases[0]()
	releases[1]()
	select {
	case <-granted:
		t.Fatal("Acquire granted while inFlight >= shrunken cap")
	case <-time.After(50 * time.Millisecond):
	}

	// Releasing the last one frees a slot under the new cap.
	releases[2]()
	select {
	case <-granted:
	case <-time.After(time.Second):
		t.Fatal("waiter not granted after inFlight drained below the new cap")
	}
}

func TestImportBudget_CtxCancelWhileQueued(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(1)

	hold, _ := b.Acquire(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.Acquire(ctx)
		done <- err
	}()

	if !waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return len(b.sem.waiters) == 1
	}) {
		t.Fatal("waiter never enqueued")
	}

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ctx error on cancel, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not return after ctx cancellation")
	}

	b.sem.mu.Lock()
	if len(b.sem.waiters) != 0 {
		t.Fatalf("expected 0 waiters after cancel, got %d", len(b.sem.waiters))
	}
	if b.sem.inFlight != 1 {
		t.Fatalf("expected inFlight=1, got %d", b.sem.inFlight)
	}
	b.sem.mu.Unlock()

	hold()
}

func TestImportBudget_CapacitySnapshot(t *testing.T) {
	b := NewImportBudget()
	if got := b.Capacity(); got != 0 {
		t.Fatalf("Capacity() = %d, want 0", got)
	}
	b.SetCapacity(42)
	if got := b.Capacity(); got != 42 {
		t.Fatalf("Capacity() = %d, want 42", got)
	}
	b.SetCapacity(-5)
	if got := b.Capacity(); got != 0 {
		t.Fatalf("Capacity() = %d, want 0 after negative set", got)
	}
}

func TestImportBudgetStreamsActive(t *testing.T) {
	b := NewImportBudget()

	if b.StreamsActive() {
		t.Fatal("StreamsActive() = true with no stream source wired")
	}

	src := &stubStreamSource{}
	b.SetStreamSource(src)
	if b.StreamsActive() {
		t.Fatal("StreamsActive() = true with zero active streams")
	}

	src.n.Store(1)
	if !b.StreamsActive() {
		t.Fatal("StreamsActive() = false with one active stream")
	}

	src.n.Store(0)
	if b.StreamsActive() {
		t.Fatal("StreamsActive() = true after streams drained")
	}
}

// pipeConnFactory returns an nntppool.ConnFactory backed by net.Pipe whose
// mock server only greets and answers DATE pings — enough for a pool to build
// without any real dialing.
func pipeConnFactory() nntppool.ConnFactory {
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 ready\r\n"))
			buf := make([]byte, 512)
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				if strings.HasPrefix(string(buf[:n]), "DATE") {
					_, _ = server.Write([]byte("111 20240101000000\r\n"))
				}
			}
		}()
		return client, nil
	}
}

func TestManagerStatSweepConcurrency(t *testing.T) {
	m := NewManager(context.Background(), nil)

	if got := m.StatSweepConcurrency(100); got != 100 {
		t.Fatalf("StatSweepConcurrency(100) = %d, want 100 (conservative when no pool exists)", got)
	}

	if err := m.SetProviders([]nntppool.Provider{{
		Factory:      pipeConnFactory(),
		Connections:  5,
		StatInflight: 100,
	}}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.ClearPool() }()

	// Idle pool: wider than conservative, but never the pool's full STAT
	// capacity — 5 conns × StatInflight 100 = 500 would stuff every pipeline
	// past what the per-attempt window can drain (see StatSweepConcurrency).
	if got := m.StatSweepConcurrency(100); got != 100*idleStatSweepFactor {
		t.Fatalf("StatSweepConcurrency(100) = %d, want %d (idle width capped at factor x conservative)", got, 100*idleStatSweepFactor)
	}
	// A pool whose capacity is below the widened bound stays at capacity.
	if got := m.StatSweepConcurrency(400); got != 500 {
		t.Fatalf("StatSweepConcurrency(400) = %d, want 500 (StatCapacity caps the widened idle bound)", got)
	}

	src := &stubStreamSource{}
	m.SetStreamSource(src)
	src.n.Store(1)
	if got := m.StatSweepConcurrency(100); got != 100 {
		t.Fatalf("StatSweepConcurrency(100) = %d, want 100 (conservative while streams are active)", got)
	}

	src.n.Store(0)
	if got := m.StatSweepConcurrency(1000); got != 1000 {
		t.Fatalf("StatSweepConcurrency(1000) = %d, want 1000 (conservative floor never shrinks the sweep)", got)
	}
}
