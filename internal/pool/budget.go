package pool

import (
	"context"
)

// defaultStreamHeadroom is the per-stream connection reserve used until the
// adaptive controller (see headroom.go) learns a better one, and the value the
// budget falls back to when no controller is wired.
//
// It is deliberately small. The budget bounds in-flight import BODIES pool-wide;
// it does not pin which connections are free, so raising it does not buy stream
// latency by keeping a connection idle. What it does buy — measured — is
// bandwidth share: on a saturated link, fewer concurrent import bodies leaves
// more of the pipe for playback. That effect is real but its right size depends
// on the link rate and pool size, which is exactly what a constant cannot know.
const defaultStreamHeadroom = 2

// ImportBudget bounds the total number of in-flight import segment (body)
// fetches pool-wide, across all concurrent imports. Its capacity tracks the
// pool's total connection count and automatically shrinks while streams are
// active:
//
//	effective cap = capacity − min(streamHeadroom × activeStreams, capacity−1)
//
// so imports expand to the full pool when idle, yield headroom to streams
// under playback, and always keep at least 1 connection so a lone import can
// make progress. A capacity of 0 disables the budget (no-op), which keeps
// pool-less paths and test fakes deadlock-free.
type ImportBudget struct {
	sem          adaptiveSemaphore
	capacity     int
	streamSource StreamActivitySource
	// headroom is the per-stream connection reserve. Owned by the adaptive
	// controller when one is running; defaultStreamHeadroom otherwise.
	headroom    int
	idleChanged chan struct{}
}

// NewImportBudget constructs a budget with capacity 0 (disabled). Use
// SetCapacity and SetStreamSource to configure it.
func NewImportBudget() *ImportBudget {
	b := &ImportBudget{headroom: defaultStreamHeadroom}
	b.sem.capLocked = b.effectiveCapLocked
	return b
}

// effectiveCapLocked computes the current cap. Called with sem.mu held.
func (b *ImportBudget) effectiveCapLocked() int {
	if b.capacity <= 0 {
		return 0 // disabled
	}
	reserve := 0
	if b.streamSource != nil {
		reserve = b.headroom * b.streamSource.ActiveStreams()
	}
	if reserve > b.capacity-1 {
		reserve = b.capacity - 1
	}
	return b.capacity - reserve
}

// SetCapacity updates the total connection capacity (sum of provider
// connections). Queued waiters are woken if the effective cap grew; on shrink,
// in-flight fetches drain naturally.
func (b *ImportBudget) SetCapacity(totalConns int) {
	if totalConns < 0 {
		totalConns = 0
	}
	b.sem.mu.Lock()
	b.capacity = totalConns
	b.sem.wakeWaitersLocked()
	b.sem.mu.Unlock()
}

// Capacity returns the configured total capacity (not the stream-adjusted
// effective cap). Useful for sizing worker pools.
func (b *ImportBudget) Capacity() int {
	b.sem.mu.Lock()
	defer b.sem.mu.Unlock()
	return b.capacity
}

// SetHeadroom sets the per-stream connection reserve and wakes any waiters the
// change frees. Called by the adaptive controller.
func (b *ImportBudget) SetHeadroom(n int) {
	if n < 0 {
		n = 0
	}
	b.sem.mu.Lock()
	b.headroom = n
	b.sem.wakeWaitersLocked()
	b.sem.mu.Unlock()
}

// Headroom returns the current per-stream connection reserve.
func (b *ImportBudget) Headroom() int {
	b.sem.mu.Lock()
	defer b.sem.mu.Unlock()
	return b.headroom
}

// SetStreamSource wires the activity signal. nil sources are tolerated and
// pin the effective cap to the full capacity.
func (b *ImportBudget) SetStreamSource(src StreamActivitySource) {
	b.sem.mu.Lock()
	b.streamSource = src
	b.signalIdleChangeLocked()
	b.sem.wakeWaitersLocked()
	b.sem.mu.Unlock()
}

// NotifyStreamChange should be called when the stream count changes so the
// budget can wake or hold waiters according to the new effective cap.
func (b *ImportBudget) NotifyStreamChange() {
	b.sem.mu.Lock()
	b.signalIdleChangeLocked()
	b.sem.wakeWaitersLocked()
	b.sem.mu.Unlock()
}

// StreamsActive reports whether the wired stream source currently has at
// least one active stream. False when no source is wired.
func (b *ImportBudget) StreamsActive() bool {
	b.sem.mu.Lock()
	defer b.sem.mu.Unlock()
	return b.streamSource != nil && b.streamSource.ActiveStreams() > 0
}

// Acquire blocks until a connection token is available or ctx is cancelled.
// The returned release function MUST be called exactly once when the fetch is
// done. When the capacity is 0 the call is a fast-path no-op.
func (b *ImportBudget) Acquire(ctx context.Context) (release func(), err error) {
	return b.sem.Acquire(ctx)
}

// WaitForIdle defers background network work while playback is active. Call it
// between chunks so a newly opened stream stops further pipeline submission.
func (b *ImportBudget) WaitForIdle(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.sem.mu.Lock()
		if b.streamSource == nil || b.streamSource.ActiveStreams() == 0 {
			b.sem.mu.Unlock()
			return nil
		}
		if b.idleChanged == nil {
			b.idleChanged = make(chan struct{})
		}
		changed := b.idleChanged
		b.sem.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (b *ImportBudget) signalIdleChangeLocked() {
	if b.idleChanged != nil {
		close(b.idleChanged)
		b.idleChanged = nil
	}
}
