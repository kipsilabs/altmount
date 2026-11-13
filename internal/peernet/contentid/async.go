package contentid

import (
	"context"
	"sync"
	"sync/atomic"
)

// DefaultQueueSize bounds the number of pending index events.
const DefaultQueueSize = 4096

type eventKind uint8

const (
	evWritten eventKind = iota
	evDeleted
	evDirDeleted
	evDirRenamed
)

type event struct {
	kind eventKind
	a, b string
}

// AsyncObserver is a metadata.FileObserver that never does the indexing work
// on the caller's goroutine. Events go to a bounded queue served by one worker
// (Run). When the queue is full the oldest event is dropped, a counter is
// bumped and onOverflow is called so the owner can schedule a reconcile pass,
// which repairs whatever a dropped event would have done. Enqueueing never
// blocks, so the import write path is never slowed by the index.
//
// The request context of the event is deliberately not kept: the work may run
// after the request is over. Events are processed in order by one worker, so a
// delete never overtakes the write before it.
type AsyncObserver struct {
	inner      *Observer
	max        int
	onOverflow func()

	mu     sync.Mutex
	q      []event
	closed bool
	wake   chan struct{}

	dropped atomic.Int64
}

// NewAsyncObserver wraps inner. size <= 0 means DefaultQueueSize; onOverflow
// may be nil.
func NewAsyncObserver(inner *Observer, size int, onOverflow func()) *AsyncObserver {
	if size <= 0 {
		size = DefaultQueueSize
	}
	return &AsyncObserver{inner: inner, max: size, onOverflow: onOverflow, wake: make(chan struct{}, 1)}
}

// Dropped returns how many events were dropped because the queue was full.
func (a *AsyncObserver) Dropped() int64 { return a.dropped.Load() }

func (a *AsyncObserver) enqueue(e event) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	overflow := false
	if len(a.q) >= a.max {
		a.q = a.q[1:] // drop the oldest
		overflow = true
	}
	a.q = append(a.q, e)
	a.mu.Unlock()
	if overflow {
		a.dropped.Add(1)
		if a.onOverflow != nil {
			a.onOverflow()
		}
	}
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// FileWritten implements metadata.FileObserver.
func (a *AsyncObserver) FileWritten(_ context.Context, p string) {
	a.enqueue(event{kind: evWritten, a: p})
}

// FileDeleted implements metadata.FileObserver.
func (a *AsyncObserver) FileDeleted(_ context.Context, p string) {
	a.enqueue(event{kind: evDeleted, a: p})
}

// DirectoryDeleted implements metadata.FileObserver.
func (a *AsyncObserver) DirectoryDeleted(_ context.Context, p string) {
	a.enqueue(event{kind: evDirDeleted, a: p})
}

// DirectoryRenamed implements metadata.FileObserver.
func (a *AsyncObserver) DirectoryRenamed(_ context.Context, from, to string) {
	a.enqueue(event{kind: evDirRenamed, a: from, b: to})
}

func (a *AsyncObserver) pop() (event, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.q) == 0 {
		return event{}, false
	}
	e := a.q[0]
	a.q = a.q[1:]
	return e, true
}

// Run is the single worker. It returns when ctx is done; events still queued
// at that point are discarded (the next enable reconciles the index anyway)
// and later events are ignored.
func (a *AsyncObserver) Run(ctx context.Context) {
	defer func() {
		a.mu.Lock()
		a.closed, a.q = true, nil
		a.mu.Unlock()
	}()
	for {
		for {
			if ctx.Err() != nil {
				return
			}
			e, ok := a.pop()
			if !ok {
				break
			}
			switch e.kind {
			case evWritten:
				a.inner.FileWritten(ctx, e.a)
			case evDeleted:
				a.inner.FileDeleted(ctx, e.a)
			case evDirDeleted:
				a.inner.DirectoryDeleted(ctx, e.a)
			case evDirRenamed:
				a.inner.DirectoryRenamed(ctx, e.a, e.b)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-a.wake:
		}
	}
}
