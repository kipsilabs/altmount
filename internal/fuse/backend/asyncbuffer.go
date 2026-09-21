package backend

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultAsyncBufSize is the default read-ahead buffer size (8MB).
	defaultAsyncBufSize = 8 * 1024 * 1024
	// fillChunkSize is how much the background goroutine reads per iteration.
	// Larger chunks reduce mutex churn in ReadAtContext. Segments are ~750KB,
	// so 4MB reads ~1 ReadAtContext call per segment.
	fillChunkSize = 4 * 1024 * 1024 // 4MB
	// nearFrontierWindow is the lookahead beyond the fill frontier treated as
	// near-sequential during streaming. Set equal to fillChunkSize so that a
	// near-frontier read is guaranteed to be satisfied within at most one fill
	// iteration.
	nearFrontierWindow = 4 * 1024 * 1024 // 4MB — kept equal to fillChunkSize intentionally
	// probingSeqTolerance is the maximum forward delta in probing mode that is
	// still counted as sequential for arming read-ahead. Kernel readahead may
	// issue 128 KB parallel probes slightly out of order; 256 KB absorbs that
	// without creating large holes in the streaming start frontier.
	probingSeqTolerance = 256 * 1024 // 256 KB
	// armThreshold is how many consecutive sequential reads must be observed
	// before the buffer promotes from probing to streaming and begins reading
	// ahead. This keeps read-ahead off during the media player's header-probe
	// phase and during seek/scrub bursts (which never produce a sustained
	// sequential run), addressing the seek-thrashing that got earlier versions
	// of this buffer reverted.
	armThreshold = 3
)

// closeDrainTimeout bounds how long Wait blocks for the fill goroutine to exit.
// Callers are expected to close/interrupt the source between Shutdown and Wait
// so the goroutine exits promptly; this timeout is only a safety net against a
// source whose read cannot be unblocked. It is a var (not a const) so tests can
// shorten it.
var closeDrainTimeout = 5 * time.Second

// frontierWaitTimeout bounds how long a foreground read waits for the fill
// goroutine to reach its offset before falling back to a direct source read,
// so a wedged fill never leaves a FUSE request unanswered. It is a var (not a
// const) so tests can shorten it.
var frontierWaitTimeout = 30 * time.Second

// readAtContexter matches nzbfilesystem.MetadataVirtualFile.ReadAtContext.
type readAtContexter interface {
	ReadAtContext(ctx context.Context, p []byte, off int64) (n int, err error)
}

// Global read-ahead memory budget shared across all open buffers. A value of
// 0 means unlimited (no accounting). Buffers reserve their size on promotion
// and release it on demotion/close, so memory is only held by handles that are
// actively streaming.
var (
	globalBudgetMax  atomic.Int64 // bytes; 0 = unlimited
	globalBudgetUsed atomic.Int64 // bytes currently reserved
)

// SetAsyncBufferBudget sets the global cap (in bytes) on total read-ahead
// memory across all open buffers. 0 disables accounting (unlimited).
func SetAsyncBufferBudget(maxBytes int64) {
	if maxBytes < 0 {
		maxBytes = 0
	}
	globalBudgetMax.Store(maxBytes)
}

// reserveBudget attempts to reserve n bytes from the global budget. Returns
// (granted, accounted): accounted is true only when the reservation was
// recorded in globalBudgetUsed (i.e. a finite budget is configured), so the
// caller knows whether a matching release is required.
func reserveBudget(n int64) (granted bool, accounted bool) {
	max := globalBudgetMax.Load()
	if max <= 0 {
		return true, false // unlimited — no accounting
	}
	for {
		used := globalBudgetUsed.Load()
		if used+n > max {
			return false, false
		}
		if globalBudgetUsed.CompareAndSwap(used, used+n) {
			return true, true
		}
	}
}

func releaseBudget(n int64) {
	globalBudgetUsed.Add(-n)
}

// AsyncReadBuffer wraps a readAtContexter and reads ahead into a ring buffer so
// FUSE reads pull from pre-filled memory instead of blocking on the
// network-backed source. This mirrors the client-side read-ahead that rclone's
// VFS provides over the WebDAV endpoint, which the native FUSE mount otherwise
// lacks.
//
// State machine (the key difference from the earlier reverted design, which
// reset and refilled on every non-sequential read and thus thrashed under
// Plex/Jellyfin seek bursts):
//
//   - probing:   no buffer allocated, no fill goroutine work; every read is a
//     direct passthrough to the source. Consecutive sequential reads are
//     counted.
//   - streaming: after armThreshold sustained sequential reads, the buffer is
//     allocated (subject to the global memory budget) and a single fill
//     goroutine reads ahead. A non-sequential read (seek) demotes back to
//     probing, freeing the buffer and budget; read-ahead only re-arms after
//     sequential reads resume.
//
// A single fill goroutine is started lazily on first promotion and parked on
// the cond while probing, so promote/demote cycles never spawn goroutines.
type AsyncReadBuffer struct {
	src      readAtContexter
	ctx      context.Context
	cancel   context.CancelFunc
	fileSize int64
	bufSize  int
	log      *slog.Logger

	mu   sync.Mutex
	cond *sync.Cond

	// Ring buffer — allocated on promotion, freed on demotion/close.
	buf     []byte
	readPos int   // read cursor in ring buffer
	filled  int   // bytes currently buffered
	baseOff int64 // absolute file offset corresponding to readPos
	fillOff int64 // absolute file offset of the next fill read

	gen     uint64 // bumped on promote/demote to invalidate in-flight fills
	srcErr  error  // terminal error from source (within current generation)
	srcDone bool   // source reached EOF/err for current generation

	// State machine.
	streaming    bool  // true = buffered + filling; false = probing/passthrough
	expectedNext int64 // next sequential offset expected
	seqRun       int   // consecutive sequential reads observed while probing
	accounted    bool  // holds an accounted slot in the global budget

	started      bool // fill goroutine launched
	closed       bool
	shutdownOnce sync.Once
	waitOnce     sync.Once
	wg           sync.WaitGroup
}

// NewAsyncReadBuffer creates an async read-ahead buffer wrapping src. The fill
// goroutine is started lazily on first promotion, so opening a file that is
// only probed (header reads) and closed never allocates the buffer or spawns a
// goroutine.
func NewAsyncReadBuffer(ctx context.Context, src readAtContexter, bufSize int, fileSize int64, log *slog.Logger) *AsyncReadBuffer {
	if bufSize <= 0 {
		bufSize = defaultAsyncBufSize
	}
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(ctx)
	a := &AsyncReadBuffer{
		src:      src,
		ctx:      ctx,
		cancel:   cancel,
		fileSize: fileSize,
		bufSize:  bufSize,
		log:      log,
	}
	a.cond = sync.NewCond(&a.mu)
	return a
}

// ReadAtContext serves a read at the given absolute offset. Sequential reads in
// streaming mode are served from the ring buffer; everything else is a direct
// passthrough to the source while the sequential-run counter decides whether to
// (re)arm read-ahead.
func (a *AsyncReadBuffer) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	a.mu.Lock()
	a.markSourceDoneIfCanceledLocked()
	if a.closed {
		a.mu.Unlock()
		return a.src.ReadAtContext(ctx, p, off)
	}

	if a.streaming {
		// bufEnd is a snapshot of the fill frontier taken once; the inner-loop
		// conditions use live a.baseOff+int64(a.filled) so they see progress
		// made by the fill goroutine while we wait.
		bufEnd := a.baseOff + int64(a.filled)

		// Offset already within the buffered window.
		if off >= a.baseOff && off < bufEnd {
			n := a.copyFromBuffer(p, off)
			a.expectedNext = off + int64(n)
			a.mu.Unlock()
			return n, nil
		}

		// Sequential read at the buffer frontier — wait for the fill goroutine.
		if off == bufEnd && off == a.expectedNext {
			gen := a.gen
			a.waitForFillLocked(ctx, gen, func() bool { return a.baseOff+int64(a.filled) > off })
			if ctx.Err() != nil {
				a.mu.Unlock()
				return 0, ctx.Err()
			}
			if a.servableLocked(gen, off) {
				n := a.copyFromBuffer(p, off)
				a.expectedNext = off + int64(n)
				a.mu.Unlock()
				return n, nil
			}
			if a.currentGenLocked(gen) && a.srcDone {
				err := a.srcErr
				a.mu.Unlock()
				return 0, err
			}
		} else if off >= bufEnd && off < bufEnd+nearFrontierWindow {
			// Near-frontier: this read is just ahead of the fill frontier —
			// likely a kernel readahead parallel read, not a genuine seek. Wait
			// for the fill goroutine to reach this offset rather than demoting.
			// A full buffer ends the wait: the fill goroutine is then blocked
			// waiting for consumers to drain it, so waiting would deadlock.
			gen := a.gen
			a.waitForFillLocked(ctx, gen, func() bool {
				return a.baseOff+int64(a.filled) > off || a.filled >= a.bufSize
			})
			if ctx.Err() != nil {
				a.mu.Unlock()
				return 0, ctx.Err()
			}
			if a.servableLocked(gen, off) {
				n := a.copyFromBuffer(p, off)
				a.expectedNext = off + int64(n)
				a.mu.Unlock()
				return n, nil
			}
			if a.currentGenLocked(gen) && a.srcDone {
				err := a.srcErr
				a.mu.Unlock()
				return 0, err
			}
		}
		// Seek, closed, demoted, buffer full or wait budget expired → demote to
		// probing and serve the read directly.
		a.demoteLocked()
	}

	// Probing mode: count sequential runs, decide on promotion, then serve the
	// read directly from the source (outside the lock).
	// Treat reads within probingSeqTolerance ahead as sequential for arming
	// purposes — kernel readahead may issue 128 KB parallel probes slightly out
	// of order; 256 KB absorbs that without creating large holes in the
	// streaming start frontier.
	if off >= a.expectedNext && off <= a.expectedNext+probingSeqTolerance {
		a.seqRun++
	} else {
		a.seqRun = 1
	}
	shouldPromote := a.seqRun >= armThreshold && a.fileSize > int64(a.bufSize)
	a.mu.Unlock()

	n, err := a.src.ReadAtContext(ctx, p, off)

	a.mu.Lock()
	a.expectedNext = off + int64(n)
	if shouldPromote && n > 0 && !a.closed && !a.streaming {
		a.promoteLocked(off + int64(n))
	}
	a.mu.Unlock()
	return n, err
}

// currentGenLocked reports whether the buffer is still streaming the same
// read-ahead generation a waiter started in. A concurrent read may have
// demoted (or demoted and re-promoted) the buffer while this read slept, in
// which case its frontier bookkeeping is stale. Caller must hold a.mu.
func (a *AsyncReadBuffer) currentGenLocked(gen uint64) bool {
	return !a.closed && a.streaming && a.gen == gen
}

// servableLocked reports whether off can be served from the ring buffer within
// the waiter's own generation. Caller must hold a.mu.
func (a *AsyncReadBuffer) servableLocked(gen uint64, off int64) bool {
	return a.currentGenLocked(gen) && off >= a.baseOff && off < a.baseOff+int64(a.filled)
}

// waitForFillLocked blocks until ready() is satisfied, the read-ahead
// generation ends (source done, demote, close), the caller's context is done,
// or frontierWaitTimeout expires. Callers re-check state afterwards.
//
// sync.Cond.Wait is not woken by context cancellation, so a watchdog goroutine
// broadcasts the cond when the wait context finishes. It is only started when a
// wait is actually required. Caller must hold a.mu; it is held again on return.
func (a *AsyncReadBuffer) waitForFillLocked(ctx context.Context, gen uint64, ready func() bool) {
	if a.waitDoneLocked(ctx, gen, ready) {
		return
	}

	waitCtx, cancel := context.WithTimeout(ctx, frontierWaitTimeout)
	defer cancel()
	defer a.wakeOnDone(waitCtx)()

	for !a.waitDoneLocked(waitCtx, gen, ready) {
		a.cond.Wait()
	}
}

// waitDoneLocked reports whether a frontier wait should stop. Caller must hold
// a.mu.
func (a *AsyncReadBuffer) waitDoneLocked(ctx context.Context, gen uint64, ready func() bool) bool {
	return ready() || a.srcDone || !a.currentGenLocked(gen) || ctx.Err() != nil
}

// wakeOnDone broadcasts the cond once ctx is done, so waiters parked in
// sync.Cond.Wait re-evaluate their exit conditions. The returned stop function
// releases the watchdog goroutine; it is safe to call while holding a.mu.
func (a *AsyncReadBuffer) wakeOnDone(ctx context.Context) (stop func()) {
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			a.mu.Lock()
			a.cond.Broadcast()
			a.mu.Unlock()
		case <-stopped:
		}
	}()
	return func() { close(stopped) }
}

// promoteLocked allocates the ring buffer and (re)starts read-ahead from
// frontier. No-op if the global memory budget is exhausted (stays probing).
// Caller must hold a.mu.
func (a *AsyncReadBuffer) promoteLocked(frontier int64) {
	if a.streaming {
		return
	}
	granted, accounted := reserveBudget(int64(a.bufSize))
	if !granted {
		// Over budget — keep serving directly. seqRun stays high, so the next
		// sequential read retries the (cheap) reservation.
		return
	}
	a.accounted = accounted
	if a.buf == nil {
		a.buf = make([]byte, a.bufSize)
	}
	a.baseOff = frontier
	a.fillOff = frontier
	a.readPos = 0
	a.filled = 0
	a.srcErr = nil
	a.srcDone = false
	a.gen++
	a.streaming = true

	if !a.started {
		a.started = true
		a.wg.Add(1)
		go a.fill()
	} else {
		a.cond.Broadcast() // wake the parked fill goroutine
	}
}

// demoteLocked tears down read-ahead and returns to probing mode, freeing the
// buffer and releasing the budget so memory is only held while streaming.
// Caller must hold a.mu.
func (a *AsyncReadBuffer) demoteLocked() {
	if !a.streaming {
		return
	}
	a.streaming = false
	a.filled = 0
	a.readPos = 0
	a.buf = nil
	a.srcDone = false
	a.srcErr = nil
	a.gen++ // invalidate any in-flight fill read
	if a.accounted {
		releaseBudget(int64(a.bufSize))
		a.accounted = false
	}
	a.cond.Broadcast() // wake the fill goroutine so it re-parks
}

// fill is the single background goroutine. It parks on the cond while probing
// and reads ahead while streaming. Started lazily on first promotion.
func (a *AsyncReadBuffer) fill() {
	defer a.wg.Done()
	tmp := make([]byte, fillChunkSize)

	for {
		a.mu.Lock()
		// Park while not actively streaming.
		for !a.streaming && !a.closed && a.ctx.Err() == nil {
			a.cond.Wait()
		}
		if a.closed || a.ctx.Err() != nil {
			a.markSourceDoneIfCanceledLocked()
			a.mu.Unlock()
			return
		}
		// Park while the buffer is full or the current generation is done.
		for a.streaming && (a.filled >= a.bufSize || a.srcDone) && !a.closed && a.ctx.Err() == nil {
			a.cond.Wait()
		}
		if a.closed || a.ctx.Err() != nil {
			a.markSourceDoneIfCanceledLocked()
			a.mu.Unlock()
			return
		}
		if !a.streaming {
			a.mu.Unlock()
			continue // demoted while waiting — re-park
		}

		// End of file for this generation.
		if a.fileSize > 0 && a.fillOff >= a.fileSize {
			a.srcErr = io.EOF
			a.srcDone = true
			a.cond.Broadcast()
			a.mu.Unlock()
			continue
		}

		space := a.bufSize - a.filled
		fillOff := a.fillOff
		myGen := a.gen
		toRead := min(fillChunkSize, space)
		if a.fileSize > 0 && fillOff+int64(toRead) > a.fileSize {
			toRead = int(a.fileSize - fillOff)
		}
		a.mu.Unlock()

		// Blocking source read happens outside the lock.
		n, err := a.src.ReadAtContext(a.ctx, tmp[:toRead], fillOff)

		a.mu.Lock()
		if a.gen != myGen || !a.streaming || a.closed {
			// Demoted / seek / closed while reading — discard.
			a.mu.Unlock()
			continue
		}
		if n > 0 {
			writePos := (a.readPos + a.filled) % a.bufSize
			first := min(n, a.bufSize-writePos)
			copy(a.buf[writePos:writePos+first], tmp[:first])
			if first < n {
				copy(a.buf[:n-first], tmp[first:n])
			}
			a.filled += n
			a.fillOff += int64(n)
		}
		if err != nil {
			a.srcErr = err
			a.srcDone = true
		}
		a.cond.Broadcast()
		a.mu.Unlock()
	}
}

func (a *AsyncReadBuffer) markSourceDoneIfCanceledLocked() {
	if err := a.ctx.Err(); err != nil && a.streaming && !a.srcDone {
		a.srcErr = err
		a.srcDone = true
		a.cond.Broadcast()
	}
}

// copyFromBuffer copies buffered data into p starting at file offset off,
// draining consumed bytes from the ring. Caller must hold a.mu and must have
// verified off is within [baseOff, baseOff+filled).
func (a *AsyncReadBuffer) copyFromBuffer(p []byte, off int64) int {
	// Skip any bytes between baseOff and off (already-consumed prefix).
	if skip := int(off - a.baseOff); skip > 0 {
		a.readPos = (a.readPos + skip) % a.bufSize
		a.filled -= skip
		a.baseOff += int64(skip)
	}

	n := min(len(p), a.filled)
	first := min(n, a.bufSize-a.readPos)
	copy(p[:first], a.buf[a.readPos:a.readPos+first])
	if first < n {
		copy(p[first:n], a.buf[:n-first])
	}
	a.readPos = (a.readPos + n) % a.bufSize
	a.filled -= n
	a.baseOff += int64(n)
	a.cond.Signal() // room available — wake fill goroutine
	return n
}

// GetBufferedOffset returns the file offset up to which data is currently
// buffered (baseOff+filled), or 0 when probing.
func (a *AsyncReadBuffer) GetBufferedOffset() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.streaming {
		return 0
	}
	return a.baseOff + int64(a.filled)
}

// Shutdown signals the fill goroutine to stop without waiting for it to exit.
// It cancels the buffer's context and marks the buffer closed so the goroutine
// exits at its next lock acquisition without issuing another source read.
//
// Shutdown does NOT close the underlying source — the FUSE handle owns that
// lifecycle. Because a sequential source read blocks on the source's own
// context (not this buffer's), the caller MUST close/interrupt the source
// between Shutdown and Wait; otherwise an in-flight read cannot be unblocked
// and Wait falls back to its bounded safety-net timeout. Idempotent.
func (a *AsyncReadBuffer) Shutdown() {
	a.shutdownOnce.Do(func() {
		a.cancel()

		a.mu.Lock()
		a.closed = true
		a.srcDone = true
		a.cond.Broadcast()
		a.mu.Unlock()
	})
}

// Wait drains the fill goroutine and releases resources. It must be called
// after Shutdown (and after the underlying source has been closed/interrupted)
// so the goroutine exits promptly. The bounded wait is a safety net against a
// wedged source: with the source interrupted first it returns near-instantly.
// Idempotent.
func (a *AsyncReadBuffer) Wait() {
	a.waitOnce.Do(func() {
		// Defensive: ensure the stop signal was delivered even if a caller
		// invokes Wait without a preceding Shutdown.
		a.Shutdown()

		a.mu.Lock()
		started := a.started
		accounted := a.accounted
		a.accounted = false
		a.mu.Unlock()

		if started {
			done := make(chan struct{})
			go func() {
				a.wg.Wait()
				close(done)
			}()

			timer := time.NewTimer(closeDrainTimeout)
			defer timer.Stop()
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()

		drain:
			for {
				select {
				case <-done:
					break drain
				case <-timer.C:
					a.log.WarnContext(a.ctx, "async read buffer fill goroutine did not exit within timeout")
					break drain
				case <-ticker.C:
					a.mu.Lock()
					a.cond.Broadcast()
					a.mu.Unlock()
				}
			}
		}

		if accounted {
			releaseBudget(int64(a.bufSize))
		}

		a.mu.Lock()
		a.buf = nil
		a.mu.Unlock()
	})
}

// Close stops the fill goroutine and releases resources in a single call. It is
// equivalent to Shutdown followed by Wait. Callers that own the underlying
// source should prefer Shutdown → close source → Wait so an in-flight source
// read is unblocked promptly; Close on its own relies on the bounded safety-net
// timeout when a source read is wedged.
func (a *AsyncReadBuffer) Close() {
	a.Shutdown()
	a.Wait()
}
