package backend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingSource implements readAtContexter for testing. It records the number
// of source reads (so tests can assert that read-ahead did/didn't happen) and
// supports an interruptible per-read delay to simulate a slow network source.
type countingSource struct {
	data    []byte
	delay   time.Duration
	readErr error // error to return once data is exhausted (nil → io.EOF)

	reads atomic.Int64
}

func (m *countingSource) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	m.reads.Add(1)
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if off >= int64(len(m.data)) {
		if m.readErr != nil {
			return 0, m.readErr
		}
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func testData(size int) []byte {
	d := make([]byte, size)
	for i := range d {
		d[i] = byte(i % 251) // prime stride so windows are distinguishable
	}
	return d
}

// readAllSequential reads the whole file in fixed-size chunks and returns the
// concatenated bytes. Drives the buffer through probing → streaming.
func readAllSequential(t *testing.T, a *AsyncReadBuffer, size, chunk int) []byte {
	t.Helper()
	got := make([]byte, 0, size)
	p := make([]byte, chunk)
	off := int64(0)
	for off < int64(size) {
		n, err := a.ReadAtContext(context.Background(), p, off)
		if n > 0 {
			got = append(got, p[:n]...)
			off += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("unexpected error at offset %d: %v", off, err)
		}
	}
	return got
}

func TestAsyncReadBuffer_BasicSequentialRead(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(64 * 1024)
	src := &countingSource{data: data}
	a := NewAsyncReadBuffer(context.Background(), src, 16*1024, int64(len(data)), nil)
	defer a.Close()

	got := readAllSequential(t, a, len(data), 512)
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestAsyncReadBuffer_PromotesAndServesInstantly(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(64 * 1024)
	src := &countingSource{data: data, delay: 5 * time.Millisecond}
	bufSize := 32 * 1024
	a := NewAsyncReadBuffer(context.Background(), src, bufSize, int64(len(data)), nil)
	defer a.Close()

	// Three sustained sequential reads promote to streaming.
	p := make([]byte, 1024)
	for i := range armThreshold {
		off := int64(i * 1024)
		if _, err := a.ReadAtContext(context.Background(), p, off); err != nil {
			t.Fatalf("seed read %d: %v", i, err)
		}
	}

	// Give the fill goroutine time to read ahead from the frontier (3072).
	deadline := time.Now().Add(2 * time.Second)
	for a.GetBufferedOffset() <= int64(armThreshold*1024) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if a.GetBufferedOffset() <= int64(armThreshold*1024) {
		t.Fatalf("buffer did not read ahead after promotion (buffered=%d)", a.GetBufferedOffset())
	}

	// A read at the frontier should now be served from memory (instant).
	start := time.Now()
	n, err := a.ReadAtContext(context.Background(), p, int64(armThreshold*1024))
	if err != nil && err != io.EOF {
		t.Fatalf("frontier read: %v", err)
	}
	if dur := time.Since(start); dur > 4*time.Millisecond {
		t.Fatalf("frontier read took %v, expected near-instant (buffered)", dur)
	}
	if !bytes.Equal(p[:n], data[armThreshold*1024:armThreshold*1024+n]) {
		t.Fatal("frontier read: data mismatch")
	}
}

func TestAsyncReadBuffer_EOFPropagation(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(8 * 1024)
	src := &countingSource{data: data}
	a := NewAsyncReadBuffer(context.Background(), src, 4*1024, int64(len(data)), nil)
	defer a.Close()

	got := readAllSequential(t, a, len(data), 1024)
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch before EOF")
	}
	// Reading at/after EOF returns io.EOF.
	n, err := a.ReadAtContext(context.Background(), make([]byte, 16), int64(len(data)))
	if n != 0 || err != io.EOF {
		t.Fatalf("expected io.EOF at end, got n=%d err=%v", n, err)
	}
}

func TestAsyncReadBuffer_SeekPreservesCorrectness(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(64 * 1024)
	src := &countingSource{data: data}
	a := NewAsyncReadBuffer(context.Background(), src, 16*1024, int64(len(data)), nil)
	defer a.Close()

	p := make([]byte, 1024)

	// Stream a bit to promote.
	for i := range armThreshold + 2 {
		off := int64(i * 1024)
		n, err := a.ReadAtContext(context.Background(), p, off)
		if err != nil {
			t.Fatalf("pre-seek read: %v", err)
		}
		if !bytes.Equal(p[:n], data[off:off+int64(n)]) {
			t.Fatalf("pre-seek read data mismatch at %d", off)
		}
	}

	// Seek backwards (non-sequential) — demotes; data must still be correct.
	n, err := a.ReadAtContext(context.Background(), p, 40000)
	if err != nil {
		t.Fatalf("seek read: %v", err)
	}
	if !bytes.Equal(p[:n], data[40000:40000+int64(n)]) {
		t.Fatal("seek read: data mismatch")
	}

	// Resume sequential reads from the new position — must re-promote and stay correct.
	off := int64(40000 + n)
	for range armThreshold + 2 {
		rn, err := a.ReadAtContext(context.Background(), p, off)
		if err != nil && err != io.EOF {
			t.Fatalf("post-seek read: %v", err)
		}
		if !bytes.Equal(p[:rn], data[off:off+int64(rn)]) {
			t.Fatalf("post-seek read data mismatch at %d", off)
		}
		off += int64(rn)
	}
}

// TestAsyncReadBuffer_ScrubNeverPromotes verifies the anti-thrash guarantee:
// a burst of non-sequential reads must never allocate the buffer or read ahead.
func TestAsyncReadBuffer_ScrubNeverPromotes(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(1024 * 1024)
	src := &countingSource{data: data}
	a := NewAsyncReadBuffer(context.Background(), src, 64*1024, int64(len(data)), nil)
	defer a.Close()

	p := make([]byte, 4096)
	// Jump around so no two reads are sequential.
	offsets := []int64{0, 500000, 10000, 700000, 30000, 900000, 50000, 200000}
	for _, off := range offsets {
		if _, err := a.ReadAtContext(context.Background(), p, off); err != nil {
			t.Fatalf("scrub read at %d: %v", off, err)
		}
	}

	if bo := a.GetBufferedOffset(); bo != 0 {
		t.Fatalf("buffer promoted during scrub (buffered=%d), expected 0", bo)
	}
	// One source read per ReadAtContext, no prefetch reads.
	if got := src.reads.Load(); got != int64(len(offsets)) {
		t.Fatalf("scrub triggered %d source reads, want %d (no read-ahead)", got, len(offsets))
	}
}

func TestAsyncReadBuffer_ZeroLengthRead(t *testing.T) {
	SetAsyncBufferBudget(0)
	src := &countingSource{data: testData(128)}
	a := NewAsyncReadBuffer(context.Background(), src, 1024, 128, nil)
	defer a.Close()

	n, err := a.ReadAtContext(context.Background(), nil, 0)
	if n != 0 || err != nil {
		t.Fatalf("zero-length read: n=%d err=%v", n, err)
	}
}

func TestAsyncReadBuffer_CloseDuringFill(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(4 * 1024 * 1024)
	src := &countingSource{data: data, delay: 100 * time.Millisecond}
	a := NewAsyncReadBuffer(context.Background(), src, 256*1024, int64(len(data)), nil)

	// Promote so the fill goroutine is active and blocked in a slow source read.
	p := make([]byte, 1024)
	for i := range armThreshold {
		_, _ = a.ReadAtContext(context.Background(), p, int64(i*1024))
	}

	done := make(chan struct{})
	go func() {
		a.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close() hung")
	}
}

func TestAsyncReadBuffer_CanceledFillContextDoesNotHangFrontierRead(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(4 * 1024 * 1024)
	parentCtx, cancel := context.WithCancel(context.Background())
	src := &countingSource{data: data}
	a := NewAsyncReadBuffer(parentCtx, src, 256*1024, int64(len(data)), nil)
	defer a.Close()

	p := make([]byte, 1024)
	for i := range armThreshold {
		off := int64(i * len(p))
		if _, err := a.ReadAtContext(context.Background(), p, off); err != nil {
			t.Fatalf("seed read %d: %v", i, err)
		}
	}

	cancel()

	done := make(chan error, 1)
	go func() {
		readCtx, readCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer readCancel()
		_, err := a.ReadAtContext(readCtx, p, int64(armThreshold*len(p)))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("frontier read after fill cancellation returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("frontier read hung after fill context cancellation")
	}
}

func TestAsyncReadBuffer_ConcurrentReadClose(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(2 * 1024 * 1024)
	src := &countingSource{data: data, delay: time.Millisecond}
	a := NewAsyncReadBuffer(context.Background(), src, 256*1024, int64(len(data)), nil)

	var wg sync.WaitGroup
	wg.Go(func() {
		p := make([]byte, 512)
		off := int64(0)
		for range 200 {
			n, err := a.ReadAtContext(context.Background(), p, off)
			if err != nil {
				return
			}
			off += int64(n)
		}
	})

	time.Sleep(10 * time.Millisecond)
	a.Close()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent read+close hung")
	}
}

// TestAsyncReadBuffer_NoGoroutineLeak opens/streams/closes many buffers and
// asserts the goroutine count returns to baseline — the regression that got
// earlier versions reverted.
func TestAsyncReadBuffer_NoGoroutineLeak(t *testing.T) {
	SetAsyncBufferBudget(0)
	data := testData(256 * 1024)

	// Warm up once so lazy runtime goroutines are already started.
	{
		src := &countingSource{data: data}
		a := NewAsyncReadBuffer(context.Background(), src, 64*1024, int64(len(data)), nil)
		_ = readAllSequential(t, a, len(data), 1024)
		a.Close()
	}
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	for range 50 {
		src := &countingSource{data: data, delay: time.Millisecond}
		a := NewAsyncReadBuffer(context.Background(), src, 64*1024, int64(len(data)), nil)
		p := make([]byte, 1024)
		for j := range armThreshold + 1 {
			_, _ = a.ReadAtContext(context.Background(), p, int64(j*1024))
		}
		a.Close()
	}

	// Allow stragglers to exit.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= baseline+2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline+2 {
		t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, got)
	}
}

// TestAsyncReadBuffer_NearFrontierWaitDoesNotDemote verifies that a read
// slightly ahead of the fill frontier (simulating kernel readahead parallelism)
// waits for the fill goroutine rather than demoting to probing mode.
func TestAsyncReadBuffer_NearFrontierWaitDoesNotDemote(t *testing.T) {
	SetAsyncBufferBudget(0)
	// Source must be larger than bufSize + nearFrontierWindow so the file is big
	// enough that streaming is expected and nearFrontierWindow fits inside.
	totalSize := defaultAsyncBufSize + nearFrontierWindow + 1*1024*1024
	data := testData(totalSize)
	src := &countingSource{data: data, delay: 5 * time.Millisecond}

	bufSize := defaultAsyncBufSize
	a := NewAsyncReadBuffer(context.Background(), src, bufSize, int64(totalSize), nil)
	defer a.Close()

	p := make([]byte, 1024)

	// Drive enough sequential reads to promote into streaming mode (>= armThreshold).
	for i := range armThreshold + 2 {
		off := int64(i * len(p))
		if _, err := a.ReadAtContext(context.Background(), p, off); err != nil {
			t.Fatalf("seed read %d: %v", i, err)
		}
	}

	// Wait until the fill goroutine has buffered at least a few bytes ahead.
	deadline := time.Now().Add(2 * time.Second)
	for a.GetBufferedOffset() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	bufEnd := a.GetBufferedOffset()
	if bufEnd == 0 {
		t.Fatal("buffer did not start filling after promotion")
	}

	// Confirm streaming was active before the near-frontier read so we know the
	// assertion below is meaningful (streaming was active throughout, not just
	// re-established after a demote).
	if bufEnd == 0 {
		t.Fatal("streaming was not active before near-frontier read")
	}

	// Issue a near-frontier read: 64KB ahead of current bufEnd.
	nearOff := bufEnd + 64*1024
	result := make([]byte, len(p))

	done := make(chan error, 1)
	go func() {
		_, err := a.ReadAtContext(context.Background(), result, nearOff)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil && err != io.EOF {
			t.Fatalf("near-frontier read returned unexpected error: %v", err)
		}
		// Verify the data is correct.
		if nearOff < int64(totalSize) {
			n := min(len(result), totalSize-int(nearOff))
			if !bytes.Equal(result[:n], data[nearOff:nearOff+int64(n)]) {
				t.Fatal("near-frontier read: data mismatch")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("near-frontier read hung — possible deadlock or no progress")
	}

	// Confirm the buffer is still in streaming mode (not demoted).
	if bo := a.GetBufferedOffset(); bo == 0 {
		t.Fatal("buffer was demoted during near-frontier read; expected to stay streaming")
	}
}

// TestAsyncReadBuffer_BudgetGate verifies that when the global budget is
// exhausted, additional buffers stay in passthrough (probing) mode.
func TestAsyncReadBuffer_BudgetGate(t *testing.T) {
	bufSize := 64 * 1024
	SetAsyncBufferBudget(int64(bufSize)) // room for exactly one buffer
	defer SetAsyncBufferBudget(0)

	data := testData(256 * 1024)
	mk := func() *AsyncReadBuffer {
		src := &countingSource{data: data}
		a := NewAsyncReadBuffer(context.Background(), src, bufSize, int64(len(data)), nil)
		p := make([]byte, 1024)
		for j := range armThreshold {
			_, _ = a.ReadAtContext(context.Background(), p, int64(j*1024))
		}
		return a
	}

	a1 := mk()
	defer a1.Close()
	// Let a1 read ahead.
	deadline := time.Now().Add(time.Second)
	for a1.GetBufferedOffset() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if a1.GetBufferedOffset() == 0 {
		t.Fatal("a1 should have promoted within budget")
	}

	a2 := mk()
	defer a2.Close()
	// a2 cannot reserve budget → stays probing.
	if bo := a2.GetBufferedOffset(); bo != 0 {
		t.Fatalf("a2 promoted despite exhausted budget (buffered=%d)", bo)
	}

	// After a1 releases, a2 can promote on its next sequential read.
	a1.Close()
	p := make([]byte, 1024)
	off := int64(armThreshold * 1024)
	for range armThreshold + 1 {
		_, _ = a2.ReadAtContext(context.Background(), p, off)
		off += 1024
	}
	deadline = time.Now().Add(time.Second)
	for a2.GetBufferedOffset() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if a2.GetBufferedOffset() == 0 {
		t.Fatal("a2 should promote after a1 released budget")
	}
}

// blockingSource models a source whose read-ahead read blocks on its OWN
// lifecycle (e.g. the UsenetReader's context) rather than the per-read context
// the async buffer passes in. Small probe reads are served immediately so the
// buffer can promote; the first large read-ahead read parks until interrupt()
// is called — modeling MetadataVirtualFile.Close interrupting an in-flight
// sequential read. The passed ctx is intentionally ignored on the blocking
// path, reproducing the cancellation-ownership gap behind the timeout warning.
type blockingSource struct {
	data     []byte
	blockMin int // reads with len(p) >= blockMin block

	blocked    chan struct{} // closed once a read has parked
	release    chan struct{} // closed by interrupt() to unblock the read
	blockOnce  sync.Once
	signalOnce sync.Once
}

func newBlockingSource(data []byte, blockMin int) *blockingSource {
	return &blockingSource{
		data:     data,
		blockMin: blockMin,
		blocked:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (b *blockingSource) ReadAtContext(_ context.Context, p []byte, off int64) (int, error) {
	if len(p) >= b.blockMin {
		b.signalOnce.Do(func() { close(b.blocked) })
		<-b.release // ignore the passed ctx — only an external interrupt unblocks
		return 0, io.ErrClosedPipe
	}
	if off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	return copy(p, b.data[off:]), nil
}

// interrupt unblocks the parked read, modeling the underlying source being
// closed by the FUSE handle between Shutdown and Wait.
func (b *blockingSource) interrupt() { b.blockOnce.Do(func() { close(b.release) }) }

// warnRecorder is a slog.Handler that records whether the drain-timeout warning
// was emitted.
type warnRecorder struct {
	mu  sync.Mutex
	got bool
}

func (w *warnRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (w *warnRecorder) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "did not exit within timeout") {
		w.mu.Lock()
		w.got = true
		w.mu.Unlock()
	}
	return nil
}

func (w *warnRecorder) WithAttrs([]slog.Attr) slog.Handler { return w }
func (w *warnRecorder) WithGroup(string) slog.Handler      { return w }

func (w *warnRecorder) warned() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.got
}

// promoteBlockingSource drives armThreshold sequential reads to promote the
// buffer, then waits for the fill goroutine to park inside the source read.
func promoteBlockingSource(t *testing.T, a *AsyncReadBuffer, src *blockingSource) {
	t.Helper()
	p := make([]byte, 1024)
	for i := range armThreshold {
		if _, err := a.ReadAtContext(context.Background(), p, int64(i*len(p))); err != nil {
			t.Fatalf("seed read %d: %v", i, err)
		}
	}
	select {
	case <-src.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("fill goroutine did not start a read-ahead read")
	}
}

// TestAsyncReadBuffer_ShutdownThenSourceCloseDrainsPromptly reproduces the
// Handle.Release ordering (Shutdown → close source → Wait) and verifies the
// fill goroutine drains promptly without logging the timeout warning — the
// regression that caused "async read buffer fill goroutine did not exit within
// timeout" on every Release with read-ahead in flight.
func TestAsyncReadBuffer_ShutdownThenSourceCloseDrainsPromptly(t *testing.T) {
	SetAsyncBufferBudget(0)
	prev := closeDrainTimeout
	closeDrainTimeout = 2 * time.Second
	defer func() { closeDrainTimeout = prev }()

	rec := &warnRecorder{}
	data := testData(4 * 1024 * 1024)
	src := newBlockingSource(data, 4096)
	a := NewAsyncReadBuffer(context.Background(), src, 256*1024, int64(len(data)), slog.New(rec))

	promoteBlockingSource(t, a, src)

	// Real Release ordering: signal stop, then unblock the source, then drain.
	a.Shutdown()
	src.interrupt()

	start := time.Now()
	a.Wait()
	if d := time.Since(start); d >= closeDrainTimeout {
		t.Fatalf("Wait took %v, expected a prompt drain well under the %v timeout", d, closeDrainTimeout)
	}
	if rec.warned() {
		t.Fatal("timeout warning was logged despite the source being interrupted before Wait")
	}
}

// TestAsyncReadBuffer_WaitWithoutSourceCloseHitsSafetyNet verifies the bounded
// drain remains a safety net: when the source read cannot be unblocked, Wait
// blocks until closeDrainTimeout and logs the warning.
func TestAsyncReadBuffer_WaitWithoutSourceCloseHitsSafetyNet(t *testing.T) {
	SetAsyncBufferBudget(0)
	prev := closeDrainTimeout
	closeDrainTimeout = 200 * time.Millisecond
	defer func() { closeDrainTimeout = prev }()

	rec := &warnRecorder{}
	data := testData(4 * 1024 * 1024)
	src := newBlockingSource(data, 4096)
	a := NewAsyncReadBuffer(context.Background(), src, 256*1024, int64(len(data)), slog.New(rec))
	defer src.interrupt() // let the parked goroutine exit after the test

	promoteBlockingSource(t, a, src)

	// Close without interrupting the source: the read stays wedged, so the drain
	// must fall back to the bounded safety-net timeout and log the warning.
	start := time.Now()
	a.Close()
	if d := time.Since(start); d < closeDrainTimeout {
		t.Fatalf("Close returned in %v, expected it to block until the %v safety-net timeout", d, closeDrainTimeout)
	}
	if !rec.warned() {
		t.Fatal("expected timeout warning when the source read is wedged")
	}
}

// TestAsyncReadBuffer_FrontierReadHonorsCallerDeadlineWhileFillBlocked covers
// the reported stall (issue #948): a read waiting at the fill frontier slept in
// sync.Cond.Wait, which cancellation does not wake, so the read never observed
// its own deadline while the background fill was wedged.
func TestAsyncReadBuffer_FrontierReadHonorsCallerDeadlineWhileFillBlocked(t *testing.T) {
	SetAsyncBufferBudget(0)

	data := testData(4 * 1024 * 1024)
	src := newBlockingSource(data, 4096)
	a := NewAsyncReadBuffer(context.Background(), src, 256*1024, int64(len(data)), nil)
	defer a.Close()
	defer src.interrupt()

	promoteBlockingSource(t, a, src)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		// Exactly at the current fill frontier.
		_, err := a.ReadAtContext(ctx, make([]byte, 1024), int64(armThreshold*1024))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ReadAtContext returned %v, want context deadline exceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("frontier read ignored its context deadline while waiting on sync.Cond")
	}
}

// TestAsyncReadBuffer_FrontierReadSurvivesConcurrentDemote verifies that a read
// parked at the frontier wakes and completes when another concurrent read
// demotes the buffer. Nothing broadcasts the cond again after a demote, so a
// wait loop that only watched filled/srcDone slept forever.
func TestAsyncReadBuffer_FrontierReadSurvivesConcurrentDemote(t *testing.T) {
	SetAsyncBufferBudget(0)

	// Large enough that the seek below lands outside nearFrontierWindow, so it
	// demotes immediately instead of waiting at the frontier itself.
	data := testData(16 * 1024 * 1024)
	src := newBlockingSource(data, 4096)
	a := NewAsyncReadBuffer(context.Background(), src, 256*1024, int64(len(data)), nil)
	defer a.Close()
	defer src.interrupt()

	promoteBlockingSource(t, a, src)

	frontier := int64(armThreshold * 1024)
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		p := make([]byte, 1024)
		n, err := a.ReadAtContext(context.Background(), p, frontier)
		done <- result{n: n, err: err}
	}()

	// Let the frontier read park on the cond, then seek far away so the other
	// read demotes read-ahead out from under it.
	time.Sleep(100 * time.Millisecond)
	if _, err := a.ReadAtContext(context.Background(), make([]byte, 1024), 12*1024*1024); err != nil {
		t.Fatalf("seek read: %v", err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("frontier read after demote: %v", res.err)
		}
		if res.n != 1024 {
			t.Fatalf("frontier read returned %d bytes, want 1024", res.n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("frontier read hung after a concurrent demote")
	}
}

// TestAsyncReadBuffer_FrontierWaitFallsBackWhenFillStalls verifies that a
// foreground read is never left unanswered by a wedged background fill: once
// the frontier wait budget expires the read falls back to a direct source read.
func TestAsyncReadBuffer_FrontierWaitFallsBackWhenFillStalls(t *testing.T) {
	SetAsyncBufferBudget(0)
	prev := frontierWaitTimeout
	frontierWaitTimeout = 100 * time.Millisecond
	defer func() { frontierWaitTimeout = prev }()

	data := testData(4 * 1024 * 1024)
	src := newBlockingSource(data, 4096)
	a := NewAsyncReadBuffer(context.Background(), src, 256*1024, int64(len(data)), nil)
	defer a.Close()
	defer src.interrupt()

	promoteBlockingSource(t, a, src)

	frontier := int64(armThreshold * 1024)
	done := make(chan error, 1)
	go func() {
		p := make([]byte, 1024)
		n, err := a.ReadAtContext(context.Background(), p, frontier)
		if err == nil && !bytes.Equal(p[:n], data[frontier:frontier+int64(n)]) {
			err = errors.New("fallback read returned wrong data")
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("frontier read fallback: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("frontier read never gave up on the wedged fill")
	}
}
