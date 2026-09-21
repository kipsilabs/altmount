package usenet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/slogutil"
	"github.com/javi11/nntppool/v5"
)

const (
	defaultMaxPrefetch = 60 // Default to 60 segments prefetched ahead
)

var (
	_ io.ReadCloser = &UsenetReader{}
)

type MetricsTracker interface {
	IncArticlesDownloaded()
	IncArticlesPosted()
	UpdateDownloadProgress(id string, bytesDownloaded int64)
}

// SegmentStore is an optional cache for decoded segment data.
// Implementations must be safe for concurrent use.
type SegmentStore interface {
	Get(messageID string) ([]byte, bool)
	Put(messageID string, data []byte) error
}

// HoleHooks lets the owner of a reader decide, synchronously, what happens
// when a segment is confirmed missing (ErrArticleNotFound — never retried).
// The reader stays dumb: it asks, the owner accounts, persists and
// transitions health status. Segments approved for padding are zero-filled
// in place so the read loop never sees an error and playback continues.
// Both callbacks run on download goroutines: they must be concurrency-safe
// and fast (no network).
type HoleHooks struct {
	// OnHole returns the pad/fail decision for a missing segment, identified
	// by its index in the file's segment space.
	OnHole func(segIndex int, segID string) holes.Decision
	// KnownHoles reports segments already known missing: those are
	// zero-filled immediately, without any fetch (replay pre-pad).
	KnownHoles func(segIndex int) bool
	// PatchLookup returns the repaired payload for a missing segment, or nil.
	// Consulted only on the hole path (known hole, or confirmed missing on
	// every provider) — never on healthy reads. Must be fast local I/O and
	// concurrency-safe. A payload whose size does not match the segment is
	// ignored. May be set alone (without OnHole/KnownHoles) so repaired
	// articles serve even for files ineligible for zero-fill.
	PatchLookup func(segID string) []byte
}

// defaultPatchLookup serves PAR2-repaired article payloads to every reader
// that has no explicit PatchLookup of its own — notably the import path, whose
// readers are built deep inside the parser. Repaired payloads are byte-exact
// and verified against the release's PAR2 checksums before being stored, and
// they are only ever consulted for articles the providers have dropped, so
// serving them anywhere is always correct.
var defaultPatchLookup atomic.Pointer[func(segID string) []byte]

// SetDefaultPatchLookup installs the process-wide patch lookup. Pass nil to
// clear it. Call during boot, before readers are created.
func SetDefaultPatchLookup(fn func(segID string) []byte) {
	if fn == nil {
		defaultPatchLookup.Store(nil)
		return
	}
	defaultPatchLookup.Store(&fn)
}

// patchLookupFor returns the reader's own PatchLookup, falling back to the
// process-wide default.
func (b *UsenetReader) patchLookupFor() func(segID string) []byte {
	if b.holeHooks != nil && b.holeHooks.PatchLookup != nil {
		return b.holeHooks.PatchLookup
	}
	if fn := defaultPatchLookup.Load(); fn != nil {
		return *fn
	}
	return nil
}

// ReaderOption customizes a UsenetReader.
type ReaderOption func(*UsenetReader)

// WithHoleHooks enables zero-filling of confirmed-missing segments under the
// owner's control. Without it, a missing segment fails the read as always.
func WithHoleHooks(h *HoleHooks) ReaderOption {
	return func(r *UsenetReader) {
		r.holeHooks = h
	}
}

// WithArticleDate tells the pool when this file's articles were posted, so a
// provider whose retention does not reach back that far is tried last or not
// at all (see nntppool Provider.MaxArticleAge). The date comes from the file's
// metadata release_date; a zero time means it is unknown, which applies no
// retention policy rather than assuming the articles are old.
func WithArticleDate(at time.Time) ReaderOption {
	return func(r *UsenetReader) {
		r.articleDate = at
	}
}

// SpecBudget grants non-blocking read-ahead slots shared across every stream
// on the pool. nil means unlimited. Implemented by *pool.SpeculativeBudget.
type SpecBudget interface {
	TryAcquire() (release func(), ok bool)
}

// demandDepth is how many segments at and just past the read position are
// fetched unconditionally. Everything further ahead is speculative and must
// find a free slot in the shared budget.
const demandDepth = 2

// WithSpeculativeBudget bounds this reader's read-ahead by a budget shared
// with every other stream, so several handles cannot each open a full
// window. Demand fetches never take a slot.
func WithSpeculativeBudget(b SpecBudget) ReaderOption {
	return func(r *UsenetReader) {
		r.specBudget = b
	}
}

const (
	// largeArticleHoldSegments is the read-ahead a fresh streaming reader
	// opens with on large-article posts, kept until the caller has read its
	// first byte. Holding the fan-out back keeps the demand article from
	// queueing behind a window of speculative ones on the same connections;
	// on 4 MiB parts that is the difference between seconds and one round
	// trip to first byte. On small articles the queueing costs a few
	// milliseconds and the hold would only slow startup, so it is skipped.
	largeArticleHoldSegments = 2
	// largeArticleBytes is the article size from which the hold applies.
	largeArticleBytes = 2 << 20
	// openingSegments is the window before the first byte on small-article
	// posts: wide enough that startup runs at full speed once bytes flow,
	// narrow enough that a handle closed before its first byte arrives has
	// not fanned a whole window out. Any consumption opens the window fully;
	// a narrower ramp was measured to cost 16-25 % of a 16 MB startup.
	openingSegments = 16
	// readAheadBytesCap bounds the window in bytes as well as segments. A
	// 60-segment window is 45 MB on 750 KB posts but 240 MB on 4 MiB posts,
	// where it starves the reader's own demand article for the link and
	// leaves a quarter of a gigabyte to abandon on every seek.
	readAheadBytesCap = config.StreamReadAheadBytesCap
)

// withFlightMap gives the reader its own in-flight article map. Tests use it
// so parallel tests reusing message-IDs do not join each other's downloads.
func withFlightMap(f *flightMap) ReaderOption {
	return func(r *UsenetReader) {
		r.flights = f
	}
}

// ConnBudget grants connection tokens for import segment fetches.
// Implemented by pool.Manager (AcquireImportConnection).
type ConnBudget interface {
	AcquireImportConnection(ctx context.Context) (release func(), err error)
}

// WithImportProfile marks the reader as import-owned: segment fetches use the
// pool's normal request lane (so they always yield to streaming reads, which
// use the priority lane) and each fetch is gated by the global import
// connection budget. Without this option the reader behaves as a streaming
// reader: priority lane, no budget. A nil budget only switches the lane.
func WithImportProfile(budget ConnBudget) ReaderOption {
	return func(r *UsenetReader) {
		r.priority = false
		r.budget = budget
	}
}

type DataCorruptionError struct {
	UnderlyingErr error
	BytesRead     int64
	NoRetry       bool
	// FileOffset is the absolute file-coordinate position where the failure
	// surfaced (-1 when unknown), enabling playback-impact classification.
	FileOffset int64
	// SegmentID is the message ID of the failing segment, when known.
	SegmentID string
	// MissVerdict says what a re-check of a missing article found.
	MissVerdict MissVerdict
}

func (e *DataCorruptionError) Error() string {
	return e.UnderlyingErr.Error()
}

func (e *DataCorruptionError) Unwrap() error {
	return e.UnderlyingErr
}

// MissVerdict records what re-checking a missing article found. A 430 is not
// proof the article is gone: backends desync and serve the same segment
// moments later (issue #749). The reader re-checks once and reports what it
// learned, so the health pipeline does not have to ask again.
type MissVerdict uint8

const (
	// MissUnverified: no re-check ran (not a miss, no budget, no pool, import).
	MissUnverified MissVerdict = iota
	// MissConfirmed: a second existence check also said it is gone.
	MissConfirmed
	// MissUnresolved: the re-check could not answer. Callers must keep their
	// pre-re-check behaviour — an unhealthy pool must not suppress repairs.
	MissUnresolved
	// MissUnfetchable: the article exists but a second fetch still failed, so
	// it counts as a real miss rather than something to re-check forever.
	MissUnfetchable
)

// missError annotates a miss with its segment and verdict without changing
// what the error is: errors.Is(err, ErrArticleNotFound) still holds, so the
// hole hooks and every existing miss branch behave as before.
type missError struct {
	segmentID string
	verdict   MissVerdict
	err       error
}

func (e *missError) Error() string { return e.err.Error() }
func (e *missError) Unwrap() error { return e.err }

// MissInfo reports the segment and verdict a miss was annotated with; ok is
// false for errors that never went through the reader's miss path.
func MissInfo(err error) (segmentID string, verdict MissVerdict, ok bool) {
	var me *missError
	if !errors.As(err, &me) {
		return "", MissUnverified, false
	}
	return me.segmentID, me.verdict, true
}

// isCorruptionError reports whether err indicates the article body itself is
// corrupt (as opposed to a transient network/pool failure), so it should be
// wrapped as a DataCorruptionError and routed into the health/repair pipeline
// instead of surfacing as an anonymous read error.
//
// nntppool.ErrCRCMismatch is checked by identity since it's an exported
// sentinel (errors.New("nntp: yEnc CRC mismatch")), returned unwrapped from
// finishBody. The substring fallback covers "data corruption detected",
// rapidyenc's own corruption sentinel text, in case a future nntppool version
// starts propagating it (today it does not: the decode error is discarded in
// nntppool's reader).
func isCorruptionError(err error) bool {
	if errors.Is(err, nntppool.ErrCRCMismatch) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "data corruption detected") ||
		strings.Contains(msg, "crc mismatch")
}

type UsenetReader struct {
	log            *slog.Logger
	wg             sync.WaitGroup
	ctx            context.Context // Reader's context for cancellation
	cancel         context.CancelFunc
	rg             *segmentRange
	maxPrefetch    int // Maximum segments prefetched ahead of current read position
	init           chan any
	initDownload   sync.Once
	closeOnce      sync.Once
	totalBytesRead int64
	poolGetter     func() (pool.NntpClient, error) // Dynamic pool getter
	metricsTracker MetricsTracker
	streamID       string
	segmentStore   SegmentStore // optional, nil = no caching
	holeHooks      *HoleHooks   // optional, nil = missing segments fail the read
	priority       bool         // true (streaming) = priority lane; false (import) = normal lane
	budget         ConnBudget   // optional; gates import fetches on the global connection budget
	specBudget     SpecBudget   // optional; bounds speculative fetches pool-wide (streaming only)
	articleSize    int64        // decoded size of the range's first article; drives the first-byte hold
	flights        *flightMap   // articles in flight, shared with every other streaming reader
	scheduledBytes int64        // usable bytes of every segment scheduled so far
	cond           *sync.Cond   // Signals downloadManager when reader advances

	// Prefetch-based download tracking
	nextToDownload int // Index of next segment to schedule

	// Tracing counters (atomic, no lock needed)
	inFlight atomic.Int32 // goroutines actively downloading right now

	// missRechecks counts re-checks attempted, refused ones included, so the
	// budget holds under concurrent prefetch without a lock.
	missRechecks atomic.Int32

	// hedger decides when a slow demand-position fetch gets a second request.
	hedger hedgePolicy

	// articleDate is when this file's articles were posted, forwarded to the
	// pool so per-provider retention limits apply. Zero = unknown.
	articleDate time.Time

	mu sync.Mutex
}

func NewUsenetReader(
	ctx context.Context,
	poolGetter func() (pool.NntpClient, error),
	rg *segmentRange,
	maxPrefetch int,
	metricsTracker MetricsTracker,
	streamID string,
	segmentStore SegmentStore,
	opts ...ReaderOption,
) (*UsenetReader, error) {
	log := slog.Default().With("component", "usenet-reader")
	ctx, cancel := context.WithCancel(ctx)

	if maxPrefetch <= 0 {
		maxPrefetch = defaultMaxPrefetch
	}

	ur := &UsenetReader{
		log:            log,
		ctx:            ctx,
		cancel:         cancel,
		rg:             rg,
		init:           make(chan any, 1),
		maxPrefetch:    maxPrefetch,
		poolGetter:     poolGetter,
		metricsTracker: metricsTracker,
		streamID:       streamID,
		segmentStore:   segmentStore,
		flights:        flights,
		priority:       true, // streaming profile by default; WithImportProfile demotes
	}
	for _, opt := range opts {
		opt(ur)
	}

	ur.cond = sync.NewCond(&ur.mu)

	ur.wg.Go(func() {
		ur.downloadManager(ctx)
	})

	return ur, nil
}

// Start triggers the background download process manually.
// This is useful for pre-fetching data before the first Read call.
func (b *UsenetReader) Start() {
	b.initDownload.Do(func() {
		select {
		case b.init <- struct{}{}:
		default:
		}
	})
}

// Interrupt cancels the reader's context and signals any blocked Read
// to return. Non-blocking and idempotent; safe to call concurrently
// with Read or Close. The caller is still responsible for invoking
// Close to release goroutines and resources. Used by callers (like
// MetadataVirtualFile.Close) that need to abort an in-flight download
// without taking the file's own lock.
func (b *UsenetReader) Interrupt() {
	b.cancel()
	b.cond.Broadcast()
	b.mu.Lock()
	rg := b.rg
	b.mu.Unlock()
	if rg != nil {
		rg.CloseSegments()
	}
}

func (b *UsenetReader) Close() error {
	b.closeOnce.Do(func() {
		b.cancel()

		// Unblock downloadManager if it's waiting on the cond
		b.cond.Broadcast()

		// Unblock any pending reads waiting for data
		if b.rg != nil {
			b.rg.CloseSegments()
		}

		// Wait for goroutines with timeout. The cancel() above ensures all
		// goroutines will eventually terminate, so the waiter goroutine is
		// not a permanent leak — it cleans up once downloads finish.
		// A periodic Broadcast pokes goroutines that entered cond.Wait()
		// after the initial Broadcast above.
		done := make(chan struct{})
		go func() {
			b.wg.Wait()
			close(done)
		}()

		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

	loop:
		for {
			select {
			case <-done:
				break loop
			case <-deadline.C:
				b.log.WarnContext(b.ctx, "Timeout waiting for downloads to complete during close")
				break loop
			case <-ticker.C:
				b.cond.Broadcast()
			}
		}

		b.mu.Lock()
		if b.rg != nil {
			_ = b.rg.Clear()
			b.rg = nil
		}
		b.mu.Unlock()

		// Final wake for any goroutines that entered cond.Wait() after the loop
		b.cond.Broadcast()
	})

	return nil
}

// Read reads len(p) byte from the Buffer starting at the current offset.
// It returns the number of bytes read and an error if any.
// Returns io.EOF error if pointer is at the end of the Buffer.
func (b *UsenetReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	b.initDownload.Do(func() {
		select {
		case b.init <- struct{}{}:
		default:
		}
	})

	b.mu.Lock()
	rg := b.rg
	b.mu.Unlock()

	if rg == nil {
		return 0, io.ErrClosedPipe
	}

	s, err := rg.Get()
	if err != nil {
		b.mu.Lock()
		totalRead := b.totalBytesRead
		b.mu.Unlock()

		if b.isArticleNotFoundError(err) {
			if totalRead > 0 {
				return 0, corruptionFromMiss(err, totalRead, rg.start+totalRead)
			} else {
				return 0, corruptionFromMiss(err, 0, rg.start)
			}
		}
		return 0, io.EOF
	}

	n := 0
	for n < len(p) {
		nn, err := s.GetReaderContext(b.ctx).Read(p[n:])
		n += nn

		b.mu.Lock()
		before := b.totalBytesRead
		b.totalBytesRead += int64(nn)
		totalRead := b.totalBytesRead
		// The read-ahead window widens once the first byte has been read; wake
		// the manager so it is not left waiting for the next segment rotation.
		widened := nn > 0 && before == 0
		b.mu.Unlock()
		if widened {
			b.cond.Signal()
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				// Segment fully read — move to next segment
				b.mu.Lock()
				rg := b.rg
				b.mu.Unlock()

				if rg == nil {
					return n, io.ErrClosedPipe
				}

				s, err = rg.Next()
				if err == nil {
					// Wake download manager — room for more prefetch
					b.cond.Signal()
				}

				if err != nil {
					if n > 0 {
						return n, nil
					}

					if b.isArticleNotFoundError(err) {
						if totalRead > 0 {
							return n, corruptionFromMiss(err, totalRead, rg.start+totalRead)
						}
					}
					return n, io.EOF
				}
			} else {
				if b.isArticleNotFoundError(err) {
					return n, corruptionFromMiss(err, totalRead, rg.start+totalRead)
				}
				return n, err
			}
		}
	}

	return n, nil
}

// corruptionFromMiss builds the DataCorruptionError a missing article surfaces
// as. Without the segment and verdict the download path recorded, the health
// pipeline has no message-ID to re-check and cannot tell a transient 430 from
// a real one (issue #749).
func corruptionFromMiss(err error, bytesRead, fileOffset int64) *DataCorruptionError {
	segmentID, verdict, _ := MissInfo(err)
	return &DataCorruptionError{
		UnderlyingErr: err,
		BytesRead:     bytesRead,
		FileOffset:    fileOffset,
		SegmentID:     segmentID,
		MissVerdict:   verdict,
	}
}

// isArticleNotFoundError checks if the error indicates articles were not found in providers
func (b *UsenetReader) isArticleNotFoundError(err error) bool {
	return errors.Is(err, nntppool.ErrArticleNotFound)
}

// IsArticleNotFound reports whether err stems from an article missing on all
// providers (permanent, never retried) — the only failure the hole model
// treats as a hole.
func IsArticleNotFound(err error) bool {
	return errors.Is(err, nntppool.ErrArticleNotFound)
}

// windowFor is how many segments may be scheduled ahead of the read position
// given what the caller has read since the reader was created.
func (b *UsenetReader) windowFor(bytesRead int64) int {
	full := b.maxPrefetch
	if b.priority && b.articleSize > 0 {
		full = max(min(full, int(readAheadBytesCap/b.articleSize)), largeArticleHoldSegments)
	}
	if !b.priority || bytesRead > 0 {
		return full
	}
	if b.articleSize >= largeArticleBytes {
		return min(full, largeArticleHoldSegments)
	}
	return min(full, openingSegments)
}

// BufferedAhead reports bytes scheduled for fetch beyond what the caller has
// read: the distance a forward skip can cover without a new reader.
func (b *UsenetReader) BufferedAhead() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return max(b.scheduledBytes-b.totalBytesRead, 0)
}

// GetBufferedOffset reports the file offset up to which this reader has
// scheduled fetches: the range start plus every scheduled segment's usable
// bytes. It reads counters only, so it never re-materialises a segment slot
// the reader has already consumed and released.
func (b *UsenetReader) GetBufferedOffset() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rg == nil {
		return 0
	}
	return b.rg.start + b.scheduledBytes
}

// keepFetchCompletionTimeout bounds how long a fetch may run on after its
// reader closed. One attempt window is enough: the article was already
// arriving when the reader left.
const keepFetchCompletionTimeout = 15 * time.Second

// fetchContext returns the context a streaming fetch runs under. Without
// keepOnClose it is ctx itself. With it, the fetch survives ctx being
// cancelled once the article has started arriving: the pool would otherwise
// abort the body (and, past its drain limit, close the connection), and the
// next open of the same head would fetch the whole article again. An
// article with no bytes yet is still cancelled with ctx.
func (b *UsenetReader) fetchContext(ctx context.Context, art *articleBuf, keepOnClose bool) (context.Context, context.CancelFunc) {
	if !keepOnClose || art == nil {
		return ctx, func() {}
	}
	fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keepFetchCompletionTimeout)
	go func() {
		select {
		case <-fetchCtx.Done():
		case <-ctx.Done():
			if art.published() == 0 {
				cancel()
			}
		}
	}()
	return fetchCtx, cancel
}

// downloadSegmentWithRetry attempts to download a segment with retry logic for
// pool unavailability. keepOnClose lets a started streaming fetch finish after
// ctx is cancelled; see fetchContext. segIdx is the segment's range-local
// index, which decides whether a slow fetch is at a demand position.
func (b *UsenetReader) downloadSegmentWithRetry(ctx context.Context, seg *segment, segIdx int, keepOnClose bool) ([]byte, error) {
	// Cache HIT: skip NNTP entirely
	if b.segmentStore != nil {
		if data, ok := b.segmentStore.Get(seg.Id); ok {
			b.log.DebugContext(ctx, "segment cache hit",
				"segment_id", seg.Id,
				"size_bytes", len(data),
			)
			// The fetch path publishes as it streams; a hit has nothing to
			// stream, so hand the bytes to the segment here.
			seg.SetData(data)
			return data, nil
		}
	}

	// Fix B: hoist pool getter outside retry loop — pool errors are not retriable
	// per-download-attempt; if the pool is unavailable we fail fast.
	poolGetStart := time.Now()
	cp, poolErr := b.poolGetter()
	poolGetDur := time.Since(poolGetStart)
	if poolErr != nil {
		b.log.DebugContext(ctx, "pool get failed",
			"segment_id", seg.Id,
			"pool_get_dur", poolGetDur,
			"error", poolErr,
		)
		return nil, poolErr
	}
	if poolGetDur > 100*time.Millisecond {
		b.log.DebugContext(ctx, "slow pool get",
			"segment_id", seg.Id,
			"pool_get_dur", poolGetDur,
		)
	}

	// Import readers take a token from the global import connection budget for
	// the whole fetch (held across retries — it represents one connection's
	// worth of work). Acquired before any per-attempt timeout is created so
	// queue wait never burns the fetch deadline. Streaming readers skip this.
	if b.budget != nil {
		release, err := b.budget.AcquireImportConnection(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
	}

	if !b.priority {
		data, err := b.fetchWithRetry(ctx, cp, seg, segIdx, nil)
		if b.segmentStore != nil && data != nil && err == nil {
			_ = b.segmentStore.Put(seg.Id, data)
		}
		return data, err
	}

	// Streaming: every reader wanting this article shares one buffer. The
	// first to arrive leads and fetches; the rest follow and read the same
	// bytes as they land. A leader whose reader closes mid-article hands the
	// lead to a follower, which continues into a fresh attempt past the
	// published watermark.
	art := seg.attachShared(b.flights)
	for {
		if art.claimLead() {
			lead := art.leadGen()
			fetchCtx, cancelFetch := b.fetchContext(ctx, art, keepOnClose)
			// A fetch that may outlive its reader keeps the article in the
			// flight map for as long as it may still finish, so a reader
			// arriving after the close joins it as a follower instead of
			// fetching the article again. The pin is tied to fetchCtx, not to
			// the fetch returning: a wire call that never comes back must not
			// hold the lead forever, or every later reader of the article
			// would wait on it for its whole context.
			if keepOnClose {
				if kept := b.flights.acquire(seg.Id, seg.SegmentSize); kept == art {
					context.AfterFunc(fetchCtx, func() {
						if ctx.Err() != nil {
							art.releaseLead(lead)
						}
						b.flights.release(seg.Id, art)
					})
				} else {
					b.flights.release(seg.Id, kept)
				}
			}
			data, err := b.fetchWithRetry(fetchCtx, cp, seg, segIdx, art)
			cancelFetch()
			switch {
			case err == nil:
				if b.segmentStore != nil && data != nil {
					_ = b.segmentStore.Put(seg.Id, data)
				}
				return data, nil
			case ctx.Err() != nil && !errors.Is(err, nntppool.ErrArticleNotFound):
				// This reader is going away; let a follower take over.
				art.releaseLead(lead)
				return nil, err
			default:
				// A definite answer (gone everywhere, or every attempt failed):
				// the same providers would tell every follower the same thing.
				art.setError(err)
				return nil, err
			}
		}
		err := art.waitDone(ctx)
		if errors.Is(err, errNoLeader) {
			continue
		}
		if err == nil {
			// Bytes are already visible through the shared buffer.
			return nil, nil
		}
		if errors.Is(err, nntppool.ErrArticleNotFound) {
			b.log.DebugContext(ctx, "missing segment", "segment_id", seg.Id)
		}
		return nil, err
	}
}

// MissRecheckTimeout bounds the existence check that confirms a miss. A
// present article STATs in tens of milliseconds; two seconds covers one
// adaptive provider window without letting a silent provider hold a slot.
const MissRecheckTimeout = 2 * time.Second

// maxMissRechecks budgets re-checks per reader: one desynced segment needs
// one, and a dead release must not pay ~1.2s per hole.
const maxMissRechecks = 1

// fetchWithRetry runs the wire fetch for one segment. With art set the
// decoded bytes stream into art as they arrive (priority lane); with art nil
// the article is buffered on the normal lane for import.
//
// A miss is re-checked once before it is allowed to stand: providers answer
// 430 transiently, and giving up on that answer is what condemned a healthy
// file in issue #749.
func (b *UsenetReader) fetchWithRetry(ctx context.Context, cp pool.NntpClient, seg *segment, segIdx int, art *articleBuf) ([]byte, error) {
	data, err := b.fetchAttempts(ctx, cp, seg, segIdx, art)
	if !errors.Is(err, nntppool.ErrArticleNotFound) {
		return data, err
	}

	present, verdict := b.recheckMiss(ctx, cp, seg)
	if !present {
		return data, &missError{segmentID: seg.Id, verdict: verdict, err: err}
	}

	b.log.InfoContext(ctx, "article present on re-check, retrying transient miss",
		"segment_id", seg.Id)
	retryData, retryErr := b.fetchAttempts(ctx, cp, seg, segIdx, art)
	if retryErr == nil {
		return retryData, nil
	}

	// Present but unfetchable: a real miss, or the file is re-checked forever
	// and never repaired.
	b.log.WarnContext(ctx, "article exists but a second fetch failed, treating the miss as real",
		"segment_id", seg.Id, "error", retryErr)
	return retryData, &missError{segmentID: seg.Id, verdict: MissUnfetchable, err: retryErr}
}

// recheckMiss asks the pool once whether a segment the fetch reported missing
// is actually there. The verdict is meaningful only when present is false.
//
// Streaming only: the import path sweeps availability up front and fast-fails.
func (b *UsenetReader) recheckMiss(ctx context.Context, cp pool.NntpClient, seg *segment) (present bool, verdict MissVerdict) {
	if !b.priority || cp == nil || seg.Id == "" || holes.IsPlaceholderID(seg.Id) {
		return false, MissUnverified
	}
	if b.missRechecks.Add(1) > maxMissRechecks {
		return false, MissUnverified
	}

	statCtx, cancel := context.WithTimeout(ctx, MissRecheckTimeout)
	defer cancel()

	switch _, err := cp.Exists(statCtx, nntppool.Req{
		MessageID:   seg.Id,
		Lane:        nntppool.LanePriority,
		ArticleDate: b.articleDate,
	}); {
	case err == nil:
		return true, MissUnverified
	case errors.Is(err, nntppool.ErrArticleNotFound):
		return false, MissConfirmed
	default:
		return false, MissUnresolved
	}
}

// fetchAttempts runs the retry loop for one wire fetch of a segment.
func (b *UsenetReader) fetchAttempts(ctx context.Context, cp pool.NntpClient, seg *segment, segIdx int, art *articleBuf) ([]byte, error) {
	segStart := time.Now()
	var resultBytes []byte
	err := retry.Do(
		func() error {
			// Fix C: reduce per-attempt timeout 30s → 15s to free stuck connections faster
			attemptCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			fetchStart := time.Now()
			var result *nntppool.ArticleBody
			var err error
			var w *articleWriter
			if art != nil {
				// Streaming: priority lane, decoded bytes published to readers as
				// each wire read lands. A failed attempt leaves its bytes visible;
				// the next attempt starts a fresh buffer and only publishes once
				// it has passed what readers already saw. A slow demand-position
				// fetch is hedged with a second request; see streamArticle.
				w, result, err = b.streamArticle(attemptCtx, cp, seg, segIdx, art)
			} else {
				// Import: normal lane, buffered — always yields to streaming reads.
				result, err = cp.Fetch(attemptCtx, nntppool.Req{
					MessageID:   seg.Id,
					ArticleDate: b.articleDate,
				})
			}
			fetchDur := time.Since(fetchStart)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					b.log.DebugContext(ctx, "segment download timed out after 15s",
						"segment_id", seg.Id,
						"fetch_dur", fetchDur,
					)
				}

				var bytesWritten int64
				switch {
				case w != nil:
					bytesWritten = int64(len(w.bytes()))
				case result != nil:
					bytesWritten = int64(result.BytesDecoded)
				}

				if isCorruptionError(err) {
					return &DataCorruptionError{
						UnderlyingErr: err,
						BytesRead:     bytesWritten,
						FileOffset:    -1,
						SegmentID:     seg.Id,
					}
				}

				return err
			}

			if w != nil {
				art.finish(w)
				resultBytes = w.bytes()
			} else {
				resultBytes = result.Bytes
			}
			b.metricsTracker.IncArticlesDownloaded()
			b.metricsTracker.UpdateDownloadProgress(b.streamID, int64(len(resultBytes)))

			return nil
		},
		// Retry strategy (post-S1/S3 fix):
		// - ErrArticleNotFound: never retry (article is permanently gone).
		// - DeadlineExceeded: retry immediately, no backoff — a fresh
		//   nntppool connection is available via round-robin.
		// - Other errors: at most one retry (Attempts=2 total wire calls
		//   per failure), with exponential backoff + jitter to break
		//   thundering-herd synchronization across readers. Base=50ms,
		//   max jitter=100ms → first retry delay drawn from [50, 150]ms.
		retry.Attempts(2),
		retry.Delay(50*time.Millisecond),
		retry.MaxJitter(100*time.Millisecond),
		retry.DelayType(func(n uint, err error, config *retry.Config) time.Duration {
			if errors.Is(err, context.DeadlineExceeded) {
				return 0
			}
			return retry.CombineDelay(retry.BackOffDelay, retry.RandomDelay)(n, err, config)
		}),
		retry.RetryIf(func(err error) bool {
			if errors.Is(err, nntppool.ErrArticleNotFound) {
				return false // permanent failure — do not retry
			}
			return true
		}),
		retry.OnRetry(func(n uint, err error) {
			if !errors.Is(err, context.Canceled) && ctx.Err() == nil {
				b.log.DebugContext(ctx, "segment download retry",
					"attempt", n+1,
					"segment_id", seg.Id,
					"error", err,
					"elapsed", time.Since(segStart),
				)
			}
		}),
		retry.Context(ctx),
	)

	if errors.Is(err, nntppool.ErrArticleNotFound) {
		b.log.DebugContext(ctx, "missing segment",
			"segment_id", seg.Id,
		)
	}

	return resultBytes, err
}

// patchFor returns the repaired payload for a segment when the owner's
// PatchLookup has one of exactly the right size, else nil. Runs on download
// goroutines; PatchLookup must be fast local I/O.
func (b *UsenetReader) patchFor(ctx context.Context, s *segment) []byte {
	lookup := b.patchLookupFor()
	if lookup == nil {
		return nil
	}
	p := lookup(s.Id)
	if p == nil {
		return nil
	}
	if int64(len(p)) != s.End+1 {
		b.log.WarnContext(ctx, "Repaired payload size mismatch, ignoring patch",
			"segment_id", s.Id, "patch_bytes", len(p), "want", s.End+1)
		return nil
	}
	return p
}

func (b *UsenetReader) downloadManager(ctx context.Context) {
	select {
	case _, ok := <-b.init:
		if !ok {
			return
		}
	case <-ctx.Done():
		return
	}

	if b.rg.Len() == 0 {
		return
	}

	totalSegments := b.rg.Len()
	if first, err := b.rg.GetSegment(0); err == nil && first != nil {
		b.mu.Lock()
		b.articleSize = first.SegmentSize
		b.mu.Unlock()
	}

	for ctx.Err() == nil {
		b.mu.Lock()
		if b.rg == nil {
			b.mu.Unlock()
			return
		}

		// Check if all segments have been scheduled
		if b.nextToDownload >= totalSegments {
			b.mu.Unlock()
			break
		}

		// Limit how far ahead we prefetch beyond the current read position
		currentRead := b.rg.GetCurrentIndex()
		ahead := b.nextToDownload - currentRead
		if ahead >= b.windowFor(b.totalBytesRead) {
			b.cond.Wait()
			b.mu.Unlock()
			if ctx.Err() != nil {
				return
			}
			continue
		}

		// Segments beyond the demand depth are speculative: they run only
		// when the pool-wide budget has a free slot right now. A refused slot
		// parks the manager until the reader advances or a fetch completes,
		// both of which signal cond, and the segment is reconsidered then —
		// by which time it may have moved into the demand window.
		releaseSlot := func() {}
		if ahead >= demandDepth && b.priority && b.specBudget != nil {
			release, ok := b.specBudget.TryAcquire()
			if !ok {
				b.cond.Wait()
				b.mu.Unlock()
				if ctx.Err() != nil {
					return
				}
				continue
			}
			releaseSlot = release
		}

		// Schedule next segment for download
		idx := b.nextToDownload
		b.nextToDownload++
		// Demand-window articles are what a reopen of this position needs
		// next, so they are worth finishing after a close; speculative
		// read-ahead is not.
		keepOnClose := b.priority && ahead < demandDepth
		b.mu.Unlock()

		seg, err := b.rg.GetSegment(idx)
		if err != nil || seg == nil {
			releaseSlot()
			continue
		}
		b.mu.Lock()
		b.scheduledBytes += seg.End - seg.Start + 1
		b.mu.Unlock()

		b.inFlight.Add(1)
		go func(segIdx int, s *segment) {
			defer b.inFlight.Add(-1)
			defer b.cond.Signal()
			defer releaseSlot()
			defer func() {
				if p := recover(); p != nil {
					b.log.ErrorContext(ctx, "Panic in download task:", "panic", p)
					s.SetError(fmt.Errorf("panic in download task: %v", p))
				}
			}()

			taskCtx := slogutil.With(ctx, "segment_id", s.Id, "segment_idx", segIdx)

			// A gap placeholder names no article: the NZB never listed one.
			// Serve a repaired patch when there is one, else zeros, and never
			// ask a provider. Unlike a discovered hole this is not subject to
			// the owner's pad policy: the bytes were unavailable at import,
			// which is when their damage was judged.
			if holes.IsPlaceholderID(s.Id) {
				if p := b.patchFor(taskCtx, s); p != nil {
					s.SetData(p)
					return
				}
				s.SetData(make([]byte, s.End+1))
				return
			}

			// Replay pre-pad: a segment already known missing (persisted hole
			// map) serves its repaired patch when one exists, else zero-fills
			// immediately — either way with no fetch round-trip.
			if b.holeHooks != nil && b.holeHooks.KnownHoles != nil && b.holeHooks.KnownHoles(s.loaderIdx) {
				if p := b.patchFor(taskCtx, s); p != nil {
					b.log.DebugContext(taskCtx, "serving repaired payload for known-missing segment")
					s.SetData(p)
					return
				}
				b.log.DebugContext(taskCtx, "zero-filling known-missing segment without fetch")
				s.SetData(make([]byte, s.End+1))
				return
			}

			// A repaired patch takes precedence over a fetch: the wire copy
			// can be corrupt-but-present (that damage is why the patch was
			// built), while the patch is IFSC-verified byte-exact. This also
			// covers articles that have vanished since the repair, with no
			// fetch round-trip.
			if p := b.patchFor(taskCtx, s); p != nil {
				b.log.DebugContext(taskCtx, "serving repaired payload instead of fetching",
					"file_segment_index", s.loaderIdx)
				s.SetData(p)
				return
			}

			data, err := b.downloadSegmentWithRetry(taskCtx, s, segIdx, keepOnClose)

			if err != nil {
				if errors.Is(err, nntppool.ErrArticleNotFound) {
					// A confirmed-missing article may be zero-filled instead
					// of failing the stream, when the owner's hole hook
					// approves.
					if b.holeHooks != nil && b.holeHooks.OnHole != nil &&
						b.holeHooks.OnHole(s.loaderIdx, s.Id) == holes.DecisionPad {
						b.log.InfoContext(taskCtx, "zero-filling missing segment",
							"file_segment_index", s.loaderIdx)
						s.SetData(make([]byte, s.End+1))
						return
					}
				}
				s.SetError(err)
			} else if !b.priority {
				// Progressive readers were finished inside the fetch.
				s.SetData(data)
			}
		}(idx, seg)
	}

}
