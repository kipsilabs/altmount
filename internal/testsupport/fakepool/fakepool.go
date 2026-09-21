// Package fakepool provides a deterministic in-process replacement for
// nntppool.Client used by tests that exercise AltMount's streaming and
// connection-management invariants.
//
// The real nntppool.Client opens TCP sockets, dials providers, and runs
// background reconnect / quota / pipelining goroutines. None of that is
// useful when the question under test is "does the streaming pipeline cap
// in-flight downloads", "does a slow provider cause a retry storm", or
// "do ephemeral readers leak goroutines". For those questions we need a
// client we can drive moment-by-moment: control latency, inject specific
// errors, count concurrent calls, and observe whether the caller honors
// the backpressure we apply.
//
// Client satisfies pool.NntpClient (see internal/pool/nntpclient.go). The
// production code never sees a difference; tests inject *Client directly via
// a pool getter closure that returns it as pool.NntpClient.
//
// # Observability primitives
//
// Every Body / BodyPriority / BodyAsync / Stat call increments InFlight on
// entry and decrements on exit. MaxInFlight records the high-water mark.
// Tests assert against these counters to pin invariants like
// "no more than N segment downloads are ever in flight".
//
// # Failure injection
//
// SegmentBehavior controls per-message-ID latency, byte payload, and error.
// Set via SetBehavior or SetDefaultBehavior. Behaviors are evaluated at the
// start of each call; later changes apply only to subsequent calls.
//
// # Backpressure simulation
//
// BlockUntil pins every in-flight call at the entry semaphore until the
// caller closes the returned channel. Use it to hold connections "open"
// while observing how the rest of the pipeline reacts.
//
// All public methods are safe for concurrent use.
package fakepool

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/javi11/nntppool/v5"
)

// compile-time assertion: Client must satisfy the narrow interface.
var _ pool.NntpClient = (*Client)(nil)

// errTransientFakepool is the default error returned during a SegmentBehavior
// FailFirst window — a transient failure that is NOT nntppool.ErrArticleNotFound,
// so retry logic must treat it as retryable rather than a permanent miss.
var errTransientFakepool = errors.New("fakepool: transient failure (all providers exhausted)")

// SegmentBehavior describes how the fake should respond to a single
// message-ID. The zero value returns an empty body with no delay.
type SegmentBehavior struct {
	// Latency is the wall-clock delay added before the call returns.
	// Honors ctx cancellation: if the context fires first, the call
	// returns ctx.Err() and does not pretend to have produced data.
	Latency time.Duration

	// Bytes is the payload returned in ArticleBody.Bytes. Length is also
	// used for BytesDecoded.
	Bytes []byte

	// Err, if non-nil, is returned instead of a body. Use nntppool sentinel
	// errors (e.g. nntppool.ErrArticleNotFound) to exercise specific paths
	// in the retry/dispatch logic.
	Err error

	// YEnc, if non-zero, is populated on ArticleBody.YEnc and fired via any
	// onMeta callbacks. Use this to inject yEnc header metadata (FileName,
	// PartSize, FileSize, etc.) without needing a real NNTP server.
	YEnc nntppool.YEncMeta

	// FailFirst makes the first N calls to this message-ID return FailErr (a
	// transient error) before subsequent calls succeed normally. Use it to
	// exercise retry logic that must distinguish transient failures from a
	// permanent article-not-found. Zero disables this behavior.
	FailFirst int

	// FailErr is the transient error returned during the FailFirst window.
	// Defaults to a generic "all providers exhausted"-style error when nil.
	FailErr error

	// ChunkSize splits a streamed payload (writer-based calls) into writes of
	// this many bytes; zero writes the whole payload at once.
	ChunkSize int

	// TailGate, when non-nil, is waited on after the first chunk is written
	// and before the rest, so a test can prove bytes were served while the
	// article was provably still arriving. Close it to release the tail.
	TailGate <-chan struct{}

	// FailAfterFirstChunk makes the first call to this message-ID write one
	// chunk and then return FailErr, modelling a connection lost mid-article.
	// Later calls serve the payload normally.
	FailAfterFirstChunk bool
}

// Client is a fake nntppool.Client suitable for unit and concurrency tests.
type Client struct {
	mu              sync.RWMutex
	defaultBehavior SegmentBehavior
	perSegment      map[string]SegmentBehavior
	releaseGate     <-chan struct{} // nil = no gate; closed = always permit
	stats           nntppool.ClientStats
	hasFixedStats   bool

	// Atomic counters for observability.
	inFlight           atomic.Int32
	maxInFlight        atomic.Int32
	totalCalls         atomic.Int64
	bodyCalls          atomic.Int64
	bodyPriCalls       atomic.Int64
	bodyBgCalls        atomic.Int64
	bodyStreamPriCalls atomic.Int64
	bodyAsyncCalls     atomic.Int64
	statCalls          atomic.Int64
	statPriCalls       atomic.Int64

	// Per-message-ID call counts (string → *atomic.Int64). Tests that need
	// to assert how often a specific segment was requested (e.g. to detect
	// retry storms) read this map via PerMessageCalls.
	perIDCalls sync.Map

	// Per-message-ID body call counts, excluding existence checks, so a STAT
	// alongside a fetch does not read as a retry.
	perIDBodyCalls sync.Map

	// Per-message-ID article dates (string → time.Time) as passed in
	// nntppool.Req.ArticleDate. Tests asserting that a call site forwards the
	// post date it holds — the whole point of the retention feature — read
	// this via ArticleDateFor.
	perIDArticleDate sync.Map
}

// New returns a fake client. Without further configuration it returns an
// empty (zero-byte) ArticleBody immediately for every message-ID.
func New() *Client {
	return &Client{
		perSegment: make(map[string]SegmentBehavior),
	}
}

// SetDefaultBehavior sets the behavior used for any message-ID that has no
// per-segment override.
func (c *Client) SetDefaultBehavior(b SegmentBehavior) {
	c.mu.Lock()
	c.defaultBehavior = b
	c.mu.Unlock()
}

// SetBehavior sets a per-message-ID behavior. Overrides SetDefaultBehavior.
func (c *Client) SetBehavior(messageID string, b SegmentBehavior) {
	c.mu.Lock()
	c.perSegment[messageID] = b
	c.mu.Unlock()
}

// BlockUntil installs a gate that pins every subsequent call inside the
// fake (after counter increment, before doing any work) until release is
// closed. Useful for asserting "exactly N calls are concurrently in flight
// while the gate is closed".
//
// Pass nil to remove the gate. Setting a new gate replaces any prior one;
// previously gated calls observing the old gate continue to wait on it.
func (c *Client) BlockUntil(release <-chan struct{}) {
	c.mu.Lock()
	c.releaseGate = release
	c.mu.Unlock()
}

// SetStats overrides what Stats() returns. Tests that exercise the metrics
// tracker can use this to feed deterministic provider data.
func (c *Client) SetStats(s nntppool.ClientStats) {
	c.mu.Lock()
	c.stats = s
	c.hasFixedStats = true
	c.mu.Unlock()
}

// InFlight returns the number of calls currently inside the fake.
func (c *Client) InFlight() int32 { return c.inFlight.Load() }

// MaxInFlight returns the high-water mark of InFlight observed since the
// client was created (or since the last ResetCounters).
func (c *Client) MaxInFlight() int32 { return c.maxInFlight.Load() }

// TotalCalls returns the total number of method invocations served.
func (c *Client) TotalCalls() int64 { return c.totalCalls.Load() }

// BodyCalls returns the count of Body invocations.
func (c *Client) BodyCalls() int64 { return c.bodyCalls.Load() }

// BodyPriorityCalls returns the count of priority-lane body invocations,
// buffered (BodyPriority) and streamed (BodyStreamPriority) alike. Tests
// asserting "streaming uses the priority lane" read this.
func (c *Client) BodyPriorityCalls() int64 {
	return c.bodyPriCalls.Load() + c.bodyStreamPriCalls.Load()
}

// BodyBackgroundCalls reports how many BodyBackground (repair) fetches were made.
func (c *Client) BodyBackgroundCalls() int64 { return c.bodyBgCalls.Load() }

// BodyStreamPriorityCalls returns the count of BodyStreamPriority invocations.
func (c *Client) BodyStreamPriorityCalls() int64 { return c.bodyStreamPriCalls.Load() }

// BodyAsyncCalls returns the count of BodyAsync invocations.
func (c *Client) BodyAsyncCalls() int64 { return c.bodyAsyncCalls.Load() }

// StatCalls returns the count of Stat invocations.
func (c *Client) StatCalls() int64 { return c.statCalls.Load() }

// StatPriorityCalls reports existence checks made on the priority lane.
// StatCalls counts those too, so "no STAT at all" assertions keep working.
func (c *Client) StatPriorityCalls() int64 { return c.statPriCalls.Load() }

// recordArticleDate remembers the most recent ArticleDate seen for a
// message-ID. A zero date is recorded as such: "this call site forwarded no
// date" is exactly what a test may need to catch.
func (c *Client) recordArticleDate(messageID string, at time.Time) {
	c.perIDArticleDate.Store(messageID, at)
}

// ArticleDateFor returns the ArticleDate last passed for the given
// message-ID, and whether it was requested at all.
func (c *Client) ArticleDateFor(messageID string) (time.Time, bool) {
	v, ok := c.perIDArticleDate.Load(messageID)
	if !ok {
		return time.Time{}, false
	}
	return v.(time.Time), true
}

// PerMessageCalls returns how many times the given message-ID was requested
// across all method types (Body / BodyPriority / BodyAsync / Stat).
//
// This is the primary signal for retry-storm tests: if a retry policy is
// re-issuing failed requests, the per-ID count climbs faster than the
// number of distinct segments and the assertion fails.
func (c *Client) PerMessageCalls(messageID string) int64 {
	return loadCount(&c.perIDCalls, messageID)
}

// PerMessageBodyCalls returns how many times the message-ID was fetched,
// ignoring existence checks. Use it to pin retry behaviour when the code under
// test may also STAT the same article.
func (c *Client) PerMessageBodyCalls(messageID string) int64 {
	return loadCount(&c.perIDBodyCalls, messageID)
}

func loadCount(m *sync.Map, messageID string) int64 {
	v, ok := m.Load(messageID)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

// ResetCounters zeroes all observability counters. Behaviors and gates are
// preserved. Useful between phases of a single test.
func (c *Client) ResetCounters() {
	c.inFlight.Store(0)
	c.maxInFlight.Store(0)
	c.totalCalls.Store(0)
	c.bodyCalls.Store(0)
	c.bodyPriCalls.Store(0)
	c.bodyStreamPriCalls.Store(0)
	c.bodyAsyncCalls.Store(0)
	c.statCalls.Store(0)
	c.statPriCalls.Store(0)
	for _, m := range []*sync.Map{&c.perIDCalls, &c.perIDBodyCalls} {
		m.Range(func(k, _ any) bool {
			m.Delete(k)
			return true
		})
	}
}

// countMessage increments the per-ID counter atomically, lazily creating
// the counter on first contact.
func (c *Client) countMessage(messageID string) {
	bumpCount(&c.perIDCalls, messageID)
}

// countBodyMessage records a fetch in both the all-methods and body-only
// per-ID counters.
func (c *Client) countBodyMessage(messageID string) {
	bumpCount(&c.perIDCalls, messageID)
	bumpCount(&c.perIDBodyCalls, messageID)
}

// bumpCount increments a per-ID counter atomically, lazily creating the
// counter on first contact.
func bumpCount(m *sync.Map, messageID string) {
	if v, ok := m.Load(messageID); ok {
		v.(*atomic.Int64).Add(1)
		return
	}
	var fresh atomic.Int64
	fresh.Add(1)
	actual, loaded := m.LoadOrStore(messageID, &fresh)
	if loaded {
		actual.(*atomic.Int64).Add(1)
	}
}

// enter increments in-flight counters and waits at the gate (if any).
// Returns a function to call on exit.
func (c *Client) enter() func() {
	cur := c.inFlight.Add(1)
	c.totalCalls.Add(1)
	for {
		hwm := c.maxInFlight.Load()
		if cur <= hwm || c.maxInFlight.CompareAndSwap(hwm, cur) {
			break
		}
	}
	c.mu.RLock()
	gate := c.releaseGate
	c.mu.RUnlock()
	if gate != nil {
		<-gate
	}
	return func() { c.inFlight.Add(-1) }
}

// behaviorFor resolves the SegmentBehavior for a given message-ID, falling
// back to the default.
func (c *Client) behaviorFor(messageID string) SegmentBehavior {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if b, ok := c.perSegment[messageID]; ok {
		return b
	}
	return c.defaultBehavior
}

// waitOrCancel waits for d to elapse or ctx to fire, whichever first.
// Returns ctx.Err() on cancellation, nil otherwise.
func waitOrCancel(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Fetch satisfies pool.NntpClient. It returns either the configured error or
// an ArticleBody filled with the configured Bytes after the configured
// latency, and when r.Writer is set writes the payload to it in ChunkSize
// pieces (one write when zero), pausing on TailGate after the first chunk when
// set and failing after the first chunk on the first call when
// FailAfterFirstChunk is set.
//
// nntppool v5 folded lane and delivery mode into Req, so one method now covers
// what used to be Body, BodyPriority, BodyBackground, and
// BodyStreamPriority. The per-lane counters are kept and derived from the Req
// instead, so assertions that distinguish playback from import from repair
// traffic keep working.
func (c *Client) Fetch(ctx context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	switch {
	case r.Lane == nntppool.LaneBackground:
		c.bodyBgCalls.Add(1)
	case r.Lane == nntppool.LanePriority && r.Writer != nil:
		c.bodyStreamPriCalls.Add(1)
	case r.Lane == nntppool.LanePriority:
		c.bodyPriCalls.Add(1)
	default:
		c.bodyCalls.Add(1)
	}
	c.countBodyMessage(r.MessageID)
	c.recordArticleDate(r.MessageID, r.ArticleDate)
	defer c.enter()()
	return c.serveBodyReq(ctx, r)
}

// FetchAsync mirrors Fetch on its own goroutine, yielding one BodyResult.
func (c *Client) FetchAsync(ctx context.Context, r nntppool.Req) <-chan nntppool.BodyResult {
	c.bodyAsyncCalls.Add(1)
	c.countBodyMessage(r.MessageID)
	c.recordArticleDate(r.MessageID, r.ArticleDate)
	ch := make(chan nntppool.BodyResult, 1)
	go func() {
		defer c.enter()()
		body, err := c.serveBodyReq(ctx, r)
		ch <- nntppool.BodyResult{Body: body, Err: err}
		close(ch)
	}()
	return ch
}

func (c *Client) serveBodyReq(ctx context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	if r.OnMeta != nil {
		return c.serveBody(ctx, r.MessageID, r.Writer, r.OnMeta)
	}
	return c.serveBody(ctx, r.MessageID, r.Writer)
}

// Exists returns a StatResult with the message-ID echoed, after the configured
// latency. If the behavior has Err set, it is returned. A priority-lane check
// counts toward both StatCalls and StatPriorityCalls, so "no existence check
// happened" assertions hold for either lane.
func (c *Client) Exists(ctx context.Context, r nntppool.Req) (*nntppool.StatResult, error) {
	c.statCalls.Add(1)
	if r.Lane == nntppool.LanePriority {
		c.statPriCalls.Add(1)
	}
	c.recordArticleDate(r.MessageID, r.ArticleDate)
	return c.serveStat(ctx, r.MessageID)
}

// ExistsMany satisfies pool.NntpClient by fanning Exists out across up to
// opts.Concurrency goroutines, mirroring nntppool's semantics: results stream
// out of order, the channel closes once every dispatched check reports, and
// ctx cancellation stops dispatch and lets in-flight sends bail out.
func (c *Client) ExistsMany(ctx context.Context, messageIDs []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	conc := opts.Concurrency
	if conc <= 0 {
		conc = 64
	}
	if conc > len(messageIDs) && len(messageIDs) > 0 {
		conc = len(messageIDs)
	}

	out := make(chan nntppool.ExistsResult, max(conc, 1))
	go func() {
		defer close(out)
		sem := make(chan struct{}, conc)
		var wg sync.WaitGroup

	dispatch:
		for _, id := range messageIDs {
			select {
			case <-ctx.Done():
				break dispatch
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()
				res, err := c.Exists(ctx, nntppool.Req{
					MessageID:   id,
					Lane:        opts.Lane,
					ArticleDate: opts.ArticleDate,
				})
				select {
				case out <- nntppool.ExistsResult{MessageID: id, Result: res, Err: err}:
				case <-ctx.Done():
				}
			}(id)
		}
		wg.Wait()
	}()
	return out
}

func (c *Client) serveStat(ctx context.Context, messageID string) (*nntppool.StatResult, error) {
	c.countMessage(messageID)
	defer c.enter()()
	b := c.behaviorFor(messageID)
	if err := waitOrCancel(ctx, b.Latency); err != nil {
		return nil, err
	}
	if b.Err != nil {
		return nil, b.Err
	}
	return &nntppool.StatResult{MessageID: messageID}, nil
}

// Stats returns the configured stats or a zero value.
func (c *Client) Stats() nntppool.ClientStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.hasFixedStats {
		return c.stats
	}
	return nntppool.ClientStats{}
}

func (c *Client) serveBody(ctx context.Context, messageID string, w io.Writer, onMeta ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	b := c.behaviorFor(messageID)
	if err := waitOrCancel(ctx, b.Latency); err != nil {
		return nil, err
	}
	// Transient-failure window: fail the first FailFirst calls to this message,
	// then serve normally. The per-ID counter was already incremented in Body.
	if b.FailFirst > 0 {
		if n, ok := c.perIDCalls.Load(messageID); ok && n.(*atomic.Int64).Load() <= int64(b.FailFirst) {
			failErr := b.FailErr
			if failErr == nil {
				failErr = errTransientFakepool
			}
			return nil, failErr
		}
	}
	if b.Err != nil {
		// Mirror nntppool: on error the result may still carry partial
		// metadata. Tests that need that can extend this; the common case
		// returns nil.
		return nil, b.Err
	}
	// Fire yEnc metadata callbacks before writing body bytes, mirroring
	// how nntppool fires onMeta after parsing =ybegin/=ypart headers.
	if b.YEnc != (nntppool.YEncMeta{}) {
		for _, fn := range onMeta {
			if fn != nil {
				fn(b.YEnc)
			}
		}
	}
	payload := b.Bytes
	if w != nil && len(payload) > 0 {
		chunk := b.ChunkSize
		if chunk <= 0 || chunk > len(payload) {
			chunk = len(payload)
		}
		for off := 0; off < len(payload); off += chunk {
			end := min(off+chunk, len(payload))
			if _, err := w.Write(payload[off:end]); err != nil {
				return nil, err
			}
			if off != 0 {
				continue
			}
			if b.FailAfterFirstChunk {
				if n, ok := c.perIDCalls.Load(messageID); ok && n.(*atomic.Int64).Load() == 1 {
					failErr := b.FailErr
					if failErr == nil {
						failErr = errTransientFakepool
					}
					return nil, failErr
				}
			}
			if b.TailGate != nil {
				select {
				case <-b.TailGate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
	}
	body := &nntppool.ArticleBody{
		MessageID:    messageID,
		BytesDecoded: len(payload),
		YEnc:         b.YEnc,
	}
	if w == nil {
		body.Bytes = payload
	}
	return body, nil
}

// AssertMaxInFlightLE fails the test if the high-water mark exceeds n.
// Use this as the standard assertion at the end of any test that pins a
// concurrency cap.
func AssertMaxInFlightLE(tb interface {
	Helper()
	Errorf(format string, args ...any)
}, c *Client, n int32) {
	tb.Helper()
	if got := c.MaxInFlight(); got > n {
		tb.Errorf("fakepool: MaxInFlight=%d, want <= %d", got, n)
	}
}

// ErrSimulated502 is a generic transient error suitable for retry-storm
// scenarios. It is wrapped so errors.Is(err, ErrSimulated502) works.
var ErrSimulated502 = errors.New("fakepool: simulated 502 service unavailable")
