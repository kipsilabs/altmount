package par2repair

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
)

// fakeStatClient satisfies BodyClient plus the stat surface PoolFetcher
// probes for; liveness is answered from the articles set.
type fakeStatClient struct {
	articles map[string]bool
}

func (c *fakeStatClient) Fetch(_ context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	messageID := r.MessageID
	if !c.articles[messageID] {
		return nil, nntppool.ErrArticleNotFound
	}
	return &nntppool.ArticleBody{}, nil
}

func (c *fakeStatClient) ExistsMany(_ context.Context, messageIDs []string, _ nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	out := make(chan nntppool.ExistsResult, len(messageIDs))
	go func() {
		defer close(out)
		for _, id := range messageIDs {
			if c.articles[id] {
				out <- nntppool.ExistsResult{MessageID: id, Result: &nntppool.StatResult{MessageID: id}}
			} else {
				out <- nntppool.ExistsResult{MessageID: id, Err: nntppool.ErrArticleNotFound}
			}
		}
	}()
	return out
}

func TestPoolFetcherStatIDs(t *testing.T) {
	client := &fakeStatClient{articles: map[string]bool{"live@x": true}}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)

	missing, err := f.StatIDs(context.Background(), []string{"live@x", "gone@x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if missing["live@x"] || !missing["gone@x"] {
		t.Fatalf("missing = %v", missing)
	}
}

// flakyStatClient answers StatMany with a transient error for the first
// failures[id] calls per id, then with the article's real verdict.
type flakyStatClient struct {
	articles map[string]bool
	mu       sync.Mutex
	failures map[string]int
	calls    map[string]int
}

func (c *flakyStatClient) Fetch(_ context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	messageID := r.MessageID
	if !c.articles[messageID] {
		return nil, nntppool.ErrArticleNotFound
	}
	return &nntppool.ArticleBody{}, nil
}

func (c *flakyStatClient) ExistsMany(_ context.Context, messageIDs []string, _ nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	out := make(chan nntppool.ExistsResult, len(messageIDs))
	go func() {
		defer close(out)
		for _, id := range messageIDs {
			c.mu.Lock()
			if c.calls == nil {
				c.calls = map[string]int{}
			}
			c.calls[id]++
			transient := c.failures[id] > 0
			if transient {
				c.failures[id]--
			}
			c.mu.Unlock()
			switch {
			case transient:
				out <- nntppool.ExistsResult{MessageID: id, Err: errors.New("nntp: all providers exhausted: connection died")}
			case c.articles[id]:
				out <- nntppool.ExistsResult{MessageID: id, Result: &nntppool.StatResult{MessageID: id}}
			default:
				out <- nntppool.ExistsResult{MessageID: id, Err: nntppool.ErrArticleNotFound}
			}
		}
	}()
	return out
}

// A STAT that fails with a transport error proves nothing about the article.
// Counting it alive poisons planning (a flapping provider makes a mostly-dead
// release look healthy), so unresolved ids must be re-STATed until they yield
// a real verdict.
func TestPoolFetcherStatIDsRetriesUnresolved(t *testing.T) {
	client := &flakyStatClient{
		articles: map[string]bool{"live@x": true},
		failures: map[string]int{"gone@x": 2, "live@x": 1},
	}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)
	f.retryDelay = time.Millisecond

	missing, err := f.StatIDs(context.Background(), []string{"live@x", "gone@x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if missing["live@x"] || !missing["gone@x"] {
		t.Fatalf("missing = %v, want only gone@x after retrying transient errors", missing)
	}
	if got := client.calls["gone@x"]; got != 3 {
		t.Fatalf("StatMany calls for gone@x = %d, want 3", got)
	}
}

// A handful of still-unresolved articles after retries is absorbable — the
// payload sweep verifies every slice anyway — so the sweep result stands,
// with the unresolved ids simply not reported missing.
func TestPoolFetcherStatIDsToleratesFewUnresolved(t *testing.T) {
	client := &flakyStatClient{
		articles: map[string]bool{"live@x": true},
		failures: map[string]int{"stuck@x": 99},
	}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)
	f.retryDelay = time.Millisecond

	missing, err := f.StatIDs(context.Background(), []string{"live@x", "stuck@x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing = %v, want empty (unresolved is not a verdict)", missing)
	}
}

// When transient failures dominate the sweep even after retries, the liveness
// picture is fiction: reporting it would let planning proceed on false "alive"
// verdicts (observed live: a flapping provider made a ~90%-dead release sweep
// clean, costing 10 minutes of serial dead-article discovery). The sweep must
// fail so the attempt is retried later instead.
func TestPoolFetcherStatIDsFailsWhenUnresolvedDominates(t *testing.T) {
	n := maxHiddenAbsorbArticles + 1
	ids := make([]string, n)
	failures := map[string]int{}
	for i := range ids {
		ids[i] = fmt.Sprintf("stuck%d@x", i)
		failures[ids[i]] = 99
	}
	client := &flakyStatClient{failures: failures}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)
	f.retryDelay = time.Millisecond

	_, err := f.StatIDs(context.Background(), ids, nil)
	if err == nil {
		t.Fatal("StatIDs must fail when most of the sweep stays unresolved")
	}
	if errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, must be transient, not unrepairable", err)
	}
}

// concurrencyRecordingStatClient records the StatManyOptions each pass used.
type concurrencyRecordingStatClient struct {
	fakeStatClient
	mu   sync.Mutex
	opts []nntppool.ManyOptions
}

func (c *concurrencyRecordingStatClient) ExistsMany(ctx context.Context, messageIDs []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	c.mu.Lock()
	c.opts = append(c.opts, opts)
	c.mu.Unlock()
	return c.fakeStatClient.ExistsMany(ctx, messageIDs, opts)
}

// An unbounded sweep lets the pool derive concurrency from its aggregate STAT
// pipeline capacity — thousands of simultaneous STATs that saturate every
// provider's dispatch window and fail the sweep itself with "all providers
// exhausted" (observed live). The sweep must cap its own burst.
func TestPoolFetcherStatIDsBoundsConcurrency(t *testing.T) {
	client := &concurrencyRecordingStatClient{fakeStatClient: fakeStatClient{articles: map[string]bool{"live@x": true}}}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)

	if _, err := f.StatIDs(context.Background(), []string{"live@x", "gone@x"}, nil); err != nil {
		t.Fatal(err)
	}
	if len(client.opts) == 0 {
		t.Fatal("StatMany never called")
	}
	for _, o := range client.opts {
		if o.Concurrency <= 0 || o.Concurrency > statSweepConcurrency {
			t.Fatalf("Concurrency = %d, want in (0, %d]", o.Concurrency, statSweepConcurrency)
		}
	}
}

// A repair configured for N connections must not occupy more than N
// connections' worth of shallow STAT pipeline either: the sweep width is the
// pool budget capped by the repair's own connection cap × sweepStatDepth.
func TestSweepStatConcurrencyRespectsRepairConnections(t *testing.T) {
	cases := []struct {
		name                string
		poolWidth, maxConns int
		want                int
	}{
		{"repair cap binds", 4096, 10, 10 * sweepStatDepth},
		{"pool budget binds", 30, 10, 30},
		{"non-positive conns falls back to pool budget", 200, 0, 200},
		{"non-positive pool width keeps conns bound", 0, 10, 10 * sweepStatDepth},
		{"floor of one", 0, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SweepStatConcurrency(tc.poolWidth, tc.maxConns); got != tc.want {
				t.Fatalf("SweepStatConcurrency(%d, %d) = %d, want %d", tc.poolWidth, tc.maxConns, got, tc.want)
			}
		})
	}
}

// The sweep bound must follow the pool-wide stat budget when one is wired
// (pool.Manager.StatSweepConcurrency: conservative while streams are active,
// full STAT pipeline capacity when idle), not the static fallback.
func TestPoolFetcherStatIDsUsesConfiguredStatConcurrency(t *testing.T) {
	client := &concurrencyRecordingStatClient{fakeStatClient: fakeStatClient{articles: map[string]bool{"live@x": true}}}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)
	f.StatConcurrency = func() int { return 7 }

	if _, err := f.StatIDs(context.Background(), []string{"live@x"}, nil); err != nil {
		t.Fatal(err)
	}
	if len(client.opts) == 0 {
		t.Fatal("StatMany never called")
	}
	for _, o := range client.opts {
		if o.Concurrency != 7 {
			t.Fatalf("Concurrency = %d, want 7 from the wired stat budget", o.Concurrency)
		}
	}
}

// laneRecordingClient counts which body lane each fetch used.
type laneRecordingClient struct {
	normal, background atomic.Int32
}

// Fetch records the lane the request declared. In v5 there is one method and
// the lane is a field, so this counts Req.Lane rather than which method the
// fetcher reached for.
func (c *laneRecordingClient) Fetch(_ context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	if r.Lane == nntppool.LaneBackground {
		c.background.Add(1)
	} else {
		c.normal.Add(1)
	}
	return &nntppool.ArticleBody{Bytes: []byte("payload")}, nil
}

// Repair reads a whole release nobody is waiting on. Every article must go
// through the pool's background lane, so the pool itself keeps playback and
// imports ahead of it instead of the fetcher guessing at a connection share.
func TestPoolFetcherFetchesOnBackgroundLane(t *testing.T) {
	client := &laneRecordingClient{}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)

	if _, err := f.Fetch(context.Background(), "a@x"); err != nil {
		t.Fatal(err)
	}
	if got := client.background.Load(); got != 1 {
		t.Fatalf("BodyBackground calls = %d, want 1", got)
	}
	if got := client.normal.Load(); got != 0 {
		t.Fatalf("Body calls = %d, want 0: repair must not ride the normal lane", got)
	}
}

// The liveness census is the same kind of work: it sweeps on the background
// lane so a full-release STAT burst never queues ahead of a stream's STAT.
func TestPoolFetcherStatIDsUsesBackgroundLane(t *testing.T) {
	client := &concurrencyRecordingStatClient{fakeStatClient: fakeStatClient{articles: map[string]bool{"live@x": true}}}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)

	if _, err := f.StatIDs(context.Background(), []string{"live@x", "gone@x"}, nil); err != nil {
		t.Fatal(err)
	}
	if len(client.opts) == 0 {
		t.Fatal("ExistsMany never called")
	}
	for _, o := range client.opts {
		if o.Lane != nntppool.LaneBackground {
			t.Fatalf("ExistsMany called on lane %v: census must ride the background lane", o.Lane)
		}
	}
}

// bodyOnlyClient has no stat surface; StatIDs must degrade to a no-op.
type bodyOnlyClient struct{}

func (bodyOnlyClient) Fetch(_ context.Context, _ nntppool.Req) (*nntppool.ArticleBody, error) {
	return &nntppool.ArticleBody{}, nil
}

func TestPoolFetcherStatIDsWithoutStatSupport(t *testing.T) {
	f := NewPoolFetcher(func() (BodyClient, error) { return bodyOnlyClient{}, nil }, nil)

	missing, err := f.StatIDs(context.Background(), []string{"a@x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if missing != nil {
		t.Fatalf("missing = %v, want nil (capability absent)", missing)
	}
}

// flakyBodyClient fails the first n Body calls per message with a transient
// pool error, then succeeds.
type flakyBodyClient struct {
	mu       sync.Mutex
	failures map[string]int // remaining transient failures per id
	calls    map[string]int
	err      error
}

func (c *flakyBodyClient) Fetch(_ context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	messageID := r.MessageID
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[messageID]++
	if c.failures[messageID] > 0 {
		c.failures[messageID]--
		return nil, c.err
	}
	return &nntppool.ArticleBody{Bytes: []byte("payload")}, nil
}

// A single transient pool failure ("all providers exhausted" during a network
// blip) must not surface: one failed article fetch aborts a repair attempt
// that took 20 minutes of sweeping, so the fetcher retries transient errors
// with a short backoff before giving up.
func TestPoolFetcherRetriesTransientErrors(t *testing.T) {
	client := &flakyBodyClient{
		failures: map[string]int{"blip@x": 2},
		err:      errors.New("nntp: all providers exhausted"),
	}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)
	f.retryDelay = time.Millisecond // keep the test fast

	data, err := f.Fetch(context.Background(), "blip@x")
	if err != nil {
		t.Fatalf("Fetch must absorb transient pool errors, got: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("payload = %q", data)
	}
	if got := client.calls["blip@x"]; got != 3 {
		t.Fatalf("Body calls = %d, want 3 (two transient failures then success)", got)
	}
}

// A 430 is a definitive verdict, not a blip: retrying it would slow every
// dead-article discovery by the whole backoff ladder.
func TestPoolFetcherDoesNotRetryArticleNotFound(t *testing.T) {
	client := &flakyBodyClient{
		failures: map[string]int{"gone@x": 99},
		err:      nntppool.ErrArticleNotFound,
	}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)
	f.retryDelay = time.Millisecond

	if _, err := f.Fetch(context.Background(), "gone@x"); !errors.Is(err, nntppool.ErrArticleNotFound) {
		t.Fatalf("err = %v, want ErrArticleNotFound", err)
	}
	if got := client.calls["gone@x"]; got != 1 {
		t.Fatalf("Body calls = %d, want 1 (no retry on a definitive 430)", got)
	}
}

// Persistent failure still surfaces after the retry budget, and a cancelled
// context aborts the backoff wait immediately.
func TestPoolFetcherRetryBudgetAndCancellation(t *testing.T) {
	client := &flakyBodyClient{
		failures: map[string]int{"down@x": 99},
		err:      errors.New("nntp: all providers exhausted"),
	}
	f := NewPoolFetcher(func() (BodyClient, error) { return client, nil }, nil)
	f.retryDelay = time.Millisecond

	if _, err := f.Fetch(context.Background(), "down@x"); err == nil {
		t.Fatal("persistent failure must surface after the retry budget")
	}
	budgetCalls := client.calls["down@x"]
	if budgetCalls < 2 {
		t.Fatalf("Body calls = %d, want the whole retry budget spent", budgetCalls)
	}

	f.retryDelay = time.Hour // cancellation must not wait this out
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.Fetch(ctx, "down@x")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled fetch must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled fetch must not sit in the retry backoff")
	}
}

// The repair's connection limiter must bound concurrent acquisitions to the
// live config value — including picking up a raised limit mid-flight, so a
// config change speeds up the next fetches without a restart.
func TestConnLimiterBoundsConcurrency(t *testing.T) {
	limit := 2
	var mu sync.Mutex
	limiter := NewConnLimiter(func() int {
		mu.Lock()
		defer mu.Unlock()
		return limit
	})

	var cur, peak int32
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := limiter.Acquire(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			n := atomic.AddInt32(&cur, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&cur, -1)
			release()
		}()
	}
	wg.Wait()
	if p := atomic.LoadInt32(&peak); p > 2 {
		t.Fatalf("peak concurrency = %d, want <= 2", p)
	}

	// Raising the limit lets more through.
	mu.Lock()
	limit = 4
	mu.Unlock()
	atomic.StoreInt32(&peak, 0)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := limiter.Acquire(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			n := atomic.AddInt32(&cur, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&cur, -1)
			release()
		}()
	}
	wg.Wait()
	if p := atomic.LoadInt32(&peak); p <= 2 || p > 4 {
		t.Fatalf("peak concurrency after raise = %d, want in (2, 4]", p)
	}

	// A cancelled context aborts a blocked acquire.
	blockers := make([]func(), 0, 4)
	for range 4 {
		release, err := limiter.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		blockers = append(blockers, release)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := limiter.Acquire(ctx)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("acquire on a cancelled context must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled acquire did not unblock")
	}
	for _, release := range blockers {
		release()
	}
}

// A combined budget must hold both underlying budgets for the fetch and
// release both afterwards — repair fetches take the repair's own cap AND the
// pool-wide import budget, so they yield to streams like imports do.
func TestCombineBudgetsAcquiresAndReleasesBoth(t *testing.T) {
	var aHeld, bHeld atomic.Int32
	a := ConnBudgetFunc(func(ctx context.Context) (func(), error) {
		aHeld.Add(1)
		return func() { aHeld.Add(-1) }, nil
	})
	b := ConnBudgetFunc(func(ctx context.Context) (func(), error) {
		if aHeld.Load() != 1 {
			t.Error("second budget acquired before the first")
		}
		bHeld.Add(1)
		return func() { bHeld.Add(-1) }, nil
	})

	release, err := CombineBudgets(a, b).Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if aHeld.Load() != 1 || bHeld.Load() != 1 {
		t.Fatalf("held = (%d, %d), want (1, 1)", aHeld.Load(), bHeld.Load())
	}
	release()
	if aHeld.Load() != 0 || bHeld.Load() != 0 {
		t.Fatalf("after release held = (%d, %d), want (0, 0)", aHeld.Load(), bHeld.Load())
	}
}

// A later budget failing must release the earlier acquisitions, or a stream
// burst that cancels a repair fetch would leak repair slots.
func TestCombineBudgetsReleasesOnLaterFailure(t *testing.T) {
	var aHeld atomic.Int32
	a := ConnBudgetFunc(func(ctx context.Context) (func(), error) {
		aHeld.Add(1)
		return func() { aHeld.Add(-1) }, nil
	})
	boom := errors.New("boom")
	b := ConnBudgetFunc(func(ctx context.Context) (func(), error) {
		return nil, boom
	})

	if _, err := CombineBudgets(a, b).Acquire(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if aHeld.Load() != 0 {
		t.Fatalf("first budget still held after later failure")
	}
}

// Nil entries are tolerated so callers can wire optional budgets without
// branching; a single non-nil budget passes straight through.
func TestCombineBudgetsSkipsNil(t *testing.T) {
	var held atomic.Int32
	a := ConnBudgetFunc(func(ctx context.Context) (func(), error) {
		held.Add(1)
		return func() { held.Add(-1) }, nil
	})

	release, err := CombineBudgets(nil, a, nil).Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if held.Load() != 1 {
		t.Fatalf("held = %d, want 1", held.Load())
	}
	release()
	if held.Load() != 0 {
		t.Fatalf("held = %d after release, want 0", held.Load())
	}
}
