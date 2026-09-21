package par2repair

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/javi11/nntppool/v5"
)

// BodyClient is the one pool method the fetcher needs: an article body fetch.
// Satisfied by pool.NntpClient. Repair traffic sets Req.Lane to
// LaneBackground; see PoolFetcher.Fetch.
type BodyClient interface {
	Fetch(ctx context.Context, r nntppool.Req) (*nntppool.ArticleBody, error)
}

// ConnBudget bounds how many article fetches the repair keeps on the wire at
// once. Repair traffic rides the pool's background lane, which the pool
// itself keeps behind playback and imports; the budget only shapes how much
// of an idle pool a repair may fill.
type ConnBudget interface {
	Acquire(ctx context.Context) (release func(), err error)
}

// ConnBudgetFunc adapts a bare acquire function — such as
// pool.Manager.AcquireImportConnection — to ConnBudget.
type ConnBudgetFunc func(ctx context.Context) (release func(), err error)

func (f ConnBudgetFunc) Acquire(ctx context.Context) (func(), error) { return f(ctx) }

// CombineBudgets stacks budgets: Acquire takes each in order and release
// returns them in reverse. A later acquisition failing hands back everything
// already held. Nil entries are skipped so optional budgets wire without
// branching. Order matters for contention: put the narrow repair cap first so
// at most that many fetches ever queue on the wider shared budget.
func CombineBudgets(budgets ...ConnBudget) ConnBudget {
	return ConnBudgetFunc(func(ctx context.Context) (func(), error) {
		releases := make([]func(), 0, len(budgets))
		releaseAll := func() {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		}
		for _, b := range budgets {
			if b == nil {
				continue
			}
			release, err := b.Acquire(ctx)
			if err != nil {
				releaseAll()
				return nil, err
			}
			releases = append(releases, release)
		}
		return releaseAll, nil
	})
}

// connLimiter is a ConnBudget whose limit is read live from the config, so a
// raised max_connections speeds up a running repair as slots churn — no
// restart needed.
type connLimiter struct {
	limit func() int
	mu    sync.Mutex
	cond  *sync.Cond
	inUse int
}

// NewConnLimiter builds a ConnBudget over a live limit getter. Values below 1
// are treated as 1.
func NewConnLimiter(limit func() int) ConnBudget {
	l := &connLimiter{limit: limit}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *connLimiter) Acquire(ctx context.Context) (func(), error) {
	// Wake blocked waiters when the context ends, so cancellation is not
	// stuck behind the next release.
	stop := context.AfterFunc(ctx, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.cond.Broadcast()
	})
	defer stop()

	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if l.inUse < max(l.limit(), 1) {
			l.inUse++
			var once sync.Once
			return func() {
				once.Do(func() {
					l.mu.Lock()
					l.inUse--
					l.mu.Unlock()
					l.cond.Broadcast()
				})
			}, nil
		}
		l.cond.Wait()
	}
}

// PoolFetcher fetches decoded article payloads through the NNTP pool's
// background lane. The client is resolved per fetch so the fetcher survives
// pool reconfiguration and boot ordering.
type PoolFetcher struct {
	getClient func() (BodyClient, error)
	budget    ConnBudget // optional; nil skips budget gating

	// StatConcurrency, when set, supplies the in-flight bound for each
	// liveness-sweep pass — wired to the config's conservative stat bound
	// (one connection's STAT pipeline depth). Nil falls back to
	// statSweepConcurrency.
	StatConcurrency func() int

	// retryDelay is the base backoff between transient-failure retries,
	// doubled per attempt. Shrunk in tests.
	retryDelay time.Duration

	// articleDate is when the release under repair was posted, forwarded to
	// the pool so a provider whose retention does not reach back that far is
	// tried last or not at all. Zero applies no policy. Set per release via
	// ForArticleDate, never mutated: one PoolFetcher is shared by every job.
	articleDate time.Time
}

// ForArticleDate returns a copy of the fetcher bound to one release's article
// date. The service calls it per job, so the shared fetcher itself is never
// mutated and concurrent repairs of different releases cannot see each
// other's date.
func (p *PoolFetcher) ForArticleDate(at time.Time) ArticleFetcher {
	if at.IsZero() {
		return p
	}
	bound := *p
	bound.articleDate = at
	return &bound
}

// NewPoolFetcher builds a fetcher over a lazily-resolved pool client
// (typically wrapping pool.Manager.GetPool). budget may be nil.
func NewPoolFetcher(getClient func() (BodyClient, error), budget ConnBudget) *PoolFetcher {
	return &PoolFetcher{getClient: getClient, budget: budget, retryDelay: fetchRetryBaseDelay}
}

// fetchRetries is how many times one article fetch is retried after a
// transient pool error before the failure surfaces. A repair attempt is tens
// of minutes of streaming; a pool-wide blip of a few seconds ("all providers
// exhausted" with no per-provider verdict) must cost a short stall, not the
// whole attempt.
const fetchRetries = 3

// fetchRetryBaseDelay is the first retry's backoff; each further retry
// doubles it (1s, 2s, 4s).
const fetchRetryBaseDelay = time.Second

// ArticleStater is an optional ArticleFetcher capability: bulk article
// existence checks without downloading bodies. Resolvers use it to learn the
// release's full damage before planning, so unrepairable verdicts land in
// seconds instead of after downloading the recovery set.
type ArticleStater interface {
	// StatIDs reports which of ids are confirmed missing on every provider.
	// Transient failures must NOT be reported missing. A nil map with a nil
	// error means the capability is unavailable and the caller should proceed
	// without liveness data. onResult, when non-nil, receives the count of ids
	// checked so far as verdicts arrive, for progress reporting.
	StatIDs(ctx context.Context, ids []string, onResult func(done int)) (missing map[string]bool, err error)
}

// statManyClient is the stat surface PoolFetcher probes its pool client for
// (satisfied by *nntppool.Client and pool.NntpClient implementations).
type statManyClient interface {
	ExistsMany(ctx context.Context, messageIDs []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult
}

// statPerItemBudget bounds a liveness sweep: STATs are single-line round
// trips dispatched dozens-at-a-time, so even a generous per-item share keeps
// the overall deadline in seconds-to-minutes for a full release.
const statPerItemBudget = 250 * time.Millisecond

// statSweepConcurrency caps how many STATs one sweep keeps outstanding.
// Left unset, StatMany derives its bound from the pool's aggregate STAT
// pipeline capacity — thousands for a typical multi-provider config — and a
// full-release census then floods every provider's dispatch queue at once.
// Requests that cannot be dispatched within the per-attempt window fail over
// silently until the sweep's own burst reads back as "all providers
// exhausted" on most of its articles. STATs are cheap round trips: this many
// outstanding still censuses thousands of articles in seconds while leaving
// the pool responsive for everything else running beside the repair.
const statSweepConcurrency = 64

// sweepStatDepth is how many STATs a repair sweep may keep outstanding per
// repair connection. The repair's connection budget gates body fetches, but
// STATs are pipelined, so "connections" alone is not the unit — this converts
// the budget into a shallow pipeline that drains well inside the pool's
// per-attempt window (a few RTTs), instead of letting a 10-connection repair
// occupy every connection's STAT pipeline.
const sweepStatDepth = 4

// SweepStatConcurrency bounds a repair liveness sweep: the pool-wide sweep
// budget (pool.Manager.StatSweepConcurrency) capped by the repair's own
// connection budget × sweepStatDepth. Non-positive inputs fall back to the
// other bound; the result is always at least 1.
func SweepStatConcurrency(poolWidth, maxConns int) int {
	width := poolWidth
	if maxConns > 0 {
		repairCap := maxConns * sweepStatDepth
		if width <= 0 || repairCap < width {
			width = repairCap
		}
	}
	return max(width, 1)
}

// StatIDs implements ArticleStater over the pool's StatMany when the client
// supports it; otherwise it reports the capability unavailable (nil, nil).
//
// A STAT that fails with a transport error ("all providers exhausted" while
// connections flap) proves nothing about the article, so unresolved ids are
// re-STATed with the same backoff ladder Fetch uses. Ids still unresolved
// after the retry budget are not reported missing (the contract), but when
// they outnumber what margin rows can absorb the whole sweep fails instead:
// planning on that many silent "alive" verdicts once let a ~90%-dead release
// sweep clean and cost ten minutes of serial dead-article discovery before
// the inevitable unrepairable verdict.
func (p *PoolFetcher) StatIDs(ctx context.Context, ids []string, onResult func(done int)) (map[string]bool, error) {
	client, err := p.getClient()
	if err != nil {
		return nil, fmt.Errorf("par2repair: nntp pool unavailable: %w", err)
	}
	sc, ok := client.(statManyClient)
	if !ok {
		return nil, nil
	}

	missing := make(map[string]bool)
	done := 0
	delay := p.retryDelay
	if delay <= 0 {
		delay = fetchRetryBaseDelay
	}
	pending := ids
	for attempt := 0; ; attempt++ {
		unresolved, err := p.statOnce(ctx, sc, pending, missing, &done, onResult)
		if err != nil {
			return nil, err
		}
		pending = unresolved
		if len(pending) == 0 || attempt >= fetchRetries {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	if len(pending) > maxHiddenAbsorbArticles {
		return nil, fmt.Errorf("par2repair: liveness sweep got no verdict for %d of %d articles (transient provider errors)",
			len(pending), len(ids))
	}
	return missing, nil
}

// statOnce runs one StatMany pass over ids, folding definitive verdicts into
// missing/done and returning the ids that got none — transport errors, plus
// ids the pass abandoned before dispatching.
func (p *PoolFetcher) statOnce(
	ctx context.Context,
	sc statManyClient,
	ids []string,
	missing map[string]bool,
	done *int,
	onResult func(done int),
) ([]string, error) {
	timeout := max(time.Duration(len(ids))*statPerItemBudget, time.Minute)
	statCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conc := statSweepConcurrency
	if p.StatConcurrency != nil {
		if v := p.StatConcurrency(); v > 0 {
			conc = v
		}
	}
	resolved := make(map[string]bool, len(ids))
	var firstErr error
	for r := range sc.ExistsMany(statCtx, ids, nntppool.ManyOptions{
		Concurrency: conc,
		Lane:        nntppool.LaneBackground,
		ArticleDate: p.articleDate,
	}) {
		switch {
		case errors.Is(r.Err, nntppool.ErrArticleNotFound):
			missing[r.MessageID] = true
		case r.Err != nil:
			// Transport failure: no verdict either way.
			if firstErr == nil {
				firstErr = r.Err
			}
			continue
		}
		resolved[r.MessageID] = true
		*done++
		if onResult != nil {
			onResult(*done)
		}
	}
	// A cancelled sweep left ids unchecked; report it rather than a partial
	// view that would understate the damage.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var unresolved []string
	for _, id := range ids {
		if !resolved[id] {
			unresolved = append(unresolved, id)
		}
	}
	if len(unresolved) > 0 {
		slog.DebugContext(ctx, "liveness sweep pass left articles unresolved",
			"unresolved", len(unresolved), "total", len(ids), "example_error", firstErr)
	}
	return unresolved, nil
}

// Fetch implements ArticleFetcher. Transient pool errors — a network blip
// timing out every provider at once surfaces as a bare "all providers
// exhausted" — are retried with a doubling backoff before they surface: one
// failed fetch aborts a repair attempt that took tens of minutes of
// streaming, so a blip must cost seconds, not the attempt. A 430
// (ErrArticleNotFound) is a definitive verdict and is never retried.
func (p *PoolFetcher) Fetch(ctx context.Context, messageID string) ([]byte, error) {
	delay := p.retryDelay
	if delay <= 0 {
		delay = fetchRetryBaseDelay
	}
	var lastErr error
	for attempt := 0; attempt <= fetchRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}
		data, err := p.fetchOnce(ctx, messageID)
		if err == nil {
			return data, nil
		}
		if errors.Is(err, nntppool.ErrArticleNotFound) || ctx.Err() != nil {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// fetchOnce performs one budget-gated article fetch. The budget is held only
// for the attempt, so a retry's backoff never sits on an import slot.
func (p *PoolFetcher) fetchOnce(ctx context.Context, messageID string) ([]byte, error) {
	if p.budget != nil {
		release, err := p.budget.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("par2repair: acquire repair connection: %w", err)
		}
		defer release()
	}
	client, err := p.getClient()
	if err != nil {
		return nil, fmt.Errorf("par2repair: nntp pool unavailable: %w", err)
	}
	// nntppool adds the angle brackets itself; message IDs here are bare.
	body, err := client.Fetch(ctx, nntppool.Req{
		MessageID:   messageID,
		Lane:        nntppool.LaneBackground,
		ArticleDate: p.articleDate,
	})
	if err != nil {
		return nil, err
	}
	return body.Bytes, nil
}
