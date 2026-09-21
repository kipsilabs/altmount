package validation

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/pool"
)

const (
	// hedgeMinReported is how many answers a sweep needs before a lull in
	// arrivals means anything: below it there is no picture of what normal
	// latency looks like, so nothing is hedged.
	hedgeMinReported = 8
	// hedgeGraceLatencyFactor scales the observed median STAT latency into the
	// lull — time since the last answer — after which the outstanding ids are
	// re-issued.
	hedgeGraceLatencyFactor = 3
	hedgeMinGrace           = 250 * time.Millisecond
	hedgeMaxGrace           = 750 * time.Millisecond
)

// hedgedStatMany runs one StatMany sweep over ids and, once answers have been
// arriving and then stop for a grace period, re-issues every id still
// outstanding on a second, priority-lane sweep. The stragglers measured were
// STATs pipelined on slow or cold connections while the rest of the sweep
// answered in ~150 ms — anywhere from one id to a third of the probe — and a
// re-issue picked up by an idle connection answers in tens of milliseconds.
// Each id is reported at most once, whichever sweep answers first; both sweeps
// are cancelled as soon as every id has reported.
func hedgedStatMany(ctx context.Context, client pool.NntpClient, ids []string, concurrency int, articleDate time.Time) <-chan nntppool.ExistsResult {
	out := make(chan nntppool.ExistsResult, len(ids))
	go func() {
		defer close(out)
		sweepCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		start := time.Now()
		var mu sync.Mutex
		reported := make(map[string]struct{}, len(ids))
		latencies := make([]time.Duration, 0, len(ids))

		primary := client.ExistsMany(sweepCtx, ids, nntppool.ManyOptions{
			Concurrency: concurrency,
			ArticleDate: articleDate,
		})
		var hedge <-chan nntppool.ExistsResult
		lull := time.NewTimer(time.Hour)
		lull.Stop()
		defer lull.Stop()

		deliver := func(r nntppool.ExistsResult, fromHedge bool) {
			mu.Lock()
			if _, dup := reported[r.MessageID]; dup {
				mu.Unlock()
				return
			}
			reported[r.MessageID] = struct{}{}
			latency := time.Since(start)
			latencies = append(latencies, latency)
			done := len(reported)
			mu.Unlock()

			if hedge != nil {
				slog.DebugContext(ctx, "hedged fast-fail STAT answered",
					"segment_id", r.MessageID,
					"from_hedge", fromHedge,
					"latency", latency,
					"error", r.Err)
			}
			out <- r
			switch {
			case done == len(ids):
				cancel()
			case hedge == nil && done >= hedgeMinReported:
				// Every answer restarts the lull: hedging begins only when
				// the sweep has gone quiet with ids still outstanding.
				lull.Reset(hedgeGrace(latencies))
			}
		}

		for primary != nil || hedge != nil {
			select {
			case r, ok := <-primary:
				if !ok {
					primary = nil
					continue
				}
				deliver(r, false)
			case r, ok := <-hedge:
				if !ok {
					hedge = nil
					continue
				}
				deliver(r, true)
			case <-lull.C:
				mu.Lock()
				stragglers := make([]string, 0, len(ids)-len(reported))
				for _, id := range ids {
					if _, ok := reported[id]; !ok {
						stragglers = append(stragglers, id)
					}
				}
				mu.Unlock()
				if len(stragglers) == 0 {
					continue
				}
				slog.DebugContext(ctx, "hedging straggling fast-fail STATs",
					"stragglers", len(stragglers),
					"reported", len(ids)-len(stragglers),
					"elapsed", time.Since(start))
				// The stragglers are queued behind other normal-lane traffic;
				// a hedge on the same lane would join the queue. The priority
				// lane lets an idle connection pick these bodyless requests up
				// ahead of it.
				hedge = client.ExistsMany(sweepCtx, stragglers, nntppool.ManyOptions{
					Concurrency: len(stragglers),
					Lane:        nntppool.LanePriority,
					ArticleDate: articleDate,
					Skip: func(id string) bool {
						mu.Lock()
						defer mu.Unlock()
						_, done := reported[id]
						return done
					},
				})
			}
		}
	}()
	return out
}

// hedgeGrace turns the latencies observed so far into how long the sweep may
// stay silent before the outstanding ids are re-issued: a few medians, clamped
// so a very fast provider is not hedged on jitter and a slow one is not waited
// out to the deadline.
func hedgeGrace(latencies []time.Duration) time.Duration {
	sorted := slices.Clone(latencies)
	slices.Sort(sorted)
	median := sorted[len(sorted)/2]
	return min(max(median*hedgeGraceLatencyFactor, hedgeMinGrace), hedgeMaxGrace)
}
