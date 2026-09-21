package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/testsupport/nntpserver"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
)

// The provider model. A premium provider reached over TLS from the same
// continent: ~40 ms round trip, ~8 MB/s per connection, 750 KB articles. At
// 100 connections the aggregate ceiling (800 MB/s) is far above anything the
// pool will actually push, so what these benchmarks measure is the pool's own
// scheduling — not a saturated link.
const (
	benchRTT       = 40 * time.Millisecond
	benchJitter    = 10 * time.Millisecond
	benchBandwidth = 8 << 20
	// benchAggregateBW models the deployment's real link ceiling. Without it the
	// harness lets 100 connections sum to 800 MB/s, which no link does — and that
	// makes spending connections on latency look far more expensive than it is.
	benchAggregateBW  = 400 << 20
	benchArticleSize  = 750 << 10
	benchConns        = 100
	benchInflight     = 10  // per-connection concurrent bodies (config default)
	benchStatInflight = 100 // per-connection STAT pipeline depth (config default)

	// benchDuration is the measurement window per scenario. Long enough for a
	// few hundred stream segments at ~134 ms each, so p99 means something.
	benchDuration = 15 * time.Second

	// streamPrefetch mirrors the VFS read-ahead depth: how many segments of a
	// single playing file are in flight at once.
	streamPrefetch = 8

	// importWorkers is deliberately larger than the import connection budget
	// (which equals benchConns) so the budget, not the worker count, is what
	// bounds import body concurrency — as it is in production.
	importWorkers = 160

	// healthSweepConcurrency is health.max_connections_for_health_checks. Its
	// config default is 100 and validation rejects <= 0, so this branch always
	// wins over the stream-aware StatSweepConcurrency path.
	healthSweepConcurrency = 100
)

// noopStatsRepo satisfies StatsRepository so NewManager's metrics tracker has
// somewhere to write without a database.
type noopStatsRepo struct{}

func (noopStatsRepo) UpdateSystemStat(context.Context, string, int64) error { return nil }
func (noopStatsRepo) BatchUpdateSystemStats(context.Context, map[string]int64) error {
	return nil
}
func (noopStatsRepo) GetSystemStats(context.Context) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (noopStatsRepo) AddBytesDownloadedToDailyStat(context.Context, int64) error { return nil }
func (noopStatsRepo) AddProviderBytesToHourlyStat(context.Context, string, int64) error {
	return nil
}
func (noopStatsRepo) RecordProviderSpeedTest(context.Context, string, float64) error { return nil }
func (noopStatsRepo) GetProviderHourlyStats(context.Context, int) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (noopStatsRepo) ClearProviderHourlyStats(context.Context) error { return nil }
func (noopStatsRepo) GetOldestStatDate(context.Context) (time.Time, error) {
	return time.Time{}, nil
}
func (noopStatsRepo) GetOldestProviderStatDates(context.Context) (map[string]time.Time, error) {
	return map[string]time.Time{}, nil
}
func (noopStatsRepo) MigrateSystemStats(context.Context, map[string]int64, []string) error {
	return nil
}

// benchStreamSource is the StreamActivitySource the ImportBudget consults. The
// benchmark drives the count itself instead of standing up api.StreamTracker.
type benchStreamSource struct{ n atomic.Int64 }

func (s *benchStreamSource) ActiveStreams() int { return int(s.n.Load()) }

// latencies collects per-call durations without contending on a shared slice.
type latencies struct {
	mu vals
}

type vals struct {
	sync.Mutex
	d []time.Duration
}

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	l.mu.d = append(l.mu.d, d)
	l.mu.Unlock()
}

func (l *latencies) percentile(p float64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.mu.d) == 0 {
		return 0
	}
	sort.Slice(l.mu.d, func(i, j int) bool { return l.mu.d[i] < l.mu.d[j] })
	idx := int(p * float64(len(l.mu.d)-1))
	return l.mu.d[idx]
}

func (l *latencies) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.mu.d)
}

// harness is one running pool wired the way production wires it.
type harness struct {
	srv    *nntpserver.Server
	mgr    Manager
	client NntpClient
	stream *benchStreamSource
	cancel context.CancelFunc

	streamLat    latencies
	streamBytes  atomic.Int64
	importBytes  atomic.Int64
	statsDone    atomic.Int64
	dispatchOuts atomic.Int64
	otherErrs    atomic.Int64
	firstErr     atomic.Pointer[string]
}

func newHarness(tb testing.TB, statInflight int) *harness {
	tb.Helper()

	srv, err := nntpserver.New(nntpserver.Config{
		RTT:                benchRTT,
		Jitter:             benchJitter,
		BandwidthPerConn:   benchBandwidth,
		AggregateBandwidth: envInt64("ALTMOUNT_BENCH_AGG_BW", benchAggregateBW),
		ArticleSize:        benchArticleSize,
	})
	if err != nil {
		tb.Fatalf("nntpserver.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgr := NewManager(ctx, noopStatsRepo{})

	if err := mgr.SetProviders([]nntppool.Provider{{
		Host:        "bench",
		Factory:     srv.Dial,
		Auth:        nntppool.Auth{Username: "bench", Password: "bench"},
		Connections: benchConns,
		// Pre-warm every slot (min_connections_alive) so the measurement
		// window is not spent dialing.
		MinConnections: benchConns,
		Inflight:       benchInflight,
		StatInflight:   statInflight,
		SkipPing:       true,
		IdleTimeout:    time.Hour,
	}}); err != nil {
		cancel()
		_ = srv.Close()
		tb.Fatalf("SetProviders: %v", err)
	}

	client, err := mgr.GetPool()
	if err != nil {
		cancel()
		_ = srv.Close()
		tb.Fatalf("GetPool: %v", err)
	}

	stream := &benchStreamSource{}
	mgr.SetStreamSource(stream)
	// Production sets this to Config.TotalProviderConnections(). The multiplier
	// lets a run model deeper import body pipelining (budget above the connection
	// count) without touching production defaults.
	mgr.SetImportConnCapacity(benchConns * int(envInt64("ALTMOUNT_BENCH_BUDGET_MULT", 1)))
	// Exercise the shipped knob rather than ImportBudget's bare default, so the
	// benchmark measures what a deployment actually runs.
	mgr.SetStreamHeadroom(int(envInt64("ALTMOUNT_BENCH_HEADROOM", 8)))

	h := &harness{srv: srv, mgr: mgr, client: client, stream: stream, cancel: cancel}

	tb.Cleanup(func() {
		_ = mgr.ClearPool()
		cancel()
		_ = srv.Close()
	})

	h.warmUp(tb)
	return h
}

// warmUp waits for every pre-warmed slot to finish dialing and authenticating,
// then runs a short sweep so the measurement window starts on a settled pool.
func (h *harness) warmUp(tb testing.TB) {
	tb.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for h.srv.Counters().Conns < benchConns && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := h.srv.Counters().Conns; got < benchConns {
		tb.Fatalf("only %d/%d connections established after warmup", got, benchConns)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ids := make([]string, benchConns*4)
	for i := range ids {
		ids[i] = segments.MessageID(i)
	}
	for res := range h.client.ExistsMany(ctx, ids, nntppool.ManyOptions{Concurrency: benchConns}) {
		if res.Err != nil {
			tb.Fatalf("warmup stat %s: %v", res.MessageID, res.Err)
		}
	}
}

func (h *harness) record(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return // teardown, not a workload failure
	}
	var ate *nntppool.AttemptTimeoutError
	if errors.As(err, &ate) && ate.Phase == nntppool.PhaseDispatch {
		h.dispatchOuts.Add(1)
		return
	}
	h.otherErrs.Add(1)
	if h.firstErr.Load() == nil {
		msg := fmt.Sprintf("%T: %v", err, err)
		h.firstErr.CompareAndSwap(nil, &msg)
	}
}

// runStream models one playing file: streamPrefetch workers pulling sequential
// segments through the priority lane, exactly as usenet.NewUsenetReader does
// with its default streaming profile.
func (h *harness) runStream(ctx context.Context, wg *sync.WaitGroup) {
	h.stream.n.Add(1)
	h.mgr.NotifyStreamChange()

	var next atomic.Int64
	for range streamPrefetch {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				id := segments.MessageID(int(next.Add(1)))
				start := time.Now()
				body, err := h.client.Fetch(ctx, nntppool.Req{MessageID: id, Lane: nntppool.LanePriority})
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					h.record(err)
					continue
				}
				h.streamLat.add(time.Since(start))
				h.streamBytes.Add(int64(len(body.Bytes)))
			}
		}()
	}
}

// runImportBodies models import segment downloads: normal lane, gated by the
// import connection budget, as usenet.NewUsenetReader does under
// WithImportProfile.
func (h *harness) runImportBodies(ctx context.Context, wg *sync.WaitGroup) {
	var next atomic.Int64
	for range importWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				release, err := h.mgr.AcquireImportConnection(ctx)
				if err != nil {
					return
				}
				id := segments.MessageID(int(next.Add(1)) + 1_000_000)
				body, err := h.client.Fetch(ctx, nntppool.Req{MessageID: id})
				release()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					h.record(err)
					continue
				}
				h.importBytes.Add(int64(len(body.Bytes)))
			}
		}()
	}
}

// runSweep models a chunked availability sweep on the normal lane. concurrency
// is resolved per chunk so the import sweep's stream-aware widening
// (StatSweepConcurrency) is exercised for real.
func (h *harness) runSweep(ctx context.Context, wg *sync.WaitGroup, idBase int, concurrency func() int) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		next := idBase
		for ctx.Err() == nil {
			// Both real sweep sites chunk by their concurrency bound
			// (fast_fail.go:385, usenet/validation.go:169), so chunk size and
			// concurrency are the same number.
			conc := concurrency()
			ids := make([]string, conc)
			for i := range ids {
				ids[i] = segments.MessageID(next)
				next++
			}

			chunkCtx, cancel := context.WithTimeout(ctx, StatManyTimeout(len(ids), conc, 30*time.Second))
			for res := range h.client.ExistsMany(chunkCtx, ids, nntppool.ManyOptions{Concurrency: conc}) {
				if ctx.Err() != nil {
					continue
				}
				if res.Err != nil {
					h.record(res.Err)
					continue
				}
				h.statsDone.Add(1)
			}
			cancel()
		}
	}()
}

type scenario struct {
	name   string
	stream bool
	body   bool
	imprt  bool // import fast-fail STAT sweep
	health bool // health-check STAT sweep
	// headroom, when non-nil, runs the adaptive import-headroom controller with
	// this policy instead of leaving the budget at its fixed default.
	headroom *HeadroomPolicy
}

var scenarios = []scenario{
	{name: "stream_only", stream: true},
	{name: "stream+import", stream: true, body: true, imprt: true},
	{name: "stream+import+health", stream: true, body: true, imprt: true, health: true},
	{name: "import+health_nostream", body: true, imprt: true, health: true},
	{name: "health_only_nostream", health: true},
	{
		name: "adaptive_free", stream: true, body: true, imprt: true, health: true,
		headroom: &HeadroomPolicy{MaxTotalCostPct: 0},
	},
	{
		name: "adaptive_spend15", stream: true, body: true, imprt: true, health: true,
		headroom: &HeadroomPolicy{MaxTotalCostPct: 15},
	},
}

// BenchmarkContention measures what background STAT sweeps and import bodies
// cost a live stream at 100 connections. Run with:
//
//	go test ./internal/pool -run '^$' -bench BenchmarkContention -benchtime 1x -timeout 30m
func BenchmarkContention(b *testing.B) {
	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			runScenario(b, sc, benchStatInflight, healthSweepConcurrency)
		})
	}
}

// BenchmarkContentionStatInflight sweeps the per-connection STAT pipeline depth
// (providers[].stat_inflight_requests) under the full three-way workload.
func BenchmarkContentionStatInflight(b *testing.B) {
	sc := scenarios[2] // stream+import+health
	for _, depth := range []int{10, 50, 100, 200} {
		b.Run(fmt.Sprintf("stat_inflight=%d", depth), func(b *testing.B) {
			runScenario(b, sc, depth, healthSweepConcurrency)
		})
	}
}

// BenchmarkContentionHealthConcurrency sweeps
// health.max_connections_for_health_checks under the full three-way workload,
// which is what an operator can actually turn down today.
func BenchmarkContentionHealthConcurrency(b *testing.B) {
	sc := scenarios[2]
	for _, conc := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("health_conc=%d", conc), func(b *testing.B) {
			runScenario(b, sc, benchStatInflight, conc)
		})
	}
}

// envInt64 reads an int64 override from the environment, falling back to def.
func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

// scenarioDuration lets a smoke run shorten the measurement window without
// editing the constant. Unset means benchDuration.
func scenarioDuration() time.Duration {
	if v := os.Getenv("ALTMOUNT_BENCH_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return benchDuration
}

func runScenario(b *testing.B, sc scenario, statInflight, healthConc int) {
	h := newHarness(b, statInflight)

	h.srv.ResetPeakInflight()
	before := h.srv.Counters()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	if sc.stream {
		h.runStream(ctx, &wg)
	}
	if sc.body {
		h.runImportBodies(ctx, &wg)
	}
	if sc.imprt {
		// The adaptive path: conservative (Config.StatConcurrency() == 100)
		// while a stream is active, StatCapacity() otherwise.
		h.runSweep(ctx, &wg, 2_000_000, func() int {
			return h.mgr.StatSweepConcurrency(100)
		})
	}
	if sc.health {
		// The real health path short-circuits on the operator knob, so the
		// stream-aware bound is never consulted.
		h.runSweep(ctx, &wg, 3_000_000, func() int { return healthConc })
	}

	// The controller samples every 15s in production; a 15s measurement window
	// would give it one decision. Drive step directly on a fast ticker so the
	// same number of decisions happens inside the window.
	var ctrl *headroomController
	if sc.headroom != nil {
		ctrl = newHeadroomController(h.mgr.(*manager).budget, *sc.headroom)
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(1500 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-t.C:
					ctrl.step(now, h.srv.Counters().BytesWritten, h.stream.ActiveStreams())
				}
			}
		}()
	}

	b.ResetTimer()
	start := time.Now()
	time.Sleep(scenarioDuration())
	elapsed := time.Since(start)
	cancel()
	wg.Wait()
	b.StopTimer()

	after := h.srv.Counters()
	secs := elapsed.Seconds()

	b.ReportMetric(0, "ns/op") // the wall clock is fixed; per-op timing is meaningless here

	if sc.stream {
		b.ReportMetric(float64(h.streamLat.percentile(0.50).Milliseconds()), "stream_p50_ms")
		b.ReportMetric(float64(h.streamLat.percentile(0.95).Milliseconds()), "stream_p95_ms")
		b.ReportMetric(float64(h.streamLat.percentile(0.99).Milliseconds()), "stream_p99_ms")
		b.ReportMetric(float64(h.streamBytes.Load())/secs/(1<<20), "stream_MB/s")
		b.ReportMetric(float64(h.streamLat.count())/secs, "stream_seg/s")
	}
	if sc.body {
		b.ReportMetric(float64(h.importBytes.Load())/secs/(1<<20), "import_MB/s")
	}
	if sc.imprt || sc.health {
		b.ReportMetric(float64(h.statsDone.Load())/secs, "stat/s")
	}
	b.ReportMetric(float64(h.dispatchOuts.Load()), "dispatch_timeouts")
	b.ReportMetric(float64(h.otherErrs.Load()), "other_errors")
	b.ReportMetric(float64(after.PeakInflight), "peak_pipeline_depth")
	if ctrl != nil {
		b.ReportMetric(float64(ctrl.reserve), "final_headroom")
		b.ReportMetric(float64(ctrl.learned), "learned_headroom")
	}

	firstErr := ""
	if p := h.firstErr.Load(); p != nil {
		firstErr = *p
	}
	fmt.Fprintf(os.Stderr, "    [%s] server: stats=%d bodies=%d peak_pipeline=%d conns=%d first_err=%q\n",
		b.Name(), after.Stats-before.Stats, after.Bodies-before.Bodies,
		after.PeakInflight, after.Conns, firstErr)
}
