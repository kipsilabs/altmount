package nzbfilesystem

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/streambench"
	"github.com/kipsilabs/altmount/internal/testsupport/nntpserver"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/javi11/nntppool/v5"
)

// Provider models for the streaming benchmarks. The premium profile matches
// internal/pool/contention_bench_test.go so numbers are comparable across
// the two suites; the slow profile models large parts over a distant link,
// where time-to-first-byte is dominated by the article transfer itself.
type benchProfile struct {
	Name         string
	RTT, Jitter  time.Duration
	PerConnBW    int64
	AggBW        int64
	ArticleSize  int
	Conns        int
	Inflight     int
	StatInflight int
}

var profilePremium750K = benchProfile{
	Name: "premium-750k", RTT: 40 * time.Millisecond, Jitter: 10 * time.Millisecond,
	PerConnBW: 8 << 20, AggBW: 400 << 20, ArticleSize: 750 << 10,
	Conns: 50, Inflight: 10, StatInflight: 100,
}

var profileSlow4M = benchProfile{
	Name: "slow-4m", RTT: 100 * time.Millisecond, Jitter: 10 * time.Millisecond,
	PerConnBW: 3 << 20, AggBW: 60 << 20, ArticleSize: 4 << 20,
	Conns: 20, Inflight: 10, StatInflight: 100,
}

type benchStreams struct{ n int }

func (b *benchStreams) ActiveStreams() int { return b.n }

// noopBenchStats satisfies pool.StatsRepository without a database.
type noopBenchStats struct{}

func (noopBenchStats) UpdateSystemStat(context.Context, string, int64) error          { return nil }
func (noopBenchStats) BatchUpdateSystemStats(context.Context, map[string]int64) error { return nil }
func (noopBenchStats) GetSystemStats(context.Context) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (noopBenchStats) AddBytesDownloadedToDailyStat(context.Context, int64) error        { return nil }
func (noopBenchStats) AddProviderBytesToHourlyStat(context.Context, string, int64) error { return nil }
func (noopBenchStats) RecordProviderSpeedTest(context.Context, string, float64) error    { return nil }
func (noopBenchStats) GetProviderHourlyStats(context.Context, int) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (noopBenchStats) ClearProviderHourlyStats(context.Context) error { return nil }
func (noopBenchStats) GetOldestStatDate(context.Context) (time.Time, error) {
	return time.Time{}, nil
}
func (noopBenchStats) GetOldestProviderStatDates(context.Context) (map[string]time.Time, error) {
	return map[string]time.Time{}, nil
}
func (noopBenchStats) MigrateSystemStats(context.Context, map[string]int64, []string) error {
	return nil
}

type benchHarness struct {
	srv     []*nntpserver.Server
	mgr     pool.Manager
	client  pool.NntpClient
	ctx     context.Context
	cancel  context.CancelFunc
	profile benchProfile
	streams *benchStreams
	// store, when set, is handed to every file opened afterwards as its
	// segment store (nil = no cache, the default for most scenarios).
	store usenet.SegmentStore
}

// newBenchHarness starts one simulated provider per cfg (defaults to a
// single provider built from the profile), wires a real pool.Manager the
// way cmd/altmount does, and pre-warms every connection so the measurement
// window is not spent dialing.
func newBenchHarness(tb testing.TB, p benchProfile, cfgs ...nntpserver.Config) *benchHarness {
	tb.Helper()
	if len(cfgs) == 0 {
		cfgs = []nntpserver.Config{{}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &benchHarness{ctx: ctx, cancel: cancel, profile: p, streams: &benchStreams{}}

	var providers []nntppool.Provider
	for i, cfg := range cfgs {
		if cfg.RTT == 0 {
			cfg.RTT = p.RTT
		}
		if cfg.Jitter == 0 {
			cfg.Jitter = p.Jitter
		}
		if cfg.BandwidthPerConn == 0 {
			cfg.BandwidthPerConn = p.PerConnBW
		}
		if cfg.AggregateBandwidth == 0 {
			cfg.AggregateBandwidth = p.AggBW
		}
		if cfg.ArticleSize == 0 {
			cfg.ArticleSize = p.ArticleSize
		}
		srv, err := nntpserver.New(cfg)
		if err != nil {
			cancel()
			tb.Fatalf("nntpserver.New: %v", err)
		}
		h.srv = append(h.srv, srv)
		providers = append(providers, nntppool.Provider{
			Host:           "bench-" + string(rune('a'+i)),
			Factory:        srv.Dial,
			Auth:           nntppool.Auth{Username: "bench", Password: "bench"},
			Connections:    p.Conns,
			MinConnections: p.Conns,
			Inflight:       p.Inflight,
			StatInflight:   p.StatInflight,
			StreamInflight: p.Inflight,
			SkipPing:       true,
			IdleTimeout:    time.Hour,
		})
	}

	h.mgr = pool.NewManager(ctx, noopBenchStats{})
	if err := h.mgr.SetProviders(providers); err != nil {
		tb.Fatalf("SetProviders: %v", err)
	}
	client, err := h.mgr.GetPool()
	if err != nil {
		tb.Fatalf("GetPool: %v", err)
	}
	h.client = client
	h.mgr.SetStreamSource(h.streams)
	h.mgr.SetImportConnCapacity(p.Conns * len(cfgs))
	h.mgr.SetStreamHeadroom(p.Conns / 4)

	tb.Cleanup(func() {
		_ = h.mgr.ClearPool()
		cancel()
		for _, s := range h.srv {
			_ = s.Close()
		}
	})

	var wg sync.WaitGroup
	for i := 0; i < p.Conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.Fetch(ctx, nntppool.Req{MessageID: "warm@bench", Lane: nntppool.LanePriority})
		}()
	}
	wg.Wait()
	for _, s := range h.srv {
		s.ResetPeakInflight()
	}
	return h
}

// openFile builds a plain (unencrypted, non-nested) virtual file of nSegs
// articles wired to the real pool. rangeEnd < 0 means no Range header.
func (h *benchHarness) openFile(tb testing.TB, nSegs, maxPrefetch int, rangeEnd int64) *MetadataVirtualFile {
	tb.Helper()
	segSize := h.profile.ArticleSize
	mvf := &MetadataVirtualFile{
		name: "bench-file",
		meta: &fileHandleMeta{
			FileSize:    int64(nSegs) * int64(segSize),
			SegmentData: buildSegmentData(tb, nSegs, segSize),
		},
		poolManager:      h.mgr,
		ctx:              h.ctx,
		maxPrefetch:      maxPrefetch,
		originalRangeEnd: rangeEnd,
		streamTracker:    noopStreamTracker{},
		streamID:         "bench-stream",
		segmentStore:     h.store,
	}
	tb.Cleanup(func() { _ = mvf.Close() })
	return mvf
}

func (h *benchHarness) bodies() int64 {
	var n int64
	for _, s := range h.srv {
		n += s.Counters().Bodies
	}
	return n
}

func (h *benchHarness) bytesWritten() int64 {
	var n int64
	for _, s := range h.srv {
		n += s.Counters().BytesWritten
	}
	return n
}

// Process-wide result accumulator, flushed by TestMain when
// ALTMOUNT_BENCH_OUT names an output path.
var (
	benchResultMu sync.Mutex
	benchResultV  *streambench.Result
)

func benchResult() *streambench.Result {
	benchResultMu.Lock()
	defer benchResultMu.Unlock()
	if benchResultV == nil {
		benchResultV = &streambench.Result{GitSHA: gitShortSHA()}
	}
	return benchResultV
}

func gitShortSHA() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func flushBenchResult() {
	path := os.Getenv("ALTMOUNT_BENCH_OUT")
	if path == "" {
		return
	}
	benchResultMu.Lock()
	r := benchResultV
	benchResultMu.Unlock()
	if r == nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err == nil {
		_ = streambench.Save(path, r)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	flushBenchResult()
	os.Exit(code)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func mbps(bytes int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(bytes) / (1 << 20) / d.Seconds()
}
