package nzbfilesystem

import (
	"context"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/streambench"
	"github.com/kipsilabs/altmount/internal/testsupport/nntpserver"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
)

// Streaming benchmarks over the real MetadataVirtualFile → UsenetReader →
// pool.Manager → nntppool stack against the simulated provider. Run with
// `make bench-stream`; results land in bench/results/<sha>.json and are
// compared by `make bench-compare BASE=<sha>`.
const (
	benchFileSegs = 400
	benchPrefetch = 60 // config default streaming.max_prefetch
	benchSeqBytes = 200 << 20
	// benchStartupBytes separates a stream's opening phase from steady state.
	benchStartupBytes = 16 << 20
	benchSeekReads    = 20
	benchSeekSize     = 64 << 10
	benchColdOpens    = 20
	benchPauseSleep   = 5 * time.Second
)

var benchProfiles = []benchProfile{profilePremium750K, profileSlow4M}

func scenario(name string, p benchProfile) string { return name + "/" + p.Name }

// benchReps is how many times each scenario runs; the recorded value is the
// per-metric median, which keeps one noisy run from moving the gate.
func benchReps() int {
	if v, err := strconv.Atoi(os.Getenv("ALTMOUNT_BENCH_REPS")); err == nil && v > 0 {
		return v
	}
	return 3
}

// record runs body benchReps times against one harness and stores the median
// of each metric under the scenario name.
func record(b *testing.B, name string, body func() []streambench.Metric) {
	runs := make([][]streambench.Metric, 0, benchReps())
	for i := 0; i < benchReps(); i++ {
		runs = append(runs, body())
	}
	med := streambench.Median(runs)
	benchResult().Add(name, med...)
	for _, m := range med {
		b.ReportMetric(m.Value, m.Name+"_"+m.Unit)
	}
}

func info(m streambench.Metric) streambench.Metric { m.Informational = true; return m }

// awaitQuietWire waits until the provider has sent nothing for quiet, so a
// measurement does not include read-ahead that was already committed.
func (h *benchHarness) awaitQuietWire(quiet, max time.Duration) {
	deadline := time.Now().Add(max)
	last := h.bytesWritten()
	for time.Now().Before(deadline) {
		time.Sleep(quiet)
		now := h.bytesWritten()
		if now == last {
			return
		}
		last = now
	}
}

// BenchmarkStreamColdOpen measures time to first byte on a fresh handle
// reading the first 1 MB, the way a player opens a file.
func BenchmarkStreamColdOpen(b *testing.B) {
	for _, p := range benchProfiles {
		b.Run(p.Name, func(b *testing.B) {
			h := newBenchHarness(b, p)
			record(b, scenario("B1-cold-open", p), func() []streambench.Metric {
				var ttfb streambench.Samples
				buf := make([]byte, 1<<20)
				before := h.bodies()
				for i := 0; i < benchColdOpens; i++ {
					mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
					start := time.Now()
					n, err := mvf.ReadAt(buf[:1], 0)
					if err != nil || n != 1 {
						b.Fatalf("first byte: n=%d err=%v", n, err)
					}
					ttfb.Add(time.Since(start))
					if _, err := io.ReadFull(io.NewSectionReader(mvf, 1, int64(len(buf)-1)), buf[1:]); err != nil {
						b.Fatalf("first MB: %v", err)
					}
					_ = mvf.Close()
				}
				h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
				articles := float64(h.bodies()-before) / benchColdOpens
				return []streambench.Metric{
					// Same-code runs spread about 7 % on the mean (the demand article
					// lands behind a varying number of speculative ones), so allow 10 %.
					{Name: "ttfb_mean", Unit: "ms", Value: ms(ttfb.Mean()), Tolerance: 0.10},
					info(streambench.Metric{Name: "ttfb_p50", Unit: "ms", Value: ms(ttfb.P(0.5))}),
					info(streambench.Metric{Name: "ttfb_p99", Unit: "ms", Value: ms(ttfb.P(0.99))}),
					info(streambench.Metric{Name: "articles", Unit: "count", Value: articles}),
				}
			})
		})
	}
}

// BenchmarkStreamSequential reads 200 MB sequentially through ReadAt in
// 1 MB chunks and reports throughput and how many bytes the provider sent
// per byte the reader consumed (1.0 is no waste).
func BenchmarkStreamSequential(b *testing.B) {
	for _, p := range benchProfiles {
		b.Run(p.Name, func(b *testing.B) {
			h := newBenchHarness(b, p)
			record(b, scenario("B2-sequential", p), func() []streambench.Metric {
				mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
				buf := make([]byte, 1<<20)
				beforeBytes := h.bytesWritten()
				start := time.Now()
				var read int64
				var startupDone time.Time
				// Read well past the prefetch window so steady state is the wire,
				// not buffered bytes: 200 MB on 750 KB articles, 600 MB on 4 MiB.
				seqBytes := int64(200 << 20)
				if p.ArticleSize >= 4<<20 {
					seqBytes = 600 << 20
				}
				for read < seqBytes {
					n, err := mvf.ReadAt(buf, read)
					if err != nil && err != io.EOF {
						b.Fatalf("ReadAt(%d): %v", read, err)
					}
					read += int64(n)
					if read >= benchStartupBytes && startupDone.IsZero() {
						startupDone = time.Now()
					}
					if err == io.EOF {
						break
					}
				}
				elapsed := time.Since(start)
				_ = mvf.Close()
				h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
				fetched := h.bytesWritten() - beforeBytes
				// Startup is the time to the first 16 MB, where a fresh reader's
				// window is still opening; steady state is everything after it,
				// which is what sustained playback runs at.
				startup := startupDone.Sub(start)
				return []streambench.Metric{
					{Name: "startup_ms", Unit: "ms", Value: ms(startup), Tolerance: 0.10},
					{Name: "throughput", Unit: "MB/s", Value: mbps(read-benchStartupBytes, elapsed-startup), HigherIsBetter: true},
					info(streambench.Metric{Name: "throughput_total", Unit: "MB/s", Value: mbps(read, elapsed), HigherIsBetter: true}),
					{Name: "waste_ratio", Unit: "ratio", Value: float64(fetched) / float64(read)},
				}
			})
		})
	}
}

// BenchmarkStreamSeekStorm models a media server probing a file: 20 random
// 64 KB reads, each far from the last. The interesting number is how many
// articles each probe drags in beyond the one it needs.
func BenchmarkStreamSeekStorm(b *testing.B) {
	for _, p := range benchProfiles {
		b.Run(p.Name, func(b *testing.B) {
			h := newBenchHarness(b, p)
			fileSize := int64(benchFileSegs) * int64(p.ArticleSize)
			record(b, scenario("B3-seek-storm", p), func() []streambench.Metric {
				mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
				rng := rand.New(rand.NewPCG(1, 2))
				buf := make([]byte, benchSeekSize)
				var lat streambench.Samples
				before := h.bodies()
				for i := 0; i < benchSeekReads; i++ {
					off := rng.Int64N(fileSize - benchSeekSize)
					start := time.Now()
					if _, err := mvf.ReadAt(buf, off); err != nil {
						b.Fatalf("ReadAt(%d): %v", off, err)
					}
					lat.Add(time.Since(start))
				}
				_ = mvf.Close()
				h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
				return []streambench.Metric{
					{Name: "read_p50", Unit: "ms", Value: ms(lat.P(0.5))},
					info(streambench.Metric{Name: "read_p99", Unit: "ms", Value: ms(lat.P(0.99))}),
					{Name: "articles_per_read", Unit: "count", Value: float64(h.bodies()-before) / benchSeekReads},
				}
			})
		})
	}
}

// BenchmarkStreamPauseResume reads 20 MB, pauses like a player whose buffer
// has filled, and reports how much the provider sent during the pause.
func BenchmarkStreamPauseResume(b *testing.B) {
	for _, p := range benchProfiles {
		b.Run(p.Name, func(b *testing.B) {
			h := newBenchHarness(b, p)
			record(b, scenario("B8-pause-resume", p), func() []streambench.Metric {
				mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
				buf := make([]byte, 1<<20)
				var off int64
				for off < 20<<20 {
					n, err := mvf.ReadAt(buf, off)
					if err != nil {
						b.Fatalf("ReadAt: %v", err)
					}
					off += int64(n)
				}
				// Read-ahead committed at the pause keeps landing for a while;
				// measure only once the wire has gone quiet so the number is what
				// keeps flowing during a pause, not what was already in flight.
				h.awaitQuietWire(time.Second, 30*time.Second)
				before := h.bytesWritten()
				time.Sleep(benchPauseSleep)
				during := h.bytesWritten() - before
				_ = mvf.Close()
				return []streambench.Metric{{Name: "bytes_during_pause", Unit: "MB", Value: float64(during) / (1 << 20)}}
			})
		})
	}
}

// BenchmarkStreamFourConcurrent runs four players on one 50-connection
// pool and reports the fairness spread and tail stall.
func BenchmarkStreamFourConcurrent(b *testing.B) {
	p := profilePremium750K
	h := newBenchHarness(b, p)
	const streams = 4
	h.streams.n = streams
	record(b, scenario("B4-four-streams", p), func() []streambench.Metric {
		var wg sync.WaitGroup
		var stalls streambench.Samples
		rates := make([]float64, streams)
		for s := 0; s < streams; s++ {
			wg.Add(1)
			go func(s int) {
				defer wg.Done()
				mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
				buf := make([]byte, 1<<20)
				start := time.Now()
				var off int64
				for off < 100<<20 {
					t0 := time.Now()
					n, err := mvf.ReadAt(buf, off)
					if err != nil {
						b.Errorf("stream %d ReadAt: %v", s, err)
						return
					}
					stalls.Add(time.Since(t0))
					off += int64(n)
				}
				rates[s] = mbps(off, time.Since(start))
			}(s)
		}
		wg.Wait()
		minR, maxR := rates[0], rates[0]
		for _, r := range rates[1:] {
			minR = math.Min(minR, r)
			maxR = math.Max(maxR, r)
		}
		h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
		return []streambench.Metric{
			// Four racing streams settle differently every run; measured spread on
			// an idle machine is about 8 %, so the gate allows 12 %.
			{Name: "min_stream_mbps", Unit: "MB/s", Value: minR, HigherIsBetter: true, Tolerance: 0.12},
			{Name: "max_stream_mbps", Unit: "MB/s", Value: maxR, HigherIsBetter: true},
			info(streambench.Metric{Name: "stall_p99", Unit: "ms", Value: ms(stalls.P(0.99))}),
		}
	})
}

// BenchmarkStreamTwoHandles reads the same 50 MB through two handles in
// lockstep and counts how many BODY commands the provider saw beyond the
// number of distinct articles plus one read-ahead window.
func BenchmarkStreamTwoHandles(b *testing.B) {
	p := profilePremium750K
	h := newBenchHarness(b, p)
	const span = 50 << 20
	unique := (span + p.ArticleSize - 1) / p.ArticleSize
	record(b, scenario("B5-two-handles", p), func() []streambench.Metric {
		before := h.bodies()
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
				buf := make([]byte, 1<<20)
				var off int64
				for off < span {
					n, err := mvf.ReadAt(buf, off)
					if err != nil {
						b.Errorf("ReadAt: %v", err)
						return
					}
					off += int64(n)
				}
			}()
		}
		wg.Wait()
		h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
		dup := float64(h.bodies()-before) - float64(unique+benchPrefetch)
		if dup < 0 {
			dup = 0
		}
		return []streambench.Metric{{Name: "duplicate_bodies", Unit: "count", Value: dup}}
	})
}

// BenchmarkStreamUnderContention plays one stream while an importer
// saturates the normal lane with Body calls, the shape of a library scan
// during playback.
func BenchmarkStreamUnderContention(b *testing.B) {
	p := profilePremium750K
	h := newBenchHarness(b, p)
	h.streams.n = 1
	record(b, scenario("B6-contention", p), func() []streambench.Metric {
		ctx, cancel := context.WithCancel(h.ctx)
		defer cancel()

		var importBytes atomic.Int64
		var iwg sync.WaitGroup
		for w := 0; w < 160; w++ {
			iwg.Add(1)
			go func(w int) {
				defer iwg.Done()
				i := w
				for ctx.Err() == nil {
					release, err := h.mgr.AcquireImportConnection(ctx)
					if err != nil {
						return
					}
					body, err := h.client.Fetch(ctx, nntppool.Req{MessageID: segments.MessageID(100000 + i)})
					release()
					if err == nil {
						importBytes.Add(int64(len(body.Bytes)))
					}
					i += 160
				}
			}(w)
		}

		mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
		buf := make([]byte, 1<<20)
		var lat streambench.Samples
		start := time.Now()
		var off int64
		var startupDone time.Time
		for off < 100<<20 {
			t0 := time.Now()
			n, err := mvf.ReadAt(buf, off)
			if err != nil {
				b.Fatalf("ReadAt: %v", err)
			}
			lat.Add(time.Since(t0))
			off += int64(n)
			if off >= benchStartupBytes && startupDone.IsZero() {
				startupDone = time.Now()
			}
		}
		elapsed := time.Since(start)
		startup := startupDone.Sub(start)
		cancel()
		iwg.Wait()
		_ = mvf.Close()
		h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
		return []streambench.Metric{
			// Single-stream throughput while an import saturates the normal lane.
			// Read-ahead must keep its edge over import. Same-code runs on a
			// saturated simulated link spread 226-252 MB/s, so allow 15 %.
			{Name: "stream_mbps", Unit: "MB/s", Value: mbps(off-benchStartupBytes, elapsed-startup), HigherIsBetter: true, Tolerance: 0.15},
			info(streambench.Metric{Name: "stream_startup_ms", Unit: "ms", Value: ms(startup)}),
			info(streambench.Metric{Name: "stream_p50", Unit: "ms", Value: ms(lat.P(0.5))}),
			info(streambench.Metric{Name: "stream_p99", Unit: "ms", Value: ms(lat.P(0.99))}),
			{Name: "import_mbps", Unit: "MB/s", Value: mbps(importBytes.Load(), elapsed), HigherIsBetter: true},
		}
	})
}

// BenchmarkStreamFailover has provider A missing every tenth article and
// provider B complete. Each miss is read on a fresh handle so the number is
// the cost of the miss itself: the 430, the failover, and the body from B.
func BenchmarkStreamFailover(b *testing.B) {
	p := profilePremium750K
	missing := map[string]struct{}{}
	for i := 0; i < benchFileSegs; i += 10 {
		missing[segments.MessageID(i)] = struct{}{}
	}
	h := newBenchHarness(b, p, nntpserver.Config{Missing: missing}, nntpserver.Config{})
	record(b, scenario("B7-failover", p), func() []streambench.Metric {
		buf := make([]byte, 1)
		var missLat streambench.Samples
		before := h.bodies()
		for i := 0; i < benchFileSegs; i += 10 {
			mvf := h.openFile(b, benchFileSegs, 1, -1)
			off := int64(i) * int64(p.ArticleSize)
			start := time.Now()
			if _, err := mvf.ReadAt(buf, off); err != nil {
				b.Fatalf("ReadAt(%d): %v", off, err)
			}
			missLat.Add(time.Since(start))
			_ = mvf.Close()
		}
		h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
		return []streambench.Metric{
			// Three round trips at 40 ms with 10 ms jitter each; measured
			// run-to-run spread is about 6 %, so the gate allows 10 %.
			{Name: "miss_ttfb_mean", Unit: "ms", Value: ms(missLat.Mean()), Tolerance: 0.10},
			info(streambench.Metric{Name: "miss_ttfb_p50", Unit: "ms", Value: ms(missLat.P(0.5))}),
			info(streambench.Metric{Name: "bodies_per_miss", Unit: "count", Value: float64(h.bodies()-before) / float64(missLat.Count())}),
		}
	})
}

// BenchmarkStreamForwardSkip models "skip intro": read the head of a file,
// jump 100 MB forward, read on. It reports the latency of the jump read and
// how many articles the whole scenario fetched; a jump beyond the buffered
// window should reopen at the target rather than drain the gap.
func BenchmarkStreamForwardSkip(b *testing.B) {
	p := profilePremium750K
	h := newBenchHarness(b, p)
	record(b, scenario("B9-forward-skip", p), func() []streambench.Metric {
		before := h.bodies()
		mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
		buf := make([]byte, 1<<20)
		var off int64
		for off < 2<<20 {
			n, err := mvf.ReadAt(buf, off)
			if err != nil {
				b.Fatalf("head ReadAt: %v", err)
			}
			off += int64(n)
		}
		h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
		openArticles := float64(h.bodies() - before)

		jump := off + 100<<20
		start := time.Now()
		if _, err := mvf.ReadAt(buf, jump); err != nil {
			b.Fatalf("jump ReadAt: %v", err)
		}
		jumpLat := time.Since(start)
		for off = jump + int64(len(buf)); off < jump+2<<20; {
			n, err := mvf.ReadAt(buf, off)
			if err != nil {
				b.Fatalf("post-jump ReadAt: %v", err)
			}
			off += int64(n)
		}
		_ = mvf.Close()
		h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
		return []streambench.Metric{
			{Name: "jump_read_ms", Unit: "ms", Value: ms(jumpLat), Tolerance: 0.10},
			{Name: "articles", Unit: "count", Value: float64(h.bodies() - before)},
			info(streambench.Metric{Name: "open_articles", Unit: "count", Value: openArticles}),
		}
	})
}

// BenchmarkStreamReplay reads the head of a file, closes it, and reads the
// same head again through a fresh handle: the media-server pattern of a
// probe followed by playback. With a memory tier the second pass should
// fetch nothing.
func BenchmarkStreamReplay(b *testing.B) {
	p := profilePremium750K
	h := newBenchHarness(b, p)
	h.store = benchReplayStore()
	const head = 32 << 20
	readHead := func() time.Duration {
		mvf := h.openFile(b, benchFileSegs, benchPrefetch, -1)
		buf := make([]byte, 1<<20)
		start := time.Now()
		var off int64
		for off < head {
			n, err := mvf.ReadAt(buf, off)
			if err != nil {
				b.Fatalf("ReadAt: %v", err)
			}
			off += int64(n)
		}
		d := time.Since(start)
		_ = mvf.Close()
		h.awaitQuietWire(500*time.Millisecond, 30*time.Second)
		return d
	}
	record(b, scenario("B10-replay", p), func() []streambench.Metric {
		readHead()
		before := h.bytesWritten()
		replay := readHead()
		fetched := float64(h.bytesWritten()-before) / (1 << 20)
		return []streambench.Metric{
			{Name: "replay_bytes_fetched_mb", Unit: "MB", Value: fetched},
			{Name: "replay_read_ms", Unit: "ms", Value: ms(replay), Tolerance: 0.10},
		}
	})
}

// BenchmarkStreamSeekAndResume models a player jumping through a file: read
// 8 MB, jump 500 MB forward, read 8 MB, five times. Each jump abandons the
// previous reader's read-ahead; the bytes still fetched after that are the
// cost of the abandoned window.
func BenchmarkStreamSeekAndResume(b *testing.B) {
	for _, p := range benchProfiles {
		b.Run(p.Name, func(b *testing.B) {
			h := newBenchHarness(b, p)
			fileSegs := int(3<<30) / p.ArticleSize // ~3 GB so five 500 MB jumps fit
			record(b, scenario("B11-seek-resume", p), func() []streambench.Metric {
				before := h.bytesWritten()
				mvf := h.openFile(b, fileSegs, benchPrefetch, -1)
				buf := make([]byte, 1<<20)
				readChunk := func(at int64) time.Duration {
					start := time.Now()
					for off := at; off < at+8<<20; {
						n, err := mvf.ReadAt(buf, off)
						if err != nil {
							b.Fatalf("ReadAt(%d): %v", off, err)
						}
						off += int64(n)
					}
					return time.Since(start)
				}
				readChunk(0)
				var seek streambench.Samples
				pos := int64(0)
				for i := 0; i < 5; i++ {
					pos += 500 << 20
					seek.Add(readChunk(pos))
				}
				_ = mvf.Close()
				h.awaitQuietWire(500*time.Millisecond, 60*time.Second)
				return []streambench.Metric{
					{Name: "seek_read_ms", Unit: "ms", Value: ms(seek.Mean()), Tolerance: 0.10},
					{Name: "bytes_fetched_mb", Unit: "MB", Value: float64(h.bytesWritten()-before) / (1 << 20)},
				}
			})
		})
	}
}
