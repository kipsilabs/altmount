package nntpserver

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
)

const testArticleSize = 64 * 1024

func newTestClient(t *testing.T, cfg Config, conns, inflight, statInflight int) (*Server, *nntppool.Client) {
	t.Helper()

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	client, err := nntppool.NewClient(ctx, []nntppool.Provider{{
		Factory:      srv.Dial,
		Auth:         nntppool.Auth{Username: "bench", Password: "bench"},
		Connections:  conns,
		Inflight:     inflight,
		StatInflight: statInflight,
		SkipPing:     true,
		IdleTimeout:  time.Hour,
	}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return srv, client
}

func TestStatHitAndMiss(t *testing.T) {
	missing := segments.MessageID(99)
	srv, client := newTestClient(t, Config{
		RTT:         time.Millisecond,
		ArticleSize: testArticleSize,
		Missing:     map[string]struct{}{missing: {}},
	}, 2, 2, 8)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := client.Exists(ctx, nntppool.Req{MessageID: segments.MessageID(1)}); err != nil {
		t.Fatalf("Stat hit: %v", err)
	}
	if _, err := client.Exists(ctx, nntppool.Req{MessageID: missing}); !errors.Is(err, nntppool.ErrArticleNotFound) {
		t.Fatalf("Stat miss: got %v, want ErrArticleNotFound", err)
	}

	if got := srv.Counters().StatMisses; got != 1 {
		t.Errorf("StatMisses = %d, want 1", got)
	}
}

func TestBodyRoundTrip(t *testing.T) {
	_, client := newTestClient(t, Config{
		RTT:              time.Millisecond,
		BandwidthPerConn: 64 * 1024 * 1024,
		ArticleSize:      testArticleSize,
	}, 2, 2, 8)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := client.Fetch(ctx, nntppool.Req{MessageID: segments.MessageID(1)})
	if err != nil {
		t.Fatalf("Body: %v", err)
	}

	want := segments.Payload(0, testArticleSize)
	if !bytes.Equal(body.Bytes, want) {
		t.Fatalf("body mismatch: got %d bytes, want %d bytes (equal prefix %d)",
			len(body.Bytes), len(want), commonPrefix(body.Bytes, want))
	}
}

// TestPipelinedRepliesStayOrdered is the property the whole harness rests on:
// many STATs outstanding on few connections, every reply matched to its own
// message-id. A server that answered out of order would cross the wires and
// nntppool's FIFO reader would pair replies with the wrong requests.
func TestPipelinedRepliesStayOrdered(t *testing.T) {
	const n = 400

	ids := make([]string, n)
	missing := make(map[string]struct{})
	for i := range ids {
		ids[i] = segments.MessageID(i)
		if i%3 == 0 {
			missing[ids[i]] = struct{}{}
		}
	}

	srv, client := newTestClient(t, Config{
		RTT:         5 * time.Millisecond,
		Jitter:      4 * time.Millisecond, // reply computation finishes out of order
		ArticleSize: testArticleSize,
		Missing:     missing,
	}, 2, 2, 64)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seen := make(map[string]bool, n)
	for res := range client.ExistsMany(ctx, ids, nntppool.ManyOptions{Concurrency: 128}) {
		if seen[res.MessageID] {
			t.Fatalf("duplicate result for %s", res.MessageID)
		}
		seen[res.MessageID] = true

		_, wantMissing := missing[res.MessageID]
		gotMissing := errors.Is(res.Err, nntppool.ErrArticleNotFound)
		if gotMissing != wantMissing {
			t.Fatalf("%s: missing=%v want %v (err=%v)", res.MessageID, gotMissing, wantMissing, res.Err)
		}
	}
	if len(seen) != n {
		t.Fatalf("got %d results, want %d", len(seen), n)
	}

	// With 2 connections and 128 dispatchers the server must have seen real
	// pipelining, otherwise the timing model is a lie.
	if peak := srv.Counters().PeakInflight; peak < 8 {
		t.Errorf("PeakInflight = %d, want >= 8 (pipelining did not happen)", peak)
	}
}

func TestBandwidthThrottleIsHonoured(t *testing.T) {
	const size = 256 * 1024
	const bw = 2 * 1024 * 1024 // 2 MB/s => ~128ms for 256 KiB

	_, client := newTestClient(t, Config{
		BandwidthPerConn: bw,
		ArticleSize:      size,
	}, 1, 1, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Warm the connection so dial/auth is not counted.
	if _, err := client.Fetch(ctx, nntppool.Req{MessageID: segments.MessageID(0)}); err != nil {
		t.Fatalf("warmup Body: %v", err)
	}

	start := time.Now()
	if _, err := client.Fetch(ctx, nntppool.Req{MessageID: segments.MessageID(1)}); err != nil {
		t.Fatalf("Body: %v", err)
	}
	elapsed := time.Since(start)

	// yEnc inflates ~2%, so the floor is a little over size/bw.
	floor := time.Duration(float64(size) / float64(bw) * float64(time.Second))
	if elapsed < floor {
		t.Errorf("transfer took %v, want at least %v", elapsed, floor)
	}
}

func commonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// TestAggregateBandwidthCaps pins the property that makes the harness model a real
// deployment: per-connection throttling alone lets N connections sum to N×rate,
// which no real link does. With an aggregate ceiling, total throughput across all
// connections must not exceed it however many connections pull at once.
func TestAggregateBandwidthCaps(t *testing.T) {
	const (
		size    = 128 * 1024
		conns   = 8
		perConn = 16 * 1024 * 1024 // generous: the aggregate cap must be what binds
		aggBW   = 8 * 1024 * 1024  // 8 MB/s total
	)

	_, client := newTestClient(t, Config{
		BandwidthPerConn:   perConn,
		AggregateBandwidth: aggBW,
		ArticleSize:        size,
	}, conns, 2, 8)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Warm the connections so dial/auth is outside the measurement.
	if _, err := client.Fetch(ctx, nntppool.Req{MessageID: segments.MessageID(0)}); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	const fetches = 16
	start := time.Now()
	errs := make(chan error, fetches)
	for i := range fetches {
		go func() {
			_, err := client.Fetch(ctx, nntppool.Req{MessageID: segments.MessageID(i + 1)})
			errs <- err
		}()
	}
	for range fetches {
		if err := <-errs; err != nil {
			t.Fatalf("body: %v", err)
		}
	}
	elapsed := time.Since(start)

	// Without an aggregate cap these 16 bodies would ride 8 connections at
	// 16 MB/s each and finish almost instantly; the ceiling is what forces them
	// to take at least total/aggBW.
	floor := time.Duration(float64(fetches*size) / float64(aggBW) * float64(time.Second))
	if elapsed < floor {
		t.Errorf("moved %d bytes in %v; aggregate cap of %d B/s implies at least %v",
			fetches*size, elapsed, aggBW, floor)
	}
}
