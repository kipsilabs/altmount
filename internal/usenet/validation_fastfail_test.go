package usenet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func batchOpts(maxConns int) BatchOptions {
	return BatchOptions{MaxConnections: maxConns, Timeout: time.Second}
}

// TestBatch_ConfirmedMissingVsTransport is the core classification contract:
// only a 430/423 counts as a confirmed missing segment. Every other error is
// unresolved — the segment's reachability was never proven either way.
func TestBatch_ConfirmedMissingVsTransport(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		wantMissing    int
		wantUnresolved int
	}{
		{"article not found 430", nntppool.ErrArticleNotFound, 1, 0},
		{"article not found 423", &nntppool.Error{Code: 423, Message: "no article with that number"}, 1, 0},
		{"connection died", nntppool.ErrConnectionDied, 0, 1},
		{"max connections", nntppool.ErrMaxConnections, 0, 1},
		{"service unavailable", nntppool.ErrServiceUnavailable, 0, 1},
		{"auth required", nntppool.ErrAuthRequired, 0, 1},
		{"quota exceeded", nntppool.ErrQuotaExceeded, 0, 1},
		{"deadline exceeded", context.DeadlineExceeded, 0, 1},
		{"providers exhausted by transport", fmt.Errorf("nntp: all providers exhausted: %w", nntppool.ErrConnectionDied), 0, 1},
		{"providers exhausted by 430", fmt.Errorf("nntp: all providers exhausted: %w", nntppool.ErrArticleNotFound), 1, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fakepool.New()
			client.SetBehavior("x-0@test", fakepool.SegmentBehavior{Err: tc.err})
			mgr := &validationTestPoolManager{client: client}

			results, err := ValidateSegmentAvailabilityBatch(
				context.Background(), [][]string{{"x-0@test", "x-1@test"}}, mgr, batchOpts(2))

			require.NoError(t, err)
			assert.Equal(t, tc.wantMissing, results[0].MissingCount, "MissingCount")
			assert.Equal(t, tc.wantUnresolved, results[0].UnresolvedCount, "UnresolvedCount")
		})
	}
}

// TestBatch_TotalCheckedCountsResolvedOnly proves an unresolved segment is not
// counted as checked: TotalChecked feeds the projection denominator in
// health.classifyHoles, and a segment we learned nothing about must not dilute
// the observed miss rate.
func TestBatch_TotalCheckedCountsResolvedOnly(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("r-1@test", fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})
	client.SetBehavior("r-2@test", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	mgr := &validationTestPoolManager{client: client}

	results, err := ValidateSegmentAvailabilityBatch(
		context.Background(), [][]string{idList("r", 4)}, mgr, batchOpts(4))

	require.NoError(t, err)
	assert.Equal(t, 3, results[0].TotalChecked, "2 available + 1 confirmed missing")
	assert.Equal(t, 1, results[0].MissingCount)
	assert.Equal(t, 1, results[0].UnresolvedCount)
}

// TestBatch_UnreportedIDCountsUnresolved covers the silent-truncation hole:
// StatMany abandons undispatched ids when its deadline expires, so an id that
// never produces a result must not be mistaken for an available segment.
func TestBatch_UnreportedIDCountsUnresolved(t *testing.T) {
	client := fakepool.New()
	// Every stat blocks past the chunk deadline, so StatMany's own ctx fires
	// and most ids are abandoned without ever reporting.
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 200 * time.Millisecond})
	mgr := &validationTestPoolManager{client: client}

	results, err := ValidateSegmentAvailabilityBatch(
		context.Background(), [][]string{idList("u", 6)}, mgr,
		BatchOptions{MaxConnections: 2, Timeout: 10 * time.Millisecond})

	require.NoError(t, err)
	assert.Equal(t, 0, results[0].MissingCount, "a timeout is never a confirmed miss")
	assert.Equal(t, 6, results[0].UnresolvedCount, "every unreported id is unresolved")
	assert.Equal(t, 0, results[0].TotalChecked)
}

// TestBatch_FastFailStopsOneFileNotTheBatch is the feature's headline
// behaviour: a file whose confirmed misses irreversibly exceed its threshold
// stops consuming stats, while its batch siblings are swept in full.
func TestBatch_FastFailStopsOneFileNotTheBatch(t *testing.T) {
	client := fakepool.New()
	doomed := idList("doomed", 40)
	// Only the very first sample is missing; with zero tolerance that alone
	// settles the file.
	client.SetBehavior(doomed[0], fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	healthy := idList("healthy", 40)

	mgr := &validationTestPoolManager{client: client}
	opts := batchOpts(4)
	opts.ShouldStop = func(_ int, r ValidationResult) bool { return r.MissingCount > 0 }

	results, err := ValidateSegmentAvailabilityBatch(
		context.Background(), [][]string{doomed, healthy}, mgr, opts)

	require.NoError(t, err)
	assert.True(t, results[0].TerminatedEarly, "doomed file terminated early")
	assert.Equal(t, 1, results[0].MissingCount)
	assert.Less(t, results[0].TotalChecked, 40, "doomed file stopped short of a full sweep")

	assert.False(t, results[1].TerminatedEarly, "sibling ran to completion")
	assert.Equal(t, 40, results[1].TotalChecked, "sibling checked every segment")
	assert.Equal(t, 0, results[1].MissingCount)

	assert.Less(t, client.StatCalls(), int64(80), "fast-fail saved stat calls")
}

// TestBatch_NoShouldStopSweepsEverything guards the default: without a stop
// policy the sweep keeps its full-hole-map behaviour.
func TestBatch_NoShouldStopSweepsEverything(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	mgr := &validationTestPoolManager{client: client}

	results, err := ValidateSegmentAvailabilityBatch(
		context.Background(), [][]string{idList("all", 30)}, mgr, batchOpts(4))

	require.NoError(t, err)
	assert.False(t, results[0].TerminatedEarly)
	assert.Equal(t, 30, results[0].MissingCount)
	assert.Equal(t, int64(30), client.StatCalls())
}

// TestBatch_FastFailIgnoresTransportErrors is the safety property from the
// issue: a flaky connection must never condemn a file, no matter how many
// segments it affects.
func TestBatch_FastFailIgnoresTransportErrors(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})
	mgr := &validationTestPoolManager{client: client}

	opts := batchOpts(4)
	opts.ShouldStop = func(_ int, r ValidationResult) bool { return r.MissingCount > 0 }

	results, err := ValidateSegmentAvailabilityBatch(
		context.Background(), [][]string{idList("flaky", 30)}, mgr, opts)

	require.NoError(t, err)
	assert.False(t, results[0].TerminatedEarly, "transport errors never trigger fast-fail")
	assert.Equal(t, 30, results[0].UnresolvedCount)
	assert.Equal(t, int64(30), client.StatCalls(), "the whole file was still swept")
}

// TestBatch_ContextCancellationSurfaces proves an aborted sweep reports the
// cancellation instead of silently returning a clean-looking partial result.
func TestBatch_ContextCancellationSurfaces(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 20 * time.Millisecond})
	mgr := &validationTestPoolManager{client: client}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := ValidateSegmentAvailabilityBatch(ctx, [][]string{idList("c", 200)}, mgr, batchOpts(2))
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "got %v", err)
}

// chunkRecorder records the size of every StatMany chunk so tests can assert
// the sweep dispatches in bounded waves rather than one giant call.
type chunkRecorder struct {
	*fakepool.Client
	mu     sync.Mutex
	chunks []int
}

func (c *chunkRecorder) ExistsMany(ctx context.Context, ids []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	c.mu.Lock()
	c.chunks = append(c.chunks, len(ids))
	c.mu.Unlock()
	return c.Client.ExistsMany(ctx, ids, opts)
}

func (c *chunkRecorder) sizes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.chunks...)
}

// TestBatch_DispatchesInBoundedChunks pins the wave size to maxConnections,
// matching the import fast-fail sweep. The chunk boundary is what makes
// per-file termination possible at all.
func TestBatch_DispatchesInBoundedChunks(t *testing.T) {
	rec := &chunkRecorder{Client: fakepool.New()}
	mgr := &validationTestPoolManager{client: rec}

	_, err := ValidateSegmentAvailabilityBatch(
		context.Background(), [][]string{idList("k", 25)}, mgr, batchOpts(10))

	require.NoError(t, err)
	assert.Equal(t, []int{10, 10, 5}, rec.sizes())
}
