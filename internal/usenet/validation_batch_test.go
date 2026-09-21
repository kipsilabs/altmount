package usenet

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingPoolManager is a pool.Manager whose GetPool always errors.
type failingPoolManager struct {
	validationTestPoolManager
}

func (m *failingPoolManager) GetPool() (pool.NntpClient, error) {
	return nil, errors.New("pool down (test)")
}

// statOrderClient wraps fakepool.Client and records the exact ID slice handed
// to StatMany, so tests can assert the round-robin interleave order.
type statOrderClient struct {
	*fakepool.Client
	gotIDs []string
}

func (c *statOrderClient) ExistsMany(ctx context.Context, messageIDs []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	c.gotIDs = append(c.gotIDs, messageIDs...)
	return c.Client.ExistsMany(ctx, messageIDs, opts)
}

func idList(prefix string, n int) []string {
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("%s-%d@test", prefix, i)
	}
	return ids
}

func TestValidateSegmentAvailabilityBatch(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		client := fakepool.New()
		mgr := &validationTestPoolManager{client: client}
		perFile := [][]string{idList("a", 3), idList("b", 2), idList("c", 4)}

		results, err := ValidateSegmentAvailabilityBatch(context.Background(), perFile, mgr, batchOpts(4))
		require.NoError(t, err)
		require.Len(t, results, 3)
		for i, r := range results {
			assert.Equal(t, 0, r.MissingCount, "file %d", i)
			assert.Empty(t, r.MissingIDs, "file %d", i)
			assert.Equal(t, len(perFile[i]), r.TotalChecked, "file %d", i)
		}
		assert.Equal(t, int64(9), client.StatCalls())
	})

	t.Run("one broken file isolated to its index", func(t *testing.T) {
		client := fakepool.New()
		broken := idList("bad", 3)
		for _, id := range broken {
			client.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
		}
		mgr := &validationTestPoolManager{client: client}
		perFile := [][]string{idList("good", 3), broken, idList("also-good", 2)}

		results, err := ValidateSegmentAvailabilityBatch(context.Background(), perFile, mgr, batchOpts(8))
		require.NoError(t, err)
		assert.Equal(t, 0, results[0].MissingCount)
		assert.Equal(t, 3, results[1].MissingCount)
		assert.ElementsMatch(t, broken, results[1].MissingIDs)
		assert.Equal(t, 0, results[2].MissingCount)
	})

	t.Run("empty file list yields zero result", func(t *testing.T) {
		client := fakepool.New()
		mgr := &validationTestPoolManager{client: client}
		perFile := [][]string{idList("a", 2), nil, idList("c", 1)}

		results, err := ValidateSegmentAvailabilityBatch(context.Background(), perFile, mgr, batchOpts(2))
		require.NoError(t, err)
		require.Len(t, results, 3)
		assert.Equal(t, 0, results[1].MissingCount)
		assert.Equal(t, 0, results[1].TotalChecked)
		assert.Equal(t, int64(3), client.StatCalls())
	})

	t.Run("missing IDs capped at 50 per file", func(t *testing.T) {
		client := fakepool.New()
		client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
		mgr := &validationTestPoolManager{client: client}
		perFile := [][]string{idList("m", 60)}

		results, err := ValidateSegmentAvailabilityBatch(context.Background(), perFile, mgr, batchOpts(8))
		require.NoError(t, err)
		assert.Equal(t, 60, results[0].MissingCount)
		assert.Len(t, results[0].MissingIDs, 50)
	})

	t.Run("duplicate ID across files attributed to both", func(t *testing.T) {
		client := fakepool.New()
		client.SetBehavior("dup@test", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
		mgr := &validationTestPoolManager{client: client}
		perFile := [][]string{{"dup@test", "a-1@test"}, {"dup@test"}}

		results, err := ValidateSegmentAvailabilityBatch(context.Background(), perFile, mgr, batchOpts(1))
		require.NoError(t, err)
		assert.Equal(t, 1, results[0].MissingCount)
		assert.Equal(t, 1, results[1].MissingCount)
	})
}

func TestValidateSegmentAvailabilityBatch_PoolUnavailable(t *testing.T) {
	mgr := &failingPoolManager{}
	_, err := ValidateSegmentAvailabilityBatch(context.Background(), [][]string{idList("a", 2)}, mgr, batchOpts(2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool unavailable")
}

// TestValidateSegmentAvailabilityBatch_EmptyInputSkipsPool proves an all-empty
// batch returns zero results without touching the pool: files that early-exit
// preparation must not fail on a down pool.
func TestValidateSegmentAvailabilityBatch_EmptyInputSkipsPool(t *testing.T) {
	mgr := &failingPoolManager{} // would error if the pool were acquired
	results, err := ValidateSegmentAvailabilityBatch(context.Background(), [][]string{nil, {}}, mgr, batchOpts(2))
	require.NoError(t, err)
	require.Len(t, results, 2)
}

// TestValidateSegmentAvailabilityBatch_Interleaves verifies IDs are dispatched
// round-robin across files (every file's first sample, then every file's
// second, …) so one file with many segments cannot serialize the sweep.
func TestValidateSegmentAvailabilityBatch_Interleaves(t *testing.T) {
	client := &statOrderClient{Client: fakepool.New()}
	mgr := &validationTestPoolManager{client: client}
	perFile := [][]string{
		{"a-0@test", "a-1@test", "a-2@test"},
		{"b-0@test", "b-1@test"},
		{"c-0@test"},
	}

	_, err := ValidateSegmentAvailabilityBatch(context.Background(), perFile, mgr, batchOpts(2))
	require.NoError(t, err)

	want := []string{"a-0@test", "b-0@test", "c-0@test", "a-1@test", "b-1@test", "a-2@test"}
	assert.Equal(t, want, client.gotIDs)
}
