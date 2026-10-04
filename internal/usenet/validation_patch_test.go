package usenet

import (
	"context"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/require"
)

func TestBatchPatchedArticlesNeedNoPool(t *testing.T) {
	opts := batchOpts(1)
	opts.HasPatch = func(id string) bool { return id == "patched@test" }
	results, err := ValidateSegmentAvailabilityBatch(context.Background(), [][]string{{"patched@test"}, {"patched@test"}}, &failingPoolManager{}, opts)
	require.NoError(t, err)
	for _, result := range results {
		require.Equal(t, 1, result.TotalChecked)
		require.Zero(t, result.MissingCount)
		require.Zero(t, result.UnresolvedCount)
	}
}

func TestBatchPatchesPreserveMissingAndUnresolvedResults(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	client.SetBehavior("flaky@test", fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})
	opts := batchOpts(3)
	opts.HasPatch = func(id string) bool { return id == "patched@test" }
	results, err := ValidateSegmentAvailabilityBatch(context.Background(), [][]string{{"patched@test", "missing@test", "flaky@test"}}, &validationTestPoolManager{client: client}, opts)
	require.NoError(t, err)
	require.Equal(t, 2, results[0].TotalChecked)
	require.Equal(t, 1, results[0].MissingCount)
	require.Equal(t, []string{"missing@test"}, results[0].MissingIDs)
	require.Equal(t, 1, results[0].UnresolvedCount)
	require.Equal(t, int64(2), client.StatCalls(), "locally repaired articles should not consume provider requests")
}

// This client publishes the local repair between planning and the provider's
// response, exactly when a concurrent health sweep would otherwise requeue it.
type patchPublishingClient struct {
	*fakepool.Client
	patched bool
}

func (c *patchPublishingClient) ExistsMany(ctx context.Context, ids []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	c.patched = true
	return c.Client.ExistsMany(ctx, ids, opts)
}
func TestBatchRecognizesPatchPublishedDuringSweep(t *testing.T) {
	client := &patchPublishingClient{Client: fakepool.New()}
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	opts := batchOpts(1)
	opts.HasPatch = func(id string) bool { return client.patched && id == "patched@test" }
	results, err := ValidateSegmentAvailabilityBatch(context.Background(), [][]string{{"patched@test"}}, &validationTestPoolManager{client: client}, opts)
	require.NoError(t, err)
	require.Equal(t, 1, results[0].TotalChecked)
	require.Zero(t, results[0].MissingCount)
	require.Empty(t, results[0].MissingIDs)
}
