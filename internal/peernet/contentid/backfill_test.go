package contentid

import (
	"context"
	"fmt"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func populate(ctx context.Context, f *fixture, releases int) []string {
	var all []string
	for i := range releases {
		dir := fmt.Sprintf("movies/r%d", i)
		all = append(all, f.release(ctx, dir,
			[]string{fmt.Sprintf("r%da", i), fmt.Sprintf("r%db", i)},
			[]string{fmt.Sprintf("r%dc", i)})...)
	}
	return all
}

func TestBackfill_IndexesEverythingOnce(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, WithBatchSize(3))
	all := populate(ctx, f, 4) // 8 files, batches of 3
	// Directory that must not be walked.
	require.NoError(t, f.ms.WriteFileMetadata("corrupted_metadata/movies/x.mkv",
		&metapb.FileMetadata{FileSize: 1, SegmentData: sd("zz")}))
	// A file with nothing real to identify is skipped, not fatal.
	require.NoError(t, f.ms.WriteFileMetadata("movies/gaps.mkv",
		&metapb.FileMetadata{FileSize: 1, SegmentData: sd(holes.PlaceholderID(1, "s"))}))

	res, err := f.idx.Backfill(ctx)
	require.NoError(t, err)
	assert.Equal(t, 8, res.Indexed)
	assert.Equal(t, 1, res.Skipped)
	assert.Equal(t, 9, res.Scanned)

	missing, err := f.repo.MissingPaths(ctx, all)
	require.NoError(t, err)
	assert.Empty(t, missing)
	gone, err := f.repo.MissingPaths(ctx, []string{"corrupted_metadata/movies/x.mkv"})
	require.NoError(t, err)
	assert.Len(t, gone, 1)

	res, err = f.idx.Backfill(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Indexed, "second run finds nothing to do")
}

func TestBackfill_ResumesAfterCancel(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, WithBatchSize(2))
	all := populate(ctx, f, 3) // 6 files

	cctx, cancel := context.WithCancel(ctx)
	repo := &cancelAfterBatch{PeernetContentRepository: f.repo, cancel: cancel}
	idx := NewIndexer(NewServiceSource(f.ms), repo, WithBatchSize(2))

	res, err := idx.Backfill(cctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, res.Indexed, "first batch is flushed before stopping")

	missing, err := f.repo.MissingPaths(ctx, all)
	require.NoError(t, err)
	assert.Len(t, missing, 4)

	res, err = f.idx.Backfill(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Indexed, "resume only does the remainder")
	missing, err = f.repo.MissingPaths(ctx, all)
	require.NoError(t, err)
	assert.Empty(t, missing)
}

func TestBackfill_PreCancelled(t *testing.T) {
	f := newFixture(t)
	populate(context.Background(), f, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := f.idx.Backfill(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, res.Indexed)
}

func TestBackfill_RepoErrorStops(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	populate(ctx, f, 1)
	idx := NewIndexer(NewServiceSource(f.ms), failingRepo{})
	_, err := idx.Backfill(ctx)
	require.ErrorIs(t, err, errRepo)
}

func TestBackfill_EmptyTree(t *testing.T) {
	f := newFixture(t)
	res, err := f.idx.Backfill(context.Background())
	require.NoError(t, err)
	assert.Equal(t, BackfillResult{}, res)
}

func TestWithBatchSize_IgnoresInvalid(t *testing.T) {
	f := newFixture(t, WithBatchSize(0), WithBatchSize(-5))
	assert.Equal(t, defaultBatchSize, f.idx.batchSize)
}

type cancelAfterBatch struct {
	*database.PeernetContentRepository
	cancel func()
}

func (c *cancelAfterBatch) UpsertBatch(ctx context.Context, e []database.PeernetContentEntry) error {
	err := c.PeernetContentRepository.UpsertBatch(ctx, e)
	c.cancel()
	return err
}
