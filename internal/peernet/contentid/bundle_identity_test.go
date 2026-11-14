package contentid

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bundleFiles writes two files the way a bundle import does (no source NZB, no
// store) and records them in the index as verified, the way Accept does.
func bundleFiles(ctx context.Context, t *testing.T, f *fixture, scheme int, rel ID) []string {
	t.Helper()
	paths := []string{"peer/Rel/a.mkv", "peer/Rel/b.mkv"}
	f.old(ctx, paths[0], "", "pa1", "pa2")
	f.old(ctx, paths[1], "", "pb1")
	for _, p := range paths {
		require.NoError(t, f.repo.Upsert(ctx, VerifiedEntry(p, f.fileID(p), rel, scheme)))
	}
	return paths
}

func filesRel(f *fixture, paths ...string) ID {
	rid, err := ReleaseIDFromFiles([]ID{f.fileID(paths[0]), f.fileID(paths[1])})
	require.NoError(f.t, err)
	return rid
}

func TestVerifiedBundleRows_SurviveIndexReconcileAndDerive(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	paths := []string{"peer/Rel/a.mkv", "peer/Rel/b.mkv"}
	f.old(ctx, paths[0], "", "pa1", "pa2")
	f.old(ctx, paths[1], "", "pb1")
	rel := filesRel(f, paths...)
	bundleFiles(ctx, t, f, database.PeernetSchemeFiles, rel)

	row := f.row(ctx, paths[0])
	require.Equal(t, rel.String(), row.ReleaseID)
	require.Equal(t, database.PeernetSchemeFiles, row.ReleaseScheme)
	assert.NotEmpty(t, row.GroupKey, "a local group key derived from the release id")
	assert.True(t, IsVerifiedBundleRow(row))

	// The observer re-indexes on every metadata write: the verified row stays.
	for _, p := range paths {
		require.NoError(t, f.idx.IndexPath(ctx, p))
	}
	assert.Equal(t, row, f.row(ctx, paths[0]))

	// Restart: backfill + reconcile + derive keep it.
	_, err := f.idx.Backfill(ctx)
	require.NoError(t, err)
	res, err := f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Zero(t, res.Reindexed+res.Pruned, "reconcile must not drop a verified id")
	dr, err := f.idx.DeriveAll(ctx)
	require.NoError(t, err)
	assert.Zero(t, dr.Assigned, "the derived id equals the verified one: nothing to write")
	assert.Equal(t, row, f.row(ctx, paths[0]))
}

func TestVerifiedBundleRows_FilesSchemeChangesWhenAFileIsDeleted(t *testing.T) {
	ctx := context.Background()
	o := &orphans{}
	f := newFixture(t, WithOrphanHandler(o.handle))
	paths := []string{"peer/Rel/a.mkv", "peer/Rel/b.mkv"}
	f.old(ctx, paths[0], "", "pa1", "pa2")
	f.old(ctx, paths[1], "", "pb1")
	rel := filesRel(f, paths...)
	bundleFiles(ctx, t, f, database.PeernetSchemeFiles, rel)

	require.NoError(t, f.ms.DeleteFileMetadata(ctx, paths[1]))
	NewObserver(f.idx).FileDeleted(ctx, paths[1])
	_, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)

	left := f.row(ctx, paths[0])
	want, err := ReleaseIDFromFiles([]ID{f.fileID(paths[0])})
	require.NoError(t, err)
	assert.Equal(t, want.String(), left.ReleaseID, "exactly as for a local import: the group id follows its files")
	assert.False(t, IsVerifiedBundleRow(left), "now derived, no longer verified")
	assert.Equal(t, []string{rel.String()}, o.list(), "the old id is withdrawn")

	// A restart after that changes nothing.
	_, err = f.idx.Reconcile(ctx)
	require.NoError(t, err)
	dr, err := f.idx.DeriveAll(ctx)
	require.NoError(t, err)
	assert.Zero(t, dr.Assigned)
	assert.Equal(t, want.String(), f.row(ctx, paths[0]).ReleaseID)
}

func TestVerifiedBundleRows_NZBSchemeIDOutlivesADeletedFile(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	paths := []string{"peer/Rel/a.mkv", "peer/Rel/b.mkv"}
	f.old(ctx, paths[0], "", "pa1", "pa2")
	f.old(ctx, paths[1], "", "pb1")
	rel := f.fileID(paths[0]) // any 32 bytes stand in for the NZB-scheme release id
	bundleFiles(ctx, t, f, database.PeernetSchemeNZB, rel)

	_, err := f.idx.Backfill(ctx)
	require.NoError(t, err)
	res, err := f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Zero(t, res.Reindexed+res.Pruned)
	assert.Equal(t, rel.String(), f.row(ctx, paths[0]).ReleaseID)
	assert.Equal(t, database.PeernetSchemeNZB, f.row(ctx, paths[0]).ReleaseScheme)

	require.NoError(t, f.ms.DeleteFileMetadata(ctx, paths[1]))
	NewObserver(f.idx).FileDeleted(ctx, paths[1])
	_, err = f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.Equal(t, rel.String(), f.row(ctx, paths[0]).ReleaseID, "an NZB-scheme id names the NZB, not the file set")
}

func TestVerifiedBundleRows_AChangedFileIsReindexedNormally(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	paths := []string{"peer/Rel/a.mkv", "peer/Rel/b.mkv"}
	f.old(ctx, paths[0], "", "pa1", "pa2")
	f.old(ctx, paths[1], "", "pb1")
	bundleFiles(ctx, t, f, database.PeernetSchemeFiles, filesRel(f, paths...))
	f.old(ctx, paths[0], "", "other1") // the file's content identity changed
	res, err := f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Reindexed)
	assert.False(t, IsVerifiedBundleRow(f.row(ctx, paths[0])))
}
