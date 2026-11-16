package contentid

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// old writes one file in the older metadata format: segments inline, no store
// reference, the source NZB path set.
func (f *fixture) old(ctx context.Context, p, src string, ids ...string) {
	f.t.Helper()
	m := &metapb.FileMetadata{FileSize: 10, SegmentData: sd(ids...), SourceNzbPath: src, Status: metapb.FileStatus_FILE_STATUS_HEALTHY}
	require.NoError(f.t, f.ms.WriteFileMetadata(p, m))
}

func (f *fixture) row(ctx context.Context, p string) database.PeernetContentEntry {
	f.t.Helper()
	e, ok, err := f.repo.GetByPath(ctx, p)
	require.NoError(f.t, err)
	require.True(f.t, ok, p)
	return e
}

func (f *fixture) fileID(p string) ID {
	f.t.Helper()
	m, err := f.ms.ReadFileMetadata(p)
	require.NoError(f.t, err)
	return mustFile(f.t, m)
}

func (f *fixture) wantFilesID(paths ...string) string {
	f.t.Helper()
	ids := make([]ID, len(paths))
	for i, p := range paths {
		ids[i] = f.fileID(p)
	}
	rid, err := ReleaseIDFromFiles(ids)
	require.NoError(f.t, err)
	return rid.String()
}

type orphans struct {
	mu  sync.Mutex
	ids map[string]bool
}

func (o *orphans) handle(_ context.Context, ids []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ids == nil {
		o.ids = map[string]bool{}
	}
	for _, id := range ids {
		o.ids[id] = true
	}
}

func (o *orphans) list() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []string
	for id := range o.ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

const (
	srcA = "/data/.nzbs/1/a.nzb.gz"
	srcB = "/data/.nzbs/2/b.nzb.gz"
	srcC = "/data/.nzbs/3/c.nzb.gz"
)

func indexAll(ctx context.Context, t *testing.T, f *fixture) DeriveResult {
	t.Helper()
	_, err := f.idx.Backfill(ctx)
	require.NoError(t, err)
	res, err := f.idx.DeriveAll(ctx)
	require.NoError(t, err)
	return res
}

// Files of one import are grouped by their source NZB, never by folder: a
// folder can hold several imports, and one import can span folders.
func TestDerive_GroupsByImportNotFolder(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// Import A spans two folders (release folder and a nested sample folder).
	f.old(ctx, "cat/RelA/a.mkv", srcA, "a1", "a2")
	f.old(ctx, "cat/RelA/Sample/s.mkv", srcA, "s1")
	// Imports B and C share one folder (a season folder with one NZB per episode).
	f.old(ctx, "cat/Season/e1.mkv", srcB, "b1", "b2")
	f.old(ctx, "cat/Season/e2.mkv", srcC, "c1")
	// A single file directly in the category folder next to unrelated files.
	f.old(ctx, "cat/lonely.mkv", "/data/.nzbs/9/lonely.nzb.gz", "l1")

	res := indexAll(ctx, t, f)
	assert.Equal(t, 4, res.Groups)

	relA := f.wantFilesID("cat/RelA/a.mkv", "cat/RelA/Sample/s.mkv")
	assert.Equal(t, relA, f.row(ctx, "cat/RelA/a.mkv").ReleaseID)
	assert.Equal(t, relA, f.row(ctx, "cat/RelA/Sample/s.mkv").ReleaseID)
	assert.Equal(t, f.wantFilesID("cat/Season/e1.mkv"), f.row(ctx, "cat/Season/e1.mkv").ReleaseID)
	assert.Equal(t, f.wantFilesID("cat/Season/e2.mkv"), f.row(ctx, "cat/Season/e2.mkv").ReleaseID)
	assert.NotEqual(t, f.row(ctx, "cat/Season/e1.mkv").ReleaseID, f.row(ctx, "cat/Season/e2.mkv").ReleaseID)
	lonely := f.row(ctx, "cat/lonely.mkv")
	assert.Equal(t, f.wantFilesID("cat/lonely.mkv"), lonely.ReleaseID, "a single file is a release of one file")
	for _, p := range []string{"cat/RelA/a.mkv", "cat/Season/e1.mkv", "cat/lonely.mkv"} {
		assert.Equal(t, database.PeernetSchemeFiles, f.row(ctx, p).ReleaseScheme)
	}
}

// Two nodes that imported the same NZB and kept the same files derive the same
// ID, whatever their local paths and folder names are.
func TestDerive_StableAcrossNodes(t *testing.T) {
	ctx := context.Background()
	f1, f2 := newFixture(t), newFixture(t)
	f1.old(ctx, "movies/Name/x.mkv", srcA, "p1", "p2")
	f1.old(ctx, "movies/Name/y.nfo", srcA, "q1")
	f2.old(ctx, "other/Renamed/y.nfo", "/elsewhere/.nzbs/77/z.nzb.gz", "q1")
	f2.old(ctx, "other/Renamed/x.mkv", "/elsewhere/.nzbs/77/z.nzb.gz", "p1", "p2")
	indexAll(ctx, t, f1)
	indexAll(ctx, t, f2)
	assert.Equal(t, f1.row(ctx, "movies/Name/x.mkv").ReleaseID, f2.row(ctx, "other/Renamed/x.mkv").ReleaseID)
	assert.NotEmpty(t, f1.row(ctx, "movies/Name/x.mkv").ReleaseID)

	// A node that kept only a subset derives a different release ID, but the
	// same File IDs.
	f3 := newFixture(t)
	f3.old(ctx, "m/x.mkv", srcB, "p1", "p2")
	indexAll(ctx, t, f3)
	assert.NotEqual(t, f1.row(ctx, "movies/Name/x.mkv").ReleaseID, f3.row(ctx, "m/x.mkv").ReleaseID)
	assert.Equal(t, f1.row(ctx, "movies/Name/x.mkv").FileID, f3.row(ctx, "m/x.mkv").FileID)
}

func TestDerive_NoSourceMeansNoID(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.old(ctx, "cat/a.mkv", "", "a1")
	f.old(ctx, "cat/b.mkv", "   ", "b1")
	res := indexAll(ctx, t, f)
	assert.Equal(t, 0, res.Groups)
	assert.Empty(t, f.row(ctx, "cat/a.mkv").ReleaseID)
	assert.Empty(t, f.row(ctx, "cat/a.mkv").GroupKey)
}

// A store-based ID always wins; a file never has both.
func TestDerive_StoreBasedWinsAndMigratedStoreIsDerived(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	nzb := f.release(ctx, "good", []string{"g1"})[0]
	m, err := f.ms.ReadFileMetadata(nzb)
	require.NoError(t, err)
	m.SourceNzbPath = srcA
	require.NoError(t, f.ms.WriteFileMetadata(nzb, m))
	f.old(ctx, "good/inline.mkv", srcA, "g2") // an old-format sibling of the same import

	// A store the metadata migration rebuilt from the surviving files.
	mig := f.release(ctx, "_migrated/rel", []string{"m1"}, []string{"m2"})
	for _, p := range mig {
		mm, err := f.ms.ReadFileMetadata(p)
		require.NoError(t, err)
		mm.SourceNzbPath = srcB
		require.NoError(t, f.ms.WriteFileMetadata(p, mm))
	}
	indexAll(ctx, t, f)

	got := f.row(ctx, nzb)
	assert.Equal(t, database.PeernetSchemeNZB, got.ReleaseScheme)
	st, err := f.ms.Store().ReadStore(m.StoreRef)
	require.NoError(t, err)
	want, err := ReleaseID(st)
	require.NoError(t, err)
	assert.Equal(t, want.String(), got.ReleaseID)
	// The derived group is the old-format files only; it does not absorb the nzb-scheme file.
	assert.Equal(t, f.wantFilesID("good/inline.mkv"), f.row(ctx, "good/inline.mkv").ReleaseID)

	for _, p := range mig {
		r := f.row(ctx, p)
		assert.Equal(t, database.PeernetSchemeFiles, r.ReleaseScheme, "a rebuilt store must not give an nzb-scheme ID")
		assert.Equal(t, f.wantFilesID(mig...), r.ReleaseID)
	}
}

func TestDerive_GroupOverCapGetsNoID(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, WithMaxGroupFiles(3))
	for i := range 4 {
		f.old(ctx, fmt.Sprintf("big/f%d.mkv", i), srcA, fmt.Sprintf("big%d", i))
	}
	for i := range 3 {
		f.old(ctx, fmt.Sprintf("ok/f%d.mkv", i), srcB, fmt.Sprintf("ok%d", i))
	}
	res := indexAll(ctx, t, f)
	assert.Equal(t, 4, res.OversizeFiles)
	assert.Equal(t, 4, f.idx.OversizeFiles())
	assert.Empty(t, f.row(ctx, "big/f0.mkv").ReleaseID)
	assert.NotEmpty(t, f.row(ctx, "ok/f0.mkv").ReleaseID, "exactly at the cap is allowed")
}

// The default cap is the one release information can serve: a group above it
// must not be given an ID that publishing would then skip.
func TestDerive_DefaultCapIsTheReleaseInfoCap(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	const over = 2001
	for i := range over {
		f.old(ctx, fmt.Sprintf("big/f%d.mkv", i), srcA, fmt.Sprintf("big%d", i))
	}
	res := indexAll(ctx, t, f)
	assert.Equal(t, over, res.OversizeFiles)
	assert.Empty(t, f.row(ctx, "big/f0.mkv").ReleaseID)
}

// A group that grew past the cap loses its ID, and the old ID is withdrawn.
func TestDerive_GroupGrowsPastCapLosesID(t *testing.T) {
	ctx := context.Background()
	var o orphans
	f := newFixture(t, WithMaxGroupFiles(2), WithOrphanHandler(o.handle))
	f.old(ctx, "g/a.mkv", srcA, "a1")
	f.old(ctx, "g/b.mkv", srcA, "b1")
	indexAll(ctx, t, f)
	old := f.row(ctx, "g/a.mkv").ReleaseID
	require.NotEmpty(t, old)

	f.old(ctx, "g/c.mkv", srcA, "c1")
	require.NoError(t, f.idx.IndexPath(ctx, "g/c.mkv"))
	_, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.Empty(t, f.row(ctx, "g/a.mkv").ReleaseID)
	assert.Empty(t, f.row(ctx, "g/c.mkv").ReleaseID)
	assert.Equal(t, []string{old}, o.list())
}

func TestDerive_AddingAFileChangesIDAndWithdrawsOld(t *testing.T) {
	ctx := context.Background()
	var o orphans
	f := newFixture(t, WithOrphanHandler(o.handle))
	f.old(ctx, "g/a.mkv", srcA, "a1")
	indexAll(ctx, t, f)
	before := f.row(ctx, "g/a.mkv").ReleaseID

	f.old(ctx, "g/b.mkv", srcA, "b1")
	require.NoError(t, f.idx.IndexPath(ctx, "g/b.mkv"))
	assert.True(t, f.idx.HasDirty())
	_, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.False(t, f.idx.HasDirty())

	after := f.row(ctx, "g/a.mkv").ReleaseID
	assert.NotEqual(t, before, after)
	assert.Equal(t, after, f.row(ctx, "g/b.mkv").ReleaseID)
	assert.Equal(t, f.wantFilesID("g/a.mkv", "g/b.mkv"), after)
	assert.Equal(t, []string{before}, o.list(), "the superseded ID is reported so its records can be withdrawn")
}

func TestDerive_RemovingFilesChangesIDAndLastFileWithdraws(t *testing.T) {
	ctx := context.Background()
	var o orphans
	f := newFixture(t, WithOrphanHandler(o.handle))
	f.ms.SetFileObserver(NewObserver(f.idx))
	f.old(ctx, "g/a.mkv", srcA, "a1")
	f.old(ctx, "g/b.mkv", srcA, "b1")
	indexAll(ctx, t, f)
	both := f.row(ctx, "g/a.mkv").ReleaseID

	require.NoError(t, f.ms.DeleteFileMetadata(ctx, "g/b.mkv"))
	_, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	one := f.row(ctx, "g/a.mkv").ReleaseID
	assert.NotEqual(t, both, one)
	assert.Equal(t, f.wantFilesID("g/a.mkv"), one)
	assert.Equal(t, []string{both}, o.list())

	require.NoError(t, f.ms.DeleteDirectory("g"))
	_, err = f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{both, one}, o.list(), "deleting the last file withdraws its ID too")
}

func TestDerive_RenameKeepsIDAndWithdrawsNothing(t *testing.T) {
	ctx := context.Background()
	var o orphans
	f := newFixture(t, WithOrphanHandler(o.handle))
	f.ms.SetFileObserver(NewObserver(f.idx))
	f.old(ctx, "old/a.mkv", srcA, "a1")
	f.old(ctx, "old/b.mkv", srcA, "b1")
	indexAll(ctx, t, f)
	before := f.row(ctx, "old/a.mkv").ReleaseID
	require.NoError(t, f.ms.RenameDirectory("old", "new"))
	_, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	_, err = f.idx.Reconcile(ctx)
	require.NoError(t, err)
	_, err = f.idx.DeriveAll(ctx)
	require.NoError(t, err)
	assert.Equal(t, before, f.row(ctx, "new/a.mkv").ReleaseID)
	assert.Empty(t, o.list())
}

// A metadata rewrite that changes nothing about identity (status, holes, a
// health check) must not touch the row, so a derived ID never flickers and the
// write path never marks a group dirty for nothing.
func TestDerive_IdentityNeutralRewriteDoesNotChurn(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.old(ctx, "g/a.mkv", srcA, "a1")
	indexAll(ctx, t, f)
	before := f.row(ctx, "g/a.mkv")
	require.NoError(t, f.ms.UpdateFileStatus("g/a.mkv", metapb.FileStatus_FILE_STATUS_CORRUPTED))
	require.NoError(t, f.idx.IndexPath(ctx, "g/a.mkv"))
	assert.False(t, f.idx.HasDirty())
	assert.Equal(t, before, f.row(ctx, "g/a.mkv"))
}

func TestDerive_FileGainsStoreBecomesNZBAndOldIDWithdrawn(t *testing.T) {
	ctx := context.Background()
	var o orphans
	f := newFixture(t, WithOrphanHandler(o.handle))
	f.old(ctx, "g/a.mkv", srcA, "a1")
	indexAll(ctx, t, f)
	derived := f.row(ctx, "g/a.mkv").ReleaseID

	p := f.release(ctx, "g2", []string{"a1"})[0] // same segments, now with a store
	m, err := f.ms.ReadFileMetadata(p)
	require.NoError(t, err)
	m.SourceNzbPath = srcA
	require.NoError(t, f.ms.WriteFileMetadata("g/a.mkv", m))
	require.NoError(t, f.idx.IndexPath(ctx, "g/a.mkv"))
	_, err = f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	got := f.row(ctx, "g/a.mkv")
	assert.Equal(t, database.PeernetSchemeNZB, got.ReleaseScheme)
	assert.NotEqual(t, derived, got.ReleaseID)
	assert.Equal(t, []string{derived}, o.list())
}

// The pass is bounded (small batches), resumable and idempotent.
func TestDerive_BatchedCancellableResumable(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, WithBatchSize(2), WithDeriveBatch(3, 4))
	for g := range 10 {
		for k := range 3 {
			f.old(ctx, fmt.Sprintf("c/g%d/f%d.mkv", g, k), fmt.Sprintf("/d/.nzbs/%d/x.nzb.gz", g), fmt.Sprintf("s%d_%d", g, k))
		}
	}
	_, err := f.idx.Backfill(ctx)
	require.NoError(t, err)

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = f.idx.DeriveAll(cctx)
	require.ErrorIs(t, err, context.Canceled)

	res, err := f.idx.DeriveAll(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Groups)
	assert.Equal(t, 30, res.Assigned)
	res, err = f.idx.DeriveAll(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Assigned, "second pass changes nothing")
	for g := range 10 {
		var paths []string
		for k := range 3 {
			paths = append(paths, fmt.Sprintf("c/g%d/f%d.mkv", g, k))
		}
		assert.Equal(t, f.wantFilesID(paths...), f.row(ctx, paths[0]).ReleaseID)
	}
}

// Reconcile must leave derived rows alone and must upgrade rows written
// before the group key existed.
func TestReconcile_KeepsDerivedAndFillsGroupKeys(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.old(ctx, "g/a.mkv", srcA, "a1")
	f.old(ctx, "g/b.mkv", srcA, "b1")
	indexAll(ctx, t, f)
	derived := f.row(ctx, "g/a.mkv").ReleaseID
	res, err := f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Reindexed)
	assert.Equal(t, derived, f.row(ctx, "g/a.mkv").ReleaseID)

	// A row as the previous version left it: file ID only, no group key.
	e := f.row(ctx, "g/a.mkv")
	require.NoError(t, f.repo.Upsert(ctx, database.PeernetContentEntry{FilePath: e.FilePath, FileID: e.FileID}))
	require.NoError(t, f.repo.Upsert(ctx, database.PeernetContentEntry{FilePath: "g/b.mkv", FileID: f.row(ctx, "g/b.mkv").FileID}))
	res, err = f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Reindexed)
	_, err = f.idx.DeriveAll(ctx)
	require.NoError(t, err)
	assert.Equal(t, derived, f.row(ctx, "g/a.mkv").ReleaseID, "same files, same ID")
}

func TestDerive_ObserverEventsMarkOnlyTheirGroups(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	obs := NewObserver(f.idx)
	f.ms.SetFileObserver(obs)
	f.old(ctx, "g/a.mkv", srcA, "a1")
	f.old(ctx, "h/b.mkv", srcB, "b1")
	obs.FileWritten(ctx, "g/a.mkv")
	obs.FileWritten(ctx, "h/b.mkv")
	res, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Groups)
	assert.Equal(t, 2, res.Assigned)
	res, err = f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Groups, "nothing dirty")
}

func TestDerive_DirtyOverflowFallsBackToFullPass(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, WithMaxDirty(2))
	for g := range 4 {
		f.old(ctx, fmt.Sprintf("d/g%d.mkv", g), fmt.Sprintf("/d/.nzbs/%d/x.nzb.gz", g), fmt.Sprintf("z%d", g))
		require.NoError(t, f.idx.IndexPath(ctx, fmt.Sprintf("d/g%d.mkv", g)))
	}
	res, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Assigned, "too many dirty groups: a full pass covers them all")
}

// The same NZB imported into two folders gives two sets of rows with identical
// File IDs; the release is still the one file set.
func TestDerive_SameImportKeptUnderTwoFoldersIsOneRelease(t *testing.T) {
	ctx := context.Background()
	once, twice := newFixture(t), newFixture(t)
	once.old(ctx, "c/One/x.mkv", srcA, "p1", "p2")
	once.old(ctx, "c/One/y.nfo", srcA, "q1")
	for _, dir := range []string{"c/One", "c/Two"} {
		twice.old(ctx, dir+"/x.mkv", srcA, "p1", "p2")
		twice.old(ctx, dir+"/y.nfo", srcA, "q1")
	}
	indexAll(ctx, t, once)
	indexAll(ctx, t, twice)
	want := once.row(ctx, "c/One/x.mkv").ReleaseID
	require.NotEmpty(t, want)
	assert.Equal(t, want, twice.row(ctx, "c/One/x.mkv").ReleaseID)
	assert.Equal(t, want, twice.row(ctx, "c/Two/y.nfo").ReleaseID)
}

// A cancelled or failed full pass must not lose the work it took: the next
// DeriveDirty still has it.
func TestDerive_FullPassCancelledKeepsItsWork(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, WithMaxDirty(2))
	for g := range 4 {
		f.old(ctx, fmt.Sprintf("d/g%d.mkv", g), fmt.Sprintf("/d/.nzbs/%d/x.nzb.gz", g), fmt.Sprintf("z%d", g))
		require.NoError(t, f.idx.IndexPath(ctx, fmt.Sprintf("d/g%d.mkv", g)))
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := f.idx.DeriveDirty(cctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, f.idx.HasDirty(), "the full pass must be remembered after a cancel")
	res, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Assigned)
}

func TestDerive_DeriveAllCancelledKeepsDirtyWork(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.old(ctx, "d/g.mkv", "/d/.nzbs/1/x.nzb.gz", "z0")
	require.NoError(t, f.idx.IndexPath(ctx, "d/g.mkv"))
	require.True(t, f.idx.HasDirty())
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := f.idx.DeriveAll(cctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, f.idx.HasDirty(), "dirty groups taken by a cancelled DeriveAll must come back")
	res, err := f.idx.DeriveDirty(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Assigned)
}
