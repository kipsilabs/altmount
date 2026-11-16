package contentid

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixture struct {
	t    *testing.T
	ms   *metadata.MetadataService
	repo *database.PeernetContentRepository
	idx  *Indexer
	dir  string
}

func newFixture(t *testing.T, opts ...Option) *fixture {
	t.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := database.NewPeernetContentRepository(db.Connection(), database.DialectSQLite)
	dir := t.TempDir()
	ms := metadata.NewMetadataService(dir)
	return &fixture{t: t, ms: ms, repo: repo, idx: NewIndexer(NewServiceSource(ms), repo, opts...), dir: dir}
}

// release writes a store with one NZB file per ids entry and one virtual file
// per NZB file under dir, returning the virtual paths.
func (f *fixture) release(ctx context.Context, dir string, files ...[]string) []string {
	f.t.Helper()
	st := &metapb.NzbStore{}
	index := map[string]int64{}
	n := int64(0)
	for _, ids := range files {
		st.Files = append(st.Files, nzbFile(ids...))
		for _, id := range ids {
			index[id] = n
			n++
		}
	}
	ref := filepath.Join(f.dir, "stores", dir+".nzbz")
	require.NoError(f.t, f.ms.Store().WriteStore(ref, st))

	var paths []string
	for i, ids := range files {
		p := fmt.Sprintf("%s/file%d.mkv", dir, i)
		m := &metapb.FileMetadata{FileSize: 10, SegmentData: sd(ids...), Status: metapb.FileStatus_FILE_STATUS_HEALTHY}
		require.NoError(f.t, f.ms.WriteFileMetadataAuto(ctx, p, m, index, ref))
		paths = append(paths, p)
	}
	return paths
}

func TestIndexer_IndexPath(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	paths := f.release(ctx, "rel", []string{"a1", "a2"}, []string{"b1"})

	for _, p := range paths {
		require.NoError(t, f.idx.IndexPath(ctx, p))
	}

	m, err := f.ms.ReadFileMetadata(paths[0])
	require.NoError(t, err)
	wantFile, err := FileID(m)
	require.NoError(t, err)
	st, err := f.ms.Store().ReadStore(m.StoreRef)
	require.NoError(t, err)
	wantRel, err := ReleaseID(st)
	require.NoError(t, err)

	got, err := f.repo.FindByFileID(ctx, wantFile.String())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "rel/file0.mkv", got[0].FilePath)
	assert.Equal(t, wantRel.String(), got[0].ReleaseID)

	byRel, err := f.repo.FindByReleaseID(ctx, wantRel.String())
	require.NoError(t, err)
	assert.Len(t, byRel, 2, "both files of the release share the release id")
}

func TestIndexer_IDsIgnoreLocalState(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	p := f.release(ctx, "rel", []string{"a1", "a2"})[0]
	require.NoError(t, f.idx.IndexPath(ctx, p))
	before, err := f.idx.Entry(ctx, p)
	require.NoError(t, err)

	require.NoError(t, f.ms.UpdateFileStatus(p, metapb.FileStatus_FILE_STATUS_CORRUPTED))
	require.NoError(t, f.ms.UpdateFileMetadata(p, func(m *metapb.FileMetadata) {
		m.Password, m.Salt, m.AesKey = "pw", "salt", []byte{1, 2}
		m.KnownHoles = []*metapb.HoleRun{{StartSegment: 0, Count: 1, RecordedAt: 1}}
	}))
	after, err := f.idx.Entry(ctx, p)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestIndexer_Errors(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	require.ErrorIs(t, f.idx.IndexPath(ctx, "nope/x.mkv"), ErrMetadataNotFound)

	// Only gap placeholders: nothing real to identify.
	m := &metapb.FileMetadata{FileSize: 1, SegmentData: sd(holes.PlaceholderID(1, "s"))}
	require.NoError(t, f.ms.WriteFileMetadata("gap/x.mkv", m))
	require.ErrorIs(t, f.idx.IndexPath(ctx, "gap/x.mkv"), ErrNoSegments)

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, f.idx.IndexPath(cctx, "gap/x.mkv"), context.Canceled)
}

func TestIndexer_LegacyMetadataWithoutStore(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	m := &metapb.FileMetadata{FileSize: 1, SegmentData: sd("v1a", "v1b")}
	require.NoError(t, f.ms.WriteFileMetadata("old/x.mkv", m))
	e, err := f.idx.Entry(ctx, "old/x.mkv")
	require.NoError(t, err)
	assert.NotEmpty(t, e.FileID)
	assert.Empty(t, e.ReleaseID)
	require.NoError(t, f.idx.IndexPath(ctx, "old/x.mkv"))
}

func TestObserver_KeepsIndexInSync(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.ms.SetFileObserver(NewObserver(f.idx))

	paths := f.release(ctx, "rel", []string{"a1"}, []string{"b1"})
	missing, err := f.repo.MissingPaths(ctx, paths)
	require.NoError(t, err)
	assert.Empty(t, missing, "writes are indexed by the observer")

	require.NoError(t, f.ms.DeleteFileMetadata(ctx, paths[0]))
	missing, err = f.repo.MissingPaths(ctx, paths)
	require.NoError(t, err)
	assert.Equal(t, []string{paths[0]}, missing)

	require.NoError(t, f.ms.RenameFileMetadata(paths[1], "rel2/moved.mkv"))
	missing, err = f.repo.MissingPaths(ctx, []string{paths[1], "rel2/moved.mkv"})
	require.NoError(t, err)
	assert.Equal(t, []string{paths[1]}, missing)

	require.NoError(t, f.ms.DeleteDirectory("rel2"))
	missing, err = f.repo.MissingPaths(ctx, []string{"rel2/moved.mkv"})
	require.NoError(t, err)
	assert.Equal(t, []string{"rel2/moved.mkv"}, missing)
}

func TestObserver_FailureNeverFailsTheWrite(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	idx := NewIndexer(NewServiceSource(f.ms), failingRepo{}, WithBatchSize(1))
	f.ms.SetFileObserver(NewObserver(idx))
	paths := f.release(ctx, "rel", []string{"a1"}) // would panic/fail the test if the error propagated
	m, err := f.ms.ReadFileMetadata(paths[0])
	require.NoError(t, err)
	assert.NotNil(t, m)
	require.NoError(t, f.ms.DeleteFileMetadata(ctx, paths[0]))
	require.NoError(t, f.ms.DeleteDirectory("rel"))
}

type failingRepo struct{ Repository }

var errRepo = fmt.Errorf("repo down")

func (failingRepo) Upsert(context.Context, database.PeernetContentEntry) error { return errRepo }
func (failingRepo) UpsertBatch(context.Context, []database.PeernetContentEntry) error {
	return errRepo
}
func (failingRepo) MissingPaths(context.Context, []string) ([]string, error) { return nil, errRepo }
func (failingRepo) Delete(context.Context, string) error                     { return errRepo }
func (failingRepo) DeleteByPrefix(context.Context, string) error             { return errRepo }
func (failingRepo) RenamePrefix(context.Context, string, string) error       { return errRepo }
func (failingRepo) GetByPath(context.Context, string) (database.PeernetContentEntry, bool, error) {
	return database.PeernetContentEntry{}, false, errRepo
}
func (failingRepo) RefByPath(context.Context, string) (database.PeernetContentRef, bool, error) {
	return database.PeernetContentRef{}, false, errRepo
}
func (failingRepo) RefsByPrefix(context.Context, string, int) ([]database.PeernetContentRef, error) {
	return nil, errRepo
}
func (failingRepo) ListPage(context.Context, string, int) ([]database.PeernetContentEntry, error) {
	return nil, errRepo
}

// A store rebuilt by the metadata migration holds only the files that
// survived import (no parity files, no filtered extras), so a hash of it is not
// the release's identity: the file keeps its File ID and gets no Release ID.
func TestIndexer_MigratedStoreYieldsNoReleaseID(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	p := f.release(ctx, "_migrated/rel", []string{"a1", "a2"})[0]
	e, err := f.idx.Entry(ctx, p)
	require.NoError(t, err)
	assert.NotEmpty(t, e.FileID)
	assert.Empty(t, e.ReleaseID, "a migrated store must not name a release")

	q := f.release(ctx, "rel", []string{"a1", "a2"})[0]
	e, err = f.idx.Entry(ctx, q)
	require.NoError(t, err)
	assert.NotEmpty(t, e.ReleaseID, "a store written by the importer still does")
}
