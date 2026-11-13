package database

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPeernetTestRepo(t *testing.T) *PeernetContentRepository {
	t.Helper()
	if hasPostgresDSN() {
		return NewPeernetContentRepository(openPostgresLatest(t), DialectPostgres)
	}
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return NewPeernetContentRepository(db.Connection(), DialectSQLite)
}

func hexID(c byte) string { return strings.Repeat(string(c), 64) }

func TestPeernetContentRepository_UpsertAndLookup(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()

	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "movies/a.mkv", FileID: hexID('a'), ReleaseID: hexID('1')}))
	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "movies/b.mkv", FileID: hexID('b'), ReleaseID: hexID('1')}))

	byFile, err := r.FindByFileID(ctx, hexID('a'))
	require.NoError(t, err)
	assert.Equal(t, []PeernetContentEntry{{FilePath: "movies/a.mkv", FileID: hexID('a'), ReleaseID: hexID('1')}}, byFile)

	byRel, err := r.FindByReleaseID(ctx, hexID('1'))
	require.NoError(t, err)
	require.Len(t, byRel, 2)
	assert.Equal(t, "movies/a.mkv", byRel[0].FilePath)
	assert.Equal(t, "movies/b.mkv", byRel[1].FilePath)

	none, err := r.FindByFileID(ctx, hexID('f'))
	require.NoError(t, err)
	assert.Empty(t, none)

	// Upsert replaces in place, and path normalisation maps variants to one row.
	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "/movies\\a.mkv", FileID: hexID('c'), ReleaseID: hexID('2')}))
	byFile, err = r.FindByFileID(ctx, hexID('a'))
	require.NoError(t, err)
	assert.Empty(t, byFile)
	byFile, err = r.FindByFileID(ctx, hexID('c'))
	require.NoError(t, err)
	require.Len(t, byFile, 1)
	assert.Equal(t, "movies/a.mkv", byFile[0].FilePath)
}

func TestPeernetContentRepository_SameFileIDManyPaths(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "x/1.mkv", FileID: hexID('a'), ReleaseID: hexID('1')},
		{FilePath: "y/1.mkv", FileID: hexID('a'), ReleaseID: hexID('1')},
	}))
	got, err := r.FindByFileID(ctx, hexID('a'))
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

func TestPeernetContentRepository_UpsertBatch(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()

	require.NoError(t, r.UpsertBatch(ctx, nil))

	var batch []PeernetContentEntry
	for i := range 1200 {
		batch = append(batch, PeernetContentEntry{FilePath: fmt.Sprintf("d/%d.mkv", i), FileID: hexID('a'), ReleaseID: hexID('1')})
	}
	require.NoError(t, r.UpsertBatch(ctx, batch))
	require.NoError(t, r.UpsertBatch(ctx, batch[:10])) // idempotent
	got, err := r.FindByReleaseID(ctx, hexID('1'))
	require.NoError(t, err)
	assert.Len(t, got, 1200)
}

func TestPeernetContentRepository_Validation(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	tests := []struct {
		name string
		e    PeernetContentEntry
	}{
		{"empty path", PeernetContentEntry{FileID: hexID('a')}},
		{"empty file id", PeernetContentEntry{FilePath: "a"}},
		{"bad file id", PeernetContentEntry{FilePath: "a", FileID: "nothex"}},
		{"bad release id", PeernetContentEntry{FilePath: "a", FileID: hexID('a'), ReleaseID: "short"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, r.Upsert(ctx, tc.e))
			require.Error(t, r.UpsertBatch(ctx, []PeernetContentEntry{tc.e}))
		})
	}
	// An empty release id is allowed (metadata without a shared store).
	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "a", FileID: hexID('a')}))
	_, err := r.FindByFileID(ctx, "bad")
	require.Error(t, err)
	_, err = r.FindByReleaseID(ctx, "")
	require.Error(t, err)
}

func TestPeernetContentRepository_Delete(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "tv/show/e1.mkv", FileID: hexID('a')},
		{FilePath: "tv/show/e2.mkv", FileID: hexID('b')},
		{FilePath: "tv/show2/e1.mkv", FileID: hexID('c')},
		{FilePath: "tv/sh_w/e1.mkv", FileID: hexID('d')},
		{FilePath: "tv/show", FileID: hexID('e')},
	}))

	require.NoError(t, r.Delete(ctx, "/tv/show/e1.mkv"))
	require.NoError(t, r.Delete(ctx, "missing")) // no error for absent rows
	left, err := r.MissingPaths(ctx, []string{"tv/show/e1.mkv", "tv/show/e2.mkv"})
	require.NoError(t, err)
	assert.Equal(t, []string{"tv/show/e1.mkv"}, left)

	// Prefix delete removes the directory subtree only: not siblings that
	// share a prefix, and '_' is not a wildcard.
	require.NoError(t, r.DeleteByPrefix(ctx, "tv/show"))
	missing, err := r.MissingPaths(ctx, []string{"tv/show", "tv/show/e2.mkv", "tv/show2/e1.mkv", "tv/sh_w/e1.mkv"})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"tv/show", "tv/show/e2.mkv"}, missing)

	require.Error(t, r.DeleteByPrefix(ctx, ""), "refuse to wipe the whole index")
}

func TestPeernetContentRepository_MissingPaths(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()

	got, err := r.MissingPaths(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, got)

	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "a/1", FileID: hexID('a')}))
	got, err = r.MissingPaths(ctx, []string{"a/1", "a/2", "/a/3", "a/2"})
	require.NoError(t, err)
	assert.Equal(t, []string{"a/2", "a/3"}, got, "deduplicated, normalised, input order")

	// Larger than one internal chunk.
	var cands []string
	for i := range 1500 {
		cands = append(cands, fmt.Sprintf("big/%d", i))
	}
	got, err = r.MissingPaths(ctx, cands)
	require.NoError(t, err)
	assert.Equal(t, cands, got)
}

func TestPeernetContentRepository_GetByPath(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	_, ok, err := r.GetByPath(ctx, "movies/a.mkv")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "movies/a.mkv", FileID: hexID('a'), ReleaseID: hexID('1')}))
	for _, p := range []string{"movies/a.mkv", "/movies/a.mkv", "movies\\a.mkv"} {
		got, ok, err := r.GetByPath(ctx, p)
		require.NoError(t, err)
		require.True(t, ok, p)
		assert.Equal(t, PeernetContentEntry{FilePath: "movies/a.mkv", FileID: hexID('a'), ReleaseID: hexID('1')}, got)
	}
	_, _, err = r.GetByPath(ctx, "")
	assert.Error(t, err)
}

func TestPeernetContent_ReleaseIDsByPaths(t *testing.T) {
	repo, ctx := newPeernetTestRepo(t), context.Background()
	rel := strings.Repeat("a", 64)
	require.NoError(t, repo.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "movies/a/a.mkv", FileID: strings.Repeat("1", 64), ReleaseID: rel},
		{FilePath: "movies/b/b.mkv", FileID: strings.Repeat("2", 64)}, // no release
	}))
	got, err := repo.ReleaseIDsByPaths(ctx, []string{"/movies/a/a.mkv", "movies/b/b.mkv", "movies/none.mkv", "movies/a/a.mkv"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"/movies/a/a.mkv": rel, "movies/a/a.mkv": rel}, got,
		"keyed by the path as asked, releases without an id and unknown paths are absent")

	empty, err := repo.ReleaseIDsByPaths(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
	tooMany := make([]string, PeernetMaxPathsPerQuery+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("movies/x%d.mkv", i)
	}
	_, err = repo.ReleaseIDsByPaths(ctx, tooMany)
	assert.ErrorIs(t, err, ErrPeernetTooManyIDs)
}

func TestPeernetContentRepository_Counts(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	c, err := r.Counts(ctx)
	require.NoError(t, err)
	assert.Equal(t, PeernetContentCounts{}, c)
	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "a.mkv", FileID: hexID('a'), ReleaseID: hexID('1')}))
	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "b.mkv", FileID: hexID('b')}))
	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "c.mkv", FileID: hexID('c')}))
	c, err = r.Counts(ctx)
	require.NoError(t, err)
	assert.Equal(t, PeernetContentCounts{Files: 3, WithReleaseID: 1}, c)
}
