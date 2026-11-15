package bundleimport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newObserved(t *testing.T) (*env, *Result) {
	e := newEnv(t)
	_, res := e.accept("po", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	return e, res
}

func TestPathObserver_DeletedFileRemovesItsRowAndNeverAnnouncesAnEmptyRelease(t *testing.T) {
	e, res := newObserved(t)
	ctx := context.Background()
	obs := e.imp.PathObserver()
	require.NoError(t, e.lib.DeleteFileMetadata(ctx, res.Paths[0]))
	obs.FileDeleted(ctx, res.Paths[0])
	obs.Flush()
	_, ok, _ := e.repo.Get(ctx, res.Paths[0])
	assert.False(t, ok, "the deleted file's row is gone")
	_, ok, _ = e.repo.Get(ctx, res.Paths[1])
	assert.True(t, ok, "the other file keeps its row")

	// Deleting the last file leaves no row, so nothing can be announced.
	require.NoError(t, e.lib.DeleteFileMetadata(ctx, res.Paths[1]))
	obs.FileDeleted(ctx, res.Paths[1])
	obs.Flush()
	assert.Zero(t, e.stateRows())
	e.imp.FullCheckPassed(ctx, res.Paths[1])
	assert.Empty(t, e.ann.got, "no announcement for a release with zero remaining files")
}

func TestPathObserver_RenamedFileKeepsItsPendingRow(t *testing.T) {
	e, res := newObserved(t)
	ctx := context.Background()
	obs := e.imp.PathObserver()
	newPath := "movies/Test.Release/renamed.mkv"
	require.NoError(t, e.lib.RenameFileMetadata(res.Paths[0], newPath))
	obs.FileDeleted(ctx, res.Paths[0])
	obs.FileWritten(ctx, newPath)
	obs.Flush()
	_, ok, _ := e.repo.Get(ctx, res.Paths[0])
	assert.False(t, ok)
	row, ok, _ := e.repo.Get(ctx, newPath)
	require.True(t, ok, "the row follows the renamed file")
	assert.Equal(t, "pending", row.State, "a rename must not lift the hold")
	assert.Contains(t, e.imp.PendingFullCheck(ctx, []string{newPath}), newPath)
}

func TestPathObserver_RenamedFolderMovesRows(t *testing.T) {
	e, res := newObserved(t)
	ctx := context.Background()
	obs := e.imp.PathObserver()
	require.NoError(t, e.lib.RenameDirectory("movies/Test.Release", "movies/Other"))
	obs.DirectoryRenamed(ctx, "movies/Test.Release", "movies/Other")
	_, ok, _ := e.repo.Get(ctx, res.Paths[0])
	assert.False(t, ok)
	row, ok, _ := e.repo.Get(ctx, "movies/Other/a.mkv")
	require.True(t, ok)
	assert.Equal(t, "movies/Other", row.ResultingPath)

	obs.DirectoryDeleted(ctx, "movies/Other")
	assert.Zero(t, e.stateRows())
}

// The library reports deletes under whatever spelling the caller used (the
// HTTP API passes a leading slash); the row is keyed by the clean path.
func TestPathObserver_DeleteWithLeadingSlashRemovesTheRow(t *testing.T) {
	e, res := newObserved(t)
	ctx := context.Background()
	obs := e.imp.PathObserver()
	require.NoError(t, e.lib.DeleteFileMetadata(ctx, res.Paths[0]))
	obs.FileDeleted(ctx, "/"+res.Paths[0])
	obs.Flush()
	_, ok, _ := e.repo.Get(ctx, res.Paths[0])
	assert.False(t, ok, "the deleted file's row is gone")
}

// Through the real metadata service: a user deleting the file ends its state
// row once the rename grace has passed, with no further attempt needed.
func TestPathObserver_UserDeleteThroughTheLibraryEndsTheStateRow(t *testing.T) {
	old := renameGrace
	renameGrace = 50 * time.Millisecond
	t.Cleanup(func() { renameGrace = old })
	e, res := newObserved(t)
	ctx := context.Background()
	e.lib.SetSecondaryFileObserver(e.imp.PathObserver())
	require.NoError(t, e.lib.DeleteFileMetadata(ctx, res.Paths[0]))
	require.Eventually(t, func() bool {
		_, ok, _ := e.repo.Get(ctx, res.Paths[0])
		return !ok
	}, 2*time.Second, 20*time.Millisecond, "the row of a deleted file must not outlive it")
}

// The user deletes an imported release and imports the same release again under
// another category before the rename grace has passed: the delete must not
// leave its row behind, and the fresh import's row (and hold) must stand.
func TestPathObserver_ReimportWithinGraceUnderAnotherCategoryDropsTheStaleRow(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	b := e.mkBundle("rg", mkvSpec("a.mkv"))
	res, err := e.imp.Accept(ctx, e.req(b))
	require.NoError(t, err)
	obs := e.imp.PathObserver()
	e.lib.SetSecondaryFileObserver(obs)
	oldPath := res.Paths[0]
	require.NoError(t, e.lib.DeleteFileMetadata(ctx, oldPath))

	req := e.req(b)
	req.Target = Target{Dir: "tv/Test.Release", Category: "tv", QueueID: 12}
	res2, err := e.imp.Accept(ctx, req)
	require.NoError(t, err)

	obs.Flush()
	_, ok, _ := e.repo.Get(ctx, oldPath)
	assert.False(t, ok, "the deleted file's row must not outlive it")
	row, ok, _ := e.repo.Get(ctx, res2.Paths[0])
	require.True(t, ok, "the fresh import keeps its row")
	assert.Equal(t, "pending", row.State, "a delete must not lift the fresh import's hold")
	assert.Equal(t, 1, e.stateRows())
}
