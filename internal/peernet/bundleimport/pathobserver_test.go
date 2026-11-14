package bundleimport

import (
	"context"
	"testing"

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
