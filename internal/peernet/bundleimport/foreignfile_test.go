package bundleimport

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replaceWithOtherFile overwrites the metadata at path with a different file
// (what a later normal import of another layout leaves there).
func replaceWithOtherFile(t *testing.T, e *env, path string) {
	t.Helper()
	m := mustMeta(t, e, path)
	m.SegmentData[0].Id = "someone-else@test"
	require.NoError(t, e.lib.WriteFileMetadata(path, m))
}

// A row whose File ID is not the file at its path describes a file that is
// gone: it must never cause a delete, a hold or a forced check.
func TestForeignFile_FailedVerdictNeverDeletesTheFileAtThePath(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	_, res := e.accept("fo1", mkvSpec("a.mkv"))
	p := res.Paths[0]
	replaceWithOtherFile(t, e, p)

	e.imp.FullCheckFailed(context.Background(), p)

	assert.True(t, e.lib.FileExists(p), "the other file at the path must survive")
	assert.Empty(t, ann.removed)
	assert.Empty(t, ann.failed, "the queue item is not failed for a file that is not ours")
	assert.Zero(t, e.rowCount(p), "the stale row is dropped")
	assert.Empty(t, e.rep.got)
}

func TestForeignFile_StaleEscalationNeverDeletesTheFileAtThePath(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	_, res := e.accept("fo2", mkvSpec("a.mkv"))
	p := res.Paths[0]
	replaceWithOtherFile(t, e, p)
	e.imp.cfg.Now = func() time.Time { return time.Now().Add(MaxPendingAge + time.Hour) }

	assert.Zero(t, e.imp.EscalateStale(context.Background()))

	assert.True(t, e.lib.FileExists(p))
	assert.Empty(t, ann.removed)
	assert.Zero(t, e.rowCount(p))
}

func TestForeignFile_IsNeitherPendingNorHeld(t *testing.T) {
	e := newEnv(t)
	e.withFailAnn()
	_, res := e.accept("fo3", mkvSpec("a.mkv"))
	p := res.Paths[0]
	ctx := context.Background()
	replaceWithOtherFile(t, e, p)

	assert.False(t, e.imp.IsPendingPeerImport(ctx, p), "no repair attempts are suppressed for it")
	assert.NotContains(t, e.imp.PendingFullCheck(ctx, []string{p}), p, "no forced full check")
	assert.Zero(t, e.rowCount(p))
}

func TestForeignFile_FailImportSparesForeignSiblingFiles(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	_, res := e.accept("fo4", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	replaceWithOtherFile(t, e, res.Paths[0]) // a sibling was replaced by another file

	e.imp.FullCheckFailed(context.Background(), res.Paths[1])

	assert.True(t, e.lib.FileExists(res.Paths[0]), "a replaced sibling is not ours to remove")
	assert.False(t, e.lib.FileExists(res.Paths[1]))
	assert.Equal(t, []string{res.Paths[1]}, ann.removed)
	assert.Zero(t, e.rowCount(res.Paths...))
}

// A delete followed by a rewrite of the same File ID at the same path is a
// normal import replacing the pending peer import, not a rename: the peer
// import ends cleanly and the hold is released.
func TestPathObserver_NormalImportOverAPendingImportEndsIt(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	_, res := e.accept("fo5", mkvSpec("a.mkv"))
	p := res.Paths[0]
	ctx := context.Background()
	repo := database.NewRepository(e.db.Connection(), database.DialectSQLite)
	item := &database.ImportQueueItem{NzbPath: "x.nzb", Status: database.QueueStatusCompleted, Priority: database.QueuePriorityNormal}
	require.NoError(t, repo.AddToQueue(ctx, item))
	_, err := e.db.Connection().Exec(`UPDATE import_queue SET id = 11 WHERE id = ?`, item.ID) // the import is bound to queue item 11
	require.NoError(t, err)
	require.NoError(t, e.repo.SetQueueHold(ctx, 11, true))
	obs := e.imp.PathObserver()
	m := mustMeta(t, e, p)
	require.NoError(t, e.lib.DeleteFileMetadata(ctx, p))
	obs.FileDeleted(ctx, p)
	require.NoError(t, e.lib.WriteFileMetadata(p, m)) // the normal import writes the same content
	obs.FileWritten(ctx, p)
	obs.Flush()

	assert.Zero(t, e.rowCount(p), "the row must not attach to a normal import")
	assert.NotContains(t, e.imp.PendingFullCheck(ctx, []string{p}), p, "no forced check, no hold")
	assert.Equal(t, []int64{11}, ann.freed, "the hold is released")
	assert.True(t, e.lib.FileExists(p), "the normal import's file stays")
	assert.Empty(t, e.ann.got)
}
