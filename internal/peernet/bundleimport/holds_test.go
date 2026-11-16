package bundleimport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// A hold whose rows are all passed and committed, and for which no ARR is
// owed, is released by housekeeping even when the release at pass time failed.
func TestHolds_SettledHoldIsReleasedByHousekeeping(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	ctx := context.Background()
	b := e.mkBundle("hs", mkvSpec("a.mkv"))
	req := e.req(b)
	req.Target.SkipArr = true
	res, err := e.imp.Accept(ctx, req)
	require.NoError(t, err)
	q := database.NewRepository(e.db.Connection(), database.DialectSQLite)
	item := &database.ImportQueueItem{NzbPath: "x.nzb", Status: database.QueueStatusCompleted, Priority: database.QueuePriorityNormal}
	require.NoError(t, q.AddToQueue(ctx, item))
	_, err = e.db.Connection().Exec(`UPDATE import_queue SET id = 11 WHERE id = ?`, item.ID)
	require.NoError(t, err)
	require.NoError(t, e.repo.SetQueueHold(ctx, 11, true))
	_, err = e.repo.MarkPassed(ctx, res.Paths[0]) // passed, but the hold release never happened
	require.NoError(t, err)

	e.imp.releaseOrphanHolds(ctx)

	assert.Equal(t, []int64{11}, ann.freed)
}

func TestHolds_PendingHoldIsKept(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	ctx := context.Background()
	b := e.mkBundle("hp", mkvSpec("a.mkv"))
	req := e.req(b)
	req.Target.SkipArr = true
	_, err := e.imp.Accept(ctx, req)
	require.NoError(t, err)
	q := database.NewRepository(e.db.Connection(), database.DialectSQLite)
	item := &database.ImportQueueItem{NzbPath: "x.nzb", Status: database.QueueStatusCompleted, Priority: database.QueuePriorityNormal}
	require.NoError(t, q.AddToQueue(ctx, item))
	_, err = e.db.Connection().Exec(`UPDATE import_queue SET id = 11 WHERE id = ?`, item.ID)
	require.NoError(t, err)
	require.NoError(t, e.repo.SetQueueHold(ctx, 11, true))

	e.imp.releaseOrphanHolds(ctx)

	assert.Empty(t, ann.freed, "a pending import keeps its hold")
}

type blockingLibrary struct {
	Library
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingLibrary) WriteFileMetadata(p string, m *metapb.FileMetadata) error {
	l.once.Do(func() { close(l.entered); <-l.release })
	return l.Library.WriteFileMetadata(p, m)
}

// Reconcile rolls back every uncommitted row, so it must wait for an Accept
// that is writing and never roll its rows back.
func TestReconcile_NeverRollsBackAnInFlightAccept(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("rc", mkvSpec("a.mkv"))
	lib := &blockingLibrary{Library: e.lib, entered: make(chan struct{}), release: make(chan struct{})}
	e.imp.cfg.Library = lib
	done := make(chan error, 1)
	go func() { _, err := e.imp.Accept(context.Background(), e.req(b)); done <- err }()
	<-lib.entered // rows are written (uncommitted), the first file is about to be

	reconciled := make(chan struct{})
	go func() { e.imp.Reconcile(context.Background()); close(reconciled) }()
	select {
	case <-reconciled:
		t.Fatal("Reconcile ran while an Accept was writing")
	case <-time.After(300 * time.Millisecond):
	}
	close(lib.release)
	require.NoError(t, <-done)
	<-reconciled
	row, ok, _ := e.repo.Get(context.Background(), "movies/Test.Release/a.mkv")
	require.True(t, ok, "the finished import keeps its row")
	assert.True(t, row.Committed)
	assert.True(t, e.lib.FileExists("movies/Test.Release/a.mkv"))
}
