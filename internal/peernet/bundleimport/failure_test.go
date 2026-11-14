package bundleimport

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failAnn is an Announcer that also ends failed imports, recording what the
// importer glue would be asked to do. RemoveFailedFile deletes the metadata
// like the real service does.
type failAnn struct {
	fakeAnnouncer
	lib interface {
		DeleteFileMetadata(context.Context, string) error
	}
	mu      sync.Mutex
	failed  map[int64]string
	removed []string
	freed   []int64
}

func (a *failAnn) FailImport(_ context.Context, queueID int64, reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failed == nil {
		a.failed = map[int64]string{}
	}
	a.failed[queueID] = reason
	return nil
}

func (a *failAnn) RemoveFailedFile(ctx context.Context, p string) error {
	a.mu.Lock()
	a.removed = append(a.removed, p)
	a.mu.Unlock()
	return a.lib.DeleteFileMetadata(ctx, p)
}

func (a *failAnn) ReleaseHold(_ context.Context, queueID int64) error {
	a.mu.Lock()
	a.freed = append(a.freed, queueID)
	a.mu.Unlock()
	return nil
}

func (e *env) withFailAnn() *failAnn {
	a := &failAnn{lib: e.lib}
	e.imp.cfg.Announcer = a
	e.imp.cfg.Index = database.NewPeernetContentRepository(e.db.Connection(), database.DialectSQLite)
	return a
}

func (e *env) rowCount(paths ...string) int {
	n := 0
	for _, p := range paths {
		if _, ok, _ := e.repo.Get(context.Background(), p); ok {
			n++
		}
	}
	return n
}

func TestFailure_FailedFullCheckFailsTheQueueItemAndRemovesTheWholeImport(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	_, res := e.accept("f", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	ctx := context.Background()
	// An unrelated, ordinary file next to it must survive.
	require.NoError(t, e.lib.WriteFileMetadata("movies/Other/other.mkv", mustMeta(t, e, res.Paths[0])))

	e.imp.FullCheckPassed(ctx, res.Paths[0])
	e.imp.FullCheckFailed(ctx, res.Paths[1])

	assert.Equal(t, failReasonCheck, ann.failed[11], "the queue item is failed, so the ARR blocklists and searches again")
	for _, p := range res.Paths {
		assert.False(t, e.lib.FileExists(p), "%s must not stay in the library", p)
	}
	assert.True(t, e.lib.FileExists("movies/Other/other.mkv"), "only the failed import is removed")
	assert.Zero(t, e.rowCount(res.Paths...), "state rows are gone")
	// The sibling that passed earlier can no longer complete the import.
	e.imp.FullCheckPassed(ctx, res.Paths[0])
	e.imp.ResumeAnnouncements(ctx)
	assert.Empty(t, ann.got, "a failed import is never announced")
}

func TestFailure_StaleImportIsFailedLikeAFailedCheck(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	_, res := e.accept("stale", mkvSpec("a.mkv"))
	e.imp.cfg.Now = func() time.Time { return time.Now().Add(MaxPendingAge + time.Hour) }

	assert.Equal(t, 1, e.imp.EscalateStale(context.Background()))

	assert.Equal(t, failReasonStale, ann.failed[11])
	assert.False(t, e.lib.FileExists(res.Paths[0]))
	assert.Zero(t, e.rowCount(res.Paths...))
	assert.Empty(t, e.rep.got, "no check failed, so the publisher is not reported")
}

func TestFailure_OrphanedHoldIsReleased(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	ctx := context.Background()
	// A held queue item (peer_hold set) that no row refers to.
	repo := database.NewRepository(e.db.Connection(), database.DialectSQLite)
	item := &database.ImportQueueItem{NzbPath: "x.nzb", Status: database.QueueStatusCompleted, Priority: database.QueuePriorityNormal}
	require.NoError(t, repo.AddToQueue(ctx, item))
	require.NoError(t, e.repo.SetQueueHold(ctx, item.ID, true))

	_, res := e.accept("orphan", mkvSpec("a.mkv")) // queue 11: a different item
	obs := e.imp.PathObserver()
	obs.FileDeleted(ctx, res.Paths[0])
	obs.Flush()

	assert.Equal(t, []int64{item.ID}, ann.freed, "a hold nothing can release is lifted")
}

func TestReconcile_DropsRowsWhoseFilesAreGoneAndAnnouncesNothing(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	ctx := context.Background()
	_, res := e.accept("gone", mkvSpec("a.mkv"))
	_, err := e.repo.MarkPassed(ctx, res.Paths[0])
	require.NoError(t, err)
	require.NoError(t, e.lib.DeleteFileMetadata(ctx, res.Paths[0]))
	e.imp.PathObserver() // not installed: the file vanished behind our back

	e.imp.Reconcile(ctx)
	e.imp.ResumeAnnouncements(ctx)

	assert.Zero(t, e.rowCount(res.Paths...))
	assert.Empty(t, ann.got, "a release with no files left is never announced")
}

// A crash at every step of write: the rows exist uncommitted, a restart rolls
// the whole import back, and until then nothing can announce or publish it.
func TestReconcile_RollsBackAnImportThatCrashedAtEveryStepOfWrite(t *testing.T) {
	for _, tc := range []struct {
		step string
		nth  int
	}{{"rows", 0}, {"file", 0}, {"file", 1}, {"files", 0}, {"index", 0}} {
		t.Run(fmt.Sprintf("%s#%d", tc.step, tc.nth), func(t *testing.T) {
			e := newEnv(t)
			ann := e.withFailAnn()
			ctx := context.Background()
			seen := 0
			e.imp.crashHook = func(step string) bool {
				if step != tc.step {
					return false
				}
				seen++
				return seen == tc.nth+1
			}
			b := e.mkBundle("crash", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
			_, err := e.imp.Accept(ctx, e.req(b))
			require.Error(t, err, "the simulated crash abandons the write")
			paths := []string{"movies/Test.Release/a.mkv", "movies/Test.Release/b.mkv"}
			require.Equal(t, 2, e.rowCount(paths...), "the crash left uncommitted rows behind")

			// Before the restart: even when everything looks passed, nothing is
			// announced or publishable.
			for _, p := range paths {
				_, _ = e.repo.MarkPassed(ctx, p)
			}
			e.imp.ResumeAnnouncements(ctx)
			assert.Empty(t, ann.got, "an unfinished import is never announced")
			_, passed, _ := e.imp.PeerBundleStatus(ctx, paths[0])
			assert.False(t, passed, "nor publishable")

			// The restart: a new importer over the same state.
			imp2 := New(Config{Library: e.lib, State: e.repo, Announcer: ann, Index: e.imp.cfg.Index})
			imp2.Reconcile(ctx)

			for _, p := range paths {
				assert.False(t, e.lib.FileExists(p), "%s was rolled back", p)
			}
			assert.Zero(t, e.rowCount(paths...))
			imp2.ResumeAnnouncements(ctx)
			assert.Empty(t, ann.got)
		})
	}
}

func TestReconcile_LeavesAFinishedImportAndAForeignFileAlone(t *testing.T) {
	e := newEnv(t)
	e.withFailAnn()
	ctx := context.Background()
	_, res := e.accept("keep", mkvSpec("a.mkv"))

	// An uncommitted row whose path holds somebody else's file: the row goes,
	// the file stays.
	foreign := "movies/Foreign/x.mkv"
	m := mustMeta(t, e, res.Paths[0])
	m.FileSize++ // a different file
	m.SegmentData[0].Id = "other-id"
	require.NoError(t, e.lib.WriteFileMetadata(foreign, m))
	require.NoError(t, e.repo.Upsert(ctx, []database.PeernetBundleImport{{FilePath: foreign, FileID: hex("e"), ReleaseID: hex("f"), NetworkID: hex("c"), PublisherID: hex("d")}}))

	e.imp.Reconcile(ctx)

	assert.True(t, e.lib.FileExists(res.Paths[0]), "a committed import stays")
	assert.Equal(t, 1, e.rowCount(res.Paths...))
	assert.True(t, e.lib.FileExists(foreign), "a file that is not the row's file is never deleted")
	assert.Zero(t, e.rowCount(foreign))
}

func hex(c string) string { return strings.Repeat(c, 64) }

func mustMeta(t *testing.T, e *env, path string) *metapb.FileMetadata {
	t.Helper()
	m, err := e.lib.ReadFileMetadata(path)
	require.NoError(t, err)
	require.NotNil(t, m)
	return m
}

// An import no ARR is told about (suppressed by the caller, or a direct
// import) still ends its hold when the last file passes.
func TestFailure_HoldEndsWhenAnImportNoARRKnowsPasses(t *testing.T) {
	e := newEnv(t)
	ann := e.withFailAnn()
	b := e.mkBundle("noarr", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	req := e.req(b)
	req.Target.SkipArr = true
	res, err := e.imp.Accept(context.Background(), req)
	require.NoError(t, err)
	ctx := context.Background()

	e.imp.FullCheckPassed(ctx, res.Paths[0])
	assert.Empty(t, ann.freed, "one file still pending")
	e.imp.FullCheckPassed(ctx, res.Paths[1])
	assert.Equal(t, []int64{11}, ann.freed)
	assert.Empty(t, ann.got, "and no ARR is told")
}
