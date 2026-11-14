package database

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func forEachBundleImportBackend(t *testing.T, fn func(t *testing.T, r *PeernetBundleImportRepository)) {
	t.Helper()
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	t.Run("sqlite", func(t *testing.T) { fn(t, NewPeernetBundleImportRepository(db.Connection(), DialectSQLite)) })
	if hasPostgresDSN() {
		t.Run("postgres", func(t *testing.T) {
			fn(t, NewPeernetBundleImportRepository(openPostgresLatest(t), DialectPostgres))
		})
	}
}

func hex64(c string) string { return strings.Repeat(c, 64) }

func bundleRow(path string, queue int64) PeernetBundleImport {
	return PeernetBundleImport{
		FilePath: path, FileID: hex64("a"), ReleaseID: hex64("b"), NetworkID: hex64("c"), PublisherID: hex64("d"),
		QueueID: queue, ArrOwed: true, Category: "movies", TargetPath: "", ResultingPath: "movies/x",
	}
}

func TestPeernetBundleImport_UpsertGetAndPending(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("/movies/x/a.mkv", 7), bundleRow("movies/x/b.mkv", 7)}))

		got, ok, err := r.Get(ctx, "movies/x/a.mkv") // slash-normalised
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, PeernetBundleStatePending, got.State)
		assert.Equal(t, int64(7), got.QueueID)
		assert.Equal(t, hex64("d"), got.PublisherID)
		assert.True(t, got.ArrOwed)

		_, ok, err = r.Get(ctx, "movies/other.mkv")
		require.NoError(t, err)
		assert.False(t, ok)

		pend, err := r.PendingAmong(ctx, []string{"movies/x/a.mkv", "movies/other.mkv"})
		require.NoError(t, err)
		assert.Len(t, pend, 1)
		assert.Contains(t, pend, "movies/x/a.mkv")

		has, err := r.QueueHasPending(ctx, 7)
		require.NoError(t, err)
		assert.True(t, has)
		has, err = r.QueueHasPending(ctx, 8)
		require.NoError(t, err)
		assert.False(t, has)
	})
}

func TestPeernetBundleImport_TransitionsAreCompareAndSet(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", 1), bundleRow("m/b.mkv", 1)}))

		ok, err := r.MarkPassed(ctx, "m/a.mkv")
		require.NoError(t, err)
		assert.True(t, ok)
		ok, err = r.MarkPassed(ctx, "m/a.mkv")
		require.NoError(t, err)
		assert.False(t, ok, "second pass is a no-op")
		ok, err = r.MarkFailed(ctx, "m/a.mkv")
		require.NoError(t, err)
		assert.False(t, ok, "a passed file cannot fail")
		ok, err = r.MarkFailed(ctx, "m/b.mkv")
		require.NoError(t, err)
		assert.True(t, ok)
		ok, err = r.MarkPassed(ctx, "m/b.mkv")
		require.NoError(t, err)
		assert.False(t, ok, "a failed file cannot pass")

		pend, err := r.PendingAmong(ctx, []string{"m/a.mkv", "m/b.mkv"})
		require.NoError(t, err)
		assert.Empty(t, pend)
	})
}

func TestPeernetBundleImport_AnnouncementClaimedOnceAfterAllPass(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", 5), bundleRow("m/b.mkv", 5)}))
		require.NoError(t, r.CommitRows(ctx, []string{"m/a.mkv", "m/b.mkv"}))

		_, claimed, err := r.ClaimAnnouncement(ctx, 5)
		require.NoError(t, err)
		assert.False(t, claimed, "nothing passed yet")

		_, _ = r.MarkPassed(ctx, "m/a.mkv")
		_, claimed, err = r.ClaimAnnouncement(ctx, 5)
		require.NoError(t, err)
		assert.False(t, claimed, "one file still pending")

		_, _ = r.MarkPassed(ctx, "m/b.mkv")
		var wins atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				row, ok, err := r.ClaimAnnouncement(ctx, 5)
				if err == nil && ok {
					wins.Add(1)
					assert.Equal(t, "movies", row.Category)
				}
			}()
		}
		wg.Wait()
		assert.Equal(t, int32(1), wins.Load(), "exactly one claimant wins")

		ids, err := r.QueuesAwaitingAnnouncement(ctx)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestPeernetBundleImport_FailedImportIsNeverAnnounced(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", 5), bundleRow("m/b.mkv", 5)}))
		_, _ = r.MarkPassed(ctx, "m/a.mkv")
		_, _ = r.MarkFailed(ctx, "m/b.mkv")
		_, claimed, err := r.ClaimAnnouncement(ctx, 5)
		require.NoError(t, err)
		assert.False(t, claimed)
	})
}

func TestPeernetBundleImport_NotOwedIsNeverClaimed(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		row := bundleRow("m/a.mkv", 5)
		row.ArrOwed = false
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{row}))
		_, _ = r.MarkPassed(ctx, "m/a.mkv")
		_, claimed, err := r.ClaimAnnouncement(ctx, 5)
		require.NoError(t, err)
		assert.False(t, claimed)
	})
}

func TestPeernetBundleImport_AwaitingAnnouncementSurvivesRestart(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", 9)}))
		require.NoError(t, r.CommitRows(ctx, []string{"m/a.mkv"}))
		_, _ = r.MarkPassed(ctx, "m/a.mkv")
		ids, err := r.QueuesAwaitingAnnouncement(ctx)
		require.NoError(t, err)
		assert.Equal(t, []int64{9}, ids)
	})
}

func TestPeernetBundleImport_QueueAndPathSummaries(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", 3), bundleRow("n/c.mkv", 4)}))
		_, _ = r.MarkPassed(ctx, "n/c.mkv")

		byQueue, err := r.StatesForQueues(ctx, []int64{3, 4, 99})
		require.NoError(t, err)
		assert.Equal(t, map[int64]bool{3: true, 4: false}, byQueue, "value is pending")

		byPath, err := r.StatesForPaths(ctx, []string{"m/a.mkv", "n/c.mkv", "zzz"})
		require.NoError(t, err)
		assert.Equal(t, map[string]bool{"m/a.mkv": true, "n/c.mkv": false}, byPath)

		empty, err := r.StatesForPaths(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, empty)
	})
}

func TestPeernetBundleImport_DeleteAndQueueSkipFlag(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", 3)}))
		require.NoError(t, r.Delete(ctx, []string{"/m/a.mkv"}))
		_, ok, err := r.Get(ctx, "m/a.mkv")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestPeernetBundleImport_SetQueueLinksRows(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", 0), bundleRow("m/b.mkv", 0)}))
		require.NoError(t, r.SetQueue(ctx, []string{"/m/a.mkv", "m/b.mkv"}, 42))
		has, err := r.QueueHasPending(ctx, 42)
		require.NoError(t, err)
		assert.True(t, has)
	})
}

func TestPeernetBundleImport_MoveDeleteAndPendingCounts(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		a, b, other := bundleRow("movies/x_y/a.mkv", 7), bundleRow("movies/x_y/b.mkv", 7), bundleRow("movies/xzy/c.mkv", 8)
		a.ResultingPath, b.ResultingPath, other.ResultingPath = "movies/x_y", "movies/x_y", "movies/xzy"
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{a, b, other}))

		moved, err := r.MoveFile(ctx, "movies/x_y/a.mkv", "movies/x_y/renamed.mkv")
		require.NoError(t, err)
		assert.True(t, moved)
		_, ok, _ := r.Get(ctx, "movies/x_y/a.mkv")
		assert.False(t, ok)

		require.NoError(t, r.MoveDir(ctx, "movies/x_y", "movies/new"))
		got, ok, err := r.Get(ctx, "movies/new/b.mkv")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "movies/new", got.ResultingPath)
		_, ok, _ = r.Get(ctx, "movies/xzy/c.mkv")
		assert.True(t, ok, "'_' is a literal, not a wildcard: the sibling folder is untouched")

		pending, stale, err := r.PendingCounts(ctx, time.Now().Add(time.Hour))
		require.NoError(t, err)
		assert.Equal(t, 3, pending)
		assert.Equal(t, 3, stale)
		pending, stale, err = r.PendingCounts(ctx, time.Now().Add(-time.Hour))
		require.NoError(t, err)
		assert.Equal(t, 3, pending)
		assert.Zero(t, stale)

		require.NoError(t, r.DeleteDir(ctx, "movies/new"))
		_, ok, _ = r.Get(ctx, "movies/new/b.mkv")
		assert.False(t, ok)
		_, ok, _ = r.Get(ctx, "movies/xzy/c.mkv")
		assert.True(t, ok)
	})
}
