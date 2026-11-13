package contentid

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func entryFor(p string) database.PeernetContentEntry {
	return database.PeernetContentEntry{FilePath: p, FileID: strings.Repeat("e", 64)}
}

func TestReconcile_PrunesVanishedAndReindexesChanged(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, WithBatchSize(2))
	paths := populate(ctx, f, 3) // 6 files
	_, err := f.idx.Backfill(ctx)
	require.NoError(t, err)

	// Changes made while the feature was off: one file deleted, one replaced
	// with different segments, one directory removed, a stray row left behind.
	require.NoError(t, f.ms.DeleteFileMetadata(ctx, paths[0]))
	changed := paths[1]
	require.NoError(t, f.ms.WriteFileMetadata(changed,
		&metapb.FileMetadata{FileSize: 10, SegmentData: sd("new1", "new2")}))
	require.NoError(t, f.ms.DeleteDirectory("movies/r2"))
	require.NoError(t, f.repo.Upsert(ctx, entryFor("ghost/never.mkv")))
	require.NoError(t, f.repo.Upsert(ctx, entryFor("corrupted_metadata/movies/x.mkv")))
	f.idx.mu.Lock()
	f.idx.lastStore = nil
	f.idx.mu.Unlock()

	res, err := f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, res.Pruned, "deleted file, directory of two, ghost, quarantined")
	assert.Equal(t, 1, res.Reindexed)

	want, err := f.idx.Entry(ctx, changed)
	require.NoError(t, err)
	got, err := f.repo.FindByFileID(ctx, want.FileID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, changed, got[0].FilePath)
	assert.Empty(t, got[0].ReleaseID, "the rewritten file has no shared store any more")

	missing, err := f.repo.MissingPaths(ctx, []string{paths[0], "ghost/never.mkv", "corrupted_metadata/movies/x.mkv"})
	require.NoError(t, err)
	assert.Len(t, missing, 3)

	// Idempotent: a second run has nothing to do.
	res, err = f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Zero(t, res.Pruned+res.Reindexed)
}

func TestReconcile_LeavesUnchangedRowsAlone(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	populate(ctx, f, 2)
	_, err := f.idx.Backfill(ctx)
	require.NoError(t, err)
	// Status and hole updates do not change the identity: no reindex.
	p := "movies/r0/file0.mkv"
	require.NoError(t, f.ms.UpdateFileStatus(p, metapb.FileStatus_FILE_STATUS_CORRUPTED))
	require.NoError(t, f.ms.AddKnownHoles(p, []holes.Run{{Start: 0, Count: 1}}, "fp"))
	res, err := f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Zero(t, res.Pruned+res.Reindexed)
	assert.Equal(t, 4, res.Checked)
}

func TestReconcile_Cancellable(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, WithBatchSize(2))
	populate(ctx, f, 3)
	_, err := f.idx.Backfill(ctx)
	require.NoError(t, err)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = f.idx.Reconcile(cctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAsyncObserver_NeverBlocksAndIsBounded(t *testing.T) {
	f := newFixture(t)
	var overflow int
	var mu sync.Mutex
	ao := NewAsyncObserver(NewObserver(f.idx), 3, func() { mu.Lock(); overflow++; mu.Unlock() })
	// No worker is running: a synchronous observer would index inline; this
	// must just queue, drop the oldest and return at once.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			ao.FileWritten(context.Background(), "x/a.mkv")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue blocked")
	}
	assert.Equal(t, int64(97), ao.Dropped())
	mu.Lock()
	assert.Equal(t, 97, overflow)
	mu.Unlock()
	ao.mu.Lock()
	assert.Len(t, ao.q, 3)
	ao.mu.Unlock()
}

func TestAsyncObserver_ProcessesInOrderAndStopsOnCancel(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	paths := f.release(ctx, "rel", []string{"a1"}, []string{"b1"})
	ao := NewAsyncObserver(NewObserver(f.idx), 0, nil)
	wctx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})
	go func() { ao.Run(wctx); close(finished) }()

	ao.FileWritten(ctx, paths[0])
	ao.FileWritten(ctx, paths[1])
	ao.FileDeleted(ctx, paths[0])
	require.Eventually(t, func() bool {
		m, err := f.repo.MissingPaths(ctx, paths)
		return err == nil && len(m) == 1 && m[0] == paths[0]
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop on cancel")
	}
	ao.FileWritten(ctx, paths[0]) // after stop: ignored, no panic, no growth
	ao.mu.Lock()
	assert.Empty(t, ao.q)
	ao.mu.Unlock()
}

func TestObserver_DirectoryRenameKeepsRows(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.ms.SetFileObserver(NewObserver(f.idx))
	paths := f.release(ctx, "old", []string{"a1"})
	require.NoError(t, f.ms.RenameDirectory("old", "new"))
	missing, err := f.repo.MissingPaths(ctx, []string{"new/file0.mkv", paths[0]})
	require.NoError(t, err)
	assert.Equal(t, []string{paths[0]}, missing)
}

func TestObserver_FailureLogIsRateLimited(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	now := time.Unix(1000, 0)
	var l failureLog
	l.now = func() time.Time { return now }
	for i := 0; i < 50; i++ {
		l.warn(context.Background(), "Peernet content indexing failed", "a", assert.AnError)
	}
	assert.Equal(t, 1, bytes.Count(buf.Bytes(), []byte("Peernet content indexing failed")))
	now = now.Add(2 * failureLogEvery)
	l.warn(context.Background(), "Peernet content indexing failed", "a", assert.AnError)
	assert.Equal(t, 2, bytes.Count(buf.Bytes(), []byte("Peernet content indexing failed")))
	assert.Contains(t, buf.String(), "suppressed_since_last=49")
}

// A release ID an earlier version derived from a migration-rebuilt store is
// withdrawn: the row keeps its file ID and loses the release ID.
func TestReconcile_DropsReleaseIDOfMigratedStore(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	p := f.release(ctx, "_migrated/rel", []string{"a1", "a2"})[0]
	e, err := f.idx.Entry(ctx, p)
	require.NoError(t, err)
	e.ReleaseID = strings.Repeat("c", 64) // what the old behaviour stored
	require.NoError(t, f.repo.Upsert(ctx, e))

	res, err := f.idx.Reconcile(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Reindexed)
	got, ok, err := f.repo.GetByPath(ctx, p)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Empty(t, got.ReleaseID)
	assert.Equal(t, e.FileID, got.FileID)
}
