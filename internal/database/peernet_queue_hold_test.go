package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// holdFixture is a completed queue item of a peer import, with its provisional
// history row, on one backend.
type holdFixture struct {
	repo *Repository
	bund *PeernetBundleImportRepository
	id   int64
}

func newHoldFixture(t *testing.T, r *PeernetBundleImportRepository, name string, skip bool) holdFixture {
	t.Helper()
	ctx := context.Background()
	repo := NewRepository(r.db.db, r.db.dialect.d)
	cat := "tv"
	size := int64(100)
	item := &ImportQueueItem{NzbPath: name + ".nzb", Category: &cat, Priority: QueuePriorityNormal,
		Status: QueueStatusProcessing, MaxRetries: 1, FileSize: &size, SkipArrNotification: skip}
	require.NoError(t, repo.AddToQueue(ctx, item))
	nzb := item.ID
	require.NoError(t, repo.AddImportHistory(ctx, &ImportHistory{NzbID: &nzb, NzbName: name + ".nzb", FileName: "a.mkv",
		FileSize: size, VirtualPath: "tv/" + name, Category: &cat}))
	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, QueueStatusCompleted, nil))
	return holdFixture{repo: repo, bund: r, id: item.ID}
}

func (f holdFixture) historyStatuses(t *testing.T) map[int64]QueueStatus {
	t.Helper()
	rows, err := f.repo.ListSABnzbdHistory(context.Background(), "", 100, 0)
	require.NoError(t, err)
	out := map[int64]QueueStatus{}
	for _, r := range rows {
		out[r.ID] = r.Status
	}
	n, err := f.repo.CountSABnzbdHistory(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, len(out), n)
	return out
}

func (f holdFixture) activeIDs(t *testing.T, withHeld bool) map[int64]bool {
	t.Helper()
	list := f.repo.ListActiveQueueItems
	count := f.repo.CountActiveQueueItems
	if withHeld {
		list, count = f.repo.ListActiveQueueItemsWithHeld, f.repo.CountActiveQueueItemsWithHeld
	}
	items, err := list(context.Background(), "", "", 100, 0, "updated_at", "desc")
	require.NoError(t, err)
	n, err := count(context.Background(), "", "")
	require.NoError(t, err)
	require.Equal(t, len(items), n)
	out := map[int64]bool{}
	for _, it := range items {
		out[it.ID] = it.PeerHold
	}
	return out
}

func TestPeerHold_PendingIsInQueueNotHistory(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		f := newHoldFixture(t, r, "pending", false)
		require.Contains(t, f.historyStatuses(t), f.id, "an ordinary completed item is in the history")

		require.NoError(t, r.SetQueueHold(ctx, f.id, true))
		assert.NotContains(t, f.historyStatuses(t), f.id, "a held item must not be in the history")
		assert.Equal(t, map[int64]bool{f.id: true}, f.activeIDs(t, true), "a held item is in the queue, marked held")
		assert.Empty(t, f.activeIDs(t, false), "the plain active list is unchanged")
		got, err := f.repo.GetQueueItem(ctx, f.id)
		require.NoError(t, err)
		assert.True(t, got.PeerHold)
		assert.False(t, got.SkipArrNotification, "the hold is not the user's skip flag")
	})
}

func TestPeerHold_PassedMovesToHistory(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		f := newHoldFixture(t, r, "passed", false)
		require.NoError(t, r.SetQueueHold(ctx, f.id, true))
		require.NoError(t, r.SetQueueHold(ctx, f.id, false))
		assert.Equal(t, QueueStatusCompleted, f.historyStatuses(t)[f.id])
		assert.Empty(t, f.activeIDs(t, true))
	})
}

func TestPeerHold_FailedIsFailedInHistoryWithMessage(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		f := newHoldFixture(t, r, "failed", false)
		require.NoError(t, r.SetQueueHold(ctx, f.id, true))
		changed, err := r.FailHeldQueueItem(ctx, f.id, "peer import failed its full check")
		require.NoError(t, err)
		require.True(t, changed)
		assert.Equal(t, QueueStatusFailed, f.historyStatuses(t)[f.id])
		rows, err := f.repo.ListSABnzbdFailedHistory(ctx, "", 10)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].ErrorMessage)
		assert.Equal(t, "peer import failed its full check", *rows[0].ErrorMessage)
		assert.Empty(t, f.activeIDs(t, true))

		// The provisional history row is gone: when the retention cleanup later
		// deletes the failed queue row, nothing resurfaces as completed.
		require.NoError(t, f.repo.RemoveFromQueue(ctx, f.id))
		assert.NotContains(t, f.historyStatuses(t), f.id)

		changed, err = r.FailHeldQueueItem(ctx, f.id, "again")
		require.NoError(t, err)
		assert.False(t, changed, "only a held item can be failed")
	})
}

func TestPeerHold_RequeueTurnsAHeldItemIntoAPendingOneWithoutHistory(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		f := newHoldFixture(t, r, "requeue", false)
		changed, err := r.RequeueHeldQueueItem(ctx, f.id)
		require.NoError(t, err)
		assert.False(t, changed, "only a held item can be requeued")

		require.NoError(t, r.SetQueueHold(ctx, f.id, true))
		changed, err = r.RequeueHeldQueueItem(ctx, f.id)
		require.NoError(t, err)
		require.True(t, changed)
		got, err := f.repo.GetQueueItem(ctx, f.id)
		require.NoError(t, err)
		assert.Equal(t, QueueStatusPending, got.Status)
		assert.False(t, got.PeerHold)
		assert.NotContains(t, f.historyStatuses(t), f.id, "the provisional history is gone")
	})
}

func TestPeerHold_SettledHoldsAreOnlyFinishedImportsOwingNoARR(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		f := newHoldFixture(t, r, "settled", false)
		require.NoError(t, r.SetQueueHold(ctx, f.id, true))
		row := bundleRow("m/a.mkv", f.id)
		row.ArrOwed = false
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{row}))
		ids, err := r.SettledHolds(ctx)
		require.NoError(t, err)
		assert.Empty(t, ids, "a pending, uncommitted row is not settled")
		_, err = r.MarkPassed(ctx, "m/a.mkv")
		require.NoError(t, err)
		require.NoError(t, r.CommitRows(ctx, []string{"m/a.mkv"}))
		ids, err = r.SettledHolds(ctx)
		require.NoError(t, err)
		assert.Equal(t, []int64{f.id}, ids)
	})
}

func TestPeerHold_UserSkipStaysHidden(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		f := newHoldFixture(t, r, "skip", true)
		require.NoError(t, r.SetQueueHold(ctx, f.id, true))
		require.NoError(t, r.SetQueueHold(ctx, f.id, false))
		assert.NotContains(t, f.historyStatuses(t), f.id, "the user's skip is still honoured after the hold ends")

		require.NoError(t, r.SetQueueHold(ctx, f.id, true))
		changed, err := r.FailHeldQueueItem(ctx, f.id, "x")
		require.NoError(t, err)
		require.True(t, changed)
		assert.NotContains(t, f.historyStatuses(t), f.id, "a failed skipped item stays hidden")
	})
}

func TestPeerBundleImport_CommitMarkerAndClaimGuard(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", 5), bundleRow("m/b.mkv", 5)}))
		_, err := r.MarkPassed(ctx, "m/a.mkv")
		require.NoError(t, err)
		_, err = r.MarkPassed(ctx, "m/b.mkv")
		require.NoError(t, err)

		un, err := r.ListUncommitted(ctx, 10)
		require.NoError(t, err)
		assert.Len(t, un, 2, "rows start uncommitted")
		_, claimed, err := r.ClaimAnnouncement(ctx, 5)
		require.NoError(t, err)
		assert.False(t, claimed, "an unfinished write is never announced")
		ids, err := r.QueuesAwaitingAnnouncement(ctx)
		require.NoError(t, err)
		assert.Empty(t, ids)

		require.NoError(t, r.CommitRows(ctx, []string{"/m/a.mkv", "m/b.mkv"}))
		un, err = r.ListUncommitted(ctx, 10)
		require.NoError(t, err)
		assert.Empty(t, un)
		byQ, err := r.ListByQueue(ctx, 5)
		require.NoError(t, err)
		require.Len(t, byQ, 2)
		assert.True(t, byQ[0].Committed)
		_, claimed, err = r.ClaimAnnouncement(ctx, 5)
		require.NoError(t, err)
		assert.True(t, claimed)

		page, err := r.ListAfter(ctx, "m/a.mkv", 10)
		require.NoError(t, err)
		require.Len(t, page, 1)
		assert.Equal(t, "m/b.mkv", page[0].FilePath)
	})
}

func TestPeerBundleImport_OrphanedHolds(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		referenced := newHoldFixture(t, r, "referenced", false)
		orphan := newHoldFixture(t, r, "orphan", false)
		plain := newHoldFixture(t, r, "plain", false) // never held
		require.NoError(t, r.SetQueueHold(ctx, referenced.id, true))
		require.NoError(t, r.SetQueueHold(ctx, orphan.id, true))
		require.NoError(t, r.Upsert(ctx, []PeernetBundleImport{bundleRow("m/a.mkv", referenced.id)}))

		got, err := r.OrphanedHolds(ctx)
		require.NoError(t, err)
		assert.Equal(t, []int64{orphan.id}, got, "only a held item nothing refers to")
		_ = plain
	})
}
