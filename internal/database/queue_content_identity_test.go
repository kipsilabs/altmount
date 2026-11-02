package database

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestFindCompletedByContentIdentity(t *testing.T) {
	sqlDB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer sqlDB.Close()
	setupQueueSchema(t, sqlDB)

	repo := NewQueueRepository(sqlDB, DialectSQLite)
	ctx := context.Background()

	size := int64(1234)
	identity := "deadbeef"
	completed := &ImportQueueItem{
		NzbPath:         "/queue/7-completed.nzb",
		StoragePath:     strPtr("/complete/Release"),
		Status:          QueueStatusCompleted,
		Priority:        QueuePriorityNormal,
		MaxRetries:      3,
		FileSize:        &size,
		ContentIdentity: &identity,
	}
	require.NoError(t, repo.AddToQueue(ctx, completed))
	require.NoError(t, repo.SetContentIdentity(ctx, completed.ID, identity))
	require.NoError(t, repo.UpdateQueueItemStatus(ctx, completed.ID, QueueStatusCompleted, nil))

	pending := &ImportQueueItem{
		NzbPath:         "/queue/pending.nzb",
		Status:          QueueStatusPending,
		Priority:        QueuePriorityNormal,
		MaxRetries:      3,
		FileSize:        &size,
		ContentIdentity: &identity,
	}
	require.NoError(t, repo.AddToQueue(ctx, pending))
	require.NoError(t, repo.SetContentIdentity(ctx, pending.ID, identity))

	// A different NZB path with the same identity and size matches the
	// completed row.
	match, err := repo.FindCompletedByContentIdentity(ctx, identity, size, "/queue/new.nzb")
	require.NoError(t, err)
	require.NotNil(t, match)
	require.Equal(t, completed.ID, match.ID)
	require.Equal(t, "/complete/Release", match.StoragePath)

	// The completed row's own path is an explicit requeue, not a duplicate.
	match, err = repo.FindCompletedByContentIdentity(ctx, identity, size, completed.NzbPath)
	require.NoError(t, err)
	require.Nil(t, match)

	// Size is a hard guard: a different size never matches, even with the same
	// fingerprint (which should not happen, but the query must enforce it).
	match, err = repo.FindCompletedByContentIdentity(ctx, identity, size+1, "/queue/new.nzb")
	require.NoError(t, err)
	require.Nil(t, match)

	// Pending imports are not usable as de-duplication targets because they do
	// not own a storage path yet, even when their identity matches.
	require.NoError(t, repo.SetContentIdentity(ctx, pending.ID, "pending-only"))
	match, err = repo.FindCompletedByContentIdentity(ctx, "pending-only", size, "/queue/new.nzb")
	require.NoError(t, err)
	require.Nil(t, match)
	require.NoError(t, repo.SetContentIdentity(ctx, pending.ID, identity))

	// Empty identity never matches.
	match, err = repo.FindCompletedByContentIdentity(ctx, "", size, "/queue/new.nzb")
	require.NoError(t, err)
	require.Nil(t, match)

	// A re-upload keeps its basename but gets a new temp path. That is an
	// explicit reprocess and must not be short-circuited to the completed row.
	match, err = repo.FindCompletedByContentIdentity(ctx, identity, size, "/uploads/completed.nzb")
	require.NoError(t, err)
	require.Nil(t, match)
	// The plain "<dir>/<basename>" suffix matches too.
	match, err = repo.FindCompletedByContentIdentity(ctx, identity, size, "/uploads/7-completed.nzb")
	require.NoError(t, err)
	require.Nil(t, match)

	// In-flight rows are visible to admission so a duplicate can be kept pending
	// until the first download finishes.
	inflight, err := repo.FindInFlightByContentIdentity(ctx, identity, size, "/queue/new.nzb")
	require.NoError(t, err)
	require.NotNil(t, inflight)
	require.Equal(t, pending.ID, inflight.ID)
	require.Equal(t, QueueStatusPending, inflight.Status)

	inflight, err = repo.FindInFlightByContentIdentity(ctx, identity, size, "/queue/pending.nzb")
	require.NoError(t, err)
	require.Nil(t, inflight, "same path is an explicit retry, not an in-flight duplicate")

	inflight, err = repo.FindInFlightByContentIdentity(ctx, "", size, "/queue/new.nzb")
	require.NoError(t, err)
	require.Nil(t, inflight)

	got, err := repo.GetContentIdentity(ctx, completed.ID)
	require.NoError(t, err)
	require.Equal(t, identity, got)

	require.NoError(t, repo.SetContentIdentity(ctx, pending.ID, "cafebabe"))
	got, err = repo.GetContentIdentity(ctx, pending.ID)
	require.NoError(t, err)
	require.Equal(t, "cafebabe", got)
}

func strPtr(s string) *string { return &s }

// TestClaimNextQueueItemSerialisesIdenticalReleases pins the claim-time half of
// admission de-duplication: two pending rows with the same identity and size are
// never claimed together, so the second cannot start a duplicate download. Once
// the first completes, the second becomes claimable again so it can be adopted
// (or, if the first failed, retried).
func TestClaimNextQueueItemSerialisesIdenticalReleases(t *testing.T) {
	sqlDB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer sqlDB.Close()
	setupQueueSchema(t, sqlDB)

	repo := NewQueueRepository(sqlDB, DialectSQLite)
	ctx := context.Background()
	size := int64(4096)
	identity := "same-release"

	first := &ImportQueueItem{
		NzbPath:         "/queue/first.nzb",
		Status:          QueueStatusPending,
		Priority:        QueuePriorityHigh,
		MaxRetries:      3,
		FileSize:        &size,
		ContentIdentity: &identity,
	}
	second := &ImportQueueItem{
		NzbPath:         "/queue/second.nzb",
		Status:          QueueStatusPending,
		Priority:        QueuePriorityHigh,
		MaxRetries:      3,
		FileSize:        &size,
		ContentIdentity: &identity,
	}
	other := &ImportQueueItem{
		NzbPath:    "/queue/other.nzb",
		Status:     QueueStatusPending,
		Priority:   QueuePriorityLow,
		MaxRetries: 3,
	}

	require.NoError(t, repo.AddToQueue(ctx, first))
	require.NoError(t, repo.SetContentIdentity(ctx, first.ID, identity))
	require.NoError(t, repo.AddToQueue(ctx, second))
	require.NoError(t, repo.SetContentIdentity(ctx, second.ID, identity))
	require.NoError(t, repo.AddToQueue(ctx, other))

	claimed, err := repo.ClaimNextQueueItem(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, first.ID, claimed.ID, "only the oldest pending row of an identical group is claimable")
	require.Equal(t, identity, *claimed.ContentIdentity)

	// While the first is processing, the identical second is held back. The
	// unrelated row is still claimable.
	claimed, err = repo.ClaimNextQueueItem(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, other.ID, claimed.ID)

	claimed, err = repo.ClaimNextQueueItem(ctx)
	require.NoError(t, err)
	require.Nil(t, claimed, "identical release must not be claimed while its sibling is processing")

	// Completing the first releases the second for adoption.
	require.NoError(t, repo.AddStoragePath(ctx, first.ID, "/complete/Release"))
	require.NoError(t, repo.UpdateQueueItemStatus(ctx, first.ID, QueueStatusCompleted, nil))

	claimed, err = repo.ClaimNextQueueItem(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, second.ID, claimed.ID)
}
