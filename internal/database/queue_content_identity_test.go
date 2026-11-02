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
		NzbPath:         "/queue/completed.nzb",
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

	got, err := repo.GetContentIdentity(ctx, completed.ID)
	require.NoError(t, err)
	require.Equal(t, identity, got)

	require.NoError(t, repo.SetContentIdentity(ctx, pending.ID, "cafebabe"))
	got, err = repo.GetContentIdentity(ctx, pending.ID)
	require.NoError(t, err)
	require.Equal(t, "cafebabe", got)
}

func strPtr(s string) *string { return &s }
