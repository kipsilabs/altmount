package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpandNzbWrapper(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit and retry", true: "rollback"}[fail], func(t *testing.T) {
			db := openMigratedTo(t, 40)
			repo := NewQueueRepository(db, DialectSQLite)
			ctx := context.Background()
			parent := &ImportQueueItem{NzbPath: "parent.nzb", Status: QueueStatusProcessing, Priority: QueuePriorityNormal, MaxRetries: 3}
			require.NoError(t, repo.AddToQueue(ctx, parent))
			children := []*ImportQueueItem{
				{NzbPath: "one.nzb", Status: QueueStatusPending, Priority: QueuePriorityNormal, MaxRetries: 3},
				{NzbPath: "two.nzb", Status: QueueStatusPending, Priority: QueuePriorityNormal, MaxRetries: 3},
			}
			if fail {
				children[1].Status = "invalid-status"
			}
			err := repo.ExpandNzbWrapper(ctx, parent, children)
			stored, readErr := repo.GetQueueItem(ctx, parent.ID)
			require.NoError(t, readErr)
			if fail {
				require.Error(t, err)
				require.Equal(t, QueueStatusProcessing, stored.Status)
				child, err := repo.GetQueueItemByNzbPath(ctx, "one.nzb")
				require.NoError(t, err)
				require.Nil(t, child)
			} else {
				require.NoError(t, err)
				require.Equal(t, QueueStatusProcessing, stored.Status)
				require.Nil(t, stored.CompletedAt)
				require.Equal(t, children[0].NzbPath, stored.NzbPath)
				require.Equal(t, parent.ID, children[0].ID)
				// A retried expansion cannot reset a completed child to pending.
				require.NoError(t, repo.UpdateQueueItemStatus(ctx, children[0].ID, QueueStatusCompleted, nil))
				require.NoError(t, repo.ExpandNzbWrapper(ctx, parent, children))
				child, err := repo.GetQueueItem(ctx, children[0].ID)
				require.NoError(t, err)
				require.Equal(t, QueueStatusCompleted, child.Status)
			}
		})
	}
}
