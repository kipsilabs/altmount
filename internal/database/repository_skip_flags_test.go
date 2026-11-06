package database

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// Repository.AddToQueue stores skip_arr_notification and skip_post_import_links
// (#964). The Repository read paths must return them too: the manual retry
// (POST /api/queue/{id}/retry) reads the item through GetQueueItem and hands it
// to the post-processor, and the SABnzbd queue and nzo_ids history lookups
// hide skipped items based on what ListActiveQueueItems, GetQueueItem and
// GetQueueItemByDownloadID return.
func TestRepositoryAddToQueue_PersistsSkipFlags(t *testing.T) {
	cases := []struct {
		name      string
		skipArr   bool
		skipLinks bool
	}{
		{"both true", true, true},
		{"arr only", true, false},
		{"links only", false, true},
		{"both false", false, false},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("sqlite3", "file:test_repo_skip_flags_"+string(rune('a'+i))+"?mode=memory&cache=shared")
			require.NoError(t, err)
			defer db.Close()
			setupQueueSchema(t, db)

			repo := NewRepository(db, DialectSQLite)
			ctx := context.Background()
			downloadID := "dl-skip-" + string(rune('a'+i))
			item := &ImportQueueItem{
				NzbPath:             "/tmp/skip-flags.nzb",
				DownloadID:          &downloadID,
				Priority:            QueuePriorityNormal,
				Status:              QueueStatusPending,
				MaxRetries:          3,
				SkipArrNotification: tc.skipArr,
				SkipPostImportLinks: tc.skipLinks,
			}
			require.NoError(t, repo.AddToQueue(ctx, item))

			var gotArr, gotLinks bool
			require.NoError(t, db.QueryRow(
				"SELECT skip_arr_notification, skip_post_import_links FROM import_queue WHERE id = ?", item.ID,
			).Scan(&gotArr, &gotLinks))
			require.Equal(t, tc.skipArr, gotArr, "skip_arr_notification column")
			require.Equal(t, tc.skipLinks, gotLinks, "skip_post_import_links column")

			byID, err := repo.GetQueueItem(ctx, item.ID)
			require.NoError(t, err)
			require.NotNil(t, byID)
			require.Equal(t, tc.skipArr, byID.SkipArrNotification, "GetQueueItem SkipArrNotification")
			require.Equal(t, tc.skipLinks, byID.SkipPostImportLinks, "GetQueueItem SkipPostImportLinks")

			byDL, err := repo.GetQueueItemByDownloadID(ctx, downloadID)
			require.NoError(t, err)
			require.NotNil(t, byDL)
			require.Equal(t, tc.skipArr, byDL.SkipArrNotification, "GetQueueItemByDownloadID SkipArrNotification")
			require.Equal(t, tc.skipLinks, byDL.SkipPostImportLinks, "GetQueueItemByDownloadID SkipPostImportLinks")

			listed, err := repo.ListQueueItems(ctx, nil, "", "", 10, 0, "created_at", "desc")
			require.NoError(t, err)
			require.Len(t, listed, 1)
			require.Equal(t, tc.skipArr, listed[0].SkipArrNotification, "ListQueueItems SkipArrNotification")
			require.Equal(t, tc.skipLinks, listed[0].SkipPostImportLinks, "ListQueueItems SkipPostImportLinks")

			active, err := repo.ListActiveQueueItems(ctx, "", "", 10, 0, "created_at", "desc")
			require.NoError(t, err)
			require.Len(t, active, 1)
			require.Equal(t, tc.skipArr, active[0].SkipArrNotification, "ListActiveQueueItems SkipArrNotification")
			require.Equal(t, tc.skipLinks, active[0].SkipPostImportLinks, "ListActiveQueueItems SkipPostImportLinks")
		})
	}
}
