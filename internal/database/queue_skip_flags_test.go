package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddToQueue_SkipFlags(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) {
		testAddToQueueSkipFlags(t, openMigratedTo(t, 40), DialectSQLite)
	})
	t.Run("Postgres", func(t *testing.T) {
		testAddToQueueSkipFlags(t, openPostgresLatest(t), DialectPostgres)
	})
}

func testAddToQueueSkipFlags(t *testing.T, db *sql.DB, dialect Dialect) {
	t.Helper()
	ctx := context.Background()
	querier := newDialectAwareDB(db, dialect)

	for _, repository := range []struct {
		name            string
		add             func(context.Context, *ImportQueueItem) error
		mutableStatus   QueueStatus
		protectedStatus QueueStatus
	}{
		{"Repository", NewRepository(db, dialect).AddToQueue, QueueStatusPending, QueueStatusCompleted},
		{"QueueRepository", NewQueueRepository(db, dialect).AddToQueue, QueueStatusCompleted, QueueStatusPending},
	} {
		t.Run(repository.name, func(t *testing.T) {
			for _, flags := range []struct {
				name  string
				arr   bool
				links bool
			}{
				{"neither", false, false},
				{"arr only", true, false},
				{"links only", false, true},
				{"both", true, true},
			} {
				for _, existing := range []bool{false, true} {
					name := flags.name + "/insert"
					if existing {
						name = flags.name + "/upsert"
					}
					t.Run(name, func(t *testing.T) {
						path := t.Name() + ".nzb"
						if existing {
							_, err := querier.ExecContext(ctx, `INSERT INTO import_queue
								(nzb_path, status, skip_arr_notification, skip_post_import_links)
								VALUES (?, ?, ?, ?)`, path, repository.mutableStatus, !flags.arr, !flags.links)
							require.NoError(t, err)
						}
						item := &ImportQueueItem{
							NzbPath: path, Status: QueueStatusPending, Priority: QueuePriorityNormal,
							MaxRetries: 3, SkipArrNotification: flags.arr, SkipPostImportLinks: flags.links,
						}
						require.NoError(t, repository.add(ctx, item))
						stored, err := NewQueueRepository(db, dialect).GetQueueItemByNzbPath(ctx, path)
						require.NoError(t, err)
						require.NotNil(t, stored)
						assert.Equal(t, flags.arr, stored.SkipArrNotification)
						assert.Equal(t, flags.links, stored.SkipPostImportLinks)
					})
				}
			}

			for _, status := range []QueueStatus{QueueStatusProcessing, repository.protectedStatus} {
				t.Run("preserves flags for "+string(status), func(t *testing.T) {
					path := t.Name() + ".nzb"
					_, err := querier.ExecContext(ctx, `INSERT INTO import_queue
						(nzb_path, status, skip_arr_notification, skip_post_import_links)
						VALUES (?, ?, TRUE, TRUE)`, path, status)
					require.NoError(t, err)
					require.NoError(t, repository.add(ctx, &ImportQueueItem{
						NzbPath: path, Status: QueueStatusPending, Priority: QueuePriorityNormal, MaxRetries: 3,
					}))
					stored, err := NewQueueRepository(db, dialect).GetQueueItemByNzbPath(ctx, path)
					require.NoError(t, err)
					require.NotNil(t, stored)
					assert.True(t, stored.SkipArrNotification)
					assert.True(t, stored.SkipPostImportLinks)
					assert.Equal(t, status, stored.Status)
				})
			}
		})
	}
}
