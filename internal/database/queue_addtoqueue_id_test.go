package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AddToQueue must leave the id of the row with the item's path in item.ID
// whether it inserted, updated or (while the row is pending or processing)
// left the existing row alone; a stale last-insert id or zero would be an
// invalid foreign key for every later write that refers to the item.
func TestAddToQueueResolvesTheExistingRowOnEveryDialect(t *testing.T) {
	backends := map[string]func(t *testing.T) *QueueRepository{
		"sqlite": func(t *testing.T) *QueueRepository {
			db, err := sql.Open("sqlite3", ":memory:")
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			db.SetMaxOpenConns(1)
			setupQueueSchema(t, db)
			return NewQueueRepository(db, DialectSQLite)
		},
	}
	if hasPostgresDSN() {
		backends["postgres"] = func(t *testing.T) *QueueRepository {
			return NewQueueRepository(openPostgresLatest(t), DialectPostgres)
		}
	}
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			r := mk(t)
			first := &ImportQueueItem{NzbPath: "peer://a.nzb", Priority: QueuePriorityNormal, Status: QueueStatusCompleted, MaxRetries: 1}
			require.NoError(t, r.AddToQueue(ctx, first))
			require.NotZero(t, first.ID)
			// Other rows move the last-insert id.
			for _, p := range []string{"/x/1.nzb", "/x/2.nzb"} {
				require.NoError(t, r.AddToQueue(ctx, &ImportQueueItem{NzbPath: p, Priority: QueuePriorityNormal, Status: QueueStatusPending, MaxRetries: 1}))
			}
			again := &ImportQueueItem{NzbPath: "peer://a.nzb", Priority: QueuePriorityNormal, Status: QueueStatusCompleted, MaxRetries: 1}
			require.NoError(t, r.AddToQueue(ctx, again))
			assert.Equal(t, first.ID, again.ID, "an update resolves to the existing row")

			busy := &ImportQueueItem{NzbPath: "/x/1.nzb", Priority: QueuePriorityNormal, Status: QueueStatusPending, MaxRetries: 1}
			require.NoError(t, r.AddToQueue(ctx, busy)) // left alone while pending
			require.NoError(t, r.AddToQueue(ctx, &ImportQueueItem{NzbPath: "/x/3.nzb", Priority: QueuePriorityNormal, Status: QueueStatusPending, MaxRetries: 1}))
			busy2 := &ImportQueueItem{NzbPath: "/x/1.nzb", Priority: QueuePriorityNormal, Status: QueueStatusPending, MaxRetries: 1}
			require.NoError(t, r.AddToQueue(ctx, busy2))
			assert.NotZero(t, busy2.ID)
			assert.Equal(t, busy.ID, busy2.ID, "a skipped upsert resolves to the existing row")
		})
	}
}
