package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepositoryAddToQueue_Indexer(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) {
		testRepositoryAddToQueueIndexer(t, openMigratedTo(t, 40), DialectSQLite)
	})
	t.Run("Postgres", func(t *testing.T) {
		testRepositoryAddToQueueIndexer(t, openPostgresLatest(t), DialectPostgres)
	})
}

func testRepositoryAddToQueueIndexer(t *testing.T, db *sql.DB, dialect Dialect) {
	t.Helper()
	ctx := context.Background()
	repo := NewRepository(db, dialect)
	indexer := "new-indexer"

	tests := []struct {
		name     string
		existing bool
		indexer  *string
		want     sql.NullString
	}{
		{"insert with indexer", false, &indexer, sql.NullString{String: "new-indexer", Valid: true}},
		{"insert without indexer", false, nil, sql.NullString{}},
		{"upsert without indexer retains existing", true, nil, sql.NullString{String: "original-indexer", Valid: true}},
		{"upsert with indexer replaces existing", true, &indexer, sql.NullString{String: "new-indexer", Valid: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nzbPath := tt.name + ".nzb"
			if tt.existing {
				_, err := repo.db.ExecContext(ctx,
					`INSERT INTO import_queue (nzb_path, status, indexer) VALUES (?, 'pending', 'original-indexer')`, nzbPath)
				require.NoError(t, err)
			}

			item := &ImportQueueItem{
				NzbPath:    nzbPath,
				Status:     QueueStatusPending,
				Priority:   QueuePriorityNormal,
				MaxRetries: 3,
				Indexer:    tt.indexer,
			}
			require.NoError(t, repo.AddToQueue(ctx, item))

			var got sql.NullString
			require.NoError(t, repo.db.QueryRowContext(ctx,
				`SELECT indexer FROM import_queue WHERE nzb_path = ?`, nzbPath).Scan(&got))
			assert.Equal(t, tt.want, got)
		})
	}
}
