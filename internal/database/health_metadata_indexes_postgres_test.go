package database

import (
	"context"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func TestHealthMetadataIndexes_Postgres(t *testing.T) {
	db := openPostgresLatest(t)
	repo := NewHealthRepository(db, DialectPostgres)
	ctx := context.Background()
	// Populate healthy rows so the planner assesses ID selectivity rather than
	// choosing the status index simply because the table is empty.
	_, err := db.ExecContext(ctx, `INSERT INTO file_health (file_path, status, metadata)
  SELECT 'index-fixture-' || n, 'healthy', jsonb_build_object('tmdbId', n + 10000, 'tvdbId', n + 10000)
  FROM generate_series(1, 2048) AS n`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ANALYZE file_health")
	require.NoError(t, err)
	for _, tc := range []struct {
		name, key, legacy, nested string
		indexes                   []string
	}{
		{"movie", "tmdbId", "tmdb_id", "movie", []string{
			"idx_file_health_metadata_tmdb_id", "idx_file_health_metadata_legacy_tmdb_id",
			"idx_file_health_metadata_movie_tmdb_id", "idx_file_health_metadata_movie_legacy_tmdb_id",
		}},
		{"series", "tvdbId", "tvdb_id", "series", []string{
			"idx_file_health_metadata_tvdb_id", "idx_file_health_metadata_legacy_tvdb_id",
			"idx_file_health_metadata_series_tvdb_id", "idx_file_health_metadata_series_legacy_tvdb_id",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			// Disable sequential scans locally to verify that every supported
			// branch can use its index.
			_, err = tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off")
			require.NoError(t, err)
			predicate, args := repo.healthMetadataIDPredicate(tc.key, tc.legacy, tc.nested, 123)
			var plan string
			err = tx.QueryRowContext(ctx, repo.dialect.q("EXPLAIN (FORMAT JSON) SELECT id FROM file_health WHERE status = 'healthy' AND "+predicate), args...).Scan(&plan)
			require.NoError(t, err)
			require.Contains(t, plan, "BitmapOr")
			for _, index := range tc.indexes {
				require.Contains(t, plan, index)
			}
		})
	}
	// Goose must remove all metadata indexes on rollback and recreate them on up.
	require.NoError(t, goose.DownTo(db, "migrations/postgres", 40))
	var count int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname LIKE 'idx_file_health_metadata_%'").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, goose.Up(db, "migrations/postgres"))
	require.NoError(t, db.QueryRow("SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname LIKE 'idx_file_health_metadata_%'").Scan(&count))
	require.Equal(t, 8, count)
}

func TestGetUnhealthyFilesWithJSONBMetadata_Postgres(t *testing.T) {
	db := openPostgresLatest(t)
	_, err := db.Exec(`INSERT INTO file_health (file_path, status, metadata, scheduled_check_at)
  VALUES ('due.mkv', 'healthy', '{"tmdbId":123}', NOW() - INTERVAL '1 minute')`)
	require.NoError(t, err)
	files, err := NewHealthRepository(db, DialectPostgres).GetUnhealthyFiles(context.Background(), 10, "ARR", "/library", 5)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, "due.mkv", files[0].FilePath)
}
