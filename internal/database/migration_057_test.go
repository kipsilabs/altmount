package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration057UpgradesPeerNetContentIndex(t *testing.T) {
	t.Run("missing table with migration 044 already applied", func(t *testing.T) {
		db := openMigratedTo(t, 56)
		_, err := db.Exec("DROP TABLE peernet_content_index")
		require.NoError(t, err)

		require.NoError(t, runMigrations(db, DialectSQLite))

		_, err = db.Exec("INSERT INTO peernet_content_index (file_path, file_id, group_key) VALUES ('movie.mkv', 'file-1', 'group-1')")
		require.NoError(t, err)
		var scheme int
		require.NoError(t, db.QueryRow("SELECT release_scheme FROM peernet_content_index WHERE file_path = 'movie.mkv'").Scan(&scheme))
		require.Zero(t, scheme)
	})

	t.Run("existing rows are preserved and backfilled", func(t *testing.T) {
		db := openMigratedTo(t, 56)
		_, err := db.Exec("INSERT INTO peernet_content_index (file_path, file_id, release_id) VALUES ('movie.mkv', 'file-1', 'release-1')")
		require.NoError(t, err)

		require.NoError(t, runMigrations(db, DialectSQLite))

		var releaseID string
		var scheme int
		require.NoError(t, db.QueryRow("SELECT release_id, release_scheme FROM peernet_content_index WHERE file_path = 'movie.mkv'").Scan(&releaseID, &scheme))
		require.Equal(t, "release-1", releaseID)
		require.Equal(t, 1, scheme)
	})
}
