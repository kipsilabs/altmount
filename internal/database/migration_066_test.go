package database

import (
	"database/sql"
	"math"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// latestSQLiteMigrationVersion is the version a fully migrated database must
// reach. Reading it from the embedded files keeps the assertion true when a
// migration after 066 is added.
func latestSQLiteMigrationVersion(t *testing.T) int64 {
	t.Helper()

	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect("sqlite3"))
	migrations, err := goose.CollectMigrations("migrations/sqlite", 0, math.MaxInt64)
	require.NoError(t, err)
	require.NotEmpty(t, migrations)
	return migrations[len(migrations)-1].Version
}

func sqliteIndexExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()

	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", name,
	).Scan(&count))
	return count == 1
}

// TestMigration066ContentIdentityCleanInstall is the control: a new database
// still executes 066 and records it.
func TestMigration066ContentIdentityCleanInstall(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "clean.db")})
	require.NoError(t, err, "a clean install must boot")
	t.Cleanup(func() { _ = db.Close() })
	conn := db.Connection()

	version, err := goose.GetDBVersion(conn)
	require.NoError(t, err)
	assert.Equal(t, latestSQLiteMigrationVersion(t), version)

	assert.True(t, hasColumn(conn, DialectSQLite, "import_queue", "content_identity"))
	assert.True(t, sqliteIndexExists(t, conn, "idx_queue_content_identity"))
}

// TestMigration066ContentIdentityPreApplied covers the drift that boot-looped
// production: an earlier identity build added the column (and possibly its
// index) but never recorded migration 066. goose must record 066 instead of
// running the ALTER again, then continue the chain, and the server must boot.
func TestMigration066ContentIdentityPreApplied(t *testing.T) {
	for _, tt := range []struct {
		name      string
		withIndex bool
	}{
		{name: "column only", withIndex: false},
		{name: "column and index", withIndex: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := seedPreAppliedContentIdentity(t, tt.withIndex)

			// NewDB is the boot path; an error here is what exits the process.
			db, err := NewDB(Config{Type: "sqlite", DatabasePath: dbPath})
			require.NoError(t, err, "an existing content_identity column must not abort startup")
			t.Cleanup(func() { _ = db.Close() })
			conn := db.Connection()

			version, err := goose.GetDBVersion(conn)
			require.NoError(t, err)
			assert.Equal(t, latestSQLiteMigrationVersion(t), version,
				"the migration chain must continue past 066")

			assert.True(t, sqliteIndexExists(t, conn, "idx_queue_content_identity"))

			var recorded int
			require.NoError(t, conn.QueryRow(
				"SELECT COUNT(*) FROM goose_db_version WHERE version_id = 66",
			).Scan(&recorded))
			assert.Equal(t, 1, recorded, "066 must be recorded exactly once")

			var identity string
			require.NoError(t, conn.QueryRow(
				"SELECT content_identity FROM import_queue WHERE nzb_path = ?", "/nzb/one.nzb",
			).Scan(&identity))
			assert.Equal(t, "identity-one", identity, "existing queue rows must survive the boot")
		})
	}
}

// seedPreAppliedContentIdentity builds a database at goose version 65 and adds
// the schema object(s) from 066 without recording the migration.
func seedPreAppliedContentIdentity(t *testing.T, withIndex bool) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "drift.db")
	seed, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	seed.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = seed.Close() })

	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect("sqlite3"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpTo(seed, "migrations/sqlite", 65))

	_, err = seed.Exec("ALTER TABLE import_queue ADD COLUMN content_identity TEXT DEFAULT NULL")
	require.NoError(t, err)
	if withIndex {
		_, err = seed.Exec("CREATE INDEX idx_queue_content_identity ON import_queue(content_identity, file_size)")
		require.NoError(t, err)
	}
	_, err = seed.Exec(
		"INSERT INTO import_queue (nzb_path, status, content_identity) VALUES ('/nzb/one.nzb', 'completed', 'identity-one')",
	)
	require.NoError(t, err)

	version, err := goose.GetDBVersion(seed)
	require.NoError(t, err)
	require.Equal(t, int64(65), version, "fixture must predate migration 066")
	require.NoError(t, seed.Close())

	return dbPath
}
