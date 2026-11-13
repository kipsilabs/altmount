package database

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// originBackend is one dialect's origin repository plus a statement runner for
// seeding the tables it reads.
type originBackend struct {
	repo  *PeernetOriginRepository
	db    *dialectAwareDB
	queue *QueueRepository
}

func (b *originBackend) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	_, err := b.db.ExecContext(context.Background(), q, args...)
	require.NoError(t, err)
}

func forEachOriginBackend(t *testing.T, fn func(t *testing.T, b *originBackend)) {
	t.Helper()
	mk := func(conn *sql.DB, d Dialect) *originBackend {
		return &originBackend{repo: NewPeernetOriginRepository(conn, d), db: newDialectAwareDB(conn, d), queue: NewQueueRepository(conn, d)}
	}
	t.Run("sqlite", func(t *testing.T) {
		db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		fn(t, mk(db.Connection(), DialectSQLite))
	})
	if hasPostgresDSN() {
		t.Run("postgres", func(t *testing.T) { fn(t, mk(openPostgresLatest(t), DialectPostgres)) })
	}
}

func TestPeernetOrigin_NothingKnown(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		o, err := b.repo.OriginForPath(context.Background(), "movies/none.mkv")
		require.NoError(t, err)
		assert.Equal(t, PeernetOrigin{}, o)
		_, err = b.repo.OriginForPath(context.Background(), "")
		assert.Error(t, err)
	})
}

func TestPeernetOrigin_HistoryOnly(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		b.exec(t, `INSERT INTO import_history (nzb_id, nzb_name, file_name, file_size, virtual_path, category, indexer, metadata)
			VALUES (NULL, 'Some.Movie.2020.nzb', 'a.mkv', 100, 'movies/a.mkv', 'movies', 'IndexerOne', '{"imdb_id":"tt1"}')`)
		// A later re-import of the same path wins.
		b.exec(t, `INSERT INTO import_history (nzb_id, nzb_name, file_name, file_size, virtual_path, category, indexer, completed_at)
			VALUES (NULL, 'Newer.nzb', 'a.mkv', 100, 'movies/a.mkv', 'films', 'IndexerTwo', '2999-01-01 00:00:00')`)
		o, err := b.repo.OriginForPath(context.Background(), "/movies\\a.mkv")
		require.NoError(t, err)
		assert.Equal(t, "Newer", o.Title)
		assert.Equal(t, "films", o.Category)
		assert.Equal(t, "IndexerTwo", o.Indexer)
		assert.Empty(t, o.QueueMetadata, "the newest history row has no metadata, and nothing else does")
	})
}

func TestPeernetOrigin_QueueFillsGaps(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		b.exec(t, `INSERT INTO import_queue (id, nzb_path, category, indexer, metadata) VALUES (7, '/nzbs/7-Queue.Title.nzb', 'tv', 'QueueIdx', '{"imdb_id":"tt9","type":"series","season":1,"episode":2}')`)
		b.exec(t, `INSERT INTO import_history (nzb_id, nzb_name, file_name, file_size, virtual_path, category, indexer)
			VALUES (7, '', 'a.mkv', 100, 'tv/a.mkv', NULL, 'Unknown')`)
		o, err := b.repo.OriginForPath(context.Background(), "tv/a.mkv")
		require.NoError(t, err)
		assert.Equal(t, "Queue.Title", o.Title, "queue file name without the queue id prefix and suffix")
		assert.Equal(t, "tv", o.Category)
		assert.Equal(t, "QueueIdx", o.Indexer, "Unknown is not an indexer")
		assert.JSONEq(t, `{"imdb_id":"tt9","type":"series","season":1,"episode":2}`, o.QueueMetadata)
	})
}

func TestPeernetOrigin_FileHealthIsLastResortForIndexerAndMetadata(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		b.exec(t, `INSERT INTO import_history (nzb_id, nzb_name, file_name, file_size, virtual_path, category, indexer)
			VALUES (NULL, 'X.nzb', 'a.mkv', 1, 'movies/a.mkv', 'movies', NULL)`)
		b.exec(t, `INSERT INTO file_health (file_path, status, indexer, metadata) VALUES ('movies/a.mkv', 'healthy', 'HealthIdx', '{"imdb_id":"tt5"}')`)
		o, err := b.repo.OriginForPath(context.Background(), "movies/a.mkv")
		require.NoError(t, err)
		assert.Equal(t, "HealthIdx", o.Indexer)
		assert.JSONEq(t, `{"imdb_id":"tt5"}`, o.QueueMetadata)
		assert.Equal(t, "movies", o.Category, "file_health has no category: it comes from history")
	})
}

func TestPeernetOrigin_FileHealthOnlyYieldsNoTitleOrCategory(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		b.exec(t, `INSERT INTO file_health (file_path, status, indexer) VALUES ('movies/b.mkv', 'healthy', 'HealthIdx')`)
		o, err := b.repo.OriginForPath(context.Background(), "movies/b.mkv")
		require.NoError(t, err)
		assert.Equal(t, PeernetOrigin{Indexer: "HealthIdx"}, o)
	})
}

func TestPeernetOrigin_PathIsAParameter(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		b.exec(t, `INSERT INTO import_history (nzb_name, file_name, virtual_path, category) VALUES ('T.nzb', 'f', 'it''s/a;DROP TABLE import_history;.mkv', 'c')`)
		o, err := b.repo.OriginForPath(context.Background(), "it's/a;DROP TABLE import_history;.mkv")
		require.NoError(t, err)
		assert.Equal(t, "c", o.Category)
	})
}

func TestPeernetOrigin_VirtualPathLookupUsesIndex(t *testing.T) {
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	rows, err := db.Connection().QueryContext(context.Background(),
		`EXPLAIN QUERY PLAN SELECT nzb_id, nzb_name, category, indexer, CAST(metadata AS TEXT) FROM import_history
		 WHERE virtual_path = ? ORDER BY completed_at DESC, id DESC LIMIT 1`, "movies/a.mkv")
	require.NoError(t, err)
	defer rows.Close()
	var plan string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan += detail + "\n"
	}
	assert.Contains(t, plan, "idx_import_history_virtual_path", plan)
}

// The importer records the release FOLDER as the history path of multi-file
// and archive imports, and the file itself only for single-file imports. These
// tests write the rows through the same repository method the importer uses.
func addHistory(t *testing.T, b *originBackend, path, name, category, indexer string, nzbID *int64) {
	t.Helper()
	require.NoError(t, b.queue.AddImportHistory(context.Background(), &ImportHistory{
		NzbID: nzbID, NzbName: name, FileName: "x", FileSize: 1, VirtualPath: path, Category: &category, Indexer: &indexer,
	}))
}

func TestPeernetOrigin_FolderKeyedHistoryResolvesFilesInsideIt(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		addHistory(t, b, "complete/Some.Release-GRP/", "Some.Release-GRP.nzb", "tv", "IdxOne", nil)
		for _, file := range []string{
			"complete/Some.Release-GRP/ep1.mkv",
			"/complete/Some.Release-GRP/Subs/en.srt",
		} {
			o, err := b.repo.OriginForPath(context.Background(), file)
			require.NoError(t, err)
			assert.Equal(t, "tv", o.Category, file)
			assert.Equal(t, "IdxOne", o.Indexer, file)
			assert.Equal(t, "Some.Release-GRP", o.Title, file)
		}
	})
}

func TestPeernetOrigin_NearestRowWinsAndSiblingsDoNotMatch(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		addHistory(t, b, "complete/Rel", "Rel.nzb", "folder-cat", "FolderIdx", nil)
		addHistory(t, b, "complete/Rel/a.mkv", "Rel.nzb", "file-cat", "FileIdx", nil)
		addHistory(t, b, "complete/Rel2", "Rel2.nzb", "other", "OtherIdx", nil)
		o, err := b.repo.OriginForPath(context.Background(), "complete/Rel/a.mkv")
		require.NoError(t, err)
		assert.Equal(t, "file-cat", o.Category, "an exact file row beats the folder row")
		o, err = b.repo.OriginForPath(context.Background(), "complete/Rel/b.mkv")
		require.NoError(t, err)
		assert.Equal(t, "folder-cat", o.Category)
		o, err = b.repo.OriginForPath(context.Background(), "complete/Rel22/b.mkv")
		require.NoError(t, err)
		assert.Equal(t, PeernetOrigin{}, o, "a sibling whose name merely starts the same must not match")
	})
}

func TestPeernetOrigin_NewestFolderRowWinsAndQueueFillsGaps(t *testing.T) {
	forEachOriginBackend(t, func(t *testing.T, b *originBackend) {
		b.exec(t, `INSERT INTO import_queue (id, nzb_path, category, indexer, metadata) VALUES (9, '/nzbs/9-Q.Title.nzb', 'qcat', 'QIdx', '{"imdb_id":"tt1"}')`)
		old := int64(9)
		addHistory(t, b, "complete/Rel", "Old.nzb", "old", "OldIdx", nil)
		addHistory(t, b, "complete/Rel", "", "", "", &old)
		o, err := b.repo.OriginForPath(context.Background(), "complete/Rel/a.mkv")
		require.NoError(t, err)
		assert.Equal(t, "qcat", o.Category, "the newest folder row points at the queue row that fills its gaps")
		assert.Equal(t, "QIdx", o.Indexer)
		assert.JSONEq(t, `{"imdb_id":"tt1"}`, o.QueueMetadata)
	})
}
