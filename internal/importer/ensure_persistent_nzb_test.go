package importer

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// newMinimalServiceForPersistTest builds just enough of *Service to exercise
// ensurePersistentNzb. It uses an in-memory SQLite database so no disk paths
// are required.
func newMinimalServiceForPersistTest(t *testing.T) *Service {
	t.Helper()

	// Open in-memory SQLite and run the minimal queue schema.
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS import_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			download_id TEXT DEFAULT NULL,
			nzb_path TEXT NOT NULL,
			relative_path TEXT DEFAULT NULL,
			storage_path TEXT DEFAULT NULL,
			priority INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'pending'
				CHECK(status IN ('pending','processing','completed','failed','fallback','paused')),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			started_at DATETIME DEFAULT NULL,
			completed_at DATETIME DEFAULT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			error_message TEXT DEFAULT NULL,
			batch_id TEXT DEFAULT NULL,
			metadata TEXT DEFAULT NULL,
			category TEXT DEFAULT NULL,
			file_size BIGINT DEFAULT NULL,
			target_path TEXT DEFAULT NULL,
			skip_arr_notification BOOLEAN NOT NULL DEFAULT FALSE,
			skip_post_import_links BOOLEAN NOT NULL DEFAULT FALSE,
			peer_hold INTEGER NOT NULL DEFAULT 0,
			indexer TEXT DEFAULT NULL,
			UNIQUE(nzb_path)
		);
		CREATE INDEX IF NOT EXISTS idx_queue_nzb_path ON import_queue(nzb_path);
	`)
	require.NoError(t, err)

	repo := database.NewQueueRepository(db, database.DialectSQLite)
	dbWrapper := &database.DB{}
	dbWrapper.Repository = repo

	// Minimal configGetter — only Database.Path is used in the OLD code.
	// After the change it is no longer used inside the function, but we keep a
	// valid value so any residual reference doesn't panic.
	tmpCfgDir := t.TempDir()
	cfgGetter := config.ConfigGetter(func() *config.Config {
		return &config.Config{
			Database: config.DatabaseConfig{
				Path: filepath.Join(tmpCfgDir, "test.db"),
			},
		}
	})

	return &Service{
		database:     dbWrapper,
		configGetter: cfgGetter,
		log:          slog.Default(),
		cancelFuncs:  make(map[int64]context.CancelFunc),
		mu:           sync.RWMutex{},
	}
}

func TestEnsurePersistentNzb_UsesOSTempQueueDir(t *testing.T) {
	// Arrange: write a real .nzb file in a temp dir (simulates stageDir).
	stageDir := t.TempDir()
	nzbPath := filepath.Join(stageDir, "movie.nzb")
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb/>"), 0644))

	item := &database.ImportQueueItem{ID: 42, NzbPath: nzbPath}

	svc := newMinimalServiceForPersistTest(t)

	// Act
	err := svc.ensurePersistentNzb(context.Background(), item)
	require.NoError(t, err)

	// Cleanup: remove the file from the OS temp queue dir (registered before assertions so it
	// always runs even if an assertion fails).
	t.Cleanup(func() { os.Remove(item.NzbPath) })

	// Assert: item.NzbPath must be inside os.TempDir()/.altmount-queue/
	expected := filepath.Join(os.TempDir(), ".altmount-queue")
	assert.True(t, strings.HasPrefix(item.NzbPath, expected),
		"expected OS temp queue dir prefix %q, got %q", expected, item.NzbPath)
	assert.False(t, strings.Contains(item.NzbPath, ".nzbs"),
		"should not be in .nzbs/ directory, got %q", item.NzbPath)

	// Assert: the file actually exists at the new path
	_, statErr := os.Stat(item.NzbPath)
	assert.NoError(t, statErr, "moved file should exist at new path")
}

// newServiceForRegenerateTest builds a *Service with a real metadata.MetadataService so
// regenerateNzbFile can write/read an actual .nzbz store, plus a configGetter whose
// Database.Path resolves to a fresh configDir the test controls.
func newServiceForRegenerateTest(t *testing.T) (svc *Service, configDir string) {
	t.Helper()

	svc = newMinimalServiceForPersistTest(t)
	configDir = t.TempDir()
	svc.configGetter = config.ConfigGetter(func() *config.Config {
		return &config.Config{
			Database: config.DatabaseConfig{
				Path: filepath.Join(configDir, "test.db"),
			},
		}
	})
	svc.metadataService = metadata.NewMetadataService(t.TempDir())
	return svc, configDir
}

// sampleNzbStoreForTest returns a minimal but valid *metapb.NzbStore, mirroring
// internal/metadata/store_test.go's sampleStore().
func sampleNzbStoreForTest() *metapb.NzbStore {
	return &metapb.NzbStore{Files: []*metapb.NzbFileEntry{
		{Subject: "Movie.mkv yEnc (1/1)", Poster: "p@x", Date: 1000, Groups: []string{"a.b.test"},
			Segments: []*metapb.NzbSeg{{Id: "m1@x", Number: 1, Bytes: 1000}}},
	}}
}

func TestEnsurePersistentNzb_RegeneratesFromStore_WhenRawFileMissing(t *testing.T) {
	// Arrange: item.NzbPath already points inside the persistent temp queue dir (as it would
	// after a prior successful import), but the raw file was deleted by handleProcessingSuccess.
	queueDir := filepath.Join(os.TempDir(), ".altmount-queue")
	require.NoError(t, os.MkdirAll(queueDir, 0755))
	missingPath := filepath.Join(queueDir, "regen-movie.nzb")
	t.Cleanup(func() { os.Remove(missingPath) })

	item := &database.ImportQueueItem{ID: 7, NzbPath: missingPath}

	svc, configDir := newServiceForRegenerateTest(t)

	// The store must live at exactly the path processor.go writes it to / handleDownloadNZB
	// reconstructs it from: configDir/.nzbs/{category}/{queueID}-{nzbBase}.nzbz.
	storeRef := filepath.Join(configDir, ".nzbs", "7-regen-movie.nzbz")
	require.NoError(t, svc.metadataService.Store().WriteStore(storeRef, sampleNzbStoreForTest()))

	// Act
	err := svc.ensurePersistentNzb(context.Background(), item)
	require.NoError(t, err)

	// Assert: the raw NZB now exists on disk again, still inside the persistent queue dir.
	_, statErr := os.Stat(item.NzbPath)
	assert.NoError(t, statErr, "regenerated file should exist on disk")
	assert.True(t, strings.HasPrefix(item.NzbPath, queueDir),
		"regenerated path should stay inside the persistent temp queue dir, got %q", item.NzbPath)

	regenerated, err := os.ReadFile(item.NzbPath)
	require.NoError(t, err)
	assert.Contains(t, string(regenerated), "Movie.mkv", "regenerated NZB should contain the file entry from the store")
}

func TestEnsurePersistentNzb_RegenerateFails_WhenNoStoreFound(t *testing.T) {
	// Arrange: raw file missing, and no .nzbz store was ever written for this item.
	queueDir := filepath.Join(os.TempDir(), ".altmount-queue")
	require.NoError(t, os.MkdirAll(queueDir, 0755))
	missingPath := filepath.Join(queueDir, "regen-nostore.nzb")

	item := &database.ImportQueueItem{ID: 8, NzbPath: missingPath}

	svc, _ := newServiceForRegenerateTest(t)

	// Act
	err := svc.ensurePersistentNzb(context.Background(), item)

	// Assert: clear, actionable error instead of a raw "no such file" from a bare file open.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no store was found")
}

func TestEnsurePersistentNzb_AlreadyInTempQueueDir_IsNoop(t *testing.T) {
	// Arrange: NZB is already in the target queue dir — should be a no-op.
	queueDir := filepath.Join(os.TempDir(), ".altmount-queue")
	require.NoError(t, os.MkdirAll(queueDir, 0755))

	existingPath := filepath.Join(queueDir, "movie.nzb")
	require.NoError(t, os.WriteFile(existingPath, []byte("<nzb/>"), 0644))
	t.Cleanup(func() { os.Remove(existingPath) })

	item := &database.ImportQueueItem{ID: 99, NzbPath: existingPath}

	svc := newMinimalServiceForPersistTest(t)

	// Act
	err := svc.ensurePersistentNzb(context.Background(), item)
	require.NoError(t, err)

	// Assert: path unchanged
	assert.Equal(t, existingPath, item.NzbPath,
		"path should not change when already in OS temp queue dir")
}
