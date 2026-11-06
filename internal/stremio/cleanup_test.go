package stremio

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

type cleanupEnv struct {
	svc    *StremioCleanupService
	db     *database.DB
	queue  *database.Repository
	health *database.HealthRepository
	meta   *metadata.MetadataService
}

func newCleanupEnv(t *testing.T) *cleanupEnv {
	t.Helper()
	root := t.TempDir()
	db, err := database.NewDB(database.Config{Type: "sqlite", DatabasePath: filepath.Join(root, "cleanup.db")})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	env := &cleanupEnv{
		db:     db,
		queue:  database.NewRepository(db.Connection(), db.Dialect()),
		health: database.NewHealthRepository(db.Connection(), db.Dialect()),
		meta:   metadata.NewMetadataService(filepath.Join(root, "metadata")),
	}
	cfg := config.DefaultConfig()
	cfg.Stremio.NzbTTLHours = 24
	env.svc = NewStremioCleanupService(env.queue, env.meta, env.health, func() *config.Config { return cfg })
	return env
}

// addExpiredStremioImport records a completed Stremio import of files under
// storage, finished two days ago, with a health record per file whose
// library_path is the import-time placeholder or, when linkedTo is set, a
// path an ARR relinked it to.
func (e *cleanupEnv) addExpiredStremioImport(t *testing.T, storage string, files []string, linkedTo string) int64 {
	t.Helper()
	ctx := context.Background()
	for _, f := range files {
		fm := e.meta.CreateFileMetadata(100, "x.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
		require.NoError(t, e.meta.WriteFileMetadata(f, fm))
		lib := "/" + f
		if linkedTo != "" {
			lib = linkedTo
		}
		require.NoError(t, e.health.BatchAddFileToHealthCheck(ctx, []database.HealthCheckUpsert{{FilePath: f, LibraryPath: &lib, MaxRetries: 2, MaxRepairRetries: 2}}))
	}
	downloadID := "stremio:" + filepath.Base(storage)
	item := &database.ImportQueueItem{NzbPath: storage + ".nzb", DownloadID: &downloadID, Status: database.QueueStatusCompleted, Priority: database.QueuePriorityHigh, StoragePath: &storage}
	require.NoError(t, e.queue.AddToQueue(ctx, item))
	_, err := e.db.Connection().ExecContext(ctx,
		`UPDATE import_queue SET status = 'completed', storage_path = ?, completed_at = datetime('now', '-48 hours') WHERE id = ?`, storage, item.ID)
	require.NoError(t, err)
	return item.ID
}

func (e *cleanupEnv) queueItemExists(t *testing.T, id int64) bool {
	t.Helper()
	item, err := e.queue.GetQueueItem(context.Background(), id)
	require.NoError(t, err)
	return item != nil
}

func (e *cleanupEnv) healthExists(t *testing.T, path string) bool {
	t.Helper()
	h, err := e.health.GetFileHealth(context.Background(), path)
	return err == nil && h != nil
}

// An expired Stremio-only download is removed completely: metadata, health
// record (otherwise health checks keep scheduling a missing file) and queue
// entry.
func TestStremioCleanupRemovesExpiredDownloadAndHealthRecord(t *testing.T) {
	env := newCleanupEnv(t)
	f := "complete/Movies/Stream.Only.2024/Stream.Only.2024.mkv"
	id := env.addExpiredStremioImport(t, f, []string{f}, "")

	env.svc.cleanupExpired(context.Background())

	assert.False(t, env.meta.FileExists(f), "metadata deleted")
	assert.False(t, env.healthExists(t, f), "health record deleted")
	assert.False(t, env.queueItemExists(t, id), "queue entry removed")
}

// A Stremio download an ARR later imported is the file the library's symlink
// points at. The cleanup must keep it (and its health record) and only stop
// tracking it as a Stremio download.
func TestStremioCleanupKeepsDownloadTheLibraryAdopted(t *testing.T) {
	env := newCleanupEnv(t)
	f := "complete/Movies/Adopted.2024/Adopted.2024.mkv"
	id := env.addExpiredStremioImport(t, f, []string{f}, "/media/Movies/Adopted (2024)/Adopted (2024).mkv")

	env.svc.cleanupExpired(context.Background())

	assert.True(t, env.meta.FileExists(f), "library file kept")
	assert.True(t, env.healthExists(t, f), "library file's health record kept")
	assert.False(t, env.queueItemExists(t, id), "no longer tracked as a Stremio download")
}

// A multi-file download (a season pack) is checked file by file: one adopted
// episode keeps the whole download, otherwise every file and health record
// goes.
func TestStremioCleanupHandlesMultiFileDownloads(t *testing.T) {
	env := newCleanupEnv(t)
	gone := []string{"complete/TV/Pack.S01/E01.mkv", "complete/TV/Pack.S01/Extras/E02.mkv"}
	goneID := env.addExpiredStremioImport(t, "complete/TV/Pack.S01", gone, "")

	kept := []string{"complete/TV/Kept.S01/E01.mkv"}
	keptID := env.addExpiredStremioImport(t, "complete/TV/Kept.S01", kept, "/media/TV/Kept/Season 01/E01.mkv")

	env.svc.cleanupExpired(context.Background())

	for _, f := range gone {
		assert.False(t, env.meta.FileExists(f), "deleted: %s", f)
		assert.False(t, env.healthExists(t, f), "health record deleted: %s", f)
	}
	assert.False(t, env.queueItemExists(t, goneID))
	assert.True(t, env.meta.FileExists(kept[0]), "adopted pack kept")
	assert.False(t, env.queueItemExists(t, keptID))
}

// A storage path recorded as a category folder (#1013) must not make the
// cleanup delete that folder's metadata tree; only the queue entry goes.
func TestStremioCleanupRefusesCategoryFolderStoragePath(t *testing.T) {
	env := newCleanupEnv(t)
	other := "complete/Movies/Other.Release.2024/Other.Release.2024.mkv"
	fm := env.meta.CreateFileMetadata(100, "x.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
	require.NoError(t, env.meta.WriteFileMetadata(other, fm))

	f := "complete/Movies/Stream.Only.2024/Stream.Only.2024.mkv"
	id := env.addExpiredStremioImport(t, "complete/Movies", []string{f}, "")

	env.svc.cleanupExpired(context.Background())

	assert.True(t, env.meta.FileExists(other), "other release in the category kept")
	assert.True(t, env.meta.FileExists(f), "nothing under the category folder deleted")
	assert.False(t, env.queueItemExists(t, id), "queue entry dropped so the refusal does not repeat")
}
