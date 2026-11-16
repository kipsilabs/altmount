package api

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer"
)

// heldPeerItem is a completed queue item of a pending peer import with the
// provisional history row the import recorded (no download id on the history
// row: only the queue row carries it).
func heldPeerItem(t *testing.T, conn *sql.DB, repo *database.Repository, downloadID string) int64 {
	t.Helper()
	ctx := context.Background()
	item := &database.ImportQueueItem{DownloadID: &downloadID, NzbPath: filepath.Join(t.TempDir(), "x.nzb"),
		Status: database.QueueStatusProcessing, Priority: database.QueuePriorityNormal, MaxRetries: 1}
	require.NoError(t, repo.AddToQueue(ctx, item))
	nzb := item.ID
	require.NoError(t, repo.AddImportHistory(ctx, &database.ImportHistory{NzbID: &nzb, NzbName: "x.nzb", FileName: "a.mkv", FileSize: 10, VirtualPath: "tv/x"}))
	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusCompleted, nil))
	require.NoError(t, database.NewPeernetBundleImportRepository(conn, database.DialectSQLite).SetQueueHold(ctx, item.ID, true))
	return item.ID
}

func historyIDs(t *testing.T, repo *database.Repository) int {
	t.Helper()
	rows, err := repo.ListSABnzbdHistory(context.Background(), "", 100, 0)
	require.NoError(t, err)
	return len(rows)
}

// Deleting the queue row of a held peer import (by download id) must take its
// provisional history with it: the history view would otherwise show a
// Completed job for a release that is still unverified.
func TestPeerHold_QueueDeleteByDownloadIDDropsTheProvisionalHistory(t *testing.T) {
	server, repo, conn := newPeerHoldServer(t)
	heldPeerItem(t, conn, repo, "dl-held-1")

	app := fiber.New()
	app.Delete("/sabnzbd", server.handleSABnzbdQueueDelete)
	resp, err := app.Test(httptest.NewRequest("DELETE", "/sabnzbd?value=dl-held-1&name=queue", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	assert.Zero(t, historyIDs(t, repo), "no Completed ghost in the history")
}

func TestPeerHold_DeleteQueueItemsByPathDropsTheProvisionalHistory(t *testing.T) {
	_, repo, conn := newPeerHoldServer(t)
	id := heldPeerItem(t, conn, repo, "dl-held-2")
	item, err := repo.GetQueueItem(context.Background(), id)
	require.NoError(t, err)

	require.NoError(t, repo.DeleteQueueItemsByPath(context.Background(), item.NzbPath))

	assert.Zero(t, historyIDs(t, repo))
}

func newPeerHoldServer(t *testing.T) (*Server, *database.Repository, *sql.DB) {
	t.Helper()
	db, err := database.NewDB(database.Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "test.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := database.NewRepository(db.Connection(), db.Dialect())
	return &Server{queueRepo: repo, importerService: &importer.Service{}}, repo, db.Connection()
}
