package api

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecentNzbGrab(t *testing.T) {
	db, err := database.NewDB(database.Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "grab.db")})
	require.NoError(t, err)
	defer db.Close()
	repo := database.NewRepository(db.Connection(), db.Dialect())
	s := &Server{queueRepo: repo}
	ctx := context.Background()

	const url = "https://indexer.example/api?t=get&id=abc&apikey=secret"
	id := nzbGrabID(url)
	assert.NotContains(t, id, "secret", "the API key must not be stored")

	storage := "/stremio/Movie.2024"
	item := &database.ImportQueueItem{NzbPath: "Movie.2024.nzb", DownloadID: &id, Status: database.QueueStatusPending, Priority: database.QueuePriorityHigh}
	require.NoError(t, repo.AddToQueue(ctx, item))
	assert.Equal(t, item.ID, s.recentNzbGrab(ctx, 24, url).ID, "a running import is joined")

	require.NoError(t, db.Repository.AddStoragePath(ctx, item.ID, storage))
	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusCompleted, nil))
	assert.Equal(t, item.ID, s.recentNzbGrab(ctx, 24, url).ID, "a fresh import is reused")
	assert.Nil(t, s.recentNzbGrab(ctx, 0, url), "0 disables the cache")
	assert.Nil(t, s.recentNzbGrab(ctx, 24, url+"&other=1"), "another URL is fetched")

	_, err = db.Connection().ExecContext(ctx, `UPDATE import_queue SET completed_at = datetime('now', '-2 hours') WHERE id = ?`, item.ID)
	require.NoError(t, err)
	assert.Nil(t, s.recentNzbGrab(ctx, 1, url), "an import older than the TTL is fetched again")

	failed := &database.ImportQueueItem{NzbPath: "Movie.2024-retry.nzb", DownloadID: &id, Status: database.QueueStatusFailed, Priority: database.QueuePriorityHigh}
	require.NoError(t, repo.AddToQueue(ctx, failed))
	assert.Nil(t, s.recentNzbGrab(ctx, 24, url), "the newest import decides; a failed one is fetched again")
}
