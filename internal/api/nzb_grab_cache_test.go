package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
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
	key := nzbGrabKey(url)
	assert.NotContains(t, key, "secret", "the API key must not be stored")
	assert.Nil(t, s.recentNzbGrab(ctx, 24, url), "nothing grabbed yet")

	item := &database.ImportQueueItem{NzbPath: "Movie.2024.nzb", Status: database.QueueStatusProcessing, Priority: database.QueuePriorityHigh}
	require.NoError(t, repo.AddToQueue(ctx, item))
	s.nzbGrabs.put(key, item.ID, 24)
	assert.Equal(t, item.ID, s.recentNzbGrab(ctx, 24, url).ID, "a running import is joined")

	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusCompleted, nil))
	assert.Nil(t, s.recentNzbGrab(ctx, 24, url), "a completed import without files is fetched again")

	s.nzbGrabs.put(key, item.ID, 24)
	require.NoError(t, db.Repository.AddStoragePath(ctx, item.ID, "/stremio/Movie.2024"))
	assert.Equal(t, item.ID, s.recentNzbGrab(ctx, 24, url).ID, "a completed import is reused")
	assert.Nil(t, s.recentNzbGrab(ctx, 0, url), "0 disables the cache")
	assert.Nil(t, s.recentNzbGrab(ctx, 24, url+"&other=1"), "another URL is fetched")

	s.nzbGrabs.entries[key] = nzbGrab{queueID: item.ID, at: time.Now().Add(-2 * time.Hour)}
	assert.Nil(t, s.recentNzbGrab(ctx, 1, url), "a grab older than the TTL is fetched again")

	s.nzbGrabs.put(key, item.ID, 24)
	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusFailed, nil))
	assert.Nil(t, s.recentNzbGrab(ctx, 24, url), "a failed import is fetched again")
	assert.NotContains(t, s.nzbGrabs.entries, key, "and forgotten")
}

func TestFetchNzbURLRedactsTheURLFromErrors(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()

	_, err := fetchNzbURL(context.Background(), &config.Config{}, closed.URL+"/get.nzb?apikey=secret")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret")
	assert.Contains(t, err.Error(), "/get.nzb", "the path stays for diagnosis")

	_, err = fetchNzbURL(context.Background(), &config.Config{}, "http://[::1/get.nzb?apikey=secret")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret")
}
