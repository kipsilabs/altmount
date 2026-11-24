package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// backdate moves the grab recorded for key d into the past.
func (c *nzbGrabCache) backdate(key string, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.entries[key]
	g.at = g.at.Add(-d)
	c.entries[key] = g
}

func newGrabTestDB(t *testing.T) (*database.DB, *database.Repository) {
	t.Helper()
	db, err := database.NewDB(database.Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "grab.db")})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db, database.NewRepository(db.Connection(), db.Dialect())
}

// addCompletedImport queues an import for nzbPath and completes it with storagePath.
func addCompletedImport(t *testing.T, db *database.DB, repo *database.Repository, nzbPath, storagePath string) *database.ImportQueueItem {
	t.Helper()
	ctx := context.Background()
	item := &database.ImportQueueItem{NzbPath: nzbPath, Status: database.QueueStatusProcessing, Priority: database.QueuePriorityHigh}
	require.NoError(t, repo.AddToQueue(ctx, item))
	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusCompleted, nil))
	require.NoError(t, db.Repository.AddStoragePath(ctx, item.ID, storagePath))
	return item
}

func TestRecentNzbGrab(t *testing.T) {
	db, repo := newGrabTestDB(t)
	s := &Server{queueRepo: repo}
	ctx := context.Background()
	cfg := &config.Config{}
	cfg.Stremio.NzbGrabCacheHours = 24

	const url = "https://indexer.example/api?t=get&id=abc&apikey=secret"
	key := nzbGrabKey(url)
	assert.NotContains(t, key, "secret", "the API key must not be stored")
	got, _ := s.recentNzbGrab(ctx, cfg, url)
	assert.Nil(t, got, "nothing grabbed yet")

	// The importer may store the NZB under another name, e.g. with a queue ID prefix.
	item := &database.ImportQueueItem{NzbPath: "/config/.nzbs/stremio/7-Movie.2024.nzb", Status: database.QueueStatusProcessing, Priority: database.QueuePriorityHigh}
	require.NoError(t, repo.AddToQueue(ctx, item))
	s.nzbGrabs.put(key, item.ID, "Movie.2024", 24)
	got, name := s.recentNzbGrab(ctx, cfg, url)
	require.NotNil(t, got, "a running import is joined")
	assert.Equal(t, item.ID, got.ID)
	assert.Equal(t, "Movie.2024", name, "with the name the first request used, not the stored nzb_path")

	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusCompleted, nil))
	got, _ = s.recentNzbGrab(ctx, cfg, url)
	assert.Nil(t, got, "a completed import without files is fetched again")

	s.nzbGrabs.put(key, item.ID, "Movie.2024", 24)
	require.NoError(t, db.Repository.AddStoragePath(ctx, item.ID, "/stremio/Movie.2024"))
	got, _ = s.recentNzbGrab(ctx, cfg, url)
	require.NotNil(t, got, "a completed import is reused")
	assert.Equal(t, item.ID, got.ID)
	got, _ = s.recentNzbGrab(ctx, cfg, url+"&other=1")
	assert.Nil(t, got, "another URL is fetched")

	cfg.Stremio.NzbGrabCacheHours = 0
	got, _ = s.recentNzbGrab(ctx, cfg, url)
	assert.Nil(t, got, "0 disables the cache")
	cfg.Stremio.NzbGrabCacheHours = 24

	s.nzbGrabs.backdate(key, 2*time.Hour)
	cfg.Stremio.NzbGrabCacheHours = 1
	got, _ = s.recentNzbGrab(ctx, cfg, url)
	assert.Nil(t, got, "a grab older than the grab TTL is fetched again")
	cfg.Stremio.NzbGrabCacheHours = 24

	s.nzbGrabs.put(key, item.ID, "Movie.2024", 24)
	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusFailed, nil))
	got, _ = s.recentNzbGrab(ctx, cfg, url)
	assert.Nil(t, got, "a failed import is fetched again")
	_, ok := s.nzbGrabs.get(key, 24)
	assert.False(t, ok, "and forgotten")
}

// grabStreamsEnv serves POST /api/nzb/streams for one completed import. Its
// indexer counts requests and answers each one only after release is called.
type grabStreamsEnv struct {
	app     *fiber.App
	cfg     *config.Config
	server  *Server
	item    *database.ImportQueueItem
	hits    atomic.Int32
	release func()
	nzbURL  string
	key     string
}

func newGrabStreamsEnv(t *testing.T, nzbPath, storagePath, urlPath string) *grabStreamsEnv {
	t.Helper()
	e := &grabStreamsEnv{}
	released := make(chan struct{})
	var once sync.Once
	e.release = func() { once.Do(func() { close(released) }) }
	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.hits.Add(1)
		<-released
		_, _ = w.Write([]byte("<nzb/>"))
	}))
	t.Cleanup(indexer.Close)
	t.Cleanup(e.release) // runs before indexer.Close, which waits for handlers
	e.nzbURL = indexer.URL + urlPath

	db, repo := newGrabTestDB(t)
	userRepo := database.NewUserRepository(db.Connection(), db.Dialect())
	apiKey := "test-api-key"
	require.NoError(t, userRepo.CreateUser(context.Background(), &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))
	e.key = auth.HashAPIKey(apiKey)
	// The release is already imported, so the handler serves it without the importer.
	e.item = addCompletedImport(t, db, repo, nzbPath, storagePath)

	enabled := true
	e.cfg = &config.Config{}
	e.cfg.Stremio.Enabled = &enabled
	e.cfg.Stremio.NzbTTLHours = 24
	e.cfg.Stremio.NzbGrabCacheHours = 24
	e.server = &Server{
		configManager:       &mockConfigManager{cfg: e.cfg},
		userRepo:            userRepo,
		queueRepo:           repo,
		progressBroadcaster: progress.NewProgressBroadcaster(),
	}
	e.app = fiber.New()
	e.app.Post("/api/nzb/streams", e.server.handleNzbStreams)
	return e
}

func (e *grabStreamsEnv) post(t *testing.T, extra url.Values) (int, StremioStreamsResponse) {
	form := url.Values{"nzb_url": {e.nzbURL}, "download_key": {e.key}}
	for k, v := range extra {
		form[k] = v
	}
	req := httptest.NewRequest(http.MethodPost, "/api/nzb/streams", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := e.app.Test(req, -1)
	if !assert.NoError(t, err) {
		return 0, StremioStreamsResponse{}
	}
	defer resp.Body.Close()
	var body StremioStreamsResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// TestNzbStreamsHitUsesStoredName pins that a hit matches episodes against the
// name the first request used, not one derived from the import's nzb_path. Here
// only the stored name carries the episode marker.
func TestNzbStreamsHitUsesStoredName(t *testing.T) {
	e := newGrabStreamsEnv(t, "/config/.nzbs/stremio/7-qzrtkplmnvbw.nzb",
		"/stremio/qzrtkplmnvbw/qzrtkplmnvbw.mkv", "/get/whatever.nzb?apikey=secret")
	e.release() // a fetch would be a failure, not a hang
	e.server.nzbGrabs.put(nzbGrabKey(e.nzbURL), e.item.ID, "Show.S01E05.1080p", 24)

	status, body := e.post(t, url.Values{"season": {"1"}, "episode": {"5"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Len(t, body.Streams, 1, "the stored name matches S01E05")

	status, body = e.post(t, url.Values{"season": {"1"}, "episode": {"6"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Empty(t, body.Streams, "the stored name does not match S01E06")
	assert.Equal(t, int32(0), e.hits.Load(), "hits skip the fetch")
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
