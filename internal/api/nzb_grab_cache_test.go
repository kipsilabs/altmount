package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
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
	"golang.org/x/sync/singleflight"
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
	cfg.Stremio.NzbTTLHours = 24

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

	require.NoError(t, repo.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusWaitingRepair, nil))
	got, _ = s.recentNzbGrab(ctx, cfg, url)
	require.NotNil(t, got, "an import waiting for repair is still active")
	assert.Equal(t, item.ID, got.ID)

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

	_, err := db.Connection().ExecContext(ctx, `UPDATE import_queue SET completed_at = datetime('now', '-2 hours') WHERE id = ?`, item.ID)
	require.NoError(t, err)
	cfg.Stremio.NzbTTLHours = 1
	got, _ = s.recentNzbGrab(ctx, cfg, url)
	assert.Nil(t, got, "a completed import past nzb_ttl_hours is fetched again, as by the filename cache")
	cfg.Stremio.NzbTTLHours = 0
	s.nzbGrabs.put(key, item.ID, "Movie.2024", 24)
	got, _ = s.recentNzbGrab(ctx, cfg, url)
	assert.NotNil(t, got, "nzb_ttl_hours 0 keeps completed imports")

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

// TestNzbStreamsGrabsEachURLOnce drives POST /api/nzb/streams: concurrent requests
// for one nzb_url share a single indexer request, and a repeat request reuses the
// import without asking the indexer again.
func TestNzbStreamsGrabsEachURLOnce(t *testing.T) {
	e := newGrabStreamsEnv(t, "/config/.nzbs/stremio/Movie.2024.1080p.nzb",
		"/stremio/Movie.2024.1080p/Movie.2024.1080p.mkv", "/get/Movie.2024.1080p.nzb?apikey=secret")

	const n = 5
	type result struct {
		status int
		body   StremioStreamsResponse
	}
	results := make([]result, n)
	var started, done sync.WaitGroup
	for i := range n {
		started.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			started.Done()
			status, body := e.post(t, nil)
			results[i] = result{status, body}
		}()
	}
	started.Wait()
	require.Eventually(t, func() bool { return e.hits.Load() > 0 }, 5*time.Second, 5*time.Millisecond)
	e.release()
	done.Wait()

	assert.Equal(t, int32(1), e.hits.Load(), "concurrent requests share one indexer request")
	for i, r := range results {
		assert.Equal(t, http.StatusOK, r.status, "request %d", i)
		assert.Equal(t, e.item.ID, r.body.QueueItemID, "request %d", i)
		assert.Len(t, r.body.Streams, 1, "request %d", i)
	}

	status, body := e.post(t, nil)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, e.item.ID, body.QueueItemID)
	assert.Len(t, body.Streams, 1)
	assert.Equal(t, int32(1), e.hits.Load(), "a repeat request reuses the import")

	e.cfg.Stremio.NzbGrabCacheHours = 0
	status, _ = e.post(t, nil)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, int32(2), e.hits.Load(), "with the grab cache off the indexer is asked again")
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

// grabPublicationBarrier pauses queue resolution after the download has
// completed but before the URL cache entry is published.
type grabPublicationBarrier struct {
	slog.Handler
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *grabPublicationBarrier) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "Returning cached Stremio streams for already-processed NZB" {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.Handler.Handle(ctx, record)
}

func TestNzbURLRequestAfterDownloadBeforeCachePublication(t *testing.T) {
	e := newGrabStreamsEnv(t, "/config/.nzbs/stremio/Movie.2024.1080p.nzb",
		"/stremio/Movie.2024.1080p.mkv", "/get/Movie.2024.1080p.nzb")
	e.release()
	original := slog.Default()
	barrier := &grabPublicationBarrier{Handler: slog.NewTextHandler(io.Discard, nil), entered: make(chan struct{}), release: make(chan struct{})}
	slog.SetDefault(slog.New(barrier))
	t.Cleanup(func() { slog.SetDefault(original) })
	var release sync.Once
	unblock := func() { release.Do(func() { close(barrier.release) }) }
	t.Cleanup(unblock)

	first := e.server.resolveNzbURL(context.Background(), e.cfg, e.nzbURL, "")
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach queue resolution after downloading")
	}
	_, cached := e.server.nzbGrabs.get(nzbGrabKey(e.nzbURL), 24)
	require.False(t, cached, "the first request has not published its cache entry")
	require.Equal(t, int32(1), e.hits.Load(), "the first download has completed")

	// Registration is synchronous: this second URL request joins before the
	// first can publish, rather than relying on a goroutine being scheduled.
	second := e.server.resolveNzbURL(context.Background(), e.cfg, e.nzbURL, "")
	unblock()
	for _, result := range []<-chan singleflight.Result{first, second} {
		select {
		case got := <-result:
			require.NoError(t, got.Err)
			assert.True(t, got.Shared, "queue resolution and publication must still be shared")
			assert.Equal(t, e.item.ID, got.Val.(nzbGrab).queueID)
		case <-time.After(5 * time.Second):
			t.Fatal("URL resolution did not complete")
		}
	}
	assert.Equal(t, int32(1), e.hits.Load(), "both requests must share one upstream grab")
	grab, ok := e.server.nzbGrabs.get(nzbGrabKey(e.nzbURL), 24)
	require.True(t, ok)
	assert.Equal(t, e.item.ID, grab.queueID)
}

func TestNzbURLRechecksCacheAfterStaleMiss(t *testing.T) {
	e := newGrabStreamsEnv(t, "/config/.nzbs/stremio/Movie.2024.1080p.nzb",
		"/stremio/Movie.2024.1080p.mkv", "/get/Movie.2024.1080p.nzb")
	e.release()
	item, _ := e.server.recentNzbGrab(context.Background(), e.cfg, e.nzbURL)
	require.Nil(t, item)
	// Another request publishes between the outer miss and coalescing.
	e.server.nzbGrabs.put(nzbGrabKey(e.nzbURL), e.item.ID, "Movie.2024.1080p", 24)
	result := <-e.server.resolveNzbURL(context.Background(), e.cfg, e.nzbURL, "")
	require.NoError(t, result.Err)
	assert.Equal(t, e.item.ID, result.Val.(nzbGrab).queueID)
	assert.Zero(t, e.hits.Load())
}

func TestNzbGrabCacheDropKeepsNewerGrab(t *testing.T) {
	var c nzbGrabCache
	c.put("k", 1, "Old", 24)
	c.put("k", 2, "New", 24)
	c.drop("k", 1)
	g, ok := c.get("k", 24)
	require.True(t, ok, "a stale drop must not erase the newer grab")
	assert.Equal(t, int64(2), g.queueID)
	c.drop("k", 2)
	_, ok = c.get("k", 24)
	assert.False(t, ok)
}

func TestNzbGrabCacheOwnsReleaseName(t *testing.T) {
	ctx, release := withQuery(t, "name=Show.S01E05")
	defer release()
	var cache nzbGrabCache
	cache.put("key", 1, ctx.Query("name"), 24)
	// Fiber strings borrow request buffers, which are reused after the handler.
	copy(ctx.Request().URI().QueryArgs().Peek("name"), "Show.S02E06")
	grab, ok := cache.get("key", 24)
	require.True(t, ok)
	assert.Equal(t, "Show.S01E05", grab.nzbName)
}

func TestNzbStreamsDoesNotCacheSubstringMatch(t *testing.T) {
	e := newGrabStreamsEnv(t, "/config/.nzbs/stremio/Other.Movie.2024.1080p.nzb",
		"/stremio/Other.Movie.2024.1080p.mkv", "/get/Movie.2024.1080p.nzb")
	e.release()
	status, _ := e.post(t, nil)
	// With no matching import or importer service, resolution must fail instead
	// of returning the unrelated import found by the broad database search.
	assert.Equal(t, http.StatusInternalServerError, status)
	_, ok := e.server.nzbGrabs.get(nzbGrabKey(e.nzbURL), 24)
	assert.False(t, ok)
}

func TestQueueNzbStreamsReusesWaitingRepair(t *testing.T) {
	_, repo := newGrabTestDB(t)
	item := &database.ImportQueueItem{
		NzbPath: "/config/.nzbs/stremio/Movie.2024.nzb",
		Status:  database.QueueStatusWaitingRepair, Priority: database.QueuePriorityHigh,
	}
	require.NoError(t, repo.AddToQueue(context.Background(), item))
	s := &Server{queueRepo: repo}
	grab, err := s.queueNzbStreams(context.Background(), &config.Config{}, "Movie.2024.nzb", nil, "")
	require.NoError(t, err, "waiting for repair must not enqueue a duplicate")
	assert.Equal(t, item.ID, grab.queueID)
}

func TestNzbStreamsExactMatchBehindNewerSubstringMatch(t *testing.T) {
	e := newGrabStreamsEnv(t, "/config/.nzbs/stremio/Movie.2024.1080p.nzb",
		"/stremio/Movie.2024.1080p.mkv", "/get/Movie.2024.1080p.nzb")
	e.release()
	other := &database.ImportQueueItem{
		NzbPath: "/config/.nzbs/stremio/Other.Movie.2024.1080p.nzb",
		Status:  database.QueueStatusProcessing, Priority: database.QueuePriorityHigh,
	}
	require.NoError(t, e.server.queueRepo.AddToQueue(context.Background(), other))
	status, body := e.post(t, nil)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, e.item.ID, body.QueueItemID)
	grab, ok := e.server.nzbGrabs.get(nzbGrabKey(e.nzbURL), 24)
	require.True(t, ok)
	assert.Equal(t, e.item.ID, grab.queueID)
}

func TestRecentNzbGrabKeepsGrabOnDatabaseError(t *testing.T) {
	db, repo := newGrabTestDB(t)
	s := &Server{queueRepo: repo}
	cfg := &config.Config{}
	cfg.Stremio.NzbGrabCacheHours = 24
	const url = "https://indexer.example/get.nzb"
	s.nzbGrabs.put(nzbGrabKey(url), 1, "Movie.2024", 24)
	require.NoError(t, db.Connection().Close())

	got, _ := s.recentNzbGrab(context.Background(), cfg, url)
	assert.Nil(t, got)
	_, ok := s.nzbGrabs.get(nzbGrabKey(url), 24)
	assert.True(t, ok, "a database error must not forget the grab")
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
