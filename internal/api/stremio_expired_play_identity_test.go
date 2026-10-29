package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
)

const expiredPlayTitle = "Same.Release.Title.2024.1080p.WEB-DL"

// expiredPlayFixture is a Stremio server holding two imported releases that
// sanitize to the same title but serve different files, plus an indexer that
// answers every search with one same-titled release.
type expiredPlayFixture struct {
	app      *fiber.App
	key      string
	candA    stremioPlayCandidate
	candB    stremioPlayCandidate
	pathA    string
	pathB    string
	searches *atomic.Int32
}

func newExpiredPlayFixture(t *testing.T) *expiredPlayFixture {
	t.Helper()

	// Metadata resolves to the movie the releases name, so the relevance gate
	// keeps the same-titled search result instead of dropping it as unrelated.
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"meta":{"name":"Same Release Title","year":"2024"}}`))
	}))
	t.Cleanup(metadata.Close)
	previousClient := tvMetadataLookupClient
	metadataURL, err := url.Parse(metadata.URL)
	require.NoError(t, err)
	tvMetadataLookupClient = &http.Client{Transport: cachedSearchMetadataTransport{target: metadataURL}}
	t.Cleanup(func() { tvMetadataLookupClient = previousClient })
	t.Cleanup(overrideTVmazeBaseURL(metadata.URL))
	resetSeriesMetadataCaches()
	t.Cleanup(resetSeriesMetadataCaches)

	searches := &atomic.Int32{}
	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		searches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"title":"` + expiredPlayTitle + `","downloadUrl":"https://indexer.example/release-c.nzb","protocol":"usenet"}]`))
	}))
	t.Cleanup(indexer.Close)

	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "test.db")})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	userRepo := database.NewUserRepository(db.Connection(), database.DialectSQLite)
	apiKey := "test-api-key"
	require.NoError(t, userRepo.CreateUser(ctx, &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))

	candA := stremioPlayCandidate{
		Title:       expiredPlayTitle,
		SafeTitle:   sanitizeFilename(expiredPlayTitle),
		DownloadURL: "https://indexer.example/api?t=get&id=release-a",
		Indexer:     "Indexer",
		Source:      "prowlarr",
	}
	candB := candA
	candB.DownloadURL = "https://indexer.example/api?t=get&id=release-b"
	require.NotEqual(t, stremioReleaseKey(candA), stremioReleaseKey(candB))

	insertImported := func(cand stremioPlayCandidate, storagePath string) {
		metadataJSON, err := encodeStremioQueueMetadata(stremioQueueMetadata{
			ContentKey: "movie:tt123:0:0",
			ReleaseKey: stremioReleaseKey(cand),
			IMDbID:     "tt123",
			Type:       "movie",
		})
		require.NoError(t, err)
		_, err = db.Connection().ExecContext(ctx,
			`INSERT INTO import_queue (nzb_path, download_id, category, priority, status, retry_count, max_retries, metadata, storage_path, completed_at)
			 VALUES (?, 'stremio:test', 'Movies', 1, 'completed', 0, 3, ?, ?, ?)`,
			"/config/.nzbs/Movies/"+stremioPlayNzbFilename(cand), metadataJSON, storagePath, time.Now())
		require.NoError(t, err)
	}
	pathA := "/storage/release-a/" + candA.SafeTitle + ".mkv"
	pathB := "/storage/release-b/" + candB.SafeTitle + ".mkv"
	insertImported(candA, pathA)
	insertImported(candB, pathB)

	cfg := &config.Config{}
	cfg.Stremio.Enabled = boolPtr(true)
	cfg.Stremio.Prowlarr.Enabled = boolPtr(true)
	cfg.Stremio.Prowlarr.Host = indexer.URL
	cfg.Stremio.Prowlarr.APIKey = "indexer-key"
	cfg.Stremio.IncludeLibraryStreams = boolPtr(false)
	cfg.Stremio.MaxFallbackReleases = 0
	cfg.Stremio.NzbTTLHours = 24

	server := &Server{
		configManager:   &mockConfigManager{cfg: cfg},
		userRepo:        userRepo,
		queueRepo:       database.NewRepository(db.Connection(), database.DialectSQLite),
		stremioFailures: newStremioFailureCache(),
	}
	app := fiber.New()
	app.Get("/stremio/:key/play", server.handleStremioAddonPlay)

	return &expiredPlayFixture{
		app:      app,
		key:      auth.HashAPIKey(apiKey),
		candA:    candA,
		candB:    candB,
		pathA:    pathA,
		pathB:    pathB,
		searches: searches,
	}
}

func (f *expiredPlayFixture) play(t *testing.T, query string) *http.Response {
	t.Helper()
	resp, err := f.app.Test(httptest.NewRequest(http.MethodGet, "/stremio/"+f.key+"/play?"+query, nil), -1)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// An expired play link that still carries the NZB URL keeps its release
// identity: it must resolve to that release's imported bytes, never to another
// release that sanitizes to the same title.
func TestStremioPlayExpiredTokenWithURLKeepsReleaseIdentity(t *testing.T) {
	f := newExpiredPlayFixture(t)

	releaseQuery := "release=expired0123456789&title=" + url.QueryEscape(f.candB.SafeTitle) +
		"&type=movie&id=tt123&indexer=Indexer&source=prowlarr" +
		"&url=" + url.QueryEscape(f.candB.DownloadURL)
	resp := f.play(t, releaseQuery)
	require.Equal(t, http.StatusFound, resp.StatusCode, "a recovered link carrying its NZB URL must play")
	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, "/api/files/stream", location.Path)
	require.Equal(t, f.pathB, location.Query().Get("path"),
		"the recovered link must play the release it names, not the same-titled one")

	// The same link naming a release this server has no imported copy of must
	// not fall back to either same-titled cached file. No importer is wired
	// here, so queueing it fails closed with 503.
	unknown := f.candA
	unknown.DownloadURL = "https://indexer.example/api?t=get&id=release-c"
	resp = f.play(t, "release=expired0123456789&title="+url.QueryEscape(unknown.SafeTitle)+
		"&type=movie&id=tt123&indexer=Indexer&source=prowlarr&url="+url.QueryEscape(unknown.DownloadURL))
	require.NotEqual(t, http.StatusFound, resp.StatusCode,
		"an unknown release must not redirect to a same-titled cached file")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// An expired play link that only carries the release name has no release
// identity, so it must search again rather than redirect to a cached file whose
// release it cannot verify.
func TestStremioPlayExpiredTokenWithoutURLDoesNotServeTitleMatch(t *testing.T) {
	f := newExpiredPlayFixture(t)

	resp := f.play(t, "release=expired0123456789&title="+url.QueryEscape(f.candA.SafeTitle)+
		"&type=movie&id=tt123&indexer=Indexer&source=prowlarr")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"a title-only recovery must search instead of serving an unverified cached file")
	require.Empty(t, resp.Header.Get("Location"),
		"no redirect may be issued to a title-matched file")
	require.Positive(t, f.searches.Load(), "the recovery must search again")
}
