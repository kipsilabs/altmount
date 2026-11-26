package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

// Two indexer URLs can carry the same release title while serving different
// NZB bytes. When both are already imported under different content identities,
// a live play link whose release key misses must not guess between them: it
// keeps searching rather than serving either cached file. The release whose
// key does match still short-circuits to its own file.
func TestStremioPlayDoesNotGuessBetweenSameTitledReleases(t *testing.T) {
	title := "Same.Release.Title.2024.1080p.WEB-DL"
	candA := stremioPlayCandidate{
		Title:       title,
		SafeTitle:   sanitizeFilename(title),
		DownloadURL: "https://indexer.example/api?t=get&id=release-a",
		Indexer:     "Indexer",
		Source:      "prowlarr",
	}
	candB := candA
	candB.DownloadURL = "https://indexer.example/api?t=get&id=release-b"

	require.NotEqual(t, stremioReleaseKey(candA), stremioReleaseKey(candB),
		"different NZB URLs must have different release keys")
	require.NotEqual(t, stremioPlayNzbFilename(candA), stremioPlayNzbFilename(candB),
		"different NZB URLs must not share a queue filename")

	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "test.db")})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	userRepo := database.NewUserRepository(db.Connection(), database.DialectSQLite)
	apiKey := "test-api-key"
	require.NoError(t, userRepo.CreateUser(ctx, &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))

	// Two same-titled releases are already imported with different content
	// identities. Release A's row keeps the title-only NZB filename older rows
	// used, so only its release key and verified identity can tell it apart.
	insertCompleted := func(cand stremioPlayCandidate, identity, dir, storagePath string) {
		metadata, err := encodeStremioQueueMetadata(stremioQueueMetadata{
			ContentKey: "movie:tt123:0:0",
			ReleaseKey: stremioReleaseKey(cand),
			IMDbID:     "tt123",
			Type:       "movie",
		})
		require.NoError(t, err)
		_, err = db.Connection().ExecContext(ctx,
			`INSERT INTO import_queue (nzb_path, download_id, category, priority, status, retry_count, max_retries, metadata, storage_path, completed_at, content_identity)
			 VALUES (?, 'stremio:test', 'Movies', 1, 'completed', 0, 3, ?, ?, ?, ?)`,
			"/config/.nzbs/Movies/"+dir+"/"+cand.SafeTitle+".nzb", metadata, storagePath, time.Now(), identity)
		require.NoError(t, err)
	}
	other := candA
	other.DownloadURL = "https://indexer.example/api?t=get&id=release-other"
	storagePath := "/storage/" + candA.SafeTitle + ".mkv"
	insertCompleted(candA, "identity-a", "a", storagePath)
	insertCompleted(other, "identity-other", "other", "/storage/other/"+candA.SafeTitle+".mkv")

	cfg := &config.Config{}
	cfg.Stremio.Enabled = boolPtr(true)
	cfg.Stremio.Prowlarr.Enabled = boolPtr(true)
	cfg.Stremio.Prowlarr.Host = "http://indexer.invalid"
	cfg.Stremio.Prowlarr.APIKey = "indexer-key"
	cfg.Stremio.IncludeLibraryStreams = boolPtr(false)
	cfg.Stremio.MaxFallbackReleases = 0

	server := &Server{
		configManager:   &mockConfigManager{cfg: cfg},
		userRepo:        userRepo,
		queueRepo:       database.NewRepository(db.Connection(), database.DialectSQLite),
		stremioFailures: newStremioFailureCache(),
	}
	app := fiber.New()
	app.Get("/stremio/:key/play", server.handleStremioAddonPlay)
	key := auth.HashAPIKey(apiKey)

	play := func(cand stremioPlayCandidate) *http.Response {
		token, err := server.storeStremioReleaseRef(cand)
		require.NoError(t, err)
		resp, err := app.Test(httptest.NewRequest(http.MethodGet,
			"/stremio/"+key+"/play?release="+token+"&type=movie&id=tt123", nil), -1)
		require.NoError(t, err)
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	// The release that was actually imported still short-circuits to its file.
	respA := play(candA)
	require.Equal(t, http.StatusFound, respA.StatusCode)
	location, err := url.Parse(respA.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, "/api/files/stream", location.Path)
	require.Equal(t, storagePath, location.Query().Get("path"))

	// The same-titled release behind a different URL must be queued, not served
	// release A's bytes. No importer is wired here, so queueing fails closed
	// with 503 instead of redirecting to the cached stream.
	respB := play(candB)
	require.NotEqual(t, http.StatusFound, respB.StatusCode,
		"a different NZB URL must not redirect to the cached release's stream")
	require.Equal(t, http.StatusServiceUnavailable, respB.StatusCode)
}
