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
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/prowlarr"
)

// Stremio replays the play link a title was played with on resume, long after
// its release token expired. That used to fail with HTTP 400; it must play the
// copy already in the library instead.
func TestStremioPlayRecoversExpiredReleaseToken(t *testing.T) {
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/lookup/shows" {
			_, _ = w.Write([]byte(`{"id":1,"name":"Black Clover","type":"Animation","externals":{"thetvdb":331753}}`))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(metadata.Close)
	t.Cleanup(overrideTVmazeBaseURL(metadata.URL))
	resetSeriesMetadataCaches()
	t.Cleanup(resetSeriesMetadataCaches)

	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "test.db")})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	userRepo := database.NewUserRepository(db.Connection(), database.DialectSQLite)
	apiKey := "test-api-key"
	require.NoError(t, userRepo.CreateUser(ctx, &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))
	filePath := "complete/TV/Black.Clover.S02E01.1080p.WEB-DL-GRP/Black.Clover.S02E01.1080p.WEB-DL-GRP.mkv"
	_, err = db.Connection().ExecContext(ctx,
		`INSERT INTO file_health (file_path, library_path, status) VALUES (?, ?, 'healthy')`,
		filePath, "/media/TV/Black Clover/Season 02/Black Clover - S02E01.mkv")
	require.NoError(t, err)

	cfg := &config.Config{}
	cfg.Stremio.Prowlarr.Host = "http://indexer.invalid"
	cfg.Stremio.Prowlarr.APIKey = "indexer-key"
	cfg.Stremio.Prowlarr.Enabled = boolPtr(true)
	cfg.Stremio.Enabled = boolPtr(true)
	server := &Server{
		configManager:   &mockConfigManager{cfg: cfg},
		userRepo:        userRepo,
		stremioFailures: newStremioFailureCache(),
		healthRepo:      database.NewHealthRepository(db.Connection(), database.DialectSQLite),
	}
	app := fiber.New()
	app.Use(recover.New())
	app.Get("/stremio/:key/play", server.handleStremioAddonPlay)
	key := auth.HashAPIKey(apiKey)

	play := func(query string) *http.Response {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/stremio/"+key+"/play?"+query, nil), -1)
		require.NoError(t, err)
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	// A link saved by Stremio: its token is unknown (expired) to this server.
	resp := play("release=0123456789abcdef&type=series&indexer=Idx&source=prowlarr&id=tt7441658&season=2&episode=1")
	require.Equal(t, http.StatusFound, resp.StatusCode, "an expired link must recover, not fail with 400")
	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, "/api/files/stream", location.Path)
	assert.Equal(t, filePath, location.Query().Get("path"))

	// Without a content id there is nothing to recover from.
	resp = play("release=0123456789abcdef&type=series")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPickStremioCandidate(t *testing.T) {
	cands := []stremioPlayCandidate{{SafeTitle: "Top.Ranked"}, {SafeTitle: "Original.Choice"}}

	got, matched := pickStremioCandidate(cands, "Original.Choice", "")
	require.NotNil(t, got)
	assert.Equal(t, "Original.Choice", got.SafeTitle)
	assert.True(t, matched, "the release the link named is preferred")

	got, matched = pickStremioCandidate(cands, "Gone.From.Search", "")
	require.NotNil(t, got)
	assert.Equal(t, "Top.Ranked", got.SafeTitle)
	assert.False(t, matched)

	// A stable token identifies the release even when the indexer renamed it.
	got, matched = pickStremioCandidate(cands, "Gone.From.Search", stremioReleaseToken(cands[0]))
	require.NotNil(t, got)
	assert.Equal(t, "Top.Ranked", got.SafeTitle)
	assert.True(t, matched, "the token names the release, not the title")

	// A legacy random token cannot name a release; the title decides.
	got, matched = pickStremioCandidate(cands, "Original.Choice", "0123456789abcdef0123456789abcdef")
	assert.Equal(t, "Original.Choice", got.SafeTitle)
	assert.True(t, matched)

	got, _ = pickStremioCandidate(nil, "Anything", "")
	assert.Nil(t, got)
}

// Play links carry the release name next to the token, so /play can recover
// the release once the token has expired.
func TestStremioPlayLinksCarryReleaseName(t *testing.T) {
	results := []prowlarr.NZBResult{{Title: "Film.2008.1080p.WEB-DL-GRP", DownloadURL: "https://indexer/dl?apikey=secret", Indexer: "Idx"}}
	entries := buildStremioStreamEntries(results, nil, 24, time.Now(), "https://host", "k", "movie", 0, 0, "tt1",
		resolveStreamDisplay(config.StremioConfig{}), func(stremioPlayCandidate) (string, error) { return "tok", nil })
	require.Len(t, entries, 1)
	u, err := url.Parse(entries[0].URL)
	require.NoError(t, err)
	assert.Equal(t, "tok", u.Query().Get("release"))
	assert.Equal(t, sanitizeFilename("Film.2008.1080p.WEB-DL-GRP"), u.Query().Get("title"))
	assert.NotContains(t, entries[0].URL, "secret", "the indexer URL stays hidden")
}
