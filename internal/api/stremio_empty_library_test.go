package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

func TestStremioAddonStreamEmptyLibrary(t *testing.T) {
	for _, tc := range []struct {
		name, results string
		wantStreams   int
	}{
		{"no provider matches", `[]`, 0},
		{"overlapping title metadata", `[{"title":"[ABC 1080p] Black Clover S02E01","downloadUrl":"https://indexer.example/release.nzb","protocol":"usenet"}]`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/lookup/shows":
					_, _ = w.Write([]byte(`{"id":1,"name":"Black Clover","type":"Animation","externals":{"thetvdb":331753}}`))
				default:
					_, _ = w.Write([]byte(`[]`))
				}
			}))
			t.Cleanup(metadata.Close)
			t.Cleanup(overrideTVmazeBaseURL(metadata.URL))
			resetSeriesMetadataCaches()
			t.Cleanup(resetSeriesMetadataCaches)

			db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "test.db")})
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			userRepo := database.NewUserRepository(db.Connection(), database.DialectSQLite)
			apiKey := "test-api-key"
			require.NoError(t, userRepo.CreateUser(context.Background(), &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))
			indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.results))
			}))
			t.Cleanup(indexer.Close)
			cfg := &config.Config{}
			cfg.Stremio.Prowlarr.Host = indexer.URL
			cfg.Stremio.Prowlarr.APIKey = "indexer-key"
			cfg.Stremio.Prowlarr.Enabled = boolPtr(true)
			cfg.Stremio.Enabled = boolPtr(true)
			server := &Server{configManager: &mockConfigManager{cfg: cfg}, userRepo: userRepo, stremioFailures: newStremioFailureCache(), healthRepo: database.NewHealthRepository(db.Connection(), database.DialectSQLite)}
			app := fiber.New()
			// Convert any handler panic into a failing HTTP assertion in this test.
			app.Use(recover.New())
			app.Get("/stremio/:key/stream/:type/:id.json", server.handleStremioAddonStream)
			resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/stremio/"+auth.HashAPIKey(apiKey)+"/stream/series/tt7441658:2:1.json", nil), -1)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var body map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			streams, ok := body["streams"].([]any)
			require.True(t, ok, "streams must be a JSON array")
			require.Len(t, streams, tc.wantStreams)
			if tc.wantStreams == 0 {
				require.Equal(t, map[string]any{"streams": []any{}}, body)
			} else {
				stream := streams[0].(map[string]any)
				require.Contains(t, stream["name"], "[ABC 1080p] Black Clover S02E01")
				require.Contains(t, stream["url"], "/play")
			}
		})
	}
}
