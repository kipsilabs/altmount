package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

// Catches searching despite a cache hit, suppressing search on a miss, and
// serving another show's episode or an expired import as a cache hit.
func TestStremioSkipSearchWhenCached(t *testing.T) {
	for _, tc := range []struct {
		name, setting, release              string
		age                                 time.Duration
		library, wantCached                 bool
		metadataMissing, otherSource, movie bool
		packFiles                           []string
		wantFile                            string
		creatorAlias                        bool
	}{
		{name: "disabled by default", release: "Black.Clover.S02E01.1080p"},
		{name: "explicitly disabled", setting: `{"skip_search_when_cached":false}`, release: "Black.Clover.S02E01.1080p"},
		{name: "cached import", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.S02E01.1080p", wantCached: true},
		{name: "cached season pack", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.S02.1080p", packFiles: []string{"Black.Clover.S02E01.mkv", "Black.Clover.S02E02.mkv"}, wantFile: "Black.Clover.S02E01.mkv", wantCached: true},
		{name: "cached season range", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.S01-S03.1080p", packFiles: []string{"Black.Clover.S01E01.mkv", "Black.Clover.S02E01.mkv"}, wantFile: "Black.Clover.S02E01.mkv", wantCached: true},
		{name: "cached multi season pack", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.2009-2019.11.Temporadas", packFiles: []string{"Black.Clover.S01E01.mkv", "Black.Clover.S02E01.mkv"}, wantFile: "Black.Clover.S02E01.mkv", wantCached: true},
		{name: "title search known creator alias", creatorAlias: true},
		{name: "library creator alias", setting: `{"skip_search_when_cached":true}`, library: true, wantCached: true, creatorAlias: true},
		{name: "healthy library", setting: `{"skip_search_when_cached":true}`, library: true, wantCached: true},
		{name: "empty cache", setting: `{"skip_search_when_cached":true}`},
		{name: "unrelated pack is not inspected", setting: `{"skip_search_when_cached":true}`, release: "Other.Show.S01", packFiles: []string{"Other.Show.S01E01.mkv"}},
		{name: "different show", setting: `{"skip_search_when_cached":true}`, release: "Other.Show.S02E01.1080p"},
		{name: "different episode", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.S02E02.1080p"},
		{name: "expired import", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.S02E01.1080p", age: 48 * time.Hour},
		{name: "expiry disabled", setting: `{"skip_search_when_cached":true,"nzb_ttl_hours":0}`, release: "Black.Clover.S02E01.1080p", age: 48 * time.Hour, wantCached: true},
		{name: "sample", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.S02E01.sample"},
		{name: "metadata unavailable", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.S02E01.1080p", metadataMissing: true},
		{name: "stremio cache without library reuse", setting: `{"skip_search_when_cached":true,"include_library_streams":false}`, release: "Black.Clover.S02E01.1080p", wantCached: true},
		{name: "other source without library reuse", setting: `{"skip_search_when_cached":true,"include_library_streams":false}`, release: "Black.Clover.S02E01.1080p", otherSource: true},
		{name: "cached movie", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.2024.1080p", movie: true, wantCached: true},
		{name: "different movie year", setting: `{"skip_search_when_cached":true}`, release: "Black.Clover.1999.1080p", movie: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.metadataMissing {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.movie {
					_, _ = w.Write([]byte(`{"meta":{"name":"Black Clover","year":"2024"}}`))
				} else if tc.creatorAlias && r.URL.Path == "/lookup/shows" {
					_, _ = w.Write([]byte(`{"id":1,"name":"Harlan Coben's I Will Find You","externals":{"thetvdb":331753}}`))
				} else if tc.creatorAlias && r.URL.Path == "/shows/1/akas" {
					_, _ = w.Write([]byte(`[{"name":"I Will Find You"}]`))
				} else if r.URL.Path == "/lookup/shows" {
					_, _ = w.Write([]byte(`{"id":1,"name":"Black Clover","externals":{"thetvdb":331753}}`))
				} else {
					_, _ = w.Write([]byte(`[]`))
				}
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

			db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "test.db")})
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			ctx := context.Background()
			users := database.NewUserRepository(db.Connection(), database.DialectSQLite)
			apiKey := "test-api-key"
			require.NoError(t, users.CreateUser(ctx, &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))
			filePath := "complete/TV/" + tc.release + ".mkv"
			if tc.release != "" {
				downloadID := "stremio:test"
				if tc.otherSource {
					downloadID = "sonarr:test"
				}
				_, err = db.Connection().ExecContext(ctx, `INSERT INTO import_queue (nzb_path, download_id, status, storage_path, completed_at) VALUES (?, ?, 'completed', ?, ?)`, tc.release+".nzb", downloadID, filePath, time.Now().Add(-tc.age))
				require.NoError(t, err)
			}
			if tc.library {
				filePath = "complete/TV/Black.Clover.S02E01.1080p.mkv"
				libraryPath := "/media/TV/Black Clover/Season 02/Black Clover - S02E01.mkv"
				if tc.creatorAlias {
					filePath = "complete/TV/I.Will.Find.You.S02E01.1080p.mkv"
					libraryPath = "/media/TV/Harlan Coben's I Will Find You/Season 02/I Will Find You - S02E01.mkv"
				}
				_, err = db.Connection().ExecContext(ctx, `INSERT INTO file_health (file_path, library_path, status) VALUES (?, ?, 'healthy')`, filePath, libraryPath)
				require.NoError(t, err)
			}
			var searches atomic.Int32
			indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				searches.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if tc.creatorAlias {
					if strings.Contains(r.URL.Query().Get("query"), "ImdbId:") || strings.Contains(r.URL.Query().Get("query"), "TvdbId:") {
						_, _ = w.Write([]byte(`[]`))
					} else {
						_, _ = w.Write([]byte(`[{"title":"I.Will.Find.You.S02E01.720p","downloadUrl":"https://indexer.example/release.nzb","protocol":"usenet"}]`))
					}
				} else if tc.movie {
					_, _ = w.Write([]byte(`[{"title":"Black.Clover.2024.720p","downloadUrl":"https://indexer.example/release.nzb","protocol":"usenet"}]`))
				} else {
					_, _ = w.Write([]byte(`[{"title":"Black.Clover.S02E01.720p","downloadUrl":"https://indexer.example/release.nzb","protocol":"usenet"}]`))
				}
			}))
			t.Cleanup(indexer.Close)
			cfg := &config.Config{}
			cfg.Stremio.NzbTTLHours = 24
			if tc.setting != "" {
				require.NoError(t, json.Unmarshal([]byte(tc.setting), &cfg.Stremio))
			}
			cfg.Stremio.Enabled = boolPtr(true)
			cfg.Stremio.Prowlarr.Enabled = boolPtr(true)
			cfg.Stremio.Prowlarr.Host = indexer.URL
			cfg.Stremio.Prowlarr.APIKey = "indexer-key"
			server := &Server{configManager: &mockConfigManager{cfg: cfg}, userRepo: users, queueRepo: database.NewRepository(db.Connection(), database.DialectSQLite), healthRepo: database.NewHealthRepository(db.Connection(), database.DialectSQLite), stremioFailures: newStremioFailureCache()}
			var logs bytes.Buffer
			if len(tc.packFiles) > 0 {
				pack, item := newStremioStreamsTestServer(t, tc.packFiles)
				server.metadataService = pack.metadataService
				filePath = filepath.ToSlash(filepath.Join(*item.StoragePath, tc.wantFile))
				_, err = db.Connection().ExecContext(ctx, `UPDATE import_queue SET storage_path = ?`, *item.StoragePath)
				require.NoError(t, err)
				previousLogger := slog.Default()
				slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
				t.Cleanup(func() { slog.SetDefault(previousLogger) })
			}
			app := fiber.New()
			app.Get("/stremio/:key/stream/:type/:id.json", server.handleStremioAddonStream)
			key := auth.HashAPIKey(apiKey)
			content := "series/tt7441658:2:1"
			if tc.movie {
				content = "movie/tt7441658"
			}
			resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/stremio/"+key+"/stream/"+content+".json", nil), -1)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var body struct {
				Streams []StremioStream `json:"streams"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			require.NotEmpty(t, body.Streams)
			require.NotContains(t, logs.String(), "No media file in Stremio release matched")
			if tc.wantCached {
				require.Zero(t, searches.Load(), "cache hit must skip indexer requests")
				require.Len(t, body.Streams, 1)
				u, err := url.Parse(body.Streams[0].URL)
				require.NoError(t, err)
				require.Equal(t, "/api/files/stream", u.Path)
				require.Equal(t, filePath, u.Query().Get("path"))
				require.Equal(t, key, u.Query().Get("download_key"))
			} else {
				require.Positive(t, searches.Load(), "cache miss or disabled option must search")
				require.Contains(t, body.Streams[len(body.Streams)-1].URL, "/play")
			}
		})
	}
}

// Route Cinemeta fallbacks as well as TVmaze calls to the local fixture.
type cachedSearchMetadataTransport struct{ target *url.URL }

func (tr cachedSearchMetadataTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	local := req.Clone(req.Context())
	local.URL.Scheme = tr.target.Scheme
	local.URL.Host = tr.target.Host
	return http.DefaultTransport.RoundTrip(local)
}
