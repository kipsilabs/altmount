package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
)

const expiredPlayTitle = "Same.Release.Title.2024.1080p.WEB-DL"

// expiredPlayRow is one completed import as the recovery path sees it.
type expiredPlayRow struct {
	nzbPath         string
	storagePath     string
	metadata        *string
	indexer         *string
	contentIdentity *string
	completedAt     time.Time
}

// expiredPlayServer is a Stremio server whose indexer answers every search with
// one same-titled release, plus the database used to seed completed imports.
type expiredPlayServer struct {
	app      *fiber.App
	key      string
	db       *sql.DB
	ctx      context.Context
	searches *atomic.Int32
}

func newExpiredPlayServer(t *testing.T) *expiredPlayServer {
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

	return &expiredPlayServer{
		app:      app,
		key:      auth.HashAPIKey(apiKey),
		db:       db.Connection(),
		ctx:      ctx,
		searches: searches,
	}
}

func (s *expiredPlayServer) insertCompleted(t *testing.T, row expiredPlayRow) {
	t.Helper()
	_, err := s.db.ExecContext(s.ctx,
		`INSERT INTO import_queue (nzb_path, download_id, category, priority, status, retry_count, max_retries, metadata, storage_path, completed_at, indexer, content_identity)
		 VALUES (?, 'stremio:test', 'Movies', 1, 'completed', 0, 3, ?, ?, ?, ?, ?)`,
		row.nzbPath, row.metadata, row.storagePath, row.completedAt, row.indexer, row.contentIdentity)
	require.NoError(t, err)
}

func (s *expiredPlayServer) play(t *testing.T, query string) *http.Response {
	t.Helper()
	resp, err := s.app.Test(httptest.NewRequest(http.MethodGet, "/stremio/"+s.key+"/play?"+query, nil), -1)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// expiredPlayFixture holds two imported releases that sanitize to the same
// title but serve different files.
type expiredPlayFixture struct {
	*expiredPlayServer
	candA stremioPlayCandidate
	candB stremioPlayCandidate
	pathA string
	pathB string
}

func newExpiredPlayFixture(t *testing.T) *expiredPlayFixture {
	t.Helper()

	s := newExpiredPlayServer(t)

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
		s.insertCompleted(t, expiredPlayRow{
			nzbPath:     "/config/.nzbs/Movies/" + stremioPlayNzbFilename(cand),
			storagePath: storagePath,
			metadata:    metadataJSON,
			completedAt: time.Now(),
		})
	}
	pathA := "/storage/release-a/" + candA.SafeTitle + ".mkv"
	pathB := "/storage/release-b/" + candB.SafeTitle + ".mkv"
	insertImported(candA, pathA)
	insertImported(candB, pathB)

	return &expiredPlayFixture{
		expiredPlayServer: s,
		candA:             candA,
		candB:             candB,
		pathA:             pathA,
		pathB:             pathB,
	}
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

// Upstream's TestStremioPlayExpiredTokenWithoutURLDoesNotServeTitleMatch
// asserted the stricter rule that a name-only recovery must always search. A
// completed import of that exact release is safe to serve, but a name that
// matches more than one row is ambiguous: only one shared content identity makes
// its newest row a deterministic answer, so every other ambiguity searches
// again instead of guessing.
func TestStremioPlayExpiredTokenWithoutURLSelectsOneCompletedRelease(t *testing.T) {
	const requestIndexer = "Indexer"

	now := time.Now()
	strPtr := func(value string) *string { return &value }
	storagePath := func(suffix string) string {
		return "/storage/release-" + suffix + "/" + sanitizeFilename(expiredPlayTitle) + ".mkv"
	}
	row := func(suffix string, completedAt time.Time, identity, indexer string) expiredPlayRow {
		result := expiredPlayRow{
			nzbPath:     fmt.Sprintf("/config/.nzbs/Movies/%s-%s.nzb", sanitizeFilename(expiredPlayTitle), suffix),
			storagePath: storagePath(suffix),
			completedAt: completedAt,
		}
		if identity != "" {
			result.contentIdentity = strPtr(identity)
		}
		if indexer != "" {
			result.indexer = strPtr(indexer)
		}
		return result
	}

	tests := []struct {
		name           string
		requestTitle   string
		requestIndexer string
		rows           []expiredPlayRow
		wantServed     string
	}{
		{
			name:           "no completed row searches",
			requestIndexer: requestIndexer,
		},
		{
			name:           "one completed row is served",
			requestIndexer: requestIndexer,
			rows:           []expiredPlayRow{row("only", now, "identity-1", requestIndexer)},
			wantServed:     storagePath("only"),
		},
		{
			name:           "newest row with one shared identity is served",
			requestIndexer: requestIndexer,
			rows: []expiredPlayRow{
				row("old", now.Add(-2*time.Hour), "identity-1", requestIndexer),
				row("newest", now.Add(-1*time.Hour), "identity-1", requestIndexer),
				row("middle", now.Add(-90*time.Minute), "identity-1", requestIndexer),
			},
			wantServed: storagePath("newest"),
		},
		{
			name:           "rows with different identities search",
			requestIndexer: requestIndexer,
			rows: []expiredPlayRow{
				row("first", now.Add(-2*time.Hour), "identity-1", requestIndexer),
				row("second", now.Add(-1*time.Hour), "identity-2", requestIndexer),
			},
		},
		{
			name:           "rows without identity search",
			requestIndexer: requestIndexer,
			rows: []expiredPlayRow{
				row("first", now.Add(-2*time.Hour), "", requestIndexer),
				row("second", now.Add(-1*time.Hour), "", requestIndexer),
			},
		},
		{
			name:           "a different indexer searches",
			requestIndexer: requestIndexer,
			rows:           []expiredPlayRow{row("other-indexer", now, "identity-1", "OtherIndexer")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newExpiredPlayServer(t)
			for _, importRow := range tt.rows {
				s.insertCompleted(t, importRow)
			}

			title := tt.requestTitle
			if title == "" {
				title = sanitizeFilename(expiredPlayTitle)
			}
			query := "release=expired0123456789&title=" + url.QueryEscape(title) +
				"&type=movie&id=tt123&source=prowlarr"
			if tt.requestIndexer != "" {
				query += "&indexer=" + url.QueryEscape(tt.requestIndexer)
			}
			resp := s.play(t, query)

			if tt.wantServed == "" {
				require.NotEqual(t, http.StatusFound, resp.StatusCode,
					"an unknown or ambiguous release name must search instead of serving a row")
				require.Empty(t, resp.Header.Get("Location"),
					"no redirect may be issued for an unresolved release")
				require.Positive(t, s.searches.Load(), "the recovery must search again")
				return
			}

			require.Equal(t, http.StatusFound, resp.StatusCode,
				"a completed release the request identifies must be served")
			location, err := url.Parse(resp.Header.Get("Location"))
			require.NoError(t, err)
			require.Equal(t, "/api/files/stream", location.Path)
			require.Equal(t, tt.wantServed, location.Query().Get("path"),
				"the completed release is served from storage, not searched again")
			assert.Zero(t, s.searches.Load(), "a resolved completed release must not search")
		})
	}
}
