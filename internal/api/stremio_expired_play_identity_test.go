package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/progress"
)

const expiredPlayTitle = "Same.Release.Title.2024.1080p.WEB-DL"

// expiredPlayRow is one completed import as the recovery path sees it.
type expiredPlayRow struct {
	nzbPath         string
	storagePath     string
	metadata        *string
	indexer         *string
	contentIdentity *string
	fileSize        *int64
	completedAt     time.Time
}

// expiredPlayServer is a Stremio server whose indexer answers every search with
// one same-titled release, plus the database used to seed completed imports.
type expiredPlayServer struct {
	app         *fiber.App
	key         string
	db          *sql.DB
	ctx         context.Context
	searches    *atomic.Int32
	indexerBody *atomic.Value
	server      *Server
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
	indexerBody := &atomic.Value{}
	indexerBody.Store(`[{"title":"` + expiredPlayTitle + `","downloadUrl":"https://indexer.example/release-c.nzb","protocol":"usenet"}]`)
	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		searches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(indexerBody.Load().(string)))
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
		configManager:       &mockConfigManager{cfg: cfg},
		userRepo:            userRepo,
		queueRepo:           database.NewRepository(db.Connection(), database.DialectSQLite),
		stremioFailures:     newStremioFailureCache(),
		progressBroadcaster: progress.NewProgressBroadcaster(),
	}
	app := fiber.New()
	app.Get("/stremio/:key/play", server.handleStremioAddonPlay)

	return &expiredPlayServer{
		app:         app,
		key:         auth.HashAPIKey(apiKey),
		db:          db.Connection(),
		ctx:         ctx,
		searches:    searches,
		indexerBody: indexerBody,
		server:      server,
	}
}

// setIndexerResults replaces the JSON body the fixture's indexer returns.
func (s *expiredPlayServer) setIndexerResults(body string) {
	s.indexerBody.Store(body)
}

func (s *expiredPlayServer) insertCompleted(t *testing.T, row expiredPlayRow) {
	t.Helper()
	_, err := s.db.ExecContext(s.ctx,
		`INSERT INTO import_queue (nzb_path, download_id, category, priority, status, retry_count, max_retries, metadata, storage_path, completed_at, indexer, content_identity, file_size)
		 VALUES (?, 'stremio:test', 'Movies', 1, 'completed', 0, 3, ?, ?, ?, ?, ?, ?)`,
		row.nzbPath, row.metadata, row.storagePath, row.completedAt, row.indexer, row.contentIdentity, row.fileSize)
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
			// Rows written before the stable token existed name every path
			// basename "<title>-<url-hash>"; the recovered title does not carry
			// that hash. The importer suffix is normalised away, so the row is
			// still the one release the play link names.
			name:           "legacy row with the importer url hash in its basenames is served",
			requestIndexer: requestIndexer,
			rows: []expiredPlayRow{{
				nzbPath:         "/config/.nzbs/Movies/" + sanitizeFilename(expiredPlayTitle) + "-71ceade2e434b5dd.nzb",
				storagePath:     "/complete/" + sanitizeFilename(expiredPlayTitle) + "-71ceade2e434b5dd/" + sanitizeFilename(expiredPlayTitle) + "-71ceade2e434b5dd.mkv",
				completedAt:     now,
				contentIdentity: strPtr("identity-1"),
				indexer:         strPtr(requestIndexer),
			}},
			wantServed: "/complete/" + sanitizeFilename(expiredPlayTitle) + "-71ceade2e434b5dd/" + sanitizeFilename(expiredPlayTitle) + "-71ceade2e434b5dd.mkv",
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

// A play link saved before a restart still carries its stable release token.
// Recovery must compare that token with the stored release_token before falling
// back to the release name: the in-memory reference is gone, and a legacy row's
// hashed basenames cannot match the title. A token minted by the old random
// scheme is not stable and falls through to the name rule instead.
func TestStremioPlayRecoveredTokenSurvivesRestart(t *testing.T) {
	s := newExpiredPlayServer(t)

	cand := stremioPlayCandidate{
		Title:       expiredPlayTitle,
		SafeTitle:   sanitizeFilename(expiredPlayTitle),
		DownloadURL: "https://indexer.example/api?t=get&id=release-restart",
		Indexer:     "Indexer",
		Source:      "prowlarr",
		GUID:        "release-restart-guid",
	}
	token := stremioReleaseToken(cand)
	require.True(t, strings.HasPrefix(token, "r2-"))

	// The row was imported under the same release, but its URL-derived release
	// key is stale and its basenames carry the importer's url hash.
	metadataJSON, err := encodeStremioQueueMetadata(stremioQueueMetadata{
		ContentKey:   "movie:tt123:0:0",
		ReleaseKey:   "stale-url-derived-key",
		ReleaseToken: token,
		IMDbID:       "tt123",
		Type:         "movie",
	})
	require.NoError(t, err)
	storage := "/complete/" + cand.SafeTitle + "-71ceade2e434b5dd/" + cand.SafeTitle + "-71ceade2e434b5dd.mkv"
	s.insertCompleted(t, expiredPlayRow{
		nzbPath:     "/config/.nzbs/Movies/" + cand.SafeTitle + "-71ceade2e434b5dd.nzb",
		storagePath: storage,
		metadata:    metadataJSON,
		completedAt: time.Now(),
	})

	// No title and no url: only the saved token can name the release. This is
	// exactly the state after an Altmount restart, when stremioReleaseRefs is
	// empty and the link's token is no longer resolvable in memory.
	resp := s.play(t, "release="+url.QueryEscape(token)+"&type=movie&id=tt123")
	require.Equal(t, http.StatusFound, resp.StatusCode,
		"a saved stable token must find its completed release after a restart")
	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, "/api/files/stream", location.Path)
	require.Equal(t, storage, location.Query().Get("path"))
	assert.Zero(t, s.searches.Load(), "the recovered release must not be searched or fetched again")
}

// A short hex run that belongs to the release name, or a suffix the importer
// did not append, stays part of the name. Only the importer's trailing
// "-<url-hash>" is removed.
func TestNormalizeReleaseNameForMatchingSuffix(t *testing.T) {
	title := normalizeTitleForMatching(expiredPlayTitle)
	year := normalizeTitleForMatching("Blade.Runner.2049")

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "importer hash on an nzb basename is removed",
			in:   "/complete/" + expiredPlayTitle + "-71ceade2e434b5dd.nzb",
			want: title,
		},
		{
			name: "importer hash on a media basename is removed",
			in:   "/complete/" + expiredPlayTitle + "-71ceade2e434b5dd/" + expiredPlayTitle + "-71ceade2e434b5dd.mkv",
			want: title,
		},
		{
			name: "a year before the importer hash is kept",
			in:   "/complete/Blade.Runner.2049-71ceade2e434b5dd.mkv",
			want: year,
		},
		{
			name: "a short hex run is part of the release name",
			in:   "/complete/Film-2024.mkv",
			want: normalizeTitleForMatching("Film-2024"),
		},
		{
			name: "an underscore suffix is not the importer's",
			in:   "/complete/" + expiredPlayTitle + "_71ceade2e434b5dd.mkv",
			want: normalizeTitleForMatching(expiredPlayTitle + "_71ceade2e434b5dd"),
		},
		{
			name: "a suffix away from the end is not stripped",
			in:   "/complete/Film-71ceade2e434b5dd-Extended.mkv",
			want: normalizeTitleForMatching("Film-71ceade2e434b5dd-Extended"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizeReleaseNameForMatching(tt.in))
		})
	}
}

// A live play token names the release even when the URL-derived release key
// stored on its queue rows no longer matches. Before fetching the NZB again,
// /play must try the release's stable token and then the strict name-and-
// identity rule. Ambiguity still fetches rather than guessing.
func TestStremioPlayLiveTokenFallsBackBeforeFetch(t *testing.T) {
	const requestIndexer = "Indexer"

	now := time.Now()
	strPtr := func(value string) *string { return &value }
	title := sanitizeFilename(expiredPlayTitle)
	cand := stremioPlayCandidate{
		Title:       expiredPlayTitle,
		SafeTitle:   title,
		DownloadURL: "https://indexer.example/api?t=get&id=release-live",
		Indexer:     requestIndexer,
		Source:      "prowlarr",
		GUID:        "release-live-guid",
		Size:        int64(9_390_000_000),
	}
	stableToken := stremioReleaseToken(cand)
	other := cand
	other.DownloadURL = "https://indexer.example/api?t=get&id=release-old"
	storedKey := stremioReleaseKey(other)

	storagePath := func(suffix string) string {
		return "/storage/" + suffix + "/" + title + ".mkv"
	}
	row := func(suffix, releaseToken, identity string) expiredPlayRow {
		metadataJSON, err := encodeStremioQueueMetadata(stremioQueueMetadata{
			ContentKey:   "movie:tt123:0:0",
			ReleaseKey:   storedKey,
			ReleaseToken: releaseToken,
			IMDbID:       "tt123",
			Type:         "movie",
		})
		require.NoError(t, err)
		result := expiredPlayRow{
			nzbPath:     fmt.Sprintf("/config/.nzbs/Movies/%s-%s.nzb", title, suffix),
			storagePath: storagePath(suffix),
			metadata:    metadataJSON,
			indexer:     strPtr(requestIndexer),
			completedAt: now,
		}
		if identity != "" {
			result.contentIdentity = strPtr(identity)
		}
		return result
	}
	// withIndexerSize records the size the indexer advertised at admission, the
	// only row-side value that is the same quantity as the listing's size.
	withIndexerSize := func(result expiredPlayRow, size int64) expiredPlayRow {
		var metadata stremioQueueMetadata
		require.NoError(t, json.Unmarshal([]byte(*result.metadata), &metadata))
		metadata.IndexerSize = size
		encoded, err := encodeStremioQueueMetadata(metadata)
		require.NoError(t, err)
		result.metadata = encoded
		return result
	}
	payloadSize := int64(8_521_189_764)

	legacyPayload := row("legacy-payload-size", "", "identity-1")
	legacyPayload.nzbPath = "/config/.nzbs/Movies/" + title + "-71ceade2e434b5dd.nzb"
	legacyPayload.storagePath = "/complete/" + title + "-71ceade2e434b5dd/" + title + "-71ceade2e434b5dd.mkv"
	legacyPayload.fileSize = &payloadSize

	// A STRM row's file_size is the media file's declared length, so unlike an
	// NZB payload it is the same quantity as the listing's size.
	mediaSize := int64(8_000_000_000)
	strmMismatch := expiredPlayRow{
		nzbPath:     "/library/" + title + ".strm",
		storagePath: storagePath("strm-size-mismatch"),
		completedAt: now,
		fileSize:    &mediaSize,
	}

	comparableMismatch := withIndexerSize(
		row("comparable-size-mismatch", "", "identity-1"), payloadSize)
	comparableMatch := withIndexerSize(
		row("comparable-size-match", "", "identity-1"), cand.Size)
	comparableNewerMismatch := withIndexerSize(
		row("comparable-size-newer-mismatch", "", "identity-1"), payloadSize)
	comparableNewerMismatch.completedAt = now.Add(time.Hour)

	tests := []struct {
		name       string
		rows       []expiredPlayRow
		wantServed string
	}{
		{
			name:       "stable token hit serves without fetching",
			rows:       []expiredPlayRow{row("token", stableToken, "")},
			wantServed: storagePath("token"),
		},
		{
			name:       "name and shared identity hit serves without fetching",
			rows:       []expiredPlayRow{row("named", "", "identity-1")},
			wantServed: storagePath("named"),
		},
		{
			// A legacy row stores the NZB payload size, not the indexer's
			// release size. The two are different quantities, so the mismatch
			// must not reject a row the name and identity resolve to.
			name:       "legacy payload size differing from the listing is served",
			rows:       []expiredPlayRow{legacyPayload},
			wantServed: legacyPayload.storagePath,
		},
		{
			// An indexer size recorded at admission is the same quantity as
			// the listing's size: a disagreement means the row is a different
			// release and must fetch again.
			name: "a comparable indexer size that disagrees fetches",
			rows: []expiredPlayRow{comparableMismatch},
		},
		{
			name: "a comparable STRM size that disagrees fetches",
			rows: []expiredPlayRow{strmMismatch},
		},
		{
			name: "a matching indexer size breaks the identity tie",
			rows: []expiredPlayRow{
				comparableNewerMismatch,
				comparableMatch,
			},
			wantServed: storagePath("comparable-size-match"),
		},
		{
			name: "ambiguous identities fetch again",
			rows: []expiredPlayRow{
				row("first", "", "identity-1"),
				row("second", "", "identity-2"),
			},
		},
		{
			name: "a genuinely new release fetches",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newExpiredPlayServer(t)
			for _, importRow := range tt.rows {
				s.insertCompleted(t, importRow)
			}

			token, err := s.server.storeStremioReleaseRef(cand)
			require.NoError(t, err)
			resp := s.play(t, "release="+url.QueryEscape(token)+"&type=movie&id=tt123")

			if tt.wantServed == "" {
				require.NotEqual(t, http.StatusFound, resp.StatusCode,
					"a release the request cannot verify must fetch instead of serving a row")
				require.Empty(t, resp.Header.Get("Location"),
					"no redirect may be issued for an unresolved release")
				// No importer is wired here, so the fallback to the fetch path
				// fails closed after recording the attempt.
				require.True(t, s.server.stremioFailures.Has(cand.SafeTitle, time.Hour),
					"the miss must reach the NZB fetch path")
				return
			}

			require.Equal(t, http.StatusFound, resp.StatusCode,
				"a completed release the live token names must be served")
			location, err := url.Parse(resp.Header.Get("Location"))
			require.NoError(t, err)
			require.Equal(t, "/api/files/stream", location.Path)
			require.Equal(t, tt.wantServed, location.Query().Get("path"),
				"the completed release is served from storage, not fetched again")
			assert.Zero(t, s.searches.Load(), "a resolved completed release must not fetch")
		})
	}
}
