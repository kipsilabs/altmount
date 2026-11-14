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
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/service"
)

var relA = strings.Repeat("a", 64)

type fakePlayback struct {
	mu         sync.Mutex
	cands      []service.PeerCandidate
	offers     map[string]bool
	playErr    error
	result     PeerPlayResult
	gate       chan struct{}
	candCalls  int
	gotSeason  *int
	gotEpisode *int
	offerCalls atomic.Int32
	playCalls  atomic.Int32
	lastReq    PeerPlayRequest
}

func (f *fakePlayback) Candidates(_ context.Context, _ string, season, episode *int) []service.PeerCandidate {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.candCalls++
	f.gotSeason, f.gotEpisode = season, episode
	return f.cands
}
func (f *fakePlayback) HasOffer(_ context.Context, rel string) bool {
	f.offerCalls.Add(1)
	return f.offers[rel]
}
func (f *fakePlayback) Play(_ context.Context, req PeerPlayRequest) (PeerPlayResult, error) {
	f.playCalls.Add(1)
	f.mu.Lock()
	f.lastReq = req
	f.mu.Unlock()
	if f.gate != nil {
		<-f.gate
	}
	return f.result, f.playErr
}

func TestPeerStreams_FeatureOffAddsNothingAndAsksNothing(t *testing.T) {
	s := &Server{}
	assert.Nil(t, s.peerCandidateStreams(context.Background(), &config.Config{}, "movie", "tt1", "http://h", "k", 0, 0))
	f := &fakePlayback{cands: []service.PeerCandidate{{ReleaseID: relA, Title: "X"}}}
	s.SetPeerPlayback(f)
	s.SetPeerPlayback(nil)
	assert.Nil(t, s.peerCandidateStreams(context.Background(), &config.Config{}, "movie", "tt1", "http://h", "k", 0, 0))
	assert.Zero(t, f.candCalls)
}

func TestPeerStreams_ListEntriesAreLabelledSafeAndBounded(t *testing.T) {
	cands := []service.PeerCandidate{
		{ReleaseID: relA, Title: "Film.2020.1080p.WEB-DL-GRP", TotalSize: 5_000_000_000, Healthy: true},
		{ReleaseID: strings.Repeat("b", 64), Title: `<script>alert(1)</script> http://evil.example/x?y=../../etc`, TotalSize: 1},
		{ReleaseID: "not-hex", Title: "bad id"},
		{ReleaseID: strings.Repeat("C", 64), Title: "upper case id"},
	}
	for range 20 {
		cands = append(cands, service.PeerCandidate{ReleaseID: strings.Repeat("d", 64), Title: "dup"})
	}
	f := &fakePlayback{cands: cands}
	s := &Server{}
	s.SetPeerPlayback(f)
	got := s.peerCandidateStreams(context.Background(), &config.Config{}, "movie", "tt0111161", "http://host", "thekey", 0, 0)
	require.GreaterOrEqual(t, len(got), 2)
	assert.LessOrEqual(t, len(got), maxPeerStreams)
	first := got[0]
	assert.Contains(t, first["title"], peerStreamMarker)
	assert.Contains(t, first["title"], "verified on play")
	assert.Contains(t, first["title"], stremioNetworkHealthyLabel, "healthy on the network is said in the fixed local words")
	u, err := url.Parse(first["url"].(string))
	require.NoError(t, err)
	assert.Equal(t, "/stremio/thekey/peer/"+relA, u.Path, "the link names the release, never an address or a bundle id")
	assert.Equal(t, "movie", u.Query().Get("type"))
	assert.Equal(t, "tt0111161", u.Query().Get("id"))
	// Peer text is only ever data inside a JSON string, never a URL or path part.
	for _, st := range got {
		pu, err := url.Parse(st["url"].(string))
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(pu.Path, "/stremio/thekey/peer/"))
		assert.NotContains(t, st["url"], "evil.example")
		assert.NotContains(t, st["url"], "script")
		assert.Regexp(t, `^[0-9a-f]{64}$`, strings.TrimPrefix(pu.Path, "/stremio/thekey/peer/"))
	}
	assert.Nil(t, f.gotSeason)
	assert.Nil(t, f.gotEpisode, "a movie asks for the movie key")

	// A series asks with its season and episode; without them there is no key.
	f.cands = nil
	s.peerCandidateStreams(context.Background(), &config.Config{}, "series", "tt1", "http://host", "k", 2, 5)
	require.NotNil(t, f.gotSeason)
	assert.Equal(t, 2, *f.gotSeason)
	assert.Equal(t, 5, *f.gotEpisode)
	before := f.candCalls
	assert.Nil(t, s.peerCandidateStreams(context.Background(), &config.Config{}, "series", "tt1", "http://host", "k", 0, 0))
	assert.Equal(t, before, f.candCalls, "no season and episode: no lookup")
}

type peerHarness struct {
	app *fiber.App
	srv *Server
	key string
	f   *fakePlayback
}

func newPeerHarness(t *testing.T, f *fakePlayback) *peerHarness {
	t.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "test.db")})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	userRepo := database.NewUserRepository(db.Connection(), database.DialectSQLite)
	apiKey := "test-api-key"
	require.NoError(t, userRepo.CreateUser(context.Background(), &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))
	cfg := &config.Config{}
	cfg.Stremio.Enabled = boolPtr(true)
	srv := &Server{configManager: &mockConfigManager{cfg: cfg}, userRepo: userRepo}
	if f != nil {
		srv.SetPeerPlayback(f)
	}
	app := fiber.New()
	app.Use(recover.New())
	app.Get("/stremio/:key/peer/:release", srv.handleStremioPeerPlay)
	return &peerHarness{app: app, srv: srv, key: auth.HashAPIKey(apiKey), f: f}
}

func (h *peerHarness) get(t *testing.T, key, release, query string) *http.Response {
	t.Helper()
	resp, err := h.app.Test(httptest.NewRequest(http.MethodGet, "/stremio/"+key+"/peer/"+release+"?"+query, nil), -1)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestPeerPlay_ImportsOnceAndRedirectsToTheLibraryStream(t *testing.T) {
	f := &fakePlayback{offers: map[string]bool{relA: true}, result: PeerPlayResult{
		Paths: []string{"complete/Movies/R/sample.mkv", "complete/Movies/R/Film.nfo", "complete/Movies/R/Film.mkv", "complete/Movies/R/extra.mkv"},
		Sizes: []int64{10, 5, 5000, 700}}}
	h := newPeerHarness(t, f)
	resp := h.get(t, h.key, relA, "type=movie&id=tt0111161")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	loc, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, "/api/files/stream", loc.Path)
	assert.Equal(t, "complete/Movies/R/Film.mkv", loc.Query().Get("path"), "the main media file, not a sample or sidecar")
	assert.Equal(t, h.key, loc.Query().Get("download_key"))

	req := f.lastReq
	assert.Equal(t, relA, req.ReleaseID)
	assert.Equal(t, "Movies", req.Category)
	assert.True(t, strings.HasPrefix(req.DownloadID, stremioDownloadIDPrefix))
	assert.Contains(t, req.Metadata, `"unverified_imdb_id":"tt0111161"`, "the media id is kept for local use, marked unverified")
	assert.NotContains(t, req.Metadata, `"imdb_id"`, "a peer's claim must not parse as a publishable media id")
	assert.NotContains(t, req.Metadata, `"type"`)
	assert.Contains(t, req.Metadata, `"content_key":"movie:tt0111161:0:0"`, "the listing still dedups on the content key")
}

func TestPeerPlay_DifferentContentOfOneReleaseDoesNotShareAnImport(t *testing.T) {
	f := &fakePlayback{offers: map[string]bool{relA: true}, gate: make(chan struct{}),
		result: PeerPlayResult{Paths: []string{"c/M/R/Film.mkv"}, Sizes: []int64{9}}}
	h := newPeerHarness(t, f)
	h.srv.peerPlayLimits.Store(newPeerPlayLimiter(100, 100, 100, 100))
	var wg sync.WaitGroup
	for _, id := range []string{"tt1", "tt2"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.get(t, h.key, relA, "type=movie&id="+id)
		}()
	}
	require.Eventually(t, func() bool { return f.playCalls.Load() >= 2 }, 3*time.Second, 5*time.Millisecond,
		"each content key gets its own import metadata")
	close(f.gate)
	wg.Wait()
}

func TestPeerPlay_SeriesCategoryAndEpisodeSelection(t *testing.T) {
	f := &fakePlayback{offers: map[string]bool{relA: true}, result: PeerPlayResult{
		Paths: []string{"c/TV/R/Show.S01E01.mkv", "c/TV/R/Show.S01E02.mkv"}, Sizes: []int64{100, 100}}}
	h := newPeerHarness(t, f)
	resp := h.get(t, h.key, relA, "type=series&id=tt7441658&season=1&episode=2")
	require.Equal(t, http.StatusFound, resp.StatusCode)
	loc, _ := url.Parse(resp.Header.Get("Location"))
	assert.Equal(t, "c/TV/R/Show.S01E02.mkv", loc.Query().Get("path"))
	assert.Equal(t, "TV", f.lastReq.Category)
	assert.Contains(t, f.lastReq.Metadata, `"unverified_season":1`)
	assert.Contains(t, f.lastReq.Metadata, `"unverified_episode":2`)
}

func TestPeerPlay_ConcurrentRequestsShareOneImport(t *testing.T) {
	f := &fakePlayback{offers: map[string]bool{relA: true}, gate: make(chan struct{}),
		result: PeerPlayResult{Paths: []string{"c/M/R/Film.mkv"}, Sizes: []int64{9}}}
	h := newPeerHarness(t, f)
	h.srv.peerPlayLimits.Store(newPeerPlayLimiter(100, 100, 100, 100))
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.Equal(t, http.StatusFound, h.get(t, h.key, relA, "type=movie&id=tt1").StatusCode)
		}()
	}
	require.Eventually(t, func() bool { return f.playCalls.Load() >= 1 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	close(f.gate)
	wg.Wait()
	assert.Equal(t, int32(1), f.playCalls.Load(), "the import ran once for all concurrent plays")
}

func TestPeerPlay_RefusesBadIdsUnknownReleasesAndBadKeysWithoutFetching(t *testing.T) {
	f := &fakePlayback{offers: map[string]bool{relA: true}}
	h := newPeerHarness(t, f)
	for name, tc := range map[string]struct {
		key, rel string
		want     int
	}{
		"not hex":         {h.key, strings.Repeat("z", 64), http.StatusBadRequest},
		"too short":       {h.key, "abcd", http.StatusBadRequest},
		"upper case":      {h.key, strings.Repeat("A", 64), http.StatusBadRequest},
		"path-like":       {h.key, "..%2f..%2fetc%2fpasswd", http.StatusBadRequest},
		"unknown release": {h.key, strings.Repeat("e", 64), http.StatusNotFound},
		"wrong key":       {"nope", relA, http.StatusUnauthorized},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, h.get(t, tc.key, tc.rel, "type=movie&id=tt1").StatusCode)
		})
	}
	assert.Zero(t, f.playCalls.Load(), "none of these may start a fetch")
}

func TestPeerPlay_FeatureOffOrStremioOffIsNotFound(t *testing.T) {
	h := newPeerHarness(t, nil)
	assert.Equal(t, http.StatusNotFound, h.get(t, h.key, relA, "type=movie&id=tt1").StatusCode)
	f := &fakePlayback{offers: map[string]bool{relA: true}}
	h2 := newPeerHarness(t, f)
	h2.srv.configManager.GetConfig().Stremio.Enabled = boolPtr(false)
	assert.Equal(t, http.StatusNotFound, h2.get(t, h2.key, relA, "type=movie&id=tt1").StatusCode)
	assert.Zero(t, f.offerCalls.Load())
}

func TestPeerPlay_FailureLooksLikeAFailedNZBAndLeaksNothing(t *testing.T) {
	f := &fakePlayback{offers: map[string]bool{relA: true}, playErr: assertErr("fetch 10.0.0.5:8470 failed: /var/secret/path")}
	h := newPeerHarness(t, f)
	resp := h.get(t, h.key, relA, "type=movie&id=tt1")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	body := readAll(t, resp)
	assert.NotContains(t, body, "10.0.0.5")
	assert.NotContains(t, body, "/var/secret")

	f2 := &fakePlayback{offers: map[string]bool{relA: true}, result: PeerPlayResult{Paths: []string{"c/M/R/Film.nfo"}, Sizes: []int64{1}}}
	h2 := newPeerHarness(t, f2)
	assert.Equal(t, http.StatusServiceUnavailable, h2.get(t, h2.key, relA, "type=movie&id=tt1").StatusCode, "no media file in the release")
}

func TestPeerPlay_RateLimitedPerKeyAndGlobally(t *testing.T) {
	f := &fakePlayback{offers: map[string]bool{relA: true}, result: PeerPlayResult{Paths: []string{"c/M/R/Film.mkv"}, Sizes: []int64{9}}}
	h := newPeerHarness(t, f)
	h.srv.peerPlayLimits.Store(newPeerPlayLimiter(2, 2, 100, 100))
	codes := []int{}
	for range 4 {
		codes = append(codes, h.get(t, h.key, relA, "type=movie&id=tt1").StatusCode)
	}
	assert.Equal(t, []int{302, 302, 429, 429}, codes, "burst of 2 per key, then refused")

	g := newPeerHarness(t, &fakePlayback{offers: map[string]bool{relA: true}, result: f.result})
	g.srv.peerPlayLimits.Store(newPeerPlayLimiter(100, 100, 1, 1))
	assert.Equal(t, 302, g.get(t, g.key, relA, "type=movie&id=tt1").StatusCode)
	assert.Equal(t, 429, g.get(t, g.key, relA, "type=movie&id=tt1").StatusCode, "the global budget holds whatever the key")
	assert.NotEmpty(t, g.get(t, g.key, relA, "type=movie&id=tt1").Header.Get("Retry-After"))
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return sb.String()
}

func streamList(t *testing.T, h *peerHarness, path string) (string, int) {
	t.Helper()
	h.app.Get("/stremio/:key/stream/:type/:id.json", h.srv.handleStremioAddonStream)
	resp, err := h.app.Test(httptest.NewRequest(http.MethodGet, "/stremio/"+h.key+"/stream/"+path, nil), -1)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return readAll(t, resp), resp.StatusCode
}

func TestStreamList_PeerCandidatesAddedOnlyWithTheFeature(t *testing.T) {
	off := newPeerHarness(t, nil)
	offBody, code := streamList(t, off, "movie/tt0111161.json")
	require.Equal(t, 200, code)

	// Feature on, nothing to offer: the response is byte-identical to off.
	empty := newPeerHarness(t, &fakePlayback{})
	emptyBody, _ := streamList(t, empty, "movie/tt0111161.json")
	assert.Equal(t, offBody, emptyBody)

	// With a candidate the list carries it, and keeps working when the indexer
	// search has nothing.
	f := &fakePlayback{cands: []service.PeerCandidate{{ReleaseID: relA, Title: "Film.2020.1080p-GRP", TotalSize: 4_000_000_000}}}
	on := newPeerHarness(t, f)
	body, _ := streamList(t, on, "movie/tt0111161.json")
	assert.Contains(t, body, peerStreamMarker)
	assert.Contains(t, body, "/stremio/"+on.key+"/peer/"+relA)
	assert.Contains(t, body, "Film.2020.1080p-GRP")
}

// funcPlayback is a PeerPlayback whose candidate lookup is a function.
type funcPlayback struct {
	fakePlayback
	lookup func(ctx context.Context) []service.PeerCandidate
}

func (f *funcPlayback) Candidates(ctx context.Context, _ string, _, _ *int) []service.PeerCandidate {
	return f.lookup(ctx)
}

func peerServer(f PeerPlayback) *Server {
	srv := &Server{}
	srv.SetPeerPlayback(f)
	return srv
}

func startPeer(srv *Server) func() []fiber.Map {
	return srv.startPeerCandidates(context.Background(), &config.Config{}, "movie", "tt0111161", "http://h", "k", 0, 0)
}

// The peer lookup runs next to the indexer search: a lookup as slow as the
// search adds nothing to the listing.
func TestPeerStreams_LookupRunsConcurrentlyWithTheSearch(t *testing.T) {
	const d = 300 * time.Millisecond
	srv := peerServer(&funcPlayback{lookup: func(context.Context) []service.PeerCandidate {
		time.Sleep(d)
		return []service.PeerCandidate{{ReleaseID: relA, Title: "Film.2020.1080p-GRP"}}
	}})
	start := time.Now()
	wait := startPeer(srv)
	time.Sleep(d) // the indexer search
	got := wait()
	elapsed := time.Since(start)
	require.Len(t, got, 1, "the lookup finished with the search and is merged")
	assert.Less(t, elapsed, d+d/2, "serial would take twice as long")
}

// A lookup that hangs never delays the response by more than the grace, and
// the listing carries no peer stream.
func TestPeerStreams_SlowLookupIsBoundedAndFailsOpen(t *testing.T) {
	cancelled := make(chan struct{})
	srv := peerServer(&funcPlayback{lookup: func(ctx context.Context) []service.PeerCandidate {
		<-ctx.Done()
		close(cancelled)
		return nil
	}})
	wait := startPeer(srv)
	start := time.Now()
	got := wait() // the search ended at once
	assert.Empty(t, got)
	assert.Less(t, time.Since(start), peerCandidateGrace+200*time.Millisecond)
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the abandoned lookup was not cancelled")
	}
}

func TestPeerStreams_PanickingLookupFailsOpen(t *testing.T) {
	srv := peerServer(&funcPlayback{lookup: func(context.Context) []service.PeerCandidate { panic("boom") }})
	assert.Empty(t, startPeer(srv)())
}

func TestPeerStreams_FeatureOffStartsNothing(t *testing.T) {
	assert.Nil(t, startPeer(&Server{})())
}

// With no peer streams the merged listing is byte for byte what a listing
// without the peer network produced: library streams, then indexer entries.
func TestStreamList_MergeWithoutPeerStreamsIsByteIdenticalToTheFormerOutput(t *testing.T) {
	library := []fiber.Map{{"name": "L", "title": "lib", "url": "http://h/api/files/stream?path=a"}}
	entries := []stremioStreamEntry{{Name: "N", Title: "T", URL: "http://h/u", BehaviorHints: stremioStreamBehaviorHints{Service: "altmount", Filename: "f"}}}

	got, err := json.Marshal(fiber.Map{"streams": assembleStremioStreams(library, nil, entries)})
	require.NoError(t, err)
	const golden = `{"streams":[{"name":"L","title":"lib","url":"http://h/api/files/stream?path=a"},` +
		`{"behaviorHints":{"service":"altmount","cached":false,"filename":"f"},"name":"N","title":"T","url":"http://h/u"}]}`
	assert.JSONEq(t, golden, string(got))
	// Key order of fiber.Map marshalling is alphabetical and stable; compare the
	// raw bytes against the former construction as well.
	former := make([]fiber.Map, 0, 2)
	former = append(former, library...)
	for _, e := range entries {
		former = append(former, fiber.Map{"name": e.Name, "title": e.Title, "url": e.URL, "behaviorHints": e.BehaviorHints})
	}
	want, err := json.Marshal(fiber.Map{"streams": former})
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))

	// Peer streams slot between the two, in order.
	merged := assembleStremioStreams(library, []fiber.Map{{"name": "P"}}, entries)
	require.Len(t, merged, 3)
	assert.Equal(t, "P", merged[1]["name"])
}
