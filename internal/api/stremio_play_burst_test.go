package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/database"
)

// The fallback list can repeat a release (Prowlarr exposes the same GUID behind
// several listings). One play request must fetch that release once: after the
// first candidate is recorded as failed, advancing to the repeated release skips
// it instead of downloading the same NZB again.
func TestStremioPlayFallbackFetchesRepeatedReleaseOnce(t *testing.T) {
	s := newExpiredPlayServer(t)
	first := expiredPlayTitle
	second := expiredPlayTitle + ".REPACK"
	// Different download URLs keep both results through deduplication; the shared
	// GUID makes them the same release, so they share one stable token.
	s.setIndexerResults(`[
		{"title":"` + first + `","downloadUrl":"https://indexer.example/repeat-1.nzb","protocol":"usenet","guid":"shared-guid"},
		{"title":"` + second + `","downloadUrl":"https://indexer.example/repeat-2.nzb","protocol":"usenet","guid":"shared-guid"}
	]`)
	s.server.configManager.GetConfig().Stremio.MaxFallbackReleases = 1

	cand := stremioPlayCandidate{
		Title:       first,
		SafeTitle:   sanitizeFilename(first),
		DownloadURL: "https://indexer.example/repeat-1.nzb",
		Source:      "prowlarr",
		GUID:        "shared-guid",
	}
	repeated := cand
	repeated.Title = second
	repeated.SafeTitle = sanitizeFilename(second)
	repeated.DownloadURL = "https://indexer.example/repeat-2.nzb"
	require.Equal(t, stremioReleaseToken(cand), stremioReleaseToken(repeated),
		"the two listings must name the same release")

	token, err := s.server.storeStremioReleaseRef(cand)
	require.NoError(t, err)
	resp := s.play(t, "release="+url.QueryEscape(token)+"&type=movie&id=tt123")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"no importer is wired, so the first fetch fails closed")

	assert.True(t, s.server.stremioFailures.Has(cand.SafeTitle, time.Hour),
		"the first candidate must reach the fetch path")
	assert.False(t, s.server.stremioFailures.Has(repeated.SafeTitle, time.Hour),
		"the repeated release must not be fetched a second time in one play")
}

// A fallback candidate whose import already completed under its stable token is
// served from storage. It must not be fetched again because an earlier candidate
// in the same burst failed.
func TestStremioPlayFallbackServesCompletedTokenWithoutFetch(t *testing.T) {
	s := newExpiredPlayServer(t)
	first := expiredPlayTitle + ".FIRST"
	second := expiredPlayTitle
	s.setIndexerResults(`[
		{"title":"` + first + `","downloadUrl":"https://indexer.example/fallback-first.nzb","protocol":"usenet","guid":"fallback-first-guid"},
		{"title":"` + second + `","downloadUrl":"https://indexer.example/fallback-second.nzb","protocol":"usenet","guid":"fallback-second-guid"}
	]`)
	s.server.configManager.GetConfig().Stremio.MaxFallbackReleases = 1

	firstCand := stremioPlayCandidate{
		Title:       first,
		SafeTitle:   sanitizeFilename(first),
		DownloadURL: "https://indexer.example/fallback-first.nzb",
		Source:      "prowlarr",
		GUID:        "fallback-first-guid",
	}
	secondCand := stremioPlayCandidate{
		Title:       second,
		SafeTitle:   sanitizeFilename(second),
		DownloadURL: "https://indexer.example/fallback-second.nzb",
		Source:      "prowlarr",
		GUID:        "fallback-second-guid",
	}

	// The second release is already imported. Its basename deliberately does not
	// match the title, so only the stable token can find it and the search does
	// not promote it as cached.
	storage := "/storage/fallback/payload.mkv"
	metadataJSON, err := encodeStremioQueueMetadata(stremioQueueMetadata{
		ContentKey:   "movie:tt123:0:0",
		ReleaseKey:   stremioReleaseKey(secondCand),
		ReleaseToken: stremioReleaseToken(secondCand),
		IMDbID:       "tt123",
		Type:         "movie",
	})
	require.NoError(t, err)
	s.insertCompleted(t, expiredPlayRow{
		nzbPath:     "/config/.nzbs/Movies/" + secondCand.SafeTitle + ".nzb",
		storagePath: storage,
		metadata:    metadataJSON,
		completedAt: time.Now(),
	})

	token, err := s.server.storeStremioReleaseRef(firstCand)
	require.NoError(t, err)
	resp := s.play(t, "release="+url.QueryEscape(token)+"&type=movie&id=tt123")
	require.Equal(t, http.StatusFound, resp.StatusCode,
		"a completed fallback release must be served instead of fetched")
	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, "/api/files/stream", location.Path)
	require.Equal(t, storage, location.Query().Get("path"))
	assert.True(t, s.server.stremioFailures.Has(firstCand.SafeTitle, time.Hour),
		"the first candidate was attempted")
}

// The attempt set is scoped to one fallback burst and keyed by the stable
// release token: a repeated release is fetched once, unrelated releases and
// unidentifiable candidates are not blocked.
func TestStremioPlayAttemptsOncePerBurst(t *testing.T) {
	tests := []struct {
		name   string
		tokens []string
		want   []bool
	}{
		{
			name:   "a repeated release is fetched once",
			tokens: []string{"r2-a", "r2-a"},
			want:   []bool{true, false},
		},
		{
			name:   "different releases each fetch once",
			tokens: []string{"r2-a", "r2-b"},
			want:   []bool{true, true},
		},
		{
			name:   "an unidentifiable candidate is never deduplicated",
			tokens: []string{"", ""},
			want:   []bool{true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var attempts stremioPlayAttempts = make(map[string]struct{})
			for i, token := range tt.tokens {
				assert.Equal(t, tt.want[i], attempts.begin(token), "attempt %d", i)
			}
		})
	}
}

// Failed rows are moved to "<safeTitle>-<url-hash>.nzb", and a basename collision
// prefixes the queue id. The exclusion lookup must recognise both shapes, or a
// dead release is fetched again on every play.
func TestStremioFailedPredicateMatchesProductionFailedPaths(t *testing.T) {
	now := time.Now()
	safeTitle := sanitizeFilename(expiredPlayTitle)
	hash := "15567aa46c5fda27"
	tests := []struct {
		name string
		path string
	}{
		{
			name: "url hash suffix",
			path: "/config/.nzbs/failed/Movies/" + safeTitle + "-" + hash + ".nzb",
		},
		{
			name: "queue id prefix and url hash suffix",
			path: "/config/.nzbs/failed/Movies/13285-" + safeTitle + "-" + hash + ".nzb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isFailed := stremioFailedPredicate(
				[]*database.ImportQueueItem{failedItem(tt.path, now)},
				nil, 1, now,
			)
			assert.True(t, isFailed(safeTitle), "path %q must match the release name", tt.path)
		})
	}
}

// An import failure leaves a failed queue row whose path may not equal the
// release name. It must still be remembered across requests, keyed by the
// stable release token, so a second /play does not enqueue the dead release.
func TestStremioImportFailureIsRememberedAcrossPlayRequests(t *testing.T) {
	s := newExpiredPlayServer(t)
	cand := stremioPlayCandidate{
		Title:       expiredPlayTitle,
		SafeTitle:   sanitizeFilename(expiredPlayTitle),
		DownloadURL: "https://indexer.example/import-failure.nzb",
		Source:      "prowlarr",
		Indexer:     "Indexer",
		GUID:        "import-failure-guid",
	}

	metadata, err := encodeStremioQueueMetadata(stremioQueueMetadata{
		ContentKey:   "movie:tt123:0:0",
		ReleaseKey:   stremioReleaseKey(cand),
		ReleaseToken: stremioReleaseToken(cand),
		IMDbID:       "tt123",
		Type:         "movie",
	})
	require.NoError(t, err)
	res, err := s.db.ExecContext(s.ctx,
		`INSERT INTO import_queue (nzb_path, download_id, category, priority, status, retry_count, max_retries, metadata)
		 VALUES (?, 'stremio:test', 'Movies', 1, 'processing', 0, 3, ?)`,
		"/config/.nzbs/Movies/13285-"+stremioPlayNzbFilename(cand), metadata)
	require.NoError(t, err)
	itemID, err := res.LastInsertId()
	require.NoError(t, err)

	token, err := s.server.storeStremioReleaseRef(cand)
	require.NoError(t, err)

	type playResult struct {
		status int
		err    error
	}
	done := make(chan playResult, 1)
	go func() {
		resp, playErr := s.app.Test(httptest.NewRequest(http.MethodGet,
			"/stremio/"+s.key+"/play?release="+url.QueryEscape(token)+"&type=movie&id=tt123", nil), -1)
		if playErr != nil {
			done <- playResult{err: playErr}
			return
		}
		defer resp.Body.Close()
		done <- playResult{status: resp.StatusCode}
	}()

	// Wait until the request is actually waiting on the import, then fail it.
	require.Eventually(t, s.server.progressBroadcaster.HasSubscribers, 5*time.Second, time.Millisecond,
		"the play request must wait on the active import")
	errMessage := "import failed"
	require.NoError(t, s.server.queueRepo.UpdateQueueItemStatus(s.ctx, itemID, database.QueueStatusFailed, &errMessage))
	s.server.progressBroadcaster.NotifyComplete(int(itemID), "failed")

	result := <-done
	require.NoError(t, result.err)
	require.Equal(t, http.StatusServiceUnavailable, result.status)
	assert.True(t, s.server.stremioFailures.Has(stremioFailureKey(cand), time.Hour),
		"the import failure must be remembered by stable release token")
	assert.False(t, s.server.stremioFailures.Has(cand.SafeTitle, time.Hour),
		"the token is preferred over the mutable release name")

	countRows := func() int {
		var count int
		require.NoError(t, s.db.QueryRowContext(s.ctx, `SELECT COUNT(*) FROM import_queue`).Scan(&count))
		return count
	}
	before := countRows()

	resp := s.play(t, "release="+url.QueryEscape(token)+"&type=movie&id=tt123")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, before, countRows(), "the second play must not enqueue the known-failed release")
}

// A fallback chain must not stop on a release that is already known to be dead:
// the play loop checks each candidate before enqueueing it and moves to a live one.
func TestStremioPlayFallbackSkipsKnownFailedCandidate(t *testing.T) {
	s := newExpiredPlayServer(t)
	firstTitle := expiredPlayTitle + ".FIRST"
	deadTitle := expiredPlayTitle + ".DEAD"
	liveTitle := expiredPlayTitle + ".LIVE"
	s.setIndexerResults(`[
		{"title":"` + firstTitle + `","downloadUrl":"https://indexer.example/burst-first.nzb","protocol":"usenet","guid":"burst-first-guid"},
		{"title":"` + deadTitle + `","downloadUrl":"https://indexer.example/burst-dead.nzb","protocol":"usenet","guid":"burst-dead-guid"},
		{"title":"` + liveTitle + `","downloadUrl":"https://indexer.example/burst-live.nzb","protocol":"usenet","guid":"burst-live-guid"}
	]`)
	s.server.configManager.GetConfig().Stremio.MaxFallbackReleases = 2

	_, err := s.db.ExecContext(s.ctx,
		`INSERT INTO import_queue (nzb_path, download_id, category, priority, status, retry_count, max_retries)
		 VALUES (?, 'stremio:dead', 'Movies', 1, 'failed', 0, 3)`,
		"/config/.nzbs/failed/Movies/13285-"+sanitizeFilename(deadTitle)+"-15567aa46c5fda27.nzb")
	require.NoError(t, err)

	firstCand := stremioPlayCandidate{
		Title:       firstTitle,
		SafeTitle:   sanitizeFilename(firstTitle),
		DownloadURL: "https://indexer.example/burst-first.nzb",
		Source:      "prowlarr",
		GUID:        "burst-first-guid",
	}
	token, err := s.server.storeStremioReleaseRef(firstCand)
	require.NoError(t, err)

	resp := s.play(t, "release="+url.QueryEscape(token)+"&type=movie&id=tt123")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	assert.True(t, s.server.stremioFailures.Has(firstCand.SafeTitle, time.Hour),
		"the first candidate was attempted")
	assert.False(t, s.server.stremioFailures.Has(sanitizeFilename(deadTitle), time.Hour),
		"the known-dead fallback must be skipped before it is attempted")
	assert.True(t, s.server.stremioFailures.Has(sanitizeFilename(liveTitle), time.Hour),
		"the live fallback after the dead one must be attempted")
}
