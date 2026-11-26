package api

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
