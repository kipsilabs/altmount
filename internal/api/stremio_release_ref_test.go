package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/config"
)

func releaseRefServer(ttlSeconds int) *Server {
	cfg := config.DefaultConfig()
	cfg.Stremio.StreamTTLSeconds = ttlSeconds
	return &Server{configManager: &mockConfigManager{cfg: cfg}}
}

// A play link must outlive the 15 minutes it used to get: players re-request
// it when seeking, so rewinding later in a film failed with HTTP 400.
func TestStremioReleaseRefOutlivesPlayback(t *testing.T) {
	s := releaseRefServer(0)
	token, err := s.storeStremioReleaseRef(stremioPlayCandidate{Title: "Film.2008.1080p.WEB-DL-GRP"})
	require.NoError(t, err)

	ref := s.stremioReleaseRefs[token]
	assert.WithinDuration(t, time.Now().Add(24*time.Hour), ref.ExpiresAt, time.Minute, "default lifetime is 24h")

	// Two hours into the film the player seeks and re-requests the link.
	ref.ExpiresAt = time.Now().Add(-2*time.Hour + 24*time.Hour)
	s.stremioReleaseRefs[token] = ref
	cand, ok := s.consumeStremioReleaseRef(token)
	require.True(t, ok)
	assert.Equal(t, "Film.2008.1080p.WEB-DL-GRP", cand.Title)
	assert.WithinDuration(t, time.Now().Add(24*time.Hour), s.stremioReleaseRefs[token].ExpiresAt, time.Minute,
		"using the link extends it")

	// It can be used again (every seek re-requests it).
	_, ok = s.consumeStremioReleaseRef(token)
	assert.True(t, ok)
}

func TestStremioReleaseRefExpires(t *testing.T) {
	s := releaseRefServer(0)
	token, err := s.storeStremioReleaseRef(stremioPlayCandidate{Title: "x"})
	require.NoError(t, err)
	ref := s.stremioReleaseRefs[token]
	ref.ExpiresAt = time.Now().Add(-time.Second)
	s.stremioReleaseRefs[token] = ref

	_, ok := s.consumeStremioReleaseRef(token)
	assert.False(t, ok)
	_, kept := s.stremioReleaseRefs[token]
	assert.False(t, kept, "an expired token is removed")

	_, ok = s.consumeStremioReleaseRef("unknown")
	assert.False(t, ok)
}

func TestStremioReleaseLifetime(t *testing.T) {
	assert.Equal(t, 24*time.Hour, releaseRefServer(0).stremioReleaseLifetime(), "unset uses 24h")
	assert.Equal(t, 6*time.Hour, releaseRefServer(6*3600).stremioReleaseLifetime(), "follows stream_ttl_seconds")
	assert.Equal(t, time.Hour, releaseRefServer(60).stremioReleaseLifetime(), "never below an hour")
	assert.Equal(t, 24*time.Hour, (&Server{}).stremioReleaseLifetime(), "no config manager")
}

// A release token is derived from the release, so a listing, a reopened play
// link and a restart all present the same value for the same release.
func TestStremioReleaseTokenIsStable(t *testing.T) {
	s := releaseRefServer(0)
	cand := stremioPlayCandidate{
		Title:     "Film.2024.1080p.WEB-DL-GRP",
		SafeTitle: sanitizeFilename("Film.2024.1080p.WEB-DL-GRP"),
		Indexer:   "Idx",
		Source:    "prowlarr",
		GUID:      "guid-a",
		Size:      100,
	}

	first, err := s.storeStremioReleaseRef(cand)
	require.NoError(t, err)
	second, err := s.storeStremioReleaseRef(cand)
	require.NoError(t, err)
	assert.Equal(t, first, second, "the same release presents the same token to every listing")
	assert.Equal(t, first, stremioReleaseToken(cand), "the token does not depend on the listing")

	other := cand
	other.GUID = "guid-b"
	assert.NotEqual(t, first, stremioReleaseToken(other), "a different GUID is a different release")

	// An indexer that reports no GUID falls back to the release name and size.
	noGUID := cand
	noGUID.GUID = ""
	assert.Equal(t, stremioReleaseToken(noGUID), stremioReleaseToken(noGUID))
	bySize := noGUID
	bySize.Size = 200
	assert.NotEqual(t, stremioReleaseToken(noGUID), stremioReleaseToken(bySize))

	// A token minted by the previous scheme still resolves while it lives.
	legacy := "0123456789abcdef0123456789abcdef"
	s.stremioReleaseRefs[legacy] = stremioReleaseRef{Candidate: cand, ExpiresAt: time.Now().Add(time.Hour)}
	got, ok := s.consumeStremioReleaseRef(legacy)
	require.True(t, ok)
	assert.Equal(t, cand.Title, got.Title)
}
