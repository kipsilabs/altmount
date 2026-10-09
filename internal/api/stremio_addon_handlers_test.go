package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/prowlarr"
	"github.com/stretchr/testify/assert"
)

func timePtr(t time.Time) *time.Time { return &t }
func boolPtr(b bool) *bool             { return &b }

// cachedItem builds a completed Stremio queue item for cache-lookup tests.
func cachedItem(nzbPath, storagePath string, completedAt *time.Time) *database.ImportQueueItem {
	item := &database.ImportQueueItem{
		NzbPath:     nzbPath,
		CompletedAt: completedAt,
	}
	if storagePath != "" {
		item.StoragePath = strPtr(storagePath)
	}
	return item
}

func TestBuildStremioStreamEntries_CacheDetection(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	title := "Some Movie 2024 1080p WEB-DL"
	// safeFilename the play handler would use for this title.
	safeFilename := sanitizeFilename(title) + ".nzb"
	matchingPath := "/config/.nzbs/Movies/7/" + safeFilename

	tests := []struct {
		name        string
		searchTitle string
		cached      []*database.ImportQueueItem
		ttlHours    int
		wantCached  bool
	}{
		{
			name:       "cached within TTL",
			cached:     []*database.ImportQueueItem{cachedItem(matchingPath, "/storage/movie", timePtr(now.Add(-1*time.Hour)))},
			ttlHours:   24,
			wantCached: true,
		},
		{
			name:       "cached but expired",
			cached:     []*database.ImportQueueItem{cachedItem(matchingPath, "/storage/movie", timePtr(now.Add(-48*time.Hour)))},
			ttlHours:   24,
			wantCached: false,
		},
		{
			name:       "ttl disabled caches regardless of age",
			cached:     []*database.ImportQueueItem{cachedItem(matchingPath, "/storage/movie", timePtr(now.Add(-1000*time.Hour)))},
			ttlHours:   0,
			wantCached: true,
		},
		{
			name:       "completed item without storage path is not cached",
			cached:     []*database.ImportQueueItem{cachedItem(matchingPath, "", timePtr(now.Add(-1*time.Hour)))},
			ttlHours:   24,
			wantCached: false,
		},
		{
			name:       "non-matching path is not cached",
			cached:     []*database.ImportQueueItem{cachedItem("/config/.nzbs/Movies/9/Different Release.nzb", "/storage/other", timePtr(now.Add(-1*time.Hour)))},
			ttlHours:   24,
			wantCached: false,
		},
		{
			name:        "special characters and dot differences match",
			searchTitle: "House.of.the.Dragon.S03E02.Queens.Landing.2160p.HMAX.WEB-DL.DDP5.1.Atmos.DoVi.HDR.H.265-playWEB",
			cached:      []*database.ImportQueueItem{cachedItem("/config/.nzbs/TV/House of the Dragon - S03E02 - Queen's Landing [2160p HMAX WEB-DL DDP5.1 Atmos DoVi HDR H.265-playWEB].nzb.gz", "/storage/hotd", timePtr(now.Add(-1*time.Hour)))},
			ttlHours:    24,
			wantCached:  true,
		},
		{
			name:        "case differences and brackets match",
			searchTitle: "lia one piece 0482 1080p",
			cached:      []*database.ImportQueueItem{cachedItem("/config/.nzbs/TV/[Lia] ONE PIECE - 0482 [1080P].nzb", "/storage/op", timePtr(now.Add(-1*time.Hour)))},
			ttlHours:    24,
			wantCached:  true,
		},
		{
			name:        "sequel title does not match earlier movie (no false positive)",
			searchTitle: "Scream 2 1997 1080p BluRay x264",
			cached:      []*database.ImportQueueItem{cachedItem("/config/.nzbs/Movies/Scream 1996 1080p BluRay x264.nzb", "/storage/scream1", timePtr(now.Add(-1*time.Hour)))},
			ttlHours:    24,
			wantCached:  false,
		},
		{
			name:        "different episode does not match (no false positive)",
			searchTitle: "Show S01E10 1080p WEB-DL",
			cached:      []*database.ImportQueueItem{cachedItem("/config/.nzbs/TV/Show S01E01 1080p WEB-DL.nzb", "/storage/s01e01", timePtr(now.Add(-1*time.Hour)))},
			ttlHours:    24,
			wantCached:  false,
		},
		{
			name:        "different resolution does not match (no false positive)",
			searchTitle: "Movie 2024 2160p UHD REMUX",
			cached:      []*database.ImportQueueItem{cachedItem("/config/.nzbs/Movies/Movie 2024 1080p WEB-DL.nzb", "/storage/movie1080", timePtr(now.Add(-1*time.Hour)))},
			ttlHours:    24,
			wantCached:  false,
		},
		{
			name:       "empty cache is not cached",
			cached:     nil,
			ttlHours:   24,
			wantCached: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sTitle := title
			if tt.searchTitle != "" {
				sTitle = tt.searchTitle
			}
			results := []prowlarr.NZBResult{{Title: sTitle, DownloadURL: "https://prowlarr/dl/1", Size: 1_500_000_000, Indexer: "IdxA"}}

			entries := buildStremioStreamEntries(results, tt.cached, tt.ttlHours, now,
				"https://host", "thekey", "movie", 0, 0, "tt123", config.ProwlarrConfig{})

			if len(entries) != 1 {
				t.Fatalf("expected 1 entry, got %d", len(entries))
			}
			got := entries[0]

			if got.Cached != tt.wantCached {
				t.Errorf("Cached = %v; want %v", got.Cached, tt.wantCached)
			}
			hasBadge := strings.Contains(got.Name, "⚡ Cached")
			if hasBadge != tt.wantCached {
				t.Errorf("badge present = %v; want %v (name=%q)", hasBadge, tt.wantCached, got.Name)
			}
			// URL always routes through /play regardless of cache status.
			if !strings.Contains(got.URL, "/stremio/thekey/play") {
				t.Errorf("URL does not route through /play: %q", got.URL)
			}
			if !strings.Contains(got.URL, "type=movie") {
				t.Errorf("URL missing type param: %q", got.URL)
			}
		})
	}
}

func TestBuildStremioStreamEntries_CachedSortedFirstStable(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

	titles := []string{"Alpha 2024 1080p", "Bravo 2024 1080p", "Charlie 2024 1080p", "Delta 2024 1080p"}
	results := make([]prowlarr.NZBResult, len(titles))
	for i, ti := range titles {
		results[i] = prowlarr.NZBResult{Title: ti, DownloadURL: "https://prowlarr/dl", Size: 1e9}
	}

	// Bravo and Delta are cached; Alpha and Charlie are not.
	cached := []*database.ImportQueueItem{
		cachedItem("/nzbs/"+sanitizeFilename("Bravo 2024 1080p")+".nzb", "/s/b", timePtr(now.Add(-time.Hour))),
		cachedItem("/nzbs/"+sanitizeFilename("Delta 2024 1080p")+".nzb", "/s/d", timePtr(now.Add(-time.Hour))),
	}

	entries := buildStremioStreamEntries(results, cached, 24, now,
		"https://host", "k", "movie", 0, 0, "tt123", config.ProwlarrConfig{})

	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}

	// Expected order: cached group first in original order (Bravo, Delta),
	// then uncached in original order (Alpha, Charlie).
	wantTitles := []string{"Bravo", "Delta", "Alpha", "Charlie"}
	for i, want := range wantTitles {
		if !strings.Contains(entries[i].Name, want) {
			t.Errorf("position %d: name %q does not contain %q", i, entries[i].Name, want)
		}
	}
	if !entries[0].Cached || !entries[1].Cached {
		t.Errorf("first two entries should be cached")
	}
	if entries[2].Cached || entries[3].Cached {
		t.Errorf("last two entries should not be cached")
	}
}

func TestFormatLibraryStream(t *testing.T) {
	libPath := "/library/movies/Sample Movie (2026)/Sample Movie (2026) - [Bluray-2160p][TrueHD Atmos 7.1][DV HDR10][x265]-GROUP.mkv"
	h := &database.FileHealth{
		FilePath:    "complete/movies/Sample.Movie.2026.2160p.UHD.BluRay.x265-GROUP/sample.movie.2026.2160p.uhd.bluray.x265-group.mkv",
		LibraryPath: &libPath,
	}

	name := formatLibraryStreamName(h)
	assert.Contains(t, name, "⚡ Altmount Library")
	assert.Contains(t, name, "2160p")

	title := formatLibraryStreamTitle(h)
	assert.Contains(t, title, "Sample Movie (2026) - [Bluray-2160p][TrueHD Atmos 7.1][DV HDR10][x265]-GROUP")
	assert.Contains(t, title, "💾 Local Library • ⚡ Instant 0s Playback")
}

func TestFindHealthyLibraryStreams_Disabled(t *testing.T) {
	s := &Server{}
	cfg := &config.Config{}
	cfg.Stremio.IncludeLibraryStreams = boolPtr(false)

	streams := s.findHealthyLibraryStreams(context.Background(), cfg, "series", "tt123", "http://host", "key", 1, 1)
	assert.Nil(t, streams)
}

