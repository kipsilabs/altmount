package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

func TestStremioSearchValidatesCachedAndFreshReleases(t *testing.T) {
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/lookup/shows" {
			_, _ = w.Write([]byte(`{"id":1,"name":"Black Clover","externals":{"thetvdb":331753}}`))
		} else {
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
	ctx := context.Background()
	titles := []string{
		"Black.Clover.S02E01.720p",
		"Other.Show.S02E01.1080p",
		"Black.Clover.S02E02.1080p",
		"Black.Clover.S03E01.1080p",
		"Black.Clover.S02E01.1080p",
	}
	for _, title := range titles[1:] {
		_, err := db.Connection().ExecContext(ctx, `INSERT INTO import_queue (nzb_path, download_id, status, storage_path, completed_at) VALUES (?, 'stremio:test', 'completed', ?, ?)`, title+".nzb", "complete/"+title+".mkv", time.Now())
		require.NoError(t, err)
	}
	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		results := make([]map[string]string, 0, len(titles))
		for _, title := range titles {
			results = append(results, map[string]string{"title": title, "downloadUrl": "https://indexer.example/" + title, "protocol": "usenet"})
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(results))
	}))
	t.Cleanup(indexer.Close)
	cfg := &config.Config{}
	cfg.Stremio.Prowlarr.Enabled = boolPtr(true)
	cfg.Stremio.Prowlarr.Host = indexer.URL
	cfg.Stremio.Prowlarr.APIKey = "test-key"
	cfg.Stremio.NzbTTLHours = 24
	server := &Server{queueRepo: database.NewRepository(db.Connection(), database.DialectSQLite), stremioFailures: newStremioFailureCache()}
	results, cached, err := server.searchStremioReleases(ctx, cfg, "series", "tvsearch", "tt7441658", 2, 1)
	require.NoError(t, err)
	require.Len(t, results, 2, "cached imports must pass the same content gate as fresh releases")
	require.Equal(t, "Black.Clover.S02E01.1080p", results[0].Title)
	require.Equal(t, "Black.Clover.S02E01.720p", results[1].Title)
	entries := buildStremioStreamEntries(results, cached, 24, time.Now(), "https://alt.example", "key", "series", 2, 1, "tt7441658", resolveStreamDisplay(cfg.Stremio), server.storeStremioReleaseRef)
	require.Len(t, entries, 2)
	require.True(t, entries[0].Cached)
	require.False(t, entries[1].Cached)
}
