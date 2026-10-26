package scanner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/instances"
	"github.com/kipsilabs/altmount/internal/config"
)

// fakeArr serves just enough of the Sonarr/Radarr v3 API for root-folder
// ownership checks: GET /api/v3/rootFolder returning the supplied paths.
func fakeArr(t *testing.T, rootPaths ...string) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/system/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"4.0.0"}`))
	})
	mux.HandleFunc("/api/v3/rootFolder", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body := "["
		for i, p := range rootPaths {
			if i > 0 {
				body += ","
			}
			body += `{"id":` + string(rune('1'+i)) + `,"path":"` + p + `","accessible":true}`
		}
		body += "]"
		_, _ = w.Write([]byte(body))
	})
	// Every other endpoint (series, movies, ...) answers with an empty list so
	// the later fallback strategies find nothing and cannot mask a Strategy 1
	// regression.
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestFindInstanceForFilePath_RootFolderBoundary pins that root-folder
// ownership is decided on path-segment boundaries and that the most specific
// (longest) matching root wins.
//
// Before the fix, root folders were matched with a raw strings.HasPrefix and
// the first matching instance won, so a file under "/mnt/media/tv-4k" was
// claimed by the instance rooted at "/mnt/media/tv" — sending the repair to an
// ARR that owns no such series (issue #749).
func TestFindInstanceForFilePath_RootFolderBoundary(t *testing.T) {
	enabled := true

	tests := []struct {
		name     string
		roots    map[string][]string // instance name -> root folders
		order    []string            // config order of the sonarr instances
		filePath string
		wantName string
	}{
		{
			name: "sibling root sharing a prefix does not claim the file",
			roots: map[string][]string{
				"sonarr":    {"/mnt/media/tv"},
				"sonarr-4k": {"/mnt/media/tv-4k"},
			},
			order:    []string{"sonarr", "sonarr-4k"},
			filePath: "/mnt/media/tv-4k/Show (2024)/Season 1/Show - S01E03.mkv",
			wantName: "sonarr-4k",
		},
		{
			name: "exact root still matches its own instance",
			roots: map[string][]string{
				"sonarr":    {"/mnt/media/tv"},
				"sonarr-4k": {"/mnt/media/tv-4k"},
			},
			order:    []string{"sonarr", "sonarr-4k"},
			filePath: "/mnt/media/tv/Show (2024)/Season 1/Show - S01E03.mkv",
			wantName: "sonarr",
		},
		{
			name: "nested root wins over its parent regardless of config order",
			roots: map[string][]string{
				"sonarr":       {"/mnt/media/tv"},
				"sonarr-anime": {"/mnt/media/tv/anime"},
			},
			order:    []string{"sonarr", "sonarr-anime"},
			filePath: "/mnt/media/tv/anime/Show (2024)/Season 1/Show - S01E03.mkv",
			wantName: "sonarr-anime",
		},
		{
			name: "trailing slash on the configured root is tolerated",
			roots: map[string][]string{
				"sonarr":    {"/mnt/media/tv/"},
				"sonarr-4k": {"/mnt/media/tv-4k/"},
			},
			order:    []string{"sonarr", "sonarr-4k"},
			filePath: "/mnt/media/tv-4k/Show (2024)/Season 1/Show - S01E03.mkv",
			wantName: "sonarr-4k",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sonarrInstances := make([]config.ArrsInstanceConfig, 0, len(tt.order))
			for _, name := range tt.order {
				sonarrInstances = append(sonarrInstances, config.ArrsInstanceConfig{
					Name:    name,
					Enabled: &enabled,
					URL:     fakeArr(t, tt.roots[name]...),
					APIKey:  "test",
				})
			}

			cfg := &config.Config{
				MountPath: "/mnt/media",
				Arrs:      config.ArrsConfig{SonarrInstances: sonarrInstances},
			}
			configGetter := func() *config.Config { return cfg }

			mgr := NewManager(
				configGetter,
				instances.NewManager(configGetter, nil),
				clients.NewManager(nil),
				nil, nil, nil,
			)

			gotType, gotName, err := mgr.findInstanceForFilePath(context.Background(), tt.filePath, "")
			if err != nil {
				t.Fatalf("findInstanceForFilePath returned error: %v", err)
			}
			if gotType != "sonarr" {
				t.Errorf("gotType = %q; want %q", gotType, "sonarr")
			}
			if gotName != tt.wantName {
				t.Errorf("gotName = %q; want %q", gotName, tt.wantName)
			}
		})
	}
}
