package clients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

// Forced legacy compatibility must remain available on both Sonarr majors. In particular,
// /api/v3/queue still returns one episodeId per record, unlike /api/v5/queue.
// See Sonarr/Sonarr, v5-develop, src/Sonarr.Api.V3/{openapi.json,Queue/QueueController.cs}.
// Exercise both versions through one manager to cover mixed-version setups.
func TestSonarrV4AndV5Compatibility(t *testing.T) {
	m := NewManager(nil)
	ctx := context.Background()
	for _, version := range []string{"4.0.16.2944", "5.0.0.0"} {
		t.Run(version, func(t *testing.T) {
			srv := sonarrCompatibilityServer(t, version)
			instanceURL := srv.URL + "/sonarr"
			require.NoError(t, m.TestConnection(ctx, "sonarr", instanceURL, "test-key", "v3"))
			client, err := m.GetOrCreateSonarrClient(version, instanceURL, "test-key", "v3")
			require.NoError(t, err)
			cached, err := m.GetOrCreateSonarrClient(version, instanceURL, "test-key", "v3")
			require.NoError(t, err)
			require.Same(t, client, cached)
			status, err := client.GetSystemStatusContext(ctx)
			require.NoError(t, err)
			require.Equal(t, version, status.Version)

			roots, err := client.GetRootFoldersContext(ctx)
			require.NoError(t, err)
			require.Len(t, roots, 1)
			require.Equal(t, "/tv", roots[0].Path)
			series, err := client.GetAllSeriesContext(ctx)
			require.NoError(t, err)
			require.Len(t, series, 1)
			require.Equal(t, "/tv/Example", series[0].Path)
			files, err := client.GetSeriesEpisodeFilesContext(ctx, 10)
			require.NoError(t, err)
			require.Len(t, files, 1)
			require.Equal(t, int64(30), files[0].ID)
			episodes, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{EpisodeFileID: 30})
			require.NoError(t, err)
			require.Len(t, episodes, 2)
			require.Equal(t, int64(30), episodes[0].EpisodeFileID)

			// A multi-episode release must retain both repair targets across pages.
			queue, err := client.GetQueueContext(ctx, 0, 1)
			require.NoError(t, err)
			require.Equal(t, 2, queue.TotalRecords)
			require.Len(t, queue.Records, 2)
			require.Equal(t, int64(20), queue.Records[0].EpisodeID)
			require.Equal(t, int64(21), queue.Records[1].EpisodeID)
			require.Equal(t, "release-1", queue.Records[1].DownloadID)
			history, err := client.GetHistoryPageContext(ctx, &starr.PageReq{PageSize: 10})
			require.NoError(t, err)
			require.Len(t, history.Records, 1)
			require.Equal(t, int64(20), history.Records[0].EpisodeID)
			require.Equal(t, "release-1", history.Records[0].DownloadID)

			notification, err := client.AddNotificationContext(ctx, &sonarr.NotificationInput{
				Name: "AltMount", Implementation: "Webhook", ConfigContract: "WebhookSettings", OnGrab: true, OnDownload: true,
			})
			require.NoError(t, err)
			require.True(t, notification.OnGrab)
			downloadClient, err := client.AddDownloadClientContext(ctx, &sonarr.DownloadClientInput{
				Name: "AltMount", Implementation: "Sabnzbd", ConfigContract: "SabnzbdSettings", Enable: true,
			})
			require.NoError(t, err)
			require.True(t, downloadClient.Enable)
			command, err := client.SendCommandContext(ctx, &sonarr.CommandRequest{Name: "EpisodeSearch", EpisodeIDs: []int64{20, 21}})
			require.NoError(t, err)
			require.Equal(t, "EpisodeSearch", command.Name)
			require.NoError(t, client.DeleteEpisodeFileContext(ctx, 30))
			require.NoError(t, client.DeleteQueueContext(ctx, queue.Records[0].ID, &starr.QueueDeleteOpts{BlockList: true}))
		})
	}
}

func sonarrCompatibilityServer(t *testing.T, version string) *httptest.Server {
	t.Helper()
	responses := map[string]string{
		"GET /system/status":     fmt.Sprintf(`{"appName":"Sonarr","version":%q}`, version),
		"GET /rootfolder":        `[{"id":1,"path":"/tv","accessible":true}]`,
		"GET /series":            `[{"id":10,"title":"Example","path":"/tv/Example","monitored":true}]`,
		"GET /episodefile":       `[{"id":30,"seriesId":10,"path":"/tv/Example/episode.mkv","relativePath":"episode.mkv","size":1024}]`,
		"GET /episode":           `[{"id":20,"seriesId":10,"episodeFileId":30,"hasFile":true},{"id":21,"seriesId":10,"episodeFileId":30,"hasFile":true}]`,
		"GET /history":           `{"totalRecords":1,"records":[{"id":40,"episodeId":20,"seriesId":10,"downloadId":"release-1","eventType":"grabbed"}]}`,
		"DELETE /episodefile/30": `{}`,
		"DELETE /queue/101":      `{}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "test-key" {
			http.Error(w, "invalid API key", http.StatusUnauthorized)
			return
		}
		const prefix = "/sonarr/api/v3"
		if !strings.HasPrefix(r.URL.Path, prefix+"/") {
			t.Errorf("unexpected API path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		path := strings.ToLower(strings.TrimPrefix(r.URL.Path, prefix))
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && path == "/queue" {
			page := r.URL.Query().Get("page")
			if page != "1" && page != "2" {
				t.Errorf("unexpected queue page %q", page)
				http.Error(w, "invalid page", http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprintf(w, `{"page":%s,"pageSize":1,"totalRecords":2,"records":[{"id":10%s,"seriesId":10,"episodeId":2%d,"downloadId":"release-1","downloadClient":"AltMount","status":"completed"}]}`, page, page, map[string]int{"1": 0, "2": 1}[page])
			return
		}
		if r.Method == http.MethodPost && (path == "/notification" || path == "/downloadclient" || path == "/command") {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("invalid %s body: %v", path, err)
				http.Error(w, "invalid body", http.StatusBadRequest)
				return
			}
			if path == "/command" {
				assert.Equal(t, []any{float64(20), float64(21)}, body["episodeIds"])
			}
			body["id"] = 50
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(body)
			return
		}
		if body, ok := responses[r.Method+" "+path]; ok {
			_, _ = w.Write([]byte(body))
			return
		}
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSonarrConnectionCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"5.0.0"}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := NewManager(srv.Client()).TestConnection(ctx, "sonarr", srv.URL, "test")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("TestConnection error = %v, want context.Canceled", err)
	}
}
