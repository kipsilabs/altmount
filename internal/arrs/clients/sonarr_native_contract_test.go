package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

func TestNativeEndpointParity(t *testing.T) {
	ctx := context.Background()
	data, err := os.ReadFile("testdata/sonarr/native-contract.json")
	require.NoError(t, err)
	var contract struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	require.NoError(t, json.Unmarshal(data, &contract))
	for _, tc := range []struct {
		name, method, path, response string
		query                        map[string][]string
		body                         map[string]any
		call                         func(SonarrClient) error
	}{
		{name: "status", method: "GET", path: "/system/status", response: `{"appName":"Sonarr","version":"5.0.0"}`, call: func(c SonarrClient) error { _, e := c.GetSystemStatusContext(ctx); return e }},
		{name: "roots", method: "GET", path: "/rootfolder", response: `[{"id":1,"path":"/tv"}]`, call: func(c SonarrClient) error {
			v, e := c.GetRootFoldersContext(ctx)
			if e == nil {
				require.Equal(t, "/tv", v[0].Path)
			}
			return e
		}},
		{name: "series", method: "GET", path: "/series", response: `[{"id":1}]`, call: func(c SonarrClient) error { _, e := c.GetAllSeriesContext(ctx); return e }},
		{name: "one series", method: "GET", path: "/series/1", response: `{"id":1}`, call: func(c SonarrClient) error { _, e := c.GetSeriesByIDContext(ctx, 1); return e }},
		{name: "series files", method: "GET", path: "/episodefile", query: map[string][]string{"seriesId": {"1"}}, response: `[]`, call: func(c SonarrClient) error { _, e := c.GetSeriesEpisodeFilesContext(ctx, 1); return e }},
		{name: "files", method: "GET", path: "/episodefile", query: map[string][]string{"episodeFileIds": {"2", "3"}}, response: `[]`, call: func(c SonarrClient) error { _, e := c.GetEpisodeFilesContext(ctx, 2, 3); return e }},
		{name: "episodes", method: "GET", path: "/episode", query: map[string][]string{"seriesId": {"1"}, "seasonNumber": {"2"}, "episodeFileId": {"3"}, "episodeIds": {"4", "5"}, "includeSubresources": {"images"}}, response: `[]`, call: func(c SonarrClient) error {
			_, e := c.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{SeriesID: 1, SeasonNumber: 2, EpisodeFileID: 3, EpisodeIDs: []int64{4, 5}, IncludeImages: true})
			return e
		}},
		{name: "one episode", method: "GET", path: "/episode/1", response: `{"id":1}`, call: func(c SonarrClient) error { _, e := c.GetEpisodeByIDContext(ctx, 1); return e }},
		{name: "history", method: "GET", path: "/history", response: `{"records":[]}`, query: map[string][]string{"seriesIds": {"1"}}, call: func(c SonarrClient) error {
			p := &starr.PageReq{}
			p.CheckSet("seriesId", "1")
			_, e := c.GetHistoryPageContext(ctx, p)
			return e
		}},
		{name: "fail", method: "POST", path: "/history/failed/1", response: `{}`, call: func(c SonarrClient) error { return c.FailContext(ctx, 1) }},
		{name: "monitor", method: "PUT", path: "/episode/monitor", body: map[string]any{"episodeIds": []any{float64(1), float64(2)}, "monitored": false}, response: `[]`, call: func(c SonarrClient) error { _, e := c.MonitorEpisodeContext(ctx, []int64{1, 2}, false); return e }},
		{name: "delete file", method: "DELETE", path: "/episodefile/1", call: func(c SonarrClient) error { return c.DeleteEpisodeFileContext(ctx, 1) }},
		{name: "connections", method: "GET", path: "/connection", response: `[]`, call: func(c SonarrClient) error { _, e := c.GetNotificationsContext(ctx); return e }},
		{name: "add connection", method: "POST", path: "/connection", response: `{"id":1}`, body: map[string]any{"name": "AltMount", "implementation": "Webhook"}, call: func(c SonarrClient) error {
			_, e := c.AddNotificationContext(ctx, &sonarr.NotificationInput{Name: "AltMount", Implementation: "Webhook"})
			return e
		}},
		{name: "update connection", method: "PUT", path: "/connection/1", response: `{"id":1}`, call: func(c SonarrClient) error {
			_, e := c.UpdateNotificationContext(ctx, &sonarr.NotificationInput{ID: 1})
			return e
		}},
		{name: "clients", method: "GET", path: "/downloadclient", response: `[]`, call: func(c SonarrClient) error { _, e := c.GetDownloadClientsContext(ctx); return e }},
		{name: "add client", method: "POST", path: "/downloadclient", response: `{"id":1}`, query: map[string][]string{"skipTesting": {"true"}}, body: map[string]any{"name": "AltMount", "implementation": "Sabnzbd"}, call: func(c SonarrClient) error {
			_, e := c.AddDownloadClientContext(ctx, &sonarr.DownloadClientInput{Name: "AltMount", Implementation: "Sabnzbd"})
			return e
		}},
		{name: "test client", method: "POST", path: "/downloadclient/test", response: `{}`, call: func(c SonarrClient) error {
			return c.TestDownloadClientContext(ctx, &sonarr.DownloadClientInput{Name: "AltMount"})
		}},
		{name: "update client", method: "PUT", path: "/downloadclient/1", response: `{"id":1}`, query: map[string][]string{"skipTesting": {"true"}}, call: func(c SonarrClient) error {
			_, e := c.UpdateDownloadClientContext(ctx, &sonarr.DownloadClientInput{ID: 1}, true)
			return e
		}},
		{name: "command list", method: "GET", path: "/command", response: `[]`, call: func(c SonarrClient) error { _, e := c.GetCommandsContext(ctx); return e }},
		{name: "send command", method: "POST", path: "/command", response: `{"id":1}`, body: map[string]any{"name": "EpisodeSearch", "episodeIds": []any{float64(1), float64(2)}}, call: func(c SonarrClient) error {
			_, e := c.SendCommandContext(ctx, &sonarr.CommandRequest{Name: "EpisodeSearch", EpisodeIDs: []int64{1, 2}})
			return e
		}},
		{name: "command poll", method: "GET", path: "/command/1", response: `{"id":1}`, call: func(c SonarrClient) error { _, e := c.GetCommandStatusContext(ctx, 1); return e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called++
				contractPath := "/api/v5" + tc.path
				if _, exists := contract.Paths[contractPath]; !exists {
					split := strings.LastIndex(contractPath, "/")
					contractPath = contractPath[:split] + "/{id}"
				}
				require.NotEmpty(t, contract.Paths[contractPath][strings.ToLower(tc.method)], "operation must exist in pinned native schema")
				require.Equal(t, tc.method, r.Method)
				require.Equal(t, "/base/api/v5"+tc.path, r.URL.Path)
				require.Equal(t, "key", r.Header.Get("X-Api-Key"))
				require.Empty(t, r.URL.Query().Get("forceSave"))
				for k, v := range tc.query {
					require.Equal(t, v, r.URL.Query()[k])
				}
				if tc.path == "/history" {
					require.Empty(t, r.URL.Query().Get("seriesId"))
				}
				if len(tc.body) > 0 {
					var body map[string]any
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					for k, v := range tc.body {
						require.Equal(t, v, body[k])
					}
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.response == "" {
					w.WriteHeader(204)
				} else {
					_, _ = w.Write([]byte(tc.response))
				}
			}))
			defer srv.Close()
			c := newNativeSonarr(&starr.Config{URL: srv.URL + "/base", APIKey: "key", Client: srv.Client()})
			require.NoError(t, tc.call(c))
			require.Equal(t, 1, called)
		})
	}
}

func TestNativeGenericCommandAndHealth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Contains(t, []string{"/api/v5/command", "/api/v5/command/1", "/api/v5/health"}, r.URL.Path)
		if r.Method == http.MethodPost {
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "manual", body["trigger"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := newNativeSonarr(&starr.Config{URL: srv.URL, Client: srv.Client()})
	var out any
	require.NoError(t, c.PostInto(context.Background(), starr.Request{URI: "v3/command", Body: strings.NewReader(`{"name":"EpisodeSearch","episodeIds":[1],"trigger":"manual"}`)}, &out))
	require.NoError(t, c.GetInto(context.Background(), starr.Request{URI: "v3/command/1"}, &out))
	require.NoError(t, c.GetInto(context.Background(), starr.Request{URI: "/health"}, &out))
}
