package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConfigImportContentVerification(t *testing.T) {
	enabled, disabled := true, false
	defaultTimeout, customTimeout := 15, 5
	tests := []struct {
		name    string
		enabled *bool
		timeout *int
		want    map[string]any
	}{
		{
			name:    "enabled",
			enabled: &enabled,
			timeout: &defaultTimeout,
			want:    map[string]any{"verify_content": true, "verify_content_timeout_seconds": float64(15)},
		},
		{
			name:    "explicitly disabled with custom timeout",
			enabled: &disabled,
			timeout: &customTimeout,
			want:    map[string]any{"verify_content": false, "verify_content_timeout_seconds": float64(5)},
		},
		{
			name: "unset",
			want: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				Import: config.ImportConfig{
					VerifyContent:               tt.enabled,
					VerifyContentTimeoutSeconds: tt.timeout,
				},
			}
			s := &Server{configManager: config.NewManager(cfg, "")}
			app := fiber.New()
			app.Get("/api/config", s.handleGetConfig)

			resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/config", nil))
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var body struct {
				Success bool `json:"success"`
				Data    struct {
					Import map[string]any `json:"import"`
				} `json:"data"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			require.True(t, body.Success)
			for _, field := range []string{"verify_content", "verify_content_timeout_seconds"} {
				got, present := body.Data.Import[field]
				want, wantPresent := tt.want[field]
				assert.Equal(t, wantPresent, present, "%s presence", field)
				assert.Equal(t, want, got, "%s value", field)
			}
		})
	}
}

func TestRSSConfigMasksAndPreservesSavedFeed(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Import.RSSFeedURL = "https://indexer.example/rss?apikey=secret"
	cfg.Import.RSSPollIntervalSeconds = 600
	response := ToImportAPIResponse(cfg.Import)
	require.Equal(t, "********", response.RSSFeedURL)
	require.Equal(t, 600, response.RSSPollIntervalSeconds)

	manager := config.NewManager(cfg, filepath.Join(t.TempDir(), "config.yaml"))
	s := &Server{configManager: manager}
	app := fiber.New()
	app.Put("/api/config", s.handleUpdateConfig)
	app.Patch("/api/config/:section", s.handlePatchConfigSection)
	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		path := "/api/config"
		if method == http.MethodPatch {
			path += "/import"
		}
		request := httptest.NewRequest(method, path, strings.NewReader(`{"import":{"rss_feed_url":"********","rss_poll_interval_seconds":900}}`))
		request.Header.Set("Content-Type", "application/json")
		result, err := app.Test(request)
		require.NoError(t, err)
		body, err := io.ReadAll(result.Body)
		require.NoError(t, err)
		result.Body.Close()
		require.Equal(t, http.StatusOK, result.StatusCode, string(body))
		require.NotContains(t, string(body), "secret")
		require.Equal(t, cfg.Import.RSSFeedURL, manager.GetConfig().Import.RSSFeedURL)
		require.Equal(t, 900, manager.GetConfig().Import.RSSPollIntervalSeconds)
	}
	request := httptest.NewRequest(http.MethodPatch, "/api/config/import", strings.NewReader(`{"import":{"rss_feed_url":""}}`))
	request.Header.Set("Content-Type", "application/json")
	result, err := app.Test(request)
	require.NoError(t, err)
	defer result.Body.Close()
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Empty(t, manager.GetConfig().Import.RSSFeedURL)
}
