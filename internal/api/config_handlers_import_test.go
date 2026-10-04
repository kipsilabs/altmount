package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
