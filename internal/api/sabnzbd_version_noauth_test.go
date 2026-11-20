package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SABnzbd answers mode=version without an API key, and some clients probe
// with it before they send the key. Every other mode still needs one.
func TestSABnzbdVersionNeedsNoAPIKey(t *testing.T) {
	s := &Server{}
	app := fiber.New()
	app.All("/sabnzbd/api", s.handleSABnzbd)

	get := func(t *testing.T, query string) map[string]any {
		t.Helper()
		resp, err := app.Test(httptest.NewRequest("GET", "/sabnzbd/api?"+query, nil))
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var out map[string]any
		require.NoError(t, json.Unmarshal(body, &out))
		return out
	}

	t.Run("version without key is answered", func(t *testing.T) {
		out := get(t, "mode=version&output=json")
		assert.Equal(t, true, out["status"])
		assert.Equal(t, "4.5.0", out["version"])
	})

	t.Run("queue without key is still refused", func(t *testing.T) {
		out := get(t, "mode=queue&output=json")
		assert.Equal(t, false, out["status"])
		assert.Contains(t, out["error"], "Authentication required")
	})

	t.Run("get_config without key is still refused", func(t *testing.T) {
		out := get(t, "mode=get_config&output=json")
		assert.Equal(t, false, out["status"])
	})
}
