package api

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

const proxyTestNzbURL = "http://indexer.invalid/release.nzb"

// newRecordingProxy returns a fake HTTP proxy that counts requests for
// proxyTestNzbURL and answers 404 so the handler stops right after the fetch.
func newRecordingProxy(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() == proxyTestNzbURL {
			hits.Add(1)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(proxy.Close)
	return proxy, &hits
}

func newProxyTestConfig(proxyURL string) *config.Config {
	enabled := true
	cfg := &config.Config{}
	cfg.Network.HTTPProxy = proxyURL
	cfg.Stremio.Enabled = &enabled
	return cfg
}

func TestNzbStreamsFetchesNzbURLThroughProxy(t *testing.T) {
	proxy, hits := newRecordingProxy(t)

	db, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL UNIQUE, email TEXT, name TEXT,
		avatar_url TEXT, provider TEXT NOT NULL, provider_id TEXT, password_hash TEXT, api_key TEXT,
		is_admin BOOLEAN DEFAULT 0, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_login DATETIME)`)
	require.NoError(t, err)
	userRepo := database.NewUserRepository(db, database.DialectSQLite)
	apiKey := "test-api-key"
	require.NoError(t, userRepo.CreateUser(context.Background(), &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))

	server := &Server{configManager: &mockConfigManager{cfg: newProxyTestConfig(proxy.URL)}, userRepo: userRepo}
	app := fiber.New()
	app.Post("/api/nzb/streams", server.handleNzbStreams)

	form := url.Values{"nzb_url": {proxyTestNzbURL}, "download_key": {auth.HashAPIKey(apiKey)}}
	req := httptest.NewRequest(http.MethodPost, "/api/nzb/streams", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.EqualValues(t, 1, hits.Load(), "nzb_url fetch must go through the configured proxy")
}

func TestSABnzbdAddUrlFetchesThroughProxy(t *testing.T) {
	proxy, hits := newRecordingProxy(t)

	server := &Server{configManager: &mockConfigManager{cfg: newProxyTestConfig(proxy.URL)}}
	app := fiber.New()
	app.Get("/sabnzbd/api", server.handleSABnzbdAddUrl)

	req := httptest.NewRequest(http.MethodGet, "/sabnzbd/api?name="+url.QueryEscape(proxyTestNzbURL), nil)
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 1, hits.Load(), "addurl fetch must go through the configured proxy")
}
