package api

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestNzbStreamsSendsConfiguredUserAgent(t *testing.T) {
	const userAgent = "AltMount-Test-Agent/1.0"

	var gotUserAgent string
	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserAgent = r.UserAgent()
		w.WriteHeader(http.StatusNotFound) // stop the handler right after the fetch
	}))
	t.Cleanup(indexer.Close)

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

	enabled := true
	cfg := &config.Config{UserAgent: userAgent}
	cfg.Stremio.Enabled = &enabled
	server := &Server{configManager: &mockConfigManager{cfg: cfg}, userRepo: userRepo}
	app := fiber.New()
	app.Post("/api/nzb/streams", server.handleNzbStreams)

	form := url.Values{"nzb_url": {indexer.URL + "/release.nzb"}, "download_key": {auth.HashAPIKey(apiKey)}}
	req := httptest.NewRequest(http.MethodPost, "/api/nzb/streams", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, userAgent, gotUserAgent)
}
