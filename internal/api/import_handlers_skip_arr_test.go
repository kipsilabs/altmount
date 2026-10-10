package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManualImportRequest_SkipArrNotification_True(t *testing.T) {
	req := ManualImportRequest{SkipArrNotification: true}
	if !req.SkipArrNotification {
		t.Error("expected SkipArrNotification to be true")
	}
}

func TestManualImportRequest_SkipArrNotification_FalseByDefault(t *testing.T) {
	req := ManualImportRequest{}
	if req.SkipArrNotification {
		t.Error("expected SkipArrNotification to be false by default")
	}
}

func TestHandleManualImportFile_PersistsSkipArrNotification(t *testing.T) {
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "test.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	cfg := &config.Config{}
	cfg.API.KeyOverride = "0123456789abcdef0123456789abcdef"
	server := &Server{
		configManager:   &mockConfigManager{cfg: cfg},
		queueRepo:       database.NewRepository(db.Connection(), db.Dialect()),
		importerService: &importer.Service{},
	}
	app := fiber.New()
	app.Post("/api/import/file", server.handleManualImportFile)

	for _, tc := range []struct {
		name string
		flag *bool
		want bool
	}{
		{"true", new(true), true},
		{"false", new(false), false},
		{"omitted", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "import.nzb")
			require.NoError(t, os.WriteFile(path, []byte("test nzb"), 0o644))
			body := map[string]any{"file_path": path}
			if tc.flag != nil {
				body["skip_arr_notification"] = *tc.flag
			}
			payload, err := json.Marshal(body)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/api/import/file?apikey="+cfg.API.KeyOverride, bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var response struct {
				Data ManualImportResponse `json:"data"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
			require.NotZero(t, response.Data.QueueID)
			stored, err := db.Repository.GetQueueItem(context.Background(), response.Data.QueueID)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, path, stored.NzbPath)
			assert.Equal(t, tc.want, stored.SkipArrNotification)
			assert.False(t, stored.SkipPostImportLinks)
		})
	}
}
