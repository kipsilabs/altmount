package api

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/arrs"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An Upgrade webhook deletes the files it lists one by one. A listed
// path that normalizes to a category folder is refused; an ordinary file of the
// upgrade is still deleted.
func TestHandleArrsWebhook_UpgradeRefusesCategoryRoot(t *testing.T) {
	root := t.TempDir()
	mount := filepath.Join(root, "mnt")
	meta := metadata.NewMetadataService(filepath.Join(root, "metadata"))

	write := func(p string) {
		fm := meta.CreateFileMetadata(100, "x.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
		require.NoError(t, meta.WriteFileMetadata(p, fm))
	}
	// A .meta stored at the category folder's own path is the observable here:
	// DeleteFileMetadata on that path would remove it.
	write("complete/tv")
	write("complete/tv/Old.Release/old.mkv")

	keyOverride := "12345678901234567890123456789012"
	cfg := &config.Config{
		API:       config.APIConfig{KeyOverride: keyOverride},
		MountPath: mount,
		SABnzbd:   config.SABnzbdConfig{CompleteDir: "/complete", Categories: []config.SABnzbdCategory{{Name: "tv"}}},
	}
	server := &Server{
		configManager:   &mockConfigManager{cfg: cfg},
		arrsService:     &arrs.Service{},
		metadataService: meta,
	}
	app := fiber.New()
	app.Post("/api/arrs/webhook", server.handleArrsWebhook)

	payload := map[string]any{
		"eventType": "Upgrade",
		"deletedFiles": []map[string]string{
			{"path": mount + "/complete/tv"},
			{"path": mount + "/complete"},
			{"path": mount},
			{"path": mount + "/complete/tv/Old.Release/old.mkv"},
		},
	}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest("POST", "/api/arrs/webhook?apikey="+keyOverride, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	assert.True(t, meta.FileExists("complete/tv"), "category folder path must not be deleted")
	assert.False(t, meta.FileExists("complete/tv/Old.Release/old.mkv"), "the upgraded file is still deleted")
}
