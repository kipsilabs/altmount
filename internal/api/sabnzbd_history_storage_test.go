package api

import (
	"context"
	"encoding/json"
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

func TestCalculateHistoryStoragePath_PreservesDirectories(t *testing.T) {
	for _, name := range []string{"Show.S01.1080p.WEB.h264", "Show.S01.nzb", "Show.S01.r20", "ShowSeasonOne", "Show.S01.symlink.h264"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			want := filepath.Join(root, "complete/tv", name)
			if name == "Show.S01.symlink.h264" {
				require.NoError(t, os.MkdirAll(filepath.Dir(want), 0o755))
				require.NoError(t, os.Symlink(t.TempDir(), want))
			} else {
				require.NoError(t, os.MkdirAll(want, 0o755))
			}
			s := &Server{configManager: &mockConfigManager{cfg: &config.Config{
				MountPath: root,
				Import:    config.ImportConfig{ImportStrategy: config.ImportStrategyNone},
				SABnzbd:   config.SABnzbdConfig{CompleteDir: "complete"},
			}}}
			item := &database.ImportQueueItem{
				Status:      database.QueueStatusCompleted,
				Category:    strPtr("tv"),
				StoragePath: strPtr("/complete/tv/" + name),
			}

			got, exists := s.calculateHistoryStoragePath(item, root)
			assert.Equal(t, want, got)
			assert.True(t, exists)
		})
	}
}

func TestCalculateHistoryStoragePath_FileBehavior(t *testing.T) {
	for _, tt := range []struct {
		name     string
		strategy config.ImportStrategy
		fileName string
		create   bool
		want     string
	}{
		{"existing file returns directory", config.ImportStrategyNone, "episode.mkv", true, "tv/Show"},
		// A missing path is reported as itself, never as its parent,
		// so an arr cannot be sent to scan (or delete) the folder above it.
		{"missing file is reported unchanged", config.ImportStrategyNone, "episode.mkv", false, "tv/Show/episode.mkv"},
		{"symlink strategy returns file", config.ImportStrategySYMLINK, "episode.mkv", true, "tv/Show/episode.mkv"},
		{"strm strategy returns file", config.ImportStrategySTRM, "episode.strm", true, "tv/Show/episode.strm"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			if tt.create {
				require.NoError(t, os.MkdirAll(filepath.Join(root, "tv/Show"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(root, "tv/Show", tt.fileName), []byte("media"), 0o644))
			}
			s := &Server{configManager: &mockConfigManager{cfg: &config.Config{
				MountPath: root,
				Import:    config.ImportConfig{ImportStrategy: tt.strategy, ImportDir: &root},
			}}}
			item := &database.ImportQueueItem{
				Status:      database.QueueStatusCompleted,
				Category:    strPtr("tv"),
				StoragePath: strPtr("/tv/Show/" + tt.fileName),
			}

			got, exists := s.calculateHistoryStoragePath(item, root)
			assert.Equal(t, filepath.Join(root, tt.want), got)
			assert.True(t, exists)
		})
	}
}

func TestSABnzbdHistory_PreservesReleaseDirectories(t *testing.T) {
	db, err := database.NewDB(database.Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "history.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := database.NewRepository(db.Connection(), db.Dialect())
	s := &Server{queueRepo: repo, importerService: &importer.Service{}}
	root := t.TempDir()
	s.configManager = &mockConfigManager{cfg: &config.Config{
		MountPath: root,
		Import:    config.ImportConfig{ImportStrategy: config.ImportStrategyNone},
		SABnzbd:   config.SABnzbdConfig{CompleteDir: "complete"},
	}}
	wantPaths := map[string]string{}
	for _, name := range []string{"Show.S01.h264", "Show.S02.nzb", "Show.S03.r20"} {
		storagePath := "/complete/tv/" + name
		want := filepath.Join(root, "complete/tv", name)
		require.NoError(t, os.MkdirAll(want, 0o755))
		item := &database.ImportQueueItem{
			NzbPath:    name + ".nzb",
			DownloadID: strPtr(name),
			Category:   strPtr("tv"),
			Status:     database.QueueStatusCompleted,
			Priority:   database.QueuePriorityNormal,
		}
		require.NoError(t, repo.AddToQueue(context.Background(), item))
		require.NoError(t, db.Repository.AddStoragePath(context.Background(), item.ID, storagePath))
		wantPaths[name] = want
	}
	app := fiber.New()
	app.Get("/history", s.handleSABnzbdHistory)
	for _, query := range []string{"", "?nzo_ids=Show.S01.h264,Show.S02.nzb,Show.S03.r20"} {
		t.Run(query, func(t *testing.T) {
			resp, err := app.Test(httptest.NewRequest("GET", "/history"+query, nil))
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, 200, resp.StatusCode)
			var result SABnzbdCompleteHistoryResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.Len(t, result.History.Slots, len(wantPaths))
			seen := map[string]bool{}
			for _, slot := range result.History.Slots {
				want, ok := wantPaths[slot.NzoID]
				require.True(t, ok, "unexpected download ID %q", slot.NzoID)
				assert.False(t, seen[slot.NzoID], "duplicate download ID")
				seen[slot.NzoID] = true
				assert.Equal(t, "Completed", slot.Status)
				assert.Equal(t, want, slot.Storage)
				assert.Equal(t, want, slot.Path)
			}
		})
	}
}

func TestCalculateHistoryStoragePathExtensionNames(t *testing.T) {
	mount := t.TempDir()
	cfg := &config.Config{
		MountPath: mount,
		SABnzbd:   config.SABnzbdConfig{CompleteDir: "complete"},
		Import:    config.ImportConfig{ImportStrategy: config.ImportStrategyNone},
	}
	server := &Server{configManager: &mockConfigManager{cfg: cfg}}
	catDir := filepath.ToSlash(filepath.Join(mount, "complete", "tv"))

	require.NoError(t, os.MkdirAll(filepath.Join(catDir, "Show.S01.1080p.h264"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(catDir, "X.r20"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(catDir, "Y.nzb"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(catDir, "Normal"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(catDir, "Normal", "ep.mkv"), []byte("x"), 0o644))

	tests := []struct {
		name        string
		storagePath string
		want        string
	}{
		{"directory ending .h264", "/complete/tv/Show.S01.1080p.h264", catDir + "/Show.S01.1080p.h264"},
		{"directory ending .rNN", "/complete/tv/X.r20", catDir + "/X.r20"},
		{"directory ending .nzb", "/complete/tv/Y.nzb", catDir + "/Y.nzb"},
		{"existing file returns its parent", "/complete/tv/Normal/ep.mkv", catDir + "/Normal"},
		{"missing .h264 path is reported unchanged", "/complete/tv/Gone.h264", catDir + "/Gone.h264"},
		{"missing .nzb path is reported unchanged", "/complete/tv/Gone.nzb", catDir + "/Gone.nzb"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &database.ImportQueueItem{
				ID:          1,
				Category:    strPtr("tv"),
				StoragePath: strPtr(tt.storagePath),
				Status:      database.QueueStatusCompleted,
			}
			got, _ := server.calculateHistoryStoragePath(item, mount)
			assert.Equal(t, tt.want, got)
		})
	}
}
