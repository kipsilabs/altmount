package metadata

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestBackupWorker_performBackup(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "metadata-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	metadataDir := filepath.Join(tempDir, "metadata")
	backupRoot := filepath.Join(tempDir, "backups")
	err = os.MkdirAll(metadataDir, 0755)
	assert.NoError(t, err)

	// Create some dummy .meta files in subdirectories
	err = os.MkdirAll(filepath.Join(metadataDir, "movies"), 0755)
	assert.NoError(t, err)

	metaFiles := []string{
		filepath.Join("movies", "test1.meta"),
		"test2.meta",
		"test3.txt",
	}
	for _, f := range metaFiles {
		err = os.WriteFile(filepath.Join(metadataDir, f), []byte("content"), 0644)
		assert.NoError(t, err)
	}

	enabled := true
	cfg := &config.Config{
		Metadata: config.MetadataConfig{
			RootPath: metadataDir,
			Backup: config.MetadataBackupConfig{
				Enabled:     &enabled,
				Schedule:    "0 3 * * *",
				KeepBackups: 2,
				Path:        backupRoot,
			},
		},
	}

	configGetter := func() *config.Config {
		return cfg
	}

	worker := NewBackupWorker(configGetter)

	// Run backup
	worker.performBackup()

	// Check if backup directory exists
	dirs, err := os.ReadDir(backupRoot)
	assert.NoError(t, err)
	assert.Len(t, dirs, 1)
	assert.True(t, dirs[0].IsDir())

	backupDir := filepath.Join(backupRoot, dirs[0].Name())

	// Verify copied files and structure
	assert.FileExists(t, filepath.Join(backupDir, "movies", "test1.meta"))
	assert.FileExists(t, filepath.Join(backupDir, "test2.meta"))
	assert.NoFileExists(t, filepath.Join(backupDir, "test3.txt"))

	// Test cleanup: create more backups
	time.Sleep(1 * time.Second)
	worker.performBackup()
	time.Sleep(1 * time.Second)
	worker.performBackup()

	dirs, err = os.ReadDir(backupRoot)
	assert.NoError(t, err)
	assert.Len(t, dirs, 2) // Should keep only 2 latest folders
}

// TestBackupWorker_performBackup_SkipsUnreadableDirs verifies that a single
// inaccessible subdirectory (mimicking Windows "System Volume Information" or
// a chmod-000 dir on Linux) does not abort the entire backup. Regression test
// for https://github.com/kipsilabs/altmount/issues/609.
func TestBackupWorker_performBackup_SkipsUnreadableDirs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod-based permission denial is unreliable on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses directory permissions")
	}

	tempDir, err := os.MkdirTemp("", "metadata-test-skip-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	metadataDir := filepath.Join(tempDir, "metadata")
	backupRoot := filepath.Join(tempDir, "backups")
	assert.NoError(t, os.MkdirAll(metadataDir, 0755))

	// Readable .meta file at the root.
	assert.NoError(t, os.WriteFile(filepath.Join(metadataDir, "readable.meta"), []byte("ok"), 0644))

	// Unreadable subdirectory containing a .meta file that should be skipped.
	denied := filepath.Join(metadataDir, "denied")
	assert.NoError(t, os.MkdirAll(denied, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(denied, "hidden.meta"), []byte("x"), 0644))
	assert.NoError(t, os.Chmod(denied, 0))
	// Restore perms so cleanup can succeed.
	defer os.Chmod(denied, 0755) //nolint:errcheck

	enabled := true
	cfg := &config.Config{
		Metadata: config.MetadataConfig{
			RootPath: metadataDir,
			Backup: config.MetadataBackupConfig{
				Enabled:     &enabled,
				Schedule:    "0 3 * * *",
				KeepBackups: 2,
				Path:        backupRoot,
			},
		},
	}

	worker := NewBackupWorker(func() *config.Config { return cfg })
	worker.performBackup()

	// Backup directory should exist (proving the walk did not abort and
	// cleanup-on-error did not RemoveAll it).
	dirs, err := os.ReadDir(backupRoot)
	assert.NoError(t, err)
	assert.Len(t, dirs, 1)

	backupDir := filepath.Join(backupRoot, dirs[0].Name())
	assert.FileExists(t, filepath.Join(backupDir, "readable.meta"))
	assert.NoFileExists(t, filepath.Join(backupDir, "denied", "hidden.meta"))
}

func TestBackupWorker_performBackup_IncludesNzbStore(t *testing.T) {
	tempDir := t.TempDir()

	metadataDir := filepath.Join(tempDir, "metadata")
	backupRoot := filepath.Join(tempDir, "backups")
	configDir := filepath.Join(tempDir, "config")
	nzbStore := filepath.Join(configDir, ".nzbs", "Movies", "42")

	assert.NoError(t, os.MkdirAll(metadataDir, 0755))
	assert.NoError(t, os.MkdirAll(nzbStore, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(metadataDir, "test.meta"), []byte("meta"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(nzbStore, "42-movie.nzbz"), []byte("gz"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(nzbStore, "legacy.nzb"), []byte("xml"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(nzbStore, "notes.txt"), []byte("skip"), 0644))

	enabled := true
	cfg := &config.Config{
		Database: config.DatabaseConfig{Path: filepath.Join(configDir, "altmount.db")},
		Metadata: config.MetadataConfig{
			RootPath: metadataDir,
			Backup: config.MetadataBackupConfig{
				Enabled:     &enabled,
				Schedule:    "0 3 * * *",
				KeepBackups: 2,
				Path:        backupRoot,
			},
		},
	}

	worker := NewBackupWorker(func() *config.Config { return cfg })
	worker.performBackup()

	dirs, err := os.ReadDir(backupRoot)
	assert.NoError(t, err)
	assert.Len(t, dirs, 1)

	backupDir := filepath.Join(backupRoot, dirs[0].Name())
	assert.FileExists(t, filepath.Join(backupDir, "test.meta"))
	assert.FileExists(t, filepath.Join(backupDir, ".nzbs", "Movies", "42", "42-movie.nzbz"))
	assert.FileExists(t, filepath.Join(backupDir, ".nzbs", "Movies", "42", "legacy.nzb"))
	assert.NoFileExists(t, filepath.Join(backupDir, ".nzbs", "Movies", "42", "notes.txt"))
}
