package health

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockRcloneClient implements rclonecli.RcloneRcClient
type MockRcloneClient struct{}

func (m *MockRcloneClient) RefreshDir(ctx context.Context, provider string, dirs []string) error {
	return nil
}

func (m *MockRcloneClient) ForgetDir(ctx context.Context, provider string, dirs []string) error {
	return nil
}

func TestSyncLibrary_WorkerPool(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	// Setup temporary directory for metadata
	tempDir, err := os.MkdirTemp("", "altmount_test_metadata")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Setup per-test database
	db, err := sql.Open("sqlite3", healthTestDSN(t))
	require.NoError(t, err)
	defer db.Close()

	// Initialize database schema
	_, err = db.Exec(`
		CREATE TABLE file_health (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT NOT NULL UNIQUE,
			library_path TEXT,
			status TEXT NOT NULL,
			last_checked DATETIME,
			last_error TEXT,
			retry_count INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 3,
			repair_retry_count INTEGER DEFAULT 0,
			max_repair_retries INTEGER DEFAULT 3,
			source_nzb_path TEXT,
			error_details TEXT,
			metadata TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			release_date DATETIME,
			scheduled_check_at DATETIME,
			streaming_failure_count INTEGER DEFAULT 0,
			is_masked BOOLEAN DEFAULT FALSE,
			indexer TEXT DEFAULT NULL,
			download_id TEXT DEFAULT NULL
		);

		CREATE TABLE IF NOT EXISTS system_state (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`)
	require.NoError(t, err)

	healthRepo := database.NewHealthRepository(db, database.DialectSQLite)
	metadataService := metadata.NewMetadataService(tempDir)

	// Setup configuration
	healthEnabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &healthEnabled
	cfg.Health.LibrarySyncIntervalMinutes = 60
	cfg.Health.LibrarySyncConcurrency = 5 // Small concurrency for testing
	cfg.Metadata.RootPath = tempDir
	cfg.Import.ImportStrategy = config.ImportStrategyNone
	cfg.MountPath = "/mnt/test" // Dummy mount path

	configManager := config.NewManager(cfg, "")

	worker := NewLibrarySyncWorker(
		metadataService,
		healthRepo,
		configManager.GetConfig,
		configManager,
		&MockRcloneClient{},
	)

	// Create some metadata files
	numFiles := 50
	for i := range numFiles {
		fileName := filepath.Join("movies", "movie_"+fmt.Sprintf("%d", i)+".mkv")

		// Create a dummy metadata object
		meta := metadataService.CreateFileMetadata(
			100, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil,
			metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
		)
		err := metadataService.WriteFileMetadata(fileName, meta)
		require.NoError(t, err)
	}

	// Run SyncLibrary
	ctx := context.Background()
	// dryRun = false
	result := worker.SyncLibrary(ctx, false)

	// SyncLibrary returns nil on success (non-dry-run)
	assert.Nil(t, result)

	// Check if files were added to database
	count, err := healthRepo.CountHealthItems(ctx, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, numFiles, count)
}

func TestFindFilesToDelete_RepairTriggered(t *testing.T) {
	worker := &LibrarySyncWorker{}

	dbRecords := []database.AutomaticHealthCheckRecord{
		{
			FilePath: "movie.mkv",
			Status:   database.HealthStatusHealthy,
		},
		{
			FilePath: "repairing.mkv",
			Status:   database.HealthStatusRepairTriggered,
		},
		{
			FilePath: "deleted.mkv",
			Status:   database.HealthStatusHealthy,
		},
	}

	metaFileSet := map[string]string{
		"movie.mkv":     "path/to/meta/movie.mkv.meta",
		"repairing.mkv": "path/to/meta/repairing.mkv.meta",
		"deleted.mkv":   "path/to/meta/deleted.mkv.meta",
	}

	filesInLibrary := map[string]bool{
		"movie.mkv": true,
		// repairing.mkv is MISSING from filesInLibrary (simulating ARR deleted it)
		// deleted.mkv is MISSING from filesInLibrary
	}

	toDelete := worker.findFilesToDelete(context.Background(), dbRecords, metaFileSet, filesInLibrary)

	// repairing.mkv should be protected by its status
	// deleted.mkv should be marked for deletion
	require.Len(t, toDelete, 1)
	assert.Equal(t, "deleted.mkv", toDelete[0])
}

func TestMetaPathToMountRelativePath(t *testing.T) {
	sep := string(filepath.Separator)

	cases := []struct {
		name     string
		rootPath string
		metaPath string
		want     string
	}{
		{
			name:     "simple_root_no_trailing_sep",
			rootPath: "metadata",
			metaPath: filepath.Join("metadata", "complete", "foo", "bar.meta"),
			want:     "complete/foo/bar",
		},
		{
			name:     "root_with_trailing_sep",
			rootPath: "metadata" + sep,
			metaPath: filepath.Join("metadata", "complete", "foo", "bar.meta"),
			want:     "complete/foo/bar",
		},
		{
			name:     "root_with_dot_prefix",
			rootPath: "." + sep + "metadata",
			metaPath: filepath.Join("metadata", "complete", "foo", "bar.meta"),
			want:     "complete/foo/bar",
		},
		{
			name:     "absolute_root",
			rootPath: filepath.Join(string(filepath.Separator), "var", "lib", "metadata"),
			metaPath: filepath.Join(string(filepath.Separator), "var", "lib", "metadata", "complete", "foo", "bar.meta"),
			want:     "complete/foo/bar",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{Metadata: config.MetadataConfig{RootPath: tc.rootPath}}
			worker := &LibrarySyncWorker{
				configGetter: func() *config.Config { return cfg },
			}
			got := worker.metaPathToMountRelativePath(tc.metaPath)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFindFilesToDelete_NormalizesBackslashes(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("filepath.ToSlash only translates backslashes on Windows")
	}
	worker := &LibrarySyncWorker{}

	dbRecords := []database.AutomaticHealthCheckRecord{
		{FilePath: "complete\\foo\\bar.mkv", Status: database.HealthStatusHealthy},
		{FilePath: "complete/orphan.mkv", Status: database.HealthStatusHealthy},
	}
	metaFileSet := map[string]string{
		"complete/foo/bar.mkv": "metadata/complete/foo/bar.mkv.meta",
	}

	toDelete := worker.findFilesToDelete(context.Background(), dbRecords, metaFileSet, nil)
	require.Len(t, toDelete, 1)
	assert.Equal(t, "complete/orphan.mkv", toDelete[0])
}
