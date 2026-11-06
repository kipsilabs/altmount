package stremio

import (
	"context"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
)

// StremioCleanupService periodically removes expired Stremio-originated queue items
// along with their associated .meta files, storage directory, health records and
// persistent NZB files. A download the media library has since adopted (an ARR
// imported the same file) is kept: only its Stremio queue entry is dropped.
type StremioCleanupService struct {
	queueRepo       *database.Repository
	metadataService *metadata.MetadataService
	health          HealthRecords
	configGetter    config.ConfigGetter
}

// HealthRecords is the health store the cleanup consults and tidies (satisfied
// by *database.HealthRepository). Optional: nil skips the library check and
// health record removal.
type HealthRecords interface {
	GetFileHealth(ctx context.Context, filePath string) (*database.FileHealth, error)
	DeleteHealthRecord(ctx context.Context, filePath string) error
}

// NewStremioCleanupService creates a new StremioCleanupService.
func NewStremioCleanupService(
	queueRepo *database.Repository,
	metadataService *metadata.MetadataService,
	health HealthRecords,
	configGetter config.ConfigGetter,
) *StremioCleanupService {
	return &StremioCleanupService{
		queueRepo:       queueRepo,
		metadataService: metadataService,
		health:          health,
		configGetter:    configGetter,
	}
}

// StartCleanup launches a background goroutine that runs cleanup every hour.
// The goroutine stops when ctx is cancelled.
func (s *StremioCleanupService) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.cleanupExpired(ctx)
			}
		}
	}()
}

func (s *StremioCleanupService) cleanupExpired(ctx context.Context) {
	cfg := s.configGetter()
	ttlHours := cfg.Stremio.NzbTTLHours
	if ttlHours <= 0 {
		return
	}

	items, err := s.queueRepo.GetExpiredStremioQueueItems(ctx, ttlHours)
	if err != nil {
		slog.ErrorContext(ctx, "StremioCleanup: failed to query expired items", "error", err)
		return
	}

	for _, item := range items {
		s.deleteItem(ctx, item)
	}

	if len(items) > 0 {
		slog.InfoContext(ctx, "StremioCleanup: cleaned up expired items", "count", len(items))
	}
}

func (s *StremioCleanupService) deleteItem(ctx context.Context, item *database.ImportQueueItem) {
	files := s.itemFiles(item)
	if linked := s.libraryLinkedFile(ctx, files); linked != "" {
		// An ARR imported this very file, so the library's symlink points at
		// it: deleting it would break the library. It is a library file now,
		// so stop tracking it as a Stremio download.
		slog.InfoContext(ctx, "StremioCleanup: keeping expired Stremio download used by the media library",
			"path", linked, "queue_id", item.ID)
		if err := s.queueRepo.RemoveFromQueue(ctx, item.ID); err != nil {
			slog.ErrorContext(ctx, "StremioCleanup: failed to remove queue item", "id", item.ID, "error", err)
		}
		return
	}

	if item.StoragePath != nil && *item.StoragePath != "" {
		storagePath := *item.StoragePath
		// A storage path recorded as a category folder (#1013) would make the
		// directory delete below remove every release in it (#1029).
		var cfg *config.Config
		if s.configGetter != nil {
			cfg = s.configGetter()
		}
		if cfg != nil && cfg.IsCategoryRootVirtualPath(storagePath) {
			slog.WarnContext(ctx, "StremioCleanup: refusing to delete a category folder; dropping the queue item only",
				"path", storagePath, "queue_id", item.ID)
			if err := s.queueRepo.RemoveFromQueue(ctx, item.ID); err != nil {
				slog.ErrorContext(ctx, "StremioCleanup: failed to remove queue item", "id", item.ID, "error", err)
			}
			return
		}
		if s.metadataService.DirectoryExists(storagePath) {
			// Multi-file: remove the entire metadata subtree (including nested .meta files)
			if err := s.metadataService.DeleteDirectory(storagePath); err != nil {
				slog.ErrorContext(ctx, "StremioCleanup: failed to delete storage directory",
					"path", storagePath, "error", err)
			}
			// Remove the persistent NZB file (one source NZB shared by all files in the dir)
			if err := os.Remove(item.NzbPath); err != nil && !os.IsNotExist(err) {
				slog.ErrorContext(ctx, "StremioCleanup: failed to delete persistent NZB",
					"path", item.NzbPath, "error", err)
			}
		} else {
			// Single file: delete .meta; the store follows once unreferenced
			if err := s.metadataService.DeleteFileMetadata(ctx, storagePath); err != nil {
				slog.ErrorContext(ctx, "StremioCleanup: failed to delete meta",
					"path", storagePath, "error", err)
			}
		}
	} else {
		// No storage path recorded — just delete the persistent NZB if it still exists
		if err := os.Remove(item.NzbPath); err != nil && !os.IsNotExist(err) {
			slog.ErrorContext(ctx, "StremioCleanup: failed to delete persistent NZB",
				"path", item.NzbPath, "error", err)
		}
	}

	// Best-effort: prune the per-id parent folder under .nzbs (e.g. /config/.nzbs/Movies/123/)
	// once it is empty. os.Remove only succeeds on empty directories, so this is safe.
	if parent := filepath.Dir(item.NzbPath); parent != "" && parent != "." && parent != "/" {
		_ = os.Remove(parent)
	}

	// The files are gone; so must their health records be, or health checks
	// keep scheduling files that no longer exist.
	if s.health != nil {
		for _, f := range files {
			_ = s.health.DeleteHealthRecord(ctx, f) // not every file has one
		}
	}

	// Remove queue DB entry regardless of file deletion outcome
	if err := s.queueRepo.RemoveFromQueue(ctx, item.ID); err != nil {
		slog.ErrorContext(ctx, "StremioCleanup: failed to remove queue item", "id", item.ID, "error", err)
	}
}

// itemFiles lists the virtual files an item imported: the single file, or
// every file under its storage directory.
func (s *StremioCleanupService) itemFiles(item *database.ImportQueueItem) []string {
	if item.StoragePath == nil || *item.StoragePath == "" {
		return nil
	}
	root := strings.TrimPrefix(*item.StoragePath, "/")
	if !s.metadataService.DirectoryExists(root) {
		return []string{root}
	}
	var files []string
	var walk func(dir string)
	walk = func(dir string) {
		dirs, names, err := s.metadataService.ListDirectoryAll(dir)
		if err != nil {
			return
		}
		for _, n := range names {
			files = append(files, path.Join(dir, n))
		}
		for _, d := range dirs {
			walk(path.Join(dir, d.Name()))
		}
	}
	walk(root)
	return files
}

// libraryLinkedFile returns the first file an ARR has relinked into the media
// library, or "".
func (s *StremioCleanupService) libraryLinkedFile(ctx context.Context, files []string) string {
	if s.health == nil {
		return ""
	}
	for _, f := range files {
		if h, err := s.health.GetFileHealth(ctx, f); err == nil && h != nil && h.IsImported() {
			return f
		}
	}
	return ""
}
