package metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/robfig/cron/v3"
)

type BackupWorker struct {
	configGetter  config.ConfigGetter
	cronRunner    *cron.Cron
	workerCtx     context.Context
	workerCancel  context.CancelFunc
	workerMu      sync.Mutex
	workerRunning bool
}

func NewBackupWorker(configGetter config.ConfigGetter) *BackupWorker {
	return &BackupWorker{
		configGetter: configGetter,
	}
}

func (w *BackupWorker) Start(ctx context.Context) error {
	w.workerMu.Lock()
	defer w.workerMu.Unlock()

	if w.workerRunning {
		return nil
	}

	cfg := w.configGetter()
	if cfg.Metadata.Backup.Enabled == nil || !*cfg.Metadata.Backup.Enabled {
		return nil
	}

	w.workerCtx, w.workerCancel = context.WithCancel(ctx)

	// Catch-up logic
	if w.shouldTriggerImmediateBackup(cfg.Metadata.Backup.Path) {
		go w.performBackup()
	}

	w.cronRunner = cron.New(cron.WithLocation(time.UTC))
	if _, err := w.cronRunner.AddFunc(cfg.Metadata.Backup.Schedule, w.performBackup); err != nil {
		w.workerCancel()
		return fmt.Errorf("invalid backup schedule %q: %w", cfg.Metadata.Backup.Schedule, err)
	}
	w.cronRunner.Start()
	w.workerRunning = true

	slog.InfoContext(ctx, "Metadata backup worker started",
		"schedule", cfg.Metadata.Backup.Schedule,
		"keep_backups", cfg.Metadata.Backup.KeepBackups,
		"path", cfg.Metadata.Backup.Path)
	return nil
}

func (w *BackupWorker) shouldTriggerImmediateBackup(backupRoot string) bool {
	files, err := os.ReadDir(backupRoot)
	if err != nil {
		return false
	}

	var latestModTime time.Time
	for _, f := range files {
		if f.IsDir() {
			info, err := f.Info()
			if err == nil {
				if info.ModTime().After(latestModTime) {
					latestModTime = info.ModTime()
				}
			}
		}
	}

	return time.Since(latestModTime) > 24*time.Hour
}

func (w *BackupWorker) Stop(ctx context.Context) {
	w.workerMu.Lock()
	defer w.workerMu.Unlock()

	if !w.workerRunning {
		return
	}

	w.workerCancel()
	cronCtx := w.cronRunner.Stop()
	<-cronCtx.Done()
	w.cronRunner = nil
	w.workerRunning = false
	slog.InfoContext(ctx, "Metadata backup worker stopped")
}

// backupSource is a directory to walk plus the filename filter deciding which of
// its files belong in the backup.
type backupSource struct {
	path    string
	matches func(name string) bool
}

func isMetaFile(name string) bool {
	return strings.HasSuffix(name, ".meta")
}

func isNzbFile(name string) bool {
	return strings.HasSuffix(name, ".nzbz") || strings.HasSuffix(name, ".nzb")
}

// nzbStorePath mirrors importer.Service.GetNzbFolder: the persistent NZB store
// lives next to the database file.
func nzbStorePath(cfg *config.Config) string {
	if cfg.Database.Path == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(cfg.Database.Path), ".nzbs")
}

func (w *BackupWorker) performBackup() {
	cfg := w.configGetter()
	backupRoot := cfg.Metadata.Backup.Path
	metadataDir := cfg.Metadata.RootPath

	timestamp := time.Now().Format("20060102-150405")
	backupDir := filepath.Join(backupRoot, timestamp)

	if err := os.MkdirAll(backupDir, 0755); err != nil {
		slog.ErrorContext(w.workerCtx, "Failed to create backup directory", "error", err, "path", backupDir)
		return
	}

	slog.InfoContext(w.workerCtx, "Starting metadata backup (copy)", "destination", backupDir)

	count := 0

	// Roots to back up, each with the set of files it contributes.
	rootsToBackup := []backupSource{{path: metadataDir, matches: isMetaFile}}
	if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
		rootsToBackup = append(rootsToBackup, backupSource{path: *cfg.Health.LibraryDir, matches: isMetaFile})
	}
	// Persistent NZB store (configDir/.nzbs) holds the .nzbz files the queue and
	// stremio endpoints replay from; without them a restored metadata set has no
	// source NZB to fall back on.
	if nzbStoreDir := nzbStorePath(cfg); nzbStoreDir != "" {
		if info, err := os.Stat(nzbStoreDir); err == nil && info.IsDir() {
			rootsToBackup = append(rootsToBackup, backupSource{path: nzbStoreDir, matches: isNzbFile})
		}
	}

	for _, root := range rootsToBackup {
		err := filepath.Walk(root.path, func(path string, info os.FileInfo, err error) error {
			if w.workerCtx != nil {
				select {
				case <-w.workerCtx.Done():
					return w.workerCtx.Err()
				default:
				}
			}

			if err != nil {
				// Tolerate per-entry errors (e.g. Windows system folders like
				// "System Volume Information" / "WindowsApps", or restricted
				// dirs on Linux) so a single permission failure doesn't abort
				// the entire backup.
				slog.WarnContext(w.workerCtx, "Skipping entry during metadata backup",
					"path", path, "error", err)
				if info != nil && info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			if info.IsDir() {
				return nil
			}

			if !root.matches(info.Name()) {
				return nil
			}

			relPath, err := filepath.Rel(root.path, path)
			if err != nil {
				return err
			}

			// Add subdir prefix to avoid collisions
			var destPath string
			if root.path == metadataDir {
				destPath = filepath.Join(backupDir, relPath)
			} else {
				destPath = filepath.Join(backupDir, filepath.Base(root.path), relPath)
			}

			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return err
			}

			if err := w.copyFile(path, destPath); err != nil {
				return err
			}
			count++
			return nil
		})

		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.InfoContext(w.workerCtx, "Metadata backup canceled")
			} else {
				slog.ErrorContext(w.workerCtx, "Failed to complete metadata backup", "error", err)
			}
			// Cleanup failed partial backup
			os.RemoveAll(backupDir)
			return
		}
	}

	slog.InfoContext(w.workerCtx, "Metadata backup completed successfully", "files_copied", count)

	w.cleanupOldBackups(backupRoot, cfg.GetMetadataBackupKeep())
}

func (w *BackupWorker) copyFile(src, dst string) error {
	const maxRetries = 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		sourceFile, err := os.Open(src)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		destFile, err := os.Create(dst)
		if err != nil {
			sourceFile.Close()
			lastErr = err
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		_, err = io.Copy(destFile, sourceFile)
		sourceFile.Close()
		destFile.Close()

		if err == nil {
			return nil
		}

		lastErr = err
		time.Sleep(time.Duration(i+1) * time.Second)
	}

	return fmt.Errorf("failed to copy file after %d attempts: %w", maxRetries, lastErr)
}

func (w *BackupWorker) cleanupOldBackups(backupRoot string, keep int) {
	files, err := os.ReadDir(backupRoot)
	if err != nil {
		slog.ErrorContext(w.workerCtx, "Failed to read backup directory for cleanup", "error", err)
		return
	}

	type backupEntry struct {
		name    string
		modTime time.Time
	}

	var backups []backupEntry
	for _, f := range files {
		if f.IsDir() {
			info, err := f.Info()
			if err == nil {
				backups = append(backups, backupEntry{
					name:    f.Name(),
					modTime: info.ModTime(),
				})
			}
		}
	}

	if len(backups) <= keep {
		return
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].modTime.After(backups[j].modTime)
	})

	for i := keep; i < len(backups); i++ {
		path := filepath.Join(backupRoot, backups[i].name)
		slog.InfoContext(w.workerCtx, "Deleting old backup directory", "path", path)
		if err := os.RemoveAll(path); err != nil {
			slog.ErrorContext(w.workerCtx, "Failed to delete old backup directory", "error", err, "path", path)
		}
	}
}
