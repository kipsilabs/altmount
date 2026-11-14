package postprocessor

import (
	"context"
	"log/slog"
	"path"
	"path/filepath"
	"strings"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
)

// maxDirExpansionDepth bounds the recursive walk of "DIR:" written-path entries.
// Import folders are shallow (an NZB folder, occasionally a Blu-ray structure from
// ISO expansion), so this is a safety net, not an expected limit.
const maxDirExpansionDepth = 8

// WillScheduleHealth reports whether an import under the current configuration
// creates a health record (and so a check) for the written file at path. It is
// the single rule ScheduleHealthCheck applies and the peer-bundle importer asks.
func WillScheduleHealth(cfg *config.Config, path string) bool {
	if cfg == nil || !cfg.GetHealthEnabled() {
		return false
	}
	return cfg.Import.ImportStrategy == config.ImportStrategyNone || isArrImportableMedia(path)
}

// ScheduleHealthCheck schedules an immediate health check for an imported file.
// Multi-file imports (e.g. season packs) schedule one check per written virtual
// file — resultingPath is a directory there, so the per-file paths are the only
// way each episode gets verified right after import and a partially-dead pack
// surfaces immediately. Single-file imports keep the old behavior (one check on
// resultingPath).
func (c *Coordinator) ScheduleHealthCheck(ctx context.Context, item *database.ImportQueueItem, resultingPath string, writtenPaths []string) error {
	if c.healthRepo == nil {
		return nil // Health checks not configured
	}

	// Health checking disabled: don't queue immediate checks or resolve repairs on
	// import — both are health-system concerns. Records queued here would only sit
	// as pending forever since the worker refuses to run when disabled.
	if !c.configGetter().GetHealthEnabled() {
		return nil
	}

	// Expand directory entries into per-file paths, and fall back to the
	// resulting path for legacy callers (single-file imports resolve to the
	// same thing).
	paths := c.expandWrittenPaths(writtenPaths)
	if len(paths) == 0 {
		paths = []string{resultingPath}
	}

	cfg := c.configGetter()

	// Under SYMLINK/STRM strategies a record only becomes checkable once an ARR
	// Download webhook relinks it to a real library path (GetUnhealthyFiles
	// filters on library_path), and ARRs only ever import media files. Sidecars
	// (.nfo, .srt, ...) would sit permanently ineligible in pending until the
	// next library sync deletes them, so don't schedule them here. Any sidecar
	// an ARR does copy into the library is still registered — with a real
	// library path — by the library sync.
	if cfg.Import.ImportStrategy != config.ImportStrategyNone {
		media := make([]string, 0, len(paths))
		for _, p := range paths {
			if WillScheduleHealth(cfg, p) {
				media = append(media, p)
			}
		}
		if skipped := len(paths) - len(media); skipped > 0 {
			slog.DebugContext(ctx, "Skipping health checks for sidecar files",
				"path", resultingPath, "skipped", skipped)
		}
		paths = media
	}

	var indexer *string = nil
	var downloadID *string = nil
	var itemMetadata *string = nil
	if item != nil {
		indexer = item.Indexer
		downloadID = item.DownloadID
		itemMetadata = item.Metadata
	}

	var lastErr error
	repairDirs := make(map[string]struct{})
	records := make([]database.HealthCheckUpsert, 0, len(paths))
	for _, p := range paths {
		// Read metadata to get SourceNzbPath needed for health check
		fileMeta, err := c.metadataService.ReadFileMetadata(p)
		if err != nil {
			slog.WarnContext(ctx, "Failed to read metadata for health check scheduling",
				"path", p,
				"error", err)
			lastErr = err
			continue
		}
		if fileMeta == nil {
			continue
		}

		// Copy the path and source NZB into per-iteration locals so the pointers stored in
		// the batch outlive the loop and do not retain the proto message.
		filePath := p
		srcNzb := fileMeta.SourceNzbPath
		records = append(records, database.HealthCheckUpsert{
			FilePath:         filePath,
			LibraryPath:      &filePath,
			SourceNzbPath:    &srcNzb,
			Indexer:          indexer,
			Priority:         database.HealthPriorityNext,
			MaxRetries:       cfg.GetMaxRetries(),
			MaxRepairRetries: cfg.GetMaxRepairRetries(),
			DownloadID:       downloadID,
			Metadata:         itemMetadata,
		})
		repairDirs[filepath.Dir(p)] = struct{}{}
	}

	// One batched upsert for the whole import instead of a write transaction per file — a
	// season pack / archive can expand to hundreds of paths. Conflict semantics match the
	// single-row upsert (reset to pending, preserve repair_retry_count).
	scheduled := 0
	if len(records) > 0 {
		if err := c.healthRepo.BatchAddFileToHealthCheck(ctx, records); err != nil {
			slog.ErrorContext(ctx, "Failed to schedule immediate health checks for imported files",
				"path", resultingPath, "files", len(records), "error", err)
			lastErr = err
		} else {
			scheduled = len(records)
		}
	}

	if scheduled > 0 {
		slog.InfoContext(ctx, "Scheduled immediate health check for imported files",
			"path", resultingPath, "files", scheduled)
	} else {
		// A successful import that schedules nothing means the file will only be
		// covered by library sync much later — and any pending repair in its
		// directory stays unresolved. Surface it instead of failing silently.
		slog.WarnContext(ctx, "No health checks scheduled for import - no checkable media files with readable metadata found",
			"path", resultingPath, "written_paths", len(writtenPaths))
	}

	// Resolve pending repairs in the affected directories if configured
	for dir := range repairDirs {
		c.resolvePendingRepairsInDir(ctx, dir)
	}

	if scheduled == 0 {
		return lastErr
	}
	return nil
}

// arrAudioBookExtensions are the non-video media types ARRs import (Lidarr audio,
// Readarr books). Video extensions are covered by fileinfo.IsVideoFile.
var arrAudioBookExtensions = map[string]bool{
	".mp3": true, ".flac": true, ".m4a": true, ".m4b": true, ".aac": true,
	".ogg": true, ".opus": true, ".wav": true,
	".epub": true, ".pdf": true, ".cbz": true, ".cbr": true, ".mobi": true, ".azw3": true,
}

// isArrImportableMedia reports whether the virtual path is a media file an ARR could
// import into the library — i.e. one that can ever be relinked to a real library path
// by a Download webhook and so become eligible for health checks under SYMLINK/STRM.
func isArrImportableMedia(p string) bool {
	if fileinfo.IsVideoFile(p) {
		return true
	}
	return arrAudioBookExtensions[strings.ToLower(path.Ext(p))]
}

// expandWrittenPaths resolves "DIR:"-prefixed entries (whole-directory imports such
// as RAR/7z archives, which only report their NZB folder) into the per-file virtual
// paths beneath them by walking the metadata tree. Plain file entries pass through
// unchanged. Without this, archive imports would schedule no health checks at all:
// ReadFileMetadata("DIR:...") finds nothing and the loop above skips silently.
func (c *Coordinator) expandWrittenPaths(writtenPaths []string) []string {
	var out []string
	for _, p := range writtenPaths {
		dir, isDir := strings.CutPrefix(p, "DIR:")
		if !isDir {
			out = append(out, p)
			continue
		}
		out = append(out, c.listMetadataFilesRecursive(dir, 0)...)
	}
	return out
}

// listMetadataFilesRecursive returns the virtual file paths of all metadata files
// under virtualDir, recursing into subdirectories up to maxDirExpansionDepth.
func (c *Coordinator) listMetadataFilesRecursive(virtualDir string, depth int) []string {
	if depth > maxDirExpansionDepth || c.metadataService == nil {
		return nil
	}

	dirs, files, err := c.metadataService.ListDirectoryAll(virtualDir)
	if err != nil {
		c.log.Warn("Failed to list metadata directory for health check scheduling",
			"dir", virtualDir, "error", err)
		return nil
	}

	var out []string
	for _, f := range files {
		out = append(out, path.Join(virtualDir, f))
	}
	for _, d := range dirs {
		out = append(out, c.listMetadataFilesRecursive(path.Join(virtualDir, d.Name()), depth+1)...)
	}
	return out
}

// resolvePendingRepairsInDir resolves pending repairs in the given directory
func (c *Coordinator) resolvePendingRepairsInDir(ctx context.Context, parentDir string) {
	cfg := c.configGetter()
	resolveRepairs := true
	if cfg.Health.ResolveRepairOnImport != nil {
		resolveRepairs = *cfg.Health.ResolveRepairOnImport
	}

	if !resolveRepairs {
		return
	}

	if parentDir == "." || parentDir == "/" {
		return
	}

	count, err := c.healthRepo.ResolvePendingRepairsInDirectory(ctx, parentDir)
	if err == nil && count > 0 {
		slog.InfoContext(ctx, "Resolved pending repairs in directory due to new import",
			"directory", parentDir,
			"resolved_count", count)
	}
}
