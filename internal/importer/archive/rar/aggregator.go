package rar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	concpool "github.com/sourcegraph/conc/pool"

	"github.com/kipsilabs/altmount/internal/encryption/aes"
	"github.com/kipsilabs/altmount/internal/importer/archive"
	"github.com/kipsilabs/altmount/internal/importer/filesystem"
	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/importer/utils"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
	"github.com/kipsilabs/altmount/internal/importer/validation"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/progress"
)

var (
	// ErrNoAllowedFiles indicates that the archive contains no files matching allowed extensions
	ErrNoAllowedFiles = archive.ErrNoAllowedFiles
	// ErrNoFilesProcessed indicates that no files were successfully processed (all files failed validation)
	ErrNoFilesProcessed = archive.ErrNoFilesProcessed
)

// getContentSegments delegates to archive.GetContentSegments.
func getContentSegments(content Content) []*metapb.SegmentData {
	return archive.GetContentSegments(content)
}

// validateSegmentIntegrity delegates to archive.ValidateSegmentIntegrity.
func validateSegmentIntegrity(ctx context.Context, content Content) error {
	return archive.ValidateSegmentIntegrity(ctx, content)
}

// newErrNoAllowedFiles builds a descriptive error showing which extensions were found
// vs which are allowed, making it actionable when imports fail silently.
func newErrNoAllowedFiles(rarContents []Content, allowedExtensions []string) error {
	extSet := make(map[string]struct{})
	for _, c := range rarContents {
		if c.IsDirectory {
			continue
		}
		ext := strings.ToLower(filepath.Ext(c.Filename))
		if ext == "" {
			ext = "(no extension)"
		}
		extSet[ext] = struct{}{}
	}
	found := make([]string, 0, len(extSet))
	for ext := range extSet {
		found = append(found, ext)
	}
	return fmt.Errorf("%w (found: %v, allowed: %v)", ErrNoAllowedFiles, found, allowedExtensions)
}

// hasAllowedFiles checks if any files within RAR archive contents match allowed extensions
// If allowedExtensions is empty, all file types are allowed
func hasAllowedFiles(rarContents []Content, allowedExtensions []string, filterSamples bool) bool {
	for _, content := range rarContents {
		// Skip directories
		if content.IsDirectory {
			continue
		}
		// Check both the internal path and filename
		if utils.IsAllowedFile(content.InternalPath, content.Size, allowedExtensions, filterSamples) ||
			utils.IsAllowedFile(content.Filename, content.Size, allowedExtensions, filterSamples) {
			return true
		}
	}
	return false
}

// ProcessArchiveOptions holds all parameters for ProcessArchive.
type ProcessArchiveOptions struct {
	VirtualDir             string
	ArchiveFiles           []parser.ParsedFile
	Password               string
	ReleaseDate            int64
	NzbPath                string
	Processor              Processor
	MetadataService        *metadata.MetadataService
	PoolManager            pool.Manager
	ArchiveProgressTracker *progress.Tracker
	AllowedFileExtensions  []string
	ExtractedFiles         []parser.ExtractedFileInfo
	MaxPrefetch            int
	ReadTimeout            time.Duration
	IsoAnalyzeTimeout      time.Duration
	ExpandBlurayIso        bool
	FilterSamples          bool
	RenameToNzbName        bool
	// SegmentIndex + StoreRef enable direct v3 store-backed metadata writes. When
	// StoreRef is empty the aggregator falls back to v1 inline-segment metadata.
	SegmentIndex map[string]int64
	StoreRef     string
	// OnMetadataWritten is invoked whenever a file's metadata is successfully written to disk.
	OnMetadataWritten func(virtualPath string)
}

// ProcessArchive analyzes and processes RAR archive files, creating metadata for all extracted files.
// This function handles the complete workflow: analysis → file processing → metadata creation.
func ProcessArchive(ctx context.Context, opts ProcessArchiveOptions) error {
	archiveFiles := opts.ArchiveFiles
	virtualDir := opts.VirtualDir
	password := opts.Password
	releaseDate := opts.ReleaseDate
	nzbPath := opts.NzbPath
	rarProcessor := opts.Processor
	metadataService := opts.MetadataService
	poolManager := opts.PoolManager
	archiveProgressTracker := opts.ArchiveProgressTracker
	allowedFileExtensions := opts.AllowedFileExtensions
	extractedFiles := opts.ExtractedFiles
	maxPrefetch := opts.MaxPrefetch
	readTimeout := opts.ReadTimeout
	analyzeTimeout := opts.IsoAnalyzeTimeout
	expandBlurayIso := opts.ExpandBlurayIso
	filterSamples := opts.FilterSamples
	renameToNzbName := opts.RenameToNzbName

	if len(archiveFiles) == 0 {
		return nil
	}

	slog.InfoContext(ctx, "Analyzing RAR archive content", "parts", len(archiveFiles))

	// Analyze RAR content with timeout
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	// Group archive files by their base name to handle NZBs with multiple separate RAR archives
	archiveGroups := GroupArchivesByBaseName(archiveFiles)

	type groupResult struct {
		contents  []Content
		err       error
		firstName string
		skipped   bool // volume gap detected: analysis skipped entirely
	}
	groupResults := make([]groupResult, len(archiveGroups))
	var wg sync.WaitGroup
	for i, group := range archiveGroups {
		// Skip groups with a detectable leading/interior volume gap before paying
		// for network analysis: a missing volume can't be repaired at import time,
		// so the set is doomed. groupHasVolumeGap is conservative (no false
		// positives on healthy sets), and a missing trailing volume still falls
		// through to analysis + segment-integrity validation.
		if groupHasVolumeGap(group) {
			slog.WarnContext(ctx, "Skipping RAR archive group with missing volume(s)",
				"first_file", group[0].Filename, "parts", len(group))
			groupResults[i] = groupResult{firstName: group[0].Filename, skipped: true}
			continue
		}
		wg.Add(1)
		go func(idx int, g []parser.ParsedFile) {
			defer wg.Done()
			groupContents, err := rarProcessor.AnalyzeRarContentFromNzb(ctx, g, password, archiveProgressTracker)
			groupResults[idx] = groupResult{contents: groupContents, err: err, firstName: g[0].Filename}
		}(i, group)
	}
	wg.Wait()

	// Isolate per-group analysis failures: a single doomed set (missing/unreadable
	// volume) must not discard the completed analyses of healthy sibling sets. Only
	// fail the whole archive when no group succeeded. Cancellation is never isolated.
	var rarContents []Content
	var groupErrs []error
	succeeded := 0
	for _, r := range groupResults {
		if r.skipped {
			continue
		}
		if r.err != nil {
			if ctx.Err() != nil || errors.Is(r.err, context.Canceled) {
				return r.err
			}
			slog.ErrorContext(ctx, "Skipping RAR archive group after failed analysis",
				"error", r.err, "first_file", r.firstName)
			groupErrs = append(groupErrs, r.err)
			continue
		}
		succeeded++
		rarContents = append(rarContents, r.contents...)
	}
	if succeeded == 0 {
		if len(groupErrs) > 0 {
			return errors.Join(groupErrs...)
		}
		// Every group was skipped for a volume gap — nothing left to import.
		return ErrNoFilesProcessed
	}

	// Expand ISO files found inside the RAR archive into their inner media
	// files. ISO analysis (filesystem walk + Blu-ray playlist resolution over
	// NNTP) can take tens of seconds, so it gets its own progress label.
	// Slice(0,1) copies the archive tracker at the same range without mutating
	// it (RAR header analysis above is already done); WithStage relabels the
	// copy. For archives with no ISO, ExpandISOContents emits no updates, so
	// the common case is unaffected.
	var isoProgressTracker *progress.Tracker
	if archiveProgressTracker != nil {
		isoProgressTracker = archiveProgressTracker.Slice(0, 1).WithStage("Analyzing ISO")
	}
	rarContents, err := archive.ExpandISOContents(ctx, expandBlurayIso, rarContents, poolManager, maxPrefetch, readTimeout, analyzeTimeout, allowedFileExtensions, isoProgressTracker)
	if err != nil {
		slog.WarnContext(ctx, "ISO expansion failed, proceeding without ISO contents", "error", err)
	}

	// Validate file extensions before processing
	if !hasAllowedFiles(rarContents, allowedFileExtensions, filterSamples) {
		err := newErrNoAllowedFiles(rarContents, allowedFileExtensions)
		slog.WarnContext(ctx, "RAR archive contains no files with allowed extensions", "error", err)
		return err
	}

	slog.InfoContext(ctx, "Starting RAR archive processing",
		"total_files", len(rarContents))

	nzbName := filepath.Base(nzbPath)
	releaseName := nzbtrim.TrimNzbExtension(nzbName)

	// Present an obfuscated payload under the release name, SABnzbd-style:
	// only the entry that is clearly the release, with its sidecars in tow.
	var payloadRenames map[string]string
	if renameToNzbName {
		payloadRenames = archive.PayloadRenames(rarContents, releaseName)
	}

	// Count ISO-expanded files so single-file ISOs omit the index suffix.
	isoExpandedCount := 0
	for _, c := range rarContents {
		if c.ISOExpansionIndex > 0 {
			isoExpandedCount++
		}
	}

	// Pre-pass: resolve paths, apply renames, and pre-compute per-file segment offsets so
	// each goroutine can track its own progress offset without any sequential shared state.
	type fileToProcess struct {
		content         Content
		baseFilename    string
		virtualFilePath string
		isPreExtracted  bool
	}

	var filesToProcess []fileToProcess
	preProcessedCount := 0 // healthy files already counted as processed

	for _, rarContent := range rarContents {
		if rarContent.IsDirectory {
			slog.DebugContext(ctx, "Skipping directory in RAR archive", "path", rarContent.InternalPath)
			continue
		}

		normalizedInternalPath := strings.ReplaceAll(rarContent.InternalPath, "\\", "/")
		baseFilename := filepath.Base(normalizedInternalPath)
		internalSubDir := filepath.ToSlash(filepath.Dir(normalizedInternalPath))

		// Deduplicate: if internalSubDir matches the base name of virtualDir, collapse it.
		// e.g. virtualDir="movies/MyMovie", internalSubDir="MyMovie" → "."
		//      virtualDir="movies/MyMovie", internalSubDir="MyMovie/Extras" → "Extras"
		virtualDirBase := filepath.Base(virtualDir)
		if internalSubDir == virtualDirBase {
			internalSubDir = "."
		} else if after, ok := strings.CutPrefix(internalSubDir, virtualDirBase+"/"); ok {
			internalSubDir = after
		}

		if !utils.IsAllowedFile(rarContent.InternalPath, rarContent.Size, allowedFileExtensions, filterSamples) &&
			!utils.IsAllowedFile(rarContent.Filename, rarContent.Size, allowedFileExtensions, filterSamples) {
			continue
		}

		if rarContent.ISOExpansionIndex > 0 {
			ext := filepath.Ext(rarContent.Filename)
			if isoExpandedCount == 1 {
				baseFilename = releaseName + ext
			} else {
				baseFilename = fmt.Sprintf("%s_%d%s", releaseName, rarContent.ISOExpansionIndex, ext)
			}
			slog.InfoContext(ctx, "Renaming ISO-expanded file using NZB release name",
				"original", rarContent.Filename,
				"renamed", baseFilename)
			internalSubDir = "."
		} else if renamed, ok := payloadRenames[rarContent.InternalPath]; ok {
			slog.InfoContext(ctx, "Normalizing obfuscated filename in RAR archive",
				"original", rarContent.Filename,
				"normalized", renamed)
			baseFilename = renamed
			internalSubDir = "."
		}

		var virtualFilePath string
		if internalSubDir == "." || internalSubDir == "" {
			virtualFilePath = filepath.Join(virtualDir, baseFilename)
		} else {
			subDir := filepath.Join(virtualDir, internalSubDir)
			if err := filesystem.EnsureDirectoryExists(subDir, metadataService); err != nil {
				return fmt.Errorf("failed to create archive subdirectory %s: %w", subDir, err)
			}
			virtualFilePath = filepath.Join(subDir, baseFilename)
		}
		virtualFilePath = strings.ReplaceAll(virtualFilePath, string(filepath.Separator), "/")

		if existingMeta, err := metadataService.ReadFileMetadata(virtualFilePath); err == nil && existingMeta != nil {
			if existingMeta.Status == metapb.FileStatus_FILE_STATUS_HEALTHY {
				slog.InfoContext(ctx, "Skipping re-import of healthy RAR-extracted file",
					"file", baseFilename,
					"virtual_path", virtualFilePath)
				preProcessedCount++
				continue
			}
		}

		isPreExtracted := false
		for _, extracted := range extractedFiles {
			if extracted.Name == baseFilename && extracted.Size == rarContent.Size {
				isPreExtracted = true
				break
			}
		}

		filesToProcess = append(filesToProcess, fileToProcess{
			content:         rarContent,
			baseFilename:    baseFilename,
			virtualFilePath: virtualFilePath,
			isPreExtracted:  isPreExtracted,
		})
	}

	// Parallel pass: validate segments and write metadata for each file concurrently.
	var filesProcessed int32
	p := concpool.New().WithErrors().WithFirstError().WithContext(ctx)

	for _, item := range filesToProcess {
		p.Go(func(ctx context.Context) error {
			if item.isPreExtracted {
				slog.InfoContext(ctx, "Skipping validation for pre-extracted file (found in database)",
					"file", item.baseFilename,
					"size", item.content.Size)
			} else {
				if err := validateSegmentIntegrity(ctx, item.content); err != nil {
					slog.ErrorContext(ctx, "Skipping RAR file due to segment integrity failure (missing segments in NZB)",
						"file", item.baseFilename,
						"error", err)
					return nil
				}

				validationSegments := getContentSegments(item.content)

				var validationSize int64
				if len(item.content.NestedSources) > 0 {
					for _, ns := range item.content.NestedSources {
						sourceSize := int64(0)
						for _, seg := range ns.Segments {
							sourceSize += seg.EndOffset - seg.StartOffset + 1
						}
						validationSize += sourceSize
					}
				} else {
					validationSize = item.content.PackedSize
					if len(item.content.AesKey) > 0 {
						validationSize = aes.EncryptedSize(item.content.PackedSize)
					}
				}

				// Local structural checks only; network reachability was confirmed at import start
				if err := validation.ValidateSegmentsForFile(
					item.baseFilename,
					validationSize,
					validationSegments,
					metapb.Encryption_NONE,
				); err != nil {
					slog.WarnContext(ctx, "Skipping RAR file due to validation error", "error", err, "file", item.baseFilename)
					return nil
				}
			}

			fileMeta := rarProcessor.CreateFileMetadataFromRarContent(item.content, nzbPath, releaseDate, item.content.NzbdavID)

			metadataPath := metadataService.GetMetadataFilePath(item.virtualFilePath)
			if _, err := os.Stat(metadataPath); err == nil {
				_ = metadataService.DeleteFileMetadata(ctx, item.virtualFilePath)
			}

			if err := metadataService.WriteFileMetadataAuto(ctx, item.virtualFilePath, fileMeta, opts.SegmentIndex, opts.StoreRef); err != nil {
				return fmt.Errorf("failed to write metadata for RAR file %s: %w", item.content.Filename, err)
			}

			if opts.OnMetadataWritten != nil {
				opts.OnMetadataWritten(item.virtualFilePath)
			}

			slog.InfoContext(ctx, "Created metadata for RAR extracted file",
				"file", item.baseFilename,
				"virtual_path", item.virtualFilePath,
				"size", item.content.Size)

			atomic.AddInt32(&filesProcessed, 1)
			return nil
		})
	}

	if err := p.Wait(); err != nil {
		return err
	}

	if int(atomic.LoadInt32(&filesProcessed))+preProcessedCount == 0 && len(rarContents) > 0 {
		return ErrNoFilesProcessed
	}

	slog.InfoContext(ctx, "Successfully processed RAR archive files",
		"files_processed", int(atomic.LoadInt32(&filesProcessed))+preProcessedCount)

	return nil
}

// GroupArchivesByBaseName groups ParsedFiles by their RAR base name (case-insensitive).
// Returns groups in deterministic order (sorted by base name) for testability.
func GroupArchivesByBaseName(files []parser.ParsedFile) [][]parser.ParsedFile {
	groupMap := make(map[string][]parser.ParsedFile)
	var keys []string
	for _, f := range files {
		key := extractRarBaseName(f.Filename)
		if _, exists := groupMap[key]; !exists {
			keys = append(keys, key)
		}
		groupMap[key] = append(groupMap[key], f)
	}
	sort.Strings(keys)
	groups := make([][]parser.ParsedFile, 0, len(keys))
	for _, k := range keys {
		groups = append(groups, groupMap[k])
	}

	// A single physical RAR set whose first volume was reposted under a different
	// base name (e.g. movie.repost.part01.rar alongside movie.r00..) splits into
	// multiple groups above; fold it back into one set so the whole archive maps.
	groups = reconcileRepostedFirstVolume(groups)

	// A fully per-volume-obfuscated set (every volume a distinct base, no PAR2 to
	// recover a shared name) shatters into one single-file group per volume; fold it
	// back into one ordered set using the volumes' own contiguous ordinals.
	return reconcileObfuscatedVolumeSet(groups)
}
