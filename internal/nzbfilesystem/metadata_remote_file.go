package nzbfilesystem

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/encryption"
	"github.com/kipsilabs/altmount/internal/encryption/aes"
	"github.com/kipsilabs/altmount/internal/encryption/rclone"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem/segcache"

	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/kipsilabs/altmount/pkg/rclonecli"
	"github.com/spf13/afero"
)

// epochFallbackModTime is reported when no real timestamp is available (a file
// whose ModifiedAt was never set, or a directory that is not backed by an
// on-disk metadata dir). A fixed date is deliberate: returning time.Now() would
// make every stat look like a fresh modification and force media scanners to
// re-probe on every call.
var epochFallbackModTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// getFileModTime maps a proto ModifiedAt to the mtime reported over WebDAV and
// FUSE. Note that ReadFileMetadataLite treats modified_at as best-effort — it
// stays zero when the field sits past liteScanBytes — so listings can legitimately
// fall back to the epoch here.
func getFileModTime(modifiedAt int64) time.Time {
	if modifiedAt > 0 {
		return time.Unix(modifiedAt, 0)
	}
	return epochFallbackModTime
}

// MetadataRemoteFile implements the RemoteFile interface for metadata-backed virtual files
type MetadataRemoteFile struct {
	metadataService  *metadata.MetadataService
	healthRepository *database.HealthRepository
	arrsService      ARRsRepairService
	rcloneClient     rclonecli.RcloneRcClient // RClone RC client for VFS notifications
	poolManager      pool.Manager             // Pool manager for dynamic pool access
	configGetter     config.ConfigGetter      // Dynamic config access
	rcloneCipher     *rclone.RcloneCrypt      // For rclone encryption/decryption
	aesCipher        *aes.AesCipher           // For AES encryption/decryption
	streamTracker    StreamTracker            // Stream tracker for monitoring active streams
	cacheSource      *segcache.Source         // Segment cache source (nil = no cache configured)
	repairCoalescer  *RepairCoalescer         // Throttles streaming-failure repair triggers and rclone VFS refreshes
	padRecorder      *padRecorder             // Process-lived worker persisting degraded-pad events
	patchSource      PatchSource              // Repaired-article payloads (nil = PAR2 repair disabled)
	repairEnqueuer   RepairEnqueuer           // Queues PAR2 repairs (nil = PAR2 repair disabled)
	renameMu         sync.Mutex               // Mutex to protect rename operations from race conditions
}

// SetPatchSource wires the PAR2 repair patch store into the read path. Call
// during boot, before the filesystem serves reads.
func (mrf *MetadataRemoteFile) SetPatchSource(ps PatchSource) {
	mrf.patchSource = ps
}

// SetRepairEnqueuer wires the PAR2 repair queue into both playback triggers:
// degraded pads (zero-filled holes) and failed streams on missing articles.
// Call during boot, before the filesystem serves reads.
func (mrf *MetadataRemoteFile) SetRepairEnqueuer(re RepairEnqueuer) {
	mrf.repairEnqueuer = re
	mrf.padRecorder.repair = re
}

// Configuration is now accessed dynamically through config.ConfigGetter
// No longer need a separate config struct

// NewMetadataRemoteFile creates a new metadata-based remote file handler
func NewMetadataRemoteFile(
	metadataService *metadata.MetadataService,
	healthRepository *database.HealthRepository,
	arrsService ARRsRepairService,
	rcloneClient rclonecli.RcloneRcClient,
	poolManager pool.Manager,
	configGetter config.ConfigGetter,
	streamTracker StreamTracker,
	cacheSource *segcache.Source,
) *MetadataRemoteFile {
	// Initialize rclone cipher with global credentials for encrypted files
	cfg := configGetter()
	rcloneConfig := &encryption.Config{
		RclonePassword: cfg.RClone.Password, // Global password fallback
		RcloneSalt:     cfg.RClone.Salt,     // Global salt fallback
	}

	rcloneCipher, _ := rclone.NewRcloneCipher(rcloneConfig)

	// Initialize AES cipher for encrypted archives
	aesCipher := aes.NewAesCipher()

	repairCoalescer := NewRepairCoalescer(rcloneClient, configGetter)

	return &MetadataRemoteFile{
		metadataService:  metadataService,
		healthRepository: healthRepository,
		arrsService:      arrsService,
		rcloneClient:     rcloneClient,
		poolManager:      poolManager,
		configGetter:     configGetter,
		rcloneCipher:     rcloneCipher,
		aesCipher:        aesCipher,
		streamTracker:    streamTracker,
		cacheSource:      cacheSource,
		repairCoalescer:  repairCoalescer,
		padRecorder:      newPadRecorder(metadataService, healthRepository, repairCoalescer),
	}
}

// Helper methods to get dynamic config values
func (mrf *MetadataRemoteFile) getMaxPrefetch() int {
	return mrf.configGetter().Streaming.MaxPrefetch
}

// resolveSegmentStore returns the active SegmentStore for a new reader, or nil if
// the cache is disabled or not configured. Called once per file-open.
func (mrf *MetadataRemoteFile) resolveSegmentStore() usenet.SegmentStore {
	if mrf.cacheSource == nil {
		return nil
	}
	return mrf.cacheSource.Store()
}

func (mrf *MetadataRemoteFile) getGlobalPassword() string {
	return mrf.configGetter().RClone.Password
}

func (mrf *MetadataRemoteFile) getGlobalSalt() string {
	return mrf.configGetter().RClone.Salt
}

// OpenFile opens a virtual file backed by metadata
func (mrf *MetadataRemoteFile) OpenFile(ctx context.Context, name string) (bool, afero.File, error) {
	// Forbid COPY operations - nzbfilesystem is read-only
	if isCopy, ok := ctx.Value(utils.IsCopy).(bool); ok && isCopy {
		return false, nil, os.ErrPermission
	}

	// Normalize the path to handle trailing slashes consistently
	normalizedName := normalizePath(name)

	// Extract showCorrupted flag from context
	showCorrupted := false
	if sc, ok := ctx.Value(utils.ShowCorrupted).(bool); ok {
		showCorrupted = sc
	}

	// Force showCorrupted if we are inside the corrupted_metadata folder
	// normalizedName is clean and has no trailing slashes
	if strings.HasPrefix(normalizedName, corruptedDirName+"/") || normalizedName == corruptedDirName {
		showCorrupted = true
	}

	// Check if this is a directory first
	if mrf.metadataService.DirectoryExists(normalizedName) {
		// Create a directory handle
		virtualDir := &MetadataVirtualDirectory{
			name:             name,
			normalizedPath:   normalizedName,
			metadataService:  mrf.metadataService,
			healthRepository: mrf.healthRepository,
			configGetter:     mrf.configGetter,
			showCorrupted:    showCorrupted,
		}
		return true, virtualDir, nil
	}

	// Check if this path exists as a file in our metadata
	exists := mrf.metadataService.FileExists(normalizedName)
	if !exists {
		// Check if it's a sharded ID path (.ids/...)
		if strings.HasPrefix(normalizedName, ".ids/") {
			// Resolve the ID path to the actual virtual path
			resolvedPath, err := mrf.resolveIDPath(normalizedName)
			if err == nil && resolvedPath != "" {
				// Continue with the resolved path
				normalizedName = resolvedPath
				exists = true
			}
		}

		if !exists {
			// Check if this could be a valid empty directory
			if mrf.isValidEmptyDirectory(normalizedName) {
				// Create a directory handle for empty directory
				virtualDir := &MetadataVirtualDirectory{
					name:             name,
					normalizedPath:   normalizedName,
					metadataService:  mrf.metadataService,
					healthRepository: mrf.healthRepository,
					configGetter:     mrf.configGetter,
					showCorrupted:    showCorrupted,
				}
				return true, virtualDir, nil
			}
			// Neither file nor directory found
			return false, nil, nil
		}
	}

	// Get file metadata using simplified schema
	fileMeta, err := mrf.metadataService.ReadFileMetadata(normalizedName)
	if err != nil {
		return false, nil, fmt.Errorf("failed to read file metadata: %w", err)
	}

	if fileMeta == nil {
		return false, nil, nil
	}

	if fileMeta.Status == metapb.FileStatus_FILE_STATUS_CORRUPTED {
		return false, nil, &CorruptedFileError{
			TotalExpected: fileMeta.FileSize,
			UnderlyingErr: ErrMissmatchedSegments,
		}
	}

	// Extract max prefetch from context if available (overrides global config)
	maxPrefetch := mrf.getMaxPrefetch()

	if w, ok := ctx.Value(utils.MaxPrefetchKey).(int); ok && w > 0 {
		maxPrefetch = w
	}

	// Start tracking stream if tracker available
	streamID := ""
	if suppress, _ := ctx.Value(utils.SuppressStreamTrackingKey).(bool); suppress {
		// Stream tracking handled at caller level (e.g. FUSE Handle)
		streamID = ""
	} else if mrf.streamTracker != nil {
		// Check if we already have a stream ID in context
		if id, ok := ctx.Value(utils.StreamIDKey).(string); ok && id != "" {
			streamID = id
		} else if stream, ok := ctx.Value(utils.ActiveStreamKey).(*ActiveStream); ok {
			streamID = stream.ID
		} else {
			// Check for source and username in context
			source := "FUSE"
			if s, ok := ctx.Value(utils.StreamSourceKey).(string); ok && s != "" {
				source = s
			}

			userName := "FUSE"
			if u, ok := ctx.Value(utils.StreamUserNameKey).(string); ok && u != "" {
				userName = u
			}

			clientIP := ""
			if ip, ok := ctx.Value(utils.ClientIPKey).(string); ok {
				clientIP = ip
			}

			userAgent := ""
			if ua, ok := ctx.Value(utils.UserAgentKey).(string); ok {
				userAgent = ua
			}

			// Fallback to FUSE if no tracking info in context
			streamID = mrf.streamTracker.Add(normalizedName, source, userName, clientIP, userAgent, fileMeta.FileSize)
		}
	}

	// Extract only the fields the handle needs from the proto. The full
	// *FileMetadata then falls out of scope and becomes eligible for GC,
	// freeing the proto wrapper overhead (~protoimpl.MessageState +
	// unknownFields + sizeCache + unused fields like NzbdavId). Slices are
	// carried by reference; they stay alive only while the handle is open.
	handleMeta := &fileHandleMeta{
		FileSize:                fileMeta.FileSize,
		ModifiedAt:              fileMeta.ModifiedAt,
		SourceNzbPath:           fileMeta.SourceNzbPath,
		Encryption:              fileMeta.Encryption,
		Password:                fileMeta.Password,
		Salt:                    fileMeta.Salt,
		AesKey:                  fileMeta.AesKey,
		AesIv:                   fileMeta.AesIv,
		SegmentData:             fileMeta.SegmentData,
		NestedSources:           fileMeta.NestedSources,
		ClipBoundaries:          fileMeta.ClipBoundaries,
		KnownHoles:              fileMeta.KnownHoles,
		ReleaseDate:             fileMeta.ReleaseDate,
		HoleProviderFingerprint: fileMeta.HoleProviderFingerprint,
	}

	fileCtx, cancel := context.WithCancel(ctx)
	// Create a metadata-based virtual file handle
	virtualFile := &MetadataVirtualFile{
		name:             name,
		meta:             handleMeta,
		metadataService:  mrf.metadataService,
		healthRepository: mrf.healthRepository,
		arrsService:      mrf.arrsService,
		rcloneClient:     mrf.rcloneClient,
		repairCoalescer:  mrf.repairCoalescer,
		padRecorder:      mrf.padRecorder,
		patchSource:      mrf.patchSource,
		repairEnqueuer:   mrf.repairEnqueuer,
		configGetter:     mrf.configGetter,
		poolManager:      mrf.poolManager,
		ctx:              fileCtx,
		cancelCtx:        cancel,
		maxPrefetch:      maxPrefetch,
		rcloneCipher:     mrf.rcloneCipher,
		aesCipher:        mrf.aesCipher,
		globalPassword:   mrf.getGlobalPassword(),
		globalSalt:       mrf.getGlobalSalt(),
		streamTracker:    mrf.streamTracker,
		streamID:         streamID,
		segmentStore:     mrf.resolveSegmentStore(),
	}

	return true, virtualFile, nil
}

// RemoveFile removes a virtual file or directory from the metadata
func (mrf *MetadataRemoteFile) RemoveFile(ctx context.Context, fileName string) (bool, error) {
	// Normalize the path to handle trailing slashes consistently
	normalizedName := normalizePath(fileName)

	// Prevent removal of root directory
	if normalizedName == RootPath {
		return false, ErrCannotRemoveRoot
	}

	// A recursive DELETE on the safety folder itself would wipe every corrupted
	// copy at once; purging is an explicit action. Paths inside it stay removable.
	if strings.EqualFold(strings.Trim(normalizedName, "/"), corruptedDirName) {
		return false, os.ErrPermission
	}

	// Prevent removal of category folders
	if mrf.isCategoryFolder(normalizedName) {
		slog.DebugContext(ctx, "Silently ignored removal request for category folder", "path", normalizedName)
		// Return true (success) but do nothing. This prevents Sonarr/Radarr/rclone
		// from logging "directory not empty" or "permission denied" errors.
		return true, nil
	}

	// Check if this is a directory
	if mrf.metadataService.DirectoryExists(normalizedName) {
		// Use MetadataService's directory delete operation
		return true, mrf.metadataService.DeleteDirectory(normalizedName)
	}

	// Check if this path exists as a file in our metadata
	exists := mrf.metadataService.FileExists(normalizedName)
	if !exists {
		// Neither file nor directory found in metadata
		return false, nil
	}

	// Try to find the physical path from health record for cleanup
	var physicalPath string
	if mrf.healthRepository != nil {
		if health, err := mrf.healthRepository.GetFileHealth(ctx, normalizedName); err == nil && health != nil {
			if health.LibraryPath != nil && *health.LibraryPath != "" {
				physicalPath = *health.LibraryPath
			}
		}
	}

	cfg := mrf.configGetter()

	// Deletes the .meta and, once the last sibling is gone, the shared .nzbz store.
	err := mrf.metadataService.DeleteFileMetadata(ctx, normalizedName)
	if err != nil {
		return true, err
	}

	// Clean up empty physical directories if we found a physical path
	if physicalPath != "" {
		var rootPath string
		if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
			rootPath = *cfg.Health.LibraryDir
		} else {
			rootPath = cfg.MountPath
		}

		if rootPath != "" {
			utils.RemoveEmptyDirs(rootPath, filepath.Dir(physicalPath))
		}
	}

	return true, nil
}

// RenameFile renames a virtual file or directory in the metadata
func (mrf *MetadataRemoteFile) RenameFile(ctx context.Context, oldName, newName string) (bool, error) {
	mrf.renameMu.Lock()
	defer mrf.renameMu.Unlock()

	// Normalize paths
	normalizedOld := normalizePath(oldName)
	normalizedNew := normalizePath(newName)

	slog.InfoContext(ctx, "MOVE operation requested", "source", normalizedOld, "destination", normalizedNew)

	// Prevent renaming of category folders
	if mrf.isCategoryFolder(normalizedOld) {
		slog.WarnContext(ctx, "Prevented renaming of category folder", "path", normalizedOld)
		return false, os.ErrPermission
	}

	// Check if old path is a directory
	if mrf.metadataService.DirectoryExists(normalizedOld) {
		// Get the filesystem paths for the directories
		oldDirPath := mrf.metadataService.GetMetadataDirectoryPath(normalizedOld)
		newDirPath := mrf.metadataService.GetMetadataDirectoryPath(normalizedNew)

		slog.InfoContext(ctx, "Moving metadata directory", "from", oldDirPath, "to", newDirPath)

		// Rename the entire directory
		if err := os.Rename(oldDirPath, newDirPath); err != nil {
			return false, fmt.Errorf("failed to rename directory: %w", err)
		}

		// Update health records for all files under the renamed directory
		if mrf.healthRepository != nil {
			if err := mrf.healthRepository.RenameHealthRecord(ctx, normalizedOld, normalizedNew); err != nil {
				slog.WarnContext(ctx, "Failed to update health records for renamed directory", "old", normalizedOld, "new", normalizedNew, "error", err)
			}
		}

		return true, nil
	}

	// Check if old path exists as a file
	exists := mrf.metadataService.FileExists(normalizedOld)
	if !exists {
		slog.WarnContext(ctx, "MOVE source not found", "path", normalizedOld)
		return false, nil
	}

	// Use atomic rename instead of read-write-delete
	if err := mrf.metadataService.RenameFileMetadata(normalizedOld, normalizedNew); err != nil {
		return false, fmt.Errorf("failed to rename metadata: %w", err)
	}

	// Update health records and resolve pending repairs
	if mrf.healthRepository != nil {
		if err := mrf.healthRepository.RenameHealthRecord(ctx, normalizedOld, normalizedNew); err != nil {
			// If DB update fails, we already renamed the file on disk. This is where ghosts are born.
			// Log it clearly as a DB sync error.
			slog.ErrorContext(ctx, "CRITICAL: Metadata moved but DB update failed. Ghost record created.",
				"old", normalizedOld, "new", normalizedNew, "error", err)
			return false, fmt.Errorf("failed to update health record path: %w", err)
		}

		// Check if we should resolve other repairs in the same directory
		cfg := mrf.configGetter()
		resolveRepairs := true
		if cfg.Health.ResolveRepairOnImport != nil {
			resolveRepairs = *cfg.Health.ResolveRepairOnImport
		}

		if resolveRepairs {
			parentDir := filepath.Dir(normalizedNew)
			if parentDir != "." && parentDir != "/" {
				if count, err := mrf.healthRepository.ResolvePendingRepairsInDirectory(ctx, parentDir); err == nil && count > 0 {
					slog.InfoContext(ctx, "Resolved pending repairs in directory due to MOVE operation",
						"directory", parentDir,
						"resolved_count", count)
				}
			}
		}
	}

	slog.InfoContext(ctx, "MOVE operation successful", "source", normalizedOld, "destination", normalizedNew)

	return true, nil
}

// isCategoryFolder checks if a path corresponds to a configured category folder
func (mrf *MetadataRemoteFile) isCategoryFolder(path string) bool {
	cfg := mrf.configGetter()
	normalizedPath := strings.Trim(normalizePath(path), "/")
	completeDir := strings.Trim(normalizePath(cfg.SABnzbd.CompleteDir), "/")

	// Helper to check if a name matches a category
	matchesCategory := func(name string) bool {
		name = strings.Trim(normalizePath(name), "/")
		if name == "" {
			return false
		}

		// Check exact match (case-insensitive)
		if strings.EqualFold(normalizedPath, name) {
			return true
		}

		// Check match with complete_dir prefix (e.g. complete/tv)
		if completeDir != "" && strings.EqualFold(normalizedPath, strings.Trim(completeDir+"/"+name, "/")) {
			return true
		}

		return false
	}

	// Check complete_dir itself
	if strings.EqualFold(normalizedPath, completeDir) {
		return true
	}

	// Check configured categories
	for _, cat := range cfg.SABnzbd.Categories {
		// Check both the category name and its specific directory if set
		if matchesCategory(cat.Name) {
			return true
		}
		if cat.Dir != "" && matchesCategory(cat.Dir) {
			return true
		}
	}

	return false
}

// Stat returns file information for a path using metadata
func (mrf *MetadataRemoteFile) Stat(ctx context.Context, name string) (bool, fs.FileInfo, error) {
	// Normalize the path
	normalizedName := normalizePath(name)

	// Check if this is a directory first
	if mrf.metadataService.DirectoryExists(normalizedName) {
		modT := mrf.metadataService.DirectoryModTime(normalizedName)
		if modT.IsZero() {
			modT = epochFallbackModTime
		}
		info := &MetadataFileInfo{
			name:    filepath.Base(normalizedName),
			size:    0,
			mode:    os.ModeDir | 0755,
			modTime: modT,
			isDir:   true,
		}
		return true, info, nil
	}

	// Check if this path exists as a file in our metadata
	exists := mrf.metadataService.FileExists(normalizedName)
	if !exists {
		// Check if it's a sharded ID path (.ids/...)
		if strings.HasPrefix(normalizedName, ".ids/") {
			// Resolve the ID path to the actual virtual path
			resolvedPath, err := mrf.resolveIDPath(normalizedName)
			if err == nil && resolvedPath != "" {
				// Continue with the resolved path
				normalizedName = resolvedPath
				exists = true
			}
		}

		if !exists {
			return false, nil, fs.ErrNotExist
		}
	}

	// Use lightweight metadata — Stat only needs size and modtime, not segments.
	fileMeta, err := mrf.metadataService.ReadFileMetadataLite(normalizedName)
	if err != nil {
		return false, nil, fmt.Errorf("failed to read file metadata: %w", err)
	}

	if fileMeta == nil {
		return false, nil, fs.ErrNotExist
	}

	// Extract showCorrupted flag from context
	showCorrupted := false
	if sc, ok := ctx.Value(utils.ShowCorrupted).(bool); ok {
		showCorrupted = sc
	}

	// Filter out masked files if masking is enabled and not showing corrupted
	if !showCorrupted {
		cfg := mrf.configGetter()
		if cfg.Streaming.FailureMasking.Enabled == nil || *cfg.Streaming.FailureMasking.Enabled {
			health, err := mrf.healthRepository.GetFileHealth(ctx, normalizedName)
			if err == nil && health != nil && health.IsMasked {
				return false, nil, fs.ErrNotExist
			}
		}
	}

	// Convert to fs.FileInfo
	info := &MetadataFileInfo{
		name:    filepath.Base(normalizedName),
		size:    fileMeta.FileSize,
		mode:    0644, // Default file mode
		modTime: getFileModTime(fileMeta.ModifiedAt),
		isDir:   false,
	}

	return true, info, nil
}

// MetadataFileInfo implements fs.FileInfo for metadata-based files
type MetadataFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (mfi *MetadataFileInfo) Name() string       { return mfi.name }
func (mfi *MetadataFileInfo) Size() int64        { return mfi.size }
func (mfi *MetadataFileInfo) Mode() os.FileMode  { return mfi.mode }
func (mfi *MetadataFileInfo) ModTime() time.Time { return mfi.modTime }
func (mfi *MetadataFileInfo) IsDir() bool        { return mfi.isDir }
func (mfi *MetadataFileInfo) Sys() any           { return nil }

// MetadataSegmentLoader adapts metadata segments to the usenet.SegmentLoader interface
type MetadataSegmentLoader struct {
	segments []*metapb.SegmentData
}

// newMetadataSegmentLoader creates a new metadata segment loader
func newMetadataSegmentLoader(segments []*metapb.SegmentData) *MetadataSegmentLoader {
	return &MetadataSegmentLoader{
		segments: segments,
	}
}

// GetSegment implements usenet.SegmentLoader interface
func (msl *MetadataSegmentLoader) GetSegment(index int) (segment usenet.Segment, groups []string, ok bool) {
	if index < 0 || index >= len(msl.segments) {
		return usenet.Segment{}, nil, false
	}

	seg := msl.segments[index]

	return usenet.Segment{
		Id:    seg.Id,
		Start: seg.StartOffset,
		End:   seg.EndOffset,
		Size:  seg.SegmentSize,
	}, []string{}, true // Empty groups for now - could be stored in metadata if needed
}

// MetadataVirtualDirectory implements afero.File for metadata-backed virtual directories
type MetadataVirtualDirectory struct {
	name             string
	normalizedPath   string
	metadataService  *metadata.MetadataService
	healthRepository *database.HealthRepository
	configGetter     config.ConfigGetter
	showCorrupted    bool
}

// Read implements afero.File.Read (not supported for directories)
func (mvd *MetadataVirtualDirectory) Read(p []byte) (n int, err error) {
	return 0, ErrCannotReadDirectory
}

// ReadAt implements afero.File.ReadAt (not supported for directories)
func (mvd *MetadataVirtualDirectory) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, ErrCannotReadDirectory
}

// Seek implements afero.File.Seek (not supported for directories)
func (mvd *MetadataVirtualDirectory) Seek(offset int64, whence int) (int64, error) {
	return 0, ErrCannotReadDirectory
}

// Close implements afero.File.Close
func (mvd *MetadataVirtualDirectory) Close() error {
	return nil
}

// Name implements afero.File.Name
func (mvd *MetadataVirtualDirectory) Name() string {
	return mvd.name
}

// Readdir implements afero.File.Readdir
func (mvd *MetadataVirtualDirectory) Readdir(count int) ([]fs.FileInfo, error) {
	// Single os.ReadDir call that returns both subdirectory infos and file names.
	// Uses ReadFileMetadataLite for files so that full protos (with SegmentData,
	// Par2Files, etc.) are NOT pulled into the main cache just for a listing.
	dirInfos, fileNames, err := mvd.metadataService.ListDirectoryAll(mvd.normalizedPath)
	if err != nil {
		return nil, err
	}

	var infos []fs.FileInfo

	// Add directories first. dirInfo already carries the backing directory's
	// real mtime from the single os.ReadDir in ListDirectoryAll, so there is
	// nothing extra to stat per subdirectory.
	for _, dirInfo := range dirInfos {
		infos = append(infos, dirInfo)
		if count > 0 && len(infos) >= count {
			return infos, nil
		}
	}

	// Check if failure masking is enabled
	cfg := mvd.configGetter()
	maskingEnabled := cfg.Streaming.FailureMasking.Enabled == nil || *cfg.Streaming.FailureMasking.Enabled

	ctx := context.Background()

	for _, fileName := range fileNames {
		virtualFilePath := filepath.Join(mvd.normalizedPath, fileName)
		fileMeta, err := mvd.metadataService.ReadFileMetadataLite(virtualFilePath)
		if err != nil || fileMeta == nil {
			continue
		}

		// Skip corrupted files unless showCorrupted flag is set
		if !mvd.showCorrupted && fileMeta.Status == metapb.FileStatus_FILE_STATUS_CORRUPTED {
			continue
		}

		// Skip masked files if masking is enabled
		if maskingEnabled && !mvd.showCorrupted {
			health, err := mvd.healthRepository.GetFileHealth(ctx, virtualFilePath)
			if err == nil && health != nil && health.IsMasked {
				continue
			}
		}

		info := &MetadataFileInfo{
			name:    fileName,
			size:    fileMeta.FileSize,
			mode:    0644,
			modTime: getFileModTime(fileMeta.ModifiedAt),
			isDir:   false,
		}
		infos = append(infos, info)
		if count > 0 && len(infos) >= count {
			return infos, nil
		}
	}

	return infos, nil
}

// Readdirnames implements afero.File.Readdirnames
func (mvd *MetadataVirtualDirectory) Readdirnames(n int) ([]string, error) {
	infos, err := mvd.Readdir(n)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}

	return names, nil
}

// Stat implements afero.File.Stat
func (mvd *MetadataVirtualDirectory) Stat() (fs.FileInfo, error) {
	modT := mvd.metadataService.DirectoryModTime(mvd.normalizedPath)
	if modT.IsZero() {
		modT = epochFallbackModTime
	}
	info := &MetadataFileInfo{
		name:    filepath.Base(mvd.normalizedPath),
		size:    0,
		mode:    os.ModeDir | 0755,
		modTime: modT,
		isDir:   true,
	}
	return info, nil
}

// Write implements afero.File.Write (not supported)
func (mvd *MetadataVirtualDirectory) Write(p []byte) (n int, err error) {
	return 0, os.ErrPermission
}

// WriteAt implements afero.File.WriteAt (not supported)
func (mvd *MetadataVirtualDirectory) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, os.ErrPermission
}

// WriteString implements afero.File.WriteString (not supported)
func (mvd *MetadataVirtualDirectory) WriteString(s string) (ret int, err error) {
	return 0, os.ErrPermission
}

// Sync implements afero.File.Sync (no-op for directories)
func (mvd *MetadataVirtualDirectory) Sync() error {
	return nil
}

// Truncate implements afero.File.Truncate (not supported)
func (mvd *MetadataVirtualDirectory) Truncate(size int64) error {
	return os.ErrPermission
}

// fileHandleMeta holds the subset of FileMetadata fields that an open
// MetadataVirtualFile actually needs. Storing this lean struct instead of the
// full protobuf lets the proto wrapper (protoimpl.MessageState, unknownFields,
// sizeCache, and unused fields like NzbdavId) be collected immediately after
// OpenFile returns. Segment and nested-source slices are carried by reference;
// they remain live only while the handle is open and are released in Close().
type fileHandleMeta struct {
	FileSize      int64
	ModifiedAt    int64
	SourceNzbPath string
	Encryption    metapb.Encryption
	Password      string
	Salt          string
	AesKey        []byte
	AesIv         []byte
	SegmentData   []*metapb.SegmentData
	NestedSources []*metapb.NestedSegmentSource
	// ClipBoundaries is the per-clip timeline table for a multi-clip BD main
	// feature. Non-empty enables the continuous-timeline TS remux on reads;
	// empty (every other file) bypasses it entirely.
	ClipBoundaries []*metapb.ClipBoundary
	// KnownHoles is the persisted hole map: segments confirmed missing on all
	// providers, zero-filled during streaming without a fetch round-trip.
	KnownHoles []*metapb.HoleRun
	// ReleaseDate is the Unix timestamp of the original Usenet post, forwarded
	// to the pool as nntppool.Req.ArticleDate so per-provider retention limits
	// apply. 0 (metadata written before the field existed) means unknown,
	// which applies no policy.
	ReleaseDate int64
	// HoleProviderFingerprint is the provider set KnownHoles were confirmed
	// against; runs recorded under another set are re-probed rather than padded.
	HoleProviderFingerprint string
}

// articleDate returns when this file's articles were posted, for the pool's
// per-provider retention limits. The zero time means unknown, which applies
// no policy rather than assuming the articles are old.
func (mvf *MetadataVirtualFile) articleDate() time.Time {
	if mvf.meta == nil || mvf.meta.ReleaseDate <= 0 {
		return time.Time{}
	}
	return time.Unix(mvf.meta.ReleaseDate, 0)
}

// MetadataVirtualFile implements afero.File for metadata-backed virtual files
type MetadataVirtualFile struct {
	name             string
	meta             *fileHandleMeta
	metadataService  *metadata.MetadataService
	healthRepository *database.HealthRepository
	arrsService      ARRsRepairService
	rcloneClient     rclonecli.RcloneRcClient // RClone RC client for VFS notifications
	repairCoalescer  *RepairCoalescer         // Throttles repair triggers; may be nil in tests
	padRecorder      *padRecorder             // Persists degraded-pad events; may be nil in tests
	patchSource      PatchSource              // Repaired-article payloads; may be nil
	repairEnqueuer   RepairEnqueuer           // Queues PAR2 repairs; may be nil
	configGetter     config.ConfigGetter
	poolManager      pool.Manager // Pool manager for dynamic pool access
	ctx              context.Context
	cancelCtx        context.CancelFunc
	maxPrefetch      int // Maximum segments prefetched ahead of current read position
	rcloneCipher     *rclone.RcloneCrypt
	aesCipher        *aes.AesCipher
	globalPassword   string
	globalSalt       string
	streamTracker    StreamTracker
	streamID         string
	segmentStore     usenet.SegmentStore // optional segment cache
	segmentIndexOnce sync.Once           // guards lazy init of segmentIndex

	// clipSpans is the lazily-built absolute byte-range + delta table for the
	// continuous-timeline remux, derived once from meta.ClipBoundaries.
	clipSpans     []clipSpan
	clipSpansOnce sync.Once

	// Reader state and position tracking
	reader io.ReadCloser
	// bufOffReader is mvf.reader pre-asserted to the GetBufferedOffset interface
	// (nil when the active reader doesn't implement it), so the hot Read/ReadAt
	// loops avoid repeating the type assertion every iteration. Kept in sync by
	// setReader and the remux-wrap step; guarded by mvf.mu like reader.
	bufOffReader interface{ GetBufferedOffset() int64 }
	// bufAheadReader is mvf.reader pre-asserted to BufferedAhead, which says
	// how far a forward skip can drain through the shared reader for free.
	bufAheadReader    interface{ BufferedAhead() int64 }
	readerInitialized bool
	position          int64 // File position (what client sees after Seek)
	originalRangeEnd  int64 // Original end requested by client (-1 for unbounded)

	// readAtSharedNext is the next file offset that the shared reader can serve
	// via ReadAtContext. Sequential ReadAt calls reuse mvf.reader when the
	// requested offset matches this cursor; non-sequential calls use ephemeral
	// readers and set this to -1 (invalidated). A value of 0 with
	// !readerInitialized means the very first ReadAt at offset 0 is shared.
	readAtSharedNext int64

	// ephemeralStreak counts consecutive non-sequential ReadAtContext calls
	// (scrubs, header probes). Short bursts keep the shared reader alive so
	// the 60-segment prefetch pipeline survives; after ephemeralStreakLimit
	// consecutive misses the player has genuinely moved and we tear down.
	ephemeralStreak int

	// metadataGone is set once a streaming failure caused this file's metadata to
	// be removed underneath the open handle (moved to the corrupted safety folder
	// by a repair, or deleted by delete-on-corruption). The path this handle
	// refers to no longer exists, so every subsequent read must fail instead of
	// rebuilding a reader against segments already known to be missing.
	//
	// Atomic rather than mu-guarded: it is set from updateFileHealthOnError,
	// which is reached both from the mu-holding read paths and from
	// createUsenetReader closures that run on background goroutines.
	metadataGone atomic.Bool

	// Segment offset index for O(1) offset→segment lookup
	segmentIndex *segmentOffsetIndex

	mu      sync.Mutex
	closeWg sync.WaitGroup // tracks the bounded closer-worker pool

	// closerCh is the per-file bounded closer queue. Lazy-initialized
	// on first closeCurrentReader; closed in mvf.Close so the worker
	// goroutines exit. See enqueueCloser / closerWorkerCount.
	closerCh chan io.Closer

	// interruptHandle tracks the latest reader for cancellation from Close
	// without taking mvf.mu. Read can hold mvf.mu for the full segment
	// download latency, so Close must be able to fire ctx-cancel on the
	// in-flight reader before contending for the lock. Stores a
	// readerInterrupter; an empty value (Load returns nil) means no
	// interruptible reader is set.
	interruptHandle atomic.Value

	// randomReadCache coalesces small random ReadAts within the same
	// segment. Lazily initialized; held under mvf.mu.
	randomReadCache *lru.Cache[int, []byte]

	// Hole padding state (see holes.go). holeAcc accumulates confirmed
	// missing segments for this handle, seeded from meta.KnownHoles; holeMu
	// guards it (hole hooks run on download goroutines).
	holeMu       sync.Mutex
	holeAcc      *holes.Accumulator
	holeHooksVal *usenet.HoleHooks
	holeOnce     sync.Once
	holeMeta     holeMetaSnapshot // set in holeHooks(); see holeMetaSnapshot doc
}

// randomReadCacheSize bounds the per-file ephemeral-read cache. 8
// segments × default segment size (~768 KB) ≈ 6 MB per open file,
// keeping the worst-case footprint bounded under library-scan loads.
const randomReadCacheSize = 8

// readerInterrupter is the interface implemented by readers that can
// abort their in-flight downloads by canceling their internal context.
// UsenetReader (and wrappers that own a UsenetReader) implement this so
// MetadataVirtualFile.Close can interrupt them without holding mvf.mu.
type readerInterrupter interface {
	Interrupt()
}

// interruptSlot wraps a readerInterrupter so we can store a nil-valued
// entry in atomic.Value without panicking (Value.Store rejects untyped
// nil and rejects type changes between calls).
type interruptSlot struct{ i readerInterrupter }

// setReader assigns a new reader and refreshes the interrupt handle.
// Callers must hold mvf.mu. Pass nil to clear.
func (mvf *MetadataVirtualFile) setReader(r io.ReadCloser) {
	mvf.reader = r
	mvf.bufOffReader, _ = r.(interface{ GetBufferedOffset() int64 })
	mvf.bufAheadReader, _ = r.(interface{ BufferedAhead() int64 })
	slot := interruptSlot{}
	if i, ok := r.(readerInterrupter); ok {
		slot.i = i
	}
	mvf.interruptHandle.Store(slot)
}

// interruptCurrentReader fires a non-blocking cancel on the in-flight
// reader if one is set. Safe to call without holding mvf.mu.
func (mvf *MetadataVirtualFile) interruptCurrentReader() {
	v := mvf.interruptHandle.Load()
	if v == nil {
		return
	}
	slot, _ := v.(interruptSlot)
	if slot.i != nil {
		slot.i.Interrupt()
	}
}

// armReadTimeout bounds a single read, returning a context carrying the
// deadline, a predicate reporting whether that deadline was hit, and a stop
// function the caller must defer.
//
// Two mechanisms are needed because the read path has two shapes:
//
//   - The shared reader ignores the caller's context entirely —
//     UsenetReader.Read waits on the reader's own context (see
//     usenet.segment.GetReaderContext), so a derived deadline is invisible to
//     it. A timer fires the reader's Interrupt hook instead, which cancels its
//     internal context and releases its segments, unblocking the read.
//   - Ephemeral (seek-driven) readers are short-lived locals that are never
//     registered for interruption, but their reads run through
//     readFullContext, which force-closes the reader when the context expires.
//     Those need the deadline on the context.
//
// Both are no-ops when the timeout is disabled; the parent context is returned
// unchanged.
//
// Safe to call without holding mvf.mu: interruptCurrentReader reads an
// atomic.Value, so the watchdog can break a jam even while another read holds
// the mutex.
func (mvf *MetadataVirtualFile) armReadTimeout(parent context.Context) (ctx context.Context, timedOut func() bool, stop func()) {
	never := func() bool { return false }
	if mvf.configGetter == nil {
		return parent, never, func() {}
	}
	cfg := mvf.configGetter()
	if cfg == nil {
		return parent, never, func() {}
	}
	timeout := cfg.GetStreamReadTimeout()
	if timeout <= 0 {
		return parent, never, func() {}
	}

	ctx, cancel := context.WithTimeout(parent, timeout)

	var interrupted atomic.Bool
	timer := time.AfterFunc(timeout, func() {
		interrupted.Store(true)
		slog.WarnContext(mvf.ctx, "Read exceeded the streaming read timeout, interrupting reader",
			"path", mvf.name,
			"timeout", timeout)
		mvf.interruptCurrentReader()
	})

	// Checking ctx.Err() rather than only the timer flag keeps this
	// deterministic: the context package sets DeadlineExceeded before it
	// releases any waiter, so a read that returns because of the deadline
	// always observes it, with no race against the timer goroutine.
	timedOut = func() bool {
		return interrupted.Load() || errors.Is(ctx.Err(), context.DeadlineExceeded)
	}

	return ctx, timedOut, func() {
		timer.Stop()
		cancel()
	}
}

// readContext returns the handle's context, falling back to Background for
// handles built without one (tests, bridges).
func (mvf *MetadataVirtualFile) readContext() context.Context {
	if mvf.ctx != nil {
		return mvf.ctx
	}
	return context.Background()
}

// classifyReadError maps a reader error to the error a read should return,
// reporting data corruption to the health pipeline on the way. This is what
// feeds streaming failure masking — without it a DMCA'd file never accumulates
// failures and is never masked. Callers must hold mvf.mu.
func (mvf *MetadataVirtualFile) classifyReadError(readErr error) error {
	var dataCorruptionErr *usenet.DataCorruptionError
	if errors.As(readErr, &dataCorruptionErr) {
		mvf.updateFileHealthOnError(dataCorruptionErr, dataCorruptionErr.NoRetry)
		return &CorruptedFileError{
			TotalExpected: mvf.meta.FileSize,
			UnderlyingErr: dataCorruptionErr,
		}
	}
	return readErr
}

// truncatedTailError describes a file whose advertised size extends past the
// bytes its articles actually hold: a reader built at `at` produced nothing
// and rebuilding it cannot help. The bytes from `at` to FileSize are gone for
// good, so this is a permanent data corruption (NoRetry) that the health
// pipeline must record and hand to repair. Without that verdict every client
// that wants the tail (MKV cues live there) re-fetches the final article on
// each attempt and retries indefinitely — the 15-requests-per-second storm
// seen in the field. The error still unwraps to io.ErrUnexpectedEOF.
func (mvf *MetadataVirtualFile) truncatedTailError(at int64) error {
	return &usenet.DataCorruptionError{
		UnderlyingErr: fmt.Errorf("%w: reader ended at offset %d before the requested end (file advertises %d bytes)",
			io.ErrUnexpectedEOF, at, mvf.meta.FileSize),
		BytesRead:  at,
		NoRetry:    true,
		FileOffset: at,
	}
}

// segmentOffsetIndex provides O(1) lookup for offset→segment mapping using binary search
type segmentOffsetIndex struct {
	offsets []int64 // Cumulative start offset of each segment in file coordinates
	sizes   []int64 // Size of each segment's usable data
}

// buildSegmentIndex builds an offset index from metadata segments for O(1) lookup
func buildSegmentIndex(segments []*metapb.SegmentData) *segmentOffsetIndex {
	if len(segments) == 0 {
		return nil
	}

	idx := &segmentOffsetIndex{
		offsets: make([]int64, len(segments)),
		sizes:   make([]int64, len(segments)),
	}

	var pos int64
	for i, seg := range segments {
		idx.offsets[i] = pos
		usableLen := seg.EndOffset - seg.StartOffset + 1
		idx.sizes[i] = usableLen
		pos += usableLen
	}

	return idx
}

// findSegmentForOffset returns the segment index containing the given file offset
// Returns -1 if offset is beyond all segments
func (idx *segmentOffsetIndex) findSegmentForOffset(offset int64) int {
	if idx == nil || len(idx.offsets) == 0 || offset < 0 {
		return -1
	}

	// Binary search for the segment containing this offset
	// We want the largest i such that offsets[i] <= offset
	n := len(idx.offsets)

	// Quick check: if offset is before first segment or beyond last
	if offset < idx.offsets[0] {
		return 0
	}

	lastSegEnd := idx.offsets[n-1] + idx.sizes[n-1] - 1
	if offset > lastSegEnd {
		return -1
	}

	// Binary search
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		if idx.offsets[mid] <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// lo-1 is the largest index where offsets[i] <= offset
	return lo - 1
}

// totalBytes is the number of bytes the indexed segments cover.
func (idx *segmentOffsetIndex) totalBytes() int64 {
	if idx == nil || len(idx.offsets) == 0 {
		return 0
	}
	n := len(idx.offsets)
	return idx.offsets[n-1] + idx.sizes[n-1]
}

// getOffsetForSegment returns the cumulative file offset at the start of the given segment index
// Returns 0 if the index is invalid or out of bounds
func (idx *segmentOffsetIndex) getOffsetForSegment(segmentIndex int) int64 {
	if idx == nil || segmentIndex < 0 || segmentIndex >= len(idx.offsets) {
		return 0
	}
	return idx.offsets[segmentIndex]
}

// GetStreamID returns the active stream ID associated with this file handle
func (mvf *MetadataVirtualFile) GetStreamID() string {
	return mvf.streamID
}

// Read implements afero.File.Read
func (mvf *MetadataVirtualFile) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Same watchdog as ReadAtContext — a stalled fetch parks a WebDAV/rclone
	// read just as indefinitely as a FUSE one. Read has no context to thread
	// through, so only the interrupt half applies here.
	_, timedOut, stopTimeout := mvf.armReadTimeout(mvf.readContext())
	defer func() {
		stopTimeout()
		if timedOut() && err != nil && !errors.Is(err, ErrReadTimeout) {
			err = fmt.Errorf("%w: %w", ErrReadTimeout, err)
		}
	}()

	mvf.mu.Lock()
	defer mvf.mu.Unlock()

	if mvf.meta == nil {
		return 0, ErrFileClosed
	}

	stalledAt := int64(-1)
	for n < len(p) {
		if err := mvf.ensureReader(); err != nil {
			return n, err
		}

		totalRead, readErr := mvf.reader.Read(p[n:])
		n += totalRead
		mvf.position += int64(totalRead)

		if totalRead > 0 && mvf.streamTracker != nil && mvf.streamID != "" {
			mvf.streamTracker.UpdateProgress(mvf.streamID, int64(totalRead))
			mvf.streamTracker.UpdateCurrentOffset(mvf.streamID, mvf.position)

			// Update buffered offset if available
			if mvf.bufOffReader != nil {
				mvf.streamTracker.UpdateBufferedOffset(mvf.streamID, mvf.bufOffReader.GetBufferedOffset())
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) && mvf.hasMoreDataToRead() {
				// A rebuilt reader that ends at the same offset again cannot reach
				// the range end; rotating once more would spin here holding mvf.mu.
				if totalRead == 0 && mvf.position == stalledAt {
					mvf.closeCurrentReader()
					return n, mvf.classifyReadError(mvf.truncatedTailError(mvf.position))
				}
				stalledAt = mvf.position
				// Close current reader and try to get a new one for the next range in next iteration
				mvf.closeCurrentReader()
				continue
			}

			// For data corruption errors, report and mark as corrupted
			var dataCorruptionErr *usenet.DataCorruptionError
			if errors.As(readErr, &dataCorruptionErr) {
				mvf.updateFileHealthOnError(dataCorruptionErr, dataCorruptionErr.NoRetry)
				return n, &CorruptedFileError{
					TotalExpected: mvf.meta.FileSize,
					UnderlyingErr: dataCorruptionErr,
				}
			}

			return n, readErr
		}
	}

	return n, nil
}

// ReadAt implements afero.File.ReadAt. It delegates to ReadAtContext using the
// file-level context.
func (mvf *MetadataVirtualFile) ReadAt(p []byte, off int64) (n int, err error) {
	ctx := mvf.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return mvf.ReadAtContext(ctx, p, off)
}

// ReadAtContext serves offset-based reads. Sequential offsets reuse the shared
// streaming reader (preserving the prefetch pipeline); non-sequential offsets
// use a short-lived range reader so the shared pipeline is not disturbed.
// All calls are serialized via mvf.mu — the caller (FUSE handle) must ensure
// per-handle ordering.
func (mvf *MetadataVirtualFile) ReadAtContext(readCtx context.Context, p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if off < 0 {
		return 0, ErrNegativeOffset
	}

	// Armed before taking mvf.mu so the budget covers queueing behind another
	// read on the same handle. A single stuck read otherwise blocks every
	// subsequent read on this file; interrupting the in-flight reader from here
	// breaks that jam.
	readCtx, timedOut, stopTimeout := mvf.armReadTimeout(readCtx)
	defer func() {
		stopTimeout()
		if timedOut() && err != nil && !errors.Is(err, ErrReadTimeout) {
			err = fmt.Errorf("%w: %w", ErrReadTimeout, err)
		}
	}()

	mvf.mu.Lock()
	defer mvf.mu.Unlock()

	// mvf.meta is nil-ed by Close() under mvf.mu — a concurrent Close (e.g. from
	// the FUSE handle release path) can complete between an unlocked check and
	// lock acquisition, so this must be checked here, not before Lock().
	if mvf.meta == nil {
		return 0, ErrFileClosed
	}
	if off >= mvf.meta.FileSize {
		return 0, io.EOF
	}

	// The ephemeral path below builds its own reader without going through
	// ensureReader, so it needs this guard of its own.
	if err := mvf.metadataGoneErr(); err != nil {
		return 0, err
	}

	// Determine whether this offset can reuse the shared reader.
	// Shared path: offset matches the next expected sequential position, OR
	// it is slightly ahead (forward-skip: gap ≤ forwardSkipLimit) — discard
	// the gap via the already-prefetched pipeline instead of creating an
	// ephemeral reader. Covers "skip intro" / chapter-jump patterns where
	// the player moves forward by a few seconds inside the prefetch window.
	const forwardSkipLimit = 16 * 1024 * 1024 // 16 MB — roughly 20 segments
	forwardSkip := mvf.readerInitialized &&
		mvf.readAtSharedNext >= 0 &&
		off > mvf.readAtSharedNext &&
		off-mvf.readAtSharedNext <= forwardSkipLimit
	// Draining is only free while the gap sits inside bytes the reader has
	// already scheduled; past that, every skipped article is a download the
	// caller never wanted, and a reader opened at the target is cheaper.
	if forwardSkip && mvf.bufAheadReader != nil && off-mvf.readAtSharedNext > mvf.bufAheadReader.BufferedAhead() {
		mvf.closeCurrentReader()
		mvf.position = off
		mvf.readAtSharedNext = off
		forwardSkip = false
	}
	useShared := forwardSkip ||
		(mvf.readAtSharedNext >= 0 && off == mvf.readAtSharedNext) ||
		(mvf.readAtSharedNext == 0 && !mvf.readerInitialized && off == mvf.position)

	if useShared {
		if err := mvf.ensureReader(); err != nil {
			return 0, err
		}

		// Forward-skip: discard the gap between the shared reader's current
		// position and the requested offset. Prefetched bytes make this
		// memory-speed; any not-yet-fetched bytes download from the pipeline
		// that would be needed for sequential play anyway.
		if forwardSkip {
			gap := off - mvf.readAtSharedNext
			if _, skipErr := io.CopyN(io.Discard, mvf.reader, gap); skipErr != nil {
				// Skip failed (e.g. EOF mid-gap). Close and let the
				// ephemeral path handle this read.
				mvf.closeCurrentReader()
				mvf.readAtSharedNext = -1
				goto ephemeral
			}
			mvf.readAtSharedNext = off
		}

		// Read from the shared reader (same logic as Read but bounded to len(p))
		want := int64(len(p))
		if off+want > mvf.meta.FileSize {
			want = mvf.meta.FileSize - off
		}
		buf := p[:want]
		var sharedErr error
		stalledAt := int64(-1)
		for n < int(want) {
			rn, readErr := mvf.reader.Read(buf[n:])
			n += rn

			if n > 0 && mvf.streamTracker != nil && mvf.streamID != "" {
				mvf.streamTracker.UpdateProgress(mvf.streamID, int64(rn))
				mvf.streamTracker.UpdateCurrentOffset(mvf.streamID, off+int64(n))
				if mvf.bufOffReader != nil {
					mvf.streamTracker.UpdateBufferedOffset(mvf.streamID, mvf.bufOffReader.GetBufferedOffset())
				}
			}

			if readErr != nil {
				if errors.Is(readErr, io.EOF) && mvf.hasMoreDataToRead() {
					// Same no-progress guard as Read: a rebuilt reader ending at the
					// same offset cannot reach the range end.
					at := off + int64(n)
					if rn == 0 && at == stalledAt {
						sharedErr = mvf.truncatedTailError(at)
						break
					}
					stalledAt = at
					mvf.closeCurrentReader()
					if rotateErr := mvf.ensureReader(); rotateErr != nil {
						sharedErr = rotateErr
						break
					}
					continue
				}
				sharedErr = readErr
				break
			}
		}

		// Advance the shared cursor so the next sequential call hits this path again.
		mvf.readAtSharedNext = off + int64(n)
		mvf.ephemeralStreak = 0 // sequential read — reset scrub counter

		// A real failure (missing segment, interrupted reader, pool error) must
		// reach the caller. Returning a silent short read here was what kept
		// streaming failure masking from ever seeing a FUSE-path failure.
		// io.EOF stays non-fatal: hasMoreDataToRead() already said there is
		// nothing left to rotate to, so it is a genuine end of data.
		if sharedErr != nil && !errors.Is(sharedErr, io.EOF) {
			// The shared reader is in an indeterminate state — drop it so the
			// next read rebuilds cleanly rather than inheriting the failure.
			mvf.closeCurrentReader()
			mvf.readAtSharedNext = -1
			return n, mvf.classifyReadError(sharedErr)
		}
		return n, nil
	}

ephemeral:
	// --- Ephemeral path: non-sequential offset ---
	// Track consecutive scrubs. Short bursts (e.g. Plex thumbnail probes) keep
	// the shared reader alive so the 60-segment prefetch pipeline survives.
	// After ephemeralStreakLimit consecutive misses the player has genuinely
	// moved, so we tear down and let the next sequential run rebuild from the
	// new position.
	const ephemeralStreakLimit = 3
	if mvf.readerInitialized {
		mvf.ephemeralStreak++
		if mvf.ephemeralStreak >= ephemeralStreakLimit {
			mvf.ephemeralStreak = 0
			mvf.readAtSharedNext = -1
			mvf.closeCurrentReader()
		}
		// else: shared reader stays alive; readAtSharedNext remains at the
		// reader's current position so the next sequential call can reuse it.
	} else {
		mvf.readAtSharedNext = -1
		mvf.ephemeralStreak = 0
	}

	end := off + int64(len(p)) - 1
	if end >= mvf.meta.FileSize {
		end = mvf.meta.FileSize - 1
	}

	// Coalesce small random reads through a per-file LRU of full segment
	// bytes. Plex/Jellyfin scrubbing produces bursts of small ReadAts
	// across a handful of segments; without this every call hit the wire
	// (storm S5). Only viable for plain (unencrypted, non-nested)
	// segments — encrypted streams don't map cleanly to segment
	// boundaries.
	if n, served := mvf.tryServeFromRandomReadCache(readCtx, p, off, end); served {
		// Only update the shared cursor when the shared reader is gone; if it
		// is still alive, readAtSharedNext already points to the reader's
		// current position and must not be overwritten.
		if !mvf.readerInitialized {
			mvf.readAtSharedNext = off + int64(n)
		}
		return n, nil
	}

	reader, err := mvf.createReaderAtOffset(off, end)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	buf := p[:end-off+1]
	n, err = readFullContext(readCtx, reader, buf)
	if err == io.ErrUnexpectedEOF {
		err = nil
	}
	// The window lies inside the advertised file, yet a fresh reader at its
	// start had nothing at all: the data behind this offset does not exist
	// (a truncated final article). Report it as corruption so the client
	// gets a definitive answer instead of a short read it will retry forever.
	if n == 0 && errors.Is(err, io.EOF) && off < mvf.meta.FileSize {
		err = mvf.truncatedTailError(off)
	}

	// Only update the shared cursor when the shared reader was torn down.
	// If it is still alive, readAtSharedNext already points to the reader's
	// current position and must not be overwritten by the ephemeral endpoint.
	if !mvf.readerInitialized {
		mvf.readAtSharedNext = off + int64(n)
	}

	if err != nil {
		// Same reporting as the shared path — scrub-driven reads must feed
		// failure masking too, otherwise a player that seeks never marks a
		// DMCA'd file as failing.
		return n, mvf.classifyReadError(err)
	}

	return n, nil
}

// tryServeFromRandomReadCache attempts to satisfy a single-segment
// ephemeral ReadAt from the per-file LRU. On miss it downloads the
// full containing segment, caches it, then serves the requested
// window. Returns (bytesCopied, true) on success or (0, false) when
// the caller must fall back to the normal ephemeral path. Caller must
// hold mvf.mu.
//
// Skipped for encrypted and nested-source files because their segment
// boundaries don't align with plaintext byte ranges.
func (mvf *MetadataVirtualFile) tryServeFromRandomReadCache(readCtx context.Context, p []byte, off, end int64) (int, bool) {
	if mvf.meta == nil ||
		mvf.meta.Encryption != metapb.Encryption_NONE ||
		len(mvf.meta.NestedSources) > 0 ||
		len(mvf.meta.SegmentData) == 0 {
		return 0, false
	}
	mvf.segmentIndexOnce.Do(func() {
		mvf.segmentIndex = buildSegmentIndex(mvf.meta.SegmentData)
	})
	if mvf.segmentIndex == nil {
		return 0, false
	}
	segIdx := mvf.segmentIndex.findSegmentForOffset(off)
	if segIdx < 0 {
		return 0, false
	}
	segStart := mvf.segmentIndex.getOffsetForSegment(segIdx)
	segSize := mvf.segmentIndex.sizes[segIdx]
	segEnd := segStart + segSize - 1
	// Only single-segment reads benefit from the per-segment cache.
	if end > segEnd {
		return 0, false
	}

	if mvf.randomReadCache == nil {
		c, err := lru.New[int, []byte](randomReadCacheSize)
		if err != nil {
			return 0, false
		}
		mvf.randomReadCache = c
	}

	// At end-of-file the caller's buffer may be larger than the readable
	// remainder of the segment — the ephemeral path clamps `end` to
	// FileSize-1 but `len(p)` is not clamped. Use the clamped window
	// (off..end inclusive) as the source of truth for how many bytes we're
	// actually allowed to copy out of the cached segment.
	want := int(end - off + 1)
	if want <= 0 {
		return 0, false
	}
	if want > len(p) {
		want = len(p)
	}

	if data, ok := mvf.randomReadCache.Get(segIdx); ok {
		rel := off - segStart
		if rel < 0 || rel >= int64(len(data)) {
			return 0, false
		}
		n := copy(p[:want], data[rel:])
		return n, true
	}

	// Miss: fetch the whole segment via an ephemeral reader so the next
	// small read in the same segment is a cache hit.
	reader, err := mvf.createReaderAtOffset(segStart, segEnd)
	if err != nil {
		return 0, false
	}
	defer reader.Close()

	full := make([]byte, segSize)
	rn, err := readFullContext(readCtx, reader, full)
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, false
	}
	rel := off - segStart
	if int64(rn) <= rel {
		return 0, false
	}
	// Only cache when we got the whole segment; partial data is more
	// trouble than it's worth.
	if int64(rn) == segSize {
		mvf.randomReadCache.Add(segIdx, full)
	}
	n := copy(p[:want], full[rel:rn])
	return n, true
}

// createReaderAtOffset creates an independent reader for reading at a specific offset.
// This reader is self-contained and can be used concurrently with other readers.
func (mvf *MetadataVirtualFile) createReaderAtOffset(start, end int64) (io.ReadCloser, error) {
	if mvf.remuxActive() {
		// Open the underlying window aligned to the packet grid and trim back to
		// [start,end] so the rewrite is independent of this (possibly unaligned)
		// window — see wrapAlignedRemux.
		return mvf.wrapAlignedRemux(start, end, mvf.createRawReaderAtOffset)
	}
	return mvf.createRawReaderAtOffset(start, end)
}

// createRawReaderAtOffset builds the underlying reader for [start,end] without
// the continuous-timeline remux wrapper.
func (mvf *MetadataVirtualFile) createRawReaderAtOffset(start, end int64) (io.ReadCloser, error) {
	if mvf.poolManager == nil {
		return nil, ErrNoUsenetPool
	}

	// Nested sources take priority — each source has its own segments and AES credentials
	if len(mvf.meta.NestedSources) > 0 {
		return mvf.createNestedReader(start, end)
	}

	if len(mvf.meta.SegmentData) == 0 {
		return nil, ErrMissmatchedSegments
	}

	// Create reader based on encryption type
	if mvf.meta.Encryption != metapb.Encryption_NONE {
		return mvf.createEncryptedReaderAtOffset(start, end)
	}

	return mvf.createUsenetReader(mvf.ctx, start, end)
}

// remuxActive reports whether this file needs the continuous-timeline TS remux
// (multi-clip BD main feature). It lazily builds the clip-span table on first
// use. For every other file it returns false with zero overhead.
func (mvf *MetadataVirtualFile) remuxActive() bool {
	if len(mvf.meta.ClipBoundaries) == 0 {
		return false
	}
	mvf.clipSpansOnce.Do(func() {
		mvf.clipSpans = buildClipSpans(mvf.meta.ClipBoundaries)
	})
	return len(mvf.clipSpans) > 0
}

// wrapAlignedRemux opens a remux-corrected reader that delivers exactly the
// bytes for [start,end]. The raw window is expanded outward to the BDAV packet
// grid (alignStartDown/alignEndUp) so the remux always rewrites whole packets,
// then skipLimitReader trims the rewritten bytes back to [start,end]. This makes
// the output independent of how the caller chunks reads — a window that starts
// or ends mid-packet yields the same bytes as the full sequential rewrite, which
// is what stops Plex's unaligned ranged reads from leaking un-shifted (raw)
// timestamps and stuttering. rawOpen creates the underlying reader for an
// arbitrary [s,e] byte range. Callers must have confirmed remuxActive().
func (mvf *MetadataVirtualFile) wrapAlignedRemux(start, end int64, rawOpen func(s, e int64) (io.ReadCloser, error)) (io.ReadCloser, error) {
	aStart := alignStartDown(mvf.clipSpans, start)
	aEnd := alignEndUp(mvf.clipSpans, end, mvf.meta.FileSize)
	raw, err := rawOpen(aStart, aEnd)
	if err != nil {
		return nil, err
	}
	remuxed := newTSRemuxReader(raw, mvf.clipSpans, aStart)
	return newSkipLimitReader(remuxed, start-aStart, end-start+1), nil
}

// createEncryptedReaderAtOffset creates an encrypted reader for a specific offset range
func (mvf *MetadataVirtualFile) createEncryptedReaderAtOffset(start, end int64) (io.ReadCloser, error) {
	switch mvf.meta.Encryption {
	case metapb.Encryption_RCLONE:
		if mvf.rcloneCipher == nil {
			return nil, ErrNoCipherConfig
		}

		password := mvf.meta.Password
		if password == "" {
			password = mvf.globalPassword
		}
		salt := mvf.meta.Salt
		if salt == "" {
			salt = mvf.globalSalt
		}

		return mvf.rcloneCipher.Open(
			mvf.ctx,
			&utils.RangeHeader{Start: start, End: end},
			mvf.meta.FileSize,
			password,
			salt,
			func(ctx context.Context, s, e int64) (io.ReadCloser, error) {
				return mvf.createUsenetReader(ctx, s, e)
			},
		)

	case metapb.Encryption_AES:
		if mvf.aesCipher == nil {
			return nil, ErrNoCipherConfig
		}
		if len(mvf.meta.AesKey) == 0 {
			return nil, fmt.Errorf("missing AES key in metadata")
		}
		if len(mvf.meta.AesIv) == 0 {
			return nil, fmt.Errorf("missing AES IV in metadata")
		}

		return mvf.aesCipher.Open(
			mvf.ctx,
			&utils.RangeHeader{Start: start, End: end},
			mvf.meta.FileSize,
			mvf.meta.AesKey,
			mvf.meta.AesIv,
			func(ctx context.Context, s, e int64) (io.ReadCloser, error) {
				return mvf.createUsenetReader(ctx, s, e)
			},
		)

	default:
		return nil, fmt.Errorf("unsupported encryption type: %v", mvf.meta.Encryption)
	}
}

// Seek implements afero.File.Seek
func (mvf *MetadataVirtualFile) Seek(offset int64, whence int) (int64, error) {
	mvf.mu.Lock()
	defer mvf.mu.Unlock()

	var abs int64

	switch whence {
	case io.SeekStart: // Relative to the origin of the file
		abs = offset
	case io.SeekCurrent: // Relative to the current offset
		abs = mvf.position + offset
	case io.SeekEnd: // Relative to the end
		abs = mvf.meta.FileSize + offset
	default:
		return 0, ErrInvalidWhence
	}

	if abs < 0 {
		return 0, ErrSeekNegative
	}

	if abs > mvf.meta.FileSize {
		return 0, ErrSeekTooFar
	}

	// Close reader if position changes - UsenetReader is forward-only and cannot seek.
	// Creating a new reader at the target position is faster than downloading and
	// discarding data to catch up.
	if mvf.readerInitialized && abs != mvf.position {
		mvf.closeCurrentReader()
	}

	// Reset originalRangeEnd when position changes to force fresh range calculation
	// on next read. This prevents stale range information from being reused after seek.
	if abs != mvf.position {
		mvf.originalRangeEnd = 0
		if mvf.streamTracker != nil && mvf.streamID != "" {
			mvf.streamTracker.UpdateCurrentOffset(mvf.streamID, abs)
		}
	}

	mvf.position = abs
	// Align ReadAt shared cursor with the new seek position so that the next
	// ReadAtContext at this offset reuses the shared reader. Also reset the
	// ephemeral-streak counter: an explicit Seek establishes a new sequential
	// starting point regardless of previous scrub activity.
	mvf.readAtSharedNext = abs
	mvf.ephemeralStreak = 0
	return abs, nil
}

// Close implements afero.File.Close
func (mvf *MetadataVirtualFile) Close() error {
	if mvf.cancelCtx != nil {
		mvf.cancelCtx()
	}
	// Cancel the in-flight reader before taking mvf.mu — a concurrent
	// Read can hold the lock for the full segment-download latency.
	mvf.interruptCurrentReader()
	mvf.mu.Lock()
	// Remove from stream tracker under the same lock that Read / ReadAtContext
	// use to read streamID. Without this, the race detector flags an
	// unsynchronized read/write between Close and a concurrent Read.
	if mvf.streamTracker != nil && mvf.streamID != "" {
		mvf.streamTracker.Remove(mvf.streamID)
		mvf.streamID = ""
	}
	if mvf.reader != nil {
		mvf.reader.Close()
		mvf.setReader(nil)
		mvf.readerInitialized = false
	}
	mvf.segmentIndex = nil // Release segment offset index for GC
	mvf.meta = nil         // Release segment/nested-source slices for GC
	if mvf.randomReadCache != nil {
		mvf.randomReadCache.Purge()
		mvf.randomReadCache = nil
	}
	// Signal the bounded closer workers to drain remaining queued
	// closes and exit. Workers are tracked by closeWg.
	if mvf.closerCh != nil {
		close(mvf.closerCh)
		mvf.closerCh = nil
	}
	mvf.mu.Unlock()

	// Wait for the closer-worker pool to finish draining.
	mvf.closeWg.Wait()

	return nil
}

// Name implements afero.File.Name
func (mvf *MetadataVirtualFile) Name() string {
	return mvf.name
}

// Readdir implements afero.File.Readdir
func (mvf *MetadataVirtualFile) Readdir(count int) ([]fs.FileInfo, error) {
	// This is a file, not a directory, so readdir is not supported
	return nil, ErrNotDirectory
}

// Readdirnames implements afero.File.Readdirnames
func (mvf *MetadataVirtualFile) Readdirnames(n int) ([]string, error) {
	infos, err := mvf.Readdir(n)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}

	return names, nil
}

// Stat implements afero.File.Stat
func (mvf *MetadataVirtualFile) Stat() (fs.FileInfo, error) {
	info := &MetadataFileInfo{
		name:    filepath.Base(mvf.name),
		size:    mvf.meta.FileSize,
		mode:    0644,
		modTime: getFileModTime(mvf.meta.ModifiedAt),
		isDir:   false, // Files are never directories in simplified schema
	}

	return info, nil
}

// Write implements afero.File.Write (not supported)
func (mvf *MetadataVirtualFile) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("write not supported")
}

// WriteAt implements afero.File.WriteAt (not supported)
func (mvf *MetadataVirtualFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("write not supported")
}

// WriteString implements afero.File.WriteString (not supported)
func (mvf *MetadataVirtualFile) WriteString(s string) (ret int, err error) {
	return 0, fmt.Errorf("write not supported")
}

// Sync implements afero.File.Sync (no-op for read-only)
func (mvf *MetadataVirtualFile) Sync() error {
	return nil
}

// Truncate implements afero.File.Truncate (not supported)
func (mvf *MetadataVirtualFile) Truncate(size int64) error {
	return fmt.Errorf("truncate not supported")
}

// hasMoreDataToRead checks if there's more data to read beyond current range
func (mvf *MetadataVirtualFile) hasMoreDataToRead() bool {
	// If we have an original range end and haven't reached it, there's more to read
	if mvf.originalRangeEnd != -1 && mvf.position < mvf.originalRangeEnd {
		return true
	}
	// If original range was unbounded (-1) and we haven't reached file end, there's more to read
	if mvf.originalRangeEnd == -1 && mvf.position < mvf.meta.FileSize {
		return true
	}
	return false
}

// closeCurrentReader hands the current reader to the bounded closer
// pool so Seek doesn't block on UsenetReader.Close. Runs inline as
// backpressure when the pool's queue is full.
func (mvf *MetadataVirtualFile) closeCurrentReader() {
	if mvf.reader != nil {
		reader := mvf.reader
		mvf.setReader(nil)
		// Interrupt immediately so in-flight NNTP downloads stop consuming pool
		// connections before the closer worker eventually calls Close().
		if i, ok := reader.(readerInterrupter); ok {
			i.Interrupt()
		}
		mvf.enqueueCloser(reader)
	}
	mvf.readerInitialized = false
	mvf.ephemeralStreak = 0
}

// closerWorkerCount bounds the number of background reader-Close
// goroutines that a single MetadataVirtualFile keeps in flight at
// once. Tuned to absorb normal Seek bursts (e.g. video-scrubbing in
// 4-direction probes) without producing the storm S6 fan-out.
const closerWorkerCount = 4

// enqueueCloser hands a reader to the per-file bounded closer pool.
// Lazy-starts the worker goroutines on first call. Caller must hold
// mvf.mu (so the lazy init is safe).
func (mvf *MetadataVirtualFile) enqueueCloser(r io.Closer) {
	// Interrupt first (idempotent) so in-flight downloads release the
	// pool connection immediately before Close() waits on drain.
	if i, ok := r.(readerInterrupter); ok {
		i.Interrupt()
	}
	if mvf.closerCh == nil {
		// Workers range over this local, not mvf.closerCh: Close() nils the
		// field under mvf.mu, which the workers do not hold, so reading the
		// field from inside the goroutine is a data race.
		ch := make(chan io.Closer, closerWorkerCount)
		mvf.closerCh = ch
		for range closerWorkerCount {
			mvf.closeWg.Go(func() {
				for c := range ch {
					_ = c.Close()
				}
			})
		}
	}
	select {
	case mvf.closerCh <- r:
	default:
		// Queue full — apply backpressure inline rather than letting
		// the closer fan-out grow unbounded. This is the rare path; a
		// real Seek burst stays under closerWorkerCount.
		_ = r.Close()
	}
}

// metadataGoneErr returns the terminal error for a handle whose file was removed
// underneath it, tearing the reader down on first observation, or nil while the
// handle is still valid. Doing the teardown here rather than at the point the
// flag is set keeps it on a path that provably holds mvf.mu.
//
// Caller must hold mvf.mu.
func (mvf *MetadataVirtualFile) metadataGoneErr() error {
	if !mvf.metadataGone.Load() {
		return nil
	}
	mvf.closeCurrentReader()
	return &CorruptedFileError{
		TotalExpected: mvf.meta.FileSize,
		UnderlyingErr: ErrMetadataGone,
	}
}

// ensureReader ensures we have a reader initialized for the current position with range support
func (mvf *MetadataVirtualFile) ensureReader() error {
	if err := mvf.metadataGoneErr(); err != nil {
		return err
	}

	if mvf.readerInitialized {
		return nil
	}

	if mvf.poolManager == nil {
		return ErrNoUsenetPool
	}

	// Get request range from args or use default range starting from current position
	start, end := mvf.getRequestRange()

	if end == -1 {
		end = mvf.meta.FileSize - 1
	}

	// When ReadAtContext has advanced the shared cursor past mvf.position (which
	// ReadAt does not move), open the reader at the shared cursor so the next
	// shared-path read picks up where the last one left off.
	if mvf.readAtSharedNext > 0 &&
		mvf.readAtSharedNext < mvf.meta.FileSize &&
		start < mvf.readAtSharedNext &&
		(end < 0 || mvf.readAtSharedNext <= end) {
		start = mvf.readAtSharedNext
	}

	// For multi-clip BD main features, expand the underlying window outward to
	// the BDAV packet grid so the remux rewrites whole packets; we trim back to
	// [start,end] below. `start` here can be unaligned (after Seek / HTTP Range /
	// shared-cursor advance) and `end` can be a client-supplied unaligned range
	// end, so both edges are aligned. For every other file rawStart/rawEnd equal
	// start/end (zero overhead).
	remux := mvf.remuxActive()
	rawStart, rawEnd := start, end
	if remux {
		rawStart = alignStartDown(mvf.clipSpans, start)
		rawEnd = alignEndUp(mvf.clipSpans, end, mvf.meta.FileSize)
	}

	// Create reader for the calculated range using metadata segments
	if len(mvf.meta.NestedSources) > 0 {
		// Nested RAR: use multi-source reader
		reader, err := mvf.createNestedReader(rawStart, rawEnd)
		if err != nil {
			return fmt.Errorf("failed to create nested reader: %w", err)
		}
		mvf.setReader(reader)
	} else if mvf.meta.Encryption != metapb.Encryption_NONE {
		// Wrap the usenet reader with encryption
		decryptedReader, err := mvf.wrapWithEncryption(rawStart, rawEnd)
		if err != nil {
			return fmt.Errorf(ErrMsgFailedWrapEncryption, err)
		}
		mvf.setReader(decryptedReader)
	} else {
		// Create plain usenet reader
		ur, err := mvf.createUsenetReader(mvf.ctx, rawStart, rawEnd)
		if err != nil {
			return err
		}
		mvf.setReader(ur)
	}

	// Apply the continuous-timeline remux for multi-clip BD main features, then
	// trim the packet-aligned window back to exactly [start,end] so file offsets
	// and sizes are unchanged. setReader already pointed the interrupt handle at
	// the inner reader; the wrappers' Close() chain through to it.
	if remux {
		mvf.reader = newTSRemuxReader(mvf.reader, mvf.clipSpans, rawStart)
		mvf.reader = newSkipLimitReader(mvf.reader, start-rawStart, end-start+1)
	}
	mvf.bufOffReader, _ = mvf.reader.(interface{ GetBufferedOffset() int64 })
	mvf.bufAheadReader, _ = mvf.reader.(interface{ BufferedAhead() int64 })

	mvf.readerInitialized = true
	return nil
}

// getRequestRange gets the range for reader creation based on HTTP range or current position
// Implements intelligent range limiting to prevent excessive memory usage when end=-1 or ranges are too large
func (mvf *MetadataVirtualFile) getRequestRange() (start, end int64) {
	// If this is the first read, check for HTTP range header and save original end
	if !mvf.readerInitialized && mvf.originalRangeEnd == 0 {
		// Extract range from context
		if rangeStr, ok := mvf.ctx.Value(utils.RangeKey).(string); ok && rangeStr != "" {
			rangeHeader, err := utils.ParseRangeHeader(rangeStr)
			if err == nil && rangeHeader != nil {
				if rangeHeader.End >= mvf.meta.FileSize {
					rangeHeader.End = mvf.meta.FileSize - 1
				}
				mvf.originalRangeEnd = rangeHeader.End
				return rangeHeader.Start, rangeHeader.End
			}
		}

		// No range header, set unbounded
		mvf.originalRangeEnd = -1
		return mvf.position, -1
	}

	// For subsequent reads, use current position and respect original range
	var targetEnd int64
	if mvf.originalRangeEnd == -1 {
		// Original was unbounded, continue unbounded
		targetEnd = -1
	} else {
		// Original had an end, respect it
		targetEnd = mvf.originalRangeEnd
	}

	return mvf.position, targetEnd
}

// createUsenetReader creates a new usenet reader for the specified range using metadata segments
func (mvf *MetadataVirtualFile) createUsenetReader(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if len(mvf.meta.SegmentData) == 0 {
		return nil, ErrMissmatchedSegments
	}

	// Build segment offset index lazily on first read (thread-safe via sync.Once)
	mvf.segmentIndexOnce.Do(func() {
		mvf.segmentIndex = buildSegmentIndex(mvf.meta.SegmentData)
	})

	// Bound the range by what the segments cover, not by FileSize: an
	// AES-encrypted file's segments extend up to 15 bytes past FileSize (the
	// padded final block), and the decryptor needs them to produce the last
	// plaintext bytes.
	covered := mvf.segmentIndex.totalBytes()
	if start >= covered {
		return nil, io.EOF
	}
	if end >= covered {
		end = covered - 1
	}

	loader := newMetadataSegmentLoader(mvf.meta.SegmentData)

	// segmentIndex is always non-nil here (built by segmentIndexOnce.Do above).
	// Use O(log n) binary search to find segment boundaries, then create a lazy
	// range with O(1) initialization. Corrupt metadata (index returning -1) results
	// in an empty range caught by HasSegments() below.
	startSegIdx := mvf.segmentIndex.findSegmentForOffset(start)
	startFilePos := mvf.segmentIndex.getOffsetForSegment(startSegIdx)
	endSegIdx := mvf.segmentIndex.findSegmentForOffset(end)
	endFilePos := mvf.segmentIndex.getOffsetForSegment(endSegIdx)

	rg := usenet.NewLazySegmentRange(ctx, start, end, loader, startSegIdx, startFilePos, endSegIdx, endFilePos)

	if !rg.HasSegments() {
		var availableBytes int64
		for _, seg := range mvf.meta.SegmentData {
			availableBytes += seg.SegmentSize
		}

		slog.ErrorContext(ctx, "[createUsenetReader] No segments to download",
			"start", start,
			"end", end,
			"available_bytes", availableBytes,
			"expected_file_size", mvf.meta.FileSize,
		)

		mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
			UnderlyingErr: ErrMissmatchedSegments,
			FileOffset:    -1,
		}, true)

		return nil, &CorruptedFileError{
			TotalExpected: mvf.meta.FileSize,
			UnderlyingErr: ErrMissmatchedSegments,
		}
	}

	// Hole hooks enable on-the-fly zero-fill of confirmed-missing segments
	// for eligible video files (nil for everything else — reads fail as
	// always). See holes.go.
	ur, err := usenet.NewUsenetReader(ctx, mvf.poolManager.GetPool, rg, mvf.maxPrefetch, mvf.streamTracker, mvf.streamID, mvf.segmentStore,
		usenet.WithHoleHooks(mvf.holeHooks()),
		usenet.WithArticleDate(mvf.articleDate()),
		usenet.WithSpeculativeBudget(mvf.poolManager.SpeculativeBudget()))
	if err != nil {
		return nil, err
	}

	// Pre-trigger the download pipeline so the first segment starts fetching
	// immediately, rather than waiting for the first Read() call. This overlaps
	// NNTP fetch time with the media player's header parsing.
	ur.Start()

	return ur, nil
}

// createNestedReader creates a reader for files backed by nested RAR sources.
// It maps the requested byte range [start, end] across multiple NestedSegmentSources,
// building a lazy reader that opens each inner-volume reader only when needed.
// This avoids opening all inner volumes simultaneously, which would cause all their
// segments to be prefetched concurrently and spike memory usage.
func (mvf *MetadataVirtualFile) createNestedReader(start, end int64) (io.ReadCloser, error) {
	sources := mvf.meta.NestedSources
	if len(sources) == 0 {
		return nil, fmt.Errorf("no nested sources available")
	}

	// Calculate which sources contain the requested byte range.
	// Sources are concatenated: source 0 covers [0, InnerLength0),
	// source 1 covers [InnerLength0, InnerLength0+InnerLength1), etc.
	var specs []nestedSourceSpec
	var sourceOffset int64

	for _, src := range sources {
		srcEnd := sourceOffset + src.InnerLength - 1

		// Skip sources before our range
		if srcEnd < start {
			sourceOffset += src.InnerLength
			continue
		}

		// Stop if we've passed our range
		if sourceOffset > end {
			break
		}

		// Calculate local offsets within this source
		localStart := int64(0)
		if start > sourceOffset {
			localStart = start - sourceOffset
		}
		localEnd := src.InnerLength - 1
		if end < srcEnd {
			localEnd = end - sourceOffset
		}

		readLen := localEnd - localStart + 1
		if readLen <= 0 {
			sourceOffset += src.InnerLength
			continue
		}

		specs = append(specs, nestedSourceSpec{src: src, localStart: localStart, readLen: readLen})
		sourceOffset += src.InnerLength
	}

	if len(specs) == 0 {
		return nil, fmt.Errorf("no nested sources cover range [%d, %d]", start, end)
	}

	return &lazyNestedMultiReader{mvf: mvf, specs: specs}, nil
}

// createNestedSourceReader creates a reader for a single NestedSegmentSource,
// starting at innerStart within the decrypted inner volume and reading readLen bytes.
func (mvf *MetadataVirtualFile) createNestedSourceReader(
	src *metapb.NestedSegmentSource,
	innerStart int64,
	readLen int64,
) (io.ReadCloser, error) {
	absoluteStart := src.InnerOffset + innerStart

	if len(src.AesKey) > 0 {
		// Encrypted source: decrypt with AES-CBC then read at inner offset
		if mvf.aesCipher == nil {
			return nil, ErrNoCipherConfig
		}

		rh := &utils.RangeHeader{
			Start: absoluteStart,
			End:   absoluteStart + readLen - 1,
		}

		return mvf.aesCipher.Open(
			mvf.ctx,
			rh,
			src.InnerVolumeSize,
			src.AesKey,
			src.AesIv,
			func(ctx context.Context, s, e int64) (io.ReadCloser, error) {
				return mvf.createUsenetReaderFromSegments(ctx, src.Segments, s, e)
			},
		)
	}

	// Unencrypted source: read directly from segments at inner offset
	return mvf.createUsenetReaderFromSegments(mvf.ctx, src.Segments, absoluteStart, absoluteStart+readLen-1)
}

// createUsenetReaderFromSegments creates a usenet reader from a specific set of segments
// (used for nested source reading where segments differ from the main file metadata).
func (mvf *MetadataVirtualFile) createUsenetReaderFromSegments(ctx context.Context, segments []*metapb.SegmentData, start, end int64) (io.ReadCloser, error) {
	if len(segments) == 0 {
		return nil, ErrMissmatchedSegments
	}

	loader := newMetadataSegmentLoader(segments)
	idx := buildSegmentIndex(segments)

	startSegIdx := idx.findSegmentForOffset(start)
	startFilePos := idx.getOffsetForSegment(startSegIdx)
	endSegIdx := idx.findSegmentForOffset(end)
	endFilePos := idx.getOffsetForSegment(endSegIdx)

	rg := usenet.NewLazySegmentRange(ctx, start, end, loader, startSegIdx, startFilePos, endSegIdx, endFilePos)

	if !rg.HasSegments() {
		return nil, fmt.Errorf("no segments cover range [%d, %d]", start, end)
	}

	ur, err := usenet.NewUsenetReader(ctx, mvf.poolManager.GetPool, rg, mvf.maxPrefetch, mvf.streamTracker, mvf.streamID, mvf.segmentStore,
		usenet.WithArticleDate(mvf.articleDate()),
		usenet.WithSpeculativeBudget(mvf.poolManager.SpeculativeBudget()))
	if err != nil {
		return nil, err
	}
	ur.Start()
	return ur, nil
}

// nestedSourceSpec holds the parameters needed to lazily open one inner-volume reader.
type nestedSourceSpec struct {
	src        *metapb.NestedSegmentSource
	localStart int64
	readLen    int64
}

// lazyNestedMultiReader opens inner-volume readers one at a time, only when needed.
// This prevents all inner volumes from being opened simultaneously, which would cause
// all their segments to be prefetched concurrently and spike memory usage.
type lazyNestedMultiReader struct {
	mvf     *MetadataVirtualFile
	specs   []nestedSourceSpec
	idx     int
	current io.ReadCloser
}

func (r *lazyNestedMultiReader) Read(p []byte) (int, error) {
	for {
		if r.current == nil {
			if r.idx >= len(r.specs) {
				return 0, io.EOF
			}
			spec := r.specs[r.idx]
			rc, err := r.mvf.createNestedSourceReader(spec.src, spec.localStart, spec.readLen)
			if err != nil {
				return 0, err
			}
			r.current = rc
			r.idx++
		}

		n, err := r.current.Read(p)
		if err == io.EOF {
			r.current.Close()
			r.current = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (r *lazyNestedMultiReader) Close() error {
	if r.current != nil {
		err := r.current.Close()
		r.current = nil
		return err
	}
	return nil
}

func (r *lazyNestedMultiReader) Interrupt() {
	if i, ok := r.current.(interface{ Interrupt() }); ok {
		i.Interrupt()
	}
}

// wrapWithEncryption wraps a usenet reader with encryption using metadata
func (mvf *MetadataVirtualFile) wrapWithEncryption(start, end int64) (io.ReadCloser, error) {
	if mvf.meta.Encryption == metapb.Encryption_NONE {
		return nil, ErrNoEncryptionParams
	}

	switch mvf.meta.Encryption {
	case metapb.Encryption_RCLONE:
		if mvf.rcloneCipher == nil {
			return nil, ErrNoCipherConfig
		}

		// Get password and salt from metadata, with global fallback
		password := mvf.meta.Password
		if password == "" {
			password = mvf.globalPassword
		}
		salt := mvf.meta.Salt
		if salt == "" {
			salt = mvf.globalSalt
		}

		// Wrap with rclone decryption
		decryptedReader, err := mvf.rcloneCipher.Open(
			mvf.ctx,
			&utils.RangeHeader{Start: start, End: end},
			mvf.meta.FileSize,
			password,
			salt,
			func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
				return mvf.createUsenetReader(ctx, start, end)
			},
		)
		if err != nil {
			return nil, fmt.Errorf(ErrMsgFailedCreateDecryptReader, err)
		}
		return decryptedReader, nil

	case metapb.Encryption_AES:
		// AES encryption for RAR archives
		if mvf.aesCipher == nil {
			return nil, ErrNoCipherConfig
		}
		if len(mvf.meta.AesKey) == 0 {
			return nil, fmt.Errorf("missing AES key in metadata")
		}
		if len(mvf.meta.AesIv) == 0 {
			return nil, fmt.Errorf("missing AES IV in metadata")
		}

		// Wrap with AES decryption - pass key and IV directly
		decryptedReader, err := mvf.aesCipher.Open(
			mvf.ctx,
			&utils.RangeHeader{Start: start, End: end},
			mvf.meta.FileSize,
			mvf.meta.AesKey,
			mvf.meta.AesIv,
			func(ctx context.Context, s, e int64) (io.ReadCloser, error) {
				// Create usenet reader first for encrypted data
				return mvf.createUsenetReader(ctx, s, e)
			},
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES decrypt reader: %w", err)
		}
		return decryptedReader, nil

	default:
		return nil, fmt.Errorf("unsupported encryption type: %v", mvf.meta.Encryption)
	}
}

// updateFileHealthOnError updates both metadata and database health status when corruption is detected.
// Uses synchronous operations with timeout to prevent goroutine leaks.
//
// A streaming-failure repair trigger for the same path is debounced through the
// shared RepairCoalescer so that repeated corrupt reads of one file (or a batch
// of corrupt files) cannot fan out into one DB write + one rclone VFS refresh
// per call. See issue #539 for the failure mode this guards against.
func (mvf *MetadataVirtualFile) updateFileHealthOnError(dataCorruptionErr *usenet.DataCorruptionError, noRetry bool) {
	// Health reporting needs both persistence layers. Handles built without
	// them (nzbdav bridge, tests) have nothing to record — bail rather than
	// nil-deref. This runs on the read path, where a panic would take the whole
	// mount down.
	if mvf.healthRepository == nil || mvf.metadataService == nil || mvf.configGetter == nil {
		return
	}

	// Per-path debounce: short-circuit if this file already triggered a repair
	// inside the debounce window. ShouldTrigger handles a nil coalescer
	// (test harness) by returning true.
	if !mvf.repairCoalescer.ShouldTrigger(mvf.name) {
		// Another handle on this path may already have had the metadata taken
		// away. Only that handle latched itself, so without this a second handle
		// failing inside the debounce window keeps rebuilding readers for a file
		// that is gone - exactly the wedge the latch exists to prevent.
		if mvf.repairCoalescer.WasRemoved(mvf.name) {
			mvf.metadataGone.Store(true)
		}
		slog.DebugContext(mvf.ctx, "Streaming failure repair already triggered recently, debouncing",
			"file", mvf.name)
		return
	}

	// Use a short timeout context to prevent blocking indefinitely
	ctx, cancel := context.WithTimeout(mvf.ctx, 5*time.Second)
	defer cancel()

	cfg := mvf.configGetter()
	healthEnabled := cfg.GetHealthEnabled()

	// Classify playback impact via the hole model — the verdict decides
	// whether this failure triggers a repair at all. Note that with hole
	// hooks wired, within-caps misses are zero-filled and never reach this
	// path; a degraded verdict here is the exception (e.g. hooks disabled).
	classification := mvf.classifyStreamingFailure(dataCorruptionErr)
	isDegraded := healthEnabled && classification != nil &&
		classification.Verdict == holes.VerdictDegraded

	// A 430 mid-stream is not proof the article is gone: one transient answer
	// was enough to hide a healthy file behind FILE_STATUS_CORRUPTED and have
	// the ARR redownload it (issue #749). The reader settles that before the
	// read fails, so this is only the backstop for failures arriving without a
	// verdict — and only a proven-present article stops the repair, since a
	// briefly unhealthy pool must not be able to suppress one.
	//
	// Gated on !isDegraded because it can cost a round trip while the caller
	// holds mvf.mu: a degraded verdict is zero-filled and still playable and
	// takes no destructive action, while everything past this point does —
	// even with health disabled the file is marked corrupted, which hides it
	// from listings and blocks opens.
	//
	// Recording pending rather than dropping the failure hands an
	// intermittently reachable segment to the health worker instead of
	// stalling playback forever with no repair. The debounce token stays
	// spent; the read has already failed, so the next attempt re-triggers.
	if !isDegraded && !mvf.confirmSegmentMissing(ctx, dataCorruptionErr) {
		// With health checking off nothing consumes the pending row. Still the
		// right call — the article is provably there — but say so rather than
		// promise a follow-up that will not happen.
		slog.WarnContext(ctx, "Streaming failure not confirmed on re-check, not condemning the file",
			"file", mvf.name,
			"segment_id", dataCorruptionErr.SegmentID,
			"deferred_to_health_worker", healthEnabled)

		errMsg := dataCorruptionErr.Error()
		sourceNzb := &mvf.meta.SourceNzbPath
		if *sourceNzb == "" {
			sourceNzb = nil
		}
		if err := mvf.healthRepository.UpdateFileHealthScheduled(ctx,
			mvf.name, database.HealthStatusPending, &errMsg, sourceNzb, nil, true, time.Now().UTC(),
		); err != nil {
			slog.WarnContext(ctx, "Failed to schedule health re-check after an unconfirmed streaming failure",
				"file", mvf.name, "error", err)
		}
		return
	}

	// A missing article is PAR2-repairable regardless of eligibility for
	// zero-fill (RAR/AES streams fail here instead of padding). Queue a
	// background repair; the repair queue dedups and the planner enforces
	// caps, so this is safe to fire on every confirmed miss.
	if mvf.repairEnqueuer != nil && usenet.IsArticleNotFound(dataCorruptionErr.UnderlyingErr) {
		mvf.repairEnqueuer.Enqueue(ctx, mvf.name, dataCorruptionErr.SegmentID)
	}

	// Increment failure count for tracking/masking if explicitly enabled with a valid
	// threshold. Masking must be opt-in: Enabled == nil means disabled (not on-by-default),
	// and Threshold <= 0 would make every file immediately masked (count+1 >= 0 is always
	// true), suppressing all repairs. Degraded files are also exempt: masking a still-
	// playable file defeats the point of keeping it available.
	shouldRepair := true
	isMasked := false
	if !isDegraded && cfg.Streaming.FailureMasking.Enabled != nil && *cfg.Streaming.FailureMasking.Enabled && cfg.Streaming.FailureMasking.Threshold > 0 {
		var err error
		isMasked, shouldRepair, err = mvf.healthRepository.IncrementStreamingFailureCount(ctx, mvf.name, cfg.Streaming.FailureMasking.Threshold)
		if err != nil {
			slog.WarnContext(ctx, "Failed to update streaming failure count, proceeding with repair", "file", mvf.name, "error", err)
			shouldRepair = true
		} else if isMasked && !shouldRepair {
			slog.InfoContext(ctx, "File masked due to streaming failure (threshold not reached)", "file", mvf.name)
		}
	}

	// Any file with missing segments or corruption is marked as corrupted in metadata
	// and DB so it stays visible on the Health page, regardless of whether repair runs.
	// Degraded files instead keep a status that leaves them visible AND streamable
	// (FILE_STATUS_CORRUPTED hides the file from listings and blocks opens).
	// Masked files keep their healthy metadata status so they remain openable.
	metadataStatus := metapb.FileStatus_FILE_STATUS_CORRUPTED
	if isDegraded {
		metadataStatus = metapb.FileStatus_FILE_STATUS_DEGRADED
	} else if isMasked && !shouldRepair {
		metadataStatus = metapb.FileStatus_FILE_STATUS_HEALTHY
	}

	// Update metadata status (blocking with timeout)
	if err := mvf.metadataService.UpdateFileStatus(mvf.name, metadataStatus); err != nil {
		slog.WarnContext(ctx, "Failed to update metadata status", "file", mvf.name, "error", err)
	}

	// Update database health tracking (blocking with timeout)
	errorMsg := dataCorruptionErr.Error()
	sourceNzbPath := &mvf.meta.SourceNzbPath
	if *sourceNzbPath == "" {
		sourceNzbPath = nil
	}

	details := database.HealthErrorDetails{
		ErrorType:       "ArticleNotFound",
		MissingArticles: 1,
		TotalArticles:   len(mvf.meta.SegmentData),
		PlaybackImpact:  classification,
	}
	errorDetails := details.Marshal()

	// The repair action (repair_triggered status + metadata safety-folder move + Arr
	// redownload) only runs when the health system is enabled. When it is disabled we
	// record the corruption for visibility but take no repair action whatsoever.
	var dbStatus database.HealthStatus
	scheduleImmediately := false
	if isDegraded {
		// Playable with glitches: record as degraded, no repair trigger, no
		// safety-folder move, no immediate scheduling — the health worker
		// re-checks it on its normal schedule.
		slog.InfoContext(ctx, "Streaming failure classified as degraded (still playable), skipping repair",
			"file", mvf.name,
			"total_missing", classification.TotalMissing,
			"longest_run", classification.LongestRun)
		dbStatus = database.HealthStatusDegraded
	} else if healthEnabled && cfg.GetHealthDeleteOnCorruption() {
		// Delete-on-corruption is enabled: remove the file instead of triggering a repair.
		slog.WarnContext(ctx, "Streaming failure detected, deleting corrupted file instead of triggering repair", "file", mvf.name)

		var physicalPath, rootPath string
		if health, err := mvf.healthRepository.GetFileHealth(ctx, mvf.name); err == nil && health != nil && health.LibraryPath != nil {
			physicalPath = *health.LibraryPath
			rootPath = cfg.MountPath
			if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
				rootPath = *cfg.Health.LibraryDir
			}
		}

		if err := mvf.metadataService.DeleteCorruptedFile(ctx, mvf.name, physicalPath, rootPath); err != nil {
			slog.ErrorContext(ctx, "Failed to delete corrupted file after streaming failure", "file", mvf.name, "error", err)
		} else {
			// The file this handle is reading no longer exists; latch it closed,
			// same as the repair path below, and publish the removal so other
			// handles on this path latch too even if their failure is debounced.
			mvf.metadataGone.Store(true)
			mvf.repairCoalescer.MarkRemoved(mvf.name)

			if err := mvf.healthRepository.DeleteHealthRecord(ctx, mvf.name); err != nil {
				slog.ErrorContext(ctx, "Failed to delete health record after deleting corrupted file", "file", mvf.name, "error", err)
			}
		}
		return
	} else if healthEnabled && shouldRepair {
		// Mark as repair_triggered with high priority to trigger the replacement immediately.
		// We skip the re-verification phase because a streaming failure is a definitive indicator of corruption.
		slog.InfoContext(ctx, "Streaming failure detected, triggering immediate ARR repair", "file", mvf.name)
		dbStatus = database.HealthStatusRepairTriggered
		// noRetry signals a definitive corruption (e.g. article-not-found); force immediate
		// pick-up by the HealthWorker so the replacement is scheduled without re-verification.
		scheduleImmediately = noRetry

		// If the file has already been imported (has a library path), move metadata to the safety folder
		// so that the ARR rescan definitively sees the file as missing and triggers a redownload.
		if health, err := mvf.healthRepository.GetFileHealth(ctx, mvf.name); err == nil && health != nil {
			if health.LibraryPath != nil && *health.LibraryPath != "" {
				relativePath := strings.TrimPrefix(mvf.name, cfg.MountPath)
				relativePath = strings.TrimPrefix(relativePath, "/")
				slog.InfoContext(ctx, "Moving metadata file for corrupted item to safety folder to trigger replacement", "file_path", mvf.name)
				if moveErr := mvf.metadataService.MoveToCorrupted(ctx, relativePath); moveErr == nil {
					// This handle is streaming the file that was just moved away, so
					// latch it closed. Without this its prefetch pipeline keeps
					// fetching segments for a path that no longer exists and the
					// client keeps reading against it (see issue #539).
					mvf.metadataGone.Store(true)
					// Publish the removal so other handles on this path latch
					// themselves even if their own failure lands inside the
					// debounce window and returns early above.
					mvf.repairCoalescer.MarkRemoved(mvf.name)
					// Successfully moved metadata, enqueue a coalesced rclone VFS
					// refresh. Multiple files in the same directory collapse into a
					// single RC call; concurrent failures across directories are
					// batched into one call as well. EnqueueRefresh is a no-op on a
					// nil coalescer (test harness). The directory is a virtual path
					// and rclone's VFS is forward-slash on every platform, so
					// filepath.Dir would enqueue "\dir" (or a bare "\" for a file at
					// the mount root) on Windows and match nothing.
					mvf.repairCoalescer.EnqueueRefresh(path.Dir(rclonecli.ToVFSPath(mvf.name)))
				} else {
					slog.WarnContext(ctx, "Failed to move corrupted metadata file, proceeding with repair trigger status", "error", moveErr)
				}
			}
		}
	} else {
		// Health system disabled or failure count has not reached the threshold:
		// record the corruption for visibility only and skip every repair action.
		if healthEnabled && !shouldRepair {
			slog.InfoContext(ctx, "Streaming failure detected but masked (threshold not reached), skipping repair", "file", mvf.name)
			dbStatus = database.HealthStatusPending // Re-schedule it for verification later
		} else {
			slog.InfoContext(ctx, "Streaming failure detected but health system disabled, marking corrupted without triggering repair", "file", mvf.name)
			dbStatus = database.HealthStatusCorrupted
		}
	}

	// Update database health tracking. scheduleImmediately (noRetry) forces immediate
	if err := mvf.healthRepository.UpdateFileHealthScheduled(ctx,
		mvf.name,
		dbStatus,
		&errorMsg,
		sourceNzbPath,
		errorDetails,
		scheduleImmediately,
		time.Now().UTC(),
	); err != nil {
		slog.WarnContext(ctx, "Failed to update health database for streaming failure", "file", mvf.name, "error", err)
	}
}

// confirmSegmentMissing re-checks a segment a stream read reported as missing,
// returning false only when the article is provably still there. Anything else
// returns true so the caller proceeds exactly as it did before.
func (mvf *MetadataVirtualFile) confirmSegmentMissing(ctx context.Context, dcErr *usenet.DataCorruptionError) bool {
	if !usenet.IsArticleNotFound(dcErr.UnderlyingErr) {
		return true
	}

	// The reader already asked; asking again costs a round trip under mvf.mu
	// for an answer we have. Every settled verdict means the miss stands,
	// including present-but-unfetchable — re-checking that one forever would
	// leave the file unplayable and unrepaired.
	if dcErr.MissVerdict != usenet.MissUnverified {
		return true
	}

	if dcErr.SegmentID == "" || mvf.poolManager == nil {
		return true
	}

	usenetPool, err := mvf.poolManager.GetPool()
	if err != nil || usenetPool == nil {
		return true
	}

	statCtx, cancel := context.WithTimeout(ctx, usenet.MissRecheckTimeout)
	defer cancel()

	// Priority lane: a playback read is blocked behind this, so it must not
	// spend its budget queued behind a large BODY on a busy connection.
	if _, err := usenetPool.Exists(statCtx, nntppool.Req{
		MessageID:   dcErr.SegmentID,
		Lane:        nntppool.LanePriority,
		ArticleDate: mvf.articleDate(),
	}); err != nil {
		return true
	}
	return false
}

// readFullContext reads exactly len(buf) bytes from r, but returns early
// if ctx is cancelled. This prevents io.ReadFull from blocking indefinitely
// when the underlying reader is stuck (e.g., waiting for network data).
//
// On cancellation the reader is force-closed to unblock io.ReadFull, and the
// goroutine is drained (with a 5 s safety timeout) so that it releases its
// reference to buf before the function returns.
func readFullContext(ctx context.Context, r io.Reader, buf []byte) (int, error) {
	type result struct {
		n   int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		n, err := io.ReadFull(r, buf)
		ch <- result{n, err}
	}()
	select {
	case res := <-ch:
		return res.n, res.err
	case <-ctx.Done():
		// Force-close the reader to unblock io.ReadFull.
		// UsenetReader.Close() is idempotent (closeOnce), so the
		// caller's defer reader.Close() becomes a safe no-op.
		if c, ok := r.(io.Closer); ok {
			c.Close()
		}
		// Drain the goroutine so it releases buf and the reader reference.
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
		}
		return 0, ctx.Err()
	}
}

// isValidEmptyDirectory checks if a path could represent a valid empty directory
// by examining parent directories and path structure
func (mrf *MetadataRemoteFile) isValidEmptyDirectory(normalizedPath string) bool {
	// Root directory is always valid
	if normalizedPath == RootPath {
		return true
	}

	// Get parent directory
	parentDir := filepath.Dir(normalizedPath)
	if parentDir == "." {
		parentDir = RootPath
	}

	// Check if parent directory exists (either physically or as a valid empty directory)
	if mrf.metadataService.DirectoryExists(parentDir) {
		return true
	}

	// Recursively check if parent could be a valid empty directory
	return mrf.isValidEmptyDirectory(parentDir)
}

func (mrf *MetadataRemoteFile) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return mrf.metadataService.CreateDirectory(name)
}

func (mrf *MetadataRemoteFile) MkdirAll(ctx context.Context, name string, perm os.FileMode) error {
	return mrf.metadataService.CreateDirectory(name)
}

// resolveIDPath resolves a sharded ID path (.ids/...) to the actual virtual path
func (mrf *MetadataRemoteFile) resolveIDPath(idPath string) (string, error) {
	cfg := mrf.configGetter()
	metadataRoot := cfg.Metadata.RootPath

	// The idPath is like .ids/4/0/e/9/a/40e9a6c9-e922-4217-ab6c-9d2207528a78
	// The metadata file is at metadataRoot/.ids/4/0/e/9/a/40e9a6c9-e922-4217-ab6c-9d2207528a78.meta

	// Ensure it has .meta extension for the check
	fullIdPath := filepath.Join(metadataRoot, idPath+".meta")

	// Read the symlink
	target, err := os.Readlink(fullIdPath)
	if err != nil {
		return "", err
	}

	// The target is relative to the directory of the symlink
	// e.g. ../../../../../movies/Spider-Man.../Spider-Man....meta

	// Calculate the absolute path of the target metadata file
	absTarget := filepath.Join(filepath.Dir(fullIdPath), target)

	// Calculate the relative path from metadataRoot to get the virtual path
	relPath, err := filepath.Rel(metadataRoot, absTarget)
	if err != nil {
		return "", err
	}

	// Remove .meta extension to get the virtual filename
	virtualPath := strings.TrimSuffix(relPath, ".meta")

	return virtualPath, nil
}
