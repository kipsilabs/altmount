package metadata

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
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/utils"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	// defaultMetadataCacheSize is the max number of file metadata entries to cache.
	defaultMetadataCacheSize = 4096
)

// ErrDirectoryNotEmpty reports that DeleteDirectoryIfEmpty left a directory in place
// because it still held entries.
var ErrDirectoryNotEmpty = errors.New("metadata directory not empty")

// metaMagicV3 is a 5-byte magic prefix prepended to v3 .meta files.
// The leading 0x00 byte is an invalid proto tag byte, so v1 files (raw proto,
// no magic) are safely distinguished from v3 files.
var metaMagicV3 = []byte{0x00, 'A', 'M', '3', 0x01}

// isV3Meta reports whether data starts with the v3 magic prefix.
func isV3Meta(data []byte) bool {
	return len(data) >= len(metaMagicV3) &&
		data[0] == metaMagicV3[0] &&
		data[1] == metaMagicV3[1] &&
		data[2] == metaMagicV3[2] &&
		data[3] == metaMagicV3[3] &&
		data[4] == metaMagicV3[4]
}

// FileMetadataLite holds the minimal metadata needed for directory listings.
// This avoids keeping full FileMetadata protos (with SegmentData, Par2Files, etc.)
// in memory just for Readdir.
type FileMetadataLite struct {
	FileSize   int64
	ModifiedAt int64
	Status     metapb.FileStatus
}

// FileObserver is told about metadata files appearing and disappearing. It is
// optional (see SetFileObserver) and purely advisory: implementations must not
// block for long and must handle their own errors, because the metadata
// operation has already succeeded when they are called. Paths are normalized
// virtual paths without a leading slash.
type FileObserver interface {
	FileWritten(ctx context.Context, virtualPath string)
	FileDeleted(ctx context.Context, virtualPath string)
	DirectoryDeleted(ctx context.Context, virtualPath string)
	// DirectoryRenamed reports a whole metadata directory moving; the files
	// keep their content, only their paths change.
	DirectoryRenamed(ctx context.Context, oldVirtualPath, newVirtualPath string)
}

// MetadataService provides low-level read/write operations for metadata files.
//
// The full FileMetadata proto is never cached: callers that need segments
// (Open, HealthChecker) re-read from disk each time, so the proto lives only
// for the duration of the open handle or the health check. Two caches hold
// state between calls, and both are bounded:
//
//   - liteCache, a lightweight projection (size, modtime, status) at
//     ~liteCache_entries × 40 bytes.
//   - store's decoded NzbStore cache, which v3 metadata resolves segments
//     against. Stores are shared per release and are the one place message-ID
//     strings do survive a call, so that cache is bounded by estimated bytes
//     (defaultStoreCacheBytes) rather than by entry count — bounding it by
//     entry count left steady-state retention unbounded (issue #819).
type MetadataService struct {
	rootPath string
	// liteCache caches lightweight metadata (size, modtime, status) used by
	// Readdir/Stat/Getattr, and populated as a side effect of ReadFileMetadata
	// so info-only callers still benefit.
	liteCache *lru.Cache[string, *FileMetadataLite]
	// store manages shared per-release NzbStore files used by v3 metadata.
	store *StoreService
	// storeRefCounter tracks reference counts for shared NzbStore files.
	// nil means reference counting is disabled.
	storeRefCounter StoreRefCounter
	// observer, when set, is notified after metadata files are written,
	// renamed or deleted. nil disables notifications. Held atomically so it
	// can be set and cleared while the service is in use.
	observer atomic.Pointer[observerBox]
}

// observerBox lets an interface value live in an atomic.Pointer.
type observerBox struct{ o FileObserver }

// fileObserver returns the current observer, or nil.
func (ms *MetadataService) fileObserver() FileObserver {
	if b := ms.observer.Load(); b != nil {
		return b.o
	}
	return nil
}

// NewMetadataService creates a new metadata service
func NewMetadataService(rootPath string) *MetadataService {
	liteCache, _ := lru.New[string, *FileMetadataLite](defaultMetadataCacheSize)
	return &MetadataService{
		rootPath:  rootPath,
		liteCache: liteCache,
		store:     NewStoreService(rootPath),
	}
}

// Store returns the StoreService used by this MetadataService.
func (ms *MetadataService) Store() *StoreService {
	return ms.store
}

// SetStoreRefCounter wires in a StoreRefCounter so that reference counts on shared
// NzbStore files are maintained when metadata is deleted or created.
func (ms *MetadataService) SetStoreRefCounter(c StoreRefCounter) {
	ms.storeRefCounter = c
}

// SetFileObserver wires in an optional FileObserver. It is safe to call while
// the service is in use. Passing nil removes the observer.
func (ms *MetadataService) SetFileObserver(o FileObserver) {
	if o == nil {
		ms.observer.Store(nil)
		return
	}
	ms.observer.Store(&observerBox{o: o})
}

// IncStoreRef increments the reference count for a v3 store file.
// No-op when no StoreRefCounter is configured.
// Refcounting is best-effort: a DB failure is logged but does not fail the caller.
// A missed increment means a later DecStoreRef may drive the count to 0 prematurely,
// but store-file deletion is tolerant of the file already being absent.
func (ms *MetadataService) IncStoreRef(ctx context.Context, storePath string) {
	if ms.storeRefCounter == nil {
		return
	}
	if err := ms.storeRefCounter.IncStoreRef(ctx, storePath); err != nil {
		slog.WarnContext(ctx, "failed to increment store ref count",
			"store_path", storePath, "error", err)
	}
}

// readStoreRef reads just the StoreRef field from a .meta file without resolving segments.
// Returns "" if the file is not v3 or cannot be read.
func (ms *MetadataService) readStoreRef(metaFilePath string) string {
	data, err := os.ReadFile(metaFilePath)
	if err != nil {
		return ""
	}
	if !isV3Meta(data) {
		return ""
	}
	payload := data[len(metaMagicV3):]
	var fm metapb.FileMetadata
	if err := proto.Unmarshal(payload, &fm); err != nil {
		return ""
	}
	return fm.StoreRef
}

// normalizeVirtualPath canonicalizes a virtual path so every caller ends up
// with exactly one on-disk name and one liteCache key for the same file.
// A backslash is a legal Linux filename byte but deadlocks the FUSE
// page-cache layer on open() (issue #660), so it's folded to forward slash;
// path.Clean (not filepath.Clean, which would turn it back into a backslash
// on Windows) then collapses "//" and "..", and the leading slash is
// stripped so "/x" and "x" collapse to one key.
func normalizeVirtualPath(virtualPath string) string {
	v := strings.ReplaceAll(virtualPath, "\\", "/")
	v = path.Clean(v)
	v = strings.TrimPrefix(v, "/")
	if v == "." {
		v = ""
	}
	return v
}

// truncateFilename truncates the base so base+ext+".meta" fits within the
// 255-byte name-component limit most filesystems enforce.
func (ms *MetadataService) truncateFilename(filename string) string {
	const maxNameLen = 255
	const metaSuffixLen = len(".meta")

	fileExt := filepath.Ext(filename)
	base := strings.TrimSuffix(filename, fileExt)

	maxBaseLen := max(maxNameLen-metaSuffixLen-len(fileExt), 0)

	if len(base) <= maxBaseLen {
		return base + fileExt
	}

	return base[:maxBaseLen] + fileExt
}

// metaFilePath is the single chokepoint for on-disk .meta file path
// construction: every caller gets the same normalized, truncated path.
func (ms *MetadataService) metaFilePath(virtualPath string) string {
	v := normalizeVirtualPath(virtualPath)
	dir := filepath.Join(ms.rootPath, filepath.Dir(v))
	name := ms.truncateFilename(filepath.Base(v))
	return filepath.Join(dir, name+".meta")
}

// metaDirPath is the single chokepoint for on-disk directory path
// construction.
func (ms *MetadataService) metaDirPath(virtualPath string) string {
	return filepath.Join(ms.rootPath, normalizeVirtualPath(virtualPath))
}

// WriteFileMetadata writes file metadata to disk and tells the file observer
// (if any) that the file was written.
func (ms *MetadataService) WriteFileMetadata(virtualPath string, metadata *metapb.FileMetadata) error {
	if err := ms.writeFileMetadata(virtualPath, metadata); err != nil {
		return err
	}
	if obs := ms.fileObserver(); obs != nil {
		obs.FileWritten(context.Background(), normalizeVirtualPath(virtualPath))
	}
	return nil
}

// writeFileMetadata is WriteFileMetadata without the observer notification,
// for callers that notify themselves (WriteFileMetadataAuto) or that cannot
// change a file's identity (UpdateFileMetadata).
func (ms *MetadataService) writeFileMetadata(virtualPath string, metadata *metapb.FileMetadata) error {
	virtualPath = normalizeVirtualPath(virtualPath)

	metadataPath := ms.metaFilePath(virtualPath)
	metadataDir := filepath.Dir(metadataPath)
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Sidecar ID handling for compatibility
	// We don't write NzbdavId to the proto to maintain compatibility with versions that don't have field 14.
	// Instead, we store it in a sidecar .id file.
	nzbdavId := metadata.NzbdavId
	metadata.NzbdavId = "" // Clear for marshalling

	// Marshal protobuf data — v3 when StoreRef is set, v1 otherwise.
	var writeData []byte
	if metadata.StoreRef != "" {
		// v3: clear inline segment fields (they live in the store), write magic + structural proto.
		// SharedOuterSources dedup is dissolved: each NestedSource carries inline SegmentRefs,
		// so the read path does not need to expand shared entries.
		structural := proto.Clone(metadata).(*metapb.FileMetadata)
		structural.SegmentData = nil
		structural.SharedOuterSources = nil // v3: dedup dissolved
		for _, p := range structural.Par2Files {
			p.SegmentData = nil
		}
		for _, ns := range structural.NestedSources {
			ns.Segments = nil
			ns.SharedOuterSourceIndex = 0 // v3: each NestedSource is self-contained
		}
		raw, err := proto.Marshal(structural)
		if err != nil {
			metadata.NzbdavId = nzbdavId
			return fmt.Errorf("failed to marshal v3 metadata: %w", err)
		}
		writeData = append(metaMagicV3, raw...)
	} else {
		// v1: marshal as-is (existing behavior).
		raw, err := proto.Marshal(metadata)
		if err != nil {
			metadata.NzbdavId = nzbdavId // Restore on error
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
		writeData = raw
	}

	// Write atomically via a uniquely-named temp file so concurrent writes
	// don't race on the same name. Not built from the truncated filename:
	// that would risk exceeding the name-length limit truncation exists for.
	tmpFile, err := os.CreateTemp(metadataDir, ".meta-*.tmp")
	if err != nil {
		metadata.NzbdavId = nzbdavId
		return fmt.Errorf("failed to create temporary metadata file: %w", err)
	}
	tmpPath := tmpFile.Name()
	if _, writeErr := tmpFile.Write(writeData); writeErr != nil {
		tmpFile.Close()
		_ = os.Remove(tmpPath)
		metadata.NzbdavId = nzbdavId
		return fmt.Errorf("failed to write temporary metadata file: %w", writeErr)
	}
	if closeErr := tmpFile.Close(); closeErr != nil {
		_ = os.Remove(tmpPath)
		metadata.NzbdavId = nzbdavId
		return fmt.Errorf("failed to close temporary metadata file: %w", closeErr)
	}

	if err := os.Rename(tmpPath, metadataPath); err != nil {
		_ = os.Remove(tmpPath)
		metadata.NzbdavId = nzbdavId
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	// Stabilise the .meta disk mtime to match ModifiedAt in the proto.
	// os.Rename always sets mtime=now on the destination file; without this
	// Chtimes call any host tool reading the ext4 mtime (rather than the WebDAV
	// Last-Modified we return from the proto) would see a freshly-updated
	// timestamp on every health-check rewrite, potentially confusing rclone after
	// a VFS cache flush or attr-timeout expiry.
	if metadata.ModifiedAt != 0 {
		t := time.Unix(metadata.ModifiedAt, 0)
		if err := os.Chtimes(metadataPath, t, t); err != nil {
			// Not fatal: the proto is written and the WebDAV/FUSE mtime we
			// report comes from ModifiedAt, not from the host filesystem.
			slog.DebugContext(context.Background(), "Failed to pin metadata file mtime",
				"path", metadataPath, "error", err)
		}
	}

	metadata.NzbdavId = nzbdavId // Restore for in-memory use

	// Update only the lightweight cache; the full proto (with SegmentData) is
	// never cached to avoid long-term retention of segment strings.
	ms.liteCache.Add(virtualPath, &FileMetadataLite{
		FileSize:   metadata.FileSize,
		ModifiedAt: metadata.ModifiedAt,
		Status:     metadata.Status,
	})

	return nil
}

// WriteFileMetadataV3 writes metadata directly in the v3 store-backed format: it
// converts the inline SegmentData of the main file, PAR2 files, and nested sources
// into SegmentRefs/SegmentRuns against the release's flat segment index, points the
// meta at storeRef, and increments the store reference count exactly once.
//
// The conversion runs on a clone so a failure (e.g. a segment id missing from the
// index) leaves the caller's in-memory meta untouched, letting the caller fall back
// to a v1 WriteFileMetadata. Freshly-built archive metas still carry the
// SharedOuterSources dedup, so it is expanded first (mirroring the read path) before
// each nested source is converted independently and the dedup dissolved.
func (ms *MetadataService) WriteFileMetadataV3(ctx context.Context, virtualPath string, metadata *metapb.FileMetadata, index map[string]int64, storeRef string) error {
	m := proto.Clone(metadata).(*metapb.FileMetadata)

	if err := ExpandSharedOuterSources(m); err != nil {
		return fmt.Errorf("expand shared outer sources: %w", err)
	}

	m.StoreRef = storeRef
	// The raw .nzb is deleted after import; the .nzbz store is the persistent source
	// of truth (NZBs are regenerated from it). Point source_nzb_path at the store so a
	// single, real artifact is recorded instead of a path to a now-deleted .nzb file.
	m.SourceNzbPath = storeRef

	mainRefs, err := segDataToRefs(m.SegmentData, index)
	if err != nil {
		return fmt.Errorf("main segments: %w", err)
	}
	m.SegmentRuns, m.SegmentRefs = splitRefs(mainRefs)

	for _, p := range m.Par2Files {
		refs, err := segDataToRefs(p.SegmentData, index)
		if err != nil {
			return fmt.Errorf("par2 segments: %w", err)
		}
		p.SegmentRuns, p.SegmentRefs = splitRefs(refs)
	}

	// Nested sources are archive-sliced (non-trivial offsets): explicit refs only.
	for _, ns := range m.NestedSources {
		if ns.SegmentRefs, err = segDataToRefs(ns.Segments, index); err != nil {
			return fmt.Errorf("nested source segments: %w", err)
		}
		ns.SharedOuterSourceIndex = 0
	}
	m.SharedOuterSources = nil

	// WriteFileMetadata's v3 branch (StoreRef set) clears inline SegmentData/
	// SharedOuterSources/NestedSource.Segments and keeps SegmentRefs/SegmentRuns.
	if err := ms.writeFileMetadata(virtualPath, m); err != nil {
		return err
	}
	ms.IncStoreRef(ctx, storeRef)
	return nil
}

// WriteFileMetadataAuto writes v3 store-backed metadata when storeRef is set,
// falling back to the v1 inline format if the v3 conversion fails (so a store/index
// problem on one file never blocks the import). With an empty storeRef it writes v1.
// This is the single entry point import processors should use.
func (ms *MetadataService) WriteFileMetadataAuto(ctx context.Context, virtualPath string, metadata *metapb.FileMetadata, index map[string]int64, storeRef string) error {
	write := func() error {
		if storeRef == "" {
			return ms.writeFileMetadata(virtualPath, metadata)
		}
		if err := ms.WriteFileMetadataV3(ctx, virtualPath, metadata, index, storeRef); err != nil {
			slog.WarnContext(ctx, "v3 metadata write failed; writing v1",
				"path", virtualPath, "error", err)
			return ms.writeFileMetadata(virtualPath, metadata)
		}
		return nil
	}

	if err := write(); err != nil {
		return err
	}

	ms.removeCorruptedCopy(ctx, virtualPath)
	if obs := ms.fileObserver(); obs != nil {
		obs.FileWritten(ctx, normalizeVirtualPath(virtualPath))
	}
	return nil
}

// ReadFileMetadata reads file metadata from disk. The full proto (including
// SegmentData and NestedSources) is returned to the caller but NOT cached —
// those slices dominate heap usage and must not be retained beyond the
// caller's handle. As a side effect, the lightweight projection is cached so
// subsequent Readdir/Stat calls are fast without a disk read.
func (ms *MetadataService) ReadFileMetadata(virtualPath string) (*metapb.FileMetadata, error) {
	virtualPath = normalizeVirtualPath(virtualPath)
	metadataPath := ms.metaFilePath(virtualPath)

	// Read file
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // File not found
		}
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Unmarshal protobuf data — v3 detection and ref resolution.
	metadata := &metapb.FileMetadata{}
	if isV3Meta(data) {
		if err := proto.Unmarshal(data[len(metaMagicV3):], metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
		if metadata.StoreRef != "" {
			store, err := ms.store.ReadStore(metadata.StoreRef)
			if err != nil {
				return nil, fmt.Errorf("failed to read store %q: %w", metadata.StoreRef, err)
			}
			flat := FlatSegments(store)
			var resolveErr error
			if metadata.SegmentData, resolveErr = resolveSegments(flat, metadata.SegmentRuns, metadata.SegmentRefs); resolveErr != nil {
				return nil, resolveErr
			}
			for _, p := range metadata.Par2Files {
				if p.SegmentData, resolveErr = resolveSegments(flat, p.SegmentRuns, p.SegmentRefs); resolveErr != nil {
					return nil, resolveErr
				}
			}
			for _, ns := range metadata.NestedSources {
				if ns.Segments, resolveErr = resolveRefs(flat, ns.SegmentRefs); resolveErr != nil {
					return nil, resolveErr
				}
			}
		}
	} else {
		if err := proto.Unmarshal(data, metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	// Resolve shared_outer_source_index references on nested sources.
	// Files imported with the dedupe writer store outer segments once at
	// the FileMetadata level; we re-populate per-source slice headers
	// here so the rest of the read path is unaware of the difference.
	if err := ExpandSharedOuterSources(metadata); err != nil {
		return nil, fmt.Errorf("failed to expand shared outer sources: %w", err)
	}

	// Read ID from sidecar file (compatibility mode)
	idPath := metadataPath + ".id"
	if idData, err := os.ReadFile(idPath); err == nil {
		metadata.NzbdavId = string(idData)
	}

	// Populate only the lightweight cache — the full proto is never cached.
	ms.liteCache.Add(virtualPath, &FileMetadataLite{
		FileSize:   metadata.FileSize,
		ModifiedAt: metadata.ModifiedAt,
		Status:     metadata.Status,
	})

	return metadata, nil
}

// liteScanBytes is how much of a .meta file we read up front when serving a
// directory listing. The lite fields (file_size=1, status=3, modified_at=5)
// are all varints near the start of the proto; the only intervening field
// that can be large is source_nzb_path=2 (a string). 4 KiB is comfortable
// headroom — virtually every real-world .meta has all three within the first
// ~200 bytes. Avoids reading and unmarshalling the full proto (which can be
// MBs for files with many NestedSources or SegmentData entries — the exact
// pattern that caused a 7.94 GB allocation spike during FileBrowser
// recursive PROPFIND walks).
const liteScanBytes = 4096

// ReadFileMetadataLite reads only the lightweight fields (size, modtime, status)
// needed for directory listings. On cache miss it reads at most liteScanBytes
// from the .meta file and scans the proto wire format for the three lite
// fields, never instantiating the full FileMetadata proto or its
// NestedSources/SegmentData slices. Falls back to a full read in the rare
// case the partial buffer doesn't cover the lite fields.
func (ms *MetadataService) ReadFileMetadataLite(virtualPath string) (*FileMetadataLite, error) {
	virtualPath = normalizeVirtualPath(virtualPath)

	// Check lite cache first
	if cached, ok := ms.liteCache.Get(virtualPath); ok {
		return cached, nil
	}

	// Cache miss — read the head of the file and scan wire-format fields.
	metadataPath := ms.metaFilePath(virtualPath)

	f, err := os.Open(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer f.Close()

	buf := make([]byte, liteScanBytes)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, fmt.Errorf("failed to read metadata head: %w", err)
	}
	buf = buf[:n]

	// Skip v3 magic if present so parseLiteFields sees clean proto wire bytes.
	if isV3Meta(buf) {
		buf = buf[len(metaMagicV3):]
	}

	lite, ok := parseLiteFields(buf)
	if !ok {
		// Lite fields not located within liteScanBytes (extreme/unusual
		// source_nzb_path length, future schema reordering, etc). Fall back
		// to the full read so the listing is correct even at the cost of
		// transient allocation.
		return ms.readFileMetadataLiteFull(virtualPath)
	}
	ms.liteCache.Add(virtualPath, lite)
	return lite, nil
}

// parseLiteFields walks proto wire format inside buf and extracts the lite
// fields without allocating a full FileMetadata struct. Returns (lite, true)
// once both file_size (field 1) and status (field 3) are seen — modified_at
// (field 5) is best-effort within the same buffer. Returns (nil, false) if
// the buffer is exhausted without the required fields, signalling the
// caller to fall back to a full read.
//
// Field numbers must match metadata.proto. Tested via TestReadFileMetadataLite_*
// in service_test.go.
func parseLiteFields(buf []byte) (*FileMetadataLite, bool) {
	var lite FileMetadataLite
	var sawFileSize, sawStatus bool
	for len(buf) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(buf)
		if tagLen < 0 {
			return nil, false
		}
		buf = buf[tagLen:]
		switch num {
		case 1: // file_size int64 (varint)
			v, l := protowire.ConsumeVarint(buf)
			if l < 0 {
				return nil, false
			}
			lite.FileSize = int64(v)
			sawFileSize = true
			buf = buf[l:]
		case 3: // status FileStatus (varint enum)
			v, l := protowire.ConsumeVarint(buf)
			if l < 0 {
				return nil, false
			}
			lite.Status = metapb.FileStatus(v)
			sawStatus = true
			buf = buf[l:]
		case 5: // modified_at int64 (varint)
			v, l := protowire.ConsumeVarint(buf)
			if l < 0 {
				return nil, false
			}
			lite.ModifiedAt = int64(v)
			buf = buf[l:]
		default:
			l := protowire.ConsumeFieldValue(num, typ, buf)
			if l < 0 {
				return nil, false
			}
			buf = buf[l:]
		}
		// Early exit once required fields are captured. modified_at is
		// best-effort within the partial buffer; if it sits past
		// liteScanBytes it stays zero and the listing still renders.
		if sawFileSize && sawStatus && lite.ModifiedAt != 0 {
			return &lite, true
		}
	}
	if sawFileSize && sawStatus {
		return &lite, true
	}
	return nil, false
}

// readFileMetadataLiteFull is the legacy slow path: read the entire .meta
// file and unmarshal the full proto. Only used as a fallback when the
// partial-read scan in ReadFileMetadataLite fails to locate the lite
// fields within liteScanBytes.
func (ms *MetadataService) readFileMetadataLiteFull(virtualPath string) (*FileMetadataLite, error) {
	virtualPath = normalizeVirtualPath(virtualPath)
	metadataPath := ms.metaFilePath(virtualPath)

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	if isV3Meta(data) {
		data = data[len(metaMagicV3):]
	}

	metadata := &metapb.FileMetadata{}
	if err := proto.Unmarshal(data, metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	lite := &FileMetadataLite{
		FileSize:   metadata.FileSize,
		ModifiedAt: metadata.ModifiedAt,
		Status:     metadata.Status,
	}
	ms.liteCache.Add(virtualPath, lite)
	return lite, nil
}

// FileExists checks if a metadata file exists for the given virtual path
func (ms *MetadataService) FileExists(virtualPath string) bool {
	_, err := os.Stat(ms.metaFilePath(virtualPath))
	return err == nil
}

// DirectoryExists checks if a metadata directory exists
func (ms *MetadataService) DirectoryExists(virtualPath string) bool {
	info, err := os.Stat(ms.metaDirPath(virtualPath))
	return err == nil && info.IsDir()
}

// ListDirectory lists all metadata files in a directory
func (ms *MetadataService) ListDirectory(virtualPath string) ([]string, error) {
	metadataDir := ms.metaDirPath(virtualPath)

	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil // Directory not found, return empty list
		}
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".meta" {
			// Remove .meta extension to get virtual filename
			virtualName := entry.Name()[:len(entry.Name())-5]
			files = append(files, virtualName)
		}
	}

	return files, nil
}

// ListDirectoryAll returns both subdirectory fs.FileInfo entries and virtual
// file names from a single os.ReadDir call. This is used by Readdir to avoid
// two separate directory reads.
func (ms *MetadataService) ListDirectoryAll(virtualPath string) (dirs []fs.FileInfo, fileNames []string, err error) {
	metadataDir := ms.metaDirPath(virtualPath)

	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			info, infoErr := entry.Info()
			if infoErr == nil {
				dirs = append(dirs, info)
			}
		} else if filepath.Ext(entry.Name()) == ".meta" {
			virtualName := entry.Name()[:len(entry.Name())-5]
			fileNames = append(fileNames, virtualName)
		}
	}
	return dirs, fileNames, nil
}

// DirectoryModTime returns the on-disk mtime of the metadata directory backing
// the given virtual directory, or a zero time.Time if it does not exist or is
// not a directory (letting callers fall back to a static epoch).
//
// This is a single stat: the kernel already advances a directory's mtime when
// children are created or removed, so there is no need to scan children. It
// stays stable across restart health sweeps because UpdateFileStatus skips the
// write entirely when the status is unchanged, and advances on real imports and
// deletions, which do mutate directory entries.
func (ms *MetadataService) DirectoryModTime(virtualPath string) time.Time {
	info, err := os.Stat(ms.metaDirPath(virtualPath))
	if err != nil || !info.IsDir() {
		return time.Time{}
	}
	return info.ModTime()
}

// CreateFileMetadata creates a new FileMetadata with basic fields
func (ms *MetadataService) CreateFileMetadata(
	fileSize int64,
	sourceNzbPath string,
	status metapb.FileStatus,
	segmentData []*metapb.SegmentData,
	encryption metapb.Encryption,
	password string,
	salt string,
	aesKey []byte,
	aesIv []byte,
	releaseDate int64,
	par2Files []*metapb.Par2FileReference,
	nzbdavId string,
) *metapb.FileMetadata {
	now := time.Now().Unix()

	return &metapb.FileMetadata{
		FileSize:      fileSize,
		SourceNzbPath: sourceNzbPath,
		Status:        status,
		Password:      password,
		Salt:          salt,
		Encryption:    encryption,
		SegmentData:   segmentData,
		AesKey:        aesKey,
		AesIv:         aesIv,
		CreatedAt:     now,
		ModifiedAt:    now,
		ReleaseDate:   releaseDate,
		Par2Files:     par2Files,
		NzbdavId:      nzbdavId,
	}
}

// UpdateFileMetadata applies updateFunc to existing metadata and writes it back.
//
// IMPORTANT — non-content RMW only: this helper is reserved for mutations that
// do NOT change the file's logical content (e.g. health status, known-holes).
// ModifiedAt is always unconditionally restored to its pre-call value, so any
// attempt to bump it inside updateFunc will be silently discarded.
//
// For paths that legitimately need to set a new ModifiedAt (initial import,
// content replacement), call WriteFileMetadata or CreateFileMetadata directly
// with the correct timestamp already set on the metadata struct.
//
// This is not a cheap call: it unmarshals the full proto and rewrites the .meta
// via temp file + rename, which also bumps the parent directory's mtime. Callers
// that may be asked to apply a no-op should guard against it first — see
// UpdateFileStatus.
func (ms *MetadataService) UpdateFileMetadata(virtualPath string, updateFunc func(*metapb.FileMetadata)) error {
	virtualPath = normalizeVirtualPath(virtualPath)

	// Read existing metadata
	metadata, err := ms.ReadFileMetadata(virtualPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}
	if metadata == nil {
		return fmt.Errorf("metadata not found for path: %s", virtualPath)
	}

	// Snapshot ModifiedAt before invoking the callback. Health-status and
	// known-holes updates are not content changes; they must not alter WebDAV
	// Last-Modified or the FUSE mtime seen by Jellyfin. Restored unconditionally
	// so future callbacks cannot accidentally mutate it.
	originalModifiedAt := metadata.ModifiedAt

	// Apply update function
	updateFunc(metadata)

	// Always restore ModifiedAt to its pre-call value.
	metadata.ModifiedAt = originalModifiedAt

	// Write back to disk. No observer notification on purpose: the update
	// callbacks in use (file status, known holes) cannot change anything a file
	// identity is derived from (segment message-IDs and structure), so the
	// content index needs no refresh.
	return ms.writeFileMetadata(virtualPath, metadata)
}

// UpdateFileStatus updates the status of a file in metadata.
//
// Re-asserting the status a file already has is a no-op and is skipped without
// touching the disk. This matters at startup: the health worker emits
// EventTypeFileHealthy for every known-healthy file, and the full read-modify-
// write path unmarshals the entire proto (megabytes once SegmentData is inline)
// and rewrites the .meta via temp file + rename for each one. The lite
// projection carries Status, is served from an LRU, and on a miss reads only
// liteScanBytes without ever instantiating the full proto — so the unchanged
// case costs approximately nothing.
//
// Skipping the write also leaves the parent directory's mtime untouched, which
// is what keeps DirectoryModTime stable across restarts.
func (ms *MetadataService) UpdateFileStatus(virtualPath string, status metapb.FileStatus) error {
	// Fall through on any error or miss so not-found and unreadable-metadata
	// behaviour stays exactly as it was.
	if lite, err := ms.ReadFileMetadataLite(virtualPath); err == nil && lite != nil && lite.Status == status {
		return nil
	}

	return ms.UpdateFileMetadata(virtualPath, func(metadata *metapb.FileMetadata) {
		metadata.Status = status
	})
}

// DeleteFileMetadata deletes a metadata file, its .id sidecar, and — when this was
// the last metadata file referencing it — the shared .nzbz store behind it.
//
// The store is a per-release artifact shared by every file of a multi-file import,
// so its lifetime is governed exclusively by the reference count. Nothing here acts
// on SourceNzbPath: for v3 metadata it aliases StoreRef, and removing it directly
// destroyed the store out from under sibling files (issue #858).
func (ms *MetadataService) DeleteFileMetadata(ctx context.Context, virtualPath string) error {
	virtualPath = normalizeVirtualPath(virtualPath)
	ms.liteCache.Remove(virtualPath)

	metadataPath := ms.metaFilePath(virtualPath)
	metadataDir := filepath.Dir(metadataPath)

	// Read StoreRef straight off the proto rather than via ReadFileMetadata: a full
	// read resolves segments against the store, so on an install whose store is
	// already missing it fails and we would lose the reference we need to decrement.
	storeRef := ms.readStoreRef(metadataPath)

	// Delete the metadata file
	err := os.Remove(metadataPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata file: %w", err)
	}

	// Clean up .id sidecar file
	idPath := metadataPath + ".id"
	if removeErr := os.Remove(idPath); removeErr != nil && !os.IsNotExist(removeErr) {
		slog.DebugContext(ctx, "Failed to remove .id sidecar file", "path", idPath, "error", removeErr)
	}

	// Clean up empty parent directories in metadata path
	utils.RemoveEmptyDirs(ms.rootPath, metadataDir)

	if obs := ms.fileObserver(); obs != nil {
		obs.FileDeleted(ctx, virtualPath)
	}

	// Decrement reference count for shared store file and delete it if no more refs.
	if ms.storeRefCounter != nil && storeRef != "" {
		newCount, tracked, err := ms.storeRefCounter.DecStoreRef(ctx, storeRef)
		if err != nil {
			slog.WarnContext(ctx, "failed to decrement store ref count",
				"store_path", storeRef, "error", err)
		} else if tracked && newCount == 0 {
			if removeErr := os.Remove(storeRef); removeErr != nil && !os.IsNotExist(removeErr) {
				slog.WarnContext(ctx, "failed to delete orphaned store file",
					"store_path", storeRef, "error", removeErr)
			}
		}
	}

	return nil
}

// DeleteCorruptedFile removes a file's metadata, then removes the physical library
// file (if any) and cleans up now-empty parent directories in the physical library
// tree. Metadata-tree cleanup is already handled by DeleteFileMetadata; the
// physical-path removal is error-tolerant since physicalPath is often just a view
// into the same mount and may already be gone.
func (ms *MetadataService) DeleteCorruptedFile(ctx context.Context, virtualPath string, physicalPath string, physicalRoot string) error {
	if err := ms.DeleteFileMetadata(ctx, virtualPath); err != nil {
		return err
	}
	ms.removeCorruptedCopy(ctx, virtualPath)
	if physicalPath == "" {
		return nil
	}
	if err := os.Remove(physicalPath); err != nil && !os.IsNotExist(err) {
		slog.WarnContext(ctx, "Failed to delete physical library file", "path", physicalPath, "error", err)
	}
	if physicalRoot != "" {
		utils.RemoveEmptyDirs(physicalRoot, filepath.Dir(physicalPath))
	}
	return nil
}

// DeleteDirectory deletes a metadata directory and all its contents
func (ms *MetadataService) DeleteDirectory(virtualPath string) error {
	ctx := context.Background()
	virtualPath = normalizeVirtualPath(virtualPath)

	ms.purgeCachedTree(virtualPath)

	metadataDir := ms.metaDirPath(virtualPath)
	if err := ms.assertNotRootDir(metadataDir); err != nil {
		return err
	}

	// Pre-pass: if refcounting is enabled, collect all v3 store refs before deletion.
	var storeRefCounts map[string]int
	if ms.storeRefCounter != nil {
		storeRefCounts = make(map[string]int)
		_ = filepath.WalkDir(metadataDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".meta") {
				return nil
			}
			if ref := ms.readStoreRef(path); ref != "" {
				storeRefCounts[ref]++
			}
			return nil
		})
	}

	err := os.RemoveAll(metadataDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata directory: %w", err)
	}

	if obs := ms.fileObserver(); obs != nil {
		obs.DirectoryDeleted(ctx, virtualPath)
	}

	// Post-pass: decrement ref counts and delete orphaned store files.
	if ms.storeRefCounter != nil {
		for storePath, count := range storeRefCounts {
			var lastCount int64
			var lastTracked bool
			for range count {
				newCount, tracked, decErr := ms.storeRefCounter.DecStoreRef(ctx, storePath)
				if decErr != nil {
					slog.WarnContext(ctx, "failed to decrement store ref count",
						"store_path", storePath, "error", decErr)
					lastCount = -1 // mark as unknown; don't delete
					lastTracked = false
					break
				}
				lastCount, lastTracked = newCount, tracked
			}
			// An untracked store has an unknown ref count, not a zero one: leaking it
			// is recoverable, deleting one a sibling still needs is not.
			if lastTracked && lastCount == 0 {
				if removeErr := os.Remove(storePath); removeErr != nil && !os.IsNotExist(removeErr) {
					slog.WarnContext(ctx, "failed to delete orphaned store file",
						"store_path", storePath, "error", removeErr)
				}
			}
		}
	}

	return nil
}

// DeleteDirectoryIfEmpty deletes a metadata directory only if it holds no files or subdirectories,
// so a release folder shared with a concurrent import is never removed from under it.
// Returns ErrDirectoryNotEmpty when the directory was preserved for that reason.
func (ms *MetadataService) DeleteDirectoryIfEmpty(virtualPath string) error {
	virtualPath = normalizeVirtualPath(virtualPath)
	metadataDir := ms.metaDirPath(virtualPath)
	if err := ms.assertNotRootDir(metadataDir); err != nil {
		return err
	}

	if err := os.Remove(metadataDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		// rmdir reports a non-empty directory with a platform-specific errno
		// (ENOTEMPTY, EEXIST, or ERROR_DIR_NOT_EMPTY), so probe the directory
		// instead of matching them.
		if notEmpty, probeErr := dirHasEntries(metadataDir); probeErr == nil && notEmpty {
			return ErrDirectoryNotEmpty
		}
		return fmt.Errorf("failed to delete metadata directory: %w", err)
	}

	ms.purgeCachedTree(virtualPath)

	return nil
}

// dirHasEntries reports whether path is a directory holding at least one entry.
func dirHasEntries(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}

	return len(entries) > 0, nil
}

// assertNotRootDir blocks deletion of the metadata root itself, guarding against a
// virtual path that resolves to the whole store.
func (ms *MetadataService) assertNotRootDir(metadataDir string) error {
	clean := filepath.Clean(metadataDir)
	if clean == filepath.Clean(ms.rootPath) || clean == "/" || clean == "." {
		return fmt.Errorf("safety block: refusing to remove root metadata directory: %s", clean)
	}

	return nil
}

// purgeCachedTree drops the cached entry for virtualPath and everything
// beneath it. Expects virtualPath already normalized (no leading slash,
// "/"-separated) — a "\" prefix would miss every key on Windows.
func (ms *MetadataService) purgeCachedTree(virtualPath string) {
	prefix := virtualPath + "/"
	if virtualPath == "" {
		prefix = ""
	}
	for _, key := range ms.liteCache.Keys() {
		if key == virtualPath || strings.HasPrefix(key, prefix) {
			ms.liteCache.Remove(key)
		}
	}
}

// RenameFileMetadata atomically renames a metadata file (and its .id sidecar) from oldVirtualPath to newVirtualPath.
// Uses os.Rename for atomicity on the same filesystem, falling back to read-write-delete for cross-device moves.
func (ms *MetadataService) RenameFileMetadata(oldVirtualPath, newVirtualPath string) error {
	oldVirtualPath = normalizeVirtualPath(oldVirtualPath)
	newVirtualPath = normalizeVirtualPath(newVirtualPath)
	ms.liteCache.Remove(oldVirtualPath)
	ms.liteCache.Remove(newVirtualPath)

	oldMetaPath := ms.metaFilePath(oldVirtualPath)
	newMetaPath := ms.metaFilePath(newVirtualPath)
	newDir := filepath.Dir(newMetaPath)

	// Ensure destination directory exists
	if err := os.MkdirAll(newDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination metadata directory: %w", err)
	}

	// Try to move metadata file
	if err := utils.MoveFile(oldMetaPath, newMetaPath); err != nil {
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	// Also rename the .id sidecar file if it exists
	oldIDPath := oldMetaPath + ".id"
	newIDPath := newMetaPath + ".id"
	if _, err := os.Stat(oldIDPath); err == nil {
		if err := utils.MoveFile(oldIDPath, newIDPath); err != nil {
			slog.WarnContext(context.Background(), "Failed to rename .id sidecar file", "old", oldIDPath, "new", newIDPath, "error", err)
		}
	}

	if obs := ms.fileObserver(); obs != nil {
		ctx := context.Background()
		obs.FileDeleted(ctx, oldVirtualPath)
		obs.FileWritten(ctx, newVirtualPath)
	}

	return nil
}

// RenameDirectory moves a whole metadata directory (and everything in it)
// from oldVirtualPath to newVirtualPath and tells the file observer.
func (ms *MetadataService) RenameDirectory(oldVirtualPath, newVirtualPath string) error {
	oldVirtualPath = normalizeVirtualPath(oldVirtualPath)
	newVirtualPath = normalizeVirtualPath(newVirtualPath)
	ms.purgeCachedTree(oldVirtualPath)
	ms.purgeCachedTree(newVirtualPath)
	if err := os.Rename(ms.metaDirPath(oldVirtualPath), ms.metaDirPath(newVirtualPath)); err != nil {
		return err
	}
	if obs := ms.fileObserver(); obs != nil {
		obs.DirectoryRenamed(context.Background(), oldVirtualPath, newVirtualPath)
	}
	return nil
}

// GetMetadataFilePath returns the filesystem path for a metadata file
func (ms *MetadataService) GetMetadataFilePath(virtualPath string) string {
	return ms.metaFilePath(virtualPath)
}

// GetMetadataDirectoryPath returns the filesystem path for a metadata directory
func (ms *MetadataService) GetMetadataDirectoryPath(virtualPath string) string {
	return ms.metaDirPath(virtualPath)
}

func (ms *MetadataService) CreateDirectory(name string) error {
	return os.MkdirAll(ms.metaDirPath(name), 0755)
}

// CleanupEmptyDirectories recursively removes empty directories under the given virtual path.
// Uses a bottom-up approach to ensure parent directories are also removed if they become empty.
func (ms *MetadataService) CleanupEmptyDirectories(virtualPath string, protected []string) error {
	fullPath := ms.metaDirPath(virtualPath)

	// Check if path exists
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return nil
	}

	return ms.cleanupEmptyDirsRecursive(fullPath, protected)
}

func (ms *MetadataService) cleanupEmptyDirsRecursive(path string, protected []string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	isEmpty := true
	for _, entry := range entries {
		if entry.IsDir() {
			subPath := filepath.Join(path, entry.Name())
			if err := ms.cleanupEmptyDirsRecursive(subPath, protected); err != nil {
				slog.DebugContext(context.Background(), "Failed to cleanup sub-directory", "path", subPath, "error", err)
				isEmpty = false // Keep parent if sub-cleanup failed
				continue
			}

			// Re-check after sub-directory cleanup
			subEntries, _ := os.ReadDir(subPath)
			if len(subEntries) > 0 {
				isEmpty = false
			}
		} else {
			isEmpty = false
		}
	}

	// Don't delete the root of the cleanup
	if isEmpty && path != ms.rootPath && !ms.isCompleteDir(path) {
		// Check protected list
		base := filepath.Base(path)
		if strings.EqualFold(base, "corrupted_metadata") {
			return nil
		}

		for _, p := range protected {
			if strings.EqualFold(base, p) {
				return nil
			}
		}

		slog.DebugContext(context.Background(), "Removing empty metadata directory", "path", path)
		return os.Remove(path)
	}

	return nil
}

// MoveToCorrupted moves a metadata file to a special corrupted directory for safety
func (ms *MetadataService) MoveToCorrupted(ctx context.Context, virtualPath string) error {
	virtualPath = normalizeVirtualPath(virtualPath)
	ms.liteCache.Remove(virtualPath)

	metadataPath := ms.metaFilePath(virtualPath)
	metaName := filepath.Base(metadataPath)
	dir := filepath.Dir(strings.TrimPrefix(virtualPath, "/"))

	// Check if source exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		return nil
	}

	// Define corrupted directory path (root/corrupted_metadata/...)
	// We use a visible folder name as requested.
	corruptedRoot := filepath.Join(ms.rootPath, "corrupted_metadata")
	targetDir := filepath.Join(corruptedRoot, dir)

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create corrupted metadata directory: %w", err)
	}

	targetPath := filepath.Join(targetDir, metaName)

	// Move the .meta file
	if err := os.Rename(metadataPath, targetPath); err != nil {
		slog.WarnContext(ctx, "Failed to move corrupted metadata, trying copy fallback", "error", err)
		// Rename can fail across different volumes, though usually metadata is on one volume.
		// For simplicity, we return the error here as it's unexpected for metadata.
		return err
	}

	if obs := ms.fileObserver(); obs != nil {
		obs.FileDeleted(ctx, virtualPath)
	}

	// The .meta's disk mtime is pinned to the proto's ModifiedAt (the import time),
	// so left alone it would date the safety copy to the import rather than to the
	// move. Retention counts from the move, so restart the clock here.
	now := time.Now()
	if err := os.Chtimes(targetPath, now, now); err != nil {
		slog.DebugContext(ctx, "Failed to stamp corrupted metadata copy mtime",
			"path", targetPath, "error", err)
	}

	// Also try to move the .id file if it exists
	idPath := metadataPath + ".id"
	if _, err := os.Stat(idPath); err == nil {
		_ = os.Rename(idPath, targetPath+".id")
	}

	slog.InfoContext(ctx, "Moved corrupted metadata to safety folder preserving structure",
		"original", metadataPath,
		"target", targetPath)
	return nil
}

// PruneCorrupted removes safety copies under corrupted_metadata whose .meta is
// older than maxAge, and reports how many were removed. A maxAge of zero or less
// prunes every copy. A missing corrupted_metadata directory is not an error.
func (ms *MetadataService) PruneCorrupted(ctx context.Context, maxAge time.Duration) (int, error) {
	corruptedRoot := filepath.Join(ms.rootPath, "corrupted_metadata")
	if _, err := os.Stat(corruptedRoot); os.IsNotExist(err) {
		return 0, nil
	}

	cutoff := time.Now().Add(-maxAge)
	removed := 0
	prunedDirs := make(map[string]struct{})

	err := filepath.WalkDir(corruptedRoot, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return nil // skip errors
		}
		if d.IsDir() || !strings.HasSuffix(path, ".meta") {
			return nil
		}

		info, infoErr := d.Info()
		if infoErr != nil {
			return nil
		}
		if maxAge > 0 && info.ModTime().After(cutoff) {
			return nil
		}

		pruned, pruneErr := ms.pruneCorruptedMeta(ctx, path)
		if pruneErr != nil {
			slog.WarnContext(ctx, "Failed to prune corrupted metadata copy", "path", path, "error", pruneErr)
			return nil
		}
		if pruned {
			removed++
			prunedDirs[filepath.Dir(path)] = struct{}{}
		}
		return nil
	})

	for dir := range prunedDirs {
		utils.RemoveEmptyDirs(corruptedRoot, dir)
	}

	if err != nil {
		return removed, err
	}

	if removed > 0 {
		slog.InfoContext(ctx, "Pruned corrupted metadata safety copies",
			"removed", removed, "max_age", maxAge)
	}

	return removed, nil
}

// CorruptedStats reports how many safety copies corrupted_metadata currently holds
// and how much disk they occupy.
func (ms *MetadataService) CorruptedStats(ctx context.Context) (int, int64, error) {
	corruptedRoot := filepath.Join(ms.rootPath, "corrupted_metadata")
	if _, err := os.Stat(corruptedRoot); os.IsNotExist(err) {
		return 0, 0, nil
	}

	count := 0
	var totalBytes int64

	err := filepath.WalkDir(corruptedRoot, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".meta") {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return nil
		}
		count++
		totalBytes += info.Size()
		return nil
	})
	if err != nil {
		return count, totalBytes, err
	}

	return count, totalBytes, nil
}

// pruneCorruptedMeta removes a single safety copy plus its .id sidecar, reporting
// whether anything was there to remove.
//
// The store reference must be released here: MoveToCorrupted renames the .meta
// without decrementing, so the copy is the only remaining holder of a reference to
// the release's shared .nzbz store, and dropping it silently would pin that store
// on disk forever.
func (ms *MetadataService) pruneCorruptedMeta(ctx context.Context, metaPath string) (bool, error) {
	storeRef := ms.readStoreRef(metaPath)

	if err := os.Remove(metaPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to delete corrupted metadata file: %w", err)
	}

	idPath := metaPath + ".id"
	if removeErr := os.Remove(idPath); removeErr != nil && !os.IsNotExist(removeErr) {
		slog.DebugContext(ctx, "Failed to remove .id sidecar file", "path", idPath, "error", removeErr)
	}

	if ms.storeRefCounter != nil && storeRef != "" {
		newCount, tracked, err := ms.storeRefCounter.DecStoreRef(ctx, storeRef)
		if err != nil {
			slog.WarnContext(ctx, "failed to decrement store ref count",
				"store_path", storeRef, "error", err)
		} else if tracked && newCount == 0 {
			if removeErr := os.Remove(storeRef); removeErr != nil && !os.IsNotExist(removeErr) {
				slog.WarnContext(ctx, "failed to delete orphaned store file",
					"store_path", storeRef, "error", removeErr)
			}
		}
	}

	return true, nil
}

// removeCorruptedCopy drops the safety copy shadowing a virtual path, if one exists,
// so a successful re-import or an explicit delete leaves nothing behind.
func (ms *MetadataService) removeCorruptedCopy(ctx context.Context, virtualPath string) {
	v := normalizeVirtualPath(virtualPath)
	corruptedRoot := filepath.Join(ms.rootPath, "corrupted_metadata")
	copyDir := filepath.Join(corruptedRoot, filepath.Dir(strings.TrimPrefix(v, "/")))
	copyPath := filepath.Join(copyDir, ms.truncateFilename(filepath.Base(v))+".meta")

	pruned, err := ms.pruneCorruptedMeta(ctx, copyPath)
	if err != nil {
		slog.WarnContext(ctx, "Failed to remove corrupted metadata safety copy",
			"path", copyPath, "error", err)
		return
	}
	if pruned {
		utils.RemoveEmptyDirs(corruptedRoot, copyDir)
	}
}

// CleanupOrphanedIDSymlinks walks the .ids/ directory and removes symlinks whose
// targets no longer exist. Empty shard directories are cleaned up afterwards.
// Returns the number of removed symlinks.
func (ms *MetadataService) CleanupOrphanedIDSymlinks(ctx context.Context) (int, error) {
	idsRoot := filepath.Join(ms.rootPath, ".ids")
	if _, err := os.Stat(idsRoot); os.IsNotExist(err) {
		return 0, nil
	}

	removed := 0
	err := filepath.WalkDir(idsRoot, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return nil // skip errors
		}
		if d.IsDir() {
			return nil
		}

		// Only process symlinks
		if d.Type()&os.ModeSymlink == 0 {
			return nil
		}

		// Check if the symlink target exists
		target, readErr := os.Readlink(path)
		if readErr != nil {
			return nil
		}

		// Make target absolute if relative
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}

		if _, statErr := os.Stat(target); os.IsNotExist(statErr) {
			if removeErr := os.Remove(path); removeErr == nil {
				removed++
			}
		}

		return nil
	})

	if err != nil {
		return removed, err
	}

	// Clean empty shard directories bottom-up
	utils.RemoveEmptyDirs(ms.rootPath, idsRoot)

	return removed, nil
}

func (ms *MetadataService) isCompleteDir(path string) bool {
	// Simple check to avoid deleting the 'complete' folder itself
	return filepath.Base(path) == "complete"
}
