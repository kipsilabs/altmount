package metadata

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertNoBackslashPersisted walks the metadata root and fails on any
// persisted name containing a backslash (issue #660).
func assertNoBackslashPersisted(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		assert.NotContains(t, d.Name(), `\`, "on-disk name must never contain a backslash: %s", path)
		return nil
	})
	require.NoError(t, err)
}

func TestDeleteFileMetadata_RemovesMetadata(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "test_movie.mkv")

	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "abcde12345",
	)
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	metaPath := ms.GetMetadataFilePath(virtualPath)
	require.FileExists(t, metaPath)

	ctx := context.Background()
	require.NoError(t, ms.DeleteFileMetadata(ctx, virtualPath))

	assert.NoFileExists(t, metaPath)
}

func TestDeleteFileMetadata_NoIDSidecar_NoError(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "no_id_movie.mkv")

	meta := ms.CreateFileMetadata(
		512, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	ctx := context.Background()
	err := ms.DeleteFileMetadata(ctx, virtualPath)
	assert.NoError(t, err, "delete should succeed even without .id sidecar")

	assert.NoFileExists(t, ms.GetMetadataFilePath(virtualPath))
}

func TestMoveToCorrupted_MovesMetadata(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "corrupted_movie.mkv")

	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "fghij67890",
	)
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	ctx := context.Background()
	require.NoError(t, ms.MoveToCorrupted(ctx, virtualPath))

	// Original location gone
	assert.NoFileExists(t, ms.GetMetadataFilePath(virtualPath))

	// Metadata now in corrupted folder
	corruptedPath := filepath.Join(root, "corrupted_metadata", "movies", "corrupted_movie.mkv.meta")
	assert.FileExists(t, corruptedPath, "metadata should exist in corrupted folder")
}

func TestCleanupOrphanedIDSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	root := t.TempDir()
	ms := NewMetadataService(root)

	// Create a valid metadata file and manually plant a valid .ids/ symlink for it
	validPath := filepath.Join("movies", "valid.mkv")
	validID := "valid12345"
	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, validID,
	)
	require.NoError(t, ms.WriteFileMetadata(validPath, meta))

	// Manually create a valid .ids/ symlink pointing at the .meta file
	validMetaPath := ms.GetMetadataFilePath(validPath)
	validShardDir := filepath.Join(root, ".ids", "v", "a", "l", "i", "d")
	require.NoError(t, os.MkdirAll(validShardDir, 0755))
	validLink := filepath.Join(validShardDir, validID+".meta")
	require.NoError(t, os.Symlink(validMetaPath, validLink))

	// Create a broken symlink (target does not exist)
	brokenID := "broke12345"
	brokenShardDir := filepath.Join(root, ".ids", "b", "r", "o", "k", "e")
	require.NoError(t, os.MkdirAll(brokenShardDir, 0755))
	brokenLink := filepath.Join(brokenShardDir, brokenID+".meta")
	require.NoError(t, os.Symlink("/nonexistent/target.meta", brokenLink))

	ctx := context.Background()
	removed, err := ms.CleanupOrphanedIDSymlinks(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, removed, "should remove exactly one orphaned symlink")

	// Broken symlink gone
	_, err = os.Lstat(brokenLink)
	assert.True(t, os.IsNotExist(err), "broken symlink should be removed")

	// Valid symlink still present
	_, err = os.Lstat(validLink)
	assert.NoError(t, err, "valid symlink should still exist")
}

func TestCleanupOrphanedIDSymlinks_NoIDsDir(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	removed, err := ms.CleanupOrphanedIDSymlinks(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, removed)
}

func TestCleanupOrphanedIDSymlinks_ContextCancellation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	root := t.TempDir()
	ms := NewMetadataService(root)

	// Create a few broken symlinks
	for _, id := range []string{"aaaaa11111", "bbbbb22222", "ccccc33333"} {
		shardDir := filepath.Join(root, ".ids", string(id[0]), string(id[1]), string(id[2]), string(id[3]), string(id[4]))
		require.NoError(t, os.MkdirAll(shardDir, 0755))
		require.NoError(t, os.Symlink("/nonexistent/"+id+".meta", filepath.Join(shardDir, id+".meta")))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := ms.CleanupOrphanedIDSymlinks(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestReadFileMetadataLite_DoesNotReadFullProto pins the fast path: when the
// `.meta` proto is multi-MB (because the file has thousands of NestedSources
// or SegmentData entries — the exact shape that caused a 7.94 GB
// PROPFIND allocation spike), ReadFileMetadataLite must read only the head
// of the file and never instantiate the giant proto. We measure this via
// the file size we write vs. the bytes read by the lite path.
func TestReadFileMetadataLite_DoesNotReadFullProto(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "huge.m2ts")

	// Build a FileMetadata with thousands of NestedSources so the on-disk
	// proto is hundreds of KB — large enough that a regression to the
	// full os.ReadFile + proto.Unmarshal path would allocate >>liteScanBytes
	// and be caught by the heap-delta assertion below.
	nested := make([]*metapb.NestedSegmentSource, 0, 5000)
	for i := range 5000 {
		nested = append(nested, &metapb.NestedSegmentSource{
			Segments: []*metapb.SegmentData{
				{Id: "msg-id-with-a-typical-length@server.example", StartOffset: int64(i * 1024), EndOffset: int64((i + 1) * 1024), SegmentSize: 1024},
			},
			InnerOffset:     0,
			InnerLength:     1024,
			InnerVolumeSize: 1024,
		})
	}
	meta := ms.CreateFileMetadata(
		17_860_995_072, "Avatar.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "huge-nzbdav-id",
	)
	meta.NestedSources = nested
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	// Confirm the on-disk file is at least 200 KB — the partial-read
	// budget is 4 KB so anything substantially larger gives the heap-delta
	// assertion enough headroom to catch a regression.
	stat, err := os.Stat(ms.GetMetadataFilePath(virtualPath))
	require.NoError(t, err)
	require.Greater(t, stat.Size(), int64(200<<10), "test setup should produce a >200KB .meta file to make the fast-path savings observable")

	// Drop the liteCache entry written by WriteFileMetadata so we hit the
	// disk-read path under test.
	ms.liteCache.Purge()

	// Snapshot heap allocations before / after the call. The full-read
	// implementation would allocate at least stat.Size() bytes (for the
	// os.ReadFile buffer) plus the unmarshalled proto. The partial-read
	// implementation should allocate well under 64 KiB.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	lite, err := ms.ReadFileMetadataLite(virtualPath)
	require.NoError(t, err)
	require.NotNil(t, lite)

	runtime.ReadMemStats(&after)
	delta := after.TotalAlloc - before.TotalAlloc
	t.Logf("ReadFileMetadataLite allocated %d bytes (on-disk .meta = %d bytes)", delta, stat.Size())

	// Correctness: lite must reflect the values we wrote.
	assert.Equal(t, int64(17_860_995_072), lite.FileSize)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, lite.Status)

	// Regression guard: the fast path must allocate dramatically less than
	// the full file. Use 5× liteScanBytes as a comfortable upper bound that
	// still catches a regression where the implementation re-reads the
	// whole file.
	const maxExpectedAlloc = 5 * liteScanBytes
	assert.LessOrEqualf(t, delta, uint64(maxExpectedAlloc),
		"ReadFileMetadataLite allocated %d bytes — should be ≤ %d. A regression to the full os.ReadFile + proto.Unmarshal would allocate >= the on-disk size (%d).",
		delta, maxExpectedAlloc, stat.Size())
}

// TestReadFileMetadataLite_FallsBackOnLongHeader covers the edge where the
// lite fields aren't reachable within liteScanBytes (e.g., a future schema
// change places one after a very large field). The fallback path produces
// the same correct lite struct, just by reading the full file.
func TestReadFileMetadataLite_FallsBackOnLongHeader(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "long-header.mkv")

	// Craft a SourceNzbPath longer than liteScanBytes so the lite fields
	// after it (status, modified_at) fall past the partial-read window.
	// file_size (field 1) is before it, so the partial-read scan sees
	// FileSize but not Status/ModifiedAt → falls back to full read.
	longPath := make([]byte, liteScanBytes+512)
	for i := range longPath {
		longPath[i] = 'a'
	}
	meta := ms.CreateFileMetadata(
		1234, string(longPath), metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "fallback-id",
	)
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))
	ms.liteCache.Purge()

	lite, err := ms.ReadFileMetadataLite(virtualPath)
	require.NoError(t, err)
	require.NotNil(t, lite)
	assert.Equal(t, int64(1234), lite.FileSize)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, lite.Status)
}

// TestUpdateFileMetadata_PreservesModifiedAt ensures status and known-holes
// RMW paths do not rewrite ModifiedAt (WebDAV Last-Modified / FUSE mtime).
func TestUpdateFileMetadata_PreservesModifiedAt(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "stable_mtime.mkv")
	const fixedModifiedAt int64 = 1_700_000_000

	meta := ms.CreateFileMetadata(
		2048, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "mtime-id",
	)
	meta.ModifiedAt = fixedModifiedAt
	meta.CreatedAt = fixedModifiedAt - 60
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	require.NoError(t, ms.UpdateFileStatus(virtualPath, metapb.FileStatus_FILE_STATUS_DEGRADED))

	afterStatus, err := ms.ReadFileMetadata(virtualPath)
	require.NoError(t, err)
	require.NotNil(t, afterStatus)
	assert.Equal(t, fixedModifiedAt, afterStatus.ModifiedAt)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_DEGRADED, afterStatus.Status)
	assert.Equal(t, fixedModifiedAt-60, afterStatus.CreatedAt)

	require.NoError(t, ms.AddKnownHoles(virtualPath, []holes.Run{{Start: 10, Count: 2}}, ""))

	afterHoles, err := ms.ReadFileMetadata(virtualPath)
	require.NoError(t, err)
	require.NotNil(t, afterHoles)
	assert.Equal(t, fixedModifiedAt, afterHoles.ModifiedAt)
	require.NotEmpty(t, afterHoles.KnownHoles)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_DEGRADED, afterHoles.Status)
}

// TestWriteFileMetadata_PinsDiskMtimeToModifiedAt covers the os.Chtimes call
// that follows the atomic rename: os.Rename always stamps the destination with
// mtime=now, which would surface as a fresh timestamp to any host tool reading
// the on-disk mtime rather than the proto.
func TestWriteFileMetadata_PinsDiskMtimeToModifiedAt(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "pinned.mkv")
	const fixedModifiedAt int64 = 1_700_000_000

	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	meta.ModifiedAt = fixedModifiedAt
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	info, err := os.Stat(ms.GetMetadataFilePath(virtualPath))
	require.NoError(t, err)
	assert.Equal(t, fixedModifiedAt, info.ModTime().Unix())
}

// TestUpdateFileStatus_UnchangedStatusSkipsWrite pins the fast path that keeps
// restarts cheap: the health worker re-asserts HEALTHY for every known-healthy
// file at startup, and rewriting an identical proto for each one is pure waste.
//
// A sentinel mtime detects the write: WriteFileMetadata always re-stamps the
// file to ModifiedAt, so the sentinel survives only if no write happened.
func TestUpdateFileStatus_UnchangedStatusSkipsWrite(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "already_healthy.mkv")
	const fixedModifiedAt int64 = 1_700_000_000

	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	meta.ModifiedAt = fixedModifiedAt
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	metaPath := ms.GetMetadataFilePath(virtualPath)
	sentinel := time.Unix(1_600_000_000, 0)
	require.NoError(t, os.Chtimes(metaPath, sentinel, sentinel))

	// Re-asserting the current status must not touch the file.
	require.NoError(t, ms.UpdateFileStatus(virtualPath, metapb.FileStatus_FILE_STATUS_HEALTHY))

	info, err := os.Stat(metaPath)
	require.NoError(t, err)
	assert.Equal(t, sentinel.Unix(), info.ModTime().Unix(),
		"re-asserting an unchanged status must not rewrite the .meta file")

	// A real transition still writes, restoring the pinned ModifiedAt mtime.
	require.NoError(t, ms.UpdateFileStatus(virtualPath, metapb.FileStatus_FILE_STATUS_DEGRADED))

	info, err = os.Stat(metaPath)
	require.NoError(t, err)
	assert.Equal(t, fixedModifiedAt, info.ModTime().Unix(),
		"a genuine status transition must write and re-pin mtime to ModifiedAt")

	after, err := ms.ReadFileMetadata(virtualPath)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_DEGRADED, after.Status)
}

// TestUpdateFileStatus_SkipsWriteOnColdCache ensures the fast path is driven by
// on-disk state, not just a warm liteCache — a restart starts with an empty one.
func TestUpdateFileStatus_SkipsWriteOnColdCache(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "cold_cache.mkv")

	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	meta.ModifiedAt = 1_700_000_000
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	ms.liteCache.Purge()

	metaPath := ms.GetMetadataFilePath(virtualPath)
	sentinel := time.Unix(1_600_000_000, 0)
	require.NoError(t, os.Chtimes(metaPath, sentinel, sentinel))

	require.NoError(t, ms.UpdateFileStatus(virtualPath, metapb.FileStatus_FILE_STATUS_HEALTHY))

	info, err := os.Stat(metaPath)
	require.NoError(t, err)
	assert.Equal(t, sentinel.Unix(), info.ModTime().Unix())
}

// TestDirectoryModTime reports the on-disk mtime of the backing metadata
// directory. The kernel already advances it when children are created or
// removed, so no per-child scan is needed to derive a virtual directory mtime.
func TestDirectoryModTime(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	dir := filepath.Join("movies", "ActionMovie")

	// Non-existent directory reports zero, letting callers fall back to an epoch.
	assert.True(t, ms.DirectoryModTime(dir).IsZero())

	meta := ms.CreateFileMetadata(100, "1.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
	meta.ModifiedAt = 1_700_000_100
	require.NoError(t, ms.WriteFileMetadata(filepath.Join(dir, "part1.mkv"), meta))

	onDisk, err := os.Stat(filepath.Join(root, dir))
	require.NoError(t, err)
	assert.Equal(t, onDisk.ModTime(), ms.DirectoryModTime(dir))

	// A file path is not a directory.
	assert.True(t, ms.DirectoryModTime(filepath.Join(dir, "part1.mkv")).IsZero())
}

// TestDirectoryModTime_StableAcrossHealthSweep is the regression guard for the
// original bug: a restart's health sweep must not make directories look freshly
// modified to media scanners that only rescan folders whose mtime changed.
func TestDirectoryModTime_StableAcrossHealthSweep(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	dir := filepath.Join("movies", "ActionMovie")

	for _, name := range []string{"part1.mkv", "part2.mkv", "part3.mkv"} {
		meta := ms.CreateFileMetadata(100, "x.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
		meta.ModifiedAt = 1_700_000_100
		require.NoError(t, ms.WriteFileMetadata(filepath.Join(dir, name), meta))
	}

	before := ms.DirectoryModTime(dir)
	require.False(t, before.IsZero())

	// Simulate the startup sweep re-asserting HEALTHY for every file.
	for _, name := range []string{"part1.mkv", "part2.mkv", "part3.mkv"} {
		require.NoError(t, ms.UpdateFileStatus(filepath.Join(dir, name), metapb.FileStatus_FILE_STATUS_HEALTHY))
	}

	assert.Equal(t, before, ms.DirectoryModTime(dir),
		"a no-op health sweep must not bump the directory mtime")

	// A genuine import advances it (allow at least 10ms for filesystem mtime tick).
	time.Sleep(15 * time.Millisecond)
	newMeta := ms.CreateFileMetadata(300, "3.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
	newMeta.ModifiedAt = 1_700_000_900
	require.NoError(t, ms.WriteFileMetadata(filepath.Join(dir, "part4.mkv"), newMeta))

	assert.True(t, ms.DirectoryModTime(dir).After(before),
		"importing a new file must advance the directory mtime")
}

func TestDeleteDirectory_RefusesToRemoveTheMetadataRoot(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	virtualPath := filepath.Join("movies", "keep.mkv")
	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	// A virtual path resolving to the store itself must never wipe the whole store.
	for _, p := range []string{"", ".", string(filepath.Separator)} {
		err := ms.DeleteDirectory(p)
		assert.ErrorContains(t, err, "safety block", "must refuse virtual path %q", p)
	}

	assert.DirExists(t, root)
	assert.FileExists(t, ms.GetMetadataFilePath(virtualPath), "the store's contents must survive a refused delete")
}

func TestDeleteDirectoryIfEmpty(t *testing.T) {
	newService := func(t *testing.T) (*MetadataService, string) {
		t.Helper()
		root := t.TempDir()
		return NewMetadataService(root), root
	}

	writeMeta := func(t *testing.T, ms *MetadataService, virtualPath string) {
		t.Helper()
		meta := ms.CreateFileMetadata(
			1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
			nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
		)
		require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))
	}

	t.Run("removes empty directory and purges its cached tree", func(t *testing.T) {
		ms, root := newService(t)

		virtualPath := filepath.Join("movies", "Release", "file.mkv")
		writeMeta(t, ms, virtualPath)
		_, err := ms.ReadFileMetadataLite(virtualPath)
		require.NoError(t, err)
		require.NoError(t, os.Remove(ms.GetMetadataFilePath(virtualPath)))

		require.NoError(t, ms.DeleteDirectoryIfEmpty(filepath.Join("movies", "Release")))

		assert.NoDirExists(t, filepath.Join(root, "movies", "Release"))
		_, cached := ms.liteCache.Get(virtualPath)
		assert.False(t, cached, "cached entries under a removed directory must be purged")
	})

	t.Run("preserves a directory that still holds files", func(t *testing.T) {
		ms, root := newService(t)

		virtualPath := filepath.Join("movies", "Release", "sibling.mkv")
		writeMeta(t, ms, virtualPath)
		_, err := ms.ReadFileMetadataLite(virtualPath)
		require.NoError(t, err)

		err = ms.DeleteDirectoryIfEmpty(filepath.Join("movies", "Release"))

		assert.ErrorIs(t, err, ErrDirectoryNotEmpty)
		assert.DirExists(t, filepath.Join(root, "movies", "Release"))
		require.FileExists(t, ms.GetMetadataFilePath(virtualPath))
		_, cached := ms.liteCache.Get(virtualPath)
		assert.True(t, cached, "a preserved directory must keep its cached entries")
	})

	t.Run("missing directory is not an error", func(t *testing.T) {
		ms, _ := newService(t)

		assert.NoError(t, ms.DeleteDirectoryIfEmpty(filepath.Join("movies", "Gone")))
	})

	t.Run("refuses to remove the metadata root", func(t *testing.T) {
		ms, root := newService(t)

		for _, p := range []string{"", ".", string(filepath.Separator)} {
			err := ms.DeleteDirectoryIfEmpty(p)
			assert.ErrorContains(t, err, "safety block", "must refuse virtual path %q", p)
		}
		assert.DirExists(t, root)
	})
}

func TestGetMetadataFilePath_NormalizesBackslashAndLeadingSlash(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	base := ms.GetMetadataFilePath("movies/Release/file.mkv")

	variants := []string{
		`movies\Release\file.mkv`,
		"/movies/Release/file.mkv",
		"movies//Release/file.mkv",
	}
	for _, v := range variants {
		assert.Equal(t, base, ms.GetMetadataFilePath(v), "variant %q must resolve to the same on-disk path", v)
	}
}

func TestWriteFileMetadata_BackslashPathNeverPersistsBackslash(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	// A single subject can contain a backslash used as a Windows-style
	// separator by the release, e.g. "Release\A.mkv\A.mkv".
	virtualPath := `Release\A.mkv\A.mkv`
	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))

	assertNoBackslashPersisted(t, root)
	assert.FileExists(t, filepath.Join(root, "Release", "A.mkv", "A.mkv.meta"))

	// Reads must resolve the same normalized path the writer used.
	got, err := ms.ReadFileMetadata(virtualPath)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(1024), got.FileSize)
}

func TestRenameFileMetadata_NormalizesBackslashPath(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	oldPath := filepath.Join("movies", "old.mkv")
	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(oldPath, meta))

	newPath := `movies\Sub\new.mkv`
	require.NoError(t, ms.RenameFileMetadata(oldPath, newPath))

	assertNoBackslashPersisted(t, root)
	assert.FileExists(t, filepath.Join(root, "movies", "Sub", "new.mkv.meta"))
}

func TestTruncateFilename_KeepsCombinedNameWithinFilesystemLimit(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	// 400+ bytes crosses the 255-byte name-component limit once ext+".meta"
	// is added; the old truncation budget didn't account for that.
	longBase := strings.Repeat("a", 400)
	virtualPath := longBase + ".mkv"

	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta), "a name this long must still be writable")

	metaPath := ms.GetMetadataFilePath(virtualPath)
	assert.LessOrEqual(t, len(filepath.Base(metaPath)), 255)
	require.FileExists(t, metaPath)

	// Reads apply the same truncation the writer used, so the file just
	// written round-trips rather than missing.
	got, err := ms.ReadFileMetadata(virtualPath)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(1024), got.FileSize)
}

func TestDeleteDirectory_CachePurgeHandlesTrailingSlash(t *testing.T) {
	ms := NewMetadataService(t.TempDir())

	dir := filepath.Join("movies", "Release")
	virtualPath := filepath.Join(dir, "file.mkv")
	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(virtualPath, meta))
	_, err := ms.ReadFileMetadataLite(virtualPath)
	require.NoError(t, err)

	// A trailing slash on the directory argument used to double the purge
	// prefix ("movies/Release/" + "/" = "movies/Release//"), which matched no
	// cached key and left the entry behind after the directory was deleted
	// from disk.
	require.NoError(t, ms.DeleteDirectory(dir+"/"))

	_, cached := ms.liteCache.Get(virtualPath)
	assert.False(t, cached, "cache entry must be purged even with a trailing-slash directory argument")
}

// The delete path behind DeleteCorruptedFile must find the .meta that was
// written for a backslash path. Without normalization the delete looks for a
// different name and returns nil, leaving the metadata behind.
func TestDeleteCorruptedFile_NormalizesBackslashPath(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	backslashPath := "movies/Release\\file.mkv"
	normalizedPath := "movies/Release/file.mkv"

	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "delete12345",
	)
	require.NoError(t, ms.WriteFileMetadata(backslashPath, meta))
	require.FileExists(t, ms.GetMetadataFilePath(normalizedPath))

	require.NoError(t, ms.DeleteCorruptedFile(context.Background(), backslashPath, "", ""))

	assert.NoFileExists(t, ms.GetMetadataFilePath(normalizedPath))
}

// MoveToCorrupted must move the normalized .meta and must not carry the
// backslash into the name under corrupted_metadata. filepath.FromSlash is a
// no-op on Linux, so it cannot be relied on for this.
func TestMoveToCorrupted_NormalizesBackslashPath(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	backslashPath := "movies/Release\\bad.mkv"
	normalizedPath := "movies/Release/bad.mkv"

	meta := ms.CreateFileMetadata(
		512, "test.nzb", metapb.FileStatus_FILE_STATUS_CORRUPTED,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "corrupt1234",
	)
	require.NoError(t, ms.WriteFileMetadata(backslashPath, meta))
	require.FileExists(t, ms.GetMetadataFilePath(normalizedPath))

	require.NoError(t, ms.MoveToCorrupted(context.Background(), backslashPath))

	assertNoBackslashPersisted(t, root)
	assert.NoFileExists(t, ms.GetMetadataFilePath(normalizedPath))
	require.FileExists(t, filepath.Join(root, "corrupted_metadata", "movies", "Release", "bad.mkv.meta"))
}
