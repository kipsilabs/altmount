package importer

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/importer/multifile"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/nzbbuild"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
	"github.com/javi11/nntppool/v5"
	"google.golang.org/protobuf/proto"
)

// partSize used to split fixtures into multiple NZB segments.
const archivePartSize = 75_000

// obfuscatedName is a 32-hex filename stem that triggers PAR2-deobfuscation.
const obfuscatedStem = "b082fa0beaa644d3aa01045d5b8d0b36"

// TestImportBattery_CleanSingleFile verifies import of a single clean video file
// split into multiple segments. Checks that the virtual path, file size, and
// segment metadata are stored correctly.
func TestImportBattery_CleanSingleFile(t *testing.T) {
	env := newBatteryEnv(t)

	content := bytes.Repeat([]byte("A"), 30_000)
	segs := env.registerContent("single-clean", content, 10_000, 1.0, nil)
	nzb := nzbbuild.Build(nzbbuild.File{Subject: "Movie.2024.mkv", Segments: segs})

	result, written, err := env.runImport(nzb, "Movie.2024.mkv")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	if result != "/" {
		t.Errorf("result = %q, want /", result)
	}
	paths := filePaths(written)
	if len(paths) != 1 || paths[0] != "/Movie.2024.mkv" {
		t.Errorf("writtenPaths = %v, want [/Movie.2024.mkv]", paths)
	}

	meta := env.readMeta("/Movie.2024.mkv")
	if meta.FileSize != int64(len(content)) {
		t.Errorf("FileSize = %d, want %d", meta.FileSize, len(content))
	}
	if len(meta.SegmentData) != 3 {
		t.Errorf("SegmentData len = %d, want 3", len(meta.SegmentData))
	}
}

// TestImportBattery_V3FormatOnDisk verifies that a completed import writes a v3 .meta
// file (starts with the 5-byte magic) and a companion .nzbz store file next to the NZB.
func TestImportBattery_V3FormatOnDisk(t *testing.T) {
	env := newBatteryEnv(t)

	content := bytes.Repeat([]byte("V"), 30_000)
	segs := env.registerContent("v3-disk", content, 10_000, 1.0, nil)
	nzb := nzbbuild.Build(nzbbuild.File{Subject: "V3Test.mkv", Segments: segs})

	_, written, err := env.runImport(nzb, "V3Test.mkv")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	paths := filePaths(written)
	if len(paths) != 1 {
		t.Fatalf("expected 1 written path, got %v", paths)
	}

	// The .meta file must start with the v3 magic: 0x00 'A' 'M' '3' 0x01
	rawMeta, err := os.ReadFile(env.rawMetaPath(paths[0]))
	if err != nil {
		t.Fatalf("read .meta: %v", err)
	}
	v3Magic := []byte{0x00, 'A', 'M', '3', 0x01}
	if len(rawMeta) < 5 || !bytes.Equal(rawMeta[:5], v3Magic) {
		t.Errorf("meta file does not start with v3 magic (got %x); v3 conversion did not run", rawMeta[:min(5, len(rawMeta))])
	}

	// The 3 uniform 10KB segments must be run-encoded (one SegmentRun, no SegmentRefs)
	// so the on-disk meta stays compact regardless of segment count.
	var onDisk metapb.FileMetadata
	if err := proto.Unmarshal(rawMeta[5:], &onDisk); err != nil {
		t.Fatalf("unmarshal v3 meta: %v", err)
	}
	if len(onDisk.SegmentRefs) != 0 {
		t.Errorf("expected no explicit SegmentRefs, got %d", len(onDisk.SegmentRefs))
	}
	if len(onDisk.SegmentRuns) != 1 {
		t.Fatalf("expected 1 SegmentRun, got %d", len(onDisk.SegmentRuns))
	}
	if got := onDisk.SegmentRuns[0].Count; got != 3 {
		t.Errorf("SegmentRun.Count = %d, want 3", got)
	}

	// Reading back must expand the run into the original 3 segments.
	resolved := env.readMeta(paths[0])
	if len(resolved.SegmentData) != 3 {
		t.Errorf("resolved SegmentData len = %d, want 3", len(resolved.SegmentData))
	}
}

// TestImportBattery_CleanMultiFile verifies that two episode files in a single NZB
// are placed under a shared nzbFolder and both get metadata entries.
func TestImportBattery_CleanMultiFile(t *testing.T) {
	env := newBatteryEnv(t)

	c1 := bytes.Repeat([]byte("B"), 20_000)
	c2 := bytes.Repeat([]byte("C"), 20_000)
	segs1 := env.registerContent("multi-ep1", c1, 10_000, 1.0, nil)
	segs2 := env.registerContent("multi-ep2", c2, 10_000, 1.0, nil)

	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: "Show.S01E01.mkv", Segments: segs1},
		nzbbuild.File{Subject: "Show.S01E02.mkv", Segments: segs2},
	)

	result, written, err := env.runImport(nzb, "Show.S01")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	if result != "/Show.S01" {
		t.Errorf("result = %q, want /Show.S01", result)
	}
	paths := filePaths(written)
	if len(paths) != 2 {
		t.Errorf("writtenPaths len = %d, want 2; paths: %v", len(paths), paths)
	}
	for _, p := range paths {
		meta := env.readMeta(p)
		if meta.FileSize != int64(len(c1)) {
			t.Errorf("path %q: FileSize = %d, want %d", p, meta.FileSize, len(c1))
		}
	}
}

// TestImportBattery_ObfuscatedYencName verifies that when an NZB file has an
// obfuscated MD5-hex subject but the yEnc header contains the real filename,
// the importer stores the file under the yEnc-derived name.
func TestImportBattery_ObfuscatedYencName(t *testing.T) {
	env := newBatteryEnv(t)
	// Disable NZB-stem rename so the yEnc name wins.
	f := false
	env.cfg.Import.RenameToNzbName = &f

	content := bytes.Repeat([]byte("D"), 30_000)
	yEncTpl := &nntppool.YEncMeta{
		FileName: "Real.Movie.2024.mkv",
		FileSize: int64(len(content)),
		Total:    3,
	}
	segs := env.registerContent("yenc-name", content, 10_000, 1.0, yEncTpl)

	subject := obfuscatedStem + ".mkv"
	nzb := nzbbuild.Build(nzbbuild.File{Subject: subject, Segments: segs})

	_, written, err := env.runImport(nzb, obfuscatedStem)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	paths := filePaths(written)
	if len(paths) != 1 {
		t.Fatalf("writtenPaths len = %d, want 1; paths: %v", len(paths), paths)
	}
	if filepath.Base(paths[0]) != "Real.Movie.2024.mkv" {
		t.Errorf("leaf name = %q, want Real.Movie.2024.mkv", filepath.Base(paths[0]))
	}

	meta := env.readMeta(paths[0])
	if meta.FileSize != int64(len(content)) {
		t.Errorf("FileSize = %d, want %d", meta.FileSize, len(content))
	}
}

// TestImportBattery_ObfuscatedNzbStemFallback verifies that when a file has an
// obfuscated subject and no yEnc filename, the importer falls back to renaming
// the file using the NZB stem (+ original extension).
func TestImportBattery_ObfuscatedNzbStemFallback(t *testing.T) {
	env := newBatteryEnv(t)
	// renameToNzbName is nil (defaults to true).

	content := bytes.Repeat([]byte("E"), 30_000)
	segs := env.registerContent("nzb-stem", content, 10_000, 1.0, nil)

	subject := obfuscatedStem + ".mkv"
	nzb := nzbbuild.Build(nzbbuild.File{Subject: subject, Segments: segs})

	_, written, err := env.runImport(nzb, "The.Movie.2024.mkv")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	paths := filePaths(written)
	if len(paths) != 1 {
		t.Fatalf("writtenPaths len = %d, want 1; paths: %v", len(paths), paths)
	}
	if filepath.Base(paths[0]) != "The.Movie.2024.mkv" {
		t.Errorf("leaf name = %q, want The.Movie.2024.mkv", filepath.Base(paths[0]))
	}
}

// TestImportBattery_Par2Deobfuscation verifies that when an NZB contains an
// obfuscated main file alongside a PAR2 index, the importer recovers the real
// filename and exact size from the PAR2 FileDesc packet.
func TestImportBattery_Par2Deobfuscation(t *testing.T) {
	env := newBatteryEnv(t)
	f := false
	env.cfg.Import.RenameToNzbName = &f

	// Content must be > 16 KB so the Hash16k is taken from real bytes,
	// not zero-padded to 16384 (which would match incorrectly for small files).
	content := bytes.Repeat([]byte("F"), 32_000)

	segs := env.registerContent("par2-video", content, 16_000, 1.0, nil)

	// Build a minimal PAR2 index describing the real file.
	par2Bytes := par2gen.Build(par2gen.FileEntry{
		Name:    "Real.Movie.2024.mkv",
		Content: content,
	})
	par2Segs := env.registerContent("par2-index", par2Bytes, len(par2Bytes), 1.0, nil)

	videoSubject := obfuscatedStem + ".mkv"
	par2Subject := obfuscatedStem + ".par2"

	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: videoSubject, Segments: segs},
		nzbbuild.File{Subject: par2Subject, Segments: par2Segs},
	)

	_, written, err := env.runImport(nzb, "obfuscated")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	paths := filePaths(written)
	if len(paths) != 1 {
		t.Fatalf("writtenPaths len = %d, want 1; paths: %v", len(paths), paths)
	}
	if filepath.Base(paths[0]) != "Real.Movie.2024.mkv" {
		t.Errorf("leaf name = %q, want Real.Movie.2024.mkv", filepath.Base(paths[0]))
	}

	meta := env.readMeta(paths[0])
	if meta.FileSize != int64(len(content)) {
		t.Errorf("FileSize = %d, want %d", meta.FileSize, len(content))
	}
}

// TestImportBattery_YencOverheadNormalization verifies that when NZB-declared
// segment sizes include yEnc encoding overhead (~3% larger than decoded), the
// importer normalizes them to actual decoded sizes using yEnc metadata headers.
func TestImportBattery_YencOverheadNormalization(t *testing.T) {
	env := newBatteryEnv(t)

	const total = 50_000
	content := bytes.Repeat([]byte("G"), total)

	yEncTpl := &nntppool.YEncMeta{
		FileName: "Normalized.mkv",
		FileSize: total,
		Total:    3,
	}
	// 1.03 overhead: NZB declares ~3% more bytes than actually decoded.
	segs := env.registerContent("yenc-norm", content, 17_000, 1.03, yEncTpl)

	nzb := nzbbuild.Build(nzbbuild.File{Subject: "Normalized.mkv", Segments: segs})

	_, written, err := env.runImport(nzb, "Normalized.mkv")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	paths := filePaths(written)
	if len(paths) != 1 {
		t.Fatalf("writtenPaths len = %d, want 1", len(paths))
	}

	meta := env.readMeta(paths[0])
	if meta.FileSize != total {
		t.Errorf("FileSize = %d, want %d (yEnc normalization did not apply)", meta.FileSize, total)
	}
	// Stored segment sizes must not exceed true decoded size.
	for i, seg := range meta.SegmentData {
		if seg.SegmentSize > 17_000 {
			t.Errorf("segment[%d].SegmentSize = %d, want ≤17000 (overhead not removed)", i, seg.SegmentSize)
		}
	}
}

// TestImportBattery_MissingSegments_FastFail verifies that when all segments
// return ErrArticleNotFound, ProcessNzbFile returns an error and writes no metadata.
func TestImportBattery_MissingSegments_FastFail(t *testing.T) {
	env := newBatteryEnv(t)

	segs := []nzbbuild.Segment{
		{ID: "missing-001@battery", Bytes: 10_000},
		{ID: "missing-002@battery", Bytes: 10_000},
	}
	env.client.SetBehavior("missing-001@battery", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	env.client.SetBehavior("missing-002@battery", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	nzb := nzbbuild.Build(nzbbuild.File{Subject: "Missing.mkv", Segments: segs})

	_, _, err := env.runImport(nzb, "Missing.mkv")
	if err == nil {
		t.Fatal("expected error when all segments are missing, got nil")
	}
}

// TestImportBattery_RarSinglePart verifies import of a single-volume RAR archive.
// The inner file must be registered in the metadata service with the correct size.
func TestImportBattery_RarSinglePart(t *testing.T) {
	entries := loadManifest(t, "rar_single")
	env := newBatteryEnv(t)

	rarBytes := loadFixture(t, filepath.Join("rar_single", "archive.rar"))
	segs := env.registerContent("rar-single", rarBytes, archivePartSize, 1.0, nil)

	nzb := nzbbuild.Build(nzbbuild.File{Subject: "archive.rar", Segments: segs})
	result, written, err := env.runImport(nzb, "archive")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	if result != "/archive" {
		t.Errorf("result = %q, want /archive", result)
	}
	hasDirMarker := false
	for _, p := range written {
		if p == "DIR:/archive" {
			hasDirMarker = true
		}
	}
	if !hasDirMarker {
		t.Errorf("writtenPaths has no DIR:/archive marker; paths: %v", written)
	}

	inner := env.listDir("/archive")
	if len(inner) == 0 {
		t.Fatal("no inner files written inside /archive")
	}
	assertInnerFile(t, env, "/archive", entries)
}

// TestImportBattery_RarMultiPart verifies import of a multi-volume RAR archive
// (.part01.rar + .part02.rar). The inner payload must be extracted correctly.
func TestImportBattery_RarMultiPart(t *testing.T) {
	entries := loadManifest(t, "rar_multi")
	env := newBatteryEnv(t)

	part01 := loadFixture(t, filepath.Join("rar_multi", "archive.part01.rar"))
	part02 := loadFixture(t, filepath.Join("rar_multi", "archive.part02.rar"))

	segs01 := env.registerContent("rar-mp-p01", part01, archivePartSize, 1.0, nil)
	segs02 := env.registerContent("rar-mp-p02", part02, archivePartSize, 1.0, nil)

	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: "archive.part01.rar", Segments: segs01},
		nzbbuild.File{Subject: "archive.part02.rar", Segments: segs02},
	)
	result, written, err := env.runImport(nzb, "archive")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	if result != "/archive" {
		t.Errorf("result = %q, want /archive", result)
	}
	hasDirMarker := false
	for _, p := range written {
		if p == "DIR:/archive" {
			hasDirMarker = true
		}
	}
	if !hasDirMarker {
		t.Errorf("writtenPaths has no DIR:/archive marker; paths: %v", written)
	}

	assertInnerFile(t, env, "/archive", entries)

	// Archive extracted files must be written directly in v3 format (this is the
	// path that previously stayed v1 because writtenPaths held only the DIR marker).
	assertV3MetaDir(t, env, "/archive")
}

// TestImportBattery_RarWidthMismatchVolumeNaming imports a multi-volume RAR whose
// volumes use non-fixed-width numbering (archive.part01.rar … part09.rar, then
// part010.rar …). rardecode follows the set by computing fixed-width names
// (…part10.rar) that don't match the NZB's …part010.rar, so BOTH volume following
// (UsenetFileSystem) and the part→segment mapping (convertAggregatedFilesToRarContent)
// must resolve by volume number. Without that, only the first 9 volumes map and the
// import fails with a segment-integrity error — so a passing import here proves the
// whole pipeline handles the width mismatch end to end.
func TestImportBattery_RarWidthMismatchVolumeNaming(t *testing.T) {
	entries := loadManifest(t, "rar_widthmismatch")
	env := newBatteryEnv(t)

	matches, err := filepath.Glob(filepath.Join("testdata", "rar_widthmismatch", "archive.part*.rar"))
	if err != nil {
		t.Fatalf("glob fixtures: %v", err)
	}
	if len(matches) < 10 {
		t.Fatalf("fixture must have >=10 volumes to cross the part09→part010 boundary, got %d", len(matches))
	}

	// Sort by parsed volume number so the NZB lists part01 first and IDs are stable.
	volNum := func(p string) int {
		b := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), "archive.part"), ".rar")
		n, _ := strconv.Atoi(b)
		return n
	}
	sort.Slice(matches, func(i, j int) bool { return volNum(matches[i]) < volNum(matches[j]) })

	var files []nzbbuild.File
	for _, m := range matches {
		name := filepath.Base(m)
		data := loadFixture(t, filepath.Join("rar_widthmismatch", name))
		segs := env.registerContent(fmt.Sprintf("rar-wm-%03d", volNum(m)), data, archivePartSize, 1.0, nil)
		files = append(files, nzbbuild.File{Subject: name, Segments: segs})
	}

	nzb := nzbbuild.Build(files...)
	result, written, err := env.runImport(nzb, "archive")
	if err != nil {
		t.Fatalf("import failed (width-mismatch volume mapping regression): %v", err)
	}
	if result != "/archive" {
		t.Errorf("result = %q, want /archive", result)
	}
	hasDirMarker := false
	for _, p := range written {
		if p == "DIR:/archive" {
			hasDirMarker = true
		}
	}
	if !hasDirMarker {
		t.Errorf("writtenPaths has no DIR:/archive marker; paths: %v", written)
	}

	assertInnerFile(t, env, "/archive", entries)

	// Explicit full-coverage assertion: every volume's segments must be attached, so
	// the summed usable segment bytes must cover the whole inner file. Pre-fix this is
	// only ~9 volumes' worth.
	want := int64(entries[0].Size)
	var covered int64
	for _, name := range env.listDir("/archive") {
		meta := env.readMeta("/archive/" + name)
		if meta.FileSize != want {
			continue
		}
		for _, s := range meta.SegmentData {
			covered += s.EndOffset - s.StartOffset + 1
		}
	}
	if covered < want {
		t.Errorf("segment coverage %d < file size %d — not all volumes mapped", covered, want)
	}
}

// TestImportBattery_SevenZipSingle verifies import of a single-volume 7z archive.
func TestImportBattery_SevenZipSingle(t *testing.T) {
	entries := loadManifest(t, "7z_single")
	env := newBatteryEnv(t)

	sevenZBytes := loadFixture(t, filepath.Join("7z_single", "archive.7z"))
	segs := env.registerContent("7z-single", sevenZBytes, archivePartSize, 1.0, nil)

	nzb := nzbbuild.Build(nzbbuild.File{Subject: "archive.7z", Segments: segs})
	result, written, err := env.runImport(nzb, "archive")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	if result != "/archive" {
		t.Errorf("result = %q, want /archive", result)
	}
	hasDirMarker := false
	for _, p := range written {
		if p == "DIR:/archive" {
			hasDirMarker = true
		}
	}
	if !hasDirMarker {
		t.Errorf("writtenPaths has no DIR:/archive marker; paths: %v", written)
	}

	assertInnerFile(t, env, "/archive", entries)
}

// TestImportBattery_SevenZipMultipart verifies import of a split 7z archive
// (.7z.001 + .7z.002). The inner payload must be extracted correctly.
func TestImportBattery_SevenZipMultipart(t *testing.T) {
	entries := loadManifest(t, "7z_multi")
	env := newBatteryEnv(t)

	part001 := loadFixture(t, filepath.Join("7z_multi", "archive.7z.001"))
	part002 := loadFixture(t, filepath.Join("7z_multi", "archive.7z.002"))

	segs001 := env.registerContent("7z-mp-001", part001, archivePartSize, 1.0, nil)
	segs002 := env.registerContent("7z-mp-002", part002, archivePartSize, 1.0, nil)

	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: "archive.7z.001", Segments: segs001},
		nzbbuild.File{Subject: "archive.7z.002", Segments: segs002},
	)
	result, written, err := env.runImport(nzb, "archive")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	if result != "/archive" {
		t.Errorf("result = %q, want /archive", result)
	}
	hasDirMarker := false
	for _, p := range written {
		if p == "DIR:/archive" {
			hasDirMarker = true
		}
	}
	if !hasDirMarker {
		t.Errorf("writtenPaths has no DIR:/archive marker; paths: %v", written)
	}

	assertInnerFile(t, env, "/archive", entries)
}

// TestImportBattery_BrokenRarSetExcluded verifies that when two RAR sets are
// present but one has all segments unavailable, the healthy set is imported
// without error and the broken set is silently excluded.
func TestImportBattery_BrokenRarSetExcluded(t *testing.T) {
	entries := loadManifest(t, "rar_multi")
	env := newBatteryEnv(t)

	// Healthy set — use the real fixture bytes.
	part01 := loadFixture(t, filepath.Join("rar_multi", "archive.part01.rar"))
	part02 := loadFixture(t, filepath.Join("rar_multi", "archive.part02.rar"))
	segs01 := env.registerContent("broken-healthy-p01", part01, archivePartSize, 1.0, nil)
	segs02 := env.registerContent("broken-healthy-p02", part02, archivePartSize, 1.0, nil)

	// Broken set — all segments return ErrArticleNotFound.
	badSegs1 := []nzbbuild.Segment{
		{ID: "broken-bad-p01-001@battery", Bytes: 10_000},
		{ID: "broken-bad-p01-002@battery", Bytes: 10_000},
	}
	badSegs2 := []nzbbuild.Segment{
		{ID: "broken-bad-p02-001@battery", Bytes: 10_000},
	}
	for _, id := range []string{
		"broken-bad-p01-001@battery", "broken-bad-p01-002@battery",
		"broken-bad-p02-001@battery",
	} {
		env.client.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	}

	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: "archive.part01.rar", Segments: segs01},
		nzbbuild.File{Subject: "archive.part02.rar", Segments: segs02},
		nzbbuild.File{Subject: "badset.part01.rar", Segments: badSegs1},
		nzbbuild.File{Subject: "badset.part02.rar", Segments: badSegs2},
	)

	result, written, err := env.runImport(nzb, "archive")
	if err != nil {
		t.Fatalf("expected success when healthy set present, got: %v", err)
	}
	if result != "/archive" {
		t.Errorf("result = %q, want /archive", result)
	}
	hasDirMarker := false
	for _, p := range written {
		if p == "DIR:/archive" {
			hasDirMarker = true
		}
	}
	if !hasDirMarker {
		t.Errorf("DIR:/archive marker missing from writtenPaths: %v", written)
	}

	assertInnerFile(t, env, "/archive", entries)
}

// TestImportBattery_AllRarSetsBroken verifies that when every RAR set in an NZB
// has unavailable segments, ProcessNzbFile returns an error wrapping
// multifile.ErrNoFilesProcessed.
func TestImportBattery_AllRarSetsBroken(t *testing.T) {
	env := newBatteryEnv(t)

	brokenSegs := func(prefix string, n int) []nzbbuild.Segment {
		segs := make([]nzbbuild.Segment, n)
		for i := range segs {
			id := prefix + "-" + string(rune('0'+i)) + "@battery"
			segs[i] = nzbbuild.Segment{ID: id, Bytes: 10_000}
			env.client.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
		}
		return segs
	}

	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: "set1.part01.rar", Segments: brokenSegs("allbroken-s1p1", 2)},
		nzbbuild.File{Subject: "set1.part02.rar", Segments: brokenSegs("allbroken-s1p2", 2)},
	)

	_, _, err := env.runImport(nzb, "set1")
	if err == nil {
		t.Fatal("expected error when all RAR sets are broken, got nil")
	}
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Errorf("err = %v, want to wrap multifile.ErrNoFilesProcessed", err)
	}
}

// TestImportBattery_EmptyNzb verifies that a completely empty NZB (no file entries)
// causes ProcessNzbFile to return an error.
func TestImportBattery_EmptyNzb(t *testing.T) {
	env := newBatteryEnv(t)
	nzb := nzbbuild.Build() // no files

	_, _, err := env.runImport(nzb, "Empty")
	if err == nil {
		t.Fatal("expected error for empty NZB, got nil")
	}
}

// assertInnerFile checks that the virtual directory contains at least one inner
// file whose size matches the first manifest entry. The archive processor may
// rename inner files (e.g. obfuscation-based normalization), so only the size
// is asserted — not the exact filename.
// assertV3MetaDir walks a virtual directory and asserts every .meta on disk is in
// v3 store-backed format (starts with the 5-byte magic). Recurses into subdirs so
// archive releases that place files under an inner folder are fully covered.
func assertV3MetaDir(t *testing.T, env *batteryEnv, dir string) {
	t.Helper()
	v3Magic := []byte{0x00, 'A', 'M', '3', 0x01}
	root := filepath.Join(env.svc.GetMetadataDirectoryPath(dir))
	count := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".meta") {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Errorf("read %q: %v", path, readErr)
			return nil
		}
		count++
		if len(raw) < 5 || !bytes.Equal(raw[:5], v3Magic) {
			t.Errorf("meta %q is not v3 (got %x); direct v3 write did not run", path, raw[:min(5, len(raw))])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %q: %v", root, err)
	}
	if count == 0 {
		t.Fatalf("no .meta files found under %q", dir)
	}
}

func assertInnerFile(t *testing.T, env *batteryEnv, dir string, entries []fixtureEntry) {
	t.Helper()
	inner := env.listDir(dir)
	if len(inner) == 0 {
		t.Fatalf("no inner files in %q", dir)
	}

	want := int64(entries[0].Size)
	for _, name := range inner {
		meta := env.readMeta(dir + "/" + name)
		if meta.FileSize == want {
			return
		}
	}
	// Collect sizes for a clear error message.
	sizes := make([]int64, 0, len(inner))
	for _, name := range inner {
		meta := env.readMeta(dir + "/" + name)
		sizes = append(sizes, meta.FileSize)
	}
	t.Errorf("no inner file with size %d in %q; found files %v with sizes %v", want, dir, inner, sizes)
}

// TestImportBattery_NzbzStoredInConfigDir verifies that the .nzbz companion store is
// written to configDir/.nzbs/{category}/ and NOT co-located with the .nzb temp file.
func TestImportBattery_NzbzStoredInConfigDir(t *testing.T) {
	env := newBatteryEnv(t)

	content := bytes.Repeat([]byte("Z"), 30_000)
	segs := env.registerContent("nzbz-location", content, 10_000, 1.0, nil)
	nzb := nzbbuild.Build(nzbbuild.File{Subject: "ConfigDirStore.mkv", Segments: segs})

	const queueID = 42
	const category = "Movies"

	_, _, err := env.runImportWithCategory(nzb, "ConfigDirStore.mkv", queueID, category)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	// The .nzbz must live under configDir/.nzbs/Movies/.
	// The NZB is written as "ConfigDirStore.mkv.nzb" so TrimNzbExtension yields "ConfigDirStore.mkv".
	expectedDir := filepath.Join(env.configDir, ".nzbs", category)
	expectedName := fmt.Sprintf("%d-ConfigDirStore.mkv.nzbz", queueID)
	expectedPath := filepath.Join(expectedDir, expectedName)

	if _, statErr := os.Stat(expectedPath); os.IsNotExist(statErr) {
		// List what is actually in expectedDir for a helpful error.
		entries, _ := os.ReadDir(expectedDir)
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf(".nzbz not found at %q; files in dir: %v", expectedPath, names)
	}

	// Confirm the .nzbz is NOT in the OS temp dir (old co-location behaviour).
	nzbzInTemp, _ := filepath.Glob(filepath.Join(os.TempDir(), "*", "*.nzbz"))
	for _, p := range nzbzInTemp {
		if filepath.Base(p) == expectedName {
			t.Errorf(".nzbz unexpectedly found in temp dir: %s", p)
		}
	}
}

// TestImportBattery_RarSidecarFileTrackedInWrittenPaths imports a RAR set alongside a
// subtitle sidecar. In a RAR NZB every non-par2, non-sidecar file is marked as an archive
// part, so the sidecar extensions are the only files that reach ProcessRegularFiles — and
// only once one of them is allowed by config, as ".srt" is here. Those writes must land in
// writtenPaths: rollback removes a release folder only after every path this job wrote is
// gone, so an untracked sidecar both survives the rollback and keeps the folder from being
// cleaned up at all.
func TestImportBattery_RarSidecarFileTrackedInWrittenPaths(t *testing.T) {
	entries := loadManifest(t, "rar_single")
	env := newBatteryEnv(t)
	env.cfg.Import.AllowedFileExtensions = append(env.cfg.Import.AllowedFileExtensions, ".srt")

	rarBytes := loadFixture(t, filepath.Join("rar_single", "archive.rar"))
	rarSegs := env.registerContent("rar-sidecar", rarBytes, archivePartSize, 1.0, nil)
	srtSegs := env.registerContent("rar-sidecar-srt", []byte("1\n00:00:01,000 --> 00:00:02,000\nhi\n"), archivePartSize, 1.0, nil)

	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: "archive.rar", Segments: rarSegs},
		nzbbuild.File{Subject: "archive.srt", Segments: srtSegs},
	)
	result, written, err := env.runImport(nzb, "archive")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if result != "/archive" {
		t.Errorf("result = %q, want /archive", result)
	}

	// The sidecar must actually have been imported, or this proves nothing.
	inner := env.listDir("/archive")
	if !slices.Contains(inner, "archive.srt") {
		t.Fatalf("sidecar was not imported into /archive, got %v", inner)
	}

	paths := filePaths(written)
	if !slices.Contains(paths, "/archive/archive.srt") {
		t.Errorf("writtenPaths is missing the sidecar /archive/archive.srt; paths: %v", paths)
	}
	if !slices.Contains(paths, "/archive/archive.bin") {
		t.Errorf("writtenPaths is missing the extracted archive file; paths: %v", paths)
	}
	assertInnerFile(t, env, "/archive", entries)
}
