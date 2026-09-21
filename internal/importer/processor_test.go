package importer

import (
	"context"
	"errors"
	"fmt"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/importer/validation"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/importer/filesystem"
	"github.com/kipsilabs/altmount/internal/importer/multifile"
	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

type processorTestPoolManager struct {
	client *fakepool.Client
}

func (m processorTestPoolManager) GetPool() (pool.NntpClient, error) { return m.client, nil }
func (m processorTestPoolManager) SetProviders([]nntppool.Provider) error {
	return nil
}
func (m processorTestPoolManager) ClearPool() error { return nil }
func (m processorTestPoolManager) HasPool() bool    { return m.client != nil }
func (m processorTestPoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}
func (m processorTestPoolManager) ResetMetrics(context.Context, bool, bool) error { return nil }
func (m processorTestPoolManager) ResetProviderErrors(context.Context) error      { return nil }
func (m processorTestPoolManager) IncArticlesDownloaded()                         {}
func (m processorTestPoolManager) UpdateDownloadProgress(string, int64)           {}
func (m processorTestPoolManager) IncArticlesPosted()                             {}
func (m processorTestPoolManager) AddProvider(nntppool.Provider) error            { return nil }
func (m processorTestPoolManager) RemoveProvider(string) error                    { return nil }
func (m processorTestPoolManager) ResetProviderQuota(context.Context, string) error {
	return nil
}
func (m processorTestPoolManager) SetProviderIDs(map[string]string) {}
func (m processorTestPoolManager) AcquireImportSlot(context.Context) (func(), error) {
	return func() {}, nil
}
func (m processorTestPoolManager) SetAdmissionCap(int) {}
func (m processorTestPoolManager) AcquireImportConnection(context.Context) (func(), error) {
	return func() {}, nil
}
func (m processorTestPoolManager) SetImportConnCapacity(int)                 {}
func (m processorTestPoolManager) ImportConnCapacity() int                   { return 0 }
func (m processorTestPoolManager) SetStreamSource(pool.StreamActivitySource) {}
func (m processorTestPoolManager) NotifyStreamChange()                       {}
func (m processorTestPoolManager) StatSweepConcurrency(conservative int) int { return conservative }

func TestPreParseFastFailSkipsOnlyMissingEpisode(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("missing-segment", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	n := buildTestNzb([]testNzbFile{
		{name: "Show.S01E01.mkv", segID: "healthy-segment"},
		{name: "Show.S01E02.mkv", segID: "missing-segment"},
		{name: "Show.S01E01.par2", segID: "par2-segment"},
	})
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100

	brokenIdx, missingIDs, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("preParseFastFail returned error: %v", err)
	}

	// Only file index 1 (the missing episode) should be broken.
	if len(brokenIdx) != 1 {
		t.Fatalf("brokenIdx len = %d, want 1", len(brokenIdx))
	}
	if _, ok := brokenIdx[1]; !ok {
		t.Error("brokenIdx missing index 1 (Show.S01E02.mkv)")
	}
	// The missing segment ID must be in missingIDs.
	if _, ok := missingIDs["missing-segment"]; !ok {
		t.Error("missingIDs missing 'missing-segment'")
	}
}

func (m processorTestPoolManager) SetStreamHeadroom(int)                      {}
func (m processorTestPoolManager) SpeculativeBudget() *pool.SpeculativeBudget { return nil }

// TestPreParseFastFailDoesNotStatPar2 verifies PAR2 segments are skipped entirely
// from the fast-fail Stat sweep: an unreachable PAR2 segment must neither be
// Stat-checked nor mark the import broken.
// TestPreParseFastFailMarksWholeRarSetBroken verifies that one unreachable part
// dooms its entire RAR set (all parts in brokenIdx) without touching a healthy
// sibling set.
func TestPreParseFastFailMarksWholeRarSetBroken(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("a2-missing", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	n := buildTestNzb([]testNzbFile{
		{name: "setA.part01.rar", segID: "a1"},
		{name: "setA.part02.rar", segID: "a2-missing"},
		{name: "setB.part01.rar", segID: "b1"},
		{name: "setB.part02.rar", segID: "b2"},
	})
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100

	brokenIdx, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("preParseFastFail returned error: %v", err)
	}
	if len(brokenIdx) != 2 {
		t.Fatalf("brokenIdx = %v, want both setA parts (indexes 0,1)", brokenIdx)
	}
	for _, idx := range []int{0, 1} {
		if _, ok := brokenIdx[idx]; !ok {
			t.Errorf("brokenIdx missing index %d (setA part)", idx)
		}
	}
	for _, idx := range []int{2, 3} {
		if _, ok := brokenIdx[idx]; ok {
			t.Errorf("brokenIdx contains index %d (healthy setB part), want absent", idx)
		}
	}
}

// TestPreParseFastFailAllRarSetsBrokenReturnsNoFilesProcessed verifies the
// logical-unit early exit: when every RAR set has a missing part, the import
// fails before parsing.
func TestPreParseFastFailAllRarSetsBrokenReturnsNoFilesProcessed(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("a2-missing", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	client.SetBehavior("b2-missing", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	n := buildTestNzb([]testNzbFile{
		{name: "setA.part01.rar", segID: "a1"},
		{name: "setA.part02.rar", segID: "a2-missing"},
		{name: "setB.part01.rar", segID: "b1"},
		{name: "setB.part02.rar", segID: "b2-missing"},
	})
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100

	_, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Fatalf("preParseFastFail error = %v, want ErrNoFilesProcessed", err)
	}
}

func TestPreParseFastFailDoesNotStatPar2(t *testing.T) {
	client := fakepool.New()
	// The PAR2 segment would error if it were ever Stat-checked.
	client.SetBehavior("par2-segment", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	n := buildTestNzb([]testNzbFile{
		{name: "Show.S01E01.mkv", segID: "healthy-segment"},
		{name: "Show.S01E01.par2", segID: "par2-segment"},
	})
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100

	brokenIdx, missingIDs, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("preParseFastFail returned error: %v", err)
	}
	if len(brokenIdx) != 0 {
		t.Fatalf("brokenIdx = %v, want empty (PAR2 must not break the import)", brokenIdx)
	}
	if len(missingIDs) != 0 {
		t.Fatalf("missingIDs = %v, want empty", missingIDs)
	}
	// Only the media file's single segment should have been Stat-checked.
	if got := client.StatCalls(); got != 1 {
		t.Fatalf("StatCalls = %d, want 1 (PAR2 segment must not be Stat-checked)", got)
	}
}

func TestPreParseFastFailAllMissingReturnsNoFilesProcessed(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	n := buildTestNzb([]testNzbFile{
		{name: "Show.S01E01.mkv", segID: "missing-1"},
		{name: "Show.S01E02.mkv", segID: "missing-2"},
		{name: "Show.S01E01.par2", segID: "par2-segment"},
	})
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100

	_, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Fatalf("preParseFastFail error = %v, want ErrNoFilesProcessed", err)
	}
}

// TestPreParseFastFailHealthyReleaseSkipsPerFileSweep verifies the phase-1
// release probe short-circuits on a healthy release: it Stats only a bounded
// sample (≤55) regardless of file count and never escalates to the per-file
// sweep, so the "Checking segment availability" stage stays cheap.
func TestPreParseFastFailHealthyReleaseSkipsPerFileSweep(t *testing.T) {
	client := fakepool.New() // all segments reachable by default
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}

	const fileCount = 100
	files := make([]testNzbFile, fileCount)
	for i := range files {
		files[i] = testNzbFile{
			name:  fmt.Sprintf("Show.S01E%02d.mkv", i),
			segID: fmt.Sprintf("seg-%d", i),
		}
	}
	n := buildTestNzb(files)
	cfg := config.DefaultConfig() // default 1% sampling, capped at 55

	brokenIdx, missingIDs, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("preParseFastFail returned error: %v", err)
	}
	if brokenIdx != nil {
		t.Errorf("brokenIdx = %v, want nil (healthy release)", brokenIdx)
	}
	if missingIDs != nil {
		t.Errorf("missingIDs = %v, want nil (healthy release)", missingIDs)
	}
	// The probe samples the whole release once (capped at 55). With no missing
	// segment it must NOT escalate, so total Stats stay well under fileCount.
	if got := client.StatCalls(); got > 55 {
		t.Errorf("StatCalls = %d, want ≤55 (release probe only, no per-file sweep)", got)
	}
}

// TestFastFailUsesStatPipelineDepthNotConnections pins the fast-fail sweep's
// concurrency to the providers' STAT pipeline depth rather than their
// connection count: STAT carries no body, so nntppool pipelines many per
// connection (Provider.StatInflight). A two-connection pool must not throttle
// the sweep to two in-flight STATs.
func TestFastFailUsesStatPipelineDepthNotConnections(t *testing.T) {
	enabled := true
	cfg := &config.Config{Providers: []config.ProviderConfig{
		{MaxConnections: 2, StatInflightRequests: 100, Enabled: &enabled},
	}}

	if got := cfg.TotalProviderConnections(); got != 2 {
		t.Fatalf("fixture TotalProviderConnections = %d, want 2", got)
	}
	if got := cfg.StatConcurrency(); got != 100 {
		t.Fatalf("StatConcurrency = %d, want 100 (the configured pipeline depth)", got)
	}
}

func TestProcessMultiFilePreservesReleaseFolderWhenOnlyOneFileRemains(t *testing.T) {
	client := fakepool.New()
	metaRoot := t.TempDir()
	cfg := config.DefaultConfig()
	proc := &Processor{
		metadataService:   metadata.NewMetadataService(metaRoot),
		poolManager:       processorTestPoolManager{client: client},
		configGetter:      func() *config.Config { return cfg },
		validationTimeout: 100 * time.Millisecond,
	}

	result, writtenPaths, err := proc.processMultiFile(
		context.Background(),
		"tv/Show/Season 01",
		[]parser.ParsedFile{processorTestParsedFile("Show.S01E01.mkv", "healthy-segment")},
		nil,
		"Show.S01.nzb",
		1,
		[]string{".mkv"},
		nil,
		nil,
		nil,
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("processMultiFile returned error: %v", err)
	}

	if result != "tv/Show/Season 01/Show.S01" {
		t.Fatalf("result = %q, want release folder", result)
	}
	wantPath := "tv/Show/Season 01/Show.S01/Show.S01E01.mkv"
	if len(writtenPaths) != 1 || writtenPaths[0] != wantPath {
		t.Fatalf("writtenPaths = %v, want %q", writtenPaths, wantPath)
	}
	if _, err := os.Stat(filepath.Join(metaRoot, wantPath+".meta")); err != nil {
		t.Fatalf("metadata for surviving episode not written in release folder: %v", err)
	}
}

func processorTestParsedFile(filename, segmentID string) parser.ParsedFile {
	return parser.ParsedFile{
		Filename: filename,
		Size:     100,
		Segments: []*metapb.SegmentData{
			{Id: segmentID, StartOffset: 0, EndOffset: 99},
		},
		ReleaseDate: time.Unix(1, 0),
	}
}

func TestApplyNzbRename(t *testing.T) {
	tests := []struct {
		name             string
		renameToNzbName  bool
		nzbName          string
		originalFilename string
		expected         string
	}{
		{
			name:             "false: single file not renamed",
			renameToNzbName:  false,
			nzbName:          "release.nzb",
			originalFilename: "obfuscated.mkv",
			expected:         "obfuscated.mkv",
		},
		{
			name:             "true: single file renamed",
			renameToNzbName:  true,
			nzbName:          "release.nzb",
			originalFilename: "obfuscated.mkv",
			expected:         "release.mkv",
		},
		{
			name:             "false: preserves path with subdirectory",
			renameToNzbName:  false,
			nzbName:          "movie.nzb",
			originalFilename: "sub/file.mkv",
			expected:         "sub/file.mkv",
		},
		{
			name:             "true: clean filename is kept",
			renameToNzbName:  true,
			nzbName:          "release.nzb",
			originalFilename: "My.Movie.2024.PROPER.1080p.mkv",
			expected:         "My.Movie.2024.PROPER.1080p.mkv",
		},
		{
			name:             "true: hash filename renamed",
			renameToNzbName:  true,
			nzbName:          "release.nzb",
			originalFilename: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv",
			expected:         "release.mkv",
		},
		{
			name:             "true: renames leaf only, preserves subdirectory",
			renameToNzbName:  true,
			nzbName:          "movie.nzb",
			originalFilename: "sub/file.mkv",
			expected:         "sub/movie.mkv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := []parser.ParsedFile{{Filename: tt.originalFilename}}
			result := applyNzbRename(tt.renameToNzbName, tt.nzbName, files)
			if result[0].Filename != tt.expected {
				t.Fatalf("applyNzbRename(%v, %q, [{%q}]) = %q, want %q",
					tt.renameToNzbName, tt.nzbName, tt.originalFilename, result[0].Filename, tt.expected)
			}
		})
	}
}

func TestNormalizeReleaseFilename(t *testing.T) {
	tests := []struct {
		name             string
		nzbFilename      string
		originalFilename string
		expected         string
	}{
		{
			name:             "keeps single extension",
			nzbFilename:      "file.mkv.nzb",
			originalFilename: "file.mkv",
			expected:         "file.mkv",
		},
		{
			name:             "adds missing extension from original",
			nzbFilename:      "obfuscated.nzb",
			originalFilename: "video.mkv",
			expected:         "obfuscated.mkv",
		},
		{
			name:             "avoids duplicate when nzb already has ext",
			nzbFilename:      "[TEST].mp4.nzb",
			originalFilename: "random.mp4",
			expected:         "[TEST].mp4",
		},
		{
			name:             "preserves nzb basename but uses original ext",
			nzbFilename:      "movie.1080p.NZB",
			originalFilename: "source.mkv",
			expected:         "movie.1080p.mkv",
		},
		{
			name:             "no extension in original",
			nzbFilename:      "sample.nzb",
			originalFilename: "filename",
			expected:         "sample",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeReleaseFilename(tt.nzbFilename, tt.originalFilename)
			if got != tt.expected {
				t.Fatalf("normalizeReleaseFilename(%q, %q) = %q, want %q", tt.nzbFilename, tt.originalFilename, got, tt.expected)
			}
		})
	}
}

func TestNormalizeSingleFileVirtualDir(t *testing.T) {
	tests := []struct {
		name        string
		virtualDir  string
		releaseName string
		filename    string
		expected    string
	}{
		{
			name:        "keeps season folder",
			virtualDir:  "/Media/Animes/Show/Season 01",
			releaseName: "Show.S01E01.1080p",
			filename:    "Show.S01E01.1080p.mkv",
			expected:    "/Media/Animes/Show/Season 01",
		},
		{
			name:        "flattens when ends with filename",
			virtualDir:  "/Media/Animes/Show/Season 01/Show.S01E01.1080p.mkv",
			releaseName: "Show.S01E01.1080p",
			filename:    "Show.S01E01.1080p.mkv",
			expected:    "/Media/Animes/Show/Season 01",
		},
		{
			name:        "flattens when ends with release name",
			virtualDir:  "/Media/Animes/Show/Season 01/Show.S01E01.1080p",
			releaseName: "Show.S01E01.1080p",
			filename:    "episode.mkv",
			expected:    "/Media/Animes/Show/Season 01",
		},
		{
			name:        "root stays root",
			virtualDir:  "/",
			releaseName: "Anything",
			filename:    "file.mkv",
			expected:    "/",
		},
		{
			name:        "does not flatten when file has path",
			virtualDir:  "/Media/Animes/Show/Season 01",
			releaseName: "Show.S01E01.1080p",
			filename:    "sub/Show.S01E01.1080p.mkv",
			expected:    "/Media/Animes/Show/Season 01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeSingleFileVirtualDir(tt.virtualDir, tt.releaseName, tt.filename)
			if got != tt.expected {
				t.Fatalf("normalizeSingleFileVirtualDir(%q, %q, %q) = %q, want %q", tt.virtualDir, tt.releaseName, tt.filename, got, tt.expected)
			}
		})
	}
}

func TestDetermineFileLocation(t *testing.T) {
	tests := []struct {
		name           string
		filename       string
		baseDir        string
		expectedParent string
		expectedName   string
	}{
		{
			name:           "simple file",
			filename:       "movie.mkv",
			baseDir:        "/base",
			expectedParent: "/base",
			expectedName:   "movie.mkv",
		},
		{
			name:           "nested file",
			filename:       "folder/movie.mkv",
			baseDir:        "/base",
			expectedParent: "/base/folder",
			expectedName:   "movie.mkv",
		},
		{
			name:           "redundant folder (exact match)",
			filename:       "movie.mkv/movie.mkv",
			baseDir:        "/base",
			expectedParent: "/base",
			expectedName:   "movie.mkv",
		},
		{
			name:           "redundant folder with leading slash",
			filename:       "/movie.mkv/movie.mkv",
			baseDir:        "/base",
			expectedParent: "/base",
			expectedName:   "movie.mkv",
		},
		{
			name:           "redundant folder with backslashes",
			filename:       `movie.mkv\\movie.mkv`,
			baseDir:        "/base",
			expectedParent: "/base",
			expectedName:   "movie.mkv",
		},
		{
			name:           "nested redundant folder",
			filename:       "series/season1/episode1.mkv/episode1.mkv",
			baseDir:        "/base",
			expectedParent: "/base/series/season1",
			expectedName:   "episode1.mkv",
		},
		{
			name:           "non-redundant folder (almost match)",
			filename:       "movie/movie.mkv",
			baseDir:        "/base",
			expectedParent: "/base",
			expectedName:   "movie.mkv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := parser.ParsedFile{Filename: tt.filename}
			parent, name := filesystem.DetermineFileLocation(file, tt.baseDir)
			if parent != tt.expectedParent {
				t.Errorf("DetermineFileLocation parent = %q, want %q", parent, tt.expectedParent)
			}
			if name != tt.expectedName {
				t.Errorf("DetermineFileLocation name = %q, want %q", name, tt.expectedName)
			}
		})
	}
}

type testNzbFile struct {
	name  string
	segID string
}

func buildTestNzb(files []testNzbFile) *nzbparser.Nzb {
	nzbFiles := make(nzbparser.NzbFiles, len(files))
	for i, f := range files {
		nzbFiles[i] = nzbparser.NzbFile{
			Filename: f.name,
			Subject:  f.name,
			Segments: nzbparser.NzbSegments{
				{Bytes: 100, Number: 1, ID: f.segID},
			},
		}
	}
	return &nzbparser.Nzb{Files: nzbFiles}
}

// buildMultiSegmentNzb builds a single video file with segCount segments,
// where the indices in missing yield ErrArticleNotFound on the client.
func buildMultiSegmentNzb(client *fakepool.Client, fileName string, segCount int, missing ...int) *nzbparser.Nzb {
	missingSet := make(map[int]struct{}, len(missing))
	for _, m := range missing {
		missingSet[m] = struct{}{}
	}
	segs := make(nzbparser.NzbSegments, segCount)
	for i := range segs {
		id := fmt.Sprintf("%s-seg-%d", fileName, i)
		segs[i] = nzbparser.NzbSegment{Bytes: 100, Number: i + 1, ID: id}
		if _, ok := missingSet[i]; ok {
			client.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
		}
	}
	return &nzbparser.Nzb{Files: nzbparser.NzbFiles{{
		Filename: fileName,
		Subject:  fileName,
		Segments: segs,
	}}}
}

// TestPreParseFastFailAcceptableThresholdImportsDegradedVideo verifies a
// standalone video file with a small hole imports (not broken) once the
// acceptable-missing threshold is raised above the actual damage.
func TestPreParseFastFailAcceptableThresholdImportsDegradedVideo(t *testing.T) {
	client := fakepool.New()
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	// 50 segments, 1 missing (2%) — well within the pad caps.
	n := buildMultiSegmentNzb(client, "Movie.2024.mkv", 50, 25)
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100
	cfg.Health.AcceptableMissingSegmentsPercentage = 5

	brokenIdx, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("preParseFastFail returned error: %v", err)
	}
	if len(brokenIdx) != 0 {
		t.Fatalf("brokenIdx = %v, want empty (within acceptable-missing threshold)", brokenIdx)
	}
}

// TestPreParseFastFailZeroToleranceFailsDegradedVideo verifies the same file
// is broken when the acceptable-missing threshold is set to 0 (issue #813).
func TestPreParseFastFailZeroToleranceFailsDegradedVideo(t *testing.T) {
	client := fakepool.New()
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	n := buildMultiSegmentNzb(client, "Movie.2024.mkv", 50, 25)
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100
	cfg.Health.AcceptableMissingSegmentsPercentage = 0

	brokenIdx, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		// Single file, and it's broken → all eligible broken → ErrNoFilesProcessed.
		t.Fatalf("zero tolerance: err = %v, want ErrNoFilesProcessed", err)
	}
	_ = brokenIdx
}

// TestPreParseFastFailAcceptableThresholdStillFailsLongRun verifies a raised
// acceptable-missing threshold does NOT rescue a file whose missing run
// exceeds the pad cap.
func TestPreParseFastFailAcceptableThresholdStillFailsLongRun(t *testing.T) {
	client := fakepool.New()
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	// 200 segments, a run of 5 consecutive missing (exceeds MaxPadRunSegments=4).
	n := buildMultiSegmentNzb(client, "Movie.2024.mkv", 50, 20, 21, 22, 23, 24)
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100
	cfg.Health.AcceptableMissingSegmentsPercentage = 100

	_, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Fatalf("raised threshold with long run: err = %v, want ErrNoFilesProcessed", err)
	}
}

func TestPreParseFastFailStremioHeaderOnly(t *testing.T) {
	client := fakepool.New()
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	n := buildMultiSegmentNzb(client, "StremioMovie.mkv", 50, 0)
	cfg := config.DefaultConfig()
	stremioCat := "stremio"

	_, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, &stremioCat, nil)
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Fatalf("stremio header fast-fail: err = %v, want ErrNoFilesProcessed", err)
	}
}

func TestCalculateVirtualDirectory(t *testing.T) {
	tests := []struct {
		name         string
		nzbPath      string
		relativePath string
		expected     string
	}{
		{
			name:         "file in root of relative path",
			nzbPath:      "/downloads/sonarr/Movie.mkv",
			relativePath: "/downloads/sonarr",
			expected:     "/",
		},
		{
			name:         "file in subfolder",
			nzbPath:      "/downloads/sonarr/MovieFolder/Movie.mkv",
			relativePath: "/downloads/sonarr",
			expected:     "/MovieFolder",
		},
		{
			name:         "empty relative path",
			nzbPath:      "/downloads/Movie.mkv",
			relativePath: "",
			expected:     "/",
		},
		{
			name:         "file with spaces",
			nzbPath:      "/downloads/sonarr/Movie Name (2023).mkv",
			relativePath: "/downloads/sonarr",
			expected:     "/",
		},
		{
			name:         "file in persistent .nzbs directory",
			nzbPath:      "/config/.nzbs/MovieFolder/Movie.nzb",
			relativePath: "/config",
			expected:     "/MovieFolder",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filesystem.CalculateVirtualDirectory(tt.nzbPath, tt.relativePath)
			if result != tt.expected {
				t.Errorf("CalculateVirtualDirectory(%q, %q) = %q, want %q", tt.nzbPath, tt.relativePath, result, tt.expected)
			}
		})
	}
}

func TestKnownHoleRunsCollapsesMissingIndices(t *testing.T) {
	segs := []*metapb.SegmentData{
		{Id: "a"}, {Id: "b"}, {Id: "c"}, {Id: "d"}, {Id: "e"}, {Id: "f"},
	}
	missing := map[string]struct{}{"b": {}, "c": {}, "e": {}, "zzz": {}}
	runs := knownHoleRuns(segs, missing)
	if len(runs) != 2 {
		t.Fatalf("runs = %+v, want 2 runs", runs)
	}
	if runs[0].Start != 1 || runs[0].Count != 2 {
		t.Fatalf("runs[0] = %+v, want {1 2}", runs[0])
	}
	if runs[1].Start != 4 || runs[1].Count != 1 {
		t.Fatalf("runs[1] = %+v, want {4 1}", runs[1])
	}
	if got := knownHoleRuns(segs, nil); len(got) != 0 {
		t.Fatalf("runs with no missing ids = %+v, want none", got)
	}
}

func TestFastFailDamageIsDegradedJudgesGapsExactly(t *testing.T) {
	const segSize = 750000
	mk := func(n int, gaps ...int) []*metapb.SegmentData {
		segs := make([]*metapb.SegmentData, n)
		for i := range segs {
			segs[i] = &metapb.SegmentData{Id: fmt.Sprintf("s%d", i)}
		}
		for _, g := range gaps {
			segs[g] = &metapb.SegmentData{Id: holes.PlaceholderID(g+1, "salt")}
		}
		return segs
	}
	fileBytes := int64(2000 * segSize)

	// Two isolated gaps in a 2000-segment file: degraded.
	segs := mk(2000, 100, 1400)
	res := validation.FastFailFileResult{Broken: true, KnownGapCount: 2,
		MissingSegmentIDs: []string{segs[100].Id, segs[1400].Id}}
	if !fastFailDamageIsDegraded(segs, res, fileBytes, 5) {
		t.Fatal("two isolated gaps must be degraded, not failed")
	}

	// A run of six consecutive gaps blows the run cap.
	segs = mk(2000, 100, 101, 102, 103, 104, 105)
	ids := make([]string, 0, 6)
	for i := 100; i <= 105; i++ {
		ids = append(ids, segs[i].Id)
	}
	res = validation.FastFailFileResult{Broken: true, KnownGapCount: 6, MissingSegmentIDs: ids}
	if fastFailDamageIsDegraded(segs, res, fileBytes, 5) {
		t.Fatal("a six-segment gap run must fail the file")
	}

	// A zero-tolerance ceiling refuses even one gap.
	segs = mk(2000, 100)
	res = validation.FastFailFileResult{Broken: true, KnownGapCount: 1, MissingSegmentIDs: []string{segs[100].Id}}
	if fastFailDamageIsDegraded(segs, res, fileBytes, 0) {
		t.Fatal("acceptable-missing 0 must refuse a gap")
	}

	// Nothing missing at all is not "degraded".
	if fastFailDamageIsDegraded(mk(10), validation.FastFailFileResult{}, fileBytes, 5) {
		t.Fatal("no damage must not be reported as degraded")
	}
}

func TestClassifyDeclaredGaps(t *testing.T) {
	const seg = int64(700000)
	fileBytes := int64(7772) * seg
	spread := func(n int) []holes.Run {
		runs := make([]holes.Run, n)
		for i := range runs {
			runs[i] = holes.Run{Start: i * 50, Count: 1}
		}
		return runs
	}
	if got := classifyDeclaredGaps(nil, fileBytes, seg); got != holes.VerdictClean {
		t.Fatalf("no gaps = %v, want clean", got)
	}
	if got := classifyDeclaredGaps(spread(84), fileBytes, seg); got != holes.VerdictDegraded {
		t.Fatalf("84 single-segment gaps over 7772 (1.1%%) = %v, want degraded", got)
	}
	if got := classifyDeclaredGaps(spread(130), fileBytes, seg); got != holes.VerdictFailed {
		t.Fatalf("130 gaps = %v, want failed (cumulative cap)", got)
	}
	if got := classifyDeclaredGaps([]holes.Run{{Start: 10, Count: 5}}, fileBytes, seg); got != holes.VerdictFailed {
		t.Fatalf("five-segment run = %v, want failed (run cap)", got)
	}
	if got := classifyDeclaredGaps(spread(20), 500*seg, seg); got != holes.VerdictFailed {
		t.Fatalf("20 gaps in a 500-segment file (4%%) = %v, want failed (ratio cap)", got)
	}
}

// buildGappedRarSetNzb builds a stored RAR set of volumeCount volumes with
// segsPerVolume segments each, all reachable on the client, and replaces the
// given 1-based segment numbers of gapVolume with declared-gap placeholders.
func buildGappedRarSetNzb(volumeCount, segsPerVolume, gapVolume int, gapNumbers ...int) (*nzbparser.Nzb, []string) {
	gaps := make(map[int]struct{}, len(gapNumbers))
	for _, g := range gapNumbers {
		gaps[g] = struct{}{}
	}
	var placeholders []string
	files := make(nzbparser.NzbFiles, volumeCount)
	for v := range files {
		name := fmt.Sprintf("release.part%03d.rar", v+1)
		segs := make(nzbparser.NzbSegments, segsPerVolume)
		for j := range segs {
			id := fmt.Sprintf("%s-seg-%d", name, j+1)
			if _, gap := gaps[j+1]; gap && v+1 == gapVolume {
				id = holes.PlaceholderID(j+1, name)
				placeholders = append(placeholders, id)
			}
			segs[j] = nzbparser.NzbSegment{Bytes: 100, Number: j + 1, ID: id}
		}
		files[v] = nzbparser.NzbFile{Filename: name, Subject: name, Segments: segs}
	}
	return &nzbparser.Nzb{Files: files}, placeholders
}

// TestPreParseFastFailDeclaredGapsOnHealthyReleaseSkipsPerFileSweep pins that
// gaps declared by the NZB itself do not buy a per-file STAT sweep: the
// release-wide probe still runs against the provider, and when it passes the
// gap mapping comes from the placeholders alone.
func TestPreParseFastFailDeclaredGapsOnHealthyReleaseSkipsPerFileSweep(t *testing.T) {
	client := fakepool.New() // every real segment reachable
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	const volumes = 100
	n, placeholders := buildGappedRarSetNzb(volumes, 10, 50, 4, 5)
	cfg := config.DefaultConfig() // 2% acceptable missing; 2 gaps of 1000 segments qualify

	brokenIdx, missingIDs, degraded, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("preParseFastFail returned error: %v", err)
	}
	if len(brokenIdx) != 0 {
		t.Errorf("brokenIdx = %v, want empty (declared gaps within the hole caps import degraded)", brokenIdx)
	}
	for _, id := range placeholders {
		if _, ok := missingIDs[id]; !ok {
			t.Errorf("missingIDs lacks placeholder %s", id)
		}
	}
	if _, ok := degraded["release.part050.rar"]; !ok {
		t.Errorf("degradedFiles = %v, want the gapped volume", degraded)
	}
	// The per-file sweep round-robins at least one STAT per volume, so any
	// count at or above the volume count means it ran.
	if got := client.StatCalls(); got == 0 || got >= volumes {
		t.Errorf("StatCalls = %d, want 0 < n < %d (release probe only, no per-file sweep)", got, volumes)
	}
}

// TestPreParseFastFailDegradedVideoReturnsHolesForPersistence pins that a
// release with degraded-but-importable damage and nothing broken still hands
// its missing segments and degraded files back, so the caller can persist the
// known holes and queue repair; an empty brokenIdx must not blank them.
func TestPreParseFastFailDegradedVideoReturnsHolesForPersistence(t *testing.T) {
	client := fakepool.New()
	proc := &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
	n := buildMultiSegmentNzb(client, "Movie.2024.mkv", 50, 25)
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100
	cfg.Health.AcceptableMissingSegmentsPercentage = 5

	brokenIdx, missingIDs, degraded, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("preParseFastFail returned error: %v", err)
	}
	if len(brokenIdx) != 0 {
		t.Fatalf("brokenIdx = %v, want empty", brokenIdx)
	}
	if _, ok := missingIDs["Movie.2024.mkv-seg-25"]; !ok {
		t.Errorf("missingIDs = %v, want the sampled miss", missingIDs)
	}
	if _, ok := degraded["Movie.2024.mkv"]; !ok {
		t.Errorf("degradedFiles = %v, want the degraded video", degraded)
	}
}
