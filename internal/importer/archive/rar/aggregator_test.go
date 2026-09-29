package rar

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/stretchr/testify/require"
)

// TestGroupArchivesByBaseNameOldStyleRollover is a regression test for old-style
// multi-volume RAR sets that roll over past .r99 into .s00…/.t00…/… . Every volume
// must land in a SINGLE group so the full set is handed to rardecode together;
// before the fix, .sNN..zNN failed SetKey and each became its own singleton group,
// starving rardecode of the continuation volumes and truncating the extracted file.
func TestGroupArchivesByBaseNameOldStyleRollover(t *testing.T) {
	names := oldRollSet("movie", 12) // movie.rar, .r00..r99, .s00..s12 (114 volumes)
	files := make([]parser.ParsedFile, len(names))
	for i, n := range names {
		files[i] = parser.ParsedFile{Filename: n}
	}

	groups := GroupArchivesByBaseName(files)

	if len(groups) != 1 {
		t.Fatalf("GroupArchivesByBaseName produced %d groups; want 1 (all volumes in one set)", len(groups))
	}
	if len(groups[0]) != len(names) {
		t.Errorf("group has %d volumes; want %d (no volume dropped)", len(groups[0]), len(names))
	}
}

// mockRarProcessor is a test double for the Processor interface that returns
// pre-configured contents without hitting Usenet.
type mockRarProcessor struct {
	contents []Content
}

func (m *mockRarProcessor) AnalyzeRarContentFromNzb(_ context.Context, _ []parser.ParsedFile, _ string, _ *progress.Tracker) ([]Content, error) {
	return m.contents, nil
}

func (m *mockRarProcessor) CreateFileMetadataFromRarContent(content Content, _ string, _ int64, _ string) *metapb.FileMetadata {
	return &metapb.FileMetadata{
		FileSize:    content.Size,
		SegmentData: content.Segments,
		Status:      metapb.FileStatus_FILE_STATUS_HEALTHY,
	}
}

// groupBehavior is the scripted result for one RAR set in scriptedRarProcessor.
type groupBehavior struct {
	contents []Content
	err      error
}

// scriptedRarProcessor returns per-set contents/errors keyed by SetKey and
// records which sets it was asked to analyze, so tests can assert group
// isolation and that gapped groups are never analyzed.
type scriptedRarProcessor struct {
	mu       sync.Mutex
	calls    []string
	behavior map[string]groupBehavior
}

func (m *scriptedRarProcessor) AnalyzeRarContentFromNzb(_ context.Context, g []parser.ParsedFile, _ string, _ *progress.Tracker) ([]Content, error) {
	key, _ := SetKey(g[0].Filename)
	m.mu.Lock()
	m.calls = append(m.calls, key)
	b := m.behavior[key]
	m.mu.Unlock()
	return b.contents, b.err
}

func (m *scriptedRarProcessor) CreateFileMetadataFromRarContent(content Content, _ string, _ int64, _ string) *metapb.FileMetadata {
	return &metapb.FileMetadata{
		FileSize:    content.Size,
		SegmentData: content.Segments,
		Status:      metapb.FileStatus_FILE_STATUS_HEALTHY,
	}
}

func (m *scriptedRarProcessor) wasCalled(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Contains(m.calls, key)
}

func TestProcessArchiveIsolatesFailedGroup(t *testing.T) {
	metaRoot := t.TempDir()
	svc := metadata.NewMetadataService(metaRoot)
	proc := &scriptedRarProcessor{behavior: map[string]groupBehavior{
		"seta": {err: errors.New("boom: missing volume")},
		"setb": {contents: []Content{
			{InternalPath: "videoB.mkv", Filename: "videoB.mkv", Size: 1000,
				Segments: []*metapb.SegmentData{{Id: "b", StartOffset: 0, EndOffset: 999}}},
		}},
	}}

	err := ProcessArchive(context.Background(), ProcessArchiveOptions{
		VirtualDir: "movies/Release",
		ArchiveFiles: []parser.ParsedFile{
			{Filename: "setA.part01.rar"}, {Filename: "setA.part02.rar"},
			{Filename: "setB.part01.rar"}, {Filename: "setB.part02.rar"},
		},
		NzbPath:         "movies/Release.nzb",
		Processor:       proc,
		MetadataService: svc,
		ExtractedFiles:  []parser.ExtractedFileInfo{{Name: "videoB.mkv", Size: 1000}},
		MaxPrefetch:     1,
		ReadTimeout:     30 * time.Second,
	})
	require.NoError(t, err, "a single failed group must not fail the whole archive")
	require.True(t, metaExists(t, metaRoot, "movies/Release/videoB.mkv"), "healthy set B must be imported")
}

func TestProcessArchiveAllGroupsFailedReturnsError(t *testing.T) {
	sentinel := errors.New("analysis failed")
	proc := &scriptedRarProcessor{behavior: map[string]groupBehavior{
		"seta": {err: sentinel},
		"setb": {err: errors.New("other failure")},
	}}

	err := ProcessArchive(context.Background(), ProcessArchiveOptions{
		VirtualDir: "movies/Release",
		ArchiveFiles: []parser.ParsedFile{
			{Filename: "setA.part01.rar"}, {Filename: "setA.part02.rar"},
			{Filename: "setB.part01.rar"}, {Filename: "setB.part02.rar"},
		},
		NzbPath:         "movies/Release.nzb",
		Processor:       proc,
		MetadataService: metadata.NewMetadataService(t.TempDir()),
		MaxPrefetch:     1,
		ReadTimeout:     30 * time.Second,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, sentinel, "joined error must preserve errors.Is on the original")
}

func TestProcessArchiveNoAllowedFilesPreservesSentinel(t *testing.T) {
	proc := &mockRarProcessor{contents: []Content{{InternalPath: "cover.jpg", Filename: "cover.jpg", Size: 100}}}
	err := ProcessArchive(context.Background(), ProcessArchiveOptions{
		VirtualDir:            "movies/Release",
		ArchiveFiles:          []parser.ParsedFile{{Filename: "extras.rar"}},
		NzbPath:               "movies/Release.nzb",
		Processor:             proc,
		MetadataService:       metadata.NewMetadataService(t.TempDir()),
		AllowedFileExtensions: []string{".mp4"},
	})
	require.ErrorIs(t, err, ErrNoAllowedFiles)
}

func TestProcessArchiveContextCancelledNotIsolated(t *testing.T) {
	proc := &scriptedRarProcessor{behavior: map[string]groupBehavior{
		"seta": {err: context.Canceled},
		"setb": {contents: []Content{
			{InternalPath: "videoB.mkv", Filename: "videoB.mkv", Size: 1000,
				Segments: []*metapb.SegmentData{{Id: "b", StartOffset: 0, EndOffset: 999}}},
		}},
	}}

	err := ProcessArchive(context.Background(), ProcessArchiveOptions{
		VirtualDir: "movies/Release",
		ArchiveFiles: []parser.ParsedFile{
			{Filename: "setA.part01.rar"}, {Filename: "setA.part02.rar"},
			{Filename: "setB.part01.rar"}, {Filename: "setB.part02.rar"},
		},
		NzbPath:         "movies/Release.nzb",
		Processor:       proc,
		MetadataService: metadata.NewMetadataService(t.TempDir()),
		ExtractedFiles:  []parser.ExtractedFileInfo{{Name: "videoB.mkv", Size: 1000}},
		MaxPrefetch:     1,
		ReadTimeout:     30 * time.Second,
	})
	require.ErrorIs(t, err, context.Canceled, "cancellation must propagate, never be isolated")
}

func TestProcessArchiveSkipsGroupWithVolumeGap(t *testing.T) {
	metaRoot := t.TempDir()
	svc := metadata.NewMetadataService(metaRoot)
	proc := &scriptedRarProcessor{behavior: map[string]groupBehavior{
		// setA would succeed if analyzed, but it has a volume gap and must be
		// skipped before any analysis call.
		"seta": {contents: []Content{
			{InternalPath: "videoA.mkv", Filename: "videoA.mkv", Size: 500,
				Segments: []*metapb.SegmentData{{Id: "a", StartOffset: 0, EndOffset: 499}}},
		}},
		"setb": {contents: []Content{
			{InternalPath: "videoB.mkv", Filename: "videoB.mkv", Size: 1000,
				Segments: []*metapb.SegmentData{{Id: "b", StartOffset: 0, EndOffset: 999}}},
		}},
	}}

	err := ProcessArchive(context.Background(), ProcessArchiveOptions{
		VirtualDir: "movies/Release",
		ArchiveFiles: []parser.ParsedFile{
			// setA missing part01 → gap.
			{Filename: "setA.part02.rar"}, {Filename: "setA.part03.rar"},
			{Filename: "setB.part01.rar"}, {Filename: "setB.part02.rar"},
		},
		NzbPath:         "movies/Release.nzb",
		Processor:       proc,
		MetadataService: svc,
		ExtractedFiles:  []parser.ExtractedFileInfo{{Name: "videoB.mkv", Size: 1000}},
		MaxPrefetch:     1,
		ReadTimeout:     30 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, proc.wasCalled("seta"), "gapped set A must never reach analysis")
	require.True(t, proc.wasCalled("setb"), "healthy set B must be analyzed")
	require.True(t, metaExists(t, metaRoot, "movies/Release/videoB.mkv"), "set B must be imported")
	require.False(t, metaExists(t, metaRoot, "movies/Release/videoA.mkv"), "gapped set A must not be imported")
}

// metaExists checks whether a .meta file exists for the given virtual path under metaRoot.
func metaExists(t *testing.T, metaRoot, virtualPath string) bool {
	t.Helper()
	metaPath := filepath.Join(metaRoot, virtualPath+".meta")
	_, err := os.Stat(metaPath)
	return err == nil
}

func TestProcessArchivePreservesInternalFolderStructure(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		contents         []Content
		virtualDir       string
		nzbPath          string
		renameToNzbName  bool
		extractedFiles   []parser.ExtractedFileInfo // override; nil = auto-build from contents
		wantMetaPaths    []string                   // virtual paths expected to have metadata
		notWantMetaPaths []string                   // virtual paths that must NOT have metadata
	}{
		{
			name:       "flat file: no subdirectory",
			virtualDir: "movies/MyMovie",
			nzbPath:    "movies/MyMovie.nzb",
			contents: []Content{
				{InternalPath: "video.mkv", Filename: "video.mkv", Size: 1000,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 999}}},
			},
			wantMetaPaths: []string{"movies/MyMovie/video.mkv"},
		},
		{
			name:       "file inside subdirectory: structure preserved",
			virtualDir: "movies/MyMovie",
			nzbPath:    "movies/MyMovie.nzb",
			contents: []Content{
				{InternalPath: "Extras/bonus.mkv", Filename: "bonus.mkv", Size: 500,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 499}}},
			},
			wantMetaPaths:    []string{"movies/MyMovie/Extras/bonus.mkv"},
			notWantMetaPaths: []string{"movies/MyMovie/bonus.mkv"},
		},
		{
			name:       "multiple files in same subdirectory",
			virtualDir: "tv/Show/Season01",
			nzbPath:    "tv/Show/Season01.nzb",
			contents: []Content{
				{InternalPath: "subs/en.srt", Filename: "en.srt", Size: 100,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 99}}},
				{InternalPath: "subs/fr.srt", Filename: "fr.srt", Size: 120,
					Segments: []*metapb.SegmentData{{Id: "seg2", StartOffset: 0, EndOffset: 119}}},
			},
			wantMetaPaths: []string{
				"tv/Show/Season01/subs/en.srt",
				"tv/Show/Season01/subs/fr.srt",
			},
			notWantMetaPaths: []string{
				"tv/Show/Season01/en.srt",
				"tv/Show/Season01/fr.srt",
			},
		},
		{
			name:       "nested RAR folder same as virtual dir base: deduplicated",
			virtualDir: "movies/MyMovie",
			nzbPath:    "movies/MyMovie.nzb",
			contents: []Content{
				{InternalPath: "MyMovie/video.mkv", Filename: "video.mkv", Size: 1000,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 999}}},
			},
			wantMetaPaths:    []string{"movies/MyMovie/video.mkv"},
			notWantMetaPaths: []string{"movies/MyMovie/MyMovie/video.mkv"},
		},
		{
			name:       "nested RAR folder same as virtual dir base with subfolder: prefix stripped",
			virtualDir: "movies/MyMovie",
			nzbPath:    "movies/MyMovie.nzb",
			contents: []Content{
				{InternalPath: "MyMovie/Extras/bonus.mkv", Filename: "bonus.mkv", Size: 500,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 499}}},
			},
			wantMetaPaths:    []string{"movies/MyMovie/Extras/bonus.mkv"},
			notWantMetaPaths: []string{"movies/MyMovie/MyMovie/Extras/bonus.mkv"},
		},
		{
			name:            "single file with rename: placed flat ignoring internal subdir",
			virtualDir:      "movies/MyMovie",
			nzbPath:         "movies/MyMovie.nzb",
			renameToNzbName: true,
			contents: []Content{
				{InternalPath: "SubFolder/obfuscated.mkv", Filename: "obfuscated.mkv", Size: 2000,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 1999}}},
			},
			// The rename happens before the pre-extracted check, so supply the post-rename name
			extractedFiles:   []parser.ExtractedFileInfo{{Name: "MyMovie.mkv", Size: 2000}},
			wantMetaPaths:    []string{"movies/MyMovie/MyMovie.mkv"},
			notWantMetaPaths: []string{"movies/MyMovie/SubFolder/obfuscated.mkv"},
		},
		{
			name:            "season pack with rename: similar-size files are left alone",
			virtualDir:      "tv/Show.S01",
			nzbPath:         "tv/Show.S01.nzb",
			renameToNzbName: true,
			contents: []Content{
				{InternalPath: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv", Filename: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv", Size: 2000,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 1999}}},
				{InternalPath: "b3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv", Filename: "b3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv", Size: 2100,
					Segments: []*metapb.SegmentData{{Id: "seg2", StartOffset: 0, EndOffset: 2099}}},
			},
			wantMetaPaths: []string{
				"tv/Show.S01/a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv",
				"tv/Show.S01/b3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv",
			},
			notWantMetaPaths: []string{"tv/Show.S01/Show.S01.mkv"},
		},
		{
			name:            "rename: subtitle sharing the payload stem follows it",
			virtualDir:      "movies/MyMovie",
			nzbPath:         "movies/MyMovie.nzb",
			renameToNzbName: true,
			contents: []Content{
				{InternalPath: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv", Filename: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv", Size: 20000,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 19999}}},
				{InternalPath: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.eng.srt", Filename: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.eng.srt", Size: 100,
					Segments: []*metapb.SegmentData{{Id: "seg2", StartOffset: 0, EndOffset: 99}}},
			},
			extractedFiles: []parser.ExtractedFileInfo{{Name: "MyMovie.mkv", Size: 20000}, {Name: "MyMovie.eng.srt", Size: 100}},
			wantMetaPaths:  []string{"movies/MyMovie/MyMovie.mkv", "movies/MyMovie/MyMovie.eng.srt"},
			notWantMetaPaths: []string{
				"movies/MyMovie/a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.mkv",
				"movies/MyMovie/a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.eng.srt",
			},
		},
		{
			name:            "rename: clean payload name is kept",
			virtualDir:      "movies/MyMovie",
			nzbPath:         "movies/MyMovie.nzb",
			renameToNzbName: true,
			contents: []Content{
				{InternalPath: "My.Movie.2024.PROPER.1080p.mkv", Filename: "My.Movie.2024.PROPER.1080p.mkv", Size: 2000,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 1999}}},
			},
			wantMetaPaths:    []string{"movies/MyMovie/My.Movie.2024.PROPER.1080p.mkv"},
			notWantMetaPaths: []string{"movies/MyMovie/MyMovie.mkv"},
		},
		{
			name:       "windows-style backslash paths normalized",
			virtualDir: "movies/MyMovie",
			nzbPath:    "movies/MyMovie.nzb",
			contents: []Content{
				{InternalPath: `Extras\featurette.mkv`, Filename: "featurette.mkv", Size: 300,
					Segments: []*metapb.SegmentData{{Id: "seg1", StartOffset: 0, EndOffset: 299}}},
			},
			wantMetaPaths:    []string{"movies/MyMovie/Extras/featurette.mkv"},
			notWantMetaPaths: []string{"movies/MyMovie/featurette.mkv"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metaRoot := t.TempDir()
			svc := metadata.NewMetadataService(metaRoot)
			proc := &mockRarProcessor{contents: tt.contents}

			// Build extractedFiles so validation is skipped (no pool manager needed).
			// Use the override when provided (e.g. rename cases change baseFilename before the check).
			extracted := tt.extractedFiles
			if extracted == nil {
				extracted = make([]parser.ExtractedFileInfo, len(tt.contents))
				for i, c := range tt.contents {
					extracted[i] = parser.ExtractedFileInfo{
						Name: filepath.Base(c.Filename),
						Size: c.Size,
					}
				}
			}

			err := ProcessArchive(ctx, ProcessArchiveOptions{
				VirtualDir:             tt.virtualDir,
				ArchiveFiles:           []parser.ParsedFile{{Filename: "archive.rar"}},
				Password:               "",
				ReleaseDate:            0,
				NzbPath:                tt.nzbPath,
				Processor:              proc,
				MetadataService:        svc,
				PoolManager:            nil,
				ArchiveProgressTracker: nil,
				AllowedFileExtensions:  nil,
				ExtractedFiles:         extracted,
				MaxPrefetch:            1,
				ReadTimeout:            30 * time.Second,
				ExpandBlurayIso:        false,
				FilterSamples:          false,
				RenameToNzbName:        tt.renameToNzbName,
			})
			require.NoError(t, err)

			for _, vp := range tt.wantMetaPaths {
				require.True(t, metaExists(t, metaRoot, vp), "expected metadata at %s", vp)
			}
			for _, vp := range tt.notWantMetaPaths {
				require.False(t, metaExists(t, metaRoot, vp), "unexpected metadata at %s", vp)
			}
		})
	}
}

func TestValidateSegmentIntegrity(t *testing.T) {
	ctx := context.Background()

	t.Run("Healthy non-nested file", func(t *testing.T) {
		content := Content{
			Size:       1000,
			PackedSize: 800,
			Segments: []*metapb.SegmentData{
				{StartOffset: 0, EndOffset: 399},
				{StartOffset: 400, EndOffset: 799},
			},
		}
		err := validateSegmentIntegrity(ctx, content)
		require.NoError(t, err)
	})

	t.Run("Corrupted non-nested file (missing segments)", func(t *testing.T) {
		content := Content{
			Size:       1000,
			PackedSize: 800,
			Segments: []*metapb.SegmentData{
				{StartOffset: 0, EndOffset: 399},
				// Missing the last 400 bytes
			},
		}
		err := validateSegmentIntegrity(ctx, content)
		require.Error(t, err)
		require.Contains(t, err.Error(), "corrupted file: missing 400 bytes")
	})

	t.Run("Minor shortfall within 1% threshold", func(t *testing.T) {
		// 1000 bytes expected, 995 provided (0.5% shortfall)
		content := Content{
			Size:       1000,
			PackedSize: 1000,
			Segments: []*metapb.SegmentData{
				{StartOffset: 0, EndOffset: 994},
			},
		}
		err := validateSegmentIntegrity(ctx, content)
		require.NoError(t, err)
	})

	t.Run("Shortfall exactly 1%", func(t *testing.T) {
		// 1000 bytes expected, 990 provided (1% shortfall)
		content := Content{
			Size:       1000,
			PackedSize: 1000,
			Segments: []*metapb.SegmentData{
				{StartOffset: 0, EndOffset: 989},
			},
		}
		err := validateSegmentIntegrity(ctx, content)
		require.Error(t, err)
		require.Contains(t, err.Error(), "1% of total size")
	})

	t.Run("Healthy nested sources", func(t *testing.T) {
		content := Content{
			Size: 1000,
			NestedSources: []NestedSource{
				{
					InnerLength: 500,
					Segments: []*metapb.SegmentData{
						{StartOffset: 0, EndOffset: 499},
					},
				},
				{
					InnerLength: 500,
					Segments: []*metapb.SegmentData{
						{StartOffset: 0, EndOffset: 499},
					},
				},
			},
		}
		err := validateSegmentIntegrity(ctx, content)
		require.NoError(t, err)
	})

	t.Run("Corrupted nested source", func(t *testing.T) {
		content := Content{
			Size: 1000,
			NestedSources: []NestedSource{
				{
					InnerLength: 500,
					Segments: []*metapb.SegmentData{
						{StartOffset: 0, EndOffset: 499},
					},
				},
				{
					InnerLength: 500,
					Segments: []*metapb.SegmentData{
						{StartOffset: 0, EndOffset: 100}, // Missing ~400 bytes
					},
				},
			},
		}
		err := validateSegmentIntegrity(ctx, content)
		require.Error(t, err)
		require.Contains(t, err.Error(), "corrupted nested source: missing 399 bytes")
	})
}
