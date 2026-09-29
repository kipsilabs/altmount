package importer

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/importer/archive/rar"
	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/stretchr/testify/require"
)

type coverOnlyRarProcessor struct {
	analyzed bool
}

func (p *coverOnlyRarProcessor) AnalyzeRarContentFromNzb(_ context.Context, _ []parser.ParsedFile, _ string, _ *progress.Tracker) ([]rar.Content, error) {
	p.analyzed = true
	return []rar.Content{{InternalPath: "cover.jpg", Filename: "cover.jpg", Size: 100}}, nil
}

func (p *coverOnlyRarProcessor) CreateFileMetadataFromRarContent(rar.Content, string, int64, string) *metapb.FileMetadata {
	return nil
}

func TestProcessRarArchiveKeepsDirectMediaWhenArchiveHasOnlyCover(t *testing.T) {
	for _, tc := range []struct {
		name        string
		withDirect  bool
		wantFailure bool
	}{
		{name: "direct MP4 survives cover-only RAR", withDirect: true},
		{name: "cover-only RAR still fails", wantFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := metadata.NewMetadataService(t.TempDir())
			cfg := config.DefaultConfig()
			proc := NewProcessor(meta, nil, nil, func() *config.Config { return cfg }, nil)
			rarProc := &coverOnlyRarProcessor{}
			proc.rarProcessor = rarProc

			var regular []parser.ParsedFile
			if tc.withDirect {
				regular = []parser.ParsedFile{{
					Filename: "release.part.4.mp4", Size: 1000,
					Segments: []*metapb.SegmentData{{Id: "video", StartOffset: 0, EndOffset: 999}},
				}}
			}
			_, written, err := proc.processRarArchive(
				context.Background(), "movies", regular,
				[]parser.ParsedFile{{Filename: "extras.rar"}},
				&parser.ParsedNzb{Path: "Release.nzb"}, 1, []string{".mp4"},
				nil, nil, nil, nil, nil, "",
			)
			require.True(t, rarProc.analyzed, "RAR analysis must still run")
			if tc.wantFailure {
				require.ErrorIs(t, err, rar.ErrNoAllowedFiles)
				return
			}
			require.NoError(t, err)
			require.Contains(t, written, "movies/Release/release.part.4.mp4")
			_, statErr := meta.ReadFileMetadata("movies/Release/release.part.4.mp4")
			require.NoError(t, statErr)
		})
	}
}
