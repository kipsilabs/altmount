package parser

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// TestParseNzbBrokenIndexMakesNoBodyCallForBrokenFile verifies that ParseNzb
// with BrokenFileIndexes never calls Body() for the flagged file index.
func TestParseNzbBrokenIndexMakesNoBodyCallForBrokenFile(t *testing.T) {
	fp := fakepool.New()
	// Serve valid yEnc for the healthy file so it normalizes and is kept;
	// otherwise it would be skipped for failing normalization (#681).
	fp.SetBehavior("healthy-seg-0", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: "healthy.mkv", PartSize: 1024},
	})
	pm := newFakeFullPoolManager(fp)

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{
				Filename: "healthy.mkv",
				Segments: nzbparser.NzbSegments{
					{Bytes: 1024, Number: 1, ID: "healthy-seg-0"},
				},
			},
			{
				Filename: "broken.mkv",
				Segments: nzbparser.NzbSegments{
					{Bytes: 1024, Number: 1, ID: "broken-seg-0"},
				},
			},
		},
	}

	p := NewParser(pm, stormConfigGetter(2))
	opts := ParseOptions{
		BrokenFileIndexes:      map[int]struct{}{1: {}},
		KnownMissingSegmentIDs: map[string]struct{}{"broken-seg-0": {}},
	}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, opts)
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}

	// The broken file must never trigger a Body call.
	if calls := fp.PerMessageCalls("broken-seg-0"); calls != 0 {
		t.Errorf("Body calls for broken-seg-0 = %d, want 0", calls)
	}

	// The broken file must not appear in the parsed output.
	for _, f := range parsed.Files {
		if f.Filename == "broken.mkv" {
			t.Errorf("broken.mkv appeared in parsed output, want it dropped")
		}
	}
}

// TestParseNzbEmptyOptionsMatchesParsedFileBehavior verifies that ParseNzb
// with an empty ParseOptions processes all files normally.
func TestParseNzbEmptyOptionsNormalBehavior(t *testing.T) {
	fp := fakepool.New()
	// Serve valid yEnc so the file normalizes and is processed normally;
	// without it normalization fails and the file is skipped (#681).
	fp.SetBehavior("movie-seg-0", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: "movie.mkv", PartSize: 1024},
	})
	pm := newFakeFullPoolManager(fp)

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{
				Filename: "movie.mkv",
				Segments: nzbparser.NzbSegments{
					{Bytes: 1024, Number: 1, ID: "movie-seg-0"},
				},
			},
		},
	}

	p := NewParser(pm, stormConfigGetter(1))
	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 {
		t.Fatalf("len(parsed.Files) = %d, want 1", len(parsed.Files))
	}
}
