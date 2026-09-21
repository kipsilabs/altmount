package parser

import (
	"bytes"
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// TestParseNzbPopulatesWarmFirstSegmentBytes verifies that a fetched file's
// decoded first-segment payload is carried onto ParsedFile.FirstSegmentBytes,
// so the archive analysis phase can serve that file's offset-0 read from memory
// instead of re-fetching the segment.
func TestParseNzbPopulatesWarmFirstSegmentBytes(t *testing.T) {
	payload := []byte("RAR!\x1a\x07\x00decoded-header-bytes-from-first-segment")

	fp := fakepool.New()
	fp.SetBehavior("seg-0", fakepool.SegmentBehavior{
		Bytes: payload,
		YEnc:  nntppool.YEncMeta{PartSize: int64(len(payload)), FileSize: int64(len(payload))},
	})

	pm := newFakeFullPoolManager(fp)
	p := NewParser(pm, stormConfigGetter(2))

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{
				Filename: "movie.part01.rar",
				Segments: nzbparser.NzbSegments{
					{Bytes: len(payload), Number: 1, ID: "seg-0"},
				},
			},
		},
	}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 {
		t.Fatalf("len(parsed.Files) = %d, want 1", len(parsed.Files))
	}
	if !bytes.Equal(parsed.Files[0].FirstSegmentBytes, payload) {
		t.Errorf("FirstSegmentBytes = %q, want %q", parsed.Files[0].FirstSegmentBytes, payload)
	}
}

// TestParseNzbLeavesWarmBytesEmptyWhenFirstSegmentSkipped pins that a file whose
// first segment fetch was intentionally skipped (clean-named multipart video)
// carries no warm bytes — there is nothing fetched to cache, and the analyzer
// reads such files lazily.
func TestParseNzbLeavesWarmBytesEmptyWhenFirstSegmentSkipped(t *testing.T) {
	const (
		fullEncoded = 720000
		lastEncoded = 51000
		partDecoded = 700000
		lastDecoded = 50000
		name        = "Inception.2010.1080p.BluRay.mkv"
	)

	fp := fakepool.New()
	// First segment must NOT be fetched (skip path); only the representative
	// middle segment and the last segment are consulted for sizing.
	fp.SetBehavior("seg-1", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: name, PartSize: partDecoded},
	})
	fp.SetBehavior("seg-2", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: name, PartSize: lastDecoded},
	})

	pm := newFakeFullPoolManager(fp)
	p := NewParser(pm, stormConfigGetter(4))

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{
				Filename: name,
				Segments: nzbparser.NzbSegments{
					{Bytes: fullEncoded, Number: 1, ID: "seg-0"},
					{Bytes: fullEncoded, Number: 2, ID: "seg-1"},
					{Bytes: lastEncoded, Number: 3, ID: "seg-2"},
				},
			},
		},
	}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 {
		t.Fatalf("len(parsed.Files) = %d, want 1", len(parsed.Files))
	}
	if len(parsed.Files[0].FirstSegmentBytes) != 0 {
		t.Errorf("FirstSegmentBytes len = %d, want 0 (first segment was skipped)", len(parsed.Files[0].FirstSegmentBytes))
	}
	// Sanity: confirm the first segment was never fetched over the wire.
	if got := fp.PerMessageCalls("seg-0"); got != 0 {
		t.Errorf("seg-0 was fetched %d times, want 0 (skip path)", got)
	}
}
