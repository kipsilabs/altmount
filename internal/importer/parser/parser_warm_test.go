package parser

import (
	"bytes"
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// Warming first segments while the fast-fail probe runs must leave the parse
// pass nothing to fetch for those articles: one wire read per first segment,
// not two.
func TestWarmFirstSegmentsMakesParseServeHeadsFromCache(t *testing.T) {
	content := bytes.Repeat([]byte("F"), 32768)
	fp := fakepool.New()
	fp.SetBehavior("vid-0", fakepool.SegmentBehavior{
		Bytes: content[:16384],
		YEnc:  nntppool.YEncMeta{FileName: "Real.Movie.2024.mkv", FileSize: 32768, Part: 1, PartBegin: 0, PartSize: 16384},
	})
	fp.SetBehavior("vid-1", fakepool.SegmentBehavior{
		Bytes: content[16384:],
		YEnc:  nntppool.YEncMeta{FileSize: 32768, Part: 2, PartBegin: 16384, PartSize: 16384},
	})
	nzb := &nzbparser.Nzb{Files: nzbparser.NzbFiles{
		{Filename: "Real.Movie.2024.mkv", Segments: nzbparser.NzbSegments{
			{Bytes: 16800, Number: 1, ID: "vid-0"},
			{Bytes: 16800, Number: 2, ID: "vid-1"},
		}},
	}}

	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

	p.WarmFirstSegments(context.Background(), nzb.Files)
	if got := fp.PerMessageCalls("vid-0"); got != 1 {
		t.Fatalf("warm-up fetched vid-0 %d times, want 1", got)
	}

	parsed, err := p.ParseNzb(context.Background(), nzb, "release.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 {
		t.Fatalf("parsed %d files, want 1", len(parsed.Files))
	}
	if got := fp.PerMessageCalls("vid-0"); got != 1 {
		t.Fatalf("parse refetched vid-0: %d calls total, want 1", got)
	}
}

// A cancelled warm-up returns promptly and leaves the parse to fetch normally.
func TestWarmFirstSegmentsHonoursCancel(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("vid-0", fakepool.SegmentBehavior{Bytes: []byte("x"), YEnc: nntppool.YEncMeta{FileSize: 1, PartSize: 1}})
	nzb := &nzbparser.Nzb{Files: nzbparser.NzbFiles{
		{Filename: "Real.Movie.2024.mkv", Segments: nzbparser.NzbSegments{{Bytes: 10, Number: 1, ID: "vid-0"}}},
	}}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.WarmFirstSegments(ctx, nzb.Files)
	if got := fp.PerMessageCalls("vid-0"); got != 0 {
		t.Fatalf("cancelled warm-up still fetched vid-0 %d times", got)
	}
}
