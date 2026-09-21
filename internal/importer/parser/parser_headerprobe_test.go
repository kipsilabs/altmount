package parser

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// obfuscated name so the first-segment fetch is not skipped by the clean-name gate.
const probeObfuscatedName = "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5"

func fourSegmentFile(name string) nzbparser.NzbFile {
	return nzbparser.NzbFile{
		Filename: name,
		Segments: nzbparser.NzbSegments{
			{Bytes: 1030000, Number: 1, ID: "seg-0"},
			{Bytes: 1030000, Number: 2, ID: "seg-1"},
			{Bytes: 1030000, Number: 3, ID: "seg-2"},
			{Bytes: 515000, Number: 4, ID: "seg-3"},
		},
	}
}

// A dead first article must not drop a file whose remaining articles still
// carry the yEnc header: the name, exact size and part size are recoverable
// from any later article of a multipart post.
func TestParseNzbRecoversHeaderFromLaterArticleWhenFirstIsMissing(t *testing.T) {
	const realName = "Movie.2020.1080p.BluRay.mkv"
	const partSize = int64(1000000)
	const fileSize = int64(3500000)

	fp := fakepool.New()
	fp.SetBehavior("seg-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	fp.SetBehavior("seg-1", fakepool.SegmentBehavior{
		Bytes: make([]byte, 64),
		YEnc: nntppool.YEncMeta{
			FileName:  realName,
			FileSize:  fileSize,
			Part:      2,
			PartBegin: partSize, // nntppool reports =ypart begin 0-based
			PartSize:  partSize,
			Total:     4,
		},
	})

	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(2))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{fourSegmentFile(probeObfuscatedName)}}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 {
		t.Fatalf("len(parsed.Files) = %d, want 1 (file recovered from later article)", len(parsed.Files))
	}
	f := parsed.Files[0]
	if f.Filename != realName {
		t.Errorf("Filename = %q, want %q (from yEnc header)", f.Filename, realName)
	}
	if f.Size != fileSize {
		t.Errorf("Size = %d, want %d (from =ybegin size)", f.Size, fileSize)
	}
	if got := f.Segments[0].SegmentSize; got != partSize {
		t.Errorf("first segment size = %d, want %d (derived from =ypart begin)", got, partSize)
	}
	if got := f.Segments[3].SegmentSize; got != fileSize-3*partSize {
		t.Errorf("last segment size = %d, want %d (derived arithmetically)", got, fileSize-3*partSize)
	}
	if segID, ok := parsed.DegradedFiles[realName]; !ok || segID != "seg-0" {
		t.Errorf("DegradedFiles[%q] = %q, %v; want \"seg-0\", true", realName, segID, ok)
	}
	// seg-0 (430) + seg-1 (header) and nothing else: no last-segment fetch.
	if calls := fp.BodyCalls(); calls != 2 {
		t.Errorf("BodyCalls = %d, want 2", calls)
	}
}

// When the leading articles are all gone the file is still dropped, after a
// bounded number of probes.
func TestParseNzbDropsFileWhenAllProbeArticlesMissing(t *testing.T) {
	fp := fakepool.New()
	for _, id := range []string{"seg-0", "seg-1", "seg-2", "seg-3"} {
		fp.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	}
	fp.SetBehavior("good-0", fakepool.SegmentBehavior{Bytes: make([]byte, 16)})

	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(2))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{
		fourSegmentFile(probeObfuscatedName),
		{Filename: "Good.2020.1080p.BluRay.mkv", Segments: nzbparser.NzbSegments{{Bytes: 12345, Number: 1, ID: "good-0"}}},
	}}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 || parsed.Files[0].Filename != "Good.2020.1080p.BluRay.mkv" {
		t.Fatalf("parsed.Files = %+v, want only the healthy file", parsed.Files)
	}
	// 4 probes on the dead file + 1 for the healthy one.
	if calls := fp.BodyCalls(); calls != 5 {
		t.Errorf("BodyCalls = %d, want 5", calls)
	}
}

// A first segment already known missing from the pre-parse Stat sweep goes
// straight to the fallback article without spending a Body call on it.
func TestParseNzbSkipsKnownMissingFirstSegmentBeforeProbing(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("seg-1", fakepool.SegmentBehavior{
		Bytes: make([]byte, 64),
		YEnc:  nntppool.YEncMeta{FileName: "Movie.mkv", FileSize: 3500000, Part: 2, PartBegin: 1000000, PartSize: 1000000},
	})

	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(2))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{fourSegmentFile(probeObfuscatedName)}}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{
		KnownMissingSegmentIDs: map[string]struct{}{"seg-0": {}},
	})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 || parsed.Files[0].Filename != "Movie.mkv" {
		t.Fatalf("parsed.Files = %+v, want the recovered file", parsed.Files)
	}
	if calls := fp.BodyCalls(); calls != 1 {
		t.Errorf("BodyCalls = %d, want 1 (seg-0 skipped, seg-1 probed)", calls)
	}
}

func TestFirstPartSizeFromLaterArticle(t *testing.T) {
	tests := []struct {
		name  string
		meta  nntppool.YEncMeta
		index int
		want  int64
	}{
		{"second article of uniform post", nntppool.YEncMeta{PartBegin: 1000000, PartSize: 1000000}, 1, 1000000},
		{"fourth article of uniform post", nntppool.YEncMeta{PartBegin: 3000000, PartSize: 1000000}, 3, 1000000},
		{"shorter last article still yields the uniform size", nntppool.YEncMeta{PartBegin: 3000000, PartSize: 500000}, 3, 1000000},
		{"index zero falls back to own part size", nntppool.YEncMeta{PartBegin: 0, PartSize: 1000000}, 0, 1000000},
		{"missing =ypart falls back to own part size", nntppool.YEncMeta{PartSize: 777}, 2, 777},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := firstPartSizeFromLaterArticle(tt.meta, tt.index); got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}
