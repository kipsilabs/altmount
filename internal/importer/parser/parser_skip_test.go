package parser

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// TestParseNzbSkipsFileThatFailsNormalization is the regression test for
// https://github.com/kipsilabs/altmount/issues/681.
//
// When a file's segment sizes cannot be normalized — here because the second
// segment (needed to learn the part size, since the total file size is unknown)
// returns 430 — the file must be SKIPPED entirely. Importing it with the NZB's
// un-normalized (yEnc-encoded) byte counts would compute wrong segment offsets
// and produce a corrupt media file. The other files in the release must still
// import with normalized (decoded) sizes.
func TestParseNzbSkipsFileThatFailsNormalization(t *testing.T) {
	const (
		firstPartDecoded = 700000
		lastPartDecoded  = 50000
		firstPartEncoded = 720000 // NZB raw byte counts carry yEnc overhead
		lastPartEncoded  = 51000
	)

	// Realistic, non-obfuscated release names so fileinfo preserves them
	// (a bare "broken.mkv" would be flagged obfuscated and replaced by the NZB stem).
	const (
		brokenName  = "The.Matrix.1999.1080p.BluRay.mkv"
		healthyName = "Inception.2010.1080p.BluRay.mkv"
	)

	fp := fakepool.New()
	// Healthy file: both parts serve valid yEnc headers → normalizes cleanly.
	fp.SetBehavior("fileB-seg-0", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: healthyName, PartSize: firstPartDecoded},
	})
	fp.SetBehavior("fileB-seg-1", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: healthyName, PartSize: lastPartDecoded},
	})
	// Failing file: the first part is fine, but the second part — required to
	// learn the last-part size because FileSize is unknown — returns 430.
	fp.SetBehavior("fileA-seg-0", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: brokenName, PartSize: firstPartDecoded},
	})
	fp.SetBehavior("fileA-seg-1", fakepool.SegmentBehavior{
		Err: nntppool.ErrArticleNotFound,
	})

	pm := newFakeFullPoolManager(fp)
	p := NewParser(pm, stormConfigGetter(4))

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{
				Filename: brokenName,
				Segments: nzbparser.NzbSegments{
					{Bytes: firstPartEncoded, Number: 1, ID: "fileA-seg-0"},
					{Bytes: lastPartEncoded, Number: 2, ID: "fileA-seg-1"},
				},
			},
			{
				Filename: healthyName,
				Segments: nzbparser.NzbSegments{
					{Bytes: firstPartEncoded, Number: 1, ID: "fileB-seg-0"},
					{Bytes: lastPartEncoded, Number: 2, ID: "fileB-seg-1"},
				},
			},
		},
	}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}

	if len(parsed.Files) != 1 {
		t.Fatalf("len(parsed.Files) = %d, want 1 (broken file skipped, healthy kept)", len(parsed.Files))
	}

	got := parsed.Files[0]
	if got.Filename != healthyName {
		t.Fatalf("kept file = %q, want %q", got.Filename, healthyName)
	}

	// The kept file must carry NORMALIZED (decoded) sizes, not the raw NZB bytes.
	if len(got.Segments) != 2 {
		t.Fatalf("kept file segments = %d, want 2", len(got.Segments))
	}
	if got.Segments[0].SegmentSize != firstPartDecoded {
		t.Errorf("segment[0] size = %d, want %d (normalized, not raw %d)",
			got.Segments[0].SegmentSize, firstPartDecoded, firstPartEncoded)
	}
	if got.Segments[1].SegmentSize != lastPartDecoded {
		t.Errorf("segment[1] size = %d, want %d (normalized, not raw %d)",
			got.Segments[1].SegmentSize, lastPartDecoded, lastPartEncoded)
	}

	// Guard: the skipped file's data must not leak into the parsed release.
	for _, f := range parsed.Files {
		if f.Filename == brokenName {
			t.Errorf("%q appeared in parsed output, want it skipped", brokenName)
		}
	}
}

// TestParseNzbKeepsFileWhenYencHeaderUnavailable pins the scope of the #681 fix:
// only a MISSING article (430) triggers a skip. When the article is present but
// carries no usable yEnc part size, the NZB-declared segment sizes remain the best
// available source, so the file must still be imported (not skipped).
func TestParseNzbKeepsFileWhenYencHeaderUnavailable(t *testing.T) {
	const declaredSize = 12345

	fp := fakepool.New()
	// Article is present (no error) but the fake returns no yEnc part size,
	// mirroring a non-yEnc / unparseable-header article.
	fp.SetBehavior("present-seg-0", fakepool.SegmentBehavior{Bytes: make([]byte, 16)})

	pm := newFakeFullPoolManager(fp)
	p := NewParser(pm, stormConfigGetter(2))

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{
				Filename: "Some.Show.S01E01.1080p.mkv",
				Segments: nzbparser.NzbSegments{
					{Bytes: declaredSize, Number: 1, ID: "present-seg-0"},
				},
			},
		},
	}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}

	if len(parsed.Files) != 1 {
		t.Fatalf("len(parsed.Files) = %d, want 1 (present article must not be skipped)", len(parsed.Files))
	}
	if got := parsed.Files[0].Segments[0].SegmentSize; got != declaredSize {
		t.Errorf("segment size = %d, want %d (NZB-declared size preserved)", got, declaredSize)
	}
}
