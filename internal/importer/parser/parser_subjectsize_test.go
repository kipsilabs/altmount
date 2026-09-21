package parser

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// Posting tools write the exact decoded file size at the end of the subject
// line. For a clean-named file whose first segment is skipped, that number
// replaces the last-segment header fetch: the last part's size follows from it.
func TestParseNzbDerivesLastPartFromSubjectSize(t *testing.T) {
	const (
		fullEncoded = 720000
		lastEncoded = 51000
		partDecoded = 700000
		fileSize    = 2*partDecoded + 50000
		name        = "Inception.2010.1080p.BluRay.mkv"
	)

	fp := fakepool.New()
	fp.SetBehavior("seg-1", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: name, PartSize: partDecoded},
	})
	// seg-2 deliberately has no behaviour: fetching it is the failure this test guards.

	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{{
		Filename: name,
		Subject:  `[1/1] - "` + name + `" yEnc (1/3) 1450000`,
		Segments: nzbparser.NzbSegments{
			{Bytes: fullEncoded, Number: 1, ID: "seg-0"},
			{Bytes: fullEncoded, Number: 2, ID: "seg-1"},
			{Bytes: lastEncoded, Number: 3, ID: "seg-2"},
		},
	}}}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	f := parsed.Files[0]
	if f.Size != fileSize {
		t.Errorf("Size = %d, want %d (from subject)", f.Size, fileSize)
	}
	if got := f.Segments[2].SegmentSize; got != 50000 {
		t.Errorf("last segment size = %d, want 50000 (derived)", got)
	}
	if got := fp.PerMessageCalls("seg-2"); got != 0 {
		t.Errorf("last segment fetched %d times, want 0", got)
	}
}

func TestSizeFromSubject(t *testing.T) {
	tests := []struct {
		name    string
		subject string
		encoded int64
		want    int64
	}{
		{"trailing size after part counter", `[1/5] - "movie.mkv" yEnc (1/170) 710427259`, 730000000, 710427259},
		{"size right after yEnc", `"movie.mkv" yEnc 710427259`, 730000000, 710427259},
		{"no size", `[1/5] - "movie.mkv" yEnc (1/170)`, 730000000, 0},
		{"number larger than the encoded bytes is not a size", `"x.mkv" yEnc (1/2) 999999999`, 730000000, 0},
		{"number below half the encoded bytes is not a size", `"x.mkv" yEnc (1/2) 1234`, 730000000, 0},
		{"unknown encoded total", `"x.mkv" yEnc (1/2) 710427259`, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sizeFromSubject(tt.subject, tt.encoded); got != tt.want {
				t.Errorf("sizeFromSubject(%q, %d) = %d, want %d", tt.subject, tt.encoded, got, tt.want)
			}
		})
	}
}
