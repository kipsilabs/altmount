package parser

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nzbparser"
)

func TestDeriveLastPartSize(t *testing.T) {
	tests := []struct {
		name             string
		fileSize         int64
		firstPartSize    int64
		standardPartSize int64
		numSegments      int
		wantSize         int64
		wantOK           bool
	}{
		{
			name:     "four segments uniform with remainder",
			fileSize: 700000*3 + 120000, firstPartSize: 700000, standardPartSize: 700000, numSegments: 4,
			wantSize: 120000, wantOK: true,
		},
		{
			name:     "three segments full last part",
			fileSize: 700000 * 3, firstPartSize: 700000, standardPartSize: 700000, numSegments: 3,
			wantSize: 700000, wantOK: true,
		},
		{
			name:     "two segments remainder",
			fileSize: 700000 + 50000, firstPartSize: 700000, standardPartSize: 0, numSegments: 2,
			wantSize: 50000, wantOK: true,
		},
		{
			name:     "unknown file size",
			fileSize: 0, firstPartSize: 700000, standardPartSize: 700000, numSegments: 4,
			wantOK: false,
		},
		{
			name:     "unknown first part size",
			fileSize: 2100000, firstPartSize: 0, standardPartSize: 700000, numSegments: 3,
			wantOK: false,
		},
		{
			name:     "junk size yields negative remainder",
			fileSize: 100, firstPartSize: 700000, standardPartSize: 700000, numSegments: 4,
			wantOK: false,
		},
		{
			name:     "junk size yields oversized remainder",
			fileSize: 700000 * 10, firstPartSize: 700000, standardPartSize: 700000, numSegments: 4,
			wantOK: false,
		},
		{
			name:     "three plus segments require standard part size",
			fileSize: 2100000, firstPartSize: 700000, standardPartSize: 0, numSegments: 3,
			wantOK: false,
		},
		{
			name:     "two segments oversized remainder rejected",
			fileSize: 700000 * 3, firstPartSize: 700000, standardPartSize: 0, numSegments: 2,
			wantOK: false,
		},
		{
			name:     "single segment not derivable",
			fileSize: 700000, firstPartSize: 700000, standardPartSize: 0, numSegments: 1,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := deriveLastPartSize(tt.fileSize, tt.firstPartSize, tt.standardPartSize, tt.numSegments)
			if ok != tt.wantOK {
				t.Fatalf("deriveLastPartSize() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.wantSize {
				t.Errorf("deriveLastPartSize() = %d, want %d", got, tt.wantSize)
			}
		})
	}
}

// normalizeSegs builds n segments with encoded (NZB-reported) sizes that will be
// rewritten by normalization.
func normalizeSegs(n int) nzbparser.NzbSegments {
	segs := make(nzbparser.NzbSegments, n)
	for i := range segs {
		segs[i] = nzbparser.NzbSegment{Bytes: 716800, Number: i + 1, ID: "norm-seg"}
	}
	return segs
}

func TestNormalizeSegmentSizes_DerivesLastWithoutFetch(t *testing.T) {
	fp := fakepool.New()
	mgr := newFakeFullPoolManager(fp)
	p := NewParser(mgr, stormConfigGetter(4))

	segs := normalizeSegs(4)
	first := firstSegmentYencInfo{PartSize: 700000, FileSize: 700000*3 + 120000}

	err := p.normalizeSegmentSizesWithYenc(context.Background(), segs, first, 700000, nil, time.Time{})
	if err != nil {
		t.Fatalf("normalize returned error: %v", err)
	}
	if got := fp.BodyAsyncCalls(); got != 0 {
		t.Fatalf("BodyAsyncCalls = %d, want 0 (last size must be derived, not fetched)", got)
	}
	wantSizes := []int{700000, 700000, 700000, 120000}
	for i, want := range wantSizes {
		if segs[i].Bytes != want {
			t.Errorf("segment %d size = %d, want %d", i, segs[i].Bytes, want)
		}
	}
}

func TestNormalizeSegmentSizes_TwoSegmentDerive(t *testing.T) {
	fp := fakepool.New()
	mgr := newFakeFullPoolManager(fp)
	p := NewParser(mgr, stormConfigGetter(4))

	segs := normalizeSegs(2)
	first := firstSegmentYencInfo{PartSize: 700000, FileSize: 700000 + 50000}

	err := p.normalizeSegmentSizesWithYenc(context.Background(), segs, first, 0, nil, time.Time{})
	if err != nil {
		t.Fatalf("normalize returned error: %v", err)
	}
	if got := fp.BodyAsyncCalls(); got != 0 {
		t.Fatalf("BodyAsyncCalls = %d, want 0", got)
	}
	if segs[0].Bytes != 700000 || segs[1].Bytes != 50000 {
		t.Errorf("sizes = [%d, %d], want [700000, 50000]", segs[0].Bytes, segs[1].Bytes)
	}
}

func TestNormalizeSegmentSizes_FallsBackToFetchWhenFileSizeUnknown(t *testing.T) {
	fp := fakepool.New()
	mgr := newFakeFullPoolManager(fp)
	p := NewParser(mgr, stormConfigGetter(4))

	segs := normalizeSegs(4)
	first := firstSegmentYencInfo{PartSize: 700000, FileSize: 0} // e.g. Opt-1-skipped file

	// The fake pool yields no yEnc headers, so the fetch attempt fails — what
	// matters here is that the fetch WAS attempted (BodyAsync issued).
	_ = p.normalizeSegmentSizesWithYenc(context.Background(), segs, first, 700000, nil, time.Time{})
	if got := fp.BodyAsyncCalls(); got == 0 {
		t.Fatal("BodyAsyncCalls = 0, want a last-segment fetch fallback when FileSize is unknown")
	}
}

func TestNormalizeSegmentSizes_FallsBackToFetchOnInsaneDerivation(t *testing.T) {
	fp := fakepool.New()
	mgr := newFakeFullPoolManager(fp)
	p := NewParser(mgr, stormConfigGetter(4))

	segs := normalizeSegs(4)
	// FileSize wildly larger than the parts can account for → derivation rejected.
	first := firstSegmentYencInfo{PartSize: 700000, FileSize: 700000 * 100}

	_ = p.normalizeSegmentSizesWithYenc(context.Background(), segs, first, 700000, nil, time.Time{})
	if got := fp.BodyAsyncCalls(); got == 0 {
		t.Fatal("BodyAsyncCalls = 0, want a last-segment fetch fallback on insane derivation")
	}
}
