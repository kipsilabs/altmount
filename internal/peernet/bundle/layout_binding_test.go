package bundle

import (
	"errors"
	"fmt"
	"testing"
)

func mkSeg(r int64, size int64) Segment { return Segment{Ref: r, Size: size, Start: 0, End: size - 1} }

// filesBundle wraps hand-made files in a files-scheme bundle that Validate
// accepts apart from the property a test exercises.
func filesBundle(pool []string, files ...File) *Bundle {
	b := &Bundle{Scheme: SchemeFiles, Title: "T", PostedUnix: 1700000000, Pool: pool, ReleaseID: [32]byte{9}}
	for i := range files {
		files[i].FileID = [32]byte{byte(i + 1)}
		b.TotalSize += files[i].Size
	}
	b.Files = files
	return b
}

func poolOf(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("part%d@poc.example", i)
	}
	return out
}

// P1: one article referenced 1,999 times must not pass as a 1.4 GB file.
func TestFilesSchemeRepeatedRefRejected(t *testing.T) {
	segs := make([]Segment, 2000)
	for i := range segs {
		segs[i] = mkSeg(int64(min(i, 1)), 700000)
	}
	b := filesBundle(poolOf(2), File{Name: "Movie.mkv", Size: 2000 * 700000, Segments: segs})
	if err := Validate(b); !errors.Is(err, ErrCoverage) {
		t.Fatalf("want ErrCoverage, got %v", err)
	}
}

// P2: interior segments that skip bytes must not shrink a big release.
func TestInteriorPartialRangeRejected(t *testing.T) {
	segs := make([]Segment, 100)
	for i := range segs {
		segs[i] = Segment{Ref: int64(i), Size: 700000, Start: 350000, End: 350009}
	}
	b := filesBundle(poolOf(100), File{Name: "Movie.mkv", Size: 1000, Segments: segs})
	if err := Validate(b); !errors.Is(err, ErrPartialRange) {
		t.Fatalf("want ErrPartialRange, got %v", err)
	}
}

func TestPartialEdgesStillAccepted(t *testing.T) {
	segs := []Segment{
		{Ref: 0, Size: 1000, Start: 100, End: 999},
		mkSeg(1, 1000),
		{Ref: 2, Size: 1000, Start: 0, End: 499},
	}
	b := filesBundle(poolOf(3), File{Name: "Movie.mkv", Size: 900 + 1000 + 500, Segments: segs})
	if err := Validate(b); err != nil {
		t.Fatal(err)
	}
	one := filesBundle(poolOf(1), File{Name: "a.bin", Size: 10, Segments: []Segment{{Ref: 0, Size: 1000, Start: 5, End: 14}}})
	if err := Validate(one); err != nil {
		t.Fatalf("single segment with both edges partial: %v", err)
	}
}

// P4: two files sharing their first article must not both pass.
func TestFilesSchemeSharedFirstEntryRejected(t *testing.T) {
	a := File{Name: "A.mkv", Size: 3000, Segments: []Segment{mkSeg(0, 1000), mkSeg(1, 1000), mkSeg(2, 1000)}}
	c := File{Name: "B.mkv", Size: 3000, Segments: []Segment{mkSeg(0, 1000), mkSeg(3, 1000), mkSeg(4, 1000)}}
	if err := Validate(filesBundle(poolOf(5), a, c)); !errors.Is(err, ErrCoverage) {
		t.Fatalf("want ErrCoverage, got %v", err)
	}
}

// An identical table (shared outer volume, PAR2 listed by two files) stays legal.
func TestIdenticalSharedTableAccepted(t *testing.T) {
	par := Par2File{Name: "x.par2", Size: 1000, Segments: []Segment{mkSeg(2, 1000)}}
	a := File{Name: "A.mkv", Size: 1000, Segments: []Segment{mkSeg(0, 1000)}, Par2: []Par2File{par}}
	c := File{Name: "B.mkv", Size: 1000, Segments: []Segment{mkSeg(1, 1000)}, Par2: []Par2File{par}}
	if err := Validate(filesBundle(poolOf(3), a, c)); err != nil {
		t.Fatal(err)
	}
	// Same articles but a different size claim is not the identical table.
	par2 := Par2File{Name: "x.par2", Size: 900, Segments: []Segment{{Ref: 2, Size: 1000, Start: 0, End: 899}}}
	c.Par2 = []Par2File{par2}
	if err := Validate(filesBundle(poolOf(3), a, c)); !errors.Is(err, ErrCoverage) {
		t.Fatalf("want ErrCoverage, got %v", err)
	}
}

func TestFilesSchemeSegmentCeiling(t *testing.T) {
	big := int64(MaxFilesSchemeSegment + 1)
	b := filesBundle(poolOf(1), File{Name: "a.bin", Size: big, Segments: []Segment{mkSeg(0, big)}})
	if err := Validate(b); !errors.Is(err, ErrImplausible) {
		t.Fatalf("want ErrImplausible, got %v", err)
	}
}
