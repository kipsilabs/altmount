package bundle

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// The multi-part inline layout: a file stored in an archive set is the
// concatenation of slices, one per volume (NZB entry). These tests pin the exact
// rules; the accept-time binding to the archive headers lives in bundleimport.

const partArticle = 1000

// entryBase returns the pool position of the first article of each entry.
func entryBase(entries []int) []int64 {
	out := make([]int64, len(entries))
	var at int64
	for i, n := range entries {
		out[i] = at
		at += int64(n)
	}
	return out
}

// run is a contiguous run of articles of one entry: articles [from, to] of
// entry e, the first starting at start and the last ending at end (-1: whole).
func run(entries []int, e, from, to int, start, end int64) []Segment {
	base := entryBase(entries)
	var out []Segment
	for j := from; j <= to; j++ {
		s := Segment{Ref: base[e] + int64(j), Size: partArticle, Start: 0, End: partArticle - 1}
		if j == from && start > 0 {
			s.Start = start
		}
		if j == to && end >= 0 {
			s.End = end
		}
		out = append(out, s)
	}
	return out
}

func usable(segs []Segment) int64 {
	var n int64
	for _, s := range segs {
		n += s.End - s.Start + 1
	}
	return n
}

func join(parts ...[]Segment) (segs []Segment, lens []int) {
	for _, p := range parts {
		segs = append(segs, p...)
		lens = append(lens, len(p))
	}
	return segs, lens
}

// nzbBundleOf builds an nzb-scheme bundle over entries (article counts).
func nzbBundleOf(entries []int, files ...File) *Bundle {
	b := &Bundle{Scheme: SchemeNZB, Title: "T", PostedUnix: 1700000000, ReleaseID: [32]byte{9}, ReleaseFiles: entries}
	n := 0
	for e, c := range entries {
		for j := 0; j < c; j++ {
			b.Pool = append(b.Pool, fmt.Sprintf("e%d-%d@parts.test", e, j))
			n++
		}
	}
	for i := range files {
		files[i].FileID = [32]byte{byte(i + 1)}
		b.TotalSize += files[i].Size
	}
	b.Files = files
	return b
}

// volumeFile is a file stored across the given entries: three volumes of
// [4,4,3] articles, partial at both ends of every part as a RAR set leaves it.
func volumeFile(entries []int) File {
	segs, lens := join(
		run(entries, 0, 0, 3, 50, 979),
		run(entries, 1, 0, 3, 40, 979),
		run(entries, 2, 0, 2, 40, 499),
	)
	return File{Name: "movie.mkv", Size: usable(segs), Segments: segs, Parts: lens}
}

var threeVolumes = []int{4, 4, 3}

func TestParts_MultiVolumeLayoutIsValid(t *testing.T) {
	f := volumeFile(threeVolumes)
	require.NoError(t, Validate(nzbBundleOf(threeVolumes, f)))
	// The same layout inside a larger release: another entry is not imported.
	require.NoError(t, Validate(nzbBundleOf([]int{4, 4, 3, 2}, volumeFile([]int{4, 4, 3, 2}))))
}

func TestParts_RulesNZBScheme(t *testing.T) {
	cases := map[string]struct {
		mutate func(f *File)
		want   error
	}{
		"interior partial range inside a part": {func(f *File) { f.Segments[1].Start = 5 }, ErrPartialRange},
		"interior short end inside a part":     {func(f *File) { f.Segments[2].End = 600 }, ErrPartialRange},
		"a part spans two entries": {func(f *File) {
			f.Segments[3].End, f.Segments[4].Start = partArticle-1, 0
			f.Parts, f.Size = []int{5, 3, 3}, usable(f.Segments)
		}, ErrCoverage},
		"two parts of one entry in one file": {func(f *File) {
			f.Segments = append(run(threeVolumes, 0, 0, 1, 50, 979), run(threeVolumes, 0, 2, 3, 0, 979)...)
			f.Segments = append(f.Segments, run(threeVolumes, 2, 0, 2, 40, 499)...)
			f.Parts = []int{2, 2, 3}
			f.Size = usable(f.Segments)
		}, ErrCoverage},
		"a part is out of article order": {func(f *File) {
			f.Segments[1].Ref, f.Segments[2].Ref = f.Segments[2].Ref, f.Segments[1].Ref
		}, ErrCoverage},
		"part lengths do not add up": {func(f *File) { f.Parts = []int{4, 4, 2} }, ErrInvalidField},
		"an empty part":              {func(f *File) { f.Parts = []int{4, 0, 4, 3} }, ErrInvalidField},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := volumeFile(threeVolumes)
			c.mutate(&f)
			err := Validate(nzbBundleOf(threeVolumes, f))
			require.Error(t, err)
			require.ErrorIs(t, err, c.want)
		})
	}
}

func TestParts_OtherNZBRules(t *testing.T) {
	t.Run("a part skips an article of its entry", func(t *testing.T) {
		entries := []int{5, 4, 3}
		segs, lens := join(
			append(run(entries, 0, 0, 2, 50, -1), run(entries, 0, 4, 4, -1, 979)...),
			run(entries, 1, 0, 3, 40, 979),
			run(entries, 2, 0, 2, 40, 499),
		)
		f := File{Name: "movie.mkv", Size: usable(segs), Segments: segs, Parts: lens}
		require.ErrorIs(t, Validate(nzbBundleOf(entries, f)), ErrCoverage)
	})
	t.Run("an entry also read whole by another file", func(t *testing.T) {
		whole := File{Name: "other.bin", Size: 4 * partArticle, Segments: run(threeVolumes, 1, 0, 3, -1, -1)}
		require.ErrorIs(t, Validate(nzbBundleOf(threeVolumes, volumeFile(threeVolumes), whole)), ErrCoverage)
	})
	t.Run("an entry also used as an outer volume", func(t *testing.T) {
		nested := File{Name: "n.mkv", Size: 100, Outer: []OuterSource{{Segments: run(threeVolumes, 1, 0, 3, -1, -1)}},
			Extents: []Extent{{Outer: 0, InnerOffset: 0, InnerLength: 100}}}
		require.ErrorIs(t, Validate(nzbBundleOf(threeVolumes, volumeFile(threeVolumes), nested)), ErrCoverage)
	})
	t.Run("parts on a nested file", func(t *testing.T) {
		nested := File{Name: "n.mkv", Size: 100, Parts: []int{1}, Outer: []OuterSource{{Segments: run(threeVolumes, 1, 0, 3, -1, -1)}},
			Extents: []Extent{{Outer: 0, InnerOffset: 0, InnerLength: 100}}}
		require.ErrorIs(t, Validate(nzbBundleOf(threeVolumes, nested)), ErrInvalidField)
	})
	t.Run("a single part that is a slice of the middle of an entry", func(t *testing.T) {
		segs := run(threeVolumes, 1, 1, 2, 10, 20)
		f := File{Name: "n.srt", Size: usable(segs), Segments: segs, Parts: []int{2}}
		require.NoError(t, Validate(nzbBundleOf(threeVolumes, f)))
		f.Parts = nil // without the declaration it is the original single-table layout
		require.ErrorIs(t, Validate(nzbBundleOf(threeVolumes, f)), ErrCoverage)
	})
}

// Two files stored back to back in one volume share the article where the
// first ends and the second begins, and only that.
func TestParts_SharedBoundaryArticle(t *testing.T) {
	entries := []int{6}
	mk := func(name string, from, to int, start, end int64) File {
		segs := run(entries, 0, from, to, start, end)
		return File{Name: name, Size: usable(segs), Segments: segs, Parts: []int{len(segs)}}
	}
	t.Run("back to back in one article", func(t *testing.T) {
		a, b := mk("a.mkv", 0, 2, 30, 499), mk("b.mkv", 2, 4, 500, 699)
		require.NoError(t, Validate(nzbBundleOf(entries, a, b)))
	})
	t.Run("ranges overlap in the shared article", func(t *testing.T) {
		a, b := mk("a.mkv", 0, 2, 30, 499), mk("b.mkv", 2, 4, 499, 699)
		require.ErrorIs(t, Validate(nzbBundleOf(entries, a, b)), ErrCoverage)
	})
	t.Run("shared article is interior in one of them", func(t *testing.T) {
		a, b := mk("a.mkv", 0, 3, 30, 499), mk("b.mkv", 2, 4, 500, 699)
		require.ErrorIs(t, Validate(nzbBundleOf(entries, a, b)), ErrCoverage)
	})
	t.Run("whole article used by both", func(t *testing.T) {
		a, b := mk("a.mkv", 0, 2, 30, -1), mk("b.mkv", 2, 4, 0, 699)
		require.ErrorIs(t, Validate(nzbBundleOf(entries, a, b)), ErrCoverage)
	})
	t.Run("disjoint article ranges of one entry", func(t *testing.T) {
		a, b := mk("a.mkv", 0, 1, 30, 499), mk("b.mkv", 2, 4, 20, 699)
		require.NoError(t, Validate(nzbBundleOf(entries, a, b)))
	})
	t.Run("a whole-entry file may not share with a sliced one", func(t *testing.T) {
		a := mk("a.mkv", 0, 2, 30, 499)
		whole := File{Name: "w.bin", Size: 6 * partArticle, Segments: run(entries, 0, 0, 5, -1, -1)}
		require.ErrorIs(t, Validate(nzbBundleOf(entries, a, whole)), ErrCoverage)
	})
	t.Run("three tiny files in one article", func(t *testing.T) {
		tiny := func(name string, start, end int64) File {
			return File{Name: name, Size: end - start + 1, Segments: []Segment{{Ref: 0, Size: partArticle, Start: start, End: end}}, Parts: []int{1}}
		}
		require.NoError(t, Validate(nzbBundleOf([]int{1}, tiny("a.srt", 100, 199), tiny("b.srt", 200, 299), tiny("c.srt", 300, 399))))
		require.ErrorIs(t, Validate(nzbBundleOf([]int{1}, tiny("a.srt", 100, 250), tiny("b.srt", 200, 299))), ErrCoverage)
	})
}

// The P2 shape (100 articles, each used for 10 bytes from the middle) is still
// refused as ONE table, and as parts it is only a layout that must be bound
// against an archive header later.
func TestParts_P2ShapeStaysRefused(t *testing.T) {
	segs := make([]Segment, 100)
	for i := range segs {
		segs[i] = Segment{Ref: int64(i), Size: 700000, Start: 350000, End: 350009}
	}
	entries := make([]int, 100)
	for i := range entries {
		entries[i] = 1
	}
	one := File{Name: "Movie.mkv", Size: 1000, Segments: segs}
	require.ErrorIs(t, Validate(nzbBundleOf(entries, one)), ErrPartialRange)
	// Interior slices of ONE part are refused too.
	lens := []int{100}
	require.ErrorIs(t, Validate(nzbBundleOf(entries, File{Name: "Movie.mkv", Size: 1000, Segments: segs, Parts: lens})), ErrPartialRange)
}

func filesPartsBundle(pool int, files ...File) *Bundle {
	return filesBundle(poolOf(pool), files...)
}

func TestParts_FilesScheme(t *testing.T) {
	mk := func(refs ...int64) []Segment {
		var out []Segment
		for i, r := range refs {
			s := mkSeg(r, partArticle)
			if i == 0 {
				s.Start = 40
			}
			if i == len(refs)-1 {
				s.End = 979
			}
			out = append(out, s)
		}
		return out
	}
	a, b := mk(0, 1, 2), mk(3, 4, 5)
	segs := append(append([]Segment{}, a...), b...)
	f := File{Name: "movie.mkv", Size: usable(segs), Segments: segs, Parts: []int{3, 3}}
	require.NoError(t, Validate(filesPartsBundle(6, f)))

	t.Run("a pool entry shared by two files at their interiors", func(t *testing.T) {
		other := File{Name: "other.mkv", Size: usable(mk(2, 6)), Segments: mk(2, 6), Parts: []int{2}}
		require.ErrorIs(t, Validate(filesPartsBundle(7, f, other)), ErrCoverage)
	})
	t.Run("an article repeated in the table", func(t *testing.T) {
		bad := f
		bad.Segments = append([]Segment(nil), f.Segments...)
		bad.Segments[3].Ref = 0
		require.ErrorIs(t, Validate(filesPartsBundle(6, bad)), ErrCoverage)
	})
	t.Run("an unreferenced pool entry is refused", func(t *testing.T) {
		require.ErrorIs(t, Validate(filesPartsBundle(7, f)), ErrBadPool)
	})
}

// partsNZBBundle is a valid, fully identified nzb-scheme bundle with one
// multi-volume file, a plain file and one NZB entry that was not imported.
func partsNZBBundle(t testing.TB) *Bundle {
	t.Helper()
	entries := []int{4, 4, 3, 2, 1}
	vol := volumeFile(entries[:3])
	plain := File{Name: "movie.nfo", Size: 2 * partArticle, Segments: run(entries, 3, 0, 1, -1, -1)}
	b := nzbBundleOf(entries, vol, plain)
	b.Files = []File{vol, plain}
	b.TotalSize = vol.Size + plain.Size
	for i := range b.Files {
		id, err := contentid.FileID(buildMeta(b, &b.Files[i]))
		require.NoError(t, err)
		b.Files[i].FileID = id
	}
	sort.Slice(b.Files, func(i, j int) bool { return bytes.Compare(b.Files[i].FileID[:], b.Files[j].FileID[:]) < 0 })
	rid, err := releaseIDOfList(b.Pool, b.ReleaseFiles)
	require.NoError(t, err)
	b.ReleaseID = rid
	require.NoError(t, Validate(b))
	return b
}

func TestParts_WireRoundTripKeepsPartsAndIsCanonical(t *testing.T) {
	b := partsNZBBundle(t)
	raw := mustEncode(t, b)
	got, err := Decode(raw)
	require.NoError(t, err)
	require.Equal(t, b, got)
	again, err := Encode(got)
	require.NoError(t, err)
	require.Equal(t, raw, again)
	wire, id, err := Pack(b)
	require.NoError(t, err)
	got2, err := Verify(wire, id, b.ReleaseID)
	require.NoError(t, err)
	require.Equal(t, b, got2)
}

// A bundle with no multi-part file encodes exactly as before the field existed:
// these digests were taken from the encoder before the change.
func TestParts_SinglePartBundlesKeepTheirBytes(t *testing.T) {
	for name, tc := range map[string]struct {
		b    *Bundle
		want string
	}{
		"files scheme": {goodBundle(t), "53df75e95861bde57dce1593781be9e4306db25abfa6ddbd93f563ce4c70c225"},
		"nzb scheme":   {goodNZBBundle(t), "cf74e63f5b2112fcc52fe57c980bb39ef77c892fe0f8673bc26faa142cfcd498"},
	} {
		raw := mustEncode(t, tc.b)
		sum := sha256.Sum256(raw)
		assert.Equal(t, tc.want, hex.EncodeToString(sum[:]), name)
	}
}

func TestParts_DecoderIsStrict(t *testing.T) {
	good := partsNZBBundle(t)
	file := func() *File { c := clone(good); return fileNamed(t, c, "movie.mkv") }
	d := func() *decoder {
		return &decoder{segBudget: MaxSegments, extentBudget: MaxExtents, clipBudget: MaxTotalClips,
			outerBudget: MaxTotalOuterSources, par2Budget: MaxTotalPar2Files, partBudget: MaxTotalParts, poolLen: int64(len(good.Pool))}
	}
	f := file()
	enc := appendFile(f)
	_, err := d().decodeFile(enc)
	require.NoError(t, err)
	t.Run("part lengths that do not add up", func(t *testing.T) {
		g := file()
		g.Parts = []int{4, 4, 2}
		_, err := Decode(encodeUnchecked(replaceFile(good, "movie.mkv", g)))
		require.ErrorIs(t, err, ErrInvalidField)
	})
	t.Run("a zero length part", func(t *testing.T) {
		g := file()
		g.Parts = []int{4, 0, 4, 3}
		_, err := Decode(encodeUnchecked(replaceFile(good, "movie.mkv", g)))
		require.Error(t, err)
	})
	t.Run("more parts than segments", func(t *testing.T) {
		g := file()
		g.Parts = make([]int, len(g.Segments)+1)
		for i := range g.Parts {
			g.Parts[i] = 1
		}
		_, err := Decode(encodeUnchecked(replaceFile(good, "movie.mkv", g)))
		require.Error(t, err)
	})
	t.Run("an empty parts field", func(t *testing.T) {
		_, err := d().decodeFile(append(append([]byte{}, enc...), appendBytes(nil, fParts, nil)...))
		require.ErrorIs(t, err, ErrMalformed)
	})
	t.Run("the parts field twice", func(t *testing.T) {
		_, err := d().decodeFile(append(append([]byte{}, enc...), enc[len(enc)-len(appendParts(f.Parts)):]...))
		require.ErrorIs(t, err, ErrMalformed)
	})
	t.Run("the parts field before the file id", func(t *testing.T) {
		tail := appendParts(f.Parts)
		_, err := d().decodeFile(append(append(append([]byte{}, enc[:len(enc)-len(tail)-34]...), tail...), enc[len(enc)-len(tail)-34:len(enc)-len(tail)]...))
		require.ErrorIs(t, err, ErrMalformed)
	})
	t.Run("a non minimal varint is not the canonical encoding", func(t *testing.T) {
		tail := appendParts(f.Parts)
		raw := mustEncode(t, good)
		// 4 -> 0x84 0x00 (same value, longer encoding), then fix the lengths by re-wrapping.
		pb := []byte{0x84, 0x00}
		for _, n := range f.Parts[1:] {
			pb = protowire.AppendVarint(pb, uint64(n))
		}
		g := appendBytes(enc[:len(enc)-len(tail)], fParts, pb)
		raw2 := rebuildWithFile(t, raw, good, "movie.mkv", g)
		_, err := Decode(raw2)
		require.ErrorIs(t, err, ErrMalformed)
	})
}

func replaceFile(b *Bundle, name string, f *File) *Bundle {
	c := clone(b)
	for i := range c.Files {
		if c.Files[i].Name == name {
			c.Files[i] = *f
		}
	}
	return c
}

// rebuildWithFile re-wraps the top-level fields of raw with the named file's
// bytes replaced by fileBytes.
func rebuildWithFile(t testing.TB, raw []byte, b *Bundle, name string, fileBytes []byte) []byte {
	t.Helper()
	var out []rawField
	idx := 0
	for _, f := range splitTop(t, raw) {
		if f.num == fFiles {
			if b.Files[idx].Name == name {
				f.b = fileBytes
			}
			idx++
		}
		out = append(out, f)
	}
	return joinTop(out)
}
