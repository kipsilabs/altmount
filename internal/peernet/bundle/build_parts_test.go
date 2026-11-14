package bundle

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stored mimics what the importer writes for a file stored in a multi-volume
// archive set: the volumes' articles (volumeArticles each), of which the file
// takes [dataStart, volumeBytes-trailer) of every volume, flattened into one
// segment list.
type storedSet struct {
	ids      [][]string // article ids per volume
	sizes    [][]int64  // decoded article sizes per volume
	segs     []specSeg
	truthLen []int // true part lengths
}

func newStoredSet(r *rand.Rand, volumes int, articles func(v int) int, head, tail int64, lastShort bool) storedSet {
	var s storedSet
	for v := 0; v < volumes; v++ {
		n := articles(v)
		ids, sizes := make([]string, n), make([]int64, n)
		for j := range ids {
			ids[j] = fmt.Sprintf("v%d-a%d@set.test", v, j)
			sizes[j] = 1000
		}
		if lastShort {
			sizes[n-1] = int64(200 + r.Intn(700))
		}
		s.ids, s.sizes = append(s.ids, ids), append(s.sizes, sizes)
		var total int64
		for _, z := range sizes {
			total += z
		}
		lo, hi := head, total-tail // usable bytes [lo, hi) of the volume
		var at, count int64
		for j, z := range sizes {
			a, b := max(lo-at, 0), min(hi-at, z)
			if b > a {
				s.segs = append(s.segs, specSeg{id: ids[j], size: z, start: a, end: b - 1})
				count++
			}
			at += z
		}
		s.truthLen = append(s.truthLen, int(count))
	}
	return s
}

func (s storedSet) nzb() [][]string { return s.ids }

func (s storedSet) file() specFile {
	var size int64
	for _, g := range s.segs {
		size += g.end - g.start + 1
	}
	return specFile{name: "movie.mkv", size: size, date: 1700000100, segs: s.segs}
}

func buildInput(scheme Scheme, nzb [][]string, files ...specFile) ReleaseInput {
	in := ReleaseInput{Scheme: scheme, Title: "Some Release 2024", PostedAt: time.Unix(1700000000, 0), NZBFiles: nzb}
	for _, f := range files {
		in.Files = append(in.Files, f.input())
	}
	return in
}

func TestFromLocal_MultiVolumeStoredSetBecomesParts(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	set := newStoredSet(r, 4, func(int) int { return 6 }, 60, 20, false)
	bd, err := FromLocal(buildInput(SchemeNZB, set.nzb(), set.file()))
	require.NoError(t, err)
	require.Len(t, bd.Files, 1)
	assert.Equal(t, set.truthLen, bd.Files[0].Parts)
	require.NoError(t, Validate(bd))
}

func TestFromLocal_PartsAreDeterministicAndOrderIndependent(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	set := newStoredSet(r, 3, func(int) int { return 5 }, 60, 20, false)
	nfo := specFile{name: "movie.nfo", size: 2000, date: 1700000100, segs: []specSeg{full("n1@set.test", 1000), full("n2@set.test", 1000)}}
	nzb := append(set.nzb(), []string{"n1@set.test", "n2@set.test"})
	a, err := FromLocal(buildInput(SchemeNZB, nzb, set.file(), nfo))
	require.NoError(t, err)
	b, err := FromLocal(buildInput(SchemeNZB, nzb, nfo, set.file()))
	require.NoError(t, err)
	assert.Equal(t, mustEncode(t, a), mustEncode(t, b))
	c, err := FromLocal(buildInput(SchemeNZB, nzb, set.file(), nfo))
	require.NoError(t, err)
	assert.Equal(t, mustEncode(t, a), mustEncode(t, c))
}

// A whole-entry table (a single-volume file, or a plain file) keeps the
// original layout: no parts, the same bytes as before the layout existed.
func TestFromLocal_WholeEntryTableStaysSinglePart(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	one := newStoredSet(r, 1, func(int) int { return 5 }, 60, 20, false)
	bd, err := FromLocal(buildInput(SchemeNZB, one.nzb(), one.file()))
	require.NoError(t, err)
	assert.Empty(t, bd.Files[0].Parts)
	bd, err = FromLocal(buildInput(SchemeFiles, nil, one.file()))
	require.NoError(t, err)
	assert.Empty(t, bd.Files[0].Parts)
}

func TestFromLocal_SliceOfOneEntryIsASinglePartLayout(t *testing.T) {
	// Two files stored back to back in one volume: neither is a whole entry.
	ids := []string{"a0@s.test", "a1@s.test", "a2@s.test", "a3@s.test", "a4@s.test"}
	a := specFile{name: "a.mkv", size: 1000 - 60 + 1000 + 500, segs: []specSeg{{id: ids[0], size: 1000, start: 60, end: 999}, full(ids[1], 1000), {id: ids[2], size: 1000, start: 0, end: 499}}}
	b := specFile{name: "b.mkv", size: 500 + 1000 + 900, segs: []specSeg{{id: ids[2], size: 1000, start: 500, end: 999}, full(ids[3], 1000), {id: ids[4], size: 1000, start: 0, end: 899}}}
	bd, err := FromLocal(buildInput(SchemeNZB, [][]string{ids}, a, b))
	require.NoError(t, err)
	for _, f := range bd.Files {
		assert.Equal(t, []int{3}, f.Parts, f.Name)
	}
}

func TestFromLocal_PartLayoutThatCannotBeRepresentedIsNotShareable(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	cases := map[string]func(set *storedSet) specFile{
		"a patched segment repeats an article": func(set *storedSet) specFile {
			f := set.file()
			last := f.segs[len(f.segs)-1]
			f.segs = append(f.segs, specSeg{id: last.id, size: last.size, start: last.start, end: last.start + 9})
			f.size += 10
			return f
		},
		"two runs of one volume": func(set *storedSet) specFile {
			f := set.file()
			f.segs = append(append([]specSeg{}, f.segs[:2]...), f.segs[3:]...) // skip an article of volume 0
			f.size = specFile{segs: f.segs}.fileSize()
			return f
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			set := newStoredSet(r, 3, func(int) int { return 5 }, 60, 20, false)
			_, err := FromLocal(buildInput(SchemeNZB, set.nzb(), mut(&set)))
			require.ErrorIs(t, err, ErrNotShareable)
			var ns *NotShareableError
			require.True(t, errors.As(err, &ns))
			assert.Equal(t, ReasonPartLayout, ns.Reason)
		})
	}
}

func (f specFile) fileSize() int64 {
	var n int64
	for _, s := range f.segs {
		n += s.end - s.start + 1
	}
	return n
}

// Files scheme: no NZB entries exist, so the boundaries come from the ranges.
// For a RAR set the rule is exact: every volume starts with a header, so the
// first segment of every part after the first starts inside its article.
func TestFromLocal_FilesSchemeBoundariesFromRanges(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	for trial := 0; trial < 300; trial++ {
		volumes := 2 + r.Intn(8)
		head, tail := int64(8+r.Intn(400)), int64(7+r.Intn(60))
		set := newStoredSet(r, volumes, func(int) int { return 2 + r.Intn(7) }, head, tail, true)
		bd, err := FromLocal(buildInput(SchemeFiles, nil, set.file()))
		require.NoError(t, err, "trial %d", trial)
		assert.Equal(t, set.truthLen, bd.Files[0].Parts, "trial %d", trial)
	}
}

// A split 7z set continues across volumes with no header: the boundary leaves
// full ranges on both sides and is only visible when the last article of a
// posted file is shorter than the rest.
func TestFromLocal_FilesSchemeSplitSevenZip(t *testing.T) {
	var segs []specSeg
	var truth []int
	for v := 0; v < 3; v++ {
		n := 4
		for j := 0; j < n; j++ {
			size := int64(1000)
			if j == n-1 {
				size = 640
			}
			s := full(fmt.Sprintf("z%d-%d@s.test", v, j), size)
			if v == 0 && j == 0 {
				s.start = 32
			}
			if v == 2 && j == n-1 {
				s.end = 400
			}
			segs = append(segs, s)
		}
		truth = append(truth, n)
	}
	f := specFile{name: "movie.mkv", segs: segs}
	f.size = f.fileSize()
	bd, err := FromLocal(buildInput(SchemeFiles, nil, f))
	require.NoError(t, err)
	assert.Equal(t, truth, bd.Files[0].Parts)
}

// ToLocal returns the flat list with partial ranges the readers expect, equal
// field by field to the metadata the file was built from.
func TestToLocal_MultiPartFileRoundTripsFlat(t *testing.T) {
	for _, scheme := range []Scheme{SchemeNZB, SchemeFiles} {
		t.Run(scheme.String(), func(t *testing.T) {
			r := rand.New(rand.NewSource(6))
			set := newStoredSet(r, 5, func(v int) int { return 3 + v }, 90, 30, true)
			f := set.file()
			var nzb [][]string
			if scheme == SchemeNZB {
				nzb = set.nzb()
			}
			in := buildInput(scheme, nzb, f)
			bd, err := FromLocal(in)
			require.NoError(t, err)
			wire, id, err := Pack(bd)
			require.NoError(t, err)
			got, err := Verify(wire, id, bd.ReleaseID)
			require.NoError(t, err)
			loc, err := ToLocal(got, testNow)
			require.NoError(t, err)
			require.Len(t, loc.Files, 1)
			want, have := in.Files[0].Meta, loc.Files[0].Meta
			assert.Equal(t, want.FileSize, have.FileSize)
			assert.Equal(t, want.OriginalFilename, have.OriginalFilename)
			require.Len(t, have.SegmentData, len(want.SegmentData))
			for i := range want.SegmentData {
				assert.Equal(t, want.SegmentData[i].Id, have.SegmentData[i].Id, "segment %d", i)
				assert.Equal(t, want.SegmentData[i].SegmentSize, have.SegmentData[i].SegmentSize, "segment %d", i)
				assert.Equal(t, want.SegmentData[i].StartOffset, have.SegmentData[i].StartOffset, "segment %d", i)
				assert.Equal(t, want.SegmentData[i].EndOffset, have.SegmentData[i].EndOffset, "segment %d", i)
			}
		})
	}
}
