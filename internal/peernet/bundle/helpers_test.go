package bundle

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// specSeg is a segment of a test file. An empty id is a gap placeholder.
type specSeg struct {
	id               string
	size, start, end int64
}

type specOuter struct {
	segs []specSeg
	key  []byte
	iv   []byte
	vol  int64
}

type specExtent struct {
	outer       int
	off, length int64
}

type specPar2 struct {
	name string
	size int64
	segs []specSeg
}

// specFile is the test-side description of one local file; meta() renders it
// as the on-disk metadata message independently of the package under test.
type specFile struct {
	name    string
	size    int64
	date    int64
	segs    []specSeg
	key, iv []byte
	outer   []specOuter
	extents []specExtent
	clips   []Clip
	par2    []specPar2
}

func full(id string, size int64) specSeg { return specSeg{id: id, size: size, start: 0, end: size - 1} }

func segData(segs []specSeg, salt string) []*metapb.SegmentData {
	out := make([]*metapb.SegmentData, len(segs))
	for i, s := range segs {
		id := s.id
		if id == "" {
			id = holes.PlaceholderID(i+1, salt)
		}
		out[i] = &metapb.SegmentData{Id: id, SegmentSize: s.size, StartOffset: s.start, EndOffset: s.end}
	}
	return out
}

func (f specFile) meta() *metapb.FileMetadata {
	m := &metapb.FileMetadata{
		FileSize:         f.size,
		SourceNzbPath:    "/local/store/path.nzbz",
		StoreRef:         "",
		Status:           metapb.FileStatus_FILE_STATUS_HEALTHY,
		CreatedAt:        1700000000,
		ModifiedAt:       1700000001,
		ReleaseDate:      f.date,
		OriginalFilename: f.name,
		NzbdavId:         "dl-id-123",
		SegmentData:      segData(f.segs, f.name),
	}
	if len(f.key) > 0 {
		m.Encryption = metapb.Encryption_AES
		m.AesKey, m.AesIv = f.key, f.iv
	}
	for _, o := range f.outer {
		m.SharedOuterSources = append(m.SharedOuterSources, &metapb.NestedSegmentSource{
			Segments: segData(o.segs, f.name+"o"), AesKey: o.key, AesIv: o.iv, InnerVolumeSize: o.vol,
		})
	}
	for _, e := range f.extents {
		m.NestedSources = append(m.NestedSources, &metapb.NestedSegmentSource{
			SharedOuterSourceIndex: int32(e.outer + 1), InnerOffset: e.off, InnerLength: e.length,
		})
	}
	for _, c := range f.clips {
		m.ClipBoundaries = append(m.ClipBoundaries, &metapb.ClipBoundary{ByteLen: c.ByteLen, Delta_90K: c.Delta90k})
	}
	for _, p := range f.par2 {
		m.Par2Files = append(m.Par2Files, &metapb.Par2FileReference{
			Filename: p.name, FileSize: p.size, SegmentData: segData(p.segs, p.name),
		})
	}
	m.KnownHoles = []*metapb.HoleRun{{StartSegment: 1, Count: 1, RecordedAt: 1700000000}}
	m.HoleProviderFingerprint = "provider-fingerprint-secret"
	return m
}

func (f specFile) input() FileInput {
	return FileInput{Name: f.name, Meta: f.meta(), PasswordFromNZB: true}
}

func key32() []byte { return bytes.Repeat([]byte{0x42}, 32) }
func iv16() []byte  { return bytes.Repeat([]byte{0x24}, 16) }

// testSpecs returns four files covering every layout the schema holds.
func testSpecs() []specFile {
	return []specFile{
		{
			name: "movie.mkv", size: 2500, date: 1700000100,
			segs: []specSeg{full("m1@x.test", 1000), full("m2@x.test", 1000), full("m3@x.test", 500)},
			par2: []specPar2{{name: "movie.par2", size: 600, segs: []specSeg{full("p1@x.test", 600)}}},
		},
		{
			// AES: 100 plain bytes -> 112 encrypted, final block padded.
			name: "secret.bin", size: 100, date: 1700000200,
			segs: []specSeg{{id: "e1@x.test", size: 200, start: 10, end: 121}},
			key:  key32(), iv: iv16(),
		},
		{
			// Encrypted outer volume with two extents (shared outer source).
			name: "nested.mkv", size: 500, date: 1700000300,
			outer: []specOuter{{
				segs: []specSeg{full("o1@x.test", 504), full("o2@x.test", 504)},
				key:  key32(), iv: iv16(), vol: 1000,
			}},
			extents: []specExtent{{0, 10, 300}, {0, 400, 200}},
		},
		{
			// Unencrypted outer with clips and partial edges.
			name: "disc.m2ts", size: 576, date: 1700000400,
			outer: []specOuter{{segs: []specSeg{
				{id: "c1@x.test", size: 500, start: 100, end: 499},
				full("c2@x.test", 500),
				full("c3@x.test", 500),
			}}},
			extents: []specExtent{{0, 50, 192}, {0, 300, 384}},
			clips:   []Clip{{ByteLen: 192, Delta90k: 5}, {ByteLen: 384, Delta90k: -7}},
		},
	}
}

// testFileID computes the File ID with contentid directly from the spec.
func testFileID(t testing.TB, f specFile) contentid.ID {
	t.Helper()
	m := f.meta()
	if err := expandShared(m); err != nil {
		t.Fatal(err)
	}
	id, err := contentid.FileID(m)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func expandShared(m *metapb.FileMetadata) error {
	for _, ns := range m.NestedSources {
		if ns.SharedOuterSourceIndex == 0 {
			continue
		}
		sh := m.SharedOuterSources[ns.SharedOuterSourceIndex-1]
		ns.Segments, ns.AesKey, ns.AesIv, ns.InnerVolumeSize = sh.Segments, sh.AesKey, sh.AesIv, sh.InnerVolumeSize
	}
	return nil
}

func testFilesRelease(ids []contentid.ID) contentid.ID {
	id, err := contentid.ReleaseIDFromFiles(ids)
	if err != nil {
		panic(err)
	}
	return id
}

// assemble builds the expected bundle for specs under the "files" scheme,
// independently of FromLocal.
func assemble(t testing.TB, specs []specFile) *Bundle {
	t.Helper()
	type pair struct {
		id contentid.ID
		f  specFile
	}
	ps := make([]pair, len(specs))
	for i, s := range specs {
		ps[i] = pair{testFileID(t, s), s}
	}
	sort.Slice(ps, func(i, j int) bool { return bytes.Compare(ps[i].id[:], ps[j].id[:]) < 0 })

	b := &Bundle{Scheme: SchemeFiles, Title: "Some Release 2024", PostedUnix: 1700000000}
	idx := map[string]int64{}
	ref := func(segs []specSeg) []Segment {
		if len(segs) == 0 {
			return nil
		}
		out := make([]Segment, len(segs))
		for i, s := range segs {
			r := GapRef
			if s.id != "" {
				v, ok := idx[s.id]
				if !ok {
					v = int64(len(b.Pool))
					idx[s.id] = v
					b.Pool = append(b.Pool, s.id)
				}
				r = v
			}
			out[i] = Segment{Ref: r, Size: s.size, Start: s.start, End: s.end}
		}
		return out
	}
	var ids []contentid.ID
	for _, p := range ps {
		f := File{Name: p.f.name, Size: p.f.size, ReleaseDate: p.f.date, FileID: p.id,
			Segments: ref(p.f.segs), AESKey: p.f.key, AESIV: p.f.iv, Clips: p.f.clips}
		for _, o := range p.f.outer {
			f.Outer = append(f.Outer, OuterSource{Segments: ref(o.segs), AESKey: o.key, AESIV: o.iv, VolumeSize: o.vol})
		}
		for _, e := range p.f.extents {
			f.Extents = append(f.Extents, Extent{Outer: e.outer, InnerOffset: e.off, InnerLength: e.length})
		}
		for _, q := range p.f.par2 {
			f.Par2 = append(f.Par2, Par2File{Name: q.name, Size: q.size, Segments: ref(q.segs)})
		}
		b.Files = append(b.Files, f)
		b.TotalSize += f.Size
		ids = append(ids, p.id)
	}
	b.ReleaseID = testFilesRelease(ids)
	return b
}

func goodBundle(t testing.TB) *Bundle { t.Helper(); return assemble(t, testSpecs()) }

// clone deep-copies a bundle so a test can break one field.
func clone(b *Bundle) *Bundle {
	c := *b
	c.ReleaseFiles = append([]int(nil), b.ReleaseFiles...)
	c.Pool = append([]string(nil), b.Pool...)
	c.Files = make([]File, len(b.Files))
	for i, f := range b.Files {
		f.Segments = append([]Segment(nil), f.Segments...)
		f.AESKey = append([]byte(nil), f.AESKey...)
		f.AESIV = append([]byte(nil), f.AESIV...)
		f.Extents = append([]Extent(nil), f.Extents...)
		f.Clips = append([]Clip(nil), f.Clips...)
		f.Outer = append([]OuterSource(nil), f.Outer...)
		for j, o := range f.Outer {
			o.Segments = append([]Segment(nil), o.Segments...)
			o.AESKey = append([]byte(nil), o.AESKey...)
			o.AESIV = append([]byte(nil), o.AESIV...)
			f.Outer[j] = o
		}
		f.Par2 = append([]Par2File(nil), f.Par2...)
		for j, p := range f.Par2 {
			p.Segments = append([]Segment(nil), p.Segments...)
			f.Par2[j] = p
		}
		c.Files[i] = f
	}
	return &c
}

func fileNamed(t testing.TB, b *Bundle, name string) *File {
	t.Helper()
	for i := range b.Files {
		if b.Files[i].Name == name {
			return &b.Files[i]
		}
	}
	t.Fatalf("no file %q", name)
	return nil
}

// synthetic release for size measurements: n segments of random ids.
func syntheticIDs(n int, seed int64) []string {
	r := rand.New(rand.NewSource(seed))
	const alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	domains := []string{"powerpost2000AA.local", "nyuu.local", "JBinUp.local", "news.example"}
	out := make([]string, n)
	for i := range out {
		l := 20 + r.Intn(12)
		b := make([]byte, l)
		for j := range b {
			b[j] = alpha[r.Intn(len(alpha))]
		}
		out[i] = fmt.Sprintf("%s@%s", b, domains[r.Intn(len(domains))])
	}
	return out
}

// syntheticBundle is one plain file of n full segments (files scheme).
func syntheticBundle(t testing.TB, n int, segSize int64) *Bundle {
	t.Helper()
	ids := syntheticIDs(n, 1)
	segs := make([]specSeg, n)
	for i := range segs {
		segs[i] = full(ids[i], segSize)
	}
	last := n - 1
	segs[last] = full(ids[last], segSize/2)
	return assemble(t, []specFile{{name: "big.mkv", size: segSize*int64(n-1) + segSize/2, date: 1700000000, segs: segs}})
}

var testNow = time.Unix(1_700_001_000, 0)

// nzbLayout is the NZB file list the nzb-scheme fixture is built on: the
// imported files' segments, plus one NZB file that was not imported.
func nzbLayout() [][]string {
	return [][]string{
		{"m1@x.test", "m2@x.test", "m3@x.test"},
		{"p1@x.test"},
		{"e1@x.test"},
		{"o1@x.test", "o2@x.test"},
		{"c1@x.test", "c2@x.test", "c3@x.test"},
		{"x1@x.test", "x2@x.test"},
	}
}

func nzbStore(files [][]string) *metapb.NzbStore {
	st := &metapb.NzbStore{}
	for _, f := range files {
		fe := &metapb.NzbFileEntry{}
		for i, id := range f {
			fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id, Number: int32(i + 1), Bytes: 1})
		}
		st.Files = append(st.Files, fe)
	}
	return st
}

// assembleNZB builds the expected nzb-scheme bundle independently of FromLocal.
func assembleNZB(t testing.TB, specs []specFile, files [][]string) *Bundle {
	t.Helper()
	b := assemble(t, specs)
	b.Scheme = SchemeNZB
	b.Pool = nil
	idx := map[string]int64{}
	for _, f := range files {
		b.ReleaseFiles = append(b.ReleaseFiles, len(f))
		for _, id := range f {
			if _, ok := idx[id]; !ok {
				idx[id] = int64(len(b.Pool))
			}
			b.Pool = append(b.Pool, id)
		}
	}
	remap := func(segs []Segment, old []string) {
		for i := range segs {
			if segs[i].Ref != GapRef {
				segs[i].Ref = idx[old[segs[i].Ref]]
			}
		}
	}
	old := assemble(t, specs).Pool
	for i := range b.Files {
		remap(b.Files[i].Segments, old)
		for j := range b.Files[i].Outer {
			remap(b.Files[i].Outer[j].Segments, old)
		}
		for j := range b.Files[i].Par2 {
			remap(b.Files[i].Par2[j].Segments, old)
		}
	}
	rid, err := contentid.ReleaseID(nzbStore(files))
	if err != nil {
		t.Fatal(err)
	}
	b.ReleaseID = rid
	return b
}

func goodNZBBundle(t testing.TB) *Bundle { t.Helper(); return assembleNZB(t, testSpecs(), nzbLayout()) }
