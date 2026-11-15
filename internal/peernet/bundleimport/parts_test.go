package bundleimport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/archivehdr"
	"github.com/kipsilabs/altmount/internal/peernet/archivehdr/archivetest"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// A file stored in a multi-volume archive set is read as the concatenation of
// slices, one per volume (the multi-part inline layout). These tests build real
// stored archive volumes, post them through the fake pool as yEnc articles and
// run the whole accept pipeline over bundles that describe them.

type setSpec struct {
	tag     string
	kind    string // rar4, rar5 or 7z
	payload []byte
	// cuts is how many payload bytes each volume holds (rar4, rar5) or the
	// volume sizes of one split 7z archive (7z).
	cuts    []int
	segSize int
	scheme  bundle.Scheme
	// extra, when set, is stored as extra.nfo after the movie's data in volume
	// extraVol (RAR only).
	extra    []byte
	extraVol int
	// junk bytes are written before the signature of volume junkVol-1 (0 = none):
	// a volume that does not start like an archive, with every offset moved.
	junk, junkVol int
}

type builtSet struct {
	b      *bundle.Bundle
	posted [][]byte
	// lo and hi are the movie's usable range inside every posted volume.
	lo, hi []int
	ids    [][]string
	names  []string
	spec   setSpec
	// extraLo and extraHi are extra.nfo's data range inside volume spec.extraVol.
	extraLo, extraHi int
}

func volumeName(tag string, v int) string { return fmt.Sprintf("%s.part%02d.rar", tag, v+1) }

type artSlice struct {
	art, size, start, end int
}

// slices lists the articles of one posted volume that [lo, hi) touches, with
// the usable range inside each.
func volSlices(posted []byte, segSize, lo, hi int) []artSlice {
	var out []artSlice
	for i := 0; i*segSize < len(posted); i++ {
		from, end := i*segSize, min((i+1)*segSize, len(posted))
		if end > lo && from < hi {
			out = append(out, artSlice{i, end - from, max(lo-from, 0), min(hi, end) - from - 1})
		}
	}
	return out
}

func (e *env) mkStoredSet(sp setSpec) builtSet {
	e.t.Helper()
	if sp.segSize == 0 {
		sp.segSize = 128
	}
	s := builtSet{spec: sp}
	total := len(sp.payload)
	var extraLo, extraHi int
	switch sp.kind {
	case "7z":
		a := archivetest.SevenZip(archivetest.SevenZipOpts{}, archivetest.File{Name: "movie.mkv", Data: sp.payload})
		at := 0
		for _, n := range sp.cuts {
			s.posted = append(s.posted, a.Data[at:at+n])
			s.lo = append(s.lo, int(max(a.DataOffsets[0]-int64(at), 0)))
			s.hi = append(s.hi, int(min(a.DataOffsets[0]+int64(total)-int64(at), int64(n))))
			at += n
		}
		require.Equal(e.t, len(a.Data), at, "cuts must cover the archive")
	default:
		at := 0
		for v, n := range sp.cuts {
			files := []archivetest.File{{Name: "movie.mkv", Data: sp.payload[at : at+n], SplitBefore: v > 0, SplitAfter: v < len(sp.cuts)-1, UnpackedSize: int64(total)}}
			if v == sp.extraVol && sp.extra != nil {
				files = append(files, archivetest.File{Name: "extra.nfo", Data: sp.extra})
			}
			var a archivetest.Built
			if sp.kind == "rar5" {
				a = archivetest.RAR5Volume(v, files...)
			} else {
				a = archivetest.RAR4(files...)
			}
			shift := 0
			if sp.junkVol == v+1 {
				shift = sp.junk
				a.Data = append(bytes.Repeat([]byte{'x'}, shift), a.Data...)
			}
			s.posted = append(s.posted, a.Data)
			s.lo = append(s.lo, int(a.DataOffsets[0])+shift)
			s.hi = append(s.hi, int(a.DataOffsets[0])+shift+n)
			if v == sp.extraVol && sp.extra != nil {
				extraLo, extraHi = int(a.DataOffsets[1]), int(a.DataOffsets[1])+len(sp.extra)
			}
			at += n
		}
	}
	for v, posted := range s.posted {
		var ids []string
		for i := 0; i*sp.segSize < len(posted); i++ {
			from, end := i*sp.segSize, min((i+1)*sp.segSize, len(posted))
			id := fmt.Sprintf("%s-v%d-%03d@test", sp.tag, v, i)
			beh := fakepoolBehavior(posted[from:end], from, len(posted))
			beh.YEnc.FileName = volumeName(sp.tag, v)
			e.fp.SetBehavior(id, beh)
			ids = append(ids, id)
		}
		s.ids = append(s.ids, ids)
		s.names = append(s.names, volumeName(sp.tag, v))
	}
	// The pool: the whole NZB list (nzb scheme) or the referenced articles in
	// first-use order (files scheme).
	b := &bundle.Bundle{Scheme: sp.scheme, Title: "Test Release " + sp.tag, PostedUnix: 1_700_000_000}
	used := map[string]bool{}
	movie := bundle.File{Name: "movie.mkv", Size: int64(total)}
	extra := bundle.File{Name: "extra.nfo", Size: int64(len(sp.extra))}
	type pending struct {
		file       *bundle.File
		id         string
		sl         artSlice
		firstOfNew bool
	}
	var order []pending
	for v, posted := range s.posted {
		first := true
		for _, sl := range volSlices(posted, sp.segSize, s.lo[v], s.hi[v]) {
			order = append(order, pending{&movie, s.ids[v][sl.art], sl, first})
			first = false
			used[s.ids[v][sl.art]] = true
		}
		movie.Parts = append(movie.Parts, len(volSlices(posted, sp.segSize, s.lo[v], s.hi[v])))
		if v == sp.extraVol && sp.extra != nil {
			n := 0
			for _, sl := range volSlices(posted, sp.segSize, extraLo, extraHi) {
				order = append(order, pending{&extra, s.ids[v][sl.art], sl, false})
				used[s.ids[v][sl.art]] = true
				n++
			}
			extra.Parts = []int{n}
		}
	}
	if sp.scheme == bundle.SchemeNZB {
		for _, ids := range s.ids {
			b.ReleaseFiles = append(b.ReleaseFiles, len(ids))
			b.Pool = append(b.Pool, ids...)
		}
	} else {
		for _, p := range order {
			if !contains(b.Pool, p.id) {
				b.Pool = append(b.Pool, p.id)
			}
		}
	}
	for _, p := range order {
		p.file.Segments = append(p.file.Segments, bundle.Segment{Ref: poolPos(b, p.id), Size: int64(p.sl.size), Start: int64(p.sl.start), End: int64(p.sl.end)})
	}
	b.Files = []bundle.File{movie}
	if sp.extra != nil {
		b.Files = append(b.Files, extra)
	}
	for _, f := range b.Files {
		b.TotalSize += f.Size
	}
	s.b = b
	s.extraLo, s.extraHi = extraLo, extraHi
	e.sealAny(b)
	return s
}

func contains(pool []string, id string) bool { return poolPos(&bundle.Bundle{Pool: pool}, id) >= 0 }

func poolPos(b *bundle.Bundle, id string) int64 {
	for i, p := range b.Pool {
		if p == id {
			return int64(i)
		}
	}
	return -1
}

func (e *env) nzbReleaseID(b *bundle.Bundle) contentid.ID {
	store := &metapb.NzbStore{}
	pos := 0
	for _, n := range b.ReleaseFiles {
		fe := &metapb.NzbFileEntry{}
		for _, id := range b.Pool[pos : pos+n] {
			fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id})
		}
		pos += n
		store.Files = append(store.Files, fe)
	}
	rid, err := contentid.ReleaseID(store)
	require.NoError(e.t, err)
	return rid
}

// sealAny recomputes the identifiers of any scheme after a test changed a bundle.
func (e *env) sealAny(b *bundle.Bundle) {
	e.t.Helper()
	require.NoError(e.t, e.tryReseal(b))
}

func (e *env) tryReseal(b *bundle.Bundle) error {
	for i := range b.Files {
		b.Files[i].FileID = contentid.ID{byte(i + 1)}
	}
	b.ReleaseID = contentid.ID{9}
	loc, err := bundle.ToLocal(b, e.imp.cfg.Now())
	if err != nil {
		return err
	}
	ids := make([]contentid.ID, len(b.Files))
	for i := range b.Files {
		id, err := contentid.FileID(loc.Files[i].Meta)
		if err != nil {
			return err
		}
		b.Files[i].FileID = id
	}
	sort.Slice(b.Files, func(i, j int) bool { return string(b.Files[i].FileID[:]) < string(b.Files[j].FileID[:]) })
	for i := range b.Files {
		ids[i] = b.Files[i].FileID
	}
	if b.Scheme == bundle.SchemeFiles {
		b.ReleaseID = bundle.FilesReleaseID(ids)
	} else {
		b.ReleaseID = e.nzbReleaseID(b)
	}
	return bundle.Validate(b)
}

func (s builtSet) movie() *bundle.File {
	for i := range s.b.Files {
		if s.b.Files[i].Name == "movie.mkv" {
			return &s.b.Files[i]
		}
	}
	return nil
}

// fastPath makes the request carry the user's NZB the way the importer's fast
// path does: the segment list, encoded sizes and subject file names.
func (s builtSet) fastPath(req Request) Request {
	req.ExpectedNZB = s.ids
	req.ExpectedNZBNames = s.names
	for v, posted := range s.posted {
		var sizes []int64
		for i := range s.ids[v] {
			sizes = append(sizes, int64(min(s.spec.segSize, len(posted)-i*s.spec.segSize))+4)
		}
		req.ExpectedNZBSizes = append(req.ExpectedNZBSizes, sizes)
	}
	return req
}

func (e *env) stagedBytes(path string) []byte {
	e.t.Helper()
	meta, err := e.lib.ReadFileMetadata(path)
	require.NoError(e.t, err)
	st, err := e.imp.cfg.Stager.NewStaging()
	require.NoError(e.t, err)
	defer func() { _ = st.Close() }()
	require.NoError(e.t, st.WriteFileMetadata("read.bin", meta))
	f, err := st.Opener().OpenFile(context.Background(), "read.bin", os.O_RDONLY, 0)
	require.NoError(e.t, err)
	defer func() { _ = f.Close() }()
	out, err := io.ReadAll(f)
	require.NoError(e.t, err)
	return out
}

func evenCuts(total, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = total / n
	}
	out[n-1] += total % n
	return out
}

// (a) and (b): honest sets, every format and scheme, open and private networks.
func TestParts_HonestStoredSetsAreAcceptedAndReadBackIdentically(t *testing.T) {
	for _, kind := range []string{"rar5", "rar4"} {
		for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
			for _, volumes := range []int{3, 5} {
				for _, open := range []bool{false, true} {
					name := fmt.Sprintf("%s/%s/%d volumes/open=%v", kind, scheme, volumes, open)
					t.Run(name, func(t *testing.T) {
						e := newEnv(t)
						payload := mkvPlain(1500)
						s := e.mkStoredSet(setSpec{tag: "h", kind: kind, payload: payload, cuts: evenCuts(len(payload), volumes), scheme: scheme, extraVol: -1})
						req := e.req(s.b)
						req.Origin.Open = open
						if open {
							req.Origin.BundleID = strings.Repeat("e", 64)
						}
						res, err := e.imp.Accept(context.Background(), req)
						require.NoError(t, err, "%v", err)
						require.Len(t, res.Paths, 1)
						assert.Equal(t, payload, e.stagedBytes(res.Paths[0]), "the staged file reads the same bytes as the original")
						assert.Empty(t, e.rep.got)
						assert.Equal(t, volumes, res.Cost.PartsBound, "every part is bound to its archive header")
						assert.Zero(t, res.Cost.PartsUnbound)
					})
				}
			}
		}
	}
}

// (b) The importer's fast path: the user's NZB is present, so the Release ID, the
// declared sizes and the subject names of every entry are checked too.
func TestParts_FastPathChecksEntriesNamesAndSizes(t *testing.T) {
	for _, kind := range []string{"rar5", "rar4"} {
		t.Run(kind, func(t *testing.T) {
			e := newEnv(t)
			payload := mkvPlain(1500)
			s := e.mkStoredSet(setSpec{tag: "fp", kind: kind, payload: payload, cuts: evenCuts(len(payload), 4), scheme: bundle.SchemeNZB, extraVol: -1})
			res, err := e.imp.Accept(context.Background(), s.fastPath(e.openReq(s.b)))
			require.NoError(t, err, "%v", err)
			assert.Equal(t, payload, e.stagedBytes(res.Paths[0]))
		})
	}
	t.Run("an entry named differently by the user's NZB", func(t *testing.T) {
		e := newEnv(t)
		payload := mkvPlain(1500)
		s := e.mkStoredSet(setSpec{tag: "fn", kind: "rar5", payload: payload, cuts: evenCuts(len(payload), 4), scheme: bundle.SchemeNZB, extraVol: -1})
		req := s.fastPath(e.openReq(s.b))
		req.ExpectedNZBNames[2] = "something.else.rar"
		_, err := e.imp.Accept(context.Background(), req)
		require.Error(t, err)
		assert.Equal(t, ReasonYencMismatch, ReasonOf(err), "%v", err)
		assert.Empty(t, e.rep.got, "a disagreement with the local NZB is not the publisher's fault")
		e.assertNothingBehind()
	})
	t.Run("a segment list that is not the user's NZB", func(t *testing.T) {
		e := newEnv(t)
		payload := mkvPlain(1500)
		s := e.mkStoredSet(setSpec{tag: "fl", kind: "rar5", payload: payload, cuts: evenCuts(len(payload), 4), scheme: bundle.SchemeNZB, extraVol: -1})
		req := s.fastPath(e.openReq(s.b))
		req.ExpectedNZB = req.ExpectedNZB[:3]
		_, err := e.imp.Accept(context.Background(), req)
		assert.Equal(t, ReasonNZBMismatch, ReasonOf(err))
	})
}

// (d) A split 7z archive continues across volumes with no header of its own: the
// middle volumes are whole posted files, the first and last are slices whose
// header (at the end of the last volume) cannot be read, so the file is
// accepted on its media structure alone.
func TestParts_SplitSevenZipIsAcceptedOnItsStructure(t *testing.T) {
	for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
		t.Run(scheme.String(), func(t *testing.T) {
			e := newEnv(t)
			payload := mkvPlain(1500)
			total := len(archivetest.SevenZip(archivetest.SevenZipOpts{}, archivetest.File{Name: "movie.mkv", Data: payload}).Data)
			cuts := evenCuts(total, 4)
			s := e.mkStoredSet(setSpec{tag: "z", kind: "7z", payload: payload, cuts: cuts, scheme: scheme, extraVol: -1})
			res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.NoError(t, err, "%v", err)
			assert.Equal(t, payload, e.stagedBytes(res.Paths[0]))
			assert.Positive(t, res.Cost.PartsUnbound, "the 7z end header is not in the volume that holds the slice")
		})
	}
}

// (c) Hostile layouts over a real set.
func TestParts_HostileLayoutsAreRejectedWithTheRightReason(t *testing.T) {
	type hostile struct {
		kinds   []string
		volumes int
		mutate  func(t *testing.T, e *env, s *builtSet)
		reason  Reason
		report  bool
		// detail is what the rejection must say, so each case is caught by the
		// check it is about.
		detail string
	}
	setSeg := func(s *builtSet, part int, f func(first, last *bundle.Segment)) {
		m := s.movie()
		at := 0
		for k := 0; k < part; k++ {
			at += m.Parts[k]
		}
		f(&m.Segments[at], &m.Segments[at+m.Parts[part]-1])
		m.Size = 0
		for _, g := range m.Segments {
			m.Size += g.End - g.Start + 1
		}
		s.b.TotalSize = m.Size
	}
	swapParts := func(s *builtSet, a, b int) {
		m := s.movie()
		var chunks [][]bundle.Segment
		at := 0
		for _, n := range m.Parts {
			chunks = append(chunks, m.Segments[at:at+n])
			at += n
		}
		chunks[a], chunks[b] = chunks[b], chunks[a]
		m.Parts[a], m.Parts[b] = m.Parts[b], m.Parts[a]
		var flat []bundle.Segment
		for _, c := range chunks {
			flat = append(flat, c...)
		}
		m.Segments = flat
	}
	cases := map[string]hostile{
		"a part shifted 6 bytes into the header area": {[]string{"rar5", "rar4"}, 3, func(t *testing.T, e *env, s *builtSet) {
			setSeg(s, 1, func(first, last *bundle.Segment) { first.Start -= 6; last.End -= 6 })
		}, ReasonExtentMismatch, true, "part 2 of 3: "},
		"a part shifted 5 bytes into the data": {[]string{"rar5", "rar4"}, 3, func(t *testing.T, e *env, s *builtSet) {
			setSeg(s, 1, func(first, last *bundle.Segment) { first.Start += 5; last.End += 5 })
		}, ReasonExtentMismatch, true, "part 2 of 3: "},
		"a part one byte too long, the file size kept": {[]string{"rar5", "rar4"}, 3, func(t *testing.T, e *env, s *builtSet) {
			setSeg(s, 0, func(_, last *bundle.Segment) { last.End++ })
			setSeg(s, 2, func(_, last *bundle.Segment) { last.End-- })
		}, ReasonExtentMismatch, true, "part 1 of 3: the header says 300 bytes in this volume, the layout 301"},
		"a file size that is not the archive's": {[]string{"rar5", "rar4"}, 3, func(t *testing.T, e *env, s *builtSet) {
			setSeg(s, 2, func(_, last *bundle.Segment) { last.End-- })
		}, ReasonExtentMismatch, true, "the header says the file is 900 bytes"},
		"first and last part swapped": {[]string{"rar5", "rar4"}, 4, func(t *testing.T, e *env, s *builtSet) { swapParts(s, 0, 3) },
			ReasonExtentMismatch, true, "continuation flag"},
		"two middle parts swapped": {[]string{"rar5"}, 5, func(t *testing.T, e *env, s *builtSet) { swapParts(s, 1, 2) },
			ReasonExtentMismatch, true, "not in the order of the parts"},
	}
	for label, c := range cases {
		for _, kind := range c.kinds {
			for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
				t.Run(fmt.Sprintf("%s/%s/%s", label, kind, scheme), func(t *testing.T) {
					e := newEnv(t)
					payload := mkvPlain(300 * c.volumes)
					s := e.mkStoredSet(setSpec{tag: "x", kind: kind, payload: payload, cuts: evenCuts(len(payload), c.volumes), scheme: scheme, extraVol: -1})
					c.mutate(t, e, &s)
					require.NoError(t, e.tryReseal(s.b), "the mutated layout must still be structurally valid")
					_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
					require.Error(t, err)
					assert.Equal(t, c.reason, ReasonOf(err), "%v", err)
					assert.Contains(t, err.Error(), c.detail)
					if c.report {
						assert.Len(t, e.rep.got, 1, "the publisher is reported")
						assert.Len(t, e.rep.failedBundles, 1, "the Bundle ID goes on the negative cache")
					}
					e.assertNothingBehind()
				})
			}
		}
	}
}

// Two files stored back to back in one volume: the movie's last slice and the
// subtitle share the article where one ends and the next begins.
func TestParts_SecondFileInTheLastVolumeIsAcceptedToo(t *testing.T) {
	for _, kind := range []string{"rar5", "rar4"} {
		for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
			t.Run(kind+"/"+scheme.String(), func(t *testing.T) {
				e := newEnv(t)
				payload := mkvPlain(900)
				nfo := []byte(strings.Repeat("a release note\n", 20))
				s := e.mkStoredSet(setSpec{tag: "sf", kind: kind, payload: payload, cuts: evenCuts(len(payload), 3), scheme: scheme, extra: nfo, extraVol: 2})
				res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
				if scheme == bundle.SchemeFiles {
					// Without the NZB the subtitle's part begins at an article the
					// bundle does not reference (deep in the volume): it cannot be
					// header-bound, so the receiver declines, and not as a fault of
					// the publisher.
					require.Error(t, err)
					assert.Equal(t, ReasonExtentUnverifiable, ReasonOf(err), "%v", err)
					assert.Empty(t, e.rep.got)
					return
				}
				require.NoError(t, err, "%v", err)
				assert.Len(t, res.Paths, 2)
				for _, p := range res.Paths {
					if strings.HasSuffix(p, "movie.mkv") {
						assert.Equal(t, payload, e.stagedBytes(p))
					}
				}
			})
		}
	}
}

func (s builtSet) dropExtra() {
	var keep []bundle.File
	for _, f := range s.b.Files {
		if f.Name == "movie.mkv" {
			keep = append(keep, f)
		}
	}
	s.b.Files = keep
	s.b.TotalSize = keep[0].Size
}

// A part may not point at another stored file of the same volume: the entry
// that starts where the part starts must be the file's own continuation.
func TestParts_PartPointingAtAnotherStoredFileIsRejected(t *testing.T) {
	for _, kind := range []string{"rar5", "rar4"} {
		for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
			t.Run(kind+"/"+scheme.String(), func(t *testing.T) {
				e := newEnv(t)
				payload := mkvPlain(900)
				nfo := []byte(strings.Repeat("N", 300)) // as long as a part of the movie
				s := e.mkStoredSet(setSpec{tag: "ot", kind: kind, payload: payload, cuts: evenCuts(len(payload), 3), scheme: scheme, extra: nfo, extraVol: 1})
				s.dropExtra()
				m := s.movie()
				// part 1 (volume 1) now reads extra.nfo's data area instead of the movie's slice
				vol := s.posted[1]
				var segs []bundle.Segment
				for _, sl := range volSlices(vol, s.spec.segSize, s.extraLo, s.extraHi) {
					segs = append(segs, bundle.Segment{Ref: poolPos(s.b, s.ids[1][sl.art]), Size: int64(sl.size), Start: int64(sl.start), End: int64(sl.end)})
				}
				at := m.Parts[0]
				m.Segments = append(append(append([]bundle.Segment{}, m.Segments[:at]...), segs...), m.Segments[at+m.Parts[1]:]...)
				m.Parts[1] = len(segs)
				if scheme == bundle.SchemeFiles {
					require.NoError(t, dropUnusedPool(s.b))
				}
				require.NoError(t, e.tryReseal(s.b))
				res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
				if scheme == bundle.SchemeFiles {
					// Without the NZB list the articles before the part are unknown, so a
					// slice that does not begin at its posted file's first article
					// cannot be bound to a header: it is refused (not attributable).
					require.Error(t, err, "%v", res)
					assert.Equal(t, ReasonExtentUnverifiable, ReasonOf(err), "%v", err)
					assert.Empty(t, e.rep.got)
					return
				}
				require.Error(t, err)
				assert.Equal(t, ReasonExtentMismatch, ReasonOf(err), "%v", err)
				assert.Contains(t, err.Error(), "part 2 of 3")
				assert.Len(t, e.rep.got, 1)
				e.assertNothingBehind()
			})
		}
	}
}

// A part that claims to be a whole posted file (full ranges) of media data may
// not be an archive volume, and a files-scheme part with full ranges must be a
// whole posted file.
func TestParts_PartDeclaredWholeThatIsNotIsRejected(t *testing.T) {
	fullPart := func(s *builtSet, part int) {
		m := s.movie()
		at := 0
		for k := 0; k < part; k++ {
			at += m.Parts[k]
		}
		for k := at; k < at+m.Parts[part]; k++ {
			m.Segments[k].Start, m.Segments[k].End = 0, m.Segments[k].Size-1
		}
		m.Size = bundle.Usable(m.Segments)
		s.b.TotalSize = m.Size
	}
	for _, kind := range []string{"rar5", "rar4"} {
		for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
			t.Run("a whole archive volume/"+kind+"/"+scheme.String(), func(t *testing.T) {
				e := newEnv(t)
				payload := mkvPlain(900)
				s := e.mkStoredSet(setSpec{tag: "wh", kind: kind, payload: payload, cuts: evenCuts(len(payload), 3), scheme: scheme, extraVol: -1})
				fullPart(&s, 0)
				require.NoError(t, e.tryReseal(s.b))
				_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
				require.Error(t, err)
				assert.Equal(t, ReasonExtentMismatch, ReasonOf(err), "%v", err)
				assert.Contains(t, err.Error(), "begins like a")
				e.assertNothingBehind()
			})
		}
		t.Run("full ranges taken from the middle of a posted file/"+kind, func(t *testing.T) {
			e := newEnv(t)
			payload := mkvPlain(900)
			s := e.mkStoredSet(setSpec{tag: "mid", kind: kind, payload: payload, cuts: evenCuts(len(payload), 3), scheme: bundle.SchemeFiles, extraVol: -1})
			m := s.movie()
			// part 1 keeps only its interior articles, full: not a whole posted file
			at := m.Parts[0]
			n := m.Parts[1]
			inner := append([]bundle.Segment{}, m.Segments[at+1:at+n-1]...)
			for k := range inner {
				inner[k].Start, inner[k].End = 0, inner[k].Size-1
			}
			m.Segments = append(append(append([]bundle.Segment{}, m.Segments[:at]...), inner...), m.Segments[at+n:]...)
			m.Parts[1] = len(inner)
			m.Size = bundle.Usable(m.Segments)
			s.b.TotalSize = m.Size
			// the dropped articles are no longer referenced: the files scheme needs a pool of used ones
			require.NoError(t, dropUnusedPool(s.b))
			require.NoError(t, e.tryReseal(s.b))
			_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.Error(t, err)
			assert.Equal(t, ReasonYencMismatch, ReasonOf(err), "%v", err)
			assert.Len(t, e.rep.got, 1, "attributable")
		})
	}
}

func dropUnusedPool(b *bundle.Bundle) error {
	used := map[int64]bool{}
	for _, f := range b.Files {
		for _, s := range f.Segments {
			used[s.Ref] = true
		}
	}
	remap := map[int64]int64{}
	var pool []string
	for i, id := range b.Pool {
		if used[int64(i)] {
			remap[int64(i)] = int64(len(pool))
			pool = append(pool, id)
		}
	}
	b.Pool = pool
	for i := range b.Files {
		for k := range b.Files[i].Segments {
			b.Files[i].Segments[k].Ref = remap[b.Files[i].Segments[k].Ref]
		}
	}
	return nil
}

// Layouts the bundle's own rules refuse never reach the network checks.
func TestParts_StructurallyInvalidLayoutsAreRefusedBeforeAnyRead(t *testing.T) {
	t.Run("an interior partial range", func(t *testing.T) {
		e := newEnv(t)
		payload := mkvPlain(900)
		s := e.mkStoredSet(setSpec{tag: "ip", kind: "rar5", payload: payload, cuts: evenCuts(len(payload), 3), scheme: bundle.SchemeNZB, extraVol: -1})
		m := s.movie()
		m.Segments[1].Start = 3
		_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
		assert.Equal(t, ReasonInvalid, ReasonOf(err), "%v", err)
		assert.ErrorIs(t, bundle.Validate(s.b), bundle.ErrPartialRange)
	})
	t.Run("two files claiming overlapping ranges of one volume", func(t *testing.T) {
		e := newEnv(t)
		payload := mkvPlain(900)
		nfo := []byte(strings.Repeat("N", 300))
		s := e.mkStoredSet(setSpec{tag: "ov", kind: "rar5", payload: payload, cuts: evenCuts(len(payload), 3), scheme: bundle.SchemeNZB, extra: nfo, extraVol: 2})
		var nfoFile, movie *bundle.File
		for i := range s.b.Files {
			if s.b.Files[i].Name == "extra.nfo" {
				nfoFile = &s.b.Files[i]
			} else {
				movie = &s.b.Files[i]
			}
		}
		// the subtitle claims the movie's last bytes of that volume
		last := movie.Segments[len(movie.Segments)-1]
		nfoFile.Segments = []bundle.Segment{{Ref: last.Ref, Size: last.Size, Start: last.End - 9, End: last.End}}
		nfoFile.Size, nfoFile.Parts = 10, []int{1}
		s.b.TotalSize = movie.Size + 10
		_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
		assert.Equal(t, ReasonInvalid, ReasonOf(err), "%v", err)
		assert.ErrorIs(t, bundle.Validate(s.b), bundle.ErrCoverage)
	})
}

// The P2 shape: 100 volumes, each contributing a 10-byte slice from the middle of
// its article. No stored file's header binds such slices, so it is not accepted
// as a multi-part layout either.
func TestParts_P2ShapeIsRejectedAsAMultiPartLayout(t *testing.T) {
	for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
		t.Run(scheme.String(), func(t *testing.T) {
			e := newEnv(t)
			payload := mkvPlain(100 * 300)
			s := e.mkStoredSet(setSpec{tag: "p2", kind: "rar5", payload: payload, cuts: evenCuts(len(payload), 100), segSize: 1024, scheme: scheme, extraVol: -1})
			m := s.movie()
			require.Len(t, m.Parts, 100)
			for k := range m.Segments {
				m.Segments[k].Start = int64(s.lo[k] + 100)
				m.Segments[k].End = m.Segments[k].Start + 9
			}
			m.Size = 1000
			s.b.TotalSize = 1000
			require.NoError(t, e.tryReseal(s.b))
			_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.Error(t, err)
			assert.Equal(t, ReasonExtentMismatch, ReasonOf(err), "%v", err)
			e.assertNothingBehind()
		})
	}
}

// What an honest stored set costs to verify.
func TestParts_VerificationCostByVolumeCount(t *testing.T) {
	for _, n := range []int{50, 200} {
		t.Run(fmt.Sprintf("%d volumes", n), func(t *testing.T) {
			e := newEnv(t)
			payload := mkvPlain(n * 200)
			s := e.mkStoredSet(setSpec{tag: "c", kind: "rar5", payload: payload, cuts: evenCuts(len(payload), n), segSize: 1024, scheme: bundle.SchemeNZB, extraVol: -1})
			res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.NoError(t, err, "%v", err)
			t.Logf("%d volumes (one article each): %d articles read, %d header checks (%d bound)", n, res.Cost.Articles, n, res.Cost.PartsBound)
			assert.Equal(t, n, res.Cost.PartsBound, "every part is header-checked")
			// one article per part (it is first and last at once): the header walk
			// reads the bytes the yEnc sample already fetched
			assert.Equal(t, n, res.Cost.Articles)
		})
	}
}

// The forced full check samples headers of the stored (flat) metadata, which
// no longer says where the parts are; the volume boundaries are recovered from
// the ranges, so an honest multi-volume file does not fail it.
func TestVerifyPeerHeaders_MultiVolumeFileIsNotAFalseAlarm(t *testing.T) {
	for _, kind := range []string{"rar5", "rar4"} {
		t.Run(kind, func(t *testing.T) {
			e := newEnv(t)
			payload := mkvPlain(1500)
			s := e.mkStoredSet(setSpec{tag: "vh", kind: kind, payload: payload, cuts: evenCuts(len(payload), 4), scheme: bundle.SchemeNZB, extraVol: -1})
			res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.NoError(t, err)
			require.NoError(t, e.imp.VerifyPeerHeaders(context.Background(), res.Paths[0]))
		})
	}
}

func TestVerifyPeerHeaders_MultiVolumeLieInAnInteriorArticleIsFound(t *testing.T) {
	e := newEnv(t)
	payload := mkvPlain(1500)
	s := e.mkStoredSet(setSpec{tag: "vl", kind: "rar5", payload: payload, cuts: evenCuts(len(payload), 4), scheme: bundle.SchemeNZB, extraVol: -1})
	res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
	require.NoError(t, err)
	id := s.ids[2][2]
	beh := e.fp.BehaviorFor(id)
	beh.YEnc.PartBegin += 5
	e.fp.SetBehavior(id, beh)
	var lm *LayoutMismatchError
	require.ErrorAs(t, e.imp.VerifyPeerHeaders(context.Background(), res.Paths[0]), &lm)
}

// Volumes written by the real archiver (the importer's fixtures): the header
// checks bind every slice, and the staged file reads the original bytes.
func TestParts_RealArchiverVolumesAreBoundAndReadBack(t *testing.T) {
	root := filepath.Join("..", "..", "importer", "testdata")
	cases := []struct{ dir, glob string }{{"rar_multi", "archive.part*.rar"}, {"rar_widthmismatch", "archive.part*.rar"}}
	for _, tc := range cases {
		t.Run(tc.dir, func(t *testing.T) {
			matches, err := filepath.Glob(filepath.Join(root, tc.dir, tc.glob))
			require.NoError(t, err)
			require.NotEmpty(t, matches)
			// natural order: part02 before part010
			sort.Slice(matches, func(a, b int) bool {
				if len(matches[a]) != len(matches[b]) {
					return len(matches[a]) < len(matches[b])
				}
				return matches[a] < matches[b]
			})
			payload, err := os.ReadFile(filepath.Join(root, "payload_a.bin"))
			require.NoError(t, err)

			e := newEnv(t)
			const segSize = 8192
			var nzb [][]string
			var segs []*metapb.SegmentData
			var names []string
			for v, m := range matches {
				data, err := os.ReadFile(m)
				require.NoError(t, err)
				names = append(names, filepath.Base(m))
				vol, err := archivehdr.Scan(bytes.NewReader(data), int64(len(data)), int64(len(data)), archivehdr.Limits{MaxBlocks: 64, MaxReads: 400})
				require.NoError(t, err)
				assert.EqualValues(t, v, vol.VolumeNumber, "the real archiver's volume numbers run in volume order")
				var blk *archivehdr.Block
				for k := range vol.Blocks {
					if vol.Blocks[k].File {
						blk = &vol.Blocks[k]
					}
				}
				require.NotNil(t, blk)
				var ids []string
				for i := 0; i*segSize < len(data); i++ {
					from, end := i*segSize, min((i+1)*segSize, len(data))
					id := fmt.Sprintf("real-%s-v%d-%03d@test", tc.dir, v, i)
					beh := fakepoolBehavior(data[from:end], from, len(data))
					beh.YEnc.FileName = filepath.Base(m)
					e.fp.SetBehavior(id, beh)
					ids = append(ids, id)
				}
				nzb = append(nzb, ids)
				for _, sl := range volSlices(data, segSize, int(blk.DataOffset), int(blk.DataOffset+blk.DataSize)) {
					segs = append(segs, &metapb.SegmentData{Id: ids[sl.art], SegmentSize: int64(sl.size), StartOffset: int64(sl.start), EndOffset: int64(sl.end)})
				}
			}
			in := bundle.ReleaseInput{Scheme: bundle.SchemeNZB, Title: "Real Archive", PostedAt: time.Unix(1_700_000_000, 0), NZBFiles: nzb,
				Files: []bundle.FileInput{{Name: "payload_a.bin", Meta: &metapb.FileMetadata{FileSize: int64(len(payload)), SegmentData: segs, Status: metapb.FileStatus_FILE_STATUS_HEALTHY}}}}
			bd, err := bundle.FromLocal(in)
			require.NoError(t, err)
			require.Len(t, bd.Files[0].Parts, len(matches))

			stage, closeStage := e.imp.stager()
			defer closeStage()
			rep, _, _, err := e.imp.verifyYenc(context.Background(), bd, names, stage)
			require.NoError(t, err)
			assert.Equal(t, len(matches), rep.partsBound, "every volume's slice is bound to its own file header")
			assert.Zero(t, rep.partsUnbound)

			loc, err := bundle.ToLocal(bd, time.Unix(1_700_000_100, 0))
			require.NoError(t, err)
			st, _, err := stage()
			require.NoError(t, err)
			require.NoError(t, st.WriteFileMetadata("read.bin", loc.Files[0].Meta))
			f, err := st.Opener().OpenFile(context.Background(), "read.bin", os.O_RDONLY, 0)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()
			got, err := io.ReadAll(f)
			require.NoError(t, err)
			assert.Equal(t, payload, got)
		})
	}
}
