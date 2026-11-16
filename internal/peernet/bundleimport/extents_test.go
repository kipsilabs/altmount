package bundleimport

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/archivehdr/archivetest"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

// volSpec is one outer volume of a nested test bundle: the bytes of the archive
// (served plain, or AES-CBC encrypted under key; the bundle carries bundleKey).
type volSpec struct {
	archive   []byte
	segSize   int
	key       []byte
	bundleKey []byte
	// wrap serves the volume as a stored file inside a wrapping RAR 5 archive
	// (the shape of an inner volume inside an outer archive): the table then
	// starts inside the first article. wrapIV overrides the IV the wrapper's
	// header carries; wrapCompressed marks the wrapper's entry compressed.
	wrap           bool
	wrapIV         []byte
	wrapCompressed bool
}

// padTo16 zero-pads an archive to a whole number of AES blocks (what a
// decrypted outer volume looks like).
func padTo16(b []byte) []byte {
	if n := len(b) % 16; n != 0 {
		b = append(append([]byte{}, b...), make([]byte, 16-n)...)
	}
	return b
}

// mkNested serves the volumes' articles and returns a valid nzb-scheme bundle
// with one nested file: name, size, read through the volumes by extents.
func (e *env) mkNested(tag, name string, size int64, vols []volSpec, extents []bundle.Extent) *bundle.Bundle {
	e.t.Helper()
	b := &bundle.Bundle{Scheme: bundle.SchemeNZB, Title: "Test Release " + tag, PostedUnix: 1_700_000_000}
	var outers []bundle.OuterSource
	for vi, v := range vols {
		payload := v.archive
		var key, iv []byte
		var volSize int64
		if v.key != nil {
			payload = cbc(e.t, v.key, padTo16(v.archive))
			key, iv, volSize = v.key, testIV, int64(len(padTo16(v.archive)))
			if v.bundleKey != nil {
				key = v.bundleKey
			}
		}
		posted, dataStart, dataEnd := payload, 0, len(payload)
		if v.wrap {
			f := archivetest.File{Name: "inner.part01.rar", Data: payload, Compressed: v.wrapCompressed}
			if key != nil {
				f.IV = testIV
				if v.wrapIV != nil {
					f.IV = v.wrapIV
				}
			}
			w := archivetest.RAR5(f)
			posted, dataStart = w.Data, int(w.DataOffsets[0])
			dataEnd = dataStart + len(payload)
		}
		var segs []bundle.Segment
		count := 0
		for i := 0; i*v.segSize < len(posted); i++ {
			from, end := i*v.segSize, min((i+1)*v.segSize, len(posted))
			id := fmt.Sprintf("%s-v%d-%03d@test", tag, vi, i)
			e.fp.SetBehavior(id, fakepoolBehavior(posted[from:end], from, len(posted)))
			count++
			b.Pool = append(b.Pool, id)
			if end <= dataStart || from >= dataEnd {
				continue
			}
			segs = append(segs, bundle.Segment{Ref: int64(len(b.Pool) - 1), Size: int64(end - from), Start: int64(max(dataStart-from, 0)), End: int64(min(dataEnd, end) - from - 1)})
		}
		b.ReleaseFiles = append(b.ReleaseFiles, count)
		outers = append(outers, bundle.OuterSource{Segments: segs, AESKey: key, AESIV: iv, VolumeSize: volSize})
	}
	b.Files = []bundle.File{{Name: name, Size: size, Outer: outers, Extents: extents}}
	b.TotalSize = size
	e.seal(b)
	return b
}

func (e *env) seal(b *bundle.Bundle) {
	e.t.Helper()
	for i := range b.Files {
		b.Files[i].FileID = contentid.ID{byte(i + 1)}
	}
	b.ReleaseID = contentid.ID{9}
	loc, err := bundle.ToLocal(b, e.imp.cfg.Now())
	require.NoError(e.t, err)
	for i := range b.Files {
		id, err := contentid.FileID(loc.Files[i].Meta)
		require.NoError(e.t, err)
		b.Files[i].FileID = id
	}
	sort.Slice(b.Files, func(i, j int) bool { return string(b.Files[i].FileID[:]) < string(b.Files[j].FileID[:]) })
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
	b.ReleaseID = rid
	require.NoError(e.t, bundle.Validate(b))
}

func ext1(vol int, off, length int64) bundle.Extent {
	return bundle.Extent{Outer: vol, InnerOffset: off, InnerLength: length}
}

func (e *env) openReq(b *bundle.Bundle) Request {
	req := e.req(b)
	req.Origin.Open = true
	req.Origin.BundleID = strings.Repeat("e", 64)
	return req
}

// archiveBuilders lists the stored archive formats the importer produces.
var archiveBuilders = map[string]func(files ...archivetest.File) archivetest.Built{
	"rar5": archivetest.RAR5,
	"rar4": archivetest.RAR4,
}

// A nested bundle from the open network is accepted when the extent matches the
// archive's own file header and the media passes.
func TestExtents_OpenNetworkAcceptsNestedAndKeyedLayouts(t *testing.T) {
	mkv := mkvPlain(512)
	for name, build := range archiveBuilders {
		for _, keyed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/keyed=%v", name, keyed), func(t *testing.T) {
				e := newEnv(t)
				a := build(archivetest.File{Name: "movie.mkv", Data: mkv})
				v := volSpec{archive: a.Data, segSize: 128}
				if keyed {
					v.key = testKey
				}
				b := e.mkNested("n", "movie.mkv", int64(len(mkv)), []volSpec{v}, []bundle.Extent{ext1(0, a.DataOffsets[0], int64(len(mkv)))})
				_, err := e.imp.Accept(context.Background(), e.openReq(b))
				require.NoError(t, err, "%v", err)
			})
		}
	}
}

func TestExtents_MultiVolumeFileIsBoundPartByPart(t *testing.T) {
	mkv := mkvPlain(600)
	for name, build := range archiveBuilders {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			parts := [][]byte{mkv[:200], mkv[200:400], mkv[400:]}
			var vols []volSpec
			var extents []bundle.Extent
			for k, p := range parts {
				a := build(archivetest.File{Name: "movie.mkv", Data: p, SplitBefore: k > 0, SplitAfter: k < 2, UnpackedSize: 600})
				vols = append(vols, volSpec{archive: a.Data, segSize: 128})
				extents = append(extents, ext1(k, a.DataOffsets[0], int64(len(p))))
			}
			b := e.mkNested("mv", "movie.mkv", 600, vols, extents)
			_, err := e.imp.Accept(context.Background(), e.openReq(b))
			require.NoError(t, err, "%v", err)
		})
	}
}

// Every way an extent can disagree with the archive's header is attributable to
// the publisher: rejected, reported, and the layout goes on the negative cache.
func TestExtents_MismatchIsRejectedAndReported(t *testing.T) {
	mkv := mkvPlain(512)
	type spec struct {
		file   archivetest.File
		extent func(off, n int64) bundle.Extent
	}
	cases := map[string]spec{
		"offset shifted into the data": {archivetest.File{Name: "movie.mkv", Data: mkv}, func(off, n int64) bundle.Extent { return ext1(0, off+8, n-8) }},
		"offset before the data":       {archivetest.File{Name: "movie.mkv", Data: mkv}, func(off, n int64) bundle.Extent { return ext1(0, off-3, n) }},
		"entry is compressed":          {archivetest.File{Name: "movie.mkv", Data: mkv, Compressed: true}, func(off, n int64) bundle.Extent { return ext1(0, off, n) }},
		"entry is a service block":     {archivetest.File{Name: "movie.mkv", Data: mkv, Service: true}, func(off, n int64) bundle.Extent { return ext1(0, off, n) }},
		"header says another size":     {archivetest.File{Name: "movie.mkv", Data: mkv, UnpackedSize: 9999}, func(off, n int64) bundle.Extent { return ext1(0, off, n) }},
		"header says it continues":     {archivetest.File{Name: "movie.mkv", Data: mkv, SplitAfter: true}, func(off, n int64) bundle.Extent { return ext1(0, off, n) }},
	}
	for name, build := range archiveBuilders {
		for label, c := range cases {
			t.Run(name+"/"+label, func(t *testing.T) {
				e := newEnv(t)
				a := build(c.file)
				ex := c.extent(a.DataOffsets[0], int64(len(mkv)))
				size := ex.InnerLength
				b := e.mkNested("mm", "movie.mkv", size, []volSpec{{archive: a.Data, segSize: 128}}, []bundle.Extent{ex})
				_, err := e.imp.Accept(context.Background(), e.openReq(b))
				require.Error(t, err)
				assert.Equal(t, ReasonExtentMismatch, ReasonOf(err), "%v", err)
				assert.Len(t, e.rep.got, 1, "the publisher is reported")
				assert.Empty(t, e.rep.failedBundles, "one contradiction is not enough to blacklist the Bundle ID")
				e.assertNothingBehind()
			})
		}
	}
}

// Files whose extents cannot be bound (no archive header can be read) are
// accepted only on a real media structure; a generic container is refused.
func TestExtents_UnverifiableNeedsAStructuralCheck(t *testing.T) {
	mkv := mkvPlain(512)
	disc := append(append(make([]byte, 40), mkv...), make([]byte, 20)...) // an unknown wrapper: no signature
	enc7 := archivetest.SevenZip(archivetest.SevenZipOpts{EncodedHeader: true}, archivetest.File{Name: "movie.mkv", Data: mkv})
	wrappers := map[string]struct {
		vol []byte
		off int64
	}{
		"unknown wrapper":     {disc, 40},
		"encoded 7z header":   {enc7.Data, enc7.DataOffsets[0]},
		"encrypted RAR heads": {encryptedRarHeaders(mkv), 33},
	}
	for label, w := range wrappers {
		t.Run(label+"/matroska is accepted", func(t *testing.T) {
			e := newEnv(t)
			b := e.mkNested("u", "movie.mkv", int64(len(mkv)), []volSpec{{archive: w.vol, segSize: 128}}, []bundle.Extent{ext1(0, w.off, int64(len(mkv)))})
			_, err := e.imp.Accept(context.Background(), e.openReq(b))
			require.NoError(t, err, "%v", err)
		})
		t.Run(label+"/generic container is refused", func(t *testing.T) {
			e := newEnv(t)
			avi := aviPlain(512)
			vol := append(append(make([]byte, int(w.off)), avi...), make([]byte, 20)...)
			b := e.mkNested("ug", "movie.avi", int64(len(avi)), []volSpec{{archive: vol, segSize: 128}}, []bundle.Extent{ext1(0, w.off, int64(len(avi)))})
			_, err := e.imp.Accept(context.Background(), e.openReq(b))
			require.Error(t, err)
			assert.Equal(t, ReasonExtentUnverifiable, ReasonOf(err), "%v", err)
			assert.Empty(t, e.rep.got, "an unverifiable layout is not a proven lie")
			e.assertNothingBehind()
		})
	}
}

// aviPlain is a generic container: a RIFF/AVI head the signature check accepts
// and nothing the structure check can read further.
func aviPlain(size int) []byte {
	out := junkPlain(size)
	copy(out, []byte("RIFF\x00\x00\x00\x00AVI LIST"))
	return out
}

// encryptedRarHeaders is a RAR 5 volume whose first block is the archive
// encryption header, followed by the media bytes.
func encryptedRarHeaders(mkv []byte) []byte {
	out := []byte{'R', 'a', 'r', '!', 0x1A, 0x07, 0x01, 0x00, 0, 0, 0, 0, 20, 4, 0, 0, 15}
	out = append(out, make([]byte, 16)...)
	return append(out, mkv...)
}

// A wrong key garbles the volume: no header can be read, and the media check
// through the layout rejects it.
func TestExtents_WrongKeyIsRejectedByTheMediaCheck(t *testing.T) {
	e := newEnv(t)
	mkv := mkvPlain(512)
	a := archivetest.RAR5(archivetest.File{Name: "movie.mkv", Data: mkv})
	wrong := []byte("ffffffffffffffffffffffffffffffff")
	b := e.mkNested("wk", "movie.mkv", int64(len(mkv)), []volSpec{{archive: a.Data, segSize: 128, key: testKey, bundleKey: wrong}},
		[]bundle.Extent{ext1(0, a.DataOffsets[0], int64(len(mkv)))})
	_, err := e.imp.Accept(context.Background(), e.openReq(b))
	require.Error(t, err)
	assert.Contains(t, []Reason{ReasonExtentMismatch, ReasonMediaInvalid, ReasonExtentUnverifiable, ReasonStructureInvalid}, ReasonOf(err), "%v", err)
	e.assertNothingBehind()
}

// A missing article of the checked volume is not about the archive.
func TestExtents_MissingArticleIsNotAMismatch(t *testing.T) {
	e := newEnv(t)
	mkv := mkvPlain(512)
	a := archivetest.RAR5(archivetest.File{Name: "movie.mkv", Data: mkv})
	b := e.mkNested("ma", "movie.mkv", int64(len(mkv)), []volSpec{{archive: a.Data, segSize: 128}}, []bundle.Extent{ext1(0, a.DataOffsets[0], int64(len(mkv)))})
	e.fp.SetBehavior(b.Pool[0], fakepoolMissing())
	_, err := e.imp.Accept(context.Background(), e.openReq(b))
	require.Error(t, err)
	assert.NotEqual(t, ReasonExtentMismatch, ReasonOf(err), "%v", err)
	assert.Empty(t, e.rep.got)
}

func TestPickExtents_CapsAndAlwaysKeepsTheEndsOfTheMainFile(t *testing.T) {
	mk := func(n int) *bundle.Bundle {
		ex := make([]bundle.Extent, n)
		return &bundle.Bundle{Files: []bundle.File{{Name: "side.nfo", Size: 10, Extents: ex[:5]}, {Name: "movie.mkv", Size: 1 << 30, Extents: ex}}}
	}
	b := mk(10)
	assert.Len(t, pickExtents(b), 15, "every extent is checked up to the cap")
	b = mk(300)
	seen := map[extentRef]int{}
	for range 40 {
		picks := pickExtents(b)
		require.Len(t, picks, maxExtentChecks)
		have := map[extentRef]bool{}
		for _, p := range picks {
			assert.False(t, have[p], "no extent twice")
			have[p] = true
			seen[p]++
		}
		assert.True(t, have[extentRef{1, 0}] && have[extentRef{1, 299}], "first and last extent of the main media file")
	}
	assert.Greater(t, len(seen), 150, "the rest is chosen at random")
}

func TestChargedReader_BoundsTheArticlesOfOneVolumeAndChargesTheBudget(t *testing.T) {
	o := &bundle.OuterSource{}
	for i := 0; i < 100; i++ {
		o.Segments = append(o.Segments, bundle.Segment{Ref: int64(i), Size: 1000, Start: 0, End: 999})
	}
	data := make([]byte, 100_000)
	bud := newCostBudget(1 << 30)
	cr := newChargedReader(bytesReaderAt(data), o, bud)
	buf := make([]byte, 10)
	for k := 0; k < maxVolumeArticles; k++ {
		_, err := cr.ReadAt(buf, int64(k*1000))
		require.NoError(t, err)
		_, err = cr.ReadAt(buf, int64(k*1000+500)) // the same article again is free
		require.NoError(t, err)
	}
	assert.EqualValues(t, maxVolumeArticles*1000, bud.used, "the declared sizes are reserved")
	_, err := cr.ReadAt(buf, 90_000)
	require.ErrorIs(t, err, errCost, "one volume may not walk more than %d articles", maxVolumeArticles)

	small := newCostBudget(1500)
	cr = newChargedReader(bytesReaderAt(data), o, small)
	_, err = cr.ReadAt(buf, 0)
	require.NoError(t, err)
	_, err = cr.ReadAt(buf, 5000)
	require.ErrorIs(t, err, errCost, "the shared budget stops the read")
}

func fakepoolBehavior(payload []byte, begin, total int) fakepool.SegmentBehavior {
	return fakepool.SegmentBehavior{Bytes: payload, YEnc: nntppool.YEncMeta{PartBegin: int64(begin), PartSize: int64(len(payload)), FileSize: int64(total)}}
}

func fakepoolMissing() fakepool.SegmentBehavior {
	return fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound}
}

type bytesReaderAt []byte

func (b bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, fmt.Errorf("eof")
	}
	return copy(p, b[off:]), nil
}

// The archive that wraps an outer volume is bound too: the header ending at the
// table's first byte must be a stored entry of the table's size whose IV (RAR 5)
// is the layout's.
func TestExtents_WrapperHeaderBindsTheOuterVolume(t *testing.T) {
	mkv := mkvPlain(512)
	inner := archivetest.RAR5(archivetest.File{Name: "movie.mkv", Data: mkv})
	extents := []bundle.Extent{ext1(0, inner.DataOffsets[0], int64(len(mkv)))}
	for _, keyed := range []bool{false, true} {
		t.Run(fmt.Sprintf("honest keyed=%v", keyed), func(t *testing.T) {
			e := newEnv(t)
			v := volSpec{archive: inner.Data, segSize: 256, wrap: true}
			if keyed {
				v.key = testKey
			}
			b := e.mkNested("w", "movie.mkv", int64(len(mkv)), []volSpec{v}, extents)
			require.Greater(t, b.Files[0].Outer[0].Segments[0].Start, int64(0), "the table starts inside its first article")
			_, err := e.imp.Accept(context.Background(), e.openReq(b))
			require.NoError(t, err, "%v", err)
		})
	}
	bad := map[string]volSpec{
		"header IV is not the layout's": {archive: inner.Data, segSize: 256, wrap: true, key: testKey, wrapIV: []byte("0000000000000000")},
		"wrapping entry is compressed":  {archive: inner.Data, segSize: 256, wrap: true, wrapCompressed: true},
	}
	for label, v := range bad {
		t.Run(label, func(t *testing.T) {
			e := newEnv(t)
			b := e.mkNested("wb", "movie.mkv", int64(len(mkv)), []volSpec{v}, extents)
			_, err := e.imp.Accept(context.Background(), e.openReq(b))
			require.Error(t, err)
			assert.Equal(t, ReasonExtentMismatch, ReasonOf(err), "%v", err)
			assert.Len(t, e.rep.got, 1)
			e.assertNothingBehind()
		})
	}
}

// The cost of the archive header checks for nested releases with many volumes
// (one extent per volume), measured as articles fetched from the pool and the
// declared bytes charged at 768,000 bytes per article. Logged for the
// documentation; the bounds are asserted.
func TestExtents_CostForManyVolumes(t *testing.T) {
	const part = 96
	for _, n := range []int{50, 200} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d volumes wrapped=%v", n, wrapped), func(t *testing.T) {
				e := newEnv(t)
				var vols []volSpec
				var extents []bundle.Extent
				for k := 0; k < n; k++ {
					a := archivetest.RAR5(archivetest.File{Name: "movie.mkv", Data: junkPlain(part), SplitBefore: k > 0, SplitAfter: k < n-1, UnpackedSize: int64(part * n)})
					vols = append(vols, volSpec{archive: a.Data, segSize: 4096, key: testKey, wrap: wrapped})
					extents = append(extents, ext1(k, a.DataOffsets[0], part))
				}
				b := e.mkNested(fmt.Sprintf("c%d", n), "movie.mkv", int64(part*n), vols, extents)
				e.fp.ResetCounters()
				stage, closeStage := e.imp.stager()
				defer closeStage()
				st, _, err := stage()
				require.NoError(t, err)
				picks := pickExtents(b)
				bud := newCostBudget(1 << 40)
				rep, reason, _, err := e.imp.verifyExtents(context.Background(), b, st, picks, bud)
				require.NoError(t, err, "%s", reason)
				calls := e.fp.TotalCalls()
				perArticle := int64(768_000)
				t.Logf("%d volumes, wrapped=%v: %d extents checked, %d bound, %d articles fetched, %d article charges (%.0f MB at 768 KB)",
					n, wrapped, rep.checked, rep.bound, calls, bud.articles(), float64(int64(bud.articles())*perArticle)/1e6)
				assert.Equal(t, min(n, maxExtentChecks), rep.checked)
				assert.Equal(t, rep.checked, rep.bound)
				assert.LessOrEqual(t, calls, int64(2*maxExtentChecks), "at most two articles per checked volume")
			})
		}
	}
}

// The meter shows what was really read: the declared charges of a staged read
// are a reservation, settled to the staging area's own download count (a read
// served from the accept's cache downloaded nothing).
func TestChargedReader_SettlesTheReservationToTheBytesReallyDownloaded(t *testing.T) {
	o := &bundle.OuterSource{}
	for i := 0; i < 10; i++ {
		o.Segments = append(o.Segments, bundle.Segment{Ref: int64(i), Size: 1000, Start: 0, End: 999})
	}
	bud := newCostBudget(1 << 30)
	cr := newChargedReader(bytesReaderAt(make([]byte, 10_000)), o, bud)
	var downloaded int64
	cr.downloaded = func() int64 { return downloaded }
	buf := make([]byte, 10)
	for k := 0; k < 4; k++ {
		_, err := cr.ReadAt(buf, int64(k*1000))
		require.NoError(t, err)
	}
	downloaded = 1500 // three of the four articles came from the cache
	_, err := cr.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.EqualValues(t, 4000, bud.used, "reserved")
	assert.EqualValues(t, 1500, bud.spent(), "settled to the bytes really downloaded")
	downloaded = 6000 // more than declared: the excess is charged as before
	_, err = cr.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.EqualValues(t, 6000, bud.spent())
}
