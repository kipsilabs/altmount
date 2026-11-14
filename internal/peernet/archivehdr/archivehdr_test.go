package archivehdr

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/archivehdr/archivetest"
)

func verify(data []byte, e Extent) Result {
	return Verify(bytes.NewReader(data), int64(len(data)), e, Limits{})
}

func ext(off, length int64, idx, count int, size int64) Extent {
	return Extent{Offset: off, Length: length, Index: idx, Count: count, FileSize: size}
}

func payload(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i%250 + 1)
	}
	return out
}

type builder struct {
	name  string
	build func(files ...archivetest.File) archivetest.Built
}

var rarBuilders = []builder{
	{"rar5", archivetest.RAR5},
	{"rar4", archivetest.RAR4},
}

func TestStoredSingleVolumeFileIsBound(t *testing.T) {
	for _, b := range rarBuilders {
		t.Run(b.name, func(t *testing.T) {
			p := payload(1000)
			a := b.build(archivetest.File{Name: "movie.mkv", Data: p})
			assert.Equal(t, Bound, verify(a.Data, ext(a.DataOffsets[0], 1000, 0, 1, 1000)).Outcome)
		})
	}
}

func TestSecondFileInTheVolumeIsReachedByWalking(t *testing.T) {
	for _, b := range rarBuilders {
		t.Run(b.name, func(t *testing.T) {
			a := b.build(archivetest.File{Name: "a.nfo", Data: payload(300)}, archivetest.File{Name: "movie.mkv", Data: payload(5000)})
			assert.Equal(t, Bound, verify(a.Data, ext(a.DataOffsets[1], 5000, 0, 1, 5000)).Outcome)
			assert.Equal(t, Bound, verify(a.Data, ext(a.DataOffsets[0], 300, 0, 1, 300)).Outcome)
		})
	}
}

func TestExtentThatContradictsTheHeaderIsAMismatch(t *testing.T) {
	for _, b := range rarBuilders {
		a := b.build(archivetest.File{Name: "movie.mkv", Data: payload(1000)})
		off := a.DataOffsets[0]
		cases := map[string]Extent{
			"data starts one byte later":        ext(off+1, 999, 0, 1, 1000),
			"data starts inside the header":     ext(off-4, 1000, 0, 1, 1000),
			"length is shorter":                 ext(off, 999, 0, 1, 1000),
			"length is longer":                  ext(off, 1001, 0, 1, 1000),
			"the file is declared bigger":       ext(off, 1000, 0, 1, 1001),
			"extent is not the first of a file": ext(off, 1000, 1, 2, 1000),
			"extent is the first of a split":    ext(off, 1000, 0, 2, 2000),
		}
		for name, e := range cases {
			t.Run(b.name+"/"+name, func(t *testing.T) {
				r := verify(a.Data, e)
				assert.Equal(t, Mismatch, r.Outcome, r.Detail)
			})
		}
	}
}

func TestEntryKindsThatCannotBeStreamedAreMismatches(t *testing.T) {
	for _, b := range rarBuilders {
		t.Run(b.name, func(t *testing.T) {
			p := payload(800)
			comp := b.build(archivetest.File{Name: "m.mkv", Data: p, Compressed: true})
			r := verify(comp.Data, ext(comp.DataOffsets[0], 800, 0, 1, 800))
			assert.Equal(t, Mismatch, r.Outcome, "a compressed entry is not stored: %s", r.Detail)
			svc := b.build(archivetest.File{Name: "CMT", Data: p, Service: true})
			r = verify(svc.Data, ext(svc.DataOffsets[0], 800, 0, 1, 800))
			assert.Equal(t, Mismatch, r.Outcome, "a service block is not a file: %s", r.Detail)
			enc := b.build(archivetest.File{Name: "m.mkv", Data: p, IV: bytes.Repeat([]byte{7}, 16)})
			r = verify(enc.Data, ext(enc.DataOffsets[0], 800, 0, 1, 800))
			assert.Equal(t, Mismatch, r.Outcome, "an encrypted entry cannot be read without a key: %s", r.Detail)
		})
	}
}

func TestContinuationFlagsMustAgreeWithTheExtentPosition(t *testing.T) {
	for _, b := range rarBuilders {
		t.Run(b.name, func(t *testing.T) {
			first := b.build(archivetest.File{Name: "m.mkv", Data: payload(500), SplitAfter: true, UnpackedSize: 1200})
			mid := b.build(archivetest.File{Name: "m.mkv", Data: payload(500), SplitBefore: true, SplitAfter: true, UnpackedSize: 1200})
			last := b.build(archivetest.File{Name: "m.mkv", Data: payload(200), SplitBefore: true, UnpackedSize: 1200})
			assert.Equal(t, Bound, verify(first.Data, ext(first.DataOffsets[0], 500, 0, 3, 1200)).Outcome)
			assert.Equal(t, Bound, verify(mid.Data, ext(mid.DataOffsets[0], 500, 1, 3, 1200)).Outcome)
			assert.Equal(t, Bound, verify(last.Data, ext(last.DataOffsets[0], 200, 2, 3, 1200)).Outcome)
			assert.Equal(t, Mismatch, verify(mid.Data, ext(mid.DataOffsets[0], 500, 0, 3, 1200)).Outcome, "a middle part claimed as the first")
			assert.Equal(t, Mismatch, verify(first.Data, ext(first.DataOffsets[0], 500, 2, 3, 1200)).Outcome, "a first part claimed as the last")
			assert.Equal(t, Mismatch, verify(last.Data, ext(last.DataOffsets[0], 200, 1, 3, 1200)).Outcome, "a last part claimed as a middle one")
		})
	}
}

func TestSevenZip(t *testing.T) {
	a := archivetest.SevenZip(archivetest.SevenZipOpts{}, archivetest.File{Name: "a.bin", Data: payload(400)}, archivetest.File{Name: "m.mkv", Data: payload(900)})
	assert.Equal(t, Bound, verify(a.Data, ext(a.DataOffsets[1], 900, 0, 1, 900)).Outcome)
	assert.Equal(t, Mismatch, verify(a.Data, ext(a.DataOffsets[1], 899, 0, 1, 900)).Outcome)
	assert.Equal(t, Mismatch, verify(a.Data, ext(a.DataOffsets[1]+1, 899, 0, 1, 899)).Outcome, "inside a stored stream")
	assert.Equal(t, Unverifiable, verify(a.Data, ext(a.DataOffsets[1], 900, 0, 2, 900)).Outcome, "7z volumes are byte splits, not continuations")

	solid := archivetest.SevenZip(archivetest.SevenZipOpts{Solid: true}, archivetest.File{Name: "a", Data: payload(100)}, archivetest.File{Name: "b", Data: payload(250)}, archivetest.File{Name: "c", Data: payload(77)})
	assert.Equal(t, Bound, verify(solid.Data, ext(solid.DataOffsets[1], 250, 0, 1, 250)).Outcome, "a stored substream of a solid folder")
	assert.Equal(t, Bound, verify(solid.Data, ext(solid.DataOffsets[2], 77, 0, 1, 77)).Outcome)

	comp := archivetest.SevenZip(archivetest.SevenZipOpts{}, archivetest.File{Name: "m.mkv", Data: payload(900), Compressed: true})
	assert.Equal(t, Mismatch, verify(comp.Data, ext(comp.DataOffsets[0], 900, 0, 1, 900)).Outcome, "a compressed folder is not stored")

	enc := archivetest.SevenZip(archivetest.SevenZipOpts{EncodedHeader: true}, archivetest.File{Name: "m.mkv", Data: payload(900)})
	r := verify(enc.Data, ext(enc.DataOffsets[0], 900, 0, 1, 900))
	assert.Equal(t, Unverifiable, r.Outcome, "an encoded end header cannot be read: %s", r.Detail)
}

func TestUnreadableVolumesAreUnverifiableNeverMismatches(t *testing.T) {
	a := archivetest.RAR5(archivetest.File{Name: "m.mkv", Data: payload(600)})
	off := a.DataOffsets[0]
	junk := bytes.Repeat([]byte{0x5A}, 4096)
	corrupt := append([]byte(nil), a.Data...)
	corrupt[12] ^= 0xFF // a header checksum
	for name, data := range map[string][]byte{
		"not an archive":            junk,
		"too short":                 a.Data[:5],
		"damaged header":            corrupt,
		"truncated before the data": a.Data[:off-3],
	} {
		t.Run(name, func(t *testing.T) {
			r := verify(data, ext(off, 600, 0, 1, 600))
			assert.Equal(t, Unverifiable, r.Outcome, r.Detail)
		})
	}
}

func TestEncryptedHeadersAreUnverifiable(t *testing.T) {
	// RAR 5: an archive encryption header (type 4) right after the signature.
	data := []byte{'R', 'a', 'r', '!', 0x1A, 0x07, 0x01, 0x00}
	body := []byte{4, 0, 0, 15, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	block := append([]byte{byte(len(body))}, body...)
	sum := crc32sum(block)
	data = append(append(data, sum[:]...), block...)
	data = append(data, make([]byte, 100)...)
	r := verify(data, ext(60, 40, 0, 1, 40))
	assert.Equal(t, Unverifiable, r.Outcome, r.Detail)
	// RAR 4: main header with the password flag.
	d4 := archivetest.RAR4(archivetest.File{Name: "m", Data: payload(50)}).Data
	d4[10] |= 0x80 // main header flags, low byte at offset 7+3
	r = verify(d4, ext(70, 50, 0, 1, 50))
	assert.Equal(t, Unverifiable, r.Outcome, r.Detail)
}

// A volume with many entries before the extent exhausts the walk budget: the
// answer is "unverifiable", not a guess, and the reads stay within the limit.
func TestWalkBudgetIsEnforced(t *testing.T) {
	for _, b := range rarBuilders {
		t.Run(b.name, func(t *testing.T) {
			files := make([]archivetest.File, 300)
			for i := range files {
				files[i] = archivetest.File{Name: "f", Data: []byte{1, 2, 3}}
			}
			files = append(files, archivetest.File{Name: "m.mkv", Data: payload(100)})
			a := b.build(files...)
			cr := &countingReader{data: a.Data}
			lim := Limits{MaxBlocks: 10, MaxReads: 25}
			r := Verify(cr, int64(len(a.Data)), ext(a.DataOffsets[300], 100, 0, 1, 100), lim)
			assert.Equal(t, Unverifiable, r.Outcome, r.Detail)
			assert.LessOrEqual(t, cr.reads, 25)
			// With a roomy budget the same extent is bound.
			assert.Equal(t, Bound, Verify(bytes.NewReader(a.Data), int64(len(a.Data)), ext(a.DataOffsets[300], 100, 0, 1, 100), Limits{MaxBlocks: 400, MaxReads: 900}).Outcome)
		})
	}
}

type countingReader struct {
	data  []byte
	reads int
	bytes int
}

func (c *countingReader) ReadAt(p []byte, off int64) (int, error) {
	c.reads++
	c.bytes += len(p)
	return bytes.NewReader(c.data).ReadAt(p, off)
}

func TestReaderErrorsAreNotAboutTheArchive(t *testing.T) {
	a := archivetest.RAR5(archivetest.File{Name: "m.mkv", Data: payload(600)})
	boom := errors.New("article missing")
	v, err := Scan(failingReader{err: boom}, int64(len(a.Data)), 100, Limits{})
	assert.Nil(t, v)
	var re *ReadError
	require.ErrorAs(t, err, &re)
	assert.ErrorIs(t, err, boom)
}

type failingReader struct{ err error }

func (f failingReader) ReadAt([]byte, int64) (int, error) { return 0, f.err }

// Hostile declared sizes: nothing is allocated from them and no length
// overflows.
func TestHostileSizesAreRejectedWithoutAllocatingThem(t *testing.T) {
	sig := []byte{'R', 'a', 'r', '!', 0x1A, 0x07, 0x01, 0x00}
	// RAR 5 header claiming a 2 GiB header.
	huge := append(append([]byte{}, sig...), 0, 0, 0, 0, 0x80, 0x80, 0x80, 0x80, 0x08)
	huge = append(huge, make([]byte, 64)...)
	assert.Equal(t, Unverifiable, verify(huge, ext(100, 10, 0, 1, 10)).Outcome)
	// A ten-byte vint that overflows 64 bits.
	over := append(append([]byte{}, sig...), 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F)
	assert.Equal(t, Unverifiable, verify(over, ext(100, 10, 0, 1, 10)).Outcome)
	// RAR 4 file header with a data size close to 2^63 once the large flag is set.
	a := archivetest.RAR4(archivetest.File{Name: "m", Data: payload(10)})
	assert.NotPanics(t, func() { verify(a.Data[:len(a.Data)-30], ext(1<<62, 10, 0, 1, 10)) })
}

func TestRealArchivesFromTheImporterFixtures(t *testing.T) {
	dir := filepath.Join("..", "..", "importer", "testdata")
	read := func(rel string) []byte {
		data, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			t.Skipf("fixture %s missing: %v", rel, err)
		}
		return data
	}
	single := read("rar_single/archive.rar")
	v, err := Scan(bytes.NewReader(single), int64(len(single)), 1<<20, Limits{})
	require.NoError(t, err)
	require.Equal(t, FormatRAR5, v.Format)
	var blk *Block
	for k := range v.Blocks {
		if v.Blocks[k].File {
			blk = &v.Blocks[k]
		}
	}
	require.NotNil(t, blk)
	assert.True(t, blk.Stored)
	assert.EqualValues(t, 200000, blk.DataSize)
	assert.Equal(t, []byte{0, 1, 2, 3}, single[blk.DataOffset:blk.DataOffset+4], "the data area begins with the payload")
	assert.Equal(t, Bound, verify(single, ext(blk.DataOffset, 200000, 0, 1, 200000)).Outcome)
	assert.Equal(t, Mismatch, verify(single, ext(blk.DataOffset, 199999, 0, 1, 200000)).Outcome)

	p1, p2 := read("rar_multi/archive.part01.rar"), read("rar_multi/archive.part02.rar")
	v1, err := Scan(bytes.NewReader(p1), int64(len(p1)), 1<<20, Limits{})
	require.NoError(t, err)
	v2, err := Scan(bytes.NewReader(p2), int64(len(p2)), 1<<20, Limits{})
	require.NoError(t, err)
	var f1, f2 Block
	for _, b := range v1.Blocks {
		if b.File {
			f1 = b
		}
	}
	for _, b := range v2.Blocks {
		if b.File {
			f2 = b
		}
	}
	assert.True(t, f1.SplitAfter && !f1.SplitBefore, "first volume: %+v", f1)
	assert.True(t, f2.SplitBefore && !f2.SplitAfter, "last volume: %+v", f2)
	assert.EqualValues(t, 200000, f1.DataSize+f2.DataSize)
	assert.Equal(t, Bound, verify(p1, ext(f1.DataOffset, f1.DataSize, 0, 2, 200000)).Outcome)
	assert.Equal(t, Bound, verify(p2, ext(f2.DataOffset, f2.DataSize, 1, 2, 200000)).Outcome)
	assert.Equal(t, Mismatch, verify(p2, ext(f2.DataOffset, f2.DataSize, 0, 2, 200000)).Outcome)

	sz := read("7z_single/archive.7z")
	v7, err := Scan(bytes.NewReader(sz), int64(len(sz)), 1<<20, Limits{})
	t.Logf("7z fixture: format=%v blocks=%+v err=%v", v7.Format, v7.Blocks, err)
	if err == nil {
		require.NotEmpty(t, v7.Blocks)
		assert.Equal(t, Bound, verify(sz, ext(v7.Blocks[0].DataOffset, v7.Blocks[0].DataSize, 0, 1, v7.Blocks[0].UnpackedSize)).Outcome)
	} else {
		assert.ErrorIs(t, err, ErrCompressedHeader, "a real 7z whose end header is encoded is unverifiable")
	}
}
