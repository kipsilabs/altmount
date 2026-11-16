package archivehdr

import (
	"bytes"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/archivehdr/archivetest"
)

// fuzzSeeds are small real stored archives of every format and shape, built
// here (there is no pure Go writer for them outside the tests).
func fuzzSeeds() []archivetest.Built {
	p := payload(300)
	iv := bytes.Repeat([]byte{9}, 16)
	return []archivetest.Built{
		archivetest.RAR5(archivetest.File{Name: "m.mkv", Data: p}),
		archivetest.RAR5(archivetest.File{Name: "a", Data: p[:40]}, archivetest.File{Name: "m.mkv", Data: p, SplitBefore: true, SplitAfter: true, UnpackedSize: 900}),
		archivetest.RAR5(archivetest.File{Name: "m.mkv", Data: p, IV: iv}, archivetest.File{Name: "c", Data: p[:9], Service: true}),
		archivetest.RAR5(archivetest.File{Name: "m.mkv", Data: p, Compressed: true}),
		archivetest.RAR5Volume(0, archivetest.File{Name: "m.mkv", Data: p, SplitAfter: true, UnpackedSize: 900}),
		archivetest.RAR5Volume(3, archivetest.File{Name: "m.mkv", Data: p, SplitBefore: true, SplitAfter: true, UnpackedSize: 900, IV: iv}),
		archivetest.RAR4(archivetest.File{Name: "m.mkv", Data: p}),
		archivetest.RAR4(archivetest.File{Name: "a", Data: p[:40]}, archivetest.File{Name: "m.mkv", Data: p, SplitBefore: true, UnpackedSize: 600}),
		archivetest.RAR4(archivetest.File{Name: "m.mkv", Data: p, IV: iv, Compressed: true}),
		archivetest.SevenZip(archivetest.SevenZipOpts{}, archivetest.File{Name: "m.mkv", Data: p}, archivetest.File{Name: "b", Data: p[:50]}),
		archivetest.SevenZip(archivetest.SevenZipOpts{Solid: true}, archivetest.File{Name: "a", Data: p[:80]}, archivetest.File{Name: "b", Data: p}),
		archivetest.SevenZip(archivetest.SevenZipOpts{}, archivetest.File{Name: "m.mkv", Data: p, Compressed: true}),
		archivetest.SevenZip(archivetest.SevenZipOpts{EncodedHeader: true}, archivetest.File{Name: "m.mkv", Data: p}),
	}
}

// FuzzVerifyUnchecked is FuzzVerify with the header checksums not enforced, so
// the fuzzer reaches the parsers under them.
func FuzzVerifyUnchecked(f *testing.F) {
	checkCRC = false
	defer func() { checkCRC = true }()
	addSeeds(f)
	f.Fuzz(fuzzVerifyBody)
}

// FuzzVerify feeds arbitrary bytes and arbitrary extent claims to the header
// check. It must never panic, never read more than its budget, never run
// unbounded and never answer Bound for a claim the volume cannot contain.
func FuzzVerify(f *testing.F) {
	addSeeds(f)
	f.Fuzz(fuzzVerifyBody)
}

func addSeeds(f *testing.F) {
	for _, s := range fuzzSeeds() {
		for _, off := range s.DataOffsets {
			f.Add(s.Data, off, int64(300), uint8(0), uint8(1), int64(300))
		}
		f.Add(s.Data, int64(40), int64(1), uint8(1), uint8(3), int64(1<<40))
	}
	f.Add([]byte{}, int64(0), int64(1), uint8(0), uint8(1), int64(1))
	f.Add([]byte("Rar!\x1a\x07\x01\x00"), int64(8), int64(1), uint8(0), uint8(1), int64(1))
}

func fuzzVerifyBody(t *testing.T, data []byte, off, length int64, idx, count uint8, fsize int64) {
	lim := Limits{MaxBlocks: 24, MaxReads: 48, MaxHeader: 4096, Max7zHeader: 4096}
	e := Extent{Offset: off, Length: length, Index: int(idx), Count: int(count), FileSize: fsize, Keyed: count&0x80 != 0}
	cr := &countingReader{data: data}
	r := Verify(cr, int64(len(data)), e, lim)
	if cr.reads > lim.MaxReads {
		t.Fatalf("%d reads, budget %d", cr.reads, lim.MaxReads)
	}
	if cr.bytes > lim.MaxReads*lim.MaxHeader {
		t.Fatalf("%d bytes read, budget %d", cr.bytes, lim.MaxReads*lim.MaxHeader)
	}
	if r.Outcome == Bound && (off < 0 || length < 1 || off > int64(len(data)) || length > int64(len(data))-off) {
		t.Fatalf("bound an extent [%d,+%d) outside a %d byte volume", off, length, len(data))
	}
	if v, _ := Scan(bytes.NewReader(data), int64(len(data)), off, lim); v != nil && v.VolumeNumber < -1 {
		t.Fatalf("volume number %d", v.VolumeNumber)
	}
	if r2 := Verify(bytes.NewReader(data), int64(len(data)), e, lim); r2 != r {
		t.Fatalf("not deterministic: %+v then %+v", r, r2)
	}
}
