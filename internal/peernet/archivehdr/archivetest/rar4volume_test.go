package archivetest_test

import (
	"encoding/binary"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/archivehdr/archivetest"
)

// flagsOf returns the flags of the block of the given type in a RAR 4 volume
// (headers only; the test volumes carry little data).
func flagsOf(t *testing.T, data []byte, kind byte) uint16 {
	t.Helper()
	for pos := 7; pos+7 <= len(data); {
		size := int(binary.LittleEndian.Uint16(data[pos+5 : pos+7]))
		flags := binary.LittleEndian.Uint16(data[pos+3 : pos+5])
		if data[pos+2] == kind {
			return flags
		}
		if flags&0x8000 != 0 && kind != data[pos+2] {
			size += int(binary.LittleEndian.Uint32(data[pos+7 : pos+11]))
		}
		pos += size
	}
	t.Fatalf("no block of type %#x", kind)
	return 0
}

// A multi-volume RAR 4 set has the volume and new-numbering flags on the
// main header (first-volume on volume 0) and "more volumes follow" on every
// end block but the last; the real RAR reader needs all of them.
func TestRAR4VolumeFlags(t *testing.T) {
	f := archivetest.File{Name: "a.mkv", Data: []byte("x"), SplitAfter: true, UnpackedSize: 2}
	first := archivetest.RAR4Volume(0, false, f)
	if got := flagsOf(t, first.Data, 0x73); got != 0x0111 {
		t.Fatalf("first volume main flags %#x", got)
	}
	if got := flagsOf(t, first.Data, 0x7B); got != 0x0001 {
		t.Fatalf("first volume end flags %#x", got)
	}
	last := archivetest.RAR4Volume(1, true, archivetest.File{Name: "a.mkv", Data: []byte("y"), SplitBefore: true, UnpackedSize: 2})
	if got := flagsOf(t, last.Data, 0x73); got != 0x0011 {
		t.Fatalf("last volume main flags %#x", got)
	}
	if got := flagsOf(t, last.Data, 0x7B); got != 0 {
		t.Fatalf("last volume end flags %#x", got)
	}
}
