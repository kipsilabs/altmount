package bundleimport

import (
	"bytes"
	"context"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func structureOpen(t *testing.T, data []byte, name string) error {
	t.Helper()
	fs := afero.NewMemMapFs()
	require.NoError(t, afero.WriteFile(fs, name, data, 0o644))
	return verifyStructure(context.Background(), memOpener{fs}, name, int64(len(data)), false)
}

func box(typ string, payload []byte) []byte {
	n := 8 + len(payload)
	return append(append([]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}, typ...), payload...)
}

// Reviewer forgery: valid fragmented head, 3 MB of garbage, then any 8 bytes
// shaped like a `free` box. It used to pass on the free box alone.
func TestForgery_FragmentedMP4WithGarbageMiddleAndFreeTail(t *testing.T) {
	var b []byte
	b = append(b, box("ftyp", make([]byte, 8))...)
	b = append(b, box("moov", make([]byte, 100))...)
	b = append(b, box("mdat", make([]byte, 100))...)
	for i := 0; i < 8; i++ {
		b = append(b, box("moof", make([]byte, 20))...)
	}
	b = append(b, bytes.Repeat([]byte{0xAB}, 3<<20)...)
	b = append(b, box("free", nil)...)
	err := structureOpen(t, b, "a.mp4")
	require.Error(t, err)
	assert.ErrorIs(t, err, errStructure)
}

// A real fragmented tail still passes: moof+mdat chaining to the end (with an
// optional free box after), or an mfra whose mfro states its own size.
func TestForgery_RealFragmentTailsStillPass(t *testing.T) {
	head := mp4Chain(mp4Box("ftyp", 16), mp4Box("moov", 300))
	for i := 0; i < 20; i++ {
		head = append(head, mp4Box("moof", 40)...)
		head = append(head, mp4Box("mdat", 200)...)
	}
	mfra := func() []byte {
		mfro := box("mfro", []byte{0, 0, 0, 0, 0, 0, 0, 24}) // 16 bytes; mfra = 8 + 16
		return box("mfra", mfro)
	}
	for name, data := range map[string][]byte{
		"moof+mdat":            head,
		"moof+mdat then free":  append(append([]byte{}, head...), mp4Box("free", 40)...),
		"moof+mdat then mfra":  append(append([]byte{}, head...), mfra()...),
		"mfra then free, skip": append(append(append([]byte{}, head...), mfra()...), append(mp4Box("free", 10), mp4Box("skip", 12)...)...),
	} {
		t.Run(name, func(t *testing.T) { assert.NoError(t, structureOpen(t, data, "a.mp4")) })
	}
	bad := append([]byte{}, head...)
	bad = append(bad, box("mfra", box("mfro", []byte{0, 0, 0, 0, 0, 0, 0, 99}))...) // mfro lies about its mfra
	assert.Error(t, structureOpen(t, bad, "a.mp4"), "an mfro that does not describe its mfra")
	lone := append(append([]byte{}, head[:len(head)-len(mp4Box("mdat", 200))]...), mp4Box("mdat", 200)...)
	_ = lone
	notPair := append(append([]byte{}, head...), mp4Box("sidx", 40)...)
	assert.Error(t, structureOpen(t, notPair, "a.mp4"), "a tail that is neither a moof+mdat pair nor an mfra")
}

func TestStructure_NonFragmentedMP4WithManyTopLevelBoxes(t *testing.T) {
	data := mp4Chain(mp4Box("ftyp", 16))
	for i := 0; i < 150; i++ {
		data = append(data, mp4Box("free", 24)...)
	}
	data = append(data, mp4Box("moov", 200)...)
	data = append(data, mp4Box("mdat", 600)...)
	assert.NoError(t, structureOpen(t, data, "a.mp4"), "a legitimate file with more than 63 boxes is not a 'too many reads' rejection")
}

// Reviewer forgery: a Matroska with valid head (SeekHead promising Cues at the
// tail), a Cues ID at the promised place and nothing but zeros between.
func forgedMKV(total int, fill func(b []byte)) []byte {
	segStartHdr := []byte{0x18, 0x53, 0x80, 0x67}
	cuesPos := total - 4096
	seek := []byte{0x4D, 0xBB, 0x8E, 0x53, 0xAB, 0x84, 0x1C, 0x53, 0xBB, 0x6B, 0x53, 0xAC, 0x84, byte(cuesPos >> 24), byte(cuesPos >> 16), byte(cuesPos >> 8), byte(cuesPos)}
	sh := append([]byte{0x11, 0x4D, 0x9B, 0x74, byte(0x80 | len(seek))}, seek...)
	ebml := []byte{0x1A, 0x45, 0xDF, 0xA3, 0x84, 0x42, 0x82, 0x81, 0x00}
	b := make([]byte, total)
	if fill != nil {
		fill(b)
	}
	copy(b, ebml)
	off := len(ebml)
	segLen := total - off - 4 - 8
	hdr := append(append([]byte{}, segStartHdr...), 0x01, byte(segLen>>48), byte(segLen>>40), byte(segLen>>32), byte(segLen>>24), byte(segLen>>16), byte(segLen>>8), byte(segLen))
	copy(b[off:], hdr)
	segStart := off + len(hdr)
	copy(b[segStart:], sh)
	copy(b[segStart+cuesPos:], []byte{0x1C, 0x53, 0xBB, 0x6B})
	return b
}

func TestForgery_MatroskaWithValidHeadAndTailButZeroMiddle(t *testing.T) {
	err := structureOpen(t, forgedMKV(4<<20, nil), "a.mkv")
	require.Error(t, err)
	assert.ErrorIs(t, err, errStructure)
}

// The same shape with real SimpleBlock chains in the middle is a real file.
func TestForgery_MatroskaWithRealClustersStillPasses(t *testing.T) {
	data := forgedMKV(4<<20, func(b []byte) { fillSimpleBlocks(b, 200, 3000) })
	assert.NoError(t, structureOpen(t, data, "a.mkv"))
}

func TestForgery_TransportStreamWithLostSyncInTheMiddle(t *testing.T) {
	// 20000 packets; sync byte lost on a band of packets in the middle, which
	// the fixed quartile positions can miss but random grid positions find.
	ts := tsFile(188, 0, 20000)
	for k := 3000; k < 17000; k++ {
		if k%4 != 0 { // keep every fourth packet in sync, so a stride probe passes
			ts[k*188] = 0
		}
	}
	hit := 0
	for range 40 {
		if structureOpen(t, ts, "a.ts") != nil {
			hit++
		}
	}
	assert.Greater(t, hit, 30, "random grid positions must find the broken band almost every time")
}

// Every network accepts the same containers: a generic one (avi, mpg, ...)
// gets the head signature and readable-windows check wherever it came from
// (the structure check no longer depends on the network).
func TestGenericContainersAreAcceptedEverywhere(t *testing.T) {
	generic := make([]byte, 5000)
	for i := range generic {
		generic[i] = byte(i%251 + 1)
	}
	assert.NoError(t, structureOpen(t, generic, "a.avi"), "readable windows are enough for a generic container")
	assert.NoError(t, structureOpen(t, mkvPlain(4096), "a.mkv"))
	assert.NoError(t, structureOpen(t, tsFile(188, 0, 60), "a.ts"))
	assert.NoError(t, structureOpen(t, append(make([]byte, 77), tsFile(188, 0, 60)...), "a.ts"),
		"a capture that starts mid-packet is read as a generic container")
}

// simpleBlocks returns exactly n bytes (n >= 7) of consecutive Matroska
// SimpleBlock elements: what the body of a real Cluster looks like.
func simpleBlocks(n int) []byte {
	out := make([]byte, 0, n)
	for k := 0; len(out) < n; k++ {
		remaining := n - len(out)
		want := min(remaining, 90+(k*37)%300)
		if remaining-want < 7 {
			want = remaining
		}
		body := want - 3 // after the ID and the 2-byte size
		out = append(out, 0xA3, 0x40|byte(body>>8), byte(body), 0x81, byte(k>>8), byte(k), 0x80)
		for j := 0; j < body-4; j++ {
			out = append(out, byte(1+(len(out)*7+k)%250))
		}
	}
	return out
}

// fillSimpleBlocks writes a SimpleBlock chain over all of b.
func fillSimpleBlocks(b []byte, _, _ int) { copy(b, simpleBlocks(len(b))) }
