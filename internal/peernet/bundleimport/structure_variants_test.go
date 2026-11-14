package bundleimport

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memOpener serves one in-memory file through the Opener the check reads from.
type memOpener struct{ fs afero.Fs }

func (m memOpener) OpenFile(_ context.Context, name string, flag int, perm os.FileMode) (afero.File, error) {
	return m.fs.OpenFile(name, flag, perm)
}

func structureOf(t *testing.T, data []byte, name string) error {
	t.Helper()
	fs := afero.NewMemMapFs()
	require.NoError(t, afero.WriteFile(fs, name, data, 0o644))
	return verifyStructure(context.Background(), memOpener{fs}, name, int64(len(data)), false)
}

func requireAccepted(t *testing.T, data []byte, name string) {
	t.Helper()
	assert.NoError(t, structureOf(t, data, name))
}

func requireRejected(t *testing.T, data []byte, name string) {
	t.Helper()
	err := structureOf(t, data, name)
	require.Error(t, err)
	assert.ErrorIs(t, err, errStructure)
}

// ---- Matroska ----

func ebmlSize(n int) []byte { // 8-byte EBML size
	return []byte{0x01, 0, byte(n >> 40), byte(n >> 32), byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}

// mkvNoSeekHead is a streaming-style file: EBML header, a Segment (known size
// ending at the file end, or unknown when live), Info, Tracks and Clusters; no
// SeekHead and no Cues.
func mkvNoSeekHead(size int, live bool) []byte {
	ebml := append([]byte{0x1A, 0x45, 0xDF, 0xA3, 0x8B, 0x42, 0x82, 0x88}, "matroska"...)
	out := append([]byte{}, ebml...)
	out = append(out, 0x18, 0x53, 0x80, 0x67)
	segStart := len(out) + 8
	if live {
		out = append(out, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
	} else {
		out = append(out, ebmlSize(size-segStart)...)
	}
	out = append(out, 0x15, 0x49, 0xA9, 0x66, 0x84, 0, 0, 0, 0) // Info
	out = append(out, 0x16, 0x54, 0xAE, 0x6B, 0x84, 0, 0, 0, 0) // Tracks
	fill := size - len(out) - 12
	out = append(out, 0x1F, 0x43, 0xB6, 0x75, 0x10, 0, byte(fill>>16), byte(fill>>8)) // Cluster, 4-byte size
	out[len(out)-4] = 0x10 | byte(fill>>24&0x0F)
	out[len(out)-3] = byte(fill >> 16)
	out[len(out)-2] = byte(fill >> 8)
	out = append(out, byte(fill))
	out = append(out, simpleBlocks(size-len(out))...)
	return out
}

func TestStructureVariants_Matroska(t *testing.T) {
	t.Run("SeekHead with Cues at the tail (mkvmerge style)", func(t *testing.T) { requireAccepted(t, mkvPlain(4096), "a.mkv") })
	t.Run("no SeekHead, known Segment size (streaming mux)", func(t *testing.T) { requireAccepted(t, mkvNoSeekHead(8192, false), "a.mkv") })
	t.Run("no SeekHead, unknown Segment size (live webm)", func(t *testing.T) { requireAccepted(t, mkvNoSeekHead(8192, true), "a.webm") })
	t.Run("SeekHead after a Void", func(t *testing.T) {
		good := mkvPlain(4096)
		// Insert a 4-byte Void in front of the SeekHead and grow the Segment.
		const segHdr = 16 // EBML header (16 bytes) precedes the Segment ID
		_ = segHdr
		requireAccepted(t, withVoidBeforeSeekHead(t, good), "a.mkv")
	})

	t.Run("head intact, Cues at the tail broken", func(t *testing.T) {
		bad := mkvPlain(4096)
		bad[len(bad)-9] ^= 0xFF
		requireRejected(t, bad, "a.mkv")
	})
	t.Run("known size claims more than the file holds, no SeekHead", func(t *testing.T) {
		bad := mkvNoSeekHead(8192, false)
		bad[27] += 9 // low byte of the Segment size
		requireRejected(t, bad, "a.mkv")
	})
	t.Run("garbage where the first Segment child belongs", func(t *testing.T) {
		bad := mkvNoSeekHead(8192, true)
		copy(bad[16+4+8:], []byte{0x7F, 0x7F, 0x7F, 0x7F})
		requireRejected(t, bad, "a.mkv")
	})
}

// withVoidBeforeSeekHead rewrites mkvPlain's file with a Void element between
// the Segment header and the SeekHead (Segment size and Seek positions are
// relative to the Segment data, so every position moves by the Void's size).
func withVoidBeforeSeekHead(t *testing.T, plain []byte) []byte {
	t.Helper()
	const voidLen = 6 // EC 84 + 4 bytes
	segDataStart := 16 + 12
	out := append([]byte{}, plain[:segDataStart]...)
	out = append(out, 0xEC, 0x84, 0, 0, 0, 0)
	out = append(out, plain[segDataStart:]...)
	// Segment size grows by voidLen.
	sz := int(plain[16+8])<<24 | int(plain[16+9])<<16 | int(plain[16+10])<<8 | int(plain[16+11])
	sz += voidLen
	out[16+8], out[16+9], out[16+10], out[16+11] = byte(sz>>24), byte(sz>>16), byte(sz>>8), byte(sz)
	// Seek positions of mkvPlain are u32 values at fixed places inside the SeekHead.
	for i := segDataStart + voidLen; i+6 < segDataStart+voidLen+2*17+5; i++ {
		if out[i] == 0x53 && out[i+1] == 0xAC && out[i+2] == 0x84 {
			pos := int(out[i+3])<<24 | int(out[i+4])<<16 | int(out[i+5])<<8 | int(out[i+6])
			pos += voidLen
			out[i+3], out[i+4], out[i+5], out[i+6] = byte(pos>>24), byte(pos>>16), byte(pos>>8), byte(pos)
		}
	}
	return out
}

// ---- MP4 ----

func mp4Chain(boxes ...[]byte) []byte { return bytes.Join(boxes, nil) }

func mp4Fragmented(pairs int, lastMdat int, mfra bool) []byte {
	out := mp4Chain(mp4Box("ftyp", 16), mp4Box("moov", 300))
	for i := 0; i < pairs; i++ {
		out = append(out, mp4Box("moof", 40)...)
		n := 200
		if i == pairs-1 {
			n = lastMdat
		}
		out = append(out, mp4Box("mdat", n)...)
	}
	if mfra {
		// A real mfra: its only child is the mfro, which states the mfra's size.
		out = append(out, box("mfra", box("mfro", []byte{0, 0, 0, 0, 0, 0, 0, 24}))...)
	}
	return out
}

func TestStructureVariants_MP4(t *testing.T) {
	base := func() []byte { return mp4Chain(mp4Box("ftyp", 16), mp4Box("moov", 200), mp4Box("mdat", 600)) }

	t.Run("plain moov-then-mdat", func(t *testing.T) { requireAccepted(t, base(), "a.mp4") })
	t.Run("moov at the end (not faststart)", func(t *testing.T) {
		requireAccepted(t, mp4Chain(mp4Box("ftyp", 16), mp4Box("mdat", 600), mp4Box("moov", 200)), "a.mp4")
	})
	t.Run("trailing free, skip and uuid boxes", func(t *testing.T) {
		requireAccepted(t, mp4Chain(base(), mp4Box("free", 100), mp4Box("skip", 30), mp4Box("uuid", 40)), "a.mp4")
	})
	t.Run("zero-sized last box runs to the end of the file", func(t *testing.T) {
		mdat := mp4Box("mdat", 600)
		copy(mdat[:4], []byte{0, 0, 0, 0})
		requireAccepted(t, mp4Chain(mp4Box("ftyp", 16), mp4Box("moov", 200), mdat), "a.mp4")
	})
	t.Run("64-bit box size", func(t *testing.T) {
		n := 16 + 600
		mdat := append([]byte{0, 0, 0, 1, 'm', 'd', 'a', 't', 0, 0, 0, 0, 0, 0, byte(n >> 8), byte(n)}, make([]byte, 600)...)
		requireAccepted(t, mp4Chain(mp4Box("ftyp", 16), mp4Box("moov", 200), mdat), "a.mp4")
	})
	t.Run("zero padding after the last box", func(t *testing.T) {
		requireAccepted(t, append(base(), make([]byte, 5000)...), "a.mp4")
	})
	t.Run("fragmented: thousands of moof/mdat pairs, no mfra", func(t *testing.T) {
		requireAccepted(t, mp4Fragmented(3000, 200, false), "a.mp4")
	})
	t.Run("fragmented with an mfra at the end", func(t *testing.T) {
		requireAccepted(t, mp4Fragmented(3000, 200, true), "a.mp4")
	})
	t.Run("fragmented: last fragment larger than the first tail window", func(t *testing.T) {
		requireAccepted(t, mp4Fragmented(20, 6<<20, false), "a.mp4")
	})

	t.Run("non-zero junk after the last box", func(t *testing.T) {
		requireRejected(t, append(base(), 1, 2, 3), "a.mp4")
	})
	t.Run("zero padding that is not padding (junk follows)", func(t *testing.T) {
		requireRejected(t, append(append(base(), make([]byte, 100)...), 7), "a.mp4")
	})
	t.Run("no moov", func(t *testing.T) { requireRejected(t, mp4Chain(mp4Box("ftyp", 16), mp4Box("mdat", 800)), "a.mp4") })
	t.Run("last box runs past the end", func(t *testing.T) {
		b := base()
		requireRejected(t, b[:len(b)-50], "a.mp4")
	})
	t.Run("fragmented, head fine, tail is garbage", func(t *testing.T) {
		f := mp4Fragmented(3000, 200, false)
		for i := len(f) - 5000; i < len(f); i++ {
			f[i] = byte(i * 31)
		}
		requireRejected(t, f, "a.mp4")
	})
	t.Run("fragmented, truncated in the middle of a box", func(t *testing.T) {
		f := mp4Fragmented(3000, 200, false)
		requireRejected(t, f[:len(f)-100], "a.mp4")
	})
}

// ---- MPEG-TS / M2TS ----

func tsFile(stride, syncOff, packets int) []byte {
	ts := make([]byte, stride*packets)
	for k := 0; k < packets; k++ {
		ts[k*stride+syncOff] = 0x47
	}
	return ts
}

func TestStructureVariants_TransportStreams(t *testing.T) {
	for name, tc := range map[string]struct {
		stride, sync int
		file         string
	}{"188-byte TS": {188, 0, "a.ts"}, "192-byte M2TS (timestamp first)": {192, 4, "a.m2ts"}, "204-byte TS (Reed-Solomon)": {204, 0, "a.ts"}} {
		t.Run(name, func(t *testing.T) {
			good := tsFile(tc.stride, tc.sync, 60)
			requireAccepted(t, good, tc.file)
			bad := append([]byte{}, good...)
			bad[59*tc.stride+tc.sync] = 0 // the last packet lost sync
			requireRejected(t, bad, tc.file)
		})
	}
	t.Run("trailing partial packet is not a loss of sync", func(t *testing.T) {
		requireAccepted(t, append(tsFile(188, 0, 60), make([]byte, 100)...), "a.ts")
	})
	t.Run("capture that starts mid-packet is read as an unknown container", func(t *testing.T) {
		requireAccepted(t, append(make([]byte, 77), tsFile(188, 0, 60)...), "a.ts")
	})
}
