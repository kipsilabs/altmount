package bundle

import (
	"runtime"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// hostileClipBundle is the shape from the review: many tiny files, each with
// the maximum number of clip pairs. It is a few tens of KB on the wire and
// about 62 MB canonical.
func hostileClipBundle(t testing.TB, files int) []byte {
	t.Helper()
	var clips []byte
	for i := 0; i < MaxClips; i++ {
		clips = protowire.AppendVarint(clips, 192)
		clips = protowire.AppendVarint(clips, 0)
	}
	var out []byte
	out = appendVarint(out, fVersion, SchemaVersion)
	out = appendVarint(out, fScheme, uint64(SchemeFiles))
	out = appendBytes(out, fReleaseID, repeat32(0xAA))
	out = appendString(out, fTitle, "t")
	out = appendVarint(out, fPosted, 1700000000)
	out = appendVarint(out, fTotal, uint64(files))
	for i := 0; i < files; i++ {
		var f []byte
		f = appendString(f, fName, string(rune('a'+i%26))+string(rune('a'+i/26%26))+string(rune('a'+i/676))+".bin")
		f = appendVarint(f, fSize, 1)
		f = appendBytes(f, fClips, clips)
		id := repeat32(0)
		id[0], id[1] = byte(i>>8), byte(i)
		id[31] = 1
		f = appendBytes(f, fFileID, id)
		out = appendBytes(out, fFiles, f)
	}
	return out
}

func repeat32(v byte) []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = v
	}
	return b
}

func TestHostileClipsCannotBloatVerify(t *testing.T) {
	raw := hostileClipBundle(t, 50)
	if len(raw) > MaxDecompressedSize {
		t.Fatalf("test input is %d bytes canonical", len(raw))
	}
	wire, err := Compress(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) > 64<<10 {
		t.Fatalf("hostile wire is %d bytes, want at most 64 KB", len(wire))
	}
	id := ComputeID(raw)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = VerifyCanonical(raw, id, [32]byte{0xAA})
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("hostile bundle verified")
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 128<<20 {
		t.Fatalf("Verify allocated %d MB for a %d byte input, want at most 128 MB", got>>20, len(wire))
	}
}
