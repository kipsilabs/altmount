package bundle

import (
	"bytes"
	"testing"
)

func seedBundles(t testing.TB) [][]byte {
	t.Helper()
	var out [][]byte
	for _, b := range []*Bundle{goodBundle(t), goodNZBBundle(t), syntheticBundle(t, 50, 716_800)} {
		out = append(out, mustEncode(t, b))
	}
	return out
}

func FuzzDecodeBundle(f *testing.F) {
	for _, s := range seedBundles(f) {
		f.Add(s)
		f.Add(s[:len(s)/2])
	}
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01})
	f.Add([]byte{0x08, 0x81, 0x00})
	f.Fuzz(func(t *testing.T, data []byte) {
		b, err := Decode(data)
		if err != nil {
			return
		}
		again, err := Encode(b)
		if err != nil {
			t.Fatalf("decoded bundle does not re-encode: %v", err)
		}
		if !bytes.Equal(again, data) {
			t.Fatal("decode then encode changed the bytes")
		}
		if _, err := ToLocal(b, testNow); err != nil {
			t.Fatalf("valid bundle does not convert: %v", err)
		}
	})
}

func FuzzDecompress(f *testing.F) {
	for _, s := range seedBundles(f) {
		w, err := Compress(s)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(w)
		f.Add(w[:len(w)/2])
	}
	f.Add(zeroBomb(f, MaxDecompressedSize+1, 0))
	f.Add(zeroBomb(f, 1<<20, 128<<20))
	f.Add([]byte("not zstd"))
	f.Add([]byte{})
	f.Add([]byte{0x28, 0xb5, 0x2f, 0xfd})
	f.Fuzz(func(t *testing.T, data []byte) {
		out, err := Decompress(data)
		if err != nil {
			if out != nil {
				t.Fatal("output returned with an error")
			}
			return
		}
		if len(out) == 0 || len(out) > MaxDecompressedSize || len(data) > MaxCompressedSize {
			t.Fatalf("limits violated: in %d out %d", len(data), len(out))
		}
	})
}

// mutateBundle breaks one field of a good bundle, chosen by sel.
func mutateBundle(b *Bundle, sel uint8, v int64) {
	f := &b.Files[int(sel/32)%len(b.Files)]
	switch sel % 16 {
	case 0:
		f.Size = v
	case 1:
		if len(f.Segments) > 0 {
			f.Segments[0].Start = v
		}
	case 2:
		if len(f.Segments) > 0 {
			f.Segments[len(f.Segments)-1].End = v
		}
	case 3:
		if len(f.Segments) > 0 {
			f.Segments[0].Size = v
		}
	case 4:
		if len(f.Segments) > 0 {
			f.Segments[0].Ref = v
		}
	case 5:
		if len(f.Extents) > 0 {
			f.Extents[0].InnerOffset = v
		}
	case 6:
		if len(f.Extents) > 0 {
			f.Extents[len(f.Extents)-1].InnerLength = v
		}
	case 7:
		if len(f.Extents) > 0 {
			f.Extents[0].Outer = int(v)
		}
	case 8:
		if len(f.Outer) > 0 {
			f.Outer[0].VolumeSize = v
		}
	case 9:
		if len(f.Clips) > 0 {
			f.Clips[0].ByteLen = v
		}
	case 10:
		if len(f.Clips) > 0 {
			f.Clips[0].Delta90k = v
		}
	case 11:
		b.TotalSize = v
	case 12:
		f.ReleaseDate = v
	case 13:
		b.PostedUnix = v
	case 14:
		if len(f.Outer) > 0 && len(f.Outer[0].Segments) > 0 {
			f.Outer[0].Segments[0].End = v
		}
	case 15:
		if len(f.Par2) > 0 && len(f.Par2[0].Segments) > 0 {
			f.Par2[0].Segments[0].Size = v
		}
	}
}

func FuzzValidate(f *testing.F) {
	for _, s := range seedBundles(f) {
		f.Add(s, uint8(0), int64(0))
	}
	f.Add([]byte{}, uint8(3), int64(-1))
	f.Add([]byte{}, uint8(36), int64(1<<62))
	f.Add([]byte{}, uint8(70), int64(576))
	// The good bundles are built (and their ids hashed) once; every exec
	// mutates a cheap deep copy, so the fuzzer spends its time on Validate.
	bases := []*Bundle{goodBundle(f), goodNZBBundle(f)}
	f.Fuzz(func(t *testing.T, data []byte, sel uint8, v int64) {
		check := func(b *Bundle) {
			if err := Validate(b); err != nil {
				return
			}
			raw, err := Encode(b)
			if err != nil {
				t.Fatalf("valid bundle does not encode: %v", err)
			}
			got, err := Decode(raw)
			if err != nil {
				t.Fatalf("valid bundle does not decode: %v", err)
			}
			if again, err := Encode(got); err != nil || !bytes.Equal(again, raw) {
				t.Fatalf("canonical re-encode differs (%v)", err)
			}
		}
		if b, err := decodeRaw(data); err == nil {
			check(b)
		}
		for _, base := range bases {
			b := clone(base)
			mutateBundle(b, sel, v)
			check(b)
		}
	})
}
