package bundle

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// rawField is one top-level wire field, for crafting hostile encodings.
type rawField struct {
	num protowire.Number
	typ protowire.Type
	v   uint64
	b   []byte
}

func splitTop(t testing.TB, b []byte) []rawField {
	t.Helper()
	var out []rawField
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			t.Fatal("bad tag")
		}
		b = b[n:]
		f := rawField{num: num, typ: typ}
		switch typ {
		case protowire.VarintType:
			f.v, n = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			f.b, n = protowire.ConsumeBytes(b)
		default:
			t.Fatal("unexpected wire type")
		}
		if n < 0 {
			t.Fatal("bad field")
		}
		b = b[n:]
		out = append(out, f)
	}
	return out
}

func joinTop(fs []rawField) []byte {
	var b []byte
	for _, f := range fs {
		b = protowire.AppendTag(b, f.num, f.typ)
		if f.typ == protowire.VarintType {
			b = protowire.AppendVarint(b, f.v)
		} else {
			b = protowire.AppendBytes(b, f.b)
		}
	}
	return b
}

func mustEncode(t testing.TB, b *Bundle) []byte {
	t.Helper()
	raw, err := Encode(b)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return raw
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	b := goodBundle(t)
	raw := mustEncode(t, b)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	assert.Equal(t, b, got, "round trip changed the bundle")
	again, err := Encode(got)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("re-encode differs (err %v)", err)
	}
}

func TestEncodeIsDeterministicAndCompact(t *testing.T) {
	b := goodBundle(t)
	a, c := mustEncode(t, b), mustEncode(t, b)
	if !bytes.Equal(a, c) {
		t.Fatal("two encodings of one bundle differ")
	}
	if len(a) == 0 || len(a) > 1500 {
		t.Fatalf("unexpected size %d", len(a))
	}
}

func TestEncodeRunLengthKeepsUniformFilesTiny(t *testing.T) {
	// 10,000 equal-size full segments must not cost per-segment bytes beyond
	// the message-ids themselves.
	b := syntheticBundle(t, 10_000, 716_800)
	raw := mustEncode(t, b)
	idBytes := 0
	for _, id := range b.Pool {
		idBytes += len(id) + 2
	}
	if overhead := len(raw) - idBytes; overhead > 400 {
		t.Fatalf("layout overhead %d bytes for 10k uniform segments", overhead)
	}
}

func TestEncodeRejectsInvalidBundle(t *testing.T) {
	b := goodBundle(t)
	b.Files[0].Size++
	if _, err := Encode(b); !errors.Is(err, ErrSizeMismatch) && !errors.Is(err, ErrFileIDMismatch) && !errors.Is(err, ErrInvalidField) {
		t.Fatalf("Encode of an invalid bundle: %v", err)
	}
	if _, err := Encode(nil); err == nil {
		t.Fatal("Encode(nil) succeeded")
	}
}

func TestDecodeRejectsNonCanonical(t *testing.T) {
	canon := mustEncode(t, goodBundle(t))
	fields := splitTop(t, canon)
	mut := func(f func(fs []rawField) []rawField) []byte {
		cp := append([]rawField(nil), fields...)
		return joinTop(f(cp))
	}
	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{"empty", nil, ErrMalformed},
		{"trailing zero byte", append(append([]byte(nil), canon...), 0), ErrMalformed},
		{"truncated", canon[:len(canon)-3], ErrMalformed},
		{"unknown field", append(append([]byte(nil), canon...), 0xF8, 0x06, 0x01), ErrMalformed},
		{"duplicate version", mut(func(fs []rawField) []rawField {
			return append([]rawField{fs[0]}, fs...)
		}), ErrMalformed},
		{"out of order", mut(func(fs []rawField) []rawField {
			fs[0], fs[1] = fs[1], fs[0]
			return fs
		}), ErrMalformed},
		{"version 2", mut(func(fs []rawField) []rawField {
			fs[0].v = 2
			return fs
		}), ErrUnsupportedVersion},
		{"version 0", mut(func(fs []rawField) []rawField {
			fs[0].v = 0
			return fs
		}), ErrUnsupportedVersion},
		{"unknown scheme", mut(func(fs []rawField) []rawField {
			fs[1].v = 3
			return fs
		}), ErrUnknownScheme},
		{"wrong wire type for scheme", mut(func(fs []rawField) []rawField {
			fs[1] = rawField{num: fs[1].num, typ: protowire.BytesType, b: []byte{1}}
			return fs
		}), ErrMalformed},
		{"short release id", mut(func(fs []rawField) []rawField {
			fs[2].b = fs[2].b[:31]
			return fs
		}), ErrInvalidField},
		{"non-minimal version varint", append([]byte{0x08, 0x81, 0x00}, canon[2:]...), ErrMalformed},
		{"missing title", mut(func(fs []rawField) []rawField {
			return append(fs[:3], fs[4:]...)
		}), ErrMalformed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Decode(tc.raw); !errors.Is(err, tc.want) {
				t.Fatalf("Decode = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeRejectsOversized(t *testing.T) {
	if _, err := Decode(make([]byte, MaxDecompressedSize+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
}

func TestDecodeRejectsHostileCounts(t *testing.T) {
	// A segment table claiming 2^60 positions must be refused before any
	// allocation proportional to the claim.
	canon := mustEncode(t, goodBundle(t))
	fields := splitTop(t, canon)
	patched := false
	for i, f := range fields {
		if f.num != fFiles || patched {
			continue
		}
		file := splitTop(t, f.b)
		for j, ff := range file {
			if ff.num == fSegs {
				seg := splitTop(t, ff.b)
				seg[0].v = 1 << 60
				file[j].b = joinTop(seg)
				patched = true
			}
		}
		fields[i].b = joinTop(file)
	}
	if !patched {
		t.Fatal("no file with a segment table")
	}
	if _, err := Decode(joinTop(fields)); !errors.Is(err, ErrLimit) {
		t.Fatalf("got %v, want ErrLimit", err)
	}
}

func TestComputeID(t *testing.T) {
	raw := mustEncode(t, goodBundle(t))
	id := ComputeID(raw)
	if id == (BundleID{}) || id != ComputeID(raw) {
		t.Fatal("ComputeID unstable or zero")
	}
	raw[len(raw)-1] ^= 1
	if ComputeID(raw) == id {
		t.Fatal("different bytes, same id")
	}
	if len(id.String()) != 64 {
		t.Fatal("bad hex")
	}
}
