package records

import (
	"bytes"
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// fieldNumbers lists the field numbers of an encoded record in order.
func fieldNumbers(t *testing.T, b []byte) []protowire.Number {
	t.Helper()
	fs, err := parseFields(b, fBackbone)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]protowire.Number, len(fs))
	for i, f := range fs {
		out[i] = f.num
	}
	return out
}

func has(nums []protowire.Number, n protowire.Number) bool {
	for _, x := range nums {
		if x == n {
			return true
		}
	}
	return false
}

// withSchemeField inserts field 9 with value v after field 8 and re-signs
// nothing: used to build hostile encodings.
func withSchemeField(t testing.TB, enc []byte, v uint64) []byte {
	t.Helper()
	var out []byte
	rest := enc
	inserted := false
	for len(rest) > 0 {
		num, typ, n := protowire.ConsumeTag(rest)
		m := protowire.ConsumeFieldValue(num, typ, rest[n:])
		if num > fExpires && !inserted {
			out = protowire.AppendVarint(protowire.AppendTag(out, 9, protowire.VarintType), v)
			inserted = true
		}
		out = append(out, rest[:n+m]...)
		rest = rest[n+m:]
	}
	return out
}

func TestScheme_AbsentIsNZBAndEncodingUnchanged(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	rec, enc := mustRelease(t, k, nw)
	if has(fieldNumbers(t, enc), 9) {
		t.Fatal("nzb scheme must not add a field: records of the released encoding must stay byte-identical")
	}
	if rec.Scheme() != SchemeNZB {
		t.Fatalf("scheme %d, want nzb", rec.Scheme())
	}
	in := releaseInput()
	in.ReleaseScheme = SchemeNZB
	_, explicit, err := BuildReleaseRecord(k, nw, in, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if has(fieldNumbers(t, explicit), 9) {
		t.Fatal("explicit nzb scheme must still be absent from the encoding")
	}
	got, err := DecodeReleaseRecord(enc)
	if err != nil || got.Scheme() != SchemeNZB {
		t.Fatalf("decode: %v scheme %v", err, got)
	}
	_, att := mustAttestation(t, k, nw)
	if has(fieldNumbers(t, att), 9) {
		t.Fatal("attestation default must not carry field 9")
	}
}

func TestScheme_FilesRoundTripBothTypes(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	rin := releaseInput()
	rin.ReleaseScheme = SchemeFiles
	rec, enc, err := BuildReleaseRecord(k, nw, rin, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if !has(fieldNumbers(t, enc), 9) {
		t.Fatal("files scheme must be encoded in field 9")
	}
	got, err := DecodeReleaseRecord(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Scheme() != SchemeFiles || rec.Scheme() != SchemeFiles {
		t.Fatalf("scheme %d / %d", got.Scheme(), rec.Scheme())
	}
	if err := got.Verify(testNow, nw); err != nil {
		t.Fatal(err)
	}
	if again, err := got.Encode(); err != nil || !bytes.Equal(again, enc) {
		t.Fatalf("re-encode differs: %v", err)
	}
	if env, err := Peek(enc); err != nil || env.ReleaseScheme != SchemeFiles {
		t.Fatalf("peek: %v %+v", err, env)
	}

	ain := attestationInput()
	ain.ReleaseScheme = SchemeFiles
	_, aenc, err := BuildHealthAttestation(k, nw, ain, testNow)
	if err != nil {
		t.Fatal(err)
	}
	a, err := DecodeHealthAttestation(aenc)
	if err != nil || a.Scheme() != SchemeFiles {
		t.Fatalf("attestation: %v", err)
	}
	if err := a.Verify(testNow, nw); err != nil {
		t.Fatal(err)
	}
	if again, err := a.Encode(); err != nil || !bytes.Equal(again, aenc) {
		t.Fatalf("attestation re-encode differs: %v", err)
	}
}

func TestScheme_AnyOtherValueRejected(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	_, enc := mustRelease(t, k, nw)
	_, att := mustAttestation(t, k, nw)
	for _, v := range []uint64{0, 1, 3, 255, 1 << 40} {
		if _, err := DecodeReleaseRecord(withSchemeField(t, enc, v)); err == nil {
			t.Fatalf("release record with scheme %d accepted", v)
		}
		if _, err := DecodeHealthAttestation(withSchemeField(t, att, v)); err == nil {
			t.Fatalf("attestation with scheme %d accepted", v)
		}
	}
	bad := releaseInput()
	bad.ReleaseScheme = 3
	if _, _, err := BuildReleaseRecord(k, nw, bad, testNow); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("build with scheme 3: %v", err)
	}
	badA := attestationInput()
	badA.ReleaseScheme = 7
	if _, _, err := BuildHealthAttestation(k, nw, badA, testNow); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("build attestation with scheme 7: %v", err)
	}
}

func TestScheme_CoveredBySignature(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	// A signed nzb-scheme record with field 9 = 2 spliced in must not verify.
	_, enc := mustRelease(t, k, nw)
	forged, err := DecodeReleaseRecord(withSchemeField(t, enc, 2))
	if err != nil {
		t.Fatalf("spliced record should decode (canonical): %v", err)
	}
	if err := forged.Verify(testNow, nw); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("scheme spliced into a signed record verified: %v", err)
	}
	// And removing the field from a files-scheme record must not verify either.
	in := releaseInput()
	in.ReleaseScheme = SchemeFiles
	rec, fenc, err := BuildReleaseRecord(k, nw, in, testNow)
	if err != nil {
		t.Fatal(err)
	}
	stripped := *rec
	stripped.ReleaseScheme = 0
	stripped.Signature = rec.Signature
	if err := stripped.Verify(testNow, nw); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("scheme stripped from a signed record verified: %v", err)
	}
	_ = fenc
	// The same for attestations.
	_, att := mustAttestation(t, k, nw)
	fa, err := DecodeHealthAttestation(withSchemeField(t, att, 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := fa.Verify(testNow, nw); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("scheme spliced into a signed attestation verified: %v", err)
	}
}
