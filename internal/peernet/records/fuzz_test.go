package records

import (
	"bytes"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func FuzzDecodeReleaseRecord(f *testing.F) {
	k := testKey(f)
	_, enc := mustRelease(f, k, netid.Open())
	_, att := mustAttestation(f, k, netid.Open())
	f.Add(enc)
	f.Add(att)
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01})
	f.Add(enc[:len(enc)/2])
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := DecodeReleaseRecord(b)
		if err != nil {
			return
		}
		again, err := r.Encode()
		if err != nil {
			t.Fatalf("decoded record does not re-encode: %v", err)
		}
		if !bytes.Equal(again, b) {
			t.Fatalf("re-encode differs:\n in %x\nout %x", b, again)
		}
		_ = r.Verify(testNow, netid.Open())
		if _, err := Peek(b); err != nil {
			t.Fatalf("peek rejects decodable record: %v", err)
		}
	})
}

func FuzzDecodeHealthAttestation(f *testing.F) {
	k := testKey(f)
	_, enc := mustRelease(f, k, netid.Open())
	_, att := mustAttestation(f, k, netid.Open())
	f.Add(att)
	f.Add(enc)
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01, 0x10, 0x02})
	f.Add(att[:len(att)/2])
	f.Fuzz(func(t *testing.T, b []byte) {
		a, err := DecodeHealthAttestation(b)
		if err != nil {
			return
		}
		again, err := a.Encode()
		if err != nil {
			t.Fatalf("decoded record does not re-encode: %v", err)
		}
		if !bytes.Equal(again, b) {
			t.Fatalf("re-encode differs:\n in %x\nout %x", b, again)
		}
		_ = a.Verify(testNow, netid.Open())
		if _, err := Peek(b); err != nil {
			t.Fatalf("peek rejects decodable record: %v", err)
		}
	})
}
