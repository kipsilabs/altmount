package identity

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"testing"
)

// weakKeyVectors are the eight small-order point encodings plus the
// non-canonical variants Go's decoder also accepts.
var weakKeyVectors = []string{
	"0100000000000000000000000000000000000000000000000000000000000000",
	"0100000000000000000000000000000000000000000000000000000000000080",
	"ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	"ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	"0000000000000000000000000000000000000000000000000000000000000000",
	"0000000000000000000000000000000000000000000000000000000000000080",
	"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a",
	"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac03fa",
	"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05",
	"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc85",
	// non-canonical: y = p, p+1 (with and without the sign bit)
	"edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	"eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	"edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	"eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
}

func TestVerifyRejectsForgedIdentityPointSignature(t *testing.T) {
	pub, _ := hex.DecodeString(weakKeyVectors[0])
	sig := make([]byte, ed25519.SignatureSize)
	copy(sig, pub) // R = identity point, S = 0
	msg, err := signedMessage("tag", []byte("anything"))
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(pub, msg, sig) {
		t.Skip("stdlib no longer accepts the forgery; nothing to guard")
	}
	if err := Verify(pub, "tag", []byte("anything"), sig); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("forged identity-point signature verified: %v", err)
	}
}

func TestValidatePublicKeyRejectsWeakKeys(t *testing.T) {
	for _, h := range weakKeyVectors {
		pub, _ := hex.DecodeString(h)
		if err := ValidatePublicKey(pub); !errors.Is(err, ErrWeakPublicKey) {
			t.Errorf("%s accepted: %v", h, err)
		}
		sig := make([]byte, ed25519.SignatureSize)
		if err := Verify(pub, "t", nil, sig); err == nil {
			t.Errorf("%s verified", h)
		}
	}
}

func TestValidatePublicKeyAcceptsNormalKeys(t *testing.T) {
	for i := 0; i < 50; i++ {
		k, err := GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidatePublicKey(k.PublicKey()); err != nil {
			t.Fatalf("normal key rejected: %v", err)
		}
	}
	if err := ValidatePublicKey(make([]byte, 5)); !errors.Is(err, ErrWeakPublicKey) {
		t.Fatalf("short key accepted: %v", err)
	}
}
