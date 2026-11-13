package identity

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
)

// ErrWeakPublicKey is returned for a public key that is the wrong size,
// non-canonically encoded or of small order.
var ErrWeakPublicKey = errors.New("identity: weak or non-canonical public key")

// smallOrderKeys are the canonical encodings of the eight Ed25519 points of
// order dividing 8, plus the two encodings that spell x = 0 with the sign bit
// set (the identity and the point of order 2). Go's ed25519.Verify accepts
// them as public keys, and with the identity point a signature R = A,
// S = 0 verifies for every message without any private key.
var smallOrderKeys = func() map[[32]byte]struct{} {
	m := map[[32]byte]struct{}{}
	for _, h := range []string{
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
	} {
		b, err := hex.DecodeString(h)
		if err != nil || len(b) != 32 {
			panic("identity: bad small-order table")
		}
		m[[32]byte(b)] = struct{}{}
	}
	return m
}()

// ValidatePublicKey rejects public keys that must never identify a node or
// sign a record: wrong length, a y coordinate not below p = 2^255-19
// (non-canonical encoding) and the small-order points.
func ValidatePublicKey(pub []byte) error {
	if len(pub) != ed25519.PublicKeySize {
		return ErrWeakPublicKey
	}
	if nonCanonicalY(pub) {
		return ErrWeakPublicKey
	}
	if _, bad := smallOrderKeys[[32]byte(pub)]; bad {
		return ErrWeakPublicKey
	}
	return nil
}

// nonCanonicalY reports y >= p for the little-endian y in the low 255 bits.
func nonCanonicalY(b []byte) bool {
	if b[31]&0x7f != 0x7f {
		return false
	}
	for i := 1; i < 31; i++ {
		if b[i] != 0xff {
			return false
		}
	}
	return b[0] >= 0xed
}
