package identity

import (
	"crypto/ed25519"
	"errors"
)

const (
	signaturePrefix = "altmount-peernet-sig-v1\x00"
	maxTagLen       = 255
)

// ErrInvalidTag is returned when a domain-separation tag is empty or too long.
var ErrInvalidTag = errors.New("identity: invalid signature tag")

// ErrBadSignature is returned when a signature does not verify.
var ErrBadSignature = errors.New("identity: signature verification failed")

// signedMessage builds prefix || len(tag) || tag || msg. The length byte makes
// the tag/message boundary unambiguous, and the prefix keeps these signatures
// distinct from any other use of the key (such as TLS handshake signatures).
func signedMessage(tag string, msg []byte) ([]byte, error) {
	if tag == "" || len(tag) > maxTagLen {
		return nil, ErrInvalidTag
	}
	out := make([]byte, 0, len(signaturePrefix)+1+len(tag)+len(msg))
	out = append(out, signaturePrefix...)
	out = append(out, byte(len(tag)))
	out = append(out, tag...)
	out = append(out, msg...)
	return out, nil
}

// Sign signs msg under a mandatory domain-separation tag. A signature made
// with one tag never verifies under another.
func (k *Key) Sign(tag string, msg []byte) ([]byte, error) {
	m, err := signedMessage(tag, msg)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(k.m.priv, m), nil
}

// Verify checks a signature produced by Sign with the same tag.
func Verify(pub ed25519.PublicKey, tag string, msg, sig []byte) error {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return ErrBadSignature
	}
	if ValidatePublicKey(pub) != nil {
		return ErrBadSignature
	}
	m, err := signedMessage(tag, msg)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, m, sig) {
		return ErrBadSignature
	}
	return nil
}
