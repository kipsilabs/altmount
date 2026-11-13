// Package netid defines peer-network identifiers.
//
// A network ID is 32 bytes. The open network has a well-known constant ID.
// A private network's ID is derived from its root public key, so the ID is
// bound to the key and nobody else can claim it.
package netid

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
)

// Size is the length of a network ID in bytes.
const Size = sha256.Size

const (
	// openDomain is hashed to produce the open network's constant ID.
	openDomain = "altmount-peernet-network-open-v1"
	// privateDomain prefixes the root public key for private network IDs.
	// It differs from openDomain, and the private preimage is always
	// privateDomain||32-byte key, so no private ID can equal Open().
	privateDomain = "altmount-peernet-network-private-v1\x00"
)

// ErrInvalidNetworkID is returned when a network ID string is malformed.
var ErrInvalidNetworkID = errors.New("netid: invalid network id")

// NetworkID identifies a peer network.
type NetworkID [Size]byte

// String returns 64 lower-case hex characters.
func (n NetworkID) String() string { return hex.EncodeToString(n[:]) }

// IsZero reports whether the ID is all zero.
func (n NetworkID) IsZero() bool { return n == NetworkID{} }

// Equal compares in constant time.
func (n NetworkID) Equal(o NetworkID) bool {
	return subtle.ConstantTimeCompare(n[:], o[:]) == 1
}

// IsHexID reports whether s is exactly Size bytes of hex. With lowerOnly set
// upper-case digits are refused. It is the single hex-ID check shared by the
// network, API and database layers.
func IsHexID(s string, lowerOnly bool) bool {
	if len(s) != hex.EncodedLen(Size) {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (lowerOnly || c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// ParseNetworkID accepts exactly 64 lower-case hex characters.
func ParseNetworkID(s string) (NetworkID, error) {
	var id NetworkID
	if !IsHexID(s, true) {
		return id, ErrInvalidNetworkID
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return NetworkID{}, ErrInvalidNetworkID
	}
	return id, nil
}

// Open returns the open network's well-known ID: SHA-256 of a fixed domain
// string.
func Open() NetworkID { return NetworkID(sha256.Sum256([]byte(openDomain))) }

// IsOpen reports whether the ID is the open network.
func (n NetworkID) IsOpen() bool { return n.Equal(Open()) }

// FromRootKey returns the ID of the private network rooted at pub.
func FromRootKey(pub ed25519.PublicKey) NetworkID {
	var b bytes.Buffer
	b.WriteString(privateDomain)
	b.Write(pub)
	return NetworkID(sha256.Sum256(b.Bytes()))
}

// MarshalText implements encoding.TextMarshaler.
func (n NetworkID) MarshalText() ([]byte, error) { return []byte(n.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler with strict parsing.
func (n *NetworkID) UnmarshalText(b []byte) error {
	id, err := ParseNetworkID(string(b))
	if err != nil {
		return err
	}
	*n = id
	return nil
}
