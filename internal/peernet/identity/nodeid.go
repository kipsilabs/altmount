// Package identity provides the peer-network node identity: an Ed25519 key
// kept in an owner-only file, the Node ID derived from it, domain-separated
// signatures, and the self-signed certificate used for mutual TLS.
//
// A certificate carries exactly one meaning: "the holder of this certificate
// owns this key". There is no certificate authority and no chain.
package identity

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
)

// NodeIDSize is the length of a Node ID in bytes.
const NodeIDSize = sha256.Size

// ErrInvalidNodeID is returned when a Node ID string is malformed.
var ErrInvalidNodeID = errors.New("identity: invalid node id")

// NodeID is the SHA-256 of a node's raw Ed25519 public key.
type NodeID [NodeIDSize]byte

// NodeIDFromPublicKey derives the Node ID of a public key.
func NodeIDFromPublicKey(pub ed25519.PublicKey) NodeID {
	return NodeID(sha256.Sum256(pub))
}

// ParseNodeID parses the canonical form produced by String: exactly 64
// lower-case hexadecimal characters. Anything else is rejected.
func ParseNodeID(s string) (NodeID, error) {
	var id NodeID
	if len(s) != hex.EncodedLen(NodeIDSize) {
		return id, ErrInvalidNodeID
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return id, ErrInvalidNodeID
		}
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return NodeID{}, ErrInvalidNodeID
	}
	return id, nil
}

// String returns the lower-case hex form.
func (n NodeID) String() string { return hex.EncodeToString(n[:]) }

// Equal compares in constant time. Use it wherever a Node ID is being
// authenticated rather than ==.
func (n NodeID) Equal(o NodeID) bool {
	return subtle.ConstantTimeCompare(n[:], o[:]) == 1
}

// IsZero reports whether the ID is the all-zero value.
func (n NodeID) IsZero() bool { return n == NodeID{} }

// MarshalText implements encoding.TextMarshaler.
func (n NodeID) MarshalText() ([]byte, error) { return []byte(n.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler with strict parsing.
func (n *NodeID) UnmarshalText(b []byte) error {
	id, err := ParseNodeID(string(b))
	if err != nil {
		return fmt.Errorf("%w", err)
	}
	*n = id
	return nil
}
