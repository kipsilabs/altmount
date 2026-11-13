package contentid

import (
	"encoding/hex"
	"errors"
	"fmt"
)

// IDSize is the length in bytes of a Release ID or File ID.
const IDSize = 32

// ID is a 32-byte SHA-256 identifier (a Release ID or a File ID).
type ID [IDSize]byte

// ErrInvalidID reports a malformed textual identifier.
var ErrInvalidID = errors.New("contentid: invalid id")

// String returns the identifier as 64 lower-case hex characters.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// IsZero reports whether id is the all-zero value (no identifier).
func (id ID) IsZero() bool { return id == ID{} }

// ParseID parses 64 hex characters (either case) into an ID. Anything else,
// including surrounding whitespace or a prefix, is rejected.
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != hex.EncodedLen(IDSize) {
		return id, fmt.Errorf("%w: want %d hex characters, got %d", ErrInvalidID, hex.EncodedLen(IDSize), len(s))
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return ID{}, fmt.Errorf("%w: %v", ErrInvalidID, err)
	}
	return id, nil
}
