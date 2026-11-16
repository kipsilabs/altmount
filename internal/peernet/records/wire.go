package records

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// Kind identifies a record type.
type Kind uint8

const (
	KindRelease     Kind = 1
	KindAttestation Kind = 2
)

func (k Kind) String() string {
	switch k {
	case KindRelease:
		return "release"
	case KindAttestation:
		return "attestation"
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// Identifier types.
type (
	// ReleaseID is the SHA-256 over a release's real segment message-IDs.
	ReleaseID [idSize]byte
	// FileID is the SHA-256 over one file's ordered segment message-IDs.
	FileID [idSize]byte
	// BundleID is the hash of a release bundle (unused in phase 2).
	BundleID [idSize]byte
)

// Field numbers (see the package documentation).
const (
	fVersion   protowire.Number = 1
	fKind      protowire.Number = 2
	fNetwork   protowire.Number = 3
	fRelease   protowire.Number = 4
	fNodeID    protowire.Number = 5
	fPubKey    protowire.Number = 6
	fCreated   protowire.Number = 7
	fExpires   protowire.Number = 8
	fSignature protowire.Number = 30

	fBundle   protowire.Number = 10
	fTitle    protowire.Number = 11
	fSize     protowire.Number = 12
	fFiles    protowire.Number = 13
	fPosted   protowire.Number = 14
	fIMDb     protowire.Number = 15
	fTMDB     protowire.Number = 16
	fTVDB     protowire.Number = 17
	fSeason   protowire.Number = 18
	fEpisode  protowire.Number = 19
	fFile     protowire.Number = 10
	fChecked  protowire.Number = 11
	fTotal    protowire.Number = 12
	fScanned  protowire.Number = 13
	fMissing  protowire.Number = 14
	fHeaderOK protowire.Number = 15
	fBackbone protowire.Number = 16
)

// Header holds the fields common to every record, including the signature.
type Header struct {
	NetworkID    netid.NetworkID
	ReleaseID    ReleaseID
	PublisherID  identity.NodeID
	PublisherKey ed25519.PublicKey
	CreatedAt    time.Time
	ExpiresAt    time.Time
	Signature    []byte
}

// field is one parsed wire field.
type field struct {
	num protowire.Number
	typ protowire.Type
	v   uint64
	b   []byte
}

// parseFields splits b into fields, requiring ascending field numbers. Only
// the field number given as repeated may occur more than once.
func parseFields(b []byte, repeated protowire.Number) ([]field, error) {
	var out []field
	var prev protowire.Number
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, fmt.Errorf("%w: bad tag", ErrMalformed)
		}
		b = b[n:]
		if num < prev || (num == prev && num != repeated) {
			return nil, fmt.Errorf("%w: field %d out of order or duplicated", ErrMalformed, num)
		}
		prev = num
		f := field{num: num, typ: typ}
		switch typ {
		case protowire.VarintType:
			f.v, n = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			f.b, n = protowire.ConsumeBytes(b)
		default:
			return nil, fmt.Errorf("%w: unsupported wire type", ErrMalformed)
		}
		if n < 0 {
			return nil, fmt.Errorf("%w: truncated field %d", ErrMalformed, num)
		}
		b = b[n:]
		out = append(out, f)
	}
	return out, nil
}

func appendBytes(b []byte, num protowire.Number, v []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), v)
}

func appendVarint(b []byte, num protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(b, num, protowire.VarintType), v)
}

func unixOf(t time.Time) uint64 { return uint64(t.Unix()) }

func timeOf(v uint64) (time.Time, error) {
	if v < 1 || v > maxUnixSeconds {
		return time.Time{}, fmt.Errorf("%w: timestamp out of range", ErrInvalidField)
	}
	return time.Unix(int64(v), 0).UTC(), nil
}

func validTime(t time.Time) bool {
	u := t.Unix()
	return u >= 1 && u <= maxUnixSeconds
}

// appendHeader writes fields 1-8.
func appendHeader(b []byte, k Kind, h *Header) []byte {
	b = appendVarint(b, fVersion, SchemaVersion)
	b = appendVarint(b, fKind, uint64(k))
	b = appendBytes(b, fNetwork, h.NetworkID[:])
	b = appendBytes(b, fRelease, h.ReleaseID[:])
	b = appendBytes(b, fNodeID, h.PublisherID[:])
	b = appendBytes(b, fPubKey, h.PublisherKey)
	b = appendVarint(b, fCreated, unixOf(h.CreatedAt))
	return appendVarint(b, fExpires, unixOf(h.ExpiresAt))
}

func appendSignature(b, sig []byte) []byte { return appendBytes(b, fSignature, sig) }

// readFixed copies a bytes field of exactly len(dst) bytes.
func readFixed(f field, dst []byte, name string) error {
	if f.typ != protowire.BytesType {
		return fmt.Errorf("%w: %s wire type", ErrMalformed, name)
	}
	if len(f.b) != len(dst) {
		return fmt.Errorf("%w: %s length", ErrInvalidField, name)
	}
	copy(dst, f.b)
	return nil
}

func readVarint(f field, max uint64, name string) (uint64, error) {
	if f.typ != protowire.VarintType {
		return 0, fmt.Errorf("%w: %s wire type", ErrMalformed, name)
	}
	if f.v > max {
		return 0, fmt.Errorf("%w: %s out of range", ErrInvalidField, name)
	}
	return f.v, nil
}

func readString(f field, name string) (string, error) {
	if f.typ != protowire.BytesType {
		return "", fmt.Errorf("%w: %s wire type", ErrMalformed, name)
	}
	return string(f.b), nil
}

func readTime(f field, name string) (time.Time, error) {
	v, err := readVarint(f, maxUnixSeconds, name)
	if err != nil {
		return time.Time{}, err
	}
	return timeOf(v)
}

// readHeaderField consumes a header field (1-8, 30). It reports whether f was
// one of them.
func readHeaderField(f field, want Kind, h *Header) (bool, error) {
	var err error
	switch f.num {
	case fVersion:
		var v uint64
		if v, err = readVarint(f, ^uint64(0), "version"); err == nil && v != SchemaVersion {
			err = fmt.Errorf("%w: %d", ErrUnsupportedVersion, v)
		}
	case fKind:
		var v uint64
		if v, err = readVarint(f, ^uint64(0), "kind"); err == nil && v != uint64(want) {
			err = fmt.Errorf("%w: %d", ErrWrongKind, v)
		}
	case fNetwork:
		err = readFixed(f, h.NetworkID[:], "network_id")
	case fRelease:
		err = readFixed(f, h.ReleaseID[:], "release_id")
	case fNodeID:
		err = readFixed(f, h.PublisherID[:], "publisher_node_id")
	case fPubKey:
		h.PublisherKey = make(ed25519.PublicKey, publicKeySize)
		err = readFixed(f, h.PublisherKey, "publisher_key")
	case fCreated:
		h.CreatedAt, err = readTime(f, "created_at")
	case fExpires:
		h.ExpiresAt, err = readTime(f, "expires_at")
	case fSignature:
		h.Signature = make([]byte, signatureSize)
		err = readFixed(f, h.Signature, "signature")
	default:
		return false, nil
	}
	return true, err
}

// validate checks header invariants; maxLife bounds expires_at - created_at.
func (h *Header) validate(maxLife time.Duration) error {
	switch {
	case h.NetworkID.IsZero():
		return fmt.Errorf("%w: zero network id", ErrInvalidField)
	case h.ReleaseID == ReleaseID{}:
		return fmt.Errorf("%w: zero release id", ErrInvalidField)
	case h.PublisherID.IsZero():
		return fmt.Errorf("%w: zero publisher id", ErrInvalidField)
	case len(h.PublisherKey) != publicKeySize:
		return fmt.Errorf("%w: publisher key length", ErrInvalidField)
	case identity.ValidatePublicKey(h.PublisherKey) != nil:
		return fmt.Errorf("%w: publisher key is weak", ErrInvalidField)
	case len(h.Signature) != signatureSize:
		return fmt.Errorf("%w: signature length", ErrInvalidField)
	case !validTime(h.CreatedAt) || !validTime(h.ExpiresAt):
		return fmt.Errorf("%w: timestamp out of range", ErrInvalidField)
	case !h.ExpiresAt.After(h.CreatedAt):
		return fmt.Errorf("%w: expiry not after creation", ErrInvalidField)
	case h.ExpiresAt.Sub(h.CreatedAt) > maxLife:
		return fmt.Errorf("%w: lifetime too long", ErrInvalidField)
	}
	return nil
}

// checkBase applies the network, publisher and clock rules that precede any
// type-specific rule.
func (h *Header) checkBase(now time.Time, arrivedIn netid.NetworkID) error {
	if !h.NetworkID.Equal(arrivedIn) {
		return ErrWrongNetwork
	}
	if len(h.PublisherKey) != publicKeySize ||
		!identity.NodeIDFromPublicKey(h.PublisherKey).Equal(h.PublisherID) {
		return ErrNodeIDMismatch
	}
	if h.CreatedAt.After(now.Add(MaxFutureSkew)) {
		return ErrFromFuture
	}
	if !now.Before(h.ExpiresAt) {
		return ErrExpired
	}
	return nil
}

func signingInput(nw netid.NetworkID, body []byte) []byte {
	return append(append(make([]byte, 0, len(nw)+len(body)), nw[:]...), body...)
}

func (h *Header) checkSignature(tag string, body []byte) error {
	if err := identity.Verify(h.PublisherKey, tag, signingInput(h.NetworkID, body), h.Signature); err != nil {
		return ErrBadSignature
	}
	return nil
}

func sign(key *identity.Key, tag string, nw netid.NetworkID, body []byte) ([]byte, error) {
	sig, err := key.Sign(tag, signingInput(nw, body))
	if err != nil {
		return nil, fmt.Errorf("records: sign: %w", err)
	}
	return sig, nil
}

// ContentHash is the SHA-256 of an encoded record.
func ContentHash(encoded []byte) [32]byte { return sha256.Sum256(encoded) }

// newHeader fills the publisher and time fields for a builder.
func newHeader(key *identity.Key, nw netid.NetworkID, rid ReleaseID, now time.Time, life time.Duration) (Header, error) {
	if key == nil {
		return Header{}, fmt.Errorf("%w: nil key", ErrInvalidField)
	}
	created := now.UTC().Truncate(time.Second)
	return Header{
		NetworkID:    nw,
		ReleaseID:    rid,
		PublisherID:  key.NodeID(),
		PublisherKey: key.PublicKey(),
		CreatedAt:    created,
		ExpiresAt:    created.Add(life),
	}, nil
}
