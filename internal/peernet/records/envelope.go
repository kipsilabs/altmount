package records

import (
	"fmt"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// Envelope is the indexable summary of an encoded record.
type Envelope struct {
	Kind        Kind
	NetworkID   netid.NetworkID
	ReleaseID   ReleaseID
	FileID      FileID // zero for release records
	PublisherID identity.NodeID
	CreatedAt   time.Time
	ExpiresAt   time.Time
	ContentHash [32]byte
}

// Peek strictly decodes an encoded record of either kind and returns its
// envelope. It does not verify the signature or clock: callers must call
// Verify on the decoded record before trusting anything.
func Peek(b []byte) (Envelope, error) {
	if len(b) > MaxEncodedSize {
		return Envelope{}, ErrTooLarge
	}
	fields, err := parseFields(b, fBackbone)
	if err != nil {
		return Envelope{}, err
	}
	var kind uint64
	for _, f := range fields {
		if f.num == fKind {
			kind = f.v
		}
	}
	switch Kind(kind) {
	case KindRelease:
		r, err := DecodeReleaseRecord(b)
		if err != nil {
			return Envelope{}, err
		}
		return envelopeOf(KindRelease, &r.Header, FileID{}, b), nil
	case KindAttestation:
		a, err := DecodeHealthAttestation(b)
		if err != nil {
			return Envelope{}, err
		}
		return envelopeOf(KindAttestation, &a.Header, a.FileID, b), nil
	}
	return Envelope{}, fmt.Errorf("%w: unknown kind", ErrMalformed)
}

func envelopeOf(k Kind, h *Header, file FileID, b []byte) Envelope {
	return Envelope{
		Kind: k, NetworkID: h.NetworkID, ReleaseID: h.ReleaseID, FileID: file,
		PublisherID: h.PublisherID, CreatedAt: h.CreatedAt, ExpiresAt: h.ExpiresAt,
		ContentHash: ContentHash(b),
	}
}
