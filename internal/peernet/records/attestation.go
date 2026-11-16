package records

import (
	"bytes"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// HealthAttestation states that a file was checked and what was found.
type HealthAttestation struct {
	Header
	FileID              FileID
	CheckedAt           time.Time
	TotalSegments       uint32
	SegmentsChecked     uint32
	MissingCount        uint32 // 0 means healthy
	MediaHeaderVerified bool
	Backbones           []string // sorted, unique, lower-case
}

// Healthy reports whether no segment was found missing.
func (a *HealthAttestation) Healthy() bool { return a.MissingCount == 0 }

func (a *HealthAttestation) body() []byte {
	b := appendHeader(nil, KindAttestation, &a.Header)
	b = appendBytes(b, fFile, a.FileID[:])
	b = appendVarint(b, fChecked, unixOf(a.CheckedAt))
	b = appendVarint(b, fTotal, uint64(a.TotalSegments))
	b = appendVarint(b, fScanned, uint64(a.SegmentsChecked))
	b = appendVarint(b, fMissing, uint64(a.MissingCount))
	var ok uint64
	if a.MediaHeaderVerified {
		ok = 1
	}
	b = appendVarint(b, fHeaderOK, ok)
	for _, l := range a.Backbones {
		b = appendBytes(b, fBackbone, []byte(l))
	}
	return b
}

func (a *HealthAttestation) marshal() []byte { return appendSignature(a.body(), a.Signature) }

// Validate checks every field rule. It does not check the signature or clock.
func (a *HealthAttestation) Validate() error {
	if err := a.validate(AttestationValidity); err != nil {
		return err
	}
	switch {
	case a.FileID == FileID{}:
		return fmt.Errorf("%w: zero file id", ErrInvalidField)
	case !validTime(a.CheckedAt):
		return fmt.Errorf("%w: checked_at", ErrInvalidField)
	case a.TotalSegments < 1 || a.TotalSegments > MaxSegments:
		return fmt.Errorf("%w: total segments", ErrInvalidField)
	case a.SegmentsChecked < 1 || a.SegmentsChecked > a.TotalSegments:
		return fmt.Errorf("%w: segments checked", ErrInvalidField)
	case a.MissingCount > a.TotalSegments:
		return fmt.Errorf("%w: missing count", ErrInvalidField)
	}
	return checkBackbones(a.Backbones)
}

// Encode validates and returns the canonical signed encoding.
func (a *HealthAttestation) Encode() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	b := a.marshal()
	if len(b) > MaxEncodedSize {
		return nil, ErrTooLarge
	}
	return b, nil
}

// Verify applies trust rule 6 for an attestation that arrived in arrivedIn.
func (a *HealthAttestation) Verify(now time.Time, arrivedIn netid.NetworkID) error {
	if err := a.checkBase(now, arrivedIn); err != nil {
		return err
	}
	if a.CheckedAt.After(a.CreatedAt) {
		return ErrCheckedAfterCreated
	}
	if now.Sub(a.CheckedAt) > AttestationValidity {
		return ErrStale
	}
	if err := a.checkSignature(attestationTag, a.body()); err != nil {
		return err
	}
	return a.Validate()
}

// BuildHealthAttestation signs an attestation with key. Header fields of in
// are ignored; expiry is AttestationValidity after creation. Backbone labels
// are trimmed, lower-cased, sorted and de-duplicated on a copy.
func BuildHealthAttestation(key *identity.Key, nw netid.NetworkID, in HealthAttestation, now time.Time) (*HealthAttestation, []byte, error) {
	h, err := newHeader(key, nw, in.ReleaseID, now, AttestationValidity)
	if err != nil {
		return nil, nil, err
	}
	a := in
	a.Header = h
	a.CheckedAt = in.CheckedAt.UTC().Truncate(time.Second)
	a.Backbones = normaliseBackbones(in.Backbones)
	a.Signature, err = sign(key, attestationTag, nw, a.body())
	if err != nil {
		return nil, nil, err
	}
	if err := a.checkBase(now, nw); err != nil {
		return nil, nil, err
	}
	if a.CheckedAt.After(a.CreatedAt) {
		return nil, nil, ErrCheckedAfterCreated
	}
	if err := a.Verify(now, nw); err != nil {
		return nil, nil, err
	}
	enc, err := a.Encode()
	if err != nil {
		return nil, nil, err
	}
	return &a, enc, nil
}

// DecodeHealthAttestation strictly decodes an attestation. It does not verify
// the signature or clock; call Verify.
func DecodeHealthAttestation(b []byte) (*HealthAttestation, error) {
	if len(b) > MaxEncodedSize {
		return nil, ErrTooLarge
	}
	fields, err := parseFields(b, fBackbone)
	if err != nil {
		return nil, err
	}
	a := &HealthAttestation{}
	var seen uint64
	for _, f := range fields {
		seen |= 1 << f.num
		if ok, err := readHeaderField(f, KindAttestation, &a.Header); ok || err != nil {
			if err != nil {
				return nil, err
			}
			continue
		}
		if err := a.readField(f); err != nil {
			return nil, err
		}
	}
	const required = 1<<fVersion | 1<<fKind | 1<<fNetwork | 1<<fRelease | 1<<fNodeID | 1<<fPubKey |
		1<<fCreated | 1<<fExpires | 1<<fFile | 1<<fChecked | 1<<fTotal | 1<<fScanned | 1<<fMissing |
		1<<fHeaderOK | 1<<fSignature
	if seen&required != required {
		return nil, fmt.Errorf("%w: missing required field", ErrMalformed)
	}
	if err := a.Validate(); err != nil {
		return nil, err
	}
	if !bytes.Equal(a.marshal(), b) {
		return nil, fmt.Errorf("%w: not canonical", ErrMalformed)
	}
	return a, nil
}

func (a *HealthAttestation) readField(f field) error {
	var err error
	var v uint64
	switch f.num {
	case fFile:
		err = readFixed(f, a.FileID[:], "file_id")
	case fChecked:
		a.CheckedAt, err = readTime(f, "checked_at")
	case fTotal:
		v, err = readVarint(f, MaxSegments, "total_segments")
		a.TotalSegments = uint32(v)
	case fScanned:
		v, err = readVarint(f, MaxSegments, "segments_checked")
		a.SegmentsChecked = uint32(v)
	case fMissing:
		v, err = readVarint(f, MaxSegments, "missing_count")
		a.MissingCount = uint32(v)
	case fHeaderOK:
		v, err = readVarint(f, 1, "media_header_ok")
		a.MediaHeaderVerified = v == 1
	case fBackbone:
		if f.typ != protowire.BytesType {
			return fmt.Errorf("%w: backbone wire type", ErrMalformed)
		}
		a.Backbones = append(a.Backbones, string(f.b))
	default:
		return fmt.Errorf("%w: unknown field %d", ErrMalformed, f.num)
	}
	return err
}
