package recsync

import (
	"bytes"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// ErrMalformedWire is returned for any undecodable, non-canonical or oversized
// page, push or push result.
var ErrMalformedWire = errors.New("recsync: malformed message")

// Wire layout (protobuf wire format, canonical: fields in this order, none
// repeated except records, none unknown):
//
//	page:        1 version=1, 2 epoch (bytes), 3 highest seq (varint),
//	             4 more (0/1), 5 record (bytes, repeated, raw encoded record)
//	push:        1 version=1, 5 record (repeated)
//	push result: 1 version=1, 2 accepted, 3 duplicate, 4 rejected (varints)
const (
	wireVersion = 1

	fVersion  protowire.Number = 1
	fEpoch    protowire.Number = 2
	fHighest  protowire.Number = 3
	fMore     protowire.Number = 4
	fRecord   protowire.Number = 5
	fAccepted protowire.Number = 2
	fDup      protowire.Number = 3
	fRejected protowire.Number = 4

	maxCount = 1 << 16
)

// Page is one page of a peer's records.
type Page struct {
	// Epoch identifies the sender's sequence space for the network (lower-case
	// alphanumeric, 16 to 64 bytes). A change means the sequence restarted.
	Epoch string
	// HighestSeq is the highest seq the sender scanned for this page (expired
	// rows included); it is the next cursor.
	HighestSeq int64
	// More reports that rows beyond this page exist.
	More bool
	// Records are raw encoded records, each 1 to records.MaxEncodedSize bytes.
	Records [][]byte
}

// PushResult is the answer to a push. It never echoes record content.
type PushResult struct {
	Accepted, Duplicate, Rejected int
}

func validEpoch(s string) bool {
	if len(s) < minEpochLen || len(s) > maxEpochLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}

func validRecords(recs [][]byte, max int) error {
	if len(recs) > max {
		return fmt.Errorf("%w: %d records", ErrMalformedWire, len(recs))
	}
	for _, r := range recs {
		if len(r) == 0 || len(r) > records.MaxEncodedSize {
			return fmt.Errorf("%w: record size", ErrMalformedWire)
		}
	}
	return nil
}

func appendRecords(b []byte, recs [][]byte) []byte {
	for _, r := range recs {
		b = protowire.AppendTag(b, fRecord, protowire.BytesType)
		b = protowire.AppendBytes(b, r)
	}
	return b
}

func appendVarintField(b []byte, n protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, n, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

// EncodePage encodes p, validating it.
func EncodePage(p Page) ([]byte, error) {
	if !validEpoch(p.Epoch) || p.HighestSeq < 0 {
		return nil, fmt.Errorf("%w: page header", ErrMalformedWire)
	}
	if err := validRecords(p.Records, PageRecords); err != nil {
		return nil, err
	}
	b := appendVarintField(nil, fVersion, wireVersion)
	b = protowire.AppendTag(b, fEpoch, protowire.BytesType)
	b = protowire.AppendString(b, p.Epoch)
	b = appendVarintField(b, fHighest, uint64(p.HighestSeq))
	var more uint64
	if p.More {
		more = 1
	}
	b = appendVarintField(b, fMore, more)
	return appendRecords(b, p.Records), nil
}

// wireField is one parsed top-level field.
type wireField struct {
	num protowire.Number
	v   uint64 // varint value
	b   []byte // bytes value (aliases the input)
}

// parseWire splits b into fields, allowing only the listed numbers, each with
// its expected wire type, at most maxRecords record fields. It does no
// semantic checks.
func parseWire(b []byte, allowed map[protowire.Number]protowire.Type, maxRecords int) ([]wireField, error) {
	var out []wireField
	nrec := 0
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, fmt.Errorf("%w: tag", ErrMalformedWire)
		}
		b = b[n:]
		want, ok := allowed[num]
		if !ok || want != typ {
			return nil, fmt.Errorf("%w: field %d", ErrMalformedWire, num)
		}
		f := wireField{num: num}
		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return nil, fmt.Errorf("%w: varint", ErrMalformedWire)
			}
			f.v, b = v, b[n:]
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return nil, fmt.Errorf("%w: bytes", ErrMalformedWire)
			}
			f.b, b = v, b[n:]
			if num == fRecord {
				if nrec++; nrec > maxRecords {
					return nil, fmt.Errorf("%w: too many records", ErrMalformedWire)
				}
			}
		}
		out = append(out, f)
	}
	return out, nil
}

var pageFields = map[protowire.Number]protowire.Type{
	fVersion: protowire.VarintType, fEpoch: protowire.BytesType, fHighest: protowire.VarintType,
	fMore: protowire.VarintType, fRecord: protowire.BytesType,
}

// DecodePage strictly decodes a page: a page that does not re-encode to the
// same bytes is rejected.
func DecodePage(b []byte) (Page, error) {
	if len(b) > MaxPageBytes {
		return Page{}, fmt.Errorf("%w: too large", ErrMalformedWire)
	}
	fields, err := parseWire(b, pageFields, PageRecords)
	if err != nil {
		return Page{}, err
	}
	var p Page
	var version uint64
	for _, f := range fields {
		switch f.num {
		case fVersion:
			version = f.v
		case fEpoch:
			p.Epoch = string(f.b)
		case fHighest:
			if f.v > 1<<62 {
				return Page{}, fmt.Errorf("%w: seq", ErrMalformedWire)
			}
			p.HighestSeq = int64(f.v)
		case fMore:
			if f.v > 1 {
				return Page{}, fmt.Errorf("%w: more", ErrMalformedWire)
			}
			p.More = f.v == 1
		case fRecord:
			p.Records = append(p.Records, append([]byte(nil), f.b...))
		}
	}
	if version != wireVersion {
		return Page{}, fmt.Errorf("%w: version", ErrMalformedWire)
	}
	again, err := EncodePage(p)
	if err != nil || !bytes.Equal(again, b) {
		return Page{}, fmt.Errorf("%w: not canonical", ErrMalformedWire)
	}
	return p, nil
}

// EncodePush encodes a push of 1 to MaxPushRecords raw records.
func EncodePush(recs [][]byte) ([]byte, error) {
	if len(recs) == 0 {
		return nil, fmt.Errorf("%w: empty push", ErrMalformedWire)
	}
	if err := validRecords(recs, MaxPushRecords); err != nil {
		return nil, err
	}
	return appendRecords(appendVarintField(nil, fVersion, wireVersion), recs), nil
}

var pushFields = map[protowire.Number]protowire.Type{
	fVersion: protowire.VarintType, fRecord: protowire.BytesType,
}

// DecodePush strictly decodes a push request body.
func DecodePush(b []byte) ([][]byte, error) {
	if len(b) > MaxPushBody {
		return nil, fmt.Errorf("%w: too large", ErrMalformedWire)
	}
	fields, err := parseWire(b, pushFields, MaxPushRecords)
	if err != nil {
		return nil, err
	}
	var recs [][]byte
	var version uint64
	for _, f := range fields {
		switch f.num {
		case fVersion:
			version = f.v
		case fRecord:
			recs = append(recs, append([]byte(nil), f.b...))
		}
	}
	if version != wireVersion {
		return nil, fmt.Errorf("%w: version", ErrMalformedWire)
	}
	again, err := EncodePush(recs)
	if err != nil || !bytes.Equal(again, b) {
		return nil, fmt.Errorf("%w: not canonical", ErrMalformedWire)
	}
	return recs, nil
}

// EncodePushResult encodes r (counts are clamped to a sane maximum).
func EncodePushResult(r PushResult) []byte {
	b := appendVarintField(nil, fVersion, wireVersion)
	b = appendVarintField(b, fAccepted, uint64(clampCount(r.Accepted)))
	b = appendVarintField(b, fDup, uint64(clampCount(r.Duplicate)))
	return appendVarintField(b, fRejected, uint64(clampCount(r.Rejected)))
}

func clampCount(n int) int { return max(0, min(n, maxCount)) }

var resultFields = map[protowire.Number]protowire.Type{
	fVersion: protowire.VarintType, fAccepted: protowire.VarintType, fDup: protowire.VarintType, fRejected: protowire.VarintType,
}

// DecodePushResult strictly decodes a push response body.
func DecodePushResult(b []byte) (PushResult, error) {
	if len(b) > MaxPushResultBytes {
		return PushResult{}, fmt.Errorf("%w: too large", ErrMalformedWire)
	}
	fields, err := parseWire(b, resultFields, 0)
	if err != nil {
		return PushResult{}, err
	}
	var r PushResult
	var version uint64
	for _, f := range fields {
		if f.v > maxCount && f.num != fVersion {
			return PushResult{}, fmt.Errorf("%w: count", ErrMalformedWire)
		}
		switch f.num {
		case fVersion:
			version = f.v
		case fAccepted:
			r.Accepted = int(f.v)
		case fDup:
			r.Duplicate = int(f.v)
		case fRejected:
			r.Rejected = int(f.v)
		}
	}
	if version != wireVersion || !bytes.Equal(EncodePushResult(r), b) {
		return PushResult{}, fmt.Errorf("%w: not canonical", ErrMalformedWire)
	}
	return r, nil
}
