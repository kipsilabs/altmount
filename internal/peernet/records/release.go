package records

import (
	"bytes"
	"fmt"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// MediaIDs are optional external identifiers. Zero values mean absent, except
// Season and Episode where nil means absent (0 is a real value).
type MediaIDs struct {
	IMDb    string
	TMDB    uint32
	TVDB    uint32
	Season  *uint32
	Episode *uint32
}

// ReleaseRecord announces that a release exists. It is a hint, never an import.
type ReleaseRecord struct {
	Header
	BundleID  BundleID // zero when the publisher serves no bundle
	Title     string
	TotalSize int64
	FileCount uint32
	PostDate  time.Time // zero when unknown
	Media     MediaIDs
}

func (r *ReleaseRecord) body() []byte {
	b := appendHeader(nil, KindRelease, &r.Header)
	if r.BundleID != (BundleID{}) {
		b = appendBytes(b, fBundle, r.BundleID[:])
	}
	b = appendBytes(b, fTitle, []byte(r.Title))
	b = appendVarint(b, fSize, uint64(r.TotalSize))
	b = appendVarint(b, fFiles, uint64(r.FileCount))
	if !r.PostDate.IsZero() {
		b = appendVarint(b, fPosted, unixOf(r.PostDate))
	}
	if r.Media.IMDb != "" {
		b = appendBytes(b, fIMDb, []byte(r.Media.IMDb))
	}
	if r.Media.TMDB != 0 {
		b = appendVarint(b, fTMDB, uint64(r.Media.TMDB))
	}
	if r.Media.TVDB != 0 {
		b = appendVarint(b, fTVDB, uint64(r.Media.TVDB))
	}
	if r.Media.Season != nil {
		b = appendVarint(b, fSeason, uint64(*r.Media.Season))
	}
	if r.Media.Episode != nil {
		b = appendVarint(b, fEpisode, uint64(*r.Media.Episode))
	}
	return b
}

// marshal encodes without validating.
func (r *ReleaseRecord) marshal() []byte { return appendSignature(r.body(), r.Signature) }

// Validate checks every field rule. It does not check the signature or clock.
func (r *ReleaseRecord) Validate() error {
	if err := r.validate(RecordLifetime); err != nil {
		return err
	}
	if err := checkText("title", r.Title, MaxTitleLen); err != nil {
		return err
	}
	switch {
	case r.TotalSize < 0 || r.TotalSize > MaxTotalSize:
		return fmt.Errorf("%w: total size", ErrInvalidField)
	case r.FileCount < 1 || r.FileCount > MaxFileCount:
		return fmt.Errorf("%w: file count", ErrInvalidField)
	case !r.PostDate.IsZero() && !validTime(r.PostDate):
		return fmt.Errorf("%w: post date", ErrInvalidField)
	case r.Media.IMDb != "" && !validIMDb(r.Media.IMDb):
		return fmt.Errorf("%w: imdb id", ErrInvalidField)
	case r.Media.Season != nil && *r.Media.Season > MaxSeason:
		return fmt.Errorf("%w: season", ErrInvalidField)
	case r.Media.Episode != nil && *r.Media.Episode > MaxEpisode:
		return fmt.Errorf("%w: episode", ErrInvalidField)
	}
	return nil
}

// Encode validates and returns the canonical signed encoding.
func (r *ReleaseRecord) Encode() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	b := r.marshal()
	if len(b) > MaxEncodedSize {
		return nil, ErrTooLarge
	}
	return b, nil
}

// Verify applies trust rule 6 for a record that arrived in network arrivedIn.
func (r *ReleaseRecord) Verify(now time.Time, arrivedIn netid.NetworkID) error {
	if err := r.checkBase(now, arrivedIn); err != nil {
		return err
	}
	if err := r.checkSignature(releaseTag, r.body()); err != nil {
		return err
	}
	return r.Validate()
}

// BuildReleaseRecord signs a record with key. Header fields of in are ignored
// (network, publisher and times are set here, expiry at RecordLifetime). The
// title is cleaned; other invalid input is an error. It returns the record and
// its encoding.
func BuildReleaseRecord(key *identity.Key, nw netid.NetworkID, in ReleaseRecord, now time.Time) (*ReleaseRecord, []byte, error) {
	h, err := newHeader(key, nw, in.ReleaseID, in.ReleaseScheme, now, RecordLifetime)
	if err != nil {
		return nil, nil, err
	}
	r := in
	r.Header = h
	r.Title = cleanText(in.Title, MaxTitleLen)
	if !r.PostDate.IsZero() {
		r.PostDate = r.PostDate.UTC().Truncate(time.Second)
	}
	r.Signature, err = sign(key, releaseTag, nw, r.body())
	if err != nil {
		return nil, nil, err
	}
	enc, err := r.Encode()
	if err != nil {
		return nil, nil, err
	}
	if err := r.Verify(now, nw); err != nil {
		return nil, nil, err
	}
	return &r, enc, nil
}

// DecodeReleaseRecord strictly decodes a release record. It does not verify
// the signature or clock; call Verify.
func DecodeReleaseRecord(b []byte) (*ReleaseRecord, error) {
	if len(b) > MaxEncodedSize {
		return nil, ErrTooLarge
	}
	fields, err := parseFields(b, fBackbone)
	if err != nil {
		return nil, err
	}
	r := &ReleaseRecord{}
	var seen uint64
	for _, f := range fields {
		seen |= 1 << f.num
		if ok, err := readHeaderField(f, KindRelease, &r.Header); ok || err != nil {
			if err != nil {
				return nil, err
			}
			continue
		}
		if err := r.readField(f); err != nil {
			return nil, err
		}
	}
	const required = 1<<fVersion | 1<<fKind | 1<<fNetwork | 1<<fRelease | 1<<fNodeID | 1<<fPubKey |
		1<<fCreated | 1<<fExpires | 1<<fTitle | 1<<fSize | 1<<fFiles | 1<<fSignature
	if seen&required != required {
		return nil, fmt.Errorf("%w: missing required field", ErrMalformed)
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if !bytes.Equal(r.marshal(), b) {
		return nil, fmt.Errorf("%w: not canonical", ErrMalformed)
	}
	return r, nil
}

func (r *ReleaseRecord) readField(f field) error {
	var err error
	var v uint64
	switch f.num {
	case fBundle:
		err = readFixed(f, r.BundleID[:], "bundle_id")
	case fTitle:
		r.Title, err = readString(f, "title")
	case fSize:
		v, err = readVarint(f, uint64(MaxTotalSize), "total_size")
		r.TotalSize = int64(v)
	case fFiles:
		v, err = readVarint(f, MaxFileCount, "file_count")
		r.FileCount = uint32(v)
	case fPosted:
		r.PostDate, err = readTime(f, "post_date")
	case fIMDb:
		r.Media.IMDb, err = readString(f, "imdb_id")
	case fTMDB:
		v, err = readVarint(f, 1<<32-1, "tmdb_id")
		r.Media.TMDB = uint32(v)
	case fTVDB:
		v, err = readVarint(f, 1<<32-1, "tvdb_id")
		r.Media.TVDB = uint32(v)
	case fSeason:
		v, err = readVarint(f, MaxSeason, "season")
		s := uint32(v)
		r.Media.Season = &s
	case fEpisode:
		v, err = readVarint(f, MaxEpisode, "episode")
		e := uint32(v)
		r.Media.Episode = &e
	default:
		return fmt.Errorf("%w: unknown field %d", ErrMalformed, f.num)
	}
	return err
}
