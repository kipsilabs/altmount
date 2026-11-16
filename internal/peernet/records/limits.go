package records

import (
	"errors"
	"time"
)

// Limits. Several are design calls the owner may tune.
const (
	// SchemaVersion is the only version this package reads and writes.
	SchemaVersion = 1

	// MaxEncodedSize is the hard cap on an encoded record; larger input is
	// rejected before decoding.
	MaxEncodedSize = 2048
	// RecordLifetime is the longest a release record may live.
	RecordLifetime = 30 * 24 * time.Hour
	// AttestationValidity is how long an attestation is valid, measured from
	// its check time for freshness and from creation for its expiry.
	AttestationValidity = 7 * 24 * time.Hour
	// MaxFutureSkew is how far ahead of the receiver's clock a record may be.
	MaxFutureSkew = 5 * time.Minute

	MaxTitleLen    = 256 // bytes
	MaxBackbones   = 16
	MaxBackboneLen = 32 // bytes
	MaxFileCount   = 10000
	MaxSegments    = 500000
	MaxTotalSize   = int64(1) << 50 // 1 PiB
	MaxSeason      = 1000
	MaxEpisode     = 10000
	maxUnixSeconds = 4102444800 // 2100-01-01
	maxIMDbDigits  = 10
	signatureSize  = 64
	releaseTag     = "altmount-peernet/release-record/v1"
	attestationTag = "altmount-peernet/health-attestation/v1"
	idSize         = 32
	publicKeySize  = 32
)

// Sentinel errors. Callers can count rejection reasons with errors.Is.
var (
	ErrTooLarge            = errors.New("records: encoded record too large")
	ErrMalformed           = errors.New("records: malformed or non-canonical encoding")
	ErrUnsupportedVersion  = errors.New("records: unsupported schema version")
	ErrWrongKind           = errors.New("records: wrong record kind")
	ErrInvalidField        = errors.New("records: invalid field value")
	ErrWrongNetwork        = errors.New("records: record is for another network")
	ErrFromFuture          = errors.New("records: created in the future")
	ErrExpired             = errors.New("records: expired")
	ErrCheckedAfterCreated = errors.New("records: checked after it was created")
	ErrStale               = errors.New("records: attestation too old")
	ErrNodeIDMismatch      = errors.New("records: publisher node id does not match key")
	ErrBadSignature        = errors.New("records: bad signature")
)
