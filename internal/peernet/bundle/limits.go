package bundle

import "errors"

// Limits. Several are design calls the owner may tune; the hard caps named in
// the design (8 MB compressed, 64 MB decompressed, 500k segments, 10k files)
// are enforced while streaming and again on the decoded structure.
const (
	// SchemaVersion is the only major version this package reads and writes.
	SchemaVersion = 1

	// MaxCompressedSize bounds the bundle as it travels on the wire.
	MaxCompressedSize = 8 << 20
	// MaxDecompressedSize bounds the canonical (uncompressed) bytes. The
	// decompressor stops as soon as the output would exceed it.
	MaxDecompressedSize = 64 << 20
	// MaxWindowSize is the largest zstd window a frame may ask for. The
	// builder uses the same value, so honest bundles always fit.
	MaxWindowSize = 8 << 20

	// MaxSegments caps the segment positions of a bundle, counted over every
	// segment table (main files, shared outer sources and PAR2 files), and
	// separately the message-ID pool.
	MaxSegments = 500_000
	// MaxFiles caps the files of one bundle.
	MaxFiles = 10_000
	// MaxReleaseFiles caps the NZB file entries of the "nzb" scheme release
	// list (it includes files that were not imported).
	MaxReleaseFiles = 50_000
	// MaxMessageIDLen caps a canonical message-ID (no angle brackets). RFC
	// 5536 limits message-IDs to 250 octets including the brackets.
	MaxMessageIDLen = 248
	// MaxFilenameLen caps a sanitised file name in bytes.
	MaxFilenameLen = 255
	// MaxTitleLen caps the release title in bytes (same as release records).
	MaxTitleLen = 256
	// MaxPasswordLen caps the opaque NZB password in bytes.
	MaxPasswordLen = 256
	// MaxNestingDepth is the deepest archive nesting a file may describe: a
	// file read through its outer archive volumes. The schema cannot express
	// deeper nesting or a cycle, so the cap is structural.
	MaxNestingDepth = 1
	// MaxOuterSources caps the shared outer sources of one file.
	MaxOuterSources = 10_000
	// MaxExtents caps the nested extents of the whole bundle.
	MaxExtents = 100_000
	// MaxClips caps the clip boundaries of one file.
	MaxClips = 100_000
	// MaxTotalClips, MaxTotalOuterSources and MaxTotalPar2Files are the
	// bundle-wide budgets next to the segment and extent budgets, so many
	// files cannot each spend their own per-file allowance.
	MaxTotalClips        = 200_000
	MaxTotalOuterSources = 50_000
	MaxTotalPar2Files    = 10_000
	// MaxPar2Files caps the PAR2 file references of one file.
	MaxPar2Files = 1_000

	// MaxSegmentSize is the largest decoded size of one article: real posts
	// are 0.25-1 MB, a few go to a few MB.
	MaxSegmentSize = 8 << 20
	// MaxFileSize caps one file and the bundle total (1 PiB, as records).
	MaxFileSize = int64(1) << 50
	// MaxClipDelta bounds a clip's 90 kHz offset (about 10 years).
	MaxClipDelta = int64(90_000) * 86_400 * 3_650
	// ClipPacketSize is the BDAV source packet size every clip length is a
	// multiple of.
	ClipPacketSize = 192

	maxUnixSeconds = 4102444800 // 2100-01-01
	idSize         = 32
	aesBlockSize   = 16
)

// Sentinel errors. Callers can count rejection reasons with errors.Is.
var (
	ErrTooLarge           = errors.New("bundle: too large")
	ErrMalformed          = errors.New("bundle: malformed or non-canonical encoding")
	ErrUnsupportedVersion = errors.New("bundle: unsupported schema version")
	ErrUnknownScheme      = errors.New("bundle: unknown release id scheme")
	ErrInvalidField       = errors.New("bundle: invalid field value")
	ErrBadFilename        = errors.New("bundle: invalid file name")
	ErrDuplicateName      = errors.New("bundle: duplicate file name")
	ErrBadMessageID       = errors.New("bundle: invalid message-id")
	ErrLimit              = errors.New("bundle: count limit exceeded")
	ErrOutOfRange         = errors.New("bundle: reference or offset out of range")
	ErrSizeMismatch       = errors.New("bundle: extents do not add up to the declared size")
	ErrImplausible        = errors.New("bundle: implausible size for the segment count")
	ErrBadEncryption      = errors.New("bundle: invalid encryption material")
	ErrBadClip            = errors.New("bundle: invalid clip boundaries")
	ErrBadPool            = errors.New("bundle: invalid message-id pool")
	ErrOverlap            = errors.New("bundle: overlapping extents")
	ErrBundleIDMismatch   = errors.New("bundle: bundle id does not match the announced id")
	ErrReleaseIDMismatch  = errors.New("bundle: release id does not match the announced id")
	ErrFileIDMismatch     = errors.New("bundle: file id does not match its segments")
	ErrDuplicateFile      = errors.New("bundle: duplicate file id")
	ErrNotShareable       = errors.New("bundle: release is not shareable")
	ErrUnresolved         = errors.New("bundle: metadata segments are not resolved")
	ErrCoverage           = errors.New("bundle: segments are not exactly one NZB file entry")
	ErrSegmentNotInNZB    = errors.New("bundle: file segment is not in the release segment list")
)
