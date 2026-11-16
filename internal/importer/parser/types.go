package parser

import (
	"time"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// ParseOptions carries pre-parse knowledge into ParseNzb, allowing the
// processor to skip Body fetches for files whose segments are already known to
// be missing (identified by a pre-parse Stat check).
type ParseOptions struct {
	// BrokenFileIndexes contains the 0-based positions (in the Nzb.Files slice)
	// of files whose sampled segments failed a pre-parse Stat check. Their first
	// segments are short-circuited to MissingFirstSegment=true without a Body call.
	BrokenFileIndexes map[int]struct{}
	// KnownMissingSegmentIDs seeds notFoundIDs so yEnc normalisation and 16KB
	// completion never re-issue Stat/Body calls for already-known-missing IDs.
	KnownMissingSegmentIDs map[string]struct{}
	// PrebuiltStore and PrebuiltIndex are metadata.BuildStore of this very NZB,
	// when the caller already built them and the NZB did not change since. Both
	// or neither.
	PrebuiltStore *metapb.NzbStore
	PrebuiltIndex map[string]int64
}

// NzbType represents the type of NZB content
type NzbType string

const (
	NzbTypeSingleFile NzbType = "single_file"
	NzbTypeMultiFile  NzbType = "multi_file"
	NzbTypeRarArchive NzbType = "rar_archive"
	NzbType7zArchive  NzbType = "7z_archive"
	NzbTypeStrm       NzbType = "strm_file"
)

type ExtractedFileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// ParsedNzb contains the parsed NZB data and extracted metadata
type ParsedNzb struct {
	Path           string
	Filename       string
	TotalSize      int64
	Type           NzbType
	Files          []ParsedFile
	SegmentsCount  int
	password       string // Private field - use GetPassword() to access
	ExtractedFiles []ExtractedFileInfo
	Store          *metapb.NzbStore // NzbStore for this release (built at parse time)
	SegmentIndex   map[string]int64 // message-id → flat store index
	// DegradedFiles maps a parsed filename to the message-id of its dead first
	// article, for files whose header was recovered from a later article. The
	// processor merges it into the repair queue so the hole gets patched.
	DegradedFiles map[string]string
	// passwordOrigin says where password was found (see PasswordOrigin).
	passwordOrigin PasswordOrigin
}

// GetPassword returns the password for this NZB
func (p *ParsedNzb) GetPassword() string {
	return p.password
}

// PasswordOrigin reports where the password in effect was found. It is
// PasswordOriginNone when there is none.
func (p *ParsedNzb) PasswordOrigin() PasswordOrigin {
	if p.password == "" {
		return PasswordOriginNone
	}
	return p.passwordOrigin
}

// SetPassword sets the password for this NZB
func (p *ParsedNzb) SetPassword(password string) {
	p.password = password
}

// ParsedFile represents a file extracted from the NZB
type ParsedFile struct {
	Subject          string
	Filename         string
	OriginalFilename string // Recovered posted name before import/display renaming.
	Size             int64
	Segments         []*metapb.SegmentData
	SegmentRefs      []*metapb.SegmentRef // v3: refs into NzbStore (populated when Store != nil)
	Groups           []string
	IsRarArchive     bool
	Is7zArchive      bool
	IsPar2Archive    bool
	Encryption       metapb.Encryption // Encryption type (e.g., "rclone"), nil if not encrypted
	Password         string            // Password from NZB meta, nil if not encrypted
	Salt             string            // Salt from NZB meta, nil if not encrypted
	ReleaseDate      time.Time         // Release date from the Usenet post
	OriginalIndex    int               // Original position in the parsed NZB file list
	NzbdavID         string            // Original ID from nzbdav (for backward compatibility)
	AesKey           []byte            // AES encryption key (for nzbdav compatibility)
	AesIv            []byte            // AES initialization vector (for nzbdav compatibility)

	// FirstSegmentBytes holds the decoded leading bytes (≤16KB) of this file's first
	// segment, captured when the parser fetched it during first-segment analysis. It
	// lets the archive analysis phase (UsenetFileSystem) serve a volume's header read,
	// which starts at offset 0, from memory instead of re-fetching a segment already
	// pulled over the wire this import. Empty when the first segment was skipped/missing
	// or for files built outside the parser — those paths fall through to the network.
	// Transient (not persisted); valid only for the lifetime of the import.
	FirstSegmentBytes []byte
}
