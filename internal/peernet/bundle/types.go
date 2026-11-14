package bundle

import (
	"encoding/hex"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// Scheme says how the Release ID of a bundle is defined.
type Scheme uint8

const (
	// SchemeNZB: the Release ID is the contentid Release ID over the release's
	// full ordered segment list. The bundle carries that list, including NZB
	// files that were not imported, so the receiver can recompute it.
	SchemeNZB Scheme = 1
	// SchemeFiles: the Release ID is SHA-256 over the tag
	// "altmount-peernet/release-files/v1" followed by the sorted File IDs.
	SchemeFiles Scheme = 2
)

func (s Scheme) String() string {
	switch s {
	case SchemeNZB:
		return "nzb"
	case SchemeFiles:
		return "files"
	}
	return "unknown"
}

// BundleID is the SHA-256 of the canonical uncompressed bundle bytes.
type BundleID [idSize]byte

// String returns the id as 64 lower-case hex characters.
func (id BundleID) String() string { return hex.EncodeToString(id[:]) }

// GapRef is the Segment.Ref of a synthetic gap placeholder (an article the
// NZB never listed). A gap has no message-ID; the receiver derives a local
// placeholder id.
const GapRef int64 = -1

// Segment is one entry of a segment table: the article it reads (an index
// into Bundle.Pool, or GapRef), its decoded size, and the usable byte range
// inside it (inclusive offsets, as on disk).
type Segment struct {
	Ref   int64
	Size  int64
	Start int64
	End   int64
}

// OuterSource is one outer archive volume a nested file reads through. With
// a key the volume is AES-CBC encrypted and VolumeSize is its decrypted size.
type OuterSource struct {
	Segments   []Segment
	AESKey     []byte
	AESIV      []byte
	VolumeSize int64
}

// Extent is one slice of an outer source that holds file bytes. Extents are
// concatenated in order to form the file.
type Extent struct {
	Outer       int
	InnerOffset int64
	InnerLength int64
}

// Clip is one clip of a byte-concatenated multi-clip main feature.
type Clip struct {
	ByteLen  int64
	Delta90k int64
}

// Par2File references one PAR2 repair file of the release.
type Par2File struct {
	Name     string
	Size     int64
	Segments []Segment
}

// File is one virtual file of the release.
type File struct {
	// Name is a sanitised base name. It is never a path.
	Name string
	Size int64
	// ReleaseDate is the Unix time of the original post (0 = unknown).
	ReleaseDate int64
	// FileID is the contentid File ID; Validate-time checks recompute it.
	FileID contentid.ID
	// Segments is the file's own segment table (empty for nested files).
	Segments []Segment
	// AESKey and AESIV are set together for a directly AES-encrypted file.
	AESKey []byte
	AESIV  []byte
	// Outer and Extents describe a file read through outer archive volumes.
	Outer   []OuterSource
	Extents []Extent
	Clips   []Clip
	Par2    []Par2File
}

// Bundle is the decoded, structurally valid form of a release bundle.
type Bundle struct {
	Scheme    Scheme
	ReleaseID contentid.ID
	Title     string
	// PostedUnix is the release post date (Unix seconds).
	PostedUnix int64
	// TotalSize is the sum of the file sizes.
	TotalSize int64
	// NZBPassword is an opaque string taken from the NZB; empty if none.
	NZBPassword string
	// ReleaseFiles (SchemeNZB only) lists, per NZB file in NZB order, how many
	// real segment ids it holds. The ids are Pool in order.
	ReleaseFiles []int
	// Pool holds canonical message-IDs. Segment.Ref indexes it.
	Pool []string
	// Files are sorted by FileID ascending.
	Files []File
}
