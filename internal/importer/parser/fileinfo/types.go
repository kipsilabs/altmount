package fileinfo

import (
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// RAR magic byte signatures
var (
	// Rar4Magic is the magic signature for RAR 4.x archives
	Rar4Magic = []byte{0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00}

	// Rar5Magic is the magic signature for RAR 5.x archives
	Rar5Magic = []byte{0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00}

	// SevenZipMagic is the magic signature for 7-Zip archives
	SevenZipMagic = []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}
)

// FileInfo represents parsed information about an NZB file
// Similar to C# GetFileInfosStep.FileInfo
type FileInfo struct {
	NzbFile       nzbparser.NzbFile  // The original NZB file
	Filename      string             // Selected filename (using priority system)
	ReleaseDate   time.Time          // Release date from NZB metadata
	FileSize      *int64             // File size (from PAR2 or yEnc headers, nil if unknown)
	IsRar         bool               // Whether this is a RAR archive (detected by magic or extension)
	Is7z          bool               // Whether this is a 7z archive (detected by extension)
	IsPar2Archive bool               // Whether this is a PAR2 archive (detected by extension)
	YencHeaders   *nntppool.YEncMeta // yEnc headers from first segment
	First16KB     []byte             // First 16KB of the file (for magic byte detection)
	OriginalIndex int                // Original position in the parsed NZB file list
}

// NzbFileWithFirstSegment represents an NZB file with its first segment data
// Similar to C# FetchFirstSegmentsStep.NzbFileWithFirstSegment
type NzbFileWithFirstSegment struct {
	NzbFile       *nzbparser.NzbFile
	Headers       *nntppool.YEncMeta
	First16KB     []byte
	ReleaseDate   time.Time
	SubjectHeader string // Release name prefix from the subject line (before the filename)
	OriginalIndex int    // Original position in the parsed NZB file list
}
