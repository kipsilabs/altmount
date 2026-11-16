package service

import metapb "github.com/kipsilabs/altmount/internal/metadata/proto"

// ReleaseFile is the narrow view of one file's metadata that release
// information is built from. The full metadata record carries encryption
// material and more; none of that crosses into the release-information code.
type ReleaseFile struct {
	FileSize int64
	// ReleaseDate is the Unix time of the original post (0 = unknown).
	ReleaseDate   int64
	SourceNzbPath string
	// Password is only compared against titles so it can be scrubbed from
	// them; it is never published.
	Password string
}

// ReleaseFileReader reads the narrow view of a file's metadata.
type ReleaseFileReader interface {
	// ReadReleaseFile returns the view, or (nil, nil) when the file is absent.
	ReadReleaseFile(virtualPath string) (*ReleaseFile, error)
}

// metadataReader is the one read the adapter needs (contentid.Source and the
// metadata service both satisfy it).
type metadataReader interface {
	ReadFileMetadata(virtualPath string) (*metapb.FileMetadata, error)
}

type releaseFileAdapter struct{ src metadataReader }

// NewReleaseFileReader narrows a metadata source: it reads the full record,
// copies the four fields and lets the record go at once.
func NewReleaseFileReader(src metadataReader) ReleaseFileReader {
	return releaseFileAdapter{src: src}
}

func (a releaseFileAdapter) ReadReleaseFile(virtualPath string) (*ReleaseFile, error) {
	m, err := a.src.ReadFileMetadata(virtualPath)
	if err != nil || m == nil {
		return nil, err
	}
	return &ReleaseFile{
		FileSize:      m.FileSize,
		ReleaseDate:   m.ReleaseDate,
		SourceNzbPath: m.SourceNzbPath,
		Password:      m.Password,
	}, nil
}
