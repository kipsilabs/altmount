package contentid

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

// MaxDerivedReleaseFiles is the most files one library-derived release may
// have on this node. A larger group is not given a Release ID. It is the single
// local limit for deriving an ID and for describing the release when
// publishing (release information reads the metadata of every file, so it must
// stay bounded).
const MaxDerivedReleaseFiles = 2000

// maxIDFiles is the wire bound of the files Release ID function (the most
// files a bundle may list); it is independent of the local cap above.
const maxIDFiles = 10000

const releaseFilesDomain = "altmount-peernet/release-files/v1"

var (
	// ErrNoFiles means a library-derived Release ID was asked for with no files.
	ErrNoFiles = errors.New("contentid: no files")
	// ErrTooManyFiles means the group is over the 10,000-file wire bound.
	ErrTooManyFiles = errors.New("contentid: too many files")
)

// ReleaseIDFromFiles computes the library-derived ("files" scheme) Release ID
// of a release whose NZB segment list is not available: SHA-256 over the
// domain label (u32 length prefix, as in every other ID) followed by the
// distinct File IDs of the release's files sorted ascending bytewise, each 32
// bytes, with no count and no separators. See the package documentation. A
// File ID listed more than once (the same file kept under two folders) counts
// once. The input is not modified. A zero File ID, an empty list or more than
// 10,000 distinct files is an error.
func ReleaseIDFromFiles(files []ID) (ID, error) {
	if len(files) == 0 {
		return ID{}, ErrNoFiles
	}
	sorted := make([]ID, len(files))
	copy(sorted, files)
	for _, f := range sorted {
		if f.IsZero() {
			return ID{}, fmt.Errorf("%w: zero file id", ErrInvalidID)
		}
	}
	sort.Slice(sorted, func(a, b int) bool { return bytes.Compare(sorted[a][:], sorted[b][:]) < 0 })
	distinct := sorted[:1]
	for _, f := range sorted[1:] {
		if f != distinct[len(distinct)-1] {
			distinct = append(distinct, f)
		}
	}
	if len(distinct) > maxIDFiles {
		return ID{}, fmt.Errorf("%w: %d", ErrTooManyFiles, len(distinct))
	}
	w := newWriter(releaseFilesDomain)
	for _, f := range distinct {
		_, _ = w.h.Write(f[:])
	}
	return w.sum(), nil
}
