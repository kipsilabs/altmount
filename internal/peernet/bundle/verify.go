package bundle

import (
	"fmt"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// Options lets the caller replace the files-scheme Release ID function, so
// this package does not depend on the one in contentid until it exists.
type Options struct {
	// FilesReleaseID computes the Release ID of the files scheme from the File
	// IDs, which are passed sorted ascending bytewise. Nil selects
	// FilesReleaseID.
	FilesReleaseID func(sortedFileIDs []contentid.ID) contentid.ID
}

// FilesReleaseID is the files-scheme Release ID, as defined by
// contentid.ReleaseIDFromFiles: SHA-256 over the "altmount-peernet/release-files/v1"
// domain label and the File IDs sorted ascending bytewise. ids is not
// modified. An invalid input (no files, a zero File ID) yields the zero ID,
// which never equals a real Release ID.
func FilesReleaseID(ids []contentid.ID) contentid.ID {
	id, err := contentid.ReleaseIDFromFiles(ids)
	if err != nil {
		return contentid.ID{}
	}
	return id
}

// Pack encodes, validates and compresses b for the wire and returns the
// Bundle ID (the hash of the canonical uncompressed bytes).
func Pack(b *Bundle) (wire []byte, id BundleID, err error) {
	raw, err := Encode(b)
	if err != nil {
		return nil, BundleID{}, err
	}
	wire, err = Compress(raw)
	if err != nil {
		return nil, BundleID{}, err
	}
	return wire, ComputeID(raw), nil
}

// Verify checks a bundle received from a peer, in this order: wire size and
// bounded decompression; the hash equals the announced Bundle ID; strict
// decoding and structural validation; every File ID recomputed from the
// segments; the Release ID recomputed for the declared scheme equals both the
// bundle's own and the announced one. It does not touch the network.
func Verify(wire []byte, announcedBundle BundleID, announcedRelease contentid.ID) (*Bundle, error) {
	return VerifyWith(wire, announcedBundle, announcedRelease, Options{})
}

// VerifyWith is Verify with options.
func VerifyWith(wire []byte, announcedBundle BundleID, announcedRelease contentid.ID, o Options) (*Bundle, error) {
	raw, err := Decompress(wire)
	if err != nil {
		return nil, err
	}
	return VerifyCanonical(raw, announcedBundle, announcedRelease, o)
}

// VerifyCanonical is Verify for bytes that are already decompressed.
func VerifyCanonical(canonical []byte, announcedBundle BundleID, announcedRelease contentid.ID, o Options) (*Bundle, error) {
	if len(canonical) > MaxDecompressedSize {
		return nil, fmt.Errorf("%w: %d bytes uncompressed", ErrTooLarge, len(canonical))
	}
	if ComputeID(canonical) != announcedBundle {
		return nil, ErrBundleIDMismatch
	}
	b, err := Decode(canonical)
	if err != nil {
		return nil, err
	}
	ids := make([]contentid.ID, len(b.Files))
	for i := range b.Files {
		f := &b.Files[i]
		id, err := contentid.FileID(buildMeta(b, f))
		if err != nil {
			return nil, fmt.Errorf("%w: file %q: %v", ErrFileIDMismatch, f.Name, err)
		}
		if id != f.FileID {
			return nil, fmt.Errorf("%w: file %q", ErrFileIDMismatch, f.Name)
		}
		ids[i] = id
	}
	var got contentid.ID
	switch b.Scheme {
	case SchemeNZB:
		if got, err = releaseIDOfList(b.Pool, b.ReleaseFiles); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrReleaseIDMismatch, err)
		}
	default:
		fn := o.FilesReleaseID
		if fn == nil {
			fn = FilesReleaseID
		}
		got = fn(ids)
	}
	if got != b.ReleaseID || got != announcedRelease {
		return nil, ErrReleaseIDMismatch
	}
	return b, nil
}
