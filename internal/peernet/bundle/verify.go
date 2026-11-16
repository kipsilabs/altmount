package bundle

import (
	"context"
	"fmt"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

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
	return VerifyContext(context.Background(), wire, announcedBundle, announcedRelease)
}

// verifySlot admits one verification at a time, node-wide: decoding a large
// bundle peaks at several times its canonical size, so concurrent fetches, fast
// paths and playback requests must queue.
var verifySlot = make(chan struct{}, 1)

// verifyEnterHook is a test seam; it returns the function to call on leaving.
var verifyEnterHook func() func()

// VerifyContext is Verify that waits for the node-wide verification slot and
// gives up when ctx ends first.
func VerifyContext(ctx context.Context, wire []byte, announcedBundle BundleID, announcedRelease contentid.ID) (*Bundle, error) {
	select {
	case verifySlot <- struct{}{}:
		defer func() { <-verifySlot }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if verifyEnterHook != nil {
		defer verifyEnterHook()()
	}
	raw, err := Decompress(wire)
	if err != nil {
		return nil, err
	}
	return VerifyCanonical(raw, announcedBundle, announcedRelease)
}

// VerifyCanonical is Verify for bytes that are already decompressed.
func VerifyCanonical(canonical []byte, announcedBundle BundleID, announcedRelease contentid.ID) (*Bundle, error) {
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
		got = FilesReleaseID(ids)
	}
	if got != b.ReleaseID || got != announcedRelease {
		return nil, ErrReleaseIDMismatch
	}
	return b, nil
}
