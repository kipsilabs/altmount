package contentid

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/kipsilabs/altmount/internal/database"
)

// A file imported from a peer bundle has no source NZB path, so the usual
// group key (a hash of that path) is empty and the usual derivation cannot see
// it. The bundle's Release ID, however, was verified cryptographically by the
// receiver against the segment lists it was given, so it is recorded directly
// in the index, under a group key derived locally from that very ID. A row
// whose group key is the key of its own release ID is a "verified bundle row".
// No schema change is needed: when the derive pass later changes the ID (the
// user deleted a file of a "files" release), the row's key no longer matches
// its ID and it is an ordinary derived row, exactly as for a local import.

// bundleGroupKey is the local-only group key of a bundle's files. It is never
// transmitted and is not part of any ID.
func bundleGroupKey(releaseHex string) string {
	sum := sha256.Sum256([]byte("altmount-peernet/bundle-group/v1\x00" + releaseHex))
	return hex.EncodeToString(sum[:])
}

// VerifiedEntry returns the index row of a file whose release identity came
// from a verified peer bundle: its File ID, the bundle's Release ID and scheme
// (database.PeernetSchemeNZB or PeernetSchemeFiles) and the local group key.
func VerifiedEntry(filePath string, fileID, releaseID ID, scheme int) database.PeernetContentEntry {
	return database.PeernetContentEntry{
		FilePath: filePath, FileID: fileID.String(), ReleaseID: releaseID.String(),
		ReleaseScheme: scheme, GroupKey: bundleGroupKey(releaseID.String()),
	}
}

// IsVerifiedBundleRow reports whether the row still carries the Release ID a
// verified bundle gave it.
func IsVerifiedBundleRow(e database.PeernetContentEntry) bool {
	return e.ReleaseID != "" && e.GroupKey != "" && e.GroupKey == bundleGroupKey(e.ReleaseID)
}

// keepsBundleGrouping reports that a freshly computed entry e (which can only
// see the metadata) must not replace the stored row old: the file is the same
// and the row's group was assigned locally from a verified bundle, which the
// metadata (no source NZB path, no store) cannot reproduce. Once the derive
// pass has changed such a row's ID the row stays in its group and is derived
// like any other; only a changed file identity re-indexes it.
func keepsBundleGrouping(old, e database.PeernetContentEntry) bool {
	return old.FileID == e.FileID && old.GroupKey != "" && e.GroupKey == "" && e.ReleaseScheme != database.PeernetSchemeNZB
}
