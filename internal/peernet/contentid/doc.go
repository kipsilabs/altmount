// Package contentid computes the stable, instance-independent identifiers the
// peer network uses to talk about content: the Release ID and the File ID.
//
// Both identifiers are SHA-256 digests over the real Usenet message-IDs of the
// posted articles and over how those IDs are grouped. They never depend on
// local paths, timestamps, health status, known holes, passwords, keys,
// segment sizes or offsets, or on segments that exist only as local PAR2
// patches. Synthetic gap placeholders (holes.IsPlaceholderID) are skipped.
//
// # Canonical byte layout (version 1)
//
// All integers are unsigned and big-endian. A message-ID is canonicalised by
// removing one leading "<" and one trailing ">" if present; the result must be
// non-empty.
//
//	id        := u32(len(canonical id)) || canonical id bytes
//	section   := u64(number of ids in section) || id*
//	domain    := u32(len(label)) || label bytes
//
// Release ID = SHA-256( domain("altmount-peernet/release/v1")
//
//	|| u64(number of files) || section* )
//
// with one section per NZB file entry, in NZB order, holding that file's
// real segment IDs in stored (segment number) order. A file whose segments
// were all placeholders still contributes an empty section, so file
// boundaries are part of the identity.
//
// File ID = SHA-256( domain("altmount-peernet/file/v1")
//
//	|| section(main segments)
//	|| u64(number of nested sources) || section* )
//
// The main section holds the file's own ordered segment IDs. Each nested
// source (an inner archive volume read through its outer archive volume)
// contributes one section holding the outer segment IDs it covers, in
// FileMetadata order. A nested source that refers to a shared outer source is
// hashed exactly as if the shared source's segments had been inlined, so the
// ID does not depend on how the metadata happens to be stored.
//
// # Library-derived Release ID ("files" scheme)
//
// A file imported before the NZB store existed (segments kept inline per file)
// has a File ID but no NZB segment list, so no Release ID of the form above.
// Such a release is identified by its files instead:
//
//	files release ID = SHA-256( domain("altmount-peernet/release-files/v1")
//
//	|| fileID(1) || fileID(2) || ... || fileID(n) )
//
// where domain() is the same u32-length-prefixed label as above, the file IDs
// are the 32-byte File IDs of the release's files sorted ascending by plain
// bytewise comparison with duplicates removed (a file kept under two folders is
// one file), and there is no count and no separator (every item has a fixed
// width). n, the number of distinct File IDs, is at least 1 and at most
// 10,000 (a node only derives
// IDs for groups up to MaxDerivedReleaseFiles, 2000). Records that carry such an ID say so in
// their release_scheme field (2); the two ID spaces are domain-separated, so
// an "nzb" ID and a "files" ID never collide.
//
// Which files make up "the release" is a local decision (see the Indexer): the
// indexed files that came from one import, found through the stored source
// NZB path of their metadata. Two nodes that imported the same NZB and kept
// the same file set derive the same ID; different kept subsets give different
// IDs. File IDs match across both schemes, so health evidence is shared per
// file.
//
// Every variable-length item is length-prefixed and every list is
// count-prefixed, and the domain label differs per ID kind, so two different
// structures cannot produce the same input stream. Future layouts must use a
// new domain label (for example ".../v2"); version 1 must never change.
package contentid
