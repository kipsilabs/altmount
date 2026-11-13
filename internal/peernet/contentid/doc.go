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
// Every variable-length item is length-prefixed and every list is
// count-prefixed, and the domain label differs per ID kind, so two different
// structures cannot produce the same input stream. Future layouts must use a
// new domain label (for example ".../v2"); version 1 must never change.
package contentid
