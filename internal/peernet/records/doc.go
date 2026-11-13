// Package records defines the signed data types the peer network gossips for
// metadata sharing: the release record and the health attestation.
//
// The package is pure: no I/O, no database, no network. Records are written
// and read by hand in protobuf wire format (no generated code) because signing
// needs one canonical byte string per record.
//
// # Canonical encoding
//
// Fields appear in ascending field-number order, at most once (the backbone
// field repeats, sorted and unique). Required scalars are always written;
// optional ones are written only when set. Unknown fields, duplicate fields,
// out-of-order fields, non-minimal varints, trailing bytes and wrong wire types
// are all rejected. A decoder re-encodes what it parsed and rejects the input
// unless the bytes are identical.
//
// Times are Unix seconds. IDs and keys are raw bytes. A schema version is
// carried in field 1; an unknown version is rejected.
//
// # Field numbers
//
// Common header (both types):
//
//	1  version             varint  (must be 1)
//	2  kind                varint  (1 release record, 2 health attestation)
//	3  network_id          bytes   32
//	4  release_id          bytes   32
//	5  publisher_node_id   bytes   32  (SHA-256 of the public key)
//	6  publisher_key       bytes   32  (Ed25519)
//	7  created_at          varint
//	8  expires_at          varint
//
// Release record:
//
//	10  bundle_id           bytes   32  optional, absent in phase 2
//	11  title               bytes   UTF-8, required
//	12  total_size          varint
//	13  file_count          varint
//	14  post_date           varint  optional
//	15  imdb_id             bytes   optional, ^tt[0-9]{1,10}$
//	16  tmdb_id             varint  optional
//	17  tvdb_id             varint  optional
//	18  season              varint  optional (0 is a real value)
//	19  episode             varint  optional (0 is a real value)
//
// Health attestation:
//
//	10  file_id             bytes   32
//	11  checked_at          varint
//	12  total_segments      varint
//	13  segments_checked    varint
//	14  missing_count       varint  (0 means healthy)
//	15  media_header_ok     varint  (0 or 1)
//	16  backbone            bytes   repeated, sorted, unique, [a-z0-9._-]
//
// Both types end with:
//
//	30  signature           bytes   64 (Ed25519)
//
// # Signing
//
// The signature covers network_id || canonical encoding without field 30,
// under a domain tag that differs per type, using the identity package's
// domain-separated Sign.
package records
