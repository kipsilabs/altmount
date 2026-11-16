// Package membership implements private peer networks: a host instance holds
// a network root key and admits members with short-lived signed certificates.
//
// # Wire and signed encodings
//
// All integers are big-endian. Signed bytes are never JSON or protobuf; they
// are the fixed layouts below, signed with identity.Sign under a dedicated
// domain tag. Decoders are strict: exact lengths, known version, no trailing
// bytes, hard size caps.
//
// Membership certificate (version 1, 147 bytes), tag
// "altmount-peernet-membership-cert-v1":
//
//	u8   version = 1
//	[32] network ID
//	[32] member node ID
//	u8   role (1 = host, 2 = member)
//	i64  issued-at (Unix seconds)
//	i64  expires-at (Unix seconds)
//	u8   signature length = 64
//	[64] Ed25519 signature over every byte above the signature length
//
// Revocation list (version 1, at most 66 KiB), tag
// "altmount-peernet-membership-revocation-v1":
//
//	u8   version = 1
//	[32] network ID
//	u64  sequence (monotonic)
//	i64  issued-at
//	u16  count (at most 2048)
//	count x [32] revoked node IDs, strictly ascending (canonical, no duplicates)
//	u8   signature length = 64
//	[64] signature over every byte above the signature length
//
// Redeem request (49 bytes): u8 version = 1, [32] network ID, [16] invite code.
// Renew request (33 bytes): u8 version = 1, [32] network ID.
//
// Join/renew response: u8 version = 1, u8 name length (<= 64), name bytes,
// u16 certificate length, certificate, u32 revocation list length, list.
//
// Request/response membership proof header "X-Peernet-Cert": the certificate
// in unpadded base64url (at most 400 characters).
//
// # Invites
//
// An invite code is 128 random bits shown as 26 lower-case base32 characters.
// Only SHA-256(domain || code) is stored. The invite link is
//
//	altmountnet://join?v=1&n=<network id hex>&k=<root key base64url>&a=<host:port>&h=<host node id hex>&c=<code>
//
// with exactly these fields in this order. Codes and links are never logged.
//
// # Trust model
//
// The network ID is derived from the root key, a certificate is valid only
// when signed by that key, naming the authenticated transport peer, inside its
// time window (small clock-skew allowance) and not on the held revocation
// list, whose sequence never decreases. Lifetime is 48 hours; renewal happens
// at half-life.
package membership
