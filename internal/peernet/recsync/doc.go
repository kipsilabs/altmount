// Package recsync exchanges signed peer-network records between nodes.
//
// It owns four things:
//
//   - the ingest pipeline, the single entry point for records from peers and
//     for locally built ones (size cap, Peek, strict decode, signature and
//     network verification, row mapping, limited insert, typed outcome);
//   - the exposure policy, one small function mapping a network's level to
//     what record exchange may do on it;
//   - the wire framing of a records page and of a push, protobuf wire format
//     with length-prefixed raw records, no JSON for record payloads;
//   - the server routes and the client rounds (pull with per-peer cursors,
//     push of own records), plus upkeep (expiry purge, orphan cleanup).
//
// Everything a peer sends is untrusted. A record is stored only after its
// signature, network and timing verified, and nothing in this package acts on
// a record beyond storing it.
package recsync
