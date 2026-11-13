// Package discovery keeps the set of known peers per network and exchanges
// peers with them.
//
// Peers are found in exactly two ways: addresses the user enters (resolved to
// a Node ID with a handshake-only probe and then pinned), and peer exchange
// once connected. Further sources can be plugged in behind the Source
// interface. There is no DHT and there are no built-in bootstrap nodes.
//
// Nothing a peer says about an address is trusted. An advertised or exchanged
// address is only marked verified, and only handed to other peers, after this
// instance has dialled it itself and the server there proved the expected
// Node ID. Every route and client call is scoped to one network, and the
// private-address allowance is a per-network setting supplied by the caller;
// it is never inferred from peer data and never applied across networks.
//
// The package starts no goroutines of its own: another component calls
// Service.Tick periodically.
package discovery
