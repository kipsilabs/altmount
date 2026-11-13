package discovery

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Route patterns and the query parameter naming the network.
const (
	RouteHello = "POST /peer/v1/hello"
	RoutePeers = "GET /peer/v1/peers"
	pathHello  = "/peer/v1/hello"
	pathPeers  = "/peer/v1/peers"
	queryNet   = "n"
)

// Hard wire limits.
const (
	// MaxHelloBody is the largest accepted hello request body.
	MaxHelloBody = 1 << 10
	// MaxPeerEntries is the largest number of entries in a peers response.
	MaxPeerEntries = 20
	// MaxPeersResponseBytes is the largest accepted peers response body.
	MaxPeersResponseBytes = 16 << 10
	maxHelloReplyBytes    = 1 << 10
)

// ErrMalformed is returned for undecodable or oversized peer-supplied data.
var ErrMalformed = errors.New("discovery: malformed message")

type helloRequest struct {
	Address string `json:"address"`
}

// PeerEntry is one peer in an exchange response.
type PeerEntry struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type peersResponse struct {
	Peers []PeerEntry `json:"peers"`
}

// strictUnmarshal decodes exactly one JSON value into v: no unknown fields, no
// trailing data.
func strictUnmarshal(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data", ErrMalformed)
	}
	return nil
}

// DecodeHello parses a hello request body and returns the advertised address
// (possibly empty). It only checks the shape; the address still has to be
// validated against the network's policy.
func DecodeHello(b []byte) (string, error) {
	if len(b) > MaxHelloBody {
		return "", fmt.Errorf("%w: too large", ErrMalformed)
	}
	var h helloRequest
	if err := strictUnmarshal(b, &h); err != nil {
		return "", err
	}
	if len(h.Address) > 255 {
		return "", fmt.Errorf("%w: address length", ErrMalformed)
	}
	return h.Address, nil
}

func encodeHello(addr string) []byte {
	b, _ := json.Marshal(helloRequest{Address: addr})
	return b
}

// DecodePeers parses a peers response body. A body larger than
// MaxPeersResponseBytes or with more than MaxPeerEntries entries is rejected
// as a whole. Entries are not validated here (see sanitizeEntries).
func DecodePeers(b []byte) ([]PeerEntry, error) {
	if len(b) > MaxPeersResponseBytes {
		return nil, fmt.Errorf("%w: too large", ErrMalformed)
	}
	var r peersResponse
	if err := strictUnmarshal(b, &r); err != nil {
		return nil, err
	}
	if len(r.Peers) > MaxPeerEntries {
		return nil, fmt.Errorf("%w: too many entries", ErrMalformed)
	}
	return r.Peers, nil
}

func encodePeers(entries []PeerEntry) ([]byte, error) {
	if entries == nil {
		entries = []PeerEntry{}
	}
	return json.Marshal(peersResponse{Peers: entries})
}

// sanitizedPeer is a validated exchange entry.
type sanitizedPeer struct {
	node identity.NodeID
	addr string // canonical IP-literal host:port
}

// sanitizeEntries drops entries that are malformed, duplicate, self, the
// answering peer itself, or whose address may not be learned from a peer: only
// public IP literals on non-denied ports are accepted (no hostnames, and no
// private addresses even in a network that allows them: those may only come
// from hello, the user's own list or a local Source). Input order is kept.
func sanitizeEntries(entries []PeerEntry, self, from identity.NodeID, allowLoopback bool) []sanitizedPeer {
	seen := make(map[identity.NodeID]struct{}, len(entries))
	var out []sanitizedPeer
	for _, e := range entries {
		id, err := identity.ParseNodeID(e.NodeID)
		if err != nil || id.IsZero() || id.Equal(self) || id.Equal(from) {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		pa, err := transport.CheckLearnedAddress(e.Address, false, allowLoopback)
		if err != nil {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, sanitizedPeer{node: id, addr: pa.HostPort()})
	}
	return out
}
