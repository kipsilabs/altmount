package discovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// errPeerStatus is returned for a non-200 answer.
var errPeerStatus = errors.New("discovery: unexpected peer status")

// call makes one authorised, network-scoped request and returns the (capped)
// response. The membership proof is verified first, whatever the status, and
// only then may the caller interpret the status. trusted marks addr as local
// configuration (see callOpts). The caller owns the body.
func (s *Service) call(ctx context.Context, nc NetworkConfig, addr string, node identity.NodeID, method, path string, body []byte, maxResp int64, trusted bool) (*transport.Response, error) {
	hdr := http.Header{}
	if len(body) > 0 {
		hdr.Set("Content-Type", "application/json")
	}
	if err := s.auth.DecorateOutbound(ctx, nc.ID, node, hdr); err != nil {
		return nil, fmt.Errorf("discovery: decorate request: %w", err)
	}
	resp, err := s.cfg.Dialer.Do(ctx, addr, node, transport.Request{
		Method: method, Path: path, Query: url.Values{queryNet: {nc.ID.String()}}, Header: hdr, Body: body,
	}, s.callOpts(nc, maxResp, trusted))
	if err != nil {
		return nil, err
	}
	if err := s.auth.VerifyResponse(ctx, nc.ID, node, resp.StatusCode, resp.Header); err != nil {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("discovery: verify response: %w", err)
	}
	return resp, nil
}

// Hello tells the peer our advertised address for the network. It is a no-op
// when the network has none, and when the same address was acknowledged by
// this peer within HelloInterval. A rate-limited reply is not an error. The
// address is treated as peer-learned (not trusted configuration).
func (s *Service) Hello(ctx context.Context, nc NetworkConfig, addr string, node identity.NodeID) error {
	return s.hello(ctx, nc, addr, node, false)
}

func (s *Service) hello(ctx context.Context, nc NetworkConfig, addr string, node identity.NodeID, trusted bool) error {
	if nc.AdvertiseAddr == "" {
		return nil
	}
	if s.helloAcked(nc.ID.String(), node, nc.AdvertiseAddr) {
		return nil
	}
	resp, err := s.call(ctx, nc, addr, node, http.MethodPost, pathHello, encodeHello(nc.AdvertiseAddr), maxHelloReplyBytes, trusted)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusOK:
		s.ackHello(nc.ID.String(), node, nc.AdvertiseAddr)
		return nil
	case http.StatusTooManyRequests:
		return nil
	}
	return fmt.Errorf("%w: hello %d", errPeerStatus, resp.StatusCode)
}

func ackKey(network string, node identity.NodeID) string { return network + "|" + node.String() }

// helloAcked reports whether node acknowledged exactly this advertised
// address within HelloInterval.
func (s *Service) helloAcked(network string, node identity.NodeID, addr string) bool {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.acks.get(ackKey(network, node))
	return ok && a.addr == addr && now.Sub(a.at) < s.limits.HelloInterval
}

func (s *Service) ackHello(network string, node identity.NodeID, addr string) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acks.put(ackKey(network, node), helloAck{addr: addr, at: now})
}

// Exchange asks a peer for its verified peers and stores what passes
// validation as unverified exchange entries. It returns how many new entries
// were stored. The peer's address is treated as peer-learned.
func (s *Service) Exchange(ctx context.Context, nc NetworkConfig, addr string, node identity.NodeID) (int, error) {
	return s.exchange(ctx, nc, addr, node, false)
}

func (s *Service) exchange(ctx context.Context, nc NetworkConfig, addr string, node identity.NodeID, trusted bool) (int, error) {
	resp, err := s.call(ctx, nc, addr, node, http.MethodGet, pathPeers, nil, MaxPeersResponseBytes, trusted)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return 0, fmt.Errorf("%w: peers %d", errPeerStatus, resp.StatusCode)
	}
	raw, err := resp.ReadAll()
	if err != nil {
		return 0, err
	}
	entries, err := DecodePeers(raw)
	if err != nil {
		return 0, err
	}
	return s.intake(ctx, nc, sanitizeEntries(entries, s.cfg.Self, node, s.cfg.AllowLoopbackForTesting))
}
