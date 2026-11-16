package discovery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/netip"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// RegisterRoutes adds the discovery routes to srv, each with a request body
// cap. It does not start the server.
func RegisterRoutes(srv *transport.Server, s *Service) error {
	if err := srv.Handle(RouteHello, MaxHelloBody, http.HandlerFunc(s.handleHello)); err != nil {
		return err
	}
	return srv.Handle(RoutePeers, 0, http.HandlerFunc(s.handlePeers))
}

// replyText writes a small fixed text answer. Once a request is authorised
// (dec non-nil) every answer, whatever its status, carries the authoriser's
// headers, so the caller can verify the membership proof before it interprets
// the status. Pre-authorisation answers pass a nil decorator.
func replyText(w http.ResponseWriter, dec ResponseDecorator, code int, msg string) {
	h := w.Header()
	if dec != nil {
		dec(h)
	}
	// Set after the decorator so it cannot change how the body is read.
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, msg+"\n")
}

func replyJSON(w http.ResponseWriter, dec ResponseDecorator, body []byte) {
	h := w.Header()
	if dec != nil {
		dec(h)
	}
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// admit resolves the caller and network and runs the authoriser. On failure
// it has already answered. An unknown network and a network that denies the
// caller get the identical answer, so the route does not reveal which network
// IDs this node serves.
func (s *Service) admit(w http.ResponseWriter, r *http.Request) (identity.NodeID, NetworkConfig, ResponseDecorator, bool) {
	var zero NetworkConfig
	peer, ok := transport.PeerFromContext(r.Context())
	if !ok {
		replyText(w, nil, http.StatusUnauthorized, "unauthorized")
		return identity.NodeID{}, zero, nil, false
	}
	vals := r.URL.Query()[queryNet]
	if len(vals) != 1 {
		replyText(w, nil, http.StatusBadRequest, "bad request")
		return peer, zero, nil, false
	}
	id, err := netid.ParseNetworkID(vals[0])
	if err != nil {
		replyText(w, nil, http.StatusBadRequest, "bad request")
		return peer, zero, nil, false
	}
	nc, ok := s.network(id.String())
	if !ok {
		replyText(w, nil, http.StatusForbidden, "forbidden")
		return peer, zero, nil, false
	}
	dec, err := s.auth.AuthorizeInbound(r.Context(), id, peer, r.Header.Clone())
	if err != nil {
		replyText(w, nil, http.StatusForbidden, "forbidden")
		return peer, zero, nil, false
	}
	return peer, nc, dec, true
}

func (s *Service) handleHello(w http.ResponseWriter, r *http.Request) {
	peer, nc, dec, ok := s.admit(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(r.Body) // already capped by the transport route
	if err != nil {
		replyText(w, dec, http.StatusBadRequest, "bad request")
		return
	}
	adv, err := DecodeHello(body)
	if err != nil {
		replyText(w, dec, http.StatusBadRequest, "bad request")
		return
	}
	if adv == "" {
		replyJSON(w, dec, []byte(`{"status":"ok"}`))
		return
	}
	ip, ok := transport.RemoteIPFromContext(r.Context())
	if !ok {
		replyText(w, dec, http.StatusBadRequest, "bad request")
		return
	}
	// Every attempt with an address uses up the peer's allowance, valid or not.
	if !s.helloAllowed(nc.ID.String(), peer, ip, s.now()) {
		replyText(w, dec, http.StatusTooManyRequests, "rate limited")
		return
	}
	addr, err := s.helloAddress(nc, adv, ip)
	if err != nil {
		replyText(w, dec, http.StatusBadRequest, "bad request")
		return
	}
	key := "hello store " + nc.ID.String()[:8]
	if err := s.recordAdvertised(r.Context(), nc, peer, addr); err != nil {
		if !errors.Is(err, database.ErrPeernetPeerCapReached) {
			s.logChange(r.Context(), key, err)
		}
		replyText(w, dec, http.StatusServiceUnavailable, "unavailable")
		return
	}
	s.logChange(r.Context(), key, nil)
	replyJSON(w, dec, []byte(`{"status":"queued"}`))
}

// helloAddress turns an advertised address into what is stored: the TCP
// remote IP of the authenticated connection with the advertised port. The
// advertised host is never trusted (it could name a third party), so only the
// port is taken from the peer. The result must pass the network's dial policy
// and not use a denied service port.
func (s *Service) helloAddress(nc NetworkConfig, advertised string, remote netip.Addr) (string, error) {
	pa, err := transport.ValidateAddress(advertised)
	if err != nil {
		return "", err
	}
	if transport.PortDenied(pa.Port) {
		return "", transport.ErrAddressRefused
	}
	bound, err := transport.CheckPeerAddress(netip.AddrPortFrom(remote, pa.Port).String(), nc.AllowPrivate, s.cfg.AllowLoopbackForTesting)
	if err != nil {
		return "", err
	}
	return bound.HostPort(), nil
}

// recordAdvertised stores an address a peer stated for itself. It is never
// verified here and never shared until a later outbound dial proves the same
// Node ID there.
func (s *Service) recordAdvertised(ctx context.Context, nc NetworkConfig, peer identity.NodeID, addr string) error {
	if peer.Equal(s.cfg.Self) {
		return nil
	}
	net, node, now := nc.ID.String(), peer.String(), s.now()
	existing, err := s.cfg.Store.Get(ctx, net, node)
	if err != nil {
		return err
	}
	if existing == nil {
		_, err := s.cfg.Store.Insert(ctx, database.PeernetPeer{
			NetworkID: net, NodeID: node, Address: addr, Source: database.PeernetSourceInbound,
			FirstSeen: now, LastSeen: now,
		}, s.caps())
		return err
	}
	switch {
	case existing.Source == database.PeernetSourceUser:
		return nil // the user's pin wins
	case existing.Address == addr:
		return s.cfg.Store.Touch(ctx, net, node, now)
	case !existing.Verified:
		_, err := s.cfg.Store.SetUnverifiedAddress(ctx, net, node, addr, now)
		return err
	default:
		return s.cfg.Store.SetCandidateAddress(ctx, net, node, addr, now)
	}
}

func (s *Service) handlePeers(w http.ResponseWriter, r *http.Request) {
	peer, nc, dec, ok := s.admit(w, r)
	if !ok {
		return
	}
	key := "list verified " + nc.ID.String()[:8]
	rows, err := s.cfg.Store.ListVerified(r.Context(), nc.ID.String(), s.limits.ExchangeMaxFailures, s.limits.ExchangeSampleSize*4)
	if err != nil {
		s.logChange(r.Context(), key, err)
		replyText(w, dec, http.StatusInternalServerError, "error")
		return
	}
	s.logChange(r.Context(), key, nil)
	out := make([]PeerEntry, 0, s.limits.ExchangeSampleSize)
	for _, p := range rows {
		if len(out) >= s.limits.ExchangeSampleSize {
			break
		}
		id, err := identity.ParseNodeID(p.NodeID)
		if err != nil || id.Equal(peer) || id.Equal(s.cfg.Self) {
			continue
		}
		// Only public IP literals on ordinary ports are relayed. A hostname
		// could be repointed by DNS at a third party after it was verified,
		// and private addresses must not leave this node.
		if _, err := transport.CheckLearnedAddress(p.Address, false, s.cfg.AllowLoopbackForTesting); err != nil {
			continue
		}
		out = append(out, PeerEntry{NodeID: p.NodeID, Address: p.Address})
	}
	body, err := encodePeers(out)
	if err != nil || len(body) > MaxPeersResponseBytes {
		replyText(w, dec, http.StatusInternalServerError, "error")
		return
	}
	replyJSON(w, dec, body)
}
