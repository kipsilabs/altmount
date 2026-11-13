package membership

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Route patterns and request body caps registered by RegisterHandlers.
const (
	RouteRedeem       = "POST /peernet/v1/membership/redeem"
	RouteRenew        = "POST /peernet/v1/membership/renew"
	RouteRevocations  = "GET /peernet/v1/membership/revocations"
	PathRedeem        = "/peernet/v1/membership/redeem"
	PathRenew         = "/peernet/v1/membership/renew"
	PathRevocations   = "/peernet/v1/membership/revocations"
	MaxRedeemBody     = 128
	MaxRenewBody      = 128
	MaxRevocationBody = 0
)

// Registrar is satisfied by *transport.Server.
type Registrar interface {
	Handle(pattern string, maxBody int64, h http.Handler) error
}

// RegisterHandlers registers the host-side membership routes. It does not
// start a server.
func (m *Manager) RegisterHandlers(r Registrar) error {
	for _, rt := range []struct {
		pattern string
		max     int64
		h       http.HandlerFunc
	}{
		{RouteRedeem, MaxRedeemBody, m.handleRedeem},
		{RouteRenew, MaxRenewBody, m.handleRenew},
		{RouteRevocations, MaxRevocationBody, m.handleRevocations},
	} {
		if err := r.Handle(rt.pattern, rt.max, rt.h); err != nil {
			return err
		}
	}
	return nil
}

// replyFail writes a small fixed error that reveals nothing.
func replyFail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, msg+"\n")
}

func replyDenied(w http.ResponseWriter) { replyFail(w, http.StatusForbidden, "denied") }

func replyBinary(w http.ResponseWriter, b []byte) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(b)
}

func (m *Manager) handleRedeem(w http.ResponseWriter, r *http.Request) {
	peer, ok := transport.PeerFromContext(r.Context())
	if !ok {
		replyDenied(w)
		return
	}
	if !m.limiter.allow(r.RemoteAddr) {
		replyFail(w, http.StatusTooManyRequests, "rate limited")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		replyDenied(w)
		return
	}
	req, err := parseRedeemRequest(body)
	if err != nil {
		replyDenied(w)
		return
	}
	resp, err := m.redeem(r.Context(), req, peer)
	if err != nil {
		replyDenied(w) // identical for unknown network, bad code, expiry, reuse
		return
	}
	replyBinary(w, resp)
}

func (m *Manager) handleRenew(w http.ResponseWriter, r *http.Request) {
	peer, ok := transport.PeerFromContext(r.Context())
	if !ok {
		replyDenied(w)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		replyDenied(w)
		return
	}
	id, err := parseRenewRequest(body)
	if err != nil {
		replyDenied(w)
		return
	}
	resp, err := m.renew(r.Context(), id, peer)
	if err != nil {
		replyDenied(w)
		return
	}
	replyBinary(w, resp)
}

func (m *Manager) handleRevocations(w http.ResponseWriter, r *http.Request) {
	peer, ok := transport.PeerFromContext(r.Context())
	if !ok {
		replyDenied(w)
		return
	}
	id, err := netid.ParseNetworkID(r.URL.Query().Get("n"))
	if err != nil {
		replyDenied(w)
		return
	}
	n, err := m.hostNetwork(r.Context(), id)
	if err != nil {
		replyDenied(w)
		return
	}
	// Only nodes the host knows (current or removed) may fetch the list.
	if _, err := m.cfg.Store.GetMember(r.Context(), n.NetworkID, peer.String()); err != nil {
		replyDenied(w)
		return
	}
	replyBinary(w, n.RevocationList)
}

// redeem consumes an invite for peer and returns the encoded response. Every
// failure returns the same sentinel so callers cannot distinguish them.
func (m *Manager) redeem(ctx context.Context, req redeemRequest, peer identity.NodeID) ([]byte, error) {
	n, err := m.hostNetwork(ctx, req.Network)
	if err != nil {
		return nil, ErrDenied
	}
	key, err := m.hostKey(req.Network)
	if err != nil {
		return nil, ErrDenied
	}
	hash := req.Code.Hash()
	inv, err := m.cfg.Store.GetInvite(ctx, n.NetworkID, hash)
	if err != nil || !hashEqual(inv.CodeHash, hash) {
		return nil, ErrDenied
	}
	now := m.now()
	cert, err := IssueCertificate(key, req.Network, peer, RoleMember, now, CertLifetime)
	if err != nil {
		return nil, ErrDenied
	}
	if err := m.cfg.Store.RedeemInvite(ctx, n.NetworkID, hash, peer.String(), now.Unix(), cert.IssuedAt, cert.ExpiresAt); err != nil {
		return nil, ErrDenied
	}
	fresh, err := m.cfg.Store.GetNetwork(ctx, n.NetworkID)
	if err != nil {
		return nil, ErrDenied
	}
	return joinResponse{Name: fresh.DisplayName, Cert: cert.Marshal(), Revocation: fresh.RevocationList}.marshal(), nil
}

// renew re-issues a member's certificate. The caller must be a current,
// non-revoked member; a certificate that expired within RenewGrace still
// renews, a revoked member never does.
func (m *Manager) renew(ctx context.Context, id netid.NetworkID, peer identity.NodeID) ([]byte, error) {
	n, err := m.hostNetwork(ctx, id)
	if err != nil {
		return nil, ErrDenied
	}
	mem, err := m.cfg.Store.GetMember(ctx, n.NetworkID, peer.String())
	if err != nil || mem.RevokedAt != 0 {
		return nil, ErrDenied
	}
	now := m.now()
	if now.Unix() > mem.CertExpiresAt+int64(RenewGrace.Seconds()) {
		return nil, ErrDenied
	}
	key, err := m.hostKey(id)
	if err != nil {
		return nil, ErrDenied
	}
	cert, err := IssueCertificate(key, id, peer, RoleMember, now, CertLifetime)
	if err != nil {
		return nil, ErrDenied
	}
	if err := m.cfg.Store.SetMemberCertificate(ctx, n.NetworkID, peer.String(), cert.IssuedAt, cert.ExpiresAt); err != nil {
		if errors.Is(err, database.ErrPeernetNotFound) {
			return nil, ErrDenied // revoked concurrently
		}
		return nil, ErrDenied
	}
	return joinResponse{Name: n.DisplayName, Cert: cert.Marshal(), Revocation: n.RevocationList}.marshal(), nil
}
