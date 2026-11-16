package membership

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net/http"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// ProofHeader carries a membership certificate on requests and responses.
const ProofHeader = "X-Peernet-Cert"

// maxProofHeaderLen caps the encoded header (the certificate is ~196 chars).
const maxProofHeaderLen = 400

// ErrRefused is returned by RefusalOrError when a peer answered 403 without any
// membership proof: it declined the request (unknown network, not a member
// from its point of view). Nothing in such a reply is trusted.
var ErrRefused = errors.New("membership: peer refused the request")

// RefusalOrError classifies a failed VerifyResponse. A 403 that carries no
// proof header at all is a plain refusal (the discovery server answers a denied
// or unknown-network request that way, undecorated); everything else, such as
// a 200 without proof or any reply with an invalid proof, stays the original
// verification failure.
func RefusalOrError(err error, status int, h http.Header) error {
	if err != nil && status == http.StatusForbidden && len(h.Values(ProofHeader)) == 0 {
		return ErrRefused
	}
	return err
}

// ErrNoProof is returned when a request or response carries no valid proof.
var ErrNoProof = errors.New("membership: missing or invalid membership proof")

// Authorizer proves and checks network membership on individual calls, so
// callers treat open and private networks uniformly.
type Authorizer interface {
	NetworkID() netid.NetworkID
	// AuthorizeIncoming checks the proof on an incoming request from the
	// authenticated transport peer.
	AuthorizeIncoming(ctx context.Context, h http.Header, peer identity.NodeID) error
	// DecorateResponse attaches this node's proof to a response header. Call
	// it before writing the response body.
	DecorateResponse(ctx context.Context, h http.Header) error
	// PrepareOutgoing attaches this node's proof to an outgoing request.
	PrepareOutgoing(ctx context.Context, h http.Header) error
	// VerifyResponse checks the proof on a response from server (the node ID
	// the call was pinned to) before the body is trusted.
	VerifyResponse(ctx context.Context, h http.Header, server identity.NodeID) error
}

// OpenAuthorizer admits everyone and attaches nothing.
type OpenAuthorizer struct{}

func (OpenAuthorizer) NetworkID() netid.NetworkID { return netid.Open() }
func (OpenAuthorizer) AuthorizeIncoming(context.Context, http.Header, identity.NodeID) error {
	return nil
}
func (OpenAuthorizer) DecorateResponse(context.Context, http.Header) error { return nil }
func (OpenAuthorizer) PrepareOutgoing(context.Context, http.Header) error  { return nil }
func (OpenAuthorizer) VerifyResponse(context.Context, http.Header, identity.NodeID) error {
	return nil
}

// Authorizer returns the authorizer for a network: the open implementation
// for the open network, the certificate-checking one for a known private
// network, ErrUnknownNetwork otherwise.
func (m *Manager) Authorizer(ctx context.Context, id netid.NetworkID) (Authorizer, error) {
	if id.IsOpen() {
		return OpenAuthorizer{}, nil
	}
	n, err := m.GetNetwork(ctx, id)
	if err != nil {
		return nil, err
	}
	if n.Role == database.PeernetRoleOpen {
		return OpenAuthorizer{}, nil
	}
	return &privateAuthorizer{m: m, id: id}, nil
}

type privateAuthorizer struct {
	m  *Manager
	id netid.NetworkID
}

func (a *privateAuthorizer) NetworkID() netid.NetworkID { return a.id }

func (a *privateAuthorizer) load(ctx context.Context) (*Network, ed25519.PublicKey, error) {
	n, err := a.m.GetNetwork(ctx, a.id)
	if err != nil {
		return nil, nil, err
	}
	root := ed25519.PublicKey(n.RootPublicKey)
	if len(root) != ed25519.PublicKeySize || !netid.FromRootKey(root).Equal(a.id) {
		return nil, nil, errMissingRootKeyRow
	}
	return n, root, nil
}

// verify checks the proof in h against the network's root key and revocation
// list, bound to peer.
func (a *privateAuthorizer) verify(ctx context.Context, h http.Header, peer identity.NodeID) error {
	vals := h.Values(ProofHeader)
	if len(vals) != 1 || len(vals[0]) == 0 || len(vals[0]) > maxProofHeaderLen {
		return ErrNoProof
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(vals[0])
	if err != nil {
		return ErrNoProof
	}
	cert, err := ParseCertificate(raw)
	if err != nil {
		return ErrNoProof
	}
	n, root, err := a.load(ctx)
	if err != nil {
		return err
	}
	if n.RevokedAt != 0 {
		return ErrRevoked
	}
	if err := cert.Verify(root, peer, a.m.now()); err != nil {
		return err
	}
	list, err := ParseRevocationList(n.RevocationList)
	if err != nil || list.Verify(root, a.m.now()) != nil {
		return ErrNoProof // fail closed without a usable list
	}
	if list.Contains(peer) {
		return ErrRevoked
	}
	return nil
}

func (a *privateAuthorizer) AuthorizeIncoming(ctx context.Context, h http.Header, peer identity.NodeID) error {
	return a.verify(ctx, h, peer)
}

func (a *privateAuthorizer) VerifyResponse(ctx context.Context, h http.Header, server identity.NodeID) error {
	return a.verify(ctx, h, server)
}

func (a *privateAuthorizer) attach(ctx context.Context, h http.Header) error {
	n, root, err := a.load(ctx)
	if err != nil {
		return err
	}
	if n.RevokedAt != 0 {
		return ErrRevoked // removed by the host: no proof is offered
	}
	var cert *Certificate
	if n.Role == database.PeernetRoleHost {
		if cert, err = a.m.ensureHostCert(ctx, n); err != nil {
			return err
		}
	} else if cert, err = ParseCertificate(n.OwnCertificate); err != nil {
		return ErrNoValidCert
	}
	if cert.NodeID != a.m.cfg.NodeID || cert.verifyWindow(root, a.m.now()) != nil {
		return ErrNoValidCert
	}
	h.Set(ProofHeader, base64.RawURLEncoding.EncodeToString(cert.Marshal()))
	return nil
}

func (a *privateAuthorizer) DecorateResponse(ctx context.Context, h http.Header) error {
	return a.attach(ctx, h)
}

func (a *privateAuthorizer) PrepareOutgoing(ctx context.Context, h http.Header) error {
	return a.attach(ctx, h)
}

// Protect wraps h so only callers proving membership reach it: it verifies
// the caller's certificate against the authenticated transport peer, attaches
// this node's certificate to the response, and otherwise answers a fixed 403.
// For the open authorizer it admits everyone.
func Protect(a Authorizer, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer, ok := transport.PeerFromContext(r.Context())
		if !ok || a.AuthorizeIncoming(r.Context(), r.Header, peer) != nil {
			replyFail(w, http.StatusForbidden, "forbidden")
			return
		}
		if a.DecorateResponse(r.Context(), w.Header()) != nil {
			replyFail(w, http.StatusServiceUnavailable, "unavailable")
			return
		}
		h.ServeHTTP(w, r)
	})
}
