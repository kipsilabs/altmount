package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/membership"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// authBridge adapts membership's per-network Authorizer to the interface
// discovery expects. The open network admits everyone; a private network
// requires a valid membership certificate; an unknown network is refused.
type authBridge struct {
	mem *membership.Manager
	// onRefused, when set, is told that a network's host refused a request
	// without any membership proof (it may have removed this node).
	onRefused func(ctx context.Context, network netid.NetworkID)
}

var _ discovery.Authorizer = authBridge{}

func (b authBridge) AuthorizeInbound(ctx context.Context, network netid.NetworkID, peer identity.NodeID, reqHeader http.Header) (discovery.ResponseDecorator, error) {
	a, err := b.mem.Authorizer(ctx, network)
	if err != nil {
		return nil, err
	}
	if err := a.AuthorizeIncoming(ctx, reqHeader, peer); err != nil {
		return nil, err
	}
	// Build the response headers now so a failure denies the request instead
	// of answering without the proof the caller will insist on.
	extra := http.Header{}
	if err := a.DecorateResponse(ctx, extra); err != nil {
		return nil, err
	}
	return func(h http.Header) {
		for k, vs := range extra {
			h[k] = append([]string(nil), vs...)
		}
	}, nil
}

func (b authBridge) DecorateOutbound(ctx context.Context, network netid.NetworkID, _ identity.NodeID, reqHeader http.Header) error {
	a, err := b.mem.Authorizer(ctx, network)
	if err != nil {
		return err
	}
	return a.PrepareOutgoing(ctx, reqHeader)
}

// VerifyResponse checks the membership proof on every reply, whatever its
// status: an authorised caller gets the proof on error replies too (400, 429,
// 503), so those verify. A 403 without any proof is the undecorated refusal of
// a denied caller or an unknown network and is reported as membership.ErrRefused;
// a reply (any status) with a missing or invalid proof on a private network
// stays a verification failure, so a 200 without a proof is never accepted.
func (b authBridge) VerifyResponse(ctx context.Context, network netid.NetworkID, peer identity.NodeID, status int, respHeader http.Header) error {
	a, err := b.mem.Authorizer(ctx, network)
	if err != nil {
		return err
	}
	err = membership.RefusalOrError(a.VerifyResponse(ctx, respHeader, peer), status, respHeader)
	if errors.Is(err, membership.ErrRefused) && b.onRefused != nil {
		b.onRefused(ctx, network)
	}
	return err
}

// hostSource makes the host of every joined private network known to
// discovery, so a member always has the host as a peer to contact.
type hostSource struct{ mem *membership.Manager }

func (hostSource) Name() string { return "network-host" }

func (h hostSource) Discover(ctx context.Context, network netid.NetworkID) ([]discovery.Candidate, error) {
	if network.IsOpen() {
		return nil, nil
	}
	n, err := h.mem.GetNetwork(ctx, network)
	switch {
	case errors.Is(err, membership.ErrUnknownNetwork):
		return nil, nil // the network is gone; nothing to offer
	case err != nil:
		return nil, fmt.Errorf("read network for its host: %w", err)
	}
	if n.Role != database.PeernetRoleMember || n.HostAddress == "" || n.RevokedAt != 0 {
		return nil, nil
	}
	node, err := identity.ParseNodeID(n.HostNodeID)
	if err != nil {
		return nil, fmt.Errorf("stored host node id: %w", err)
	}
	return []discovery.Candidate{{NodeID: node, Address: n.HostAddress}}, nil
}
