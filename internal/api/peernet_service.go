package api

import (
	"context"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/service"
)

// PeernetService is the narrow view of the peer network the API needs. It is
// implemented by *service.Service.
type PeernetService interface {
	Status(ctx context.Context) (service.Status, error)
	Peers(ctx context.Context, id netid.NetworkID) ([]discovery.PeerStatus, error)
	ConfirmPeer(ctx context.Context, id netid.NetworkID, address string) error
	CreateNetwork(ctx context.Context, name string) (service.NetworkStatus, error)
	JoinNetwork(ctx context.Context, link string, allowPrivate bool) (service.NetworkStatus, error)
	UpdateNetwork(ctx context.Context, id netid.NetworkID, u service.NetworkUpdate) (service.NetworkStatus, error)
	DeleteNetwork(ctx context.Context, id netid.NetworkID) error
	ListInvites(ctx context.Context, id netid.NetworkID) ([]service.Invite, error)
	CreateInvite(ctx context.Context, id netid.NetworkID, ttl time.Duration, alias string) (string, error)
	RevokeInvite(ctx context.Context, id netid.NetworkID, inviteID string) error
	ListMembers(ctx context.Context, id netid.NetworkID) ([]service.Member, error)
	RemoveMember(ctx context.Context, id netid.NetworkID, node identity.NodeID) error
}

// SetPeernetService wires the peer network service into the API.
func (s *Server) SetPeernetService(p PeernetService) {
	s.peernet = p
}
