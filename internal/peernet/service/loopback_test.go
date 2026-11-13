package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Loopback is never a usable host address: refusing it up front stops a host
// from issuing invites nobody can redeem.
func TestLoopbackHostAddressRejected(t *testing.T) {
	ctx := context.Background()
	hostI := newInstance(t, Settings{Enabled: true, AcceptIncoming: true, AdvertiseAddress: "127.0.0.1:8470"})
	hostI.svc.deps.allowLoopback = false
	hostI.start(t)
	ns, err := hostI.svc.CreateNetwork(ctx, "Club")
	if err != nil {
		t.Fatal(err)
	}
	nw, _ := netid.ParseNetworkID(ns.ID)

	addr := "127.0.0.1:9000"
	if _, err := hostI.svc.UpdateNetwork(ctx, nw, NetworkUpdate{HostAddress: &addr}); !errors.Is(err, transport.ErrLoopbackRefused) {
		t.Fatalf("update host address: %v", err)
	}
	// The advertise address is the default host address: also refused.
	if _, err := hostI.svc.CreateInvite(ctx, nw, time.Hour, ""); !errors.Is(err, transport.ErrLoopbackRefused) {
		t.Fatalf("invite with loopback advertise: %v", err)
	}

	// A link naming a loopback host is refused before any dial.
	other, _ := listeningInstance(t, Settings{})
	ons, _ := other.svc.CreateNetwork(ctx, "Other")
	onw, _ := netid.ParseNetworkID(ons.ID)
	link, err := other.svc.CreateInvite(ctx, onw, time.Hour, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hostI.svc.JoinNetwork(ctx, link, true); !errors.Is(err, transport.ErrLoopbackRefused) {
		t.Fatalf("join loopback link: %v", err)
	}
}
