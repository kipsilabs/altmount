package service

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/membership"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func TestAuthBridgeVerifyResponseCases(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true, AcceptIncoming: true})
	i.start(t)
	ctx := context.Background()
	ns, err := i.svc.CreateNetwork(ctx, "Club")
	if err != nil {
		t.Fatal(err)
	}
	nw, _ := netid.ParseNetworkID(ns.ID)
	n := i.node(t)
	b := authBridge{mem: n.mem}
	proof := http.Header{}
	if err := b.DecorateOutbound(ctx, nw, n.nodeID, proof); err != nil {
		t.Fatal(err)
	}
	for _, st := range []int{200, 400, 429, 503} {
		if err := b.VerifyResponse(ctx, nw, n.nodeID, st, proof); err != nil {
			t.Errorf("status %d with a valid proof: %v", st, err)
		}
	}
	// Undecorated 403: a plain refusal.
	if err := b.VerifyResponse(ctx, nw, n.nodeID, 403, http.Header{}); !errors.Is(err, membership.ErrRefused) {
		t.Errorf("undecorated 403: %v", err)
	}
	// A 200 (or any non-403) without a proof on a private network fails and is not a refusal.
	for _, st := range []int{200, 400, 503} {
		err := b.VerifyResponse(ctx, nw, n.nodeID, st, http.Header{})
		if err == nil || errors.Is(err, membership.ErrRefused) {
			t.Errorf("status %d without a proof: %v", st, err)
		}
	}
	// A 403 carrying a forged proof is a failure, not a refusal.
	bad := http.Header{membership.ProofHeader: {"AAAA"}}
	if err := b.VerifyResponse(ctx, nw, n.nodeID, 403, bad); err == nil || errors.Is(err, membership.ErrRefused) {
		t.Errorf("forged proof on 403: %v", err)
	}
	// The open network accepts any reply.
	if err := b.VerifyResponse(ctx, netid.Open(), n.nodeID, 403, http.Header{}); err != nil {
		t.Errorf("open network: %v", err)
	}
}

func TestHostSourceReportsErrors(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true})
	i.start(t)
	n := i.node(t)
	ctx := context.Background()
	src := hostSource{mem: n.mem}
	if cs, err := src.Discover(ctx, netid.Open()); err != nil || cs != nil {
		t.Fatalf("open: %v %v", cs, err)
	}
	if cs, err := src.Discover(ctx, netid.NetworkID{7}); err != nil || cs != nil {
		t.Fatalf("unknown network: %v %v", cs, err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := src.Discover(cctx, netid.NetworkID{8}); err == nil {
		t.Fatal("a failing store read was swallowed")
	}
	// A member row with a corrupt host node ID is reported, not skipped.
	id := netid.NetworkID{9}
	_, err := i.db.Connection().Exec(`INSERT INTO peernet_networks (network_id, display_name, role, exposure_level,
		allow_private_addresses, host_address, host_node_id, root_public_key, own_certificate, revocation_list,
		revocation_seq, created_at, updated_at) VALUES (?, 'x', 'member', 0, 0, 'h.example:1', 'zz', x'00', x'00', x'00', 0, 1, 1)`, id.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Discover(ctx, id); err == nil {
		t.Fatal("a corrupt host node id was swallowed")
	}
}
