package service

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func memberStatus(t *testing.T, i *instance, nw netid.NetworkID) NetworkStatus {
	t.Helper()
	st, err := i.svc.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range st.Networks {
		if n.ID == nw.String() {
			return n
		}
	}
	t.Fatalf("network %s not in status", nw)
	return NetworkStatus{}
}

// Removal takes effect at once on the host (no verified peer row for the
// removed node) and a member learns of it from the first refusal.
func TestRemovedMemberIsSeenOnBothSides(t *testing.T) {
	ctx := context.Background()
	host, _ := listeningInstance(t, Settings{})
	member, _ := listeningInstance(t, Settings{})
	ns, err := host.svc.CreateNetwork(ctx, "Club")
	if err != nil {
		t.Fatal(err)
	}
	nw, _ := netid.ParseNetworkID(ns.ID)
	if _, err := host.svc.UpdateNetwork(ctx, nw, NetworkUpdate{Level: ptr(1)}); err != nil {
		t.Fatal(err)
	}
	link, err := host.svc.CreateInvite(ctx, nw, time.Hour, "m")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.svc.JoinNetwork(ctx, link, false); err != nil {
		t.Fatal(err)
	}
	if _, err := member.svc.UpdateNetwork(ctx, nw, NetworkUpdate{Level: ptr(1)}); err != nil {
		t.Fatal(err)
	}
	for r := 0; r < 3; r++ {
		member.node(t).step(ctx)
		host.node(t).step(ctx)
	}
	if !host.peerVerified(t, nw, member.id(t)) || !member.peerVerified(t, nw, host.id(t)) {
		t.Fatal("pair did not connect")
	}
	if memberStatus(t, member, nw).Revoked {
		t.Fatal("revoked before removal")
	}

	if err := host.svc.RemoveMember(ctx, nw, member.id(t)); err != nil {
		t.Fatal(err)
	}
	// Host side, immediately: the removed node is not shown as a peer.
	if host.peerVerified(t, nw, member.id(t)) {
		t.Fatal("host still shows the removed member as a verified peer")
	}
	ps, _ := host.svc.Peers(ctx, nw)
	for _, p := range ps {
		if p.NodeID == member.id(t).String() {
			t.Fatalf("host still lists the removed member: %+v", p)
		}
	}

	// Member side: the next round runs into the refusal and records it.
	nc := discovery.NetworkConfig{ID: nw}
	if _, err := member.node(t).disc.Exchange(ctx, nc, host.addr(t), host.id(t)); err == nil {
		t.Fatal("removed member still admitted")
	}
	eventually(t, "member to record its removal", func() bool { return memberStatus(t, member, nw).Revoked })
	got := memberStatus(t, member, nw)
	if got.RevokedAt.IsZero() || got.Peers.Active != 0 {
		t.Fatalf("status = %+v", got)
	}
	member.node(t).mu.Lock()
	_, active := member.node(t).active[nw.String()]
	member.node(t).mu.Unlock()
	if active {
		t.Fatal("removed network still used for peer traffic")
	}
	// It can be left.
	if err := member.svc.DeleteNetwork(ctx, nw); err != nil {
		t.Fatal(err)
	}
}
