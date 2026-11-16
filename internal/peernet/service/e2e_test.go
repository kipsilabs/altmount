package service

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// listeningInstance starts an instance that accepts connections and
// advertises its real loopback address.
func listeningInstance(t *testing.T, base Settings) (*instance, Settings) {
	t.Helper()
	base.Enabled, base.AcceptIncoming = true, true
	i := newInstance(t, base)
	i.start(t)
	base.AdvertiseAddress = i.addr(t)
	base.ListenPort = i.port(t)
	i.svc.Apply(context.Background(), base)
	return i, base
}

func (i *instance) id(t *testing.T) identity.NodeID {
	id, err := identity.ParseNodeID(i.node(t).nodeID.String())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (i *instance) peerVerified(t *testing.T, nw netid.NetworkID, other identity.NodeID) bool {
	ps, err := i.svc.Peers(context.Background(), nw)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps {
		if p.NodeID == other.String() && p.Verified {
			return true
		}
	}
	return false
}

func TestPrivateNetworkEndToEnd(t *testing.T) {
	ctx := context.Background()
	host, _ := listeningInstance(t, Settings{})
	member, _ := listeningInstance(t, Settings{})
	outsider, _ := listeningInstance(t, Settings{})

	ns, err := host.svc.CreateNetwork(ctx, "Club")
	if err != nil {
		t.Fatal(err)
	}
	nw, _ := netid.ParseNetworkID(ns.ID)
	if ns.Role != "host" || ns.Kind != KindPrivate || ns.CertExpiresAt.IsZero() {
		t.Fatalf("created = %+v", ns)
	}
	if _, err := host.svc.UpdateNetwork(ctx, nw, NetworkUpdate{Level: ptr(1)}); err != nil {
		t.Fatal(err)
	}
	link, err := host.svc.CreateInvite(ctx, nw, 1_000_000_000*3600, "bob") // host address defaults from advertise
	if err != nil || !strings.HasPrefix(link, "altmountnet://") {
		t.Fatalf("invite: %q %v", link, err)
	}
	invs, _ := host.svc.ListInvites(ctx, nw)
	if len(invs) != 1 || invs[0].Alias != "bob" || strings.Contains(invs[0].ID, link) {
		t.Fatalf("invites = %+v", invs)
	}

	joined, err := member.svc.JoinNetwork(ctx, link, false)
	if err != nil || joined.Role != "member" || joined.CertExpiresAt.IsZero() {
		t.Fatalf("join: %+v %v", joined, err)
	}
	if _, err := member.svc.JoinNetwork(ctx, link, false); err == nil {
		t.Fatal("second join with a used invite succeeded")
	}
	if _, err := member.svc.UpdateNetwork(ctx, nw, NetworkUpdate{Level: ptr(1)}); err != nil {
		t.Fatal(err)
	}
	members, _ := host.svc.ListMembers(ctx, nw)
	if len(members) != 1 || members[0].NodeID != member.id(t).String() {
		t.Fatalf("members = %+v", members)
	}

	// Ticks: the member says hello, the host dials back, the member contacts the host.
	for r := 0; r < 3; r++ {
		member.node(t).step(ctx)
		host.node(t).step(ctx)
	}
	if !member.peerVerified(t, nw, host.id(t)) {
		t.Fatal("member does not see the host as verified peer")
	}
	if !host.peerVerified(t, nw, member.id(t)) {
		t.Fatal("host does not see the member as verified peer")
	}

	// A non-member is refused on the private network's discovery route.
	hostAddr := host.addr(t)
	call := func(i *instance) int {
		resp, err := i.node(t).client.Do(ctx, hostAddr, host.id(t), transport.Request{
			Method: http.MethodGet, Path: "/peer/v1/peers", Query: url.Values{"n": {nw.String()}},
		}, transport.CallOptions{MaxResponseBytes: 1 << 16})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = resp.ReadAll()
		return resp.StatusCode
	}
	if code := call(outsider); code != http.StatusForbidden {
		t.Fatalf("outsider status = %d", code)
	}
	// An unknown network is refused too.
	resp, err := outsider.node(t).client.Do(ctx, hostAddr, host.id(t), transport.Request{
		Method: http.MethodGet, Path: "/peer/v1/peers", Query: url.Values{"n": {strings.Repeat("ab", 32)}},
	}, transport.CallOptions{MaxResponseBytes: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = resp.ReadAll()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("unknown network answered")
	}

	// The member itself is admitted until removed, then refused.
	nc := discovery.NetworkConfig{ID: nw}
	if _, err := member.node(t).disc.Exchange(ctx, nc, hostAddr, host.id(t)); err != nil {
		t.Fatalf("member exchange before removal: %v", err)
	}
	if err := host.svc.RemoveMember(ctx, nw, member.id(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := member.node(t).disc.Exchange(ctx, nc, hostAddr, host.id(t)); err == nil {
		t.Fatal("removed member still admitted")
	}

	// Deleting the network and leaving it.
	if err := member.svc.DeleteNetwork(ctx, nw); err != nil {
		t.Fatal(err)
	}
	if err := host.svc.DeleteNetwork(ctx, nw); err != nil {
		t.Fatal(err)
	}
	if err := host.svc.DeleteNetwork(ctx, netid.Open()); err != ErrOpenNetwork {
		t.Fatalf("delete open = %v", err)
	}
	if _, err := host.svc.UpdateNetwork(ctx, netid.Open(), NetworkUpdate{Level: ptr(2)}); err != ErrOpenNetwork {
		t.Fatalf("update open = %v", err)
	}
}

func ptr[T any](v T) *T { return &v }

func TestOpenNetworkPairWithUserAddress(t *testing.T) {
	ctx := context.Background()
	b, _ := listeningInstance(t, Settings{OpenLevel: 1})
	a, aset := listeningInstance(t, Settings{OpenLevel: 1})
	aset.OpenPeers = []string{b.addr(t)}
	a.svc.Apply(ctx, aset)

	eventually(t, "pinned", func() bool {
		a.node(t).step(ctx)
		return a.peerVerified(t, netid.Open(), b.id(t))
	})
	ps, _ := a.svc.Peers(ctx, netid.Open())
	if len(ps) == 0 || ps[0].Source != discovery.SourceUser || ps[0].Unresolved {
		t.Fatalf("peers = %+v", ps)
	}
	// Pinning does not say hello; an explicit hello lets b learn a and dial back.
	nc := discovery.NetworkConfig{ID: netid.Open(), AdvertiseAddr: a.addr(t)}
	if err := a.node(t).disc.Hello(ctx, nc, b.addr(t), b.id(t)); err != nil {
		t.Fatal(err)
	}
	b.node(t).step(ctx)
	if !b.peerVerified(t, netid.Open(), a.id(t)) {
		t.Fatal("b does not know a")
	}
	st, _ := a.svc.Status(ctx)
	if st.Networks[0].Peers.Verified < 1 {
		t.Fatalf("counts = %+v", st.Networks[0].Peers)
	}
}

func TestCreateNetworkNeedsAcceptIncoming(t *testing.T) {
	i := newInstance(t, enabledSettings())
	i.start(t)
	if _, err := i.svc.CreateNetwork(context.Background(), "x"); err != ErrAcceptIncomingRequired {
		t.Fatalf("err = %v", err)
	}
}
