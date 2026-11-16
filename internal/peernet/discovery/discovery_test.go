package discovery

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

func TestUserEnteredAddressResolvesAndPins(t *testing.T) {
	a, b := newNode(t, nodeOpts{}), newNode(t, nodeOpts{})
	ctx := context.Background()
	if err := a.svc.Reconcile(ctx, testNet, []string{b.addr}); err != nil {
		t.Fatal(err)
	}
	peers, _ := a.svc.Peers(ctx, testNet)
	if len(peers) != 1 || !peers[0].Unresolved || peers[0].NodeID != "" {
		t.Fatalf("want one unresolved entry, got %+v", peers)
	}
	r := tick(a)
	if r.Resolved != 1 {
		t.Fatalf("resolved %d", r.Resolved)
	}
	p := a.peer(b.id)
	if p == nil || p.Source != database.PeernetSourceUser || p.Address != b.addr {
		t.Fatalf("pin wrong: %+v", p)
	}
	if p.Verified || !p.LastSuccess.IsZero() {
		t.Fatalf("a handshake probe alone must not verify the peer: %+v", p)
	}
	peers, _ = a.svc.Peers(ctx, testNet)
	if len(peers) != 1 || peers[0].Unresolved || peers[0].NodeID != b.id.String() {
		t.Fatalf("status %+v", peers)
	}
	r = tick(a)
	if r.Contacted != 1 || r.Succeeded != 1 {
		t.Fatalf("contact after pin: %+v", r)
	}
	if p = a.peer(b.id); !p.Verified || p.LastSuccess.IsZero() {
		t.Fatalf("full contact must verify the peer: %+v", p)
	}
}

func TestUserEnteredChangedNodeIDIsFlaggedNotAccepted(t *testing.T) {
	a, b := newNode(t, nodeOpts{}), newNode(t, nodeOpts{})
	ctx := context.Background()
	_ = a.svc.Reconcile(ctx, testNet, []string{b.addr})
	tick(a)
	// Replace the node behind the same address with one holding a new key.
	_ = b.srv.Stop(ctx)
	impostor := newNodeWithKey(t, mustKey(t), b.addr, nodeOpts{})
	a.clock.Advance(time.Hour)
	r := tick(a)
	if r.Failed != 1 {
		t.Fatalf("want failure, got %+v", r)
	}
	old := a.peer(b.id)
	if old == nil || old.ChangedNodeID != impostor.id.String() {
		t.Fatalf("not flagged: %+v", old)
	}
	if a.peer(impostor.id) != nil {
		t.Fatal("impostor must not be stored")
	}
	peers, _ := a.svc.Peers(ctx, testNet)
	if len(peers) != 1 || !peers[0].NodeIDChanged || peers[0].NodeID != b.id.String() {
		t.Fatalf("status %+v", peers)
	}
	c, _ := a.svc.Counts(ctx, testNet)
	if c.NodeIDChanged != 1 {
		t.Fatalf("counts %+v", c)
	}
	// The user re-confirms.
	if err := a.svc.ConfirmChangedNode(ctx, testNet, b.addr); err != nil {
		t.Fatal(err)
	}
	if a.peer(b.id) != nil || a.peer(impostor.id) == nil {
		t.Fatal("pin not moved")
	}
	if err := a.svc.ConfirmChangedNode(ctx, testNet, b.addr); err == nil {
		t.Fatal("second confirm must fail")
	}
}

func TestConfirmRefusesWhenIdentityMovedAgain(t *testing.T) {
	a, b := newNode(t, nodeOpts{}), newNode(t, nodeOpts{})
	ctx := context.Background()
	_ = a.svc.Reconcile(ctx, testNet, []string{b.addr})
	tick(a)
	_ = b.srv.Stop(ctx)
	first := newNodeWithKey(t, mustKey(t), b.addr, nodeOpts{})
	a.clock.Advance(time.Hour)
	tick(a)
	_ = first.srv.Stop(ctx)
	newNodeWithKey(t, mustKey(t), b.addr, nodeOpts{})
	if err := a.svc.ConfirmChangedNode(ctx, testNet, b.addr); err == nil {
		t.Fatal("must refuse a third identity")
	}
	if a.peer(b.id) == nil {
		t.Fatal("old pin must be kept")
	}
}

func TestReconcileAddsRemovesAndReportsInvalid(t *testing.T) {
	a, b, c := newNode(t, nodeOpts{}), newNode(t, nodeOpts{}), newNode(t, nodeOpts{})
	ctx := context.Background()
	err := a.svc.Reconcile(ctx, testNet, []string{b.addr, c.addr, "not an address", b.addr, "http://x/"})
	if err == nil {
		t.Fatal("invalid entries must be reported")
	}
	tick(a)
	if a.peer(b.id) == nil || a.peer(c.id) == nil {
		t.Fatal("valid entries not applied")
	}
	// Remove c, keep b. Add nothing.
	if err := a.svc.Reconcile(ctx, testNet, []string{b.addr}); err != nil {
		t.Fatal(err)
	}
	if a.peer(c.id) != nil || a.peer(b.id) == nil {
		t.Fatal("removal not reconciled")
	}
	// Removing a still-unresolved entry drops it from the pending list.
	_ = a.svc.Reconcile(ctx, testNet, []string{b.addr, "8.8.4.4:9000"})
	cnt, _ := a.svc.Counts(ctx, testNet)
	if cnt.Unresolved != 1 {
		t.Fatalf("%+v", cnt)
	}
	_ = a.svc.Reconcile(ctx, testNet, nil)
	cnt, _ = a.svc.Counts(ctx, testNet)
	if cnt.Total != 0 || cnt.Unresolved != 0 {
		t.Fatalf("%+v", cnt)
	}
	if err := a.svc.Reconcile(ctx, hexNet(9), nil); err != ErrUnknownNetwork {
		t.Fatalf("got %v", err)
	}
}

func TestUserEnteredExistingExchangePeerIsPromoted(t *testing.T) {
	a, b := newNode(t, nodeOpts{}), newNode(t, nodeOpts{})
	a.seed(b.id, "8.8.8.8:1", database.PeernetSourceExchange, false)
	_ = a.svc.Reconcile(context.Background(), testNet, []string{b.addr})
	tick(a)
	p := a.peer(b.id)
	if p.Source != database.PeernetSourceUser || p.Address != b.addr {
		t.Fatalf("%+v", p)
	}
}

func TestUnresolvableUserAddressBacksOff(t *testing.T) {
	a := newNode(t, nodeOpts{})
	ctx := context.Background()
	_ = a.svc.Reconcile(ctx, testNet, []string{"127.0.0.1:1"})
	if r := tick(a); r.Resolved != 0 {
		t.Fatal("resolved a dead address")
	}
	// Immediately after, it is not due again.
	if got := a.svc.duePending(NetworkConfig{ID: testNet}, a.clock.Now()); len(got) != 0 {
		t.Fatal("no backoff")
	}
	a.clock.Advance(16 * time.Second)
	if got := a.svc.duePending(NetworkConfig{ID: testNet}, a.clock.Now()); len(got) != 1 {
		t.Fatal("not due after backoff")
	}
	peers, _ := a.svc.Peers(ctx, testNet)
	if len(peers) != 1 || peers[0].Failures != 1 || peers[0].LastError == "" {
		t.Fatalf("%+v", peers)
	}
}

func TestAdvertisedAddressNotExchangedUntilDialBackSucceeds(t *testing.T) {
	a := newNode(t, nodeOpts{})                // the server being told
	b := newNode(t, nodeOpts{advertise: true}) // advertises itself to a
	c := newNode(t, nodeOpts{})                // later asks a for peers
	ctx := context.Background()
	_ = b.svc.Reconcile(ctx, testNet, []string{a.addr})
	tick(b) // resolve a
	tick(b) // hello + exchange with a
	p := a.peer(b.id)
	if p == nil || p.Source != database.PeernetSourceInbound || p.Verified || p.Address != b.addr {
		t.Fatalf("advertised address not stored as unverified inbound: %+v", p)
	}
	for _, e := range c.rawPeers(a) {
		if e.NodeID == b.id.String() {
			t.Fatal("unverified advertised peer was exchanged")
		}
	}
	r := tick(a) // dial-back
	if r.Succeeded != 1 {
		t.Fatalf("dial back: %+v", r)
	}
	if p = a.peer(b.id); !p.Verified {
		t.Fatal("not verified after dial-back")
	}
	found := false
	for _, e := range c.rawPeers(a) {
		found = found || e.NodeID == b.id.String() && e.Address == b.addr
	}
	if !found {
		t.Fatal("verified peer not exchanged")
	}
}

func TestAdvertisedAddressOfAnotherNodeIsNeverVerified(t *testing.T) {
	a := newNode(t, nodeOpts{})
	victim := newNode(t, nodeOpts{})
	liar := newNode(t, nodeOpts{netCfg: func(nc *NetworkConfig) { nc.AdvertiseAddr = victim.addr }})
	c := newNode(t, nodeOpts{})
	ctx := context.Background()
	_ = liar.svc.Reconcile(ctx, testNet, []string{a.addr})
	tick(liar)
	tick(liar)
	p := a.peer(liar.id)
	if p == nil || p.Address != victim.addr {
		t.Fatalf("%+v", p)
	}
	for i := 0; i < 3; i++ {
		tick(a)
		a.clock.Advance(time.Hour)
	}
	if p = a.peer(liar.id); p == nil || p.Verified || p.Failures == 0 {
		t.Fatalf("must stay unverified: %+v", p)
	}
	for _, e := range c.rawPeers(a) {
		if e.NodeID == liar.id.String() || e.Address == victim.addr {
			t.Fatal("lie was relayed")
		}
	}
}

func TestHelloRateLimitPerPeer(t *testing.T) {
	a := newNode(t, nodeOpts{})
	b := newNode(t, nodeOpts{advertise: true})
	ctx := context.Background()
	nc := NetworkConfig{ID: testNet, AdvertiseAddr: "8.8.8.8:1000"}
	if err := b.svc.Hello(ctx, nc, a.addr, a.id); err != nil {
		t.Fatal(err)
	}
	nc.AdvertiseAddr = "8.8.4.4:2000"
	if err := b.svc.Hello(ctx, nc, a.addr, a.id); err != nil { // 429 is not an error
		t.Fatal(err)
	}
	if p := a.peer(b.id); p == nil || p.Address != "127.0.0.1:1000" {
		t.Fatalf("second hello within the interval must be ignored: %+v", p)
	}
	a.clock.Advance(11 * time.Minute)
	_ = b.svc.Hello(ctx, nc, a.addr, a.id)
	if p := a.peer(b.id); p.Address != "127.0.0.1:2000" {
		t.Fatalf("hello after interval ignored: %+v", p)
	}
	// A verified peer's advertised change becomes a candidate, not an address.
	_ = a.store.RecordSuccess(ctx, testNet.String(), b.id.String(), "", a.clock.Now(), a.clock.Now())
	a.clock.Advance(11 * time.Minute)
	nc.AdvertiseAddr = "8.8.1.1:3000"
	_ = b.svc.Hello(ctx, nc, a.addr, a.id)
	p := a.peer(b.id)
	if p.Address != "127.0.0.1:2000" || p.CandidateAddress != "127.0.0.1:3000" || !p.Verified {
		t.Fatalf("%+v", p)
	}
}

func TestHelloRejectsBadAddressesAndStillCountsAttempt(t *testing.T) {
	a := newNode(t, nodeOpts{netCfg: func(*NetworkConfig) {}})
	b := newNode(t, nodeOpts{})
	ctx := context.Background()
	for _, addr := range []string{"x@y:80", "no-port", ":80", "8.8.8.8:0", "8.8.8.8:22", "8.8.8.8:5432", "http://8.8.8.8:80/"} {
		body := encodeHello(addr)
		resp, err := b.svc.call(ctx, NetworkConfig{ID: testNet}, a.addr, a.id, http.MethodPost, pathHello, body, 1024, false)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d", addr, resp.StatusCode)
		}
		a.clock.Advance(11 * time.Minute)
	}
	if a.peer(b.id) != nil {
		t.Fatal("bad advertised address stored")
	}
}

func TestGlobalDialBackBudget(t *testing.T) {
	rec := &recDialer{}
	a := newNode(t, nodeOpts{
		limits: Limits{DialBacksPerWindow: 2, MaxContactsPerTick: 10},
		wrap:   func(Dialer) Dialer { return rec },
		netCfg: func(nc *NetworkConfig) { nc.TargetPeers, nc.MaxPeers = 20, 20 },
	})
	for i := 0; i < 6; i++ {
		a.seed(randID(t), "8.8.8.8:1000", database.PeernetSourceInbound, false)
	}
	r := tick(a)
	if r.Contacted != 2 {
		t.Fatalf("budget not applied: %+v", r)
	}
	if r = tick(a); r.Contacted != 0 {
		t.Fatalf("budget window not respected: %+v", r)
	}
	a.clock.Advance(2 * time.Minute)
	a.clock.Advance(time.Hour) // clear the failure backoff as well
	if r = tick(a); r.Contacted != 2 {
		t.Fatalf("budget must refill: %+v", r)
	}
	// Verified peers are not subject to it.
	b := newNode(t, nodeOpts{limits: Limits{DialBacksPerWindow: 1}, wrap: func(Dialer) Dialer { return &recDialer{} }})
	for i := 0; i < 3; i++ {
		b.seed(randID(t), "8.8.8.8:1000", database.PeernetSourceExchange, true)
		_ = b.store.RecordSuccess(context.Background(), testNet.String(), randID(t).String(), "", b.clock.Now(), b.clock.Now())
	}
	b.clock.Advance(time.Hour)
	if r := tick(b); r.Contacted != 3 {
		t.Fatalf("%+v", r)
	}
}

func TestExchangeNeverReturnsUnverifiedOrRequester(t *testing.T) {
	a := newNode(t, nodeOpts{})
	req := newNode(t, nodeOpts{})
	v1, v2, u1 := randID(t), randID(t), randID(t)
	a.seed(v1, "8.8.8.1:7000", database.PeernetSourceExchange, true)
	a.seed(v2, "8.8.8.2:7000", database.PeernetSourceUser, true)
	a.seed(u1, "8.8.8.3:7000", database.PeernetSourceExchange, false)
	a.seed(req.id, "8.8.8.4:7000", database.PeernetSourceInbound, true)
	got := req.rawPeers(a)
	ids := map[string]bool{}
	for _, e := range got {
		ids[e.NodeID] = true
	}
	if !ids[v1.String()] || !ids[v2.String()] || ids[u1.String()] || ids[req.id.String()] || ids[a.id.String()] {
		t.Fatalf("wrong set: %+v", got)
	}
	// A failing verified peer is withdrawn.
	for i := 0; i < 3; i++ {
		_, _ = a.store.RecordFailure(context.Background(), testNet.String(), v1.String(), a.clock.Now(), func(int) time.Duration { return 0 })
	}
	for _, e := range req.rawPeers(a) {
		if e.NodeID == v1.String() {
			t.Fatal("failing peer still exchanged")
		}
	}
}

func TestExchangeResponseCaps(t *testing.T) {
	a := newNode(t, nodeOpts{})
	req := newNode(t, nodeOpts{})
	for i := 0; i < 80; i++ {
		a.seed(randID(t), "8.8.8.8:7000", database.PeernetSourceExchange, true)
	}
	if got := req.rawPeers(a); len(got) != MaxPeerEntries {
		t.Fatalf("sample size %d", len(got))
	}
	// Randomised: two samples differ.
	first := req.rawPeers(a)
	differs := false
	for i := 0; i < 5 && !differs; i++ {
		second := req.rawPeers(a)
		for j := range second {
			if second[j] != first[j] {
				differs = true
			}
		}
	}
	if !differs {
		t.Fatal("sample not randomised")
	}
}

func TestExchangeServerFiltersPrivateWhenNotAllowed(t *testing.T) {
	a := newNode(t, nodeOpts{})
	req := newNode(t, nodeOpts{})
	pub, priv := randID(t), randID(t)
	a.seed(pub, "8.8.8.8:1", database.PeernetSourceExchange, true)
	a.seed(priv, "10.2.3.4:1", database.PeernetSourceUser, true)
	for _, e := range req.rawPeers(a) {
		if e.NodeID == priv.String() {
			t.Fatal("private user-entered address was relayed")
		}
	}
	// Private addresses are never relayed, even in a network that allows them
	// (receivers refuse them from exchange anyway).
	pa := newNode(t, nodeOpts{netCfg: func(nc *NetworkConfig) { nc.AllowPrivate = true }})
	pa.seed(priv, "10.2.3.4:1", database.PeernetSourceUser, true)
	for _, e := range req.rawPeers(pa) {
		if e.NodeID == priv.String() {
			t.Fatal("private address relayed in a private network")
		}
	}
}

func TestPrivateAddressLearnedInOneNetworkNeverDialledForAnother(t *testing.T) {
	rec := &recDialer{}
	netA, netB := hexNet(0xaa), hexNet(0xbb)
	a := newNode(t, nodeOpts{
		wrap:   func(Dialer) Dialer { return rec },
		nets:   []netid.NetworkID{netA, netB},
		netCfg: nil,
	})
	if err := a.svc.SetNetwork(NetworkConfig{ID: netA, AllowPrivate: true}); err != nil {
		t.Fatal(err)
	}
	if err := a.svc.SetNetwork(NetworkConfig{ID: netB, AllowPrivate: false}); err != nil {
		t.Fatal(err)
	}
	id := randID(t)
	a.seedIn(netA, id, "10.9.9.9:7000", database.PeernetSourceExchange, false)
	tick(a)
	calls := rec.snapshot()
	if len(calls) != 1 || calls[0].addr != "10.9.9.9:7000" || !calls[0].allowPrivate || calls[0].net != netA.String() {
		t.Fatalf("calls %+v", calls)
	}
	for _, c := range calls {
		if c.net == netB.String() {
			t.Fatal("dialled for the wrong network")
		}
	}
	// Same node and address seeded into B (as if hearsay) is dialled under B's
	// policy only: without private allowance.
	a.seedIn(netB, id, "10.9.9.9:7000", database.PeernetSourceExchange, false)
	a.clock.Advance(2 * time.Hour)
	tick(a)
	for _, c := range rec.snapshot()[1:] {
		if c.net == netB.String() && c.allowPrivate {
			t.Fatal("private allowance leaked across networks")
		}
	}
}

func TestBackoffCapsAndPruning(t *testing.T) {
	l := DefaultLimits()
	if backoff(l.UserBackoffBase, l.UserBackoffMax, 1) != 15*time.Second ||
		backoff(l.UserBackoffBase, l.UserBackoffMax, 2) != 30*time.Second ||
		backoff(l.UserBackoffBase, l.UserBackoffMax, 50) != 5*time.Minute ||
		backoff(l.PeerBackoffBase, l.PeerBackoffMax, 50) != time.Hour ||
		backoff(l.PeerBackoffBase, l.PeerBackoffMax, 0) != time.Minute {
		t.Fatal("backoff wrong")
	}
	rec := &recDialer{}
	a := newNode(t, nodeOpts{wrap: func(Dialer) Dialer { return rec }, netCfg: func(nc *NetworkConfig) { nc.TargetPeers, nc.MaxPeers = 50, 50 }})
	ex, usr := randID(t), randID(t)
	a.seed(ex, "8.8.8.8:1", database.PeernetSourceExchange, true)
	a.seed(usr, "8.8.8.9:1", database.PeernetSourceUser, true)
	for i := 0; i < 8; i++ {
		a.clock.Advance(2 * time.Hour)
		tick(a)
	}
	if a.peer(ex) == nil {
		t.Fatal("pruned too early")
	}
	pe := a.peer(ex)
	if pe.Failures != 8 {
		t.Fatalf("failures %d", pe.Failures)
	}
	if d := pe.NextAttempt.Sub(a.clock.Now()); d > time.Hour+time.Second || d < time.Hour-time.Second {
		t.Fatalf("non-user backoff cap: %v", d)
	}
	pu := a.peer(usr)
	if d := pu.NextAttempt.Sub(a.clock.Now()); d > 5*time.Minute || d < 5*time.Minute-time.Second {
		t.Fatalf("user backoff cap: %v", d)
	}
	a.clock.Advance(48 * time.Hour)
	if r := tick(a); r.Pruned != 1 {
		t.Fatalf("%+v", r)
	}
	if a.peer(ex) != nil || a.peer(usr) == nil {
		t.Fatal("prune must remove the stale peer and keep the user peer")
	}
}

func TestSuccessSchedulesRefreshAndTargetGating(t *testing.T) {
	servers := []*tnode{newNode(t, nodeOpts{}), newNode(t, nodeOpts{}), newNode(t, nodeOpts{}), newNode(t, nodeOpts{})}
	a := newNode(t, nodeOpts{netCfg: func(nc *NetworkConfig) { nc.TargetPeers, nc.MaxPeers = 2, 2 }})
	for _, s := range servers {
		a.seed(s.id, s.addr, database.PeernetSourceExchange, false)
	}
	r := tick(a)
	if r.Contacted != 2 || r.Succeeded != 2 {
		t.Fatalf("target gating: %+v", r)
	}
	// Active peers are not due again until the refresh interval passes.
	if r = tick(a); r.Contacted != 0 {
		t.Fatalf("%+v", r)
	}
	c, _ := a.svc.Counts(context.Background(), testNet)
	if c.Active != 2 || c.Verified != 2 || c.Exchange != 4 {
		t.Fatalf("%+v", c)
	}
}

func TestPeerExchangeBetweenRealNodesLearnsOnlyVerified(t *testing.T) {
	a, b := newNode(t, nodeOpts{}), newNode(t, nodeOpts{})
	third, junk := newNode(t, nodeOpts{}), randID(t)
	a.seed(third.id, third.addr, database.PeernetSourceExchange, true)
	a.seed(junk, "8.8.8.8:7", database.PeernetSourceExchange, false)
	_ = b.svc.Reconcile(context.Background(), testNet, []string{a.addr})
	tick(b)
	tick(b)
	p := b.peer(third.id)
	if p == nil || p.Source != database.PeernetSourceExchange || p.Verified {
		t.Fatalf("learned peer should be unverified exchange: %+v", p)
	}
	if b.peer(junk) != nil {
		t.Fatal("learned an unverified peer")
	}
	// Dial it ourselves: now verified.
	b.clock.Advance(time.Second)
	tick(b)
	if !b.peer(third.id).Verified {
		t.Fatal("not verified after our own dial")
	}
}

type fixedSource struct{ c []Candidate }

func (f fixedSource) Name() string { return "fixed" }
func (f fixedSource) Discover(context.Context, netid.NetworkID) ([]Candidate, error) {
	return f.c, nil
}

func TestKnownFor(t *testing.T) {
	a := newNode(t, nodeOpts{})
	id, unverified := randID(t), randID(t)
	a.seed(id, "8.8.8.8:1", database.PeernetSourceExchange, true)
	a.seed(unverified, "8.8.4.4:1", database.PeernetSourceExchange, false)
	ctx := context.Background()
	if ok, _ := a.svc.KnownFor(ctx, testNet, id, 24*time.Hour); ok {
		t.Fatal("too early")
	}
	a.clock.Advance(25 * time.Hour)
	if ok, _ := a.svc.KnownFor(ctx, testNet, id, 24*time.Hour); !ok {
		t.Fatal("should be known")
	}
	if ok, _ := a.svc.KnownFor(ctx, testNet, unverified, 24*time.Hour); ok {
		t.Fatal("a peer-exchange entry nobody connected to is never known")
	}
	_ = a.store.Touch(ctx, testNet.String(), id.String(), a.clock.Now())
	if ok, _ := a.svc.KnownFor(ctx, testNet, id, 24*time.Hour); !ok {
		t.Fatal("first-seen moved")
	}
}

func TestExtraSourceCandidatesAreValidatedAndCapped(t *testing.T) {
	a := newNode(t, nodeOpts{limits: Limits{MaxNewPerExchange: 2}})
	var cands []Candidate
	cands = append(cands, Candidate{NodeID: a.id, Address: "8.8.8.8:1"})         // self
	cands = append(cands, Candidate{NodeID: randID(t), Address: "127.0.0.1:99"}) // loopback is fine in tests
	cands = append(cands, Candidate{NodeID: randID(t), Address: "bad address"})  // invalid
	cands = append(cands, Candidate{NodeID: randID(t), Address: "10.0.0.1:1"})   // private
	for i := 0; i < 5; i++ {
		cands = append(cands, Candidate{NodeID: randID(t), Address: "8.8.8.8:" + strings.Repeat("1", i+1)})
	}
	a.svc.AddSource(fixedSource{c: cands})
	r := tick(a)
	if r.Learned != 2 {
		t.Fatalf("%+v", r)
	}
	if a.peer(a.id) != nil {
		t.Fatal("stored self")
	}
}

var _ = transport.ErrInvalidAddress

// A verified contact records the IP the node really connected to; that, and
// not the stored address string, is what corroboration keys on.
func TestContactRecordsTheConnectedIP(t *testing.T) {
	a, b := newNode(t, nodeOpts{}), newNode(t, nodeOpts{})
	a.seed(b.id, b.addr, database.PeernetSourceExchange, false)
	if r := tick(a); r.Succeeded != 1 {
		t.Fatalf("contact: %+v", r)
	}
	ip, err := a.store.ConnectedIP(context.Background(), testNet.String(), b.id.String())
	if err != nil {
		t.Fatal(err)
	}
	if ip != "127.0.0.1" {
		t.Fatalf("connected ip %q, want the loopback address the test server listens on", ip)
	}
}

// A peer that cannot be contacted shows why in the status, instead of an empty
// last error while it backs off.
func TestFailedContactSurfacesTheLastErrorInThePeerStatus(t *testing.T) {
	a := newNode(t, nodeOpts{})
	ghost := randID(t)
	a.seed(ghost, "127.0.0.1:1", database.PeernetSourceExchange, false) // nothing listens there
	tick(a)
	peers, err := a.svc.Peers(context.Background(), testNet)
	if err != nil {
		t.Fatal(err)
	}
	var got *PeerStatus
	for i := range peers {
		if peers[i].NodeID == ghost.String() {
			got = &peers[i]
		}
	}
	if got == nil || got.Failures == 0 {
		t.Fatalf("the contact should have failed: %+v", peers)
	}
	if got.LastError == "" {
		t.Fatal("the status must say why the peer is not connecting")
	}
	if len(got.LastError) > 200 {
		t.Fatalf("error text is bounded: %d", len(got.LastError))
	}
}
