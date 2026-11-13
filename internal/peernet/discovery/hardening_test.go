package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// proofAuth models a private network: requests must carry a credential and
// every response (any status) must carry a proof.
type proofAuth struct{}

func (proofAuth) AuthorizeInbound(_ context.Context, _ netid.NetworkID, _ identity.NodeID, h http.Header) (ResponseDecorator, error) {
	if h.Get("X-Test-Cred") != "token" {
		return nil, errors.New("no credential")
	}
	return func(h http.Header) { h.Set("X-Test-Proof", "ok") }, nil
}

func (proofAuth) DecorateOutbound(_ context.Context, _ netid.NetworkID, _ identity.NodeID, h http.Header) error {
	h.Set("X-Test-Cred", "token")
	return nil
}

func (proofAuth) VerifyResponse(_ context.Context, _ netid.NetworkID, _ identity.NodeID, status int, h http.Header) error {
	if status == http.StatusForbidden {
		return nil // an unauthorised caller cannot expect a proof
	}
	if h.Get("X-Test-Proof") != "ok" {
		return errors.New("missing membership proof")
	}
	return nil
}

func TestPrivateNetworkPeersDoNotFlapOnRepeatedContacts(t *testing.T) {
	a := newNode(t, nodeOpts{auth: proofAuth{}})
	b := newNode(t, nodeOpts{auth: proofAuth{}, advertise: true})
	ctx := context.Background()
	if err := b.svc.Reconcile(ctx, testNet, []string{a.addr}); err != nil {
		t.Fatal(err)
	}
	tick(b) // resolve
	for i := 0; i < 8; i++ {
		// HelloInterval (10 min) is longer than RefreshInterval (5 min), so
		// every second contact used to hit the server's hello rate limit.
		r := tick(b)
		if r.Failed != 0 || r.Succeeded != 1 {
			t.Fatalf("contact %d: %+v", i, r)
		}
		a.clock.Advance(5 * time.Minute)
		b.clock.Advance(5 * time.Minute)
	}
	if p := b.peer(a.id); p == nil || p.Failures != 0 || !p.Verified {
		t.Fatalf("peer flapped: %+v", p)
	}
}

func TestEveryAuthorisedReplyCarriesTheProof(t *testing.T) {
	a := newNode(t, nodeOpts{auth: proofAuth{}})
	b := newNode(t, nodeOpts{auth: proofAuth{}})
	ctx := context.Background()
	nc := NetworkConfig{ID: testNet}
	hello := func(body []byte) int {
		resp, err := b.svc.call(ctx, nc, a.addr, a.id, http.MethodPost, pathHello, body, 1024, false)
		if err != nil {
			t.Fatalf("reply without a valid proof: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := hello(encodeHello("8.8.8.8:1000")); c != 200 {
		t.Fatalf("first hello %d", c)
	}
	if c := hello(encodeHello("8.8.8.8:1001")); c != http.StatusTooManyRequests {
		t.Fatalf("second hello %d, want 429", c)
	}
	a.clock.Advance(11 * time.Minute)
	if c := hello(encodeHello("x@y:1")); c != http.StatusBadRequest {
		t.Fatalf("bad hello %d", c)
	}
	if c := hello([]byte("garbage")); c != http.StatusBadRequest {
		t.Fatalf("garbage hello %d", c)
	}
}

// countingDialer counts hello requests passing through to the real client.
type countingDialer struct {
	Dialer
	hellos atomic.Int32
}

func (c *countingDialer) Do(ctx context.Context, addr string, n identity.NodeID, req transport.Request, o transport.CallOptions) (*transport.Response, error) {
	if req.Path == pathHello {
		c.hellos.Add(1)
	}
	return c.Dialer.Do(ctx, addr, n, req, o)
}

func TestHelloNotResentWhileAddressAcknowledged(t *testing.T) {
	cd := &countingDialer{}
	a := newNode(t, nodeOpts{})
	b := newNode(t, nodeOpts{wrap: func(d Dialer) Dialer { cd.Dialer = d; return cd }})
	ctx := context.Background()
	nc := NetworkConfig{ID: testNet, AdvertiseAddr: "8.8.8.8:1000"}
	send := func() {
		if err := b.svc.Hello(ctx, nc, a.addr, a.id); err != nil {
			t.Fatal(err)
		}
	}
	send()
	send()
	if got := cd.hellos.Load(); got != 1 {
		t.Fatalf("hello resent while acknowledged: %d requests", got)
	}
	nc.AdvertiseAddr = "8.8.8.8:2000" // a changed address is sent
	send()
	if got := cd.hellos.Load(); got != 2 {
		t.Fatalf("changed address not sent: %d", got)
	}
	b.clock.Advance(11 * time.Minute)
	a.clock.Advance(11 * time.Minute)
	send()
	if got := cd.hellos.Load(); got != 3 {
		t.Fatalf("hello not refreshed after the interval: %d", got)
	}
}

func TestHelloStoresRemoteIPWithAdvertisedPort(t *testing.T) {
	a := newNode(t, nodeOpts{})
	ctx := context.Background()
	for i, tc := range []struct{ advertised, want string }{
		{"8.8.8.8:7001", "127.0.0.1:7001"},          // a third party's IP is never stored
		{"evil.example.org:7002", "127.0.0.1:7002"}, // nor a hostname
		{"[2606:4700::1]:7003", "127.0.0.1:7003"},   // nor an IPv6 literal
		{"10.0.0.1:7004", "127.0.0.1:7004"},         // nor a private one
		{"169.254.169.254:7005", "127.0.0.1:7005"},  // nor link-local
	} {
		b := newNode(t, nodeOpts{})
		nc := NetworkConfig{ID: testNet, AdvertiseAddr: "8.8.8.8:1"}
		resp, err := b.svc.call(ctx, nc, a.addr, a.id, http.MethodPost, pathHello, encodeHello(tc.advertised), 1024, false)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%d: status %d", i, resp.StatusCode)
		}
		if p := a.peer(b.id); p == nil || p.Address != tc.want || p.Source != database.PeernetSourceInbound {
			t.Fatalf("%d: advertised %q stored as %+v, want %s", i, tc.advertised, p, tc.want)
		}
		a.clock.Advance(11 * time.Minute)
	}
	// An empty address stores nothing.
	b := newNode(t, nodeOpts{})
	resp, err := b.svc.call(ctx, NetworkConfig{ID: testNet}, a.addr, a.id, http.MethodPost, pathHello, encodeHello(""), 1024, false)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if a.peer(b.id) != nil {
		t.Fatal("empty hello stored a peer")
	}
}

func TestHelloAddressPolicy(t *testing.T) {
	s := newNode(t, nodeOpts{}).svc
	nc := NetworkConfig{ID: testNet}
	for _, adv := range []string{"8.8.8.8:22", "8.8.8.8:3306", "x@y:80", "nope"} {
		if _, err := s.helloAddress(nc, adv, netip.MustParseAddr("8.8.4.4")); err == nil {
			t.Errorf("advertised %q accepted", adv)
		}
	}
	if _, err := s.helloAddress(nc, "8.8.8.8:80", netip.MustParseAddr("10.1.1.1")); err == nil {
		t.Error("private remote IP accepted in a network that does not allow private addresses")
	}
	got, err := s.helloAddress(NetworkConfig{ID: testNet, AllowPrivate: true}, "8.8.8.8:80", netip.MustParseAddr("10.1.1.1"))
	if err != nil || got != "10.1.1.1:80" {
		t.Errorf("private network hello: %q %v", got, err)
	}
}

func TestExchangeServesOnlyPublicIPLiterals(t *testing.T) {
	a := newNode(t, nodeOpts{netCfg: func(nc *NetworkConfig) { nc.AllowPrivate = true }})
	req := newNode(t, nodeOpts{})
	good, host, hostUser, priv, port := randID(t), randID(t), randID(t), randID(t), randID(t)
	a.seed(good, "8.8.8.8:7000", database.PeernetSourceExchange, true)
	a.seed(host, "peer.example.org:7000", database.PeernetSourceLocal, true)
	a.seed(hostUser, "mine.example.org:7000", database.PeernetSourceUser, true)
	a.seed(priv, "192.168.1.5:7000", database.PeernetSourceUser, true)
	a.seed(port, "8.8.4.4:22", database.PeernetSourceExchange, true)
	got := map[string]bool{}
	for _, e := range req.rawPeers(a) {
		got[e.NodeID] = true
	}
	if !got[good.String()] {
		t.Fatal("public IP literal not relayed")
	}
	for name, id := range map[string]identity.NodeID{"hostname": host, "user hostname": hostUser, "private": priv, "denied port": port} {
		if got[id.String()] {
			t.Errorf("%s peer relayed", name)
		}
	}
}

func TestExchangeRefusesHostnamesAndDeniedPorts(t *testing.T) {
	a := newNode(t, nodeOpts{})
	addrs := []string{"peer.example.org:7000", "8.8.8.8:22", "8.8.8.8:3306", "8.8.8.8:6379", "[fd00::1]:7000"}
	addr, id := hostile(t, asBody(entries(len(addrs), func(i int) string { return addrs[i] })))
	n, err := a.svc.Exchange(context.Background(), NetworkConfig{ID: testNet, AllowPrivate: true}, addr, id)
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestSourceCandidatesKeepHostnamesAndAreNeverRelayed(t *testing.T) {
	rec := &recDialer{}
	a := newNode(t, nodeOpts{wrap: func(Dialer) Dialer { return rec }})
	host := randID(t)
	a.svc.AddSource(fixedSource{c: []Candidate{{NodeID: host, Address: "Host.Example.org:7000"}}})
	tick(a)
	p := a.peer(host)
	if p == nil || p.Address != "host.example.org:7000" || p.Source != database.PeernetSourceLocal {
		t.Fatalf("source candidate not stored as a local peer: %+v", p)
	}
	st, _ := a.svc.Peers(context.Background(), testNet)
	if len(st) != 1 || st[0].Source != SourceExchange {
		t.Fatalf("status %+v", st)
	}
	// Dialled as trusted configuration.
	found := false
	for _, c := range rec.snapshot() {
		if c.addr == "host.example.org:7000" {
			found = true
			if !c.trusted {
				t.Error("local-source address dialled as untrusted")
			}
		}
	}
	if !found {
		t.Fatalf("source peer not contacted: %+v", rec.snapshot())
	}
}

func TestDialTrustFlag(t *testing.T) {
	rec := &recDialer{}
	a := newNode(t, nodeOpts{wrap: func(Dialer) Dialer { return rec }, netCfg: func(nc *NetworkConfig) { nc.TargetPeers, nc.MaxPeers = 20, 20 }})
	user, local, ex, in := randID(t), randID(t), randID(t), randID(t)
	a.seed(user, "8.8.1.1:1000", database.PeernetSourceUser, false)
	a.seed(local, "8.8.2.1:1000", database.PeernetSourceLocal, false)
	a.seed(ex, "8.8.3.1:1000", database.PeernetSourceExchange, false)
	a.seed(in, "8.8.4.1:1000", database.PeernetSourceInbound, false)
	tick(a)
	want := map[string]bool{"8.8.1.1:1000": true, "8.8.2.1:1000": true, "8.8.3.1:1000": false, "8.8.4.1:1000": false}
	seen := map[string]bool{}
	for _, c := range rec.snapshot() {
		if w, ok := want[c.addr]; ok {
			seen[c.addr] = true
			if c.trusted != w {
				t.Errorf("%s trusted=%v, want %v", c.addr, c.trusted, w)
			}
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("not all contacted: %v", seen)
	}
	// User-entered resolution probes are trusted too.
	_ = a.svc.Reconcile(context.Background(), testNet, []string{"mine.example.org:7000"})
	tick(a)
	ok := false
	for _, c := range rec.snapshot() {
		if c.addr == "mine.example.org:7000" {
			ok = c.trusted
		}
	}
	if !ok {
		t.Fatal("user-entered probe not trusted")
	}
}

func TestSourceReportingNewAddressVerifiesBeforeReplacing(t *testing.T) {
	a := newNode(t, nodeOpts{})
	b := newNode(t, nodeOpts{})
	dead := newNode(t, nodeOpts{})
	ctx := context.Background()
	// A verified peer stored at a stale address.
	a.seed(b.id, "8.8.8.8:1000", database.PeernetSourceExchange, true)
	src := &mutableSource{}
	src.set(b.id, dead.addr) // the source first reports an address nobody answers for b
	a.svc.AddSource(src)
	tick(a)
	p := a.peer(b.id)
	if p.CandidateAddress != "" || p.Address != "8.8.8.8:1000" {
		// The candidate was dialled in the same tick and rejected by the pin.
		t.Fatalf("unverified address adopted: %+v", p)
	}
	if p.Failures == 0 {
		t.Fatalf("failed candidate dial not recorded: %+v", p)
	}
	if p.Source != database.PeernetSourceLocal {
		t.Fatalf("source not adopted: %+v", p)
	}
	// Now the source reports the real address: adopted after a pinned dial.
	src.set(b.id, b.addr)
	a.clock.Advance(time.Hour)
	tick(a)
	p = a.peer(b.id)
	if p.Address != b.addr || p.CandidateAddress != "" || !p.Verified || p.Failures != 0 {
		t.Fatalf("verified candidate not adopted: %+v", p)
	}
	// A user pin is never changed by a source.
	u := newNode(t, nodeOpts{})
	_ = a.svc.Reconcile(ctx, testNet, []string{b.addr, u.addr})
	tick(a)
	src.set(u.id, dead.addr)
	a.clock.Advance(time.Hour)
	tick(a)
	if p := a.peer(u.id); p.Address != u.addr || p.Source != database.PeernetSourceUser || p.CandidateAddress != "" {
		t.Fatalf("user pin altered by a source: %+v", p)
	}
}

type mutableSource struct {
	mu sync.Mutex
	c  []Candidate
}

func (m *mutableSource) set(id identity.NodeID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.c = []Candidate{{NodeID: id, Address: addr}}
}
func (m *mutableSource) Name() string { return "mutable" }
func (m *mutableSource) Discover(context.Context, netid.NetworkID) ([]Candidate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Candidate(nil), m.c...), nil
}

func TestSourceCandidateAddressIsNotRetriedEveryTickWhilePending(t *testing.T) {
	a := newNode(t, nodeOpts{})
	id := randID(t)
	a.seed(id, "8.8.8.8:1000", database.PeernetSourceExchange, true)
	a.svc.AddSource(fixedSource{c: []Candidate{{NodeID: id, Address: "8.8.4.4:1000"}}})
	ctx := context.Background()
	// Set the candidate by hand as the first tick would, then make sure a
	// second intake leaves its schedule alone.
	if err := a.store.SetCandidateAddress(ctx, testNet.String(), id.String(), "8.8.4.4:1000", a.clock.Now()); err != nil {
		t.Fatal(err)
	}
	_ = a.store.RecordFailure
	before := a.peer(id)
	a.clock.Advance(time.Minute)
	if _, err := a.svc.intakeCandidates(ctx, NetworkConfig{ID: testNet}, []Candidate{{NodeID: id, Address: "8.8.4.4:1000"}}); err != nil {
		t.Fatal(err)
	}
	after := a.peer(id)
	if !after.LastSeen.Equal(before.LastSeen) || after.CandidateAddress != before.CandidateAddress {
		t.Fatalf("repeated candidate reset the peer: %+v -> %+v", before, after)
	}
}

func TestDialBackBudgetPerTargetPrefix(t *testing.T) {
	rec := &recDialer{}
	a := newNode(t, nodeOpts{
		limits: Limits{DialBacksPerWindow: 100, MaxContactsPerTick: 30, DialBacksPerPrefix: 3},
		wrap:   func(Dialer) Dialer { return rec },
		netCfg: func(nc *NetworkConfig) { nc.TargetPeers, nc.MaxPeers = 50, 50 },
	})
	for i := 0; i < 10; i++ {
		a.seed(randID(t), fmt.Sprintf("8.8.8.%d:1000", i+1), database.PeernetSourceInbound, false)
	}
	for i := 0; i < 2; i++ {
		a.seed(randID(t), fmt.Sprintf("9.9.9.%d:1000", i+1), database.PeernetSourceInbound, false)
	}
	r := tick(a)
	if r.Contacted != 5 { // 3 in 8.8.8.0/24 plus 2 in 9.9.9.0/24
		t.Fatalf("per-prefix cap not applied: %+v", r)
	}
	a.clock.Advance(30 * time.Minute)
	a.clock.Advance(40 * time.Minute) // clears backoff, and the one-hour prefix window
	r = tick(a)
	if r.Contacted == 0 {
		t.Fatal("per-prefix budget must refill after the window")
	}
}

func TestHelloLimiterPerPrefix(t *testing.T) {
	s := newNode(t, nodeOpts{limits: Limits{HelloPerPrefixPerInterval: 3, HelloGlobalPerWindow: 1000, MaxTrackedHello: 64}}).svc
	now := time.Now()
	count := func(addr func(i int) string) int {
		n := 0
		for i := 0; i < 20; i++ {
			if s.helloAllowed("n", randID(t), netip.MustParseAddr(addr(i)), now) {
				n++
			}
		}
		return n
	}
	// Fresh identities from one /24 (or one IPv6 /64) share one allowance.
	if got := count(func(i int) string { return fmt.Sprintf("8.8.8.%d", i+1) }); got != 3 {
		t.Fatalf("ipv4 /24: %d allowed", got)
	}
	if got := count(func(i int) string { return fmt.Sprintf("2001:4860:1:1::%x", i+1) }); got != 3 {
		t.Fatalf("ipv6 /64: %d allowed", got)
	}
	// Another prefix still gets its own allowance.
	if got := count(func(i int) string { return fmt.Sprintf("9.9.9.%d", i+1) }); got != 3 {
		t.Fatalf("other prefix: %d allowed", got)
	}
	// The allowance expires.
	later := now.Add(11 * time.Minute)
	if !s.helloAllowed("n", randID(t), netip.MustParseAddr("8.8.8.1"), later) {
		t.Fatal("limits must expire")
	}
}

func TestHelloLimiterGlobalBudget(t *testing.T) {
	s := newNode(t, nodeOpts{limits: Limits{HelloPerPrefixPerInterval: 100, HelloGlobalPerWindow: 5, MaxTrackedHello: 64}}).svc
	now := time.Now()
	n := 0
	for i := 0; i < 50; i++ {
		if s.helloAllowed("n", randID(t), netip.MustParseAddr(fmt.Sprintf("11.%d.1.1", i)), now) {
			n++
		}
	}
	if n != 5 {
		t.Fatalf("global budget: %d allowed, want 5", n)
	}
	if !s.helloAllowed("n", randID(t), netip.MustParseAddr("12.1.1.1"), now.Add(2*time.Minute)) {
		t.Fatal("global budget must refill")
	}
}

func TestUnverifiedInboundRowsCappedPerPrefix(t *testing.T) {
	a := newNode(t, nodeOpts{limits: Limits{MaxUnverifiedPerPrefix: 3}})
	nc := NetworkConfig{ID: testNet}
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		a.clock.Advance(time.Second)
		if err := a.svc.recordAdvertised(ctx, nc, randID(t), fmt.Sprintf("8.8.8.%d:7000", i+1)); err != nil {
			t.Fatal(err)
		}
	}
	other := randID(t)
	if err := a.svc.recordAdvertised(ctx, nc, other, "9.9.9.9:7000"); err != nil {
		t.Fatal(err)
	}
	c, _ := a.svc.Counts(ctx, testNet)
	if c.Inbound != 4 {
		t.Fatalf("one /24 filled the table: %+v", c)
	}
	if a.peer(other) == nil {
		t.Fatal("an unrelated prefix was locked out")
	}
}

func TestFullNetworkEvictsVerifiedInsteadOfRefusing(t *testing.T) {
	a := newNode(t, nodeOpts{limits: Limits{MaxPeersPerNetwork: 3, MaxNewPerExchange: 3}})
	for i := 0; i < 3; i++ {
		a.seed(randID(t), fmt.Sprintf("8.8.%d.1:7000", i+1), database.PeernetSourceExchange, true)
	}
	fresh := randID(t)
	addr, id := hostile(t, asBody([]PeerEntry{{NodeID: fresh.String(), Address: "9.9.9.9:7000"}}))
	n, err := a.svc.Exchange(context.Background(), NetworkConfig{ID: testNet}, addr, id)
	if err != nil || n != 1 || a.peer(fresh) == nil {
		t.Fatalf("n=%d err=%v peer=%v", n, err, a.peer(fresh))
	}
}

// blockDialer blocks every call until its context ends.
type blockDialer struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (b *blockDialer) note(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.seen == nil {
		b.seen = map[string]bool{}
	}
	b.seen[addr] = true
}

func (b *blockDialer) Do(ctx context.Context, addr string, _ identity.NodeID, _ transport.Request, _ transport.CallOptions) (*transport.Response, error) {
	b.note(addr)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockDialer) ProbeNodeID(ctx context.Context, addr string, _ transport.CallOptions) (identity.NodeID, error) {
	b.note(addr)
	<-ctx.Done()
	return identity.NodeID{}, ctx.Err()
}

func (b *blockDialer) addrs() map[string]bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]bool{}
	for k := range b.seen {
		out[k] = true
	}
	return out
}

func TestTickIsBoundedAndNetworksAreNotStarved(t *testing.T) {
	bd := &blockDialer{}
	netA, netB := hexNet(0xa1), hexNet(0xb2)
	a := newNode(t, nodeOpts{
		limits: Limits{MaxTickDuration: 300 * time.Millisecond, CallTimeout: time.Minute},
		wrap:   func(Dialer) Dialer { return bd },
		nets:   []netid.NetworkID{netA, netB},
	})
	ctx := context.Background()
	_ = a.svc.Reconcile(ctx, netA, []string{"8.8.1.1:7000"})
	_ = a.svc.Reconcile(ctx, netB, []string{"8.8.2.2:7000"})
	peer := randID(t)
	a.seedIn(netA, peer, "8.8.3.3:7000", database.PeernetSourceUser, false)
	start := time.Now()
	r := a.svc.Tick(ctx)
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("tick ignored its wall-clock cap: %v", d)
	}
	seen := bd.addrs()
	if !seen["8.8.1.1:7000"] || !seen["8.8.2.2:7000"] {
		t.Fatalf("a network was starved by the other: %v", seen)
	}
	if r.Resolved != 0 {
		t.Fatalf("%+v", r)
	}
	// Time ran out on these peers; they were not at fault.
	if p, _ := a.store.Get(ctx, netA.String(), peer.String()); p == nil || p.Failures != 0 {
		t.Fatalf("stalled-by-tick peer penalised: %+v", p)
	}
	st, _ := a.svc.Peers(ctx, netA)
	for _, p := range st {
		if p.Unresolved && p.Failures != 0 {
			t.Fatalf("unresolved address penalised for the tick's own timeout: %+v", p)
		}
	}
}

func TestUnknownNetworkLooksLikeDeniedNetwork(t *testing.T) {
	deny := &stubAuth{denyInbound: true}
	a := newNode(t, nodeOpts{auth: deny})
	b := newNode(t, nodeOpts{})
	ctx := context.Background()
	fetch := func(net netid.NetworkID) (int, string) {
		resp, err := b.cl.Do(ctx, a.addr, a.id, transport.Request{Method: "GET", Path: pathPeers,
			Query: map[string][]string{queryNet: {net.String()}}}, transport.CallOptions{MaxResponseBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		body, _ := resp.ReadAll()
		return resp.StatusCode, string(body)
	}
	cu, bu := fetch(hexNet(0x77))
	ck, bk := fetch(testNet)
	if cu != ck || bu != bk {
		t.Fatalf("route-existence oracle: unknown=%d %q known=%d %q", cu, bu, ck, bk)
	}
}

// countHandler counts warning-and-above records.
type countHandler struct{ n atomic.Int32 }

func (c *countHandler) Enabled(context.Context, slog.Level) bool { return true }
func (c *countHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		c.n.Add(1)
	}
	return nil
}
func (c *countHandler) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *countHandler) WithGroup(string) slog.Handler      { return c }

func TestFailingStoreIsLoggedOncePerStateChange(t *testing.T) {
	ch := &countHandler{}
	old := slog.Default()
	slog.SetDefault(slog.New(ch))
	t.Cleanup(func() { slog.SetDefault(old) })

	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	if err != nil {
		t.Fatal(err)
	}
	store := database.NewPeernetPeerRepository(db.Connection(), database.DialectSQLite)
	svc, err := New(Config{Self: randID(t), Dialer: &recDialer{}, Store: store, AllowLoopbackForTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.SetNetwork(NetworkConfig{ID: testNet}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close() // every store call fails from now on
	for i := 0; i < 5; i++ {
		svc.Tick(context.Background())
	}
	if got := ch.n.Load(); got != 1 {
		t.Fatalf("%d log records for 5 identical failing ticks, want 1", got)
	}
	// A change of state is reported again after recovery.
	svc.logChange(context.Background(), svc.problemKey("list peers", NetworkConfig{ID: testNet}), nil)
	svc.Tick(context.Background())
	if got := ch.n.Load(); got != 2 {
		t.Fatalf("recovery then failure not logged again: %d", got)
	}
}

func TestLRUBounded(t *testing.T) {
	l := newLRU[int](3)
	for i := 0; i < 100; i++ {
		l.put(fmt.Sprint(i), i)
		if l.len() > 3 {
			t.Fatal("grew")
		}
	}
	if _, ok := l.get("99"); !ok {
		t.Fatal("most recent evicted")
	}
	if _, ok := l.get("0"); ok {
		t.Fatal("oldest kept")
	}
}
