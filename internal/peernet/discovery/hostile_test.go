package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// hostile starts a transport server whose peers route answers with body.
func hostile(t *testing.T, body func() []byte) (addr string, id identity.NodeID) {
	t.Helper()
	key := mustKey(t)
	srv, err := transport.NewServer(transport.ServerConfig{Identity: key, ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	err = srv.Handle(RoutePeers, 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body())
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = srv.Stop(ctx)
	})
	return srv.Addr().String(), key.NodeID()
}

func entries(n int, addr func(i int) string) []PeerEntry {
	out := make([]PeerEntry, n)
	for i := range out {
		k, _ := identity.GenerateKey()
		out[i] = PeerEntry{NodeID: k.NodeID().String(), Address: addr(i)}
	}
	return out
}

func asBody(es []PeerEntry) func() []byte {
	b, _ := json.Marshal(peersResponse{Peers: es})
	return func() []byte { return b }
}

func exchangeWith(t *testing.T, a *tnode, body func() []byte) (int, error) {
	t.Helper()
	addr, id := hostile(t, body)
	return a.svc.Exchange(context.Background(), NetworkConfig{ID: testNet}, addr, id)
}

func TestHostileExchangeIntakeCap(t *testing.T) {
	a := newNode(t, nodeOpts{})
	n, err := exchangeWith(t, a, asBody(entries(MaxPeerEntries, func(i int) string { return fmt.Sprintf("8.8.8.%d:7000", i+1) })))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("stored %d, want at most 5", n)
	}
	c, _ := a.svc.Counts(context.Background(), testNet)
	if c.Exchange != 5 || c.Verified != 0 {
		t.Fatalf("%+v", c)
	}
}

func TestHostileExchangeResponses(t *testing.T) {
	self := newNode(t, nodeOpts{})
	good := entries(1, func(int) string { return "8.8.8.8:7000" })[0]
	big := strings.Repeat("a", MaxPeersResponseBytes+10)
	cases := []struct {
		name    string
		body    []byte
		wantErr bool
		wantNew int
	}{
		{"oversized body", []byte(`{"peers":[],"pad":"` + big + `"}`), true, 0},
		{"too many entries", func() []byte {
			b, _ := json.Marshal(peersResponse{Peers: entries(MaxPeerEntries+1, func(int) string { return "8.8.8.8:1" })})
			return b
		}(), true, 0},
		{"not json", []byte(`<html>`), true, 0},
		{"trailing data", []byte(`{"peers":[]} {"peers":[]}`), true, 0},
		{"unknown field", []byte(`{"peers":[],"extra":1}`), true, 0},
		{"wrong types", []byte(`{"peers":[{"node_id":1,"address":2}]}`), true, 0},
		{"null", []byte(`null`), false, 0},
		{"empty", []byte(``), true, 0},
		{"invalid ids and addresses", func() []byte {
			b, _ := json.Marshal(peersResponse{Peers: []PeerEntry{
				{NodeID: "zz", Address: "8.8.8.8:1"},
				{NodeID: strings.Repeat("0", 64), Address: "8.8.8.8:1"},
				{NodeID: strings.ToUpper(good.NodeID), Address: "8.8.8.8:1"},
				{NodeID: good.NodeID, Address: "http://8.8.8.8:1/"},
				{NodeID: good.NodeID, Address: "user@8.8.8.8:1"},
				{NodeID: good.NodeID, Address: "8.8.8.8"},
				{NodeID: good.NodeID, Address: "8.8.8.8:0"},
				{NodeID: good.NodeID, Address: strings.Repeat("a", 300) + ":1"},
			}})
			return b
		}(), false, 0},
		{"private and local addresses refused", func() []byte {
			b, _ := json.Marshal(peersResponse{Peers: entries(6, func(i int) string {
				return []string{"10.0.0.1:1", "192.168.1.1:1", "169.254.169.254:80", "[fd00::1]:1", "172.16.0.1:1", "100.64.0.1:1"}[i]
			})})
			return b
		}(), false, 0},
		{"duplicates and self", func() []byte {
			dup := entries(1, func(int) string { return "8.8.8.8:2" })[0]
			b, _ := json.Marshal(peersResponse{Peers: []PeerEntry{
				dup, dup, {NodeID: dup.NodeID, Address: "8.8.4.4:2"}, {NodeID: self.id.String(), Address: "8.8.8.8:9"},
			}})
			return b
		}(), false, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := self
			// Fresh store state per case: use a new node but same self id is
			// needed for the self case, so reuse and compare counts instead.
			before, _ := a.svc.Counts(context.Background(), testNet)
			body := tc.body
			n, err := exchangeWith(t, a, func() []byte { return body })
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			after, _ := a.svc.Counts(context.Background(), testNet)
			if n != tc.wantNew || after.Total-before.Total != tc.wantNew {
				t.Fatalf("stored %d (delta %d), want %d", n, after.Total-before.Total, tc.wantNew)
			}
			if a.peer(self.id) != nil {
				t.Fatal("stored self")
			}
		})
	}
}

func TestHostileExchangeNeverAcceptsPrivateEvenWhereConfigured(t *testing.T) {
	a := newNode(t, nodeOpts{})
	body := asBody(entries(2, func(i int) string { return []string{"10.1.1.1:1", "192.168.0.5:1"}[i] }))
	addr, id := hostile(t, body)
	// Private addresses may only come from hello, the user's list or a local
	// Source, never from a peer's exchange, whatever the network allows.
	n, err := a.svc.Exchange(context.Background(), NetworkConfig{ID: testNet, AllowPrivate: true}, addr, id)
	if err != nil || n != 0 {
		t.Fatalf("allowPrivate network: n=%d err=%v", n, err)
	}
	// Even with allowPrivate, loopback-class and link-local stay refused.
	b := newNode(t, nodeOpts{})
	addr, id = hostile(t, asBody(entries(1, func(int) string { return "169.254.1.1:1" })))
	n, err = b.svc.Exchange(context.Background(), NetworkConfig{ID: testNet, AllowPrivate: true}, addr, id)
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestHostileExchangeDoesNotOverwriteOrRefreshKnownPeers(t *testing.T) {
	a := newNode(t, nodeOpts{})
	known := randID(t)
	a.seed(known, "8.8.8.8:1", database.PeernetSourceUser, true)
	before := a.peer(known)
	a.clock.Advance(time.Hour)
	es := []PeerEntry{{NodeID: known.String(), Address: "8.8.4.4:99"}}
	if n, err := exchangeWith(t, a, asBody(es)); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	after := a.peer(known)
	if after.Address != before.Address || !after.LastSeen.Equal(before.LastSeen) || after.Source != database.PeernetSourceUser {
		t.Fatalf("known peer altered by hearsay: %+v", after)
	}
}

func TestIntakeRespectsStoreCaps(t *testing.T) {
	a := newNode(t, nodeOpts{limits: Limits{MaxExchangePeers: 3, MaxNewPerExchange: 10}})
	for round := 0; round < 3; round++ {
		a.clock.Advance(time.Minute)
		if _, err := exchangeWith(t, a, asBody(entries(10, func(i int) string { return fmt.Sprintf("8.8.8.%d:1", i+1) }))); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := a.svc.Counts(context.Background(), testNet)
	if c.Exchange != 3 {
		t.Fatalf("exchange rows %d, cap 3", c.Exchange)
	}
}

func TestHostileExchangeHeaderStatuses(t *testing.T) {
	a := newNode(t, nodeOpts{})
	key := mustKey(t)
	srv, _ := transport.NewServer(transport.ServerConfig{Identity: key, ListenAddr: "127.0.0.1:0"})
	_ = srv.Handle(RoutePeers, 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://127.0.0.1:1/", http.StatusFound)
	}))
	_ = srv.Start(context.Background())
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	_, err := a.svc.Exchange(context.Background(), NetworkConfig{ID: testNet}, srv.Addr().String(), key.NodeID())
	if !errors.Is(err, errPeerStatus) {
		t.Fatalf("redirect must be a plain failure: %v", err)
	}
}

func TestAuthorizerServerDenial(t *testing.T) {
	deny := &stubAuth{denyInbound: true}
	server := newNode(t, nodeOpts{auth: deny})
	client := newNode(t, nodeOpts{})
	n, err := client.svc.Exchange(context.Background(), NetworkConfig{ID: testNet}, server.addr, server.id)
	if !errors.Is(err, errPeerStatus) || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	// Hello is denied too, and nothing is stored.
	err = client.svc.Hello(context.Background(), NetworkConfig{ID: testNet, AdvertiseAddr: "8.8.8.8:1"}, server.addr, server.id)
	if !errors.Is(err, errPeerStatus) {
		t.Fatalf("hello: %v", err)
	}
	if server.peer(client.id) != nil {
		t.Fatal("denied peer stored")
	}
	if len(deny.sawNets) != 2 || deny.sawNets[0] != testNet.String() {
		t.Fatalf("authoriser not called with the network: %v", deny.sawNets)
	}
}

func TestAuthorizerClientSide(t *testing.T) {
	srvAuth := &stubAuth{}
	server := newNode(t, nodeOpts{auth: srvAuth})
	ctx := context.Background()
	nc := NetworkConfig{ID: testNet}

	ok := newNode(t, nodeOpts{auth: &stubAuth{requireProof: true}})
	if _, err := ok.svc.Exchange(ctx, nc, server.addr, server.id); err != nil {
		t.Fatalf("decorate + verify path: %v", err)
	}
	if srvAuth.sawHeader != "token" {
		t.Fatalf("server did not see the decorated request: %q", srvAuth.sawHeader)
	}

	noCreds := newNode(t, nodeOpts{auth: &stubAuth{failOutbound: true}})
	if _, err := noCreds.svc.Exchange(ctx, nc, server.addr, server.id); err == nil {
		t.Fatal("outbound decoration failure must fail the call")
	}

	badProof := newNode(t, nodeOpts{auth: &stubAuth{failVerify: true}})
	if _, err := badProof.svc.Exchange(ctx, nc, server.addr, server.id); err == nil {
		t.Fatal("response verification failure must fail the call")
	}

	// A server without the proof header fails a client that requires it.
	plain := newNode(t, nodeOpts{})
	strict := newNode(t, nodeOpts{auth: &stubAuth{requireProof: true}})
	if _, err := strict.svc.Exchange(ctx, nc, plain.addr, plain.id); err == nil {
		t.Fatal("unverifiable response accepted")
	}
}

func TestServerRouteValidation(t *testing.T) {
	a := newNode(t, nodeOpts{})
	b := newNode(t, nodeOpts{})
	ctx := context.Background()
	do := func(method, path string, q map[string][]string, body []byte) int {
		resp, err := b.cl.Do(ctx, a.addr, a.id, transport.Request{Method: method, Path: path, Query: q, Body: body},
			transport.CallOptions{MaxResponseBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	good := map[string][]string{queryNet: {testNet.String()}}
	if c := do("GET", pathPeers, nil, nil); c != 400 {
		t.Errorf("missing network: %d", c)
	}
	if c := do("GET", pathPeers, map[string][]string{queryNet: {"nothex"}}, nil); c != 400 {
		t.Errorf("bad network: %d", c)
	}
	if c := do("GET", pathPeers, map[string][]string{queryNet: {testNet.String(), testNet.String()}}, nil); c != 400 {
		t.Errorf("duplicate network param: %d", c)
	}
	if c := do("GET", pathPeers, map[string][]string{queryNet: {hexNet(7).String()}}, nil); c != 403 {
		t.Errorf("unknown network: %d", c)
	}
	if c := do("GET", pathPeers, good, nil); c != 200 {
		t.Errorf("ok: %d", c)
	}
	if c := do("POST", pathHello, good, []byte(`{"address":""}`)); c != 200 {
		t.Errorf("empty hello: %d", c)
	}
	if c := do("POST", pathHello, good, []byte(`{"address":"8.8.8.8:1","x":1}`)); c != 400 {
		t.Errorf("unknown field: %d", c)
	}
	if c := do("POST", pathHello, good, []byte(`garbage`)); c != 400 {
		t.Errorf("garbage: %d", c)
	}
	if c := do("POST", pathHello, good, []byte(`{"address":"`+strings.Repeat("a", MaxHelloBody)+`"}`)); c != 413 {
		t.Errorf("oversized hello body must hit the route cap: %d", c)
	}
	if c := do("GET", pathPeers, good, []byte("body not allowed")); c != 413 {
		t.Errorf("body on GET route: %d", c)
	}
}

func TestRegisterRoutesTwiceFails(t *testing.T) {
	a := newNode(t, nodeOpts{})
	srv, _ := transport.NewServer(transport.ServerConfig{Identity: mustKey(t), ListenAddr: "127.0.0.1:0"})
	if err := RegisterRoutes(srv, a.svc); err != nil {
		t.Fatal(err)
	}
	if err := RegisterRoutes(srv, a.svc); err == nil {
		t.Fatal("duplicate registration accepted")
	}
}

func TestRemovedNetworkStopsServing(t *testing.T) {
	a := newNode(t, nodeOpts{})
	b := newNode(t, nodeOpts{})
	a.svc.RemoveNetwork(testNet)
	resp, err := b.svc.call(context.Background(), NetworkConfig{ID: testNet}, a.addr, a.id, "GET", pathPeers, nil, 1024, false)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("%d", resp.StatusCode)
	}
}

func TestHelloLimiterIsBounded(t *testing.T) {
	a := newNode(t, nodeOpts{limits: Limits{MaxTrackedHello: 3, HelloPerPrefixPerInterval: 100, HelloGlobalPerWindow: 1000}})
	now := a.clock.Now()
	for i := 0; i < 50; i++ {
		ip := netip.MustParseAddr(fmt.Sprintf("8.%d.8.8", i))
		if !a.svc.helloAllowed("n", randID(t), ip, now) {
			t.Fatalf("fresh identity %d refused: a full table must evict, not lock out", i)
		}
		a.svc.mu.Lock()
		nodes, prefixes := a.svc.helloLim.nodes.len(), a.svc.helloLim.prefixes.len()
		a.svc.mu.Unlock()
		if nodes > 3 || prefixes > 3 {
			t.Fatalf("limiter grew past its bound: %d %d", nodes, prefixes)
		}
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("empty config accepted")
	}
	a := newNode(t, nodeOpts{})
	if err := a.svc.SetNetwork(NetworkConfig{}); err == nil {
		t.Fatal("zero network accepted")
	}
	if err := a.svc.SetNetwork(NetworkConfig{ID: testNet, AdvertiseAddr: "bad"}); err == nil {
		t.Fatal("bad advertise address accepted")
	}
	if err := a.svc.SetNetwork(NetworkConfig{ID: testNet, AdvertiseAddr: "10.0.0.1:1"}); err == nil {
		t.Fatal("private advertise address accepted without allowance")
	}
}

func TestDefaultsApplied(t *testing.T) {
	l := Limits{ExchangeSampleSize: 1000}.withDefaults()
	if l.ExchangeSampleSize != MaxPeerEntries || l.MaxNewPerExchange != 5 || l.DialBacksPerWindow != 20 {
		t.Fatalf("%+v", l)
	}
}

func TestAllowAll(t *testing.T) {
	var a Authorizer = AllowAll{}
	dec, err := a.AuthorizeInbound(context.Background(), testNet, identity.NodeID{}, http.Header{})
	if err != nil || dec == nil {
		t.Fatal("allow-all denied")
	}
	dec(http.Header{})
	if a.DecorateOutbound(context.Background(), testNet, identity.NodeID{}, http.Header{}) != nil ||
		a.VerifyResponse(context.Background(), testNet, identity.NodeID{}, 500, http.Header{}) != nil {
		t.Fatal("allow-all must accept")
	}
}
