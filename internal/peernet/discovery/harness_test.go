package discovery

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Now()} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type nodeOpts struct {
	auth      Authorizer
	limits    Limits
	netCfg    func(*NetworkConfig)
	wrap      func(Dialer) Dialer
	advertise bool
	noRoutes  bool
	nets      []netid.NetworkID
}

type tnode struct {
	t     *testing.T
	key   *identity.Key
	id    identity.NodeID
	addr  string
	svc   *Service
	store *database.PeernetPeerRepository
	clock *fakeClock
	srv   *transport.Server
	cl    *transport.Client
}

var testNet = netid.Open()

func mustKey(t *testing.T) *identity.Key {
	t.Helper()
	k, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func newNode(t *testing.T, o nodeOpts) *tnode {
	t.Helper()
	return newNodeWithKey(t, mustKey(t), "127.0.0.1:0", o)
}

func newNodeWithKey(t *testing.T, key *identity.Key, listen string, o nodeOpts) *tnode {
	t.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := database.NewPeernetPeerRepository(db.Connection(), database.DialectSQLite)
	cl, err := transport.NewClient(transport.ClientConfig{Identity: key, AllowLoopbackForTesting: true,
		DialTimeout: 2 * time.Second, HandshakeTimeout: 2 * time.Second, ResponseHeaderTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	srv, err := transport.NewServer(transport.ServerConfig{Identity: key, ListenAddr: listen})
	if err != nil {
		t.Fatal(err)
	}
	clk := newClock()
	var d Dialer = cl
	if o.wrap != nil {
		d = o.wrap(cl)
	}
	svc, err := New(Config{Self: key.NodeID(), Dialer: d, Store: store, Auth: o.auth, Limits: o.limits,
		Now: clk.Now, AllowLoopbackForTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	if !o.noRoutes {
		if err := RegisterRoutes(srv, svc); err != nil {
			t.Fatal(err)
		}
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})
	n := &tnode{t: t, key: key, id: key.NodeID(), addr: srv.Addr().String(), svc: svc, store: store, clock: clk, srv: srv, cl: cl}
	nets := o.nets
	if len(nets) == 0 {
		nets = []netid.NetworkID{testNet}
	}
	for _, id := range nets {
		nc := NetworkConfig{ID: id}
		if o.advertise {
			nc.AdvertiseAddr = n.addr
		}
		if o.netCfg != nil {
			o.netCfg(&nc)
		}
		if err := svc.SetNetwork(nc); err != nil {
			t.Fatal(err)
		}
	}
	return n
}

// seed stores a peer directly.
func (n *tnode) seed(id identity.NodeID, addr, source string, verified bool) {
	n.seedIn(testNet, id, addr, source, verified)
}

func (n *tnode) seedIn(net netid.NetworkID, id identity.NodeID, addr, source string, verified bool) {
	n.t.Helper()
	now := n.clock.Now()
	_, err := n.store.Insert(context.Background(), database.PeernetPeer{NetworkID: net.String(), NodeID: id.String(),
		Address: addr, Source: source, FirstSeen: now, LastSeen: now}, database.PeernetPeerCaps{})
	if err != nil {
		n.t.Fatal(err)
	}
	if verified {
		if err := n.store.RecordSuccess(context.Background(), net.String(), id.String(), "", now, now); err != nil {
			n.t.Fatal(err)
		}
	}
}

func (n *tnode) peer(id identity.NodeID) *database.PeernetPeer {
	n.t.Helper()
	p, err := n.store.Get(context.Background(), testNet.String(), id.String())
	if err != nil {
		n.t.Fatal(err)
	}
	return p
}

// rawPeers asks target for its peer list the way a client would, and decodes it.
func (n *tnode) rawPeers(target *tnode) []PeerEntry {
	n.t.Helper()
	resp, err := n.svc.call(context.Background(), NetworkConfig{ID: testNet}, target.addr, target.id,
		http.MethodGet, pathPeers, nil, MaxPeersResponseBytes, false)
	if err != nil {
		n.t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		n.t.Fatalf("status %d", resp.StatusCode)
	}
	b, err := resp.ReadAll()
	if err != nil {
		n.t.Fatal(err)
	}
	e, err := DecodePeers(b)
	if err != nil {
		n.t.Fatal(err)
	}
	return e
}

func randID(t *testing.T) identity.NodeID { return mustKey(t).NodeID() }

func tick(n *tnode) TickResult { return n.svc.Tick(context.Background()) }

// recDialer records calls and fails them, to observe what would be dialled.
type recDialer struct {
	mu    sync.Mutex
	calls []recCall
}

type recCall struct {
	addr         string
	allowPrivate bool
	net          string
	trusted      bool
}

func (r *recDialer) Do(_ context.Context, addr string, _ identity.NodeID, req transport.Request, opts transport.CallOptions) (*transport.Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recCall{addr, opts.AllowPrivate, req.Query.Get(queryNet), opts.TrustedAddress})
	return nil, errors.New("recorded")
}

func (r *recDialer) ProbeNodeID(_ context.Context, addr string, opts transport.CallOptions) (identity.NodeID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recCall{addr, opts.AllowPrivate, "", opts.TrustedAddress})
	return identity.NodeID{}, errors.New("recorded")
}

func (r *recDialer) snapshot() []recCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recCall(nil), r.calls...)
}

// stubAuth is a configurable Authorizer.
type stubAuth struct {
	denyInbound  bool
	failOutbound bool
	failVerify   bool
	requireProof bool
	mu           sync.Mutex
	sawHeader    string
	sawNets      []string
}

func (a *stubAuth) AuthorizeInbound(_ context.Context, n netid.NetworkID, _ identity.NodeID, h http.Header) (ResponseDecorator, error) {
	a.mu.Lock()
	a.sawNets = append(a.sawNets, n.String())
	a.sawHeader = h.Get("X-Test-Cred")
	a.mu.Unlock()
	if a.denyInbound {
		return nil, errors.New("denied")
	}
	return func(h http.Header) { h.Set("X-Test-Proof", "ok") }, nil
}

func (a *stubAuth) DecorateOutbound(_ context.Context, _ netid.NetworkID, _ identity.NodeID, h http.Header) error {
	if a.failOutbound {
		return errors.New("no credentials")
	}
	h.Set("X-Test-Cred", "token")
	return nil
}

func (a *stubAuth) VerifyResponse(_ context.Context, _ netid.NetworkID, _ identity.NodeID, _ int, h http.Header) error {
	if a.failVerify || a.requireProof && h.Get("X-Test-Proof") != "ok" {
		return errors.New("bad proof")
	}
	return nil
}

func hexNet(c byte) netid.NetworkID {
	var n netid.NetworkID
	for i := range n {
		n[i] = c
	}
	return n
}
