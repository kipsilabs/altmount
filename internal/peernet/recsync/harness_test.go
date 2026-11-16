package recsync

import (
	"context"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// fakePeers is a PeerSource with a fixed list per network.
type fakePeers struct {
	mu   sync.Mutex
	list map[string][]Peer
}

func (f *fakePeers) set(nw netid.NetworkID, ps ...Peer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.list == nil {
		f.list = map[string][]Peer{}
	}
	f.list[nw.String()] = ps
}

func (f *fakePeers) SyncPeers(_ context.Context, nw netid.NetworkID, limit int) ([]Peer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ps := f.list[nw.String()]
	if len(ps) > limit {
		ps = ps[:limit]
	}
	return append([]Peer(nil), ps...), nil
}

// countingDialer counts calls per "METHOD path" and per target node.
type countingDialer struct {
	inner Dialer
	mu    sync.Mutex
	calls map[string]int
	nodes map[string]int
}

func (c *countingDialer) Do(ctx context.Context, addr string, expected identity.NodeID, req transport.Request, opts transport.CallOptions) (*transport.Response, error) {
	c.mu.Lock()
	if c.calls == nil {
		c.calls, c.nodes = map[string]int{}, map[string]int{}
	}
	c.calls[req.Method+" "+req.Path]++
	c.nodes[expected.String()]++
	c.mu.Unlock()
	return c.inner.Do(ctx, addr, expected, req, opts)
}

func (c *countingDialer) count(method, path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[method+" "+path]
}

func (c *countingDialer) nodeCalls(n identity.NodeID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[n.String()]
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// tnode is one node: key, database, exchange, optional listener.
type tnode struct {
	key    *identity.Key
	id     identity.NodeID
	db     *database.DB
	recs   *database.PeernetRecordRepository
	cur    *database.PeernetSyncCursorRepository
	peers  *fakePeers
	client *transport.Client
	dial   *countingDialer
	x      *Exchange
	clock  *testClock
	srv    *transport.Server
	addr   string
}

type nodeOpts struct {
	serve  bool
	limits Limits
}

func newTNode(t *testing.T, o nodeOpts) *tnode {
	t.Helper()
	key := mustKey(t)
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "n.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	clock := func() time.Time { return testNow }
	n := &tnode{key: key, id: key.NodeID(), db: db, peers: &fakePeers{}, clock: &testClock{t: testNow}}
	n.recs = database.NewPeernetRecordRepositoryWithClock(db.Connection(), db.Dialect(), clock)
	n.cur = database.NewPeernetSyncCursorRepositoryWithClock(db.Connection(), db.Dialect(), clock)
	n.client, err = transport.NewClient(transport.ClientConfig{Identity: key, AllowLoopbackForTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.client.Close)
	n.dial = &countingDialer{inner: n.client}
	n.x, err = New(Config{Self: n.id, Store: n.recs, Cursors: n.cur, Dialer: n.dial, Auth: discovery.AllowAll{},
		Peers: n.peers, Limits: o.limits, Now: n.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	if o.serve {
		n.srv, err = transport.NewServer(transport.ServerConfig{Identity: key, ListenAddr: "127.0.0.1:0"})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.x.RegisterRoutes(n.srv); err != nil {
			t.Fatal(err)
		}
		if err := n.srv.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = n.srv.Stop(ctx)
		})
		n.addr = n.srv.Addr().String()
	}
	return n
}

// peer is how another node sees this one.
func (n *tnode) peer() Peer { return Peer{NodeID: n.id, Address: n.addr, Trusted: true} }

func (n *tnode) join(nw netid.NetworkID, level int) {
	n.x.SetNetwork(NetworkInfo{ID: nw, Level: level})
}

// publishRelease publishes a release record signed by this node.
func (n *tnode) publishRelease(t *testing.T, nw netid.NetworkID, seed int) []byte {
	t.Helper()
	_, raw, err := records.BuildReleaseRecord(n.key, nw, relInput(seed), testNow)
	if err != nil {
		t.Fatal(err)
	}
	out, err := n.x.Publish(context.Background(), nw, raw)
	if err != nil || out.Result != ResultStored {
		t.Fatalf("publish: %+v %v", out, err)
	}
	return raw
}

func (n *tnode) count(t *testing.T, nw netid.NetworkID) int {
	t.Helper()
	st, err := n.recs.Stats(context.Background(), nw.String())
	if err != nil {
		t.Fatal(err)
	}
	return st.Total
}

// rawGet issues a records GET as this node.
func (n *tnode) rawGet(t *testing.T, to *tnode, nw netid.NetworkID, since string) (*transport.Response, error) {
	t.Helper()
	q := url.Values{"n": {nw.String()}}
	if since != "" {
		q.Set("since", since)
	}
	return n.client.Do(context.Background(), to.addr, to.id, transport.Request{
		Method: http.MethodGet, Path: PathRecords, Query: q,
	}, transport.CallOptions{MaxResponseBytes: MaxPageBytes, TrustedAddress: true})
}

func (n *tnode) rawPost(t *testing.T, to *tnode, nw netid.NetworkID, body []byte) (*transport.Response, error) {
	t.Helper()
	return n.client.Do(context.Background(), to.addr, to.id, transport.Request{
		Method: http.MethodPost, Path: PathRecords, Query: url.Values{"n": {nw.String()}}, Body: body,
	}, transport.CallOptions{MaxResponseBytes: 4096, TrustedAddress: true})
}

func (n *tnode) page(t *testing.T, from *tnode, nw netid.NetworkID, since string) Page {
	t.Helper()
	resp, err := n.rawGet(t, from, nw, since)
	if err != nil {
		t.Fatal(err)
	}
	body, err := resp.ReadAll()
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d err %v", resp.StatusCode, err)
	}
	p, err := DecodePage(body)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
