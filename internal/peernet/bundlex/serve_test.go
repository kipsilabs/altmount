package bundlex

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

var (
	nwA = netid.NetworkID{0xa}
	nwB = netid.NetworkID{0xb}
	nwC = netid.NetworkID{0xc, 0xc}
)

type fakeNets struct {
	mu sync.Mutex
	m  map[netid.NetworkID]int
}

func (f *fakeNets) NetworkLevel(id netid.NetworkID) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.m[id]
	return l, ok
}
func (f *fakeNets) set(id netid.NetworkID, l int) { f.mu.Lock(); f.m[id] = l; f.mu.Unlock() }

type fakeAnn struct {
	mu       sync.Mutex
	byNet    map[netid.NetworkID]map[bundle.BundleID]Announcement
	withdrew []string
	stale    []string
}

func (f *fakeAnn) Announced(_ context.Context, nw netid.NetworkID, id bundle.BundleID) (Announcement, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.byNet[nw][id]
	return a, ok, nil
}
func (f *fakeAnn) Withdraw(_ context.Context, _ netid.NetworkID, rel string) error {
	f.mu.Lock()
	f.withdrew = append(f.withdrew, rel)
	f.mu.Unlock()
	return nil
}
func (f *fakeAnn) MarkStale(_ context.Context, _ netid.NetworkID, rel string) error {
	f.mu.Lock()
	f.stale = append(f.stale, rel)
	f.mu.Unlock()
	return nil
}

type fakeProvider struct {
	built *Built
	err   error
	gate  chan struct{}
	calls int
	mu    sync.Mutex
}

func (f *fakeProvider) Bundle(context.Context, string) (*Built, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.gate != nil {
		<-f.gate
	}
	return f.built, f.err
}

type denyAuth struct{ discovery.AllowAll }

func (denyAuth) AuthorizeInbound(context.Context, netid.NetworkID, identity.NodeID, http.Header) (discovery.ResponseDecorator, error) {
	return nil, errors.New("not a member")
}

type serveEnv struct {
	srv    *Server
	nets   *fakeNets
	ann    *fakeAnn
	prov   *fakeProvider
	excl   *publish.Exclusions
	client *transport.Client
	addr   string
	id     identity.NodeID
	built  *Built
}

func newServeEnv(t *testing.T, mut func(*ServerConfig)) *serveEnv {
	t.Helper()
	lib, rel := nzbRelease()
	built, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "T", posted)
	if err != nil {
		t.Fatal(err)
	}
	e := &serveEnv{nets: &fakeNets{m: map[netid.NetworkID]int{nwA: 3}}, prov: &fakeProvider{built: built},
		ann:  &fakeAnn{byNet: map[netid.NetworkID]map[bundle.BundleID]Announcement{nwA: {built.ID: {ReleaseID: rel, OriginCategory: "movies", OriginIndexer: "idx"}}}},
		excl: &publish.Exclusions{}, built: built}
	cfg := ServerConfig{Networks: e.nets, Announcements: e.ann, Provider: e.prov, Exclusions: func() publish.Exclusions { return *e.excl }}
	if mut != nil {
		mut(&cfg)
	}
	e.srv = NewServer(cfg)
	key, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	ts, err := transport.NewServer(transport.ServerConfig{Identity: key, ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.srv.RegisterRoutes(ts); err != nil {
		t.Fatal(err)
	}
	if err := ts.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = ts.Stop(ctx)
	})
	e.addr, e.id = ts.Addr().String(), key.NodeID()
	ck, _ := identity.GenerateKey()
	e.client, err = transport.NewClient(transport.ClientConfig{Identity: ck, AllowLoopbackForTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.client.Close)
	return e
}

func (e *serveEnv) get(t *testing.T, nw netid.NetworkID, b string, hdr http.Header) (int, http.Header, []byte) {
	t.Helper()
	return e.getFrom(t, e.client, nw, b, hdr)
}

func (e *serveEnv) getFrom(t *testing.T, c *transport.Client, nw netid.NetworkID, b string, hdr http.Header) (int, http.Header, []byte) {
	t.Helper()
	resp, err := c.Do(context.Background(), e.addr, e.id, transport.Request{
		Method: http.MethodGet, Path: PathBundle, Query: url.Values{"n": {nw.String()}, "b": {b}}, Header: hdr,
	}, transport.CallOptions{MaxResponseBytes: bundle.MaxCompressedSize, TrustedAddress: true, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	body, err := resp.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, body
}

func TestServeSendsExactBytesWithoutRangeOrCompression(t *testing.T) {
	e := newServeEnv(t, nil)
	hdr := http.Header{"Range": {"bytes=10-20"}, "Accept-Encoding": {"gzip, br"}}
	code, h, body := e.get(t, nwA, e.built.ID.String(), hdr)
	if code != 200 || !bytes.Equal(body, e.built.Wire) {
		t.Fatalf("status %d, %d bytes", code, len(body))
	}
	if h.Get("Content-Length") != itoa(len(e.built.Wire)) || h.Get("Content-Encoding") != "" || h.Get("Accept-Ranges") != "none" {
		t.Fatalf("headers %v", h)
	}
	st := e.srv.Status()
	if st.Served != 1 || st.Bytes != int64(len(e.built.Wire)) {
		t.Fatalf("status %+v", st)
	}
}

func TestServeRefusalsAreIdentical(t *testing.T) {
	e := newServeEnv(t, nil)
	e.nets.set(nwC, 2)
	// The bundle is announced in nwC too, but nwC is only at level 2.
	e.ann.byNet[nwC] = map[bundle.BundleID]Announcement{e.built.ID: {ReleaseID: e.built.ReleaseID}}
	e.nets.set(nwB, 3)
	unknown := bytes.Repeat([]byte{7}, 32)
	cases := map[string]func() (int, []byte){
		"unknown network": func() (int, []byte) { c, _, b := e.get(t, netid.NetworkID{0xc}, e.built.ID.String(), nil); return c, b },
		"level 2":         func() (int, []byte) { c, _, b := e.get(t, nwC, e.built.ID.String(), nil); return c, b },
		"unknown bundle":  func() (int, []byte) { c, _, b := e.get(t, nwA, bundle.BundleID(unknown).String(), nil); return c, b },
		"announced only in another network": func() (int, []byte) {
			c, _, b := e.get(t, nwB, e.built.ID.String(), nil)
			return c, b
		},
	}
	var wantCode int
	var wantBody []byte
	for name, f := range cases {
		code, body := f()
		if code != http.StatusForbidden {
			t.Errorf("%s: status %d", name, code)
		}
		if wantBody == nil {
			wantCode, wantBody = code, body
		}
		if code != wantCode || !bytes.Equal(body, wantBody) {
			t.Errorf("%s: answer differs: %d %q", name, code, body)
		}
	}
	if e.prov.calls != 0 {
		t.Fatal("a refused request caused a build")
	}
}

func TestServeDeniedCallerAndBackedOffPeerAreRefused(t *testing.T) {
	e := newServeEnv(t, func(c *ServerConfig) { c.Auth = denyAuth{} })
	if code, _, body := e.get(t, nwA, e.built.ID.String(), nil); code != 403 || e.prov.calls != 0 {
		t.Fatalf("denied caller: %d %q", code, body)
	}
	e2 := newServeEnv(t, func(c *ServerConfig) {
		c.BackedOff = func(netid.NetworkID, identity.NodeID, string) bool { return true }
	})
	if code, _, _ := e2.get(t, nwA, e2.built.ID.String(), nil); code != 403 {
		t.Fatalf("backed off peer: %d", code)
	}
}

func TestServeRechecksExclusionsAndWithdraws(t *testing.T) {
	e := newServeEnv(t, nil)
	e.excl.Indexers = []string{"IDX"}
	if code, _, _ := e.get(t, nwA, e.built.ID.String(), nil); code != 403 {
		t.Fatalf("excluded release served: %d", code)
	}
	if len(e.ann.withdrew) != 1 || e.prov.calls != 0 {
		t.Fatalf("withdrew %v, builds %d", e.ann.withdrew, e.prov.calls)
	}
}

func TestServeNeverServesDifferentBytesThanAnnounced(t *testing.T) {
	e := newServeEnv(t, nil)
	other := *e.built
	other.ID[0] ^= 0xff
	e.prov.built = &other
	if code, _, _ := e.get(t, nwA, e.built.ID.String(), nil); code != 403 {
		t.Fatalf("status %d", code)
	}
	if len(e.ann.stale) != 1 {
		t.Fatalf("stale %v", e.ann.stale)
	}
	e.prov.built, e.prov.err = nil, ErrBusy
	if code, _, _ := e.get(t, nwA, e.built.ID.String(), nil); code != 503 {
		t.Fatalf("busy status %d", code)
	}
}

func TestServePerPeerRateLimit(t *testing.T) {
	e := newServeEnv(t, func(c *ServerConfig) { c.Limits = ServeLimits{PerPeerPerHour: 1, PerPeerBurst: 3} })
	for i := range 3 {
		if code, _, _ := e.get(t, nwA, e.built.ID.String(), nil); code != 200 {
			t.Fatalf("request %d: %d", i, code)
		}
	}
	if code, _, _ := e.get(t, nwA, e.built.ID.String(), nil); code != http.StatusTooManyRequests {
		t.Fatalf("over the burst: %d", code)
	}
	// Another peer has its own allowance (loopback: no per-address limit).
	ck, _ := identity.GenerateKey()
	c2, err := transport.NewClient(transport.ClientConfig{Identity: ck, AllowLoopbackForTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if code, _, _ := e.getFrom(t, c2, nwA, e.built.ID.String(), nil); code != 200 {
		t.Fatalf("second peer: %d", code)
	}
}

func TestServeNodeWideByteBudget(t *testing.T) {
	var size int64
	e := newServeEnv(t, nil)
	size = int64(len(e.built.Wire))
	e = newServeEnv(t, func(c *ServerConfig) { c.Limits = ServeLimits{BytesBurst: size*2 + 1, BytesPerMinute: 1} })
	for i := range 2 {
		if code, _, _ := e.get(t, nwA, e.built.ID.String(), nil); code != 200 {
			t.Fatalf("request %d: %d", i, code)
		}
	}
	if code, _, _ := e.get(t, nwA, e.built.ID.String(), nil); code != http.StatusTooManyRequests {
		t.Fatalf("over the byte budget: %d", code)
	}
}

func TestServeConcurrencyPerPeer(t *testing.T) {
	e := newServeEnv(t, nil)
	e.prov.gate = make(chan struct{})
	done := make(chan int, 1)
	go func() { c, _, _ := e.get(t, nwA, e.built.ID.String(), nil); done <- c }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		e.prov.mu.Lock()
		n := e.prov.calls
		e.prov.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if code, _, _ := e.get(t, nwA, e.built.ID.String(), nil); code != http.StatusTooManyRequests {
		t.Fatalf("second concurrent request from one peer: %d", code)
	}
	close(e.prov.gate)
	if c := <-done; c != 200 {
		t.Fatalf("first request: %d", c)
	}
}

func TestServeMalformedRequests(t *testing.T) {
	e := newServeEnv(t, nil)
	for _, b := range []string{"zz", "ABCDEF", e.built.ID.String()[:62]} {
		if code, _, _ := e.get(t, nwA, b, nil); code != http.StatusBadRequest {
			t.Errorf("%q: %d", b, code)
		}
	}
}

func TestSharedIPClassification(t *testing.T) {
	for ip, want := range map[string]bool{"10.1.2.3": true, "127.0.0.1": true, "192.168.0.9": true, "fe80::1": true, "8.8.8.8": false, "2001:4860::1": false} {
		if sharedIP(ip) != want {
			t.Errorf("%s: shared = %v", ip, !want)
		}
	}
}
