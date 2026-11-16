package membership

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

type clock struct{ off atomic.Int64 }

func (c *clock) now() time.Time { return time.Now().Add(time.Duration(c.off.Load())) }
func (c *clock) advance(d time.Duration) {
	c.off.Add(int64(d))
}

type node struct {
	key    *identity.Key
	mgr    *Manager
	server *transport.Server
	addr   string
	client *transport.Client
}

func newNode(t *testing.T, clk *clock, limits RedeemLimits, serve bool) *node {
	t.Helper()
	key := newID(t)
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	client, err := transport.NewClient(transport.ClientConfig{Identity: key, AllowLoopbackForTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	mgr, err := NewManager(Config{
		Store:   database.NewPeernetNetworkRepository(db.Connection(), database.DialectSQLite),
		KeysDir: filepath.Join(t.TempDir(), "keys"), NodeID: key.NodeID(), Caller: client,
		Now: clk.now, RedeemLimits: limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	n := &node{key: key, mgr: mgr, client: client}
	if serve {
		srv, err := transport.NewServer(transport.ServerConfig{Identity: key, ListenAddr: "127.0.0.1:0"})
		if err != nil {
			t.Fatal(err)
		}
		if err := mgr.RegisterHandlers(srv); err != nil {
			t.Fatal(err)
		}
		if err := srv.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Stop(ctx)
		})
		n.server, n.addr = srv, srv.Addr().String()
	}
	return n
}

func hostWithNetwork(t *testing.T, clk *clock, limits RedeemLimits) (*node, netid.NetworkID) {
	t.Helper()
	h := newNode(t, clk, limits, true)
	n, err := h.mgr.CreateNetwork(context.Background(), "Test\x00 net")
	if err != nil {
		t.Fatal(err)
	}
	id, err := netid.ParseNetworkID(n.NetworkID)
	if err != nil {
		t.Fatal(err)
	}
	if n.DisplayName != "Test net" || n.Role != database.PeernetRoleHost || n.ExposureLevel != 0 {
		t.Fatalf("network %+v", n)
	}
	if err := h.mgr.SetHostAddress(context.Background(), id, h.addr); err != nil {
		t.Fatal(err)
	}
	return h, id
}

func invite(t *testing.T, h *node, id netid.NetworkID, alias string) string {
	t.Helper()
	l, err := h.mgr.CreateInvite(context.Background(), id, time.Hour, alias)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestFullFlow(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	a := newNode(t, clk, RedeemLimits{}, true)
	b := newNode(t, clk, RedeemLimits{}, true)

	linkA := invite(t, host, id, "alice\n")
	na, err := a.mgr.Join(ctx, linkA, JoinOptions{})
	if err != nil {
		t.Fatalf("join a: %v", err)
	}
	if na.Role != database.PeernetRoleMember || na.DisplayName != "Test net" || na.HostNodeID != host.key.NodeID().String() {
		t.Fatalf("joined %+v", na)
	}
	// Link is single use, and re-joining the same network is refused.
	if _, err := b.mgr.Join(ctx, linkA, JoinOptions{}); err == nil {
		t.Fatal("reused invite accepted")
	}
	if _, err := a.mgr.Join(ctx, invite(t, host, id, ""), JoinOptions{}); !errors.Is(err, ErrAlreadyJoined) {
		t.Fatalf("rejoin: %v", err)
	}
	if _, err := b.mgr.Join(ctx, invite(t, host, id, "bob"), JoinOptions{}); err != nil {
		t.Fatalf("join b: %v", err)
	}
	members, _ := host.mgr.ListMembers(ctx, id)
	if len(members) != 2 || (members[0].Alias+members[1].Alias != "alicebob" && members[0].Alias+members[1].Alias != "bobalice") {
		t.Fatalf("members %+v", members)
	}
	invs, _ := host.mgr.ListInvites(ctx, id)
	if len(invs) != 3 {
		t.Fatalf("invites %d", len(invs))
	}

	// Per-request proof, across real transport, A -> B.
	authA, err := a.mgr.Authorizer(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	authB, _ := b.mgr.Authorizer(ctx, id)
	if err := b.server.Handle("GET /ping", 0, Protect(authB, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("pong"))
	}))); err != nil {
		t.Fatal(err)
	}
	call := func(authFn func(http.Header) error) (*transport.Response, error) {
		h := http.Header{}
		if authFn != nil {
			if err := authFn(h); err != nil {
				return nil, err
			}
		}
		return a.client.Do(ctx, b.addr, b.key.NodeID(), transport.Request{Method: "GET", Path: "/ping", Header: h},
			transport.CallOptions{MaxResponseBytes: 1024})
	}
	r, err := call(func(h http.Header) error { return authA.PrepareOutgoing(ctx, h) })
	if err != nil || r.StatusCode != 200 {
		t.Fatalf("authorised call: %v %v", err, r)
	}
	if err := authA.VerifyResponse(ctx, r.Header, b.key.NodeID()); err != nil {
		t.Fatalf("verify response: %v", err)
	}
	// A response proof for the wrong server identity is rejected.
	if err := authA.VerifyResponse(ctx, r.Header, host.key.NodeID()); !errors.Is(err, ErrPeerMismatch) {
		t.Fatalf("wrong server: %v", err)
	}
	_ = r.Body.Close()
	if r, err := call(nil); err != nil || r.StatusCode != http.StatusForbidden {
		t.Fatalf("no proof should be 403: %v", err)
	} else {
		_ = r.Body.Close()
	}

	// Renewal at half life.
	before, _ := a.mgr.GetNetwork(ctx, id)
	c1, _ := ParseCertificate(before.OwnCertificate)
	clk.advance(10 * time.Hour)
	if err := a.mgr.RenewDue(ctx); err != nil {
		t.Fatalf("renew (not due): %v", err)
	}
	mid, _ := a.mgr.GetNetwork(ctx, id)
	if c, _ := ParseCertificate(mid.OwnCertificate); c.IssuedAt != c1.IssuedAt {
		t.Fatal("renewed before half-life")
	}
	clk.advance(15 * time.Hour)
	if err := a.mgr.RenewDue(ctx); err != nil {
		t.Fatalf("renew: %v", err)
	}
	after, _ := a.mgr.GetNetwork(ctx, id)
	if c, _ := ParseCertificate(after.OwnCertificate); c.IssuedAt <= c1.IssuedAt {
		t.Fatal("certificate not renewed at half-life")
	}
	if err := b.mgr.RenewDue(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.mgr.RenewDue(ctx); err != nil {
		t.Fatal(err)
	}

	// Remove A. Renewal fails; B rejects A after fetching the list.
	if err := host.mgr.RemoveMember(ctx, id, a.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	if err := host.mgr.RemoveMember(ctx, id, a.key.NodeID()); !errors.Is(err, ErrNotMember) {
		t.Fatalf("double remove: %v", err)
	}
	clk.advance(25 * time.Hour)
	if err := a.mgr.RenewDue(ctx); err == nil {
		t.Fatal("revoked member renewed")
	}
	if err := b.mgr.RenewDue(ctx); err != nil { // refreshes list and renews B
		t.Fatalf("b renew: %v", err)
	}
	// A's old cert (issued earlier, may be near expiry) presented to B.
	oldHdr := http.Header{}
	oldHdr.Set(ProofHeader, encodeCert(after.OwnCertificate))
	clk.off.Add(-int64(24 * time.Hour)) // step back inside the old certificate's window
	if err := authB.AuthorizeIncoming(ctx, oldHdr, a.key.NodeID()); !errors.Is(err, ErrRevoked) {
		t.Fatalf("removed member's certificate accepted by B: %v", err)
	}
	// Member A cannot fetch anything useful either: it learns it is revoked.
	if err := a.mgr.RefreshRevocations(ctx, id); !errors.Is(err, ErrRevoked) {
		t.Fatalf("a refresh: %v", err)
	}
	// Host listing shows A revoked.
	members, _ = host.mgr.ListMembers(ctx, id)
	revoked := 0
	for _, m := range members {
		if m.RevokedAt != 0 {
			revoked++
		}
	}
	if revoked != 1 {
		t.Fatalf("revoked count %d", revoked)
	}
	// A removed node can never rejoin with the same node identity.
	if err := a.mgr.Leave(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := a.mgr.Join(ctx, invite(t, host, id, ""), JoinOptions{}); err == nil {
		t.Fatal("revoked node rejoined")
	}

	// Leave / delete.
	if err := b.mgr.Leave(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := host.mgr.Leave(ctx, id); !errors.Is(err, ErrNotMember) {
		t.Fatalf("host leave: %v", err)
	}
	if err := host.mgr.DeleteNetwork(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := host.mgr.GetNetwork(ctx, id); !errors.Is(err, ErrUnknownNetwork) {
		t.Fatalf("after delete: %v", err)
	}
	if _, err := host.mgr.hostKey(id); err == nil {
		t.Fatal("root key survived delete")
	}
}

func encodeCert(b []byte) string {
	return strings.TrimRight(b64(b), "=")
}

func TestHostCertAndOwnAuthorizer(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	auth, err := host.mgr.Authorizer(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	h := http.Header{}
	if err := auth.PrepareOutgoing(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := auth.VerifyResponse(ctx, h, host.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	// After more than a lifetime the host re-issues lazily.
	clk.advance(3 * CertLifetime)
	h = http.Header{}
	if err := auth.PrepareOutgoing(ctx, h); err != nil {
		t.Fatalf("host cert not re-issued: %v", err)
	}
	if err := auth.VerifyResponse(ctx, h, host.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	// Proof header hygiene.
	for _, hdr := range []http.Header{{}, {ProofHeader: {""}}, {ProofHeader: {"!!!"}}, {ProofHeader: {"a", "b"}},
		{ProofHeader: {strings.Repeat("A", 401)}}} {
		if err := auth.AuthorizeIncoming(ctx, hdr, host.key.NodeID()); !errors.Is(err, ErrNoProof) {
			t.Errorf("accepted bad header %v: %v", hdr, err)
		}
	}
	if _, err := host.mgr.Authorizer(ctx, netid.NetworkID{1}); !errors.Is(err, ErrUnknownNetwork) {
		t.Fatalf("unknown net authorizer: %v", err)
	}
	open, err := host.mgr.Authorizer(ctx, netid.Open())
	if err != nil || open.NetworkID() != netid.Open() {
		t.Fatal("open authorizer")
	}
	if open.AuthorizeIncoming(ctx, http.Header{}, identity.NodeID{}) != nil || open.PrepareOutgoing(ctx, http.Header{}) != nil ||
		open.VerifyResponse(ctx, http.Header{}, identity.NodeID{}) != nil || open.DecorateResponse(ctx, http.Header{}) != nil {
		t.Fatal("open authorizer must admit everyone")
	}
}

func TestRedeemSingleUseConcurrent(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	link, err := ParseLink(invite(t, host, id, ""))
	if err != nil {
		t.Fatal(err)
	}
	const workers = 60
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		peer := newID(t).NodeID()
		go func() {
			defer wg.Done()
			if _, err := host.mgr.redeem(ctx, redeemRequest{Network: id, Code: link.Code}, peer); err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrDenied) {
				t.Errorf("unexpected error %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d redemptions succeeded, want exactly 1", wins.Load())
	}
	members, _ := host.mgr.ListMembers(ctx, id)
	if len(members) != 1 {
		t.Fatalf("members %d", len(members))
	}
}

func TestRedeemFailuresIndistinguishable(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	l, _ := host.mgr.CreateInvite(ctx, id, time.Minute, "")
	expired, _ := ParseLink(l)
	clk.advance(time.Hour)
	unknownCode, _ := NewInviteCode()
	peer := newID(t).NodeID()
	other := id
	other[0] ^= 0xff
	for name, req := range map[string]redeemRequest{
		"expired": {id, expired.Code}, "unknown code": {id, unknownCode}, "unknown network": {other, unknownCode},
	} {
		_, err := host.mgr.redeem(ctx, req, peer)
		if err != ErrDenied {
			t.Errorf("%s: %v", name, err)
		}
	}
	// And over the wire: identical status and body.
	a := newNode(t, clk, RedeemLimits{}, false)
	bodies := map[string]string{}
	for name, req := range map[string]redeemRequest{
		"expired": {id, expired.Code}, "unknown code": {id, unknownCode}, "unknown network": {other, unknownCode},
	} {
		resp, err := a.client.Do(ctx, host.addr, host.key.NodeID(), transport.Request{Method: "POST", Path: PathRedeem, Body: req.marshal()},
			transport.CallOptions{MaxResponseBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := resp.ReadAll()
		bodies[name] = fmt.Sprint(resp.StatusCode) + string(b)
	}
	if bodies["expired"] != bodies["unknown code"] || bodies["expired"] != bodies["unknown network"] {
		t.Fatalf("distinguishable failures: %v", bodies)
	}
}

func TestRedeemRateLimits(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{PerIPBurst: 2, PerIPEvery: time.Hour, GlobalBurst: 100, GlobalEvery: time.Second})
	a := newNode(t, clk, RedeemLimits{}, false)
	code, _ := NewInviteCode()
	status := func() int {
		resp, err := a.client.Do(ctx, host.addr, host.key.NodeID(),
			transport.Request{Method: "POST", Path: PathRedeem, Body: redeemRequest{id, code}.marshal()},
			transport.CallOptions{MaxResponseBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = resp.ReadAll()
		return resp.StatusCode
	}
	if s1, s2, s3 := status(), status(), status(); s1 != 403 || s2 != 403 || s3 != 429 {
		t.Fatalf("per-IP limit: %d %d %d", s1, s2, s3)
	}
	// Global cap, direct on the limiter.
	l := newRedeemLimiter(RedeemLimits{PerIPBurst: 100, PerIPEvery: time.Millisecond, GlobalBurst: 3, GlobalEvery: time.Hour}, clk.now)
	ok := 0
	for i := 0; i < 10; i++ {
		if l.allow("10.0.0." + string(rune('1'+i)) + ":1") {
			ok++
		}
	}
	if ok != 3 {
		t.Fatalf("global cap allowed %d", ok)
	}
	// Bounded memory.
	l = newRedeemLimiter(RedeemLimits{MaxTrackedIPs: 5}, clk.now)
	for i := 0; i < 50; i++ {
		l.allow("10.0.1." + string(rune('0'+i%10)) + string(rune('a'+i/10)) + ":1")
		if len(l.ips) > 5 {
			t.Fatalf("tracked %d ips", len(l.ips))
		}
	}
	if remoteKey("[2001:db8::1:2:3:4]:5") != remoteKey("[2001:db8::ffff:ffff:ffff:ffff]:9") {
		t.Fatal("ipv6 not grouped by /64")
	}
	if remoteKey("garbage") != "garbage" {
		t.Fatal("bad remote")
	}
}

func TestRenewRules(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	a := newNode(t, clk, RedeemLimits{}, false)
	if _, err := a.mgr.Join(ctx, invite(t, host, id, ""), JoinOptions{}); err != nil {
		t.Fatal(err)
	}
	stranger := newID(t).NodeID()
	if _, err := host.mgr.renew(ctx, id, stranger); err == nil {
		t.Fatal("non-member renewed")
	}
	// Just-expired within grace may renew.
	clk.advance(CertLifetime + time.Hour)
	if _, err := host.mgr.renew(ctx, id, a.key.NodeID()); err != nil {
		t.Fatalf("grace renew: %v", err)
	}
	// Beyond grace: no.
	clk.advance(CertLifetime + RenewGrace + time.Hour)
	if _, err := host.mgr.renew(ctx, id, a.key.NodeID()); err == nil {
		t.Fatal("renewed beyond grace")
	}
	// Revoked never renews even inside the window.
	clk.advance(-CertLifetime)
	_ = host.mgr.cfg.Store.SetMemberCertificate(ctx, id.String(), a.key.NodeID().String(), clk.now().Unix(), clk.now().Add(CertLifetime).Unix())
	if err := host.mgr.RemoveMember(ctx, id, a.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	if _, err := host.mgr.renew(ctx, id, a.key.NodeID()); err == nil {
		t.Fatal("revoked renewed")
	}
}

func TestRevocationSequenceBumpsAndMemberKeepsHighest(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	a := newNode(t, clk, RedeemLimits{}, false)
	b := newNode(t, clk, RedeemLimits{}, false)
	for _, n := range []*node{a, b} {
		if _, err := n.mgr.Join(ctx, invite(t, host, id, ""), JoinOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	n0, _ := a.mgr.GetNetwork(ctx, id)
	if err := host.mgr.RemoveMember(ctx, id, b.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	if err := a.mgr.RefreshRevocations(ctx, id); err != nil {
		t.Fatal(err)
	}
	n1, _ := a.mgr.GetNetwork(ctx, id)
	if n1.RevocationSeq <= n0.RevocationSeq {
		t.Fatal("sequence did not increase")
	}
	l, _ := ParseRevocationList(n1.RevocationList)
	if !l.Contains(b.key.NodeID()) {
		t.Fatal("list missing removed node")
	}
	// A stale (lower-sequence) list is refused by the store and by the verifier.
	if err := a.mgr.cfg.Store.SetRevocationList(ctx, id.String(), n0.RevocationSeq, n0.RevocationList, 1); !errors.Is(err, database.ErrPeernetStale) {
		t.Fatalf("store accepted lower sequence: %v", err)
	}
	if _, err := a.mgr.verifyList(n0.RevocationList, n1.RootPublicKey, n1.RevocationSeq); !errors.Is(err, ErrStaleRevocation) {
		t.Fatalf("verifier accepted lower sequence: %v", err)
	}
}

func TestInviteManagement(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	if _, err := host.mgr.CreateInvite(ctx, id, 0, ""); !errors.Is(err, ErrInviteTTL) {
		t.Fatal("zero ttl")
	}
	if _, err := host.mgr.CreateInvite(ctx, id, 365*24*time.Hour, ""); !errors.Is(err, ErrInviteTTL) {
		t.Fatal("huge ttl")
	}
	link := invite(t, host, id, "")
	invs, _ := host.mgr.ListInvites(ctx, id)
	if len(invs) != 1 || strings.Contains(link, invs[0].CodeHash) {
		t.Fatal("list")
	}
	code, _ := ParseLink(link)
	if invs[0].CodeHash != code.Code.Hash() {
		t.Fatal("hash mismatch")
	}
	if err := host.mgr.RevokeInvite(ctx, id, invs[0].CodeHash); err != nil {
		t.Fatal(err)
	}
	a := newNode(t, clk, RedeemLimits{}, false)
	if _, err := a.mgr.Join(ctx, link, JoinOptions{}); err == nil {
		t.Fatal("revoked invite accepted")
	}
	// No host address: no invite.
	h2 := newNode(t, clk, RedeemLimits{}, true)
	n2, _ := h2.mgr.CreateNetwork(ctx, "x")
	id2, _ := netid.ParseNetworkID(n2.NetworkID)
	if _, err := h2.mgr.CreateInvite(ctx, id2, time.Hour, ""); !errors.Is(err, ErrNoHostAddress) {
		t.Fatalf("no address: %v", err)
	}
	if err := h2.mgr.SetHostAddress(ctx, id2, "http://bad"); err == nil {
		t.Fatal("bad host address accepted")
	}
	// Member of a network cannot host operations.
	if err := a.mgr.SetHostAddress(ctx, id2, "x.example:1"); !errors.Is(err, ErrUnknownNetwork) {
		t.Fatal(err)
	}
}

func TestJoinVerifiesHost(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	a := newNode(t, clk, RedeemLimits{}, false)
	link, _ := ParseLink(invite(t, host, id, ""))
	// Pinned host node ID that does not match the server: transport refuses.
	link.HostNode = newID(t).NodeID()
	s, _ := link.Encode()
	if _, err := a.mgr.Join(ctx, s, JoinOptions{}); err == nil {
		t.Fatal("joined a host with the wrong node id")
	}
	if _, err := a.mgr.GetNetwork(ctx, id); !errors.Is(err, ErrUnknownNetwork) {
		t.Fatal("network stored after failed join")
	}
	// Garbage links.
	if _, err := a.mgr.Join(ctx, "nope", JoinOptions{}); err == nil {
		t.Fatal("garbage link")
	}
}

func TestSettingsAndOpenNetwork(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	h, id := hostWithNetwork(t, clk, RedeemLimits{})
	nets, err := h.mgr.ListNetworks(ctx)
	if err != nil || len(nets) != 2 {
		t.Fatalf("list: %v %d", err, len(nets))
	}
	if err := h.mgr.SetExposureLevel(ctx, id, 3); err != nil {
		t.Fatal(err)
	}
	if err := h.mgr.SetExposureLevel(ctx, id, 4); !errors.Is(err, ErrInvalidLevel) {
		t.Fatal(err)
	}
	if err := h.mgr.SetAllowPrivateAddresses(ctx, id, true); err != nil {
		t.Fatal(err)
	}
	n, _ := h.mgr.GetNetwork(ctx, id)
	if n.ExposureLevel != 3 || !n.AllowPrivateAddresses {
		t.Fatalf("%+v", n)
	}
	o, err := h.mgr.GetNetwork(ctx, netid.Open())
	if err != nil || o.Role != database.PeernetRoleOpen || o.ExposureLevel != 0 {
		t.Fatalf("open: %v %+v", err, o)
	}
	// The open network's level lives in the configuration, not in the row.
	if err := h.mgr.SetExposureLevel(ctx, netid.Open(), 1); !errors.Is(err, ErrOpenNetwork) {
		t.Fatal(err)
	}
	if err := h.mgr.SetExposureLevel(ctx, netid.NetworkID{9}, 1); !errors.Is(err, ErrUnknownNetwork) {
		t.Fatal(err)
	}
	if err := h.mgr.DeleteNetwork(ctx, netid.Open()); !errors.Is(err, ErrNotHost) {
		t.Fatal(err)
	}
	// Two hosted networks use separate keys.
	n2, err := h.mgr.CreateNetwork(ctx, "second")
	if err != nil || n2.NetworkID == n.NetworkID {
		t.Fatal("second network")
	}
	if _, err := NewManager(Config{}); err == nil {
		t.Fatal("empty config accepted")
	}
}

func TestRevokeFlowViaProtectedRevocationRoute(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	stranger := newNode(t, clk, RedeemLimits{}, false)
	resp, err := stranger.client.Do(ctx, host.addr, host.key.NodeID(), transport.Request{Method: "GET", Path: PathRevocations,
		Query: map[string][]string{"n": {id.String()}}}, transport.CallOptions{MaxResponseBytes: MaxRevocationListSize})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("stranger fetched revocation list: %d", resp.StatusCode)
	}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
