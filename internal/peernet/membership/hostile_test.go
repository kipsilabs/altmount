package membership

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

type fakeCaller struct {
	status int
	body   []byte
}

func (f *fakeCaller) Do(context.Context, string, identity.NodeID, transport.Request, transport.CallOptions) (*transport.Response, error) {
	return &transport.Response{StatusCode: f.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(f.body)))}, nil
}

// A malicious or buggy host must not be able to hand a joiner anything that
// fails verification against the root key from the link.
func TestJoinRejectsHostileHostResponses(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	root, evil := newID(t), newID(t)
	netID := netid.FromRootKey(root.PublicKey())
	me := newID(t)
	now := clk.now()
	mkList := func(k *identity.Key, seq uint64, ids ...identity.NodeID) []byte {
		l, err := IssueRevocationList(k, netID, seq, now, ids)
		if err != nil {
			t.Fatal(err)
		}
		return l.Marshal()
	}
	mkCert := func(k *identity.Key, node identity.NodeID, role Role) []byte {
		c, err := IssueCertificate(k, netID, node, role, now, CertLifetime)
		if err != nil {
			t.Fatal(err)
		}
		return c.Marshal()
	}
	good := mkList(root, 1)
	cases := map[string]joinResponse{
		"forged cert":     {Name: "n", Cert: mkCert(evil, me.NodeID(), RoleMember), Revocation: good},
		"other node cert": {Name: "n", Cert: mkCert(root, newID(t).NodeID(), RoleMember), Revocation: good},
		"host role cert":  {Name: "n", Cert: mkCert(root, me.NodeID(), RoleHost), Revocation: good},
		"forged list":     {Name: "n", Cert: mkCert(root, me.NodeID(), RoleMember), Revocation: mkList(evil, 1)},
		"self revoked":    {Name: "n", Cert: mkCert(root, me.NodeID(), RoleMember), Revocation: mkList(root, 1, me.NodeID())},
		"garbage cert":    {Name: "n", Cert: make([]byte, CertSize), Revocation: good},
		"garbage list":    {Name: "n", Cert: mkCert(root, me.NodeID(), RoleMember), Revocation: []byte("zzz")},
	}
	code, _ := NewInviteCode()
	link, err := Link{NetworkID: netID, RootKey: root.PublicKey(), HostAddr: "host.example.org:1",
		HostNode: newID(t).NodeID(), Code: code}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	for name, resp := range cases {
		node := newNode(t, clk, RedeemLimits{}, false)
		fc := &fakeCaller{status: 200, body: resp.marshal()}
		mgr, _ := NewManager(Config{Store: node.mgr.cfg.Store, KeysDir: t.TempDir(), NodeID: me.NodeID(), Caller: fc, Now: clk.now})
		if _, err := mgr.Join(ctx, link, JoinOptions{}); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if _, err := mgr.GetNetwork(ctx, netID); !errors.Is(err, ErrUnknownNetwork) {
			t.Errorf("%s: stored after rejection", name)
		}
	}
	// Non-200, garbage and truncated bodies.
	for name, fc := range map[string]*fakeCaller{
		"403": {status: 403, body: []byte("denied")}, "garbage": {status: 200, body: []byte("xx")},
		"truncated": {status: 200, body: joinResponse{Name: "n", Cert: mkCert(root, me.NodeID(), RoleMember), Revocation: good}.marshal()[:50]},
	} {
		node := newNode(t, clk, RedeemLimits{}, false)
		mgr, _ := NewManager(Config{Store: node.mgr.cfg.Store, KeysDir: t.TempDir(), NodeID: me.NodeID(), Caller: fc, Now: clk.now})
		if _, err := mgr.Join(ctx, link, JoinOptions{}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A valid response is accepted, and a hostile display name is cleaned/capped.
	node := newNode(t, clk, RedeemLimits{}, false)
	fc := &fakeCaller{status: 200, body: joinResponse{Name: "\x1b[31mevil\x00" + strings.Repeat("é", 100),
		Cert: mkCert(root, me.NodeID(), RoleMember), Revocation: good}.marshal()}
	mgr, _ := NewManager(Config{Store: node.mgr.cfg.Store, KeysDir: t.TempDir(), NodeID: me.NodeID(), Caller: fc, Now: clk.now})
	n, err := mgr.Join(ctx, link, JoinOptions{AllowPrivateAddresses: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(n.DisplayName, "\x1b\x00") || len([]rune(n.DisplayName)) > database.PeernetMaxDisplayNameRunes || !n.AllowPrivateAddresses {
		t.Fatalf("name %q", n.DisplayName)
	}
	// Renewal/refresh from a hostile host: stale and forged lists are refused.
	fc.body = mkList(root, 1)
	if err := mgr.RefreshRevocations(ctx, netID); err != nil {
		t.Fatal(err)
	}
	fc.body = mkList(evil, 9)
	if err := mgr.RefreshRevocations(ctx, netID); err == nil {
		t.Fatal("forged list accepted")
	}
	fc.body = mkList(root, 3)
	if err := mgr.RefreshRevocations(ctx, netID); err != nil {
		t.Fatal(err)
	}
	fc.body = mkList(root, 2)
	if err := mgr.RefreshRevocations(ctx, netID); !errors.Is(err, ErrStaleRevocation) {
		t.Fatalf("lower sequence accepted: %v", err)
	}
	// Expired cert + hostile renewal answer: must not replace a held cert.
	clk.advance(30 * time.Hour)
	fc.body = joinResponse{Name: "n", Cert: mkCert(evil, me.NodeID(), RoleMember), Revocation: mkList(root, 5)}.marshal()
	if err := mgr.RenewDue(ctx); err == nil {
		t.Fatal("forged renewal accepted")
	}
	held, _ := mgr.GetNetwork(ctx, netID)
	if c, _ := ParseCertificate(held.OwnCertificate); c.verifyWindow(root.PublicKey(), now) != nil {
		t.Fatal("held certificate replaced")
	}
	// Host-only operations are refused on a member row.
	if err := mgr.RemoveMember(ctx, netID, newID(t).NodeID()); !errors.Is(err, ErrNotHost) {
		t.Fatal(err)
	}
	if _, err := mgr.ListMembers(ctx, netID); !errors.Is(err, ErrNotHost) {
		t.Fatal(err)
	}
	if _, err := mgr.ListInvites(ctx, netID); !errors.Is(err, ErrNotHost) {
		t.Fatal(err)
	}
	if err := mgr.RevokeInvite(ctx, netID, hexOf('a')); !errors.Is(err, ErrNotHost) {
		t.Fatal(err)
	}
	if err := mgr.RefreshRevocations(ctx, netid.Open()); !errors.Is(err, ErrNotMember) {
		t.Fatal(err)
	}
	clk.advance(20 * time.Hour)
	// Member authorizer without a usable certificate fails closed.
	auth, _ := mgr.Authorizer(ctx, netID)
	if err := auth.PrepareOutgoing(ctx, http.Header{}); !errors.Is(err, ErrNoValidCert) {
		t.Fatalf("expired own cert attached: %v", err)
	}
	// Self-revocation (last: it is recorded, and a revoked member renews nothing).
	fc.body = mkList(root, 4, me.NodeID())
	if err := mgr.RefreshRevocations(ctx, netID); !errors.Is(err, ErrRevoked) {
		t.Fatalf("self-revocation not detected: %v", err)
	}
}

func hexOf(c byte) string { return strings.Repeat(string(c), 64) }

func TestWireHandlersRejectGarbage(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	a := newNode(t, clk, RedeemLimits{}, false)
	do := func(method, path string, body []byte, q map[string][]string) int {
		resp, err := a.client.Do(ctx, host.addr, host.key.NodeID(), transport.Request{Method: method, Path: path, Body: body, Query: q},
			transport.CallOptions{MaxResponseBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = resp.ReadAll()
		return resp.StatusCode
	}
	if c := do("POST", PathRedeem, []byte("junk"), nil); c != 403 {
		t.Fatalf("redeem junk %d", c)
	}
	if c := do("POST", PathRedeem, make([]byte, MaxRedeemBody+1), nil); c != 413 {
		t.Fatalf("oversized redeem %d", c)
	}
	if c := do("POST", PathRenew, []byte("junk"), nil); c != 403 {
		t.Fatalf("renew junk %d", c)
	}
	if c := do("POST", PathRenew, marshalRenewRequest(id), nil); c != 403 {
		t.Fatalf("non-member renew %d", c)
	}
	if c := do("GET", PathRevocations, nil, map[string][]string{"n": {"zz"}}); c != 403 {
		t.Fatalf("bad net %d", c)
	}
	if c := do("GET", PathRevocations, nil, nil); c != 403 {
		t.Fatalf("no net %d", c)
	}
	if c := do("PUT", PathRedeem, nil, nil); c != 404 && c != 405 {
		t.Fatalf("wrong method %d", c)
	}
	// Joined member can fetch the list.
	if _, err := a.mgr.Join(ctx, invite(t, host, id, ""), JoinOptions{}); err != nil {
		t.Fatal(err)
	}
	if c := do("GET", PathRevocations, nil, map[string][]string{"n": {id.String()}}); c != 200 {
		t.Fatalf("member fetch %d", c)
	}
	if err := host.mgr.RegisterHandlers(host.server); err == nil {
		t.Fatal("duplicate registration accepted")
	}
	// Join on a node without a caller.
	nc, _ := NewManager(Config{Store: a.mgr.cfg.Store, KeysDir: t.TempDir(), NodeID: a.key.NodeID()})
	if _, err := nc.call(ctx, &Network{HostNodeID: host.key.NodeID().String(), HostAddress: host.addr}, transport.Request{}, 10); err == nil {
		t.Fatal("call without caller")
	}
	if Role(0).String() != "unknown" || RoleHost.String() != "host" || RoleMember.String() != "member" {
		t.Fatal("role strings")
	}
}
