package membership

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// recordingCaller wraps a Caller, counting calls per address and optionally
// failing or blocking them.
type recordingCaller struct {
	inner Caller
	mu    sync.Mutex
	calls map[string]int
	paths []string
	fail  func(addr string) error
	block func(addr string) bool // block until ctx is done
	first map[string]time.Time
}

func newRecordingCaller(inner Caller) *recordingCaller {
	return &recordingCaller{inner: inner, calls: map[string]int{}, first: map[string]time.Time{}}
}

func (c *recordingCaller) Do(ctx context.Context, addr string, expected identity.NodeID, req transport.Request, opts transport.CallOptions) (*transport.Response, error) {
	c.mu.Lock()
	c.calls[addr]++
	c.paths = append(c.paths, req.Path)
	if _, ok := c.first[addr]; !ok {
		c.first[addr] = time.Now()
	}
	fail, block := c.fail, c.block
	c.mu.Unlock()
	if block != nil && block(addr) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if fail != nil {
		if err := fail(addr); err != nil {
			return nil, err
		}
	}
	return c.inner.Do(ctx, addr, expected, req, opts)
}

func (c *recordingCaller) count(addr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[addr]
}

func joined(t *testing.T, clk *clock) (host *node, id netid.NetworkID, member *node) {
	t.Helper()
	ctx := context.Background()
	host, id = hostWithNetwork(t, clk, RedeemLimits{})
	member = newNode(t, clk, RedeemLimits{}, false)
	if _, err := member.mgr.Join(ctx, invite(t, host, id, ""), JoinOptions{}); err != nil {
		t.Fatal(err)
	}
	return host, id, member
}

func restarted(t *testing.T, n *node, caller Caller, clk *clock, timeout time.Duration) *Manager {
	t.Helper()
	m, err := NewManager(Config{Store: n.mgr.cfg.Store, KeysDir: n.mgr.cfg.KeysDir, NodeID: n.key.NodeID(),
		Caller: caller, Now: clk.now, CallTimeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func listHas(t *testing.T, m *Manager, id netid.NetworkID, node identity.NodeID) bool {
	t.Helper()
	n, err := m.cfg.Store.GetNetwork(context.Background(), id.String())
	if err != nil {
		t.Fatal(err)
	}
	l, err := ParseRevocationList(n.RevocationList)
	if err != nil {
		t.Fatal(err)
	}
	return l.Contains(node)
}

// Item 1: the member row was revoked but the signed list was never updated.
func TestRemoveMemberRetryConvergesAfterInterruptedRevocation(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id, a := joined(t, clk)
	// Simulate RemoveMember failing between marking the row and storing the list.
	if err := host.mgr.cfg.Store.RevokeMember(ctx, id.String(), a.key.NodeID().String(), clk.now().Unix()); err != nil {
		t.Fatal(err)
	}
	if listHas(t, host.mgr, id, a.key.NodeID()) {
		t.Fatal("precondition: list must not list the node yet")
	}
	// A retry must converge instead of answering "not a member".
	if err := host.mgr.RemoveMember(ctx, id, a.key.NodeID()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !listHas(t, host.mgr, id, a.key.NodeID()) {
		t.Fatal("retry did not put the node into the signed list")
	}
	// Once converged, a further removal is "not a member" and does not re-sign.
	before, _ := host.mgr.cfg.Store.GetNetwork(ctx, id.String())
	if err := host.mgr.RemoveMember(ctx, id, a.key.NodeID()); !errors.Is(err, ErrNotMember) {
		t.Fatalf("converged removal: %v", err)
	}
	after, _ := host.mgr.cfg.Store.GetNetwork(ctx, id.String())
	if after.RevocationSeq != before.RevocationSeq {
		t.Fatal("an already converged removal re-signed the list")
	}
	if err := host.mgr.RemoveMember(ctx, id, newID(t).NodeID()); !errors.Is(err, ErrNotMember) {
		t.Fatalf("stranger: %v", err)
	}
}

func TestRenewDueRepairsRevokedButUnlistedMember(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id, a := joined(t, clk)
	if err := host.mgr.cfg.Store.RevokeMember(ctx, id.String(), a.key.NodeID().String(), clk.now().Unix()); err != nil {
		t.Fatal(err)
	}
	// "Start" a fresh host manager: its first pass repairs the list.
	fresh := restarted(t, host, nil, clk, 0)
	if err := fresh.RenewDue(ctx); err != nil {
		t.Fatal(err)
	}
	if !listHas(t, fresh, id, a.key.NodeID()) {
		t.Fatal("RenewDue did not repair the list")
	}
	// A removal older than the retention window is not repaired back in.
	clk.advance(revocationRetention + time.Hour)
	if err := fresh.RenewDue(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fresh.RemoveMember(ctx, id, a.key.NodeID()); !errors.Is(err, ErrNotMember) {
		t.Fatalf("outside retention: %v", err)
	}
}

// Item 2.
func TestRenewDueThrottlesRevocationRefresh(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id, a := joined(t, clk)
	_ = id
	rc := newRecordingCaller(a.client)
	m := restarted(t, a, rc, clk, 0)
	for i := 0; i < 5; i++ {
		if err := m.RenewDue(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := rc.count(host.addr); got != 1 {
		t.Fatalf("calls after start = %d, want exactly one refresh right after start", got)
	}
	clk.advance(RevocationRefreshEvery - time.Minute)
	_ = m.RenewDue(ctx)
	if got := rc.count(host.addr); got != 1 {
		t.Fatalf("refreshed early: %d", got)
	}
	clk.advance(2 * time.Minute)
	_ = m.RenewDue(ctx)
	if got := rc.count(host.addr); got != 2 {
		t.Fatalf("calls after interval = %d, want 2", got)
	}
}

func TestRenewDueDoesNotRefreshRightAfterJoin(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, _, a := joined(t, clk)
	rc := newRecordingCaller(a.client)
	a.mgr.cfg.Caller = rc
	for i := 0; i < 3; i++ {
		_ = a.mgr.RenewDue(ctx)
	}
	if got := rc.count(host.addr); got != 0 {
		t.Fatalf("a join already carries the list; got %d refreshes", got)
	}
}

func TestRenewDueBacksOffAfterFailure(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, _, a := joined(t, clk)
	rc := newRecordingCaller(a.client)
	rc.fail = func(string) error { return errors.New("down") }
	m := restarted(t, a, rc, clk, 0)
	if err := m.RenewDue(ctx); err == nil {
		t.Fatal("expected failure")
	}
	for i := 0; i < 3; i++ {
		if err := m.RenewDue(ctx); err != nil {
			t.Fatalf("backed-off pass should skip quietly: %v", err)
		}
	}
	if got := rc.count(host.addr); got != 1 {
		t.Fatalf("calls = %d, want 1 inside the backoff", got)
	}
	clk.advance(maintBackoffBase + time.Second)
	_ = m.RenewDue(ctx)
	if got := rc.count(host.addr); got != 2 {
		t.Fatalf("calls = %d, want a retry after the backoff", got)
	}
	// The second failure doubles the wait.
	clk.advance(maintBackoffBase + time.Second)
	_ = m.RenewDue(ctx)
	if got := rc.count(host.addr); got != 2 {
		t.Fatalf("calls = %d, backoff did not grow", got)
	}
}

func TestRenewDueUnreachableHostDelaysNoOtherNetwork(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host1, _, a := joined(t, clk)
	host2, id2 := hostWithNetwork(t, clk, RedeemLimits{})
	if _, err := a.mgr.Join(ctx, invite(t, host2, id2, ""), JoinOptions{}); err != nil {
		t.Fatal(err)
	}
	rc := newRecordingCaller(a.client)
	rc.block = func(addr string) bool { return addr == host1.addr }
	m := restarted(t, a, rc, clk, 300*time.Millisecond)
	start := time.Now()
	err := m.RenewDue(ctx)
	if err == nil {
		t.Fatal("blocked host should have timed out")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("RenewDue took %v with a 300ms call timeout", d)
	}
	rc.mu.Lock()
	started := rc.first[host2.addr].Sub(start)
	rc.mu.Unlock()
	if rc.count(host2.addr) != 1 || started > 150*time.Millisecond {
		t.Fatalf("second network was delayed by the first: calls=%d started after %v", rc.count(host2.addr), started)
	}
}

// Item 5.
func TestRedeemByExistingMemberReissuesButRevokedIsRefused(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id, a := joined(t, clk)
	// A misses the renewal grace period: renewal is refused for good.
	clk.advance(CertLifetime + RenewGrace + time.Hour)
	if _, err := host.mgr.renew(ctx, id, a.key.NodeID()); err == nil {
		t.Fatal("renewed beyond grace")
	}
	// It leaves locally and redeems a fresh invite: the host re-issues.
	if err := a.mgr.Leave(ctx, id); err != nil {
		t.Fatal(err)
	}
	link := invite(t, host, id, "")
	if _, err := a.mgr.Join(ctx, link, JoinOptions{}); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	ms, _ := host.mgr.ListMembers(ctx, id)
	count := 0
	for _, m := range ms {
		if m.NodeID == a.key.NodeID().String() {
			count++
			if m.RevokedAt != 0 {
				t.Fatal("member became revoked")
			}
		}
	}
	if count != 1 {
		t.Fatalf("member rows = %d", count)
	}
	// A revoked node is refused with the same error as any other failure.
	if err := host.mgr.RemoveMember(ctx, id, a.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	if err := a.mgr.Leave(ctx, id); err != nil {
		t.Fatal(err)
	}
	link2 := invite(t, host, id, "")
	_, err := a.mgr.Join(ctx, link2, JoinOptions{})
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked rejoin: %v", err)
	}
	invs, _ := host.mgr.ListInvites(ctx, id)
	unused := 0
	for _, i := range invs {
		if i.UsedAt == 0 {
			unused++
		}
	}
	if unused != 1 {
		t.Fatalf("a refused redemption must leave the invite usable; unused=%d", unused)
	}
}

// Item 14.
func TestJoinRetryAfterLocalFailureSucceeds(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	a := newNode(t, clk, RedeemLimits{}, false)
	link := invite(t, host, id, "")
	if _, err := a.mgr.Join(ctx, link, JoinOptions{}); err != nil {
		t.Fatal(err)
	}
	// The host redeemed the invite, but storing the network locally is lost.
	if err := a.mgr.cfg.Store.DeleteNetwork(ctx, id.String()); err != nil {
		t.Fatal(err)
	}
	a.mgr.invalidate()
	if _, err := a.mgr.Join(ctx, link, JoinOptions{}); err != nil {
		t.Fatalf("retry of the same join: %v", err)
	}
	// A stranger with the used link still cannot get in.
	b := newNode(t, clk, RedeemLimits{}, false)
	if _, err := b.mgr.Join(ctx, link, JoinOptions{}); !errors.Is(err, ErrDenied) {
		t.Fatalf("stranger with a used link: %v", err)
	}
}

func TestJoinDoesNotIgnoreStoreErrors(t *testing.T) {
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	a := newNode(t, clk, RedeemLimits{}, false)
	rc := newRecordingCaller(a.client)
	a.mgr.cfg.Caller = rc
	link := invite(t, host, id, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.mgr.Join(ctx, link, JoinOptions{}); err == nil || errors.Is(err, ErrAlreadyJoined) {
		t.Fatalf("join with a failing store lookup: %v", err)
	}
	if rc.count(host.addr) != 0 {
		t.Fatal("the invite was redeemed although the local lookup failed")
	}
}

// Item 10.
func TestEnsureOpenNetworkWritesOnce(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	h := newNode(t, clk, RedeemLimits{}, false)
	if err := h.mgr.EnsureOpenNetwork(ctx); err != nil {
		t.Fatal(err)
	}
	// Remove the row behind the manager's back: reads must not recreate it.
	if err := h.mgr.cfg.Store.DeleteNetwork(ctx, netid.Open().String()); err != nil {
		t.Fatal(err)
	}
	h.mgr.invalidate()
	if _, err := h.mgr.GetNetwork(ctx, netid.Open()); !errors.Is(err, ErrUnknownNetwork) {
		t.Fatalf("a read wrote the open row: %v", err)
	}
	if nets, err := h.mgr.ListNetworks(ctx); err != nil || len(nets) != 0 {
		t.Fatalf("list wrote the open row: %v %d", err, len(nets))
	}
}

func TestNetworkCacheServesAndInvalidates(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	h, id := hostWithNetwork(t, clk, RedeemLimits{})
	if _, err := h.mgr.GetNetwork(ctx, id); err != nil {
		t.Fatal(err)
	}
	// A change behind the manager's back is not seen inside the TTL...
	if err := h.mgr.cfg.Store.SetExposureLevel(ctx, id.String(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if n, _ := h.mgr.GetNetwork(ctx, id); n.ExposureLevel != 0 {
		t.Fatal("expected the cached row inside the TTL")
	}
	// ...but every change through the manager is.
	if err := h.mgr.SetExposureLevel(ctx, id, 3); err != nil {
		t.Fatal(err)
	}
	if n, _ := h.mgr.GetNetwork(ctx, id); n.ExposureLevel != 3 {
		t.Fatal("cache survived a level change")
	}
}

func TestCachedRowNeverExtendsRevokedMembership(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	host, id, a := joined(t, clk)
	b := newNode(t, clk, RedeemLimits{}, false)
	if _, err := b.mgr.Join(ctx, invite(t, host, id, ""), JoinOptions{}); err != nil {
		t.Fatal(err)
	}
	// a (acting as a verifier) authorises b and caches its row.
	authz, err := a.mgr.Authorizer(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	proof := http.Header{}
	bAuth, _ := b.mgr.Authorizer(ctx, id)
	if err := bAuth.PrepareOutgoing(ctx, proof); err != nil {
		t.Fatal(err)
	}
	if err := authz.AuthorizeIncoming(ctx, proof, b.key.NodeID()); err != nil {
		t.Fatalf("member refused: %v", err)
	}
	// The host revokes b; a learns it through a normal refresh.
	if err := host.mgr.RemoveMember(ctx, id, b.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	if err := a.mgr.RefreshRevocations(ctx, id); err != nil {
		t.Fatal(err)
	}
	authz2, _ := a.mgr.Authorizer(ctx, id)
	if err := authz2.AuthorizeIncoming(ctx, proof, b.key.NodeID()); !errors.Is(err, ErrRevoked) {
		t.Fatalf("revoked member still authorised from a cached row: %v", err)
	}
}

// Item 12 and 13.
func TestOpenNetworkLevelRefusedAndUpdateSettingsIsAtomic(t *testing.T) {
	ctx := context.Background()
	clk := &clock{}
	h, id := hostWithNetwork(t, clk, RedeemLimits{})
	if err := h.mgr.SetExposureLevel(ctx, netid.Open(), 1); !errors.Is(err, ErrOpenNetwork) {
		t.Fatalf("open level: %v", err)
	}
	if err := h.mgr.UpdateSettings(ctx, netid.Open(), SettingsUpdate{}); !errors.Is(err, ErrOpenNetwork) {
		t.Fatalf("open update: %v", err)
	}
	lvl, allow, bad := 2, true, "not a valid address"
	err := h.mgr.UpdateSettings(ctx, id, SettingsUpdate{Level: &lvl, AllowPrivateAddresses: &allow, HostAddress: &bad})
	if err == nil {
		t.Fatal("invalid address accepted")
	}
	n, _ := h.mgr.GetNetwork(ctx, id)
	if n.ExposureLevel != 0 || n.AllowPrivateAddresses {
		t.Fatalf("partial change applied: %+v", n)
	}
	good := "example.org:8470"
	if err := h.mgr.UpdateSettings(ctx, id, SettingsUpdate{Level: &lvl, AllowPrivateAddresses: &allow, HostAddress: &good}); err != nil {
		t.Fatal(err)
	}
	n, _ = h.mgr.GetNetwork(ctx, id)
	if n.ExposureLevel != 2 || !n.AllowPrivateAddresses || n.HostAddress != good {
		t.Fatalf("%+v", n)
	}
	// A joined network has no host address of its own to set.
	_, _, a := joined(t, clk)
	nets, _ := a.mgr.ListNetworks(ctx)
	var memberID netid.NetworkID
	for _, nw := range nets {
		if nw.Role == database.PeernetRoleMember {
			memberID, _ = netid.ParseNetworkID(nw.NetworkID)
		}
	}
	lvl0 := 0
	if err := a.mgr.UpdateSettings(ctx, memberID, SettingsUpdate{Level: &lvl0, HostAddress: &good}); !errors.Is(err, ErrNotHost) {
		t.Fatalf("member host address: %v", err)
	}
}
