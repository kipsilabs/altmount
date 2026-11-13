package membership

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func joinedMember(t *testing.T) (*clock, *node, *node, *Network) {
	t.Helper()
	ctx := context.Background()
	clk := &clock{}
	host, id := hostWithNetwork(t, clk, RedeemLimits{})
	m := newNode(t, clk, RedeemLimits{}, true)
	n, err := m.mgr.Join(ctx, invite(t, host, id, "m"), JoinOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = n
	got, _ := m.mgr.GetNetwork(ctx, id)
	return clk, host, m, got
}

func TestMemberRecordsRemovalFromRefreshedList(t *testing.T) {
	ctx := context.Background()
	_, host, m, n := joinedMember(t)
	id := mustNetID(t, n.NetworkID)
	if n.RevokedAt != 0 {
		t.Fatal("fresh member already revoked")
	}
	if err := host.mgr.RemoveMember(ctx, id, m.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	if err := m.mgr.RefreshRevocations(ctx, id); !errors.Is(err, ErrRevoked) {
		t.Fatalf("refresh: %v", err)
	}
	got, _ := m.mgr.GetNetwork(ctx, id)
	if got.RevokedAt == 0 {
		t.Fatal("removal not persisted")
	}
	// A revoked member offers no proof and accepts none.
	if err := (&privateAuthorizer{m: m.mgr, id: id}).PrepareOutgoing(ctx, http.Header{}); !errors.Is(err, ErrRevoked) {
		t.Fatalf("PrepareOutgoing: %v", err)
	}
}

func TestRevokedMemberStopsRenewing(t *testing.T) {
	ctx := context.Background()
	clk, host, m, n := joinedMember(t)
	id := mustNetID(t, n.NetworkID)
	if err := host.mgr.RemoveMember(ctx, id, m.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	clk.advance(30 * time.Hour) // certificate due for renewal
	// The host denies the renewal; the member checks the list at once instead
	// of waiting for the next refresh.
	_ = m.mgr.RenewDue(ctx)
	got, _ := m.mgr.GetNetwork(ctx, id)
	if got.RevokedAt == 0 {
		t.Fatal("denied renewal did not lead to the removal being recorded")
	}
	// From now on no host call is made: stop the host and expect a clean pass.
	_ = host.server.Stop(ctx)
	clk.advance(RevocationRefreshEvery + time.Hour)
	if err := m.mgr.RenewDue(ctx); err != nil {
		t.Fatalf("revoked member still contacts the host: %v", err)
	}
}

func TestRefusalChecksListImmediately(t *testing.T) {
	ctx := context.Background()
	_, host, m, n := joinedMember(t)
	id := mustNetID(t, n.NetworkID)
	// Not removed: a refusal changes nothing.
	m.mgr.NoteRefused(ctx, id)
	if got, _ := m.mgr.GetNetwork(ctx, id); got.RevokedAt != 0 {
		t.Fatal("revoked without being removed")
	}
	if err := host.mgr.RemoveMember(ctx, id, m.key.NodeID()); err != nil {
		t.Fatal(err)
	}
	m.mgr.forgetMaint(n.NetworkID) // lift the throttle of the previous check
	m.mgr.NoteRefused(ctx, id)
	if got, _ := m.mgr.GetNetwork(ctx, id); got.RevokedAt == 0 {
		t.Fatal("refusal did not record the removal")
	}
}

func mustNetID(t *testing.T, s string) netid.NetworkID {
	t.Helper()
	id, err := netid.ParseNetworkID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
