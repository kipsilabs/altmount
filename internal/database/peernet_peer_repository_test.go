package database

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPeerTestRepo(t *testing.T) *PeernetPeerRepository {
	t.Helper()
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return NewPeernetPeerRepository(db.Connection(), DialectSQLite)
}

var peerT0 = time.Unix(1_700_000_000, 0)

func nodeHex(i int) string { return fmt.Sprintf("%064x", i) }

func mkPeer(i int, src string, at time.Time) PeernetPeer {
	return PeernetPeer{NetworkID: hexID('a'), NodeID: nodeHex(i), Address: fmt.Sprintf("8.8.8.%d:7000", i%250+1),
		Source: src, FirstSeen: at, LastSeen: at}
}

func TestPeerRepo_InsertGetRoundTrip(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	p := mkPeer(1, PeernetSourceExchange, peerT0)
	ok, err := r.Insert(ctx, p, PeernetPeerCaps{})
	require.NoError(t, err)
	assert.True(t, ok)
	got, err := r.Get(ctx, p.NetworkID, p.NodeID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, p.Address, got.Address)
	assert.False(t, got.Verified)
	assert.Equal(t, peerT0.Unix(), got.FirstSeen.Unix())
	assert.True(t, got.LastSuccess.IsZero())
	missing, err := r.Get(ctx, p.NetworkID, nodeHex(99))
	require.NoError(t, err)
	assert.Nil(t, missing)
}

func TestPeerRepo_InvalidInputRejected(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	bad := []PeernetPeer{
		{NetworkID: "x", NodeID: nodeHex(1), Address: "a:1", Source: "user", FirstSeen: peerT0},
		{NetworkID: hexID('a'), NodeID: "x", Address: "a:1", Source: "user", FirstSeen: peerT0},
		{NetworkID: hexID('a'), NodeID: nodeHex(1), Address: "", Source: "user", FirstSeen: peerT0},
		{NetworkID: hexID('a'), NodeID: nodeHex(1), Address: "a:1", Source: "bogus", FirstSeen: peerT0},
		{NetworkID: hexID('a'), NodeID: nodeHex(1), Address: "a:1", Source: "user"},
		{NetworkID: hexID('A'), NodeID: nodeHex(1), Address: "a:1", Source: "user", FirstSeen: peerT0},
	}
	for i, p := range bad {
		_, err := r.Insert(ctx, p, PeernetPeerCaps{})
		assert.Error(t, err, "case %d", i)
	}
}

func TestPeerRepo_FirstSeenNeverChanges(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	p := mkPeer(1, PeernetSourceExchange, peerT0)
	_, err := r.Insert(ctx, p, PeernetPeerCaps{})
	require.NoError(t, err)

	later := peerT0.Add(48 * time.Hour)
	again := p
	again.FirstSeen, again.LastSeen, again.Address = later, later, "9.9.9.9:1"
	ok, err := r.Insert(ctx, again, PeernetPeerCaps{})
	require.NoError(t, err)
	assert.False(t, ok, "existing row is not replaced")

	require.NoError(t, r.Touch(ctx, p.NetworkID, p.NodeID, later))
	require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "", later, later))
	_, err = r.RecordFailure(ctx, p.NetworkID, p.NodeID, later, func(int) time.Duration { return time.Minute })
	require.NoError(t, err)
	require.NoError(t, r.PromoteToUser(ctx, p.NetworkID, p.NodeID, p.Address))
	got, _ := r.Get(ctx, p.NetworkID, p.NodeID)
	assert.Equal(t, peerT0.Unix(), got.FirstSeen.Unix())
	assert.Equal(t, later.Unix(), got.LastSeen.Unix())
}

func TestPeerRepo_KnownFor(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	p := mkPeer(1, PeernetSourceExchange, peerT0)
	_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
	require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "", peerT0, peerT0.Add(time.Hour)))
	ok, err := r.KnownFor(ctx, p.NetworkID, p.NodeID, 24*time.Hour, peerT0.Add(23*time.Hour))
	require.NoError(t, err)
	assert.False(t, ok)
	ok, _ = r.KnownFor(ctx, p.NetworkID, p.NodeID, 24*time.Hour, peerT0.Add(24*time.Hour))
	assert.True(t, ok)
	ok, _ = r.KnownFor(ctx, p.NetworkID, nodeHex(7), time.Hour, peerT0.Add(time.Hour*100))
	assert.False(t, ok, "unknown node")
	ok, _ = r.KnownFor(ctx, hexID('b'), p.NodeID, time.Hour, peerT0.Add(time.Hour*100))
	assert.False(t, ok, "other network")
}

func TestPeerRepo_KnownAmongMatchesKnownFor(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	known := mkPeer(1, PeernetSourceExchange, peerT0)
	young := mkPeer(2, PeernetSourceExchange, peerT0)
	exchangeOnly := mkPeer(3, PeernetSourceExchange, peerT0)
	for _, p := range []PeernetPeer{known, young, exchangeOnly} {
		_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
	}
	require.NoError(t, r.RecordSuccess(ctx, known.NetworkID, known.NodeID, "", peerT0, peerT0.Add(time.Hour)))
	require.NoError(t, r.RecordSuccess(ctx, young.NetworkID, young.NodeID, "", peerT0.Add(20*time.Hour), peerT0.Add(21*time.Hour)))
	now := peerT0.Add(25 * time.Hour)
	got, err := r.KnownAmong(ctx, known.NetworkID, []string{known.NodeID, young.NodeID, exchangeOnly.NodeID, nodeHex(9)}, 24*time.Hour, now)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{known.NodeID: true}, got)
	for _, p := range []PeernetPeer{known, young, exchangeOnly} {
		one, _ := r.KnownFor(ctx, p.NetworkID, p.NodeID, 24*time.Hour, now)
		assert.Equal(t, one, got[p.NodeID], p.NodeID)
	}
	empty, err := r.KnownAmong(ctx, known.NetworkID, nil, time.Hour, now)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestPeerRepo_KnownPrefixesReportTheVerifiedAddressPrefix(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	a, b, v6, young := mkPeer(1, PeernetSourceExchange, peerT0), mkPeer(2, PeernetSourceExchange, peerT0),
		mkPeer(3, PeernetSourceExchange, peerT0), mkPeer(4, PeernetSourceExchange, peerT0)
	v6.Address = "[2001:db8:1:2::5]:7000"
	for _, p := range []PeernetPeer{a, b, v6, young} {
		_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
	}
	for _, p := range []PeernetPeer{a, b, v6} {
		require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "", peerT0, peerT0.Add(time.Hour)))
	}
	require.NoError(t, r.RecordSuccess(ctx, young.NetworkID, young.NodeID, "", peerT0.Add(20*time.Hour), peerT0.Add(21*time.Hour)))
	for _, p := range []PeernetPeer{a, b, v6} {
		host, _, _ := net.SplitHostPort(p.Address)
		require.NoError(t, r.RecordConnectedIP(ctx, p.NetworkID, p.NodeID, strings.Trim(host, "[]")))
	}
	got, err := r.KnownPrefixes(ctx, a.NetworkID, []string{a.NodeID, b.NodeID, v6.NodeID, young.NodeID, nodeHex(9)}, 24*time.Hour, peerT0.Add(25*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{a.NodeID: "8.8.8.0/24", b.NodeID: "8.8.8.0/24", v6.NodeID: "2001:db8:1::/48"}, got)
}

// Two DNS names for one server are one range: corroboration keys on the IP the
// node actually connected to, never on the stored address string.
func TestPeerRepo_KnownPrefixesKeyOnTheConnectedIPNotTheName(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	a, b, c, d := mkPeer(1, PeernetSourceExchange, peerT0), mkPeer(2, PeernetSourceExchange, peerT0),
		mkPeer(3, PeernetSourceExchange, peerT0), mkPeer(4, PeernetSourceExchange, peerT0)
	a.Address, b.Address, c.Address, d.Address = "one.example.org:7000", "two.example.org:7000", "[2001:db8::9]:7000", "ip.example.org:7000"
	for _, p := range []PeernetPeer{a, b, c, d} {
		_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
		require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "", peerT0, peerT0.Add(time.Hour)))
	}
	require.NoError(t, r.RecordConnectedIP(ctx, a.NetworkID, a.NodeID, "203.0.113.7"))
	require.NoError(t, r.RecordConnectedIP(ctx, b.NetworkID, b.NodeID, "203.0.113.200"))
	require.NoError(t, r.RecordConnectedIP(ctx, c.NetworkID, c.NodeID, "2001:db8:1:2::5"))
	// d never recorded a connected IP.
	got, err := r.KnownPrefixes(ctx, a.NetworkID, []string{a.NodeID, b.NodeID, c.NodeID, d.NodeID}, 24*time.Hour, peerT0.Add(25*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{a.NodeID: "203.0.113.0/24", b.NodeID: "203.0.113.0/24", c.NodeID: "2001:db8:1::/48"}, got,
		"two names, one /24; a peer without a connected IP counts for nothing")
	ip, err := r.ConnectedIP(ctx, a.NetworkID, a.NodeID)
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.7", ip)
}

func TestPeerRepo_PerSourceCapEvictsStalestUnverifiedOfSource(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	caps := PeernetPeerCaps{MaxPerSource: map[string]int{PeernetSourceExchange: 3}}
	for i := 1; i <= 3; i++ {
		_, err := r.Insert(ctx, mkPeer(i, PeernetSourceExchange, peerT0.Add(time.Duration(i)*time.Minute)), caps)
		require.NoError(t, err)
	}
	// Verify the stalest one: it must now be protected.
	require.NoError(t, r.RecordSuccess(ctx, hexID('a'), nodeHex(1), "", peerT0, peerT0))
	ok, err := r.Insert(ctx, mkPeer(4, PeernetSourceExchange, peerT0.Add(10*time.Minute)), caps)
	require.NoError(t, err)
	assert.True(t, ok)
	g1, _ := r.Get(ctx, hexID('a'), nodeHex(1))
	g2, _ := r.Get(ctx, hexID('a'), nodeHex(2))
	assert.NotNil(t, g1, "verified peer survives")
	assert.Nil(t, g2, "stalest unverified evicted")

	// Everything verified: the worst verified peer (oldest success among equal
	// failures) is evicted instead of refusing forever; see
	// TestPeernetPeerRepo_VerifiedPeersAreEvictedWorstFirst.
	for _, i := range []int{3, 4} {
		require.NoError(t, r.RecordSuccess(ctx, hexID('a'), nodeHex(i), "", peerT0.Add(time.Duration(i)*time.Hour), peerT0))
	}
	ok, err = r.Insert(ctx, mkPeer(5, PeernetSourceExchange, peerT0), caps)
	require.NoError(t, err)
	assert.True(t, ok)
	g1, _ = r.Get(ctx, hexID('a'), nodeHex(1))
	assert.Nil(t, g1, "verified peer with the oldest success evicted")
}

func TestPeerRepo_TotalCapEvictionOrderAndUserProtected(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	caps := PeernetPeerCaps{MaxTotal: 4}
	_, _ = r.Insert(ctx, mkPeer(1, PeernetSourceUser, peerT0), caps)
	_, _ = r.Insert(ctx, mkPeer(2, PeernetSourceInbound, peerT0.Add(time.Minute)), caps)
	_, _ = r.Insert(ctx, mkPeer(3, PeernetSourceExchange, peerT0.Add(5*time.Minute)), caps)
	_, _ = r.Insert(ctx, mkPeer(4, PeernetSourceExchange, peerT0.Add(2*time.Minute)), caps)

	// Exchange first (stalest of them = 4), even though inbound 2 is older.
	_, err := r.Insert(ctx, mkPeer(5, PeernetSourceExchange, peerT0.Add(9*time.Minute)), caps)
	require.NoError(t, err)
	g, _ := r.Get(ctx, hexID('a'), nodeHex(4))
	assert.Nil(t, g)
	// Then the remaining exchange, then inbound.
	_, _ = r.Insert(ctx, mkPeer(6, PeernetSourceExchange, peerT0.Add(9*time.Minute)), caps)
	g, _ = r.Get(ctx, hexID('a'), nodeHex(3))
	assert.Nil(t, g)
	_, _ = r.Insert(ctx, mkPeer(7, PeernetSourceExchange, peerT0.Add(9*time.Minute)), caps)
	_, _ = r.Insert(ctx, mkPeer(8, PeernetSourceExchange, peerT0.Add(9*time.Minute)), caps)
	g, _ = r.Get(ctx, hexID('a'), nodeHex(1))
	assert.NotNil(t, g, "user peer is never evicted")

	// A full network of users and verified peers refuses new non-user peers
	// but still accepts a user peer.
	r2 := newPeerTestRepo(t)
	small := PeernetPeerCaps{MaxTotal: 2}
	_, _ = r2.Insert(ctx, mkPeer(1, PeernetSourceUser, peerT0), small)
	_, _ = r2.Insert(ctx, mkPeer(2, PeernetSourceUser, peerT0), small)
	_, err = r2.Insert(ctx, mkPeer(3, PeernetSourceExchange, peerT0), small)
	assert.True(t, errors.Is(err, ErrPeernetPeerCapReached))
	ok, err := r2.Insert(ctx, mkPeer(4, PeernetSourceUser, peerT0), small)
	require.NoError(t, err)
	assert.True(t, ok)
	c, _ := r2.Counts(ctx, hexID('a'))
	assert.Equal(t, 3, c.User)
}

func TestPeerRepo_CapsAreScopedPerNetwork(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	caps := PeernetPeerCaps{MaxTotal: 1}
	a := mkPeer(1, PeernetSourceExchange, peerT0)
	b := mkPeer(1, PeernetSourceExchange, peerT0)
	b.NetworkID = hexID('b')
	for _, p := range []PeernetPeer{a, b} {
		ok, err := r.Insert(ctx, p, caps)
		require.NoError(t, err)
		assert.True(t, ok)
	}
	la, _ := r.List(ctx, a.NetworkID)
	assert.Len(t, la, 1)
}

func TestPeerRepo_BackoffAndSuccessReset(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	p := mkPeer(1, PeernetSourceExchange, peerT0)
	_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
	bo := func(f int) time.Duration { return time.Duration(f) * time.Minute }
	n, err := r.RecordFailure(ctx, p.NetworkID, p.NodeID, peerT0, bo)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	n, _ = r.RecordFailure(ctx, p.NetworkID, p.NodeID, peerT0, bo)
	assert.Equal(t, 2, n)
	g, _ := r.Get(ctx, p.NetworkID, p.NodeID)
	assert.Equal(t, peerT0.Add(2*time.Minute).Unix(), g.NextAttempt.Unix())

	due, _ := r.ListDue(ctx, p.NetworkID, peerT0.Add(time.Minute), 10)
	assert.Empty(t, due)
	due, _ = r.ListDue(ctx, p.NetworkID, peerT0.Add(2*time.Minute), 10)
	assert.Len(t, due, 1)

	require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "1.1.1.1:5", peerT0.Add(3*time.Minute), peerT0.Add(8*time.Minute)))
	g, _ = r.Get(ctx, p.NetworkID, p.NodeID)
	assert.True(t, g.Verified)
	assert.Equal(t, 0, g.Failures)
	assert.Equal(t, "1.1.1.1:5", g.Address)

	n, err = r.RecordFailure(ctx, p.NetworkID, nodeHex(404), peerT0, bo)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "absent row")
}

func TestPeerRepo_ListDueOrdering(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	for i, src := range []string{PeernetSourceExchange, PeernetSourceInbound, PeernetSourceExchange, PeernetSourceUser} {
		_, _ = r.Insert(ctx, mkPeer(i+1, src, peerT0), PeernetPeerCaps{})
	}
	require.NoError(t, r.RecordSuccess(ctx, hexID('a'), nodeHex(2), "", peerT0, peerT0)) // verified inbound
	_, _ = r.RecordFailure(ctx, hexID('a'), nodeHex(1), peerT0, func(int) time.Duration { return 0 })
	due, err := r.ListDue(ctx, hexID('a'), peerT0.Add(time.Second), 10)
	require.NoError(t, err)
	var got []string
	for _, p := range due {
		got = append(got, p.NodeID)
	}
	// user(4), verified(2), then fewest failures: 3 (0 failures) before 1 (1 failure).
	assert.Equal(t, []string{nodeHex(4), nodeHex(2), nodeHex(3), nodeHex(1)}, got)
	limited, _ := r.ListDue(ctx, hexID('a'), peerT0.Add(time.Second), 2)
	assert.Len(t, limited, 2)
	none, _ := r.ListDue(ctx, hexID('a'), peerT0, 0)
	assert.Empty(t, none)
}

func TestPeerRepo_ListVerifiedFilters(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	for i := 1; i <= 4; i++ {
		_, _ = r.Insert(ctx, mkPeer(i, PeernetSourceExchange, peerT0), PeernetPeerCaps{})
	}
	for _, i := range []int{1, 2, 3} {
		require.NoError(t, r.RecordSuccess(ctx, hexID('a'), nodeHex(i), "", peerT0, peerT0))
	}
	for k := 0; k < 3; k++ {
		_, _ = r.RecordFailure(ctx, hexID('a'), nodeHex(3), peerT0, func(int) time.Duration { return 0 })
	}
	got, err := r.ListVerified(ctx, hexID('a'), 3, 10)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	one, _ := r.ListVerified(ctx, hexID('a'), 3, 1)
	assert.Len(t, one, 1)
	other, _ := r.ListVerified(ctx, hexID('b'), 3, 10)
	assert.Empty(t, other)
}

func TestPeerRepo_AddressesAndCandidates(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	p := mkPeer(1, PeernetSourceInbound, peerT0)
	_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
	changed, err := r.SetUnverifiedAddress(ctx, p.NetworkID, p.NodeID, "7.7.7.7:1", peerT0.Add(time.Minute))
	require.NoError(t, err)
	assert.True(t, changed)
	require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "", peerT0, peerT0))
	changed, _ = r.SetUnverifiedAddress(ctx, p.NetworkID, p.NodeID, "6.6.6.6:1", peerT0)
	assert.False(t, changed, "verified address is never overwritten directly")
	require.NoError(t, r.SetCandidateAddress(ctx, p.NetworkID, p.NodeID, "6.6.6.6:1", peerT0))
	g, _ := r.Get(ctx, p.NetworkID, p.NodeID)
	assert.Equal(t, "7.7.7.7:1", g.Address)
	assert.Equal(t, "6.6.6.6:1", g.CandidateAddress)
	require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "6.6.6.6:1", peerT0, peerT0))
	g, _ = r.Get(ctx, p.NetworkID, p.NodeID)
	assert.Equal(t, "6.6.6.6:1", g.Address)
	assert.Empty(t, g.CandidateAddress)

	u := mkPeer(2, PeernetSourceUser, peerT0)
	_, _ = r.Insert(ctx, u, PeernetPeerCaps{})
	require.NoError(t, r.SetCandidateAddress(ctx, u.NetworkID, u.NodeID, "1.2.3.4:5", peerT0))
	gu, _ := r.Get(ctx, u.NetworkID, u.NodeID)
	assert.Equal(t, u.Address, gu.Address)
}

func TestPeerRepo_UserPinChangedAndReconcile(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	u1, u2 := mkPeer(1, PeernetSourceUser, peerT0), mkPeer(2, PeernetSourceUser, peerT0)
	x := mkPeer(3, PeernetSourceExchange, peerT0)
	for _, p := range []PeernetPeer{u1, u2, x} {
		_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
	}
	by, err := r.GetUserByAddress(ctx, u1.NetworkID, u1.Address)
	require.NoError(t, err)
	require.NotNil(t, by)
	assert.Equal(t, u1.NodeID, by.NodeID)
	nilp, _ := r.GetUserByAddress(ctx, u1.NetworkID, x.Address)
	assert.Nil(t, nilp, "non-user rows are not user pins")

	require.NoError(t, r.SetChangedNode(ctx, u1.NetworkID, u1.NodeID, nodeHex(77)))
	require.NoError(t, r.SetChangedNode(ctx, x.NetworkID, x.NodeID, nodeHex(77)))
	g, _ := r.Get(ctx, u1.NetworkID, u1.NodeID)
	assert.Equal(t, nodeHex(77), g.ChangedNodeID)
	gx, _ := r.Get(ctx, x.NetworkID, x.NodeID)
	assert.Empty(t, gx.ChangedNodeID, "only user peers can be flagged")
	c, _ := r.Counts(ctx, u1.NetworkID)
	assert.Equal(t, PeernetPeerCounts{Total: 3, User: 2, Exchange: 1, Changed: 1}, c)

	n, err := r.DeleteUserNotIn(ctx, u1.NetworkID, []string{u1.Address})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	list, _ := r.List(ctx, u1.NetworkID)
	assert.Len(t, list, 2)
	assert.Equal(t, PeernetSourceUser, list[0].Source, "user peers listed first")
}

func TestPeerRepo_Prune(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	old := peerT0.Add(-72 * time.Hour)
	stale := mkPeer(1, PeernetSourceExchange, old)
	failingRecent := mkPeer(2, PeernetSourceExchange, peerT0)
	healthy := mkPeer(3, PeernetSourceExchange, old)
	user := mkPeer(4, PeernetSourceUser, old)
	for _, p := range []PeernetPeer{stale, failingRecent, healthy, user} {
		_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
	}
	zero := func(int) time.Duration { return 0 }
	for _, id := range []int{1, 2, 4} {
		for k := 0; k < 5; k++ {
			_, _ = r.RecordFailure(ctx, hexID('a'), nodeHex(id), peerT0, zero)
		}
	}
	n, err := r.Prune(ctx, hexID('a'), peerT0.Add(-24*time.Hour), 5)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	list, _ := r.List(ctx, hexID('a'))
	assert.Len(t, list, 3)
}

func TestPeerRepo_CountsEmptyAndDelete(t *testing.T) {
	r := newPeerTestRepo(t)
	ctx := context.Background()
	c, err := r.Counts(ctx, hexID('a'))
	require.NoError(t, err)
	assert.Equal(t, PeernetPeerCounts{}, c)
	p := mkPeer(1, PeernetSourceInbound, peerT0)
	_, _ = r.Insert(ctx, p, PeernetPeerCaps{})
	require.NoError(t, r.Delete(ctx, p.NetworkID, p.NodeID))
	require.NoError(t, r.Delete(ctx, p.NetworkID, p.NodeID))
	g, _ := r.Get(ctx, p.NetworkID, p.NodeID)
	assert.Nil(t, g)
}
