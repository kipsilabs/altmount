package database

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPeernetNetRepo(t *testing.T) *PeernetNetworkRepository {
	t.Helper()
	if hasPostgresDSN() {
		return NewPeernetNetworkRepository(openPostgresLatest(t), DialectPostgres)
	}
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return NewPeernetNetworkRepository(db.Connection(), DialectSQLite)
}

func seedNet(t *testing.T, r *PeernetNetworkRepository, id string) {
	t.Helper()
	require.NoError(t, r.CreateNetwork(context.Background(), PeernetNetwork{
		NetworkID: id, DisplayName: "Net\x00\x1b work", Role: PeernetRoleHost,
		RootPublicKey: []byte{1, 2, 3}, CreatedAt: 10, UpdatedAt: 10,
	}))
}

func TestPeernetNetworkRepository_NetworkRoundTrip(t *testing.T) {
	r := newPeernetNetRepo(t)
	ctx := context.Background()
	id := hexID('a')
	seedNet(t, r, id)
	assert.ErrorIs(t, r.CreateNetwork(ctx, PeernetNetwork{NetworkID: id, Role: PeernetRoleHost}), ErrPeernetExists)
	assert.Error(t, r.CreateNetwork(ctx, PeernetNetwork{NetworkID: "bad", Role: PeernetRoleHost}))
	assert.Error(t, r.CreateNetwork(ctx, PeernetNetwork{NetworkID: hexID('b'), Role: "x"}))
	assert.Error(t, r.CreateNetwork(ctx, PeernetNetwork{NetworkID: hexID('b'), Role: PeernetRoleHost, ExposureLevel: 4}))

	n, err := r.GetNetwork(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "Net work", n.DisplayName)
	assert.Equal(t, 0, n.ExposureLevel)
	assert.False(t, n.AllowPrivateAddresses)
	assert.Equal(t, []byte{1, 2, 3}, n.RootPublicKey)

	require.NoError(t, r.SetExposureLevel(ctx, id, 2, 11))
	require.NoError(t, r.SetAllowPrivateAddresses(ctx, id, true, 12))
	require.NoError(t, r.SetHostAddress(ctx, id, "h:1", 13))
	require.NoError(t, r.SetOwnCertificate(ctx, id, []byte("c"), 14))
	require.NoError(t, r.SetDisplayName(ctx, id, strings.Repeat("é", 200), 15))
	n, _ = r.GetNetwork(ctx, id)
	assert.Equal(t, 2, n.ExposureLevel)
	assert.True(t, n.AllowPrivateAddresses)
	assert.Equal(t, "h:1", n.HostAddress)
	assert.Equal(t, []byte("c"), n.OwnCertificate)
	assert.Len(t, []rune(n.DisplayName), PeernetMaxDisplayNameRunes)
	assert.Equal(t, int64(15), n.UpdatedAt)

	assert.Error(t, r.SetExposureLevel(ctx, id, 9, 1))
	assert.ErrorIs(t, r.SetExposureLevel(ctx, hexID('z'), 1, 1), ErrPeernetNotFound)
	_, err = r.GetNetwork(ctx, hexID('z'))
	assert.ErrorIs(t, err, ErrPeernetNotFound)

	list, err := r.ListNetworks(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 1)
}

func TestPeernetNetworkRepository_RevocationMonotonic(t *testing.T) {
	r := newPeernetNetRepo(t)
	ctx := context.Background()
	id := hexID('a')
	seedNet(t, r, id)
	require.NoError(t, r.SetRevocationList(ctx, id, 5, []byte("five"), 1))
	require.NoError(t, r.SetRevocationList(ctx, id, 5, []byte("five"), 1))
	assert.ErrorIs(t, r.SetRevocationList(ctx, id, 4, []byte("four"), 1), ErrPeernetStale)
	require.NoError(t, r.SetRevocationList(ctx, id, 6, []byte("six"), 1))
	n, _ := r.GetNetwork(ctx, id)
	assert.Equal(t, uint64(6), n.RevocationSeq)
	assert.Equal(t, []byte("six"), n.RevocationList)
	assert.ErrorIs(t, r.SetRevocationList(ctx, hexID('z'), 1, nil, 1), ErrPeernetNotFound)
}

func TestPeernetNetworkRepository_InvitesAndMembers(t *testing.T) {
	r := newPeernetNetRepo(t)
	ctx := context.Background()
	id := hexID('a')
	seedNet(t, r, id)
	require.NoError(t, r.CreateInvite(ctx, PeernetInvite{NetworkID: id, CodeHash: hexID('1'), Alias: "bob\n", CreatedAt: 1, ExpiresAt: 100}))
	require.NoError(t, r.CreateInvite(ctx, PeernetInvite{NetworkID: id, CodeHash: hexID('2'), CreatedAt: 2, ExpiresAt: 5}))
	assert.Error(t, r.CreateInvite(ctx, PeernetInvite{NetworkID: id, CodeHash: "x"}))

	// Expired and unknown give the same error.
	assert.ErrorIs(t, r.RedeemInvite(ctx, id, hexID('2'), hexID('m'), 50, 50, 100), ErrPeernetInviteDenied)
	assert.ErrorIs(t, r.RedeemInvite(ctx, id, hexID('9'), hexID('m'), 50, 50, 100), ErrPeernetInviteDenied)

	require.NoError(t, r.RedeemInvite(ctx, id, hexID('1'), hexID('m'), 50, 50, 200))
	assert.ErrorIs(t, r.RedeemInvite(ctx, id, hexID('1'), hexID('n'), 50, 50, 200), ErrPeernetInviteDenied)

	inv, err := r.ListInvites(ctx, id)
	require.NoError(t, err)
	require.Len(t, inv, 2)
	var used PeernetInvite
	for _, i := range inv {
		if i.CodeHash == hexID('1') {
			used = i
		}
	}
	assert.Equal(t, int64(50), used.UsedAt)
	assert.Equal(t, hexID('m'), used.UsedBy)

	m, err := r.GetMember(ctx, id, hexID('m'))
	require.NoError(t, err)
	assert.Equal(t, "bob", m.Alias)
	assert.Equal(t, int64(200), m.CertExpiresAt)
	assert.Zero(t, m.RevokedAt)

	// An existing non-revoked member redeeming again is re-issued (covered in
	// TestPeernetNetworkRepository_RedeemReissuesForExistingMember).
	require.NoError(t, r.CreateInvite(ctx, PeernetInvite{NetworkID: id, CodeHash: hexID('3'), CreatedAt: 2, ExpiresAt: 100}))
	require.NoError(t, r.RedeemInvite(ctx, id, hexID('3'), hexID('o'), 50, 50, 200))

	require.NoError(t, r.SetMemberCertificate(ctx, id, hexID('m'), 60, 260))
	require.NoError(t, r.RevokeMember(ctx, id, hexID('m'), 70))
	assert.ErrorIs(t, r.RevokeMember(ctx, id, hexID('m'), 71), ErrPeernetNotFound)
	assert.ErrorIs(t, r.SetMemberCertificate(ctx, id, hexID('m'), 1, 2), ErrPeernetNotFound)
	rev, err := r.ListRevokedSince(ctx, id, 70)
	require.NoError(t, err)
	assert.Equal(t, []string{hexID('m')}, rev)
	rev, _ = r.ListRevokedSince(ctx, id, 71)
	assert.Empty(t, rev)
	ms, err := r.ListMembers(ctx, id)
	require.NoError(t, err)
	assert.Len(t, ms, 2)

	require.NoError(t, r.DeleteInvite(ctx, id, hexID('2')))
	assert.ErrorIs(t, r.DeleteInvite(ctx, id, hexID('2')), ErrPeernetNotFound)

	require.NoError(t, r.DeleteNetwork(ctx, id))
	assert.ErrorIs(t, r.DeleteNetwork(ctx, id), ErrPeernetNotFound)
	ms, _ = r.ListMembers(ctx, id)
	assert.Empty(t, ms)
	inv, _ = r.ListInvites(ctx, id)
	assert.Empty(t, inv)
}

func TestPeernetNetworkRepository_RedeemConcurrent(t *testing.T) {
	r := newPeernetNetRepo(t)
	ctx := context.Background()
	id := hexID('a')
	seedNet(t, r, id)
	require.NoError(t, r.CreateInvite(ctx, PeernetInvite{NetworkID: id, CodeHash: hexID('1'), CreatedAt: 1, ExpiresAt: 100}))
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node := strings.Repeat(string(rune('a'+i%26)), 31) + string(rune('a'+i/26))
			node = strings.Repeat("0", 64-len(node)) + node
			if err := r.RedeemInvite(ctx, id, hexID('1'), node, 50, 50, 200); err == nil {
				wins.Add(1)
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int32(1), wins.Load())
}

func TestCleanPeerText(t *testing.T) {
	assert.Equal(t, "ab", CleanPeerText("\x00a\u202eb\xff\n", 10))
	assert.Equal(t, "ab", CleanPeerText("  ab  ", 10))
	assert.Equal(t, "ab", CleanPeerText("abcdef", 2))
}

func TestPeernetNetworkRepository_RevokedState(t *testing.T) {
	r := newPeernetNetRepo(t)
	ctx := context.Background()
	id := hexID('c')
	seedNet(t, r, id)
	n, err := r.GetNetwork(ctx, id)
	require.NoError(t, err)
	assert.Zero(t, n.RevokedAt)

	require.NoError(t, r.SetRevoked(ctx, id, 500, 501))
	n, err = r.GetNetwork(ctx, id)
	require.NoError(t, err)
	assert.EqualValues(t, 500, n.RevokedAt)
	// The first revocation time is kept.
	require.NoError(t, r.SetRevoked(ctx, id, 900, 901))
	n, _ = r.GetNetwork(ctx, id)
	assert.EqualValues(t, 500, n.RevokedAt)
	assert.ErrorIs(t, r.SetRevoked(ctx, hexID('d'), 1, 1), ErrPeernetNotFound)
}
