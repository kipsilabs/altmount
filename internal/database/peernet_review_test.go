package database

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeernetContentRepository_DeleteByPrefixIsCaseExact(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "Movies/a.mkv", FileID: hexID('a')},
		{FilePath: "movies/a.mkv", FileID: hexID('b')},
		{FilePath: "MOVIES", FileID: hexID('c')},
	}))
	require.NoError(t, r.DeleteByPrefix(ctx, "Movies"))
	missing, err := r.MissingPaths(ctx, []string{"Movies/a.mkv", "movies/a.mkv", "MOVIES"})
	require.NoError(t, err)
	assert.Equal(t, []string{"Movies/a.mkv"}, missing, "only the exact-case subtree is removed")
}

func TestPeernetContentRepository_RenamePrefix(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "old/a.mkv", FileID: hexID('a')},
		{FilePath: "old/sub/b.mkv", FileID: hexID('b')},
		{FilePath: "older/c.mkv", FileID: hexID('c')},
		{FilePath: "Old/d.mkv", FileID: hexID('d')},
		{FilePath: "new/stale.mkv", FileID: hexID('e')},
	}))
	require.NoError(t, r.RenamePrefix(ctx, "old", "new"))
	missing, err := r.MissingPaths(ctx, []string{"new/a.mkv", "new/sub/b.mkv", "older/c.mkv", "Old/d.mkv", "new/stale.mkv", "old/a.mkv"})
	require.NoError(t, err)
	assert.Equal(t, []string{"new/stale.mkv", "old/a.mkv"}, missing, "old rows moved, destination leftovers replaced")
	got, err := r.FindByFileID(ctx, hexID('b'))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "new/sub/b.mkv", got[0].FilePath)
	require.Error(t, r.RenamePrefix(ctx, "", "x"))
	require.Error(t, r.RenamePrefix(ctx, "x", ""))
}

func TestPeernetContentRepository_ListPathsPages(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	for i, p := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: p, FileID: hexID(byte('a' + i))}))
	}
	var all []string
	after := ""
	for {
		page, err := r.ListPage(ctx, after, 2)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			all = append(all, e.FilePath)
		}
		after = page[len(page)-1].FilePath
	}
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, all)
}

func TestPeernetNetworkRepository_RedeemReissuesForExistingMember(t *testing.T) {
	r := newPeernetNetRepo(t)
	ctx := context.Background()
	id := hexID('a')
	seedNet(t, r, id)
	for _, h := range []byte{'1', '2', '3'} {
		require.NoError(t, r.CreateInvite(ctx, PeernetInvite{NetworkID: id, CodeHash: hexID(h), Alias: "bob", CreatedAt: 1, ExpiresAt: 1000}))
	}
	require.NoError(t, r.RedeemInvite(ctx, id, hexID('1'), hexID('m'), 50, 50, 200))
	// The same node redeems again (lost its certificate): reissued, invite consumed.
	require.NoError(t, r.RedeemInvite(ctx, id, hexID('2'), hexID('m'), 60, 60, 300))
	m, err := r.GetMember(ctx, id, hexID('m'))
	require.NoError(t, err)
	assert.Equal(t, int64(300), m.CertExpiresAt)
	assert.Equal(t, int64(50), m.JoinedAt, "join time is kept")
	inv, err := r.GetInvite(ctx, id, hexID('2'))
	require.NoError(t, err)
	assert.Equal(t, int64(60), inv.UsedAt)
	// A revoked node is refused and the invite stays usable.
	require.NoError(t, r.RevokeMember(ctx, id, hexID('m'), 70))
	assert.ErrorIs(t, r.RedeemInvite(ctx, id, hexID('3'), hexID('m'), 80, 80, 400), ErrPeernetInviteDenied)
	inv, err = r.GetInvite(ctx, id, hexID('3'))
	require.NoError(t, err)
	assert.Zero(t, inv.UsedAt)
}

func TestPeernetNetworkRepository_UpdateSettingsIsOneStep(t *testing.T) {
	r := newPeernetNetRepo(t)
	ctx := context.Background()
	id := hexID('a')
	seedNet(t, r, id)
	lvl, allow, addr := 2, true, "h.example:9"
	require.NoError(t, r.UpdateSettings(ctx, id, NetworkSettingsUpdate{Level: &lvl, AllowPrivateAddresses: &allow, HostAddress: &addr}, 20))
	n, err := r.GetNetwork(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, 2, n.ExposureLevel)
	assert.True(t, n.AllowPrivateAddresses)
	assert.Equal(t, "h.example:9", n.HostAddress)

	bad := 9
	allow2 := false
	require.Error(t, r.UpdateSettings(ctx, id, NetworkSettingsUpdate{Level: &bad, AllowPrivateAddresses: &allow2}, 21))
	n, _ = r.GetNetwork(ctx, id)
	assert.True(t, n.AllowPrivateAddresses, "an invalid field leaves everything unchanged")
	assert.ErrorIs(t, r.UpdateSettings(ctx, hexID('f'), NetworkSettingsUpdate{Level: &lvl}, 22), ErrPeernetNotFound)
	require.Error(t, r.UpdateSettings(ctx, id, NetworkSettingsUpdate{}, 23))
}

func TestPeernetNetworkRepository_RevocationSeqOverflow(t *testing.T) {
	r := newPeernetNetRepo(t)
	ctx := context.Background()
	id := hexID('a')
	seedNet(t, r, id)
	require.Error(t, r.SetRevocationList(ctx, id, uint64(math.MaxInt64)+1, []byte("x"), 5))
	require.Error(t, r.CreateNetwork(ctx, PeernetNetwork{NetworkID: hexID('b'), Role: PeernetRoleHost, RevocationSeq: math.MaxUint64}))
	require.NoError(t, r.SetRevocationList(ctx, id, uint64(math.MaxInt64), []byte("x"), 5))
	n, err := r.GetNetwork(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxInt64), n.RevocationSeq)
}

func TestPeernetNetworkRepository_OrphanPeerNetworks(t *testing.T) {
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	r := NewPeernetNetworkRepository(db.Connection(), DialectSQLite)
	ctx := context.Background()
	seedNet(t, r, hexID('a'))
	for i, nw := range []string{hexID('a'), hexID('b'), hexID('c'), hexID('c')} {
		_, err := db.Connection().ExecContext(ctx, `INSERT INTO peernet_peers
			(network_id, node_id, address, source, first_seen, last_seen) VALUES (?, ?, 'h:1', 'user', 1, 1)`,
			nw, fmt.Sprintf("node-%d", i))
		require.NoError(t, err)
	}
	ids, err := r.ListOrphanPeerNetworkIDs(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{hexID('b'), hexID('c')}, ids)
}
