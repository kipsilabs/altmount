package database

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// peerRepoBackends returns the SQLite repository and, when
// ALTMOUNT_TEST_PG_DSN points at a disposable PostgreSQL database (the schema
// is dropped and recreated), a PostgreSQL one. Each call yields fresh, empty
// repositories.
func peerRepoBackends(t *testing.T) map[string]*PeernetPeerRepository {
	t.Helper()
	out := map[string]*PeernetPeerRepository{"sqlite": newPeerTestRepo(t)}
	if hasPostgresDSN() {
		out["postgres"] = NewPeernetPeerRepository(openPostgresLatest(t), DialectPostgres)
	}
	return out
}

func hasPostgresDSN() bool { return os.Getenv("ALTMOUNT_TEST_PG_DSN") != "" }

func forEachPeerBackend(t *testing.T, fn func(t *testing.T, r *PeernetPeerRepository)) {
	t.Helper()
	for name, r := range peerRepoBackends(t) {
		t.Run(name, func(t *testing.T) { fn(t, r) })
	}
}

func TestPeernetPeerRepo_ConcurrentInsertSameKeyIsNotAnError(t *testing.T) {
	forEachPeerBackend(t, func(t *testing.T, r *PeernetPeerRepository) {
		ctx := context.Background()
		const workers = 16
		var wg sync.WaitGroup
		var mu sync.Mutex
		inserted, errs := 0, []error{}
		start := make(chan struct{})
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ok, err := r.Insert(ctx, mkPeer(1, PeernetSourceExchange, peerT0), PeernetPeerCaps{})
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err)
				}
				if ok {
					inserted++
				}
			}()
		}
		close(start)
		wg.Wait()
		assert.Empty(t, errs)
		assert.Equal(t, 1, inserted, "exactly one concurrent insert wins")
	})
}

func TestPeernetPeerRepo_ConcurrentInsertsRespectCap(t *testing.T) {
	forEachPeerBackend(t, func(t *testing.T, r *PeernetPeerRepository) {
		ctx := context.Background()
		// Local-source rows are never evicted, so the cap is a hard stop.
		caps := PeernetPeerCaps{MaxTotal: 5}
		const workers = 30
		var wg sync.WaitGroup
		var mu sync.Mutex
		ok, capped, other := 0, 0, []error{}
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				done, err := r.Insert(ctx, mkPeer(i+1, PeernetSourceLocal, peerT0), caps)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case errors.Is(err, ErrPeernetPeerCapReached):
					capped++
				case err != nil:
					other = append(other, err)
				case done:
					ok++
				}
			}()
		}
		wg.Wait()
		assert.Empty(t, other)
		assert.Equal(t, 5, ok)
		assert.Equal(t, workers-5, capped)
		c, err := r.Counts(ctx, hexID('a'))
		require.NoError(t, err)
		assert.Equal(t, 5, c.Total)
	})
}

func TestPeernetPeerRepo_ExistingRowDoesNotEvictAnyone(t *testing.T) {
	forEachPeerBackend(t, func(t *testing.T, r *PeernetPeerRepository) {
		ctx := context.Background()
		caps := PeernetPeerCaps{MaxTotal: 2}
		for i := 1; i <= 2; i++ {
			_, err := r.Insert(ctx, mkPeer(i, PeernetSourceExchange, peerT0.Add(time.Duration(i)*time.Minute)), caps)
			require.NoError(t, err)
		}
		ok, err := r.Insert(ctx, mkPeer(2, PeernetSourceExchange, peerT0), caps)
		require.NoError(t, err)
		assert.False(t, ok)
		c, err := r.Counts(ctx, hexID('a'))
		require.NoError(t, err)
		assert.Equal(t, 2, c.Total, "re-inserting a known peer must not evict another")
	})
}

func TestPeernetPeerRepo_VerifiedPeersAreEvictedWorstFirst(t *testing.T) {
	forEachPeerBackend(t, func(t *testing.T, r *PeernetPeerRepository) {
		ctx := context.Background()
		caps := PeernetPeerCaps{MaxTotal: 4}
		user := mkPeer(1, PeernetSourceUser, peerT0)
		_, err := r.Insert(ctx, user, caps)
		require.NoError(t, err)
		for i := 2; i <= 4; i++ {
			_, err := r.Insert(ctx, mkPeer(i, PeernetSourceExchange, peerT0), caps)
			require.NoError(t, err)
			require.NoError(t, r.RecordSuccess(ctx, hexID('a'), nodeHex(i), "", peerT0.Add(time.Duration(i)*time.Hour), peerT0))
		}
		// node 3 has failed most, node 2 and 4 nothing; among equals oldest success goes.
		for i := 0; i < 3; i++ {
			_, err := r.RecordFailure(ctx, hexID('a'), nodeHex(3), peerT0, func(int) time.Duration { return 0 })
			require.NoError(t, err)
		}
		ok, err := r.Insert(ctx, mkPeer(5, PeernetSourceExchange, peerT0), caps)
		require.NoError(t, err, "a full table of verified peers must not refuse forever")
		assert.True(t, ok)
		gone, err := r.Get(ctx, hexID('a'), nodeHex(3))
		require.NoError(t, err)
		assert.Nil(t, gone, "most failing verified peer evicted")

		ok, err = r.Insert(ctx, mkPeer(6, PeernetSourceExchange, peerT0), caps)
		require.NoError(t, err)
		assert.True(t, ok)
		// Node 5 is unverified: it is the cheaper victim, verified peers stay.
		gone5, _ := r.Get(ctx, hexID('a'), nodeHex(5))
		assert.Nil(t, gone5)
		for _, i := range []int{2, 4} {
			kept, _ := r.Get(ctx, hexID('a'), nodeHex(i))
			assert.NotNil(t, kept, "verified node %d kept while an unverified victim exists", i)
		}
		kept, _ := r.Get(ctx, hexID('a'), nodeHex(1))
		assert.NotNil(t, kept, "user peer never evicted")
	})
}

func TestPeernetPeerRepo_UserAndLocalNeverEvicted(t *testing.T) {
	forEachPeerBackend(t, func(t *testing.T, r *PeernetPeerRepository) {
		ctx := context.Background()
		caps := PeernetPeerCaps{MaxTotal: 2}
		_, err := r.Insert(ctx, mkPeer(1, PeernetSourceUser, peerT0), caps)
		require.NoError(t, err)
		_, err = r.Insert(ctx, mkPeer(2, PeernetSourceLocal, peerT0), caps)
		require.NoError(t, err)
		require.NoError(t, r.RecordSuccess(ctx, hexID('a'), nodeHex(2), "", peerT0, peerT0))
		_, err = r.Insert(ctx, mkPeer(3, PeernetSourceExchange, peerT0), caps)
		assert.ErrorIs(t, err, ErrPeernetPeerCapReached)
	})
}

func TestPeernetPeerRepo_UnverifiedRowsAreCappedPerPrefix(t *testing.T) {
	forEachPeerBackend(t, func(t *testing.T, r *PeernetPeerRepository) {
		ctx := context.Background()
		caps := PeernetPeerCaps{MaxUnverifiedPerPrefix: 2}
		mk := func(i int, addr string, src string, at time.Time) PeernetPeer {
			p := mkPeer(i, src, at)
			p.Address = addr
			return p
		}
		for i := 1; i <= 3; i++ {
			ok, err := r.Insert(ctx, mk(i, fmt.Sprintf("8.8.8.%d:7000", i), PeernetSourceInbound, peerT0.Add(time.Duration(i)*time.Minute)), caps)
			require.NoError(t, err)
			require.True(t, ok)
		}
		other, err := r.Insert(ctx, mk(10, "9.9.9.9:7000", PeernetSourceInbound, peerT0), caps)
		require.NoError(t, err)
		require.True(t, other)
		c, err := r.Counts(ctx, hexID('a'))
		require.NoError(t, err)
		assert.Equal(t, 3, c.Total, "two rows for 8.8.8.0/24 plus one for 9.9.9.0/24")
		oldest, _ := r.Get(ctx, hexID('a'), nodeHex(1))
		assert.Nil(t, oldest, "the prefix's own stalest row is evicted")
		verified := mk(11, "8.8.8.50:7000", PeernetSourceExchange, peerT0)
		_, err = r.Insert(ctx, verified, caps)
		require.NoError(t, err)
		require.NoError(t, r.RecordSuccess(ctx, hexID('a'), nodeHex(11), "", peerT0, peerT0))
		_, err = r.Insert(ctx, mk(12, "8.8.8.60:7000", PeernetSourceInbound, peerT0.Add(time.Hour)), caps)
		require.NoError(t, err)
		keptVerified, _ := r.Get(ctx, hexID('a'), nodeHex(11))
		assert.NotNil(t, keptVerified, "verified rows do not count against the unverified prefix cap")
	})
}

func TestPeernetPeerRepo_PromoteToLocal(t *testing.T) {
	forEachPeerBackend(t, func(t *testing.T, r *PeernetPeerRepository) {
		ctx := context.Background()
		_, err := r.Insert(ctx, mkPeer(1, PeernetSourceExchange, peerT0), PeernetPeerCaps{})
		require.NoError(t, err)
		_, err = r.Insert(ctx, mkPeer(2, PeernetSourceUser, peerT0), PeernetPeerCaps{})
		require.NoError(t, err)
		require.NoError(t, r.PromoteToLocal(ctx, hexID('a'), nodeHex(1)))
		require.NoError(t, r.PromoteToLocal(ctx, hexID('a'), nodeHex(2)))
		p1, _ := r.Get(ctx, hexID('a'), nodeHex(1))
		p2, _ := r.Get(ctx, hexID('a'), nodeHex(2))
		assert.Equal(t, PeernetSourceLocal, p1.Source)
		assert.Equal(t, PeernetSourceUser, p2.Source, "a user pin is never downgraded")
	})
}

func TestPeernetPeerAddrPrefix(t *testing.T) {
	assert.Equal(t, "8.8.8.0/24", peerAddrPrefix("8.8.8.200:1"))
	assert.Equal(t, "2606:4700:1234::/48", peerAddrPrefix("[2606:4700:1234:5678::1]:1"))
	assert.Equal(t, "peer.example.org", peerAddrPrefix("Peer.Example.org:99"))
}

// "Known" means verified by a completed mutual-TLS contact: peer-exchange
// entries are stored unverified and cost nothing to pre-register, so first
// seen alone proves nothing.
func TestPeernetPeerRepo_KnownForNeedsVerifiedAndRecentContact(t *testing.T) {
	forEachPeerBackend(t, func(t *testing.T, r *PeernetPeerRepository) {
		ctx := context.Background()
		day := 24 * time.Hour
		p := mkPeer(1, PeernetSourceExchange, peerT0)
		_, err := r.Insert(ctx, p, PeernetPeerCaps{})
		require.NoError(t, err)

		known := func(at time.Time) bool {
			ok, err := r.KnownFor(ctx, p.NetworkID, p.NodeID, day, at)
			require.NoError(t, err)
			return ok
		}
		assert.False(t, known(peerT0.Add(30*day)), "an unverified peer-exchange entry is never known, however old")

		// First verified contact at +10 days.
		verifiedAt := peerT0.Add(10 * day)
		require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "", verifiedAt, verifiedAt.Add(time.Hour)))
		assert.False(t, known(verifiedAt.Add(23*time.Hour)), "not yet known for the required time since the first verified contact")
		assert.True(t, known(verifiedAt.Add(25*time.Hour)))

		// Later contacts and peer-exchange touches never reset the first verified time.
		later := verifiedAt.Add(3 * day)
		require.NoError(t, r.RecordSuccess(ctx, p.NetworkID, p.NodeID, "", later, later.Add(time.Hour)))
		require.NoError(t, r.Touch(ctx, p.NetworkID, p.NodeID, later.Add(time.Hour)))
		assert.True(t, known(later.Add(time.Hour)))

		// Silent for longer than the window: no longer known.
		assert.True(t, known(later.Add(PeernetKnownMaxSilence)))
		assert.False(t, known(later.Add(PeernetKnownMaxSilence+time.Hour)), "not seen recently")

		// A verified row inserted directly (already proven) counts from its first success.
		q := mkPeer(2, PeernetSourceExchange, peerT0)
		q.Verified, q.LastSuccess = true, peerT0
		_, err = r.Insert(ctx, q, PeernetPeerCaps{})
		require.NoError(t, err)
		ok, err := r.KnownFor(ctx, q.NetworkID, q.NodeID, day, peerT0.Add(2*day))
		require.NoError(t, err)
		assert.True(t, ok)
	})
}
