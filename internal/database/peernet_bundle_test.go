package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bundleHex(i int) string { return relHex(i + 7_000_000) }

func TestPeernetPublishState_BundleRoundTripAndLookup(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		in := st(hexID('a'), PeernetRecordKindRelease, hexID('1'), "", 1000)
		in.BundleID, in.BundleLevel = bundleHex(1), 3
		in.OriginCategory, in.OriginIndexer = "Movies", "IdxOne"
		require.NoError(t, b.state.Put(ctx, in))

		got, ok, err := b.state.Get(ctx, hexID('a'), PeernetRecordKindRelease, hexID('1'), "")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, bundleHex(1), got.BundleID)
		assert.Equal(t, 3, got.BundleLevel)

		byBundle, ok, err := b.state.GetReleaseByBundle(ctx, hexID('a'), bundleHex(1))
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, hexID('1'), byBundle.ReleaseID)
		assert.Equal(t, "movies", byBundle.OriginCategory)

		_, ok, err = b.state.GetReleaseByBundle(ctx, hexID('b'), bundleHex(1))
		require.NoError(t, err)
		assert.False(t, ok, "announcements are per network")
		_, ok, err = b.state.GetReleaseByBundle(ctx, hexID('a'), bundleHex(2))
		require.NoError(t, err)
		assert.False(t, ok, "an unknown bundle id is not announced")
		_, _, err = b.state.GetReleaseByBundle(ctx, hexID('a'), "nothex")
		require.Error(t, err)
		_, ok, err = b.state.GetReleaseByBundle(ctx, hexID('a'), "")
		assert.False(t, ok)
		_ = err
	})
}

func TestPeernetPublishState_BundleLevelMismatchAndStale(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		mk := func(rel string, bundle string, level int) {
			s := st(hexID('a'), PeernetRecordKindRelease, rel, "", 10)
			s.BundleID, s.BundleLevel = bundle, level
			require.NoError(t, b.state.Put(ctx, s))
		}
		mk(hexID('1'), bundleHex(1), 3) // announced with a bundle
		mk(hexID('2'), "", 3)           // decided at level 3: not shareable
		mk(hexID('3'), "", 2)           // published at level 2
		att := st(hexID('a'), PeernetRecordKindAttestation, hexID('1'), hexID('f'), 10)
		require.NoError(t, b.state.Put(ctx, att))

		// Network now at level 3: only the level-2 record needs re-announcing.
		got, err := b.state.ListBundleLevelMismatch(ctx, hexID('a'), true, 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, hexID('3'), got[0].ReleaseID)

		// Network dropped to level 2: both level-3 records must be superseded.
		got, err = b.state.ListBundleLevelMismatch(ctx, hexID('a'), false, 10)
		require.NoError(t, err)
		require.Len(t, got, 2)

		got, err = b.state.ListBundleLevelMismatch(ctx, hexID('a'), false, 1)
		require.NoError(t, err)
		assert.Len(t, got, 1, "bounded")

		// A stale bundle is marked so the next level-3 sweep re-announces it.
		require.NoError(t, b.state.MarkBundleStale(ctx, hexID('a'), hexID('1')))
		got, err = b.state.ListBundleLevelMismatch(ctx, hexID('a'), true, 10)
		require.NoError(t, err)
		assert.Len(t, got, 2)
		n, err := b.state.CountAnnouncedBundles(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n, "stale marking leaves the announcement until it is replaced")
	})
}

func TestPeernetRecords_BundleOffers(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		rel := relHex(1)
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ReleaseID, r.PublisherNodeID, r.BundleID = rel, nodeHex(1), bundleHex(1) }))
		insertOK(t, b, mkRec(func(r *PeernetRecord) {
			r.ReleaseID, r.PublisherNodeID, r.BundleID, r.ReleaseScheme = rel, nodeHex(2), bundleHex(2), PeernetSchemeFiles
			r.CreatedAt = recT0.Add(time.Minute)
		}))
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ReleaseID, r.PublisherNodeID = rel, nodeHex(3) })) // no bundle
		insertOK(t, b, mkRec(func(r *PeernetRecord) {
			r.ReleaseID, r.PublisherNodeID, r.BundleID = relHex(9), nodeHex(4), bundleHex(9)
		})) // other release
		insertOK(t, b, mkRec(func(r *PeernetRecord) {
			r.NetworkID, r.ReleaseID, r.PublisherNodeID, r.BundleID = hexID('b'), rel, nodeHex(5), bundleHex(5)
		}))
		insertOK(t, b, mkRec(func(r *PeernetRecord) {
			r.ReleaseID, r.PublisherNodeID, r.BundleID, r.Source, r.RelayerNodeID = rel, nodeHex(6), bundleHex(6), PeernetRecordSourceLocal, ""
		}))

		got, err := b.recs.BundleOffers(ctx, []string{hexID('a')}, rel, 10)
		require.NoError(t, err)
		require.Len(t, got, 3, "network a, this release, with a bundle id; the local record is listed too")
		assert.Equal(t, nodeHex(2), got[0].PublisherNodeID, "newest creation time first")
		assert.Equal(t, nodeHex(6), got[1].PublisherNodeID, "then newest seq among equal times")
		byPub := map[string]PeernetBundleOffer{}
		for _, o := range got {
			byPub[o.PublisherNodeID] = o
		}
		assert.Equal(t, bundleHex(2), byPub[nodeHex(2)].BundleID)
		assert.Equal(t, PeernetSchemeFiles, byPub[nodeHex(2)].ReleaseScheme)
		assert.Equal(t, PeernetSchemeNZB, byPub[nodeHex(1)].ReleaseScheme)
		assert.Equal(t, PeernetRecordSourceLocal, byPub[nodeHex(6)].Source)

		got, err = b.recs.BundleOffers(ctx, []string{hexID('a'), hexID('b')}, rel, 10)
		require.NoError(t, err)
		assert.Len(t, got, 4)
		got, err = b.recs.BundleOffers(ctx, []string{hexID('a'), hexID('b')}, rel, 2)
		require.NoError(t, err)
		assert.Len(t, got, 2, "bounded")

		// Expired records are not offers.
		b.setNow(recT0.Add(48 * time.Hour))
		got, err = b.recs.BundleOffers(ctx, []string{hexID('a')}, rel, 10)
		require.NoError(t, err)
		assert.Empty(t, got)

		_, err = b.recs.BundleOffers(ctx, nil, rel, 10)
		require.Error(t, err)
		_, err = b.recs.BundleOffers(ctx, []string{hexID('a')}, "bad", 10)
		require.Error(t, err)
	})
}

func TestPeernetRecords_BundleIDValidation(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		_, err := b.recs.Insert(context.Background(), mkRec(func(r *PeernetRecord) { r.BundleID = "short" }), PeernetRecordLimits{})
		require.ErrorIs(t, err, ErrPeernetRecordInvalid)
		_, err = b.recs.Insert(context.Background(), mkAtt(true, func(r *PeernetRecord) { r.BundleID = bundleHex(1) }), PeernetRecordLimits{})
		require.ErrorIs(t, err, ErrPeernetRecordInvalid, "an attestation never carries a bundle id")
	})
}

// AnyBundleOffer answers the cheap "does this node hold any bundle offer"
// question, and the generation counter moves exactly when a record announcing
// a bundle is stored (what invalidates the cached answer).
func TestPeernetRecords_AnyBundleOfferAndGeneration(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		any, err := b.recs.AnyBundleOffer(ctx)
		require.NoError(t, err)
		assert.False(t, any)

		gen := BundleOfferGeneration()
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ReleaseID, r.PublisherNodeID = relHex(1), nodeHex(1) })) // no bundle
		assert.Equal(t, gen, BundleOfferGeneration(), "a record without a bundle changes nothing")
		any, err = b.recs.AnyBundleOffer(ctx)
		require.NoError(t, err)
		assert.False(t, any)

		insertOK(t, b, mkRec(func(r *PeernetRecord) {
			r.ReleaseID, r.PublisherNodeID, r.BundleID = relHex(2), nodeHex(2), bundleHex(2)
		}))
		assert.Greater(t, BundleOfferGeneration(), gen, "a bundle-announcing record moves the generation")
		any, err = b.recs.AnyBundleOffer(ctx)
		require.NoError(t, err)
		assert.True(t, any)
	})
}
