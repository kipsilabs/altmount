package service

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
)

// A file that became healthy while the network was below level 2 was skipped
// (not_publishing). When the level rises, a bounded catch-up pass publishes the
// release record of indexed, locally healthy releases that were never
// published. It never invents freshness: no attestation is published without a
// new real check.
func TestCatchUpPublishesHealthyIndexedReleasesNeverPublished(t *testing.T) {
	lh := &fakeLocalHealth{statuses: map[string]string{pubPath: "healthy"}}
	f := newPubInstance(t, level(2), func(d *Deps) { d.Health = lh })
	f.seedFile(t)
	f.start(t)
	awaitFirstUpkeep(t, f)
	ctx := context.Background()
	nw := openID.String()

	// No health notice was ever sent for this file: whatever publishes it is the
	// catch-up pass of the first upkeep (or this explicit one).
	f.node(t).pipe.catchUp(ctx, 10)

	if _, ok, _ := f.state.Get(ctx, nw, database.PeernetRecordKindRelease, pubRelease, ""); !ok {
		t.Fatal("the healthy indexed release must be published by the catch-up pass")
	}
	if _, ok, _ := f.state.Get(ctx, nw, database.PeernetRecordKindAttestation, pubRelease, pubFile); ok {
		t.Fatal("an attestation is never published without a new check")
	}
}

func TestCatchUpDoesNothingWhenNotLocallyHealthy(t *testing.T) {
	lh := &fakeLocalHealth{statuses: map[string]string{pubPath: "corrupted"}}
	f := newPubInstance(t, level(2), func(d *Deps) { d.Health = lh })
	f.seedFile(t)
	f.start(t)
	awaitFirstUpkeep(t, f)
	ctx := context.Background()
	f.node(t).pipe.catchUp(ctx, 10)
	if _, ok, _ := f.state.Get(ctx, openID.String(), database.PeernetRecordKindRelease, pubRelease, ""); ok {
		t.Fatal("a release whose file is not healthy must not be published")
	}
	if f.status(t).Publish.Skipped["not_healthy"] == 0 {
		t.Fatalf("expected a not_healthy skip: %+v", f.status(t).Publish.Skipped)
	}
}

func TestLevelsRoseTriggersOnlyWhenANetworkStartsPublishing(t *testing.T) {
	var nets []publish.Network
	p := &pipeline{cfg: pipelineConfig{Networks: func() []publish.Network { return nets }}}
	if p.levelsRose() {
		t.Fatal("the first pass after start is not a level change")
	}
	nets = []publish.Network{{ID: openID, Level: 2}}
	if !p.levelsRose() {
		t.Fatal("a network that started publishing must trigger a catch-up")
	}
	if p.levelsRose() {
		t.Fatal("no change, no catch-up")
	}
	nets = []publish.Network{{ID: openID, Level: 3}}
	if !p.levelsRose() {
		t.Fatal("a rise from 2 to 3 triggers one")
	}
	nets = []publish.Network{{ID: openID, Level: 2}}
	if p.levelsRose() {
		t.Fatal("a drop does not")
	}
}
