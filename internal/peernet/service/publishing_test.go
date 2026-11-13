package service

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
)

func level(l int) Settings { return Settings{Enabled: true, OpenLevel: l} }

func TestHealthyFilePublishesReleaseAndAttestation(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)

	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)

	ns := f.openNetStatus(t)
	if ns.Records.Releases.Local != 1 || ns.Records.Attestations.Local != 1 {
		t.Fatalf("records: %+v", ns.Records)
	}
	pub := f.status(t).Publish
	if pub.Published["release"] != 1 || pub.Published["attestation"] != 1 || pub.Dropped != 0 || pub.Errors != 0 {
		t.Fatalf("publish status: %+v", pub)
	}
	// The state store remembers both, so the same verdict is not sent twice.
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	if got := f.status(t).Publish; got.Published["release"] != 1 || got.Published["attestation"] != 1 || got.Skipped["not_due"] != 2 {
		t.Fatalf("repeat: %+v", got)
	}
}

func TestLevelOnePublishesNothing(t *testing.T) {
	f := newPubInstance(t, level(1), nil)
	f.seedFile(t)
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	ns := f.openNetStatus(t)
	if ns.Records.Releases.Local+ns.Records.Attestations.Local != 0 {
		t.Fatalf("level 1 published: %+v", ns.Records)
	}
	if got := f.status(t).Publish; got.Skipped["not_publishing"] != 1 || len(got.Published) != 0 {
		t.Fatalf("publish status: %+v", got)
	}
}

func TestDisabledFeatureBuildsNothing(t *testing.T) {
	f := newPubInstance(t, Settings{}, nil)
	f.seedFile(t)
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice()) // must not panic or block
	if f.svc.currentNode() != nil {
		t.Fatal("a disabled feature has no node")
	}
	if got := f.status(t).Publish; len(got.Published)+len(got.Skipped) != 0 {
		t.Fatalf("publish status: %+v", got)
	}
	ok, err := f.state.ListReleasesPublishedBefore(context.Background(), time.Now().Add(time.Hour), 10)
	if err != nil || len(ok) != 0 {
		t.Fatalf("state rows: %v %v", ok, err)
	}
}

func TestExclusionsAppliedLiveBeforeAnythingIsSigned(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)

	f.excl.set(publish.Exclusions{Categories: []string{"Movies"}})
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	f.excl.set(publish.Exclusions{Indexers: []string{" indexerone "}})
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)

	ns := f.openNetStatus(t)
	if ns.Records.Releases.Local+ns.Records.Attestations.Local != 0 {
		t.Fatalf("excluded release was published: %+v", ns.Records)
	}
	got := f.status(t).Publish
	if got.Skipped["excluded_category"] != 1 || got.Skipped["excluded_indexer"] != 1 {
		t.Fatalf("publish status: %+v", got)
	}

	// Lifting the exclusion takes effect without a restart.
	f.excl.set(publish.Exclusions{})
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	if f.openNetStatus(t).Records.Releases.Local != 1 {
		t.Fatal("not published after the exclusion was lifted")
	}
}

func TestNoRecognisedBackboneMeansReleaseRecordOnly(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.bb.known = nil // list not available: fail closed
	f.seedFile(t)
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	ns := f.openNetStatus(t)
	if ns.Records.Releases.Local != 1 || ns.Records.Attestations.Local != 0 {
		t.Fatalf("records: %+v", ns.Records)
	}
	if got := f.status(t).Publish.Skipped["no_recognised_backbone"]; got != 1 {
		t.Fatalf("skipped: %+v", f.status(t).Publish)
	}
}

func TestUnknownPatchStatusIsNotAttested(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)

	n := healthyNotice()
	n.PatchKnown = false // the check began before the observer was installed
	f.svc.NotifyHealthy(n)
	f.drain(t)
	ns := f.openNetStatus(t)
	if ns.Records.Releases.Local != 1 || ns.Records.Attestations.Local != 0 {
		t.Fatalf("records: %+v", ns.Records)
	}
	if got := f.status(t).Publish.Skipped["patch_status_unknown"]; got != 1 {
		t.Fatalf("skipped: %+v", f.status(t).Publish)
	}
}

func TestPatchedAndKnownHoleFilesAreNotAttested(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)

	n := healthyNotice()
	n.PatchedByPAR2 = true
	f.svc.NotifyHealthy(n)
	f.drain(t)
	ns := f.openNetStatus(t)
	if ns.Records.Releases.Local != 1 || ns.Records.Attestations.Local != 0 {
		t.Fatalf("patched file: %+v", ns.Records)
	}

	n = healthyNotice()
	n.KnownHoles = true
	f.svc.NotifyHealthy(n)
	f.drain(t)
	ns = f.openNetStatus(t)
	if ns.Records.Attestations.Local != 0 {
		t.Fatalf("known holes were attested: %+v", ns.Records)
	}
	if got := f.status(t).Publish.Skipped["known_holes"]; got != 1 {
		t.Fatalf("skipped: %+v", f.status(t).Publish)
	}
}

func TestUnindexedOrIncompleteFilesAreSkippedWithAReason(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.start(t)

	f.svc.NotifyHealthy(healthyNotice()) // not in the content index
	f.drain(t)
	if got := f.status(t).Publish.Skipped["no_release_id"]; got != 1 {
		t.Fatalf("skipped: %+v", f.status(t).Publish)
	}

	f.seedFile(t, func(s *seed) { s.release = "" })
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	if got := f.status(t).Publish.Skipped["no_release_id"]; got != 2 {
		t.Fatalf("skipped: %+v", f.status(t).Publish)
	}

	n := healthyNotice()
	n.TotalSegments = 0
	f.svc.NotifyHealthy(n)
	f.drain(t)
	if got := f.status(t).Publish.Skipped["no_segments"]; got != 1 {
		t.Fatalf("skipped: %+v", f.status(t).Publish)
	}
}

func TestFullQueueDropsAndCounts(t *testing.T) {
	hold := make(chan struct{})
	f := newPubInstance(t, level(2), func(d *Deps) {
		d.publishQueueSize = 2
		d.beforePublish = func() { <-hold }
	})
	f.seedFile(t)
	f.start(t)
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 20 {
			f.svc.NotifyHealthy(healthyNotice())
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("NotifyHealthy blocked on a full queue")
	}
	if got := f.status(t).Publish.Dropped; got < 15 {
		t.Fatalf("dropped = %d, want most of 20 with a queue of 2", got)
	}
	close(hold)
	f.drain(t)
}

func TestReannounceRefreshesOldReleaseRecordsOnly(t *testing.T) {
	lh := &fakeLocalHealth{statuses: map[string]string{pubPath: "healthy"}}
	f := newPubInstance(t, level(2), func(d *Deps) { d.Health = lh })
	f.seedFile(t)
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	awaitFirstUpkeep(t, f)

	n := f.node(t)
	nw := openID.String()
	// Age the release record past the refresh interval; leave the attestation.
	old := time.Now().Add(-publish.DefaultRecordRefresh - time.Hour)
	if err := f.state.Put(context.Background(), database.PeernetPublishState{NetworkID: nw, Kind: database.PeernetRecordKindRelease, ReleaseID: pubRelease, PublishedAt: old}); err != nil {
		t.Fatal(err)
	}
	attBefore, _, _ := f.state.Get(context.Background(), nw, database.PeernetRecordKindAttestation, pubRelease, pubFile)

	n.pipe.reannounce(context.Background(), 10)

	rel, ok, _ := f.state.Get(context.Background(), nw, database.PeernetRecordKindRelease, pubRelease, "")
	if !ok || !rel.PublishedAt.After(old.Add(time.Hour)) {
		t.Fatalf("release state not refreshed: %+v", rel)
	}
	attAfter, _, _ := f.state.Get(context.Background(), nw, database.PeernetRecordKindAttestation, pubRelease, pubFile)
	if !attAfter.PublishedAt.Equal(attBefore.PublishedAt) || !attAfter.CheckedAt.Equal(attBefore.CheckedAt) {
		t.Fatal("an attestation must never be refreshed without a new check")
	}
	if got := f.status(t).Publish.Reannounced; got != 1 {
		t.Fatalf("reannounced = %d", got)
	}

	// Nothing is due now.
	n.pipe.reannounce(context.Background(), 10)
	if got := f.status(t).Publish.Reannounced; got != 1 {
		t.Fatalf("reannounced twice: %d", got)
	}
}

func TestReannounceSkipsReleasesThatLeftTheLibrary(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.start(t)
	n := f.node(t)
	old := time.Now().Add(-publish.DefaultRecordRefresh - time.Hour)
	if err := f.state.Put(context.Background(), database.PeernetPublishState{NetworkID: openID.String(), Kind: database.PeernetRecordKindRelease, ReleaseID: pubRelease, PublishedAt: old}); err != nil {
		t.Fatal(err)
	}
	n.pipe.reannounce(context.Background(), 10)
	if got := f.status(t).Publish; got.Reannounced != 0 || f.openNetStatus(t).Records.Releases.Local != 0 {
		t.Fatalf("a release no longer indexed was re-announced: %+v", got)
	}
}

func TestUpkeepPrunesStateOfReleasesNoLongerIndexed(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	n := f.node(t)

	n.maintain(context.Background())
	if _, ok, _ := f.state.Get(context.Background(), openID.String(), database.PeernetRecordKindRelease, pubRelease, ""); !ok {
		t.Fatal("state of an indexed release must stay")
	}
	if err := f.content.Delete(context.Background(), pubPath); err != nil {
		t.Fatal(err)
	}
	// The scan runs only on the orphan cadence, never on an ordinary pass.
	n.maintainPublishing(context.Background(), false)
	if _, ok, _ := f.state.Get(context.Background(), openID.String(), database.PeernetRecordKindRelease, pubRelease, ""); !ok {
		t.Fatal("the prune scan ran on an ordinary upkeep pass")
	}
	n.maintainPublishing(context.Background(), true)
	if _, ok, _ := f.state.Get(context.Background(), openID.String(), database.PeernetRecordKindRelease, pubRelease, ""); ok {
		t.Fatal("state of a release that left the content index was kept")
	}
}

func TestOrphanNetworkStateIsRemovedByUpkeep(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.start(t)
	ghost := "c" + pubRelease[1:]
	if err := f.state.Put(context.Background(), database.PeernetPublishState{NetworkID: ghost, Kind: database.PeernetRecordKindRelease, ReleaseID: pubRelease, PublishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	f.node(t).maintainPublishing(context.Background(), true)
	if _, ok, _ := f.state.Get(context.Background(), ghost, database.PeernetRecordKindRelease, pubRelease, ""); ok {
		t.Fatal("state of a network that no longer exists was kept")
	}
}

func TestDeletingANetworkRemovesItsPublishState(t *testing.T) {
	f := newPubInstance(t, Settings{Enabled: true, OpenLevel: 2, AcceptIncoming: true, ListenPort: 0}, nil)
	f.start(t)
	st, err := f.svc.CreateNetwork(context.Background(), "mine")
	if err != nil {
		t.Fatal(err)
	}
	nw, _ := parseID(st.ID)
	if err := f.state.Put(context.Background(), database.PeernetPublishState{NetworkID: st.ID, Kind: database.PeernetRecordKindRelease, ReleaseID: pubRelease, PublishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.DeleteNetwork(context.Background(), nw); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := f.state.Get(context.Background(), st.ID, database.PeernetRecordKindRelease, pubRelease, ""); ok {
		t.Fatal("publish state survived the network")
	}
}

func TestBackboneListRefreshedOnlyWhilePublishing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		set     Settings
		refresh bool
	}{
		{"disabled", Settings{}, false},
		{"level 1", level(1), false},
		{"level 2", level(2), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPubInstance(t, tc.set, func(d *Deps) { d.backboneEvery = 10 * time.Millisecond })
			f.start(t)
			if tc.refresh {
				eventually(t, "the backbone list to be refreshed", func() bool { return f.bb.refreshes.Load() > 0 })
				return
			}
			// Absence can only be shown by waiting: many intervals' worth.
			time.Sleep(150 * time.Millisecond)
			if got := f.bb.refreshes.Load(); got != 0 {
				t.Fatalf("refreshed %d times although nothing is published", got)
			}
		})
	}
}

// A check peer advice lowered to its floor is weaker than the configured one:
// nodes must not cite each other's floor-level checks, so it is not attested.
// The release record may still go out.
func TestPeerReducedChecksAreNotAttested(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)

	n := healthyNotice()
	n.ReducedByPeerAdvice = true
	f.svc.NotifyHealthy(n)
	f.drain(t)
	ns := f.openNetStatus(t)
	if ns.Records.Releases.Local != 1 || ns.Records.Attestations.Local != 0 {
		t.Fatalf("reduced check: %+v", ns.Records)
	}
	sk := f.status(t).Publish.Skipped
	if sk["reduced_by_peer_advice"] != 1 || sk["patched_by_par2"] != 0 {
		t.Fatalf("skipped: %+v", sk)
	}
}

func TestExclusionAddedLaterRetractsOwnRecordsAndState(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	if ns := f.openNetStatus(t); ns.Records.Releases.Local != 1 || ns.Records.Attestations.Local != 1 {
		t.Fatalf("setup: %+v", ns.Records)
	}

	// The user now excludes the category the release was published under.
	f.excl.set(publish.Exclusions{Categories: []string{" MOVIES "}})
	f.node(t).maintainPublishing(context.Background(), true)

	ns := f.openNetStatus(t)
	if ns.Records.Releases.Local != 0 || ns.Records.Attestations.Local != 0 {
		t.Fatalf("own records of an excluded release survived: %+v", ns.Records)
	}
	rows, err := f.state.ListReleasesPublishedBefore(context.Background(), time.Now().Add(time.Hour), 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("publish state survived: %v %v", rows, err)
	}
}
