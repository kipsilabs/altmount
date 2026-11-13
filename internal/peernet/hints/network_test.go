package hints

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func (e *env) health(t *testing.T, ids ...string) map[string]NetworkHealth {
	t.Helper()
	out, err := e.a.NetworkHealthy(context.Background(), ids...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestNetworkHealthyOpenNeedsCorroboration(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	h := e.health(t, release)[release]
	if h.Healthy || h.HealthyPublishers != 1 {
		t.Fatalf("%+v", h)
	}
	e.seed(openNet, 2, nil)
	h = e.health(t, release)[release]
	if !h.Healthy || h.HealthyPublishers != 2 || h.Newest.IsZero() {
		t.Fatalf("%+v", h)
	}
}

func TestNetworkHealthyDoesNotCountYoungOwnOrStale(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, nil)
	e.peers.first[node(2)] = e.now.Add(-time.Hour)
	e.seed(openNet, 3, func(a *database.PeernetAttestation) { a.Source = database.PeernetRecordSourceLocal })
	e.seed(openNet, 4, func(a *database.PeernetAttestation) { a.CheckedAt = e.now.Add(-8 * 24 * time.Hour) })
	e.store.add(openNet, release, database.PeernetAttestation{
		PublisherNodeID: self, FileID: file, Healthy: true, CheckedAt: e.now, CreatedAt: e.now, Source: database.PeernetRecordSourcePeer,
	})
	e.peers.first[self] = e.now.Add(-30 * 24 * time.Hour)
	h := e.health(t, release)[release]
	if h.Healthy || h.HealthyPublishers != 1 {
		t.Fatalf("%+v", h)
	}
}

func TestNetworkHealthyDuplicatePublisherCountsOnce(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	e.seed(openNet, 1, func(a *database.PeernetAttestation) { a.FileID = otherFil })
	h := e.health(t, release)[release]
	if h.Healthy || h.HealthyPublishers != 1 {
		t.Fatalf("%+v", h)
	}
}

func TestNetworkHealthyConflictIsConservative(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, nil)
	e.seed(openNet, 3, unhealthy)
	e.seed(openNet, 4, unhealthy)
	h := e.health(t, release)[release]
	if h.Healthy || h.UnhealthyPublishers != 2 || h.HealthyPublishers != 2 {
		t.Fatalf("%+v", h)
	}
	// A publisher that attests both ways is unhealthy, not healthy.
	e2 := newEnv(t, openRecv)
	e2.seed(openNet, 1, nil)
	e2.seed(openNet, 2, nil)
	e2.seed(openNet, 2, func(a *database.PeernetAttestation) { a.FileID = otherFil; a.Healthy = false })
	if h := e2.health(t, release)[release]; h.Healthy {
		t.Fatalf("%+v", h)
	}
	// An unhealthy claim from a young identity cannot suppress the label.
	e3 := newEnv(t, openRecv)
	e3.seed(openNet, 1, nil)
	e3.seed(openNet, 2, nil)
	e3.seed(openNet, 3, unhealthy)
	e3.peers.first[node(3)] = e3.now.Add(-time.Hour)
	if h := e3.health(t, release)[release]; !h.Healthy || h.UnhealthyPublishers != 0 {
		t.Fatalf("%+v", h)
	}
}

func TestNetworkHealthyPrivateThresholdOne(t *testing.T) {
	e := newEnv(t, privRecv)
	e.seed(privNet, 1, nil)
	e.peers.first[node(1)] = e.now // young members are fine
	if h := e.health(t, release)[release]; !h.Healthy {
		t.Fatalf("%+v", h)
	}
}

func TestNetworkHealthyMixedNetworks(t *testing.T) {
	e := newEnv(t, openRecv, privRecv)
	e.seed(openNet, 1, nil) // below the open threshold
	if h := e.health(t, release)[release]; h.Healthy {
		t.Fatalf("a single open attestation must not make a release healthy: %+v", h)
	}
	e.seed(privNet, 2, nil)
	if h := e.health(t, release)[release]; !h.Healthy {
		t.Fatalf("%+v", h)
	}
	e.seed(privNet, 3, unhealthy)
	if h := e.health(t, release)[release]; h.Healthy {
		t.Fatalf("unhealthy in one network must block: %+v", h)
	}
}

func TestNetworkHealthyAbsentAndPrefilter(t *testing.T) {
	e := newEnv(t, openRecv)
	out := e.health(t, release, hexID(9))
	if len(out) != 2 || out[release].Healthy || out[hexID(9)].Healthy {
		t.Fatalf("%+v", out)
	}
	if e.store.calls["att"] != 0 {
		t.Fatal("full attestation scan for a release with no evidence")
	}
	if len(e.health(t)) != 0 {
		t.Fatal("empty input must give an empty result")
	}
}

func TestNetworkHealthyInputBounds(t *testing.T) {
	e := newEnv(t, openRecv)
	if _, err := e.a.NetworkHealthy(context.Background(), "nope"); err == nil {
		t.Fatal("expected invalid id error")
	}
	var many []string
	for i := 0; i <= maxReleaseIDs; i++ {
		many = append(many, hexID(byte(i)))
	}
	if _, err := e.a.NetworkHealthy(context.Background(), many...); !errors.Is(err, ErrTooManyIDs) {
		t.Fatalf("err = %v", err)
	}
	// Duplicates collapse before the bound is applied.
	dup := make([]string, maxReleaseIDs+50)
	for i := range dup {
		dup[i] = release
	}
	if out, err := e.a.NetworkHealthy(context.Background(), dup...); err != nil || len(out) != 1 {
		t.Fatalf("%v %v", out, err)
	}
}

func TestNetworkHealthyNoReceivingNetworks(t *testing.T) {
	e := newEnv(t, Network{ID: openNet, Level: 0})
	out := e.health(t, release)
	if out[release].Healthy || e.store.calls["sum"] != 0 {
		t.Fatalf("%+v calls %v", out, e.store.calls)
	}
}

func TestNetworkHealthyErrors(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, nil)
	e.store.err = errors.New("db")
	if _, err := e.a.NetworkHealthy(context.Background(), release); err == nil {
		t.Fatal("store error swallowed")
	}
	e.store.err = nil
	e.peers.err = errors.New("peers")
	if _, err := e.a.NetworkHealthy(context.Background(), release); err == nil {
		t.Fatal("peer error swallowed")
	}
}

func TestNetworkHealthyBoundsPublisherLookups(t *testing.T) {
	e := newEnv(t, openRecv)
	for i := 1; i <= 300; i++ {
		e.seed(openNet, i, nil)
		e.peers.first[node(i)] = e.now // all young
	}
	e.health(t, release)
	if e.peers.calls > maxPublisherLookups {
		t.Fatalf("%d first-seen lookups", e.peers.calls)
	}
}

func cand(rel string, nw netid.NetworkID, publisher int, title string, age time.Duration, src int) database.PeernetCandidate {
	return database.PeernetCandidate{
		ReleaseID: rel, NetworkID: nw.String(), PublisherNodeID: node(publisher), IMDbID: "tt1", Title: title,
		TotalSize: 100, CreatedAt: t0.Add(-age), Source: src,
	}
}

func TestCandidates(t *testing.T) {
	e := newEnv(t, openRecv, privRecv, Network{ID: netid.NetworkID{0x77}, Level: 0})
	r2, r3, r4 := hexID(20), hexID(21), hexID(22)
	e.store.cands = []database.PeernetCandidate{
		cand(release, openNet, 1, "Movie 1080p", time.Hour, 1),
		cand(release, privNet, 2, "Movie 1080p copy", 2*time.Hour, 1), // duplicate release, other network
		cand(r2, openNet, 3, "Movie 2160p\x00/evil\u202e", 3*time.Hour, 1),
		cand(r3, openNet, 4, "Other", time.Minute, 1),
		cand(r4, openNet, 5, "/", time.Minute, 1), // title empty after cleaning: dropped
		{ReleaseID: "not-hex", NetworkID: openNet.String(), PublisherNodeID: node(1), Title: "bad", CreatedAt: t0},
		{ReleaseID: hexID(23), NetworkID: "zz", PublisherNodeID: node(1), Title: "bad net", CreatedAt: t0},
	}
	// r2 healthy on the open network, release unhealthy-free but uncorroborated.
	e.seed(openNet, 11, nil)
	e.store.add(openNet, r2, database.PeernetAttestation{PublisherNodeID: node(11), FileID: file, Healthy: true, CheckedAt: t0.Add(-time.Hour), CreatedAt: t0, Source: 1, SegmentsChecked: 100, TotalSegments: 1000})
	e.store.add(openNet, r2, database.PeernetAttestation{PublisherNodeID: node(12), FileID: file, Healthy: true, CheckedAt: t0.Add(-time.Hour), CreatedAt: t0, Source: 1, SegmentsChecked: 100, TotalSegments: 1000})
	e.peers.first[node(12)] = t0.Add(-10 * 24 * time.Hour)

	season := 1
	got, err := e.a.Candidates(context.Background(), "tt1", &season, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("%d candidates: %+v", len(got), got)
	}
	if got[0].ReleaseID != r2 || !got[0].Health.Healthy {
		t.Fatalf("healthy release must come first: %+v", got[0])
	}
	if got[1].ReleaseID != r3 || got[2].ReleaseID != release {
		t.Fatalf("order by recency after health: %s %s", got[1].ReleaseID, got[2].ReleaseID)
	}
	if got[0].Title != "Movie 2160p evil" {
		t.Fatalf("title not cleaned: %q", got[0].Title)
	}
	if len(got[2].Networks) != 2 || got[2].Networks[0].String() > got[2].Networks[1].String() {
		t.Fatalf("networks %v", got[2].Networks)
	}
	if got[2].CreatedAt != t0.Add(-time.Hour) {
		t.Fatalf("newest record must define the entry: %v", got[2].CreatedAt)
	}
	if len(e.store.lastNets) != 2 || e.store.lastSeason == nil || *e.store.lastSeason != 1 {
		t.Fatalf("store args: %v %v", e.store.lastNets, e.store.lastSeason)
	}
}

func TestCandidatesOwnFlag(t *testing.T) {
	e := newEnv(t, openRecv)
	e.store.cands = []database.PeernetCandidate{
		cand(release, openNet, 1, "Mine", time.Hour, database.PeernetRecordSourceLocal),
		cand(hexID(5), openNet, 2, "Relayed own", time.Hour, 1),
		cand(hexID(6), openNet, 3, "Theirs", time.Hour, 1),
	}
	e.store.cands[1].PublisherNodeID = self
	got, err := e.a.Candidates(context.Background(), "tt1", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	own := map[string]bool{}
	for _, c := range got {
		own[c.Title] = c.Own
	}
	if !own["Mine"] || !own["Relayed own"] || own["Theirs"] {
		t.Fatalf("%v", own)
	}
}

func TestCandidatesBoundsAndValidation(t *testing.T) {
	e := newEnv(t, openRecv)
	a, err := New(Config{Store: e.store, Peers: e.peers, Budget: e.budget, CandidateLimit: 2,
		Networks: func() []Network { return e.nets }, Now: func() time.Time { return e.now }})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		e.store.cands = append(e.store.cands, cand(hexID(byte(30+i)), openNet, i, "T", time.Duration(i)*time.Hour, 1))
	}
	got, err := a.Candidates(context.Background(), "tt1", nil, nil)
	if err != nil || len(got) != 2 || e.store.lastLimit != 2 {
		t.Fatalf("len %d limit %d err %v", len(got), e.store.lastLimit, err)
	}
	if got[0].CreatedAt.Before(got[1].CreatedAt) {
		t.Fatal("not sorted newest first")
	}
	for _, bad := range []string{"", "x1", "tt", strings.Repeat("1", 30)} {
		if _, err := a.Candidates(context.Background(), bad, nil, nil); err == nil {
			t.Errorf("imdb %q accepted", bad)
		}
	}
	big, _ := New(Config{Store: e.store, Peers: e.peers, Budget: e.budget, CandidateLimit: 5000,
		Networks: func() []Network { return e.nets }})
	if _, err := big.Candidates(context.Background(), "tt1", nil, nil); err != nil || e.store.lastLimit != maxCandidates {
		t.Fatalf("limit %d err %v", e.store.lastLimit, err)
	}
}

func TestCandidatesNoReceivingNetworks(t *testing.T) {
	e := newEnv(t, Network{ID: openNet, Level: 0})
	got, err := e.a.Candidates(context.Background(), "tt1", nil, nil)
	if err != nil || len(got) != 0 || e.store.calls["cand"] != 0 {
		t.Fatalf("%v %v %v", got, err, e.store.calls)
	}
}

func TestCandidatesStoreError(t *testing.T) {
	e := newEnv(t, openRecv)
	e.store.err = errors.New("db")
	if _, err := e.a.Candidates(context.Background(), "tt1", nil, nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestCandidatesDeterministicOrder(t *testing.T) {
	e := newEnv(t, openRecv)
	e.store.cands = []database.PeernetCandidate{
		cand(hexID(2), openNet, 1, "B", time.Hour, 1),
		cand(hexID(1), openNet, 2, "A", time.Hour, 1),
		cand(hexID(3), openNet, 3, "C", time.Hour, 1),
	}
	first, _ := e.a.Candidates(context.Background(), "tt1", nil, nil)
	for i := 0; i < 5; i++ {
		again, _ := e.a.Candidates(context.Background(), "tt1", nil, nil)
		for j := range first {
			if first[j].ReleaseID != again[j].ReleaseID {
				t.Fatal("order not deterministic")
			}
		}
	}
	if first[0].ReleaseID != hexID(1) {
		t.Fatalf("ties must break on release id: %s", first[0].ReleaseID)
	}
}
