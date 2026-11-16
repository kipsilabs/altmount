package hints

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

var (
	// The same file as published by an instance that kept the NZB ("nzb"
	// release ID) and by one that only knows the files ("files" release ID).
	relNZB   = hexID(0x11)
	relFiles = hexID(0x12)
	fileA    = hexID(0x21)
	fileB    = hexID(0x22)
	fileC    = hexID(0x23)
)

// seedFileEv adds node i's attestation of fileID under release rel.
func (e *env) seedFileEv(nw netid.NetworkID, rel, fileID string, i int, mut func(*database.PeernetAttestation)) {
	a := database.PeernetAttestation{
		PublisherNodeID: node(i), FileID: fileID, Healthy: true, Backbones: []string{"omicron"},
		CheckedAt: e.now.Add(-time.Hour), CreatedAt: e.now.Add(-time.Hour), Source: database.PeernetRecordSourcePeer,
		SegmentsChecked: 100, TotalSegments: 1000,
	}
	if mut != nil {
		mut(&a)
	}
	e.store.add(nw, rel, a)
	e.peers.first[node(i)] = e.now.Add(-7 * 24 * time.Hour)
}

func (e *env) healthFiles(t *testing.T, releases map[string][]string) map[string]NetworkHealth {
	t.Helper()
	out, err := e.a.NetworkHealthyFiles(context.Background(), releases)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Evidence about a file applies to every release the file is part of.
func TestHealthAdviceMatchesFileAcrossReleaseIDs(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 3
	e.seedFileEv(openNet, relNZB, fileA, 1, nil)
	e.seedFileEv(openNet, relNZB, fileA, 2, nil)
	e.seedFileEv(openNet, relFiles, fileA, 3, nil) // published by a node that only has the file set
	for _, rel := range []string{relNZB, relFiles, hexID(0x13), ""} {
		adv, err := e.a.HealthAdvice(context.Background(), rel, fileA, []string{"omicron"}, e.now)
		if err != nil {
			t.Fatalf("release %q: %v", rel, err)
		}
		if adv.Kind != AdviceReduceSampleToFloor {
			t.Fatalf("release %q: %+v (all three publishers attest the same file)", rel, adv)
		}
	}
	if len(e.store.lastFiles) != 1 || e.store.lastFiles[0] != fileA {
		t.Fatalf("lookup was by %v, want the file ID only", e.store.lastFiles)
	}
}

// A publisher that attests one file under many release IDs is still one
// publisher: corroboration is per distinct publisher, never per record.
func TestFileEvidencePublisherCountsOncePerFile(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 2
	for i := range 6 {
		e.seedFileEv(openNet, hexID(byte(0x30+i)), fileA, 1, nil)
	}
	if adv, _ := e.a.HealthAdvice(context.Background(), relNZB, fileA, []string{"omicron"}, e.now); adv.Kind != AdviceNone {
		t.Fatalf("one publisher with six records corroborated itself: %+v", adv)
	}
	e.seedFileEv(openNet, relFiles, fileA, 2, nil)
	if adv, _ := e.a.HealthAdvice(context.Background(), relNZB, fileA, []string{"omicron"}, e.now); adv.Kind != AdviceReduceSampleToFloor {
		t.Fatalf("two publishers: %+v", adv)
	}
}

// Every existing rule still applies to evidence found by file: freshness,
// coverage, backbone subset, own records, known publishers, network prefixes.
func TestFileEvidenceKeepsEveryRule(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*env)
	}{
		{"stale", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, func(a *database.PeernetAttestation) { a.CheckedAt = e.now.Add(-8 * 24 * time.Hour) })
		}},
		{"future-dated", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, func(a *database.PeernetAttestation) { a.CheckedAt = e.now.Add(time.Hour) })
		}},
		{"coverage too low", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, func(a *database.PeernetAttestation) { a.SegmentsChecked = 5 })
		}},
		{"unknown coverage", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, func(a *database.PeernetAttestation) { a.SegmentsChecked, a.TotalSegments = 0, 0 })
		}},
		{"backbone not a subset", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, func(a *database.PeernetAttestation) { a.Backbones = []string{"omicron", "zeta"} })
		}},
		{"no backbone", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, func(a *database.PeernetAttestation) { a.Backbones = nil })
		}},
		{"own record", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, func(a *database.PeernetAttestation) { a.Source = database.PeernetRecordSourceLocal })
		}},
		{"own node id relayed", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, func(a *database.PeernetAttestation) { a.PublisherNodeID = self })
		}},
		{"young identity", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, nil)
			e.peers.first[node(1)] = e.now.Add(-time.Hour)
		}},
		{"same network prefix", func(e *env) {
			e.seedFileEv(openNet, relFiles, fileA, 1, nil)
			e.peers.prefix[node(1)] = "10.1.1"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, openRecv)
			e.peers.prefix = map[string]string{}
			e.minC = 1
			tc.mut(e)
			if tc.name == "same network prefix" {
				e.seedFileEv(openNet, relNZB, fileA, 2, nil)
				e.peers.prefix[node(2)] = "10.1.1"
				e.minC = 2
			}
			if adv, _ := e.a.HealthAdvice(context.Background(), relNZB, fileA, []string{"omicron"}, e.now); adv.Kind != AdviceNone {
				t.Fatalf("%s: counted as evidence: %+v", tc.name, adv)
			}
		})
	}
}

// Another file's evidence is no evidence for this file, whatever the release.
func TestFileEvidenceIsPerFile(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 1
	e.seedFileEv(openNet, relNZB, fileB, 1, nil)
	if adv, _ := e.a.HealthAdvice(context.Background(), relNZB, fileA, []string{"omicron"}, e.now); adv.Kind != AdviceNone {
		t.Fatalf("%+v", adv)
	}
}

func TestUnhealthyFileEvidenceUnderAnotherReleaseMovesACheckOnce(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 2
	e.seedFileEv(openNet, relFiles, fileA, 1, unhealthy)
	e.seedFileEv(openNet, relNZB, fileA, 2, unhealthy)
	adv, _ := e.a.HealthAdvice(context.Background(), relNZB, fileA, []string{"omicron"}, e.now)
	if adv.Kind != AdviceRecheckSooner {
		t.Fatalf("%+v", adv)
	}
	if !e.a.SpendRecheck(relNZB, fileA, adv.EvidenceAt, e.now) {
		t.Fatal("budget refused")
	}
	// The same evidence reached through another release ID is the same
	// evidence: it must not be paid for a second time.
	again, _ := e.a.HealthAdvice(context.Background(), relFiles, fileA, []string{"omicron"}, e.now)
	if again.Kind != AdviceNone {
		t.Fatalf("evidence acted on twice, once per release ID: %+v", again)
	}
	if e.a.SpendRecheck(relFiles, fileA, adv.EvidenceAt, e.now) {
		t.Fatal("spent a second token for one piece of evidence")
	}
}

func TestNetworkHealthyFilesNeedsEveryFile(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 2
	for _, f := range []string{fileA, fileB} {
		e.seedFileEv(openNet, relNZB, f, 1, nil)
		e.seedFileEv(openNet, relNZB, f, 2, nil)
	}
	rel := map[string][]string{relNZB: {fileA, fileB}}
	if h := e.healthFiles(t, rel)[relNZB]; !h.Healthy || h.HealthyPublishers != 2 {
		t.Fatalf("both files corroborated: %+v", h)
	}
	// A release with a third file nobody attested is not labelled.
	if h := e.healthFiles(t, map[string][]string{relNZB: {fileA, fileB, fileC}})[relNZB]; h.Healthy {
		t.Fatalf("unattested file: %+v", h)
	}
	// Evidence for the third file under another release ID counts.
	e.seedFileEv(openNet, relFiles, fileC, 1, nil)
	e.seedFileEv(openNet, relFiles, fileC, 2, nil)
	if h := e.healthFiles(t, map[string][]string{relNZB: {fileA, fileB, fileC}})[relNZB]; !h.Healthy {
		t.Fatalf("evidence under another release ID: %+v", h)
	}
}

func TestNetworkHealthyFilesPublisherDoesNotCorroborateViaSecondFile(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 2
	// Publisher 1 attests both files, publisher 2 only the first: the second
	// file has one publisher, which is not corroboration.
	e.seedFileEv(openNet, relNZB, fileA, 1, nil)
	e.seedFileEv(openNet, relNZB, fileB, 1, nil)
	e.seedFileEv(openNet, relNZB, fileA, 2, nil)
	if h := e.healthFiles(t, map[string][]string{relNZB: {fileA, fileB}})[relNZB]; h.Healthy {
		t.Fatalf("%+v", h)
	}
}

func TestNetworkHealthyFilesUnhealthyFileVetoes(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 2
	for _, f := range []string{fileA, fileB} {
		e.seedFileEv(openNet, relNZB, f, 1, nil)
		e.seedFileEv(openNet, relNZB, f, 2, nil)
	}
	e.seedFileEv(openNet, relFiles, fileB, 3, unhealthy)
	if h := e.healthFiles(t, map[string][]string{relNZB: {fileA, fileB}})[relNZB]; !h.Healthy {
		t.Fatalf("one unhealthy publisher is below the threshold and must not veto: %+v", h)
	}
	e.seedFileEv(openNet, relFiles, fileB, 4, unhealthy)
	if h := e.healthFiles(t, map[string][]string{relNZB: {fileA, fileB}})[relNZB]; h.Healthy || h.UnhealthyPublishers != 2 {
		t.Fatalf("unhealthy evidence at the threshold on one file: %+v", h)
	}
}

func TestNetworkHealthyFilesCoverageAndFreshnessPerFile(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 1
	e.seedFileEv(openNet, relNZB, fileA, 1, nil)
	e.seedFileEv(openNet, relNZB, fileB, 1, func(a *database.PeernetAttestation) { a.SegmentsChecked = 3 })
	if h := e.healthFiles(t, map[string][]string{relNZB: {fileA, fileB}})[relNZB]; h.Healthy {
		t.Fatalf("insufficient coverage on one file: %+v", h)
	}
	e2 := newEnv(t, openRecv)
	e2.minC = 1
	e2.seedFileEv(openNet, relNZB, fileA, 1, func(a *database.PeernetAttestation) { a.CheckedAt = e2.now.Add(-8 * 24 * time.Hour) })
	if h := e2.healthFiles(t, map[string][]string{relNZB: {fileA}})[relNZB]; h.Healthy {
		t.Fatalf("stale: %+v", h)
	}
}

// The release label never costs more than one attestation lookup per network
// for the whole batch, and bounds both the files per release and in total.
func TestNetworkHealthyFilesIsOneBoundedBatch(t *testing.T) {
	e := newEnv(t, openRecv, privRecv)
	e.minC = 1
	releases := map[string][]string{}
	for i := range 100 {
		rel, f := hexID(byte(0x40+i%100)), hexID(byte(0x80+i%100))
		releases[rel] = []string{f}
		e.seedFileEv(openNet, rel, f, 1, nil)
	}
	out := e.healthFiles(t, releases)
	if len(out) != 100 {
		t.Fatalf("every requested release is present: %d", len(out))
	}
	if e.store.calls["att"] != 2 {
		t.Fatalf("%d attestation lookups for two networks, want 2", e.store.calls["att"])
	}
	healthy := 0
	for _, h := range out {
		if h.Healthy {
			healthy++
		}
	}
	if healthy != 100 {
		t.Fatalf("%d healthy", healthy)
	}

	var big []string
	for i := range MaxLabelFilesPerRelease + 1 {
		big = append(big, hexID(byte(0xa0+i)))
	}
	for _, f := range big {
		e.seedFileEv(openNet, hexID(0x90), f, 1, nil)
	}
	if h := e.healthFiles(t, map[string][]string{hexID(0x90): big})[hexID(0x90)]; h.Healthy {
		t.Fatal("a release with more files than the cap must not be labelled")
	}
	if h := e.healthFiles(t, map[string][]string{hexID(0x91): nil})[hexID(0x91)]; h.Healthy {
		t.Fatal("a release with no known files must not be labelled")
	}
	if len(e.store.lastFiles) > MaxLabelFiles {
		t.Fatalf("looked up %d files in one query", len(e.store.lastFiles))
	}
}

func TestNetworkHealthyFilesValidatesInput(t *testing.T) {
	e := newEnv(t, openRecv)
	if _, err := e.a.NetworkHealthyFiles(context.Background(), map[string][]string{"nope": {fileA}}); err == nil {
		t.Fatal("bad release id accepted")
	}
	if _, err := e.a.NetworkHealthyFiles(context.Background(), map[string][]string{relNZB: {"nothex"}}); err == nil {
		t.Fatal("bad file id accepted")
	}
	many := map[string][]string{}
	for i := 0; i <= maxReleaseIDs; i++ {
		many[hexID(byte(i))] = []string{fileA}
	}
	if _, err := e.a.NetworkHealthyFiles(context.Background(), many); err == nil {
		t.Fatal("more releases than the bound accepted")
	}
}

// A publisher's own attestation is not independent evidence for its own offer.
func TestNetworkHealthyFilesExcludingLeavesOutThePublishersOwnEvidence(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 2
	e.seedFileEv(openNet, relNZB, fileA, 1, nil)
	e.seedFileEv(openNet, relNZB, fileA, 2, nil)
	rel := map[string][]string{relNZB: {fileA}}
	if h := e.healthFiles(t, rel)[relNZB]; !h.Healthy || h.HealthyPublishers != 2 {
		t.Fatalf("baseline: %+v", h)
	}
	got, err := e.a.NetworkHealthyFilesExcluding(context.Background(), node(1), rel)
	if err != nil {
		t.Fatal(err)
	}
	if h := got[relNZB]; h.Healthy || h.HealthyPublishers != 1 {
		t.Fatalf("own attestation still counted: %+v", h)
	}
	got, err = e.a.NetworkHealthyFilesExcluding(context.Background(), "", rel)
	if err != nil || !got[relNZB].Healthy {
		t.Fatalf("empty exclude must exclude nobody: %+v %v", got[relNZB], err)
	}
}

func TestNetworkHealthyExcludingLeavesOutThePublishersOwnEvidence(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 2
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, nil)
	got, err := e.a.NetworkHealthyExcluding(context.Background(), node(1), release)
	if err != nil {
		t.Fatal(err)
	}
	if h := got[release]; h.Healthy || h.HealthyPublishers != 1 {
		t.Fatalf("own attestation still counted: %+v", h)
	}
	got, err = e.a.NetworkHealthy(context.Background(), release)
	if err != nil || !got[release].Healthy || got[release].HealthyPublishers != 2 {
		t.Fatalf("without exclusion both count: %+v %v", got[release], err)
	}
}
