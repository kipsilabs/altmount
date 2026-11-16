package hints

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
)

func coverage(checked, total int) func(*database.PeernetAttestation) {
	return func(a *database.PeernetAttestation) { a.SegmentsChecked, a.TotalSegments = checked, total }
}

func TestCoverageSufficient(t *testing.T) {
	cases := []struct {
		checked, total int
		want           bool
	}{
		{0, 0, false},     // unknown (rows stored before the columns existed)
		{50, 0, false},    // total unknown
		{0, 1000, false},  // nothing checked
		{20, 1000, false}, // floor-level check (2%)
		{49, 1000, false}, // just under 5%
		{50, 1000, true},  // default 5% check
		{10, 100, true},   // 10 segments is the minimum
		{9, 100, false},   // under the minimum
		{5000, 100000, true},
		{2000, 100000, false}, // floor-level on a big file
		{8, 8, true},          // whole small file
		{7, 8, false},
		{11, 10, false}, // impossible: more than the total
	}
	for _, c := range cases {
		if got := CoverageSufficient(c.checked, c.total); got != c.want {
			t.Errorf("CoverageSufficient(%d,%d)=%v want %v", c.checked, c.total, got, c.want)
		}
	}
}

func TestHealthyEvidenceNeedsKnownSufficientCoverage(t *testing.T) {
	for name, tc := range map[string]struct {
		mut  func(*database.PeernetAttestation)
		want AdviceKind
	}{
		"unknown 0/0":  {coverage(0, 0), AdviceNone},
		"floor level":  {coverage(20, 1000), AdviceNone},
		"normal check": {coverage(50, 1000), AdviceReduceSampleToFloor},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, openRecv)
			e.seed(openNet, 1, tc.mut)
			e.seed(openNet, 2, tc.mut)
			if got := e.advise(t).Kind; got != tc.want {
				t.Fatalf("kind %v want %v", got, tc.want)
			}
		})
	}
}

func TestOneInsufficientPublisherDoesNotCountTowardCorroboration(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, coverage(0, 0))
	if got := e.advise(t).Kind; got != AdviceNone {
		t.Fatalf("kind %v", got)
	}
}

func TestPrivateHealthyEvidenceNeedsCoverageToo(t *testing.T) {
	e := newEnv(t, privRecv)
	e.seed(privNet, 1, coverage(0, 0))
	if got := e.advise(t).Kind; got != AdviceNone {
		t.Fatalf("kind %v", got)
	}
}

func TestUnhealthyEvidenceIgnoresCoverage(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 1 // one unhealthy publisher meets the threshold
	e.seed(openNet, 1, func(a *database.PeernetAttestation) { a.Healthy = false; a.SegmentsChecked, a.TotalSegments = 0, 0 })
	if got := e.advise(t).Kind; got != AdviceRecheckSooner {
		t.Fatalf("kind %v", got)
	}
}

func TestNetworkHealthyNeedsCoverage(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, coverage(0, 0))
	e.seed(openNet, 2, coverage(0, 0))
	if h := e.health(t, release)[release]; h.Healthy || h.HealthyPublishers != 0 {
		t.Fatalf("%+v", h)
	}
}

// Fresh identities that sort first must not use up the lookup bound before
// the known publishers are reached.
func TestFreshIdentitiesCannotCrowdOutKnownPublishers(t *testing.T) {
	e := newEnv(t, openRecv)
	// Known publishers have older attestations than 300 fresh identities.
	e.seed(openNet, 1000, func(a *database.PeernetAttestation) { a.CheckedAt = e.now.Add(-5 * time.Hour) })
	e.seed(openNet, 1001, func(a *database.PeernetAttestation) { a.CheckedAt = e.now.Add(-5 * time.Hour) })
	// Newest first, as the store returns them: fresh ones in front.
	for i := 1; i <= 300; i++ {
		e.seed(openNet, i, nil)
		e.peers.first[node(i)] = e.now // young
	}
	k := key(openNet, release)
	all := e.store.atts[k]
	ordered := append(append([]database.PeernetAttestation{}, all[2:]...), all[:2]...)
	e.store.atts[k] = ordered
	if got := e.advise(t).Kind; got != AdviceReduceSampleToFloor {
		t.Fatalf("known publishers crowded out: %v (%d lookups)", got, e.peers.calls)
	}
	// Same through the label path.
	if h := e.health(t, release)[release]; !h.Healthy {
		t.Fatalf("label crowded out: %+v", h)
	}
}

var _ = context.Background
