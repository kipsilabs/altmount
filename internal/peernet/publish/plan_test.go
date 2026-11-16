package publish

import (
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/records"
)

func TestPlanConfigDefaults(t *testing.T) {
	c := PlanConfig{}.withDefaults()
	if c.MinRepublish != 24*time.Hour || c.RecordRefresh != 20*24*time.Hour {
		t.Fatalf("%+v", c)
	}
	if c.RecordRefresh >= records.RecordLifetime || c.RecordRefresh <= 0 {
		t.Fatal("record refresh must precede expiry")
	}
	custom := PlanConfig{MinRepublish: time.Hour, RecordRefresh: time.Hour}.withDefaults()
	if custom.MinRepublish != time.Hour || custom.RecordRefresh != time.Hour {
		t.Fatal("custom values overwritten")
	}
	// A refresh interval at or past expiry is clamped.
	if got := (PlanConfig{RecordRefresh: 40 * 24 * time.Hour}).withDefaults().RecordRefresh; got >= records.RecordLifetime {
		t.Fatalf("not clamped: %v", got)
	}
}

func TestDueRelease(t *testing.T) {
	c := PlanConfig{}.withDefaults()
	now := t0
	cases := []struct {
		name string
		last *State
		want bool
	}{
		{"never published", nil, true},
		{"just published", &State{PublishedAt: now.Add(-time.Hour)}, false},
		{"19 days", &State{PublishedAt: now.Add(-19 * 24 * time.Hour)}, false},
		{"20 days", &State{PublishedAt: now.Add(-20 * 24 * time.Hour)}, true},
		{"clock went backwards", &State{PublishedAt: now.Add(48 * time.Hour)}, false},
	}
	for _, tc := range cases {
		if got := c.DueRelease(tc.last, now); got != tc.want {
			t.Errorf("%s: %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestDueAttestation(t *testing.T) {
	c := PlanConfig{}.withDefaults()
	now := t0
	last := &State{PublishedAt: now.Add(-25 * time.Hour), CheckedAt: now.Add(-26 * time.Hour), Verdict: VerdictHealthy}
	cases := []struct {
		name    string
		last    *State
		verdict Verdict
		checked time.Time
		want    bool
	}{
		{"never", nil, VerdictHealthy, now, true},
		{"newer check after interval", last, VerdictHealthy, now.Add(-time.Minute), true},
		{"same check", last, VerdictHealthy, last.CheckedAt, false},
		{"older check", last, VerdictHealthy, last.CheckedAt.Add(-time.Hour), false},
		{"too soon", &State{PublishedAt: now.Add(-time.Hour), CheckedAt: now.Add(-time.Hour), Verdict: VerdictHealthy}, VerdictHealthy, now, false},
		{"verdict changed is immediate", &State{PublishedAt: now.Add(-time.Minute), CheckedAt: now.Add(-time.Minute), Verdict: VerdictHealthy}, VerdictUnhealthy, now, true},
		{"verdict changed with older check is not", &State{PublishedAt: now.Add(-time.Minute), CheckedAt: now, Verdict: VerdictHealthy}, VerdictUnhealthy, now.Add(-time.Hour), false},
		{"clock backwards", &State{PublishedAt: now.Add(time.Hour), CheckedAt: now.Add(-time.Hour), Verdict: VerdictHealthy}, VerdictHealthy, now, false},
	}
	for _, tc := range cases {
		if got := c.DueAttestation(tc.last, tc.verdict, tc.checked, now); got != tc.want {
			t.Errorf("%s: %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestRecordsDueForReannounce(t *testing.T) {
	c := PlanConfig{}.withDefaults()
	now := t0
	entries := []Tracked{
		{Kind: records.KindRelease, ReleaseID: records.ReleaseID{1}, State: State{PublishedAt: now.Add(-21 * 24 * time.Hour)}},
		{Kind: records.KindRelease, ReleaseID: records.ReleaseID{2}, State: State{PublishedAt: now.Add(-time.Hour)}},
		{Kind: records.KindAttestation, ReleaseID: records.ReleaseID{3}, State: State{PublishedAt: now.Add(-29 * 24 * time.Hour)}},
		{Kind: records.KindRelease, ReleaseID: records.ReleaseID{4}, State: State{PublishedAt: now.Add(-25 * 24 * time.Hour)}},
	}
	got := c.RecordsDueForReannounce(entries, now)
	if len(got) != 2 || got[0].ReleaseID[0] != 4 || got[1].ReleaseID[0] != 1 {
		t.Fatalf("got %+v (oldest first expected, attestations excluded)", got)
	}
	if len(entries) != 4 || entries[0].ReleaseID[0] != 1 {
		t.Fatal("input mutated")
	}
}
