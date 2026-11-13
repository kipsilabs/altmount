package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/peernet/hints"
)

type fakeAdviceSource struct {
	advice  hints.Advice
	applied []hints.AdviceKind
}

func (f *fakeAdviceSource) HealthAdvice(context.Context, string) hints.Advice { return f.advice }
func (f *fakeAdviceSource) NotePeerAdviceApplied(k hints.AdviceKind) {
	f.applied = append(f.applied, k)
}

func TestPeerAdviserConvertsAdvice(t *testing.T) {
	until := time.Now().Add(time.Hour)
	for _, tc := range []struct {
		in   hints.Advice
		want health.PeerAdvice
	}{
		{hints.Advice{}, health.PeerAdvice{}},
		{hints.Advice{Kind: hints.AdviceReduceSampleToFloor}, health.PeerAdvice{Kind: health.PeerAdviceReduceSample}},
		{hints.Advice{Kind: hints.AdviceMaySkipOneCheck, SkipUntil: until}, health.PeerAdvice{Kind: health.PeerAdviceSkipOnce, SkipUntil: until}},
		// Not a check-time advice: the check is running already.
		{hints.Advice{Kind: hints.AdviceRecheckSooner}, health.PeerAdvice{}},
	} {
		got := NewPeerAdviser(&fakeAdviceSource{advice: tc.in}).PeerAdvice(context.Background(), "a.mkv")
		if got != tc.want {
			t.Errorf("%v: got %+v want %+v", tc.in.Kind, got, tc.want)
		}
	}
}

func TestPeerAdviserReportsAppliedAdvice(t *testing.T) {
	src := &fakeAdviceSource{}
	a := NewPeerAdviser(src)
	a.PeerAdviceApplied(health.PeerAdviceReduceSample)
	a.PeerAdviceApplied(health.PeerAdviceSkipOnce)
	a.PeerAdviceApplied(health.PeerAdviceNone)
	want := []hints.AdviceKind{hints.AdviceReduceSampleToFloor, hints.AdviceMaySkipOneCheck}
	if len(src.applied) != 2 || src.applied[0] != want[0] || src.applied[1] != want[1] {
		t.Fatalf("applied = %v", src.applied)
	}
}

func TestNewPeerAdviserNilSourceIsNil(t *testing.T) {
	if NewPeerAdviser(nil) != nil {
		t.Fatal("a nil source must give no adviser at all")
	}
}
