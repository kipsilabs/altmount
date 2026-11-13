package bridge

import (
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/peernet/service"
)

type recorder struct{ got []service.HealthyNotice }

func (r *recorder) NotifyHealthy(n service.HealthyNotice) { r.got = append(r.got, n) }

func TestNilNotifierMeansNoObserver(t *testing.T) {
	if NewHealthObserver(nil) != nil {
		t.Fatal("a nil notifier must give a nil observer (zero overhead in the health worker)")
	}
}

func TestFileHealthyConvertsEveryField(t *testing.T) {
	r := &recorder{}
	NewHealthObserver(r).FileHealthy(health.HealthyNotice{
		FilePath: "a/b.mkv", CheckedAt: time.Unix(7, 0),
		Stats: health.CheckStats{TotalSegments: 10, SegmentsChecked: 9, MissingCount: 1, FullCheck: true,
			MediaHeaderVerified: true, PatchedByPAR2: true, PatchKnown: true, KnownHoles: true, ReducedByPeerAdvice: true},
	})
	want := service.HealthyNotice{FilePath: "a/b.mkv", CheckedAt: time.Unix(7, 0), TotalSegments: 10, SegmentsChecked: 9,
		MissingCount: 1, FullCheck: true, MediaHeaderVerified: true, PatchedByPAR2: true, PatchKnown: true, KnownHoles: true, ReducedByPeerAdvice: true}
	if len(r.got) != 1 || r.got[0] != want {
		t.Fatalf("got %+v", r.got)
	}
}
