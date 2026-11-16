package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
)

// fakeLocalHealth answers the status lookup of the re-announce guard.
type fakeLocalHealth struct {
	mu       sync.Mutex
	statuses map[string]string
	err      error
	calls    int
}

func (f *fakeLocalHealth) Statuses(_ context.Context, paths []string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]string{}
	for _, p := range paths {
		if s, ok := f.statuses[p]; ok {
			out[p] = s
		}
	}
	return out, nil
}

func (f *fakeLocalHealth) set(path, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[path] = status
}

func ageRelease(t *testing.T, f *pubFixture, release string) {
	t.Helper()
	old := time.Now().Add(-publish.DefaultRecordRefresh - time.Hour)
	if err := f.state.Put(context.Background(), database.PeernetPublishState{
		NetworkID: openID.String(), Kind: database.PeernetRecordKindRelease, ReleaseID: release, PublishedAt: old}); err != nil {
		t.Fatal(err)
	}
}

func reannounceFixture(t *testing.T, lh *fakeLocalHealth, extra ...func(*Deps)) *pubFixture {
	t.Helper()
	f := newPubInstance(t, level(2), func(d *Deps) {
		if lh != nil {
			d.Health = lh
		}
		for _, mut := range extra {
			mut(d)
		}
	})
	f.seedFile(t)
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	awaitFirstUpkeep(t, f)
	return f
}

func TestReannounceRequiresALocallyHealthyFile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string // "" = no health record
		err    error
		want   int64
		skip   string
	}{
		{"healthy", "healthy", nil, 1, ""},
		{"a later check failed", "corrupted", nil, 0, "not_healthy"},
		{"retrying after a failed check", "pending", nil, 0, "not_healthy"},
		{"repair in progress", "repair_triggered", nil, 0, "not_healthy"},
		{"degraded", "degraded", nil, 0, "not_healthy"},
		{"no health record", "", nil, 0, "not_healthy"},
		{"lookup failure fails closed", "healthy", errors.New("db down"), 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lh := &fakeLocalHealth{statuses: map[string]string{}, err: tc.err}
			if tc.status != "" {
				lh.statuses[pubPath] = tc.status
			}
			f := reannounceFixture(t, lh)
			ageRelease(t, f, pubRelease)
			f.node(t).pipe.reannounce(context.Background(), 10)
			got := f.status(t).Publish
			if got.Reannounced != tc.want {
				t.Fatalf("reannounced = %d, want %d (%+v)", got.Reannounced, tc.want, got)
			}
			if tc.skip != "" && got.Skipped[tc.skip] == 0 {
				t.Fatalf("expected skip reason %q: %+v", tc.skip, got.Skipped)
			}
		})
	}
}

func TestReannounceWithoutAHealthSourceFailsClosed(t *testing.T) {
	f := reannounceFixture(t, nil)
	ageRelease(t, f, pubRelease)
	f.node(t).pipe.reannounce(context.Background(), 10)
	if got := f.status(t).Publish.Reannounced; got != 0 {
		t.Fatalf("re-announced with no way to check local health: %d", got)
	}
}

// One bad file blocks the re-announce of its release even when another file of
// the same release is healthy.
func TestReannounceNeedsEveryIndexedFileOfTheReleaseHealthy(t *testing.T) {
	lh := &fakeLocalHealth{statuses: map[string]string{pubPath: "healthy"}}
	f := reannounceFixture(t, lh)
	second := "movies/Some.Movie.2020/extra.mkv"
	if err := f.content.Upsert(context.Background(), database.PeernetContentEntry{FilePath: second, FileID: strings.Repeat("c", 64), ReleaseID: pubRelease}); err != nil {
		t.Fatal(err)
	}
	lh.set(second, "corrupted")
	ageRelease(t, f, pubRelease)
	f.node(t).pipe.reannounce(context.Background(), 10)
	if got := f.status(t).Publish.Reannounced; got != 0 {
		t.Fatalf("re-announced a release with a corrupted file: %d", got)
	}
}

// A release that keeps failing must not starve the others.
func TestReannounceFailingReleaseDoesNotStarveTheBatch(t *testing.T) {
	lh := &fakeLocalHealth{statuses: map[string]string{pubPath: "healthy"}}
	f := reannounceFixture(t, lh)
	n := f.node(t)

	// A release with no usable information, and older than the healthy one.
	broken := strings.Repeat("d", 64)
	if err := f.state.Put(context.Background(), database.PeernetPublishState{
		NetworkID: openID.String(), Kind: database.PeernetRecordKindRelease, ReleaseID: broken,
		PublishedAt: time.Now().Add(-publish.DefaultRecordRefresh - 48*time.Hour)}); err != nil {
		t.Fatal(err)
	}
	ageRelease(t, f, pubRelease)

	// A batch of one: before back-off the broken release was retried first on
	// every pass and the healthy one never got a turn.
	n.pipe.reannounce(context.Background(), 1)
	if got := f.status(t).Publish.Reannounced; got != 0 {
		t.Fatalf("first pass spent its slot on the broken release only: %d", got)
	}
	n.pipe.reannounce(context.Background(), 1)
	if got := f.status(t).Publish.Reannounced; got != 1 {
		t.Fatalf("second pass must reach the healthy release: %d", got)
	}
}

func TestReannounceBackOffExpiresAndGrows(t *testing.T) {
	lh := &fakeLocalHealth{statuses: map[string]string{pubPath: "corrupted"}}
	// The clock is installed before the node starts: its maintenance loop
	// reads it from the first moment.
	now := time.Now()
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	f := reannounceFixture(t, lh, func(d *Deps) { d.publishNow = clock })
	n := f.node(t)
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	ageRelease(t, f, pubRelease)

	lookups := func() int { lh.mu.Lock(); defer lh.mu.Unlock(); return lh.calls }
	n.pipe.reannounce(context.Background(), 5)
	first := lookups()
	if first == 0 {
		t.Fatal("no lookup on the first pass")
	}
	n.pipe.reannounce(context.Background(), 5)
	if lookups() != first {
		t.Fatal("a release in back-off was looked up again at once")
	}
	advance(7 * time.Hour) // longer than any back-off
	lh.set(pubPath, "healthy")
	n.pipe.reannounce(context.Background(), 5)
	if got := f.status(t).Publish.Reannounced; got != 1 {
		t.Fatalf("after the back-off the release must be retried: %d", got)
	}
}
