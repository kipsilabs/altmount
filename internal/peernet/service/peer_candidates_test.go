package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/hints"
)

const candIMDb = "tt0111161"

// candidateFixture: a level-3 publisher whose release carries a media id, a
// level-1 receiver that synced its records and has a real advisor installed.
func candidateFixture(t *testing.T, pubLevel int) (*bundlePub, *instance) {
	t.Helper()
	a := newBundlePublisher(t, pubLevel, nil)
	if _, err := a.db.Connection().Exec(`UPDATE import_history SET metadata = ? WHERE virtual_path = ?`,
		`{"imdb_id":"`+candIMDb+`","type":"movie"}`, pubPath); err != nil {
		t.Fatal(err)
	}
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	installAdvisor(t, b)
	return a, b
}

func installAdvisor(t *testing.T, b *instance) {
	t.Helper()
	conn, d := b.db.Connection(), b.db.Dialect()
	adv, err := hints.New(hints.Config{Store: database.NewPeernetRecordRepository(conn, d), Peers: database.NewPeernetPeerRepository(conn, d),
		Budget: hints.NewBudget(6), Networks: b.svc.ReceivingNetworks, SelfNodeID: b.svc.NodeID()})
	if err != nil {
		t.Fatal(err)
	}
	b.svc.SetHintSources(HintSources{Advisor: realAdvisor{adv}, Budget: hints.NewBudget(6), Backbones: func() []string { return nil }})
}

// realAdvisor adapts *hints.Advisor to HintAdvisor for tests (the production
// wiring passes the real one, which already has every method).
type realAdvisor struct{ *hints.Advisor }

func TestPeerCandidatesListsFetchableOffersOnly(t *testing.T) {
	a, b := candidateFixture(t, 3)
	got := b.svc.PeerCandidates(context.Background(), candIMDb, nil, nil)
	if len(got) != 1 || got[0].ReleaseID != a.rel || got[0].Title == "" || got[0].TotalSize == 0 {
		t.Fatalf("candidates %+v", got)
	}
	// Another media key finds nothing.
	if got := b.svc.PeerCandidates(context.Background(), "tt0000001", nil, nil); len(got) != 0 {
		t.Fatalf("unrelated media: %+v", got)
	}
	// A level 2 publisher announces no bundle: nothing to fetch, nothing offered.
	_, b2 := candidateFixture(t, 2)
	if got := b2.svc.PeerCandidates(context.Background(), candIMDb, nil, nil); len(got) != 0 {
		t.Fatalf("a record without a bundle was offered: %+v", got)
	}
}

func TestPeerCandidatesSkipWhatIsAlreadyLocal(t *testing.T) {
	a, b := candidateFixture(t, 3)
	content := database.NewPeernetContentRepository(b.db.Connection(), b.db.Dialect())
	if err := content.Upsert(context.Background(), database.PeernetContentEntry{FilePath: "movies/x/m.mkv", FileID: hexOf('f'), ReleaseID: a.rel}); err != nil {
		t.Fatal(err)
	}
	if got := b.svc.PeerCandidates(context.Background(), candIMDb, nil, nil); len(got) != 0 {
		t.Fatalf("a release this node already has was offered: %+v", got)
	}
}

func TestPeerCandidatesFailOpenAndCache(t *testing.T) {
	_, b := candidateFixture(t, 3)
	// Not installed (feature consumers off): nothing, no panic.
	b.svc.ClearHintSources()
	if got := b.svc.PeerCandidates(context.Background(), candIMDb, nil, nil); got != nil {
		t.Fatalf("without hint sources: %+v", got)
	}
	// An advisor failure is no candidate and no error.
	b.svc.SetHintSources(HintSources{Advisor: failingCandidates{}, Budget: hints.NewBudget(1), Backbones: func() []string { return nil }})
	if got := b.svc.PeerCandidates(context.Background(), candIMDb, nil, nil); got != nil {
		t.Fatalf("advisor error: %+v", got)
	}
	// Invalid media ids never reach the store.
	installAdvisor(t, b)
	if got := b.svc.PeerCandidates(context.Background(), "not-an-id", nil, nil); got != nil {
		t.Fatalf("invalid id: %+v", got)
	}
	// A repeat inside the cache window does not ask the advisor again.
	counting := &countingCandidates{realAdvisor: realAdvisor{}}
	conn, d := b.db.Connection(), b.db.Dialect()
	adv, _ := hints.New(hints.Config{Store: database.NewPeernetRecordRepository(conn, d), Peers: database.NewPeernetPeerRepository(conn, d),
		Budget: hints.NewBudget(6), Networks: b.svc.ReceivingNetworks, SelfNodeID: b.svc.NodeID()})
	counting.realAdvisor = realAdvisor{adv}
	b.svc.SetHintSources(HintSources{Advisor: counting, Budget: hints.NewBudget(6), Backbones: func() []string { return nil }})
	b.svc.PeerCandidates(context.Background(), candIMDb, nil, nil)
	b.svc.PeerCandidates(context.Background(), candIMDb, nil, nil)
	if counting.calls != 1 {
		t.Fatalf("advisor asked %d times", counting.calls)
	}
}

type failingCandidates struct{ realAdvisor }

func (failingCandidates) Candidates(context.Context, string, *int, *int) ([]hints.Candidate, error) {
	return nil, errors.New("db down")
}

type countingCandidates struct {
	realAdvisor
	calls int
}

func (c *countingCandidates) Candidates(ctx context.Context, imdb string, s, e *int) ([]hints.Candidate, error) {
	c.calls++
	return c.realAdvisor.Candidates(ctx, imdb, s, e)
}

func hexOf(c byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

var _ = time.Second
