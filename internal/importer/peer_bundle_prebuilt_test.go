package importer

import (
	"context"
	"testing"
)

func TestPrebuiltStoreIsNotReusedAfterTrimOrGaps(t *testing.T) {
	p := &prebuiltStore{}
	if !prebuiltStillDescribes(p, 0, false) {
		t.Fatal("an untouched NZB reuses the store")
	}
	if prebuiltStillDescribes(p, 0, true) {
		t.Fatal("a trimmed NZB must rebuild the store")
	}
	if prebuiltStillDescribes(p, 2, false) {
		t.Fatal("gap placeholders force a rebuild")
	}
	if prebuiltStillDescribes(nil, 0, false) {
		t.Fatal("nothing to reuse")
	}
}

// A degraded peer import is redone from its NZB with the fast path skipped for
// that one run only.
func TestPeerFastPath_SkippedOnceForARequeuedItem(t *testing.T) {
	e := newBatteryEnv(t)
	fp := &fakeFastPath{take: true, env: e}
	e.proc.SetPeerBundleFastPath(fp)
	nzbPath := cleanNZB(e)
	n := parseTestNzb(t, nzbPath)

	e.proc.SkipPeerFastPathOnce(5)
	_, _, taken, _ := e.proc.tryPeerBundle(context.Background(), n, nzbPath, 5, "/", nil, nil, nil)
	if taken || len(fp.reqs) != 0 {
		t.Fatal("the fast path must not run for a requeued item")
	}
	_, _, taken, _ = e.proc.tryPeerBundle(context.Background(), n, nzbPath, 5, "/", nil, nil, nil)
	if !taken {
		t.Fatal("the skip applies once only")
	}
}

// fakeSkipStore stands for the persisted mark on the queue row; it outlives the
// processor, as the database does a restart.
type fakeSkipStore map[int64]bool

func (s fakeSkipStore) ClaimSkipFastPath(_ context.Context, id int64) (bool, error) {
	was := s[id]
	delete(s, id)
	return was, nil
}

// The skip survives a restart: a new processor reads it from the queue row, and
// consumes it once.
func TestPeerFastPath_SkipSurvivesARestart(t *testing.T) {
	e := newBatteryEnv(t)
	store := fakeSkipStore{5: true}
	fp := &fakeFastPath{take: true, env: e}
	e.proc.SetPeerBundleFastPath(fp)
	e.proc.SetPeerSkipStore(store) // a fresh processor: nothing in memory
	nzbPath := cleanNZB(e)
	n := parseTestNzb(t, nzbPath)

	_, _, taken, _ := e.proc.tryPeerBundle(context.Background(), n, nzbPath, 5, "/", nil, nil, nil)
	if taken || len(fp.reqs) != 0 {
		t.Fatal("the persisted mark must keep the fast path off for the requeued item")
	}
	if len(store) != 0 {
		t.Fatal("the mark is consumed")
	}
	_, _, taken, _ = e.proc.tryPeerBundle(context.Background(), n, nzbPath, 5, "/", nil, nil, nil)
	if !taken {
		t.Fatal("the skip applies once only")
	}
}
