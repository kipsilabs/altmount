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
