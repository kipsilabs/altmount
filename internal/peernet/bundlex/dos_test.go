package bundlex

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func TestStrikeTablesEvictTheOldestExpiryNotARandomEntry(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{MaxTracked: 3})
	now := time.Now()
	mk := func() identity.NodeID { k, _ := identity.GenerateKey(); return k.NodeID() }
	a, b, c, d := mk(), mk(), mk(), mk()
	e.f.mu.Lock()
	e.f.strikes[strikeKey(nwA, a)] = &strike{n: 5, until: now.Add(2 * time.Hour)}
	e.f.strikes[strikeKey(nwA, b)] = &strike{n: 1, until: now.Add(time.Minute)} // expires first
	e.f.strikes[strikeKey(nwA, c)] = &strike{n: 5, until: now.Add(3 * time.Hour)}
	e.f.mu.Unlock()
	e.f.fail(nwA, d, failTransport, true)
	e.f.mu.Lock()
	defer e.f.mu.Unlock()
	if _, kept := e.f.strikes[strikeKey(nwA, b)]; kept {
		t.Fatal("the entry that expires first must be the one evicted")
	}
	for _, p := range []identity.NodeID{a, c, d} {
		if _, ok := e.f.strikes[strikeKey(nwA, p)]; !ok {
			t.Fatalf("a longer-lived strike was evicted")
		}
	}
}

func TestBuilderDoesNotCacheAWireLargerThanItsCap(t *testing.T) {
	lib, _ := nzbRelease()
	b := newTestBuilder(t, lib, NoPatches{})
	b.store("rel", &Built{Wire: make([]byte, maxBuiltCacheBytes+1)})
	if st := b.Status(); st.CachedBytes > maxBuiltCacheBytes || st.Cached != 0 {
		t.Fatalf("cache holds %d items / %d bytes, cap %d", st.Cached, st.CachedBytes, maxBuiltCacheBytes)
	}
}

func TestServeBuildsNeverBlockPublishBuilds(t *testing.T) {
	lib, rel := nzbRelease()
	lib.slowRead = 300 * time.Millisecond
	b := NewBuilder(BuilderConfig{Source: lib, Library: lib, Patches: NoPatches{}, BuildWait: 50 * time.Millisecond})
	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = b.BuildForServe(context.Background(), rel, "T", posted)
	}()
	<-started
	time.Sleep(50 * time.Millisecond) // the serve build now holds its slot and is reading
	_, err := b.Build(context.Background(), rel, "T", posted)
	if errors.Is(err, ErrBusy) {
		t.Fatal("a publish build waited behind a serve build")
	}
}

func TestServeBuildsAreRateLimitedNodeWideAndUnbuildableIsRememberedBriefly(t *testing.T) {
	lib, _ := nzbRelease()
	b := newTestBuilder(t, lib, NoPatches{})
	busy := 0
	for i := 0; i < 20; i++ {
		rel := netid.Open().String()[:62] + string(rune('a'+i/16)) + string(rune('a'+i%16)) // distinct valid hex-ish ids
		_ = rel
		id := make([]byte, 32)
		id[0], id[1] = byte(i), byte(i>>8)
		hexID := ""
		for _, c := range id {
			hexID += string("0123456789abcdef"[c>>4]) + string("0123456789abcdef"[c&15])
		}
		if _, err := b.BuildForServe(context.Background(), hexID, "T", posted); errors.Is(err, ErrBusy) {
			busy++
		}
	}
	if busy == 0 {
		t.Fatal("20 distinct serve builds in a burst must hit the node-wide build limit")
	}

	lib2, rel2 := nzbRelease()
	b2 := newTestBuilder(t, lib2, nil) // patch status unknown: permanently unbuildable
	_, err := b2.BuildForServe(context.Background(), rel2, "T", posted)
	if err == nil {
		t.Fatal("expected a not-shareable error")
	}
	reads := lib2.readCount()
	if _, err2 := b2.BuildForServe(context.Background(), rel2, "T", posted); err2 == nil {
		t.Fatal("expected the remembered error")
	}
	if lib2.readCount() != reads {
		t.Fatal("the second attempt must be answered from the negative cache")
	}
}
