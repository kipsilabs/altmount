package publish

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/records"
)

var wantBundle = records.BundleID{0xb, 0xd, 0x1}

type fakeBundles struct {
	calls int
	dec   BundleDecision
}

func (f *fakeBundles) BundleFor(context.Context, records.ReleaseID, ReleaseInfo) BundleDecision {
	f.calls++
	return f.dec
}

// releaseBundle returns the bundle id of the n-th release record sunk.
func releaseBundle(t *testing.T, e *env, n int) records.BundleID {
	t.Helper()
	seen := 0
	for _, s := range e.sink.got {
		env, err := records.Peek(s.enc)
		if err != nil {
			t.Fatal(err)
		}
		if env.Kind != records.KindRelease {
			continue
		}
		if seen == n {
			r, err := records.DecodeReleaseRecord(s.enc)
			if err != nil {
				t.Fatal(err)
			}
			return r.BundleID
		}
		seen++
	}
	t.Fatalf("no release record #%d", n)
	return records.BundleID{}
}

func lastRelease(t *testing.T, e *env) State {
	t.Helper()
	st, ok, err := e.store.LastPublished(context.Background(), openNet, records.KindRelease, relID, records.FileID{})
	if err != nil || !ok {
		t.Fatalf("no release state: %v %v", ok, err)
	}
	return st
}

func TestBundleIDIsAnnouncedOnlyAtLevelThree(t *testing.T) {
	for _, tc := range []struct {
		level     int
		want      records.BundleID
		wantCalls int
	}{{2, records.BundleID{}, 0}, {3, wantBundle, 1}} {
		e := newEnv(t)
		fb := &fakeBundles{dec: BundleDecision{ID: wantBundle}}
		e.cfg.Bundles = fb
		e.nets = []Network{{ID: openNet, Level: tc.level}}
		if _, err := e.publisher(t).Publish(context.Background(), healthy(), info()); err != nil {
			t.Fatal(err)
		}
		if got := releaseBundle(t, e, 0); got != tc.want {
			t.Errorf("level %d: bundle id = %x, want %x", tc.level, got, tc.want)
		}
		if fb.calls != tc.wantCalls {
			t.Errorf("level %d: bundle source called %d times, want %d", tc.level, fb.calls, tc.wantCalls)
		}
		st := lastRelease(t, e)
		if st.BundleID != tc.want || st.BundleLevel != tc.level {
			t.Errorf("level %d: state bundle %x level %d", tc.level, st.BundleID, st.BundleLevel)
		}
	}
}

func TestLevelChangeSupersedesTheRecord(t *testing.T) {
	e := newEnv(t)
	fb := &fakeBundles{dec: BundleDecision{ID: wantBundle}}
	e.cfg.Bundles = fb
	p := e.publisher(t)
	ctx := context.Background()
	if _, err := p.Publish(ctx, healthy(), info()); err != nil {
		t.Fatal(err)
	}
	// Nothing changed: not due, and the bundle source is not consulted again.
	if o := p.ReannounceRelease(ctx, openNet, relID, info()); o.Skip != SkipNotDue {
		t.Fatalf("unchanged: %+v", o)
	}
	// Level 2 -> 3: the record is announced again, now with the bundle.
	e.nets = []Network{{ID: openNet, Level: 3}}
	if o := p.ReannounceRelease(ctx, openNet, relID, info()); o.Skip != SkipNone || o.Err != nil {
		t.Fatalf("2->3: %+v", o)
	}
	if got := releaseBundle(t, e, 1); got != wantBundle {
		t.Fatalf("2->3 record bundle = %x", got)
	}
	if o := p.ReannounceRelease(ctx, openNet, relID, info()); o.Skip != SkipNotDue {
		t.Fatalf("settled at 3: %+v", o)
	}
	// Level 3 -> 2: superseded by a record without one.
	e.nets = []Network{{ID: openNet, Level: 2}}
	e.now = e.now.Add(time.Second)
	if o := p.ReannounceRelease(ctx, openNet, relID, info()); o.Skip != SkipNone || o.Err != nil {
		t.Fatalf("3->2: %+v", o)
	}
	if got := releaseBundle(t, e, 2); got != (records.BundleID{}) {
		t.Fatalf("3->2 record still carries a bundle id %x", got)
	}
	if st := lastRelease(t, e); st.BundleID != (records.BundleID{}) || st.BundleLevel != 2 {
		t.Fatalf("state after drop: %+v", st)
	}
	if fb.calls != 1 {
		t.Fatalf("bundle source called %d times, want exactly once", fb.calls)
	}
}

func TestNotShareableReleaseStillPublishesItsRecordAndIsNotRetried(t *testing.T) {
	e := newEnv(t)
	fb := &fakeBundles{dec: BundleDecision{Skip: "rclone_encrypted"}}
	e.cfg.Bundles = fb
	e.nets = []Network{{ID: openNet, Level: 3}}
	p := e.publisher(t)
	outs, err := p.Publish(context.Background(), healthy(), info())
	if err != nil {
		t.Fatal(err)
	}
	if published(outs, records.KindRelease) != 1 {
		t.Fatalf("release record not published: %+v", outs)
	}
	if releaseBundle(t, e, 0) != (records.BundleID{}) {
		t.Fatal("a not shareable release announced a bundle")
	}
	if st := lastRelease(t, e); st.BundleLevel != 3 || st.BundleID != (records.BundleID{}) {
		t.Fatalf("the decision must be remembered: %+v", st)
	}
	if o := p.ReannounceRelease(context.Background(), openNet, relID, info()); o.Skip != SkipNotDue || fb.calls != 1 {
		t.Fatalf("retried a settled decision: %+v calls=%d", o, fb.calls)
	}
}

func TestTransientBundleFailureSendsNothingAndKeepsState(t *testing.T) {
	e := newEnv(t)
	fb := &fakeBundles{dec: BundleDecision{ID: wantBundle}}
	e.cfg.Bundles = fb
	e.nets = []Network{{ID: openNet, Level: 3}}
	p := e.publisher(t)
	ctx := context.Background()
	if _, err := p.Publish(ctx, healthy(), info()); err != nil {
		t.Fatal(err)
	}
	before := lastRelease(t, e)
	sent := len(e.sink.got)

	// The announced record had a bundle: a busy builder must not supersede it
	// with a bundle-less one, and must not churn while the failure lasts.
	fb.dec = BundleDecision{Err: errors.New("builder busy")}
	e.now = e.now.Add(DefaultRecordRefresh + time.Hour) // the record is due for refresh
	for range 3 {
		o := p.ReannounceRelease(ctx, openNet, relID, info())
		if o.BundleErr == nil || o.Skip == SkipNone || o.Err != nil {
			t.Fatalf("busy: %+v", o)
		}
	}
	if len(e.sink.got) != sent {
		t.Fatalf("records sent during failure: %d, want %d", len(e.sink.got)-sent, 0)
	}
	if got := lastRelease(t, e); got != before {
		t.Fatalf("state changed during failure: %+v -> %+v", before, got)
	}

	// Recovery: exactly one record, with the bundle.
	fb.dec = BundleDecision{ID: wantBundle}
	if o := p.ReannounceRelease(ctx, openNet, relID, info()); o.Skip != SkipNone || o.Err != nil || o.BundleErr != nil {
		t.Fatalf("retry: %+v", o)
	}
	if len(e.sink.got) != sent+1 || releaseBundle(t, e, 1) != wantBundle {
		t.Fatalf("recovery sent %d records", len(e.sink.got)-sent)
	}
}

func TestTransientBundleFailureOnFirstPublishIsRetriedWithoutChurn(t *testing.T) {
	e := newEnv(t)
	fb := &fakeBundles{dec: BundleDecision{Err: errors.New("builder busy")}}
	e.cfg.Bundles = fb
	e.nets = []Network{{ID: openNet, Level: 3}}
	p := e.publisher(t)
	ctx := context.Background()
	for range 3 {
		outs, err := p.Publish(ctx, healthy(), info())
		if err != nil {
			t.Fatal(err)
		}
		if published(outs, records.KindRelease) != 0 {
			t.Fatalf("release published during failure: %+v", outs)
		}
	}
	for _, s := range e.sink.got {
		if env, _ := records.Peek(s.enc); env.Kind == records.KindRelease {
			t.Fatal("release record sent during failure")
		}
	}
	fb.dec = BundleDecision{ID: wantBundle}
	if o := p.ReannounceRelease(ctx, openNet, relID, info()); o.Skip != SkipNone || o.Err != nil {
		t.Fatalf("retry: %+v", o)
	}
	if releaseBundle(t, e, 0) != wantBundle {
		t.Fatal("retry did not announce the bundle")
	}
}

func TestMoreFileNoticesOfAPublishedReleaseDoNotRebuild(t *testing.T) {
	e := newEnv(t)
	fb := &fakeBundles{dec: BundleDecision{ID: wantBundle}}
	e.cfg.Bundles = fb
	e.nets = []Network{{ID: openNet, Level: 3}}
	p := e.publisher(t)
	for range 3 {
		if _, err := p.Publish(context.Background(), healthy(), info()); err != nil {
			t.Fatal(err)
		}
	}
	if fb.calls != 1 {
		t.Fatalf("bundle source called %d times for one release", fb.calls)
	}
}

func TestExclusionsCoverOrigin(t *testing.T) {
	ex := Exclusions{Categories: []string{" Movies "}, Indexers: []string{"IdxOne", "unknown"}}
	for _, tc := range []struct {
		cat, idx string
		want     bool
	}{
		{"movies", "", true}, {"tv", "idxone", true}, {"tv", "other", false},
		{"", "", false}, {"tv", "unknown", false},
	} {
		if got := ex.CoversOrigin(tc.cat, tc.idx); got != tc.want {
			t.Errorf("%q/%q = %v, want %v", tc.cat, tc.idx, got, tc.want)
		}
	}
}
