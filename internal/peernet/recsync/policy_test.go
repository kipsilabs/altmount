package recsync

import "testing"

func TestPolicyFor(t *testing.T) {
	cases := []struct {
		level int
		want  Policy
	}{
		// Level 0: the network does not exist as far as record routes go.
		{0, Policy{}},
		// Level 1: receive only. Pulls and takes pushes, never publishes,
		// never serves its list.
		{1, Policy{Exists: true, Pull: true, AcceptPush: true}},
		// Level 2: shares metadata. Publishes its own records, pushes them,
		// and serves own and relayed records.
		{2, Policy{Exists: true, Pull: true, AcceptPush: true, Publish: true, Push: true, Serve: true}},
		// Level 3 behaves like 2 for records and also serves bundles.
		{3, Policy{Exists: true, Pull: true, AcceptPush: true, Publish: true, Push: true, Serve: true, ServeBundles: true}},
		// Anything out of range fails closed.
		{-1, Policy{}},
		{4, Policy{}},
		{1 << 30, Policy{}},
	}
	for _, c := range cases {
		if got := PolicyFor(c.level); got != c.want {
			t.Errorf("level %d: got %+v, want %+v", c.level, got, c.want)
		}
	}
}

func TestPolicyLevelOneNeverPublishesOrServes(t *testing.T) {
	p := PolicyFor(1)
	if p.Publish || p.Push || p.Serve || p.ServeBundles {
		t.Fatalf("level 1 must publish and serve nothing: %+v", p)
	}
}

func TestOnlyLevelThreeServesBundles(t *testing.T) {
	for level := -1; level <= 4; level++ {
		if got, want := PolicyFor(level).ServeBundles, level == 3; got != want {
			t.Errorf("level %d: ServeBundles = %v, want %v", level, got, want)
		}
	}
}
