package service

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// The live run had the open network at level 0 and a private network at level
// 3: the status must report each network's own level (the open one from the
// configuration, the private one from its row).
func TestStatusReportsEachNetworksOwnLevel(t *testing.T) {
	s := enabledSettings()
	s.OpenLevel, s.AcceptIncoming = 0, true
	i := newInstance(t, s)
	i.start(t)
	ctx := context.Background()
	nw, err := i.svc.CreateNetwork(ctx, "home")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := netid.ParseNetworkID(nw.ID)
	if _, err := i.svc.UpdateNetwork(ctx, id, NetworkUpdate{Level: ptr(3)}); err != nil {
		t.Fatal(err)
	}
	st := i.statusOf(t)
	levels := map[string]int{}
	for _, n := range st.Networks {
		levels[n.Kind] = n.Level
	}
	if levels[KindOpen] != 0 || levels[KindPrivate] != 3 {
		t.Fatalf("levels by kind = %v", levels)
	}
}
