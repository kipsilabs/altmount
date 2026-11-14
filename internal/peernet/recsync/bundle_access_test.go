package recsync

import "testing"

func TestExchangeNetworkLevelAndPeerBackedOff(t *testing.T) {
	n := newTNode(t, nodeOpts{})
	nw := netOf(5)
	if _, ok := n.x.NetworkLevel(nw); ok {
		t.Fatal("unregistered network has a level")
	}
	n.join(nw, 3)
	if lvl, ok := n.x.NetworkLevel(nw); !ok || lvl != 3 {
		t.Fatalf("level = %d, %v", lvl, ok)
	}
	n.join(nw, 0)
	if _, ok := n.x.NetworkLevel(nw); ok {
		t.Fatal("level 0 network still registered")
	}

	other := mustKey(t).NodeID()
	if n.x.PeerBackedOff(nw, other, "") {
		t.Fatal("fresh peer is backed off")
	}
	n.x.penalize(nw, other)
	if !n.x.PeerBackedOff(nw, other, "") {
		t.Fatal("penalised peer is not backed off")
	}
	if n.x.PeerBackedOff(netOf(6), other, "") {
		t.Fatal("back-off leaked to another network")
	}
}
