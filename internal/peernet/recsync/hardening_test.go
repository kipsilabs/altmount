package recsync

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

func TestPullStopsWithoutMovingTheCursorWhenTheIngestRateIsExhausted(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{limits: Limits{IngestRate: 0.001, IngestBurst: 4}})
	nw := netOf(1)
	a.join(nw, 2)
	b.join(nw, 1)
	for i := 1; i <= 5; i++ {
		a.publishRelease(t, nw, i)
	}
	b.peers.set(nw, a.peer())
	res := b.x.SyncRound(ctx)
	if res.Err != nil || !res.Backlog || b.count(t, nw) != 2 {
		t.Fatalf("round %+v, stored %d", res, b.count(t, nw))
	}
	if cur, _ := b.cur.Get(ctx, nw.String(), a.id.String()); cur.PulledSeq != 0 {
		t.Fatalf("cursor advanced to %d past unprocessed records", cur.PulledSeq)
	}
}

func TestEpochFlippingIsBoundedPerHour(t *testing.T) {
	d := &cannedDialer{reply: func(_ transport.Request, call int) (int, []byte) {
		b, _ := EncodePage(Page{Epoch: fmt.Sprintf("%016d", call), HighestSeq: 0})
		return 200, b
	}}
	n, peer := newCannedNode(t, Limits{MaxEpochResetsPerHour: 2}, d)
	nw := netOf(1)
	n.join(nw, 1)
	n.peers.set(nw, peer)
	for i := 0; i < 4; i++ { // first contact, then flips 1, 2 and the refused 3rd
		n.x.SyncRound(ctx)
	}
	if st := n.x.Status(ctx, nw); st.PeersBackedOff != 1 {
		t.Fatalf("status %+v: a peer that keeps changing its epoch must be backed off", st)
	}
	if cur, _ := n.cur.Get(ctx, nw.String(), peer.NodeID.String()); cur.RemoteEpoch != fmt.Sprintf("%016d", 3) {
		t.Fatalf("epoch %q: the refused reset must not be applied", cur.RemoteEpoch)
	}
}

func TestPushBodyIsReadBeforeAGlobalSlotIsTaken(t *testing.T) {
	o := newTNode(t, nodeOpts{})
	r := newTNode(t, nodeOpts{serve: true, limits: Limits{MaxConcurrentPushes: 1}})
	nw := netOf(1)
	o.join(nw, 2)
	r.join(nw, 2)
	r.x.pushSlots <- struct{}{} // every ingest slot busy
	resp, err := o.rawPost(t, r, nw, []byte("not a push body"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d: the body must be read and rejected before a slot is needed", resp.StatusCode)
	}
}

func TestConcurrentPushesPerNodeAndPerIPAreCapped(t *testing.T) {
	o := newTNode(t, nodeOpts{})
	r := newTNode(t, nodeOpts{serve: true, limits: Limits{MaxPushesPerNode: 1, MaxPushesPerIP: 1}})
	nw := netOf(1)
	o.join(nw, 2)
	r.join(nw, 2)
	body, _ := EncodePush([][]byte{[]byte("x")})

	release, ok := r.x.pushGuard.enter(o.id.String(), "")
	if !ok {
		t.Fatal("first push refused")
	}
	resp, _ := o.rawPost(t, r, nw, body)
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second push of the same node: status %d", resp.StatusCode)
	}
	release()

	release, _ = r.x.pushGuard.enter("some other node", "127.0.0.1")
	other := newTNode(t, nodeOpts{})
	other.join(nw, 2)
	resp, _ = other.rawPost(t, r, nw, body)
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second push from the same address: status %d", resp.StatusCode)
	}
	release()
}

func TestHostilePushersAreBackedOffByNode(t *testing.T) {
	o := newTNode(t, nodeOpts{})
	r := newTNode(t, nodeOpts{serve: true, limits: Limits{InvalidPerRound: 3, PeerBackoffBase: time.Minute}})
	nw := netOf(1)
	o.join(nw, 2)
	r.join(nw, 2)
	junk, _ := EncodePush([][]byte{[]byte("junk 1"), []byte("junk 2"), []byte("junk 3")})
	resp, _ := o.rawPost(t, r, nw, junk)
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first push: status %d", resp.StatusCode)
	}
	// Now refused by node ID.
	resp, _ = o.rawPost(t, r, nw, junk)
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("hostile pusher not refused: status %d", resp.StatusCode)
	}
	// A different node ID from the same (loopback) address is not affected.
	other := newTNode(t, nodeOpts{})
	other.join(nw, 2)
	resp, _ = other.rawPost(t, r, nw, junk)
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("an honest pusher behind the same address was refused: status %d", resp.StatusCode)
	}
	// After the back-off it is accepted again.
	r.clock.Advance(2 * time.Minute)
	resp, _ = o.rawPost(t, r, nw, junk)
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("after back-off: status %d", resp.StatusCode)
	}
}

func TestMaintainReconcilesDriftedStoreCounters(t *testing.T) {
	n := newTNode(t, nodeOpts{})
	nw := netOf(1)
	n.join(nw, 2)
	n.publishRelease(t, nw, 1)
	if _, err := n.db.Connection().ExecContext(ctx, `UPDATE peernet_record_counts SET n = n + 9`); err != nil {
		t.Fatal(err)
	}
	if diff, _ := n.recs.CheckCounts(ctx, nw.String()); len(diff) == 0 {
		t.Fatal("setup: no drift")
	}
	res := n.x.Maintain(ctx, nil)
	if res.Reconciled == 0 {
		t.Fatalf("maintain result %+v", res)
	}
	if diff, _ := n.recs.CheckCounts(ctx, nw.String()); len(diff) != 0 {
		t.Fatalf("still drifted: %+v", diff)
	}
}

func TestHostilityDoesNotDeleteRecordsRelayedFromHonestPublishers(t *testing.T) {
	honest := newTNode(t, nodeOpts{serve: true})
	bad := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{limits: Limits{InvalidPerRound: 3}})
	nw := netOf(1)
	honest.join(nw, 2)
	bad.join(nw, 2)
	b.join(nw, 1)
	honest.publishRelease(t, nw, 1)
	// bad relays the honest record first, then serves forged ones.
	bad.peers.set(nw, honest.peer())
	bad.join(nw, 2)
	if res := bad.x.SyncRound(ctx); res.Stored != 1 {
		t.Fatalf("setup: %+v", res)
	}
	for i := 0; i < 5; i++ {
		plant(t, bad, nw, []byte("junk junk junk"), i)
	}
	b.peers.set(nw, bad.peer())
	b.x.SyncRound(ctx)
	if st := b.x.Status(ctx, nw); st.PeersBackedOff != 1 {
		t.Fatalf("status %+v: the hostile relayer must be backed off", st)
	}
	if n := b.count(t, nw); n < 1 {
		t.Fatalf("%d rows: records of an honest publisher must survive the relayer's misbehaviour", n)
	}
}

func TestHostilePusherCannotDeleteWhatItRelayed(t *testing.T) {
	honest := newTNode(t, nodeOpts{})
	o := newTNode(t, nodeOpts{})
	r := newTNode(t, nodeOpts{serve: true, limits: Limits{InvalidPerRound: 3}})
	nw := netOf(1)
	honest.join(nw, 2)
	o.join(nw, 2)
	r.join(nw, 2)
	raw := honest.publishRelease(t, nw, 1)
	body, _ := EncodePush([][]byte{raw})
	resp, _ := o.rawPost(t, r, nw, body)
	_, _ = resp.ReadAll()
	junk, _ := EncodePush([][]byte{[]byte("junk 1"), []byte("junk 2"), []byte("junk 3")})
	resp, _ = o.rawPost(t, r, nw, junk)
	_, _ = resp.ReadAll()
	if n := r.count(t, nw); n != 1 {
		t.Fatalf("%d rows: the honest record the pusher relayed must survive", n)
	}
}

func TestSlowSyncOfAlreadyHeldRecordsIsNotPunished(t *testing.T) {
	const total = 10000
	d := &cannedDialer{reply: func(req transport.Request, _ int) (int, []byte) {
		since := sinceOf(req)
		next := min(since+PageRecords, total)
		b, _ := EncodePage(Page{Epoch: strings.Repeat("a", 16), HighestSeq: next, More: next < total})
		return 200, b
	}}
	n, peer := newCannedNode(t, Limits{MaxPagesPerPeerRound: 20}, d)
	nw := netOf(1)
	n.join(nw, 1)
	n.peers.set(nw, peer)
	for round := 0; round < 5; round++ {
		n.x.SyncRound(ctx)
		if st := n.x.Status(ctx, nw); st.PeersBackedOff != 0 {
			t.Fatalf("round %d: status %+v: a peer serving records we already hold must not be backed off", round, st)
		}
	}
	if cur, _ := n.cur.Get(ctx, nw.String(), peer.NodeID.String()); cur.PulledSeq != total {
		t.Fatalf("cursor = %d, want the sync to reach the end (%d)", cur.PulledSeq, total)
	}
}

func TestIPBackoffNeedsHostileStrikesFromDistinctNodes(t *testing.T) {
	r := newTNode(t, nodeOpts{limits: Limits{InvalidPerRound: 3, PeerBackoffBase: time.Minute}})
	nw := netOf(1)
	r.join(nw, 2)
	const ip = "203.0.113.7"
	n1, n2, n3, bystander := mustKey(t).NodeID(), mustKey(t).NodeID(), mustKey(t).NodeID(), mustKey(t).NodeID()
	r.x.notePushHostility(ctx, nw, n1, ip, 3)
	r.x.notePushHostility(ctx, nw, n1, ip, 3) // the same node again is still one node
	if !r.x.pusherBackedOff(nw, n1, ip) {
		t.Fatal("the hostile node itself must be backed off")
	}
	if r.x.pusherBackedOff(nw, bystander, ip) {
		t.Fatal("one hostile node must not block everyone behind its address")
	}
	r.x.notePushHostility(ctx, nw, n2, ip, 3)
	if r.x.pusherBackedOff(nw, bystander, ip) {
		t.Fatal("two hostile nodes are not enough to strike the address")
	}
	r.x.notePushHostility(ctx, nw, n3, ip, 3)
	if !r.x.pusherBackedOff(nw, bystander, ip) {
		t.Fatal("three distinct hostile nodes must strike the address")
	}
}

func TestPrivateAddressesAreNeverStruck(t *testing.T) {
	r := newTNode(t, nodeOpts{limits: Limits{InvalidPerRound: 3, PeerBackoffBase: time.Minute}})
	nw := netOf(1)
	r.join(nw, 2)
	for _, ip := range []string{"192.168.1.5", "10.0.0.2", "127.0.0.1", "169.254.1.1", "::1", "fe80::1", "fd00::1"} {
		for i := 0; i < 4; i++ {
			r.x.notePushHostility(ctx, nw, mustKey(t).NodeID(), ip, 3)
		}
		if r.x.pusherBackedOff(nw, mustKey(t).NodeID(), ip) {
			t.Fatalf("%s: private addresses are shared by honest nodes and must not be struck", ip)
		}
	}
}
