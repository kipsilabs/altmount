package recsync

import (
	"context"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

var ctx = context.Background()

// plant stores payload directly in n's record table, bypassing ingest, as if a
// hostile or buggy node had put it there. seed makes the row unique.
func plant(t *testing.T, n *tnode, nw netid.NetworkID, payload []byte, seed int) {
	t.Helper()
	var rel, hash [32]byte
	rel[0], rel[1] = byte(seed), 0xAB
	hash = records.ContentHash(append([]byte{byte(seed)}, payload...))
	_, err := n.recs.Insert(ctx, database.PeernetRecord{
		NetworkID: nw.String(), Kind: database.PeernetRecordKindRelease, ContentHash: hex.EncodeToString(hash[:]),
		ReleaseID: hex.EncodeToString(rel[:]), PublisherNodeID: n.id.String(), CreatedAt: testNow, ExpiresAt: testNow.Add(24 * time.Hour),
		Source: database.PeernetRecordSourceLocal, Title: "t", TotalSize: 1, Payload: payload,
	}, database.PeernetRecordLimits{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPullStoresRecordsAndAdvancesCursor(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{})
	nw := netOf(1)
	a.join(nw, 2)
	b.join(nw, 1)
	for seed := 1; seed <= 3; seed++ {
		a.publishRelease(t, nw, seed)
	}
	b.peers.set(nw, a.peer())

	res := b.x.SyncRound(ctx)
	if res.Stored != 3 || b.count(t, nw) != 3 {
		t.Fatalf("round = %+v, stored rows = %d", res, b.count(t, nw))
	}
	st, _ := b.recs.Stats(ctx, nw.String())
	if st.Count(database.PeernetRecordKindRelease, database.PeernetRecordSourcePeer) != 3 {
		t.Fatalf("pulled records must be stored as peer sourced: %+v", st)
	}
	cur, _ := b.cur.Get(ctx, nw.String(), a.id.String())
	if cur.PulledSeq != 3 || cur.RemoteEpoch == "" {
		t.Fatalf("cursor = %+v", cur)
	}

	// Nothing new: the next round stores nothing.
	if res := b.x.SyncRound(ctx); res.Stored != 0 {
		t.Fatalf("second round = %+v", res)
	}
	a.publishRelease(t, nw, 4)
	if res := b.x.SyncRound(ctx); res.Stored != 1 || b.count(t, nw) != 4 {
		t.Fatalf("third round = %+v", res)
	}
	if b.x.Status(ctx, nw).LastPull.IsZero() {
		t.Fatal("last successful pull not recorded")
	}
	// After a restart the in-memory time is gone; the persisted cursors still
	// say when records were pulled.
	b.x.mu.Lock()
	b.x.lastPull = map[string]time.Time{}
	b.x.mu.Unlock()
	if b.x.Status(ctx, nw).LastPull.IsZero() {
		t.Fatal("the last pull must survive a restart")
	}
}

func TestPullFollowsMoreFlagAcrossPages(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{limits: Limits{IngestBurst: 1000}}) // one relayer may use half the burst
	nw := netOf(1)
	a.join(nw, 2)
	b.join(nw, 1)
	total := PageRecords + 30
	for i := 1; i <= total; i++ {
		a.publishRelease(t, nw, i)
	}
	b.peers.set(nw, a.peer())
	if res := b.x.SyncRound(ctx); res.Stored != total || b.count(t, nw) != total {
		t.Fatalf("round = %+v, rows = %d", res, b.count(t, nw))
	}
	if got := b.dial.count(http.MethodGet, PathRecords); got != 2 {
		t.Fatalf("%d page requests, want 2", got)
	}
}

func TestForgedRecordIsRejectedAndDoesNotStallThePuller(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{})
	nw := netOf(1)
	a.join(nw, 2)
	b.join(nw, 1)

	forger := mustKey(t)
	_, good, err := records.BuildReleaseRecord(forger, nw, relInput(50), testNow)
	if err != nil {
		t.Fatal(err)
	}
	forged := append([]byte(nil), good...)
	forged[strings.Index(string(forged), "Some.Show")] = 'X'
	plant(t, a, nw, forged, 1)
	a.publishRelease(t, nw, 2) // a valid record behind the forged one
	b.peers.set(nw, a.peer())

	res := b.x.SyncRound(ctx)
	if res.Stored != 1 || b.count(t, nw) != 1 {
		t.Fatalf("round = %+v rows = %d", res, b.count(t, nw))
	}
	if got := b.x.Status(ctx, nw).Rejected["bad_signature"]; got != 1 {
		t.Fatalf("bad_signature counter = %d", got)
	}
	cur, _ := b.cur.Get(ctx, nw.String(), a.id.String())
	if cur.PulledSeq != 2 {
		t.Fatalf("cursor must move past the rejected record, got %d", cur.PulledSeq)
	}
	// The forged payload is nowhere in the store.
	page, _ := b.recs.ListSince(ctx, nw.String(), 0, 10)
	for _, r := range page.Rows {
		if string(r.Payload) == string(forged) {
			t.Fatal("forged record stored")
		}
	}
}

func TestHostilePeerIsBackedOffWithoutAffectingOthers(t *testing.T) {
	lim := Limits{InvalidPerRound: 3, PeerBackoffBase: time.Minute, PeerBackoffMax: 8 * time.Minute}
	bad := newTNode(t, nodeOpts{serve: true})
	good := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{limits: lim})
	nw := netOf(1)
	bad.join(nw, 2)
	good.join(nw, 2)
	b.join(nw, 1)
	for i := 0; i < 5; i++ {
		plant(t, bad, nw, []byte("junk junk junk"), i)
	}
	good.publishRelease(t, nw, 1)
	b.peers.set(nw, bad.peer(), good.peer())

	b.x.SyncRound(ctx)
	if b.count(t, nw) != 1 {
		t.Fatalf("the good peer's record was not stored: %d", b.count(t, nw))
	}
	if st := b.x.Status(ctx, nw); st.PeersBackedOff != 1 || st.PeerBackoffs != 1 || st.Rejected["malformed"] != 5 {
		t.Fatalf("status = %+v", st)
	}
	if cur, _ := b.cur.Get(ctx, nw.String(), bad.id.String()); cur.PulledSeq != 5 {
		t.Fatalf("cursor of the hostile peer = %d (the page was processed, so it advances)", cur.PulledSeq)
	}

	// While backed off the hostile peer is not contacted; the good one is.
	badCalls, goodCalls := b.dial.nodeCalls(bad.id), b.dial.nodeCalls(good.id)
	bad.publishRelease(t, nw, 9)
	good.publishRelease(t, nw, 2)
	b.x.SyncRound(ctx)
	if b.dial.nodeCalls(bad.id) != badCalls || b.dial.nodeCalls(good.id) == goodCalls {
		t.Fatalf("calls bad %d->%d good %d->%d", badCalls, b.dial.nodeCalls(bad.id), goodCalls, b.dial.nodeCalls(good.id))
	}
	if b.count(t, nw) != 2 {
		t.Fatalf("rows = %d", b.count(t, nw))
	}

	// After the back-off it is tried again and its valid record arrives.
	b.clock.Advance(2 * time.Minute)
	b.x.SyncRound(ctx)
	if b.count(t, nw) != 3 || b.x.Status(ctx, nw).PeersBackedOff != 0 {
		t.Fatalf("rows = %d status = %+v", b.count(t, nw), b.x.Status(ctx, nw))
	}
}

func TestBackoffGrowsWithRepeatedMisbehaviourAndIsCapped(t *testing.T) {
	lim := Limits{PeerBackoffBase: time.Minute, PeerBackoffMax: 4 * time.Minute, InvalidPerRound: 1}.WithDefaults()
	bo := newBackoff(lim, 8)
	now := testNow
	nw, peer := netOf(1), mustKey(t).NodeID()
	var waits []time.Duration
	for i := 0; i < 5; i++ {
		bo.strike(nw, peer, now)
		waits = append(waits, bo.until(nw, peer).Sub(now))
	}
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 4 * time.Minute, 4 * time.Minute}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("waits = %v, want %v", waits, want)
		}
	}
	if !bo.active(nw, peer, now) || bo.active(nw, peer, now.Add(5*time.Minute)) {
		t.Fatal("active window wrong")
	}
	bo.clear(nw, peer)
	if bo.active(nw, peer, now) {
		t.Fatal("clear did not clear")
	}
}

func TestBackoffTableIsBounded(t *testing.T) {
	bo := newBackoff(DefaultLimits(), 4)
	nw := netOf(1)
	for i := 0; i < 50; i++ {
		var id identity.NodeID
		id[0] = byte(i + 1)
		bo.strike(nw, id, testNow.Add(time.Duration(i)*time.Second))
	}
	if n := bo.size(); n > 4 {
		t.Fatalf("table holds %d entries", n)
	}
}

func TestEpochChangeResetsThePullCursor(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{})
	nw := netOf(1)
	a.join(nw, 2)
	b.join(nw, 1)
	a.publishRelease(t, nw, 1)
	a.publishRelease(t, nw, 2)
	b.peers.set(nw, a.peer())
	b.x.SyncRound(ctx)
	before, _ := b.cur.Get(ctx, nw.String(), a.id.String())
	if before.PulledSeq != 2 || b.count(t, nw) != 2 {
		t.Fatalf("setup: %+v rows %d", before, b.count(t, nw))
	}

	// a leaves and rejoins: its sequence restarts at 1 with a new epoch.
	if err := a.x.DeleteNetworkData(ctx, nw); err != nil {
		t.Fatal(err)
	}
	a.join(nw, 2)
	a.publishRelease(t, nw, 3)

	b.x.SyncRound(ctx)
	after, _ := b.cur.Get(ctx, nw.String(), a.id.String())
	if after.RemoteEpoch == before.RemoteEpoch || after.PulledSeq != 1 {
		t.Fatalf("cursor after epoch change = %+v (before %+v)", after, before)
	}
	if b.count(t, nw) != 3 {
		t.Fatalf("the record under the restarted sequence was missed: %d rows", b.count(t, nw))
	}
}

func TestLevelOneServesNothingButStillAcceptsPushes(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	c := newTNode(t, nodeOpts{})
	nw := netOf(1)
	a.join(nw, 2)
	a.publishRelease(t, nw, 1)
	a.join(nw, 1) // drops to receive only

	p := c.page(t, a, nw, "0")
	if len(p.Records) != 0 || p.More || p.HighestSeq != 0 || p.Epoch == "" {
		t.Fatalf("level 1 page = %+v", p)
	}
	// It does take a push.
	c.join(nw, 2)
	_, raw, err := records.BuildReleaseRecord(c.key, nw, relInput(77), testNow)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := EncodePush([][]byte{raw})
	resp, err := c.rawPost(t, a, nw, body)
	if err != nil {
		t.Fatal(err)
	}
	rb, _ := resp.ReadAll()
	if res, err := DecodePushResult(rb); err != nil || resp.StatusCode != 200 || res.Accepted != 1 {
		t.Fatalf("push to level 1: %d %+v %v", resp.StatusCode, res, err)
	}
	// ... but still serves nothing of it.
	if p := c.page(t, a, nw, "0"); len(p.Records) != 0 {
		t.Fatalf("level 1 served relayed records: %+v", p)
	}
}

func TestLevelZeroAndUnknownNetworkAnswerIdentically(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	c := newTNode(t, nodeOpts{})
	nwOff, nwUnknown := netOf(1), netOf(2)
	a.join(nwOff, 2)
	a.join(nwOff, 0)

	status := func(nw netid.NetworkID, post bool) (int, string) {
		t.Helper()
		var resp interface {
			ReadAll() ([]byte, error)
		}
		var code int
		if post {
			body, _ := EncodePush([][]byte{{1}})
			r, err := c.rawPost(t, a, nw, body)
			if err != nil {
				t.Fatal(err)
			}
			resp, code = r, r.StatusCode
		} else {
			r, err := c.rawGet(t, a, nw, "0")
			if err != nil {
				t.Fatal(err)
			}
			resp, code = r, r.StatusCode
		}
		b, _ := resp.ReadAll()
		return code, string(b)
	}
	for _, post := range []bool{false, true} {
		c1, b1 := status(nwOff, post)
		c2, b2 := status(nwUnknown, post)
		if c1 != http.StatusForbidden || c1 != c2 || b1 != b2 {
			t.Fatalf("post=%v: off %d %q vs unknown %d %q", post, c1, b1, c2, b2)
		}
	}
}

func TestBadRequestsAreRefused(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	c := newTNode(t, nodeOpts{})
	nw := netOf(1)
	a.join(nw, 2)
	for _, since := range []string{"abc", "-1", "1.5", "99999999999999999999", "1 "} {
		resp, err := c.rawGet(t, a, nw, since)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = resp.ReadAll()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("since=%q: status %d", since, resp.StatusCode)
		}
	}
	for name, body := range map[string][]byte{"garbage": []byte("garbage"), "empty": nil} {
		resp, err := c.rawPost(t, a, nw, body)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = resp.ReadAll()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s push: status %d", name, resp.StatusCode)
		}
	}
	over := protowire.AppendTag(nil, 1, protowire.VarintType)
	over = protowire.AppendVarint(over, 1)
	for i := 0; i <= MaxPushRecords; i++ {
		over = protowire.AppendTag(over, 5, protowire.BytesType)
		over = protowire.AppendBytes(over, []byte{1})
	}
	if resp, err := c.rawPost(t, a, nw, over); err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("too many records: %v %v", resp, err)
	}
	big := make([]byte, MaxPushBody+1)
	if resp, err := c.rawPost(t, a, nw, big); err != nil || resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize body: %v %v", resp, err)
	}
}

func TestPushFromOutboundOnlyPublisher(t *testing.T) {
	o := newTNode(t, nodeOpts{}) // no listener
	r := newTNode(t, nodeOpts{serve: true})
	nw := netOf(1)
	o.join(nw, 2)
	r.join(nw, 1)
	o.publishRelease(t, nw, 1)
	o.publishRelease(t, nw, 2)
	o.peers.set(nw, r.peer())

	o.x.SyncRound(ctx)
	if r.count(t, nw) != 2 {
		t.Fatalf("receiver has %d records", r.count(t, nw))
	}
	st, _ := r.recs.Stats(ctx, nw.String())
	if st.Count(database.PeernetRecordKindRelease, database.PeernetRecordSourcePeer) != 2 {
		t.Fatalf("pushed records must be peer sourced on the receiver: %+v", st)
	}
	cur, _ := o.cur.Get(ctx, nw.String(), r.id.String())
	if cur.PushedSeq != 2 {
		t.Fatalf("push cursor = %d", cur.PushedSeq)
	}
	posts := o.dial.count(http.MethodPost, PathRecords)
	o.x.SyncRound(ctx)
	if o.dial.count(http.MethodPost, PathRecords) != posts {
		t.Fatal("already pushed records were pushed again")
	}
	o.publishRelease(t, nw, 3)
	o.x.SyncRound(ctx)
	if r.count(t, nw) != 3 {
		t.Fatalf("receiver has %d records", r.count(t, nw))
	}
}

func TestLevelOneNodeDoesNotPush(t *testing.T) {
	o := newTNode(t, nodeOpts{})
	r := newTNode(t, nodeOpts{serve: true})
	nw := netOf(1)
	o.join(nw, 2)
	o.publishRelease(t, nw, 1)
	o.join(nw, 1)
	r.join(nw, 1)
	o.peers.set(nw, r.peer())
	o.x.SyncRound(ctx)
	if o.dial.count(http.MethodPost, PathRecords) != 0 || r.count(t, nw) != 0 {
		t.Fatal("a level 1 node pushed")
	}
}

func TestRelayedRecordsAreNotPushed(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{})
	r := newTNode(t, nodeOpts{serve: true})
	nw := netOf(1)
	a.join(nw, 2)
	b.join(nw, 2)
	r.join(nw, 1)
	a.publishRelease(t, nw, 1)
	b.peers.set(nw, a.peer())
	b.x.SyncRound(ctx) // b relays a's record
	b.peers.set(nw, r.peer())
	b.x.SyncRound(ctx)
	if r.count(t, nw) != 0 {
		t.Fatal("pull-only relay: a relayed record must not be pushed")
	}
}

func TestPushResultCountsAndRateLimit(t *testing.T) {
	o := newTNode(t, nodeOpts{})
	r := newTNode(t, nodeOpts{serve: true, limits: Limits{PushRecordsPerMinute: 5}})
	nw := netOf(1)
	o.join(nw, 2)
	r.join(nw, 2)
	_, good, err := records.BuildReleaseRecord(o.key, nw, relInput(1), testNow)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := EncodePush([][]byte{good, good, []byte("junk")})
	resp, err := o.rawPost(t, r, nw, body)
	if err != nil {
		t.Fatal(err)
	}
	rb, _ := resp.ReadAll()
	res, err := DecodePushResult(rb)
	if err != nil || res != (PushResult{Accepted: 1, Duplicate: 1, Rejected: 1}) {
		t.Fatalf("result = %+v %v", res, err)
	}
	if strings.Contains(string(rb), "junk") || len(rb) > 16 {
		t.Fatalf("response echoes content or is large: %q", rb)
	}
	// 3 records used so far; 3 more exceed the budget of 5 per minute.
	resp, err = o.rawPost(t, r, nw, body)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d", resp.StatusCode)
	}
	// The budget refills over time.
	r.clock.Advance(2 * time.Minute)
	resp, _ = o.rawPost(t, r, nw, body)
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("after refill: status %d", resp.StatusCode)
	}
}

func TestPushConcurrencyCap(t *testing.T) {
	o := newTNode(t, nodeOpts{})
	r := newTNode(t, nodeOpts{serve: true, limits: Limits{MaxConcurrentPushes: 1}})
	nw := netOf(1)
	o.join(nw, 2)
	r.join(nw, 2)
	_, good, _ := records.BuildReleaseRecord(o.key, nw, relInput(1), testNow)
	body, _ := EncodePush([][]byte{good})
	r.x.pushSlots <- struct{}{} // all slots busy
	resp, err := o.rawPost(t, r, nw, body)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d", resp.StatusCode)
	}
	<-r.x.pushSlots
	resp, _ = o.rawPost(t, r, nw, body)
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d after the slot freed", resp.StatusCode)
	}
}

func TestPublishRules(t *testing.T) {
	n := newTNode(t, nodeOpts{})
	other := mustKey(t)
	nw := netOf(1)
	_, mine, _ := records.BuildReleaseRecord(n.key, nw, relInput(1), testNow)
	_, theirs, _ := records.BuildReleaseRecord(other, nw, relInput(2), testNow)

	if _, err := n.x.Publish(ctx, nw, mine); err != ErrNotPublishing {
		t.Fatalf("unknown network: %v", err)
	}
	n.join(nw, 1)
	if _, err := n.x.Publish(ctx, nw, mine); err != ErrNotPublishing {
		t.Fatalf("level 1: %v", err)
	}
	if got := n.x.PublishNetworks(); len(got) != 0 {
		t.Fatalf("publish networks at level 1 = %v", got)
	}
	n.join(nw, 2)
	n.join(netOf(2), 3)
	n.join(netOf(3), 1)
	got := n.x.PublishNetworks()
	if len(got) != 2 || got[0] != nw || got[1] != netOf(2) {
		t.Fatalf("publish networks = %v", got)
	}
	if out, err := n.x.Publish(ctx, nw, theirs); err != nil || out.Reason != ReasonNotOwn {
		t.Fatalf("foreign key: %+v %v", out, err)
	}
	if out, err := n.x.Publish(ctx, nw, mine); err != nil || out.Result != ResultStored {
		t.Fatalf("own: %+v %v", out, err)
	}
	if out, _ := n.x.Publish(ctx, nw, mine); out.Result != ResultDuplicate {
		t.Fatalf("republish: %+v", out)
	}
	n.x.RemoveNetwork(nw)
	if _, err := n.x.Publish(ctx, nw, mine); err != ErrNotPublishing {
		t.Fatalf("removed network: %v", err)
	}
}

func TestRemovedNetworkStopsImmediately(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{})
	nw := netOf(1)
	a.join(nw, 2)
	b.join(nw, 1)
	a.publishRelease(t, nw, 1)
	b.peers.set(nw, a.peer())

	a.x.RemoveNetwork(nw)
	resp, err := b.rawGet(t, a, nw, "0")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = resp.ReadAll()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("server answered for a removed network: %d", resp.StatusCode)
	}
	a.join(nw, 2)
	b.x.RemoveNetwork(nw)
	b.x.SyncRound(ctx)
	if b.dial.count(http.MethodGet, PathRecords) != 0 || b.count(t, nw) != 0 {
		t.Fatalf("a removed network was still pulled (%d calls, %d rows)", b.dial.count(http.MethodGet, PathRecords), b.count(t, nw))
	}
}

func TestMaintainPurgesInBatchesAndCleansOrphans(t *testing.T) {
	n := newTNode(t, nodeOpts{limits: Limits{PurgeBatch: 2, PurgeBatchesPerPass: 2}})
	nw, gone := netOf(1), netOf(2)
	early := database.NewPeernetRecordRepositoryWithClock(n.db.Connection(), n.db.Dialect(), func() time.Time { return testNow.Add(-48 * time.Hour) })
	for i := 0; i < 5; i++ {
		var rel, hash [32]byte
		rel[0], hash[0] = byte(i+1), byte(i+1)
		_, err := early.Insert(ctx, database.PeernetRecord{
			NetworkID: nw.String(), Kind: database.PeernetRecordKindRelease, ContentHash: hex.EncodeToString(hash[:]),
			ReleaseID: hex.EncodeToString(rel[:]), PublisherNodeID: n.id.String(), CreatedAt: testNow.Add(-72 * time.Hour),
			ExpiresAt: testNow.Add(-24 * time.Hour), Source: database.PeernetRecordSourcePeer, Payload: []byte("p"),
		}, database.PeernetRecordLimits{})
		if err != nil {
			t.Fatal(err)
		}
	}
	plant(t, n, gone, []byte("orphan"), 1)

	known := func(context.Context) (map[string]bool, error) { return map[string]bool{nw.String(): true}, nil }
	res := n.x.Maintain(ctx, known)
	if res.Purged != 4 || res.Err != nil {
		t.Fatalf("first pass = %+v", res)
	}
	if res = n.x.Maintain(ctx, known); res.Purged != 1 {
		t.Fatalf("second pass = %+v", res)
	}
	if res.OrphanNetworks != 0 {
		t.Fatalf("orphans were already removed in the first pass: %+v", res)
	}
	ids, _ := n.recs.NetworkIDs(ctx)
	for _, id := range ids {
		if id == gone.String() {
			t.Fatal("records of a network that no longer exists were kept")
		}
	}
}

func TestStatusReportsCountsPublishersAndCounters(t *testing.T) {
	a := newTNode(t, nodeOpts{serve: true})
	b := newTNode(t, nodeOpts{})
	nw := netOf(1)
	a.join(nw, 2)
	b.join(nw, 2)
	a.publishRelease(t, nw, 1)
	b.publishRelease(t, nw, 2)
	b.peers.set(nw, a.peer())
	b.x.SyncRound(ctx)
	st := b.x.Status(ctx, nw)
	if st.Releases.Local != 1 || st.Releases.Peer != 1 || st.Attestations.Local != 0 || st.Publishers != 2 ||
		st.Stored != 2 || st.LastPull.IsZero() {
		t.Fatalf("status = %+v", st)
	}
}
