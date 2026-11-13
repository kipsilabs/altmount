package service

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

func releaseInput(seed int) records.ReleaseRecord {
	var id records.ReleaseID
	for i := range id {
		id[i] = 5
	}
	id[0], id[1] = byte(seed), byte(seed>>8)
	return records.ReleaseRecord{ReleaseID: id, Title: "Some.Show.S01E01", TotalSize: 1 << 30, FileCount: 2}
}

func attestationInput(seed int) records.HealthAttestation {
	var rid records.ReleaseID
	var fid records.FileID
	rid[0], fid[0], fid[1] = byte(seed), byte(seed), 1
	return records.HealthAttestation{ReleaseID: rid, FileID: fid, CheckedAt: time.Now().Add(-time.Minute),
		TotalSegments: 50, SegmentsChecked: 10, MediaHeaderVerified: true, Backbones: []string{"omicron"}}
}

// publish builds a release record through the signer facade and publishes it.
func (i *instance) publish(t *testing.T, nw netid.NetworkID, seed int) []byte {
	t.Helper()
	sig, err := i.svc.Signer()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sig.BuildRelease(nw, releaseInput(seed), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	out, err := i.svc.PublishLocal(context.Background(), nw, raw)
	if err != nil || out.Result != recsync.ResultStored {
		t.Fatalf("publish: %+v %v", out, err)
	}
	return raw
}

func (i *instance) syncRound(t *testing.T) recsync.RoundResult {
	t.Helper()
	return i.node(t).rx.SyncRound(context.Background())
}

func (i *instance) stored(t *testing.T, nw netid.NetworkID) []database.PeernetRecordRow {
	t.Helper()
	page, err := i.recs.ListSince(context.Background(), nw.String(), 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	return page.Rows
}

// connect makes from's discovery verify to as a peer (from has to's address
// configured as an open-network peer).
func connect(t *testing.T, from, to *instance, nw netid.NetworkID) {
	t.Helper()
	eventually(t, "peer verified", func() bool {
		from.node(t).step(context.Background())
		return from.peerVerified(t, nw, to.id(t))
	})
}

func openPair(t *testing.T, levelA, levelB int) (a, b *instance, aset, bset Settings) {
	t.Helper()
	a, aset = listeningInstance(t, Settings{OpenLevel: levelA})
	b, bset = listeningInstance(t, Settings{OpenLevel: levelB})
	bset.OpenPeers = []string{a.addr(t)}
	b.svc.Apply(context.Background(), bset)
	connect(t, b, a, netid.Open())
	return a, b, aset, bset
}

func TestRecordsPullBetweenTwoNodes(t *testing.T) {
	a, b, _, _ := openPair(t, 2, 1)
	raw := a.publish(t, netid.Open(), 1)
	a.publish(t, netid.Open(), 2)

	if res := b.syncRound(t); res.Stored != 2 {
		t.Fatalf("round = %+v", res)
	}
	rows := b.stored(t, netid.Open())
	if len(rows) != 2 || string(rows[0].Payload) != string(raw) {
		t.Fatalf("b stored %d records", len(rows))
	}
	st := memberStatus(t, b, netid.Open())
	if st.Records.Releases.Peer != 2 || st.Records.Releases.Local != 0 || st.Records.Publishers != 1 || st.Records.LastPull.IsZero() {
		t.Fatalf("status = %+v", st.Records)
	}
	sa := memberStatus(t, a, netid.Open())
	if sa.Records.Releases.Local != 2 {
		t.Fatalf("publisher status = %+v", sa.Records)
	}
}

func TestOutboundOnlyPublisherReachesPeerByPush(t *testing.T) {
	ctx := context.Background()
	r, _ := listeningInstance(t, Settings{OpenLevel: 1})
	o := newInstance(t, Settings{Enabled: true, OpenLevel: 2, OpenPeers: []string{r.addr(t)}})
	o.start(t)
	if st, _ := o.svc.Status(ctx); st.Listening {
		t.Fatal("the publisher must not listen")
	}
	connect(t, o, r, netid.Open())
	raw := o.publish(t, netid.Open(), 1)

	if res := o.syncRound(t); res.Pushed != 1 {
		t.Fatalf("round = %+v", res)
	}
	rows := r.stored(t, netid.Open())
	if len(rows) != 1 || string(rows[0].Payload) != string(raw) {
		t.Fatalf("receiver stored %d records", len(rows))
	}
	if o.syncRound(t).Pushed != 0 {
		t.Fatal("pushed the same record twice")
	}
}

func TestLevelOneServesNothingAndRelayedRecordStillVerifies(t *testing.T) {
	ctx := context.Background()
	// a publishes; b (level 1) pulls from a; c pulls only from b.
	a, b, _, bset := openPair(t, 2, 1)
	c, cset := listeningInstance(t, Settings{OpenLevel: 1})
	cset.OpenPeers = []string{b.addr(t)}
	c.svc.Apply(ctx, cset)
	connect(t, c, b, netid.Open())

	raw := a.publish(t, netid.Open(), 1)
	if res := b.syncRound(t); res.Stored != 1 {
		t.Fatalf("b round = %+v", res)
	}

	// The publisher goes away. c may learn a's address from b by peer
	// exchange and would pull from it directly (when that happens depends on
	// timing), which is not what this test is about: from here on b is the
	// only node c can get records from.
	aID := a.id(t)
	a.svc.Stop(ctx)

	// b is receive only: it holds a's record but serves nothing.
	c.syncRound(t)
	if got := c.stored(t, netid.Open()); len(got) != 0 {
		t.Fatalf("a level 1 node served %d records", len(got))
	}
	// The empty result must come from a real exchange with b, not from c never
	// having talked to it: a level 1 server answers with an empty page that
	// still carries its epoch.
	if cur, _ := c.cur.Get(ctx, netid.Open().String(), b.id(t).String()); cur.RemoteEpoch == "" {
		t.Fatal("c never synced with b in the level 1 round, so the empty result proves nothing")
	}

	// b moves to level 2 and now relays a's record.
	bset.OpenLevel = 2
	b.svc.Apply(ctx, bset)
	if res := c.syncRound(t); res.Stored != 1 {
		t.Fatalf("c round = %+v", res)
	}
	got := c.stored(t, netid.Open())
	if len(got) != 1 || string(got[0].Payload) != string(raw) {
		t.Fatalf("c stored %d records", len(got))
	}
	// The relayed record is the publisher's, untouched, and still verifies.
	rec, err := records.DecodeReleaseRecord(got[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Verify(time.Now(), netid.Open()); err != nil {
		t.Fatalf("relayed record does not verify: %v", err)
	}
	if !rec.PublisherID.Equal(aID) {
		t.Fatal("the publisher changed in relay")
	}
	// c got it from b, never from a.
	if cur, _ := c.cur.Get(ctx, netid.Open().String(), aID.String()); cur.PulledSeq != 0 {
		t.Fatalf("c pulled from the publisher directly: %+v", cur)
	}
	if cur, _ := c.cur.Get(ctx, netid.Open().String(), b.id(t).String()); cur.PulledSeq == 0 {
		t.Fatal("c did not pull from the relay")
	}
}

func rawRecordsGet(t *testing.T, from, to *instance, nw string) int {
	t.Helper()
	resp, err := from.node(t).client.Do(context.Background(), to.addr(t), to.id(t), transport.Request{
		Method: http.MethodGet, Path: recsync.PathRecords, Query: url.Values{"n": {nw}},
	}, transport.CallOptions{MaxResponseBytes: recsync.MaxPageBytes, TrustedAddress: true})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = resp.ReadAll()
	return resp.StatusCode
}

func TestLevelZeroAnswersAsIfTheNetworkDoesNotExist(t *testing.T) {
	off, _ := listeningInstance(t, Settings{OpenLevel: 0})
	asker, _ := listeningInstance(t, Settings{OpenLevel: 1})
	unknown := strings.Repeat("ab", 32)
	got := rawRecordsGet(t, asker, off, netid.Open().String())
	if got != http.StatusForbidden || got != rawRecordsGet(t, asker, off, unknown) {
		t.Fatalf("level 0 answered %d", got)
	}
	// The same node at level 1 does answer.
	on, _ := listeningInstance(t, Settings{OpenLevel: 1})
	if code := rawRecordsGet(t, asker, on, netid.Open().String()); code != http.StatusOK {
		t.Fatalf("level 1 answered %d", code)
	}
}

func TestForgedRecordFromAPeerIsRejected(t *testing.T) {
	ctx := context.Background()
	a, b, _, _ := openPair(t, 2, 1)

	forger, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	_, good, err := records.BuildReleaseRecord(forger, netid.Open(), releaseInput(40), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	forged := append([]byte(nil), good...)
	forged[strings.Index(string(forged), "Some.Show")] = 'X'
	var rel, hash [32]byte
	rel[0], hash[0] = 99, 99
	if _, err := a.recs.Insert(ctx, database.PeernetRecord{
		NetworkID: netid.Open().String(), Kind: database.PeernetRecordKindRelease, ContentHash: hex.EncodeToString(hash[:]),
		ReleaseID: hex.EncodeToString(rel[:]), PublisherNodeID: forger.NodeID().String(), CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour), Source: database.PeernetRecordSourcePeer, Title: "x", Payload: forged,
	}, database.PeernetRecordLimits{}); err != nil {
		t.Fatal(err)
	}
	a.publish(t, netid.Open(), 1)

	if res := b.syncRound(t); res.Stored != 1 || res.Rejected != 1 {
		t.Fatalf("round = %+v", res)
	}
	for _, r := range b.stored(t, netid.Open()) {
		if string(r.Payload) == string(forged) {
			t.Fatal("forged record stored")
		}
	}
	if got := memberStatus(t, b, netid.Open()).Records.Rejected["bad_signature"]; got != 1 {
		t.Fatalf("rejected = %d", got)
	}
}

func TestPublisherSequenceRestartIsHandled(t *testing.T) {
	ctx := context.Background()
	a, b, _, _ := openPair(t, 2, 1)
	a.publish(t, netid.Open(), 1)
	a.publish(t, netid.Open(), 2)
	b.syncRound(t)
	if len(b.stored(t, netid.Open())) != 2 {
		t.Fatal("setup failed")
	}
	// a's record store is reset: the sequence restarts under a new epoch.
	if err := a.recs.DeleteNetwork(ctx, netid.Open().String()); err != nil {
		t.Fatal(err)
	}
	a.publish(t, netid.Open(), 3)
	if res := b.syncRound(t); res.Stored != 1 {
		t.Fatalf("round after restart = %+v", res)
	}
	cur, _ := b.cur.Get(ctx, netid.Open().String(), a.id(t).String())
	if cur.PulledSeq != 1 {
		t.Fatalf("cursor = %+v", cur)
	}
}

// privatePair joins member to a private network hosted by host (both at the
// given level) and also connects member to host on the open network.
func privatePair(t *testing.T, level int) (host, member *instance, nw netid.NetworkID) {
	t.Helper()
	ctx := context.Background()
	host, _ = listeningInstance(t, Settings{OpenLevel: 2})
	member, mset := listeningInstance(t, Settings{OpenLevel: 1})
	mset.OpenPeers = []string{host.addr(t)}
	member.svc.Apply(ctx, mset)

	ns, err := host.svc.CreateNetwork(ctx, "Club")
	if err != nil {
		t.Fatal(err)
	}
	nw, _ = netid.ParseNetworkID(ns.ID)
	if _, err := host.svc.UpdateNetwork(ctx, nw, NetworkUpdate{Level: ptr(2)}); err != nil {
		t.Fatal(err)
	}
	link, err := host.svc.CreateInvite(ctx, nw, time.Hour, "m")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.svc.JoinNetwork(ctx, link, false); err != nil {
		t.Fatal(err)
	}
	if _, err := member.svc.UpdateNetwork(ctx, nw, NetworkUpdate{Level: ptr(level)}); err != nil {
		t.Fatal(err)
	}
	for r := 0; r < 3; r++ {
		member.node(t).step(ctx)
		host.node(t).step(ctx)
	}
	if !member.peerVerified(t, nw, host.id(t)) || !member.peerVerified(t, netid.Open(), host.id(t)) {
		t.Fatal("member did not connect on both networks")
	}
	return host, member, nw
}

func TestPrivateAndOpenRecordsNeverMix(t *testing.T) {
	ctx := context.Background()
	host, member, nw := privatePair(t, 1)

	openRaw := host.publish(t, netid.Open(), 1)
	privRaw := host.publish(t, nw, 2)

	// A record built for one network is refused in the other, at publish time.
	sig, _ := host.svc.Signer()
	wrong, _ := sig.BuildRelease(nw, releaseInput(3), time.Now())
	if out, err := host.svc.PublishLocal(ctx, netid.Open(), wrong); err != nil || out.Reason != recsync.ReasonWrongNetwork {
		t.Fatalf("cross-network publish: %+v %v", out, err)
	}

	if res := member.syncRound(t); res.Stored != 2 {
		t.Fatalf("round = %+v", res)
	}
	op, pr := member.stored(t, netid.Open()), member.stored(t, nw)
	if len(op) != 1 || string(op[0].Payload) != string(openRaw) || len(pr) != 1 || string(pr[0].Payload) != string(privRaw) {
		t.Fatalf("open has %d, private has %d records", len(op), len(pr))
	}
	for _, r := range op {
		env, _ := records.Peek(r.Payload)
		if !env.NetworkID.IsOpen() {
			t.Fatal("a private record is in the open network")
		}
	}

	// A non-member asking for the private network's records is refused.
	outsider, _ := listeningInstance(t, Settings{OpenLevel: 1})
	if code := rawRecordsGet(t, outsider, host, nw.String()); code != http.StatusForbidden {
		t.Fatalf("outsider got %d", code)
	}
}

func TestLeavingANetworkDeletesItsRecords(t *testing.T) {
	ctx := context.Background()
	host, member, nw := privatePair(t, 1)
	host.publish(t, netid.Open(), 1)
	host.publish(t, nw, 2)
	member.syncRound(t)
	if len(member.stored(t, nw)) != 1 || len(member.stored(t, netid.Open())) != 1 {
		t.Fatal("setup failed")
	}
	if err := member.svc.DeleteNetwork(ctx, nw); err != nil {
		t.Fatal(err)
	}
	if got := member.stored(t, nw); len(got) != 0 {
		t.Fatalf("%d records survived leaving", len(got))
	}
	if cur, _ := member.cur.Get(ctx, nw.String(), host.id(t).String()); cur.PulledSeq != 0 {
		t.Fatalf("cursor survived: %+v", cur)
	}
	if len(member.stored(t, netid.Open())) != 1 {
		t.Fatal("the open network's records were deleted too")
	}
	// The host deleting its network clears its own records of it.
	if err := host.svc.DeleteNetwork(ctx, nw); err != nil {
		t.Fatal(err)
	}
	if got := host.stored(t, nw); len(got) != 0 {
		t.Fatalf("%d host records survived deletion", len(got))
	}
}

func TestPublishNetworksListsOnlyPublishingNetworks(t *testing.T) {
	ctx := context.Background()
	host, member, nw := privatePair(t, 1)
	if got := host.svc.PublishNetworks(); len(got) != 2 {
		t.Fatalf("host publishes to %v", got)
	}
	got := member.svc.PublishNetworks()
	if len(got) != 0 {
		t.Fatalf("a level 1 member publishes to %v", got)
	}
	if _, err := member.svc.UpdateNetwork(ctx, nw, NetworkUpdate{Level: ptr(2)}); err != nil {
		t.Fatal(err)
	}
	got = member.svc.PublishNetworks()
	if len(got) != 1 || got[0] != nw {
		t.Fatalf("member publishes to %v", got)
	}
	// Publishing into a network this node does not publish to is refused.
	sig, _ := member.svc.Signer()
	raw, _ := sig.BuildRelease(netid.Open(), releaseInput(1), time.Now())
	if _, err := member.svc.PublishLocal(ctx, netid.Open(), raw); err != recsync.ErrNotPublishing {
		t.Fatalf("publish at level 1: %v", err)
	}
	// A removed member's network is no longer listed.
	if err := host.svc.RemoveMember(ctx, nw, member.id(t)); err != nil {
		t.Fatal(err)
	}
	// The member finds out from the host's refusal.
	if _, err := member.node(t).disc.Exchange(ctx, discovery.NetworkConfig{ID: nw}, host.addr(t), host.id(t)); err == nil {
		t.Fatal("removed member still admitted")
	}
	eventually(t, "revoked network unlisted", func() bool { return len(member.svc.PublishNetworks()) == 0 })
	if st := memberStatus(t, member, nw); !st.Revoked {
		t.Fatalf("status = %+v", st)
	}
}

func TestSignerAndPublishWhenDisabled(t *testing.T) {
	i := newInstance(t, Settings{})
	i.start(t)
	if _, err := i.svc.Signer(); err != ErrDisabled {
		t.Fatalf("Signer = %v", err)
	}
	if _, err := i.svc.PublishLocal(context.Background(), netid.Open(), []byte("x")); err != ErrDisabled {
		t.Fatalf("PublishLocal = %v", err)
	}
	if got := i.svc.PublishNetworks(); len(got) != 0 {
		t.Fatalf("PublishNetworks = %v", got)
	}
}

func TestSignerBuildsVerifiableRecordsWithoutExposingTheKey(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true, OpenLevel: 2})
	i.start(t)
	sig, err := i.svc.Signer()
	if err != nil {
		t.Fatal(err)
	}
	if !sig.NodeID().Equal(i.id(t)) {
		t.Fatal("signer id differs from the node id")
	}
	raw, err := sig.BuildAttestation(netid.Open(), attestationInput(1), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	a, err := records.DecodeHealthAttestation(raw)
	if err != nil || a.Verify(time.Now(), netid.Open()) != nil || !a.PublisherID.Equal(i.id(t)) {
		t.Fatalf("attestation: %v", err)
	}
	if out, err := i.svc.PublishLocal(context.Background(), netid.Open(), raw); err != nil || out.Result != recsync.ResultStored {
		t.Fatalf("publish attestation: %+v %v", out, err)
	}
	st := memberStatus(t, i, netid.Open())
	if st.Records.Attestations.Local != 1 {
		t.Fatalf("status = %+v", st.Records)
	}
}

func TestMaintenanceRemovesRecordsOfNetworksThatNoLongerExist(t *testing.T) {
	ctx := context.Background()
	i := newInstance(t, Settings{Enabled: true, OpenLevel: 1})
	i.start(t)
	gone := strings.Repeat("cd", 32)
	var rel, hash [32]byte
	rel[0], hash[0] = 1, 1
	if _, err := i.recs.Insert(ctx, database.PeernetRecord{
		NetworkID: gone, Kind: database.PeernetRecordKindRelease, ContentHash: hex.EncodeToString(hash[:]),
		ReleaseID: hex.EncodeToString(rel[:]), PublisherNodeID: strings.Repeat("ef", 32), CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour), Source: database.PeernetRecordSourcePeer, Payload: []byte("p"),
	}, database.PeernetRecordLimits{}); err != nil {
		t.Fatal(err)
	}
	i.node(t).maintainRecords(ctx, true)
	ids, err := i.recs.NetworkIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id == gone {
			t.Fatal("orphan network records survived upkeep")
		}
	}
}
