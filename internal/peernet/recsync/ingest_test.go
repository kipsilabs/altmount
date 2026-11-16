package recsync

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

var testNow = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

func mustKey(t testing.TB) *identity.Key {
	t.Helper()
	k, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func netOf(c byte) netid.NetworkID {
	var n netid.NetworkID
	for i := range n {
		n[i] = c
	}
	return n
}

func relInput(seed int) records.ReleaseRecord {
	var id records.ReleaseID
	for i := range id {
		id[i] = 7
	}
	id[0], id[1] = byte(seed), byte(seed>>8)
	s, e := uint32(2), uint32(5)
	return records.ReleaseRecord{
		ReleaseID: id, Title: "Some.Show.S02E05", TotalSize: 5 << 30, FileCount: 3,
		PostDate: testNow.Add(-time.Hour),
		Media:    records.MediaIDs{IMDb: "tt1234567", Season: &s, Episode: &e},
	}
}

func attInput(seed byte, missing uint32) records.HealthAttestation {
	var rid records.ReleaseID
	var fid records.FileID
	for i := range rid {
		rid[i], fid[i] = seed, seed+1
	}
	return records.HealthAttestation{
		ReleaseID: rid, FileID: fid, CheckedAt: testNow.Add(-time.Hour), TotalSegments: 100,
		SegmentsChecked: 20, MissingCount: missing, MediaHeaderVerified: true, Backbones: []string{"Omicron", "abavia"},
	}
}

func buildRelease(t testing.TB, k *identity.Key, nw netid.NetworkID, now time.Time) []byte {
	t.Helper()
	_, b, err := records.BuildReleaseRecord(k, nw, relInput(7), now)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func buildAtt(t testing.TB, k *identity.Key, nw netid.NetworkID, missing uint32, now time.Time) []byte {
	t.Helper()
	_, b, err := records.BuildHealthAttestation(k, nw, attInput(9, missing), now)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeStore records what Insert received and answers a canned result.
type fakeStore struct {
	mu      sync.Mutex
	rows    []database.PeernetRecord
	limits  []database.PeernetRecordLimits
	result  database.PeernetInsertResult
	err     error
	have    map[string]bool
	lookups int
}

func (f *fakeStore) HasContentHash(_ context.Context, _, hash string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	return f.have[hash], nil
}

func (f *fakeStore) Insert(_ context.Context, r database.PeernetRecord, l database.PeernetRecordLimits) (database.PeernetInsertResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, r)
	f.limits = append(f.limits, l)
	if f.err != nil {
		return database.PeernetInsertResult{}, f.err
	}
	res := f.result
	if res.Outcome == 0 {
		res = database.PeernetInsertResult{Outcome: database.PeernetRecordInserted, Seq: int64(len(f.rows))}
	}
	return res, nil
}

func newTestIngester(t testing.TB, self identity.NodeID, st RecordStore) *Ingester {
	t.Helper()
	return NewIngester(IngestConfig{Self: self, Store: st, Now: func() time.Time { return testNow },
		Limits: database.PeernetRecordLimits{MaxPerPublisher: 11, MaxPerNetwork: 22}})
}

func TestIngestStoresVerifiedReleaseWithDerivedColumns(t *testing.T) {
	pub, self := mustKey(t), mustKey(t)
	nw := netOf(1)
	st := &fakeStore{}
	ing := newTestIngester(t, self.NodeID(), st)
	raw := buildRelease(t, pub, nw, testNow)

	out, err := ing.Ingest(context.Background(), nw, raw, OriginPeer)
	if err != nil {
		t.Fatal(err)
	}
	if out.Result != ResultStored || out.Seq != 1 || out.RecordKind != records.KindRelease {
		t.Fatalf("outcome = %+v", out)
	}
	row := st.rows[0]
	if row.NetworkID != nw.String() || row.Kind != database.PeernetRecordKindRelease ||
		row.PublisherNodeID != pub.NodeID().String() || row.Source != database.PeernetRecordSourcePeer ||
		row.Title != "Some.Show.S02E05" || row.TotalSize != 5<<30 || row.IMDbID != "tt1234567" ||
		row.Season == nil || *row.Season != 2 || row.Episode == nil || *row.Episode != 5 ||
		row.FileID != "" || row.Healthy != nil || string(row.Payload) != string(raw) ||
		row.ContentHash != hexOf(records.ContentHash(raw)) || !row.ExpiresAt.After(row.CreatedAt) {
		t.Fatalf("row = %+v", row)
	}
	if st.limits[0].MaxPerPublisher != 11 || st.limits[0].MaxPerNetwork != 22 {
		t.Fatalf("limits not passed: %+v", st.limits[0])
	}
}

func TestIngestAttestationColumns(t *testing.T) {
	pub, self := mustKey(t), mustKey(t)
	nw := netOf(1)
	st := &fakeStore{}
	ing := newTestIngester(t, self.NodeID(), st)
	for _, missing := range []uint32{0, 3} {
		if out, err := ing.Ingest(context.Background(), nw, buildAtt(t, pub, nw, missing, testNow), OriginPeer); err != nil || out.Result != ResultStored {
			t.Fatalf("missing=%d: %+v %v", missing, out, err)
		}
	}
	healthy, sick := st.rows[0], st.rows[1]
	if healthy.Kind != database.PeernetRecordKindAttestation || healthy.Healthy == nil || !*healthy.Healthy ||
		sick.Healthy == nil || *sick.Healthy {
		t.Fatalf("health columns wrong: %+v / %+v", healthy.Healthy, sick.Healthy)
	}
	if healthy.FileID == "" || strings.Join(healthy.Backbones, ",") != "abavia,omicron" ||
		!healthy.CheckedAt.Equal(testNow.Add(-time.Hour)) || healthy.Title != "" || healthy.IMDbID != "" {
		t.Fatalf("attestation row = %+v", healthy)
	}
}

func TestIngestLocalMustBeOwnKey(t *testing.T) {
	self, other := mustKey(t), mustKey(t)
	nw := netOf(1)
	st := &fakeStore{}
	ing := newTestIngester(t, self.NodeID(), st)

	out, err := ing.Ingest(context.Background(), nw, buildRelease(t, self, nw, testNow), OriginLocal)
	if err != nil || out.Result != ResultStored || st.rows[0].Source != database.PeernetRecordSourceLocal {
		t.Fatalf("own local: %+v %v", out, err)
	}
	out, err = ing.Ingest(context.Background(), nw, buildRelease(t, other, nw, testNow), OriginLocal)
	if err != nil || out.Result != ResultRejected || out.Reason != ReasonNotOwn {
		t.Fatalf("foreign local: %+v %v", out, err)
	}
	if len(st.rows) != 1 {
		t.Fatal("a foreign local record reached the store")
	}
}

func TestIngestPeerCannotClaimThisNodeAsPublisher(t *testing.T) {
	self := mustKey(t)
	nw := netOf(1)
	st := &fakeStore{}
	ing := newTestIngester(t, self.NodeID(), st)
	// Even a perfectly signed record of ours is dropped when a peer hands it
	// to us (an echo or a spoof; they look the same without extra state).
	out, err := ing.Ingest(context.Background(), nw, buildRelease(t, self, nw, testNow), OriginPeer)
	if err != nil || out.Result != ResultRejected || out.Reason != ReasonSelfPublisher {
		t.Fatalf("outcome = %+v %v", out, err)
	}
	if len(st.rows) != 0 {
		t.Fatal("stored")
	}
	if ReasonSelfPublisher.Hostile() {
		t.Error("an echoed own record must not penalise the relaying peer")
	}
}

func TestIngestRejectionReasons(t *testing.T) {
	pub, self := mustKey(t), mustKey(t)
	nw, other := netOf(1), netOf(2)
	good := buildRelease(t, pub, nw, testNow)

	tampered := append([]byte(nil), good...)
	i := strings.Index(string(tampered), "Some.Show")
	tampered[i] = 'X'

	badVersion := append([]byte(nil), good...)
	if badVersion[0] != 0x08 || badVersion[1] != 0x01 {
		t.Fatalf("unexpected header bytes % x", badVersion[:2])
	}
	badVersion[1] = 0x02

	oldAtt, err := func() ([]byte, error) {
		in := attInput(3, 0)
		in.CheckedAt = testNow.Add(-6 * 24 * time.Hour)
		_, b, err := records.BuildHealthAttestation(pub, nw, in, testNow)
		return b, err
	}()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		raw    []byte
		now    time.Time
		reason Reason
		hostil bool
	}{
		{"oversize", make([]byte, records.MaxEncodedSize+1), testNow, ReasonOversize, true},
		{"empty", nil, testNow, ReasonMalformed, true},
		{"garbage", []byte("definitely not a record"), testNow, ReasonMalformed, true},
		{"bad version", badVersion, testNow, ReasonUnsupportedVersion, false},
		{"wrong network", buildRelease(t, pub, other, testNow), testNow, ReasonWrongNetwork, true},
		{"bad signature", tampered, testNow, ReasonBadSignature, true},
		{"from the future", buildRelease(t, pub, nw, testNow.Add(10*time.Minute)), testNow, ReasonFuture, false},
		{"expired", buildRelease(t, pub, nw, testNow.Add(-31*24*time.Hour)), testNow, ReasonExpired, false},
		{"stale attestation", oldAtt, testNow.Add(6*24*time.Hour + 12*time.Hour), ReasonStale, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := &fakeStore{}
			now := c.now
			ing := NewIngester(IngestConfig{Self: self.NodeID(), Store: st, Now: func() time.Time { return now }})
			out, err := ing.Ingest(context.Background(), nw, c.raw, OriginPeer)
			if err != nil {
				t.Fatal(err)
			}
			if out.Result != ResultRejected || out.Reason != c.reason {
				t.Fatalf("outcome = %+v, want reason %v", out, c.reason)
			}
			if out.Reason.Hostile() != c.hostil {
				t.Errorf("Hostile() = %v, want %v", out.Reason.Hostile(), c.hostil)
			}
			if len(st.rows) != 0 {
				t.Fatal("a rejected record reached the store")
			}
			if got := ing.Counters(nw).Rejected[c.reason.String()]; got != 1 {
				t.Fatalf("counter %q = %d", c.reason, got)
			}
		})
	}
}

func TestIngestStoreOutcomes(t *testing.T) {
	pub, self := mustKey(t), mustKey(t)
	nw := netOf(1)
	raw := buildRelease(t, pub, nw, testNow)
	cases := []struct {
		name   string
		store  database.PeernetInsertResult
		result Result
		reason Reason
	}{
		{"inserted", database.PeernetInsertResult{Outcome: database.PeernetRecordInserted, Seq: 4}, ResultStored, 0},
		{"superseded", database.PeernetInsertResult{Outcome: database.PeernetRecordSuperseded, Seq: 5}, ResultStored, 0},
		{"duplicate", database.PeernetInsertResult{Outcome: database.PeernetRecordDuplicate}, ResultDuplicate, 0},
		{"publisher quota", database.PeernetInsertResult{Outcome: database.PeernetRecordRejectedQuota, QuotaScope: database.PeernetRecordQuotaPublisher}, ResultRejected, ReasonQuotaPublisher},
		{"storage quota", database.PeernetInsertResult{Outcome: database.PeernetRecordRejectedQuota, QuotaScope: database.PeernetRecordQuotaStorage}, ResultRejected, ReasonQuotaStorage},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ing := newTestIngester(t, self.NodeID(), &fakeStore{result: c.store})
			out, err := ing.Ingest(context.Background(), nw, raw, OriginPeer)
			if err != nil || out.Result != c.result || out.Reason != c.reason {
				t.Fatalf("outcome = %+v %v", out, err)
			}
			if c.reason != 0 && c.reason.Hostile() {
				t.Error("a quota refusal is not the peer's fault")
			}
		})
	}
}

func TestIngestStoreErrorsAreErrorsNotRejections(t *testing.T) {
	pub, self := mustKey(t), mustKey(t)
	nw := netOf(1)
	boom := errors.New("disk on fire")
	ing := newTestIngester(t, self.NodeID(), &fakeStore{err: boom})
	out, err := ing.Ingest(context.Background(), nw, buildRelease(t, pub, nw, testNow), OriginPeer)
	if !errors.Is(err, boom) || out.Result != 0 {
		t.Fatalf("outcome = %+v err = %v", out, err)
	}
	if c := ing.Counters(nw); len(c.Rejected) != 0 || c.Stored != 0 {
		t.Fatalf("a store failure was counted: %+v", c)
	}
	// A validation error from the store is a rejection, not a failure.
	ing = newTestIngester(t, self.NodeID(), &fakeStore{err: database.ErrPeernetRecordInvalid})
	out, err = ing.Ingest(context.Background(), nw, buildRelease(t, pub, nw, testNow), OriginPeer)
	if err != nil || out.Reason != ReasonInvalid {
		t.Fatalf("outcome = %+v err = %v", out, err)
	}
}

func TestIngestCountersAndForget(t *testing.T) {
	pub, self := mustKey(t), mustKey(t)
	nw := netOf(1)
	st := &fakeStore{}
	ing := newTestIngester(t, self.NodeID(), st)
	raw := buildRelease(t, pub, nw, testNow)
	_, _ = ing.Ingest(context.Background(), nw, raw, OriginPeer)
	st.result = database.PeernetInsertResult{Outcome: database.PeernetRecordDuplicate}
	_, _ = ing.Ingest(context.Background(), nw, raw, OriginPeer)
	_, _ = ing.Ingest(context.Background(), nw, []byte("junk"), OriginPeer)
	_, _ = ing.Ingest(context.Background(), nw, []byte("junk"), OriginPeer)

	c := ing.Counters(nw)
	if c.Stored != 1 || c.Duplicates != 1 || c.Rejected["malformed"] != 2 {
		t.Fatalf("counters = %+v", c)
	}
	if other := ing.Counters(netOf(9)); other.Stored != 0 || len(other.Rejected) != 0 {
		t.Fatalf("counters leaked across networks: %+v", other)
	}
	ing.Forget(nw)
	if c := ing.Counters(nw); c.Stored != 0 || len(c.Rejected) != 0 {
		t.Fatalf("counters survived Forget: %+v", c)
	}
}

func TestReasonNamesAreStable(t *testing.T) {
	seen := map[string]bool{}
	for r := Reason(1); r < reasonCount; r++ {
		n := r.String()
		if n == "" || strings.HasPrefix(n, "reason(") || seen[n] {
			t.Errorf("reason %d has bad or duplicate name %q", r, n)
		}
		seen[n] = true
	}
}

func hexOf(b [32]byte) string { return hex.EncodeToString(b[:]) }

func TestIngestDuplicateCostsOneReadAndNoSignatureWork(t *testing.T) {
	pub, self := mustKey(t), mustKey(t)
	nw := netOf(1)
	raw := buildRelease(t, pub, nw, testNow)
	// Same bytes with a corrupted signature byte: it cannot verify, so a
	// Duplicate answer proves the signature was never looked at.
	bad := append([]byte(nil), raw...)
	bad[len(bad)-1] ^= 0xff
	st := &fakeStore{have: map[string]bool{hexOf(records.ContentHash(bad)): true}}
	ing := newTestIngester(t, self.NodeID(), st)
	out, err := ing.Ingest(context.Background(), nw, bad, OriginPeer)
	if err != nil {
		t.Fatal(err)
	}
	if out.Result != ResultDuplicate || len(st.rows) != 0 {
		t.Fatalf("outcome %+v, inserts %d: a stored hash must short-circuit before verification and the write", out, len(st.rows))
	}
}

func TestIngestAttributesPeerRowsToTheRelayerAndMapsCoverage(t *testing.T) {
	pub, self, relay := mustKey(t), mustKey(t), mustKey(t)
	nw := netOf(1)
	st := &fakeStore{}
	ing := newTestIngester(t, self.NodeID(), st)
	if _, err := ing.IngestFrom(context.Background(), nw, buildAtt(t, pub, nw, 0, testNow), OriginPeer, relay.NodeID()); err != nil {
		t.Fatal(err)
	}
	row := st.rows[0]
	if row.RelayerNodeID != relay.NodeID().String() {
		t.Fatalf("relayer = %q", row.RelayerNodeID)
	}
	if row.SegmentsChecked != 20 || row.TotalSegments != 100 {
		t.Fatalf("coverage = %d/%d", row.SegmentsChecked, row.TotalSegments)
	}
	// Own records carry no relayer.
	if _, err := ing.IngestFrom(context.Background(), nw, buildRelease(t, self, nw, testNow), OriginLocal, relay.NodeID()); err != nil {
		t.Fatal(err)
	}
	if st.rows[1].RelayerNodeID != "" {
		t.Fatalf("own record attributed to %q", st.rows[1].RelayerNodeID)
	}
}

func TestIngestRateLimitIsNodeWideAndSparesDuplicates(t *testing.T) {
	self := mustKey(t)
	nw := netOf(1)
	st := &fakeStore{have: map[string]bool{}}
	ing := NewIngester(IngestConfig{Self: self.NodeID(), Store: st, Now: func() time.Time { return testNow },
		RatePerSecond: 1, Burst: 2})
	mk := func() []byte {
		_, b, err := records.BuildReleaseRecord(mustKey(t), nw, relInput(7), testNow)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for i := 0; i < 2; i++ { // distinct publishers: the burst
		if _, err := ing.Ingest(context.Background(), nw, mk(), OriginPeer); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	third := mk()
	if _, err := ing.Ingest(context.Background(), nw, third, OriginPeer); !errors.Is(err, ErrIngestRateLimited) {
		t.Fatalf("third record: err = %v", err)
	}
	// A duplicate does not need a token.
	st.have[hexOf(records.ContentHash(third))] = true
	out, err := ing.Ingest(context.Background(), nw, third, OriginPeer)
	if err != nil || out.Result != ResultDuplicate {
		t.Fatalf("duplicate under rate limit: %+v %v", out, err)
	}
}

func TestIngestRateIsFairAcrossRelayers(t *testing.T) {
	self, pub := mustKey(t), mustKey(t)
	nw := netOf(1)
	st := &fakeStore{}
	ing := NewIngester(IngestConfig{Self: self.NodeID(), Store: st, Now: func() time.Time { return testNow },
		RatePerSecond: 0.001, Burst: 8})
	flooder, honest := mustKey(t).NodeID(), mustKey(t).NodeID()
	build := func(seed int) []byte {
		_, b, err := records.BuildReleaseRecord(pub, nw, relInput(seed), testNow)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	stored := func(relayer identity.NodeID, from, n int) int {
		got := 0
		for i := from; i < from+n; i++ {
			out, err := ing.IngestFrom(context.Background(), nw, build(i), OriginPeer, relayer)
			if errors.Is(err, ErrIngestRateLimited) {
				break
			}
			if err != nil || out.Result != ResultStored {
				t.Fatalf("ingest %d: %+v %v", i, out, err)
			}
			got++
		}
		return got
	}
	if n := stored(flooder, 1, 20); n >= 8 {
		t.Fatalf("a single relayer ingested %d of the node's 8 tokens", n)
	}
	if n := stored(honest, 100, 2); n != 2 {
		t.Fatalf("an honest relayer ingested %d of 2 records after another relayer flooded", n)
	}
}

func TestIngestStoresTheAnnouncedBundleIDFromTheSignedRecord(t *testing.T) {
	pub, self := mustKey(t), mustKey(t)
	nw := netOf(1)
	st := &fakeStore{}
	ing := newTestIngester(t, self.NodeID(), st)
	in := relInput(9)
	in.BundleID = records.BundleID{1, 2, 3}
	_, raw, err := records.BuildReleaseRecord(pub, nw, in, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ing.Ingest(context.Background(), nw, raw, OriginPeer); err != nil {
		t.Fatal(err)
	}
	if got, want := st.rows[0].BundleID, hexOf(in.BundleID); got != want {
		t.Fatalf("stored bundle id = %q, want %q", got, want)
	}
	// A record without one stores none.
	raw = buildRelease(t, pub, nw, testNow)
	if _, err := ing.Ingest(context.Background(), nw, raw, OriginPeer); err != nil {
		t.Fatal(err)
	}
	if got := st.rows[1].BundleID; got != "" {
		t.Fatalf("bundle id of a record without one = %q", got)
	}
}
