package bundlex

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

type fakeOffers struct {
	rows []database.PeernetBundleOffer
	atts []database.PeernetAttestation
}

func (f *fakeOffers) BundleOffers(context.Context, []string, string, int) ([]database.PeernetBundleOffer, error) {
	return f.rows, nil
}
func (f *fakeOffers) AttestationsFor(context.Context, string, string, string, time.Time) ([]database.PeernetAttestation, error) {
	return f.atts, nil
}

type fakeBook struct {
	peers map[identity.NodeID]recsync.Peer
	known map[string]bool
}

func (f *fakeBook) VerifiedPeer(_ context.Context, _ netid.NetworkID, n identity.NodeID) (recsync.Peer, bool, error) {
	p, ok := f.peers[n]
	return p, ok, nil
}
func (f *fakeBook) Known(context.Context, netid.NetworkID, []string) (map[string]bool, error) {
	return f.known, nil
}

type fakeRecv struct{}

func (fakeRecv) ReceivingNetworks() []recsync.NetworkInfo {
	return []recsync.NetworkInfo{{ID: nwA, Level: 1}}
}

// hostile starts a peer server with one custom handler on the bundle route.
func hostile(t *testing.T, h http.HandlerFunc) (string, identity.NodeID) {
	t.Helper()
	key, _ := identity.GenerateKey()
	ts, err := transport.NewServer(transport.ServerConfig{Identity: key, ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.Handle("GET "+PathBundle, 0, h); err != nil {
		t.Fatal(err)
	}
	if err := ts.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = ts.Stop(c)
	})
	return ts.Addr().String(), key.NodeID()
}

type fetchEnv struct {
	f      *Fetcher
	offers *fakeOffers
	book   *fakeBook
	built  *Built
	rel    string
}

func newFetchEnv(t *testing.T, lim FetchLimits) *fetchEnv {
	t.Helper()
	lib, rel := nzbRelease()
	built, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "T", posted)
	if err != nil {
		t.Fatal(err)
	}
	ck, _ := identity.GenerateKey()
	client, err := transport.NewClient(transport.ClientConfig{Identity: ck, AllowLoopbackForTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	e := &fetchEnv{offers: &fakeOffers{}, book: &fakeBook{peers: map[identity.NodeID]recsync.Peer{}, known: map[string]bool{}}, built: built, rel: rel}
	e.f = NewFetcher(FetchConfig{Self: ck.NodeID(), Store: e.offers, Peers: e.book, Networks: fakeRecv{}, Dialer: client, Limits: lim})
	return e
}

// publisher registers a node as a verified peer that announced the bundle.
func (e *fetchEnv) publisher(addr string, id identity.NodeID, age time.Duration) {
	e.book.peers[id] = recsync.Peer{NodeID: id, Address: addr, Trusted: true}
	e.offers.rows = append(e.offers.rows, database.PeernetBundleOffer{NetworkID: nwA.String(), PublisherNodeID: id.String(),
		BundleID: e.built.ID.String(), ReleaseScheme: database.PeernetSchemeNZB, CreatedAt: time.Now().Add(-age), Source: database.PeernetRecordSourcePeer})
}

func honest(t *testing.T, e *fetchEnv) (string, identity.NodeID) {
	return hostile(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", itoa(len(e.built.Wire)))
		_, _ = w.Write(e.built.Wire)
	})
}

func TestFetchVerifiesAndReportsSource(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	addr, id := honest(t, e)
	e.publisher(addr, id, time.Hour)
	b, src, err := e.f.Fetch(context.Background(), e.rel)
	if err != nil {
		t.Fatal(err)
	}
	rid, _ := contentid.ParseID(e.rel)
	if b.ReleaseID != rid || src.Publisher != id || src.Network != nwA || src.BundleID != e.built.ID || src.Scheme != bundle.SchemeNZB {
		t.Fatalf("bundle %v source %+v", b.ReleaseID, src)
	}
	if e.f.Status().OK != 1 {
		t.Fatal("success not counted")
	}
}

func TestFetchRejectsSubstitutedGarbageOversizedAndStrikes(t *testing.T) {
	// A valid bundle of another release, served under the announced IDs.
	lib2 := &fakeLib{scheme: database.PeernetSchemeNZB, paths: []string{"x"}, nzb: [][]string{ids("z", 4)}}
	lib2.files = map[string]*metapb.FileMetadata{"x": plainFile("z.mkv", ids("z", 4))}
	other, err := newTestBuilder(t, lib2, NoPatches{}).Build(context.Background(), releaseHexOfNZB(lib2.nzb), "T", posted)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		h      http.HandlerFunc
		reason string
	}{
		"substituted": {func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(other.Wire) }, failVerification},
		"garbage":     {func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(bytes.Repeat([]byte{0x42}, 5000)) }, failVerification},
		"oversized": {func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", itoa(bundle.MaxCompressedSize+1))
			_, _ = w.Write(make([]byte, 1000))
		}, failTooLarge},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			e := newFetchEnv(t, FetchLimits{})
			bad, badID := hostile(t, c.h)
			good, goodID := honest(t, e)
			e.publisher(bad, badID, time.Minute) // newest: tried first
			e.publisher(good, goodID, time.Hour)
			b, src, err := e.f.Fetch(context.Background(), e.rel)
			if err != nil || src.Publisher != goodID || b == nil {
				t.Fatalf("the honest offer was not used: %v %+v", err, src)
			}
			st := e.f.Status()
			if st.Failed[c.reason] != 1 || st.BackedOff != 1 {
				t.Fatalf("status %+v", st)
			}
			// The hostile publisher is skipped from now on, the honest one still works.
			e.f.glob = newBuckets(3600, 100, 1)
			if _, src, err := e.f.Fetch(context.Background(), e.rel); err != nil || src.Publisher != goodID {
				t.Fatalf("second fetch: %v %+v", err, src)
			}
			if e.f.Status().Failed[c.reason] != 1 {
				t.Fatal("the struck publisher was asked again")
			}
		})
	}
}

func TestFetchTruncatedAndSlowAreSoftFailures(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{Timeout: 400 * time.Millisecond})
	trunc, tid := hostile(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", itoa(len(e.built.Wire)))
		_, _ = w.Write(e.built.Wire[:len(e.built.Wire)/2])
	})
	slow, sid := hostile(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", itoa(len(e.built.Wire)))
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(3 * time.Second)
	})
	e.publisher(trunc, tid, time.Minute)
	e.publisher(slow, sid, time.Hour)
	_, _, err := e.f.Fetch(context.Background(), e.rel)
	if !errors.Is(err, ErrAllFailed) {
		t.Fatalf("err = %v", err)
	}
	if st := e.f.Status(); st.Failed[failTruncated] != 2 {
		t.Fatalf("status %+v", st)
	}
}

func TestFetchOnlyFromVerifiedPublishers(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	addr, id := honest(t, e)
	e.publisher(addr, id, time.Hour)
	delete(e.book.peers, id) // relayed record: no verified address for the publisher
	if _, _, err := e.f.Fetch(context.Background(), e.rel); !errors.Is(err, ErrNoOffers) {
		t.Fatalf("err = %v", err)
	}
	offers, _ := e.f.Offers(context.Background(), e.rel)
	if len(offers) != 1 || offers[0].Fetchable {
		t.Fatalf("offers %+v", offers)
	}
	// An own record is never fetched.
	e.offers.rows[0].Source = database.PeernetRecordSourceLocal
	e.book.peers[id] = recsync.Peer{NodeID: id, Address: addr, Trusted: true}
	if _, _, err := e.f.Fetch(context.Background(), e.rel); !errors.Is(err, ErrNoOffers) {
		t.Fatalf("own offer: %v", err)
	}
}

func TestOffersAreOrderedKnownThenAttestedThenNewest(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	var ids3 [4]identity.NodeID
	for i := range ids3 {
		k, _ := identity.GenerateKey()
		ids3[i] = k.NodeID()
		e.publisher("127.0.0.1:1", ids3[i], time.Duration(i+1)*time.Hour) // 0 is newest
	}
	e.book.known[ids3[3].String()] = true
	e.offers.atts = []database.PeernetAttestation{{PublisherNodeID: ids3[2].String(), Healthy: true}, {PublisherNodeID: ids3[1].String(), Healthy: false}}
	got, err := e.f.Offers(context.Background(), e.rel)
	if err != nil {
		t.Fatal(err)
	}
	// ids3[2] attested only itself: a self-attestation never raises its own
	// rank, while the others are attested by it.
	want := []identity.NodeID{ids3[3], ids3[0], ids3[1], ids3[2]}
	for i, o := range got {
		if o.Publisher != want[i] {
			t.Fatalf("position %d: got %v", i, o.Publisher)
		}
	}
}

func TestFetchRateLimitsAndAttemptCap(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{GlobalPerHour: 1, GlobalBurst: 1})
	addr, id := honest(t, e)
	e.publisher(addr, id, time.Hour)
	if _, _, err := e.f.Fetch(context.Background(), e.rel); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.f.Fetch(context.Background(), e.rel); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("second fetch: %v", err)
	}

	// Attempt cap: five dead publishers, three attempts.
	e = newFetchEnv(t, FetchLimits{MaxAttempts: 3})
	var mu sync.Mutex
	hits := 0
	for range 5 {
		a, i := hostile(t, func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			hits++
			mu.Unlock()
			w.WriteHeader(500)
		})
		e.publisher(a, i, time.Hour)
	}
	if _, _, err := e.f.Fetch(context.Background(), e.rel); !errors.Is(err, ErrAllFailed) {
		t.Fatal(err)
	}
	if hits != 3 {
		t.Fatalf("%d attempts", hits)
	}
}

func TestFetchConcurrencyCap(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{MaxConcurrent: 1})
	gate := make(chan struct{})
	addr, id := hostile(t, func(w http.ResponseWriter, _ *http.Request) {
		<-gate
		_, _ = w.Write(e.built.Wire)
	})
	e.publisher(addr, id, time.Hour)
	done := make(chan error, 1)
	go func() { _, _, err := e.f.Fetch(context.Background(), e.rel); done <- err }()
	time.Sleep(200 * time.Millisecond)
	if _, _, err := e.f.Fetch(context.Background(), e.rel); !errors.Is(err, ErrFetchBusy) {
		t.Fatalf("err = %v", err)
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFullCheckPenaltyIsMildBoundedAndCleared(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	addr, id := honest(t, e)
	e.publisher(addr, id, time.Hour)
	now := time.Now()
	e.f.cfg.Now = func() time.Time { return now }
	fetch := func() error {
		e.f.glob = newBuckets(3600, 100, 1)
		e.f.pub = newBuckets(3600, 100, 8)
		_, _, err := e.f.Fetch(context.Background(), e.rel)
		return err
	}
	if err := fetch(); err != nil {
		t.Fatal(err)
	}
	// First report: 30 minutes.
	e.f.ReportBadBundle(nwA, id)
	if err := fetch(); !errors.Is(err, ErrNoOffers) {
		t.Fatalf("a penalised publisher was asked: %v", err)
	}
	st := e.f.Status()
	if st.FullCheckReports != 1 || st.FullCheckBackedOff != 1 {
		t.Fatalf("status %+v", st)
	}
	if len(st.Failed) != 0 || st.BackedOff != 0 {
		t.Fatalf("the penalty must not look like a hostile strike: %+v", st)
	}
	now = now.Add(31 * time.Minute)
	if err := fetch(); err != nil {
		t.Fatalf("not released after 30 minutes: %v", err)
	}
	// Second report doubles it.
	e.f.ReportBadBundle(nwA, id)
	now = now.Add(31 * time.Minute)
	if err := fetch(); !errors.Is(err, ErrNoOffers) {
		t.Fatal("the second penalty must last longer than 31 minutes")
	}
	now = now.Add(30 * time.Minute)
	if err := fetch(); err != nil {
		t.Fatalf("not released after 60 minutes: %v", err)
	}
	// Many reports stay under the cap.
	for range 20 {
		e.f.ReportBadBundle(nwA, id)
	}
	now = now.Add(6*time.Hour + time.Minute)
	if err := fetch(); err != nil {
		t.Fatalf("the penalty is not capped at 6 hours: %v", err)
	}
	// A later verified bundle clears it at once.
	e.f.ReportBadBundle(nwA, id)
	e.f.ReportVerifiedBundle(nwA, id)
	if err := fetch(); err != nil {
		t.Fatalf("a good full check must clear the penalty: %v", err)
	}
	// A long quiet period decays the doubling back to the base.
	e.f.ReportBadBundle(nwA, id)
	e.f.ReportBadBundle(nwA, id)
	now = now.Add(25 * time.Hour)
	e.f.ReportBadBundle(nwA, id)
	now = now.Add(31 * time.Minute)
	if err := fetch(); err != nil {
		t.Fatalf("quiet period must reset the doubling: %v", err)
	}
}

func TestOffersPreferBundlesAnnouncedByMostDistinctNetworkRanges(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	mk := func() identity.NodeID { k, _ := identity.GenerateKey(); return k.NodeID() }
	announce := func(id identity.NodeID, addr, bundleID string, age time.Duration) {
		e.book.known[id.String()] = true
		host, _, _ := net.SplitHostPort(addr)
		e.book.peers[id] = recsync.Peer{NodeID: id, Address: addr, Trusted: true, ConnectedIP: host}
		e.offers.rows = append(e.offers.rows, database.PeernetBundleOffer{NetworkID: nwA.String(), PublisherNodeID: id.String(),
			BundleID: bundleID, ReleaseScheme: database.PeernetSchemeNZB, CreatedAt: time.Now().Add(-age), Source: database.PeernetRecordSourcePeer})
	}
	x, y, z := e.built.ID.String(), "ab"+e.built.ID.String()[2:], "cd"+e.built.ID.String()[2:]
	loner, a, b, d1, d2 := mk(), mk(), mk(), mk(), mk()
	announce(loner, "10.9.9.1:8470", y, time.Minute) // newest, but announced by one publisher
	announce(a, "10.1.1.5:8470", x, time.Hour)
	announce(b, "10.2.2.5:8470", x, 2*time.Hour)
	announce(d1, "10.3.3.5:8470", z, 3*time.Hour) // same /24: counts once
	announce(d2, "10.3.3.77:8470", z, 4*time.Hour)

	got, err := e.f.Offers(context.Background(), e.rel)
	if err != nil {
		t.Fatal(err)
	}
	by := map[identity.NodeID]Offer{}
	for _, o := range got {
		by[o.Publisher] = o
	}
	if by[a].Corroborators != 2 || by[b].Corroborators != 2 {
		t.Fatalf("two distinct ranges announce bundle x: got %d and %d", by[a].Corroborators, by[b].Corroborators)
	}
	if by[loner].Corroborators != 1 || by[d1].Corroborators != 1 || by[d2].Corroborators != 1 {
		t.Fatalf("one range counts once: loner %d, same-range %d and %d", by[loner].Corroborators, by[d1].Corroborators, by[d2].Corroborators)
	}
	if got[0].Publisher != a || got[1].Publisher != b {
		t.Fatalf("the corroborated bundle must rank first, got %v then %v", got[0].Publisher, got[1].Publisher)
	}
}

// Two DNS names for one server are one range, and a peer with no recorded
// connected IP counts for nothing.
func TestCorroborationKeysOnTheConnectedIPNotTheName(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	mk := func() identity.NodeID { k, _ := identity.GenerateKey(); return k.NodeID() }
	x := e.built.ID.String()
	add := func(id identity.NodeID, addr, ip string) {
		e.book.known[id.String()] = true
		e.book.peers[id] = recsync.Peer{NodeID: id, Address: addr, Trusted: true, ConnectedIP: ip}
		e.offers.rows = append(e.offers.rows, database.PeernetBundleOffer{NetworkID: nwA.String(), PublisherNodeID: id.String(),
			BundleID: x, ReleaseScheme: database.PeernetSchemeNZB, CreatedAt: time.Now(), Source: database.PeernetRecordSourcePeer})
	}
	a, b, c := mk(), mk(), mk()
	add(a, "one.example.org:8470", "198.51.100.7")
	add(b, "two.example.org:8470", "198.51.100.88") // same /24 under another name
	add(c, "three.example.org:8470", "")            // never recorded
	got, err := e.f.Offers(context.Background(), e.rel)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range got {
		if o.Corroborators != 1 {
			t.Fatalf("%v: %d ranges, want 1 (two names, one /24; a peer without a connected IP counts nothing)", o.Publisher, o.Corroborators)
		}
	}
}

// An attestation from a node that is not a known peer must not rank an offer.
func TestAttestersMustBeKnown(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	mk := func() identity.NodeID { k, _ := identity.GenerateKey(); return k.NodeID() }
	pub, stranger := mk(), mk()
	e.book.known[pub.String()] = true
	e.book.peers[pub] = recsync.Peer{NodeID: pub, Address: "10.1.1.1:8470", Trusted: true, ConnectedIP: "10.1.1.1"}
	e.offers.rows = append(e.offers.rows, database.PeernetBundleOffer{NetworkID: nwA.String(), PublisherNodeID: pub.String(),
		BundleID: e.built.ID.String(), ReleaseScheme: database.PeernetSchemeNZB, CreatedAt: time.Now(), Source: database.PeernetRecordSourcePeer})
	e.offers.atts = []database.PeernetAttestation{{PublisherNodeID: stranger.String(), Healthy: true}}
	got, err := e.f.Offers(context.Background(), e.rel)
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %v", got, err)
	}
	if got[0].AttestedHealthy {
		t.Fatal("an unknown node's attestation must not count")
	}
	e.book.known[stranger.String()] = true
	if got, _ = e.f.Offers(context.Background(), e.rel); !got[0].AttestedHealthy {
		t.Fatal("control: a known attester counts")
	}
}

type openRecv struct{}

func (openRecv) ReceivingNetworks() []recsync.NetworkInfo {
	return []recsync.NetworkInfo{{ID: netid.Open(), Level: 3}}
}

type failedSet map[string]bool

func (f failedSet) IsFailedBundle(_ context.Context, id string) bool { return f[id] }

// The caller's requirements filter the offers BEFORE anything is dialled: an
// open-network offer the caller will not take is never contacted (the contact
// itself reveals the Release ID).
func TestFetchWithFiltersOffersBeforeDialing(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	e.f.cfg.Networks = openRecv{}
	var hits int
	var mu sync.Mutex
	addr, id := hostile(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.Header().Set("Content-Length", itoa(len(e.built.Wire)))
		_, _ = w.Write(e.built.Wire)
	})
	e.book.known[id.String()] = true
	e.book.peers[id] = recsync.Peer{NodeID: id, Address: addr, Trusted: true, ConnectedIP: "127.0.0.1"}
	e.offers.rows = append(e.offers.rows, database.PeernetBundleOffer{NetworkID: netid.Open().String(), PublisherNodeID: id.String(),
		BundleID: e.built.ID.String(), ReleaseScheme: database.PeernetSchemeNZB, CreatedAt: time.Now(), Source: database.PeernetRecordSourcePeer})
	ctx := context.Background()

	for name, opts := range map[string]FetchOptions{
		"open not allowed":           {Restrict: true},
		"one corroborator, two need": {Restrict: true, AllowOpen: true, MinOpenCorroborators: 2},
	} {
		if _, _, err := e.f.FetchWith(ctx, e.rel, opts); !errors.Is(err, ErrNoOffers) {
			t.Fatalf("%s: want ErrNoOffers, got %v", name, err)
		}
		mu.Lock()
		n := hits
		mu.Unlock()
		if n != 0 {
			t.Fatalf("%s: the peer was dialled %d times", name, n)
		}
	}
	if _, _, err := e.f.FetchWith(ctx, e.rel, FetchOptions{Restrict: true, AllowOpen: true, MinOpenCorroborators: 1}); err != nil {
		t.Fatalf("control: %v", err)
	}
}

// A Bundle ID that failed local verification is skipped, whoever offers it.
func TestFetchSkipsBundlesOnTheNegativeCache(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	var hits int
	var mu sync.Mutex
	addr, id := hostile(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.Header().Set("Content-Length", itoa(len(e.built.Wire)))
		_, _ = w.Write(e.built.Wire)
	})
	e.publisher(addr, id, time.Hour)
	e.f.cfg.Failed = failedSet{e.built.ID.String(): true}
	if _, _, err := e.f.Fetch(context.Background(), e.rel); !errors.Is(err, ErrNoOffers) {
		t.Fatalf("want ErrNoOffers, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Fatalf("dialled %d times", hits)
	}
}

// A publisher that lowered its level or excluded the release answers with the
// deliberately undecorated 403, which a membership-checking authoriser cannot
// tell from a non-member. It must count as a plain refusal ("refused"), back
// off that bundle only for a short while, and never as the publisher's fault.
type undecoratedRefusalAuth struct{ discovery.AllowAll }

func (undecoratedRefusalAuth) VerifyResponse(_ context.Context, _ netid.NetworkID, _ identity.NodeID, status int, _ http.Header) error {
	if status == http.StatusForbidden {
		return errors.New("response is not decorated")
	}
	return nil
}

func TestFetchUndecoratedRefusalIsNeutralAndOnlyBacksOffThatBundle(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{})
	e.f.cfg.Auth = undecoratedRefusalAuth{}
	refuse := true
	var mu sync.Mutex
	addr, id := hostile(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if refuse {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Length", itoa(len(e.built.Wire)))
		_, _ = w.Write(e.built.Wire)
	})
	e.publisher(addr, id, time.Hour)
	now := time.Now()
	e.f.cfg.Now = func() time.Time { return now }
	if _, _, err := e.f.Fetch(context.Background(), e.rel); err == nil {
		t.Fatal("the refusal must fail the fetch")
	}
	st := e.f.Status()
	if st.Failed[failMembership] != 0 || st.Failed[failRefused] != 1 {
		t.Fatalf("an undecorated refusal is 'refused', not 'membership': %+v", st.Failed)
	}
	if st.BackedOff != 0 {
		t.Fatalf("the publisher must not be struck: %+v", st)
	}
	// The same bundle is skipped for the short soft back-off only.
	if _, _, err := e.f.Fetch(context.Background(), e.rel); !errors.Is(err, ErrNoOffers) {
		t.Fatalf("the refused bundle is skipped briefly: %v", err)
	}
	mu.Lock()
	refuse = false
	mu.Unlock()
	now = now.Add(e.f.lim.SoftBackoff + time.Second)
	if _, _, err := e.f.Fetch(context.Background(), e.rel); err != nil {
		t.Fatalf("the publisher returned to serving: %v", err)
	}
}

// A per-publisher limit and a publisher back-off are not "no offer".
func TestFetchReportsPublisherLimitAndBackOffAsTheirOwnErrors(t *testing.T) {
	e := newFetchEnv(t, FetchLimits{PerPublisherPerHour: 1, PerPublisherBurst: 1})
	addr, id := honest(t, e)
	e.publisher(addr, id, time.Hour)
	if _, _, err := e.f.Fetch(context.Background(), e.rel); err != nil {
		t.Fatal(err)
	}
	_, _, err := e.f.Fetch(context.Background(), e.rel)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("a per-publisher limit must be ErrRateLimited, got %v", err)
	}
	if e.f.Status().RateLimited != 1 {
		t.Fatalf("fetch_rate_limited stays %d", e.f.Status().RateLimited)
	}

	e2 := newFetchEnv(t, FetchLimits{})
	addr2, id2 := honest(t, e2)
	e2.publisher(addr2, id2, time.Hour)
	e2.f.ReportBadBundle(nwA, id2)
	_, _, err = e2.f.Fetch(context.Background(), e2.rel)
	if !errors.Is(err, ErrBackedOff) || !errors.Is(err, ErrNoOffers) {
		t.Fatalf("a backed-off publisher is ErrBackedOff (still an ErrNoOffers): %v", err)
	}
}
