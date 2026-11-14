package bundlex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Errors of Fetch.
var (
	ErrNoOffers    = errors.New("bundlex: no fetchable offer for this release")
	ErrRateLimited = errors.New("bundlex: fetch rate limit reached")
	ErrFetchBusy   = errors.New("bundlex: too many bundle downloads in progress")
	ErrAllFailed   = errors.New("bundlex: no offered bundle could be fetched and verified")
)

// Fetch failure reasons (counted in the status).
const (
	failRefused      = "refused"
	failStatus       = "bad_status"
	failTransport    = "transport"
	failTruncated    = "truncated"
	failTooLarge     = "too_large"
	failVerification = "verification_failed"
	failScheme       = "scheme_mismatch"
	failMembership   = "membership"
)

// OfferStore finds stored release records that announce a bundle.
type OfferStore interface {
	BundleOffers(ctx context.Context, networkIDs []string, releaseID string, limit int) ([]database.PeernetBundleOffer, error)
	AttestationsFor(ctx context.Context, networkID, releaseID, fileID string, notBefore time.Time) ([]database.PeernetAttestation, error)
}

// PeerBook knows the verified peers of a network.
type PeerBook interface {
	// VerifiedPeer returns the peer with a verified address, or false.
	VerifiedPeer(ctx context.Context, nw netid.NetworkID, node identity.NodeID) (recsync.Peer, bool, error)
	// Known returns which of nodes are known verified peers of nw.
	Known(ctx context.Context, nw netid.NetworkID, nodes []string) (map[string]bool, error)
}

// ReceivingNetworks lists the networks this node receives from.
type ReceivingNetworks interface {
	ReceivingNetworks() []recsync.NetworkInfo
}

// FetchConfig wires a Fetcher.
type FetchConfig struct {
	Self     identity.NodeID
	Store    OfferStore
	Peers    PeerBook
	Networks ReceivingNetworks
	Dialer   recsync.Dialer
	Auth     discovery.Authorizer
	Limits   FetchLimits
	Now      func() time.Time
}

// Offer is one stored release record that announces a bundle for a release.
type Offer struct {
	Network   netid.NetworkID
	Publisher identity.NodeID
	BundleID  bundle.BundleID
	Scheme    bundle.Scheme
	CreatedAt time.Time
	// Known: the publisher is a known verified peer. AttestedHealthy: it has a
	// fresh healthy attestation for this release ID.
	Known, AttestedHealthy bool
	// Fetchable: the publisher is a verified peer with an address in that
	// network. A record relayed from a publisher this node has no verified
	// address for cannot be fetched: bundles come from the publisher itself.
	Fetchable bool
	// Own: the record is this node's own.
	Own bool
	// Corroborators is how many distinct network ranges (/24 for IPv4, /48 for
	// IPv6) of known verified publishers announce this very Bundle ID (at
	// least 1 for a known verified publisher, 0 otherwise).
	Corroborators int
}

// FetchSource says where a fetched bundle came from.
type FetchSource struct {
	Network   netid.NetworkID
	Publisher identity.NodeID
	BundleID  bundle.BundleID
	Scheme    bundle.Scheme
	// Corroborators is the offer's count of distinct network ranges announcing
	// the same Bundle ID.
	Corroborators int
}

// Fetcher downloads and verifies bundles on demand.
type Fetcher struct {
	cfg  FetchConfig
	lim  FetchLimits
	glob *buckets
	pub  *buckets
	slot chan struct{}

	mu      sync.Mutex
	strikes map[string]*strike
	ok      int64
	failed  map[string]int64
	refused int64

	// fcStrikes are the full-check penalties (see ReportBadBundle), kept apart
	// from the strikes of hostile bytes.
	fcStrikes map[string]*fcStrike
	fcReports int64
}

type fcStrike struct {
	n           int
	until, last time.Time
}

type strike struct {
	n     int
	until time.Time
}

// NewFetcher returns a Fetcher.
func NewFetcher(cfg FetchConfig) *Fetcher {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Auth == nil {
		cfg.Auth = discovery.AllowAll{}
	}
	l := cfg.Limits.withDefaults()
	return &Fetcher{cfg: cfg, lim: l,
		glob: newBuckets(l.GlobalPerHour, l.GlobalBurst, 1), pub: newBuckets(l.PerPublisherPerHour, l.PerPublisherBurst, l.MaxTracked),
		slot: make(chan struct{}, l.MaxConcurrent), strikes: map[string]*strike{}, fcStrikes: map[string]*fcStrike{}, failed: map[string]int64{}}
}

// FetchStatus counts downloads since start.
type FetchStatus struct {
	OK          int64
	Failed      map[string]int64
	RateLimited int64
	BackedOff   int
	// FullCheckReports counts bundles reported as failing the local full check;
	// FullCheckBackedOff the publishers skipped for that reason now.
	FullCheckReports   int64
	FullCheckBackedOff int
}

// Status returns the counters.
func (f *Fetcher) Status() FetchStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := FetchStatus{OK: f.ok, Failed: map[string]int64{}, RateLimited: f.refused, FullCheckReports: f.fcReports}
	for k, v := range f.failed {
		st.Failed[k] = v
	}
	now := f.cfg.Now()
	for _, s := range f.strikes {
		if s.until.After(now) {
			st.BackedOff++
		}
	}
	for _, s := range f.fcStrikes {
		if s.until.After(now) {
			st.FullCheckBackedOff++
		}
	}
	return st
}

func strikeKey(nw netid.NetworkID, p identity.NodeID) string { return nw.String() + "|" + p.String() }

func (f *Fetcher) backedOff(nw netid.NetworkID, p identity.NodeID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.cfg.Now()
	if s := f.strikes[strikeKey(nw, p)]; s != nil && s.until.After(now) {
		return true
	}
	s := f.fcStrikes[strikeKey(nw, p)]
	return s != nil && s.until.After(now)
}

func (f *Fetcher) fail(nw netid.NetworkID, p identity.NodeID, reason string, hostile bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed[reason]++
	k := strikeKey(nw, p)
	s := f.strikes[k]
	if s == nil {
		if len(f.strikes) >= f.lim.MaxTracked {
			oldest, at := "", time.Time{}
			for kk, st := range f.strikes {
				if oldest == "" || st.until.Before(at) {
					oldest, at = kk, st.until
				}
			}
			delete(f.strikes, oldest)
		}
		s = &strike{}
		f.strikes[k] = s
	}
	wait := f.lim.SoftBackoff
	if hostile {
		s.n++
		wait = f.lim.BackoffBase
		for i := 1; i < s.n && wait < f.lim.BackoffMax; i++ {
			wait *= 2
		}
		wait = min(wait, f.lim.BackoffMax)
	}
	if until := f.cfg.Now().Add(wait); until.After(s.until) {
		s.until = until
	}
}

// Offers lists the offers for a release across the networks this node
// receives from, best first: known publishers, then those with a fresh
// healthy attestation, then newest. At most MaxOffers are returned.
func (f *Fetcher) Offers(ctx context.Context, releaseHex string) ([]Offer, error) {
	if _, err := contentid.ParseID(releaseHex); err != nil {
		return nil, fmt.Errorf("bundlex: release id: %w", err)
	}
	nets := f.cfg.Networks.ReceivingNetworks()
	if len(nets) == 0 {
		return nil, nil
	}
	if len(nets) > database.PeernetMaxNetworksPerQuery {
		nets = nets[:database.PeernetMaxNetworksPerQuery]
	}
	ids := make([]string, len(nets))
	byID := make(map[string]netid.NetworkID, len(nets))
	for i, n := range nets {
		ids[i] = n.ID.String()
		byID[ids[i]] = n.ID
	}
	rows, err := f.cfg.Store.BundleOffers(ctx, ids, releaseHex, f.lim.MaxOffers)
	if err != nil {
		return nil, err
	}
	known := map[string]map[string]bool{}
	attesters := map[string]map[string]bool{}
	var out []Offer
	for _, r := range rows {
		nw, ok := byID[r.NetworkID]
		pub, perr := identity.ParseNodeID(r.PublisherNodeID)
		raw, berr := hexTo32(r.BundleID)
		if !ok || perr != nil || berr != nil {
			continue
		}
		o := Offer{Network: nw, Publisher: pub, BundleID: raw, Scheme: bundle.Scheme(schemeOf(r.ReleaseScheme)),
			CreatedAt: r.CreatedAt, Own: r.Source == database.PeernetRecordSourceLocal || pub.Equal(f.cfg.Self)}
		if _, done := known[r.NetworkID]; !done {
			nodes := make([]string, 0, len(rows))
			for _, rr := range rows {
				if rr.NetworkID == r.NetworkID {
					nodes = append(nodes, rr.PublisherNodeID)
				}
			}
			k, err := f.cfg.Peers.Known(ctx, nw, nodes)
			if err != nil {
				return nil, err
			}
			known[r.NetworkID] = k
			set := map[string]bool{}
			atts, err := f.cfg.Store.AttestationsFor(ctx, r.NetworkID, releaseHex, "", f.cfg.Now().Add(-records.AttestationValidity))
			if err != nil {
				return nil, err
			}
			for _, a := range atts {
				if a.Healthy {
					set[a.PublisherNodeID] = true
				}
			}
			attesters[r.NetworkID] = set
		}
		o.Known = known[r.NetworkID][r.PublisherNodeID]
		// A publisher's own attestation never raises the rank of its own offer.
		for q := range attesters[r.NetworkID] {
			if q != r.PublisherNodeID {
				o.AttestedHealthy = true
				break
			}
		}
		if !o.Own {
			_, ok, err := f.cfg.Peers.VerifiedPeer(ctx, nw, pub)
			if err != nil {
				return nil, err
			}
			o.Fetchable = ok
		}
		out = append(out, o)
	}
	if err := f.corroborate(ctx, out); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Known != b.Known {
			return a.Known
		}
		if a.AttestedHealthy != b.AttestedHealthy {
			return a.AttestedHealthy
		}
		if a.Corroborators != b.Corroborators {
			return a.Corroborators > b.Corroborators
		}
		return a.CreatedAt.After(b.CreatedAt)
	})
	return out, nil
}

// rangeOf returns the network range of a peer address: /24 for IPv4, /48 for
// IPv6, the host itself for a name.
func rangeOf(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip := net.ParseIP(host)
	switch {
	case ip == nil:
		return "host:" + host
	case ip.To4() != nil:
		return ip.Mask(net.CIDRMask(24, 32)).String() + "/24"
	}
	return ip.Mask(net.CIDRMask(48, 128)).String() + "/48"
}

// corroborate sets, per offer, how many distinct network ranges of known
// verified publishers announce the same Bundle ID. This node's own records and
// publishers without a verified address do not count.
func (f *Fetcher) corroborate(ctx context.Context, offers []Offer) error {
	ranges := map[bundle.BundleID]map[string]struct{}{}
	for _, o := range offers {
		if o.Own || !o.Known {
			continue
		}
		peer, ok, err := f.cfg.Peers.VerifiedPeer(ctx, o.Network, o.Publisher)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if ranges[o.BundleID] == nil {
			ranges[o.BundleID] = map[string]struct{}{}
		}
		ranges[o.BundleID][rangeOf(peer.Address)] = struct{}{}
	}
	for i := range offers {
		offers[i].Corroborators = len(ranges[offers[i].BundleID])
	}
	return nil
}

func schemeOf(stored int) uint8 {
	if stored == database.PeernetSchemeFiles {
		return uint8(bundle.SchemeFiles)
	}
	return uint8(bundle.SchemeNZB)
}

func hexTo32(s string) (bundle.BundleID, error) {
	var id bundle.BundleID
	if len(s) != 64 || !lowerHex(s) {
		return id, errors.New("bad id")
	}
	for i := 0; i < 32; i++ {
		id[i] = unhex(s[2*i])<<4 | unhex(s[2*i+1])
	}
	return id, nil
}

func unhex(c byte) byte {
	if c >= 'a' {
		return c - 'a' + 10
	}
	return c - '0'
}

// Fetch downloads the bundle of release releaseHex from the best offers (at
// most MaxAttempts) and returns the verified bundle and where it came from.
// The Bundle ID and Release ID checked are those of the signed record, never
// anything in the response. A publisher whose bytes fail verification is
// backed off. Fetch never runs speculatively: only a local caller starts it.
func (f *Fetcher) Fetch(ctx context.Context, releaseHex string) (*bundle.Bundle, FetchSource, error) {
	rid, err := contentid.ParseID(releaseHex)
	if err != nil {
		return nil, FetchSource{}, fmt.Errorf("bundlex: release id: %w", err)
	}
	offers, err := f.Offers(ctx, releaseHex)
	if err != nil {
		return nil, FetchSource{}, err
	}
	var cands []Offer
	for _, o := range offers {
		if o.Fetchable && !f.backedOff(o.Network, o.Publisher) {
			cands = append(cands, o)
		}
	}
	if len(cands) == 0 {
		return nil, FetchSource{}, ErrNoOffers
	}
	select {
	case f.slot <- struct{}{}:
		defer func() { <-f.slot }()
	default:
		return nil, FetchSource{}, ErrFetchBusy
	}
	var errs []error
	attempts := 0
	for _, o := range cands {
		if attempts >= f.lim.MaxAttempts || ctx.Err() != nil {
			break
		}
		now := f.cfg.Now()
		if !f.glob.take("all", 1, now) {
			f.mu.Lock()
			f.refused++
			f.mu.Unlock()
			if attempts == 0 {
				return nil, FetchSource{}, ErrRateLimited
			}
			break
		}
		if !f.pub.take(strikeKey(o.Network, o.Publisher), 1, now) {
			continue
		}
		attempts++
		b, err := f.attempt(ctx, o, rid)
		if err == nil {
			f.mu.Lock()
			f.ok++
			f.mu.Unlock()
			return b, FetchSource{Network: o.Network, Publisher: o.Publisher, BundleID: o.BundleID, Scheme: o.Scheme, Corroborators: o.Corroborators}, nil
		}
		errs = append(errs, err)
	}
	if ctx.Err() != nil {
		return nil, FetchSource{}, ctx.Err()
	}
	if len(errs) == 0 {
		return nil, FetchSource{}, ErrNoOffers
	}
	return nil, FetchSource{}, fmt.Errorf("%w: %w", ErrAllFailed, errors.Join(errs...))
}

type fetchError struct{ reason string }

func (e *fetchError) Error() string { return "bundlex: fetch failed: " + e.reason }

func (f *Fetcher) attempt(ctx context.Context, o Offer, rid contentid.ID) (*bundle.Bundle, error) {
	nets := f.cfg.Networks.ReceivingNetworks()
	var info recsync.NetworkInfo
	for _, n := range nets {
		if n.ID == o.Network {
			info = n
		}
	}
	peer, ok, err := f.cfg.Peers.VerifiedPeer(ctx, o.Network, o.Publisher)
	if err != nil || !ok || info.ID.IsZero() {
		f.fail(o.Network, o.Publisher, failTransport, false)
		return nil, &fetchError{failTransport}
	}
	hdr := http.Header{}
	if err := f.cfg.Auth.DecorateOutbound(ctx, o.Network, o.Publisher, hdr); err != nil {
		f.fail(o.Network, o.Publisher, failMembership, false)
		return nil, &fetchError{failMembership}
	}
	resp, err := f.cfg.Dialer.Do(ctx, peer.Address, peer.NodeID, transport.Request{
		Method: http.MethodGet, Path: PathBundle, Header: hdr,
		Query: url.Values{queryNet: {o.Network.String()}, queryBundle: {o.BundleID.String()}},
	}, transport.CallOptions{AllowPrivate: info.AllowPrivate, TrustedAddress: peer.Trusted,
		MaxResponseBytes: bundle.MaxCompressedSize, Timeout: f.lim.Timeout})
	if err != nil {
		reason, hostile := failTransport, false
		if errors.Is(err, transport.ErrBodyTooLarge) {
			reason, hostile = failTooLarge, true
		}
		f.fail(o.Network, o.Publisher, reason, hostile)
		return nil, &fetchError{reason}
	}
	if err := f.cfg.Auth.VerifyResponse(ctx, o.Network, o.Publisher, resp.StatusCode, resp.Header); err != nil {
		_ = resp.Body.Close()
		f.fail(o.Network, o.Publisher, failMembership, false)
		return nil, &fetchError{failMembership}
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		reason := failStatus
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
			reason = failRefused
		}
		f.fail(o.Network, o.Publisher, reason, false)
		return nil, &fetchError{reason}
	}
	wire, err := resp.ReadAll()
	if err != nil {
		reason, hostile := failTruncated, false
		if errors.Is(err, transport.ErrBodyTooLarge) {
			reason, hostile = failTooLarge, true
		}
		f.fail(o.Network, o.Publisher, reason, hostile)
		return nil, &fetchError{reason}
	}
	b, err := bundle.Verify(wire, o.BundleID, rid)
	if err != nil {
		f.fail(o.Network, o.Publisher, failVerification, true)
		return nil, &fetchError{failVerification}
	}
	if b.Scheme != o.Scheme {
		f.fail(o.Network, o.Publisher, failScheme, true)
		return nil, &fetchError{failScheme}
	}
	return b, nil
}

// ReportBadBundle records that a bundle from publisher p passed hash
// verification but its release then failed the local full health check. The
// publisher is skipped for bundle fetches only: 30 minutes, doubling per
// report, capped at 6 hours, and the doubling decays after a day without a
// report. It never touches record sync or stored records. The penalty is mild
// because the same failure is also produced by this node's own providers
// lacking articles the publisher's had; a later verified bundle from the same
// publisher clears it (ReportVerifiedBundle).
func (f *Fetcher) ReportBadBundle(nw netid.NetworkID, p identity.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fcReports++
	now := f.cfg.Now()
	k := strikeKey(nw, p)
	s := f.fcStrikes[k]
	if s == nil {
		if len(f.fcStrikes) >= f.lim.MaxTracked {
			oldest, at := "", time.Time{}
			for kk, st := range f.fcStrikes {
				if oldest == "" || st.until.Before(at) {
					oldest, at = kk, st.until
				}
			}
			delete(f.fcStrikes, oldest)
		}
		s = &fcStrike{}
		f.fcStrikes[k] = s
	}
	if !s.last.IsZero() && now.Sub(s.last) > f.lim.FullCheckDecay {
		s.n = 0
	}
	s.n++
	s.last = now
	wait := f.lim.FullCheckBackoffBase
	for i := 1; i < s.n && wait < f.lim.FullCheckBackoffMax; i++ {
		wait *= 2
	}
	s.until = now.Add(min(wait, f.lim.FullCheckBackoffMax))
}

// ReportVerifiedBundle records that a bundle from publisher p passed its full
// local check, which clears any full-check penalty of that publisher.
func (f *Fetcher) ReportVerifiedBundle(nw netid.NetworkID, p identity.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.fcStrikes, strikeKey(nw, p))
}
