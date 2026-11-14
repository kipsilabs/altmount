package bundlex

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Announcement is what this node remembers of a bundle it announced.
type Announcement struct {
	ReleaseID string
	// OriginCategory and OriginIndexer are the private origin tags kept with
	// the publish state, used to re-check exclusions at serve time.
	OriginCategory, OriginIndexer string
}

// Announcements is this node's record of what it announced.
type Announcements interface {
	// Announced finds the release this node announced in nw with Bundle ID id.
	Announced(ctx context.Context, nw netid.NetworkID, id bundle.BundleID) (Announcement, bool, error)
	// Withdraw deletes this node's records and publish state of the release.
	Withdraw(ctx context.Context, nw netid.NetworkID, releaseHex string) error
	// MarkStale makes the release's record be announced again.
	MarkStale(ctx context.Context, nw netid.NetworkID, releaseHex string) error
}

// Provider returns the packed bundle of a release (building it if needed).
type Provider interface {
	Bundle(ctx context.Context, releaseHex string) (*Built, error)
}

// NetworkLevels reports the exposure level of a registered network.
type NetworkLevels interface {
	NetworkLevel(id netid.NetworkID) (int, bool)
}

// ServerConfig wires the serve route.
type ServerConfig struct {
	Networks      NetworkLevels
	Auth          discovery.Authorizer
	Announcements Announcements
	Provider      Provider
	Exclusions    func() publish.Exclusions
	// BackedOff reports a peer or address record exchange has struck.
	BackedOff func(nw netid.NetworkID, peer identity.NodeID, ip string) bool
	Limits    ServeLimits
	Now       func() time.Time
	Log       func(ctx context.Context, key string, err error)
}

// Server serves the bundles this node announced.
type Server struct {
	cfg    ServerConfig
	lim    ServeLimits
	peers  *buckets
	ips    *buckets
	bytes  *buckets
	perPer *counter
	perIP  *counter
	slots  chan struct{}

	mu       sync.Mutex
	served   int64
	sentByte int64
	refused  map[string]int64
}

// NewServer returns a Server.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Auth == nil {
		cfg.Auth = discovery.AllowAll{}
	}
	if cfg.Log == nil {
		cfg.Log = func(context.Context, string, error) {}
	}
	l := cfg.Limits.withDefaults()
	s := &Server{cfg: cfg, lim: l,
		peers:  newBuckets(l.PerPeerPerHour, l.PerPeerBurst, l.MaxTracked),
		ips:    newBuckets(l.PerIPPerHour, l.PerIPBurst, l.MaxTracked),
		perPer: newCounter(), perIP: newCounter(),
		slots: make(chan struct{}, l.MaxConcurrent), refused: map[string]int64{}}
	// The byte budget refills per minute; express it per hour for the bucket.
	s.bytes = &buckets{rate: float64(l.BytesPerMinute) / 60, burst: float64(l.BytesBurst), max: 1, m: map[string]*bucket{}}
	return s
}

// RegisterRoutes adds the bundle route (no request body allowed).
func (s *Server) RegisterRoutes(r recsync.Registrar) error {
	return r.Handle("GET "+PathBundle, 0, http.HandlerFunc(s.handle))
}

// ServeStatus counts what was served since start.
type ServeStatus struct {
	Served, Bytes int64
	Refused       map[string]int64
}

// Status returns the counters.
func (s *Server) Status() ServeStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref := make(map[string]int64, len(s.refused))
	for k, v := range s.refused {
		ref[k] = v
	}
	return ServeStatus{Served: s.served, Bytes: s.sentByte, Refused: ref}
}

func (s *Server) refuse(reason string) {
	s.mu.Lock()
	s.refused[reason]++
	s.mu.Unlock()
}

func text(w http.ResponseWriter, dec discovery.ResponseDecorator, code int, msg string) {
	h := w.Header()
	if dec != nil {
		dec(h)
	}
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, msg+"\n")
}

// deny is the one answer for every refusal that must not tell the caller why:
// level below 3, unknown network, denied caller, unknown or unavailable bundle.
func (s *Server) deny(w http.ResponseWriter, reason string) {
	s.refuse(reason)
	text(w, nil, http.StatusForbidden, "forbidden")
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	peer, ok := transport.PeerFromContext(r.Context())
	if !ok {
		s.deny(w, "unauthenticated")
		return
	}
	q := r.URL.Query()
	nv, bv := q[queryNet], q[queryBundle]
	if len(nv) != 1 || len(bv) != 1 || len(q) != 2 {
		text(w, nil, http.StatusBadRequest, "bad request")
		return
	}
	nw, err := netid.ParseNetworkID(nv[0])
	var bid bundle.BundleID
	raw, herr := hex.DecodeString(bv[0])
	if err != nil || herr != nil || len(raw) != len(bid) || !lowerHex(bv[0]) {
		text(w, nil, http.StatusBadRequest, "bad request")
		return
	}
	copy(bid[:], raw)

	level, known := s.cfg.Networks.NetworkLevel(nw)
	if !known || !recsync.PolicyFor(level).ServeBundles {
		s.deny(w, "level")
		return
	}
	dec, err := s.cfg.Auth.AuthorizeInbound(r.Context(), nw, peer, r.Header.Clone())
	if err != nil {
		s.deny(w, "denied")
		return
	}
	ip := ""
	if a, ok := transport.RemoteIPFromContext(r.Context()); ok {
		ip = a.String()
	}
	if s.cfg.BackedOff != nil && s.cfg.BackedOff(nw, peer, ip) {
		s.deny(w, "backed_off")
		return
	}
	// From here the caller is a member: tell it about load, not about content.
	now := s.cfg.Now()
	if !s.peers.take(peer.String(), 1, now) || (ip != "" && !sharedIP(ip) && !s.ips.take(ip, 1, now)) {
		s.refuse("rate_limited")
		text(w, dec, http.StatusTooManyRequests, "rate limited")
		return
	}
	release, ok := s.admit(peer.String(), ip)
	if !ok {
		s.refuse("busy")
		text(w, dec, http.StatusTooManyRequests, "busy")
		return
	}
	defer release()

	ann, found, err := s.cfg.Announcements.Announced(r.Context(), nw, bid)
	if err != nil {
		s.cfg.Log(r.Context(), "bundle announcement lookup", err)
		s.refuse("error")
		text(w, dec, http.StatusServiceUnavailable, "unavailable")
		return
	}
	if !found {
		s.deny(w, "unknown_bundle")
		return
	}
	if s.cfg.Exclusions != nil && s.cfg.Exclusions().CoversOrigin(ann.OriginCategory, ann.OriginIndexer) {
		if err := s.cfg.Announcements.Withdraw(r.Context(), nw, ann.ReleaseID); err != nil {
			s.cfg.Log(r.Context(), "withdraw excluded release", err)
		}
		s.deny(w, "excluded")
		return
	}
	built, err := s.cfg.Provider.Bundle(r.Context(), ann.ReleaseID)
	switch {
	case errors.Is(err, ErrBusy):
		s.refuse("busy")
		text(w, dec, http.StatusServiceUnavailable, "busy")
		return
	case err != nil || built == nil || built.ID != bid:
		// Not buildable any more, or not the bytes that were announced (the
		// library changed): never serve other bytes; announce again.
		if err == nil {
			_ = s.cfg.Announcements.MarkStale(r.Context(), nw, ann.ReleaseID)
		}
		s.deny(w, "unavailable")
		return
	}
	if !s.bytes.take("all", float64(len(built.Wire)), now) {
		s.refuse("byte_budget")
		text(w, dec, http.StatusTooManyRequests, "busy")
		return
	}
	s.send(w, dec, built.Wire)
}

func lowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// admit takes a global slot and the per-peer and per-address slots.
func (s *Server) admit(peer, ip string) (func(), bool) {
	select {
	case s.slots <- struct{}{}:
	default:
		return nil, false
	}
	if !s.perPer.enter(peer, s.lim.MaxConcurrentPerPeer) {
		<-s.slots
		return nil, false
	}
	useIP := ip != "" && !sharedIP(ip)
	if useIP && !s.perIP.enter(ip, s.lim.MaxConcurrentPerIP) {
		s.perPer.leave(peer)
		<-s.slots
		return nil, false
	}
	return func() {
		if useIP {
			s.perIP.leave(ip)
		}
		s.perPer.leave(peer)
		<-s.slots
	}, true
}

const chunkSize = 32 << 10

// send streams body with an exact Content-Length, a deadline per chunk and a
// total deadline. Range requests are not supported: the header is ignored and
// the whole body is sent. The body is stored compressed and never re-encoded.
func (s *Server) send(w http.ResponseWriter, dec discovery.ResponseDecorator, body []byte) {
	h := w.Header()
	if dec != nil {
		dec(h)
	}
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", itoa(len(body)))
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Accept-Ranges", "none")
	h.Del("Content-Encoding")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	start := s.cfg.Now()
	sent := 0
	for sent < len(body) {
		dl := start.Add(s.lim.TotalDeadline)
		if c := s.cfg.Now().Add(s.lim.ChunkDeadline); c.Before(dl) {
			dl = c
		}
		_ = rc.SetWriteDeadline(dl)
		n := min(chunkSize, len(body)-sent)
		if _, err := w.Write(body[sent : sent+n]); err != nil {
			break
		}
		sent += n
		if s.cfg.Now().Sub(start) > s.lim.TotalDeadline {
			break
		}
	}
	s.mu.Lock()
	s.sentByte += int64(sent)
	if sent == len(body) {
		s.served++
	} else {
		s.refused["aborted"]++
	}
	s.mu.Unlock()
}

func itoa(n int) string {
	var b [20]byte
	i := len(b)
	if n == 0 {
		return "0"
	}
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
