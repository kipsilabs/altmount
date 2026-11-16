package recsync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Routes. Both use the same path with different methods; the network is
// always named by the "n" query parameter, exactly one.
const (
	PathRecords = "/peer/v1/records"
	RoutePull   = "GET " + PathRecords
	RoutePush   = "POST " + PathRecords
	queryNet    = "n"
	querySince  = "since"
	maxSinceLen = 18 // digits, always fits int64
)

// Registrar is satisfied by *transport.Server.
type Registrar interface {
	Handle(pattern string, maxBody int64, h http.Handler) error
}

// RegisterRoutes adds the record routes to r, each with a request body cap
// (a pull has no body; a push is capped at MaxPushBody). It does not start a
// server. Every request is scoped to one network the caller is admitted to by
// the same authoriser discovery uses.
func (x *Exchange) RegisterRoutes(r Registrar) error {
	if err := r.Handle(RoutePull, 0, http.HandlerFunc(x.handlePull)); err != nil {
		return err
	}
	return r.Handle(RoutePush, MaxPushBody, http.HandlerFunc(x.handlePush))
}

func replyText(w http.ResponseWriter, dec discovery.ResponseDecorator, code int, msg string) {
	h := w.Header()
	if dec != nil {
		dec(h)
	}
	// Set after the decorator so it cannot change how the body is read.
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, msg+"\n")
}

func replyBinary(w http.ResponseWriter, dec discovery.ResponseDecorator, body []byte) {
	h := w.Header()
	if dec != nil {
		dec(h)
	}
	h.Set("Content-Type", "application/octet-stream")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// admit resolves the caller and network and runs the authoriser. On failure
// it has already answered. An unknown network, a network at level 0 and a
// network that denies the caller all get the identical answer, so the route
// does not reveal which networks this node serves.
func (x *Exchange) admit(w http.ResponseWriter, r *http.Request) (identity.NodeID, NetworkInfo, discovery.ResponseDecorator, bool) {
	var zero NetworkInfo
	peer, ok := transport.PeerFromContext(r.Context())
	if !ok {
		replyText(w, nil, http.StatusUnauthorized, "unauthorized")
		return identity.NodeID{}, zero, nil, false
	}
	vals := r.URL.Query()[queryNet]
	if len(vals) != 1 {
		replyText(w, nil, http.StatusBadRequest, "bad request")
		return peer, zero, nil, false
	}
	id, err := netid.ParseNetworkID(vals[0])
	if err != nil {
		replyText(w, nil, http.StatusBadRequest, "bad request")
		return peer, zero, nil, false
	}
	info, ok := x.network(id)
	if !ok || !PolicyFor(info.Level).Exists {
		replyText(w, nil, http.StatusForbidden, "forbidden")
		return peer, zero, nil, false
	}
	dec, err := x.auth.AuthorizeInbound(r.Context(), id, peer, r.Header.Clone())
	if err != nil {
		replyText(w, nil, http.StatusForbidden, "forbidden")
		return peer, zero, nil, false
	}
	return peer, info, dec, true
}

// parseSince reads the optional cursor: digits only, at most maxSinceLen.
func parseSince(r *http.Request) (int64, bool) {
	vals := r.URL.Query()[querySince]
	switch len(vals) {
	case 0:
		return 0, true
	case 1:
	default:
		return 0, false
	}
	s := vals[0]
	if s == "" || len(s) > maxSinceLen {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil
}

func (x *Exchange) handlePull(w http.ResponseWriter, r *http.Request) {
	_, info, dec, ok := x.admit(w, r)
	if !ok {
		return
	}
	since, ok := parseSince(r)
	if !ok {
		replyText(w, dec, http.StatusBadRequest, "bad request")
		return
	}
	body, err := x.servePage(r.Context(), info, since)
	if err != nil {
		x.logChange(r.Context(), "serve records "+info.ID.String()[:8], err)
		replyText(w, dec, http.StatusInternalServerError, "error")
		return
	}
	x.logChange(r.Context(), "serve records "+info.ID.String()[:8], nil)
	replyBinary(w, dec, body)
}

// servePage builds the encoded page for a pull. A network whose policy does
// not serve (level 1) answers with an empty page that keeps the caller's
// cursor, so the puller learns nothing and does not move.
func (x *Exchange) servePage(ctx context.Context, info NetworkInfo, since int64) ([]byte, error) {
	epoch, err := x.cfg.Store.Epoch(ctx, info.ID.String())
	if err != nil {
		return nil, err
	}
	page := Page{Epoch: epoch, HighestSeq: since}
	if PolicyFor(info.Level).Serve {
		rows, err := x.cfg.Store.ListSince(ctx, info.ID.String(), since, PageRecords)
		if err != nil {
			return nil, err
		}
		page.HighestSeq, page.More = rows.HighestSeq, rows.More
		for _, row := range rows.Rows {
			page.Records = append(page.Records, row.Payload)
		}
	}
	return EncodePage(page)
}

func (x *Exchange) handlePush(w http.ResponseWriter, r *http.Request) {
	peer, info, dec, ok := x.admit(w, r)
	if !ok {
		return
	}
	if !PolicyFor(info.Level).AcceptPush {
		replyText(w, nil, http.StatusForbidden, "forbidden")
		return
	}
	ip := ""
	if addr, ok := transport.RemoteIPFromContext(r.Context()); ok {
		ip = addr.String()
	}
	// A pusher backed off for hostile records (by node ID or by address) is
	// refused before anything is read.
	if x.pusherBackedOff(info.ID, peer, ip) {
		replyText(w, dec, http.StatusTooManyRequests, "backed off")
		return
	}
	// Per node and per address, a push counts from the first byte: slow-body
	// connections of one source cannot pile up. They do not hold a global
	// ingest slot, which is only taken once the body has arrived.
	release, ok := x.pushGuard.enter(peer.String(), ip)
	if !ok {
		replyText(w, dec, http.StatusTooManyRequests, "busy")
		return
	}
	defer release()
	body, err := io.ReadAll(r.Body) // already capped by the transport route
	if err != nil {
		replyText(w, dec, http.StatusBadRequest, "bad request")
		return
	}
	recs, err := DecodePush(body)
	if err != nil {
		replyText(w, dec, http.StatusBadRequest, "bad request")
		return
	}
	// Every record of an accepted push uses up the peer's allowance, valid or
	// not, before any signature work is done.
	if !x.budget.take(peer, len(recs), x.now()) {
		replyText(w, dec, http.StatusTooManyRequests, "rate limited")
		return
	}
	select {
	case x.pushSlots <- struct{}{}:
		defer func() { <-x.pushSlots }()
	default:
		replyText(w, dec, http.StatusTooManyRequests, "busy")
		return
	}
	var res PushResult
	hostile := 0
	for _, raw := range recs {
		if cur, ok := x.network(info.ID); !ok || !PolicyFor(cur.Level).AcceptPush {
			replyText(w, nil, http.StatusForbidden, "forbidden") // removed meanwhile
			return
		}
		out, err := x.ing.IngestFrom(r.Context(), info.ID, raw, OriginPeer, peer)
		if errors.Is(err, ErrIngestRateLimited) {
			x.notePushHostility(r.Context(), info.ID, peer, ip, hostile)
			replyText(w, dec, http.StatusTooManyRequests, "rate limited")
			return
		}
		if err != nil {
			x.logChange(r.Context(), "ingest pushed record "+info.ID.String()[:8], err)
			replyText(w, dec, http.StatusServiceUnavailable, "unavailable")
			return
		}
		switch out.Result {
		case ResultStored:
			res.Accepted++
		case ResultDuplicate:
			res.Duplicate++
		default:
			res.Rejected++
			if out.Reason.Hostile() {
				hostile++
			}
		}
	}
	x.logChange(r.Context(), "ingest pushed record "+info.ID.String()[:8], nil)
	x.notePushHostility(r.Context(), info.ID, peer, ip, hostile)
	replyBinary(w, dec, EncodePushResult(res))
}

// pusherBackedOff reports whether the pusher is refused: its node ID or its
// remote address is in back-off.
func (x *Exchange) pusherBackedOff(nw netid.NetworkID, peer identity.NodeID, ip string) bool {
	now := x.now()
	return x.backoff.active(nw, peer, now) || (ip != "" && x.backoff.activeKey(ipBackoffKey(ip), now))
}

// notePushHostility adds the hostile records of a push to the pusher's tally
// and, once InvalidPerRound of them fall inside a minute, backs off the node
// ID the same way a hostile pull source is. The delivered rows are kept. The
// remote address is struck only after hostile strikes from several distinct
// node IDs behind it, and never when it is a private, loopback or link-local
// address, so one hostile node cannot lock out honest nodes sharing its
// address.
func (x *Exchange) notePushHostility(_ context.Context, nw netid.NetworkID, peer identity.NodeID, ip string, hostile int) {
	if hostile == 0 {
		return
	}
	key := nw.String() + "|" + peer.String()
	if x.hostile.add(key, hostile, x.now(), time.Minute) < x.limits.InvalidPerRound {
		return
	}
	x.hostile.reset(key)
	x.penalize(nw, peer)
	if ip != "" && strikableIP(ip) && x.ipHostile.add(ip, peer.String(), x.now()) {
		x.backoff.strikeKey(ipBackoffKey(ip), x.now())
	}
}
