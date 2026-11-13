package recsync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// RoundResult summarises one SyncRound.
type RoundResult struct {
	// Peers is how many peers were synced with (back-off peers excluded).
	Peers int
	// Stored, Duplicates and Rejected count the records pulled this round by
	// outcome; Pushed the records handed to peers.
	Stored, Duplicates, Rejected, Pushed int
	// Backlog is set when a peer still had more records after the round's
	// page budget: the caller should run the next round soon.
	Backlog bool
	// Err is the first failure of this node's own store (a failing peer is not
	// an error here).
	Err error
}

type roundCounters struct {
	peers, stored, dups, rejected, pushed atomic.Int64
	backlog                               atomic.Bool
	errMu                                 sync.Mutex
	err                                   error
}

func (c *roundCounters) fail(err error) {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.err == nil {
		c.err = err
	}
}

// SyncRound runs one round for every registered network: for each verified
// peer it pulls records past the stored cursor (policy Pull) and pushes own
// records past the push cursor (policy Push). It is bounded by the page and
// batch budgets, a per-peer timeout and a whole-round timeout, stops when ctx
// ends, and never runs concurrently with itself. A peer in back-off is
// skipped. The caller schedules rounds.
func (x *Exchange) SyncRound(ctx context.Context) RoundResult {
	x.roundMu.Lock()
	defer x.roundMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, x.limits.RoundTimeout)
	defer cancel()
	var rc roundCounters
	sem := make(chan struct{}, x.limits.MaxConcurrentPeers)
	var wg sync.WaitGroup
loop:
	for _, nc := range x.networks() {
		peers, err := x.selectPeers(ctx, nc)
		if err != nil {
			rc.fail(err)
			x.logChange(ctx, "list sync peers "+nc.ID.String()[:8], err)
			continue
		}
		x.logChange(ctx, "list sync peers "+nc.ID.String()[:8], nil)
		for _, p := range peers {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				break loop
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				x.syncPeer(ctx, nc, p, &rc)
			}()
		}
	}
	wg.Wait()
	return RoundResult{
		Peers: int(rc.peers.Load()), Stored: int(rc.stored.Load()), Duplicates: int(rc.dups.Load()),
		Rejected: int(rc.rejected.Load()), Pushed: int(rc.pushed.Load()), Backlog: rc.backlog.Load(), Err: rc.err,
	}
}

// selectPeers returns the peers to sync with in a network: not this node,
// not in back-off. A nil peer source means no peers.
func (x *Exchange) selectPeers(ctx context.Context, nc NetworkInfo) ([]Peer, error) {
	if x.cfg.Peers == nil {
		return nil, nil
	}
	all, err := x.cfg.Peers.SyncPeers(ctx, nc.ID, x.limits.MaxPeersPerNetwork)
	if err != nil {
		return nil, err
	}
	now := x.now()
	out := make([]Peer, 0, len(all))
	for _, p := range all {
		if p.NodeID.IsZero() || p.NodeID.Equal(x.cfg.Self) || x.backoff.active(nc.ID, p.NodeID, now) {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

func (x *Exchange) syncPeer(ctx context.Context, nc NetworkInfo, p Peer, rc *roundCounters) {
	rc.peers.Add(1)
	pctx, cancel := context.WithTimeout(ctx, x.limits.PeerTimeout)
	defer cancel()
	pol := PolicyFor(nc.Level)
	if pol.Pull {
		x.pullPeer(pctx, nc, p, rc)
	}
	// A peer that just misbehaved gets nothing from us this round.
	if pol.Push && !x.backoff.active(nc.ID, p.NodeID, x.now()) {
		x.pushPeer(pctx, nc, p, rc)
	}
}

// current reports whether nc is still registered with a policy satisfying
// ok, so a removed or downgraded network stops mid-round.
func (x *Exchange) current(nc NetworkInfo, ok func(Policy) bool) bool {
	cur, found := x.network(nc.ID)
	return found && ok(PolicyFor(cur.Level))
}

// call makes one authorised, network-scoped request. The membership proof is
// verified first, whatever the status, and only then may the caller read the
// status. The caller owns the body.
func (x *Exchange) call(ctx context.Context, nc NetworkInfo, p Peer, method string, extra url.Values, body []byte, maxResp int64) (*transport.Response, error) {
	hdr := http.Header{}
	if len(body) > 0 {
		hdr.Set("Content-Type", "application/octet-stream")
	}
	if err := x.auth.DecorateOutbound(ctx, nc.ID, p.NodeID, hdr); err != nil {
		return nil, fmt.Errorf("recsync: decorate request: %w", err)
	}
	q := url.Values{queryNet: {nc.ID.String()}}
	for k, v := range extra {
		q[k] = v
	}
	resp, err := x.cfg.Dialer.Do(ctx, p.Address, p.NodeID, transport.Request{
		Method: method, Path: PathRecords, Query: q, Header: hdr, Body: body,
	}, transport.CallOptions{AllowPrivate: nc.AllowPrivate, MaxResponseBytes: maxResp,
		Timeout: x.limits.CallTimeout, TrustedAddress: p.Trusted})
	if err != nil {
		return nil, err
	}
	if err := x.auth.VerifyResponse(ctx, nc.ID, p.NodeID, resp.StatusCode, resp.Header); err != nil {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("recsync: verify response: %w", err)
	}
	return resp, nil
}

var (
	errPeerStatus = errors.New("recsync: unexpected peer status")
	errBadPage    = errors.New("recsync: peer sent an inconsistent page")
)

// pullPeer pulls pages from one peer until it has no more, the page budget is
// spent or the network stops being pulled. The cursor moves only past a page
// whose records were all processed; a rejected record still counts as
// processed, so one bad record cannot stall the puller.
func (x *Exchange) pullPeer(ctx context.Context, nc NetworkInfo, p Peer, rc *roundCounters) {
	nw, node := nc.ID.String(), p.NodeID.String()
	cur, err := x.cfg.Cursors.Get(ctx, nw, node)
	if err != nil {
		rc.fail(err)
		x.logChange(ctx, "read sync cursor "+nw[:8], err)
		return
	}
	since, epoch := cur.PulledSeq, cur.RemoteEpoch
	if epoch == "" {
		since = 0 // a cursor without a known epoch cannot be trusted
	}
	hostile := 0
	for page := 0; ; page++ {
		if page >= x.limits.MaxPagesPerPeerRound {
			rc.backlog.Store(true)
			return
		}
		if ctx.Err() != nil || !x.current(nc, func(po Policy) bool { return po.Pull }) {
			return
		}
		pg, err := x.fetchPage(ctx, nc, p, since)
		switch {
		case errors.Is(err, errPeerStatus), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return
		case errors.Is(err, ErrMalformedWire), errors.Is(err, transport.ErrBodyTooLarge):
			x.penalize(nc.ID, p.NodeID)
			return
		case err != nil:
			return // transport or membership failure: discovery tracks the peer
		}
		if pg.Epoch != epoch {
			// First contact, or the peer's sequence restarted (it left and
			// rejoined, or lost its database): the stored cursor means
			// nothing now. Start over and ask again; the page just received
			// was cut for the wrong cursor. A peer that keeps changing its
			// epoch only makes us start over, so resets are bounded per hour.
			if epoch != "" && !x.epochFlips.allow(nw+"|"+node, x.now()) {
				x.penalize(nc.ID, p.NodeID)
				x.logChange(ctx, "peer epoch "+nw[:8], errEpochFlapping)
				return
			}
			if err := x.cfg.Cursors.ResetPullCursorEpoch(ctx, nw, node, 0, pg.Epoch); err != nil {
				rc.fail(err)
				x.logChange(ctx, "reset sync cursor "+nw[:8], err)
				return
			}
			epoch = pg.Epoch
			if since != 0 {
				since = 0
				continue
			}
		}
		if pg.HighestSeq < since || (pg.More && pg.HighestSeq <= since) {
			x.penalize(nc.ID, p.NodeID)
			x.logChange(ctx, "peer cursor "+nw[:8], errBadPage)
			return
		}
		x.noteSuccessfulPull(nc.ID)
		h, err := x.ingestPage(ctx, nc, p, pg.Records, rc)
		hostile += h
		if errors.Is(err, ErrIngestRateLimited) {
			// Over the node-wide ingest rate: stop for this round and keep the
			// cursor where it is, so nothing unprocessed is skipped.
			rc.backlog.Store(true)
			return
		}
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, errStopped) {
				rc.fail(err)
				x.logChange(ctx, "ingest pulled record "+nw[:8], err)
			}
			return
		}
		x.logChange(ctx, "ingest pulled record "+nw[:8], nil)
		if pg.HighestSeq > since {
			if err := x.cfg.Cursors.SetPullCursor(context.WithoutCancel(ctx), nw, node, pg.HighestSeq); err != nil {
				rc.fail(err)
				x.logChange(ctx, "advance sync cursor "+nw[:8], err)
				return
			}
			since = pg.HighestSeq
		}
		if hostile >= x.limits.InvalidPerRound {
			// Back-off only: the rows it delivered stay, because a relayer
			// may well have delivered honest publishers' records first and
			// those are not re-sent once the cursors have moved on.
			x.penalize(nc.ID, p.NodeID)
			return
		}
		// A page that stores nothing new but moves the cursor is an honest
		// peer whose records we already hold; a replay loop does not advance
		// the cursor and is caught above.
		if !pg.More {
			x.backoff.clear(nc.ID, p.NodeID)
			return
		}
	}
}

var (
	errStopped       = errors.New("recsync: network stopped")
	errEpochFlapping = errors.New("recsync: peer keeps changing its epoch")
)

// fetchPage requests and decodes one page.
func (x *Exchange) fetchPage(ctx context.Context, nc NetworkInfo, p Peer, since int64) (Page, error) {
	resp, err := x.call(ctx, nc, p, http.MethodGet, url.Values{querySince: {fmt.Sprint(since)}}, nil, MaxPageBytes)
	if err != nil {
		return Page{}, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return Page{}, fmt.Errorf("%w: records %d", errPeerStatus, resp.StatusCode)
	}
	body, err := resp.ReadAll()
	if err != nil {
		return Page{}, err
	}
	return DecodePage(body)
}

// ingestPage runs every record of a page through the pipeline and returns how
// many looked hostile. It stops, without finishing
// the page, when ctx ends or the network is removed or no longer pulls.
func (x *Exchange) ingestPage(ctx context.Context, nc NetworkInfo, p Peer, recs [][]byte, rc *roundCounters) (hostile int, err error) {
	for _, raw := range recs {
		if err := ctx.Err(); err != nil {
			return hostile, err
		}
		if !x.current(nc, func(po Policy) bool { return po.Pull }) {
			return hostile, errStopped
		}
		out, err := x.ing.IngestFrom(ctx, nc.ID, raw, OriginPeer, p.NodeID)
		if err != nil {
			return hostile, err
		}
		switch out.Result {
		case ResultStored:
			rc.stored.Add(1)
		case ResultDuplicate:
			rc.dups.Add(1)
		default:
			rc.rejected.Add(1)
			if out.Reason.Hostile() {
				hostile++
			}
		}
	}
	return hostile, nil
}

// pushPeer pushes this node's own records past the push cursor. The cursor
// moves only after the peer answered 200, so a refused or failed push is
// retried next round. Relayed records are not pushed: relaying is pull-only.
func (x *Exchange) pushPeer(ctx context.Context, nc NetworkInfo, p Peer, rc *roundCounters) {
	nw, node := nc.ID.String(), p.NodeID.String()
	cur, err := x.cfg.Cursors.Get(ctx, nw, node)
	if err != nil {
		rc.fail(err)
		x.logChange(ctx, "read sync cursor "+nw[:8], err)
		return
	}
	after := cur.PushedSeq
	for batch := 0; batch < x.limits.MaxPushBatchesPerPeerRound; batch++ {
		if ctx.Err() != nil || !x.current(nc, func(po Policy) bool { return po.Push }) {
			return
		}
		page, err := x.cfg.Store.ListOwnSince(ctx, nw, after, MaxPushRecords)
		if err != nil {
			rc.fail(err)
			x.logChange(ctx, "list own records "+nw[:8], err)
			return
		}
		if len(page.Rows) > 0 {
			payloads := make([][]byte, len(page.Rows))
			for i, r := range page.Rows {
				payloads[i] = r.Payload
			}
			if !x.sendBatch(ctx, nc, p, payloads) {
				return
			}
			rc.pushed.Add(int64(len(payloads)))
		}
		if page.HighestSeq > after {
			if err := x.cfg.Cursors.SetPushCursor(context.WithoutCancel(ctx), nw, node, page.HighestSeq); err != nil {
				rc.fail(err)
				x.logChange(ctx, "advance push cursor "+nw[:8], err)
				return
			}
			after = page.HighestSeq
		}
		if !page.More {
			return
		}
	}
	rc.backlog.Store(true)
}

// sendBatch posts one batch and reports whether the peer accepted it.
func (x *Exchange) sendBatch(ctx context.Context, nc NetworkInfo, p Peer, payloads [][]byte) bool {
	body, err := EncodePush(payloads)
	if err != nil {
		return false
	}
	resp, err := x.call(ctx, nc, p, http.MethodPost, nil, body, MaxPushResultBytes)
	if err != nil {
		return false
	}
	raw, err := resp.ReadAll()
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	_, err = DecodePushResult(raw)
	return err == nil
}

// penalize puts a peer in back-off for misbehaving and counts it.
func (x *Exchange) penalize(nw netid.NetworkID, peer identity.NodeID) {
	x.backoff.strike(nw, peer, x.now())
	x.ing.noteBackoff(nw)
}
