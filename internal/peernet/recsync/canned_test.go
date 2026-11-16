package recsync

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// cannedDialer answers every call from a function, with no network.
type cannedDialer struct {
	calls atomic.Int32
	reply func(req transport.Request, call int) (int, []byte)
}

func (c *cannedDialer) Do(_ context.Context, _ string, _ identity.NodeID, req transport.Request, _ transport.CallOptions) (*transport.Response, error) {
	n := int(c.calls.Add(1))
	code, body := c.reply(req, n)
	return &transport.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func newCannedNode(t *testing.T, lim Limits, d *cannedDialer) (*tnode, Peer) {
	t.Helper()
	n := newTNode(t, nodeOpts{limits: lim})
	var err error
	n.x, err = New(Config{Self: n.id, Store: n.recs, Cursors: n.cur, Dialer: d, Peers: n.peers, Limits: lim, Now: n.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	return n, Peer{NodeID: mustKey(t).NodeID(), Address: "203.0.113.9:7777", Trusted: true}
}

func sinceOf(req transport.Request) int64 {
	var v int64
	for _, c := range req.Query.Get("since") {
		v = v*10 + int64(c-'0')
	}
	return v
}

func TestPullStopsWhenMoreMakesNoProgress(t *testing.T) {
	d := &cannedDialer{reply: func(req transport.Request, _ int) (int, []byte) {
		b, _ := EncodePage(Page{Epoch: strings.Repeat("a", 16), HighestSeq: sinceOf(req), More: true})
		return 200, b
	}}
	n, peer := newCannedNode(t, Limits{}, d)
	nw := netOf(1)
	n.join(nw, 1)
	n.peers.set(nw, peer)
	n.x.SyncRound(ctx)
	if got := d.calls.Load(); got != 1 {
		t.Fatalf("%d requests: a page that says more without progress must end the round", got)
	}
}

func TestPullPageBudgetBoundsARoundAndReportsBacklog(t *testing.T) {
	d := &cannedDialer{reply: func(req transport.Request, _ int) (int, []byte) {
		b, _ := EncodePage(Page{Epoch: strings.Repeat("a", 16), HighestSeq: sinceOf(req) + 1, More: true})
		return 200, b
	}}
	n, peer := newCannedNode(t, Limits{MaxPagesPerPeerRound: 3}, d)
	nw := netOf(1)
	n.join(nw, 1)
	n.peers.set(nw, peer)
	res := n.x.SyncRound(ctx)
	if got := d.calls.Load(); got != 3 || !res.Backlog {
		t.Fatalf("%d requests, result %+v", got, res)
	}
	if cur, _ := n.cur.Get(ctx, nw.String(), peer.NodeID.String()); cur.PulledSeq != 3 {
		t.Fatalf("cursor = %d", cur.PulledSeq)
	}
}

func TestPullIgnoresMalformedAndErrorReplies(t *testing.T) {
	for name, reply := range map[string]func(transport.Request, int) (int, []byte){
		"garbage": func(transport.Request, int) (int, []byte) { return 200, []byte("garbage") },
		"500":     func(transport.Request, int) (int, []byte) { return 500, []byte("x") },
		"403":     func(transport.Request, int) (int, []byte) { return 403, []byte("forbidden") },
		"429":     func(transport.Request, int) (int, []byte) { return 429, nil },
		"backwards": func(transport.Request, int) (int, []byte) {
			b, _ := EncodePage(Page{Epoch: strings.Repeat("a", 16), HighestSeq: 0})
			return 200, b
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := &cannedDialer{reply: reply}
			n, peer := newCannedNode(t, Limits{}, d)
			nw := netOf(1)
			n.join(nw, 1)
			n.peers.set(nw, peer)
			// Start from a non-zero cursor so a lower highest seq is detectable.
			if err := n.cur.ResetPullCursorEpoch(ctx, nw.String(), peer.NodeID.String(), 5, strings.Repeat("a", 16)); err != nil {
				t.Fatal(err)
			}
			n.x.SyncRound(ctx)
			if got := d.calls.Load(); got != 1 {
				t.Fatalf("%d requests", got)
			}
			if cur, _ := n.cur.Get(ctx, nw.String(), peer.NodeID.String()); cur.PulledSeq != 5 {
				t.Fatalf("cursor moved to %d", cur.PulledSeq)
			}
			if n.count(t, nw) != 0 {
				t.Fatal("stored something")
			}
		})
	}
}
