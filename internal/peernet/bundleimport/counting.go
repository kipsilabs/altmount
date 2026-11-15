package bundleimport

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/pool"
)

// countingManager wraps the pool a staging area reads through and counts the
// decoded bytes every fetch really delivers, whatever the layout declared. It
// is what lets an accept charge its budget with actual bytes for the reads that
// go through the serving stack.
type countingManager struct {
	pool.Manager
	n atomic.Int64
}

func newCountingManager(m pool.Manager) *countingManager { return &countingManager{Manager: m} }

// Downloaded is the number of decoded bytes fetched so far.
func (m *countingManager) Downloaded() int64 { return m.n.Load() }

func (m *countingManager) GetPool() (pool.NntpClient, error) {
	c, err := m.Manager.GetPool()
	if err != nil {
		return nil, err
	}
	return &countingClient{NntpClient: c, n: &m.n}, nil
}

type countingClient struct {
	pool.NntpClient
	n *atomic.Int64
}

type countWriter struct {
	w io.Writer
	n *atomic.Int64
}

func (c countWriter) Write(p []byte) (int, error) {
	k, err := c.w.Write(p)
	c.n.Add(int64(k))
	return k, err
}

// wrap counts through the request's writer; without one the pool buffers the
// body and its length is counted after the call.
func (c *countingClient) wrap(r nntppool.Req) (nntppool.Req, bool) {
	if r.Writer == nil {
		return r, false
	}
	r.Writer = countWriter{w: r.Writer, n: c.n}
	return r, true
}

func (c *countingClient) Fetch(ctx context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	r, streamed := c.wrap(r)
	body, err := c.NntpClient.Fetch(ctx, r)
	if !streamed && body != nil {
		c.n.Add(int64(len(body.Bytes)))
	}
	return body, err
}

func (c *countingClient) FetchAsync(ctx context.Context, r nntppool.Req) <-chan nntppool.BodyResult {
	r, streamed := c.wrap(r)
	in := c.NntpClient.FetchAsync(ctx, r)
	if streamed {
		return in
	}
	out := make(chan nntppool.BodyResult, 1)
	go func() {
		res := <-in
		if res.Body != nil {
			c.n.Add(int64(len(res.Body.Bytes)))
		}
		out <- res
	}()
	return out
}
