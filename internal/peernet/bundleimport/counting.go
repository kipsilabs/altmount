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
	// cache, when set, serves articles this accept already fetched and keeps
	// the ones it fetches; what it serves is not counted.
	cache atomic.Pointer[articleCache]
}

func (m *countingManager) setCache(c *articleCache) { m.cache.Store(c) }

func newCountingManager(m pool.Manager) *countingManager { return &countingManager{Manager: m} }

// Downloaded is the number of decoded bytes fetched so far.
func (m *countingManager) Downloaded() int64 { return m.n.Load() }

func (m *countingManager) GetPool() (pool.NntpClient, error) {
	c, err := m.Manager.GetPool()
	if err != nil {
		return nil, err
	}
	return &countingClient{NntpClient: c, n: &m.n, m: m}, nil
}

type countingClient struct {
	pool.NntpClient
	n *atomic.Int64
	m *countingManager
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
	ac := c.m.cache.Load()
	if a, ok := ac.get(r.MessageID); ok {
		return serveCached(a, r)
	}
	streamedTo := r.Writer
	var cw *captureWriter
	if ac != nil {
		cw = &captureWriter{}
		if streamedTo != nil {
			r.Writer = io.MultiWriter(streamedTo, cw)
		}
	}
	r, streamed := c.wrap(r)
	body, err := c.NntpClient.Fetch(ctx, r)
	if !streamed && body != nil {
		c.n.Add(int64(len(body.Bytes)))
	}
	if err == nil && body != nil && ac != nil {
		switch {
		case !streamed:
			ac.put(r.MessageID, *body, append([]byte(nil), body.Bytes...))
		case !cw.over && len(cw.buf) > 0:
			ac.put(r.MessageID, *body, cw.buf)
		}
	}
	return body, err
}

// serveCached answers a request from a cached article.
func serveCached(a cachedArticle, r nntppool.Req) (*nntppool.ArticleBody, error) {
	if r.OnMeta != nil {
		r.OnMeta(a.head.YEnc)
	}
	out := a.head
	if r.Writer != nil {
		if _, err := r.Writer.Write(a.body); err != nil {
			return nil, err
		}
		return &out, nil
	}
	out.Bytes = append([]byte(nil), a.body...)
	return &out, nil
}

func (c *countingClient) FetchAsync(ctx context.Context, r nntppool.Req) <-chan nntppool.BodyResult {
	out := make(chan nntppool.BodyResult, 1)
	go func() {
		b, err := c.Fetch(ctx, r)
		out <- nntppool.BodyResult{Body: b, Err: err}
	}()
	return out
}
