package bundleimport

import (
	"container/list"
	"context"
	"sync"

	"github.com/javi11/nntppool/v5"
)

// One accept reads some articles more than once: the first and last article of
// a volume for the yEnc sample, again for the archive header walk, again for
// the part-order windows and the media probe. The article cache of one accept
// keeps the most recent articles it fetched, so no article is downloaded twice
// while it is in the cache, and what the cache served costs nothing (the
// budget meter counts bytes that really came off the wire).
const (
	// articleCacheBytes bounds the memory of one accept's cache.
	articleCacheBytes int64 = 32 << 20
	// maxCachedArticle is the largest article the cache keeps (a larger one is
	// read through, and costs a second download when read again).
	maxCachedArticle = 2 << 20
)

type cachedArticle struct {
	id   string
	head nntppool.ArticleBody // everything but Bytes
	body []byte
}

// articleCache is a byte-bounded LRU of decoded articles. Safe for concurrent
// use.
type articleCache struct {
	mu   sync.Mutex
	max  int64
	size int64
	ll   *list.List // front = most recent
	m    map[string]*list.Element
}

func newArticleCache(max int64) *articleCache {
	return &articleCache{max: max, ll: list.New(), m: map[string]*list.Element{}}
}

func (c *articleCache) get(id string) (cachedArticle, bool) {
	if c == nil {
		return cachedArticle{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if !ok {
		return cachedArticle{}, false
	}
	c.ll.MoveToFront(e)
	return *e.Value.(*cachedArticle), true
}

func (c *articleCache) put(id string, head nntppool.ArticleBody, body []byte) {
	if c == nil || len(body) > maxCachedArticle || int64(len(body)) > c.max {
		return
	}
	head.Bytes = nil
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[id]; ok {
		c.ll.MoveToFront(e)
		return
	}
	c.m[id] = c.ll.PushFront(&cachedArticle{id: id, head: head, body: body})
	c.size += int64(len(body))
	for c.size > c.max {
		last := c.ll.Back()
		if last == nil {
			break
		}
		a := last.Value.(*cachedArticle)
		c.ll.Remove(last)
		delete(c.m, a.id)
		c.size -= int64(len(a.body))
	}
}

// len reports how many articles are cached (tests).
func (c *articleCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

type articleCacheKey struct{}

func withArticleCache(ctx context.Context, c *articleCache) context.Context {
	return context.WithValue(ctx, articleCacheKey{}, c)
}

func articleCacheFrom(ctx context.Context) *articleCache {
	c, _ := ctx.Value(articleCacheKey{}).(*articleCache)
	return c
}

// captureWriter keeps what is written up to maxCachedArticle bytes (more than
// that and the article is not cached).
type captureWriter struct {
	buf  []byte
	over bool
}

func (w *captureWriter) Write(p []byte) (int, error) {
	if !w.over {
		if len(w.buf)+len(p) > maxCachedArticle {
			w.over, w.buf = true, nil
		} else {
			w.buf = append(w.buf, p...)
		}
	}
	return len(p), nil
}

// articleCacheAware is implemented by a staging area that can read through an
// accept's article cache.
type articleCacheAware interface{ SetArticleCache(*articleCache) }
