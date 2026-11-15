package bundleimport

import (
	"bytes"
	"context"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// No article is downloaded twice within one accept: a header read after a
// header read, and a staged read after either, come from the cache.
func TestArticleCache_AnArticleIsDownloadedOncePerAccept(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("ac", mkvSpec("movie.mkv"))
	id := b.Pool[0]
	ac := newArticleCache(articleCacheBytes)
	ctx := withArticleCache(context.Background(), ac)

	h1, err := e.imp.cfg.Headers.SegmentHeader(ctx, id)
	require.NoError(t, err)
	h2, err := e.imp.cfg.Headers.SegmentHeader(ctx, id)
	require.NoError(t, err)
	assert.EqualValues(t, 1, e.fp.PerMessageBodyCalls(id), "the second header read must not hit the wire")
	assert.Positive(t, h1.Bytes)
	assert.Zero(t, h2.Bytes, "a cached read downloaded nothing")
	assert.Equal(t, h1.PartSize, h2.PartSize)
	assert.Equal(t, h1.Prefix, h2.Prefix)

	// A staged read goes through the counting pool: served from the cache,
	// counted as nothing, and the bytes are the article's.
	cm := newCountingManager(e.imp.cfg.Headers.(poolHeaders).p)
	cm.setCache(ac)
	cli, err := cm.GetPool()
	require.NoError(t, err)
	var got bytes.Buffer
	_, err = cli.Fetch(ctx, nntppool.Req{MessageID: id, Writer: &got})
	require.NoError(t, err)
	assert.EqualValues(t, 1, e.fp.PerMessageBodyCalls(id))
	assert.Zero(t, cm.Downloaded())
	assert.Equal(t, int(h1.PartSize), got.Len())

	// A staged fetch of a new article is downloaded, counted, and then cached
	// for a header read.
	id2 := b.Pool[1]
	_, err = cli.Fetch(ctx, nntppool.Req{MessageID: id2, Writer: &bytes.Buffer{}})
	require.NoError(t, err)
	assert.Positive(t, cm.Downloaded())
	_, err = e.imp.cfg.Headers.SegmentHeader(ctx, id2)
	require.NoError(t, err)
	assert.EqualValues(t, 1, e.fp.PerMessageBodyCalls(id2))
}

func TestArticleCache_IsByteBounded(t *testing.T) {
	c := newArticleCache(100)
	for i := 0; i < 10; i++ {
		c.put(string(rune('a'+i)), nntppool.ArticleBody{}, make([]byte, 30))
	}
	assert.LessOrEqual(t, c.len(), 3)
	_, ok := c.get("a")
	assert.False(t, ok, "the oldest was evicted")
	_, ok = c.get("j")
	assert.True(t, ok)
	c.put("huge", nntppool.ArticleBody{}, make([]byte, maxCachedArticle+1))
	_, ok = c.get("huge")
	assert.False(t, ok, "an article over the per-article cap is not kept")
}
