package bundleimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

func segsOfSizes(sizes ...int64) []bundle.Segment {
	out := make([]bundle.Segment, len(sizes))
	for i, s := range sizes {
		out[i] = bundle.Segment{Ref: int64(i), Size: s, End: s - 1}
	}
	return out
}

func TestSizeShape_UniformArticlesAndASmallerLastOneAreHonest(t *testing.T) {
	for _, sizes := range [][]int64{{100}, {100, 100, 100}, {100, 100, 37}, {100, 100, 100, 100}} {
		assert.NoError(t, runShape(segsOfSizes(sizes...)), "%v", sizes)
	}
	for _, sizes := range [][]int64{{100, 120}, {100, 100, 101}, {1, 750_000, 750_000}, {100, 90, 80, 100}} {
		assert.Error(t, runShape(segsOfSizes(sizes...)), "%v", sizes)
	}
}

// A post with a stray smaller article (a resumed upload) is still honest: up to
// one percent of the articles, and never a larger one.
func TestSizeShape_AFewStraySmallerArticlesAreTolerated(t *testing.T) {
	sizes := make([]int64, 300)
	for i := range sizes {
		sizes[i] = 750_000
	}
	sizes[120], sizes[200] = 700_000, 749_000 // 2 of 299: under one percent
	assert.NoError(t, runShape(segsOfSizes(sizes...)))
	assert.Error(t, runShape(segsOfSizes(100, 90, 100, 100, 37)), "a short table has no tolerance")
	sizes[10], sizes[11] = 1, 2 // 4 of 299: over one percent
	assert.Error(t, runShape(segsOfSizes(sizes...)))
	sizes = make([]int64, 300)
	for i := range sizes {
		sizes[i] = 750_000
	}
	sizes[50] = 750_001
	assert.Error(t, runShape(segsOfSizes(sizes...)), "a larger article is never tolerated")
}

func runShape(segs []bundle.Segment) error {
	return checkRunShape("t", segs)
}

// A table whose articles are not all the poster's one size (but a smaller last
// one) is declined before any article is read.
func TestAccept_ArticleSizesThatAreNotOnePostersShapeAreDeclinedUnread(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("shape", mkvSpec("movie.mkv"))
	sizes := []int64{128, 192, 128, 64} // 512 bytes in total, as the file says
	var at int64
	for i := range b.Files[0].Segments {
		b.Files[0].Segments[i].Size, b.Files[0].Segments[i].Start, b.Files[0].Segments[i].End = sizes[i], 0, sizes[i]-1
		at += sizes[i]
	}
	require.EqualValues(t, 512, at)
	ch := &countingHeaders{SegmentHeaders: e.imp.cfg.Headers}
	e.imp.cfg.Headers = ch
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonSizeImplausible, ReasonOf(err), "%v", err)
	assert.Zero(t, ch.n.Load())
	e.assertNothingBehind()
}
