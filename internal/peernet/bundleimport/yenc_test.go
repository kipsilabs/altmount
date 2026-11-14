package bundleimport

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccept_SampledYencHeadersMustAgreeWithTheLayout(t *testing.T) {
	lies := map[string]func(i int, m nntppool.YEncMeta) nntppool.YEncMeta{
		"part size":   func(_ int, m nntppool.YEncMeta) nntppool.YEncMeta { m.PartSize++; return m },
		"part offset": func(_ int, m nntppool.YEncMeta) nntppool.YEncMeta { m.PartBegin += 7; return m },
		"file total":  func(_ int, m nntppool.YEncMeta) nntppool.YEncMeta { m.FileSize *= 2; return m },
	}
	for name, lie := range lies {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			b := e.mkBundle("yl", mkvSpec("movie.mkv"))
			for i, id := range b.Pool {
				beh := e.fp.BehaviorFor(id)
				beh.YEnc = lie(i, beh.YEnc)
				e.fp.SetBehavior(id, beh)
			}
			_, err := e.imp.Accept(context.Background(), e.req(b))
			require.Error(t, err)
			assert.Equal(t, ReasonYencMismatch, ReasonOf(err))
			assert.Len(t, e.rep.got, 1, "the publisher is reported as having served a bad bundle")
			e.assertNothingBehind()
		})
	}
}

func TestAccept_NoHeaderReaderMeansNoAccept(t *testing.T) {
	e := newEnv(t)
	e.imp.cfg.Headers = nil
	_, err := e.imp.Accept(context.Background(), e.req(e.mkBundle("nh", mkvSpec("movie.mkv"))))
	require.Error(t, err)
	e.assertNothingBehind()
}

type countingHeaders struct {
	SegmentHeaders
	n atomic.Int32
}

func (c *countingHeaders) SegmentHeader(ctx context.Context, id string) (YencHeader, error) {
	c.n.Add(1)
	return c.SegmentHeaders.SegmentHeader(ctx, id)
}

func TestAccept_YencSamplesAreBounded(t *testing.T) {
	e := newEnv(t)
	files := []fileSpec{}
	for i := 0; i < 10; i++ {
		files = append(files, mkvSpec(fmt.Sprintf("m%d.mkv", i)))
	}
	b := e.mkBundle("cap", files...)
	ch := &countingHeaders{SegmentHeaders: e.imp.cfg.Headers}
	e.imp.cfg.Headers = ch
	e.imp.cfg.Policy.MaxYencSamples = 6
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	assert.EqualValues(t, 6, ch.n.Load())
}

func TestPlanSamples_ChoosesFirstLastAndUnpredictableMiddles(t *testing.T) {
	tab := sampleTable{ids: make([]string, 200)}
	seen := map[int]struct{}{}
	for range 40 {
		plan := planSamples([]sampleTable{tab}, 2, 24)
		require.Len(t, plan, 4)
		assert.Equal(t, 0, plan[0].pos)
		assert.Equal(t, 199, plan[1].pos)
		for _, s := range plan[2:] {
			assert.Greater(t, s.pos, 0)
			assert.Less(t, s.pos, 199)
			seen[s.pos] = struct{}{}
		}
	}
	assert.Greater(t, len(seen), 20, "middle positions must vary from call to call")

	many := make([]sampleTable, 50)
	for i := range many {
		many[i] = sampleTable{ids: make([]string, 10)}
	}
	assert.Len(t, planSamples(many, 2, 24), 24, "the total cap holds")
}
