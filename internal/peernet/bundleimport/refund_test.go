package bundleimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A bundle the local file policy refuses before any article is read gives the
// publisher's fetch budget back; one that failed on article evidence does not.
func TestAcceptRefundsTheFetchBudgetOnlyForLocalPolicyRejections(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("refund", fileSpec{name: "data.bin", plain: junkPlain(512), segSize: 128})
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonNoMediaFile, ReasonOf(err))
	assert.Len(t, e.rep.refunds, 1, "no article was read: the attempt is refunded")

	e2 := newEnv(t)
	b2 := e2.mkBundle("fail", fileSpec{name: "movie.mkv", plain: junkPlain(512), segSize: 128})
	_, err = e2.imp.Accept(context.Background(), e2.req(b2))
	require.Error(t, err)
	assert.NotEqual(t, ReasonNoMediaFile, ReasonOf(err))
	assert.Empty(t, e2.rep.refunds, "an attempt that read articles keeps its charge")
}

// The import counters say how many parts and extents of accepted bundles were
// bound to an archive header and how many could not be.
func TestCountersExposeHeaderBinding(t *testing.T) {
	e := newEnv(t)
	payload := mkvPlain(1500)
	s := e.mkStoredSet(setSpec{tag: "hb", kind: "rar5", payload: payload, cuts: evenCuts(len(payload), 3), scheme: 1, extraVol: -1})
	res, err := e.imp.Accept(context.Background(), e.req(s.b))
	require.NoError(t, err)
	require.Equal(t, 3, res.Cost.PartsBound)
	c := e.imp.Counters()
	assert.Equal(t, uint64(3), c.PartsHeaderBound)
	assert.Equal(t, uint64(res.Cost.PartsUnbound), c.PartsHeaderUnbound)
	assert.Equal(t, uint64(res.Cost.ExtentsBound), c.ExtentsHeaderBound)
	assert.Equal(t, uint64(res.Cost.ExtentsUnbound), c.ExtentsHeaderUnbound)
}
