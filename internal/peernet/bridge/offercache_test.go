package bridge

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeOfferSource struct {
	any   atomic.Bool
	err   error
	calls atomic.Int32
}

func (f *fakeOfferSource) AnyBundleOffer(context.Context) (bool, error) {
	f.calls.Add(1)
	return f.any.Load(), f.err
}

func TestOfferCache_AsksOnceUntilARecordArrives(t *testing.T) {
	var gen atomic.Uint64
	src := &fakeOfferSource{}
	check := NewOfferCache(src, gen.Load)
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		assert.False(t, check(ctx))
	}
	assert.Equal(t, int32(1), src.calls.Load(), "no offer: one lookup, then cached")

	src.any.Store(true)
	assert.False(t, check(ctx), "no record arrived: still the cached answer")
	gen.Add(1) // a bundle-announcing record arrived
	assert.True(t, check(ctx), "the arrival invalidates the cache")
	assert.True(t, check(ctx))
	assert.Equal(t, int32(2), src.calls.Load())
}

func TestOfferCache_ErrorIsNeverCachedAndSaysYes(t *testing.T) {
	src := &fakeOfferSource{err: errors.New("db busy")}
	check := NewOfferCache(src, func() uint64 { return 0 })
	assert.True(t, check(context.Background()))
	src.err = nil
	assert.False(t, check(context.Background()), "the next call looks again")
}
