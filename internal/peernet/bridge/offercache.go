package bridge

import (
	"context"
	"sync"
)

// OfferSource answers whether any stored record announces a bundle.
type OfferSource interface {
	AnyBundleOffer(ctx context.Context) (bool, error)
}

// NewOfferCache returns a cheap "does this node hold any bundle offer" check for
// FastPathConfig.AnyOffers. The answer is cached and recomputed only when
// generation (a counter of bundle-announcing records stored) changed, so with
// no offer around every import pays one atomic load. A lookup error is never
// cached and answers true: an unsure answer must not hide an offer.
func NewOfferCache(src OfferSource, generation func() uint64) func(ctx context.Context) bool {
	var (
		mu     sync.Mutex
		valid  bool
		cached bool
		seen   uint64
	)
	return func(ctx context.Context) bool {
		mu.Lock()
		defer mu.Unlock()
		gen := generation()
		if valid && gen == seen {
			return cached
		}
		any, err := src.AnyBundleOffer(ctx)
		if err != nil {
			return true
		}
		valid, cached, seen = true, any, gen
		return any
	}
}
