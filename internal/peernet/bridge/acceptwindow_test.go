package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
)

func TestAcceptWindow_GrowsWithThePartsUpToACeiling(t *testing.T) {
	assert.Equal(t, DefaultFastPathAcceptTimeout, AcceptWindow(0))
	assert.Equal(t, DefaultFastPathAcceptTimeout+FastPathPerPartAllowance, AcceptWindow(1))
	assert.Equal(t, DefaultFastPathAcceptTimeout+100*FastPathPerPartAllowance, AcceptWindow(100))
	assert.Equal(t, MaxFastPathAcceptTimeout, AcceptWindow(100_000))
	assert.Greater(t, AcceptWindow(600), AcceptWindow(50), "a 600-volume set gets more time than a 50-volume one")
}

type deadlineAcceptor struct {
	fakeAcceptor
	left time.Duration
	auto bool
}

func (a *deadlineAcceptor) Accept(ctx context.Context, r bundleimport.Request) (*bundleimport.Result, error) {
	d, _ := ctx.Deadline()
	a.left, a.auto = time.Until(d), r.Automatic
	return a.fakeAcceptor.Accept(ctx, r)
}

func hookWith(t *testing.T, a BundleAcceptor, f *blockingFetcher) importer.PeerBundleFastPath {
	t.Helper()
	h := NewPeerBundleFastPath(FastPathConfig{Acceptor: a, Fetcher: f, Enabled: func() bool { return true }, FetchTimeout: time.Second})
	require.NotNil(t, h)
	return h
}

func TestFastPath_AcceptWindowScalesWithThePartsOfTheOfferedBundle(t *testing.T) {
	b := matching(t)
	b.Files = []bundle.File{{Name: "movie.mkv", Parts: make([]int, 300)}}
	a := &deadlineAcceptor{fakeAcceptor: fakeAcceptor{err: assert.AnError}}
	_, ok := hookWith(t, a, &blockingFetcher{b: b}).TryBundle(context.Background(), req())
	require.False(t, ok)
	assert.Greater(t, a.left, DefaultFastPathAcceptTimeout+60*time.Second, "300 parts earn more than the base window")
	assert.LessOrEqual(t, a.left, MaxFastPathAcceptTimeout)
	assert.True(t, a.auto, "the importer's own attempt is the automatic class of the verification budget")

	b.Files = []bundle.File{{Name: "movie.mkv"}}
	_, _ = hookWith(t, a, &blockingFetcher{b: b}).TryBundle(context.Background(), req())
	assert.LessOrEqual(t, a.left, DefaultFastPathAcceptTimeout, "a plain file keeps the base window")
}
