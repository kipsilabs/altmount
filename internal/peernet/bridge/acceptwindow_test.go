package bridge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/importer"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
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

func nzbOfEntries(n int) *metapb.NzbStore {
	st := &metapb.NzbStore{}
	for k := 0; k < n; k++ {
		st.Files = append(st.Files, &metapb.NzbFileEntry{Segments: []*metapb.NzbSeg{{Id: fmt.Sprintf("s%d@x", k)}}})
	}
	return st
}

// The window follows the user's own NZB (its entries), never the part count the
// peer declares: a peer cannot stretch how long the user's import is held.
func TestFastPath_AcceptWindowFollowsTheUsersNZBNotThePeersBundle(t *testing.T) {
	st := nzbOfEntries(300)
	rid, err := contentid.ReleaseID(st)
	require.NoError(t, err)
	b := &bundle.Bundle{Scheme: bundle.SchemeNZB, ReleaseID: rid, Files: []bundle.File{{Name: "movie.mkv", Parts: make([]int, 300)}}}
	a := &deadlineAcceptor{fakeAcceptor: fakeAcceptor{err: assert.AnError}}
	r := req()
	r.NZB = st
	_, ok := hookWith(t, a, &blockingFetcher{b: b}).TryBundle(context.Background(), r)
	require.False(t, ok)
	assert.LessOrEqual(t, a.left, DefaultFastPathExtraDelayCap, "a big NZB is held at most the cap unless verification progresses")
	assert.True(t, a.auto, "the importer's own attempt is the automatic class of the verification budget")

	// The same bundle declaring 300 parts against a one-file NZB of two entries
	// earns the base window only.
	b2 := matching(t)
	b2.Files = []bundle.File{{Name: "movie.mkv", Parts: make([]int, 300)}}
	_, _ = hookWith(t, a, &blockingFetcher{b: b2}).TryBundle(context.Background(), req())
	assert.LessOrEqual(t, a.left, DefaultFastPathAcceptTimeout+time.Second, "the peer's declared parts earn nothing")
}

type progressAcceptor struct {
	fakeAcceptor
	every, total time.Duration
}

func (a *progressAcceptor) Accept(ctx context.Context, r bundleimport.Request) (*bundleimport.Result, error) {
	end := time.Now().Add(a.total)
	for time.Now().Before(end) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(a.every):
			if r.Progress != nil {
				r.Progress()
			}
		}
	}
	return a.res, nil
}

// A verification that is making progress may run past the cap; one that is not
// is cut at it.
func TestFastPath_ProgressExtendsTheCapButNeverPastTheHardWindow(t *testing.T) {
	st := nzbOfEntries(50)
	rid, err := contentid.ReleaseID(st)
	require.NoError(t, err)
	b := &bundle.Bundle{Scheme: bundle.SchemeNZB, ReleaseID: rid}
	r := req()
	r.NZB = st
	cfg := func(a BundleAcceptor) importer.PeerBundleFastPath {
		return NewPeerBundleFastPath(FastPathConfig{Acceptor: a, Fetcher: &blockingFetcher{b: b}, Enabled: func() bool { return true },
			FetchTimeout: time.Second, ExtraDelayCap: 60 * time.Millisecond, ProgressAllowance: 60 * time.Millisecond, HardWindow: 400 * time.Millisecond})
	}
	res := &bundleimport.Result{Dir: "movies/R", Paths: []string{"movies/R/a.mkv"}}

	// Progressing for 200 ms: past the 60 ms cap, inside the hard window.
	_, ok := cfg(&progressAcceptor{fakeAcceptor: fakeAcceptor{res: res}, every: 15 * time.Millisecond, total: 200 * time.Millisecond}).TryBundle(context.Background(), r)
	assert.True(t, ok, "progress keeps the verification alive")

	// No progress at all: cut at the cap.
	start := time.Now()
	_, ok = cfg(&progressAcceptor{fakeAcceptor: fakeAcceptor{res: res}, every: time.Hour, total: time.Second}).TryBundle(context.Background(), r)
	assert.False(t, ok)
	assert.Less(t, time.Since(start), 300*time.Millisecond)

	// Progress forever: still cut at the hard window.
	start = time.Now()
	_, ok = cfg(&progressAcceptor{fakeAcceptor: fakeAcceptor{res: res}, every: 10 * time.Millisecond, total: 5 * time.Second}).TryBundle(context.Background(), r)
	assert.False(t, ok)
	assert.Less(t, time.Since(start), time.Second)
}
