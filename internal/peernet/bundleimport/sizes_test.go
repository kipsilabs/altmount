package bundleimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// nzbFor returns the NZB view of b: the ids and, per segment, an encoded size
// derived from its decoded size with encode.
func nzbFor(t *testing.T, e *env, b *bundle.Bundle, encode func(int64) int64) ([][]string, [][]int64) {
	t.Helper()
	loc, err := bundle.ToLocal(b, e.imp.cfg.Now())
	require.NoError(t, err)
	dec := map[string]int64{}
	for _, f := range b.Files {
		for _, s := range f.Segments {
			dec[b.Pool[s.Ref]] = s.Size
		}
	}
	sizes := make([][]int64, len(loc.NZBFiles))
	for i, ids := range loc.NZBFiles {
		for _, id := range ids {
			sizes[i] = append(sizes[i], encode(dec[id]))
		}
	}
	return loc.NZBFiles, sizes
}

func TestAccept_SizesMustBePlausibleAgainstTheUsersNZB(t *testing.T) {
	cases := map[string]struct {
		encode func(int64) int64
		ok     bool
	}{
		"yenc sized":            {func(n int64) int64 { return n + n/32 + 20 }, true},
		"declared far too big":  {func(n int64) int64 { return n / 4 }, false},
		"declared far too tiny": {func(n int64) int64 { return n * 1000 }, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			b := e.mkBundle("sz", mkvSpec("movie.mkv"))
			req := e.req(b)
			req.ExpectedNZB, req.ExpectedNZBSizes = nzbFor(t, e, b, tc.encode)
			_, err := e.imp.Accept(context.Background(), req)
			if tc.ok {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, ReasonSizeImplausible, ReasonOf(err))
			e.assertNothingBehind()
		})
	}
}
