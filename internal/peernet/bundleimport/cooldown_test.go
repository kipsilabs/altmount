package bundleimport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeclineBook_BacksOffAndDoublesAndForgets(t *testing.T) {
	var d declineBook
	t0 := time.Unix(1_700_000_000, 0)
	_, blocked := d.blocked("b1", t0)
	assert.False(t, blocked)

	d.note("b1", ReasonSizeImplausible, t0)
	r, blocked := d.blocked("b1", t0.Add(9*time.Minute))
	require.True(t, blocked)
	assert.Equal(t, ReasonSizeImplausible, r)
	_, blocked = d.blocked("b1", t0.Add(11*time.Minute))
	assert.False(t, blocked, "the first decline skips the bundle for ten minutes")

	d.note("b1", ReasonSizeImplausible, t0.Add(11*time.Minute))
	_, blocked = d.blocked("b1", t0.Add(11*time.Minute+19*time.Minute))
	assert.True(t, blocked, "the second skip doubles to twenty minutes")

	at := t0.Add(time.Hour)
	for k := 0; k < 12; k++ {
		d.note("b1", ReasonSizeImplausible, at)
	}
	_, blocked = d.blocked("b1", at.Add(declineMax-time.Second))
	assert.True(t, blocked)
	_, blocked = d.blocked("b1", at.Add(declineMax+time.Second))
	assert.False(t, blocked, "the skip is capped")

	// Forgotten after a day without a decline: the next decline starts over.
	later := at.Add(declineForget + time.Hour)
	d.note("b1", ReasonSizeImplausible, later)
	_, blocked = d.blocked("b1", later.Add(declineBase+time.Second))
	assert.False(t, blocked, "a bundle declined again after a day starts at the base wait")
}

func TestDeclineBook_IsBounded(t *testing.T) {
	var d declineBook
	now := time.Unix(1_700_000_000, 0)
	for k := 0; k < maxDeclined*3; k++ {
		d.note(string(rune('a'+k%26))+time.Duration(k).String(), ReasonStructureInvalid, now.Add(time.Duration(k)*time.Second))
	}
	assert.LessOrEqual(t, d.size(), maxDeclined)
}

// A deterministic local decline is not read again for a while: the second
// accept of the same Bundle ID costs no article, and the publisher's standing
// is untouched.
func TestAccept_ALocalDeclineIsNotVerifiedAgainForAWhile(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("cd", mkvSpec("movie.mkv"))
	// Declared sizes that cannot be the NZB's: a deterministic local decline.
	req := e.req(b)
	req.Origin.BundleID = "bundle-cd"
	req.ExpectedNZB = [][]string{b.Pool}
	enc := make([]int64, len(b.Pool))
	for k := range enc {
		enc[k] = 1_000_000
	}
	req.ExpectedNZBSizes = [][]int64{enc}
	_, err := e.imp.Accept(context.Background(), req)
	require.Error(t, err)
	require.Equal(t, ReasonSizeImplausible, ReasonOf(err))
	assert.True(t, e.imp.declines.has("bundle-cd"))

	// Even with the user's NZB now agreeing, the same Bundle ID is skipped
	// without any work for the wait.
	req.ExpectedNZBSizes = nil
	req.ExpectedNZB = nil
	_, err = e.imp.Accept(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, ReasonSizeImplausible, ReasonOf(err))
	assert.Contains(t, err.Error(), "declined")
	assert.Empty(t, e.rep.got, "the publisher is never blamed for a local decline")
}
