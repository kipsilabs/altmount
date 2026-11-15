package bundleimport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func TestVerifyLimiter_BytesRefillOverAnHour(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	v := NewVerifyLimiter(VerifyLimits{BytesPerHour: 1000, MaxConcurrent: 4}, clk.now)
	l, err := v.Begin(VerifyExplicit, 1000)
	require.NoError(t, err)
	l.End(1000)
	_, err = v.Begin(VerifyExplicit, 100)
	assert.ErrorIs(t, err, ErrVerifyBudget, "the bucket is drained")
	assert.EqualValues(t, 1000, v.Usage().UsedBytes)
	clk.t = clk.t.Add(30 * time.Minute)
	l, err = v.Begin(VerifyExplicit, 400)
	require.NoError(t, err, "half an hour gave back half the bucket")
	l.End(0)
	assert.EqualValues(t, 500, v.Usage().UsedBytes, "an unused reservation is returned")
}

func TestVerifyLimiter_ActualBytesOverTheReservationAreCharged(t *testing.T) {
	v := NewVerifyLimiter(VerifyLimits{BytesPerHour: 1000, MaxConcurrent: 4}, func() time.Time { return time.Unix(1, 0) })
	l, err := v.Begin(VerifyExplicit, 100)
	require.NoError(t, err)
	l.End(900)
	assert.EqualValues(t, 900, v.Usage().UsedBytes)
	l.End(900) // idempotent
	assert.EqualValues(t, 900, v.Usage().UsedBytes)
}

func TestVerifyLimiter_AutomaticAttemptsLeaveARoomForExplicitOnes(t *testing.T) {
	v := NewVerifyLimiter(VerifyLimits{BytesPerHour: 1000, MaxConcurrent: 2}, func() time.Time { return time.Unix(1, 0) })
	a, err := v.Begin(VerifyAutomatic, 700)
	require.NoError(t, err)
	_, err = v.Begin(VerifyAutomatic, 10)
	assert.ErrorIs(t, err, ErrVerifyBudget, "the last slot is not for automatic attempts")
	a.End(700)
	_, err = v.Begin(VerifyAutomatic, 100)
	assert.ErrorIs(t, err, ErrVerifyBudget, "the last quarter of the bytes is not for automatic attempts")
	e, err := v.Begin(VerifyExplicit, 250)
	require.NoError(t, err, "an explicit request still gets it")
	e.End(250)
	assert.EqualValues(t, 2, v.Usage().Declined)
}

func TestVerifyLimiter_ConcurrencyIsLimited(t *testing.T) {
	v := NewVerifyLimiter(VerifyLimits{BytesPerHour: 1 << 40, MaxConcurrent: 2}, nil)
	a, err := v.Begin(VerifyExplicit, 1)
	require.NoError(t, err)
	b, err := v.Begin(VerifyExplicit, 1)
	require.NoError(t, err)
	_, err = v.Begin(VerifyExplicit, 1)
	assert.ErrorIs(t, err, ErrVerifyBudget)
	a.End(0)
	c, err := v.Begin(VerifyExplicit, 1)
	require.NoError(t, err)
	b.End(0)
	c.End(0)
	assert.Equal(t, 0, v.Usage().Active)
}

func TestAccept_DeclinedWhenTheNodeWideVerificationBudgetIsSpent(t *testing.T) {
	e := newEnv(t)
	e.imp.cfg.Verify = NewVerifyLimiter(VerifyLimits{BytesPerHour: 10_000, MaxConcurrent: 2}, func() time.Time { return time.Unix(1, 0) })
	e.imp.cfg.Verify.Debit(10_000)
	b := e.mkBundle("vb", mkvSpec("movie.mkv"))
	ch := &countingHeaders{SegmentHeaders: e.imp.cfg.Headers}
	e.imp.cfg.Headers = ch
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonVerifyBudget, ReasonOf(err))
	assert.Zero(t, ch.n.Load(), "nothing is read from the providers")
	assert.Empty(t, e.rep.got, "the publisher is not blamed")
	assert.Equal(t, []string{e.req(b).Origin.PublisherID}, e.rep.refunds, "the publisher's fetch is given back")
	e.assertNothingBehind()
}

func TestAccept_AutomaticAttemptCannotTakeTheExplicitShare(t *testing.T) {
	e := newEnv(t)
	e.imp.cfg.Verify = NewVerifyLimiter(VerifyLimits{BytesPerHour: 200 << 20, MaxConcurrent: 2}, func() time.Time { return time.Unix(1, 0) })
	e.imp.cfg.Verify.Debit(120 << 20) // 80 MiB left: under the quarter kept for explicit requests once a 64 MiB reservation is made
	b := e.mkBundle("va", mkvSpec("movie.mkv"))
	req := e.req(b)
	req.Automatic = true
	_, err := e.imp.Accept(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, ReasonVerifyBudget, ReasonOf(err))
	req.Automatic = false
	_, err = e.imp.Accept(context.Background(), req)
	require.NoError(t, err, "the same bundle is verified for an explicit request")
	assert.Equal(t, 0, e.imp.VerifyUsage().Active)
}
