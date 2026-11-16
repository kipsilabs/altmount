package bundleimport

import (
	"context"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

func TestCountingManager_CountsWhatTheStagedReadsReallyDownload(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("a", fakepool.SegmentBehavior{Bytes: make([]byte, 700)})
	fp.SetBehavior("b", fakepool.SegmentBehavior{Bytes: make([]byte, 300)})
	m := newCountingManager(&fakePoolManager{client: fp})
	cli, err := m.GetPool()
	require.NoError(t, err)
	w := &prefixWriter{}
	_, err = cli.Fetch(context.Background(), nntppool.Req{MessageID: "a", Writer: w})
	require.NoError(t, err)
	_, err = cli.Fetch(context.Background(), nntppool.Req{MessageID: "b"})
	require.NoError(t, err)
	res := <-cli.FetchAsync(context.Background(), nntppool.Req{MessageID: "b"})
	require.NoError(t, res.Err)
	assert.EqualValues(t, 700+300+300, m.Downloaded())
	assert.EqualValues(t, 700, w.n, "the caller's own writer still gets everything")
}

func TestCostBudget_StagedReadsBeyondTheirDeclaredSizeAreCharged(t *testing.T) {
	b := newCostBudget(1000)
	require.NoError(t, b.chargeStaged(300))
	require.NoError(t, b.noteStaged(300), "as declared")
	assert.EqualValues(t, 300, b.spent())
	require.NoError(t, b.noteStaged(900), "600 bytes over what was declared")
	assert.EqualValues(t, 900, b.spent())
	assert.ErrorIs(t, b.noteStaged(1500), errOverrun)
}
