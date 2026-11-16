package health

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A deferred check ran nothing, so the worker's check counters must not move.
func TestCycle_DeferredCheckIsNotCounted(t *testing.T) {
	env, _ := cycleEnv(t)
	env.hw.SetPeerAdviser(&fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(5 * 24 * time.Hour)}})
	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))
	stats := env.hw.GetStats()
	assert.Zero(t, stats.TotalFilesChecked)
	assert.Zero(t, stats.TotalFilesHealthy)
}
