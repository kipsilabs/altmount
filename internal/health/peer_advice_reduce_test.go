package health

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerFloorPercent_SameSampleIsNoReduction(t *testing.T) {
	// 30 segments: 34 % and 35 % both sample 10 of them.
	_, ok := peerFloorPercent(30, 35)
	assert.False(t, ok, "a lower percentage that samples the same segments is no reduction")
	_, ok = peerFloorPercent(30, 60)
	assert.True(t, ok)
}

func TestPeerAdvice_AtMostOneReducedCheckAndNeverAfterFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		info func(PeerFileInfo) PeerFileInfo
		want int
	}{
		{"first reduction", func(i PeerFileInfo) PeerFileInfo { return i }, bigFileSegments * PeerFloorPercent / 100},
		{"already reduced once", func(i PeerFileInfo) PeerFileInfo { i.AlreadyReduced = true; return i }, 2048},
		{"prior local failure", func(i PeerFileInfo) PeerFileInfo { i.PriorFailure = true; return i }, 2048},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := sampledEnv(t)
			env.checker.SetPeerAdviser(&fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}})
			opts := autoOpts(env, database.HealthStatusHealthy)
			opts.PeerFiles[env.filePath] = tc.info(opts.PeerFiles[env.filePath])
			var ev HealthEvent
			n := statCount(env, func() { ev = env.checker.CheckFile(context.Background(), env.filePath, opts) })
			require.Equal(t, EventTypeFileHealthy, ev.Type)
			assert.Equal(t, tc.want, n)
			assert.Equal(t, tc.want != 2048, ev.ReducedByPeerAdvice)
			require.NotNil(t, ev.Stats)
			assert.Equal(t, tc.want != 2048, ev.Stats.ReducedByPeerAdvice)
		})
	}
}

func TestPeerFilesFor_FlagsReductionAndPriorFailure(t *testing.T) {
	now := time.Now().UTC()
	reduced := database.WithPeerReduction(nil, now)
	failed := `{"error_type":"missing_segments"}`
	lastErr := "boom"
	files := peerFilesFor([]*database.FileHealth{
		{FilePath: "clean", Status: database.HealthStatusHealthy, CreatedAt: now},
		{FilePath: "reduced", Status: database.HealthStatusHealthy, CreatedAt: now, ErrorDetails: reduced},
		{FilePath: "retried", Status: database.HealthStatusHealthy, CreatedAt: now, RetryCount: 1},
		{FilePath: "repaired", Status: database.HealthStatusHealthy, CreatedAt: now, RepairRetryCount: 1},
		{FilePath: "lasterr", Status: database.HealthStatusHealthy, CreatedAt: now, LastError: &lastErr},
		{FilePath: "details", Status: database.HealthStatusHealthy, CreatedAt: now, ErrorDetails: &failed},
	})
	assert.False(t, files["clean"].AlreadyReduced || files["clean"].PriorFailure)
	assert.True(t, files["reduced"].AlreadyReduced)
	assert.False(t, files["reduced"].PriorFailure, "a reduction marker is not a failure")
	for _, p := range []string{"retried", "repaired", "lasterr", "details"} {
		assert.True(t, files[p].PriorFailure, p)
	}
}

func TestPrepareUpdateForResult_ReductionMarkerLifecycle(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	fh := &database.FileHealth{FilePath: "movies/a.mkv", Status: database.HealthStatusChecking, CreatedAt: time.Now().UTC()}

	reduced := healthyEvent(fh.FilePath)
	reduced.ReducedByPeerAdvice = true
	update, _ := env.hw.prepareUpdateForResult(context.Background(), fh, reduced)
	assert.True(t, database.HasPeerReduction(update.ErrorDetails), "a reduced healthy check is remembered")

	// An inconclusive check proves nothing and must not release a second reduction.
	fh.ErrorDetails = update.ErrorDetails
	inc, _ := env.hw.prepareUpdateForResult(context.Background(), fh,
		HealthEvent{Type: EventTypeCheckInconclusive, FilePath: fh.FilePath})
	assert.True(t, database.HasPeerReduction(inc.ErrorDetails))

	// A deferral keeps it too.
	def, _ := env.hw.prepareUpdateForResult(context.Background(), fh,
		HealthEvent{Type: EventTypeCheckDeferred, FilePath: fh.FilePath, DeferUntil: time.Now().Add(time.Hour)})
	assert.True(t, database.HasPeerReduction(def.ErrorDetails))

	// A normal healthy check clears it.
	normal, _ := env.hw.prepareUpdateForResult(context.Background(), fh, healthyEvent(fh.FilePath))
	assert.False(t, database.HasPeerReduction(normal.ErrorDetails))
}
