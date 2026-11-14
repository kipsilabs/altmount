package health

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeGate struct {
	mu      sync.Mutex
	pending map[string]struct{}
	queried int
	passed  []string
	failed  []string
}

func (g *fakeGate) PendingFullCheck(_ context.Context, paths []string) map[string]struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.queried++
	out := map[string]struct{}{}
	for _, p := range paths {
		if _, ok := g.pending[p]; ok {
			out[p] = struct{}{}
		}
	}
	return out
}
func (g *fakeGate) IsPendingPeerImport(_ context.Context, p string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.pending[p]
	return ok
}
func (g *fakeGate) FullCheckPassed(_ context.Context, p string) {
	g.mu.Lock()
	g.passed = append(g.passed, p)
	g.mu.Unlock()
}
func (g *fakeGate) FullCheckFailed(_ context.Context, p string) {
	g.mu.Lock()
	g.failed = append(g.failed, p)
	g.mu.Unlock()
}

func TestPeerBundle_FullCheckFilesAreSampledInFullAndNeverGetAdvice(t *testing.T) {
	env := sampledEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}}
	env.checker.SetPeerAdviser(adv)
	opts := autoOpts(env, database.HealthStatusHealthy)
	opts.FullCheckFiles = map[string]struct{}{env.filePath: {}}

	var ev HealthEvent
	n := statCount(env, func() { ev = env.checker.CheckFile(context.Background(), env.filePath, opts) })
	require.Equal(t, EventTypeFileHealthy, ev.Type)
	assert.Equal(t, bigFileSegments, n, "every segment is checked")
	assert.True(t, ev.Stats.FullCheck)
	assert.Zero(t, adv.consulted(), "peer advice never applies to a pending peer file")
	assert.False(t, ev.Stats.ReducedByPeerAdvice)
}

func TestPeerBundle_AdviceGuardHoldsEvenWhenTheSampleIsNotForced(t *testing.T) {
	env := sampledEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(5 * 24 * time.Hour)}}
	env.checker.SetPeerAdviser(adv)
	prep := &preparedCheck{filePath: env.filePath, realSegments: bigFileSegments}
	opts := autoOpts(env, database.HealthStatusHealthy)
	opts.FullCheckFiles = map[string]struct{}{env.filePath: {}}
	_, ok := env.checker.peerEligible(context.Background(), prep, nil, []CheckOptions{opts}, env.cfg)
	assert.False(t, ok, "a pending peer file is not eligible")
	opts.FullCheckFiles = nil
	_, ok = env.checker.peerEligible(context.Background(), prep, nil, []CheckOptions{opts}, env.cfg)
	assert.True(t, ok, "control: the same file is eligible without the guard")
}

func TestPeerBundle_CyclePassNotifiesGateOnce(t *testing.T) {
	ctx := context.Background()
	env, h := cycleEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(5 * 24 * time.Hour)}}
	env.hw.SetPeerAdviser(adv)
	gate := &fakeGate{pending: map[string]struct{}{healthPath(h): {}}}
	env.hw.SetPeerBundleGate(gate)

	n := statCount(h, func() { require.NoError(t, env.hw.runHealthCheckCycle(ctx)) })
	assert.Equal(t, bigFileSegments, n, "full check, not deferred")
	assert.Zero(t, adv.consulted())
	assert.Equal(t, []string{healthPath(h)}, gate.passed)
	assert.Empty(t, gate.failed)
}

func TestPeerBundle_CycleFailureNotifiesGate(t *testing.T) {
	ctx := context.Background()
	env, h := cycleEnv(t)
	h.markSegmentMissing(bigFileSegments / 2) // only a full check can see this
	gate := &fakeGate{pending: map[string]struct{}{healthPath(h): {}}}
	env.hw.SetPeerBundleGate(gate)
	_, err := env.db.ExecContext(ctx, `UPDATE file_health SET retry_count = ?`, h.cfg.GetMaxRetries()-1)
	require.NoError(t, err)

	require.NoError(t, env.hw.runHealthCheckCycle(ctx))
	assert.Empty(t, gate.passed)
	assert.Equal(t, []string{healthPath(h)}, gate.failed)
}

func TestPeerBundle_NoGateMeansNoLookup(t *testing.T) {
	ctx := context.Background()
	env, h := cycleEnv(t)
	assert.Nil(t, env.hw.pendingPeerBundles(ctx, []string{healthPath(h)}))
	gate := &fakeGate{}
	env.hw.SetPeerBundleGate(gate)
	env.hw.SetPeerBundleGate(nil)
	require.NoError(t, env.hw.runHealthCheckCycle(ctx))
	assert.Zero(t, gate.queried, "removed gate is never asked")
}

func TestPeerBundle_OnlyDefinitiveVerdictsReachTheGate(t *testing.T) {
	const p = "m/a.mkv"
	for name, tc := range map[string]struct {
		update  database.HealthStatusUpdate
		event   EventType
		applied bool
		passed  int
		failed  int
	}{
		"retry is no verdict":      {database.HealthStatusUpdate{FilePath: p, Type: database.UpdateTypeRetry}, EventTypeFileCorrupted, true, 0, 0},
		"inconclusive":             {database.HealthStatusUpdate{FilePath: p, Type: database.UpdateTypeInconclusive}, EventTypeCheckInconclusive, true, 0, 0},
		"check failed w/o verdict": {database.HealthStatusUpdate{FilePath: p, Type: database.UpdateTypeRetry}, EventTypeCheckFailed, true, 0, 0},
		"write lost a race":        {database.HealthStatusUpdate{FilePath: p, Type: database.UpdateTypeHealthy}, EventTypeFileHealthy, false, 0, 0},
		"healthy":                  {database.HealthStatusUpdate{FilePath: p, Type: database.UpdateTypeHealthy}, EventTypeFileHealthy, true, 1, 0},
		"repair triggered":         {database.HealthStatusUpdate{FilePath: p, Type: database.UpdateTypeRepairTrigger}, EventTypeFileCorrupted, true, 0, 1},
		"corrupted":                {database.HealthStatusUpdate{FilePath: p, Type: database.UpdateTypeCorrupted}, EventTypeFileCorrupted, true, 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			hw := &HealthWorker{}
			gate := &fakeGate{}
			hw.SetPeerBundleGate(gate)
			hw.notifyPeerBundleResults(context.Background(), map[string]struct{}{p: {}},
				[]database.HealthStatusUpdate{tc.update}, []bool{tc.applied}, map[string]HealthEvent{p: {Type: tc.event, FilePath: p}})
			assert.Len(t, gate.passed, tc.passed)
			assert.Len(t, gate.failed, tc.failed)
		})
	}
}

// orderObserver records whether the gate had already been told the full check
// passed when the healthy notice (which the publisher acts on) arrived.
type orderObserver struct {
	gate        *fakeGate
	passedFirst []bool
}

func (o *orderObserver) FileHealthy(n HealthyNotice) {
	o.gate.mu.Lock()
	defer o.gate.mu.Unlock()
	o.passedFirst = append(o.passedFirst, len(o.gate.passed) > 0)
}

func TestPeerBundle_GateIsToldBeforeThePublisherSeesTheHealthyNotice(t *testing.T) {
	ctx := context.Background()
	env, h := cycleEnv(t)
	gate := &fakeGate{pending: map[string]struct{}{healthPath(h): {}}}
	obs := &orderObserver{gate: gate}
	env.hw.SetPeerBundleGate(gate)
	env.hw.SetHealthyObserver(obs)
	require.NoError(t, env.hw.runHealthCheckCycle(ctx))
	require.Equal(t, []bool{true}, obs.passedFirst)
}

// A pending peer import whose full check fails for good must not reach the
// ARR repair path (an ARR that grabbed the item learns of the failure through
// its history; a direct import has no ARR): the gate is told, once, and the
// ARR is never called. An ordinary file in the same state still is repaired.
func TestPeerBundle_FailedFullCheckNeverTriggersAnARRRepair(t *testing.T) {
	for name, pending := range map[string]bool{"pending peer import": true, "ordinary file (control)": false} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			env := newRepairTestEnv(t, t.TempDir(), nil)
			filePath := "series/show.s01e01.mkv"
			libraryPath := filepath.Join(t.TempDir(), "show.s01e01.mkv")
			require.NoError(t, os.WriteFile(libraryPath, []byte("x"), 0o644))
			require.NoError(t, env.metadataService.WriteFileMetadata(filePath, validSegmentMeta(env.metadataService, 1024)))
			insertFileHealth(t, env.db, filePath, libraryPath, 2, 3)

			gate := &fakeGate{pending: map[string]struct{}{}}
			if pending {
				gate.pending[filePath] = struct{}{}
			}
			env.hw.SetPeerBundleGate(gate)

			require.NoError(t, env.hw.runHealthCheckCycle(ctx))

			env.mockARRs.mu.Lock()
			calls := len(env.mockARRs.calls)
			env.mockARRs.mu.Unlock()
			if pending {
				assert.Zero(t, calls, "no ARR repair for a failed peer import")
				assert.Equal(t, []string{filePath}, gate.failed, "the gate fails the import exactly once")
				return
			}
			assert.Equal(t, 1, calls, "an ordinary file is repaired as before")
			assert.Empty(t, gate.failed)
		})
	}
}
