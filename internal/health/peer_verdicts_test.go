package health

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
)

// degradedGate is a gate that also wants to hear about degraded verdicts.
type degradedGate struct {
	fakeGate
	mu2      sync.Mutex
	degraded []string
}

func (g *degradedGate) FullCheckDegraded(_ context.Context, p string) {
	g.mu2.Lock()
	g.degraded = append(g.degraded, p)
	g.mu2.Unlock()
}

// A check that failed with no segment data and ended as a corrupted status
// is a definitive failure like any corrupted verdict.
func TestPeerBundle_CheckFailedEndingCorruptedFailsTheImport(t *testing.T) {
	const p = "m/a.mkv"
	hw := &HealthWorker{}
	gate := &fakeGate{}
	hw.SetPeerBundleGate(gate)
	hw.notifyPeerBundleResults(context.Background(), map[string]struct{}{p: {}},
		[]database.HealthStatusUpdate{{FilePath: p, Type: database.UpdateTypeCorrupted}}, []bool{true},
		map[string]HealthEvent{p: {Type: EventTypeCheckFailed, FilePath: p}})
	assert.Equal(t, []string{p}, gate.failed)
}

// A degraded verdict (playable with a few missing segments) ends a pending
// peer import: the gate is told it was degraded, never that it passed.
func TestPeerBundle_DegradedVerdictEndsThePendingImport(t *testing.T) {
	const p = "m/a.mkv"
	hw := &HealthWorker{}
	gate := &degradedGate{}
	hw.SetPeerBundleGate(gate)
	ev := HealthEvent{Type: EventTypeFileCorrupted, FilePath: p, Classification: &holes.Impact{Verdict: holes.VerdictDegraded}}
	hw.notifyPeerBundleResults(context.Background(), map[string]struct{}{p: {}},
		[]database.HealthStatusUpdate{{FilePath: p, Type: database.UpdateTypeCorrupted}}, []bool{true},
		map[string]HealthEvent{p: ev})
	assert.Equal(t, []string{p}, gate.degraded)
	assert.Empty(t, gate.failed)
	assert.Empty(t, gate.passed)
}

func TestPeerBundle_DegradedVerdictOfAPendingImportIsNotRecordedAsDegraded(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	const filePath = "/movies/movie.mp4"
	env.hw.SetPeerBundleGate(&fakeGate{pending: map[string]struct{}{filePath: {}}})
	fh := database.FileHealth{FilePath: filePath, Status: database.HealthStatusPending}
	ev := HealthEvent{Type: EventTypeFileCorrupted, FilePath: filePath, Status: database.HealthStatusCorrupted,
		Classification: &holes.Impact{Verdict: holes.VerdictDegraded, TotalMissing: 2, LongestRun: 1}}
	update, _ := env.hw.prepareUpdateForResult(context.Background(), &fh, ev)
	assert.Equal(t, database.UpdateTypeCorrupted, update.Type, "a degraded peer import is a failed one at once, no PAR2 repair queued")
}

type fakeHeaderVerifier struct {
	err   error
	calls []string
}

func (v *fakeHeaderVerifier) VerifyPeerHeaders(_ context.Context, p string) error {
	v.calls = append(v.calls, p)
	return v.err
}

func TestPeerBundle_HeaderSampleRunsOnlyForFullCheckFilesAndDecidesTheVerdict(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want EventType
	}{
		"matches":      {nil, EventTypeFileHealthy},
		"contradicts":  {ErrPeerLayoutMismatch, EventTypeFileCorrupted},
		"inconclusive": {errors.New("provider down"), EventTypeCheckInconclusive},
	} {
		t.Run(name, func(t *testing.T) {
			env := sampledEnv(t)
			v := &fakeHeaderVerifier{err: tc.err}
			env.checker.SetPeerHeaderVerifier(v)
			opts := CheckOptions{FullCheckFiles: map[string]struct{}{env.filePath: {}}}
			ev := env.checker.CheckFile(context.Background(), env.filePath, opts)
			require.Equal(t, tc.want, ev.Type)
			assert.Equal(t, []string{env.filePath}, v.calls)
			if tc.want == EventTypeFileCorrupted {
				require.Error(t, ev.Error)
				assert.ErrorIs(t, ev.Error, ErrPeerLayoutMismatch)
			}
		})
	}
	t.Run("ordinary files never call it", func(t *testing.T) {
		env := sampledEnv(t)
		v := &fakeHeaderVerifier{err: ErrPeerLayoutMismatch}
		env.checker.SetPeerHeaderVerifier(v)
		ev := env.checker.CheckFile(context.Background(), env.filePath, CheckOptions{})
		assert.Equal(t, EventTypeFileHealthy, ev.Type)
		assert.Empty(t, v.calls)
	})
}
