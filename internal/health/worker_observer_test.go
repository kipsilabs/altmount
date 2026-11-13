package health

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingObserver struct {
	mu      sync.Mutex
	notices []HealthyNotice
	panics  bool
}

func (o *recordingObserver) FileHealthy(n HealthyNotice) {
	o.mu.Lock()
	o.notices = append(o.notices, n)
	o.mu.Unlock()
	if o.panics {
		panic("observer bug")
	}
}

func (o *recordingObserver) got() []HealthyNotice {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]HealthyNotice(nil), o.notices...)
}

func healthyEvent(path string) HealthEvent {
	return HealthEvent{Type: EventTypeFileHealthy, FilePath: path, Timestamp: time.Unix(1_700_000_000, 0),
		Stats: &CheckStats{TotalSegments: 10, SegmentsChecked: 10, FullCheck: true}}
}

func TestHealthyObserver_NotifiedOnlyForWritesThatLanded(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	obs := &recordingObserver{}
	env.hw.SetHealthyObserver(obs)
	fh := &database.FileHealth{FilePath: "movies/a.mkv", Status: database.HealthStatusChecking, CreatedAt: time.Now().UTC()}
	ev := healthyEvent(fh.FilePath)

	update, effect := env.hw.prepareUpdateForResult(context.Background(), fh, ev)
	require.NotNil(t, effect)
	_ = effect()
	assert.Empty(t, obs.got(), "neither preparing nor the side effect may notify: the write has not landed")

	events := map[string]HealthEvent{fh.FilePath: ev}
	env.hw.notifyHealthyApplied([]database.HealthStatusUpdate{*update}, []bool{false}, events)
	assert.Empty(t, obs.got(), "a guarded write that was a no-op must not publish")

	env.hw.notifyHealthyApplied([]database.HealthStatusUpdate{*update}, []bool{true}, events)
	got := obs.got()
	require.Len(t, got, 1)
	assert.Equal(t, "movies/a.mkv", got[0].FilePath)
	assert.Equal(t, time.Unix(1_700_000_000, 0), got[0].CheckedAt)
	assert.True(t, got[0].Stats.FullCheck)
}

func TestHealthyObserver_OnlyHealthyResultsNotify(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	obs := &recordingObserver{}
	env.hw.SetHealthyObserver(obs)
	fh := &database.FileHealth{FilePath: "movies/a.mkv", Status: database.HealthStatusChecking, CreatedAt: time.Now().UTC()}

	for _, ev := range []HealthEvent{
		{Type: EventTypeCheckInconclusive, FilePath: fh.FilePath, Stats: &CheckStats{}},
		{Type: EventTypeFileRemoved, FilePath: fh.FilePath},
		{Type: EventTypeCheckFailed, FilePath: fh.FilePath, Status: database.HealthStatusCorrupted},
		{Type: EventTypeFileHealthy, FilePath: fh.FilePath}, // no stats: nothing to attest
	} {
		_, effect := env.hw.prepareUpdateForResult(context.Background(), fh, ev)
		if effect != nil {
			_ = effect()
		}
	}
	assert.Empty(t, obs.got())
}

func TestHealthyObserver_NilObserverChangesNothing(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	fh := &database.FileHealth{FilePath: "movies/a.mkv", Status: database.HealthStatusChecking, CreatedAt: time.Now().UTC()}
	update, effect := env.hw.prepareUpdateForResult(context.Background(), fh, healthyEvent(fh.FilePath))
	assert.Equal(t, database.HealthStatusHealthy, update.Status)
	require.NotNil(t, effect)
	assert.NotPanics(t, func() { _ = effect() })

	env.hw.SetHealthyObserver(nil)
	_, effect = env.hw.prepareUpdateForResult(context.Background(), fh, healthyEvent(fh.FilePath))
	assert.NotPanics(t, func() { _ = effect() })
}

func TestHealthyObserver_PanicNeverFailsTheCheck(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	obs := &recordingObserver{panics: true}
	env.hw.SetHealthyObserver(obs)
	fh := &database.FileHealth{FilePath: "movies/a.mkv", Status: database.HealthStatusChecking, CreatedAt: time.Now().UTC()}
	update, _ := env.hw.prepareUpdateForResult(context.Background(), fh, healthyEvent(fh.FilePath))
	events := map[string]HealthEvent{fh.FilePath: healthyEvent(fh.FilePath)}
	assert.NotPanics(t, func() {
		env.hw.notifyHealthyApplied([]database.HealthStatusUpdate{*update}, []bool{true}, events)
	})
	assert.Len(t, obs.got(), 1)
}

// A file made healthy by a repair outcome (metadata regenerated) was not
// checked, so it must never reach the observer.
func TestHealthyObserver_RepairOutcomesNeverNotify(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	obs := &recordingObserver{}
	env.hw.SetHealthyObserver(obs)
	for _, o := range []repairOutcome{repairOutcomeTriggered, repairOutcomeCorrupted, repairOutcomeDeleted, repairOutcomeRegenerated, repairOutcomeDeferred} {
		update := &database.HealthStatusUpdate{FilePath: "movies/a.mkv"}
		applyRepairOutcome(update, o, nil)
	}
	assert.Empty(t, obs.got())
}
