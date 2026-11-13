package health

import (
	"log/slog"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
)

// HealthyNotice tells an observer that a file was just found healthy by a
// completed local check, after the healthy status was written for that file.
// It is only sent for a fresh check result: never for a file that became
// healthy through repair, never for an inconclusive, failed or corrupted
// result, and never when the write lost a race with a concurrent change.
// Scheduled cycles and manual rechecks both notify: a manual recheck is a real
// full local check. A check whose sample peer advice lowered is flagged in
// Stats.ReducedByPeerAdvice.
type HealthyNotice struct {
	FilePath  string
	CheckedAt time.Time
	Stats     CheckStats
}

// HealthyObserver receives HealthyNotice values. FileHealthy runs on the
// health worker's goroutine, so an implementation must return at once: hand
// the notice to a bounded queue, never touch a database or the network, and
// drop rather than block. A panic is recovered and logged; it cannot fail the
// check.
type HealthyObserver interface {
	FileHealthy(n HealthyNotice)
}

// observerHolder lets the worker store the observer atomically (an interface
// value cannot be stored in an atomic.Pointer directly).
type observerHolder struct{ o HealthyObserver }

// SetHealthyObserver installs (or, with nil, removes) the observer. It is safe
// to call at any time. With no observer a healthy result costs one atomic
// load.
func (hw *HealthWorker) SetHealthyObserver(o HealthyObserver) {
	if o == nil {
		hw.healthyObserver.Store(nil)
		hw.healthChecker.patchStats.Store(false)
		return
	}
	// The patch summary is switched on first: a check that reaches the
	// observer must never have skipped the summary, because "not computed"
	// would be read as "not patched".
	hw.healthChecker.patchStats.Store(true)
	hw.healthyObserver.Store(&observerHolder{o: o})
}

// notifyHealthyApplied announces the healthy results whose write changed a row
// (applied is index-aligned with updates). With no observer it does nothing.
func (hw *HealthWorker) notifyHealthyApplied(updates []database.HealthStatusUpdate, applied []bool, events map[string]HealthEvent) {
	if hw.healthyObserver.Load() == nil {
		return
	}
	for i, u := range updates {
		if i >= len(applied) || !applied[i] || u.Type != database.UpdateTypeHealthy {
			continue
		}
		if ev, ok := events[u.FilePath]; ok {
			hw.notifyHealthy(ev)
		}
	}
}

// notifyHealthy delivers a healthy result to the observer, if any.
func (hw *HealthWorker) notifyHealthy(event HealthEvent) {
	h := hw.healthyObserver.Load()
	if h == nil || event.Stats == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Health observer panicked", "panic", r)
		}
	}()
	h.o.FileHealthy(HealthyNotice{FilePath: event.FilePath, CheckedAt: event.Timestamp, Stats: *event.Stats})
}
