package bundleimport

import (
	"sync"
	"time"
)

// A local, decaying back-off per Bundle ID for deterministic local declines.
//
// Some declines are decided by this node alone and repeat identically every
// time the same bundle is offered (the layout does not fit the user's NZB, an
// archive part cannot be bound, the order of the parts cannot be confirmed, the
// media structure is invalid). Verifying such a bundle again reads articles for
// the same answer, so after one decline the Bundle ID is skipped for a short
// time that doubles with every repeat and is forgotten after a day. It is a
// purely local, in-memory measure: it never touches the publisher's standing or
// the failed-bundle cache, and a restart forgets it (the safe direction).
const (
	declineBase   = 10 * time.Minute
	declineMax    = 6 * time.Hour
	declineForget = 24 * time.Hour
	maxDeclined   = 1024
)

// declinesBackOff says whether a decline reason is a deterministic local one.
func declinesBackOff(r Reason) bool {
	switch r {
	case ReasonSizeImplausible, ReasonExtentUnverifiable, ReasonPartOrder, ReasonPartOrderUnverifiable,
		ReasonStructureInvalid, ReasonMediaInvalid, ReasonNoMediaFile, ReasonTooManyTables:
		return true
	}
	return false
}

type declineEntry struct {
	until, last time.Time
	n           int
	reason      Reason
}

// declineBook holds the declined Bundle IDs. The zero value is ready.
type declineBook struct {
	mu sync.Mutex
	m  map[string]*declineEntry
}

// note records a decline of bundleID at now.
func (d *declineBook) note(bundleID string, r Reason, now time.Time) {
	if bundleID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.m == nil {
		d.m = map[string]*declineEntry{}
	}
	e := d.m[bundleID]
	if e != nil && now.Sub(e.last) > declineForget {
		e = nil
	}
	if e == nil {
		if len(d.m) >= maxDeclined {
			d.evictLocked(now)
		}
		e = &declineEntry{}
		d.m[bundleID] = e
	}
	e.n++
	wait := declineBase
	for k := 1; k < e.n && wait < declineMax; k++ {
		wait *= 2
	}
	e.until, e.last, e.reason = now.Add(min(wait, declineMax)), now, r
}

// blocked says whether bundleID is being skipped, and the reason it was
// declined for.
func (d *declineBook) blocked(bundleID string, now time.Time) (Reason, bool) {
	if bundleID == "" {
		return "", false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	e := d.m[bundleID]
	if e == nil {
		return "", false
	}
	if now.Sub(e.last) > declineForget {
		delete(d.m, bundleID)
		return "", false
	}
	return e.reason, now.Before(e.until)
}

// has reports whether an entry exists (tests).
func (d *declineBook) has(bundleID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.m[bundleID] != nil
}

func (d *declineBook) size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.m)
}

// evictLocked drops forgotten entries, and the least recently declined one when
// none was forgotten.
func (d *declineBook) evictLocked(now time.Time) {
	oldest, oldestAt := "", time.Time{}
	for id, e := range d.m {
		if now.Sub(e.last) > declineForget {
			delete(d.m, id)
			continue
		}
		if oldest == "" || e.last.Before(oldestAt) {
			oldest, oldestAt = id, e.last
		}
	}
	if len(d.m) >= maxDeclined && oldest != "" {
		delete(d.m, oldest)
	}
}
