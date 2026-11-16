package hints

import (
	"sync"
	"time"
)

// maxBudgetPerHour bounds a configured budget so a typo cannot turn peer data
// into an unbounded re-check generator.
const maxBudgetPerHour = 1000

// Budget is an in-memory token bucket for peer-triggered re-checks. One
// Budget is shared by every network and every publisher, so adding identities
// never raises it.
//
// Accounting is in whole time units (each token costs Hour/perHour of credit),
// so there is no floating-point drift. The bucket starts full. A clock that
// goes backwards neither refills nor loses credit, and the time it already
// counted is not counted twice when the clock recovers.
type Budget struct {
	mu      sync.Mutex
	period  time.Duration // credit one token costs
	credit  time.Duration // current credit, at most time.Hour
	last    time.Time     // latest time seen
	started bool
}

// NewBudget returns a full bucket that grants perHour tokens per hour. A
// perHour below 1 grants nothing.
func NewBudget(perHour int) *Budget {
	if perHour < 1 {
		return &Budget{}
	}
	perHour = min(perHour, maxBudgetPerHour)
	return &Budget{period: time.Hour / time.Duration(perHour), credit: time.Hour}
}

// Take spends one token at time now and reports whether one was available.
func (b *Budget) Take(now time.Time) bool {
	if b == nil || b.period <= 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(now)
	if b.credit < b.period {
		return false
	}
	b.credit -= b.period
	return true
}

// Remaining reports how many tokens are available at time now.
func (b *Budget) Remaining(now time.Time) int {
	if b == nil || b.period <= 0 {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(now)
	return int(b.credit / b.period)
}

func (b *Budget) refill(now time.Time) {
	if !b.started {
		b.started, b.last = true, now
		return
	}
	if !now.After(b.last) {
		return
	}
	b.credit = min(time.Hour, b.credit+now.Sub(b.last))
	b.last = now
}
