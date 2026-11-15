package bundleimport

import (
	"context"
	"errors"
	"sync"
	"time"
)

// The node-wide verification budget. Every accept reads real articles from the
// user's providers to check a peer's layout, and a hostile peer can offer a new
// Bundle ID each time, so the cost of verifying is bounded for the whole node,
// not only per bundle: a token bucket of bytes per hour and a limit on the
// accepts running at once. Two classes share it: an automatic attempt (the
// importer fast path, which falls back to a normal import when declined) and an
// explicit one (the user asked for this release). Automatic attempts can never
// take the last quarter of the bytes or the last concurrency slot, so a user's
// own play or import is not starved by them.
const (
	// DefaultVerifyBytesPerHour is the bytes per hour the node spends verifying
	// peer bundles (8 GiB: a handful of large honest sets, hundreds of small
	// ones).
	DefaultVerifyBytesPerHour int64 = 8 << 30
	// DefaultVerifyConcurrent is how many accepts may verify at the same time.
	DefaultVerifyConcurrent = 2
	// verifyAutoReserveDiv: automatic attempts leave 1/4 of the bucket to
	// explicit ones.
	verifyAutoReserveDiv = 4
	verifyWindow         = time.Hour
)

// ErrVerifyBudget means the node-wide verification budget has no room for
// another bundle right now. It says nothing about the bundle or its publisher.
var ErrVerifyBudget = errors.New("the node-wide verification budget is exhausted")

// VerifyClass says who asked for a verification.
type VerifyClass uint8

const (
	// VerifyExplicit is a user's own request (play, direct import).
	VerifyExplicit VerifyClass = iota
	// VerifyAutomatic is an attempt the importer makes on its own.
	VerifyAutomatic
)

type verifyClassKey struct{}

// WithVerifyClass marks ctx as carrying an accept of the given class.
func WithVerifyClass(ctx context.Context, c VerifyClass) context.Context {
	return context.WithValue(ctx, verifyClassKey{}, c)
}

func verifyClassOf(ctx context.Context) VerifyClass {
	c, _ := ctx.Value(verifyClassKey{}).(VerifyClass)
	return c
}

// VerifyLimits configures a VerifyLimiter (zero fields take the defaults).
type VerifyLimits struct {
	BytesPerHour  int64
	MaxConcurrent int
}

// VerifyUsage is a snapshot of the node-wide verification budget.
type VerifyUsage struct {
	// UsedBytes is how much of the hourly bucket is spent right now (the bucket
	// refills continuously over an hour); LimitBytes is its size.
	UsedBytes, LimitBytes int64
	Active, MaxConcurrent int
	// Declined counts attempts refused for lack of budget since start.
	Declined uint64
}

// VerifyLimiter is the node-wide verification budget. Safe for concurrent use.
type VerifyLimiter struct {
	mu         sync.Mutex
	limit      int64
	maxActive  int
	tokens     float64
	at         time.Time
	active     int
	activeAuto int
	declined   uint64
	now        func() time.Time
}

// NewVerifyLimiter creates a full bucket. now may be nil (time.Now).
func NewVerifyLimiter(l VerifyLimits, now func() time.Time) *VerifyLimiter {
	if now == nil {
		now = time.Now
	}
	v := &VerifyLimiter{limit: l.BytesPerHour, maxActive: l.MaxConcurrent, now: now}
	if v.limit <= 0 {
		v.limit = DefaultVerifyBytesPerHour
	}
	if v.maxActive <= 0 {
		v.maxActive = DefaultVerifyConcurrent
	}
	v.tokens, v.at = float64(v.limit), now()
	return v
}

// refill must hold mu.
func (v *VerifyLimiter) refill() {
	t := v.now()
	if d := t.Sub(v.at); d > 0 {
		v.tokens = min(v.tokens+float64(v.limit)*float64(d)/float64(verifyWindow), float64(v.limit))
		v.at = t
	}
}

// VerifyLease is one running verification's claim on the budget.
type VerifyLease struct {
	v        *VerifyLimiter
	class    VerifyClass
	reserved int64
	once     sync.Once
}

// Begin claims a concurrency slot and reserves the bytes the verification is
// expected to read (clamped to the whole bucket). It returns ErrVerifyBudget
// when either is not available to the class.
func (v *VerifyLimiter) Begin(class VerifyClass, expect int64) (*VerifyLease, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.refill()
	expect = min(max(expect, 0), v.limit)
	floor := float64(0)
	if class == VerifyAutomatic {
		floor = float64(v.limit / verifyAutoReserveDiv)
		if v.activeAuto >= max(v.maxActive-1, 1) {
			v.declined++
			return nil, ErrVerifyBudget
		}
	}
	if v.active >= v.maxActive || v.tokens-float64(expect) < floor || v.tokens < float64(expect) {
		v.declined++
		return nil, ErrVerifyBudget
	}
	v.tokens -= float64(expect)
	v.active++
	if class == VerifyAutomatic {
		v.activeAuto++
	}
	return &VerifyLease{v: v, class: class, reserved: expect}, nil
}

// End gives the slot back and settles the reservation against what was really
// read: unused bytes return to the bucket, bytes over the reservation are taken
// from it (it may go below zero, which declines the next attempts until it
// refills).
func (l *VerifyLease) End(actual int64) {
	if l == nil {
		return
	}
	l.once.Do(func() {
		v := l.v
		v.mu.Lock()
		defer v.mu.Unlock()
		v.refill()
		v.tokens = min(v.tokens+float64(l.reserved-max(actual, 0)), float64(v.limit))
		v.tokens = max(v.tokens, -float64(v.limit))
		v.active--
		if l.class == VerifyAutomatic {
			v.activeAuto--
		}
	})
}

// Debit records bytes read by work that cannot be declined (the forced full
// check of an import already written).
func (v *VerifyLimiter) Debit(n int64) {
	if v == nil || n <= 0 {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.refill()
	v.tokens = max(v.tokens-float64(n), -float64(v.limit))
}

// Usage returns a snapshot.
func (v *VerifyLimiter) Usage() VerifyUsage {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.refill()
	return VerifyUsage{UsedBytes: min(max(v.limit-int64(v.tokens), 0), v.limit), LimitBytes: v.limit,
		Active: v.active, MaxConcurrent: v.maxActive, Declined: v.declined}
}

// verifyDebit charges bytes the forced full check read to the node-wide budget.
func (i *Importer) verifyDebit(n int64) { i.cfg.Verify.Debit(n) }

// VerifyUsage is the node-wide verification budget snapshot (zero when none).
func (i *Importer) VerifyUsage() VerifyUsage {
	if i.cfg.Verify == nil {
		return VerifyUsage{}
	}
	return i.cfg.Verify.Usage()
}
