package hints

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBudgetBasics(t *testing.T) {
	b := NewBudget(2)
	for i := range 2 {
		if !b.Take(t0) {
			t.Fatalf("full bucket must grant its capacity (token %d)", i+1)
		}
	}
	if b.Take(t0) {
		t.Fatal("exhausted bucket granted")
	}
	if b.Take(t0.Add(29 * time.Minute)) {
		t.Fatal("refilled too early")
	}
	if !b.Take(t0.Add(30 * time.Minute)) {
		t.Fatal("no refill after half an hour at 2/hour")
	}
	if b.Take(t0.Add(30 * time.Minute)) {
		t.Fatal("only one token should have refilled")
	}
}

func TestBudgetCapsAtCapacity(t *testing.T) {
	b := NewBudget(2)
	b.Take(t0)
	if got := b.Remaining(t0.Add(48 * time.Hour)); got != 2 {
		t.Fatalf("remaining %d after a long idle", got)
	}
}

func TestBudgetClockGoingBackwards(t *testing.T) {
	b := NewBudget(2)
	b.Take(t0)
	b.Take(t0)
	// Time jumps back by a day: no refill, no panic.
	if b.Take(t0.Add(-24 * time.Hour)) {
		t.Fatal("granted after the clock went backwards")
	}
	if got := b.Remaining(t0.Add(-24 * time.Hour)); got != 0 {
		t.Fatalf("remaining %d", got)
	}
	// Returning to the original time must not credit the time that was
	// already counted before the jump.
	if b.Take(t0.Add(time.Minute)) {
		t.Fatal("tokens minted by clock oscillation")
	}
	if !b.Take(t0.Add(31 * time.Minute)) {
		t.Fatal("no refill after the clock recovered")
	}
}

func TestBudgetDegenerateSizes(t *testing.T) {
	for _, n := range []int{0, -5} {
		b := NewBudget(n)
		if b.Take(t0.Add(100 * time.Hour)) {
			t.Fatalf("budget of %d granted", n)
		}
	}
	var nilBudget *Budget
	if nilBudget.Take(t0) || nilBudget.Remaining(t0) != 0 {
		t.Fatal("nil budget must deny")
	}
	if NewBudget(1<<30).Remaining(t0) > maxBudgetPerHour {
		t.Fatal("budget not capped")
	}
}

func TestBudgetConcurrent(t *testing.T) {
	b := NewBudget(10)
	var granted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Take(t0) {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()
	if granted.Load() != 10 {
		t.Fatalf("granted %d", granted.Load())
	}
}
