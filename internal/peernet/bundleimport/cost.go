package bundleimport

import (
	"errors"
	"sync"
)

// errCost means the shared cost budget of one accept is used up.
var errCost = errors.New("accept cost budget exhausted")

// errOverrun means the articles really read were larger than the layout
// declares, which spent the budget: the bundle's numbers are contradicted.
var errOverrun = errors.New("articles are larger than the layout declares")

// costBudget is the one budget the accept-time reads of a bundle share: the
// yEnc header samples and the archive header reads are charged to it, so a
// hostile bundle cannot multiply its cost by presenting several kinds of work.
// The unit is bytes: what a read really delivered (chargeActual), or, for a
// read whose size is only known after the fact, the declared size of every
// article it touches before it starts (charge; the pool reads whole articles).
type costBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
	n     int
	// declStaged is what charge calls made for staged reads declared;
	// excess is what those reads really downloaded beyond it (already added
	// to used).
	declStaged, excess int64
}

func newCostBudget(limit int64) *costBudget { return &costBudget{limit: limit} }

// charge takes n bytes from the budget, or reports errCost.
func (c *costBudget) charge(n int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n < 0 || c.used+n > c.limit {
		return errCost
	}
	c.used += n
	c.n++
	return nil
}

// chargeActual takes what a read really delivered. The bytes are always
// recorded; errOverrun says the budget is now spent.
func (c *costBudget) chargeActual(n int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used += max(n, 0)
	c.n++
	if c.used > c.limit {
		return errOverrun
	}
	return nil
}

// chargeStaged is charge for a staged read: the declared size is taken before
// the read and remembered, so noteStaged can tell what the read exceeded it by.
func (c *costBudget) chargeStaged(n int64) error {
	if err := c.charge(n); err != nil {
		return err
	}
	c.mu.Lock()
	c.declStaged += n
	c.mu.Unlock()
	return nil
}

// noteStaged is told the total bytes the staging area has downloaded so far.
// What exceeds the declared charges of the staged reads is charged too;
// errOverrun says that spent the budget.
func (c *costBudget) noteStaged(total int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if over := total - c.declStaged; over > c.excess {
		c.used += over - c.excess
		c.excess = over
	}
	if c.used > c.limit {
		return errOverrun
	}
	return nil
}

// left is what remains.
func (c *costBudget) left() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return max(c.limit-c.used, 0)
}

// articles is how many charges were made.
func (c *costBudget) articles() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// spent is what was charged.
func (c *costBudget) spent() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}
