package bundleimport

import (
	"errors"
	"sync"
)

// errCost means the shared cost budget of one accept is used up.
var errCost = errors.New("accept cost budget exhausted")

// costBudget is the one budget the accept-time reads of a bundle share: the
// yEnc header samples and the archive header reads are charged to it, so a
// hostile bundle cannot multiply its cost by presenting several kinds of work.
// The unit is the declared size of every article touched (the pool reads whole
// articles).
type costBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
	n     int
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
