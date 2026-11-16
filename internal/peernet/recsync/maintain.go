package recsync

import (
	"context"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// MaintainResult summarises one upkeep pass.
type MaintainResult struct {
	// Purged is the number of expired records deleted this pass.
	Purged int
	// OrphanNetworks is how many networks without a network row had their
	// records deleted; OrphanCursors how many cursors without a peer.
	OrphanNetworks int
	OrphanCursors  int
	// Reconciled is how many maintained store counters were found out of step
	// with the rows and corrected (0 on every pass that did not reconcile).
	Reconciled int
	// Err is the first failure; the pass carries on past it.
	Err error
}

// Maintain runs one bounded upkeep pass: it purges expired records in
// batches (at most PurgeBatchesPerPass of PurgeBatch rows), deletes the
// records of networks that no longer exist, and forgets the cursors of peers
// that are no longer known.
//
// known lists the network IDs that currently exist. It is called after the
// stored networks are listed, so a network created in between can never be
// mistaken for an orphan. A nil known skips the orphan-network cleanup.
func (x *Exchange) Maintain(ctx context.Context, known func(ctx context.Context) (map[string]bool, error)) MaintainResult {
	var res MaintainResult
	fail := func(what string, err error) {
		if err != nil && ctx.Err() != nil {
			return // shutting down: not a problem worth reporting
		}
		x.logChange(ctx, what, err)
		if err != nil && res.Err == nil {
			res.Err = err
		}
	}
	for i := 0; i < x.limits.PurgeBatchesPerPass && ctx.Err() == nil; i++ {
		n, err := x.cfg.Store.PurgeExpired(ctx, x.limits.PurgeBatch)
		if err != nil {
			fail("purge expired records", err)
			break
		}
		fail("purge expired records", nil)
		res.Purged += n
		if n < x.limits.PurgeBatch {
			break
		}
	}
	if known != nil {
		res.OrphanNetworks = x.cleanupOrphanNetworks(ctx, known, fail)
	}
	res.Reconciled = x.maybeReconcile(ctx, fail)
	n, err := x.cfg.Cursors.DeleteOrphans(ctx)
	fail("forget orphan cursors", err)
	res.OrphanCursors = int(n)
	return res
}

func (x *Exchange) cleanupOrphanNetworks(ctx context.Context, known func(context.Context) (map[string]bool, error), fail func(string, error)) int {
	// The stored IDs are read before the live ones: a network created after
	// this snapshot is not in it, and one created before it is in the live set.
	stored, err := x.cfg.Store.NetworkIDs(ctx)
	if err != nil {
		fail("list record networks", err)
		return 0
	}
	live, err := known(ctx)
	if err != nil {
		fail("list networks for cleanup", err)
		return 0
	}
	removed := 0
	for _, id := range stored {
		if live[id] {
			continue
		}
		nw, err := netid.ParseNetworkID(id)
		if err != nil {
			continue
		}
		if err := x.DeleteNetworkData(ctx, nw); err != nil {
			fail("delete orphan network records", err)
			continue
		}
		removed++
	}
	return removed
}

// CountReconciler is implemented by stores that keep maintained counters.
type CountReconciler interface {
	ReconcileCounts(ctx context.Context, networkID string) ([]database.PeernetCountMismatch, error)
}

// maybeReconcile re-derives the store's maintained counters every
// ReconcileEveryPasses passes, one cheap grouped count per stored network.
func (x *Exchange) maybeReconcile(ctx context.Context, fail func(string, error)) int {
	rec, ok := x.cfg.Store.(CountReconciler)
	if !ok {
		return 0
	}
	x.mu.Lock()
	due := x.maintRuns%x.limits.ReconcileEveryPasses == 0
	x.maintRuns++
	x.mu.Unlock()
	if !due {
		return 0
	}
	ids, err := x.cfg.Store.NetworkIDs(ctx)
	if err != nil {
		fail("list record networks for reconcile", err)
		return 0
	}
	fixed := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			break
		}
		diff, err := rec.ReconcileCounts(ctx, id)
		if err != nil {
			fail("reconcile record counters", err)
			continue
		}
		fixed += len(diff)
	}
	return fixed
}
