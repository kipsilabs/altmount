package service

import (
	"context"
	"time"
)

// maintLoop is the membership upkeep worker. It runs on its own schedule,
// independent of discovery's rounds and of how many networks are joined: the
// membership layer decides per network whether anything is due (a certificate
// at half-life, a revocation list older than the refresh interval) and skips
// the rest without touching the network. It stops with ctx.
func (n *node) maintLoop(ctx context.Context) {
	defer n.wg.Done()
	every := n.deps.MaintainInterval
	if every <= 0 {
		every = defaultMaintainEvery
	}
	timer := time.NewTimer(0) // once right after start
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.maintKick:
		case <-timer.C:
		}
		n.maintain(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(jitter(every))
	}
}

// maintain is one upkeep pass.
func (n *node) maintain(ctx context.Context) {
	n.cleanupOrphanPeers(ctx)
	n.logChange(ctx, "membership renewal", n.mem.RenewDue(ctx))
	orphans := n.maintRuns.Load()%orphanCleanupEvery == 0
	n.maintainRecords(ctx, orphans)
	n.maintainPublishing(ctx, orphans)
	n.maintainHints(ctx)
	n.maintRuns.Add(1)
}

// kickMaintenance asks for an upkeep pass soon.
func (n *node) kickMaintenance() {
	select {
	case n.maintKick <- struct{}{}:
	default:
	}
}
