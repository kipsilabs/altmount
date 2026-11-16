package service

import (
	"context"
	"time"
)

const (
	defaultBackboneEvery = 10 * time.Minute
	// backboneFailureFactor stretches the wait after a failed refresh.
	backboneFailureFactor = 3
)

// backboneLoop keeps the known backbone list fresh, but only while this node
// publishes to a network at level 2 or higher: a node that does not publish
// attestations never needs the list, so the feature then causes no outbound
// request for it. Refresh itself is cheap while the list is fresh. It stops
// with ctx.
func (n *node) backboneLoop(ctx context.Context) {
	defer n.wg.Done()
	src := n.deps.Backbones
	if src == nil {
		return
	}
	every := n.deps.backboneEvery
	if every <= 0 {
		every = defaultBackboneEvery
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		wait := every
		if len(n.rx.PublishNetworks()) > 0 {
			err := src.Refresh(ctx)
			n.logChange(ctx, "backbone list", err)
			if err != nil {
				wait = every * backboneFailureFactor
			}
		}
		timer.Reset(jitter(wait))
	}
}
