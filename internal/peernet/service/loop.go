package service

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// loop is the discovery worker: it retries a failed listener bind, reconciles
// the open network's configured peers when they change and runs one discovery
// round. Membership upkeep runs on its own schedule (maintLoop). It stops with
// ctx.
func (n *node) loop(ctx context.Context) {
	defer n.wg.Done()
	every := n.deps.TickInterval
	if every <= 0 {
		every = defaultTickEvery
	}
	timer := time.NewTimer(0) // first round right after start
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.kick:
		case <-timer.C:
		}
		n.step(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		wait := jitter(every)
		if retry := n.listenerRetryIn(); retry > 0 && retry < wait {
			wait = retry
		}
		timer.Reset(wait)
	}
}

// jitter spreads d by +-20% so instances do not tick in lockstep.
func jitter(d time.Duration) time.Duration {
	return d - d/5 + time.Duration(rand.Int64N(int64(d)*2/5+1))
}

func (n *node) step(ctx context.Context) {
	n.retryListener(ctx)
	n.reconcileOpen(ctx)
	n.disc.Tick(ctx)
}

// reconcileOpen hands the configured open-network addresses to discovery when
// they changed, and once after the network (re)appears: unresolved user
// addresses live in memory only. The peer list counts as reconciled only when
// discovery accepted it, so a transient failure is retried on the next pass.
func (n *node) reconcileOpen(ctx context.Context) {
	n.mu.Lock()
	s := n.settings
	key := strings.Join(s.OpenPeers, "\n")
	_, participating := n.active[openID.String()]
	if !participating || (n.reconciledValid && n.reconciledKey == key) {
		n.mu.Unlock()
		return
	}
	gen, reconcile := n.reconcileGen, n.reconcileFn
	n.mu.Unlock()
	err := reconcile(ctx, openID, s.OpenPeers)
	if err == nil {
		n.mu.Lock()
		if n.reconcileGen == gen {
			n.reconciledKey, n.reconciledValid = key, true
		}
		n.mu.Unlock()
	}
	n.logChange(ctx, "open network peers", err)
}

// syncNetworks makes discovery's network set match the stored networks and
// the configuration. The open network's level comes from the configuration
// only; a stored row's level is ignored for it.
func (n *node) syncNetworks(ctx context.Context) {
	// Held across the read of the stored rows and the apply: API goroutines
	// and configuration changes both call this, and an older read must never
	// be applied after a newer one.
	n.syncMu.Lock()
	defer n.syncMu.Unlock()
	rows, err := n.mem.ListNetworks(ctx)
	if err != nil {
		n.logChange(ctx, "list networks", err)
		return
	}
	n.logChange(ctx, "list networks", nil)
	s := n.snapshot()
	want := map[string]struct{}{}
	for _, row := range rows {
		id, err := netid.ParseNetworkID(row.NetworkID)
		if err != nil {
			continue
		}
		level, allowPrivate := row.ExposureLevel, row.AllowPrivateAddresses
		if row.Role == database.PeernetRoleOpen {
			level, allowPrivate = s.OpenLevel, false
		}
		if level < 1 || row.RevokedAt != 0 {
			continue // off, or removed by the host: no peer traffic
		}
		nc := discovery.NetworkConfig{ID: id, AllowPrivate: allowPrivate}
		if n.listening() {
			nc.AdvertiseAddr = s.AdvertiseAddress
			if nc.AdvertiseAddr == "" && row.Role == database.PeernetRoleHost {
				nc.AdvertiseAddr = row.HostAddress
			}
		}
		err = n.disc.SetNetwork(nc)
		if err != nil && nc.AdvertiseAddr != "" {
			// An advertise address this network's policy rejects must not
			// keep the network out.
			n.logChange(ctx, "advertise address "+row.NetworkID[:8], err)
			nc.AdvertiseAddr = ""
			err = n.disc.SetNetwork(nc)
		}
		if err != nil {
			n.logChange(ctx, "network "+row.NetworkID[:8], err)
			continue
		}
		want[row.NetworkID] = struct{}{}
		n.setRecordNetwork(id, level, allowPrivate)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.active {
		if _, ok := want[id]; !ok {
			if nid, err := netid.ParseNetworkID(id); err == nil {
				n.disc.RemoveNetwork(nid)
				n.removeRecordNetwork(nid)
			}
		}
	}
	if _, was := n.active[openID.String()]; !was {
		n.reconciledValid = false
		n.reconcileGen++
	}
	if _, now := want[openID.String()]; !now {
		n.reconciledValid = false
		n.reconcileGen++
	}
	n.active = want
}
