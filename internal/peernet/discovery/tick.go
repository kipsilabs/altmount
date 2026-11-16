package discovery

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// TickResult summarises one Tick.
type TickResult struct {
	// Contacted counts peers contacted (resolutions of user-entered addresses
	// excluded); Succeeded and Failed partition it.
	Contacted, Succeeded, Failed int
	// Resolved counts user-entered addresses pinned to a Node ID.
	Resolved int
	// Learned counts new peers stored from exchange and extra sources.
	Learned int
	// Pruned counts stale failing peers deleted.
	Pruned int
}

// Tick runs one round of discovery for every network. The caller invokes it
// periodically; it starts no background goroutines that outlive it and
// returns when all the work it started is done, or when Limits.MaxTickDuration
// (wall clock, across all networks) has passed: contacts still pending then
// are deferred to the next Tick and their peers are not penalised. Networks
// run concurrently, so a network full of stalling peers cannot starve the
// others. Per network it:
//   - resolves pending user-entered addresses that are due,
//   - asks the extra Sources for candidates,
//   - contacts due peers (user-entered first, then verified, then the rest,
//     fewest failures first), at most MaxContactsPerTick and
//     MaxConcurrentContacts at once. Peers that are not yet connected are only
//     started while fewer than TargetPeers are connected (success within
//     ActiveWindow), at most MaxPeers non-user peers per Tick, and unverified
//     peers (dial-backs) also draw from the global DialBacksPerWindow budget
//     and the budget of their target prefix,
//   - prunes stale failing peers.
func (s *Service) Tick(ctx context.Context) TickResult {
	ctx, cancel := context.WithTimeout(ctx, s.limits.MaxTickDuration)
	defer cancel()
	s.mu.Lock()
	ids := make([]string, 0, len(s.nets))
	for id := range s.nets {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	sort.Strings(ids)
	results := make([]TickResult, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		nc, ok := s.network(id)
		if !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = s.tickNetwork(ctx, nc)
		}()
	}
	wg.Wait()
	var total TickResult
	for _, r := range results {
		total.Contacted += r.Contacted
		total.Succeeded += r.Succeeded
		total.Failed += r.Failed
		total.Resolved += r.Resolved
		total.Learned += r.Learned
		total.Pruned += r.Pruned
	}
	return total
}

// tickCounters collects one network's results from concurrent jobs.
type tickCounters struct {
	contacted, succeeded, failed, resolved, learned atomic.Int64
	storeDown                                       atomic.Bool // listing failed; skip work that needs the store
}

func (s *Service) tickNetwork(ctx context.Context, nc NetworkConfig) TickResult {
	var c tickCounters
	now := s.now()
	budget := s.limits.MaxContactsPerTick

	jobs := s.resolveJobs(ctx, nc, now, &budget, &c)
	s.learnFromSources(ctx, nc, &c)
	jobs = append(jobs, s.contactJobs(ctx, nc, now, &budget, &c)...)
	s.runJobs(ctx, jobs)

	var pruned int64
	if ctx.Err() == nil && !c.storeDown.Load() {
		pruned = s.prune(ctx, nc, now)
	}
	return TickResult{
		Contacted: int(c.contacted.Load()), Succeeded: int(c.succeeded.Load()), Failed: int(c.failed.Load()),
		Resolved: int(c.resolved.Load()), Learned: int(c.learned.Load()), Pruned: int(pruned),
	}
}

func (s *Service) problemKey(what string, nc NetworkConfig) string {
	return what + " " + nc.ID.String()[:8]
}

// resolveJobs builds the jobs resolving user-entered addresses that are due.
func (s *Service) resolveJobs(ctx context.Context, nc NetworkConfig, now time.Time, budget *int, c *tickCounters) []func() {
	var jobs []func()
	for _, addr := range s.duePending(nc, now) {
		if *budget <= 0 {
			break
		}
		*budget--
		jobs = append(jobs, func() {
			cctx, cancel := context.WithTimeout(ctx, s.limits.CallTimeout)
			defer cancel()
			err := s.resolveUser(cctx, nc, addr)
			if err != nil && ctx.Err() != nil {
				return // the tick ran out of time: not the address's fault
			}
			s.notePending(nc, addr, err)
			if err == nil {
				c.resolved.Add(1)
			}
		})
	}
	return jobs
}

// learnFromSources asks the extra Sources for candidates and stores them.
func (s *Service) learnFromSources(ctx context.Context, nc NetworkConfig, c *tickCounters) {
	s.mu.Lock()
	sources := append([]Source(nil), s.sources...)
	s.mu.Unlock()
	for _, src := range sources {
		if ctx.Err() != nil {
			return
		}
		cands, err := src.Discover(ctx, nc.ID)
		if err != nil {
			s.logChange(ctx, s.problemKey("source "+src.Name(), nc), err)
			continue
		}
		s.logChange(ctx, s.problemKey("source "+src.Name(), nc), nil)
		n, err := s.intakeCandidates(ctx, nc, cands)
		s.logChange(ctx, s.problemKey("store candidates", nc), err)
		c.learned.Add(int64(n))
	}
}

// contactJobs selects the due peers to contact and builds their jobs.
func (s *Service) contactJobs(ctx context.Context, nc NetworkConfig, now time.Time, budget *int, c *tickCounters) []func() {
	net := nc.ID.String()
	all, err := s.cfg.Store.List(ctx, net)
	if err != nil {
		c.storeDown.Store(true)
		s.logChange(ctx, s.problemKey("list peers", nc), err)
		return nil
	}
	active := map[string]bool{}
	for _, p := range all {
		if !p.LastSuccess.IsZero() && now.Sub(p.LastSuccess) <= s.limits.ActiveWindow {
			active[p.NodeID] = true
		}
	}
	due, err := s.cfg.Store.ListDue(ctx, net, now, s.limits.MaxContactsPerTick*4)
	if err != nil {
		c.storeDown.Store(true)
		s.logChange(ctx, s.problemKey("list peers", nc), err)
		return nil
	}
	s.logChange(ctx, s.problemKey("list peers", nc), nil)
	var jobs []func()
	newStarted, nonUser := 0, 0
	for _, p := range due {
		if *budget <= 0 {
			break
		}
		if p.Source != database.PeernetSourceUser {
			if nonUser >= nc.MaxPeers {
				continue
			}
			if !active[p.NodeID] && len(active)+newStarted >= nc.TargetPeers {
				continue
			}
			if (!p.Verified || p.CandidateAddress != "") && !s.takeDialBack(contactAddr(p), now) {
				continue
			}
			nonUser++
			if !active[p.NodeID] {
				newStarted++
			}
		}
		*budget--
		jobs = append(jobs, func() {
			c.contacted.Add(1)
			n, err := s.contact(ctx, nc, p)
			c.learned.Add(int64(n))
			if err != nil {
				c.failed.Add(1)
				return
			}
			c.succeeded.Add(1)
		})
	}
	return jobs
}

func (s *Service) prune(ctx context.Context, nc NetworkConfig, now time.Time) int64 {
	n, err := s.cfg.Store.Prune(ctx, nc.ID.String(), now.Add(-s.limits.PruneAfter), s.limits.PruneFailures)
	s.logChange(ctx, s.problemKey("prune peers", nc), err)
	return n
}

// runJobs runs the jobs with bounded concurrency. Jobs not started before ctx
// ends are dropped (their peers stay due for the next Tick).
func (s *Service) runJobs(ctx context.Context, jobs []func()) {
	sem := make(chan struct{}, s.limits.MaxConcurrentContacts)
	var wg sync.WaitGroup
loop:
	for _, j := range jobs {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			j()
		}()
	}
	wg.Wait()
}

// duePending returns the unresolved user-entered addresses to try now.
func (s *Service) duePending(nc NetworkConfig, now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, ok := s.nets[nc.ID.String()]
	if !ok {
		return nil
	}
	var out []string
	for _, a := range ns.order {
		if pu := ns.pending[a]; pu != nil && !pu.next.After(now) {
			out = append(out, a)
		}
	}
	return out
}

// notePending records the outcome of resolving a pending address.
func (s *Service) notePending(nc NetworkConfig, addr string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, ok := s.nets[nc.ID.String()]
	if !ok {
		return
	}
	pu := ns.pending[addr]
	if pu == nil || err == nil {
		return
	}
	pu.failures++
	pu.next = s.now().Add(backoff(s.limits.UserBackoffBase, s.limits.UserBackoffMax, pu.failures))
	pu.lastErr = truncate(err.Error(), 200)
}

func (s *Service) backoffFor(p database.PeernetPeer) func(int) time.Duration {
	if p.Source == database.PeernetSourceUser {
		return func(f int) time.Duration { return backoff(s.limits.UserBackoffBase, s.limits.UserBackoffMax, f) }
	}
	return func(f int) time.Duration { return backoff(s.limits.PeerBackoffBase, s.limits.PeerBackoffMax, f) }
}

// contactAddr is the address contact will dial for p.
func contactAddr(p database.PeernetPeer) string {
	if p.CandidateAddress != "" && p.Source != database.PeernetSourceUser {
		return p.CandidateAddress
	}
	return p.Address
}

// contact does hello and exchange with one stored peer and records the
// outcome. A success proves, by the pinned handshake, that the stored (or
// candidate) address is served by exactly this Node ID, which is what makes a
// peer verified. It returns the number of new peers learned. Only addresses
// of user-entered and local-source peers are dialled as trusted configuration.
func (s *Service) contact(ctx context.Context, nc NetworkConfig, p database.PeernetPeer) (int, error) {
	node, err := identity.ParseNodeID(p.NodeID)
	if err != nil {
		s.logChange(ctx, s.problemKey("delete invalid peer", nc), s.cfg.Store.Delete(ctx, p.NetworkID, p.NodeID))
		return 0, err
	}
	addr := contactAddr(p)
	isCandidate := addr != p.Address
	cctx, cancel := context.WithTimeout(ctx, 2*s.limits.CallTimeout)
	defer cancel()
	learned, err := s.talk(cctx, nc, addr, node, trustedSource(p.Source))
	if err != nil && ctx.Err() != nil {
		return learned, err // the tick ran out of time: nobody's fault, stays due
	}
	// Recording the outcome must not depend on the tick's remaining time.
	sctx := context.WithoutCancel(ctx)
	now := s.now()
	if err == nil {
		promote := ""
		if isCandidate {
			promote = addr
		}
		serr := s.cfg.Store.RecordSuccess(sctx, p.NetworkID, p.NodeID, promote, now, now.Add(s.limits.RefreshInterval))
		s.logChange(ctx, s.problemKey("record success", nc), serr)
		return learned, serr
	}
	if isCandidate {
		s.logChange(ctx, s.problemKey("clear candidate", nc), s.cfg.Store.SetCandidateAddress(sctx, p.NetworkID, p.NodeID, "", now))
	}
	if p.Source == database.PeernetSourceUser && errors.Is(err, transport.ErrNodeIDMismatch) {
		s.flagChanged(sctx, nc, p)
	}
	_, ferr := s.cfg.Store.RecordFailure(sctx, p.NetworkID, p.NodeID, now, s.backoffFor(p))
	s.logChange(ctx, s.problemKey("record failure", nc), ferr)
	return learned, err
}

func (s *Service) talk(ctx context.Context, nc NetworkConfig, addr string, node identity.NodeID, trusted bool) (int, error) {
	if err := s.hello(ctx, nc, addr, node, trusted); err != nil {
		return 0, err
	}
	return s.exchange(ctx, nc, addr, node, trusted)
}

// flagChanged checks what a pinned user-entered address presents now and, if
// it is a different Node ID, flags it without accepting it.
func (s *Service) flagChanged(ctx context.Context, nc NetworkConfig, p database.PeernetPeer) {
	got, err := s.cfg.Dialer.ProbeNodeID(ctx, p.Address, s.callOpts(nc, 1, true))
	if err != nil || got.String() == p.NodeID || got.IsZero() {
		return
	}
	serr := s.cfg.Store.SetChangedNode(ctx, p.NetworkID, p.NodeID, got.String())
	s.logChange(ctx, s.problemKey("flag changed node", nc), serr)
}
