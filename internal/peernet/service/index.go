package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// contentIndex is the content-index worker and its metadata observer. It lives
// as long as the feature is enabled and is deliberately independent of the
// node: rebuilding the listener or client (a port, accept-incoming or proxy
// change) must not restart the backfill.
type contentIndex struct {
	deps   Deps
	cancel context.CancelFunc
	wg     sync.WaitGroup
	kick   chan struct{} // asks the worker for another pass

	running atomic.Bool
	scanned atomic.Int64
	indexed atomic.Int64
	skipped atomic.Int64
	// oversize is the number of files the last full derive pass left without
	// a release ID because their group is over the cap.
	oversize atomic.Int64

	deriveKick     chan struct{} // a group changed: derive it soon
	deriveDebounce time.Duration
	deriveMaxWait  time.Duration
}

// Timing of the incremental derive: wait for a quiet period so a burst of
// metadata writes (an import, a health sweep) is derived once, but never
// longer than the maximum.
const (
	defaultDeriveDebounce = 3 * time.Second
	defaultDeriveMaxWait  = 30 * time.Second
)

// startContentIndex hooks the metadata observer and starts the index worker.
// Observer events go through a bounded queue served by one worker, so the
// import write path never waits for hashing or the database; when the queue
// overflows the worker is asked for another reconcile pass, which repairs what
// the dropped events would have done. It returns nil when there is nothing to
// index.
func startContentIndex(parent context.Context, deps Deps) *contentIndex {
	if deps.Metadata == nil || deps.Content == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	c := &contentIndex{deps: deps, cancel: cancel, kick: make(chan struct{}, 1), deriveKick: make(chan struct{}, 1),
		deriveDebounce: defaultDeriveDebounce, deriveMaxWait: defaultDeriveMaxWait}
	if deps.deriveDebounce > 0 {
		c.deriveDebounce = deps.deriveDebounce
	}
	idx := contentid.NewIndexer(deps.Metadata, deps.Content,
		contentid.WithProgress(func(r contentid.BackfillResult) { c.store(r) }),
		contentid.WithOrphanHandler(c.withdraw),
		contentid.WithDirtyNotify(c.requestDerive))
	obs := contentid.NewAsyncObserver(contentid.NewObserver(idx), 0, c.requestReindex)
	c.wg.Add(3)
	go func() {
		defer c.wg.Done()
		obs.Run(ctx)
	}()
	go c.worker(ctx, idx)
	go c.deriveLoop(ctx, idx)
	deps.Metadata.SetFileObserver(obs)
	return c
}

func (c *contentIndex) store(r contentid.BackfillResult) {
	c.scanned.Store(int64(r.Scanned))
	c.indexed.Store(int64(r.Indexed))
	c.skipped.Store(int64(r.Skipped))
}

func (c *contentIndex) requestReindex() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *contentIndex) requestDerive() {
	select {
	case c.deriveKick <- struct{}{}:
	default:
	}
}

// deriveLoop runs the incremental derive pass (see contentid.DeriveDirty)
// after a quiet period following a change to an import group.
func (c *contentIndex) deriveLoop(ctx context.Context, idx *contentid.Indexer) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.deriveKick:
		}
		first := time.Now()
		timer := time.NewTimer(c.deriveDebounce)
	quiet:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-c.deriveKick:
				if time.Since(first) >= c.deriveMaxWait {
					break quiet
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(c.deriveDebounce)
			case <-timer.C:
				break quiet
			}
		}
		timer.Stop()
		if _, err := idx.DeriveDirty(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "Peernet incremental release id derivation failed", "error", err)
		}
	}
}

// withdraw retracts what this node published under library-derived release IDs
// that no file carries any more (the group gained or lost files, or vanished):
// its own records are deleted from every network and the publish state
// forgotten, so a stale release identity is neither served nor re-announced.
// Copies other nodes already hold cannot be recalled and expire on their own.
func (c *contentIndex) withdraw(ctx context.Context, releaseIDs []string) {
	if c.deps.onWithdraw != nil {
		c.deps.onWithdraw(releaseIDs)
	}
	recs, state := c.deps.Records, c.deps.PublishState
	if recs == nil && state == nil {
		return
	}
	var networks []string
	if recs != nil {
		var err error
		if networks, err = recs.NetworkIDs(ctx); err != nil {
			slog.WarnContext(ctx, "Peernet could not list networks to withdraw superseded releases", "error", err)
			return
		}
	}
	for _, id := range releaseIDs {
		for _, nw := range networks {
			if _, err := recs.DeleteOwnRelease(ctx, nw, id); err != nil {
				slog.WarnContext(ctx, "Peernet could not withdraw a superseded release", "error", err)
			}
		}
		if state != nil {
			for _, nw := range networks {
				if err := state.DeleteRelease(ctx, nw, id); err != nil {
					slog.WarnContext(ctx, "Peernet could not forget a superseded release", "error", err)
				}
			}
		}
	}
}

// stop unhooks the observer and waits for the worker.
func (c *contentIndex) stop() {
	c.deps.Metadata.SetFileObserver(nil)
	c.cancel()
	c.wg.Wait()
}

// worker reconciles the index with the metadata tree on every enable (pruning
// rows of vanished files, re-indexing changed ones) and then fills in missing
// rows; it runs again whenever the observer queue overflowed.
func (c *contentIndex) worker(ctx context.Context, idx *contentid.Indexer) {
	defer c.wg.Done()
	for {
		c.running.Store(true)
		if _, err := idx.Reconcile(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "Peernet content index reconcile failed", "error", err)
		}
		res, err := idx.Backfill(ctx)
		c.store(res)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "Peernet content index backfill failed", "error", err)
		}
		// Older imports: give each import group its library-derived release ID
		// from the index alone (no metadata is read again).
		if dres, err := idx.DeriveAll(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "Peernet library release id derivation failed", "error", err)
		} else if err == nil {
			c.oversize.Store(int64(dres.OversizeFiles))
		}
		c.running.Store(false)
		select {
		case <-ctx.Done():
			return
		case <-c.kick:
		}
	}
}

// LibraryStatus says how much of the library can be shared. A file can be
// shared when it has a Release ID: either one from its release's NZB
// ("nzb"), or one derived from the files of its import ("files", for imports
// that predate the NZB store). A file with only a File ID is in an import
// whose files could not be grouped, and one that could not be identified at
// all is not indexable.
type LibraryStatus struct {
	// Files is every library file the index knows about plus those that could
	// not be identified.
	Files int
	// WithReleaseID is the files that can be shared: WithNZBReleaseID plus
	// WithDerivedReleaseID.
	WithReleaseID        int
	WithNZBReleaseID     int
	WithDerivedReleaseID int
	// FileIDOnly are indexed files with no Release ID; NoSourceNZB, GroupTooLarge
	// and Pending say why (they add up to FileIDOnly).
	FileIDOnly int
	// NotIndexable files could not be identified at all.
	NotIndexable int
	// NoSourceNZB: the metadata does not say which import the file came from.
	NoSourceNZB int
	// GroupTooLarge: the import has more files than a derived release may have.
	GroupTooLarge int
	// Pending: the file is in a group the derive pass has not reached yet.
	Pending int
}

// library reads the index counts. It never fails the status: on a read error
// it reports what it has.
func (c *contentIndex) library(ctx context.Context) LibraryStatus {
	counts, err := c.deps.Content.Counts(ctx)
	if err != nil {
		slog.WarnContext(ctx, "Peernet library counts unavailable", "error", err)
	}
	skipped := int(c.skipped.Load())
	idOnly := counts.Files - counts.WithReleaseID
	tooLarge := min(int(c.oversize.Load()), max(idOnly-counts.NoSourceGroup, 0))
	return LibraryStatus{
		Files:                counts.Files + skipped,
		WithReleaseID:        counts.WithReleaseID,
		WithNZBReleaseID:     counts.WithNZBReleaseID,
		WithDerivedReleaseID: counts.WithDerivedReleaseID,
		FileIDOnly:           idOnly,
		NotIndexable:         skipped,
		NoSourceNZB:          counts.NoSourceGroup,
		GroupTooLarge:        tooLarge,
		Pending:              max(idOnly-counts.NoSourceGroup-tooLarge, 0),
	}
}
