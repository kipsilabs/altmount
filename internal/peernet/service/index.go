package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

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
}

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
	c := &contentIndex{deps: deps, cancel: cancel, kick: make(chan struct{}, 1)}
	idx := contentid.NewIndexer(deps.Metadata, deps.Content,
		contentid.WithProgress(func(r contentid.BackfillResult) { c.store(r) }))
	obs := contentid.NewAsyncObserver(contentid.NewObserver(idx), 0, c.requestReindex)
	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		obs.Run(ctx)
	}()
	go c.worker(ctx, idx)
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
		c.running.Store(false)
		select {
		case <-ctx.Done():
			return
		case <-c.kick:
		}
	}
}

// LibraryStatus says how much of the library can be shared. A file can be
// shared when it has a Release ID; one with only a File ID was imported before
// the release's NZB was kept (or from a rebuilt store), and one that could not
// be identified at all is not indexable.
type LibraryStatus struct {
	// Files is every library file the index knows about plus those that could
	// not be identified.
	Files         int
	WithReleaseID int
	FileIDOnly    int
	NotIndexable  int
}

// library reads the index counts. It never fails the status: on a read error
// it reports what it has.
func (c *contentIndex) library(ctx context.Context) LibraryStatus {
	counts, err := c.deps.Content.Counts(ctx)
	if err != nil {
		slog.WarnContext(ctx, "Peernet library counts unavailable", "error", err)
	}
	skipped := int(c.skipped.Load())
	return LibraryStatus{
		Files:         counts.Files + skipped,
		WithReleaseID: counts.WithReleaseID,
		FileIDOnly:    counts.Files - counts.WithReleaseID,
		NotIndexable:  skipped,
	}
}
