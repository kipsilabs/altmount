package contentid

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// failureLogEvery is the shortest gap between two logged indexing failures.
// Failures in between are counted and reported with the next line, so a broken
// store cannot flood the log (every import write would otherwise add a line).
const failureLogEvery = time.Minute

// failureLog rate-limits a warning.
type failureLog struct {
	mu         sync.Mutex
	now        func() time.Time
	last       time.Time
	suppressed int
}

func (l *failureLog) warn(ctx context.Context, msg, path string, err error) {
	l.mu.Lock()
	now := time.Now
	if l.now != nil {
		now = l.now
	}
	t := now()
	if !l.last.IsZero() && t.Sub(l.last) < failureLogEvery {
		l.suppressed++
		l.mu.Unlock()
		return
	}
	suppressed := l.suppressed
	l.suppressed, l.last = 0, t
	l.mu.Unlock()
	slog.WarnContext(ctx, msg, "path", path, "error", err, "suppressed_since_last", suppressed)
}

// Observer adapts an Indexer to metadata.FileObserver so the index follows
// metadata writes, renames and deletions. It never reports an error: indexing
// is advisory and must not fail the operation that triggered it. It works
// synchronously; wrap it in an AsyncObserver to keep it off the caller's path.
type Observer struct {
	idx  *Indexer
	logs failureLog
}

// NewObserver returns an Observer feeding idx.
func NewObserver(idx *Indexer) *Observer { return &Observer{idx: idx} }

// FileWritten indexes a freshly written file.
func (o *Observer) FileWritten(ctx context.Context, virtualPath string) {
	err := o.idx.IndexPath(ctx, virtualPath)
	if err == nil || errors.Is(err, ErrMetadataNotFound) || errors.Is(err, context.Canceled) {
		// A file that vanished again before it was indexed is not a failure:
		// its delete event follows.
		return
	}
	o.logs.warn(ctx, "Peernet content indexing failed", virtualPath, err)
}

// FileDeleted drops the index row of a removed or moved file.
func (o *Observer) FileDeleted(ctx context.Context, virtualPath string) {
	if err := o.idx.repo.Delete(ctx, virtualPath); err != nil && !errors.Is(err, context.Canceled) {
		o.logs.warn(ctx, "Peernet content index delete failed", virtualPath, err)
	}
}

// DirectoryDeleted drops every index row beneath a removed directory.
func (o *Observer) DirectoryDeleted(ctx context.Context, virtualPath string) {
	if err := o.idx.repo.DeleteByPrefix(ctx, virtualPath); err != nil && !errors.Is(err, context.Canceled) {
		o.logs.warn(ctx, "Peernet content index directory delete failed", virtualPath, err)
	}
}

// DirectoryRenamed moves the index rows of a renamed directory; the
// identifiers are unchanged because a rename does not change content.
func (o *Observer) DirectoryRenamed(ctx context.Context, oldVirtualPath, newVirtualPath string) {
	if err := o.idx.repo.RenamePrefix(ctx, oldVirtualPath, newVirtualPath); err != nil && !errors.Is(err, context.Canceled) {
		o.logs.warn(ctx, "Peernet content index directory rename failed", oldVirtualPath, err)
	}
}
