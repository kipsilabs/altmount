package bundleimport

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// renameGrace is how long a deleted file's row waits for the matching write
// (the metadata service reports a rename as a delete followed by a write).
const renameGrace = 3 * time.Second

// PathObserver keeps the peer-bundle state consistent with the library when
// the user deletes, moves or renames a file or folder: a deleted file loses its
// row (so a release with no files left is never announced), and a renamed file
// or folder keeps its row, state included, so a rename cannot lift the hold.
// It implements metadata.FileObserver and is meant to be a secondary observer.
type PathObserver struct {
	i *Importer

	mu    sync.Mutex
	stash map[string]stashedRow // by File ID
}

type stashedRow struct {
	path  string
	timer *time.Timer
}

// PathObserver returns the observer of this importer.
func (i *Importer) PathObserver() *PathObserver {
	return &PathObserver{i: i, stash: map[string]stashedRow{}}
}

// FileWritten re-attaches a row to a file that appeared under a new name.
func (o *PathObserver) FileWritten(ctx context.Context, path string) {
	o.mu.Lock()
	n := len(o.stash)
	o.mu.Unlock()
	if n == 0 || o.i.cfg.State == nil {
		return
	}
	m, err := o.i.cfg.Library.ReadFileMetadata(path)
	if err != nil || m == nil {
		return
	}
	id, err := contentid.FileID(m)
	if err != nil {
		return
	}
	o.mu.Lock()
	s, ok := o.stash[id.String()]
	if ok {
		s.timer.Stop()
		delete(o.stash, id.String())
	}
	o.mu.Unlock()
	if !ok || s.path == path {
		return
	}
	if _, err := o.i.cfg.State.MoveFile(ctx, s.path, path); err != nil {
		slog.WarnContext(ctx, "peer bundle state could not follow a renamed file", "error", err)
	}
}

// FileDeleted holds the row for a moment: a rename arrives as delete + write.
func (o *PathObserver) FileDeleted(ctx context.Context, path string) {
	if o.i.cfg.State == nil {
		return
	}
	row, ok, err := o.i.cfg.State.Get(ctx, path)
	if err != nil || !ok {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if old, dup := o.stash[row.FileID]; dup {
		old.timer.Stop()
	}
	bg := context.WithoutCancel(ctx)
	o.stash[row.FileID] = stashedRow{path: path, timer: time.AfterFunc(renameGrace, func() { o.expire(bg, row.FileID, path) })}
}

func (o *PathObserver) expire(ctx context.Context, fileID, path string) {
	o.mu.Lock()
	s, ok := o.stash[fileID]
	if ok && s.path == path {
		delete(o.stash, fileID)
	} else {
		ok = false
	}
	o.mu.Unlock()
	if ok {
		_ = o.i.cfg.State.Delete(ctx, []string{path})
		o.i.releaseOrphanHolds(ctx)
	}
}

// Flush settles every held row now (rows nobody claimed are deleted). Tests and
// shutdown use it.
func (o *PathObserver) Flush() {
	o.mu.Lock()
	pending := o.stash
	o.stash = map[string]stashedRow{}
	o.mu.Unlock()
	for _, s := range pending {
		s.timer.Stop()
		_ = o.i.cfg.State.Delete(context.Background(), []string{s.path})
	}
	if len(pending) > 0 {
		o.i.releaseOrphanHolds(context.Background())
	}
}

// DirectoryDeleted removes the rows below a deleted folder.
func (o *PathObserver) DirectoryDeleted(ctx context.Context, path string) {
	if o.i.cfg.State == nil {
		return
	}
	if err := o.i.cfg.State.DeleteDir(ctx, path); err != nil {
		slog.WarnContext(ctx, "peer bundle state could not drop a deleted folder", "error", err)
		return
	}
	o.i.releaseOrphanHolds(ctx)
}

// DirectoryRenamed moves the rows below a folder with it.
func (o *PathObserver) DirectoryRenamed(ctx context.Context, from, to string) {
	if o.i.cfg.State == nil {
		return
	}
	if err := o.i.cfg.State.MoveDir(ctx, from, to); err != nil {
		slog.WarnContext(ctx, "peer bundle state could not follow a renamed folder", "error", err)
	}
}
