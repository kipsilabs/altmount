package bundleimport

import (
	"context"
	"log/slog"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// reconcilePage is how many state rows one page of the start-up walk reads.
const reconcilePage = 500

// Reconcile makes the peer-import state match the library after a restart,
// and is safe to run at any time:
//
//   - A write that never finished (rows not committed: the process died
//     between the first row and the last step of Importer.write) is rolled back
//     whole: the files it wrote, its content-index entries and its rows are
//     removed, so a partially written release is never announced, published or
//     served. A file is removed only when it is the file the row describes.
//   - A row whose file no longer exists is dropped. A release left with no row
//     fires no announcement.
//   - A held queue item that no row refers to any more is released.
func (i *Importer) Reconcile(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "peer import reconcile stopped", "panic", r)
		}
	}()
	if i.cfg.State == nil {
		return
	}
	i.rollbackUncommitted(ctx)
	i.dropRowsWithoutFiles(ctx)
	i.releaseOrphanHolds(ctx)
}

func (i *Importer) rollbackUncommitted(ctx context.Context) {
	rows, err := i.cfg.State.ListUncommitted(ctx, 5000)
	if err != nil {
		slog.WarnContext(ctx, "could not list unfinished peer imports", "error", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	paths := make([]string, 0, len(rows))
	for _, r := range rows {
		paths = append(paths, r.FilePath)
		if i.ownsFile(r) {
			if err := i.cfg.Library.DeleteFileMetadata(ctx, r.FilePath); err != nil {
				slog.WarnContext(ctx, "could not remove a file of an unfinished peer import", "file_path", r.FilePath, "error", err)
			}
		}
		if i.cfg.Index != nil {
			_ = i.cfg.Index.Delete(ctx, r.FilePath)
		}
	}
	if err := i.cfg.State.Delete(ctx, paths); err != nil {
		slog.WarnContext(ctx, "could not drop the rows of an unfinished peer import", "error", err)
		return
	}
	slog.WarnContext(ctx, "rolled back peer imports that a restart interrupted", "files", len(paths))
}

// ownsFile reports whether the file at the row's path is the one the row
// describes (a missing file is not: nothing to remove).
func (i *Importer) ownsFile(r database.PeernetBundleImport) bool {
	if !i.cfg.Library.FileExists(r.FilePath) {
		return false
	}
	m, err := i.cfg.Library.ReadFileMetadata(r.FilePath)
	if err != nil || m == nil {
		// Unreadable: it can only be the half-written file of this import.
		return true
	}
	id, err := contentid.FileID(m)
	return err == nil && id.String() == r.FileID
}

func (i *Importer) dropRowsWithoutFiles(ctx context.Context) {
	var gone []string
	after := ""
	for {
		rows, err := i.cfg.State.ListAfter(ctx, after, reconcilePage)
		if err != nil {
			slog.WarnContext(ctx, "could not walk the peer import state", "error", err)
			return
		}
		for _, r := range rows {
			if !i.cfg.Library.FileExists(r.FilePath) {
				gone = append(gone, r.FilePath)
			}
		}
		if len(rows) < reconcilePage {
			break
		}
		after = rows[len(rows)-1].FilePath
	}
	if len(gone) == 0 {
		return
	}
	if err := i.cfg.State.Delete(ctx, gone); err != nil {
		slog.WarnContext(ctx, "could not drop peer import rows whose files are gone", "error", err)
	}
}
