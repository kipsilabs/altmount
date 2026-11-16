package contentid

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/kipsilabs/altmount/internal/database"
)

// ReconcileResult summarises one Reconcile run.
type ReconcileResult struct {
	Checked   int // index rows examined
	Pruned    int // rows removed because their metadata is gone (or unidentifiable)
	Reindexed int // rows rewritten because their metadata changed
	Skipped   int // rows left alone because their metadata could not be read
}

// Reconcile brings existing index rows in line with the metadata tree. Backfill
// only adds missing rows; Reconcile removes the rows of files that no longer
// exist and re-indexes the rows of files whose metadata changed, which is what
// happens to anything changed while the feature was off or while an observer
// event was dropped.
//
// The change signal is the file ID itself, recomputed from the resolved
// metadata: it is derived from the segment message-IDs and structure only, so
// it moves exactly when the content identity moves and not on status, hole or
// timestamp updates. (The .meta mtime is no signal: it is pinned to the
// proto's ModifiedAt.) That costs one metadata read per row and no store read:
// the release ID is recomputed only for rows whose file ID changed or whose
// release ID presence no longer matches the metadata's store reference. A
// store rewritten under the same reference changes the resolved segments and
// therefore the file ID, so it is caught too.
//
// The scan is a keyset walk of the index in batches of the configured size, so
// memory stays bounded, rows may change underneath it, and cancellation is
// honoured between rows; rows handled so far are kept.
func (i *Indexer) Reconcile(ctx context.Context) (ReconcileResult, error) {
	var res ReconcileResult
	after := ""
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		page, err := i.repo.ListPage(ctx, after, i.batchSize)
		if err != nil {
			return res, fmt.Errorf("list index page: %w", err)
		}
		if len(page) == 0 {
			break
		}
		after = page[len(page)-1].FilePath
		upserts := make([]database.PeernetContentEntry, 0, len(page))
		for _, row := range page {
			if err := ctx.Err(); err != nil {
				i.flushReconcile(ctx, upserts, &res)
				return res, err
			}
			res.Checked++
			act, entry, err := i.reconcileRow(ctx, row)
			if err != nil {
				if ctx.Err() != nil {
					i.flushReconcile(ctx, upserts, &res)
					return res, ctx.Err()
				}
				return res, err
			}
			switch act {
			case actPrune:
				res.Pruned++
			case actReindex:
				upserts = append(upserts, entry)
			case actSkip:
				res.Skipped++
			}
		}
		if err := i.flushReconcile(ctx, upserts, &res); err != nil {
			return res, err
		}
	}
	if res.Pruned > 0 || res.Reindexed > 0 {
		slog.InfoContext(ctx, "Peernet content index reconciled",
			"checked", res.Checked, "pruned", res.Pruned, "reindexed", res.Reindexed)
	}
	return res, nil
}

func (i *Indexer) flushReconcile(ctx context.Context, upserts []database.PeernetContentEntry, res *ReconcileResult) error {
	if len(upserts) == 0 {
		return nil
	}
	if err := i.repo.UpsertBatch(context.WithoutCancel(ctx), upserts); err != nil {
		return fmt.Errorf("write reindexed rows: %w", err)
	}
	res.Reindexed += len(upserts)
	return nil
}

type reconcileAction int

const (
	actKeep reconcileAction = iota
	actPrune
	actReindex
	actSkip
)

// reconcileRow decides what to do with one index row; a prune is already done
// when it returns.
func (i *Indexer) reconcileRow(ctx context.Context, row database.PeernetContentEntry) (reconcileAction, database.PeernetContentEntry, error) {
	var zero database.PeernetContentEntry
	prune := func() (reconcileAction, database.PeernetContentEntry, error) {
		if err := i.repo.Delete(ctx, row.FilePath); err != nil {
			return actKeep, zero, fmt.Errorf("prune %q: %w", row.FilePath, err)
		}
		return actPrune, zero, nil
	}
	if strings.HasPrefix(row.FilePath, corruptedMetadataDir+"/") {
		return prune() // quarantined metadata is never indexed
	}
	m, err := i.src.ReadFileMetadata(row.FilePath)
	if err != nil {
		return actSkip, zero, nil // unreadable now: leave it for a later run
	}
	if m == nil {
		return prune()
	}
	fid, err := FileID(m)
	switch {
	case errors.Is(err, ErrNoSegments):
		return prune() // nothing real left to identify
	case err != nil:
		return actSkip, zero, nil
	}
	if fid.String() == row.FileID && (m.StoreRef != "" && !migratedStore(m.StoreRef)) == (row.ReleaseID != "") {
		return actKeep, zero, nil
	}
	entry, err := i.Entry(ctx, row.FilePath)
	if err != nil {
		if ctx.Err() != nil {
			return actKeep, zero, ctx.Err()
		}
		return actSkip, zero, nil
	}
	if entry.FileID == row.FileID && entry.ReleaseID == row.ReleaseID {
		return actKeep, zero, nil // e.g. a store with no real article keeps an empty release ID
	}
	return actReindex, entry, nil
}
