package contentid

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"

	"github.com/kipsilabs/altmount/internal/database"
)

// corruptedMetadataDir is the top-level safety folder for quarantined
// metadata; it is not part of the library and is never indexed.
const corruptedMetadataDir = "corrupted_metadata"

// progressEvery is how many indexed files separate two progress log lines.
const progressEvery = 5000

// BackfillResult summarises one Backfill run.
type BackfillResult struct {
	Scanned int // metadata files seen
	Indexed int // rows written
	Skipped int // files that could not be identified (no real segments, unreadable metadata)
}

// Backfill walks the metadata tree and indexes every file that has no row yet.
//
// It works in batches of the configured size, so memory stays bounded, and it
// is safe to interrupt and to run again: rows written so far are kept, and a
// later run only does the remainder. On cancellation the work gathered for the
// current batch is flushed first and ctx.Err() is returned with the partial
// result. A repository failure stops the run; a file that cannot be identified
// is counted as skipped and retried on the next run.
func (i *Indexer) Backfill(ctx context.Context) (BackfillResult, error) {
	var res BackfillResult
	var nextLog = progressEvery
	batch := make([]string, 0, i.batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		paths := batch
		batch = make([]string, 0, i.batchSize)
		res.Scanned += len(paths)

		missing, err := i.repo.MissingPaths(ctx, paths)
		if err != nil {
			return fmt.Errorf("find unindexed paths: %w", err)
		}
		entries := make([]database.PeernetContentEntry, 0, len(missing))
		var stop error
		for _, p := range missing {
			e, err := i.Entry(ctx, p)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					stop = ctxErr
					break
				}
				res.Skipped++
				continue
			}
			entries = append(entries, e)
		}
		// Persist what we have even when cancelled mid-batch.
		if err := i.repo.UpsertBatch(context.WithoutCancel(ctx), entries); err != nil {
			return fmt.Errorf("write index batch: %w", err)
		}
		res.Indexed += len(entries)
		if i.progress != nil {
			i.progress(res)
		}
		if res.Indexed >= nextLog {
			slog.InfoContext(ctx, "Peernet content index backfill progress", "indexed", res.Indexed)
			nextLog = res.Indexed + progressEvery
		}
		return stop
	}

	walkErr := i.walk(ctx, "", func(p string) error {
		batch = append(batch, p)
		if len(batch) >= i.batchSize {
			return flush()
		}
		return nil
	})
	if walkErr == nil {
		walkErr = flush()
	} else if errors.Is(walkErr, context.Canceled) || errors.Is(walkErr, context.DeadlineExceeded) {
		_ = flush() // best effort; result already carries cancellation
	}

	if walkErr != nil {
		return res, walkErr
	}
	if res.Indexed > 0 || res.Skipped > 0 {
		slog.InfoContext(ctx, "Peernet content index backfill finished",
			"scanned", res.Scanned, "indexed", res.Indexed, "skipped", res.Skipped)
	}
	return res, nil
}

// walk visits every metadata file under dir depth-first, in name order.
func (i *Indexer) walk(ctx context.Context, dir string, visit func(string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dirs, files, err := i.src.ListDirectoryAll(dir)
	if err != nil {
		return fmt.Errorf("list %q: %w", dir, err)
	}
	for _, name := range files {
		if err := visit(path.Join(dir, name)); err != nil {
			return err
		}
	}
	for _, d := range dirs {
		if dir == "" && d.Name() == corruptedMetadataDir {
			continue
		}
		if err := i.walk(ctx, path.Join(dir, d.Name()), visit); err != nil {
			return err
		}
	}
	return nil
}
