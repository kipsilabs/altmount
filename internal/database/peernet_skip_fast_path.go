package database

import (
	"context"
	"fmt"
)

// SetSkipFastPath marks (or unmarks) a queue item as having to skip the peer
// fast path on its next import attempt. It is set before a held peer import is
// put back for a normal import, so a restart between the requeue and the
// attempt cannot send the item through the same bundle again.
func (r *PeernetBundleImportRepository) SetSkipFastPath(ctx context.Context, queueID int64, skip bool) error {
	v := 0
	if skip {
		v = 1
	}
	if _, err := r.db.ExecContext(ctx, `UPDATE import_queue SET peer_skip_fast_path = ? WHERE id = ?`, v, queueID); err != nil {
		return fmt.Errorf("peernet bundle import: mark skip of the fast path: %w", err)
	}
	return nil
}

// ClaimSkipFastPath reports whether the queue item is marked to skip the peer
// fast path, and clears the mark, so the fast path is skipped exactly once.
func (r *PeernetBundleImportRepository) ClaimSkipFastPath(ctx context.Context, queueID int64) (bool, error) {
	res, err := r.db.ExecContext(ctx, `UPDATE import_queue SET peer_skip_fast_path = 0 WHERE id = ? AND peer_skip_fast_path <> 0`, queueID)
	if err != nil {
		return false, fmt.Errorf("peernet bundle import: claim skip of the fast path: %w", err)
	}
	n, err := res.RowsAffected()
	return n > 0, err
}
