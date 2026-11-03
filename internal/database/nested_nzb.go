package database

import (
	"context"
	"fmt"
)

// ExpandNzbWrapper continues the first release in the original job and publishes
// its siblings atomically. Comparing the source path makes expansion idempotent.
func (r *QueueRepository) ExpandNzbWrapper(ctx context.Context, parent *ImportQueueItem, children []*ImportQueueItem) error {
	if len(children) < 2 {
		return fmt.Errorf("NZB wrapper must contain multiple children")
	}
	return r.withQueueTransaction(ctx, func(tx *QueueRepository) error {
		first := children[0]
		result, err := tx.db.ExecContext(ctx, `UPDATE import_queue SET nzb_path = ?, metadata = ?, file_size = ?,
   status = 'processing', retry_count = 0, storage_path = NULL, completed_at = NULL,
   updated_at = datetime('now'), error_message = NULL WHERE id = ? AND nzb_path = ?`,
			first.NzbPath, first.Metadata, first.FileSize, parent.ID, parent.NzbPath)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			var id int64
			return tx.db.QueryRowContext(ctx, `SELECT id FROM import_queue WHERE id = ?`, parent.ID).Scan(&id)
		}
		first.ID = parent.ID
		for _, child := range children[1:] {
			if err := tx.AddToQueue(ctx, child); err != nil {
				return err
			}
		}
		return nil
	})
}
