package database

import "context"

// HasRSSImport checks durable acknowledgements. Queue rows are checked by the
// poller because an incomplete enqueue can still point to a staging file.
func (db *DB) HasRSSImport(ctx context.Context, id string) (bool, error) {
	var found bool
	err := db.conn.QueryRowContext(ctx, db.dialect.q(`SELECT EXISTS (
 SELECT 1 FROM rss_import_receipts WHERE entry_id = ?
 UNION ALL SELECT 1 FROM import_history WHERE download_id = ?
 )`), id, id).Scan(&found)
	return found, err
}

func (db *DB) RecordRSSImport(ctx context.Context, id string) error {
	_, err := db.conn.ExecContext(ctx, db.dialect.q(`INSERT INTO rss_import_receipts (entry_id) VALUES (?) ON CONFLICT (entry_id) DO NOTHING`), id)
	return err
}

// RemoveIncompleteRSSImport removes only an unclaimed RSS enqueue. A concurrent
// claim makes this a no-op, so recovery cannot interrupt an active importer.
func (db *DB) RemoveIncompleteRSSImport(ctx context.Context, id string) (bool, error) {
	result, err := db.conn.ExecContext(ctx, db.dialect.q(`DELETE FROM import_queue WHERE download_id = ? AND status IN ('pending', 'failed')`), id)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}
