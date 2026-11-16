-- +goose Up
-- PeernetOriginRepository.OriginForPath filters import_history by
-- virtual_path (up to six lookups per published release); without an index it
-- scanned the whole history each time.
CREATE INDEX IF NOT EXISTS idx_import_history_virtual_path ON import_history(virtual_path);

-- +goose Down
DROP INDEX IF EXISTS idx_import_history_virtual_path;
