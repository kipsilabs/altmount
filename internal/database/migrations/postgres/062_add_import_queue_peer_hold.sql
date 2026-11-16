-- +goose Up
-- peer_hold marks a queue item whose files came from a peer bundle and still
-- owe their full health check (1), as opposed to skip_arr_notification, which
-- only ever records what the user asked for. The SABnzbd-compatible API shows a
-- held item in its queue (never in its history) until the check ends: a pass
-- clears the mark, a failure turns the item into a failed one. 0 is every row
-- that never came from the peer network.
ALTER TABLE import_queue ADD COLUMN peer_hold SMALLINT NOT NULL DEFAULT 0;
CREATE INDEX idx_import_queue_peer_hold ON import_queue(peer_hold) WHERE peer_hold <> 0;

-- committed says a bundle import finished writing all its files: a row is
-- inserted uncommitted (0) before the first file is written and committed last,
-- so a crash in between is told apart at start-up and rolled back whole. Rows
-- that existed before this column are complete ones (default 1).
ALTER TABLE peernet_bundle_imports ADD COLUMN committed SMALLINT NOT NULL DEFAULT 1;

-- +goose Down
ALTER TABLE peernet_bundle_imports DROP COLUMN committed;
DROP INDEX IF EXISTS idx_import_queue_peer_hold;
ALTER TABLE import_queue DROP COLUMN peer_hold;
