-- +goose Up
-- A random value per network identifying this node's record sequence space.
-- It is created on first use and removed with the network's records, so a
-- puller sees it change whenever the sequence restarts (leave and rejoin, or a
-- database reset) and resets its cursor.
CREATE TABLE peernet_record_epochs (
    network_id TEXT PRIMARY KEY,
    epoch TEXT NOT NULL
);

-- The epoch a peer last reported for its own sequence space, stored beside the
-- pull cursor that is only meaningful within it.
ALTER TABLE peernet_sync_cursors ADD COLUMN remote_epoch TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE peernet_sync_cursors DROP COLUMN remote_epoch;
DROP TABLE IF EXISTS peernet_record_epochs;
