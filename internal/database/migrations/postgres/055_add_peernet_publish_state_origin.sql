-- +goose Up
-- A private, never-transmitted tag of where a published release came from
-- (lower-cased category and indexer at publish time). Records do not carry
-- either, so this is what lets upkeep retract a node's own records for a
-- release that a later-added exclusion now covers. Rows written before this
-- migration have empty tags and cannot be matched.
ALTER TABLE peernet_publish_state ADD COLUMN origin_category TEXT NOT NULL DEFAULT '';
ALTER TABLE peernet_publish_state ADD COLUMN origin_indexer TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_peernet_publish_state_origin_category ON peernet_publish_state(origin_category);
CREATE INDEX idx_peernet_publish_state_origin_indexer ON peernet_publish_state(origin_indexer);

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_publish_state_origin_indexer;
DROP INDEX IF EXISTS idx_peernet_publish_state_origin_category;
ALTER TABLE peernet_publish_state DROP COLUMN origin_indexer;
ALTER TABLE peernet_publish_state DROP COLUMN origin_category;
