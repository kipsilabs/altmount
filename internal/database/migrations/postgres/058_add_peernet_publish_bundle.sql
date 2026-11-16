-- +goose Up
-- The Bundle ID this node announced in a release record, and the exposure
-- level the record was published at. A record published at level 3 carries a
-- Bundle ID (or none, when the release cannot be shared as a bundle); at level
-- 2 it never does. Comparing bundle_level with the network's current level is
-- how a level change is noticed and the record superseded. bundle_id is also
-- the key by which a bundle request is matched to an announcement: a node
-- serves only a Bundle ID it announced itself.
ALTER TABLE peernet_publish_state ADD COLUMN bundle_id TEXT NOT NULL DEFAULT '';
ALTER TABLE peernet_publish_state ADD COLUMN bundle_level INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_peernet_publish_state_bundle ON peernet_publish_state(network_id, bundle_id) WHERE bundle_id <> '';

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_publish_state_bundle;
ALTER TABLE peernet_publish_state DROP COLUMN bundle_level;
ALTER TABLE peernet_publish_state DROP COLUMN bundle_id;
