-- +goose Up
-- connected_ip is the remote IP this node actually connected to when it last
-- verified the peer (a completed mutual-TLS contact). Corroboration counts
-- distinct /24 (IPv4) or /48 (IPv6) ranges of these addresses, never of the
-- stored address string, which may be a host name: two names for one server
-- are one range. A peer with no recorded connected IP does not count.
ALTER TABLE peernet_peers ADD COLUMN connected_ip TEXT NOT NULL DEFAULT '';

-- bundle_id is the Bundle ID of the bundle an import was accepted from, so a
-- later failed full check can blacklist exactly that layout.
ALTER TABLE peernet_bundle_imports ADD COLUMN bundle_id TEXT NOT NULL DEFAULT '';

-- Bundle IDs whose layout failed local verification, kept for a while so the
-- same bad layout offered by another publisher is not accepted again. Rows
-- are pruned after their expiry (7 days). Persisted: a failure surfaces up to
-- a week after the import, across restarts. failed_at is Unix seconds.
CREATE TABLE peernet_failed_bundles (
    bundle_id TEXT NOT NULL PRIMARY KEY,
    release_id TEXT NOT NULL,
    failed_at INTEGER NOT NULL
);
CREATE INDEX idx_peernet_failed_bundles_failed_at ON peernet_failed_bundles(failed_at);

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_failed_bundles_failed_at;
DROP TABLE IF EXISTS peernet_failed_bundles;
ALTER TABLE peernet_bundle_imports DROP COLUMN bundle_id;
ALTER TABLE peernet_peers DROP COLUMN connected_ip;
