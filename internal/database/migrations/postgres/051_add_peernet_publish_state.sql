-- +goose Up
-- What this node last published to each network, per record kind, release and
-- file (file_id is empty for release records). It drives the republish
-- planner: an unchanged verdict is not published again inside the minimum
-- interval, and a release record is announced again before it expires. Times
-- are Unix seconds. Rows go with their network and with releases that have left
-- the content index.
CREATE TABLE peernet_publish_state (
    network_id TEXT NOT NULL,
    kind INTEGER NOT NULL,
    release_id TEXT NOT NULL,
    file_id TEXT NOT NULL DEFAULT '',
    published_at BIGINT NOT NULL,
    checked_at BIGINT NOT NULL DEFAULT 0,
    verdict INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (network_id, kind, release_id, file_id)
);
CREATE INDEX idx_peernet_publish_state_release ON peernet_publish_state(release_id);
CREATE INDEX idx_peernet_publish_state_due ON peernet_publish_state(kind, published_at);

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_publish_state_due;
DROP INDEX IF EXISTS idx_peernet_publish_state_release;
DROP TABLE IF EXISTS peernet_publish_state;
