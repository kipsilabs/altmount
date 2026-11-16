-- +goose Up
-- Per (network, peer) sync positions. pulled_seq is a sequence number local to
-- the remote peer; pushed_seq is local to this instance. Never timestamps.
CREATE TABLE peernet_sync_cursors (
    network_id TEXT NOT NULL,
    peer_node_id TEXT NOT NULL,
    pulled_seq BIGINT NOT NULL DEFAULT 0,
    pushed_seq BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (network_id, peer_node_id)
);

-- +goose Down
DROP TABLE IF EXISTS peernet_sync_cursors;
