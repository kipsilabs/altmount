-- +goose Up
-- Known peers per peer network. Times are unix seconds (0 = unset).
-- first_seen is written once and never updated.
CREATE TABLE peernet_peers (
    network_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    address TEXT NOT NULL,
    source TEXT NOT NULL,
    verified INTEGER NOT NULL DEFAULT 0,
    first_seen BIGINT NOT NULL,
    last_seen BIGINT NOT NULL,
    last_success BIGINT NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    next_attempt BIGINT NOT NULL DEFAULT 0,
    candidate_address TEXT NOT NULL DEFAULT '',
    changed_node_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (network_id, node_id)
);
CREATE INDEX idx_peernet_peers_due ON peernet_peers(network_id, next_attempt);
CREATE INDEX idx_peernet_peers_address ON peernet_peers(network_id, address);

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_peers_address;
DROP INDEX IF EXISTS idx_peernet_peers_due;
DROP TABLE IF EXISTS peernet_peers;
