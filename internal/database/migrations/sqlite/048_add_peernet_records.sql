-- +goose Up
-- Signed peer-network records (release records and health attestations).
-- Times are unix seconds. seq is allocated from peernet_record_counters inside
-- the insert transaction (see peernet_record_repository.go), never reused.
CREATE TABLE peernet_records (
    network_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind IN (1, 2)),
    content_hash TEXT NOT NULL,
    release_id TEXT NOT NULL,
    file_id TEXT NOT NULL DEFAULT '',
    publisher_node_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    received_at BIGINT NOT NULL,
    source INTEGER NOT NULL CHECK (source IN (0, 1)),
    healthy INTEGER CHECK (healthy IN (0, 1)),
    backbones TEXT NOT NULL DEFAULT '',
    checked_at BIGINT NOT NULL DEFAULT 0,
    imdb_id TEXT NOT NULL DEFAULT '',
    season INTEGER,
    episode INTEGER,
    title TEXT NOT NULL DEFAULT '',
    total_size BIGINT NOT NULL DEFAULT 0,
    payload BLOB NOT NULL CHECK (length(payload) <= 2048),
    PRIMARY KEY (network_id, seq)
);
CREATE UNIQUE INDEX idx_peernet_records_hash ON peernet_records(network_id, content_hash);
CREATE UNIQUE INDEX idx_peernet_records_key ON peernet_records(network_id, publisher_node_id, kind, release_id, file_id);
CREATE INDEX idx_peernet_records_release ON peernet_records(network_id, release_id, kind);
CREATE INDEX idx_peernet_records_media ON peernet_records(network_id, imdb_id, season, episode);
CREATE INDEX idx_peernet_records_publisher ON peernet_records(network_id, publisher_node_id);
CREATE INDEX idx_peernet_records_source ON peernet_records(network_id, source, seq);
CREATE INDEX idx_peernet_records_expires ON peernet_records(expires_at);

CREATE TABLE peernet_record_counters (
    network_id TEXT PRIMARY KEY,
    next_seq BIGINT NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS peernet_record_counters;
DROP INDEX IF EXISTS idx_peernet_records_expires;
DROP INDEX IF EXISTS idx_peernet_records_source;
DROP INDEX IF EXISTS idx_peernet_records_publisher;
DROP INDEX IF EXISTS idx_peernet_records_media;
DROP INDEX IF EXISTS idx_peernet_records_release;
DROP INDEX IF EXISTS idx_peernet_records_key;
DROP INDEX IF EXISTS idx_peernet_records_hash;
DROP TABLE IF EXISTS peernet_records;
