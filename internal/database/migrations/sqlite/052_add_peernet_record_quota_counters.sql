-- +goose Up
-- Record store flooding defences. relayer_node_id is the authenticated node a
-- peer-sourced record was pulled from or pushed by ('' for own records); the
-- relayer, not the publisher, is the identity a flood cannot fabricate freely.
-- peernet_record_counts keeps exact row counts so an insert never scans the
-- network: scope 1 = whole network (key ''), 2 = per publisher, 3 = per relayer
-- (peer-sourced rows only). They are maintained in the same transaction as
-- every insert and delete and re-derived periodically (see
-- PeernetRecordRepository.ReconcileCounts).
ALTER TABLE peernet_records ADD COLUMN relayer_node_id TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_peernet_records_relayer ON peernet_records(network_id, relayer_node_id, seq);
-- Finds a network's expired rows without sorting the whole network.
CREATE INDEX idx_peernet_records_net_expires ON peernet_records(network_id, expires_at, seq);

CREATE TABLE peernet_record_counts (
    network_id TEXT NOT NULL,
    scope INTEGER NOT NULL,
    key TEXT NOT NULL,
    n BIGINT NOT NULL,
    PRIMARY KEY (network_id, scope, key)
);
CREATE INDEX idx_peernet_record_counts_n ON peernet_record_counts(network_id, scope, n);

INSERT INTO peernet_record_counts (network_id, scope, key, n)
    SELECT network_id, 1, '', COUNT(*) FROM peernet_records GROUP BY network_id;
INSERT INTO peernet_record_counts (network_id, scope, key, n)
    SELECT network_id, 2, publisher_node_id, COUNT(*) FROM peernet_records GROUP BY network_id, publisher_node_id;
INSERT INTO peernet_record_counts (network_id, scope, key, n)
    SELECT network_id, 3, relayer_node_id, COUNT(*) FROM peernet_records WHERE source = 1 GROUP BY network_id, relayer_node_id;

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_record_counts_n;
DROP TABLE IF EXISTS peernet_record_counts;
DROP INDEX IF EXISTS idx_peernet_records_net_expires;
DROP INDEX IF EXISTS idx_peernet_records_relayer;
ALTER TABLE peernet_records DROP COLUMN relayer_node_id;
