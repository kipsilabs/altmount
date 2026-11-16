-- +goose Up
CREATE TABLE peernet_networks (
    network_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL CHECK (role IN ('host', 'member', 'open')),
    exposure_level INTEGER NOT NULL DEFAULT 0 CHECK (exposure_level BETWEEN 0 AND 3),
    allow_private_addresses INTEGER NOT NULL DEFAULT 0 CHECK (allow_private_addresses IN (0, 1)),
    host_address TEXT NOT NULL DEFAULT '',
    host_node_id TEXT NOT NULL DEFAULT '',
    root_public_key BLOB,
    own_certificate BLOB,
    revocation_list BLOB,
    revocation_seq BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE peernet_invites (
    network_id TEXT NOT NULL REFERENCES peernet_networks(network_id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL,
    alias TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    used_at BIGINT,
    used_by TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (network_id, code_hash)
);

CREATE TABLE peernet_members (
    network_id TEXT NOT NULL REFERENCES peernet_networks(network_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    alias TEXT NOT NULL DEFAULT '',
    joined_at BIGINT NOT NULL,
    cert_issued_at BIGINT NOT NULL,
    cert_expires_at BIGINT NOT NULL,
    revoked_at BIGINT,
    PRIMARY KEY (network_id, node_id)
);

-- +goose Down
DROP TABLE IF EXISTS peernet_members;
DROP TABLE IF EXISTS peernet_invites;
DROP TABLE IF EXISTS peernet_networks;
