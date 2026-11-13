-- +goose Up
ALTER TABLE peernet_networks ADD COLUMN revoked_at BIGINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE peernet_networks DROP COLUMN revoked_at;
