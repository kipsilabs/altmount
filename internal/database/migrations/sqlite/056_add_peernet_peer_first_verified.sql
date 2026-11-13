-- +goose Up
-- Unix seconds of the first completed mutual-TLS contact that proved the
-- node ID; 0 = never verified. Set once, never reset by peer exchange.
ALTER TABLE peernet_peers ADD COLUMN first_verified BIGINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE peernet_peers DROP COLUMN first_verified;
