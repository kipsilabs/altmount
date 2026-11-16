-- +goose Up
-- The Bundle ID a stored release record announces (empty: the publisher serves
-- no bundle). It comes from the signed record. Offers for a release are found
-- through the existing (network, release, kind) index.
ALTER TABLE peernet_records ADD COLUMN bundle_id TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE peernet_records DROP COLUMN bundle_id;
