-- +goose Up
-- The signed coverage of a health attestation: how many segments the publisher
-- checked out of how many the file has. Rows stored before this migration (and
-- release records) carry 0/0, meaning unknown.
ALTER TABLE peernet_records ADD COLUMN segments_checked BIGINT NOT NULL DEFAULT 0;
ALTER TABLE peernet_records ADD COLUMN total_segments BIGINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE peernet_records DROP COLUMN total_segments;
ALTER TABLE peernet_records DROP COLUMN segments_checked;
