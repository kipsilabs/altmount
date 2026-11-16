-- +goose Up
-- How each indexed file's release_id was derived: 0 none, 1 "nzb" (hash of the
-- release's NZB segment list), 2 "files" (derived from the File IDs of the
-- files of one import, for metadata that predates the NZB store). Existing
-- rows with a release_id came from a store, so they are scheme 1.
ALTER TABLE peernet_content_index ADD COLUMN release_scheme INTEGER NOT NULL DEFAULT 0;
-- A local-only grouping key (hash of the metadata's source NZB path) that
-- says which indexed files came from one import. It is never transmitted.
ALTER TABLE peernet_content_index ADD COLUMN group_key TEXT NOT NULL DEFAULT '';
UPDATE peernet_content_index SET release_scheme = 1 WHERE release_id <> '';
CREATE INDEX idx_peernet_content_group ON peernet_content_index(group_key) WHERE group_key <> '';

-- Which scheme a stored record's release_id uses (1 nzb, 2 files). Records
-- stored before this migration are scheme 1.
ALTER TABLE peernet_records ADD COLUMN release_scheme INTEGER NOT NULL DEFAULT 1;
-- Health evidence is matched by file ID across release IDs.
CREATE INDEX idx_peernet_records_file ON peernet_records(network_id, file_id, kind);

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_records_file;
ALTER TABLE peernet_records DROP COLUMN release_scheme;
DROP INDEX IF EXISTS idx_peernet_content_group;
ALTER TABLE peernet_content_index DROP COLUMN group_key;
ALTER TABLE peernet_content_index DROP COLUMN release_scheme;
