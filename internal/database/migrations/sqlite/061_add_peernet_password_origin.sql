-- +goose Up
-- Files whose archive password came with the NZB's own content (a meta
-- password present in the NZB as received, not one the user typed, an ARR
-- supplied or the NZB name spelled). A row exists only for such files; a file
-- with no row is "not from the NZB", which is also what every file imported
-- before this table existed is. It is keyed by File ID (derived from the
-- file's segments), so a rename or move of the file changes nothing. The
-- password itself is never stored. Times are Unix seconds.
CREATE TABLE peernet_password_origin (
    file_id TEXT NOT NULL PRIMARY KEY,
    recorded_at BIGINT NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS peernet_password_origin;
