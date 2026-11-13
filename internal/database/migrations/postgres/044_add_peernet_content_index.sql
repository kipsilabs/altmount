-- +goose Up
CREATE TABLE peernet_content_index (
    file_path TEXT PRIMARY KEY,
    file_id TEXT NOT NULL,
    release_id TEXT NOT NULL DEFAULT '',
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_peernet_content_file_id ON peernet_content_index(file_id);
CREATE INDEX idx_peernet_content_release_id ON peernet_content_index(release_id);

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_content_release_id;
DROP INDEX IF EXISTS idx_peernet_content_file_id;
DROP TABLE IF EXISTS peernet_content_index;
