-- +goose Up
CREATE TABLE rss_import_receipts (
    entry_id TEXT PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- +goose Down
DROP TABLE rss_import_receipts;
