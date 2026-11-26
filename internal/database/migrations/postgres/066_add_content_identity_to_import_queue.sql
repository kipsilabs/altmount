-- +goose Up
-- content_identity is the admission fingerprint of the release's largest
-- payload file: SHA-256 over its size and ordered Usenet message-IDs. It is
-- deliberately independent of the NZB path, name, URL, GUID and title so the
-- same release queued under different NZB names can be matched.
ALTER TABLE import_queue ADD COLUMN content_identity TEXT DEFAULT NULL;
CREATE INDEX idx_queue_content_identity ON import_queue(content_identity, file_size);

-- +goose Down
DROP INDEX IF EXISTS idx_queue_content_identity;
ALTER TABLE import_queue DROP COLUMN content_identity;
