-- +goose Up
-- peer_skip_fast_path is set when a held peer import is put back for a normal
-- import: that import must not try the peer fast path again (it would take the
-- same bundle that just failed its full check). It is consumed (cleared) by the
-- first import attempt that reads it. Persisted so a restart between the
-- requeue and the attempt cannot loop.
ALTER TABLE import_queue ADD COLUMN peer_skip_fast_path SMALLINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE import_queue DROP COLUMN peer_skip_fast_path;
