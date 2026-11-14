-- +goose Up
-- Imports that came from a peer bundle, one row per virtual file. A bundle
-- import is playable at once but is held back from the ARRs and from the peer
-- network until a full health check of every file passed (state "pending"
-- becomes "passed"); a failed check marks it "failed". queue_id groups the
-- files of one import (0: none). arr_owed says the ARRs must be told when the
-- last file passes (the import was not suppressed by its caller);
-- arr_notified is set by the single winner of that notification. network_id and
-- publisher_id name the origin, kept only so a bad bundle can be reported;
-- they are never shown as a link. Times are Unix seconds.
CREATE TABLE peernet_bundle_imports (
    file_path TEXT NOT NULL PRIMARY KEY,
    file_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    network_id TEXT NOT NULL,
    publisher_id TEXT NOT NULL,
    queue_id BIGINT NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',
    arr_owed INTEGER NOT NULL DEFAULT 0,
    arr_notified INTEGER NOT NULL DEFAULT 0,
    category TEXT NOT NULL DEFAULT '',
    target_path TEXT NOT NULL DEFAULT '',
    resulting_path TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE INDEX idx_peernet_bundle_imports_queue ON peernet_bundle_imports(queue_id) WHERE queue_id <> 0;
CREATE INDEX idx_peernet_bundle_imports_release ON peernet_bundle_imports(release_id);

-- +goose Down
DROP INDEX IF EXISTS idx_peernet_bundle_imports_release;
DROP INDEX IF EXISTS idx_peernet_bundle_imports_queue;
DROP TABLE IF EXISTS peernet_bundle_imports;
