-- +goose Up
CREATE TABLE availability_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope_digest TEXT NOT NULL,
    scope_generation TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    fact_kind TEXT NOT NULL CHECK (fact_kind IN ('manifest', 'article')),
    article_digest TEXT NOT NULL DEFAULT '',
    outcome TEXT NOT NULL CHECK (outcome IN ('present', 'confirmed_missing')),
    article_count INTEGER NOT NULL DEFAULT 0,
    observed_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (fact_kind = 'manifest' AND outcome = 'present' AND article_digest = '' AND article_count > 0)
        OR
        (fact_kind = 'article' AND outcome = 'confirmed_missing' AND article_digest <> '' AND article_count = 0)
    ),
    UNIQUE (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest)
);

CREATE INDEX idx_availability_facts_expires_at ON availability_facts (expires_at);
CREATE INDEX idx_availability_facts_lookup ON availability_facts (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, expires_at);

-- +goose Down
DROP INDEX IF EXISTS idx_availability_facts_lookup;
DROP INDEX IF EXISTS idx_availability_facts_expires_at;
DROP TABLE IF EXISTS availability_facts;
