package database

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/javi11/altmount/internal/availability"
)

const maxAvailabilityFactAge = 7 * 24 * time.Hour

// AvailabilityFact is a definitive provider-pool-scoped availability fact.
type AvailabilityFact struct {
	Scope          availability.Scope
	ManifestDigest string
	FactKind       string
	ArticleDigest  string
	Outcome        string
	ArticleCount   int
	ObservedAt     time.Time
	ExpiresAt      time.Time
}

// AvailabilityRepository persists definitive availability facts.
type AvailabilityRepository struct {
	db      *dialectAwareDB
	dialect dialectHelper
}

// NewAvailabilityRepository creates a repository for the given database dialect.
func NewAvailabilityRepository(db *sql.DB, d Dialect) *AvailabilityRepository {
	return &AvailabilityRepository{
		db:      newDialectAwareDB(db, d),
		dialect: dialectHelper{d: d},
	}
}

// UpsertManifestSummary records the currently observed manifest summary.
func (r *AvailabilityRepository) UpsertManifestSummary(ctx context.Context, scope availability.Scope, manifestDigest string, articleCount int, observedAt, expiresAt time.Time) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateScope(scope); err != nil {
		return err
	}
	if err := validateDigest("manifest digest", manifestDigest); err != nil {
		return err
	}
	if articleCount <= 0 {
		return fmt.Errorf("article count must be positive")
	}
	if err := validateExpiry(observedAt, expiresAt); err != nil {
		return err
	}

	const query = `
		INSERT INTO availability_facts
			(scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at)
		VALUES (?, ?, ?, 'manifest', '', 'present', ?, ?, ?)
		ON CONFLICT (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest) DO UPDATE SET
			observed_at = excluded.observed_at,
			expires_at = excluded.expires_at,
			article_count = excluded.article_count,
			updated_at = datetime('now')
	`
	if _, err := r.db.ExecContext(ctx, query, scope.Digest, scope.Generation, manifestDigest, articleCount, observedAt.UTC(), expiresAt.UTC()); err != nil {
		return fmt.Errorf("upsert manifest summary: %w", err)
	}
	return nil
}

// UpsertConfirmedMissing records one article confirmed missing from the pool.
func (r *AvailabilityRepository) UpsertConfirmedMissing(ctx context.Context, scope availability.Scope, manifestDigest, articleDigest string, observedAt, expiresAt time.Time) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateScope(scope); err != nil {
		return err
	}
	if err := validateDigest("manifest digest", manifestDigest); err != nil {
		return err
	}
	if err := validateDigest("article digest", articleDigest); err != nil {
		return err
	}
	if err := validateExpiry(observedAt, expiresAt); err != nil {
		return err
	}

	const query = `
		INSERT INTO availability_facts
			(scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at)
		VALUES (?, ?, ?, 'article', ?, 'confirmed_missing', 0, ?, ?)
		ON CONFLICT (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest) DO UPDATE SET
			observed_at = excluded.observed_at,
			expires_at = excluded.expires_at,
			article_count = excluded.article_count,
			updated_at = datetime('now')
	`
	if _, err := r.db.ExecContext(ctx, query, scope.Digest, scope.Generation, manifestDigest, articleDigest, observedAt.UTC(), expiresAt.UTC()); err != nil {
		return fmt.Errorf("upsert confirmed missing article: %w", err)
	}
	return nil
}

// GetManifestSummary returns an unexpired manifest summary, or nil when absent.
func (r *AvailabilityRepository) GetManifestSummary(ctx context.Context, scope availability.Scope, manifestDigest string, at time.Time) (*AvailabilityFact, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	if err := validateDigest("manifest digest", manifestDigest); err != nil {
		return nil, err
	}

	const query = `
		SELECT scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at
		FROM availability_facts
		WHERE scope_digest = ? AND scope_generation = ? AND manifest_digest = ?
			AND fact_kind = 'manifest' AND expires_at > ?
	`
	fact, err := scanAvailabilityFact(r.db.QueryRowContext(ctx, query, scope.Digest, scope.Generation, manifestDigest, at.UTC()))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get manifest summary: %w", err)
	}
	return fact, nil
}

// ListConfirmedMissing returns unexpired confirmed-missing article facts.
func (r *AvailabilityRepository) ListConfirmedMissing(ctx context.Context, scope availability.Scope, manifestDigest string, at time.Time) ([]AvailabilityFact, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	if err := validateDigest("manifest digest", manifestDigest); err != nil {
		return nil, err
	}

	const query = `
		SELECT scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at
		FROM availability_facts
		WHERE scope_digest = ? AND scope_generation = ? AND manifest_digest = ?
			AND fact_kind = 'article' AND expires_at > ?
		ORDER BY article_digest
	`
	rows, err := r.db.QueryContext(ctx, query, scope.Digest, scope.Generation, manifestDigest, at.UTC())
	if err != nil {
		return nil, fmt.Errorf("list confirmed missing articles: %w", err)
	}
	defer rows.Close()

	var facts []AvailabilityFact
	for rows.Next() {
		fact, err := scanAvailabilityFact(rows)
		if err != nil {
			return nil, fmt.Errorf("scan confirmed missing article: %w", err)
		}
		facts = append(facts, *fact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list confirmed missing articles: %w", err)
	}
	return facts, nil
}

// DeleteExpired removes at most limit expired facts. A non-positive limit is a no-op.
func (r *AvailabilityRepository) DeleteExpired(ctx context.Context, at time.Time, limit int) (int64, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, nil
	}
	const query = `
		DELETE FROM availability_facts
		WHERE id IN (
			SELECT id FROM availability_facts
			WHERE expires_at <= ?
			ORDER BY expires_at, id
			LIMIT ?
		)
	`
	result, err := r.db.ExecContext(ctx, query, at.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete expired availability facts: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count deleted availability facts: %w", err)
	}
	return deleted, nil
}

func scanAvailabilityFact(scanner rowScanner) (*AvailabilityFact, error) {
	var fact AvailabilityFact
	err := scanner.Scan(
		&fact.Scope.Digest,
		&fact.Scope.Generation,
		&fact.ManifestDigest,
		&fact.FactKind,
		&fact.ArticleDigest,
		&fact.Outcome,
		&fact.ArticleCount,
		&fact.ObservedAt,
		&fact.ExpiresAt,
	)
	if err != nil {
		return nil, err
	}
	return &fact, nil
}

func validateContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func validateScope(scope availability.Scope) error {
	if err := validateDigest("scope digest", scope.Digest); err != nil {
		return err
	}
	if len(scope.Generation) != 32 {
		return fmt.Errorf("scope generation must be 32 hexadecimal characters")
	}
	if _, err := hex.DecodeString(scope.Generation); err != nil {
		return fmt.Errorf("scope generation must be hexadecimal: %w", err)
	}
	return nil
}

func validateDigest(name, value string) error {
	if len(value) != 64 {
		return fmt.Errorf("%s must be a 64-character SHA-256 hex digest", name)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s must be hexadecimal: %w", name, err)
	}
	return nil
}

func validateExpiry(observedAt, expiresAt time.Time) error {
	if expiresAt.IsZero() || observedAt.IsZero() || !expiresAt.After(observedAt) {
		return fmt.Errorf("expiry must be after observation")
	}
	if expiresAt.Sub(observedAt) > maxAvailabilityFactAge {
		return fmt.Errorf("expiry window exceeds seven days")
	}
	return nil
}
