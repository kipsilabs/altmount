package database

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func TestMigrationAvailabilitySchemaAndConstraints(t *testing.T) {
	ctx := context.Background()
	db := openMigratedTo(t, 35)

	rows, err := db.QueryContext(ctx, "PRAGMA table_info(availability_facts)")
	require.NoError(t, err)
	defer rows.Close()

	columns := map[string]struct {
		notNull      bool
		defaultValue sql.NullString
		primaryKey   bool
	}{}
	for rows.Next() {
		var cid, notNull, pk int
		var name, columnType string
		var defaultValue sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk))
		columns[name] = struct {
			notNull      bool
			defaultValue sql.NullString
			primaryKey   bool
		}{notNull == 1, defaultValue, pk == 1}
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	for _, name := range []string{"scope_digest", "scope_generation", "manifest_digest", "fact_kind", "article_digest", "outcome", "article_count", "observed_at", "expires_at", "created_at", "updated_at"} {
		column, ok := columns[name]
		require.Truef(t, ok, "missing column %s", name)
		require.Truef(t, column.notNull, "column %s must be NOT NULL", name)
	}
	require.True(t, columns["id"].primaryKey)
	require.False(t, columns["id"].notNull)
	require.False(t, columns["id"].defaultValue.Valid)
	require.Equal(t, "''", columns["article_digest"].defaultValue.String)
	require.Equal(t, "0", columns["article_count"].defaultValue.String)
	require.Equal(t, "CURRENT_TIMESTAMP", columns["created_at"].defaultValue.String)
	require.Equal(t, "CURRENT_TIMESTAMP", columns["updated_at"].defaultValue.String)

	var tableSQL string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'availability_facts'").Scan(&tableSQL))
	require.Contains(t, tableSQL, "fact_kind IN ('manifest', 'article')")
	require.Contains(t, tableSQL, "outcome IN ('present', 'confirmed_missing')")
	require.Contains(t, tableSQL, "article_digest = ''")
	require.Contains(t, tableSQL, "article_digest <> ''")
	require.Contains(t, tableSQL, "article_count > 0")
	require.Contains(t, tableSQL, "article_count = 0")

	var uniqueIndexName string
	indexRows, err := db.QueryContext(ctx, "PRAGMA index_list(availability_facts)")
	require.NoError(t, err)
	for indexRows.Next() {
		var seq int
		var indexName string
		var unique, partial int
		var origin string
		require.NoError(t, indexRows.Scan(&seq, &indexName, &unique, &origin, &partial))
		if unique == 1 {
			uniqueIndexName = indexName
		}
	}
	require.NoError(t, indexRows.Close())

	indexInfo, err := db.Query("PRAGMA index_info(" + uniqueIndexName + ")")
	require.NoError(t, err)
	var uniqueColumns []string
	for indexInfo.Next() {
		var seqNo, columnNo int
		var name string
		require.NoError(t, indexInfo.Scan(&seqNo, &columnNo, &name))
		uniqueColumns = append(uniqueColumns, name)
	}
	require.NoError(t, indexInfo.Close())
	require.Equal(t, []string{"scope_digest", "scope_generation", "manifest_digest", "fact_kind", "article_digest"}, uniqueColumns)

	for _, indexName := range []string{"idx_availability_facts_expires_at", "idx_availability_facts_lookup"} {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", indexName).Scan(&count))
		require.Equal(t, 1, count, indexName)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO availability_facts
		(scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at)
		VALUES ('scope', 'generation', 'manifest', 'manifest', '', 'present', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO availability_facts
		(scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at)
		VALUES ('scope', 'generation', 'manifest', 'article', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'confirmed_missing', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	require.NoError(t, err)

	invalidRows := []string{
		`INSERT INTO availability_facts (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at) VALUES ('s2', 'g', 'm', 'manifest', NULL, 'present', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		`INSERT INTO availability_facts (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at) VALUES ('s3', 'g', 'm', 'manifest', 'digest', 'present', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		`INSERT INTO availability_facts (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at) VALUES ('s4', 'g', 'm', 'manifest', '', 'present', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		`INSERT INTO availability_facts (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at) VALUES ('s5', 'g', 'm', 'article', '', 'confirmed_missing', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		`INSERT INTO availability_facts (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at) VALUES ('s6', 'g', 'm', 'article', 'digest', 'confirmed_missing', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		`INSERT INTO availability_facts (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at) VALUES ('s7', 'g', 'm', 'manifest', '', 'confirmed_missing', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		`INSERT INTO availability_facts (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at) VALUES ('s8', 'g', 'm', 'unknown', '', 'present', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
	}
	for _, query := range invalidRows {
		_, err = db.ExecContext(ctx, query)
		require.Error(t, err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO availability_facts
		(scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at)
		VALUES ('scope', 'generation', 'manifest', 'manifest', '', 'present', 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest) DO UPDATE SET article_count = excluded.article_count`)
	require.NoError(t, err)
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM availability_facts WHERE scope_digest = 'scope' AND fact_kind = 'manifest'").Scan(&count))
	require.Equal(t, 1, count)
	var articleCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT article_count FROM availability_facts WHERE scope_digest = 'scope' AND fact_kind = 'manifest'").Scan(&articleCount))
	require.Equal(t, 2, articleCount)

	_, err = db.ExecContext(ctx, `INSERT INTO availability_facts
		(scope_digest, scope_generation, manifest_digest, fact_kind, article_digest, outcome, article_count, observed_at, expires_at)
		VALUES ('scope', 'generation', 'manifest', 'article', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'confirmed_missing', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest) DO UPDATE SET article_count = excluded.article_count`)
	require.NoError(t, err)
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM availability_facts WHERE scope_digest = 'scope' AND fact_kind = 'article'").Scan(&count))
	require.Equal(t, 1, count)
}

func TestMigrationAvailabilityDown(t *testing.T) {
	db := openMigratedTo(t, 35)
	require.NoError(t, goose.DownTo(db, "migrations/sqlite", 34))

	var tableName string
	err := db.QueryRow("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'availability_facts'").Scan(&tableName)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestMigrationAvailabilitySQLDialectParity(t *testing.T) {
	for _, path := range []string{
		"migrations/sqlite/035_add_availability_facts.sql",
		"migrations/postgres/035_add_availability_facts.sql",
	} {
		content, err := embedMigrations.ReadFile(path)
		require.NoError(t, err)
		sqlText := string(content)
		for _, fragment := range []string{
			"fact_kind IN ('manifest', 'article')",
			"outcome IN ('present', 'confirmed_missing')",
			"(fact_kind = 'manifest' AND outcome = 'present' AND article_digest = '' AND article_count > 0)",
			"(fact_kind = 'article' AND outcome = 'confirmed_missing' AND article_digest <> '' AND article_count = 0)",
			"UNIQUE (scope_digest, scope_generation, manifest_digest, fact_kind, article_digest)",
			"idx_availability_facts_expires_at",
			"idx_availability_facts_lookup",
			"DROP TABLE IF EXISTS availability_facts",
		} {
			require.Containsf(t, sqlText, fragment, "missing %q in %s", fragment, path)
		}
		require.NotContains(t, strings.ToLower(sqlText), "file_health")
	}
}
