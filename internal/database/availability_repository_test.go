package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/availability"
	"github.com/stretchr/testify/require"
)

func availabilityDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func testAvailabilityScope() availability.Scope {
	return availability.Scope{
		Digest:     availabilityDigest("scope"),
		Generation: "0123456789abcdef0123456789abcdef",
	}
}

func TestAvailabilityRepositoryUpsertsReadsAndIsolatesFacts(t *testing.T) {
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "availability.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := db.AvailabilityRepo
	ctx := context.Background()
	scope := testAvailabilityScope()
	manifestDigest := availabilityDigest("manifest")
	articleDigest := availability.BuildArticleDigest("<article@example>")
	otherArticleDigest := availability.BuildArticleDigest("<other-article@example>")
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiresAt := now.Add(6 * time.Hour)

	require.NoError(t, repo.UpsertManifestSummary(ctx, scope, manifestDigest, 3, now, expiresAt))
	summary, err := repo.GetManifestSummary(ctx, scope, manifestDigest, now)
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, manifestDigest, summary.ManifestDigest)
	require.Equal(t, 3, summary.ArticleCount)
	require.Equal(t, "", summary.ArticleDigest)

	require.NoError(t, repo.UpsertConfirmedMissing(ctx, scope, manifestDigest, articleDigest, now, expiresAt))
	require.NoError(t, repo.UpsertConfirmedMissing(ctx, scope, manifestDigest, otherArticleDigest, now, expiresAt))
	missing, err := repo.ListConfirmedMissing(ctx, scope, manifestDigest, now)
	require.NoError(t, err)
	require.Len(t, missing, 2)
	require.ElementsMatch(t, []string{articleDigest, otherArticleDigest}, []string{missing[0].ArticleDigest, missing[1].ArticleDigest})

	otherScope := scope
	otherScope.Digest = availabilityDigest("other-scope")
	require.NoError(t, repo.UpsertManifestSummary(ctx, otherScope, manifestDigest, 1, now, expiresAt))
	otherSummary, err := repo.GetManifestSummary(ctx, otherScope, manifestDigest, now)
	require.NoError(t, err)
	require.NotNil(t, otherSummary)
	require.Equal(t, 1, otherSummary.ArticleCount)
	isolatedMissing, err := repo.ListConfirmedMissing(ctx, scope, manifestDigest, now)
	require.NoError(t, err)
	require.Len(t, isolatedMissing, 2)

	otherManifest := availabilityDigest("other-manifest")
	require.NoError(t, repo.UpsertManifestSummary(ctx, scope, otherManifest, 2, now, expiresAt))
	otherManifestSummary, err := repo.GetManifestSummary(ctx, scope, otherManifest, now)
	require.NoError(t, err)
	require.NotNil(t, otherManifestSummary)
	require.Equal(t, 2, otherManifestSummary.ArticleCount)

	otherGeneration := scope
	otherGeneration.Generation = "fedcba9876543210fedcba9876543210"
	require.NoError(t, repo.UpsertManifestSummary(ctx, otherGeneration, manifestDigest, 1, now, expiresAt))
	generationSummary, err := repo.GetManifestSummary(ctx, otherGeneration, manifestDigest, now)
	require.NoError(t, err)
	require.NotNil(t, generationSummary)
	require.Equal(t, 1, generationSummary.ArticleCount)

	updatedObserved := now.Add(time.Minute)
	updatedExpiry := now.Add(12 * time.Hour)
	require.NoError(t, repo.UpsertManifestSummary(ctx, scope, manifestDigest, 4, updatedObserved, updatedExpiry))
	require.NoError(t, repo.UpsertConfirmedMissing(ctx, scope, manifestDigest, articleDigest, updatedObserved, updatedExpiry))
	require.NoError(t, repo.UpsertConfirmedMissing(ctx, scope, manifestDigest, otherArticleDigest, updatedObserved, updatedExpiry))
	updatedMissing, err := repo.ListConfirmedMissing(ctx, scope, manifestDigest, updatedObserved)
	require.NoError(t, err)
	require.Len(t, updatedMissing, 2)
	for _, fact := range updatedMissing {
		require.Equal(t, updatedObserved, fact.ObservedAt.UTC())
		require.Equal(t, updatedExpiry, fact.ExpiresAt.UTC())
	}
	var rowCount int
	require.NoError(t, db.Connection().QueryRowContext(ctx, "SELECT COUNT(*) FROM availability_facts WHERE scope_digest = ? AND scope_generation = ?", scope.Digest, scope.Generation).Scan(&rowCount))
	require.Equal(t, 4, rowCount)
	require.NoError(t, db.Connection().QueryRowContext(ctx, "SELECT COUNT(*) FROM availability_facts WHERE scope_digest = ? AND scope_generation = ? AND manifest_digest = ? AND fact_kind = 'article' AND article_digest = ?", scope.Digest, scope.Generation, manifestDigest, articleDigest).Scan(&rowCount))
	require.Equal(t, 1, rowCount)

	var storedText string
	require.NoError(t, db.Connection().QueryRowContext(ctx, `
		SELECT group_concat(scope_digest || '|' || scope_generation || '|' || manifest_digest || '|' || fact_kind || '|' || article_digest || '|' || outcome)
		FROM availability_facts`).Scan(&storedText))
	require.NotContains(t, storedText, "article@example")
	require.NotContains(t, storedText, "<article@example>")

	summary, err = repo.GetManifestSummary(ctx, scope, manifestDigest, updatedObserved)
	require.NoError(t, err)
	require.Equal(t, 4, summary.ArticleCount)
	require.Equal(t, updatedExpiry, summary.ExpiresAt.UTC())
}

func TestAvailabilityRepositoryExpiryValidationAndCleanup(t *testing.T) {
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "availability.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := db.AvailabilityRepo
	ctx := context.Background()
	scope := testAvailabilityScope()
	manifestDigest := availabilityDigest("manifest")
	now := time.Now().UTC().Truncate(time.Microsecond)

	require.Error(t, repo.UpsertManifestSummary(ctx, scope, manifestDigest, 1, now, now))
	require.Error(t, repo.UpsertManifestSummary(ctx, scope, manifestDigest, 1, now, now.Add(7*24*time.Hour+time.Second)))
	require.Error(t, repo.UpsertManifestSummary(ctx, scope, manifestDigest, 0, now, now.Add(time.Hour)))
	require.Error(t, repo.UpsertConfirmedMissing(ctx, scope, manifestDigest, "not-a-digest", now, now.Add(time.Hour)))
	require.NoError(t, repo.UpsertManifestSummary(ctx, scope, manifestDigest, 1, now, now.Add(time.Hour)))

	expired := now.Add(-2 * time.Hour)
	require.NoError(t, repo.UpsertManifestSummary(ctx, scope, availabilityDigest("expired"), 1, expired, expired.Add(time.Minute)))
	missing, err := repo.GetManifestSummary(ctx, scope, manifestDigest, now.Add(2*time.Hour))
	require.NoError(t, err)
	require.Nil(t, missing)
	deleted, err := repo.DeleteExpired(ctx, now, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, deleted)
	deleted, err = repo.DeleteExpired(ctx, now.Add(2*time.Hour), 10)
	require.NoError(t, err)
	require.EqualValues(t, 1, deleted)
	deleted, err = repo.DeleteExpired(ctx, now, 0)
	require.NoError(t, err)
	require.Zero(t, deleted)
}

func TestAvailabilityRepositoryCancellation(t *testing.T) {
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "availability.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := db.AvailabilityRepo
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scope := testAvailabilityScope()
	now := time.Now().UTC()
	manifestDigest := availabilityDigest("manifest")

	err = repo.UpsertManifestSummary(ctx, scope, manifestDigest, 1, now, now.Add(time.Hour))
	require.ErrorIs(t, err, context.Canceled)
	_, err = repo.GetManifestSummary(ctx, scope, manifestDigest, now)
	require.ErrorIs(t, err, context.Canceled)
	_, err = repo.ListConfirmedMissing(ctx, scope, manifestDigest, now)
	require.ErrorIs(t, err, context.Canceled)
	_, err = repo.DeleteExpired(ctx, now, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, errors.Is(err, context.DeadlineExceeded))
}
