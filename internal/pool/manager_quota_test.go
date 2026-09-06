package pool

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/javi11/nntppool/v4"
)

type quotaStatsRepo struct {
	stats        map[string]int64
	deleted      []string
	migrationErr error
}

func (r *quotaStatsRepo) UpdateSystemStat(_ context.Context, key string, value int64) error {
	if r.stats == nil {
		r.stats = make(map[string]int64)
	}
	r.stats[key] = value
	return nil
}

func (r *quotaStatsRepo) BatchUpdateSystemStats(_ context.Context, stats map[string]int64) error {
	for key, value := range stats {
		if err := r.UpdateSystemStat(context.Background(), key, value); err != nil {
			return err
		}
	}
	return nil
}

func (r *quotaStatsRepo) MigrateSystemStats(_ context.Context, stats map[string]int64, keys []string) error {
	if r.migrationErr != nil {
		return r.migrationErr
	}
	for key, value := range stats {
		if err := r.UpdateSystemStat(context.Background(), key, value); err != nil {
			return err
		}
	}
	for _, key := range keys {
		delete(r.stats, key)
		r.deleted = append(r.deleted, key)
	}
	return nil
}

func (r *quotaStatsRepo) GetSystemStats(context.Context) (map[string]int64, error) {
	stats := make(map[string]int64, len(r.stats))
	for key, value := range r.stats {
		stats[key] = value
	}
	return stats, nil
}

func (r *quotaStatsRepo) AddBytesDownloadedToDailyStat(context.Context, int64) error { return nil }

func (r *quotaStatsRepo) AddProviderBytesToHourlyStat(context.Context, string, int64) error {
	return nil
}

func (r *quotaStatsRepo) RecordProviderSpeedTest(context.Context, string, float64) error {
	return nil
}

func (r *quotaStatsRepo) GetProviderHourlyStats(context.Context, int) (map[string]int64, error) {
	return nil, nil
}

func (r *quotaStatsRepo) ClearProviderHourlyStats(context.Context) error { return nil }

func (r *quotaStatsRepo) GetOldestStatDate(context.Context) (time.Time, error) {
	return time.Time{}, nil
}

func (r *quotaStatsRepo) GetOldestProviderStatDates(context.Context) (map[string]time.Time, error) {
	return nil, nil
}

var _ StatsRepository = (*quotaStatsRepo)(nil)

func TestInjectQuotaStateMigratesLegacyProviderKeys(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).Truncate(time.Nanosecond)
	legacyName := "news.example.test:563+account-a"
	repo := &quotaStatsRepo{stats: map[string]int64{
		"quota_used:" + legacyName:     123,
		"quota_reset_at:" + legacyName: resetAt.UnixNano(),
	}}
	m := &manager{ctx: context.Background(), repo: repo, logger: testLogger()}
	providers := []nntppool.Provider{{
		Name:       "provider-primary",
		Host:       "news.example.test:563",
		Auth:       nntppool.Auth{Username: "account-a"},
		QuotaBytes: 1000,
	}}

	m.injectQuotaState(providers)

	if providers[0].QuotaUsed != 123 {
		t.Fatalf("QuotaUsed = %d, want migrated value 123", providers[0].QuotaUsed)
	}
	if !providers[0].QuotaResetAt.Equal(resetAt) {
		t.Fatalf("QuotaResetAt = %v, want %v", providers[0].QuotaResetAt, resetAt)
	}
	if got := repo.stats["quota_used:provider-primary"]; got != 123 {
		t.Fatalf("migrated quota_used = %d, want 123", got)
	}
	if got := repo.stats["quota_reset_at:provider-primary"]; got != resetAt.UnixNano() {
		t.Fatalf("migrated quota_reset_at = %d, want %d", got, resetAt.UnixNano())
	}
	if _, ok := repo.stats["quota_used:"+legacyName]; ok {
		t.Fatal("legacy quota_used key was not removed")
	}
	if _, ok := repo.stats["quota_reset_at:"+legacyName]; ok {
		t.Fatal("legacy quota_reset_at key was not removed")
	}
}

func TestInjectQuotaStateMigratesSameHostAccountsToDistinctIDs(t *testing.T) {
	legacyA := "news.example.test:563+account-a"
	legacyB := "news.example.test:563+account-b"
	repo := &quotaStatsRepo{stats: map[string]int64{
		"quota_used:" + legacyA: 11,
		"quota_used:" + legacyB: 22,
	}}
	m := &manager{ctx: context.Background(), repo: repo, logger: testLogger()}
	providers := []nntppool.Provider{
		{Name: "provider-a", Host: "news.example.test:563", Auth: nntppool.Auth{Username: "account-a"}},
		{Name: "provider-b", Host: "news.example.test:563", Auth: nntppool.Auth{Username: "account-b"}},
	}

	m.injectQuotaState(providers)

	if providers[0].QuotaUsed != 11 || providers[1].QuotaUsed != 22 {
		t.Fatalf("same-host quota migration lost account distinction: got %d and %d", providers[0].QuotaUsed, providers[1].QuotaUsed)
	}
	if repo.stats["quota_used:provider-a"] != 11 || repo.stats["quota_used:provider-b"] != 22 {
		t.Fatal("same-host quota keys were not migrated to distinct IDs")
	}
}

func TestInjectQuotaStatePrefersStableStateAfterPartialMigration(t *testing.T) {
	legacyName := "news.example.test:563+account-a"
	repo := &quotaStatsRepo{stats: map[string]int64{
		"quota_used:provider-primary": 99,
		"quota_used:" + legacyName:    11,
	}}
	m := &manager{ctx: context.Background(), repo: repo, logger: testLogger()}
	providers := []nntppool.Provider{{
		Name: "provider-primary",
		Host: "news.example.test:563",
		Auth: nntppool.Auth{Username: "account-a"},
	}}

	m.injectQuotaState(providers)

	if providers[0].QuotaUsed != 99 {
		t.Fatalf("QuotaUsed = %d, want stable value 99", providers[0].QuotaUsed)
	}
	if _, ok := repo.stats["quota_used:"+legacyName]; ok {
		t.Fatal("legacy key survived partial migration")
	}
}

func TestInjectQuotaStateDoesNotDuplicateAmbiguousLegacyKey(t *testing.T) {
	legacyName := "news.example.test:563+account-a"
	repo := &quotaStatsRepo{stats: map[string]int64{
		"quota_used:" + legacyName: 77,
	}}
	m := &manager{ctx: context.Background(), repo: repo, logger: testLogger()}
	providers := []nntppool.Provider{
		{Name: "provider-a", Host: "news.example.test:563", Auth: nntppool.Auth{Username: "account-a"}},
		{Name: "provider-b", Host: "news.example.test:563", Auth: nntppool.Auth{Username: "account-a"}},
	}

	m.injectQuotaState(providers)

	if providers[0].QuotaUsed != 0 || providers[1].QuotaUsed != 0 {
		t.Fatalf("ambiguous legacy quota was credited to a provider: got %d and %d", providers[0].QuotaUsed, providers[1].QuotaUsed)
	}
	if _, ok := repo.stats["quota_used:"+legacyName]; !ok {
		t.Fatal("ambiguous legacy key was removed instead of being retained")
	}
}

func TestInjectQuotaStateMigrationLogOmitsRepositoryError(t *testing.T) {
	legacyName := "news.example.test:563+account-a"
	repo := &quotaStatsRepo{
		stats:        map[string]int64{"quota_used:" + legacyName: 77},
		migrationErr: errors.New("write failed for legacy quota key " + legacyName),
	}
	var logs bytes.Buffer
	m := &manager{
		ctx:    context.Background(),
		repo:   repo,
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}
	providers := []nntppool.Provider{{
		Name: "provider-a", Host: "news.example.test:563", Auth: nntppool.Auth{Username: "account-a"},
	}}

	m.injectQuotaState(providers)

	if strings.Contains(logs.String(), "account-a") {
		t.Fatalf("migration log exposed a legacy provider key: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "error_class=repository") {
		t.Fatalf("migration log did not retain a value-free error classification: %s", logs.String())
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
