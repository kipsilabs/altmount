package availability

import (
	"strings"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/stretchr/testify/require"
)

func boolPtr(v bool) *bool { return &v }

func scopeConfig() *config.Config {
	return &config.Config{Providers: []config.ProviderConfig{
		{ID: "a", Host: "NEWS.EXAMPLE.COM", Port: 563, Username: "alice", Password: "secret", Enabled: boolPtr(true), TLS: true, InsecureTLS: true, IsBackupProvider: boolPtr(false), StorageGroup: "fast", SkipPing: true, MaxConnections: 4, InflightRequests: 9, StatInflightRequests: 7, MinConnectionsAlive: 2, KeepaliveIntervalSeconds: 30, KeepaliveCommand: "NOOP", UserAgent: "agent", QuotaBytes: 100, QuotaPeriodHours: 24},
		{ID: "disabled", Host: "disabled.example", Port: 119, Username: "disabled-user", Password: "disabled-secret", Enabled: boolPtr(false)},
	}}
}

func TestScopeDigestIsOrderStableAndExcludesCredentials(t *testing.T) {
	first := scopeConfig()
	second := scopeConfig()
	second.Providers[0], second.Providers[1] = second.Providers[1], second.Providers[0]

	got := BuildPoolScopeDigest(first)
	require.Equal(t, got, BuildPoolScopeDigest(second))
	require.NotContains(t, got, "alice")
	require.NotContains(t, got, "secret")
	require.NotContains(t, got, "disabled-user")
	require.Len(t, got, 64)
}

func TestScopeDigestChangesActivePoolSettings(t *testing.T) {
	base := BuildPoolScopeDigest(scopeConfig())
	for name, mutate := range map[string]func(*config.ProviderConfig){
		"endpoint": func(p *config.ProviderConfig) { p.Host = "other.example" },
		"tls":      func(p *config.ProviderConfig) { p.TLS = false },
		"storage":  func(p *config.ProviderConfig) { p.StorageGroup = "slow" },
		"enabled":  func(p *config.ProviderConfig) { p.Enabled = boolPtr(false) },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := scopeConfig()
			mutate(&cfg.Providers[0])
			require.NotEqual(t, base, BuildPoolScopeDigest(cfg))
		})
	}
}

func TestScopeTrackerKeepsEquivalentConfigAndRotatesChanges(t *testing.T) {
	cfg := scopeConfig()
	tracker := NewScopeTracker(cfg)
	initial := tracker.Current()

	equivalent := scopeConfig()
	tracker.OnConfigChange(cfg, equivalent)
	require.Equal(t, initial, tracker.Current())

	changed := scopeConfig()
	changed.Providers[0].Host = "changed.example"
	tracker.OnConfigChange(equivalent, changed)
	require.NotEqual(t, initial, tracker.Current())

	changedOrder := scopeConfig()
	changedOrder.Providers[0], changedOrder.Providers[1] = changedOrder.Providers[1], changedOrder.Providers[0]
	tracker.OnConfigChange(changed, changedOrder)
	require.NotEqual(t, initial, tracker.Current())
}

func TestArticleDigestNormalizesMessageID(t *testing.T) {
	got := BuildArticleDigest("  <message@example>  ")
	require.Equal(t, got, BuildArticleDigest("message@example"))
	require.NotEqual(t, got, BuildArticleDigest("other@example"))
	require.True(t, strings.TrimSpace(got) == got)
	require.Len(t, got, 64)
}
