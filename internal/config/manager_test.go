package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestConfig_Validate_MountPaths(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		wantErr     bool
		errContains string
	}{
		{
			name: "mount type fuse - ok",
			config: &Config{
				MountType: MountTypeFuse,
				MountPath: "/mnt/remotes/altmount",
				Metadata: MetadataConfig{
					RootPath: "/metadata",
				},
				WebDAV: WebDAVConfig{
					Port: 8080,
				},
				Streaming: StreamingConfig{
					MaxPrefetch: 30,
				},
				Import: ImportConfig{
					MaxProcessorWorkers:            2,
					QueueProcessingIntervalSeconds: 5,
					MaxImportConnections:           5,
					MaxDownloadPrefetch:            3,
					SegmentSamplePercentage:        1,
					ImportStrategy:                 ImportStrategyNone,
				},
				Health: HealthConfig{
					CheckIntervalSeconds:          5,
					MaxConnectionsForHealthChecks: 5,
					MaxConcurrentJobs:             1,
					SegmentSamplePercentage:       5,
				},
			},
			wantErr: false,
		},
		{
			name: "mount type rclone - ok",
			config: &Config{
				MountType: MountTypeRClone,
				MountPath: "/mnt/remotes/altmount",
				Metadata: MetadataConfig{
					RootPath: "/metadata",
				},
				WebDAV: WebDAVConfig{
					Port: 8080,
				},
				Streaming: StreamingConfig{
					MaxPrefetch: 30,
				},
				Import: ImportConfig{
					MaxProcessorWorkers:            2,
					QueueProcessingIntervalSeconds: 5,
					MaxImportConnections:           5,
					MaxDownloadPrefetch:            3,
					SegmentSamplePercentage:        1,
					ImportStrategy:                 ImportStrategyNone,
				},
				Health: HealthConfig{
					CheckIntervalSeconds:          5,
					MaxConnectionsForHealthChecks: 5,
					MaxConcurrentJobs:             1,
					SegmentSamplePercentage:       5,
				},
			},
			wantErr: false,
		},
		{
			name: "mount type none - ok",
			config: &Config{
				MountType: MountTypeNone,
				Metadata: MetadataConfig{
					RootPath: "/metadata",
				},
				WebDAV: WebDAVConfig{
					Port: 8080,
				},
				Streaming: StreamingConfig{
					MaxPrefetch: 30,
				},
				Import: ImportConfig{
					MaxProcessorWorkers:            2,
					QueueProcessingIntervalSeconds: 5,
					MaxImportConnections:           5,
					MaxDownloadPrefetch:            3,
					SegmentSamplePercentage:        1,
					ImportStrategy:                 ImportStrategyNone,
				},
				Health: HealthConfig{
					CheckIntervalSeconds:          5,
					MaxConnectionsForHealthChecks: 5,
					MaxConcurrentJobs:             1,
					SegmentSamplePercentage:       5,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_Validate_QueueCleanupRuleAction(t *testing.T) {
	// Build an otherwise-valid config carrying a single rule with the given action.
	// The action check runs at the end of Validate(), so everything else must pass.
	newValidConfig := func(action string) *Config {
		return &Config{
			MountType: MountTypeNone,
			Metadata:  MetadataConfig{RootPath: "/metadata"},
			WebDAV:    WebDAVConfig{Port: 8080},
			Streaming: StreamingConfig{MaxPrefetch: 30},
			Import: ImportConfig{
				MaxProcessorWorkers:            2,
				QueueProcessingIntervalSeconds: 5,
				MaxImportConnections:           5,
				MaxDownloadPrefetch:            3,
				SegmentSamplePercentage:        1,
				ImportStrategy:                 ImportStrategyNone,
			},
			Health: HealthConfig{
				CheckIntervalSeconds:          5,
				MaxConnectionsForHealthChecks: 5,
				MaxConcurrentJobs:             1,
				SegmentSamplePercentage:       5,
			},
			Arrs: ArrsConfig{
				QueueCleanupRules: []StuckCleanupRule{
					{Message: "Sample", Enabled: true, Action: action},
				},
			},
		}
	}

	// Known actions — and empty, which safely degrades to "remove" at runtime — pass.
	for _, action := range []string{"", StuckActionRemove, StuckActionBlocklist, StuckActionBlocklistSearch} {
		assert.NoError(t, newValidConfig(action).Validate(), "action %q should be valid", action)
	}

	// An unknown action is rejected.
	err := newValidConfig("delete_everything").Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid action")
}

func TestConfig_GetWebhookBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "explicitly set",
			config: Config{
				Arrs: ArrsConfig{
					WebhookBaseURL: "http://custom:1234",
				},
				WebDAV: WebDAVConfig{
					Port: 8080,
				},
			},
			expected: "http://custom:1234",
		},
		{
			name: "default with port 8080",
			config: Config{
				Arrs: ArrsConfig{
					WebhookBaseURL: "",
				},
				WebDAV: WebDAVConfig{
					Port: 8080,
				},
			},
			expected: "http://altmount:8080",
		},
		{
			name: "default with port 8084",
			config: Config{
				Arrs: ArrsConfig{
					WebhookBaseURL: "",
				},
				WebDAV: WebDAVConfig{
					Port: 8084,
				},
			},
			expected: "http://altmount:8084",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.GetWebhookBaseURL())
		})
	}
}

func TestConfig_GetDownloadClientBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "explicitly set",
			config: Config{
				SABnzbd: SABnzbdConfig{
					DownloadClientBaseURL: "http://custom:1234/sab",
				},
				WebDAV: WebDAVConfig{
					Port: 8080,
				},
			},
			expected: "http://custom:1234/sab",
		},
		{
			name: "default with port 8080",
			config: Config{
				SABnzbd: SABnzbdConfig{
					DownloadClientBaseURL: "",
				},
				WebDAV: WebDAVConfig{
					Port: 8080,
				},
			},
			expected: "http://altmount:8080/sabnzbd",
		},
		{
			name: "default with port 8084",
			config: Config{
				SABnzbd: SABnzbdConfig{
					DownloadClientBaseURL: "",
				},
				WebDAV: WebDAVConfig{
					Port: 8084,
				},
			},
			expected: "http://altmount:8084/sabnzbd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.GetDownloadClientBaseURL())
		})
	}
}

func TestConfig_NetworkRoundTrip(t *testing.T) {
	in := Config{
		Network: NetworkConfig{
			HTTPProxy:  "http://proxy:3128",
			HTTPSProxy: "http://proxy:3128",
			NoProxy:    "localhost,10.0.0.0/8",
		},
	}
	b, err := yaml.Marshal(in)
	assert.NoError(t, err)

	var out Config
	err = yaml.Unmarshal(b, &out)
	assert.NoError(t, err)

	assert.Equal(t, in.Network, out.Network)
	assert.Equal(t, "http://proxy:3128", out.Network.GetHTTPProxy())
	assert.Equal(t, "http://proxy:3128", out.Network.GetHTTPSProxy())
	assert.Equal(t, "localhost,10.0.0.0/8", out.Network.GetNoProxy())
}

func TestConfig_NetworkDefaultsEmpty(t *testing.T) {
	cfg := Config{}
	assert.Empty(t, cfg.Network.HTTPProxy)
	assert.Empty(t, cfg.Network.HTTPSProxy)
	assert.Empty(t, cfg.Network.NoProxy)
}

func TestMigrateArrsCleanup_FoldsLegacyConfig(t *testing.T) {
	legacyEnabled := true
	cfg := &Config{
		Arrs: ArrsConfig{
			// No unified rules or enable flag / grace yet — simulate a pre-merge config.
			StuckCleanupEnabled:            &legacyEnabled,
			StuckCleanupGracePeriodMinutes: 7,
			StuckCleanupRules: []StuckCleanupRule{
				{Message: "is not a valid video file", Enabled: true, Action: StuckActionBlocklistSearch},
			},
			QueueCleanupAllowlist: []IgnoredMessage{
				{Message: "Could not find file", Enabled: true},
				// Exact duplicate of an existing rule message — must not be added twice.
				{Message: "is not a valid video file", Enabled: false},
				// Substring-covered by the "is not a valid video file" rule above
				// (matching is substring-based) — must be skipped as a dead duplicate.
				{Message: "is not a valid video file (sample)", Enabled: true},
			},
		},
	}

	migrateArrsCleanup(cfg)
	a := cfg.Arrs

	// Stuck rule kept; unique allowlist entry folded in as a remove rule; exact and
	// substring-covered duplicates skipped.
	assert.Equal(t, []StuckCleanupRule{
		{Message: "is not a valid video file", Enabled: true, Action: StuckActionBlocklistSearch},
		{Message: "Could not find file", Enabled: true, Action: StuckActionRemove},
	}, a.QueueCleanupRules)

	// Legacy enable + grace carried over.
	assert.NotNil(t, a.QueueCleanupEnabled)
	assert.True(t, *a.QueueCleanupEnabled)
	assert.Equal(t, 7, a.QueueCleanupGracePeriodMinutes)

	// Legacy fields cleared so they drop out of saved YAML.
	assert.Nil(t, a.QueueCleanupAllowlist)
	assert.Nil(t, a.StuckCleanupEnabled)
	assert.Nil(t, a.StuckCleanupRules)
	assert.Zero(t, a.StuckCleanupGracePeriodMinutes)

	// Idempotent: a second pass changes nothing.
	before := a.QueueCleanupRules
	migrateArrsCleanup(cfg)
	assert.Equal(t, before, cfg.Arrs.QueueCleanupRules)

	// Saved YAML must not emit any legacy keys.
	b, err := yaml.Marshal(cfg.Arrs)
	assert.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "queue_cleanup_rules:")
	assert.NotContains(t, out, "queue_cleanup_allowlist")
	assert.NotContains(t, out, "stuck_cleanup_enabled")
	assert.NotContains(t, out, "stuck_cleanup_grace_period_minutes")
	assert.NotContains(t, out, "stuck_cleanup_rules")
}

func TestMigrateArrsCleanup_NoLegacyNoop(t *testing.T) {
	rules := []StuckCleanupRule{
		{Message: "Sample", Enabled: true, Action: StuckActionBlocklistSearch},
	}
	cfg := &Config{Arrs: ArrsConfig{QueueCleanupRules: rules}}
	migrateArrsCleanup(cfg)
	// Unified-only config is left untouched.
	assert.Equal(t, rules, cfg.Arrs.QueueCleanupRules)
}

func TestMigrateArrsCleanup_AutoFailureFlag_SeedsRemoveRule(t *testing.T) {
	enabled := true
	cfg := &Config{
		Arrs: ArrsConfig{
			// A modern config: it already has custom rules AND the legacy toggle on,
			// with none of the legacy split-cleanup fields. The rules must be preserved
			// (not rebuilt/wiped) and the auto-failure rule appended.
			QueueCleanupRules: []StuckCleanupRule{
				{Message: "Sample", Enabled: true, Action: StuckActionBlocklistSearch},
			},
			CleanupAutomaticImportFailure: &enabled,
		},
	}

	migrateArrsCleanup(cfg)
	a := cfg.Arrs

	assert.Equal(t, []StuckCleanupRule{
		{Message: "Sample", Enabled: true, Action: StuckActionBlocklistSearch},
		{Message: "automatic import is not possible", Enabled: true, Action: StuckActionRemove},
	}, a.QueueCleanupRules)

	// Legacy flag cleared so it drops out of saved YAML.
	assert.Nil(t, a.CleanupAutomaticImportFailure)

	// Idempotent: a second pass changes nothing.
	before := a.QueueCleanupRules
	migrateArrsCleanup(cfg)
	assert.Equal(t, before, cfg.Arrs.QueueCleanupRules)

	b, err := yaml.Marshal(cfg.Arrs)
	assert.NoError(t, err)
	assert.NotContains(t, string(b), "cleanup_automatic_import_failure")
}

func TestMigrateArrsCleanup_AutoFailureFlag_EnablesExistingRule(t *testing.T) {
	enabled := true
	cfg := &Config{
		Arrs: ArrsConfig{
			// Mirrors a fresh-install config (the seeded rule loads disabled) whose owner
			// had the legacy toggle on: the existing rule is enabled in place, not duplicated.
			QueueCleanupRules: []StuckCleanupRule{
				{Message: "automatic import is not possible", Enabled: false, Action: StuckActionRemove},
			},
			CleanupAutomaticImportFailure: &enabled,
		},
	}

	migrateArrsCleanup(cfg)

	assert.Equal(t, []StuckCleanupRule{
		{Message: "automatic import is not possible", Enabled: true, Action: StuckActionRemove},
	}, cfg.Arrs.QueueCleanupRules)
	assert.Nil(t, cfg.Arrs.CleanupAutomaticImportFailure)
}

func TestMigrateArrsCleanup_AutoFailureFlag_EnablesSubstringRule(t *testing.T) {
	enabled := true
	cfg := &Config{
		Arrs: ArrsConfig{
			// A rule whose message is a substring of the phrase already covers it (matching
			// is substring-based), so it is enabled rather than a duplicate appended.
			QueueCleanupRules: []StuckCleanupRule{
				{Message: "Automatic import", Enabled: false, Action: StuckActionBlocklistSearch},
			},
			CleanupAutomaticImportFailure: &enabled,
		},
	}

	migrateArrsCleanup(cfg)

	assert.Equal(t, []StuckCleanupRule{
		{Message: "Automatic import", Enabled: true, Action: StuckActionBlocklistSearch},
	}, cfg.Arrs.QueueCleanupRules)
	assert.Nil(t, cfg.Arrs.CleanupAutomaticImportFailure)
}

func TestMigrateArrsCleanup_AutoFailureFlag_FalseClearsOnly(t *testing.T) {
	disabled := false
	rules := []StuckCleanupRule{
		{Message: "Sample", Enabled: true, Action: StuckActionBlocklistSearch},
	}
	cfg := &Config{
		Arrs: ArrsConfig{
			QueueCleanupRules:             rules,
			CleanupAutomaticImportFailure: &disabled,
		},
	}

	migrateArrsCleanup(cfg)

	// Flag off: no rule seeded, existing rules untouched, flag cleared.
	assert.Equal(t, rules, cfg.Arrs.QueueCleanupRules)
	assert.Nil(t, cfg.Arrs.CleanupAutomaticImportFailure)
}

// A provider written by hand with no "id" (or an empty one) predates the id
// requirement and must not prevent the process from starting: LoadConfig
// should migrate it to a stable id rather than failing Validate.
func TestLoadConfig_ProviderWithBlankIDDoesNotFailStartup(t *testing.T) {
	tempDir := t.TempDir()
	configFile := tempDir + "/config.yaml"

	yamlContent := `
providers:
  - id: ''
    host: news.example.test
    port: 563
    max_connections: 5
  - host: news.other.test
    port: 119
    max_connections: 5
`
	err := os.WriteFile(configFile, []byte(yamlContent), 0644)
	assert.NoError(t, err)

	cfg, err := LoadConfig(configFile)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	assert.NotEmpty(t, cfg.Providers[0].ID)
	assert.NotEmpty(t, cfg.Providers[1].ID)
	assert.NotEqual(t, cfg.Providers[0].ID, cfg.Providers[1].ID)
}
