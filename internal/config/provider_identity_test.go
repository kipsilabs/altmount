package config

import (
	"strings"
	"testing"
)

func TestProviderIdentityUsesStableID(t *testing.T) {
	provider := ProviderConfig{
		ID:       "provider-primary",
		Host:     "news.example.test",
		Port:     563,
		Username: "synthetic-account",
	}

	if got := provider.NNTPPoolName(); got != provider.ID {
		t.Fatalf("NNTPPoolName() = %q, want stable ID %q", got, provider.ID)
	}

	poolProvider := provider.ToNNTPProvider()
	if poolProvider.Name != provider.ID {
		t.Fatalf("ToNNTPProvider().Name = %q, want stable ID %q", poolProvider.Name, provider.ID)
	}
	if strings.Contains(poolProvider.Name, provider.Username) {
		t.Fatalf("pool provider name contains the authentication username")
	}
}

func TestProviderIdentityKeepsSameHostAccountsDistinct(t *testing.T) {
	first := ProviderConfig{ID: "provider-first", Host: "news.example.test", Port: 563, Username: "first-account"}
	second := ProviderConfig{ID: "provider-second", Host: "news.example.test", Port: 563, Username: "second-account"}

	if first.NNTPPoolName() == second.NNTPPoolName() {
		t.Fatal("same-host providers must retain distinct stable IDs")
	}
	if first.ToNNTPProvider().Name == second.ToNNTPProvider().Name {
		t.Fatal("same-host nntppool providers must retain distinct stable IDs")
	}
}

func TestMigrateProviderIDsAssignsBlankIDs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Providers = []ProviderConfig{
		{Host: "news.example.test", Port: 563, MaxConnections: 1},
		{ID: "provider_2", Host: "news.other.test", Port: 563, MaxConnections: 1},
		{Host: "news.third.test", Port: 563, MaxConnections: 1},
	}

	migrateProviderIDs(cfg)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error after migration = %v", err)
	}

	seen := make(map[string]struct{}, len(cfg.Providers))
	for i, p := range cfg.Providers {
		if strings.TrimSpace(p.ID) == "" {
			t.Fatalf("provider %d still has a blank id after migration", i)
		}
		if _, exists := seen[p.ID]; exists {
			t.Fatalf("migration assigned duplicate id %q", p.ID)
		}
		seen[p.ID] = struct{}{}
	}
	if cfg.Providers[1].ID != "provider_2" {
		t.Fatalf("migration changed an already-set id: got %q, want %q", cfg.Providers[1].ID, "provider_2")
	}
}

// A whitespace-only id (e.g. `id: ' '` left over from hand-editing) is not a
// usable id: Validate rejects it just like an empty one, so migration must
// treat it the same way rather than treating it as already-set.
func TestMigrateProviderIDsReplacesWhitespaceOnlyID(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Providers = []ProviderConfig{
		{ID: "  ", Host: "news.example.test", Port: 563, MaxConnections: 1},
	}

	migrateProviderIDs(cfg)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error after migration = %v", err)
	}
	if strings.TrimSpace(cfg.Providers[0].ID) == "" {
		t.Fatalf("provider still has a whitespace-only id after migration: %q", cfg.Providers[0].ID)
	}
}

// A reload must not keep reassigning ids on every call, or persisted quota
// state (keyed by id) would drift each time the config is re-read from disk.
func TestMigrateProviderIDsIsIdempotent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Providers = []ProviderConfig{
		{Host: "news.example.test", Port: 563, MaxConnections: 1},
		{Host: "news.other.test", Port: 563, MaxConnections: 1},
	}

	migrateProviderIDs(cfg)
	firstPass := []string{cfg.Providers[0].ID, cfg.Providers[1].ID}

	migrateProviderIDs(cfg)
	secondPass := []string{cfg.Providers[0].ID, cfg.Providers[1].ID}

	if firstPass[0] != secondPass[0] || firstPass[1] != secondPass[1] {
		t.Fatalf("migration is not idempotent: first pass %v, second pass %v", firstPass, secondPass)
	}
}

func TestConfigValidateRequiresUniqueProviderIDs(t *testing.T) {
	validProvider := func(id string) ProviderConfig {
		return ProviderConfig{ID: id, Host: "news.example.test", Port: 563, MaxConnections: 1}
	}

	t.Run("rejects empty ID", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Providers = []ProviderConfig{validProvider("")}
		if err := cfg.Validate(); err == nil {
			t.Fatal("Validate() accepted an empty provider ID")
		}
	})

	t.Run("rejects duplicate IDs", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Providers = []ProviderConfig{validProvider("provider-duplicate"), validProvider("provider-duplicate")}
		if err := cfg.Validate(); err == nil {
			t.Fatal("Validate() accepted duplicate provider IDs")
		}
	})

	for name, id := range map[string]string{
		"leading whitespace":  " provider",
		"trailing whitespace": "provider ",
		"newline":             "provider\nline",
		"line separator":      "provider\u2028line",
		"paragraph separator": "provider\u2029line",
		"bidi control":        "provider\u202Eline",
	} {
		t.Run("rejects "+name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Providers = []ProviderConfig{validProvider(id)}
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate() accepted unsafe provider ID")
			}
		})
	}
}
