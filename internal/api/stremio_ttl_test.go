package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
)

// TestStremioNzbTTLHoursZeroRoundTrip verifies that an explicit NzbTTLHours of 0
// ("cache forever / disable expiry") survives serialization to the API response.
// A dropped 0 causes the frontend to fall back to its default of 24, silently
// overwriting the user's choice on the next save.
func TestStremioNzbTTLHoursZeroRoundTrip(t *testing.T) {
	enabled := true
	cfg := &config.Config{
		Stremio: config.StremioConfig{
			Enabled:     &enabled,
			NzbTTLHours: 0,
		},
	}

	resp := ToConfigAPIResponse(cfg, "")

	data, err := json.Marshal(resp.Stremio)
	if err != nil {
		t.Fatalf("marshal stremio response: %v", err)
	}

	if !strings.Contains(string(data), `"nzb_ttl_hours":0`) {
		t.Errorf("expected nzb_ttl_hours:0 to be present in response JSON, got: %s", data)
	}
}

// TestStremioFallbackFieldsZeroRoundTrip guards the same class of bug for the
// failed-release settings. A dropped max_fallback_releases:0 would silently
// re-enable fallback for a user who deliberately turned it off.
func TestStremioFallbackFieldsZeroRoundTrip(t *testing.T) {
	enabled := true
	cfg := &config.Config{
		Stremio: config.StremioConfig{
			Enabled:               &enabled,
			FailedReleaseTTLHours: 0,
			MaxFallbackReleases:   0,
		},
	}

	data, err := json.Marshal(ToConfigAPIResponse(cfg, "").Stremio)
	if err != nil {
		t.Fatalf("marshal stremio response: %v", err)
	}

	for _, want := range []string{`"failed_release_ttl_hours":0`, `"max_fallback_releases":0`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("expected %s in response JSON, got: %s", want, data)
		}
	}
}

// TestStremioNzbGrabCacheHoursZeroRoundTrip guards nzb_grab_cache_hours the same
// way: a dropped 0 would silently turn the grab cache back on at the next save.
func TestStremioNzbGrabCacheHoursZeroRoundTrip(t *testing.T) {
	enabled := true
	cfg := &config.Config{Stremio: config.StremioConfig{Enabled: &enabled, NzbGrabCacheHours: 0}}

	data, err := json.Marshal(ToConfigAPIResponse(cfg, "").Stremio)
	if err != nil {
		t.Fatalf("marshal stremio response: %v", err)
	}
	if !strings.Contains(string(data), `"nzb_grab_cache_hours":0`) {
		t.Errorf("expected nzb_grab_cache_hours:0 in response JSON, got: %s", data)
	}
}

func TestStremioDefaults(t *testing.T) {
	cfg := config.DefaultConfig(t.TempDir())

	if cfg.Stremio.NzbGrabCacheHours != 24 {
		t.Errorf("NzbGrabCacheHours = %d; want 24", cfg.Stremio.NzbGrabCacheHours)
	}

	if cfg.Stremio.FailedReleaseTTLHours != 24 {
		t.Errorf("FailedReleaseTTLHours = %d; want 24", cfg.Stremio.FailedReleaseTTLHours)
	}
	if cfg.Stremio.MaxFallbackReleases != 2 {
		t.Errorf("MaxFallbackReleases = %d; want 2", cfg.Stremio.MaxFallbackReleases)
	}
}

func TestEffectiveMaxFallbackReleases(t *testing.T) {
	tests := []struct {
		name string
		set  int
		want int
	}{
		{name: "zero disables fallback", set: 0, want: 0},
		{name: "default passes through", set: 2, want: 2},
		{name: "at the cap", set: 4, want: 4},
		{name: "above the cap is clamped", set: 99, want: 4},
		{name: "negative is treated as disabled", set: -1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := config.StremioConfig{MaxFallbackReleases: tt.set}
			if got := c.EffectiveMaxFallbackReleases(); got != tt.want {
				t.Errorf("EffectiveMaxFallbackReleases() = %d; want %d", got, tt.want)
			}
		})
	}
}
