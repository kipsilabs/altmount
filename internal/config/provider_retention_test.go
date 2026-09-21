package config

import (
	"testing"
	"time"
)

// The config's days-based field is what an operator sets; the pool wants a
// Duration. A wrong conversion here would silently mis-scale every retention
// limit, so the mapping is pinned.
func TestToNNTPProviderMapsArticleRetention(t *testing.T) {
	tests := []struct {
		name       string
		days       int
		strict     bool
		wantMaxAge time.Duration
		wantStrict bool
	}{
		{"unset means unlimited", 0, false, 0, false},
		{"days become hours", 100, false, 100 * 24 * time.Hour, false},
		{"strict is carried through", 30, true, 30 * 24 * time.Hour, true},
		// A negative value in a hand-edited YAML must not become a negative
		// duration, which nntppool would read as "no limit" by a different
		// route than intended. Clamp it to unlimited explicitly.
		{"negative clamps to unlimited", -5, false, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := ProviderConfig{
				Host:                "news.example.com",
				Port:                563,
				MaxConnections:      10,
				MaxArticleAgeDays:   tc.days,
				StrictMaxArticleAge: tc.strict,
			}
			got := p.ToNNTPProvider()
			if got.MaxArticleAge != tc.wantMaxAge {
				t.Errorf("MaxArticleAge = %v, want %v", got.MaxArticleAge, tc.wantMaxAge)
			}
			if got.StrictMaxAge != tc.wantStrict {
				t.Errorf("StrictMaxAge = %v, want %v", got.StrictMaxAge, tc.wantStrict)
			}
		})
	}
}

// A retention change must reach the pool. providersFieldsEqual gates the
// incremental provider update, so omitting the new fields there would leave
// an edited limit applied only after a restart.
func TestProvidersFieldsEqualNoticesRetentionChange(t *testing.T) {
	base := ProviderConfig{ID: "p1", Host: "h", Port: 563, MaxConnections: 5}

	withAge := base
	withAge.MaxArticleAgeDays = 100
	if providersFieldsEqual(base, withAge) {
		t.Error("a changed max_article_age_days must be seen as a provider change")
	}

	withStrict := base
	withStrict.StrictMaxArticleAge = true
	if providersFieldsEqual(base, withStrict) {
		t.Error("a changed strict_max_article_age must be seen as a provider change")
	}

	same := base
	if !providersFieldsEqual(base, same) {
		t.Error("identical providers must compare equal")
	}
}
