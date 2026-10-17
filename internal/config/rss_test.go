package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRSSConfigurationValidation(t *testing.T) {
	for _, test := range []struct {
		name, url string
		interval  int
		valid     bool
	}{
		{"disabled", "", 0, true},
		{"configured", "https://example.com/rss?apikey=secret", 300, true},
		{"local", "http://localhost:9696/rss", 1, true},
		{"maximum interval", "", 86400, true},
		{"negative interval", "", -1, false},
		{"excess interval", "", 86401, false},
		{"relative feed", "/rss", 300, false},
		{"invalid scheme", "file:///secret", 300, false},
		{"invalid URL", "http://%zz?apikey=secret", 300, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Import.RSSFeedURL = test.url
			cfg.Import.RSSPollIntervalSeconds = test.interval
			err := cfg.Validate()
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "secret")
			}
		})
	}
}
