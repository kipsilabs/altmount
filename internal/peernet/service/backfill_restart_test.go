package service

import (
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
)

func cfgWith(mut func(*config.Config)) *config.Config {
	c := config.DefaultConfig()
	on := true
	c.Peernet.Enabled = &on
	if mut != nil {
		mut(c)
	}
	return c
}

// A config save must not restart the content-index backfill unless a setting
// the index depends on changed.
func TestConfigSaveDoesNotRestartBackfill(t *testing.T) {
	base := cfgWith(nil)
	i := newInstance(t, SettingsFromConfig(base))
	i.start(t)
	eventually(t, "first backfill pass", func() bool { return i.meta.walkCount() >= 1 })
	before := i.meta.walkCount()

	cases := map[string]func(*config.Config){
		"unrelated":       func(c *config.Config) { c.Log.Level = "debug" },
		"open level":      func(c *config.Config) { c.Peernet.OpenNetwork.Level = 1 },
		"advertise":       func(c *config.Config) { c.Peernet.AdvertiseAddress = "example.org:8470" },
		"proxy":           func(c *config.Config) { c.Network.HTTPProxy = "http://p:1" },
		"accept incoming": func(c *config.Config) { c.Peernet.AcceptIncoming = true; c.Peernet.ListenPort = 0 },
	}
	prev := base
	for name, mut := range cases {
		next := cfgWith(mut)
		i.svc.OnConfigChange(prev, next)
		prev = next
		_ = name
	}
	// Every change was queued synchronously, so waiting for the worker covers
	// them all; the pause then gives a wrongly restarted backfill time to show.
	i.svc.asyncWG.Wait()
	time.Sleep(300 * time.Millisecond)
	if got := i.meta.walkCount(); got != before {
		t.Fatalf("backfill restarted: walks %d -> %d", before, got)
	}
}
