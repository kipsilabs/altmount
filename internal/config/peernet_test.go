package config

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPeernetDefaults(t *testing.T) {
	c := DefaultConfig()
	require.NotNil(t, c.Peernet.Enabled)
	assert.False(t, *c.Peernet.Enabled)
	assert.False(t, c.Peernet.AcceptIncoming)
	assert.Equal(t, DefaultPeernetListenPort, c.Peernet.ListenPort)
	assert.Empty(t, c.Peernet.AdvertiseAddress)
	assert.Equal(t, PeernetLevelOff, c.Peernet.OpenNetwork.Level)
	assert.Empty(t, c.Peernet.OpenNetwork.Peers)
	assert.Empty(t, c.Peernet.Sharing.ExcludedCategories)
	assert.Empty(t, c.Peernet.Sharing.ExcludedIndexers)
	assert.Equal(t, 3, c.Peernet.MinCorroboration)
	assert.NoError(t, c.Validate())
	assert.False(t, c.PeernetEnabled())
}

func TestPeernetValidate(t *testing.T) {
	tooMany := make([]string, MaxPeernetPeers+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("peer%d.example.org:8470", i)
	}
	tests := []struct {
		name    string
		mutate  func(*PeernetConfig)
		wantErr string
	}{
		{"defaults ok", func(p *PeernetConfig) {}, ""},
		{"port low", func(p *PeernetConfig) { p.ListenPort = 1023 }, "listen_port"},
		{"port high", func(p *PeernetConfig) { p.ListenPort = 65536 }, "listen_port"},
		{"port min ok", func(p *PeernetConfig) { p.ListenPort = 1024 }, ""},
		{"port max ok", func(p *PeernetConfig) { p.ListenPort = 65535 }, ""},
		{"level -1", func(p *PeernetConfig) { p.OpenNetwork.Level = -1 }, "level"},
		{"level 4", func(p *PeernetConfig) { p.OpenNetwork.Level = 4 }, "level"},
		{"level 3 ok", func(p *PeernetConfig) { p.OpenNetwork.Level = PeernetLevelShareBundles }, ""},
		{"corroboration 0", func(p *PeernetConfig) { p.MinCorroboration = 0 }, "min_corroboration"},
		{"corroboration 1 ok", func(p *PeernetConfig) { p.MinCorroboration = 1 }, ""},
		{"advertise ok", func(p *PeernetConfig) { p.AdvertiseAddress = "my.host.example:8470" }, ""},
		{"advertise ipv6 ok", func(p *PeernetConfig) { p.AdvertiseAddress = "[2001:db8::1]:8470" }, ""},
		{"advertise no port", func(p *PeernetConfig) { p.AdvertiseAddress = "my.host.example" }, "advertise_address"},
		{"advertise scheme", func(p *PeernetConfig) { p.AdvertiseAddress = "https://a.example:8470" }, "advertise_address"},
		{"peer ok ip", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"203.0.113.5:8470"} }, ""},
		{"peer loopback ip", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"127.0.0.1:8470"} }, "loopback"},
		{"peer loopback v6", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"[::1]:8470"} }, "loopback"},
		{"peer localhost", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"localhost:8470"} }, "loopback"},
		{"peer private ok", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"192.168.1.5:8470"} }, ""},
		{"peer scheme", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"http://a.example:8470"} }, "peers"},
		{"peer userinfo", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"user@a.example:8470"} }, "peers"},
		{"peer path", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"a.example:8470/x"} }, "peers"},
		{"peer no port", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"a.example"} }, "peers"},
		{"peer port 0", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"a.example:0"} }, "peers"},
		{"peer port big", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"a.example:70000"} }, "peers"},
		{"peer empty host", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{":8470"} }, "peers"},
		{"peer space", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{"a b.example:8470"} }, "peers"},
		{"peer empty", func(p *PeernetConfig) { p.OpenNetwork.Peers = []string{""} }, "peers"},
		{"peer too many", func(p *PeernetConfig) { p.OpenNetwork.Peers = tooMany }, "peers"},
		{"peer max ok", func(p *PeernetConfig) { p.OpenNetwork.Peers = tooMany[:MaxPeernetPeers] }, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfig()
			tt.mutate(&c.Peernet)
			err := c.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestPeernetDeepCopyIndependent(t *testing.T) {
	c := DefaultConfig()
	c.Peernet.OpenNetwork.Peers = []string{"a.example:8470"}
	c.Peernet.Sharing.ExcludedCategories = []string{"movies"}
	c.Peernet.Sharing.ExcludedIndexers = []string{"idx"}
	en := true
	c.Peernet.Enabled = &en

	cp := c.DeepCopy()
	cp.Peernet.OpenNetwork.Peers[0] = "b.example:1"
	cp.Peernet.Sharing.ExcludedCategories[0] = "tv"
	cp.Peernet.Sharing.ExcludedIndexers[0] = "other"
	*cp.Peernet.Enabled = false

	assert.Equal(t, "a.example:8470", c.Peernet.OpenNetwork.Peers[0])
	assert.Equal(t, "movies", c.Peernet.Sharing.ExcludedCategories[0])
	assert.Equal(t, "idx", c.Peernet.Sharing.ExcludedIndexers[0])
	assert.True(t, *c.Peernet.Enabled)
}

func TestPeernetAccessorsNilAndZero(t *testing.T) {
	var nilCfg *Config
	assert.False(t, nilCfg.PeernetEnabled())
	assert.False(t, nilCfg.PeernetAcceptIncoming())
	assert.Equal(t, PeernetLevelOff, nilCfg.PeernetOpenNetworkLevel())
	assert.False(t, nilCfg.PeernetMayPublishMetadata())
	assert.False(t, nilCfg.PeernetMayServeBundles())
	assert.Equal(t, DefaultPeernetListenPort, nilCfg.PeernetListenPort())
	assert.Equal(t, 3, nilCfg.PeernetMinCorroboration())
	assert.False(t, nilCfg.PeernetCategoryExcluded("x"))
	assert.False(t, nilCfg.PeernetIndexerExcluded("x"))

	zero := &Config{}
	assert.False(t, zero.PeernetEnabled())
	assert.Equal(t, DefaultPeernetListenPort, zero.PeernetListenPort())
	assert.Equal(t, 3, zero.PeernetMinCorroboration())
	assert.False(t, zero.PeernetCategoryExcluded(""))
}

func TestPeernetAccessorsLevels(t *testing.T) {
	en, dis := true, false
	for lvl, want := range map[PeernetLevel][2]bool{
		PeernetLevelOff: {false, false}, PeernetLevelReceiveOnly: {false, false},
		PeernetLevelShareMetadata: {true, false}, PeernetLevelShareBundles: {true, true},
	} {
		c := DefaultConfig()
		c.Peernet.Enabled = &en
		c.Peernet.OpenNetwork.Level = lvl
		assert.Equal(t, lvl, c.PeernetOpenNetworkLevel())
		assert.Equal(t, want[0], c.PeernetMayPublishMetadata(), "publish at %d", lvl)
		assert.Equal(t, want[1], c.PeernetMayServeBundles(), "serve at %d", lvl)

		c.Peernet.Enabled = &dis
		assert.Equal(t, PeernetLevelOff, c.PeernetOpenNetworkLevel())
		assert.False(t, c.PeernetMayPublishMetadata())
		assert.False(t, c.PeernetMayServeBundles())
	}
}

func TestPeernetAcceptIncomingRequiresEnabled(t *testing.T) {
	c := DefaultConfig()
	c.Peernet.AcceptIncoming = true
	assert.False(t, c.PeernetAcceptIncoming())
	en := true
	c.Peernet.Enabled = &en
	assert.True(t, c.PeernetAcceptIncoming())
}

func TestPeernetExclusions(t *testing.T) {
	c := DefaultConfig()
	c.Peernet.Sharing.ExcludedCategories = []string{" Movies ", "TV"}
	c.Peernet.Sharing.ExcludedIndexers = []string{"My Indexer"}
	assert.True(t, c.PeernetCategoryExcluded("movies"))
	assert.True(t, c.PeernetCategoryExcluded("  tv"))
	assert.False(t, c.PeernetCategoryExcluded("music"))
	assert.False(t, c.PeernetCategoryExcluded(""))
	assert.True(t, c.PeernetIndexerExcluded("my indexer "))
	assert.False(t, c.PeernetIndexerExcluded("other"))
}

func TestPeernetYAMLRoundTrip(t *testing.T) {
	en := true
	in := PeernetConfig{
		Enabled: &en, AcceptIncoming: true, ListenPort: 9001, AdvertiseAddress: "h.example:9001",
		OpenNetwork:      PeernetOpenNetworkConfig{Level: PeernetLevelShareMetadata, Peers: []string{"a.example:8470"}},
		Sharing:          PeernetSharingConfig{ExcludedCategories: []string{"c"}, ExcludedIndexers: []string{"i"}},
		MinCorroboration: 3,
	}
	b, err := yaml.Marshal(in)
	require.NoError(t, err)
	for _, k := range []string{"enabled:", "accept_incoming:", "listen_port:", "advertise_address:", "open_network:", "level:", "peers:", "sharing:", "excluded_categories:", "excluded_indexers:", "min_corroboration:"} {
		assert.True(t, strings.Contains(string(b), k), k)
	}
	var out PeernetConfig
	require.NoError(t, yaml.Unmarshal(b, &out))
	assert.Equal(t, in, out)

	// Partial YAML over defaults keeps the other defaults.
	c := DefaultConfig()
	require.NoError(t, yaml.Unmarshal([]byte("peernet:\n  enabled: true\n"), c))
	assert.True(t, c.PeernetEnabled())
	assert.Equal(t, 3, c.Peernet.MinCorroboration)
	assert.Equal(t, DefaultPeernetListenPort, c.Peernet.ListenPort)
}

func TestPeernetChangeIsLiveApplicable(t *testing.T) {
	cur := DefaultConfig()
	m := NewManager(cur, "")
	next := cur.DeepCopy()
	en := true
	next.Peernet.Enabled = &en
	next.Peernet.AcceptIncoming = true
	next.Peernet.ListenPort = 9100
	next.Peernet.OpenNetwork.Level = PeernetLevelShareBundles
	assert.NoError(t, m.ValidateConfigUpdate(next))

	next.Peernet.OpenNetwork.Level = 9
	assert.Error(t, m.ValidateConfigUpdate(next))
}

func TestSampleConfigPeernetSection(t *testing.T) {
	data, err := os.ReadFile("../../config.sample.yaml")
	require.NoError(t, err)
	c := DefaultConfig()
	require.NoError(t, yaml.Unmarshal(data, c))
	assert.False(t, c.PeernetEnabled())
	assert.Equal(t, DefaultPeernetListenPort, c.Peernet.ListenPort)
	assert.Equal(t, PeernetLevelOff, c.Peernet.OpenNetwork.Level)
	assert.NoError(t, c.Peernet.Validate())
}

// Config validation and the transport must accept exactly the same addresses:
// an address that saves fine but is dropped later is a silent failure.
func TestPeernetAddressValidationMatchesTransport(t *testing.T) {
	longHost := strings.Repeat("a", 60) + "." + strings.Repeat("b", 60) + "." + strings.Repeat("c", 60) + "." +
		strings.Repeat("d", 60) + ".example"
	cases := []string{
		"peer.example.org:8470", "10.0.0.1:8470", "[::1]:8470", "[2001:db8::1]:1", "localhost:1024",
		"example.1com:80", "a.b.123:80", "host:+80", "host:000080", "host:65536", "host:0", "host:",
		":80", "host", "http://host:80", "user@host:80", "host:80/x", "host name:80", "Host.Example:80",
		"-a.example:80", "a-.example:80", "a..example:80", "[fe80::1%eth0]:80", "[::1]", "::1:80",
		longHost + ":80", strings.Repeat("x", 300) + ":80", "host:80?q", "host:80#f", "host%41:80",
		"ex*ample.org:80", "0x7f.1:80", "1.2.3:80", "123.example:80", "exa\x00mple.org:80",
	}
	for _, a := range cases {
		_, terr := transport.ValidateAddress(a)
		cerr := validatePeernetAddress(a)
		if (terr == nil) != (cerr == nil) {
			t.Errorf("%q: transport err=%v, config err=%v", a, terr, cerr)
		}
	}
}

func TestPeernetValidateRejectsAddressesTheTransportDrops(t *testing.T) {
	p := defaultPeernetConfig()
	p.OpenNetwork.Peers = []string{"example.1com:80"}
	if err := p.Validate(); err == nil {
		t.Fatal("a peer the transport would silently drop was accepted")
	}
	p.OpenNetwork.Peers = nil
	p.AdvertiseAddress = "host:+80"
	if err := p.Validate(); err == nil {
		t.Fatal("advertise address with a plus sign accepted")
	}
}
