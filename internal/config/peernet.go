package config

import (
	"fmt"
	"strings"

	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// PeernetLevel is how much of this instance's data a network may see.
type PeernetLevel int

const (
	// PeernetLevelOff takes no part in the network.
	PeernetLevelOff PeernetLevel = 0
	// PeernetLevelReceiveOnly receives records, attestations and bundles but
	// publishes and serves nothing.
	PeernetLevelReceiveOnly PeernetLevel = 1
	// PeernetLevelShareMetadata also publishes release records and health
	// attestations for this instance's own healthy imports.
	PeernetLevelShareMetadata PeernetLevel = 2
	// PeernetLevelShareBundles also serves finished-import bundles on request.
	PeernetLevelShareBundles PeernetLevel = 3
)

const (
	// DefaultPeernetListenPort is the default port of the peer listener. It is
	// unprivileged and distinct from the web (8080) and rclone RC (5573) ports.
	DefaultPeernetListenPort = 8470
	// DefaultPeernetMinCorroboration is how many distinct nodes, counted once
	// per network prefix, must attest a release before it is shown as healthy
	// on the open network.
	DefaultPeernetMinCorroboration = 3
	// MaxPeernetPeers caps the user-entered open-network peer list.
	MaxPeernetPeers = 64
)

// Valid reports whether l is one of the defined levels.
func (l PeernetLevel) Valid() bool {
	return l >= PeernetLevelOff && l <= PeernetLevelShareBundles
}

// PeernetConfig configures the peer network. Off by default: when Enabled is
// false nothing listens, nothing connects and no key is generated. Private
// networks (hosted or joined) and their per-network settings live in the
// database, not here. All fields apply live; none requires a restart.
type PeernetConfig struct {
	Enabled *bool `yaml:"enabled" mapstructure:"enabled" json:"enabled"`
	// AcceptIncoming starts the peer listener on ListenPort. Not needed at any
	// level (a node can publish and serve over connections it opened).
	AcceptIncoming bool `yaml:"accept_incoming" mapstructure:"accept_incoming" json:"accept_incoming"`
	// ListenPort is the peer listener port (1024-65535).
	ListenPort int `yaml:"listen_port" mapstructure:"listen_port" json:"listen_port"`
	// AdvertiseAddress is the optional host:port other peers should use to
	// reach this node.
	AdvertiseAddress string `yaml:"advertise_address" mapstructure:"advertise_address" json:"advertise_address"`
	// OpenNetwork is this node's participation in the open network.
	OpenNetwork PeernetOpenNetworkConfig `yaml:"open_network" mapstructure:"open_network" json:"open_network"`
	// Sharing lists what is never published, whatever the level.
	Sharing PeernetSharingConfig `yaml:"sharing" mapstructure:"sharing" json:"sharing"`
	// MinCorroboration is the number of distinct nodes that must attest a
	// release before it is shown as healthy on the open network (>= 1).
	MinCorroboration int `yaml:"min_corroboration" mapstructure:"min_corroboration" json:"min_corroboration"`
}

// PeernetOpenNetworkConfig holds the open-network exposure level and the
// peer addresses the user entered.
type PeernetOpenNetworkConfig struct {
	Level PeernetLevel `yaml:"level" mapstructure:"level" json:"level"`
	// Peers are host:port addresses (no scheme, userinfo or path).
	Peers []string `yaml:"peers" mapstructure:"peers" json:"peers"`
	// FastPath lets layouts offered by open-network peers replace archive
	// analysis when the user imports an NZB. Off by default: such a layout is
	// verified by sampling only. Private networks do not need it (their
	// offers use the fast path by default); direct imports (Stremio peer play)
	// are unaffected.
	FastPath bool `yaml:"fast_path" mapstructure:"fast_path" json:"fast_path"`
}

// PeernetSharingConfig holds publish-time exclusions.
type PeernetSharingConfig struct {
	ExcludedCategories []string `yaml:"excluded_categories" mapstructure:"excluded_categories" json:"excluded_categories"`
	ExcludedIndexers   []string `yaml:"excluded_indexers" mapstructure:"excluded_indexers" json:"excluded_indexers"`
}

// defaultPeernetConfig returns the opt-in defaults: feature off, level 0.
func defaultPeernetConfig() PeernetConfig {
	enabled := false
	return PeernetConfig{
		Enabled:          &enabled,
		ListenPort:       DefaultPeernetListenPort,
		OpenNetwork:      PeernetOpenNetworkConfig{Level: PeernetLevelOff, Peers: []string{}},
		Sharing:          PeernetSharingConfig{ExcludedCategories: []string{}, ExcludedIndexers: []string{}},
		MinCorroboration: DefaultPeernetMinCorroboration,
	}
}

// isUnset reports whether the section is entirely zero-valued (never populated).
func (p PeernetConfig) isUnset() bool {
	return p.Enabled == nil && !p.AcceptIncoming && p.ListenPort == 0 &&
		p.AdvertiseAddress == "" && p.OpenNetwork.Level == 0 &&
		len(p.OpenNetwork.Peers) == 0 && len(p.Sharing.ExcludedCategories) == 0 &&
		len(p.Sharing.ExcludedIndexers) == 0 && p.MinCorroboration == 0
}

// validatePeernetAddress checks a bare host:port with the same validator the
// peer transport uses (transport.ValidateAddress), so an address that saves
// here is never dropped later for failing a stricter check.
func validatePeernetAddress(addr string) error {
	if _, err := transport.ValidateAddress(addr); err != nil {
		return fmt.Errorf("%q must be a valid host:port without scheme, userinfo or path: %w", addr, err)
	}
	return nil
}

// Validate checks every field, whether or not the feature is enabled.
func (p PeernetConfig) Validate() error {
	if p.ListenPort < 1024 || p.ListenPort > 65535 {
		return fmt.Errorf("peernet listen_port must be between 1024 and 65535")
	}
	if p.AdvertiseAddress != "" {
		if err := validatePeernetAddress(p.AdvertiseAddress); err != nil {
			return fmt.Errorf("peernet advertise_address: %w", err)
		}
	}
	if !p.OpenNetwork.Level.Valid() {
		return fmt.Errorf("peernet open_network level must be between 0 and 3")
	}
	if len(p.OpenNetwork.Peers) > MaxPeernetPeers {
		return fmt.Errorf("peernet open_network peers may list at most %d addresses", MaxPeernetPeers)
	}
	for i, peer := range p.OpenNetwork.Peers {
		if err := validatePeernetAddress(peer); err != nil {
			return fmt.Errorf("peernet open_network peers[%d]: %w", i, err)
		}
		// Loopback can never be dialled (it is refused by the transport), so
		// saving it would only hide a mistake.
		if pa, _ := transport.ValidateAddress(peer); transport.IsLoopbackAddress(pa) {
			return fmt.Errorf("peernet open_network peers[%d]: loopback addresses are never allowed", i)
		}
	}
	if p.MinCorroboration < 1 {
		return fmt.Errorf("peernet min_corroboration must be at least 1")
	}
	return nil
}

func peernetNameExcluded(list []string, name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}
	for _, entry := range list {
		if strings.ToLower(strings.TrimSpace(entry)) == name {
			return true
		}
	}
	return false
}

// Accessors below are nil-safe on *Config and treat a disabled feature as
// level 0 / nothing allowed.

// PeernetEnabled reports whether the peer network feature is on.
func (c *Config) PeernetEnabled() bool {
	return c != nil && c.Peernet.Enabled != nil && *c.Peernet.Enabled
}

// PeernetAcceptIncoming reports whether the peer listener should run.
func (c *Config) PeernetAcceptIncoming() bool {
	return c.PeernetEnabled() && c.Peernet.AcceptIncoming
}

// PeernetOpenNetworkLevel returns the open-network level; Off when the feature
// is disabled or the stored value is out of range.
func (c *Config) PeernetOpenNetworkLevel() PeernetLevel {
	if !c.PeernetEnabled() || !c.Peernet.OpenNetwork.Level.Valid() {
		return PeernetLevelOff
	}
	return c.Peernet.OpenNetwork.Level
}

// PeernetMayPublishMetadata reports whether the open network level allows
// publishing release records and health attestations (level >= 2).
func (c *Config) PeernetMayPublishMetadata() bool {
	return c.PeernetOpenNetworkLevel() >= PeernetLevelShareMetadata
}

// PeernetMayServeBundles reports whether the open network level allows serving
// bundles (level >= 3).
func (c *Config) PeernetMayServeBundles() bool {
	return c.PeernetOpenNetworkLevel() >= PeernetLevelShareBundles
}

// PeernetListenPort returns the peer listener port, defaulting when unset.
func (c *Config) PeernetListenPort() int {
	if c == nil || c.Peernet.ListenPort <= 0 {
		return DefaultPeernetListenPort
	}
	return c.Peernet.ListenPort
}

// PeernetMinCorroboration returns the corroboration threshold, defaulting to 3
// when unset or invalid.
func (c *Config) PeernetMinCorroboration() int {
	if c == nil || c.Peernet.MinCorroboration < 1 {
		return DefaultPeernetMinCorroboration
	}
	return c.Peernet.MinCorroboration
}

// PeernetCategoryExcluded reports whether releases in the category must never
// be published (case-insensitive, trimmed).
func (c *Config) PeernetCategoryExcluded(category string) bool {
	return c != nil && peernetNameExcluded(c.Peernet.Sharing.ExcludedCategories, category)
}

// PeernetIndexerExcluded reports whether releases from the indexer must never
// be published (case-insensitive, trimmed).
func (c *Config) PeernetIndexerExcluded(indexer string) bool {
	return c != nil && peernetNameExcluded(c.Peernet.Sharing.ExcludedIndexers, indexer)
}
