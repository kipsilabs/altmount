// Package service owns the lifecycle of the peer network feature: identity,
// the optional peer listener, the outbound client, membership, discovery and
// the content index. When the feature is off nothing here exists: no key, no
// listener, no client, no goroutines and no metadata observer.
package service

import (
	"slices"

	"github.com/kipsilabs/altmount/internal/config"
)

// Settings is the part of the configuration this package may read: the
// peernet section and the outbound proxy settings. Nothing else from the
// configuration (providers, ARR clients, users) is visible here.
type Settings struct {
	Enabled          bool
	AcceptIncoming   bool
	ListenPort       int
	AdvertiseAddress string
	// OpenLevel is the open network's exposure level, taken from the
	// configuration only (0 when the feature is disabled).
	OpenLevel int
	// OpenPeers are the user-entered open-network peer addresses.
	OpenPeers []string

	HTTPProxy  string
	HTTPSProxy string
	NoProxy    string
}

// SettingsFromConfig extracts Settings from cfg. A nil cfg or a disabled
// feature yields the zero (off) Settings.
func SettingsFromConfig(cfg *config.Config) Settings {
	if !cfg.PeernetEnabled() {
		return Settings{}
	}
	return Settings{
		Enabled:          true,
		AcceptIncoming:   cfg.PeernetAcceptIncoming(),
		ListenPort:       cfg.PeernetListenPort(),
		AdvertiseAddress: cfg.Peernet.AdvertiseAddress,
		OpenLevel:        int(cfg.PeernetOpenNetworkLevel()),
		OpenPeers:        slices.Clone(cfg.Peernet.OpenNetwork.Peers),
		HTTPProxy:        cfg.Network.HTTPProxy,
		HTTPSProxy:       cfg.Network.HTTPSProxy,
		NoProxy:          cfg.Network.NoProxy,
	}
}

// listenerChanged reports whether a change requires rebuilding the listener
// and client (as opposed to just re-syncing networks).
func (s Settings) listenerChanged(o Settings) bool {
	return s.AcceptIncoming != o.AcceptIncoming || s.ListenPort != o.ListenPort ||
		s.HTTPProxy != o.HTTPProxy || s.HTTPSProxy != o.HTTPSProxy || s.NoProxy != o.NoProxy
}

// Equal reports whether two Settings describe the same configuration.
func (s Settings) Equal(o Settings) bool {
	return s.Enabled == o.Enabled && s.AcceptIncoming == o.AcceptIncoming && s.ListenPort == o.ListenPort &&
		s.AdvertiseAddress == o.AdvertiseAddress && s.OpenLevel == o.OpenLevel &&
		slices.Equal(s.OpenPeers, o.OpenPeers) &&
		s.HTTPProxy == o.HTTPProxy && s.HTTPSProxy == o.HTTPSProxy && s.NoProxy == o.NoProxy
}
