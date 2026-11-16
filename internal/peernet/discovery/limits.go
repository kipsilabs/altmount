package discovery

import "time"

// Limits are the tunable hard limits. Zero fields take the defaults from
// DefaultLimits.
type Limits struct {
	// MaxPeersPerNetwork caps stored rows per network (user-entered peers are
	// never evicted to satisfy it).
	MaxPeersPerNetwork int
	// MaxExchangePeers, MaxInboundPeers and MaxLocalPeers cap rows per source.
	MaxExchangePeers int
	MaxInboundPeers  int
	MaxLocalPeers    int
	// MaxUnverifiedPerPrefix caps stored unverified learned rows per address
	// prefix (/24 IPv4, /48 IPv6). Past it the prefix's own stalest rows are
	// replaced, so one range cannot crowd out the table.
	MaxUnverifiedPerPrefix int
	// ExchangeSampleSize is how many peers an exchange response carries.
	ExchangeSampleSize int
	// MaxNewPerExchange is how many new entries one exchange response may add.
	MaxNewPerExchange int
	// HelloInterval is the minimum time between advertised addresses accepted
	// from one peer for one network.
	HelloInterval time.Duration
	// MaxTrackedHello bounds the memory of each table of the hello limiter
	// (per node and per address prefix); the oldest entry is evicted when a
	// table is full.
	MaxTrackedHello int
	// HelloPerPrefixPerInterval is how many hellos one source address prefix
	// (/24 IPv4, /64 IPv6) may have processed per HelloInterval, however many
	// identities it uses.
	HelloPerPrefixPerInterval int
	// HelloGlobalPerWindow caps hellos processed across all peers per
	// HelloGlobalWindow.
	HelloGlobalPerWindow int
	HelloGlobalWindow    time.Duration
	// DialBacksPerWindow caps, across all networks, the contacts of peers that
	// are not yet verified (dial-backs of advertised and exchanged addresses)
	// per DialBackWindow.
	DialBacksPerWindow int
	DialBackWindow     time.Duration
	// DialBacksPerPrefix caps, per DialBackPrefixWindow, the contacts of
	// unverified peers inside one target prefix (/24 IPv4, /48 IPv6, or one
	// hostname), so a target network cannot be made to receive more than a few
	// probes per hour from this node. MaxTrackedDialPrefixes bounds the table.
	DialBacksPerPrefix     int
	DialBackPrefixWindow   time.Duration
	MaxTrackedDialPrefixes int
	// MaxTickDuration is the wall-clock cap of one Tick across all networks;
	// contacts still pending are deferred to the next Tick.
	MaxTickDuration time.Duration
	// MaxContactsPerTick and MaxConcurrentContacts bound a Tick, per network.
	MaxContactsPerTick    int
	MaxConcurrentContacts int
	// Backoff after consecutive failures: base doubling up to max.
	UserBackoffBase time.Duration
	UserBackoffMax  time.Duration
	PeerBackoffBase time.Duration
	PeerBackoffMax  time.Duration
	// RefreshInterval is the time to the next contact after a success.
	RefreshInterval time.Duration
	// ActiveWindow is how recent a success must be for a peer to count as
	// connected.
	ActiveWindow time.Duration
	// PruneAfter and PruneFailures select stale failing peers to delete.
	PruneAfter    time.Duration
	PruneFailures int
	// ExchangeMaxFailures hides a verified peer from exchange once it has this
	// many consecutive failures.
	ExchangeMaxFailures int
	// CallTimeout bounds one outbound call.
	CallTimeout time.Duration
	// DefaultTargetPeers and DefaultMaxPeers apply when a network leaves its
	// own values zero.
	DefaultTargetPeers int
	DefaultMaxPeers    int
}

// DefaultLimits returns conservative defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxPeersPerNetwork:        512,
		MaxExchangePeers:          256,
		MaxInboundPeers:           64,
		MaxLocalPeers:             64,
		MaxUnverifiedPerPrefix:    8,
		ExchangeSampleSize:        MaxPeerEntries,
		MaxNewPerExchange:         5,
		HelloInterval:             10 * time.Minute,
		MaxTrackedHello:           4096,
		HelloPerPrefixPerInterval: 8,
		HelloGlobalPerWindow:      120,
		HelloGlobalWindow:         time.Minute,
		DialBacksPerWindow:        20,
		DialBackWindow:            time.Minute,
		DialBacksPerPrefix:        3,
		DialBackPrefixWindow:      time.Hour,
		MaxTrackedDialPrefixes:    4096,
		MaxTickDuration:           60 * time.Second,
		MaxContactsPerTick:        16,
		MaxConcurrentContacts:     4,
		UserBackoffBase:           15 * time.Second,
		UserBackoffMax:            5 * time.Minute,
		PeerBackoffBase:           time.Minute,
		PeerBackoffMax:            time.Hour,
		RefreshInterval:           5 * time.Minute,
		ActiveWindow:              15 * time.Minute,
		PruneAfter:                24 * time.Hour,
		PruneFailures:             5,
		ExchangeMaxFailures:       3,
		CallTimeout:               20 * time.Second,
		DefaultTargetPeers:        8,
		DefaultMaxPeers:           32,
	}
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits()
	pi := func(v *int, def int) {
		if *v <= 0 {
			*v = def
		}
	}
	pd := func(v *time.Duration, def time.Duration) {
		if *v <= 0 {
			*v = def
		}
	}
	pi(&l.MaxPeersPerNetwork, d.MaxPeersPerNetwork)
	pi(&l.MaxExchangePeers, d.MaxExchangePeers)
	pi(&l.MaxInboundPeers, d.MaxInboundPeers)
	pi(&l.MaxLocalPeers, d.MaxLocalPeers)
	pi(&l.MaxUnverifiedPerPrefix, d.MaxUnverifiedPerPrefix)
	pi(&l.ExchangeSampleSize, d.ExchangeSampleSize)
	if l.ExchangeSampleSize > MaxPeerEntries {
		l.ExchangeSampleSize = MaxPeerEntries
	}
	pi(&l.MaxNewPerExchange, d.MaxNewPerExchange)
	pd(&l.HelloInterval, d.HelloInterval)
	pi(&l.MaxTrackedHello, d.MaxTrackedHello)
	pi(&l.HelloPerPrefixPerInterval, d.HelloPerPrefixPerInterval)
	pi(&l.HelloGlobalPerWindow, d.HelloGlobalPerWindow)
	pd(&l.HelloGlobalWindow, d.HelloGlobalWindow)
	pi(&l.DialBacksPerWindow, d.DialBacksPerWindow)
	pd(&l.DialBackWindow, d.DialBackWindow)
	pi(&l.DialBacksPerPrefix, d.DialBacksPerPrefix)
	pd(&l.DialBackPrefixWindow, d.DialBackPrefixWindow)
	pi(&l.MaxTrackedDialPrefixes, d.MaxTrackedDialPrefixes)
	pd(&l.MaxTickDuration, d.MaxTickDuration)
	pi(&l.MaxContactsPerTick, d.MaxContactsPerTick)
	pi(&l.MaxConcurrentContacts, d.MaxConcurrentContacts)
	pd(&l.UserBackoffBase, d.UserBackoffBase)
	pd(&l.UserBackoffMax, d.UserBackoffMax)
	pd(&l.PeerBackoffBase, d.PeerBackoffBase)
	pd(&l.PeerBackoffMax, d.PeerBackoffMax)
	pd(&l.RefreshInterval, d.RefreshInterval)
	pd(&l.ActiveWindow, d.ActiveWindow)
	pd(&l.PruneAfter, d.PruneAfter)
	pi(&l.PruneFailures, d.PruneFailures)
	pi(&l.ExchangeMaxFailures, d.ExchangeMaxFailures)
	pd(&l.CallTimeout, d.CallTimeout)
	pi(&l.DefaultTargetPeers, d.DefaultTargetPeers)
	pi(&l.DefaultMaxPeers, d.DefaultMaxPeers)
	return l
}

// backoff returns base*2^(failures-1) capped at max.
func backoff(base, max time.Duration, failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	d := base
	for i := 1; i < failures; i++ {
		d *= 2
		if d >= max || d <= 0 {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}
