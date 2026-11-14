package bundlex

import "time"

// Wire constants.
const (
	// PathBundle is the serve route: GET with query n (network) and b (Bundle ID).
	PathBundle  = "/peer/v1/bundle"
	queryNet    = "n"
	queryBundle = "b"
)

// ServeLimits bound what this node serves. Zero fields take the defaults.
// Numbers are design calls: a bundle is up to 8 MB, so a handful an hour per
// peer is plenty for on-demand imports.
type ServeLimits struct {
	// PerPeerPerHour and PerPeerBurst: bundle requests per authenticated peer.
	PerPeerPerHour, PerPeerBurst int
	// PerIPPerHour and PerIPBurst: requests per public remote address. Private,
	// loopback and link-local addresses are shared by honest nodes of one LAN
	// and are not limited per address.
	PerIPPerHour, PerIPBurst int
	// BytesPerMinute and BytesBurst: node-wide budget of bundle bytes sent.
	BytesPerMinute, BytesBurst int64
	// MaxConcurrent, MaxConcurrentPerPeer and MaxConcurrentPerIP cap bundles
	// being served at once.
	MaxConcurrent, MaxConcurrentPerPeer, MaxConcurrentPerIP int
	// ChunkDeadline bounds each write; TotalDeadline the whole response.
	ChunkDeadline, TotalDeadline time.Duration
	// MaxTracked bounds the memory of the per-peer and per-address accounting.
	MaxTracked int
}

// DefaultServeLimits returns the defaults.
func DefaultServeLimits() ServeLimits {
	return ServeLimits{
		PerPeerPerHour: 30, PerPeerBurst: 5,
		PerIPPerHour: 60, PerIPBurst: 10,
		BytesPerMinute: 128 << 20, BytesBurst: 32 << 20,
		MaxConcurrent: 4, MaxConcurrentPerPeer: 1, MaxConcurrentPerIP: 2,
		ChunkDeadline: 15 * time.Second, TotalDeadline: 60 * time.Second,
		MaxTracked: 4096,
	}
}

func (l ServeLimits) withDefaults() ServeLimits {
	d := DefaultServeLimits()
	i := func(v *int, def int) {
		if *v <= 0 {
			*v = def
		}
	}
	i(&l.PerPeerPerHour, d.PerPeerPerHour)
	i(&l.PerPeerBurst, d.PerPeerBurst)
	i(&l.PerIPPerHour, d.PerIPPerHour)
	i(&l.PerIPBurst, d.PerIPBurst)
	i(&l.MaxConcurrent, d.MaxConcurrent)
	i(&l.MaxConcurrentPerPeer, d.MaxConcurrentPerPeer)
	i(&l.MaxConcurrentPerIP, d.MaxConcurrentPerIP)
	i(&l.MaxTracked, d.MaxTracked)
	if l.BytesPerMinute <= 0 {
		l.BytesPerMinute = d.BytesPerMinute
	}
	if l.BytesBurst <= 0 {
		l.BytesBurst = d.BytesBurst
	}
	if l.ChunkDeadline <= 0 {
		l.ChunkDeadline = d.ChunkDeadline
	}
	if l.TotalDeadline <= 0 {
		l.TotalDeadline = d.TotalDeadline
	}
	return l
}

// FetchLimits bound what this node downloads. Fetching happens only on demand
// from a local caller, never speculatively.
type FetchLimits struct {
	// MaxAttempts is how many offers one Fetch tries.
	MaxAttempts int
	// GlobalPerHour/GlobalBurst and PerPublisherPerHour/PerPublisherBurst are
	// download attempts.
	GlobalPerHour, GlobalBurst             int
	PerPublisherPerHour, PerPublisherBurst int
	// MaxConcurrent caps downloads at once.
	MaxConcurrent int
	// Timeout bounds one download including the body.
	Timeout time.Duration
	// BackoffBase and BackoffMax: hostile publishers are skipped for a time that
	// doubles per strike. SoftBackoff is the skip after an ordinary failure.
	BackoffBase, BackoffMax, SoftBackoff time.Duration
	// FullCheckBackoffBase, FullCheckBackoffMax and FullCheckDecay shape the mild
	// penalty of a publisher whose verified bundle failed the local full check.
	FullCheckBackoffBase, FullCheckBackoffMax, FullCheckDecay time.Duration
	// MaxTracked bounds the accounting tables.
	MaxTracked int
	// MaxOffers bounds the offers read from the store.
	MaxOffers int
}

// DefaultFetchLimits returns the defaults.
func DefaultFetchLimits() FetchLimits {
	return FetchLimits{
		MaxAttempts: 3, GlobalPerHour: 60, GlobalBurst: 10, PerPublisherPerHour: 12, PerPublisherBurst: 3,
		MaxConcurrent: 2, Timeout: 60 * time.Second,
		BackoffBase: 10 * time.Minute, BackoffMax: 2 * time.Hour, SoftBackoff: 5 * time.Minute,
		FullCheckBackoffBase: 30 * time.Minute, FullCheckBackoffMax: 6 * time.Hour, FullCheckDecay: 24 * time.Hour,
		MaxTracked: 2048, MaxOffers: 32,
	}
}

func (l FetchLimits) withDefaults() FetchLimits {
	d := DefaultFetchLimits()
	i := func(v *int, def int) {
		if *v <= 0 {
			*v = def
		}
	}
	dur := func(v *time.Duration, def time.Duration) {
		if *v <= 0 {
			*v = def
		}
	}
	i(&l.MaxAttempts, d.MaxAttempts)
	i(&l.GlobalPerHour, d.GlobalPerHour)
	i(&l.GlobalBurst, d.GlobalBurst)
	i(&l.PerPublisherPerHour, d.PerPublisherPerHour)
	i(&l.PerPublisherBurst, d.PerPublisherBurst)
	i(&l.MaxConcurrent, d.MaxConcurrent)
	i(&l.MaxTracked, d.MaxTracked)
	i(&l.MaxOffers, d.MaxOffers)
	dur(&l.Timeout, d.Timeout)
	dur(&l.BackoffBase, d.BackoffBase)
	dur(&l.BackoffMax, d.BackoffMax)
	dur(&l.SoftBackoff, d.SoftBackoff)
	dur(&l.FullCheckBackoffBase, d.FullCheckBackoffBase)
	dur(&l.FullCheckBackoffMax, d.FullCheckBackoffMax)
	dur(&l.FullCheckDecay, d.FullCheckDecay)
	return l
}
