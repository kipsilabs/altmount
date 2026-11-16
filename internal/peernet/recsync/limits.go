package recsync

import (
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// Wire limits. They are constants because both ends must agree on them.
const (
	// PageRecords is the most records the server puts in one page.
	PageRecords = 200
	// MaxPushRecords is the most records accepted in one push.
	MaxPushRecords = 100

	// frameOverhead bounds the tag and length prefix of one record.
	frameOverhead = 8
	// MaxPageBytes is the largest page body the client accepts.
	MaxPageBytes = PageRecords*(records.MaxEncodedSize+frameOverhead) + 128
	// MaxPushBody is the largest push request body the server accepts.
	MaxPushBody = MaxPushRecords*(records.MaxEncodedSize+frameOverhead) + 128
	// MaxPushResultBytes is the largest push response body the client accepts.
	MaxPushResultBytes = 64

	// DefaultIngestRate and DefaultIngestBurst bound, node-wide and across all
	// peers and both directions, the records per second that reach signature
	// verification and a database write. 200/s is far above what an honest
	// network produces (a library check publishes a handful of records) and
	// keeps verification below ~1% of a core; a burst of 400 absorbs two pages.
	DefaultIngestRate  = 200.0
	DefaultIngestBurst = 400

	minEpochLen = 16
	maxEpochLen = 64
)

// Limits are the tunable client and server limits. A zero field takes its
// default. They live in one place so they are easy to review.
type Limits struct {
	// PullInterval is the base period between sync rounds (jittered by the
	// caller). BacklogInterval replaces it while a peer still has more.
	PullInterval    time.Duration
	BacklogInterval time.Duration

	// MaxPagesPerPeerRound bounds the pages pulled from one peer in a round,
	// MaxPushBatchesPerPeerRound the push batches sent to one peer.
	MaxPagesPerPeerRound       int
	MaxPushBatchesPerPeerRound int
	// PeerTimeout bounds one peer's pull or push work in a round; RoundTimeout
	// bounds the whole round.
	PeerTimeout  time.Duration
	RoundTimeout time.Duration
	// CallTimeout bounds one request.
	CallTimeout time.Duration
	// MaxPeersPerNetwork is how many verified peers a round talks to per
	// network; MaxConcurrentPeers how many at once overall.
	MaxPeersPerNetwork int
	MaxConcurrentPeers int
	// MaxPeerFailures excludes verified peers with this many consecutive
	// contact failures.
	MaxPeerFailures int

	// InvalidPerRound is how many hostile-looking records from one peer in one
	// round cause a back-off of that peer. PeerBackoffBase doubles per
	// consecutive back-off up to PeerBackoffMax.
	InvalidPerRound int
	PeerBackoffBase time.Duration
	PeerBackoffMax  time.Duration

	// PushRecordsPerMinute is the records one peer may push per minute;
	// MaxConcurrentPushes the pushes being ingested at once, node-wide.
	// MaxTrackedPushers bounds the memory of the per-peer accounting.
	PushRecordsPerMinute int
	MaxConcurrentPushes  int
	MaxTrackedPushers    int

	// PurgeBatch and PurgeBatchesPerPass bound the expiry purge of one upkeep
	// pass.
	PurgeBatch          int
	PurgeBatchesPerPass int

	// IngestRate and IngestBurst are the node-wide token bucket in records per
	// second (see DefaultIngestRate).
	IngestRate  float64
	IngestBurst int

	// MaxEpochResetsPerHour bounds how often one peer may change its epoch.
	MaxEpochResetsPerHour int
	// MaxPushesPerNode and MaxPushesPerIP cap concurrent pushes (body read
	// included) per authenticated node and per remote IP.
	MaxPushesPerNode int
	MaxPushesPerIP   int
	// ReconcileEveryPasses is how many upkeep passes lie between two counter
	// reconciliations of the record store.
	ReconcileEveryPasses int

	// Store limits (per network), see database.PeernetRecordLimits. Zero uses
	// the store defaults, which are defined in one place (the database
	// package).
	MaxPerPublisher int
	MaxPerRelayer   int
	MaxPerNetwork   int
}

// DefaultLimits returns the defaults. Numbers are design calls.
func DefaultLimits() Limits {
	return Limits{
		PullInterval:               2 * time.Minute,
		BacklogInterval:            5 * time.Second,
		MaxPagesPerPeerRound:       50,
		MaxPushBatchesPerPeerRound: 10,
		PeerTimeout:                45 * time.Second,
		RoundTimeout:               3 * time.Minute,
		CallTimeout:                20 * time.Second,
		MaxPeersPerNetwork:         8,
		MaxConcurrentPeers:         4,
		MaxPeerFailures:            5,
		InvalidPerRound:            20,
		PeerBackoffBase:            10 * time.Minute,
		PeerBackoffMax:             2 * time.Hour,
		PushRecordsPerMinute:       3000,
		MaxConcurrentPushes:        8,
		MaxTrackedPushers:          2048,
		PurgeBatch:                 500,
		PurgeBatchesPerPass:        20,
		IngestRate:                 DefaultIngestRate,
		IngestBurst:                DefaultIngestBurst,
		MaxEpochResetsPerHour:      3,
		MaxPushesPerNode:           1,
		MaxPushesPerIP:             4,
		ReconcileEveryPasses:       120,
	}
}

// WithDefaults fills zero fields from DefaultLimits.
func (l Limits) WithDefaults() Limits {
	d := DefaultLimits()
	dur := func(v *time.Duration, def time.Duration) {
		if *v <= 0 {
			*v = def
		}
	}
	num := func(v *int, def int) {
		if *v <= 0 {
			*v = def
		}
	}
	dur(&l.PullInterval, d.PullInterval)
	dur(&l.BacklogInterval, d.BacklogInterval)
	num(&l.MaxPagesPerPeerRound, d.MaxPagesPerPeerRound)
	num(&l.MaxPushBatchesPerPeerRound, d.MaxPushBatchesPerPeerRound)
	dur(&l.PeerTimeout, d.PeerTimeout)
	dur(&l.RoundTimeout, d.RoundTimeout)
	dur(&l.CallTimeout, d.CallTimeout)
	num(&l.MaxPeersPerNetwork, d.MaxPeersPerNetwork)
	num(&l.MaxConcurrentPeers, d.MaxConcurrentPeers)
	num(&l.MaxPeerFailures, d.MaxPeerFailures)
	num(&l.InvalidPerRound, d.InvalidPerRound)
	dur(&l.PeerBackoffBase, d.PeerBackoffBase)
	dur(&l.PeerBackoffMax, d.PeerBackoffMax)
	num(&l.PushRecordsPerMinute, d.PushRecordsPerMinute)
	num(&l.MaxConcurrentPushes, d.MaxConcurrentPushes)
	num(&l.MaxTrackedPushers, d.MaxTrackedPushers)
	num(&l.PurgeBatch, d.PurgeBatch)
	num(&l.PurgeBatchesPerPass, d.PurgeBatchesPerPass)
	if l.IngestRate <= 0 {
		l.IngestRate = d.IngestRate
	}
	num(&l.IngestBurst, d.IngestBurst)
	num(&l.MaxEpochResetsPerHour, d.MaxEpochResetsPerHour)
	num(&l.MaxPushesPerNode, d.MaxPushesPerNode)
	num(&l.MaxPushesPerIP, d.MaxPushesPerIP)
	num(&l.ReconcileEveryPasses, d.ReconcileEveryPasses)
	return l
}
