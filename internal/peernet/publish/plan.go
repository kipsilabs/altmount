package publish

import (
	"sort"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// Default planner intervals.
const (
	// DefaultMinRepublish is the shortest gap between two publications of the
	// same verdict for the same file on one network.
	DefaultMinRepublish = 24 * time.Hour
	// DefaultRecordRefresh is the age at which a release record is announced
	// again. It leaves 10 days of the 30-day lifetime as margin so a node that
	// is briefly offline does not let its live data expire.
	DefaultRecordRefresh = 20 * 24 * time.Hour
)

// PlanConfig tunes the republish planner. Zero values take the defaults.
type PlanConfig struct {
	MinRepublish  time.Duration
	RecordRefresh time.Duration
}

func (c PlanConfig) withDefaults() PlanConfig {
	if c.MinRepublish <= 0 {
		c.MinRepublish = DefaultMinRepublish
	}
	if c.RecordRefresh <= 0 {
		c.RecordRefresh = DefaultRecordRefresh
	}
	if c.RecordRefresh >= records.RecordLifetime {
		c.RecordRefresh = records.RecordLifetime - 24*time.Hour
	}
	return c
}

// State is what is remembered about the last publication of one record.
type State struct {
	PublishedAt time.Time
	// CheckedAt and Verdict are set for attestations only.
	CheckedAt time.Time
	Verdict   Verdict
	// OriginCategory and OriginIndexer are the lower-cased category and
	// indexer the release had when it was published. They are local
	// bookkeeping for retracting records when an exclusion is added later and
	// are never put in a record.
	OriginCategory string
	OriginIndexer  string
	// BundleID is the Bundle ID the release record announced (zero: none) and
	// BundleLevel the exposure level the record was published at. Release
	// records only. A transient bundle failure stores nothing: the release is
	// skipped and retried.
	BundleID    records.BundleID
	BundleLevel int
}

// DueRelease reports whether a release record should be published now: never
// published, or old enough that it must be announced again before it expires.
// A last time in the future (the clock went backwards) is not due.
func (c PlanConfig) DueRelease(last *State, now time.Time) bool {
	c = c.withDefaults()
	if last == nil {
		return true
	}
	return now.Sub(last.PublishedAt) >= c.RecordRefresh
}

// DueAttestation reports whether an attestation for a check made at checkedAt
// should be published now.
//
// Freshness of an attestation is measured from its check time, so
// re-announcing an old check never extends it: only a check newer than the one
// last published is ever due. Among newer checks, a changed verdict is due at
// once and an identical verdict waits MinRepublish since the last publication.
func (c PlanConfig) DueAttestation(last *State, verdict Verdict, checkedAt, now time.Time) bool {
	c = c.withDefaults()
	if last == nil {
		return true
	}
	if !checkedAt.After(last.CheckedAt) {
		return false
	}
	if verdict != last.Verdict {
		return true
	}
	return now.Sub(last.PublishedAt) >= c.MinRepublish
}

// Tracked is one remembered publication, as listed by the persistence layer
// for the periodic re-announce sweep.
type Tracked struct {
	Network   netid.NetworkID
	Kind      records.Kind
	ReleaseID records.ReleaseID
	FileID    records.FileID
	State     State
}

// RecordsDueForReannounce returns the release records whose refresh age has
// been reached, oldest first, on a copy. Attestations are not listed: they
// are refreshed by new local checks (see DueAttestation).
func (c PlanConfig) RecordsDueForReannounce(entries []Tracked, now time.Time) []Tracked {
	var out []Tracked
	for _, e := range entries {
		if e.Kind != records.KindRelease {
			continue
		}
		st := e.State
		if c.DueRelease(&st, now) {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].State.PublishedAt.Before(out[j].State.PublishedAt) })
	return out
}
