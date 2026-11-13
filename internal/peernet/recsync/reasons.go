package recsync

import "fmt"

// Reason says why a record was rejected.
type Reason uint8

// Rejection reasons. Names (String) are stable: they key the counters shown
// in the status.
const (
	reasonNone Reason = iota
	ReasonOversize
	ReasonMalformed
	ReasonUnsupportedVersion
	ReasonInvalid
	ReasonWrongNetwork
	ReasonFuture
	ReasonExpired
	ReasonStale
	ReasonBadSignature
	ReasonSelfPublisher
	ReasonNotOwn
	ReasonQuotaPublisher
	ReasonQuotaStorage
	reasonCount
)

var reasonNames = [reasonCount]string{
	ReasonOversize:           "oversize",
	ReasonMalformed:          "malformed",
	ReasonUnsupportedVersion: "unsupported_version",
	ReasonInvalid:            "invalid",
	ReasonWrongNetwork:       "wrong_network",
	ReasonFuture:             "from_future",
	ReasonExpired:            "expired",
	ReasonStale:              "stale",
	ReasonBadSignature:       "bad_signature",
	ReasonSelfPublisher:      "self_publisher",
	ReasonNotOwn:             "not_own_key",
	ReasonQuotaPublisher:     "quota_publisher",
	ReasonQuotaStorage:       "quota_storage",
}

func (r Reason) String() string {
	if r > reasonNone && r < reasonCount {
		return reasonNames[r]
	}
	return fmt.Sprintf("reason(%d)", uint8(r))
}

// Hostile reports whether a record rejected for this reason is evidence that
// the peer that sent it misbehaves, as opposed to being merely old, ahead of
// our clock, newer than our schema, over a local quota or an echo of our own
// record. Only hostile rejections count towards a peer's back-off.
func (r Reason) Hostile() bool {
	switch r {
	case ReasonOversize, ReasonMalformed, ReasonInvalid, ReasonWrongNetwork, ReasonBadSignature:
		return true
	}
	return false
}
