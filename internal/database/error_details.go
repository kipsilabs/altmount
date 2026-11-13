package database

import (
	"encoding/json"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
)

// HealthErrorDetails is the structured JSON stored in file_health.error_details.
// Both the health checker and the streaming failure path marshal this envelope;
// the frontend parses it to render playback-impact information. Legacy rows may
// contain other ad-hoc JSON shapes or plain strings — parsers must tolerate that.
type HealthErrorDetails struct {
	ErrorType       string        `json:"error_type,omitempty"`
	Message         string        `json:"message,omitempty"`
	MissingArticles int           `json:"missing_articles,omitempty"`
	TotalArticles   int           `json:"total_articles,omitempty"`
	Sampled         int           `json:"sampled,omitempty"`
	PlaybackImpact  *holes.Impact `json:"playback_impact,omitempty"`

	// UnresolvedSegments counts segments whose availability was never
	// established — transport failures, or ids the sweep never reached.
	// They are deliberately not counted as missing.
	UnresolvedSegments int `json:"unresolved_segments,omitempty"`

	// TerminatedEarly marks a result produced by a check that stopped before
	// examining every planned segment, so Sampled is a partial count and the
	// missing-segment map is incomplete by design.
	TerminatedEarly bool `json:"terminated_early,omitempty"`

	// TerminationReason explains why the check stopped early.
	TerminationReason string `json:"termination_reason,omitempty"`

	// ContentVerification records what the media-container header probe
	// concluded, and is the one field also written on an otherwise-healthy
	// record: without it a clean result is indistinguishable from a file that
	// was never probed at all. Empty means verification did not run — the
	// feature is off, or the file is not an eligible media type.
	ContentVerification string `json:"content_verification,omitempty"`

	// PeerDeferredAt is set while a due check of a healthy file was deferred
	// once on the strength of peer evidence and no real check has run since.
	// Every real check rewrites error_details, which ends the deferral.
	PeerDeferredAt string `json:"peer_deferred_at,omitempty"`

	// PeerReducedAt is set on a healthy record whose last check ran with a
	// sample lowered by peer advice. The next check of the file must be a
	// normal one; any normal check rewrites error_details and ends it.
	PeerReducedAt string `json:"peer_reduced_at,omitempty"`

	// Legacy holds details from before this envelope existed (a bare string,
	// an array or plain text) that a marker had to be added next to.
	Legacy string `json:"legacy,omitempty"`
}

// ContentVerification values for HealthErrorDetails.ContentVerification.
const (
	// ContentVerificationPassed: a recognized media container signature was
	// found in the file's header.
	ContentVerificationPassed = "passed"
	// ContentVerificationFailed: the probe definitively rejected the file —
	// no recognized signature, or its head article is missing.
	ContentVerificationFailed = "failed"
	// ContentVerificationUnavailable: the probe could not complete (transient
	// provider, timeout, or connection error), so the file's content is
	// unproven rather than bad.
	ContentVerificationUnavailable = "unavailable"
)

// Marshal renders the envelope for storage, returning nil on the (practically
// impossible) marshal error so callers can assign it directly to *string fields.
func (d *HealthErrorDetails) Marshal() *string {
	data, err := json.Marshal(d)
	if err != nil {
		return nil
	}
	s := string(data)
	return &s
}

func parseDetails(raw *string) (HealthErrorDetails, bool) {
	var d HealthErrorDetails
	if raw == nil || *raw == "" {
		return d, false
	}
	if err := json.Unmarshal([]byte(*raw), &d); err != nil {
		return HealthErrorDetails{}, false
	}
	return d, true
}

// HasPeerDeferral reports whether the details record a peer-evidence deferral
// that no real check has ended yet.
func HasPeerDeferral(raw *string) bool {
	d, _ := parseDetails(raw)
	return d.PeerDeferredAt != ""
}

// WithPeerDeferral returns the details with the deferral marker set. Every
// other field is kept; details that are not a JSON object are preserved under
// the "legacy" key.
func WithPeerDeferral(raw *string, at time.Time) *string {
	return withDetailsKey(raw, "peer_deferred_at", at.UTC().Format(time.RFC3339))
}

// CarryPeerDeferral returns details with the deferral marker of from added
// when from has one; otherwise details are returned untouched. It keeps an
// inconclusive check (which proves nothing) from ending a deferral.
func CarryPeerDeferral(details, from *string) *string {
	d, _ := parseDetails(from)
	if d.PeerDeferredAt == "" {
		return details
	}
	return withDetailsKey(details, "peer_deferred_at", d.PeerDeferredAt)
}

// HasPeerReduction reports whether the details record that the last check of
// the file was a peer-reduced one.
func HasPeerReduction(raw *string) bool {
	d, _ := parseDetails(raw)
	return d.PeerReducedAt != ""
}

// WithPeerReduction returns the details with the reduction marker set, keeping
// everything else.
func WithPeerReduction(raw *string, at time.Time) *string {
	return withDetailsKey(raw, "peer_reduced_at", at.UTC().Format(time.RFC3339))
}

// HasFailureDetails reports whether the details record a failure of any kind:
// anything but the content-verification outcome a healthy record may carry.
// Details that are not the envelope count as failure evidence (a healthy
// record never has any).
func HasFailureDetails(raw *string) bool {
	if raw == nil || *raw == "" {
		return false
	}
	d, ok := parseDetails(raw)
	if !ok {
		return true
	}
	return d.ErrorType != "" || d.MissingArticles > 0 || d.UnresolvedSegments > 0 ||
		d.TerminatedEarly || d.PlaybackImpact != nil || d.Legacy != ""
}

// withDetailsKey sets one string key in the details JSON object, keeping every
// other key as stored (including ad-hoc legacy ones). Details that are not a
// JSON object (a bare string, an array, plain text) are kept verbatim under
// the "legacy" key rather than replaced.
func withDetailsKey(raw *string, key, value string) *string {
	obj := map[string]json.RawMessage{}
	if raw != nil && *raw != "" {
		if err := json.Unmarshal([]byte(*raw), &obj); err != nil || obj == nil {
			obj = map[string]json.RawMessage{}
			if legacy, mErr := json.Marshal(*raw); mErr == nil {
				obj["legacy"] = legacy
			}
		}
	}
	v, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	obj[key] = v
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	s := string(out)
	return &s
}

// CarryPeerReduction returns details with the reduction marker of from added
// when from has one; otherwise details are returned untouched. An inconclusive
// check proves nothing, so it must not let a second reduced check through.
func CarryPeerReduction(details, from *string) *string {
	d, _ := parseDetails(from)
	if d.PeerReducedAt == "" {
		return details
	}
	return withDetailsKey(details, "peer_reduced_at", d.PeerReducedAt)
}
