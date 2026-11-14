package hints

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// NetworkHealth is the network-wide health label of one release. It is never
// a status.
type NetworkHealth struct {
	// Healthy: on some receiving network enough eligible publishers attest it
	// healthy and unhealthy evidence does not itself reach the threshold on
	// any network.
	Healthy bool
	// HealthyPublishers and UnhealthyPublishers count eligible publishers
	// (distinct network prefixes on the open network), summed over networks
	// (informational).
	HealthyPublishers   int
	UnhealthyPublishers int
	// Newest is the latest check time among eligible healthy attestations.
	Newest time.Time
}

// NetworkHealthy labels releases for candidate lists (trust rule 5). Every
// requested release is present in the result. At most 200 distinct IDs are
// accepted per call.
func (a *Advisor) NetworkHealthy(ctx context.Context, releaseIDs ...string) (map[string]NetworkHealth, error) {
	return a.NetworkHealthyExcluding(ctx, "", releaseIDs...)
}

// NetworkHealthyExcluding is NetworkHealthy with every attestation of the
// publisher exclude (a node ID, 64 lower-case hex characters; empty excludes
// nobody) left out of the evidence. A publisher's own attestation is not
// independent evidence for its own offer.
func (a *Advisor) NetworkHealthyExcluding(ctx context.Context, exclude string, releaseIDs ...string) (map[string]NetworkHealth, error) {
	ids := make([]string, 0, len(releaseIDs))
	seen := make(map[string]struct{}, len(releaseIDs))
	for _, id := range releaseIDs {
		if !validID(id) {
			return nil, errors.New("hints: release ids must be 64 lower-case hex characters")
		}
		if _, dup := seen[id]; !dup {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if len(ids) > maxReleaseIDs {
		return nil, ErrTooManyIDs
	}
	out := make(map[string]NetworkHealth, len(ids))
	anyHealthy := make(map[string]bool, len(ids))
	vetoed := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = NetworkHealth{}
	}
	if len(ids) == 0 {
		return out, nil
	}
	now := a.cfg.Now()
	for _, n := range a.receiving() {
		if err := a.labelNetwork(ctx, n, ids, now, exclude, out, anyHealthy, vetoed); err != nil {
			return nil, err
		}
	}
	for id, h := range out {
		h.Healthy = anyHealthy[id] && !vetoed[id]
		out[id] = h
	}
	return out, nil
}

func (a *Advisor) labelNetwork(ctx context.Context, n Network, ids []string, now time.Time, exclude string, out map[string]NetworkHealth, anyHealthy, vetoed map[string]bool) error {
	notBefore := now.Add(-records.AttestationValidity)
	summary, err := a.cfg.Store.HealthSummaryForReleases(ctx, n.ID.String(), ids, notBefore)
	if err != nil {
		return fmt.Errorf("hints: health summary: %w", err)
	}
	for _, id := range ids {
		if _, ok := summary[id]; !ok {
			continue
		}
		atts, err := a.cfg.Store.AttestationsFor(ctx, n.ID.String(), id, "", notBefore)
		if err != nil {
			return fmt.Errorf("hints: attestations: %w", err)
		}
		hE, uE, newest, err := a.tally(ctx, n, withoutPublisher(atts, exclude), now)
		if err != nil {
			return err
		}
		h := out[id]
		h.HealthyPublishers += hE
		h.UnhealthyPublishers += uE
		if newest.After(h.Newest) {
			h.Newest = newest
		}
		out[id] = h
		// A lone unhealthy publisher does not veto: unhealthy evidence has to
		// meet the same threshold as healthy corroboration.
		if uE >= a.threshold(n) {
			vetoed[id] = true
		}
		if hE >= a.threshold(n) {
			anyHealthy[id] = true
		}
	}
	return nil
}

// tally counts eligible publishers, one per distinct group (see groupOf). A
// publisher that attested anything unhealthy counts as unhealthy only.
func (a *Advisor) tally(ctx context.Context, n Network, atts []database.PeernetAttestation, now time.Time) (healthy, unhealthy int, newest time.Time, err error) {
	known := newKnownCache(a, ctx, n, now)
	if err := known.prefetch(publishersOf(atts, a, now)); err != nil {
		return 0, 0, time.Time{}, err
	}
	return a.tallyWith(known, atts, now)
}

// publishersOf lists the usable publishers of atts in order, without repeats.
func publishersOf(atts []database.PeernetAttestation, a *Advisor, now time.Time) []string {
	var order []string
	seen := map[string]struct{}{}
	for _, att := range atts {
		if !a.usable(att, now) {
			continue
		}
		if _, dup := seen[att.PublisherNodeID]; !dup {
			seen[att.PublisherNodeID] = struct{}{}
			order = append(order, att.PublisherNodeID)
		}
	}
	return order
}

// tallyWith is tally over a knownCache the caller already filled, so many
// files can share one batch of standing lookups.
func (a *Advisor) tallyWith(known *knownCache, atts []database.PeernetAttestation, now time.Time) (healthy, unhealthy int, newest time.Time, err error) {
	type verdict struct {
		bad    bool
		newest time.Time
	}
	order := []string{}
	per := map[string]*verdict{}
	for _, att := range atts {
		if !a.usable(att, now) {
			continue
		}
		v := per[att.PublisherNodeID]
		if v == nil {
			v = &verdict{}
			per[att.PublisherNodeID] = v
			order = append(order, att.PublisherNodeID)
		}
		if !att.Healthy {
			v.bad = true
		} else if CoverageSufficient(att.SegmentsChecked, att.TotalSegments) && att.CheckedAt.After(v.newest) {
			v.newest = att.CheckedAt
		}
	}
	healthyG, unhealthyG := map[string]struct{}{}, map[string]struct{}{}
	for _, node := range order {
		ok, kerr := known.eligible(node)
		if kerr != nil {
			return 0, 0, time.Time{}, kerr
		}
		if !ok {
			continue
		}
		v := per[node]
		g := known.groupOf(node)
		switch {
		case v.bad:
			unhealthyG[g] = struct{}{}
		case !v.newest.IsZero():
			healthyG[g] = struct{}{}
			if v.newest.After(newest) {
				newest = v.newest
			}
		}
	}
	return len(healthyG), len(unhealthyG), newest, nil
}

// NetworkHealthyFiles labels releases for candidate lists from file-level
// evidence, so it works for a release this node only knows by its files and
// counts evidence published under any release ID. releases maps a release ID
// to the File IDs of its main (video) files.
//
// A release is labelled healthy when every one of its files is healthy under
// the same rule NetworkHealthy applies to a release: on some receiving network
// enough eligible publishers (distinct network prefixes on the open network)
// attest the file healthy with enough coverage, and on no network does
// unhealthy evidence reach the same threshold. Requiring every main file is the
// conservative reading of "the release is healthy": a release is only as
// available as its worst file, and a publisher that attested only some of its
// files has not vouched for the release. A release with no files, or with more
// than MaxLabelFilesPerRelease, is not labelled (it cannot be proven), and
// once MaxLabelFiles distinct files are collected the remaining releases are
// left unlabelled. The cost is one attestation lookup per receiving network
// for the whole call plus the batched publisher-standing lookups. Every
// requested release is present in the result; at most 200 are accepted.
func (a *Advisor) NetworkHealthyFiles(ctx context.Context, releases map[string][]string) (map[string]NetworkHealth, error) {
	return a.NetworkHealthyFilesExcluding(ctx, "", releases)
}

// NetworkHealthyFilesExcluding is NetworkHealthyFiles with every attestation
// of the publisher exclude (a node ID, 64 lower-case hex characters; empty
// excludes nobody) left out of the evidence, so a publisher's own attestation
// is not counted as independent evidence for its own offer.
func (a *Advisor) NetworkHealthyFilesExcluding(ctx context.Context, exclude string, releases map[string][]string) (map[string]NetworkHealth, error) {
	if len(releases) > maxReleaseIDs {
		return nil, ErrTooManyIDs
	}
	out := make(map[string]NetworkHealth, len(releases))
	rels := make([]string, 0, len(releases))
	for rel := range releases {
		if !validID(rel) {
			return nil, errors.New("hints: release ids must be 64 lower-case hex characters")
		}
		for _, f := range releases[rel] {
			if !validID(f) {
				return nil, errors.New("hints: file ids must be 64 lower-case hex characters")
			}
		}
		out[rel] = NetworkHealth{}
		rels = append(rels, rel)
	}
	sort.Strings(rels)

	// Decide which releases can be looked at, within the file budget.
	files := map[string][]string{}
	all := map[string]struct{}{}
	for _, rel := range rels {
		fs := uniqueStrings(releases[rel])
		if len(fs) == 0 || len(fs) > MaxLabelFilesPerRelease {
			continue
		}
		fresh := 0
		for _, f := range fs {
			if _, in := all[f]; !in {
				fresh++
			}
		}
		if len(all)+fresh > MaxLabelFiles {
			continue
		}
		for _, f := range fs {
			all[f] = struct{}{}
		}
		files[rel] = fs
	}
	if len(files) == 0 {
		return out, nil
	}
	ids := make([]string, 0, len(all))
	for f := range all {
		ids = append(ids, f)
	}
	sort.Strings(ids)

	now := a.cfg.Now()
	notBefore := now.Add(-records.AttestationValidity)
	type fileState struct {
		healthyOK, vetoed bool
		healthy, bad      int
		newest            time.Time
	}
	state := make(map[string]*fileState, len(ids))
	for _, f := range ids {
		state[f] = &fileState{}
	}
	for _, n := range a.receiving() {
		atts, err := a.cfg.Store.AttestationsForFiles(ctx, n.ID.String(), ids, notBefore)
		if err != nil {
			return nil, fmt.Errorf("hints: attestations: %w", err)
		}
		byFile := map[string][]database.PeernetAttestation{}
		for _, att := range withoutPublisher(atts, exclude) {
			if _, wanted := all[att.FileID]; wanted {
				byFile[att.FileID] = append(byFile[att.FileID], att)
			}
		}
		known := newKnownCache(a, ctx, n, now)
		var pubs []string
		seen := map[string]struct{}{}
		for _, f := range ids {
			for _, p := range publishersOf(byFile[f], a, now) {
				if _, dup := seen[p]; !dup {
					seen[p] = struct{}{}
					pubs = append(pubs, p)
				}
			}
		}
		if err := known.prefetch(pubs); err != nil {
			return nil, err
		}
		for _, f := range ids {
			hE, uE, newest, err := a.tallyWith(known, byFile[f], now)
			if err != nil {
				return nil, err
			}
			st := state[f]
			st.healthy += hE
			st.bad += uE
			if newest.After(st.newest) {
				st.newest = newest
			}
			if hE >= a.threshold(n) {
				st.healthyOK = true
			}
			if uE >= a.threshold(n) {
				st.vetoed = true
			}
		}
	}
	for rel, fs := range files {
		h := NetworkHealth{Healthy: true}
		for i, f := range fs {
			st := state[f]
			if !st.healthyOK || st.vetoed {
				h.Healthy = false
			}
			// The weakest file describes the release.
			if i == 0 || st.healthy < h.HealthyPublishers {
				h.HealthyPublishers = st.healthy
			}
			if st.bad > h.UnhealthyPublishers {
				h.UnhealthyPublishers = st.bad
			}
			if i == 0 || st.newest.Before(h.Newest) {
				h.Newest = st.newest
			}
		}
		out[rel] = h
	}
	return out, nil
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, dup := seen[s]; !dup {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// withoutPublisher drops the attestations of the publisher exclude (none when
// empty). The input is not modified.
func withoutPublisher(atts []database.PeernetAttestation, exclude string) []database.PeernetAttestation {
	if exclude == "" {
		return atts
	}
	out := make([]database.PeernetAttestation, 0, len(atts))
	for _, att := range atts {
		if att.PublisherNodeID != exclude {
			out = append(out, att)
		}
	}
	return out
}
