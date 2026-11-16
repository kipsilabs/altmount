package hints

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// Candidate is one release found for a media key. Title is peer-supplied data
// to be rendered as text only.
type Candidate struct {
	ReleaseID string
	Title     string
	TotalSize int64
	CreatedAt time.Time
	// Networks lists every receiving network the release was seen in, sorted.
	Networks []netid.NetworkID
	// Own marks releases this node published itself.
	Own    bool
	Health NetworkHealth
}

func validIMDb(s string) bool {
	if len(s) < 3 || len(s) > 2+maxIMDbDigits || s[:2] != "tt" {
		return false
	}
	for i := 2; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Candidates returns the release records known for a media key across the
// networks this node receives from, one entry per release ID, healthy-labelled
// first, then newest, then by release ID; at most CandidateLimit entries. A
// nil season and episode match movies only (see the store contract).
func (a *Advisor) Candidates(ctx context.Context, imdbID string, season, episode *int) ([]Candidate, error) {
	if !validIMDb(imdbID) {
		return nil, errors.New("hints: invalid imdb id")
	}
	if (season != nil && *season < 0) || (episode != nil && *episode < 0) {
		return nil, errors.New("hints: negative season or episode")
	}
	nets := a.receiving()
	if len(nets) == 0 {
		return nil, nil
	}
	idStrs := make([]string, len(nets))
	for i, n := range nets {
		idStrs[i] = n.ID.String()
	}
	rows, err := a.cfg.Store.CandidatesByMedia(ctx, idStrs, imdbID, season, episode, a.cfg.CandidateLimit)
	if err != nil {
		return nil, fmt.Errorf("hints: candidates: %w", err)
	}
	merged := a.merge(rows)
	if len(merged) == 0 {
		return nil, nil
	}
	ids := make([]string, len(merged))
	for i, c := range merged {
		ids[i] = c.ReleaseID
	}
	health, err := a.NetworkHealthy(ctx, ids...)
	if err != nil {
		return nil, err
	}
	for i := range merged {
		merged[i].Health = health[merged[i].ReleaseID]
	}
	sort.Slice(merged, func(i, j int) bool {
		x, y := merged[i], merged[j]
		switch {
		case x.Health.Healthy != y.Health.Healthy:
			return x.Health.Healthy
		case !x.CreatedAt.Equal(y.CreatedAt):
			return x.CreatedAt.After(y.CreatedAt)
		}
		return x.ReleaseID < y.ReleaseID
	})
	if len(merged) > a.cfg.CandidateLimit {
		merged = merged[:a.cfg.CandidateLimit]
	}
	return merged, nil
}

// merge de-duplicates rows by release ID; the newest record defines the entry.
func (a *Advisor) merge(rows []database.PeernetCandidate) []Candidate {
	byID := map[string]*Candidate{}
	var order []string
	for _, r := range rows {
		nw, err := netid.ParseNetworkID(r.NetworkID)
		title := records.CleanTitle(r.Title)
		if err != nil || !validID(r.ReleaseID) || title == "" {
			continue
		}
		own := r.Source == database.PeernetRecordSourceLocal || strings.EqualFold(r.PublisherNodeID, a.cfg.SelfNodeID)
		c := byID[r.ReleaseID]
		if c == nil {
			c = &Candidate{ReleaseID: r.ReleaseID}
			byID[r.ReleaseID] = c
			order = append(order, r.ReleaseID)
		}
		if c.Title == "" || r.CreatedAt.After(c.CreatedAt) {
			c.Title, c.TotalSize, c.CreatedAt = title, r.TotalSize, r.CreatedAt
		}
		c.Own = c.Own || own
		if !containsNet(c.Networks, nw) {
			c.Networks = append(c.Networks, nw)
		}
	}
	out := make([]Candidate, 0, len(order))
	for _, id := range order {
		c := byID[id]
		sort.Slice(c.Networks, func(i, j int) bool { return c.Networks[i].String() < c.Networks[j].String() })
		out = append(out, *c)
	}
	return out
}

func containsNet(list []netid.NetworkID, n netid.NetworkID) bool {
	for _, x := range list {
		if x == n {
			return true
		}
	}
	return false
}
