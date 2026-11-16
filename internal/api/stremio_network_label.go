package api

import (
	"context"
	"net/url"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/database"
)

// stremioNetworkHealthyLabel marks a library stream whose release other nodes
// attest healthy. It is a fixed local string; nothing a peer sent is ever
// rendered here.
const stremioNetworkHealthyLabel = "🌐 Healthy on the peer network"

// StremioNetworkLabeler answers which of the given library paths belong to a
// release that is healthy on the peer network (exact Release ID match). It
// must be one bounded, short lookup that fails open to an empty result. It is
// implemented by the peer network service.
type StremioNetworkLabeler interface {
	NetworkHealthyPaths(ctx context.Context, paths []string) map[string]bool
}

type networkLabelerHolder struct{ l StremioNetworkLabeler }

// SetStremioNetworkLabeler installs (or, with nil, removes) the labeler. With
// none installed Stremio responses are exactly what they were without the
// peer network and no extra lookup runs.
func (s *Server) SetStremioNetworkLabeler(l StremioNetworkLabeler) {
	if l == nil {
		s.networkLabeler.Store(nil)
		return
	}
	s.networkLabeler.Store(&networkLabelerHolder{l: l})
}

// labelNetworkHealthy appends the network label to the description of every
// stream whose file path (from its stream URL) belongs to a release that is
// healthy on the network. Streams without a library path (indexer results,
// whose NZB was never fetched and so have no Release ID) are never labelled.
func (s *Server) labelNetworkHealthy(ctx context.Context, streams []fiber.Map) []fiber.Map {
	h := s.networkLabeler.Load()
	if h == nil || len(streams) == 0 {
		return streams
	}
	pathOf := make([]string, len(streams))
	seen := map[string]struct{}{}
	var paths []string
	for i, st := range streams {
		raw, _ := st["url"].(string)
		u, err := url.Parse(raw)
		if err != nil || u.Path != "/api/files/stream" {
			continue
		}
		p := u.Query().Get("path")
		if p == "" {
			continue
		}
		pathOf[i] = p
		if _, dup := seen[p]; !dup && len(paths) < database.PeernetMaxPathsPerQuery {
			seen[p] = struct{}{}
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		return streams
	}
	healthy := h.l.NetworkHealthyPaths(ctx, paths)
	if len(healthy) == 0 {
		return streams
	}
	for i, st := range streams {
		if pathOf[i] != "" && healthy[pathOf[i]] {
			title, _ := st["title"].(string)
			st["title"] = title + "\n" + stremioNetworkHealthyLabel
		}
	}
	return streams
}
