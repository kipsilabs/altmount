package api

import (
	"context"
	"log/slog"
	"strings"

	"github.com/kipsilabs/altmount/internal/database"
)

// PeerBundleLookup tells which queue items and files came from a peer bundle
// and whether their full check is still pending (the value is "pending").
// *database.PeernetBundleImportRepository satisfies it.
type PeerBundleLookup interface {
	StatesForQueues(ctx context.Context, ids []int64) (map[int64]bool, error)
	StatesForPaths(ctx context.Context, paths []string) (map[string]bool, error)
}

type peerBundleLookupHolder struct{ l PeerBundleLookup }

// SetPeerBundleLookup installs (or, with nil, removes) the lookup. With none
// installed no query runs and every item reports false for both flags.
func (s *Server) SetPeerBundleLookup(l PeerBundleLookup) {
	if l == nil {
		s.peerBundles.Store(nil)
		return
	}
	s.peerBundles.Store(&peerBundleLookupHolder{l: l})
}

func (s *Server) annotateQueuePeerOrigin(ctx context.Context, items []*database.ImportQueueItem, resp []*QueueItemResponse) {
	h := s.peerBundles.Load()
	if h == nil || len(items) == 0 {
		return
	}
	ids := make([]int64, len(items))
	for i, it := range items {
		ids[i] = it.ID
	}
	states, err := h.l.StatesForQueues(ctx, ids)
	if err != nil {
		slog.WarnContext(ctx, "Could not look up peer bundle imports", "error", err)
		return
	}
	for i, it := range items {
		if pending, ok := states[it.ID]; ok && i < len(resp) {
			resp[i].PeerOrigin, resp[i].PeerFullCheckPending = true, pending
		}
	}
}

func (s *Server) annotateHealthPeerOrigin(ctx context.Context, items []*database.FileHealth, resp []*HealthItemResponse) {
	h := s.peerBundles.Load()
	if h == nil || len(items) == 0 {
		return
	}
	paths := make([]string, len(items))
	for i, it := range items {
		paths[i] = it.FilePath
	}
	states, err := h.l.StatesForPaths(ctx, paths)
	if err != nil {
		slog.WarnContext(ctx, "Could not look up peer bundle imports", "error", err)
		return
	}
	for i, it := range items {
		if pending, ok := states[strings.Trim(strings.ReplaceAll(it.FilePath, `\`, "/"), "/")]; ok && i < len(resp) {
			resp[i].PeerOrigin, resp[i].PeerFullCheckPending = true, pending
		}
	}
}
