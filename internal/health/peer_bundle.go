package health

import (
	"context"
	"log/slog"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
)

// PeerBundleGate is what the health worker needs to know about imports that
// came from a peer bundle. Such a file is playable at once but owes a full
// check of every segment: until it passes, the import is not announced to the
// ARRs and not published to the peer network, and peer advice never applies to
// it. The interface is defined here so this package never imports the peer
// network; the cmd wiring installs an adapter and removes it while the feature
// is off, so with no gate nothing changes and nothing extra runs (no query).
type PeerBundleGate interface {
	// PendingFullCheck returns the subset of paths whose full check is pending.
	// It must fail closed (report pending on an error).
	PendingFullCheck(ctx context.Context, paths []string) map[string]struct{}
	// IsPendingPeerImport reports whether the file is a peer import whose full
	// check is pending. Unlike PendingFullCheck it fails open (false on an
	// error): it decides to stop repair attempts for a file, which a database
	// hiccup must not do to an ordinary one.
	IsPendingPeerImport(ctx context.Context, filePath string) bool
	// FullCheckPassed is called after a pending file's full check came back
	// healthy and its status was written.
	FullCheckPassed(ctx context.Context, filePath string)
	// FullCheckFailed is called when a pending file's full check failed
	// definitively (corrupted, retries exhausted). The gate fails the whole
	// import (the queue item, the ARR-visible history entry) and removes its
	// files; the worker never tries an ARR repair for such a file. It is
	// idempotent.
	FullCheckFailed(ctx context.Context, filePath string)
}

// PeerBundleDegradedGate is optionally implemented by a gate that wants to hear
// about a degraded verdict (the file would play with a few missing segments)
// separately from other failures. A pending peer import never ends degraded: it
// fails at once, and an implementation that does not implement this interface
// simply sees FullCheckFailed.
type PeerBundleDegradedGate interface {
	FullCheckDegraded(ctx context.Context, filePath string)
}

type peerBundleGateHolder struct{ g PeerBundleGate }

// SetPeerBundleGate installs (or, with nil, removes) the gate. Safe at any time.
//
// A gate that also implements PeerHeaderVerifier gets the header sample of the
// forced full check; removing the gate removes the verifier.
func (hw *HealthWorker) SetPeerBundleGate(g PeerBundleGate) {
	if g == nil {
		hw.peerBundleGate.Store(nil)
		if hw.healthChecker != nil {
			hw.healthChecker.SetPeerHeaderVerifier(nil)
		}
		return
	}
	hw.peerBundleGate.Store(&peerBundleGateHolder{g: g})
	if hw.healthChecker != nil {
		v, _ := g.(PeerHeaderVerifier)
		hw.healthChecker.SetPeerHeaderVerifier(v)
	}
}

// pendingPeerBundles returns which paths owe a full check; nil with no gate.
func (hw *HealthWorker) pendingPeerBundles(ctx context.Context, paths []string) map[string]struct{} {
	h := hw.peerBundleGate.Load()
	if h == nil || len(paths) == 0 {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Peer bundle gate panicked", "panic", r)
		}
	}()
	return h.g.PendingFullCheck(ctx, paths)
}

// notifyPeerBundleResults tells the gate how the full checks of pending peer
// files ended. Only results whose write landed count; a retry or inconclusive
// result is no verdict.
func (hw *HealthWorker) notifyPeerBundleResults(ctx context.Context, pending map[string]struct{}, updates []database.HealthStatusUpdate, applied []bool, events map[string]HealthEvent) {
	h := hw.peerBundleGate.Load()
	if h == nil || len(pending) == 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Peer bundle gate panicked", "panic", r)
		}
	}()
	for i, u := range updates {
		if _, ok := pending[u.FilePath]; !ok || i >= len(applied) || !applied[i] {
			continue
		}
		ev, ok := events[u.FilePath]
		if !ok {
			continue
		}
		switch {
		case u.Type == database.UpdateTypeHealthy && ev.Type == EventTypeFileHealthy:
			h.g.FullCheckPassed(ctx, u.FilePath)
		case (ev.Type == EventTypeFileCorrupted || ev.Type == EventTypeCheckFailed) && u.Type != database.UpdateTypeRetry &&
			u.Type != database.UpdateTypeInconclusive && u.Type != database.UpdateTypeDeferred && !u.Skip:
			// A check that failed outright and ended as a corrupted status is
			// a verdict like any other; a degraded one (playable with a few
			// missing segments) is told apart so the fast path can fall back.
			if g, ok := h.g.(PeerBundleDegradedGate); ok && ev.Classification != nil && ev.Classification.Verdict == holes.VerdictDegraded {
				g.FullCheckDegraded(ctx, u.FilePath)
				continue
			}
			h.g.FullCheckFailed(ctx, u.FilePath)
		}
	}
}

// peerImportFailsFor reports whether a corrupted verdict for the file ends a
// pending peer import (and so is handled by the gate, never by an ARR repair).
func (hw *HealthWorker) peerImportFailsFor(ctx context.Context, filePath string) bool {
	h := hw.peerBundleGate.Load()
	if h == nil {
		return false
	}
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Peer bundle gate panicked", "panic", r)
		}
	}()
	return h.g.IsPendingPeerImport(ctx, filePath)
}
