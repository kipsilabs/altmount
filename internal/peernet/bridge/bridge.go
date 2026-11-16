// Package bridge connects the health worker to the peer network's publishing
// pipeline without either side importing the other's internals: the health
// package defines the observer interface and a plain notice type, the service
// package defines its own notice type, and this package converts between them.
package bridge

import (
	"context"

	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/peernet/hints"
	"github.com/kipsilabs/altmount/internal/peernet/service"
)

// Notifier accepts a healthy-check notice. It must return at once and never
// fail; *service.Service satisfies it.
type Notifier interface {
	NotifyHealthy(service.HealthyNotice)
}

type observer struct{ n Notifier }

// NewHealthObserver returns a health.HealthyObserver that forwards every
// notice to n. A nil n yields nil, so the health worker then has no observer
// at all.
func NewHealthObserver(n Notifier) health.HealthyObserver {
	if n == nil {
		return nil
	}
	return observer{n: n}
}

func (o observer) FileHealthy(h health.HealthyNotice) {
	o.n.NotifyHealthy(service.HealthyNotice{
		FilePath:            h.FilePath,
		CheckedAt:           h.CheckedAt,
		TotalSegments:       h.Stats.TotalSegments,
		SegmentsChecked:     h.Stats.SegmentsChecked,
		MissingCount:        h.Stats.MissingCount,
		FullCheck:           h.Stats.FullCheck,
		MediaHeaderVerified: h.Stats.MediaHeaderVerified,
		PatchedByPAR2:       h.Stats.PatchedByPAR2,
		PatchKnown:          h.Stats.PatchKnown,
		KnownHoles:          h.Stats.KnownHoles,
		ReducedByPeerAdvice: h.Stats.ReducedByPeerAdvice,
	})
}

// AdviceSource is what the health adviser needs from the peer network;
// *service.Service satisfies it.
type AdviceSource interface {
	HealthAdvice(ctx context.Context, filePath string) hints.Advice
	NotePeerAdviceApplied(kind hints.AdviceKind)
}

type peerAdviser struct{ src AdviceSource }

// NewPeerAdviser returns a health.PeerAdviser backed by src. A nil src yields
// nil, so the health worker then has no adviser at all.
func NewPeerAdviser(src AdviceSource) health.PeerAdviser {
	if src == nil {
		return nil
	}
	return peerAdviser{src: src}
}

func (a peerAdviser) PeerAdvice(ctx context.Context, filePath string) health.PeerAdvice {
	adv := a.src.HealthAdvice(ctx, filePath)
	switch adv.Kind {
	case hints.AdviceReduceSampleToFloor:
		return health.PeerAdvice{Kind: health.PeerAdviceReduceSample}
	case hints.AdviceMaySkipOneCheck:
		return health.PeerAdvice{Kind: health.PeerAdviceSkipOnce, SkipUntil: adv.SkipUntil}
	}
	// A recheck-sooner answer is for the periodic pass; the check that is
	// being prepared right now is the recheck.
	return health.PeerAdvice{}
}

func (a peerAdviser) PeerAdviceApplied(kind health.PeerAdviceKind) {
	switch kind {
	case health.PeerAdviceReduceSample:
		a.src.NotePeerAdviceApplied(hints.AdviceReduceSampleToFloor)
	case health.PeerAdviceSkipOnce:
		a.src.NotePeerAdviceApplied(hints.AdviceMaySkipOneCheck)
	}
}
