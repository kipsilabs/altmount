package health

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/kipsilabs/altmount/internal/database"
)

// ErrPeerLayoutMismatch is what a PeerHeaderVerifier returns when the yEnc
// headers of sampled articles contradict the file's recorded layout.
var ErrPeerLayoutMismatch = errors.New("article headers contradict the imported layout")

// layoutMismatch is implemented by an error a verifier returns for a
// contradiction without importing this package's sentinel.
type layoutMismatch interface{ PeerLayoutMismatch() bool }

func isLayoutMismatch(err error) bool {
	var m layoutMismatch
	return errors.Is(err, ErrPeerLayoutMismatch) || (errors.As(err, &m) && m.PeerLayoutMismatch())
}

// PeerHeaderVerifier is an optional extra check of a pending peer import's
// forced full check. The STAT sweep proves the articles exist; this reads the
// yEnc headers of a further random sample (part size and offset against the
// layout), which is sampling, not proof. It returns nil when the sample
// agrees, an error wrapping ErrPeerLayoutMismatch (or implementing PeerLayoutMismatch() bool) when it contradicts the
// layout, and any other error when it could not be completed. The interface
// is defined here so this package never imports the peer network; with no
// verifier installed nothing changes.
type PeerHeaderVerifier interface {
	VerifyPeerHeaders(ctx context.Context, filePath string) error
}

type peerHeaderHolder struct{ v PeerHeaderVerifier }

// SetPeerHeaderVerifier installs (or, with nil, removes) the verifier.
func (hc *HealthChecker) SetPeerHeaderVerifier(v PeerHeaderVerifier) {
	if v == nil {
		hc.peerHeaders.Store(nil)
		return
	}
	hc.peerHeaders.Store(&peerHeaderHolder{v: v})
}

// SetPeerHeaderVerifier installs the verifier on the worker's checker.
func (hw *HealthWorker) SetPeerHeaderVerifier(v PeerHeaderVerifier) {
	hw.healthChecker.SetPeerHeaderVerifier(v)
}

// verifyPeerHeaders runs the verifier for a file owing a full check. A nil
// event means the sample agreed (or there is no verifier).
func (hc *HealthChecker) verifyPeerHeaders(ctx context.Context, prep preparedCheck, base HealthEvent) *HealthEvent {
	h := hc.peerHeaders.Load()
	if h == nil || !prep.fullCheck {
		return nil
	}
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("header verifier panicked: %v", r)
			}
		}()
		err = h.v.VerifyPeerHeaders(ctx, prep.filePath)
	}()
	switch {
	case err == nil:
		return nil
	case isLayoutMismatch(err):
		slog.WarnContext(ctx, "Peer import: article headers contradict the layout", "file_path", prep.filePath, "error", err)
		base.Type = EventTypeFileCorrupted
		base.Status = database.HealthStatusCorrupted
		base.Error = err
		details := database.HealthErrorDetails{ErrorType: "peer_layout_mismatch", Message: err.Error()}
		base.Details = details.Marshal()
		return &base
	default:
		base.Type = EventTypeCheckInconclusive
		base.Status = database.HealthStatusPending
		base.Error = fmt.Errorf("peer header check inconclusive: %w", err)
		details := database.HealthErrorDetails{ErrorType: "check_inconclusive", Message: base.Error.Error()}
		base.Details = details.Marshal()
		return &base
	}
}
