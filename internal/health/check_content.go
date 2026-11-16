package health

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/kipsilabs/altmount/internal/contentverify"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
)

// shouldVerifyContent decides whether judgeValidation should run a content
// probe for this file: explicit override wins; otherwise only a first-time
// (Pending) check is probed, so repeated repair-recheck cycles on an
// already-flagged file don't re-probe on every pass.
func (hc *HealthChecker) shouldVerifyContent(prep preparedCheck) bool {
	if hc.contentVerifyFS == nil {
		// Probing through a nil Opener panics, so the wiring check comes
		// first — an API-forced override must not be able to reach it.
		return false
	}
	if prep.verifyContentOverride != nil {
		return *prep.verifyContentOverride
	}
	if !hc.configGetter().GetHealthVerifyContent() {
		return false
	}
	return prep.currentStatus == database.HealthStatusPending
}

// judgeContentVerification probes prep.filePath's content signature. It
// returns a Corrupted event when the result is definitive, and otherwise the
// error_details to attach to the healthy event: a nil event with non-nil
// details records that the probe ran and passed, or that it could not
// complete. Both returns are nil when the file is not an eligible media type
// and no probe happened at all. A transient probe error must never mark a
// file corrupted, so it stays on the healthy path — but it is recorded there
// rather than only logged, because a healthy record that says nothing cannot
// be told apart from one that was verified clean.
func (hc *HealthChecker) judgeContentVerification(ctx context.Context, prep preparedCheck) (*HealthEvent, *string) {
	event, details, _ := hc.runContentVerification(ctx, prep)
	return event, details
}

// runContentVerification is judgeContentVerification plus whether the probe
// ran and passed in this call.
func (hc *HealthChecker) runContentVerification(ctx context.Context, prep preparedCheck) (*HealthEvent, *string, bool) {
	if !fileinfo.IsVerifiableMediaFile(prep.filePath) {
		return nil, nil, false
	}

	cfg := hc.configGetter()
	result := contentverify.Probe(ctx, hc.contentVerifyFS, prep.filePath, cfg.GetHealthVerifyContentTimeout())

	var errType, message string
	switch result.Result {
	case contentverify.ContentValid:
		details := database.HealthErrorDetails{ContentVerification: database.ContentVerificationPassed}
		return nil, details.Marshal(), true
	case contentverify.ContentProbeError:
		slog.WarnContext(ctx, "Content verification could not complete",
			"file_path", prep.filePath,
			"error", result.Err)
		details := database.HealthErrorDetails{
			ContentVerification: database.ContentVerificationUnavailable,
			Message:             probeErrorMessage(result.Err),
		}
		return nil, details.Marshal(), false
	case contentverify.ContentInvalid:
		errType = "content_invalid"
		message = "no recognized media container signature was found in the file's header"
	case contentverify.ContentSegmentMissing:
		errType = "content_segment_missing"
		message = "the article needed to read the file's header is missing from your Usenet provider"
	default:
		return nil, nil, false
	}

	event := baseResultEvent(prep.filePath, prep.sourceNzbPath)
	event.Type = EventTypeFileCorrupted
	event.Status = database.HealthStatusCorrupted
	event.Error = fmt.Errorf("content verification failed: %s", message)
	details := database.HealthErrorDetails{
		ErrorType:           errType,
		Message:             message,
		ContentVerification: database.ContentVerificationFailed,
	}
	event.Details = details.Marshal()
	return &event, nil, false
}

// probeErrorMessage renders why a probe could not complete, so the UI can
// explain an unproven file instead of just flagging it as unverified.
func probeErrorMessage(err error) string {
	if err == nil {
		return "the content probe could not complete"
	}
	return fmt.Sprintf("the content probe could not complete: %s", err)
}
