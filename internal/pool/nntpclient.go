package pool

import (
	"context"

	"github.com/javi11/nntppool/v5"
)

// NntpClient is the narrow surface of the underlying nntppool.Client that the
// rest of AltMount calls through Manager.GetPool. Defining it here lets tests
// inject a deterministic fake (see internal/testsupport/fakepool) without
// standing up real NNTP connections, and pins exactly which operations the
// streaming, import, validation, and metrics paths depend on.
//
// Implementations must be safe for concurrent use. The production
// implementation is *nntppool.Client; the contract below intentionally mirrors
// its signatures so the existing client satisfies the interface without an
// adapter.
//
// Keep this interface small. Anything that needs a behavior not listed here
// should add the method explicitly so callers stay observable. Note that what
// used to be eight methods is five: nntppool v5 moved lane selection and
// buffered-vs-streamed delivery into fields on nntppool.Req, so a new
// combination of those needs no new method here.
type NntpClient interface {
	// Fetch downloads and decodes an article body. Req.Lane selects the queue
	// (normal for imports, priority for playback reads, background for PAR2
	// repair), and Req.Writer streams the decoded bytes instead of buffering
	// them. A streamed fetch that has delivered bytes is not failed over, so
	// callers retrying it must supply a fresh writer.
	Fetch(ctx context.Context, r nntppool.Req) (*nntppool.ArticleBody, error)

	// FetchAsync is Fetch on its own goroutine; the returned channel yields
	// exactly one BodyResult. The importer uses it to overlap a segment
	// download with its own bookkeeping.
	FetchAsync(ctx context.Context, r nntppool.Req) <-chan nntppool.BodyResult

	// Exists checks whether an article is retrievable from at least one
	// provider without downloading the body. Used by health checks, import
	// validation, and the mid-stream miss re-check (which passes
	// LanePriority, since a playback read is blocked on the answer and on the
	// normal lane could spend its whole budget queued behind a large body).
	Exists(ctx context.Context, r nntppool.Req) (*nntppool.StatResult, error)

	// ExistsMany checks many articles concurrently, streaming a result per
	// message-id as each completes. Used by health checks and fast-fail import
	// validation to batch existence sweeps instead of issuing one Exists per
	// segment.
	ExistsMany(ctx context.Context, messageIDs []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult

	// Stats returns a snapshot of pool/provider statistics used by the metrics
	// tracker and the system handlers.
	Stats() nntppool.ClientStats
}

// Compile-time assertion: the real client must satisfy the narrow interface.
// If nntppool changes a signature, this line will fail to build and the
// interface above must be updated to match.
var _ NntpClient = (*nntppool.Client)(nil)
