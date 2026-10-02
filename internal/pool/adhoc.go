package pool

import (
	"context"
	"time"

	"github.com/javi11/nntppool/v5"
)

// NewAdHocClient creates a single-provider client for a bounded speed test.
// The caller owns the returned client and must Close it.
func NewAdHocClient(ctx context.Context, provider nntppool.Provider, connections, inflight int) (*nntppool.Client, error) {
	provider.Connections = connections
	// Tuning uses fewer connections than the production pool. Its warm
	// minimum must fit that smaller allowance for NewClient validation.
	if provider.MinConnections > connections {
		provider.MinConnections = connections
	}
	provider.Inflight = inflight
	provider.IdleTimeout = 60 * time.Second
	return nntppool.NewClient(ctx, []nntppool.Provider{provider})
}
