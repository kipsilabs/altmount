package pool

import (
	"context"
	"testing"

	"github.com/javi11/nntppool/v5"
)

func TestNewAdHocClientBoundsWarmConnections(t *testing.T) {
	for _, tt := range []struct {
		name        string
		minimum     int
		connections int
	}{
		{name: "minimum exceeds tuning cap", minimum: 8, connections: 4},
		{name: "minimum equals tuning cap", minimum: 4, connections: 4},
		{name: "minimum below tuning cap", minimum: 2, connections: 4},
		{name: "single connection", minimum: 2, connections: 1},
		{name: "no warm connections", minimum: 0, connections: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// A cancelled context prevents pre-warming from opening sockets;
			// NewClient still performs its real provider validation.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			provider := nntppool.Provider{
				Host: "news.example.test:119", Connections: 20, MinConnections: tt.minimum,
			}
			client, err := NewAdHocClient(ctx, provider, tt.connections, 1)
			if err != nil {
				t.Fatalf("creating bounded tuning client: %v", err)
			}
			client.Close()
		})
	}
}
