package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The queue row of a direct peer import has no NZB behind it: retry, restart
// and download must refuse it with a clear message and change nothing.
func TestQueuePeerImport_NzbActionsAreRefusedCleanly(t *testing.T) {
	server, repo, _ := newSABnzbdHistoryTestServer(t)
	ctx := context.Background()
	item := &database.ImportQueueItem{NzbPath: database.PeerImportNzbPrefix + "abc/Release.nzb", Status: database.QueueStatusCompleted,
		Priority: database.QueuePriorityNormal}
	require.NoError(t, repo.AddToQueue(ctx, item))

	app := fiber.New()
	app.Post("/queue/:id/retry", server.handleRetryQueue)
	app.Post("/queue/bulk/restart", server.handleRestartQueueBulk)
	app.Get("/queue/:id/download", server.handleDownloadNZB)

	for name, req := range map[string]func() *fiberReq{
		"retry":    func() *fiberReq { return newReq("POST", "/queue/1/retry", "") },
		"restart":  func() *fiberReq { return newReq("POST", "/queue/bulk/restart", `{"ids":[1]}`) },
		"download": func() *fiberReq { return newReq("GET", "/queue/1/download", "") },
	} {
		t.Run(name, func(t *testing.T) {
			r := req()
			resp, err := app.Test(r.build())
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, 409, resp.StatusCode)
			body, _ := io.ReadAll(resp.Body)
			assert.Contains(t, string(body), "peer bundle")
			got, err := repo.GetQueueItem(ctx, item.ID)
			require.NoError(t, err)
			assert.Equal(t, database.QueueStatusCompleted, got.Status, "nothing changed")
		})
	}
}

type fiberReq struct{ method, url, body string }

func newReq(method, url, body string) *fiberReq { return &fiberReq{method, url, body} }

func (r *fiberReq) build() *http.Request {
	req := httptest.NewRequest(r.method, r.url, strings.NewReader(r.body))
	if r.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}
