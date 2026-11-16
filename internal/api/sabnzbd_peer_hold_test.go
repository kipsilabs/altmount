package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A release imported from a peer bundle is playable at once but owes a full
// health check. Until it passes, the SABnzbd-compatible API shows it in the
// queue as work in progress and not in the history (so the ARR does not import
// unverified content); a pass moves it to the history as completed, a failure
// (the full check failed, or the 7-day limit) as failed.

type peerHoldEnv struct {
	server *Server
	repo   *database.Repository
	state  *database.PeernetBundleImportRepository
	id     int64
	dlID   string
}

func newPeerHoldEnv(t *testing.T, skip bool) *peerHoldEnv {
	t.Helper()
	server, repo, conn := newSABnzbdHistoryTestServer(t)
	id := addHistoryQueueRow(t, repo, conn, "peer", database.QueueStatusCompleted, "tv", "nzo_peer", time.Now().UTC().Add(-time.Minute))
	if skip {
		_, err := conn.Exec(`UPDATE import_queue SET skip_arr_notification = 1 WHERE id = ?`, id)
		require.NoError(t, err)
	}
	size := int64(1 << 30)
	_, err := conn.Exec(`UPDATE import_queue SET file_size = ?, storage_path = '/complete/tv/peer' WHERE id = ?`, size, id)
	require.NoError(t, err)
	state := database.NewPeernetBundleImportRepository(conn, database.DialectSQLite)
	require.NoError(t, state.SetQueueHold(context.Background(), id, true))
	return &peerHoldEnv{server: server, repo: repo, state: state, id: id, dlID: "nzo_peer"}
}

func (e *peerHoldEnv) queue(t *testing.T) SABnzbdQueueObject {
	t.Helper()
	app := fiber.New()
	app.Get("/sabnzbd", e.server.handleSABnzbdQueue)
	resp, err := app.Test(httptest.NewRequest("GET", "/sabnzbd?mode=queue&category=tv", nil))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out SABnzbdQueueResponse
	require.NoError(t, json.Unmarshal(body, &out), "body: %s", body)
	return out.Queue
}

func queueIDs(q SABnzbdQueueObject) []string {
	ids := make([]string, 0, len(q.Slots))
	for _, s := range q.Slots {
		ids = append(ids, s.NzoID)
	}
	return ids
}

func (e *peerHoldEnv) history(t *testing.T, query string) []SABnzbdHistorySlot {
	t.Helper()
	return getSABnzbdHistory(t, e.server, "category=tv"+query).Slots
}

func TestSABnzbdPeerHold_PendingIsInTheQueueAsWorkInProgressNotInHistory(t *testing.T) {
	e := newPeerHoldEnv(t, false)

	q := e.queue(t)
	require.Equal(t, []string{e.dlID}, queueIDs(q))
	assert.Equal(t, "Downloading", q.Slots[0].Status, "an existing in-progress status, never a terminal one")
	assert.Equal(t, "99", q.Slots[0].Percentage)
	assert.Equal(t, 1, q.Noofslots)

	assert.Empty(t, e.history(t, ""), "unverified content is not in the history")
	assert.Empty(t, e.history(t, "&nzo_ids="+e.dlID), "nor when asked for by id")
	assert.Empty(t, e.history(t, "&nzo_ids=1"), "nor by numeric id")
}

func TestSABnzbdPeerHold_PassedMovesToHistoryAsCompleted(t *testing.T) {
	e := newPeerHoldEnv(t, false)
	require.NoError(t, e.state.SetQueueHold(context.Background(), e.id, false))

	assert.Empty(t, queueIDs(e.queue(t)))
	h := e.history(t, "")
	require.Len(t, h, 1)
	assert.Equal(t, e.dlID, h[0].NzoID)
	assert.Equal(t, "Completed", h[0].Status)
	assert.Len(t, e.history(t, "&nzo_ids="+e.dlID), 1)
}

func TestSABnzbdPeerHold_FailedAndStaleAreFailedInHistoryWithAMessage(t *testing.T) {
	for name, msg := range map[string]string{
		"check failed": "Peer-supplied release failed its full health check and was removed",
		"stale":        "Peer-supplied release was not fully verified within 7 days and was removed",
	} {
		t.Run(name, func(t *testing.T) {
			e := newPeerHoldEnv(t, false)
			changed, err := e.state.FailHeldQueueItem(context.Background(), e.id, msg)
			require.NoError(t, err)
			require.True(t, changed)

			assert.Empty(t, queueIDs(e.queue(t)))
			h := e.history(t, "")
			require.Len(t, h, 1)
			assert.Equal(t, "Failed", h[0].Status)
			assert.Equal(t, msg, h[0].Fail_message)
			byID := e.history(t, "&nzo_ids="+e.dlID)
			require.Len(t, byID, 1)
			assert.Equal(t, "Failed", byID[0].Status)
		})
	}
}

func TestSABnzbdPeerHold_UserSkipStaysHiddenInEveryState(t *testing.T) {
	e := newPeerHoldEnv(t, true)
	assert.Empty(t, queueIDs(e.queue(t)), "held and skipped by the user: not in the queue")
	assert.Empty(t, e.history(t, ""))

	require.NoError(t, e.state.SetQueueHold(context.Background(), e.id, false))
	assert.Empty(t, e.history(t, ""), "passed: still hidden")
	assert.Empty(t, e.history(t, "&nzo_ids="+e.dlID))

	require.NoError(t, e.state.SetQueueHold(context.Background(), e.id, true))
	_, err := e.state.FailHeldQueueItem(context.Background(), e.id, "x")
	require.NoError(t, err)
	assert.Empty(t, e.history(t, ""), "failed: still hidden")
}
