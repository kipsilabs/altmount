package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Sonarr and Radarr poll SABnzbd history with start=0 and a fixed limit
// (DownloadClientHistoryLimit, default 60) and never read a later page. A
// tracked download that is in neither the queue nor that first page is made
// untrackable without a downloadFailed event, so a failed job must not be
// pushed off the first page by newer completed jobs (#498).

func newSABnzbdHistoryTestServer(t *testing.T) (*Server, *database.Repository, *sql.DB) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := database.NewDB(database.Config{Type: "sqlite", DatabasePath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	repo := database.NewRepository(db.Connection(), db.Dialect())
	server := &Server{
		queueRepo:       repo,
		importerService: &importer.Service{}, // non-nil sentinel, not used by the history handler
	}
	return server, repo, db.Connection()
}

// addHistoryQueueRow inserts an import_queue row with the given status,
// category, download id and completion time.
func addHistoryQueueRow(t *testing.T, repo *database.Repository, conn *sql.DB, name string, status database.QueueStatus, category, downloadID string, at time.Time) int64 {
	t.Helper()

	cat := category
	item := &database.ImportQueueItem{
		NzbPath:  "/nzbs/" + name + ".nzb",
		Status:   database.QueueStatusPending,
		Priority: database.QueuePriorityNormal,
		Category: &cat,
	}
	require.NoError(t, repo.AddToQueue(context.Background(), item))

	ts := at.UTC().Format("2006-01-02 15:04:05")
	_, err := conn.Exec(`UPDATE import_queue
		SET status = ?, category = ?, download_id = ?, created_at = ?, updated_at = ?, completed_at = ?
		WHERE id = ?`, string(status), category, downloadID, ts, ts, ts, item.ID)
	require.NoError(t, err)
	return item.ID
}

func getSABnzbdHistory(t *testing.T, server *Server, query string) SABnzbdHistoryObject {
	t.Helper()

	app := fiber.New()
	app.Get("/sabnzbd", server.handleSABnzbdHistory)

	resp, err := app.Test(httptest.NewRequest("GET", "/sabnzbd?mode=history&"+query, nil))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out SABnzbdCompleteHistoryResponse
	require.NoError(t, json.Unmarshal(body, &out), "body: %s", body)
	return out.History
}

func slotIDs(slots []SABnzbdHistorySlot) []string {
	ids := make([]string, 0, len(slots))
	for _, s := range slots {
		ids = append(ids, s.NzoID)
	}
	return ids
}

// seedBurst creates one failed job in "tv", then `completed` newer completed
// jobs in "tv", plus one failed job in "movies" that a "tv" request must not see.
func seedBurst(t *testing.T, completed int) (*Server, []string) {
	t.Helper()

	server, repo, conn := newSABnzbdHistoryTestServer(t)
	base := time.Now().UTC().Add(-2 * time.Hour)

	addHistoryQueueRow(t, repo, conn, "failed-tv", database.QueueStatusFailed, "tv", "nzo_failed_tv", base)
	addHistoryQueueRow(t, repo, conn, "failed-movies", database.QueueStatusFailed, "movies", "nzo_failed_movies", base)

	// Newest first, the order the history view returns them in.
	newestFirst := make([]string, completed)
	for i := 0; i < completed; i++ {
		id := fmt.Sprintf("nzo_done_%02d", i)
		addHistoryQueueRow(t, repo, conn, fmt.Sprintf("done-%02d", i), database.QueueStatusCompleted, "tv", id,
			base.Add(time.Duration(i+1)*time.Minute))
		newestFirst[completed-1-i] = id
	}
	return server, newestFirst
}

func TestSABnzbdHistory_FailedRowOnFirstPageDuringBurst(t *testing.T) {
	const limit = 5
	server, newestCompleted := seedBurst(t, 8)

	page := getSABnzbdHistory(t, server, fmt.Sprintf("start=0&limit=%d&category=tv", limit))
	ids := slotIDs(page.Slots)

	// The failed tv job is older than every completed job, so by time alone it
	// is on page 2. It must still be on page 0.
	require.Contains(t, ids, "nzo_failed_tv", "failed job fell off the first history page")
	for _, s := range page.Slots {
		if s.NzoID == "nzo_failed_tv" {
			assert.Equal(t, "Failed", s.Status)
		}
	}

	// The normal page is untouched: the `limit` newest completed jobs, in order.
	require.GreaterOrEqual(t, len(ids), limit)
	assert.Equal(t, newestCompleted[:limit], ids[:limit])

	// The category filter applies to the extra rows as well.
	assert.NotContains(t, ids, "nzo_failed_movies")

	// noofslots stays the real total of the tv view: 8 completed + 1 failed.
	assert.Equal(t, 9, page.Noofslots)

	// Slot indices stay unique and continue after the normal page.
	for i, s := range page.Slots {
		assert.Equal(t, i, s.Index)
	}
}

func TestSABnzbdHistory_LaterPagesUnchanged(t *testing.T) {
	server, newestCompleted := seedBurst(t, 8)

	page := getSABnzbdHistory(t, server, "start=5&limit=5&category=tv")
	// Natural order: completed 5..7, then the failed job. No extra rows here.
	assert.Equal(t, append(append([]string{}, newestCompleted[5:]...), "nzo_failed_tv"), slotIDs(page.Slots))
	assert.Equal(t, 9, page.Noofslots)
	assert.Equal(t, 5, page.Slots[0].Index)
}

func TestSABnzbdHistory_FailedAlreadyOnPageNotDuplicated(t *testing.T) {
	server, _ := seedBurst(t, 2)

	page := getSABnzbdHistory(t, server, "start=0&limit=10&category=tv")
	assert.Equal(t, []string{"nzo_done_01", "nzo_done_00", "nzo_failed_tv"}, slotIDs(page.Slots))
	assert.Equal(t, 3, page.Noofslots)
}

func TestSABnzbdHistory_ByNzoIDsUnchanged(t *testing.T) {
	server, _ := seedBurst(t, 8)

	page := getSABnzbdHistory(t, server, "start=0&limit=5&category=tv&nzo_ids=nzo_done_03")
	assert.Equal(t, []string{"nzo_done_03"}, slotIDs(page.Slots))
}
