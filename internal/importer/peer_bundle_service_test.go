package importer

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer/filesystem"
	"github.com/kipsilabs/altmount/internal/importer/postprocessor"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPeerService(t *testing.T) *Service {
	t.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	cfg := config.DefaultConfig()
	getCfg := func() *config.Config { return cfg }
	ms := metadata.NewMetadataService(t.TempDir())
	return &Service{
		database: db, metadataService: ms, configGetter: getCfg,
		postProcessor: postprocessor.NewCoordinator(postprocessor.Config{ConfigGetter: getCfg, MetadataService: ms}),
		log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func addItem(t *testing.T, s *Service, skip bool) *database.ImportQueueItem {
	t.Helper()
	item := &database.ImportQueueItem{NzbPath: "/x/a.nzb", Priority: database.QueuePriorityNormal, Status: database.QueueStatusProcessing,
		MaxRetries: 1, SkipArrNotification: skip}
	require.NoError(t, s.database.Repository.AddToQueue(context.Background(), item))
	return item
}

func apiRepo(s *Service) *database.Repository {
	return database.NewRepository(s.database.Connection(), s.database.Dialect())
}

func dbSkip(t *testing.T, s *Service, id int64) bool {
	got, err := s.database.Repository.GetQueueItem(context.Background(), id)
	require.NoError(t, err)
	return got.SkipArrNotification
}

func dbHold(t *testing.T, s *Service, id int64) bool {
	got, err := apiRepo(s).GetQueueItem(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, got)
	return got.PeerHold
}

func TestHoldPeerQueueItem(t *testing.T) {
	ctx := context.Background()
	s := newPeerService(t)
	item := addItem(t, s, false)

	assert.False(t, s.holdPeerQueueItem(ctx, item), "no hook: nothing held")
	assert.False(t, item.SkipArrNotification)

	s.peerBundle = &fakeFastPath{hold: map[int64]bool{item.ID + 1: true}}
	assert.False(t, s.holdPeerQueueItem(ctx, item), "not a peer import")
	assert.False(t, dbHold(t, s, item.ID))

	s.peerBundle = &fakeFastPath{hold: map[int64]bool{item.ID: true}}
	assert.True(t, s.holdPeerQueueItem(ctx, item))
	assert.True(t, item.SkipArrNotification, "the post-processing step sees the hold")
	assert.True(t, dbHold(t, s, item.ID), "the hold is persisted, so it survives a restart")
	assert.False(t, dbSkip(t, s, item.ID), "the user's skip flag is not used as the hold")
}

func TestAnnounceVerifiedImportLiftsTheHold(t *testing.T) {
	ctx := context.Background()
	s := newPeerService(t)
	item := addItem(t, s, false)
	require.NoError(t, s.bundleState().SetQueueHold(ctx, item.ID, true))
	require.NoError(t, s.AnnounceVerifiedImport(ctx, item.ID, "movies", "", "movies/x"))
	assert.False(t, dbHold(t, s, item.ID))
}

func TestAnnounceVerifiedImportHonoursTheUsersSkipAndAMissingItem(t *testing.T) {
	ctx := context.Background()
	s := newPeerService(t)
	item := addItem(t, s, true)
	require.NoError(t, s.bundleState().SetQueueHold(ctx, item.ID, true))
	require.NoError(t, s.AnnounceVerifiedImport(ctx, item.ID, "movies", "", "movies/x"))
	assert.False(t, dbHold(t, s, item.ID))
	assert.True(t, dbSkip(t, s, item.ID), "the user's own skip survives the hold")

	// The user removed the queue item while the check was pending.
	require.NoError(t, s.database.Repository.RemoveFromQueue(ctx, item.ID))
	require.NoError(t, s.AnnounceVerifiedImport(ctx, item.ID, "movies", "", "movies/x"))
}

func TestFailImportTurnsAHeldItemIntoAFailedOne(t *testing.T) {
	ctx := context.Background()
	s := newPeerService(t)
	item := addItem(t, s, false)
	require.NoError(t, s.bundleState().SetQueueHold(ctx, item.ID, true))
	require.NoError(t, s.FailImport(ctx, item.ID, "gone bad"))

	got, err := apiRepo(s).GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, database.QueueStatusFailed, got.Status)
	require.NotNil(t, got.ErrorMessage)
	assert.Equal(t, "gone bad", *got.ErrorMessage)
	assert.False(t, got.PeerHold)
	require.NoError(t, s.FailImport(ctx, item.ID, "again"), "idempotent")
	require.NoError(t, s.FailImport(ctx, 9999, "no such item"), "a removed item is fine")
}

func TestRecordPeerImportCreatesQueueAndHistoryEntries(t *testing.T) {
	ctx := context.Background()
	s := newPeerService(t)
	fp := &fakeFastPath{hold: map[int64]bool{1: true}}
	s.peerBundle = fp
	id, err := s.RecordPeerImport(ctx, PeerImportRecord{Title: "My Release", Category: "movies", ResultingPath: "movies/My Release",
		WrittenPaths: []string{"movies/My Release/a.mkv"}, TotalSize: 99, ReleaseID: "abcdef0123456789abcdef"})
	require.NoError(t, err)
	got, err := s.database.Repository.GetQueueItem(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, database.QueueStatusCompleted, got.Status)
	assert.Equal(t, "movies", *got.Category)
	assert.False(t, got.SkipArrNotification, "the user's skip flag is not the hold")
	assert.True(t, dbHold(t, s, id), "held from the ARRs until the full check passed")
	rows, err := database.NewRepository(s.database.Connection(), s.database.Dialect()).ListSABnzbdHistory(ctx, "", 10, 0)
	require.NoError(t, err)
	assert.Empty(t, rows, "a held import is not in the SABnzbd-compatible history")

	require.NoError(t, s.AnnounceVerifiedImport(ctx, id, "movies", "", "movies/My Release"))
	rows, err = database.NewRepository(s.database.Connection(), s.database.Dialect()).ListSABnzbdHistory(ctx, "", 10, 0)
	require.NoError(t, err)
	require.Len(t, rows, 1, "visible to the ARRs once verified")
	assert.Contains(t, rows[0].Name, "My Release")
}

// The queue row of a direct import has no NZB behind it. It must survive a
// restart as it is (not be taken for interrupted work and retried into a
// failure), and no action that needs the NZB may run on it.
func TestRecordPeerImportRowIsRestartSafeAndNotRetryable(t *testing.T) {
	ctx := context.Background()
	s := newPeerService(t)
	var midStatus database.QueueStatus
	var midPeer bool
	fp := &fakeFastPath{hold: map[int64]bool{1: true}}
	// A crash between AddToQueue and the end of the success handling leaves the
	// row as it is at this point; the restart recovery must not touch it.
	fp.onAttach = func(id int64) {
		require.NoError(t, s.database.Repository.ResetStaleItems(ctx))
		got, err := s.database.Repository.GetQueueItem(ctx, id)
		require.NoError(t, err)
		midStatus, midPeer = got.Status, got.IsPeerImport()
	}
	s.peerBundle = fp
	id, err := s.RecordPeerImport(ctx, PeerImportRecord{Title: "My Release", Category: "movies", ResultingPath: "movies/My Release",
		WrittenPaths: []string{"movies/My Release/a.mkv"}, TotalSize: 99, ReleaseID: "abcdef0123456789abcdef"})
	require.NoError(t, err)
	assert.True(t, midPeer)
	assert.Equal(t, database.QueueStatusCompleted, midStatus, "a restart mid-import does not reset the row to pending")

	got, err := s.database.Repository.GetQueueItem(ctx, id)
	require.NoError(t, err)
	assert.True(t, got.IsPeerImport())

	assert.ErrorIs(t, s.ExecuteItem(ctx, id), ErrPeerImportNoNZB)
	got, _ = s.database.Repository.GetQueueItem(ctx, id)
	assert.Equal(t, database.QueueStatusCompleted, got.Status, "a refused retry changes nothing")
}

func TestRecordPeerImportRollsTheQueueRowBackOnFailure(t *testing.T) {
	ctx := context.Background()
	s := newPeerService(t)
	s.peerBundle = &fakeFastPath{attach: assert.AnError}
	_, err := s.RecordPeerImport(ctx, PeerImportRecord{Title: "My Release", Category: "movies", ResultingPath: "movies/My Release",
		WrittenPaths: []string{"movies/My Release/a.mkv"}, TotalSize: 99, ReleaseID: "abcdef0123456789abcdef"})
	require.Error(t, err)
	items, err := apiRepo(s).ListQueueItems(ctx, nil, "", "", 10, 0, "updated_at", "desc")
	require.NoError(t, err)
	assert.Empty(t, items, "no completed row without files is left behind")
}

func TestRecordPeerImportKeepsMediaMetadataAndDownloadID(t *testing.T) {
	ctx := context.Background()
	s := newPeerService(t)
	meta := `{"imdb_id":"tt0111161","type":"movie"}`
	dl := "stremio:abc"
	id, err := s.RecordPeerImport(ctx, PeerImportRecord{Title: "My Release", Category: "movies", ResultingPath: "movies/My Release",
		WrittenPaths: []string{"movies/My Release/a.mkv"}, TotalSize: 99, ReleaseID: "abcdef0123456789abcdef", Metadata: meta, DownloadID: dl})
	require.NoError(t, err)
	got, err := s.database.Repository.GetQueueItem(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, got.Metadata)
	assert.Equal(t, meta, *got.Metadata)
	require.NotNil(t, got.DownloadID)
	assert.Equal(t, dl, *got.DownloadID)
	o, err := database.NewPeernetOriginRepository(s.database.Connection(), s.database.Dialect()).OriginForPath(ctx, "movies/My Release/a.mkv")
	require.NoError(t, err)
	assert.Equal(t, meta, o.QueueMetadata, "the record published for this release carries the media ids")
}

func TestPeerImportDirFollowsTheNormalImportRules(t *testing.T) {
	s := newPeerService(t)
	cfg := s.configGetter()
	cfg.SABnzbd.CompleteDir = "/complete"
	for _, category := range []string{"Movies", "TV", "no-such-category"} {
		// The virtual folder a queued NZB of this category and title lands in.
		item := &database.ImportQueueItem{NzbPath: filepath.Join(os.TempDir(), ".altmount-queue", "x.nzb"), Category: &category}
		base := cfg.SABnzbd.CompleteDir
		want := filesystem.NzbFolderPath(s.calculateProcessVirtualDir(item, &base), "My.Title.2020.1080p.nzb")
		got := s.PeerImportDir(category, "My.Title.2020.1080p")
		assert.Equal(t, want, got, category)
		assert.Contains(t, got, "My.Title.2020.1080p")
	}
	assert.NotEqual(t, s.PeerImportDir("Movies", "A"), s.PeerImportDir("TV", "A"), "the category decides the folder")
}

func TestPeerImportDirRefusesTitlesThatEscapeTheCategoryFolder(t *testing.T) {
	s := newPeerService(t)
	cfg := s.configGetter()
	cfg.SABnzbd.CompleteDir = "/complete"
	parent := s.PeerImportDir("Movies", "ok")
	catDir := path.Dir(parent)
	for _, title := range []string{"...mkv", ".mkv", "..", ".", "", ".hidden", "..mkv.nzb", ".nzbs", "patches", "corrupted_metadata", "_migrated", ".peernet-staging", "PATCHES"} {
		got := s.PeerImportDir("Movies", title)
		if got != "" {
			assert.Equal(t, catDir, path.Dir(got), "title %q must stay one level below the category folder, got %q", title, got)
			assert.NotContains(t, []string{".", "..", ""}, path.Base(got))
		}
		assert.Empty(t, got, "title %q must be refused", title)
	}
	assert.Equal(t, catDir+"/Some Movie", s.PeerImportDir("Movies", "Some Movie.mkv"))
}
