package scanner

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/stretchr/testify/require"
	"golift.io/starr/sonarr"
)

func TestPlanSonarrPackRepair_PartialCoverage(t *testing.T) {
	var snapshot model.SonarrRepairSnapshot
	for id := int64(1); id <= 10; id++ {
		snapshot.Episodes = append(snapshot.Episodes, model.SonarrEpisodeRepairState{
			ID: id, Known: true, Monitored: true, Healthy: id <= 8, ReplacementPending: id == 9,
		})
	}
	plan := planSonarrPackRepair(snapshot)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8}, plan.Satisfied)
	require.Equal(t, []int64{9}, plan.Covered)
	require.Equal(t, []int64{10}, plan.Search)
	require.Empty(t, plan.Unknown)
}

func TestPlanSonarrPackRepair_PreservesIdentityAndEligibility(t *testing.T) {
	snapshot := model.SonarrRepairSnapshot{Episodes: []model.SonarrEpisodeRepairState{
		{ID: 1, Known: true, Monitored: true, FileID: 30}, // corrupt shared file
		{ID: 2, Known: true, Monitored: true, FileID: 30},
		{ID: 2, Known: true, Monitored: true, FileID: 30},                // duplicate
		{ID: 3, Known: true, Monitored: true, Healthy: true, FileID: 31}, // healthy replacement
		{ID: 4, Known: true, Monitored: true, Exhausted: true},
		{ID: 5, Known: false, Monitored: true}, // failed/incomplete read
		{ID: 6, Known: true, Monitored: false},
		{ID: 0, Known: true},
	}}
	plan := planSonarrPackRepair(snapshot)
	require.Equal(t, []int64{1, 2}, plan.Search)
	require.Equal(t, []int64{30}, plan.FileIDs)
	require.Equal(t, []int64{3}, plan.Satisfied)
	require.Equal(t, []int64{4}, plan.GiveUp)
	require.Equal(t, []int64{5}, plan.Unknown)
	require.Equal(t, []int64{6}, plan.Ignored)
}

func TestSettleSonarrAutoRedownload_PartialQueuedReplacementDoesNotEndPackRepair(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	stub.extraEpisodeID = 11
	stub.queueEpisodeIDs = []int64{10}
	stub.commands = []arrCommandResponse{autoEpisodeSearch(1, 10, 11)}
	stub.statuses[1] = []string{"completed"}
	srv := stub.serve()
	defer srv.Close()
	mgr := testManager(2, 0)
	_, state, err := mgr.settleSonarrAutoRedownload(context.Background(), sonarrClient(t, srv), "sonarr", sonarrRedownloadTarget{seriesID: 7, episodeIDs: []int64{10, 11}, episodeFileID: 500}, time.Now().Add(-time.Second))
	require.NoError(t, err)
	require.Equal(t, redownloadProceed, state)
	ids, err := unresolvedSonarrRepairEpisodes(context.Background(), sonarrClient(t, srv), []int64{10, 11}, 500)
	require.NoError(t, err)
	require.Equal(t, []int64{11}, ids)
}

func TestSonarrAutomaticSearchMatchingUsesSeriesAndSeason(t *testing.T) {
	since := time.Now().Add(-time.Second)
	searches := []*arrCommand{
		{ID: 1, Name: "SeasonSearch", Status: "started", Body: map[string]any{"seriesId": float64(7), "seasonNumber": float64(2)}},
		{ID: 2, Name: "SeasonSearch", Status: "started", Body: map[string]any{"seriesId": float64(7), "seasonNumber": float64(1)}},
		{ID: 3, Name: "EpisodeSearch", Status: "queued", Body: map[string]any{"episodeIds": []any{float64(11)}}},
	}
	matches := findSonarrAutoCommands(searches, sonarrRedownloadTarget{seriesID: 7, seasonNumbers: []int64{1}, episodeIDs: []int64{10, 11}}, since)
	require.Len(t, matches, 2)
	require.Equal(t, int64(2), matches[0].ID)
	require.Equal(t, int64(3), matches[1].ID)
}

func TestSettleSonarrAutoRedownload_OwnStaleQueueDoesNotCoverRepair(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	stub.queueEpisodeIDs = []int64{10}
	stub.queueDownloadID = "failed-download"
	srv := stub.serve()
	defer srv.Close()
	_, state, err := testManager(0, 0).settleSonarrAutoRedownload(context.Background(), sonarrClient(t, srv), "sonarr", sonarrRedownloadTarget{episodeIDs: []int64{10}, episodeFileID: 500, downloadID: "FAILED-DOWNLOAD"}, time.Now())
	require.NoError(t, err)
	require.Equal(t, redownloadProceed, state)
	require.Equal(t, 1, stub.deletes)
	ids, err := unresolvedSonarrRepairEpisodes(context.Background(), sonarrClient(t, srv), []int64{10}, 500, "FAILED-DOWNLOAD")
	require.NoError(t, err)
	require.Equal(t, []int64{10}, ids)
}

func TestFailSonarrQueueItemByPath_LeavesForeignClientUntouched(t *testing.T) {
	stub := newArrStub()
	stub.queueEpisodeIDs = []int64{10}
	srv := stub.serve()
	defer srv.Close()
	err := testManager(0, 0).failSonarrQueueItemByPath(context.Background(), sonarrClient(t, srv), "sonarr", "/downloads/file.mkv")
	require.Error(t, err)
	require.Zero(t, stub.deletes)
}

func TestScannerRepair_OnePackDoesNotSuppressDifferentBrokenFiles(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	srv := stub.serve()
	defer srv.Close()
	mgr := testManager(0, 0)
	mgr.failures = failures.NewTracker()
	metadata := &model.WebhookMetadata{DownloadID: "one-pack", Series: &model.SeriesMetadata{Id: 7}, EpisodeFile: &model.EpisodeFileMetadata{Id: 500, SceneName: "broken"}}
	client := sonarrClient(t, srv)
	require.NoError(t, mgr.triggerSonarrRescanByPath(context.Background(), client, "/tv/first.mkv", "first.mkv", "sonarr", metadata))
	stub.mu.Lock()
	stub.episodeFileID = 600
	stub.primaryEpisodeID = 11
	stub.mu.Unlock()
	metadata.EpisodeFile.Id = 600
	require.NoError(t, mgr.triggerSonarrRescanByPath(context.Background(), client, "/tv/second.mkv", "second.mkv", "sonarr", metadata))
	require.Len(t, stub.postedSonarr, 2)
	require.Equal(t, []int64{10}, stub.postedSonarr[0].EpisodeIDs)
	require.Equal(t, []int64{11}, stub.postedSonarr[1].EpisodeIDs)
	require.Equal(t, 1, stub.failedHistory, "the same download is blocklisted once")
}

func TestScannerQueueFallbackPassesAllPackTargetsToGroupedExecutor(t *testing.T) {
	stub := newArrStub()
	stub.queueEpisodeIDs = []int64{10, 11}
	stub.queueDownloadID = "pack"
	stub.queueClient = "AltMount"
	stub.queueOutputPath = "/downloads/pack"
	srv := stub.serve()
	defer srv.Close()
	mgr := testManager(0, 0)
	var targets []int64
	mgr.SetSonarrQueueRepair(func(ctx context.Context, instance string, c clients.SonarrClient, g model.SonarrDownloadGroup) error {
		targets = g.EpisodeIDs
		return nil
	})
	require.NoError(t, mgr.failSonarrQueueItemByPath(context.Background(), sonarrClient(t, srv), "sonarr", "/downloads/pack/episode.mkv"))
	require.Equal(t, []int64{10, 11}, targets)
}

func TestSettleSonarrAutoRedownload_DoesNotSearchBeforeFileDeletionIsConfirmed(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	stub.clearOnDelete = false
	srv := stub.serve()
	defer srv.Close()
	_, _, err := testManager(0, 1).settleSonarrAutoRedownload(context.Background(), sonarrClient(t, srv), "sonarr", sonarrRedownloadTarget{episodeIDs: []int64{10}, episodeFileID: 500}, time.Now())
	require.Error(t, err, "a still-linked file must defer replacement search")
	require.Empty(t, stub.postedSonarr)
}

func TestScannerRepair_RetainsTargetsAfterDeletionAndRefreshFailure(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	stub.failQueueAfterDelete = true
	srv := stub.serve()
	defer srv.Close()
	mgr := testManager(0, 0)
	mgr.failures = failures.NewTracker()
	metadata := &model.WebhookMetadata{DownloadID: "one-pack", Series: &model.SeriesMetadata{Id: 7}, EpisodeFile: &model.EpisodeFileMetadata{Id: 500, SceneName: "broken"}}
	client := sonarrClient(t, srv)
	require.Error(t, mgr.triggerSonarrRescanByPath(context.Background(), client, "/tv/file.mkv", "file.mkv", "sonarr", metadata))
	require.Equal(t, 1, stub.deletes)
	require.NoError(t, mgr.triggerSonarrRescanByPath(context.Background(), client, "/tv/file.mkv", "file.mkv", "sonarr", metadata))
	require.Equal(t, 1, stub.deletes)
	require.Len(t, stub.postedSonarr, 1)
	require.Equal(t, []int64{10}, stub.postedSonarr[0].EpisodeIDs)
}

func TestScannerHistoryFailure_ReconcilesAcceptedMutation(t *testing.T) {
	stub := newArrStub()
	stub.failHistoryOnce = true
	srv := stub.serve()
	defer srv.Close()
	mgr := testManager(0, 0)
	mgr.failures = failures.NewTracker()
	client := sonarrClient(t, srv)
	event := failures.SonarrDownloadKey("sonarr", "one-pack")
	require.Error(t, mgr.blocklistSonarrRepairOnce(context.Background(), client, event, 7, 500, "file.mkv", []int64{10}, "broken"))
	require.NoError(t, mgr.blocklistSonarrRepairOnce(context.Background(), client, event, 7, 500, "file.mkv", []int64{10}, "broken"))
	require.Equal(t, 1, stub.failedHistory)
}

func TestScannerPackMetadata_BreakerNeverUnmonitorsHealthyOrCoveredEpisodes(t *testing.T) {
	stub := newArrStub()
	stub.queueEpisodeIDs = []int64{9}
	stub.queueDownloadID = "replacement-nine"
	metadata := &model.WebhookMetadata{DownloadID: "one-pack", Series: &model.SeriesMetadata{Id: 7}}
	for id := int64(1); id <= 10; id++ {
		ep := &sonarr.Episode{ID: id, Monitored: true}
		if id <= 8 {
			ep.EpisodeFileID = 100 + id
			ep.HasFile = true
		}
		stub.episodeStates = append(stub.episodeStates, ep)
		metadata.Episodes = append(metadata.Episodes, model.EpisodeMetadata{Id: id})
	}
	srv := stub.serve()
	defer srv.Close()
	mgr := testManager(0, 0)
	mgr.failures = failures.NewTracker()
	mgr.configGetter().Arrs.QueueCleanupMaxFailures = 1
	require.NoError(t, mgr.triggerSonarrRescanByPath(context.Background(), sonarrClient(t, srv), "/tv/missing.mkv", "missing.mkv", "sonarr", metadata))
	require.Equal(t, [][]int64{{10}}, stub.unmonitored)
	require.Empty(t, stub.postedSonarr)
	for id := int64(1); id <= 9; id++ {
		require.Equal(t, 1, mgr.failures.Bump(failures.EpisodeKey("sonarr", id)), "healthy/covered episode %d was counted", id)
	}
}

func TestScannerRetryStillWaitsForOriginalAutomaticCommand(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	automatic := autoEpisodeSearch(1, 10)
	automatic.Queued = time.Now().Add(time.Second)
	stub.commands = []arrCommandResponse{automatic}
	stub.statuses[1] = []string{"started"}
	srv := stub.serve()
	defer srv.Close()
	mgr := testManager(1, 0)
	mgr.failures = failures.NewTracker()
	metadata := &model.WebhookMetadata{DownloadID: "one-pack", Series: &model.SeriesMetadata{Id: 7}, EpisodeFile: &model.EpisodeFileMetadata{Id: 500, SceneName: "broken"}}
	client := sonarrClient(t, srv)
	require.Error(t, mgr.triggerSonarrRescanByPath(context.Background(), client, "/tv/file.mkv", "file.mkv", "sonarr", metadata))
	require.Error(t, mgr.triggerSonarrRescanByPath(context.Background(), client, "/tv/file.mkv", "file.mkv", "sonarr", metadata))
	require.Zero(t, stub.deletes)
	require.Empty(t, stub.postedSonarr)
	require.Equal(t, 1, stub.failedHistory)
}

func TestScannerExhaustedTargetsStillReconcileCleanupAfterUnmonitor(t *testing.T) {
	stub := newArrStub()
	stub.failHistoryOnce = true
	stub.episodeStates = []*sonarr.Episode{{ID: 10, Monitored: true, EpisodeFileID: 500}}
	srv := stub.serve()
	defer srv.Close()
	mgr := testManager(0, 0)
	mgr.failures = failures.NewTracker()
	mgr.configGetter().Arrs.QueueCleanupMaxFailures = 1
	metadata := &model.WebhookMetadata{DownloadID: "one-pack", Series: &model.SeriesMetadata{Id: 7}, EpisodeFile: &model.EpisodeFileMetadata{Id: 500, SceneName: "broken"}}
	client := sonarrClient(t, srv)
	require.Error(t, mgr.triggerSonarrRescanByPath(context.Background(), client, "/tv/file.mkv", "file.mkv", "sonarr", metadata))
	require.Equal(t, [][]int64{{10}}, stub.unmonitored)
	require.NoError(t, mgr.triggerSonarrRescanByPath(context.Background(), client, "/tv/file.mkv", "file.mkv", "sonarr", metadata))
	require.Equal(t, 1, stub.deletes, "cleanup must resume despite no searchable targets")
	require.Equal(t, 1, stub.failedHistory)
	require.Empty(t, stub.postedSonarr)
}
