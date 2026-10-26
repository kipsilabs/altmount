package worker

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/stretchr/testify/require"
)

func TestImportFailurePack_MixedThresholdsPreserveImportsAndSearchEligibleSiblings(t *testing.T) {
	fake := &fakeSonarrQueue{records: []map[string]any{
		queueRecord(101, 42, "pack", "AltMount"), queueRecord(102, 43, "pack", "AltMount"), queueRecord(103, 44, "pack", "AltMount"),
	}, episodeFileIDs: map[int64]int64{44: 777}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)
	w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42))
	w.HandleImportFailure(context.Background(), "pack", "tv")
	require.Equal(t, [][]int64{{42}}, fake.unmonitored)
	require.Len(t, fake.deletes, 1)
	require.Equal(t, [][]int64{{43}}, fake.searches)
	// Same callback, including another case variant, is not a new generation.
	w.HandleImportFailure(context.Background(), "PACK", "tv")
	require.Len(t, fake.deletes, 1)
	require.Len(t, fake.searches, 1)
	require.Equal(t, 1, w.breaker.BumpOnce(failures.EpisodeKey("test-sonarr", 43), failures.SonarrDownloadKey("test-sonarr", "pack")))
}

func TestImportFailurePack_UnmonitorFailureStopsReplacement(t *testing.T) {
	fake := &fakeSonarrQueue{records: []map[string]any{queueRecord(101, 42, "pack", "AltMount")}, monitorFails: true}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 1)
	w.HandleImportFailure(context.Background(), "pack", "tv")
	require.Empty(t, fake.deletes)
	require.Empty(t, fake.searches)
}

func TestImportFailurePack_DuplicateBelowThresholdDoesNotCountAgain(t *testing.T) {
	fake := &fakeSonarrQueue{records: []map[string]any{queueRecord(101, 42, "pack", "AltMount"), queueRecord(102, 42, "pack", "AltMount")}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)
	w.HandleImportFailure(context.Background(), "pack", "tv")
	w.HandleImportFailure(context.Background(), "pack", "tv")
	require.Empty(t, fake.unmonitored)
	require.Empty(t, fake.deletes)
}

func TestSonarrPackImportedResetsOnlySatisfiedEpisodes(t *testing.T) {
	fake := &fakeSonarrQueue{episodeFileIDs: map[int64]int64{42: 777}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)
	w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42))
	w.breaker.Bump(failures.EpisodeKey("test-sonarr", 43))
	client, err := w.clients.GetOrCreateSonarrClient("test-sonarr", srv.URL, "key", "v3")
	require.NoError(t, err)
	imported, err := w.confirmSonarrPackImported(context.Background(), client, "test-sonarr", []int64{42, 43})
	require.NoError(t, err)
	require.False(t, imported)
	require.Equal(t, 1, w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42)))
	require.Equal(t, 2, w.breaker.Bump(failures.EpisodeKey("test-sonarr", 43)))
}

func TestPackRepairV3AndV5HaveEquivalentEpisodeOutcomes(t *testing.T) {
	for _, native := range []bool{false, true} {
		name := "v3"
		if native {
			name = "v5"
		}
		t.Run(name, func(t *testing.T) {
			fake := &fakeSonarrQueue{native: native, episodeFileIDs: map[int64]int64{44: 777}}
			if native {
				row := queueRecord(101, 0, "pack", "AltMount")
				row["episodeIds"] = []int64{42, 43, 44}
				row["isFullSeason"] = true
				row["episodesWithFilesCount"] = 1
				fake.records = []map[string]any{row}
			} else {
				fake.records = []map[string]any{queueRecord(101, 42, "pack", "AltMount"), queueRecord(102, 43, "pack", "AltMount"), queueRecord(103, 44, "pack", "AltMount")}
			}
			srv := httptest.NewServer(fake.handler())
			defer srv.Close()
			w := newImportFailureTestWorker(srv.URL, 2)
			w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42))
			w.HandleImportFailure(context.Background(), "pack", "tv")
			require.Equal(t, [][]int64{{42}}, fake.unmonitored)
			require.Equal(t, [][]int64{{43}}, fake.searches)
			require.Len(t, fake.deletes, 1)
			if native {
				require.True(t, strings.Contains(fake.deletes[0], "blocklistMethod=blocklistOnly"))
				require.True(t, strings.Contains(fake.deletes[0], "removalMethod=removeFromClient"))
			} else {
				require.True(t, strings.Contains(fake.deletes[0], "skipRedownload=true"))
			}
		})
	}
}
