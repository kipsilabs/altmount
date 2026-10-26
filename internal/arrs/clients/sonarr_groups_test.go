package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/stretchr/testify/require"
	"golift.io/starr"
)

func TestLegacyQueueGroupsAcrossPages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "1" {
			_, _ = w.Write([]byte(`{"totalRecords":2,"records":[{"id":1,"seriesId":10,"episodeId":20,"downloadId":"ABC","downloadClient":"AltMount"}]}`))
		} else {
			_, _ = w.Write([]byte(`{"totalRecords":2,"records":[{"id":2,"seriesId":10,"episodeId":21,"downloadId":"abc","downloadClient":"AltMount"}]}`))
		}
	}))
	defer srv.Close()
	c := NewLegacySonarr(&starr.Config{URL: srv.URL, APIKey: "key", Client: srv.Client()})
	groups, err := c.QueueGroups(context.Background())
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, []int64{20, 21}, groups[0].EpisodeIDs)
	require.Equal(t, []int64{1, 2}, groups[0].QueueIDs)
}
func TestNativeQueueAndRemovalContract(t *testing.T) {
	var deleted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "key", r.Header.Get("X-Api-Key"))
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "GET /base/api/v5/queue":
			_ = json.NewEncoder(w).Encode(map[string]any{"totalRecords": 1, "records": []any{map[string]any{"id": 3, "seriesId": 10, "episodeIds": []int{20, 21, 20}, "downloadId": "pack", "isFullSeason": true, "episodesWithFilesCount": 1, "seasonNumbers": []int{1}}}})
		case "DELETE /base/api/v5/queue/3":
			require.Equal(t, "removeFromClient", r.URL.Query().Get("removalMethod"))
			require.Equal(t, "blocklistOnly", r.URL.Query().Get("blocklistMethod"))
			require.Empty(t, r.URL.Query().Get("blocklist"))
			deleted = true
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := newNativeSonarr(&starr.Config{URL: srv.URL + "/base", APIKey: "key", Client: srv.Client()})
	groups, err := c.QueueGroups(context.Background())
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, []int64{20, 21}, groups[0].EpisodeIDs)
	require.True(t, *groups[0].IsFullSeason)
	q, err := c.GetQueueContext(context.Background(), 0, 1)
	require.NoError(t, err)
	require.Len(t, q.Records, 2)
	require.Equal(t, int64(21), q.Records[1].EpisodeID)
	require.NoError(t, c.RemoveDownload(context.Background(), groups[0], model.SonarrRemovalOptions{RemoveFromClient: true, Blocklist: true}))
	require.True(t, deleted)
}

func TestLegacyGroupingSeparatesUnknownAndConflictingIdentities(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"totalRecords":7,"records":[null,{"id":1,"seriesId":10,"episodeId":20,"downloadId":"pack","downloadClient":"A"},{"id":2,"seriesId":11,"episodeId":21,"downloadId":"PACK","downloadClient":"A"},{"id":3,"seriesId":10,"episodeId":22,"downloadId":"pack","downloadClient":"B"},{"id":4,"episodeId":23},{"id":5,"episodeId":24},{"id":6,"episodeId":-1}]}`))
	}))
	defer srv.Close()
	c := NewLegacySonarr(&starr.Config{URL: srv.URL, Client: srv.Client()})
	groups, err := c.QueueGroups(context.Background())
	require.NoError(t, err)
	require.Len(t, groups, 6)
	require.Empty(t, groups[5].EpisodeIDs)
}

func TestNativeRemovalMethods(t *testing.T) {
	for _, tc := range []struct {
		name               string
		opts               model.SonarrRemovalOptions
		removal, blocklist string
		invalid            bool
	}{
		{name: "ignore", removal: "ignore", blocklist: "doNotBlocklist"},
		{name: "remove", opts: model.SonarrRemovalOptions{RemoveFromClient: true}, removal: "removeFromClient", blocklist: "doNotBlocklist"},
		{name: "category", opts: model.SonarrRemovalOptions{ChangeCategory: true}, removal: "changeCategory", blocklist: "doNotBlocklist"},
		{name: "blocklist", opts: model.SonarrRemovalOptions{RemoveFromClient: true, Blocklist: true}, removal: "removeFromClient", blocklist: "blocklistOnly"},
		{name: "search", opts: model.SonarrRemovalOptions{RemoveFromClient: true, Blocklist: true, Search: true}, removal: "removeFromClient", blocklist: "blocklistAndSearch"},
		{name: "conflicting remove", opts: model.SonarrRemovalOptions{RemoveFromClient: true, ChangeCategory: true}, invalid: true},
		{name: "search without blocklist", opts: model.SonarrRemovalOptions{Search: true}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, "DELETE", r.Method)
				require.Equal(t, "/api/v5/queue/1", r.URL.Path)
				require.Equal(t, tc.removal, r.URL.Query().Get("removalMethod"))
				require.Equal(t, tc.blocklist, r.URL.Query().Get("blocklistMethod"))
				w.WriteHeader(500)
			}))
			defer srv.Close()
			c := newNativeSonarr(&starr.Config{URL: srv.URL, Client: srv.Client()})
			err := c.RemoveDownload(context.Background(), model.SonarrDownloadGroup{QueueIDs: []int64{1, 2}}, tc.opts)
			require.Error(t, err)
			if tc.invalid {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls, "mutation errors must never cause retries, sibling deletes, or downgrade")
			}
		})
	}
}

func TestNativeQueuePaginationRetainsPackHintsAndTargets(t *testing.T) {
	pages := []string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		pages = append(pages, page)
		w.Header().Set("Content-Type", "application/json")
		if page == "1" {
			_, _ = w.Write([]byte(`{"page":1,"totalRecords":2,"records":[{"id":1,"seriesId":10,"episodeIds":[20,21],"downloadId":"pack","seasonNumbers":[1,2],"isFullSeason":false,"episodesWithFilesCount":0}]}`))
		} else {
			_, _ = w.Write([]byte(`{"page":2,"totalRecords":2,"records":[{"id":2,"seriesId":11,"episodeIds":[22,23],"downloadId":"other"}]}`))
		}
	}))
	defer srv.Close()
	c := newNativeSonarr(&starr.Config{URL: srv.URL, Client: srv.Client()})
	groups, err := c.QueueGroups(context.Background())
	require.NoError(t, err)
	require.Len(t, groups, 2)
	require.Equal(t, []string{"1", "2"}, pages)
	require.Equal(t, []int64{20, 21}, groups[0].EpisodeIDs)
	require.Equal(t, []int{1, 2}, groups[0].SeasonNumbers)
	require.NotNil(t, groups[0].IsFullSeason)
	require.False(t, *groups[0].IsFullSeason)
	require.Equal(t, 0, *groups[0].EpisodesWithFilesCount)
	require.Nil(t, groups[1].IsFullSeason)
	require.Nil(t, groups[1].EpisodesWithFilesCount)
}
