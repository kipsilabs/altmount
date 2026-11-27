package api

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/database"
)

func stremioQueueItemForDisplay(t *testing.T, candidate stremioPlayCandidate) *database.ImportQueueItem {
	t.Helper()
	metadata, err := encodeStremioQueueMetadata(stremioQueueMetadata{
		ContentKey: "movie:tt1234567:0:0",
		ReleaseKey: stremioReleaseKey(candidate),
	})
	require.NoError(t, err)
	return &database.ImportQueueItem{
		NzbPath:    "/tmp/altmount-uploads/" + stremioPlayNzbFilename(candidate),
		DownloadID: strPtr(stremioDownloadIDPrefix + "display-test"),
		Metadata:   metadata,
	}
}

func TestQueueItemDisplayNameHidesStremioImporterSuffix(t *testing.T) {
	candidate := stremioPlayCandidate{
		SafeTitle:   "Cult.2026.2160p.AMZN.WEB-DL.MULTi.DDP5.1.H.265-JeRi",
		DownloadURL: "https://indexer.example/1/download",
		Indexer:     "Indexer",
		Source:      "prowlarr",
	}
	item := stremioQueueItemForDisplay(t, candidate)

	resp := ToQueueItemResponse(item)
	assert.Equal(t, candidate.SafeTitle+".nzb", resp.NzbDisplayName)
	assert.Equal(t, item.NzbPath, resp.NzbPath, "nzb_path must keep the hash")
}

func TestQueueItemDisplayNameLeavesNonStremioNames(t *testing.T) {
	tests := []struct {
		name       string
		nzbPath    string
		downloadID *string
	}{
		{
			name:       "radarr release ending in a hex-like tag",
			nzbPath:    "/downloads/Some.Movie.2024.1080p.WEB-DL.x264-DEADBEEF.nzb",
			downloadID: strPtr("radarr-release-guid"),
		},
		{
			name:       "sabnzbd release ending in a CRC",
			nzbPath:    "/downloads/Show.S01E01.720p.HDTV-0123456789abcdef.nzb",
			downloadID: strPtr("SABnzbd_nzo_0123456789abcdef"),
		},
		{
			name:    "manual upload ending in hex",
			nzbPath: "/tmp/altmount-uploads/My.Upload-0123456789abcdef.nzb",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &database.ImportQueueItem{NzbPath: tt.nzbPath, DownloadID: tt.downloadID}
			resp := ToQueueItemResponse(item)
			assert.Equal(t, filepath.Base(tt.nzbPath), resp.NzbDisplayName)
		})
	}
}

func TestQueueItemDisplayNameLeavesStremioTitleHex(t *testing.T) {
	candidate := stremioPlayCandidate{
		SafeTitle:   "Cult.2026.2160p.WEB-DL.H.265-JeRi",
		DownloadURL: "https://indexer.example/2/download",
		Indexer:     "Indexer",
		Source:      "prowlarr",
	}

	t.Run("no importer suffix", func(t *testing.T) {
		item := stremioQueueItemForDisplay(t, candidate)
		item.NzbPath = "/tmp/altmount-uploads/Movie.2024.nzb"
		resp := ToQueueItemResponse(item)
		assert.Equal(t, "Movie.2024.nzb", resp.NzbDisplayName)
	})

	t.Run("title ends in short hex", func(t *testing.T) {
		item := stremioQueueItemForDisplay(t, candidate)
		item.NzbPath = "/tmp/altmount-uploads/Movie.2024-abcdef12.nzb"
		resp := ToQueueItemResponse(item)
		assert.Equal(t, "Movie.2024-abcdef12.nzb", resp.NzbDisplayName)
	})

	t.Run("title ends in 16 hex but does not match the release key", func(t *testing.T) {
		item := stremioQueueItemForDisplay(t, candidate)
		item.NzbPath = "/tmp/altmount-uploads/Movie.2024-0123456789abcdef.nzb"
		resp := ToQueueItemResponse(item)
		assert.Equal(t, "Movie.2024-0123456789abcdef.nzb", resp.NzbDisplayName,
			"provenance alone must not trim a title that merely ends in hex")
	})
}

func TestImportHistoryDisplayNameHidesStremioImporterSuffix(t *testing.T) {
	candidate := stremioPlayCandidate{
		SafeTitle:   "Cult.2026.2160p.AMZN.WEB-DL.MULTi.DDP5.1.H.265-JeRi",
		DownloadURL: "https://indexer.example/3/download",
		Indexer:     "Indexer",
		Source:      "prowlarr",
	}
	queueItem := stremioQueueItemForDisplay(t, candidate)
	history := &database.ImportHistory{
		DownloadID: queueItem.DownloadID,
		NzbName:    stremioPlayNzbFilename(candidate),
		Metadata:   queueItem.Metadata,
	}

	resp := ToImportHistoryResponse(history)
	assert.Equal(t, candidate.SafeTitle+".nzb", resp.NzbDisplayName)
	assert.Equal(t, history.NzbName, resp.NzbName, "nzb_name must keep the hash")
}

func TestImportHistoryDisplayNameLeavesOtherRows(t *testing.T) {
	tests := []struct {
		name    string
		history *database.ImportHistory
		want    string
	}{
		{
			name:    "radarr release ending in hex",
			history: &database.ImportHistory{DownloadID: strPtr("radarr-release-guid"), NzbName: "Movie.2024-0123456789abcdef.nzb"},
			want:    "Movie.2024-0123456789abcdef.nzb",
		},
		{
			name:    "manual upload ending in hex",
			history: &database.ImportHistory{NzbName: "Upload-0123456789abcdef.nzb"},
			want:    "Upload-0123456789abcdef.nzb",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ToImportHistoryResponse(tt.history).NzbDisplayName)
		})
	}

	candidate := stremioPlayCandidate{
		SafeTitle:   "Cult.2026.2160p.WEB-DL.H.265-JeRi",
		DownloadURL: "https://indexer.example/4/download",
		Indexer:     "Indexer",
		Source:      "prowlarr",
	}
	queueItem := stremioQueueItemForDisplay(t, candidate)
	stremio := &database.ImportHistory{
		DownloadID: queueItem.DownloadID,
		NzbName:    candidate.SafeTitle + ".nzb",
		Metadata:   queueItem.Metadata,
	}
	assert.Equal(t, candidate.SafeTitle+".nzb", ToImportHistoryResponse(stremio).NzbDisplayName,
		"a Stremio row without an importer suffix is unchanged")
}
