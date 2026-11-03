package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/testsupport/nzbbuild"
	"github.com/stretchr/testify/require"
)

func TestNestedNzbQueueExpansion(t *testing.T) {
	e := newBatteryEnv(t)
	s := newMinimalServiceForPersistTest(t)
	s.processor, s.configGetter = e.proc, e.proc.configGetter
	ctx := context.Background()
	entries := map[string][]byte{}
	for _, name := range []string{"One", "Two"} {
		data, err := nzbparser.Write(nzbbuild.Build(nzbbuild.File{Subject: name + ".mkv", Segments: e.registerContent(name, bytes.Repeat([]byte("M"), 1000), 1000, 1, nil)}))
		require.NoError(t, err)
		entries["../../"+name+".nzb"] = data
	}
	payload := nestedZip(t, entries)
	outer := nzbbuild.Build(nzbbuild.File{Subject: "wrapper.zip", Segments: e.registerContent("wrapper", payload, 100, 1.1, &nntppool.YEncMeta{FileName: "wrapper.zip", FileSize: int64(len(payload))})})
	category, downloadID, indexer, target, relative := "movies", "parent-download", "test-indexer", "/library", "incoming"
	metadata := `{"extracted_files":[{"name":"stale"}],"custom":true}`
	parent := &database.ImportQueueItem{NzbPath: nzbbuild.WriteTemp(t, outer, "Parent"), Category: &category, DownloadID: &downloadID, Priority: database.QueuePriorityHigh, MaxRetries: 5, Status: database.QueueStatusProcessing, Indexer: &indexer, TargetPath: &target, RelativePath: &relative, Metadata: &metadata, SkipArrNotification: true, SkipPostImportLinks: true}
	require.NoError(t, s.database.Repository.AddToQueue(ctx, parent))
	t.Cleanup(func() { os.Remove(parent.NzbPath) })
	t.Cleanup(func() {
		if parent.Metadata != &metadata {
			os.RemoveAll(filepath.Dir(filepath.Dir(parent.NzbPath)))
		}
	})
	path, _, err := s.processNzbItem(ctx, parent)
	require.NoError(t, err)
	require.NotEmpty(t, path)
	stored, err := s.database.Repository.GetQueueItem(ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, database.QueueStatusProcessing, stored.Status)
	require.Equal(t, &downloadID, stored.DownloadID)
	children, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(ctx, filepath.Dir(filepath.Dir(parent.NzbPath)))
	require.NoError(t, err)
	require.Len(t, children, 1)
	seenIDs := map[string]bool{downloadID: true}
	for _, summary := range children {
		child, err := s.database.Repository.GetQueueItem(ctx, summary.ID)
		require.NoError(t, err)
		t.Cleanup(func() { os.RemoveAll(filepath.Dir(filepath.Dir(child.NzbPath))) })
		require.Equal(t, parent.Category, child.Category)
		require.Equal(t, parent.Priority, child.Priority)
		require.Equal(t, parent.MaxRetries, child.MaxRetries)
		require.Equal(t, parent.Indexer, child.Indexer)
		require.Equal(t, parent.TargetPath, child.TargetPath)
		require.Equal(t, parent.RelativePath, child.RelativePath)
		require.True(t, child.SkipArrNotification)
		require.True(t, child.SkipPostImportLinks)
		require.NotNil(t, child.DownloadID)
		require.False(t, seenIDs[*child.DownloadID])
		seenIDs[*child.DownloadID] = true
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(*child.Metadata), &fields))
		require.NotContains(t, fields, "extracted_files")
		require.JSONEq(t, "true", string(fields["custom"]))
		chain, err := readNestedNzbChain(child.Metadata)
		require.NoError(t, err)
		require.Equal(t, 1, chain.Depth)
		require.Len(t, chain.Seen, 1)
		_, paths, err := s.processNzbItem(ctx, child)
		require.NoError(t, err)
		require.Len(t, filePaths(paths), 1)
	}
	// Retrying the original job imports its first child without another expansion.
	require.NoError(t, s.database.Repository.UpdateQueueItemStatus(ctx, parent.ID, database.QueueStatusProcessing, nil))
	parent, err = s.database.Repository.GetQueueItem(ctx, parent.ID)
	require.NoError(t, err)
	_, _, err = s.processNzbItem(ctx, parent)
	require.NoError(t, err)
	again, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(ctx, filepath.Dir(filepath.Dir(parent.NzbPath)))
	require.NoError(t, err)
	require.Len(t, again, 1)
}

func TestNestedNzbQueueCanceledExpansionCleansStaging(t *testing.T) {
	s := newMinimalServiceForPersistTest(t)
	ctx := context.Background()
	dir := t.TempDir()
	parent := &database.ImportQueueItem{NzbPath: filepath.Join(dir, "parent.nzb"), Status: database.QueueStatusProcessing, Priority: database.QueuePriorityNormal, MaxRetries: 3}
	require.NoError(t, s.database.Repository.AddToQueue(ctx, parent))
	data, err := nzbparser.Write(nzbbuild.Build(nzbbuild.File{Subject: "Movie.mkv", Segments: []nzbbuild.Segment{{ID: "media", Bytes: 1000}}}))
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	err = s.enqueueNestedNzbs(canceled, parent, &nestedNzbJobsError{Jobs: []nestedNzbJob{{Name: "../../release.nzb", Data: data}, {Name: "other.nzb", Data: data}}})
	require.ErrorIs(t, err, context.Canceled)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
	stored, err := s.database.Repository.GetQueueItem(ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, database.QueueStatusProcessing, stored.Status)
}
