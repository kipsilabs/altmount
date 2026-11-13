package metadata

import (
	"context"
	"sync"
	"testing"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingObserver struct {
	mu     sync.Mutex
	events []string
}

func (o *recordingObserver) add(e string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *recordingObserver) FileWritten(_ context.Context, p string)      { o.add("write:" + p) }
func (o *recordingObserver) FileDeleted(_ context.Context, p string)      { o.add("delete:" + p) }
func (o *recordingObserver) DirectoryDeleted(_ context.Context, p string) { o.add("deldir:" + p) }
func (o *recordingObserver) DirectoryRenamed(_ context.Context, a, b string) {
	o.add("mvdir:" + a + ">" + b)
}

func TestFileObserver_Events(t *testing.T) {
	ctx := context.Background()
	ms := NewMetadataService(t.TempDir())
	obs := &recordingObserver{}
	ms.SetFileObserver(obs)

	meta := func() *metapb.FileMetadata {
		return &metapb.FileMetadata{FileSize: 1, SegmentData: []*metapb.SegmentData{{Id: "a", SegmentSize: 1}}}
	}

	require.NoError(t, ms.WriteFileMetadataAuto(ctx, "/rel/a.mkv", meta(), nil, ""))
	require.NoError(t, ms.WriteFileMetadataAuto(ctx, "rel/b.mkv", meta(), nil, ""))
	require.NoError(t, ms.RenameFileMetadata("rel/b.mkv", "rel2/b.mkv"))
	require.NoError(t, ms.DeleteFileMetadata(ctx, "rel/a.mkv"))
	require.NoError(t, ms.MoveToCorrupted(ctx, "rel2/b.mkv"))
	require.NoError(t, ms.WriteFileMetadataAuto(ctx, "rel3/c.mkv", meta(), nil, ""))
	require.NoError(t, ms.DeleteDirectory("rel3"))

	assert.Equal(t, []string{
		"write:rel/a.mkv", "write:rel/b.mkv",
		"delete:rel/b.mkv", "write:rel2/b.mkv",
		"delete:rel/a.mkv",
		"delete:rel2/b.mkv",
		"write:rel3/c.mkv",
		"deldir:rel3",
	}, obs.events)
}

func TestFileObserver_NilIsNoop(t *testing.T) {
	ctx := context.Background()
	ms := NewMetadataService(t.TempDir())
	m := &metapb.FileMetadata{FileSize: 1, SegmentData: []*metapb.SegmentData{{Id: "a", SegmentSize: 1}}}
	require.NoError(t, ms.WriteFileMetadataAuto(ctx, "x/a.mkv", m, nil, ""))
	require.NoError(t, ms.DeleteFileMetadata(ctx, "x/a.mkv"))
}

func newMeta() *metapb.FileMetadata {
	return &metapb.FileMetadata{FileSize: 1, SegmentData: []*metapb.SegmentData{{Id: "a", SegmentSize: 1}}}
}

// The plain WriteFileMetadata (used by the health library sync) is observed
// exactly once, and WriteFileMetadataAuto does not notify twice.
func TestFileObserver_PlainWriteIsObserved(t *testing.T) {
	ms := NewMetadataService(t.TempDir())
	obs := &recordingObserver{}
	ms.SetFileObserver(obs)
	require.NoError(t, ms.WriteFileMetadata("\\lib\\a.mkv", newMeta()))
	assert.Equal(t, []string{"write:lib/a.mkv"}, obs.events)
}

func TestFileObserver_DirectoryRename(t *testing.T) {
	ctx := context.Background()
	ms := NewMetadataService(t.TempDir())
	require.NoError(t, ms.WriteFileMetadataAuto(ctx, "old/a.mkv", newMeta(), nil, ""))
	obs := &recordingObserver{}
	ms.SetFileObserver(obs)
	require.NoError(t, ms.RenameDirectory("/old/", "new"))
	assert.Equal(t, []string{"mvdir:old>new"}, obs.events)
	got, err := ms.ReadFileMetadata("new/a.mkv")
	require.NoError(t, err)
	require.NotNil(t, got)
	// nil observer: unchanged behaviour.
	ms.SetFileObserver(nil)
	require.NoError(t, ms.RenameDirectory("new", "newer"))
	assert.Error(t, ms.RenameDirectory("missing", "x"))
}

// UpdateFileStatus and known-holes updates cannot change a file's identity
// (segment IDs and structure), so they are deliberately not observed.
func TestFileObserver_StatusUpdatesAreNotObserved(t *testing.T) {
	ctx := context.Background()
	ms := NewMetadataService(t.TempDir())
	require.NoError(t, ms.WriteFileMetadataAuto(ctx, "a/b.mkv", newMeta(), nil, ""))
	obs := &recordingObserver{}
	ms.SetFileObserver(obs)
	require.NoError(t, ms.UpdateFileStatus("a/b.mkv", metapb.FileStatus_FILE_STATUS_CORRUPTED))
	require.NoError(t, ms.UpdateFileMetadata("a/b.mkv", func(m *metapb.FileMetadata) { m.Status = metapb.FileStatus_FILE_STATUS_HEALTHY }))
	assert.Empty(t, obs.events)
}
