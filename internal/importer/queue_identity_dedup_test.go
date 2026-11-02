package importer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

// nzbWithSegmentBytes builds a minimal NZB with one payload file. name and the
// segment IDs are varied by callers to prove the admission identity ignores
// file names while following the message-ID set.
func nzbWithSegmentBytes(name string, size int, segmentIDs ...string) string {
	segments := ""
	for i, id := range segmentIDs {
		segments += fmt.Sprintf(`<segment bytes="%d" number="%d">%s</segment>`, size, i+1, id)
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="x@x" date="1700000000" subject="%s (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>%s</segments>
  </file>
</nzb>
`, name, segments)
}

// writeNzb writes content into a unique temp directory and returns the path.
func writeNzb(t *testing.T, dir, name, content string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

// completeWithStorage marks an admitted row completed against a storage path,
// mirroring what handleProcessingSuccess does after an import.
func completeWithStorage(t *testing.T, s *Service, item *database.ImportQueueItem, storagePath string) {
	t.Helper()
	require.NoError(t, s.database.Repository.AddStoragePath(context.Background(), item.ID, storagePath))
	require.NoError(t, s.database.Repository.UpdateQueueItemStatus(context.Background(), item.ID, database.QueueStatusCompleted, nil))
}

func TestAddToQueueDeduplicatesSameReleaseUnderDifferentNzbNames(t *testing.T) {
	s, configDir := newDedupTestService(t)
	ctx := context.Background()

	firstPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Kraken.2026.1080p.BluRay.x264-NoNE.nzb",
		nzbWithSegmentBytes("Kraken.2026.1080p.BluRay.x264-NoNE.mkv", 100, "seg-a@example", "seg-b@example"))
	first, err := s.AddToQueue(ctx, firstPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, database.QueueStatusPending, first.Status)
	require.NotNil(t, first.ContentIdentity)
	t.Cleanup(func() { _ = os.Remove(first.NzbPath) })

	storage := filepath.Join(configDir, "complete", "Movies", "Kraken.2026")
	completeWithStorage(t, s, first, storage)

	// Different NZB name and different payload filename, same segment bytes and
	// ordered message-IDs: this is the same release.
	secondPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Kraken.2026.1080p.BluRay.x264-NoNE.1.nzb",
		nzbWithSegmentBytes("obfuscated-payload.bin", 100, "seg-a@example", "seg-b@example"))
	second, err := s.AddToQueue(ctx, secondPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	require.NotEqual(t, first.ID, second.ID)
	require.Equal(t, database.QueueStatusCompleted, second.Status)
	require.NotNil(t, second.StoragePath)
	require.Equal(t, storage, *second.StoragePath)
	require.NotNil(t, second.CompletedAt)

	// The de-duplicated row carries the identity too, so a third NZB of the
	// same release matches this row as well.
	storedIdentity, err := s.database.Repository.GetContentIdentity(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, *first.ContentIdentity, storedIdentity)

	// The de-duplicated row must not enter the pending queue.
	var pending int
	require.NoError(t, s.database.Connection().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM import_queue WHERE status = 'pending'`).Scan(&pending))
	require.Zero(t, pending)
}

func TestAddToQueueDoesNotMergeDifferentSizes(t *testing.T) {
	s, configDir := newDedupTestService(t)
	ctx := context.Background()

	firstPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Release.nzb",
		nzbWithSegmentBytes("Release.mkv", 100, "seg-a@example", "seg-b@example"))
	first, err := s.AddToQueue(ctx, firstPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(first.NzbPath) })
	completeWithStorage(t, s, first, filepath.Join(configDir, "complete", "Release"))

	// Same message-IDs, one segment is larger: a different payload size must
	// never merge into the completed import.
	secondPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Release.1.nzb",
		nzbWithSegmentBytes("Release.mkv", 101, "seg-a@example", "seg-b@example"))
	second, err := s.AddToQueue(ctx, secondPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(second.NzbPath) })

	require.Equal(t, database.QueueStatusPending, second.Status)
	require.Nil(t, second.StoragePath)
}

func TestAddToQueueDoesNotMergeRepackedMessageIDs(t *testing.T) {
	s, configDir := newDedupTestService(t)
	ctx := context.Background()

	firstPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Release.nzb",
		nzbWithSegmentBytes("Release.mkv", 100, "seg-a@example", "seg-b@example"))
	first, err := s.AddToQueue(ctx, firstPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(first.NzbPath) })
	completeWithStorage(t, s, first, filepath.Join(configDir, "complete", "Release"))

	// Same name and size, new articles: a re-upload with new message-IDs is a
	// distinct article set (out of scope for de-duplication), not this release.
	secondPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Release.1.nzb",
		nzbWithSegmentBytes("Release.mkv", 100, "seg-c@example", "seg-d@example"))
	second, err := s.AddToQueue(ctx, secondPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(second.NzbPath) })

	require.Equal(t, database.QueueStatusPending, second.Status)
	require.Nil(t, second.StoragePath)
}

func TestAddToQueueRequeueOfSameNzbPathStaysPending(t *testing.T) {
	s, configDir := newDedupTestService(t)
	ctx := context.Background()

	path := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Requeue.nzb",
		nzbWithSegmentBytes("Requeue.mkv", 100, "seg-a@example", "seg-b@example"))

	// Seed a completed row for this exact path, as a prior import would leave it.
	_, identity, err := s.analyzeNzbForQueue(path)
	require.NoError(t, err)
	require.NotEmpty(t, identity)
	size := int64(200)
	seeded := &database.ImportQueueItem{
		NzbPath:         path,
		Status:          database.QueueStatusCompleted,
		Priority:        database.QueuePriorityNormal,
		RetryCount:      0,
		MaxRetries:      3,
		FileSize:        &size,
		ContentIdentity: &identity,
	}
	require.NoError(t, s.database.Repository.AddToQueue(ctx, seeded))
	require.NoError(t, s.database.Repository.SetContentIdentity(ctx, seeded.ID, identity))
	completeWithStorage(t, s, seeded, filepath.Join(configDir, "complete", "Requeue"))
	t.Cleanup(func() { _ = os.Remove(path) })

	requeued, err := s.AddToQueue(ctx, path, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(requeued.NzbPath) })

	require.Equal(t, seeded.ID, requeued.ID)
	require.Equal(t, database.QueueStatusPending, requeued.Status,
		"same nzb_path is an explicit retry and must not be short-circuited to completed")
}

// TestRecordContentIdentityAtCompletion covers rows admitted through a path
// that does not compute an identity (e.g. a directory scan): completion records
// it so the next duplicate can be admitted against this import.
func TestRecordContentIdentityAtCompletion(t *testing.T) {
	s, configDir := newDedupTestService(t)
	ctx := context.Background()

	path := writeNzb(t, filepath.Join(configDir, "scan"),
		"Scanned.nzb",
		nzbWithSegmentBytes("Scanned.mkv", 100, "seg-a@example", "seg-b@example"))
	item := &database.ImportQueueItem{
		NzbPath:    path,
		Status:     database.QueueStatusPending,
		Priority:   database.QueuePriorityNormal,
		MaxRetries: 3,
	}
	require.NoError(t, s.database.Repository.AddToQueue(ctx, item))

	before, err := s.database.Repository.GetContentIdentity(ctx, item.ID)
	require.NoError(t, err)
	require.Empty(t, before)

	s.recordContentIdentity(ctx, item)

	after, err := s.database.Repository.GetContentIdentity(ctx, item.ID)
	require.NoError(t, err)
	require.NotEmpty(t, after)
}

// TestNzbContentIdentityIgnoresFileOrder pins the fingerprint to the ordered
// message-IDs and size, not to the NZB file order or payload filename.
func TestNzbContentIdentityIgnoresFileOrder(t *testing.T) {
	first := nzbWithSegmentBytes("one.mkv", 100, "seg-a@example", "seg-b@example")
	second := `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="y@y" date="1700000000" subject="two.bin (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="100" number="2">seg-b@example</segment>
      <segment bytes="100" number="1">seg-a@example</segment>
    </segments>
  </file>
</nzb>
`
	dir := t.TempDir()
	firstPath := writeNzb(t, dir, "one.nzb", first)
	secondPath := writeNzb(t, dir, "two.nzb", second)

	firstSize, firstIdentity, err := (&Service{}).analyzeNzbForQueue(firstPath)
	require.NoError(t, err)
	secondSize, secondIdentity, err := (&Service{}).analyzeNzbForQueue(secondPath)
	require.NoError(t, err)

	require.Equal(t, firstSize, secondSize)
	require.Equal(t, firstIdentity, secondIdentity)
}
