package importer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
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

// TestDedupedAdmissionCanRegenerateNzbForRetry covers B1: the de-duplicated row
// drops its raw NZB but must leave a .nzbz store at the path its own retry reads,
// otherwise POST /queue/{id}/retry fails forever.
func TestDedupedAdmissionCanRegenerateNzbForRetry(t *testing.T) {
	s, configDir := newDedupTestService(t)
	s.metadataService = metadata.NewMetadataService(filepath.Join(configDir, "metadata"))
	ctx := context.Background()

	firstPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"RetryStore.nzb",
		nzbWithSegmentBytes("RetryStore.mkv", 100, "seg-a@example", "seg-b@example"))
	first, err := s.AddToQueue(ctx, firstPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(first.NzbPath) })

	// The raw NZB is dropped after a successful import, so the store is what a
	// retry regenerates from.
	firstStore := nzbStoreRef(configDir, first.Category, first.ID, first.NzbPath)
	require.NotEmpty(t, firstStore)
	require.NoError(t, s.metadataService.Store().WriteStore(firstStore, sampleNzbStoreForTest()))

	storage := filepath.Join(configDir, "complete", "RetryStore")
	completeWithStorage(t, s, first, storage)

	secondPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"RetryStore.1.nzb",
		nzbWithSegmentBytes("obfuscated.bin", 100, "seg-a@example", "seg-b@example"))
	second, err := s.AddToQueue(ctx, secondPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(second.NzbPath) })

	require.Equal(t, database.QueueStatusCompleted, second.Status)
	require.NotNil(t, second.StoragePath)

	_, err = os.Stat(second.NzbPath)
	require.True(t, os.IsNotExist(err), "de-duplicated admission should drop the raw NZB")

	secondStore := nzbStoreRef(configDir, second.Category, second.ID, second.NzbPath)
	require.NotEmpty(t, secondStore)
	require.NotEqual(t, firstStore, secondStore)
	_, err = os.Stat(secondStore)
	require.NoError(t, err, "store must be copied to the de-duplicated row's own retry path")

	// Retry path: the raw NZB is missing, so ensurePersistentNzb regenerates it
	// from the copied store.
	require.NoError(t, s.ensurePersistentNzb(ctx, second))
	regenerated, err := os.ReadFile(second.NzbPath)
	require.NoError(t, err)
	require.Contains(t, string(regenerated), "Movie.mkv")
}

// TestAddToQueueSameBasenameReuploadKeepsNormalAdmission covers B3: the same
// release uploaded again under the same filename is an explicit reprocess and
// must not be short-circuited to the completed row, even though
// ensurePersistentNzb has rewritten the completed row's nzb_path.
func TestAddToQueueSameBasenameReuploadKeepsNormalAdmission(t *testing.T) {
	s, configDir := newDedupTestService(t)
	ctx := context.Background()

	firstPath := writeNzb(t, filepath.Join(configDir, "uploads-a"),
		"Reupload.nzb",
		nzbWithSegmentBytes("Reupload.mkv", 100, "seg-a@example", "seg-b@example"))
	first, err := s.AddToQueue(ctx, firstPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(first.NzbPath) })
	completeWithStorage(t, s, first, filepath.Join(configDir, "complete", "Reupload"))

	// New temp directory, same basename: a re-upload from the queue UI.
	secondPath := writeNzb(t, filepath.Join(configDir, "uploads-b"),
		"Reupload.nzb",
		nzbWithSegmentBytes("Reupload.mkv", 100, "seg-a@example", "seg-b@example"))
	second, err := s.AddToQueue(ctx, secondPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(second.NzbPath) })

	require.Equal(t, database.QueueStatusPending, second.Status,
		"same-basename re-upload is an explicit reprocess and must import normally")
	require.Nil(t, second.StoragePath)
}

// TestConcurrentIdenticalAdmissionsAreSerialisedAndAdopted covers B2: the second
// admission of an identical payload stays pending while the first processes, and
// after the first completes the worker adopts its storage instead of downloading
// the same articles again.
func TestConcurrentIdenticalAdmissionsAreSerialisedAndAdopted(t *testing.T) {
	s, configDir := newDedupTestService(t)
	s.metadataService = metadata.NewMetadataService(filepath.Join(configDir, "metadata"))
	ctx := context.Background()

	firstPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"RaceOne.nzb",
		nzbWithSegmentBytes("Race.mkv", 100, "seg-race-a@example", "seg-race-b@example"))
	first, err := s.AddToQueue(ctx, firstPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(first.NzbPath) })

	secondPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"RaceTwo.nzb",
		nzbWithSegmentBytes("Race.mkv", 100, "seg-race-a@example", "seg-race-b@example"))
	second, err := s.AddToQueue(ctx, secondPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(second.NzbPath) })
	require.Equal(t, database.QueueStatusPending, second.Status)
	require.Equal(t, *first.ContentIdentity, *second.ContentIdentity)

	claimedFirst, err := s.database.Repository.ClaimNextQueueItem(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimedFirst)
	require.Equal(t, first.ID, claimedFirst.ID)

	claimed, err := s.database.Repository.ClaimNextQueueItem(ctx)
	require.NoError(t, err)
	require.Nil(t, claimed, "identical release must not be claimed while its sibling is processing")

	storage := filepath.Join(configDir, "complete", "Race")
	firstStore := nzbStoreRef(configDir, first.Category, first.ID, first.NzbPath)
	require.NoError(t, s.metadataService.Store().WriteStore(firstStore, sampleNzbStoreForTest()))
	completeWithStorage(t, s, first, storage)

	claimedSecond, err := s.database.Repository.ClaimNextQueueItem(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimedSecond)
	require.Equal(t, second.ID, claimedSecond.ID)
	require.NotNil(t, claimedSecond.ContentIdentity)

	adopted, err := s.AdoptCompletedItem(ctx, claimedSecond)
	require.NoError(t, err)
	require.True(t, adopted)

	stored, err := s.database.Repository.GetQueueItem(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, database.QueueStatusCompleted, stored.Status)
	require.NotNil(t, stored.StoragePath)
	require.Equal(t, storage, *stored.StoragePath)

	// Adoption also leaves a retryable store for the adopted row.
	secondStore := nzbStoreRef(configDir, second.Category, second.ID, second.NzbPath)
	_, err = os.Stat(secondStore)
	require.NoError(t, err)
}

// TestAddToQueueFallbackResetsDuplicateAdmission covers the fallback when an
// admission is inserted as completed but cannot be attached to the completed
// import. The stored row must return to pending with no storage path, otherwise
// it stays completed (with stale or missing storage) and is never claimed.
func TestAddToQueueFallbackResetsDuplicateAdmission(t *testing.T) {
	s, configDir := newDedupTestService(t)
	ctx := context.Background()

	firstPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Fallback.nzb",
		nzbWithSegmentBytes("Fallback.mkv", 100, "seg-fallback-a@example", "seg-fallback-b@example"))
	first, err := s.AddToQueue(ctx, firstPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(first.NzbPath) })
	completeWithStorage(t, s, first, filepath.Join(configDir, "complete", "Fallback"))

	// Fail the status write inside completeAsDuplicate after AddStoragePath has
	// already attached the source storage: the interleave the fallback must undo.
	_, err = s.database.Connection().ExecContext(ctx, `
		CREATE TRIGGER reject_duplicate_completion
		BEFORE UPDATE ON import_queue
		WHEN NEW.status = 'completed' AND NEW.completed_at IS NOT NULL
		BEGIN
			SELECT RAISE(ABORT, 'simulated status update failure');
		END`)
	require.NoError(t, err)

	secondPath := writeNzb(t, filepath.Join(configDir, "uploads"),
		"Fallback.1.nzb",
		nzbWithSegmentBytes("obfuscated.bin", 100, "seg-fallback-a@example", "seg-fallback-b@example"))
	second, err := s.AddToQueue(ctx, secondPath, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err, "admission falls back to a normal import instead of failing")
	t.Cleanup(func() { _ = os.Remove(second.NzbPath) })

	stored, err := s.database.Repository.GetQueueItem(ctx, second.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, database.QueueStatusPending, stored.Status,
		"a failed duplicate completion must reset the stored row to pending")
	require.Nil(t, stored.StoragePath,
		"a failed duplicate completion must not leave the source storage attached")
	require.Nil(t, stored.CompletedAt)

	require.Equal(t, database.QueueStatusPending, second.Status)
	require.Nil(t, second.StoragePath)

	claimed, err := s.database.Repository.ClaimNextQueueItem(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, second.ID, claimed.ID, "the fallback row must be claimable for a normal import")
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
