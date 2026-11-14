package importer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer/filesystem"
)

// SetPeerBundleFastPath installs (or, with nil, removes) the peer-bundle fast
// path on the service and its processor. With none installed an import runs
// no peer-network code and no peer-network query.
func (s *Service) SetPeerBundleFastPath(h PeerBundleFastPath) {
	s.mu.Lock()
	if h == nil {
		s.peerBundle = nil
	} else {
		s.peerBundle = h
	}
	s.mu.Unlock()
	s.processor.SetPeerBundleFastPath(h)
}

func (s *Service) peerBundleHook() PeerBundleFastPath {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerBundle
}

func (s *Service) bundleState() *database.PeernetBundleImportRepository {
	return database.NewPeernetBundleImportRepository(s.database.Connection(), s.database.Dialect())
}

// holdPeerQueueItem keeps a queue item whose files came from a peer bundle
// away from the ARRs until every file passed its full check. The persisted
// peer_hold mark (not the user's skip flag) keeps the item out of the
// SABnzbd-compatible history and shows it in the queue as work in progress; the
// in-memory skip flag stops this run's post-processing from notifying the ARRs.
// The mark survives a restart. It reports whether the item is held.
func (s *Service) holdPeerQueueItem(ctx context.Context, item *database.ImportQueueItem) bool {
	h := s.peerBundleHook()
	if h == nil || !h.HoldQueueItem(ctx, item.ID) {
		return false
	}
	if err := s.bundleState().SetQueueHold(ctx, item.ID, true); err != nil {
		s.log.ErrorContext(ctx, "Could not hold a peer bundle import back from the ARRs", "queue_id", item.ID, "error", err)
	}
	item.SkipArrNotification = true
	return true
}

// AnnounceVerifiedImport lifts the hold on a peer bundle import whose files all
// passed their full check and tells the ARRs, as a normal import would have at
// completion. The peer-bundle state guarantees it is called at most once per
// import. A queue item the user already removed is not announced.
func (s *Service) AnnounceVerifiedImport(ctx context.Context, queueID int64, category, targetPath, resultingPath string) error {
	queued, err := s.database.Repository.GetQueueItem(ctx, queueID)
	if err != nil {
		return fmt.Errorf("read the queue item: %w", err)
	}
	if queued == nil {
		return nil
	}
	if err := s.bundleState().SetQueueHold(ctx, queueID, false); err != nil {
		return fmt.Errorf("lift the ARR hold: %w", err)
	}
	if s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
	if queued.SkipArrNotification {
		return nil // the user's own skip is honoured
	}
	item := &database.ImportQueueItem{ID: queueID}
	if category != "" {
		item.Category = &category
	}
	if targetPath != "" {
		item.TargetPath = &targetPath
	}
	if err := s.postProcessor.NotifyARR(ctx, item, resultingPath); err != nil {
		s.log.DebugContext(ctx, "ARR notification not sent for a verified peer import", "queue_id", queueID, "error", err)
	}
	return nil
}

// FailImport turns the held queue item of a peer import that failed its full
// check into a failed one: the SABnzbd-compatible history then reports it as
// failed, so the ARR that grabbed it blocklists the release and searches again.
// A user's own skip keeps the item out of that history.
func (s *Service) FailImport(ctx context.Context, queueID int64, reason string) error {
	changed, err := s.bundleState().FailHeldQueueItem(ctx, queueID, reason)
	if err != nil {
		return err
	}
	if changed && s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
	return nil
}

// ReleaseHold lifts the hold of a queue item no peer file refers to any more.
func (s *Service) ReleaseHold(ctx context.Context, queueID int64) error {
	if err := s.bundleState().SetQueueHold(ctx, queueID, false); err != nil {
		return err
	}
	if s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
	return nil
}

// RemoveFailedFile removes one file of a failed peer import the way the health
// worker removes a corrupted file when corruption handling is "delete":
// metadata (and with it the content index entry), the library copy and the
// health record.
func (s *Service) RemoveFailedFile(ctx context.Context, virtualPath string) error {
	var physicalPath, rootPath string
	if s.healthRepo != nil {
		fh, err := s.healthRepo.GetFileHealth(ctx, virtualPath)
		if err != nil {
			s.log.WarnContext(ctx, "Could not read the health record of a failed peer file", "file_path", virtualPath, "error", err)
		}
		if fh != nil && fh.LibraryPath != nil {
			physicalPath = *fh.LibraryPath
			if cfg := s.configGetter(); cfg != nil {
				rootPath = cfg.MountPath
				if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
					rootPath = *cfg.Health.LibraryDir
				}
			}
		}
	}
	if err := s.metadataService.DeleteCorruptedFile(ctx, virtualPath, physicalPath, rootPath); err != nil {
		return err
	}
	if s.healthRepo != nil {
		if err := s.healthRepo.DeleteHealthRecord(ctx, virtualPath); err != nil {
			return fmt.Errorf("delete the health record: %w", err)
		}
	}
	return nil
}

// ErrPeerImportNoNZB is returned for actions that need the NZB of a queue item
// whose release came from a peer bundle.
var ErrPeerImportNoNZB = errors.New("this release was imported from a peer bundle and has no NZB; remove the item and add the release again")

// PeerImportRecord describes a finished direct import (one with no NZB).
type PeerImportRecord struct {
	Title         string
	Category      string
	TargetPath    string
	ResultingPath string
	WrittenPaths  []string
	TotalSize     int64
	ReleaseID     string
	SkipArr       bool
	// Metadata is the queue metadata JSON of the import (media ids, as a
	// Stremio-originated import stores them) and DownloadID its download id;
	// both may be empty.
	Metadata   string
	DownloadID string
}

// RecordPeerImport gives a direct peer-bundle import the queue and history
// entries of a normal import and runs the normal success handling (VFS
// notification, symlinks, health scheduling, held ARR notification). It
// returns the queue id.
func (s *Service) RecordPeerImport(ctx context.Context, rec PeerImportRecord) (int64, error) {
	name := sanitizeFilename(rec.Title)
	if name == "" {
		name = "release"
	}
	rid := rec.ReleaseID
	if len(rid) > 16 {
		rid = rid[:16]
	}
	// The path marks the row as a peer import (database.PeerImportNzbPrefix):
	// no file is behind it. The row starts out completed, so a restart never
	// takes it for interrupted work and retries it, and it is held at once.
	item := &database.ImportQueueItem{
		NzbPath:             database.PeerImportNzbPrefix + rid + "/" + name + ".nzb",
		Priority:            database.QueuePriorityNormal,
		Status:              database.QueueStatusCompleted,
		MaxRetries:          1,
		FileSize:            &rec.TotalSize,
		SkipArrNotification: rec.SkipArr,
	}
	if rec.Category != "" {
		item.Category = &rec.Category
	}
	if rec.TargetPath != "" {
		item.TargetPath = &rec.TargetPath
	}
	var metadata, downloadID *string
	if rec.Metadata != "" {
		metadata = &rec.Metadata
		item.Metadata = metadata
	}
	if rec.DownloadID != "" {
		downloadID = &rec.DownloadID
		item.DownloadID = downloadID
	}
	if err := s.database.Repository.AddToQueue(ctx, item); err != nil {
		return 0, fmt.Errorf("add peer import to the queue: %w", err)
	}
	nzbID := item.ID
	// Roll the queue row back with the import when a later step fails: a
	// completed row with no files would otherwise stay behind.
	undo := func() {
		bg := context.WithoutCancel(ctx)
		_ = s.database.Repository.RemoveFromQueue(bg, nzbID)
		_ = s.database.Repository.DeleteImportHistoryByNzbID(bg, nzbID)
	}
	if err := s.database.Repository.AddImportHistory(ctx, &database.ImportHistory{
		NzbID: &nzbID, NzbName: name + ".nzb", FileName: filepath.Base(rec.ResultingPath), FileSize: rec.TotalSize,
		VirtualPath: rec.ResultingPath, Category: item.Category, CompletedAt: time.Now(),
		Metadata: metadata, DownloadID: downloadID,
	}); err != nil {
		s.log.ErrorContext(ctx, "Failed to add import history for a peer import", "error", err)
	}
	if h := s.peerBundleHook(); h != nil {
		if err := h.AttachQueueItem(ctx, rec.WrittenPaths, item.ID); err != nil {
			undo()
			return 0, fmt.Errorf("link peer import to its queue item: %w", err)
		}
		if h.HoldQueueItem(ctx, item.ID) {
			if err := s.bundleState().SetQueueHold(ctx, item.ID, true); err != nil {
				undo()
				return 0, fmt.Errorf("hold the peer import: %w", err)
			}
		}
	}
	if err := s.handleProcessingSuccess(ctx, item, strings.TrimSuffix(rec.ResultingPath, "/"), rec.WrittenPaths); err != nil {
		undo()
		return 0, err
	}
	return item.ID, nil
}

// ProbePeerRelease runs the import's release probe over a peer bundle's
// segments (see Processor.ProbePeerRelease).
func (s *Service) ProbePeerRelease(ctx context.Context, files []PeerProbeList) error {
	return s.processor.ProbePeerRelease(ctx, files)
}

// PeerImportDir is the virtual folder a direct peer import of the given
// category and title is written to: the one a queued NZB of that category and
// name would land in (the configured complete directory, the category's own
// folder, then the release folder), so a peer import and a normal import of
// the same release end up side by side. It returns "" for a title that cannot
// be turned into a plain folder name (see filesystem.SafeFolderName).
func (s *Service) PeerImportDir(category, title string) string {
	base := ""
	var reserved []string
	if cfg := s.configGetter(); cfg != nil {
		base = cfg.SABnzbd.CompleteDir
		for _, c := range cfg.SABnzbd.Categories {
			reserved = append(reserved, c.Name, filepath.Base(strings.TrimRight(c.Dir, "/")))
		}
	}
	name, ok := filesystem.SafeFolderName(title, reserved...)
	if !ok {
		return ""
	}
	item := &database.ImportQueueItem{NzbPath: filepath.Join(os.TempDir(), ".altmount-queue", "peer.nzb")}
	if category != "" {
		item.Category = &category
	}
	dir := s.calculateProcessVirtualDir(item, &base)
	return strings.ReplaceAll(filepath.Join(dir, name), string(filepath.Separator), "/")
}
