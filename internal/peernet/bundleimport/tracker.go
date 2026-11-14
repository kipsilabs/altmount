package bundleimport

import (
	"context"
	"log/slog"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
)

// Tracker methods: the state machine of an accepted import. They are the
// read and write side of "from the peer network, full check pending".

// PendingFullCheck returns which of paths are peer-bundle files whose full
// check has not passed. The health worker forces a full check for them and
// never gives them peer advice. It fails closed: on a database error every
// path is reported pending.
func (i *Importer) PendingFullCheck(ctx context.Context, paths []string) map[string]struct{} {
	rows, err := i.cfg.State.PendingAmong(ctx, paths)
	if err != nil {
		slog.WarnContext(ctx, "peer bundle state unreadable; treating files as pending", "error", err)
		all := make(map[string]struct{}, len(paths))
		for _, p := range paths {
			all[p] = struct{}{}
		}
		return all
	}
	out := make(map[string]struct{}, len(rows))
	for p := range rows {
		out[p] = struct{}{}
	}
	return out
}

// IsPendingPeerImport reports whether the file is a peer import with its full
// check pending. It fails open: a state error says false.
func (i *Importer) IsPendingPeerImport(ctx context.Context, filePath string) bool {
	if i.cfg.State == nil {
		return false
	}
	row, ok, err := i.cfg.State.Get(ctx, filePath)
	return err == nil && ok && row.State == "pending"
}

// PeerBundleStatus is the read method the publisher uses: fromPeer says the
// file was imported from a peer bundle, fullCheckPassed that every check it
// owed passed. A file may be published when !fromPeer || fullCheckPassed; an
// error must be treated as "not eligible".
func (i *Importer) PeerBundleStatus(ctx context.Context, filePath string) (fromPeer, fullCheckPassed bool, err error) {
	row, ok, err := i.cfg.State.Get(ctx, filePath)
	if err != nil || !ok {
		return false, false, err
	}
	return true, row.State == "passed" && row.Committed, nil
}

// AttachQueue links freshly written bundle files to the queue item that
// carries the import (direct imports learn their queue id only afterwards).
func (i *Importer) AttachQueue(ctx context.Context, paths []string, queueID int64) error {
	return i.cfg.State.SetQueue(ctx, paths, queueID)
}

// QueueHasPending reports whether a queue item still awaits its full checks,
// which is when its ARR announcement must be held back.
func (i *Importer) QueueHasPending(ctx context.Context, queueID int64) bool {
	if queueID <= 0 {
		return false
	}
	has, err := i.cfg.State.QueueHasPending(ctx, queueID)
	if err != nil {
		slog.WarnContext(ctx, "peer bundle state unreadable; holding the ARR announcement", "error", err)
		return true
	}
	return has
}

// FullCheckPassed records that a file's full check passed. When it was the
// last file of its import, the ARRs are told, exactly once.
func (i *Importer) FullCheckPassed(ctx context.Context, filePath string) {
	won, err := i.cfg.State.MarkPassed(ctx, filePath)
	if err != nil || !won {
		if err != nil {
			slog.WarnContext(ctx, "could not record a passed full check", "error", err)
		}
		return
	}
	i.cnt.add(func(c *counters) { c.passed++ })
	row, ok, err := i.cfg.State.Get(ctx, filePath)
	if err != nil || !ok {
		return
	}
	i.announce(ctx, row.QueueID)
	if !row.ArrOwed && !i.QueueHasPending(ctx, row.QueueID) {
		// No ARR is told about this import (the caller suppressed it, or it is a
		// direct import), so nothing else lifts the hold.
		if fh, ok := i.cfg.Announcer.(FailureHandler); ok && row.QueueID > 0 {
			_ = fh.ReleaseHold(ctx, row.QueueID)
		}
	}
	if v, ok := i.cfg.Reporter.(BundleVerifiedReporter); ok && !i.QueueHasPending(ctx, row.QueueID) {
		v.ReportVerifiedBundle(ctx, Origin{NetworkID: row.NetworkID, PublisherID: row.PublisherID}, row.ReleaseID)
	}
}

func (i *Importer) announce(ctx context.Context, queueID int64) {
	if queueID <= 0 || i.cfg.Announcer == nil {
		return
	}
	row, claimed, err := i.cfg.State.ClaimAnnouncement(ctx, queueID)
	if err != nil || !claimed {
		return
	}
	err = i.cfg.Announcer.AnnounceVerifiedImport(ctx, Announcement{
		QueueID: queueID, Category: row.Category, TargetPath: row.TargetPath, ResultingPath: row.ResultingPath,
	})
	if err != nil {
		slog.WarnContext(ctx, "could not announce a verified peer import; will retry", "queue_id", queueID, "error", err)
		_ = i.cfg.State.ReleaseAnnouncement(ctx, queueID)
		return
	}
	i.cnt.add(func(c *counters) { c.announce++ })
}

// FullCheckFailed records that a file's full check failed: the whole import is
// failed (see failImport), it is never announced or published, and the origin publisher is reported once per
// release as having served a bad bundle.
func (i *Importer) FullCheckFailed(ctx context.Context, filePath string) {
	row, ok, err := i.cfg.State.Get(ctx, filePath)
	if err != nil || !ok {
		return
	}
	won, err := i.cfg.State.MarkFailed(ctx, filePath)
	if err != nil || !won {
		return
	}
	i.cnt.add(func(c *counters) { c.failed++ })
	i.failImport(ctx, row, failReasonCheck)
	if i.cfg.Reporter == nil {
		return
	}
	i.reportedMu.Lock()
	first := i.reported == nil || !i.reported[row.ReleaseID+"/"+row.PublisherID]
	if i.reported == nil || len(i.reported) >= maxReported {
		// Bounded: forgetting old entries only means a publisher may be
		// reported once more for a release, which is harmless.
		i.reported = map[string]bool{}
	}
	i.reported[row.ReleaseID+"/"+row.PublisherID] = true
	i.reportedMu.Unlock()
	if !first {
		return
	}
	i.cfg.Reporter.ReportBadBundle(ctx, Origin{NetworkID: row.NetworkID, PublisherID: row.PublisherID}, row.ReleaseID)
	i.cnt.add(func(c *counters) { c.reports++ })
}

// Messages a failed peer import carries in the SABnzbd-compatible history.
const (
	failReasonCheck = "Peer-supplied release failed its full health check and was removed"
	failReasonStale = "Peer-supplied release was not fully verified within 7 days and was removed"
)

// failImport ends a peer import that failed its full check, or never got one:
// the queue item becomes a failed one (the ARR that grabbed it sees it in the
// history, blocklists the release and searches again; a direct import has no
// ARR), then every file of the import is removed (metadata, content index,
// library copy, health record) and its state rows deleted, so a corrupt file
// does not stay in the library and the surviving files of the release cannot
// complete the import by passing later. It touches only the rows of this
// import, and every step is idempotent.
func (i *Importer) failImport(ctx context.Context, row database.PeernetBundleImport, reason string) {
	bg := context.WithoutCancel(ctx)
	fh, _ := i.cfg.Announcer.(FailureHandler)
	rows := []database.PeernetBundleImport{row}
	if row.QueueID > 0 {
		if l, err := i.cfg.State.ListByQueue(bg, row.QueueID); err == nil && len(l) > 0 {
			rows = l
		}
		if fh != nil {
			if err := fh.FailImport(bg, row.QueueID, reason); err != nil {
				slog.WarnContext(ctx, "could not mark a failed peer import as failed in the queue", "queue_id", row.QueueID, "error", err)
			}
		}
	}
	paths := make([]string, 0, len(rows))
	for _, r := range rows {
		paths = append(paths, r.FilePath)
		var err error
		if fh != nil {
			err = fh.RemoveFailedFile(bg, r.FilePath)
		} else {
			err = i.cfg.Library.DeleteFileMetadata(bg, r.FilePath)
		}
		if err != nil {
			slog.WarnContext(ctx, "could not remove a file of a failed peer import", "file_path", r.FilePath, "error", err)
		}
		if i.cfg.Index != nil {
			_ = i.cfg.Index.Delete(bg, r.FilePath)
		}
	}
	if err := i.cfg.State.Delete(bg, paths); err != nil {
		slog.WarnContext(ctx, "could not drop the state of a failed peer import", "error", err)
	}
}

// releaseOrphanHolds lifts the hold of every queue item that no state row
// refers to any more. A hold nothing can release would keep the item in the
// ARR-visible queue forever.
func (i *Importer) releaseOrphanHolds(ctx context.Context) {
	fh, ok := i.cfg.Announcer.(FailureHandler)
	if !ok || i.cfg.State == nil {
		return
	}
	ids, err := i.cfg.State.OrphanedHolds(ctx)
	if err != nil {
		slog.WarnContext(ctx, "could not list orphaned peer holds", "error", err)
		return
	}
	for _, id := range ids {
		if err := fh.ReleaseHold(ctx, id); err != nil {
			slog.WarnContext(ctx, "could not release an orphaned peer hold", "queue_id", id, "error", err)
		}
	}
}

// maxReported bounds the set of (release, publisher) pairs already reported.
const maxReported = 4096

// MaxPendingAge is how long a peer import may owe its full check. After it the
// import is failed (never announced, never published) instead of waiting
// forever, for example because health checking was turned off.
const MaxPendingAge = 7 * 24 * time.Hour

// PendingStatus returns how many files owe their full check and how many of
// those are older than MaxPendingAge.
func (i *Importer) PendingStatus(ctx context.Context) (pending, stale int) {
	if i.cfg.State == nil {
		return 0, 0
	}
	p, s, err := i.cfg.State.PendingCounts(ctx, i.cfg.Now().Add(-MaxPendingAge))
	if err != nil {
		return 0, 0
	}
	return p, s
}

// EscalateStale fails the pending files older than MaxPendingAge and returns how
// many. The hold on the ARRs is never lifted for them and they are never
// published; the publisher is not reported, since no check ever failed.
func (i *Importer) EscalateStale(ctx context.Context) int {
	if i.cfg.State == nil {
		return 0
	}
	rows, err := i.cfg.State.StalePending(ctx, i.cfg.Now().Add(-MaxPendingAge), 500)
	if err != nil {
		slog.WarnContext(ctx, "could not list stale peer imports", "error", err)
		return 0
	}
	n := 0
	for _, r := range rows {
		if won, err := i.cfg.State.MarkFailed(ctx, r.FilePath); err == nil && won {
			n++
			i.failImport(ctx, r, failReasonStale)
		}
	}
	if n > 0 {
		i.cnt.add(func(c *counters) { c.failed += uint64(n) })
		slog.WarnContext(ctx, "peer imports never got their full check and were failed", "files", n)
	}
	return n
}

// RunHousekeeping escalates stale imports once at start and then hourly until
// ctx ends.
func (i *Importer) RunHousekeeping(ctx context.Context) {
	// A background task must never take the process down.
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "peer import housekeeping stopped", "panic", r)
		}
	}()
	i.Reconcile(ctx)
	i.EscalateStale(ctx)
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			i.EscalateStale(ctx)
			i.releaseOrphanHolds(ctx)
		}
	}
}

// ResumeAnnouncements delivers announcements whose files all passed but that a
// restart interrupted. Call it once at start-up.
func (i *Importer) ResumeAnnouncements(ctx context.Context) {
	if i.cfg.State == nil || i.cfg.Announcer == nil {
		return
	}
	ids, err := i.cfg.State.QueuesAwaitingAnnouncement(ctx)
	if err != nil {
		slog.WarnContext(ctx, "could not list pending peer announcements", "error", err)
		return
	}
	for _, id := range ids {
		i.announce(ctx, id)
	}
}
