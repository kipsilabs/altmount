package bundleimport

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/kipsilabs/altmount/internal/contentverify"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/importer/filesystem"
	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// cleanTarget normalises the local target folder and refuses anything that is
// not a plain relative folder.
func cleanTarget(dir string) (string, error) {
	dir = strings.Trim(strings.ReplaceAll(dir, `\`, "/"), "/")
	if dir == "" {
		return "", errors.New("empty target folder")
	}
	parts := strings.Split(dir, "/")
	if len(parts) < 2 {
		return "", errors.New("target folder must be below a category folder")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid target folder component %q", part)
		}
		for _, r := range filesystem.ReservedFolderNames {
			if strings.EqualFold(part, r) {
				return "", fmt.Errorf("reserved target folder component %q", part)
			}
		}
		for _, r := range part {
			if unicode.IsControl(r) {
				return "", errors.New("control character in target folder")
			}
		}
	}
	return path.Clean(dir), nil
}

func (i *Importer) rejected(r Reason, err error) (*Result, error) {
	i.cnt.add(func(c *counters) { c.rejected[r]++ })
	return nil, reject(r, err)
}

// Accept is the accept pipeline: re-validate, local policy, release probe,
// media probe through the layout in a hidden staging area, and only then the
// local write in pending state. Any rejection leaves nothing behind.
func (i *Importer) Accept(ctx context.Context, req Request) (*Result, error) {
	if r, blocked := i.declines.blocked(req.Origin.BundleID, i.cfg.Now()); blocked {
		// Declined by this node a moment ago for a reason that repeats: no
		// article is read again for the same answer.
		res, err := i.rejected(r, errors.New("declined by this node a moment ago; not verified again for a while"))
		return res, err
	}
	res, err := i.accept(ctx, req)
	if err != nil && declinesBackOff(ReasonOf(err)) {
		i.declines.note(req.Origin.BundleID, ReasonOf(err), i.cfg.Now())
	}
	if err != nil && localPolicyReason(ReasonOf(err)) && req.Origin.PublisherID != "" {
		if rf, ok := i.cfg.Reporter.(FetchRefunder); ok {
			rf.RefundFetch(ctx, req.Origin)
		}
	}
	return res, err
}

func (i *Importer) accept(ctx context.Context, req Request) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, i.cfg.Policy.AcceptTimeout)
	defer cancel()
	b := req.Bundle
	if b == nil {
		return i.rejected(ReasonInvalid, errors.New("no bundle"))
	}
	if err := bundle.Validate(b); err != nil {
		if errors.Is(err, bundle.ErrBadFilename) || errors.Is(err, bundle.ErrDuplicateName) {
			return i.rejected(ReasonBadName, err)
		}
		return i.rejected(ReasonInvalid, err)
	}
	pol := i.cfg.Policy
	switch {
	case b.TotalSize > pol.MaxTotalSize:
		return i.rejected(ReasonTooLarge, fmt.Errorf("%d bytes", b.TotalSize))
	case len(b.Files) > pol.MaxFiles:
		return i.rejected(ReasonTooManyFiles, fmt.Errorf("%d files", len(b.Files)))
	}
	if i.cfg.HealthEnabled == nil || !i.cfg.HealthEnabled() {
		return i.rejected(ReasonHealthDisabled, errors.New("the full check could never run"))
	}
	dir, err := cleanTarget(req.Target.Dir)
	if err != nil {
		return i.rejected(ReasonBadTarget, err)
	}
	loc, err := bundle.ToLocal(b, i.cfg.Now())
	if err != nil {
		return i.rejected(ReasonInvalid, err)
	}
	if err := verifyIdentity(b, loc); err != nil {
		return i.rejected(ReasonInvalid, err)
	}
	if req.ExpectedNZB != nil && (b.Scheme != bundle.SchemeNZB || !slices.EqualFunc(req.ExpectedNZB, loc.NZBFiles, slices.Equal[[]string])) {
		return i.rejected(ReasonNZBMismatch, errors.New("segment list differs from the supplied NZB"))
	}

	if err := checkSizes(b, req); err != nil {
		return i.rejected(ReasonSizeImplausible, err)
	}
	keep, r, err := i.selectFiles(b)
	if err != nil {
		return i.rejected(r, err)
	}
	b, loc = subset(b, loc, keep)
	if err := checkSizeShape(b); err != nil {
		return i.rejected(ReasonSizeImplausible, err)
	}

	paths := make([]string, len(loc.Files))
	for n, lf := range loc.Files {
		paths[n] = dir + "/" + lf.Name
	}
	existing, err := i.checkTargets(b, paths)
	if err != nil {
		return i.rejected(ReasonCollision, err)
	}
	sizes := make([]int64, len(loc.Files))
	for n, lf := range loc.Files {
		sizes[n] = lf.Meta.FileSize
	}
	res := &Result{Title: b.Title, Dir: dir, Paths: paths, Sizes: sizes, TotalSize: b.TotalSize}
	if existing == len(paths) {
		res.AlreadyImported = true
		i.cnt.add(func(c *counters) { c.already++ })
		return res, nil
	}

	if i.cfg.Prober == nil {
		return i.rejected(ReasonProbeInconclusive, errors.New("no release prober"))
	}
	if err := i.cfg.Prober.ProbeRelease(ctx, probeFiles(loc)); err != nil {
		if errors.Is(err, ErrProbeMissing) {
			return i.rejected(ReasonProbeMissing, err)
		}
		return i.rejected(ReasonProbeInconclusive, err)
	}
	ac := newArticleCache(articleCacheBytes)
	ctx = withProgress(withArticleCache(ctx, ac), req.Progress)
	stage, closeStage := i.stagerWith(ac)
	defer closeStage()
	vclass := VerifyExplicit
	if req.Automatic {
		vclass = VerifyAutomatic
	}
	rep, r, attributable, err := i.verifyYenc(WithVerifyClass(ctx, vclass), b, req.ExpectedNZBNames, stage)
	if err != nil {
		d := i.decide(ctx, req.Origin.BundleID, rejection{Reason: r, Bytes: attributable, Evidence: evidenceOf(err)})
		if d.Attributable && i.cfg.Reporter != nil {
			// The layout contradicts the articles themselves: the publisher
			// served a bad bundle, a mild strike (fetch domain only).
			i.cfg.Reporter.ReportBadBundle(ctx, req.Origin, b.ReleaseID.String())
			i.cnt.add(func(c *counters) { c.reports++ })
		}
		if d.Blacklist {
			i.noteFailedBundle(ctx, req.Origin.BundleID, b.ReleaseID.String())
		}
		return i.rejected(r, err)
	}
	res.Cost = Cost{Articles: rep.articles, Bytes: rep.bytes, ExtentsBound: rep.bound, ExtentsUnbound: rep.checked - rep.bound,
		PartsBound: rep.partsBound, PartsUnbound: rep.partsUnbound}
	// One lease from the first read to the last: the media probe reads through
	// the same staging area and is part of what the node-wide budget meters.
	defer func() { rep.lease.End(rep.bud.spent()) }()
	probeReason, probeErr := i.probeMedia(ctx, b, loc, stage, rep)
	res.Cost.Bytes = rep.bud.spent()
	if probeErr != nil {
		return i.rejected(probeReason, probeErr)
	}

	unlock := i.lockTarget(dir)
	defer unlock()
	// Re-check under the lock: another accept may have written meanwhile.
	existing, err = i.checkTargets(b, paths)
	if err != nil {
		return i.rejected(ReasonCollision, err)
	}
	if existing == len(paths) {
		res.AlreadyImported = true
		i.cnt.add(func(c *counters) { c.already++ })
		return res, nil
	}
	i.writeMu.RLock()
	err = i.write(ctx, b, loc, req, res)
	i.writeMu.RUnlock()
	if err != nil {
		return i.rejected(ReasonWriteFailed, err)
	}
	i.cnt.add(func(c *counters) {
		c.accepted++
		c.partsBound += uint64(res.Cost.PartsBound)
		c.partsUnbound += uint64(res.Cost.PartsUnbound)
		c.extBound += uint64(res.Cost.ExtentsBound)
		c.extUnbound += uint64(res.Cost.ExtentsUnbound)
	})
	return res, nil
}

// noteFailedBundle puts a layout that contradicted the articles or the media on
// the negative cache (when the reporter keeps one).
func (i *Importer) noteFailedBundle(ctx context.Context, bundleID, releaseID string) {
	if bundleID == "" {
		return
	}
	if r, ok := i.cfg.Reporter.(BundleFailedReporter); ok {
		r.ReportFailedBundle(ctx, bundleID, releaseID)
	}
}

// verifyIdentity recomputes every File ID and the Release ID from the layout
// the bundle describes, so this package does not depend on its caller having
// run bundle.Verify.
func verifyIdentity(b *bundle.Bundle, loc *bundle.Local) error {
	ids := make([]contentid.ID, len(loc.Files))
	for n, lf := range loc.Files {
		id, err := contentid.FileID(lf.Meta)
		if err != nil {
			return fmt.Errorf("file %d: %w", n, err)
		}
		if id != b.Files[n].FileID {
			return bundle.ErrFileIDMismatch
		}
		ids[n] = id
	}
	var rid contentid.ID
	if b.Scheme == bundle.SchemeFiles {
		rid = bundle.FilesReleaseID(ids)
	} else {
		store := &metapb.NzbStore{}
		for _, f := range loc.NZBFiles {
			fe := &metapb.NzbFileEntry{}
			for _, id := range f {
				fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id})
			}
			store.Files = append(store.Files, fe)
		}
		var err error
		if rid, err = contentid.ReleaseID(store); err != nil {
			return err
		}
	}
	if rid != b.ReleaseID {
		return bundle.ErrReleaseIDMismatch
	}
	return nil
}

// foldName is the comparison key of a file name: Unicode NFC, then lower case.
func foldName(n string) string { return strings.ToLower(norm.NFC.String(n)) }

// checkTargets returns how many target paths already hold the very same file
// of the very same release. A path holding a different file, a name that only
// differs by case or normalisation form from one of ours, or only some paths
// holding theirs, is a collision.
func (i *Importer) checkTargets(b *bundle.Bundle, paths []string) (int, error) {
	taken := map[string]string{}
	if len(paths) > 0 {
		names, err := i.cfg.Library.ListDirectory(path.Dir(paths[0]))
		if err != nil {
			return 0, fmt.Errorf("list target folder: %w", err)
		}
		for _, n := range names {
			taken[foldName(n)] = n
		}
	}
	same := 0
	for n, p := range paths {
		base := path.Base(p)
		if other, ok := taken[foldName(base)]; ok && other != base {
			return 0, fmt.Errorf("%s: %q is already there under another spelling", p, other)
		}
		if !i.cfg.Library.FileExists(p) {
			continue
		}
		m, err := i.cfg.Library.ReadFileMetadata(p)
		if err != nil || m == nil {
			return 0, fmt.Errorf("%s: unreadable existing file", p)
		}
		id, err := contentid.FileID(m)
		if err != nil || id != b.Files[n].FileID {
			return 0, fmt.Errorf("%s: a different file is already there", p)
		}
		if i.cfg.State != nil {
			row, found, err := i.cfg.State.Get(context.Background(), p)
			if err != nil {
				return 0, fmt.Errorf("%s: %w", p, err)
			}
			if !found || row.ReleaseID != b.ReleaseID.String() {
				return 0, fmt.Errorf("%s: the same file belongs to another release", p)
			}
		}
		same++
	}
	if same != 0 && same != len(paths) {
		return 0, errors.New("only part of the release is already present")
	}
	return same, nil
}

func realIDs(segs []*metapb.SegmentData, out []string) []string {
	for _, s := range segs {
		if s != nil && s.Id != "" && !holes.IsPlaceholderID(s.Id) {
			out = append(out, s.Id)
		}
	}
	return out
}

func probeFiles(loc *bundle.Local) []ProbeFile {
	out := make([]ProbeFile, 0, len(loc.Files))
	for _, lf := range loc.Files {
		ids := realIDs(lf.Meta.SegmentData, nil)
		for _, o := range lf.Meta.SharedOuterSources {
			ids = realIDs(o.Segments, ids)
		}
		out = append(out, ProbeFile{Name: lf.Name, SegmentIDs: ids})
	}
	return out
}

// stageFunc returns the hidden staging area of one accept, creating it on first
// use.
type stageFunc func() (Staging, Reason, error)

// stager returns the lazily created staging area of one accept and the function
// that removes it (a no-op when it was never created).
func (i *Importer) stager() (stageFunc, func()) { return i.stagerWith(nil) }

// stagerWith is stager with the accept's article cache shared by the staged
// reads.
func (i *Importer) stagerWith(ac *articleCache) (stageFunc, func()) {
	var st Staging
	get := func() (Staging, Reason, error) {
		if st != nil {
			return st, "", nil
		}
		if i.cfg.Stager == nil {
			return nil, ReasonStaging, errors.New("no stager")
		}
		s, err := i.cfg.Stager.NewStaging()
		if err != nil {
			return nil, ReasonStaging, err
		}
		if a, ok := s.(articleCacheAware); ok && ac != nil {
			a.SetArticleCache(ac)
		}
		st = s
		return st, "", nil
	}
	return get, func() {
		if st != nil {
			_ = st.Close()
		}
	}
}

// probeMedia reads the head of the main media file(s) through the real reader
// stack, in a hidden staging area that is removed on every outcome. strong
// names the files whose archive extents could not be bound to an archive
// header: their container must have a real structural check.
func (i *Importer) probeMedia(ctx context.Context, b *bundle.Bundle, loc *bundle.Local, stage stageFunc, rep extentReport) (Reason, error) {
	bud := rep.bud
	strong := needsStructure(rep, b)
	sizeEq, orderRequired := map[string]bool{}, map[string]bool{}
	for fi := range rep.sizeEquation {
		sizeEq[b.Files[fi].Name] = true
	}
	for fi := range rep.orderRequired {
		orderRequired[b.Files[fi].Name] = true
	}
	var media []bundle.LocalFile
	for _, lf := range loc.Files {
		if fileinfo.IsVerifiableMediaFile(lf.Name) {
			media = append(media, lf)
		}
	}
	if len(media) == 0 {
		return ReasonNoMediaFile, errors.New("no file the media probe can verify")
	}
	st, r, err := stage()
	if err != nil {
		return r, err
	}
	defer func() {
		if d := stagedDownloads(st); d != nil {
			_ = bud.noteStaged(d())
		}
	}()
	for _, lf := range media {
		if err := st.WriteFileMetadata(lf.Name, lf.Meta); err != nil {
			return ReasonStaging, err
		}
		res := contentverify.Probe(ctx, st.Opener(), lf.Name, i.cfg.Policy.MediaTimeout())
		switch res.Result {
		case contentverify.ContentValid:
		case contentverify.ContentInvalid:
			return ReasonMediaInvalid, fmt.Errorf("%s: %w", lf.Name, res.Err)
		case contentverify.ContentSegmentMissing:
			return ReasonMediaMissing, fmt.Errorf("%s: %w", lf.Name, res.Err)
		default:
			return ReasonMediaInconclusive, fmt.Errorf("%s: %w", lf.Name, res.Err)
		}
		why, unbound := strong[lf.Name]
		mode := structLoose
		switch {
		case sizeEq[lf.Name]:
			mode = structSizeEquation
		case unbound:
			mode = structStrong
		}
		var over func() bool
		if d := stagedDownloads(st); d != nil {
			from := d()
			over = func() bool { return d()-from > structMaxBytesPerFile }
		}
		if err := verifyStructureMode(ctx, st.Opener(), lf.Name, lf.Meta.FileSize, mode, over); err != nil {
			if unbound && errors.Is(err, errNotVerifiable) {
				err = fmt.Errorf("%w (its archive extents could not be checked: %s)", err, why)
			}
			return structureReason(err), fmt.Errorf("%s: %w", lf.Name, err)
		}
		if r, err := i.verifyPayloadOrder(ctx, st, b, lf.Name, lf.Meta.FileSize, bud, orderRequired[lf.Name]); err != nil {
			return r, err
		}
	}
	return "", nil
}

// write is the only step that changes anything the user can see. The order is
// state rows (pending), then the metadata files, then the content index, so a
// crash can never leave a visible file without its pending row, and a failure,
// panic or cancellation after any step removes exactly what this call created
// (the caller holds the per-target lock, so nothing here belongs to anyone
// else).
func (i *Importer) write(ctx context.Context, b *bundle.Bundle, loc *bundle.Local, req Request, res *Result) (err error) {
	var written []string
	var rowsDone, indexDone bool
	committed := false
	defer func() {
		if committed || i.crashed {
			return
		}
		bg := context.WithoutCancel(ctx)
		for _, p := range written {
			_ = i.cfg.Library.DeleteFileMetadata(bg, p)
		}
		if rowsDone {
			_ = i.cfg.State.Delete(bg, res.Paths)
		}
		if indexDone && i.cfg.Index != nil {
			for _, p := range res.Paths {
				_ = i.cfg.Index.Delete(bg, p)
			}
		}
	}()

	rows := make([]database.PeernetBundleImport, 0, len(loc.Files))
	for n := range loc.Files {
		rows = append(rows, database.PeernetBundleImport{
			FilePath: res.Paths[n], FileID: b.Files[n].FileID.String(), ReleaseID: b.ReleaseID.String(),
			NetworkID: req.Origin.NetworkID, PublisherID: req.Origin.PublisherID, QueueID: req.Target.QueueID,
			ArrOwed: !req.Target.SkipArr, Category: req.Target.Category, TargetPath: req.Target.TargetPath,
			ResultingPath: res.Dir,
		})
	}
	rowsDone = true // set first: a failed batch may have inserted some rows
	if err := i.cfg.State.Upsert(ctx, rows); err != nil {
		return err
	}
	if req.Origin.BundleID != "" {
		if err := i.cfg.State.SetBundleID(ctx, res.Paths, req.Origin.BundleID); err != nil {
			return err
		}
	}
	for n := range b.Files { // the exact boundaries verified above, for the forced full check
		if err := i.cfg.State.SetParts(ctx, res.Paths[n], b.Files[n].Parts); err != nil {
			return err
		}
	}
	if i.crashAt("rows") {
		return errCrashed
	}
	// A file the health system will never create a record for would stay
	// pending forever and hold the whole import back: it is marked passed at
	// once (the media files that do get checked decide the import).
	if i.cfg.WillBeChecked != nil {
		for n := range loc.Files {
			if !i.cfg.WillBeChecked(res.Paths[n]) {
				if _, err := i.cfg.State.MarkPassed(ctx, res.Paths[n]); err != nil {
					return err
				}
			}
		}
	}
	for n, lf := range loc.Files {
		if loc.NZBPassword != "" {
			lf.Meta.Password = loc.NZBPassword
		}
		if err := i.cfg.Library.WriteFileMetadata(res.Paths[n], lf.Meta); err != nil {
			return err
		}
		written = append(written, res.Paths[n])
		if i.crashAt("file") {
			return errCrashed
		}
	}
	if i.crashAt("files") {
		return errCrashed
	}
	// The verified release identity goes into the index after the files: the
	// metadata observer may have computed a provisional entry meanwhile, and
	// this one replaces it (nothing in the written metadata can reproduce the
	// ID or the group).
	if i.cfg.Index != nil {
		scheme := database.PeernetSchemeNZB
		if b.Scheme == bundle.SchemeFiles {
			scheme = database.PeernetSchemeFiles
		}
		entries := make([]database.PeernetContentEntry, len(loc.Files))
		for n := range loc.Files {
			entries[n] = contentid.VerifiedEntry(res.Paths[n], b.Files[n].FileID, b.ReleaseID, scheme)
		}
		indexDone = true
		if err := i.cfg.Index.UpsertBatch(ctx, entries); err != nil {
			return err
		}
	}
	if i.crashAt("index") {
		return errCrashed
	}
	// Last: only now is the import complete. A crash before this leaves rows
	// that are not committed, which start-up rolls back whole.
	if err := i.cfg.State.CommitRows(ctx, res.Paths); err != nil {
		return err
	}
	committed = true
	return nil
}

// lockTarget serialises the check-and-write of one target folder.
func (i *Importer) lockTarget(dir string) func() {
	i.tmu.Lock()
	l := i.targets[dir]
	if l == nil {
		l = &targetLock{}
		i.targets[dir] = l
	}
	l.refs++
	i.tmu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		i.tmu.Lock()
		if l.refs--; l.refs == 0 {
			delete(i.targets, dir)
		}
		i.tmu.Unlock()
	}
}
