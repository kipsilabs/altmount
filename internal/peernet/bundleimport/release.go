package bundleimport

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/importer/filesystem"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// directImportTimeout bounds one shared direct import (fetch plus accept).
const directImportTimeout = 3 * time.Minute

// ImportRelease is the direct import without an NZB: fetch the bundle of
// releaseID, accept it, and record it as a normal import (queue and history
// entry, post-processing, health scheduling). The folder is the target
// category plus the bundle's sanitised title, which is also the title shown.
// It is idempotent per release and concurrent requests for one release
// coalesce into one fetch and one import.
func (i *Importer) ImportRelease(ctx context.Context, releaseID string, target Target) (*Result, error) {
	if i.cfg.Fetcher == nil {
		return i.rejected(ReasonNoNetwork, errors.New("no fetcher installed"))
	}
	v, err, _ := i.flight.Do(releaseID, func() (any, error) {
		// The work is shared by every caller of this release, so it must not
		// die with the first caller's context: detach, with a bounded life.
		work, cancel := context.WithTimeout(context.WithoutCancel(ctx), directImportTimeout)
		defer cancel()
		return i.importRelease(work, releaseID, target)
	})
	if err != nil {
		return nil, err
	}
	return v.(*Result), nil
}

func (i *Importer) importRelease(ctx context.Context, releaseID string, target Target) (*Result, error) {
	b, origin, err := i.cfg.Fetcher.FetchBundle(ctx, releaseID)
	if err != nil || b == nil {
		return i.rejected(ReasonUnavailable, err)
	}
	if b.ReleaseID.String() != releaseID {
		return i.rejected(ReasonInvalid, errors.New("bundle is for another release"))
	}
	title := bundle.SanitizeFilename(b.Title)
	name, ok := filesystem.SafeFolderName(title)
	if !ok {
		return i.rejected(ReasonBadTarget, errors.New("the bundle title cannot name a folder"))
	}
	if target.Dir == "" && target.DirFunc != nil {
		target.Dir = target.DirFunc(title)
		if target.Dir == "" {
			return i.rejected(ReasonBadTarget, errors.New("the caller refused the bundle title"))
		}
	}
	if target.Dir == "" {
		category := strings.Trim(strings.ReplaceAll(target.Category, `\`, "/"), "/")
		target.Dir = path.Join(category, name)
	}
	if d, err := cleanTarget(target.Dir); err != nil || path.Base(d) == "." || path.Base(d) == ".." {
		return i.rejected(ReasonBadTarget, errors.New("the target folder is not a plain folder below its category"))
	}
	res, err := i.Accept(ctx, Request{Bundle: b, Origin: origin, Target: target})
	if err != nil {
		return nil, err
	}
	if res.AlreadyImported || i.cfg.Recorder == nil {
		return res, nil
	}
	queueID, err := i.cfg.Recorder.RecordPeerImport(ctx, ImportRecord{
		Title: title, Category: target.Category, TargetPath: target.TargetPath, ResultingPath: res.Dir,
		WrittenPaths: res.Paths, TotalSize: res.TotalSize, ReleaseID: releaseID, SkipArr: target.SkipArr, Metadata: target.Metadata, DownloadID: target.DownloadID,
	})
	if err != nil {
		bg := context.WithoutCancel(ctx)
		for _, p := range res.Paths {
			_ = i.cfg.Library.DeleteFileMetadata(bg, p)
		}
		_ = i.cfg.State.Delete(bg, res.Paths)
		return nil, fmt.Errorf("bundleimport: record import: %w", err)
	}
	if err := i.AttachQueue(ctx, res.Paths, queueID); err != nil {
		return nil, fmt.Errorf("bundleimport: link queue item: %w", err)
	}
	return res, nil
}
