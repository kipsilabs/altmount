package importer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer/filesystem"
	"github.com/kipsilabs/altmount/internal/importer/validation"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// PeerBundleRequest is what the peer-bundle fast path is told about the NZB
// about to be processed. It carries plain data only; this package never imports
// the peer network (internal/peernet/bridge adapts it).
type PeerBundleRequest struct {
	// NZB is the segment list of the NZB the user supplied, per NZB file in NZB
	// order, parsed but not yet analysed.
	NZB *metapb.NzbStore
	// QueueID is the queue item being processed.
	QueueID int
	// NzbName is the clean release name of the queue item.
	NzbName string
	// TargetDir is the local virtual folder the files would be written to,
	// derived exactly as for a normal multi-file import.
	TargetDir string
	// Category is the queue item's category.
	Category string
	// TargetPath and SkipArr are the queue item's own notification settings.
	TargetPath string
	SkipArr    bool
}

// PeerBundleOutcome is a successful fast-path import.
type PeerBundleOutcome struct {
	// ResultPath is the release folder; WrittenPaths are the file paths.
	ResultPath   string
	WrittenPaths []string
	TotalSize    int64
}

// PeerBundleFastPath imports a release from a peer bundle instead of
// analysing it. It is nil by default and installed only while the peer
// network is enabled. TryBundle fails open: ok=false means "import normally",
// and it must not make an import slower than its own short deadline.
type PeerBundleFastPath interface {
	// MayHaveOffers is a cheap, cached answer to "does this node hold any
	// bundle offer at all". With false the import skips every peer-network step
	// (no store, no hashing); an unsure answer must be true.
	MayHaveOffers(ctx context.Context) bool
	TryBundle(ctx context.Context, req PeerBundleRequest) (out PeerBundleOutcome, ok bool)
	// HoldQueueItem reports whether the queue item came from a peer bundle and
	// still awaits its full checks, which is when the ARRs must not be told.
	HoldQueueItem(ctx context.Context, queueID int64) bool
	// AttachQueueItem links freshly written bundle files to the queue item of a
	// direct import, before its success handling runs.
	AttachQueueItem(ctx context.Context, paths []string, queueID int64) error
}

type peerFastPathHolder struct{ h PeerBundleFastPath }

// peerItemInfo is the queue item's notification settings, parked for the
// processor (ProcessNzbFile does not receive the item).
type peerItemInfo struct {
	targetPath string
	skipArr    bool
}

// SetPeerBundleFastPath installs (or, with nil, removes) the fast path.
func (proc *Processor) SetPeerBundleFastPath(h PeerBundleFastPath) {
	if h == nil {
		proc.peerFastPath.Store(nil)
		return
	}
	proc.peerFastPath.Store(&peerFastPathHolder{h: h})
}

func (proc *Processor) setPeerItem(queueID int, info peerItemInfo) {
	proc.peerItems.Store(queueID, info)
}
func (proc *Processor) clearPeerItem(queueID int) { proc.peerItems.Delete(queueID) }

// tryPeerBundle is the fast path, called with the NZB parsed and nothing else
// done. It never fails an import: any problem is "not taken".
//
// When it declines after building the NZB store, the store is returned so the
// import's own parse reuses it (see parser.ParseOptions.PrebuiltStore).
func (proc *Processor) tryPeerBundle(ctx context.Context, n *nzbparser.Nzb, filePath string, queueID int, virtualDir string, category *string, metadataJSON *string, downloadID *string) (result string, written []string, taken bool, prebuilt *prebuiltStore) {
	holder := proc.peerFastPath.Load()
	if holder == nil {
		return "", nil, false, nil
	}
	defer func() {
		if r := recover(); r != nil {
			proc.log.ErrorContext(ctx, "Peer bundle fast path panicked; importing normally", "panic", r)
			result, written, taken = "", nil, false
		}
	}()
	// The common case: no peer offers anything. Nothing is hashed or built.
	if !holder.h.MayHaveOffers(ctx) {
		return "", nil, false, nil
	}
	store, index := metadata.BuildStore(n)
	prebuilt = &prebuiltStore{store: store, index: index}
	nzbName := proc.getCleanNzbName(filePath, queueID)
	req := PeerBundleRequest{NZB: store, QueueID: queueID, NzbName: nzbName, TargetDir: filesystem.NzbFolderPath(virtualDir, nzbName)}
	if category != nil {
		req.Category = *category
	}
	if v, ok := proc.peerItems.Load(queueID); ok {
		info := v.(peerItemInfo)
		req.TargetPath, req.SkipArr = info.targetPath, info.skipArr
	}
	out, ok := holder.h.TryBundle(ctx, req)
	if !ok {
		return "", nil, false, prebuilt
	}
	if proc.recorder != nil {
		nzbID := int64(queueID)
		if err := proc.recorder.AddImportHistory(ctx, &database.ImportHistory{
			DownloadID: downloadID, NzbID: &nzbID, NzbName: nzbName, FileName: filepath.Base(out.ResultPath),
			FileSize: out.TotalSize, VirtualPath: out.ResultPath, Category: category, Metadata: metadataJSON,
			CompletedAt: time.Now(),
		}); err != nil {
			proc.log.ErrorContext(ctx, "Failed to add import history", "error", err, "nzb_name", nzbName)
		}
	}
	proc.updateProgress(queueID, 100)
	return out.ResultPath, out.WrittenPaths, true, nil
}

// prebuiltStore is the NZB store (and message-id index) built for the peer
// lookup, kept for the import's parse when the NZB was not changed meanwhile.
type prebuiltStore struct {
	store *metapb.NzbStore
	index map[string]int64
}

// PeerProbeList is one file of a peer bundle for the release probe.
type PeerProbeList struct {
	Name       string
	SegmentIDs []string
}

var (
	// ErrPeerProbeMissing: sampled articles are missing on the providers.
	ErrPeerProbeMissing = errors.New("importer: peer bundle probe found missing articles")
	// ErrPeerProbeInconclusive: the providers could not answer.
	ErrPeerProbeInconclusive = errors.New("importer: peer bundle probe inconclusive")
)

// ProbePeerRelease runs the import's own release probe (a STAT sample against
// the user's providers, same sampling percentage and first/last-segment rules)
// over the segments of a peer bundle, plus the first and last segment of every
// file. It never downloads a body.
func (proc *Processor) ProbePeerRelease(ctx context.Context, files []PeerProbeList) error {
	if !proc.poolManager.HasPool() {
		return fmt.Errorf("%w: no NNTP provider", ErrPeerProbeInconclusive)
	}
	cfg := proc.configGetter()
	all := make([]validation.FastFailFile, 0, len(files))
	edges := make([]validation.FastFailFile, 0, len(files)*2)
	for _, f := range files {
		segs := make([]*metapb.SegmentData, len(f.SegmentIDs))
		for i, id := range f.SegmentIDs {
			segs[i] = &metapb.SegmentData{Id: id}
		}
		all = append(all, validation.FastFailFile{Filename: f.Name, Segments: segs})
		if len(segs) > 0 {
			edges = append(edges, validation.FastFailFile{Filename: f.Name + "#first", Segments: segs[:1]})
			if len(segs) > 1 {
				edges = append(edges, validation.FastFailFile{Filename: f.Name + "#last", Segments: segs[len(segs)-1:]})
			}
		}
	}
	concurrency := proc.poolManager.StatSweepConcurrency(cfg.StatConcurrency())
	for _, probe := range []struct {
		files []validation.FastFailFile
		pct   int
	}{{all, cfg.Import.SegmentSamplePercentage}, {edges, 100}} {
		verdict, err := validation.FastFailReleaseProbeVerdict(ctx, probe.files, proc.poolManager, probe.pct,
			concurrency, proc.validationTimeout, proc.patchIndex, time.Time{})
		if err != nil {
			return fmt.Errorf("%w: %w", ErrPeerProbeInconclusive, err)
		}
		if verdict.Missing {
			return ErrPeerProbeMissing
		}
	}
	return nil
}
