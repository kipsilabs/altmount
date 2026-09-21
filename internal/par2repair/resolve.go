package par2repair

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// Resolution is everything RunJob needs, derived from a file's metadata.
type Resolution struct {
	Plan      *Plan
	Index     *par2.Index
	Par2Files []SetFile
}

// Resolve turns a damaged file's metadata into a repair plan:
//
//  1. Parse the PAR2 set from the metadata's Par2Files segments (recovery
//     payloads are located, not downloaded, via seek-aware lazy readers).
//  2. Match every recovery-set member (RAR volume / content file) to its
//     NzbStore entry — by filename in the subject first, then by Hash16k of
//     the file's first bytes.
//  3. Derive per-article decoded sizes from the first live article (usenet
//     posts use a uniform part size; the last article takes the remainder).
//  4. Mark dead articles (trigger's failing segment + persisted known holes)
//     and build the plan under the given caps.
func Resolve(
	ctx context.Context,
	fm *metapb.FileMetadata,
	store *metapb.NzbStore,
	deadSegmentIDs []string,
	fetch ArticleFetcher,
	caps Caps,
	log *slog.Logger,
	progress JobProgress,
) (*Resolution, error) {
	if len(fm.Par2Files) == 0 {
		return nil, fmt.Errorf("%w: no PAR2 files recorded for this release", ErrUnrepairable)
	}
	if store == nil || len(store.Files) == 0 {
		return nil, fmt.Errorf("%w: no NzbStore for this release", ErrUnrepairable)
	}

	// 1. PAR2 set files, smallest first so the index file is parsed cheaply.
	par2Refs := make([]*metapb.Par2FileReference, len(fm.Par2Files))
	copy(par2Refs, fm.Par2Files)
	sort.Slice(par2Refs, func(i, j int) bool { return par2Refs[i].FileSize < par2Refs[j].FileSize })

	cache := newArticleCache(resolveCacheCap)
	var par2Files []SetFile
	for _, ref := range par2Refs {
		sf := SetFile{Length: uint64(ref.FileSize)}
		for _, seg := range ref.SegmentData {
			sf.Articles = append(sf.Articles, Article{
				MessageID: normalizeMsgID(seg.Id),
				Size:      seg.SegmentSize,
			})
		}
		par2Files = append(par2Files, sf)
	}

	dead := map[string]bool{}
	for _, id := range deadSegmentIDs {
		if id != "" {
			dead[normalizeMsgID(id)] = true
		}
	}
	if err := releaseSizePrecheck(store.Files, par2Files, caps); err != nil {
		return nil, err
	}

	started := time.Now()
	hidden, err := statSweepBudgeted(ctx, fetch, releaseArticleIDs(store, par2Files), dead,
		newDamageBudget(store.Files, par2Files, caps), progress)
	if err != nil {
		return nil, err
	}
	caps.ExpectedHiddenArticles = hidden
	log.InfoContext(ctx, "PAR2 repair liveness check complete",
		"dead_articles", len(dead), "hidden_estimate", hidden, "duration", time.Since(started).Round(time.Millisecond))
	if err := ratioPrecheck(store.Files, par2Files, dead, caps); err != nil {
		return nil, err
	}

	idx, files, err := planSet(ctx, fetch, store, par2Files, dead, cache, log, progress)
	if err != nil {
		return nil, err
	}

	plan, err := BuildPlan(idx, files, caps)
	if err != nil {
		return nil, err
	}
	log.InfoContext(ctx, "PAR2 repair plan built",
		"missing_slices", len(plan.Missing), "recovery_rows", len(plan.Recovery),
		"spares", len(plan.SpareRecovery), "spill_to_disk", plan.SpillToDisk)
	return &Resolution{Plan: plan, Index: idx, Par2Files: par2Files}, nil
}

// planSet is the planning stage shared by both resolvers: parse the PAR2 set
// and match/size every recovery-set member. It is minutes of article fetches
// on a large release, so it reports progress in steps — one per PAR2 file
// parsed, one per member matched — and logs each phase.
func planSet(
	ctx context.Context,
	fetch ArticleFetcher,
	store *metapb.NzbStore,
	par2Files []SetFile,
	dead map[string]bool,
	cache *articleCache,
	log *slog.Logger,
	progress JobProgress,
) (*par2.Index, []SetFile, error) {
	report := func(done, total int) {
		if progress != nil {
			progress(StagePlanning, done, total)
		}
	}
	report(0, len(par2Files))
	log.InfoContext(ctx, "Planning PAR2 repair: parsing recovery set", "par2_files", len(par2Files))

	// Streams come after the liveness sweep so the lazy readers know which
	// articles are dead and zero-fill them instead of asking the pool. Each
	// stream reports itself when parsing first touches it.
	started := time.Now()
	streams := par2Streams(ctx, fetch, par2Files, dead, cache)
	for i, s := range streams {
		streams[i] = &planningStream{lazy: s.(*lazyFileReader), start: func() { report(i, len(par2Files)) }}
	}

	idx, err := par2.ParseIndex(streams)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse PAR2 set: %v", ErrUnrepairable, err)
	}
	dropDeadRecovery(idx, par2Files, dead)
	log.InfoContext(ctx, "PAR2 set parsed",
		"members", len(idx.RecoveryIDs), "recovery_slices", len(idx.Recovery),
		"slice_size", idx.SliceSize, "duration", time.Since(started).Round(time.Millisecond))

	// Match recovery-set members to NzbStore entries and size their articles,
	// one planning step per member.
	started = time.Now()
	total := len(par2Files) + len(idx.RecoveryIDs)
	report(len(par2Files), total)
	matched := 0
	files, err := matchSetFiles(ctx, idx, store, dead, fetch, cache, func() {
		matched++
		report(len(par2Files)+matched, total)
	})
	if err != nil {
		return nil, nil, err
	}
	log.InfoContext(ctx, "Recovery-set members matched",
		"files", len(files), "duration", time.Since(started).Round(time.Millisecond))
	report(total, total)
	return idx, files, nil
}

// planningStream reports the moment parsing first touches its PAR2 file, so
// the planning progress advances per file parsed.
type planningStream struct {
	lazy  *lazyFileReader
	start func()
}

func (p *planningStream) Read(b []byte) (int, error) {
	if p.start != nil {
		p.start()
		p.start = nil
	}
	return p.lazy.Read(b)
}

func (p *planningStream) Seek(offset int64, whence int) (int64, error) {
	return p.lazy.Seek(offset, whence)
}

// statSampleSize is how many articles the pre-plan liveness check STATs
// before deciding whether the full release needs sweeping. Large enough that
// broad hidden damage is all but certain to hit the sample, small enough that
// the check costs seconds instead of the minutes a full-release STAT sweep
// takes.
const statSampleSize = 512

// statSweep checks article liveness before planning, when the fetcher
// supports it, and folds confirmed misses into dead.
//
// It STATs a random sample first: when the sample confirms no damage beyond
// the already-known dead articles, the rest of the release is not checked —
// the plan trusts the known holes, and the payload sweep's margin rows absorb
// any stragglers the sample missed (worst case, a replan-and-retry). Only when
// the sample finds hidden damage does the sweep STAT every article, buying an
// exact damage picture: caps and recovery-count verdicts become accurate at
// plan time instead of after downloading the recovery set.
func statSweep(ctx context.Context, fetch ArticleFetcher, ids []string, dead map[string]bool, progress JobProgress) (int, error) {
	return statSweepBudgeted(ctx, fetch, ids, dead, nil, progress)
}

// statSweepBudgeted is statSweep with an early verdict: when budget is set and
// the sample alone proves the damage exceeds the repair cap, the sweep stops
// with ErrUnrepairable instead of spending minutes confirming it article by
// article. A nil budget never aborts.
func statSweepBudgeted(ctx context.Context, fetch ArticleFetcher, ids []string, dead map[string]bool, budget *damageBudget, progress JobProgress) (int, error) {
	stater, ok := fetch.(ArticleStater)
	if !ok {
		return 0, nil
	}
	unknown := make([]string, 0, len(ids))
	for _, id := range ids {
		if !dead[id] {
			unknown = append(unknown, id)
		}
	}
	if len(unknown) == 0 {
		return 0, nil
	}
	report := func(done, total int) {
		if progress != nil {
			progress(StageChecking, done, total)
		}
	}

	sample, rest := unknown, []string(nil)
	if len(unknown) > statSampleSize {
		rand.Shuffle(len(unknown), func(i, j int) { unknown[i], unknown[j] = unknown[j], unknown[i] })
		sample, rest = unknown[:statSampleSize], unknown[statSampleSize:]
	}
	report(0, len(sample))
	missing, err := stater.StatIDs(ctx, sample, func(done int) { report(done, len(sample)) })
	if err != nil {
		return 0, fmt.Errorf("par2repair: liveness sweep: %w", err)
	}
	for id := range missing {
		dead[id] = true
	}
	if len(rest) == 0 || len(missing) == 0 {
		return 0, nil
	}
	if budget != nil {
		if ratio, proven := budget.sampleProves(sample, dead); proven {
			return 0, fmt.Errorf("%w: damage ratio at least %.4f exceeds max_repair_ratio %.4f (decided from a %d-article sample)",
				ErrUnrepairable, ratio, budget.maxRatio, len(sample))
		}
	}

	// The sample surfaced damage nothing declared. STATing the whole release
	// would pin the damage exactly, but it costs one round trip per article —
	// minutes on a large release — while the payload sweep verifies every
	// slice anyway. So when the sample's estimate of the hidden damage is
	// small enough to absorb on margin rows, the estimate is the answer: the
	// plan provisions rows for it and the sweep finds the articles for free.
	estimate := (len(missing)*len(rest) + len(sample) - 1) / len(sample)
	if estimate <= maxHiddenAbsorbArticles {
		return estimate, nil
	}

	// Damage too broad for margin rows: only the full census can size the
	// plan, so its minutes are worth spending.
	total := len(unknown)
	missing, err = stater.StatIDs(ctx, rest, func(done int) { report(len(sample)+done, total) })
	if err != nil {
		return 0, fmt.Errorf("par2repair: liveness sweep: %w", err)
	}
	for id := range missing {
		dead[id] = true
	}
	return 0, nil
}

// damageBudget is what the liveness sweep needs to call a release unrepairable
// early: the encoded size of every content article, their sum, and the cap.
type damageBudget struct {
	bytes    map[string]int64
	total    int64
	maxRatio float64
}

// newDamageBudget builds the budget from the NZB layout, PAR2 articles
// excluded on both sides of the ratio. nil when no cap is configured.
func newDamageBudget(files []*metapb.NzbFileEntry, par2Files []SetFile, caps Caps) *damageBudget {
	if caps.MaxRepairRatio <= 0 {
		return nil
	}
	par2IDs := map[string]bool{}
	for _, f := range par2Files {
		for _, a := range f.Articles {
			par2IDs[a.MessageID] = true
		}
	}
	b := &damageBudget{bytes: map[string]int64{}, maxRatio: caps.MaxRepairRatio * ratioPrecheckMargin}
	for _, f := range files {
		if len(f.Segments) == 0 || par2IDs[normalizeMsgID(f.Segments[0].Id)] {
			continue
		}
		for _, seg := range f.Segments {
			id := normalizeMsgID(seg.Id)
			b.bytes[id] = seg.Bytes
			b.total += seg.Bytes
		}
	}
	if b.total == 0 {
		return nil
	}
	return b
}

// sampleProves reports whether the STATed sample already proves the damage
// ratio exceeds the cap, and the ratio it proves. The dead fraction of the
// sampled bytes is taken three standard errors low and applied to the bytes
// not yet checked, known-dead bytes are added in full, and the result is
// divided by every content byte — the largest denominator ratioPrecheck could
// use. Each step errs towards "repairable": a wrong "unrepairable" here is a
// wrong verdict, not just a slow one, so the bound is wide (about one release
// in a thousand sitting exactly on the cap), while a 95%-dead release is still
// decided from the sample alone.
func (b *damageBudget) sampleProves(sample []string, dead map[string]bool) (float64, bool) {
	var sampled, sampledDead int64
	inSample := make(map[string]bool, len(sample))
	for _, id := range sample {
		n, ok := b.bytes[id]
		if !ok {
			continue
		}
		inSample[id] = true
		sampled += n
		if dead[id] {
			sampledDead += n
		}
	}
	if sampled == 0 {
		return 0, false
	}
	var knownDead int64
	for id := range dead {
		if !inSample[id] {
			knownDead += b.bytes[id]
		}
	}
	p := float64(sampledDead) / float64(sampled)
	lower := p - 3*math.Sqrt(p*(1-p)/float64(len(sample)))
	if lower < 0 {
		lower = 0
	}
	unknown := float64(b.total - sampled - knownDead)
	ratio := (float64(sampledDead) + float64(knownDead) + lower*unknown) / float64(b.total)
	return ratio, ratio > b.maxRatio
}

// par2Streams builds one lazy reader per PAR2 file, with the dead flags the
// liveness sweep produced applied so known holes read as zeros without a
// pointless fetch.
func par2Streams(ctx context.Context, fetch ArticleFetcher, par2Files []SetFile, dead map[string]bool, cache *articleCache) []io.Reader {
	streams := make([]io.Reader, len(par2Files))
	for i := range par2Files {
		for j := range par2Files[i].Articles {
			if dead[par2Files[i].Articles[j].MessageID] {
				par2Files[i].Articles[j].Dead = true
			}
		}
		streams[i] = newLazyFileReader(ctx, fetch, par2Files[i], cache)
	}
	return streams
}

// releaseArticleIDs collects every article ID of the release: all NzbStore
// segments plus the PAR2 set's articles (which metadata records separately).
func releaseArticleIDs(store *metapb.NzbStore, par2Files []SetFile) []string {
	var ids []string
	for _, f := range store.Files {
		for _, seg := range f.Segments {
			ids = append(ids, normalizeMsgID(seg.Id))
		}
	}
	for _, f := range par2Files {
		for _, a := range f.Articles {
			ids = append(ids, a.MessageID)
		}
	}
	return ids
}

// releaseSizePrecheck refuses releases outside the configured size range
// before any network work: the release size is known from the NZB layout
// alone, so the verdict is free. Sizes are encoded segment bytes (a few
// percent above the decoded size), which is close enough for a threshold.
// PAR2 files are excluded — the bound is about the content being streamed.
func releaseSizePrecheck(files []*metapb.NzbFileEntry, par2Files []SetFile, caps Caps) error {
	if caps.MinReleaseSizeBytes <= 0 && caps.MaxReleaseSizeBytes <= 0 {
		return nil
	}
	par2IDs := map[string]bool{}
	for _, f := range par2Files {
		for _, a := range f.Articles {
			par2IDs[a.MessageID] = true
		}
	}
	var total int64
	for _, f := range files {
		if len(f.Segments) == 0 || par2IDs[normalizeMsgID(f.Segments[0].Id)] {
			continue
		}
		for _, seg := range f.Segments {
			total += seg.Bytes
		}
	}
	mb := func(n int64) int64 { return n >> 20 }
	if caps.MinReleaseSizeBytes > 0 && total < caps.MinReleaseSizeBytes {
		return fmt.Errorf("%w: release size %d MB is below min_release_size_mb %d",
			ErrUnrepairable, mb(total), mb(caps.MinReleaseSizeBytes))
	}
	if caps.MaxReleaseSizeBytes > 0 && total > caps.MaxReleaseSizeBytes {
		return fmt.Errorf("%w: release size %d MB exceeds max_release_size_mb %d",
			ErrUnrepairable, mb(total), mb(caps.MaxReleaseSizeBytes))
	}
	return nil
}

// ratioPrecheckMargin keeps the pre-parse ratio check conservative: it works
// from encoded segment bytes, which only approximate the decoded sizes
// BuildPlan measures (yEnc overhead mostly cancels in the ratio). The margin
// ensures borderline releases fall through to BuildPlan's exact check instead
// of being rejected on an estimate.
const ratioPrecheckMargin = 1.05

// ratioPrecheck rejects damage far above the repair-ratio cap right after the
// liveness sweep — before the PAR2 set is parsed or any article body is
// downloaded. The estimate needs only the NZB layout: per damaged content
// file, dead segment bytes over the file's total segment bytes. PAR2 files
// (identified by their articles) are excluded, mirroring BuildPlan's ratio
// over recovery-set members. The error matches BuildPlan's exact-check
// message so callers and the UI treat both identically.
func ratioPrecheck(files []*metapb.NzbFileEntry, par2Files []SetFile, dead map[string]bool, caps Caps) error {
	if caps.MaxRepairRatio <= 0 || len(dead) == 0 {
		return nil
	}
	par2IDs := map[string]bool{}
	for _, f := range par2Files {
		for _, a := range f.Articles {
			par2IDs[a.MessageID] = true
		}
	}
	var missingBytes, damagedFileBytes int64
	for _, f := range files {
		if len(f.Segments) == 0 || par2IDs[normalizeMsgID(f.Segments[0].Id)] {
			continue
		}
		var fileBytes, deadBytes int64
		for _, seg := range f.Segments {
			fileBytes += seg.Bytes
			if dead[normalizeMsgID(seg.Id)] {
				deadBytes += seg.Bytes
			}
		}
		if deadBytes > 0 {
			missingBytes += deadBytes
			damagedFileBytes += fileBytes
		}
	}
	if damagedFileBytes == 0 {
		return nil
	}
	if ratio := float64(missingBytes) / float64(damagedFileBytes); ratio > caps.MaxRepairRatio*ratioPrecheckMargin {
		return fmt.Errorf("%w: damage ratio %.4f exceeds max_repair_ratio %.4f",
			ErrUnrepairable, ratio, caps.MaxRepairRatio)
	}
	return nil
}

// dropDeadRecovery removes recovery slice refs whose payload overlaps a dead
// article. ParseIndex seeks past recovery payloads without fetching them, so
// a ref can parse fine while its payload is gone; counting such refs would
// make BuildPlan promise recovery slices the job can never fetch.
func dropDeadRecovery(idx *par2.Index, par2Files []SetFile, dead map[string]bool) {
	if len(dead) == 0 {
		return
	}
	sliceSize := int64(idx.SliceSize)
	live := make([]par2.RecoverySliceRef, 0, len(idx.Recovery))
	for _, ref := range idx.Recovery {
		if payloadArticlesLive(par2Files[ref.FileIndex], ref.BodyOffset, sliceSize, dead) {
			live = append(live, ref)
		}
	}
	idx.Recovery = live
}

// payloadArticlesLive reports whether every article overlapping the payload
// range [off, off+n) of a file's concatenated articles is live.
func payloadArticlesLive(f SetFile, off, n int64, dead map[string]bool) bool {
	var artStart int64
	for _, a := range f.Articles {
		artEnd := artStart + a.Size
		if artEnd > off && artStart < off+n && dead[a.MessageID] {
			return false
		}
		artStart = artEnd
		if artStart >= off+n {
			break
		}
	}
	return true
}

// matchSetFiles pairs recovery-set FileDescs with NzbStore entries. onMember,
// when non-nil, is called once per member whose articles have been sized —
// the unit of planning progress.
func matchSetFiles(
	ctx context.Context,
	idx *par2.Index,
	store *metapb.NzbStore,
	dead map[string]bool,
	fetch ArticleFetcher,
	cache *articleCache,
	onMember func(),
) ([]SetFile, error) {
	used := make([]bool, len(store.Files))
	var out []SetFile
	var matched []matchedFile

	for _, id := range idx.RecoveryIDs {
		fd := idx.Files[id]
		entry := -1
		// Pass 1: filename appears in the subject.
		for i, f := range store.Files {
			if !used[i] && fd.Name != "" && strings.Contains(f.Subject, fd.Name) {
				entry = i
				break
			}
		}
		// Pass 2: Hash16k of the entry's first bytes.
		if entry < 0 {
			for i, f := range store.Files {
				if used[i] || len(f.Segments) == 0 || dead[normalizeMsgID(f.Segments[0].Id)] {
					continue
				}
				prefix, err := filePrefix(ctx, fetch, f, cache)
				if err != nil {
					continue // dead or unreadable head article: cannot match by content
				}
				if bytes.HasPrefix(prefix, []byte("PAR2\x00PKT")) {
					continue // a PAR2 file, never a recovery-set member
				}
				if hash16k(prefix, int64(fd.Length)) == fd.Hash16k {
					entry = i
					break
				}
			}
		}
		if entry < 0 {
			return nil, fmt.Errorf("%w: recovery-set member %q not found in NZB", ErrUnrepairable, fd.Name)
		}
		used[entry] = true
		matched = append(matched, matchedFile{id: id, entry: store.Files[entry]})
	}

	// Warm every member's head article: sizing probes them one at a time
	// below, and each would otherwise pay a full fetch round-trip in
	// sequence. The warm pool bounds the fan-out on huge releases.
	for _, m := range matched {
		if len(m.entry.Segments) > 0 {
			if id := normalizeMsgID(m.entry.Segments[0].Id); !dead[id] {
				cache.warm(ctx, fetch, id)
			}
		}
	}

	// Pass 1: size every file that still has a live article to probe, and
	// remember the part size the release was posted with.
	out = make([]SetFile, len(matched))
	var releasePartSize int64
	var deferred []int
	for i, m := range matched {
		sf, partSize, err := sizeArticles(ctx, idx, m.id, m.entry, dead, fetch, cache, 0)
		if errors.Is(err, errNoLiveArticle) {
			deferred = append(deferred, i)
			continue
		}
		if err != nil {
			return nil, err
		}
		if releasePartSize == 0 && partSize > 0 {
			releasePartSize = partSize
		}
		out[i] = sf
		if onMember != nil {
			onMember()
		}
	}

	// Pass 2: a volume with no live article at all is exactly what PAR2 repair
	// is for. Its decoded part size cannot be probed, so borrow the release's —
	// usenet posts split every file of a release at one uniform size. The
	// borrowed layout is checked against the file's exact PAR2 length here, and
	// every rebuilt slice is verified against its PAR2 MD5 before being stored,
	// so a wrong guess fails the repair rather than corrupting it.
	for _, i := range deferred {
		if releasePartSize == 0 {
			fd := idx.Files[matched[i].id]
			return nil, fmt.Errorf("%w: no live article anywhere in the release to derive the part size (every article of %q and its siblings is missing)",
				ErrUnrepairable, fd.Name)
		}
		sf, _, err := sizeArticles(ctx, idx, matched[i].id, matched[i].entry, dead, fetch, cache, releasePartSize)
		if err != nil {
			return nil, err
		}
		out[i] = sf
		if onMember != nil {
			onMember()
		}
	}
	return out, nil
}

// matchedFile pairs a recovery-set member with the NZB entry it resolved to.
type matchedFile struct {
	id    [16]byte
	entry *metapb.NzbFileEntry
}

// errNoLiveArticle marks a file whose every article is missing, so its part
// size must come from elsewhere in the release.
var errNoLiveArticle = errors.New("par2repair: no live article to probe")

// sizeArticles derives per-article decoded sizes for one recovery-set member
// from the uniform part size of its first live article. When no article is
// live, hint (the release-wide part size, if already known) is used instead;
// a zero hint yields errNoLiveArticle so the caller can retry once it knows
// one. It returns the part size it used, or 0 when the file is a single
// article and therefore says nothing about the release's part size.
func sizeArticles(
	ctx context.Context,
	idx *par2.Index,
	fileID [16]byte,
	entry *metapb.NzbFileEntry,
	dead map[string]bool,
	fetch ArticleFetcher,
	cache *articleCache,
	hint int64,
) (SetFile, int64, error) {
	fd := idx.Files[fileID]
	n := len(entry.Segments)
	length := int64(fd.Length)
	sf := SetFile{FileID: fileID, Length: fd.Length}

	if n == 0 {
		return sf, 0, fmt.Errorf("%w: file %q has no segments in NZB", ErrUnrepairable, fd.Name)
	}

	partSize := length // single-article fallback: the whole file
	derived := int64(0)
	if n > 1 {
		partSize = -1
		for _, seg := range entry.Segments {
			msgID := normalizeMsgID(seg.Id)
			if dead[msgID] {
				continue
			}
			payload, err := fetchCached(ctx, fetch, msgID, cache)
			if err != nil {
				if errors.Is(err, nntppool.ErrArticleNotFound) {
					dead[msgID] = true // discovered dead while probing
					continue
				}
				return sf, 0, fmt.Errorf("par2repair: probe article %s: %w", msgID, err)
			}
			partSize = int64(len(payload))
			break
		}
		if partSize <= 0 {
			if hint <= 0 {
				return sf, 0, errNoLiveArticle
			}
			partSize = hint
		} else {
			derived = partSize
		}
		// The probe may have hit the (smaller) final article; a uniform part
		// size must satisfy (n-1)*partSize < length <= n*partSize. This is also
		// what validates a borrowed hint.
		if int64(n-1)*partSize >= length || int64(n)*partSize < length {
			return sf, 0, fmt.Errorf("%w: part size %d inconsistent with %d articles of %d bytes in %q",
				ErrUnrepairable, partSize, n, length, fd.Name)
		}
	}

	var off int64
	for i, seg := range entry.Segments {
		msgID := normalizeMsgID(seg.Id)
		size := partSize
		if i == n-1 {
			size = length - off
		}
		sf.Articles = append(sf.Articles, Article{
			MessageID: msgID,
			Size:      size,
			Dead:      dead[msgID],
		})
		off += size
	}
	if off != length {
		return sf, 0, fmt.Errorf("%w: article sizes for %q sum to %d, want %d", ErrUnrepairable, fd.Name, off, length)
	}
	return sf, derived, nil
}

// filePrefix returns the first up-to-16KiB of a store file's content.
func filePrefix(ctx context.Context, fetch ArticleFetcher, entry *metapb.NzbFileEntry, cache *articleCache) ([]byte, error) {
	payload, err := fetchCached(ctx, fetch, normalizeMsgID(entry.Segments[0].Id), cache)
	if err != nil {
		return nil, err
	}
	if len(payload) > 16384 {
		return payload[:16384], nil
	}
	return payload, nil
}

// hash16k computes the PAR2 Hash16k: MD5 of the first 16KiB of the file,
// zero-padded when the file itself is shorter (matching par2gen; files this
// small are rare in practice).
func hash16k(prefix []byte, fileLength int64) [16]byte {
	if fileLength >= 16384 && len(prefix) >= 16384 {
		return md5.Sum(prefix[:16384])
	}
	padded := make([]byte, 16384)
	copy(padded, prefix)
	return md5.Sum(padded)
}

// resolveCacheCap bounds the resolve/job article caches by entry count. The
// caches only exist to spare adjacent readers a refetch, so a small window is
// enough; without a cap a large PAR2 set would pin its header articles on the
// heap for the whole job.
const resolveCacheCap = 64

// articleCache is a FIFO-bounded article payload cache.
// warmPoolSize bounds how many background warm fetches run at once across one
// resolve. Planning shares the pool's normal lane, so warming stays modest.
const warmPoolSize = 8

// articleCache is a concurrency-safe FIFO article cache with singleflight:
// concurrent demands for the same article share one download, and background
// warms (see warm) pipeline the otherwise latency-bound planning fetches.
type articleCache struct {
	cap     int
	warmSem chan struct{}

	mu       sync.Mutex
	data     map[string][]byte
	order    []string
	inflight map[string]chan struct{} // closed when the fetch finishes (either way)
}

func newArticleCache(cap int) *articleCache {
	return &articleCache{
		cap:      cap,
		warmSem:  make(chan struct{}, warmPoolSize),
		data:     map[string][]byte{},
		inflight: map[string]chan struct{}{},
	}
}

// get returns a cached payload.
func (c *articleCache) get(id string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.data[id]
	return data, ok
}

// put stores an entry, evicting the oldest past the cap.
func (c *articleCache) put(id string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putLocked(id, data)
}

// putLocked stores an entry; the caller holds c.mu.
func (c *articleCache) putLocked(id string, data []byte) {
	if _, ok := c.data[id]; ok {
		return
	}
	c.data[id] = data
	c.order = append(c.order, id)
	for len(c.order) > c.cap {
		delete(c.data, c.order[0])
		c.order = c.order[1:]
	}
}

// warm fetches an article into the cache in the background — deduped against
// cached and in-flight fetches, bounded by the warm pool. Best-effort: fetch
// errors are dropped, the demand path refetches and classifies them.
func (c *articleCache) warm(ctx context.Context, fetch ArticleFetcher, msgID string) {
	c.mu.Lock()
	if _, ok := c.data[msgID]; ok {
		c.mu.Unlock()
		return
	}
	if _, busy := c.inflight[msgID]; busy {
		c.mu.Unlock()
		return
	}
	ch := make(chan struct{})
	c.inflight[msgID] = ch
	c.mu.Unlock()

	go func() {
		defer close(ch) // after the bookkeeping below, so waiters see the result
		select {
		case c.warmSem <- struct{}{}:
			defer func() { <-c.warmSem }()
		case <-ctx.Done():
			c.mu.Lock()
			delete(c.inflight, msgID)
			c.mu.Unlock()
			return
		}
		data, err := fetch.Fetch(ctx, msgID)
		c.mu.Lock()
		delete(c.inflight, msgID)
		if err == nil {
			c.putLocked(msgID, data)
		}
		c.mu.Unlock()
	}()
}

// fetchCached returns the article's payload, from cache when possible.
// Concurrent callers of the same article singleflight onto one download; a
// caller landing on a failed in-flight fetch retries the download itself, so
// warm failures never surface as anyone else's error.
func fetchCached(ctx context.Context, fetch ArticleFetcher, msgID string, cache *articleCache) ([]byte, error) {
	var ch chan struct{}
	for {
		cache.mu.Lock()
		if data, ok := cache.data[msgID]; ok {
			cache.mu.Unlock()
			return data, nil
		}
		theirs, busy := cache.inflight[msgID]
		if !busy {
			ch = make(chan struct{})
			cache.inflight[msgID] = ch
			cache.mu.Unlock()
			break // this caller downloads
		}
		cache.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-theirs:
		}
	}

	data, err := fetch.Fetch(ctx, msgID)
	cache.mu.Lock()
	delete(cache.inflight, msgID)
	if err == nil {
		cache.putLocked(msgID, data)
	}
	cache.mu.Unlock()
	close(ch)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// normalizeMsgID strips optional angle brackets so NzbStore ids ("abc@x") and
// SegmentData ids ("<abc@x>") compare equal. Patch-store keys use the
// normalized form.
func normalizeMsgID(id string) string {
	return strings.Trim(id, "<>")
}

// lazyFileReader is a seekable reader over a file's concatenated article
// payloads, fetching articles only when their bytes are actually read.
// ParseIndex's seek-aware skips make scanning a PAR2 volume cost roughly one
// article per packet header instead of the whole file.
//
// A dead article — flagged up front or discovered on fetch — reads as zeros
// rather than an error: the packet parser's per-packet hashes reject whatever
// the hole damaged and its resync recovers the packets behind it, so one dead
// article costs the packets it overlapped instead of the rest of the volume.
type lazyFileReader struct {
	ctx   context.Context
	fetch ArticleFetcher
	file  SetFile
	cache *articleCache
	pos   int64
	size  int64

	// Stride prediction: ParseIndex reads a packet header, seeks past the
	// payload, reads the next header — so within one PAR2 file, successive
	// article fetches land a uniform article-stride apart. Once two strides
	// agree, the next few articles on that stride are warmed in the
	// background, turning the latency-bound header walk into a pipeline
	// without prefetching the payload articles the parser skips.
	lastArt   int // index of the last article fetched; -1 before the first
	lastDelta int // last observed forward article stride
}

// lazyWarmDepth is how many articles ahead a stable stride warms — the depth
// of the planning pipeline. Bounded by the warm pool and the cache cap.
const lazyWarmDepth = 4

func newLazyFileReader(ctx context.Context, fetch ArticleFetcher, file SetFile, cache *articleCache) *lazyFileReader {
	return &lazyFileReader{
		ctx: ctx, fetch: fetch, file: file, cache: cache,
		size:    articleSizeSum(file.Articles),
		lastArt: -1,
	}
}

// maybeWarm records that article i was just fetched and, when the article
// stride has stabilized, warms the next lazyWarmDepth articles on it.
func (l *lazyFileReader) maybeWarm(i int) {
	if l.lastArt >= 0 {
		if d := i - l.lastArt; d > 0 {
			if d == l.lastDelta {
				for k := 1; k <= lazyWarmDepth; k++ {
					j := i + k*d
					if j >= len(l.file.Articles) {
						break
					}
					if a := l.file.Articles[j]; !a.Dead {
						l.cache.warm(l.ctx, l.fetch, a.MessageID)
					}
				}
			}
			l.lastDelta = d
		}
	}
	l.lastArt = i
}

func (l *lazyFileReader) Read(p []byte) (int, error) {
	if l.pos >= l.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if l.pos+n > l.size {
		n = l.size - l.pos
	}
	// Locate and fetch only the articles overlapping [pos, pos+n).
	var artStart int64
	read := 0
	for i, a := range l.file.Articles {
		artEnd := artStart + a.Size
		if artEnd > l.pos && artStart < l.pos+n {
			from := max(l.pos, artStart)
			to := min(l.pos+n, artEnd)
			span := p[from-l.pos : to-l.pos]

			var data []byte
			if !a.Dead {
				var err error
				data, err = fetchCached(l.ctx, l.fetch, a.MessageID, l.cache)
				if err != nil && !errors.Is(err, nntppool.ErrArticleNotFound) {
					return read, err
				}
				l.maybeWarm(i)
			}
			// Dead, vanished or short articles read as zeros; p may carry the
			// caller's stale bytes, so the gap is cleared explicitly.
			clear(span)
			if srcFrom := from - artStart; srcFrom < int64(len(data)) {
				copy(span, data[srcFrom:min(int64(len(data)), to-artStart)])
			}
			read = int(to - l.pos)
		}
		artStart = artEnd
		if artStart >= l.pos+n {
			break
		}
	}
	l.pos += int64(read)
	return read, nil
}

func (l *lazyFileReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = l.pos + offset
	case io.SeekEnd:
		abs = l.size + offset
	default:
		return 0, fmt.Errorf("par2repair: invalid seek whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("par2repair: negative seek position")
	}
	l.pos = abs
	return abs, nil
}

// maxHiddenAbsorbArticles is the largest sample-estimated count of hidden
// dead articles the plan absorbs on margin rows instead of escalating to a
// full-release STAT. Beyond it the exact census is worth its minutes: the
// margin rows it would take exceed maxHiddenMarginRows.
const maxHiddenAbsorbArticles = 64
