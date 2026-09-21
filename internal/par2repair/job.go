package par2repair

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"slices"
	"sort"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
)

// ArticleFetcher fetches one decoded article payload. Implementations must be
// safe for concurrent use; the job keeps several fetches in flight.
type ArticleFetcher interface {
	Fetch(ctx context.Context, messageID string) ([]byte, error)
}

// fetchAhead is the default prefetch depth when no concurrency was
// configured (WithConcurrency): how many article fetches a job keeps in
// flight, and how many fetched-but-unconsumed payloads it buffers. Fetches
// ride the pool's background lane, which is what keeps them behind playback
// and imports.
const fetchAhead = 8

// SweepDeadArticleError reports an article that was live at planning time but
// vanished during the sweep. Transient at the job level (a retry can replan),
// but the caller should persist the discoveries so the next plan includes them
// in the missing set instead of hitting the same wall.
//
// Absorbed carries the OTHER articles the same sweep already proved dead and
// fitted onto margin rows before the margin ran out. Reporting them together
// is what lets the retry converge: the sweep pays for a full read of the
// release, so every death it observed must survive the failure. Persisting
// only MessageID would advance the plan by one article per attempt while
// re-parsing the PAR2 set and re-reading the release each time, and a release
// with more dead articles than margin rows would exhaust maxJobAttempts
// instead of ever being repaired.
type SweepDeadArticleError struct {
	MessageID string
	Absorbed  []string
	Err       error
}

// DeadMessageIDs lists every article this sweep proved dead: the one that
// broke the margin plus the ones absorbed before it.
func (e *SweepDeadArticleError) DeadMessageIDs() []string {
	ids := make([]string, 0, len(e.Absorbed)+1)
	ids = append(ids, e.MessageID)
	for _, id := range e.Absorbed {
		if id != e.MessageID {
			ids = append(ids, id)
		}
	}
	return ids
}

func (e *SweepDeadArticleError) Error() string {
	if len(e.Absorbed) > 0 {
		return fmt.Sprintf("par2repair: article %s vanished during sweep (with %d more absorbed this sweep): %v",
			e.MessageID, len(e.Absorbed), e.Err)
	}
	return fmt.Sprintf("par2repair: article %s vanished during sweep: %v", e.MessageID, e.Err)
}

func (e *SweepDeadArticleError) Unwrap() error { return e.Err }

// Stage identifies a phase of a repair job for progress reporting.
type Stage string

const (
	// StageChecking is the pre-plan liveness check: STATing release articles
	// to learn which are dead before building the plan.
	StageChecking Stage = "checking"
	// StagePlanning is the resolve work after the liveness check: parsing the
	// PAR2 set and matching recovery-set members to NZB files. It has no
	// meaningful unit count (done/total are zero); reporting it keeps the UI
	// off the finished liveness numbers while minutes of fetches run.
	StagePlanning Stage = "planning"
	// StageDownloading is the recovery payload download that precedes a sweep.
	StageDownloading Stage = "downloading"
	// StageRepairing is the verification sweep that streams the release and
	// folds present slices into the solver.
	StageRepairing Stage = "repairing"
)

// JobProgress receives job progress: units processed so far out of the
// stage's total (articles STATed, recovery slices downloaded, or release
// articles swept, depending on the stage). Called from the job goroutine;
// must be fast.
type JobProgress func(stage Stage, done, total int)

// JobOption customizes RunJob.
type JobOption func(*jobOptions)

type jobOptions struct {
	progress      JobProgress
	concurrency   func() int
	streamsActive func() bool
}

// WithProgress reports job progress through cb. A re-sweep (singular-matrix
// or corrupt-slice replan) restarts the count of its stage.
func WithProgress(cb JobProgress) JobOption {
	return func(o *jobOptions) { o.progress = cb }
}

// WithConcurrency bounds how many article fetches the job keeps in flight.
// Values <= 0 keep the default; the depth is capped at maxFetchAhead.
func WithConcurrency(n int) JobOption {
	return WithLiveConcurrency(func() int { return n })
}

// WithLiveConcurrency is WithConcurrency with a live bound (typically the
// configured repair connection count, read from config): the pipeline
// re-reads it as fetches complete, so raising max_connections speeds up a
// job already running. Per-call values <= 0 fall back to the default depth.
func WithLiveConcurrency(get func() int) JobOption {
	return func(o *jobOptions) { o.concurrency = get }
}

// WithYieldToStreams wires a live playback-activity signal (typically the
// stream tracker's count) into the job. While active() reports true, the
// solver's fold runs on a single goroutine so a repair sharing the box with
// playback costs it no CPU. Connections are not throttled here: repair
// fetches ride the pool's background lane, and the pool keeps them behind
// playback and imports itself. The bound is re-read continuously — a stream
// starting mid-sweep narrows the fold at the next slice, and its end restores
// full width without restarting anything.
func WithYieldToStreams(active func() bool) JobOption {
	return func(o *jobOptions) { o.streamsActive = active }
}

// yieldFoldWorkers is the solver fold width while playback streams are
// active. The fold's memory traffic is rows × input rate on every core it
// gets; one goroutine keeps the repair correct while playback keeps the box.
const yieldFoldWorkers = 1

// maxFetchAhead caps the fetch concurrency however large the configured
// connection count is. It is also the pipeline's buffer bound:
// fetched-but-unconsumed payloads are held in memory, so maxFetchAhead x
// article size bounds what a sweep outpacing its consumer can pile up.
const maxFetchAhead = 64

// fetchDepth is the live prefetch depth this job runs with: the configured
// concurrency getter clamped to [1, maxFetchAhead], or the fetchAhead default
// when unset or non-positive.
func (o jobOptions) fetchDepth() func() int {
	return func() int {
		n := fetchAhead
		if o.concurrency != nil {
			if c := o.concurrency(); c > 0 {
				n = c
			}
		}
		return min(n, maxFetchAhead)
	}
}

// foldWorkerLimit is the solver's live fold-width bound for this job: capped
// at yieldFoldWorkers while playback streams are active, unbounded otherwise.
func (o jobOptions) foldWorkerLimit() func() int {
	if o.streamsActive == nil {
		return nil
	}
	return func() int {
		if o.streamsActive() {
			return yieldFoldWorkers
		}
		return 0
	}
}

// maxCorruptReplans bounds how many corrupt present slices a single job may
// reclassify as missing beyond what the plan's margin rows absorb in-sweep.
// Each overflow reclassification grows the solver by one accumulator
// (slice-size bytes) and costs a fresh sweep, so the bound caps both memory
// growth and re-sweep count.
const maxCorruptReplans = 8

// corruptSliceError signals a present slice whose CRC32 didn't match its IFSC
// checksum during the sweep and could not be absorbed on a margin row. Unlike
// SweepDeadArticleError (article vanished), the fetch succeeded but the data
// is bad: the attempt loop reclassifies the slice as missing and retries with
// one more recovery slice.
type corruptSliceError struct {
	global int
	fileID [16]byte
	local  int
}

func (e *corruptSliceError) Error() string {
	return fmt.Sprintf("par2repair: file %x slice %d (global %d) failed CRC32 verification", e.fileID, e.local, e.global)
}

// RunJob executes a repair plan: fetches the chosen recovery slices, sweeps
// every present input slice of the recovery set (verifying each against its
// IFSC CRC32), solves for the missing slices, verifies them against their
// IFSC MD5s and writes the dead articles' payloads to the patch store.
//
// par2Files describes the usenet articles of each PAR2 file, positionally
// matching the streams given to par2.ParseIndex (so RecoverySliceRef.FileIndex
// indexes into it).
//
// Errors wrapping ErrUnrepairable are permanent; anything else is transient
// and worth retrying later.
func RunJob(
	ctx context.Context,
	plan *Plan,
	idx *par2.Index,
	par2Files []SetFile,
	fetch ArticleFetcher,
	store *PatchStore,
	log *slog.Logger,
	opts ...JobOption,
) error {
	var o jobOptions
	for _, opt := range opts {
		opt(&o)
	}
	sliceSize := int64(plan.SliceSize)
	k := len(plan.Missing)

	// Global slice bookkeeping: start slice per file and missing positions.
	startSlice := make([]int64, len(plan.Files))
	var total int64
	for i, f := range plan.Files {
		startSlice[i] = total
		total += (int64(f.Length) + sliceSize - 1) / sliceSize
	}
	missingPos := make(map[int]int, k) // global slice index -> position in missing
	for i, m := range plan.Missing {
		missingPos[m] = i
	}
	missing := slices.Clone(plan.Missing)

	refs := slices.Clone(plan.Recovery)
	spares := slices.Clone(plan.SpareRecovery)

	totalArticles := 0
	for _, f := range plan.Files {
		totalArticles += len(f.Articles)
	}

	// Dead articles discovered mid-sweep, absorbed on margin rows. They are
	// patched alongside the planned ones: their slices join the unknowns and
	// are solved in the same pass.
	var discovered []DeadArticle
	discoveredIDs := map[string]bool{}

	// Slices whose present data failed CRC32 verification. Their articles are
	// patched after the solve — which article inside the slice carried the
	// bad bytes is unknown, so every overlapping article gets a byte-exact
	// payload spliced from recovered slices and refetched wire bytes.
	corruptSlices := map[int]bool{}

	scratch := store.ScratchDir()

	// Attempt loop. Margin rows absorb most surprises inside a single sweep —
	// mid-sweep dead articles, corrupt present slices, singular submatrices
	// (Solve skips dependent rows) — so a retry here is the rare fallback:
	// margin exhausted with spares left, or a matrix no loaded subset can
	// invert. Each attempt refetches the recovery payloads, since the previous
	// attempt's fold consumed them as accumulators.
	corruptReplans := 0
	// On a spill plan each attempt gets a fresh disk arena for its recovery
	// payloads, solver accumulators and recovered slices; fresh file pages
	// arrive zeroed, so a re-sweep never sees a stale accumulator.
	var attemptArena *arena
	defer func() {
		if attemptArena != nil {
			_ = attemptArena.Close()
		}
	}()
	for {
		if attemptArena != nil {
			_ = attemptArena.Close()
			attemptArena = nil
		}
		alloc := heapAlloc
		if plan.SpillToDisk {
			// Payload buffers plus prepared-layout accumulators (one of each
			// per loaded row) plus recovered slices (at most as many, since
			// missing never exceeds the loaded rows).
			accBytes, err := accumulatorArenaBytes(int(sliceSize))
			if err != nil {
				return err
			}
			capacity := int64(len(refs)) * (2*sliceSize + accBytes)
			a, err := newArena(scratch, capacity)
			if err != nil {
				return err
			}
			attemptArena = a
			alloc = attemptArena.alloc
		}

		var payloads [][]byte
		var err error
		refs, payloads, spares, err = loadRecoveryPayloads(
			ctx, fetch, par2Files, refs, spares, len(missing), sliceSize, alloc, plan.SpillToDisk, o.fetchDepth(), o.progress, log)
		if err != nil {
			return err
		}

		exps := make([]uint32, len(refs))
		for i, r := range refs {
			exps[i] = r.Exponent
		}
		solver, err := NewSolverAlloc(missing, exps, plan.SliceSize, alloc)
		if err != nil {
			return err
		}
		solver.WorkerLimit = o.foldWorkerLimit()
		// Attempts are bounded (spares and the corrupt-replan budget), so a
		// deferred close per attempt cannot pile up.
		defer solver.Close()
		// The payload buffers were read for this attempt alone, so they are
		// donated: seeding consumes them into the accumulators' prepared
		// layout and nothing reads them again.
		for i, p := range payloads {
			if err := solver.SeedRecoveryOwning(i, p); err != nil {
				return err
			}
		}

		// Absorb callbacks: a slice discovered dead or corrupt mid-sweep
		// joins the unknowns on a margin row instead of forcing a re-sweep.
		absorbCorrupt := func(global int) bool {
			if solver.AddMissing(global) != nil {
				return false
			}
			missingPos[global] = len(missing)
			missing = append(missing, global)
			corruptSlices[global] = true
			return true
		}
		absorbDead := func(fi, ai int, artOff int64, a Article) bool {
			first := int(startSlice[fi] + artOff/sliceSize)
			last := int(startSlice[fi] + (artOff+a.Size-1)/sliceSize)
			var fresh []int
			for g := first; g <= last; g++ {
				if _, ok := missingPos[g]; !ok {
					fresh = append(fresh, g)
				}
			}
			if len(missing)+len(fresh) > len(refs) {
				return false
			}
			for _, g := range fresh {
				_ = solver.AddMissing(g) // capacity checked above
				missingPos[g] = len(missing)
				missing = append(missing, g)
			}
			if !discoveredIDs[a.MessageID] {
				discoveredIDs[a.MessageID] = true
				discovered = append(discovered, DeadArticle{
					FileIdx: fi, ArtIdx: ai, MessageID: a.MessageID,
					FileStart: artOff, Size: a.Size,
				})
			}
			return true
		}

		doneArticles := 0
		onArticle := func() {
			doneArticles++
			if o.progress != nil {
				o.progress(StageRepairing, doneArticles, totalArticles)
			}
		}
		if err := sweep(ctx, plan, idx, fetch, solver, o.fetchDepth(), startSlice, missingPos, absorbDead, absorbCorrupt, onArticle, log); err != nil {
			var corrupt *corruptSliceError
			if !errors.As(err, &corrupt) {
				// The margin ran out, but this sweep already proved other
				// articles dead. Carry them out with the failure so the retry
				// replans against every death observed here, not just the last
				// one — otherwise each attempt re-reads the whole release to
				// learn a single article.
				var sweepDead *SweepDeadArticleError
				if errors.As(err, &sweepDead) && len(discovered) > 0 {
					for _, d := range discovered {
						if d.MessageID != sweepDead.MessageID {
							sweepDead.Absorbed = append(sweepDead.Absorbed, d.MessageID)
						}
					}
					log.WarnContext(ctx, "sweep margin exhausted; reporting every article proved dead this sweep",
						"breaking_article", sweepDead.MessageID, "absorbed", len(sweepDead.Absorbed))
				}
				return err
			}
			// Margin exhausted: a present-but-corrupt slice is still just
			// another unknown — take one more recovery row from the spares
			// at the cost of a fresh sweep.
			corruptReplans++
			if corruptReplans > maxCorruptReplans {
				return fmt.Errorf("%w: %v and the corrupt-slice replan budget (%d) is exhausted",
					ErrUnrepairable, corrupt, maxCorruptReplans)
			}
			if len(spares) == 0 {
				return fmt.Errorf("%w: %v and no spare recovery slices remain", ErrUnrepairable, corrupt)
			}
			log.WarnContext(ctx, "present slice failed CRC32, reclassifying as missing",
				"global_slice", corrupt.global, "spare_exponent", spares[0].Exponent)
			missing = append(missing, corrupt.global)
			missingPos[corrupt.global] = len(missing) - 1
			corruptSlices[corrupt.global] = true
			refs = append(refs, spares[0])
			spares = spares[1:]
			continue
		}

		if len(missing) == 0 {
			// A verify sweep found the release intact: every slice matched its
			// IFSC CRC32, so there is nothing to patch — and the caller's
			// damage is not article damage. Surface the standard sentinel so
			// a parked import is failed rather than resumed into the same
			// analysis failure again. (Normal plans start with missing slices
			// and can never reach this.)
			return ErrNothingToRepair
		}

		recovered, err := solver.Solve()
		if errors.Is(err, ErrSingularMatrix) {
			// Solve already tried every loaded row: no invertible subset
			// exists. A spare brings a genuinely new exponent into play, at
			// the cost of a fresh sweep.
			if len(spares) == 0 {
				return fmt.Errorf("%w: recovery matrix singular and no spare recovery slices remain", ErrUnrepairable)
			}
			swap := len(refs) - 1
			log.WarnContext(ctx, "recovery matrix singular, retrying with spare recovery slice",
				"dropped_exponent", refs[swap].Exponent, "spare_exponent", spares[0].Exponent)
			refs[swap] = spares[0]
			spares = spares[1:]
			continue
		}
		if err != nil {
			return err
		}

		// Verify every recovered slice against its IFSC MD5 before storing.
		if err := verifyRecovered(plan, idx, recovered, startSlice, missingPos); err != nil {
			return err
		}

		if err := emitPatches(plan, discovered, recovered, sliceSize, startSlice, missingPos, store, log); err != nil {
			return err
		}
		return emitCorruptArticlePatches(ctx, plan, corruptSlices, discoveredIDs, recovered,
			sliceSize, startSlice, missingPos, fetch, store, log)
	}
}

// emitCorruptArticlePatches stores byte-exact payloads for every article
// overlapping a corrupt slice. Slice CRC is the only corruption signal — which
// article inside the slice carried the bad bytes is unknown — so all
// overlapping articles are patched. Ranges covered by solved slices come from
// recovered data; ranges inside verified-good slices keep their wire bytes,
// refetched once per article. Articles already patched as dead are skipped.
// Failures to patch one article are logged, never fatal: the solve itself
// succeeded and the remaining patches are still worth keeping.
func emitCorruptArticlePatches(
	ctx context.Context,
	plan *Plan,
	corrupt map[int]bool,
	alreadyPatched map[string]bool,
	recovered [][]byte,
	sliceSize int64,
	startSlice []int64,
	missingPos map[int]int,
	fetch ArticleFetcher,
	store *PatchStore,
	log *slog.Logger,
) error {
	if len(corrupt) == 0 {
		return nil
	}
	patched := map[string]bool{}
	for _, da := range plan.DeadArticles {
		patched[da.MessageID] = true
	}
	for id := range alreadyPatched {
		patched[id] = true
	}

	globals := make([]int, 0, len(corrupt))
	for g := range corrupt {
		globals = append(globals, g)
	}
	sort.Ints(globals)

	for _, g := range globals {
		fi := fileForSlice(startSlice, g)
		f := plan.Files[fi]
		sliceStart := (int64(g) - startSlice[fi]) * sliceSize
		sliceEnd := sliceStart + sliceSize

		var artOff int64
		for _, a := range f.Articles {
			artEnd := artOff + a.Size
			overlaps := artEnd > sliceStart && artOff < sliceEnd
			if overlaps && !patched[a.MessageID] {
				patched[a.MessageID] = true
				if err := patchCorruptArticle(ctx, fi, a, artOff, recovered,
					sliceSize, startSlice, missingPos, fetch, store); err != nil {
					log.WarnContext(ctx, "failed to patch article overlapping corrupt slice; leaving it unpatched",
						"message_id", a.MessageID, "error", err)
				} else {
					log.InfoContext(ctx, "stored repaired payload for corrupt article",
						"message_id", a.MessageID, "bytes", a.Size)
				}
			}
			artOff = artEnd
		}
	}
	return nil
}

// patchCorruptArticle assembles one article's true payload: recovered bytes
// where a slice was solved, wire bytes (one refetch) where the slice verified
// good, then stores it as the article's patch.
func patchCorruptArticle(
	ctx context.Context,
	fi int,
	a Article,
	artOff int64,
	recovered [][]byte,
	sliceSize int64,
	startSlice []int64,
	missingPos map[int]int,
	fetch ArticleFetcher,
	store *PatchStore,
) error {
	payload := make([]byte, a.Size)
	var wire []byte
	first := startSlice[fi] + artOff/sliceSize
	last := startSlice[fi] + (artOff+a.Size-1)/sliceSize
	for g := first; g <= last; g++ {
		sliceFileStart := (g - startSlice[fi]) * sliceSize
		from := max(sliceFileStart, artOff)
		to := min(sliceFileStart+sliceSize, artOff+a.Size)
		if pos, ok := missingPos[int(g)]; ok {
			copy(payload[from-artOff:to-artOff], recovered[pos][from-sliceFileStart:to-sliceFileStart])
			continue
		}
		if wire == nil {
			data, err := fetch.Fetch(ctx, a.MessageID)
			if err != nil {
				return fmt.Errorf("refetch wire bytes: %w", err)
			}
			if int64(len(data)) != a.Size {
				return fmt.Errorf("refetched article is %d bytes, want %d", len(data), a.Size)
			}
			wire = data
		}
		copy(payload[from-artOff:to-artOff], wire[from-artOff:to-artOff])
	}
	if err := store.Put(a.MessageID, payload); err != nil {
		return fmt.Errorf("store patch: %w", err)
	}
	return nil
}

// loadRecoveryPayloads fetches the payload of every recovery ref, swapping in
// spares for refs whose backing articles are dead — or, with no spares left,
// dropping the ref (margin rows exist to be expendable). Refs are visited in
// (file, offset) order so their backing articles stream through a bounded
// prefetch window instead of piling up on the heap. Returned payloads are
// exclusively owned — fresh buffers, or arena-backed when spill is set — and
// exactly one slice long, ready to donate to the solver.
func loadRecoveryPayloads(
	ctx context.Context,
	fetch ArticleFetcher,
	par2Files []SetFile,
	refs, spares []par2.RecoverySliceRef,
	minRows int,
	sliceSize int64,
	alloc bufAlloc,
	spill bool,
	depth func() int,
	progress JobProgress,
	log *slog.Logger,
) ([]par2.RecoverySliceRef, [][]byte, []par2.RecoverySliceRef, error) {
	order := make([]int, len(refs))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(x, y int) bool {
		a, b := refs[order[x]], refs[order[y]]
		if a.FileIndex != b.FileIndex {
			return a.FileIndex < b.FileIndex
		}
		return a.BodyOffset < b.BodyOffset
	})

	payloads := make([][]byte, len(refs))
	kept := make([]bool, len(refs))
	if progress != nil {
		progress(StageDownloading, 0, len(refs))
	}
	loaded := 0
	feed := newArticleFeed(ctx, fetch, recoveryArticleIDs(par2Files, refs, order, sliceSize), depth)
	defer feed.stop()
	for _, i := range order {
		for {
			ref := refs[i]
			data, err := readRangeFrom(feed.get, par2Files[ref.FileIndex], ref.BodyOffset, sliceSize)
			if err == nil && !recoveryPayloadIntact(ref, data) {
				// A corrupt recovery payload poisons every slice the solver
				// rebuilds, and the damage only surfaces after the whole
				// release has been swept. Caught against the RecvSlic packet's
				// own MD5, it is just an unreachable slice by another name.
				err = fmt.Errorf("payload does not match its packet MD5: %w", nntppool.ErrArticleNotFound)
			}
			if err != nil {
				if errors.Is(err, nntppool.ErrArticleNotFound) {
					if len(spares) > 0 {
						log.WarnContext(ctx, "recovery slice unreachable or corrupt, swapping in spare",
							"exponent", ref.Exponent, "spare_exponent", spares[0].Exponent, "reason", err)
						refs[i] = spares[0]
						spares = spares[1:]
						continue
					}
					log.WarnContext(ctx, "recovery slice unreachable or corrupt, dropping margin row",
						"exponent", ref.Exponent, "reason", err)
					break
				}
				return nil, nil, nil, fmt.Errorf("par2repair: fetch recovery slice exponent %d: %w", ref.Exponent, err)
			}
			if spill {
				buf, aerr := alloc(len(data))
				if aerr != nil {
					return nil, nil, nil, aerr
				}
				copy(buf, data)
				data = buf
			}
			payloads[i] = data
			kept[i] = true
			break
		}
		loaded++
		if progress != nil {
			progress(StageDownloading, loaded, len(refs))
		}
	}

	outRefs := make([]par2.RecoverySliceRef, 0, len(refs))
	outPayloads := make([][]byte, 0, len(refs))
	for i, ok := range kept {
		if ok {
			outRefs = append(outRefs, refs[i])
			outPayloads = append(outPayloads, payloads[i])
		}
	}
	if len(outRefs) < minRows {
		return nil, nil, nil, fmt.Errorf("%w: %d slice(s) missing but only %d recovery slice(s) reachable",
			ErrUnrepairable, minRows, len(outRefs))
	}
	return outRefs, outPayloads, spares, nil
}

// recoveryPayloadIntact verifies a fetched recovery payload against its
// RecvSlic packet MD5, which covers everything after the header's hash field:
// set ID, packet type, exponent and payload. Refs without a recorded hash
// (hand-built rather than parsed) pass unchecked.
func recoveryPayloadIntact(ref par2.RecoverySliceRef, payload []byte) bool {
	if ref.PacketMD5 == ([16]byte{}) {
		return true
	}
	sum := md5.New()
	sum.Write(ref.SetID[:])
	sum.Write(par2.PacketTypeRecoverySlice[:])
	var exp [4]byte
	binary.LittleEndian.PutUint32(exp[:], ref.Exponent)
	sum.Write(exp[:])
	sum.Write(payload)
	return [16]byte(sum.Sum(nil)) == ref.PacketMD5
}

// sweep streams every article of the recovery set in order, assembles input
// slices, verifies present slices against IFSC CRC32 and folds them into the
// solver. Slices touched by dead articles are skipped (they are the unknowns).
//
// Surprises are first offered to the absorb callbacks, which reclassify the
// affected slices as missing on the solver's margin rows: absorbDead for an
// article that died between planning and sweeping, absorbCorrupt for a slice
// whose CRC32 does not match. Only when a callback declines (margin
// exhausted) does the sweep fail with the corresponding typed error.
func sweep(
	ctx context.Context,
	plan *Plan,
	idx *par2.Index,
	fetch ArticleFetcher,
	solver *Solver,
	depth func() int,
	startSlice []int64,
	missingPos map[int]int,
	absorbDead func(fi, ai int, artOff int64, a Article) bool,
	absorbCorrupt func(global int) bool,
	onArticle func(),
	log *slog.Logger,
) error {
	sliceSize := plan.SliceSize

	// One pipeline over every article of the recovery set, so the prefetch
	// stays full across file boundaries instead of draining at each one.
	var all []Article
	for _, f := range plan.Files {
		all = append(all, f.Articles...)
	}
	slots, stop := prefetchArticles(ctx, fetch, all, depth)
	defer stop()

	buf := make([]byte, sliceSize)
	for fi, f := range plan.Files {
		checks := idx.SliceChecks[f.FileID]
		fill := 0
		local := 0

		completeSlice := func() error {
			global := int(startSlice[fi]) + local
			if _, isMissing := missingPos[global]; !isMissing {
				if local < len(checks) && crc32.ChecksumIEEE(buf) != checks[local].CRC32 {
					// Present but corrupt: another unknown, absorbed on a
					// margin row — or, with none left, signalled to the
					// attempt loop for a replan.
					if !absorbCorrupt(global) {
						return &corruptSliceError{global: global, fileID: f.FileID, local: local}
					}
					log.WarnContext(ctx, "present slice failed CRC32, absorbed as missing on a margin row",
						"global_slice", global)
				} else {
					solver.FoldPresent(global, buf)
				}
			}
			local++
			fill = 0
			return nil
		}

		feed := func(data []byte) error {
			for len(data) > 0 {
				n := copy(buf[fill:], data)
				fill += n
				data = data[n:]
				if fill == sliceSize {
					if err := completeSlice(); err != nil {
						return err
					}
				}
			}
			return nil
		}

		var artOff int64
		for ai, a := range f.Articles {
			if err := ctx.Err(); err != nil {
				return err
			}
			if onArticle != nil {
				onArticle()
			}
			if a.Dead {
				// Zero-advance: the affected slices are in the missing set and
				// will be skipped by completeSlice.
				if err := feed(make([]byte, a.Size)); err != nil {
					return err
				}
				artOff += a.Size
				continue
			}
			slot, ok := <-slots
			if !ok {
				// The prefetcher only closes early when the context ended.
				return ctx.Err()
			}
			res := <-slot
			if res.err != nil {
				if errors.Is(res.err, nntppool.ErrArticleNotFound) {
					// An article that died between planning and sweeping:
					// absorbed on margin rows when possible, and patched with
					// the planned dead articles. Otherwise surface a typed
					// error so the service persists the discovery and the
					// retry's plan includes it in the missing set.
					if absorbDead(fi, ai, artOff, a) {
						log.WarnContext(ctx, "article died mid-sweep, absorbed on margin rows",
							"message_id", a.MessageID)
						if err := feed(make([]byte, a.Size)); err != nil {
							return err
						}
						artOff += a.Size
						continue
					}
					return &SweepDeadArticleError{MessageID: a.MessageID, Err: res.err}
				}
				return fmt.Errorf("par2repair: fetch article %s: %w", a.MessageID, res.err)
			}
			if int64(len(res.data)) != a.Size {
				return fmt.Errorf("%w: article %s decoded to %d bytes, expected %d", ErrUnrepairable, a.MessageID, len(res.data), a.Size)
			}
			if err := feed(res.data); err != nil {
				return err
			}
			artOff += a.Size
		}
		// Final partial slice: a full slice overwrites every byte of buf, so
		// only this tail ever needs the zero padding PAR2 defines.
		if fill > 0 {
			clear(buf[fill:])
			if err := completeSlice(); err != nil {
				return err
			}
		}
		log.DebugContext(ctx, "swept recovery-set file", "file_index", fi, "slices", local)
	}
	return nil
}

// recoveryArticleIDs lists, in consumption order, every distinct article
// backing the recovery slices refs, visited in the given order.
func recoveryArticleIDs(par2Files []SetFile, refs []par2.RecoverySliceRef, order []int, sliceSize int64) []string {
	seen := map[string]bool{}
	var ids []string
	for _, i := range order {
		ref := refs[i]
		f := par2Files[ref.FileIndex]
		var artStart int64
		for _, a := range f.Articles {
			artEnd := artStart + a.Size
			if artEnd > ref.BodyOffset && artStart < ref.BodyOffset+sliceSize && !seen[a.MessageID] {
				seen[a.MessageID] = true
				ids = append(ids, a.MessageID)
			}
			artStart = artEnd
			if artStart >= ref.BodyOffset+sliceSize {
				break
			}
		}
	}
	return ids
}

// feedRetain bounds how many consumed articles a feed keeps cached: enough
// for payloads straddling article boundaries and adjacent refs sharing a
// boundary article, without growing with the recovery set.
const feedRetain = 8

// articleFeed streams a known-in-advance article sequence through the bounded
// prefetch pipeline. get serves prefetched payloads for upcoming sequence ids
// and falls back to a direct fetch for ids outside the sequence (spare
// recovery slices swapped in after planning). Not safe for concurrent use.
type articleFeed struct {
	ctx    context.Context
	fetch  ArticleFetcher
	ids    []string
	posOf  map[string]int
	next   int
	have   map[string][]byte
	order  []string
	slots  <-chan chan fetchResult
	cancel func()
}

func newArticleFeed(ctx context.Context, fetch ArticleFetcher, ids []string, depth func() int) *articleFeed {
	arts := make([]Article, len(ids))
	posOf := make(map[string]int, len(ids))
	for i, id := range ids {
		arts[i] = Article{MessageID: id}
		posOf[id] = i
	}
	slots, cancel := prefetchArticles(ctx, fetch, arts, depth)
	return &articleFeed{
		ctx: ctx, fetch: fetch, ids: ids, posOf: posOf,
		have: map[string][]byte{}, slots: slots, cancel: cancel,
	}
}

// get returns one article's payload, popping the prefetch pipeline forward to
// the article's position in the sequence.
func (f *articleFeed) get(id string) ([]byte, error) {
	if data, ok := f.have[id]; ok {
		return data, nil
	}
	pos, ok := f.posOf[id]
	if !ok || pos < f.next {
		// Outside the planned sequence (a spare swap) or already evicted.
		return f.fetch.Fetch(f.ctx, id)
	}
	for f.next <= pos {
		slot, ok := <-f.slots
		if !ok {
			// The prefetcher only closes early when the context ended.
			if err := f.ctx.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("par2repair: article feed exhausted before %s", id)
		}
		res := <-slot
		cur := f.ids[f.next]
		f.next++
		if res.err != nil {
			if cur == id {
				return nil, res.err
			}
			continue // an id the caller ended up not needing (spare swap)
		}
		f.retain(cur, res.data)
	}
	return f.have[id], nil
}

func (f *articleFeed) retain(id string, data []byte) {
	f.have[id] = data
	f.order = append(f.order, id)
	for len(f.order) > feedRetain {
		delete(f.have, f.order[0])
		f.order = f.order[1:]
	}
}

// stop cancels outstanding prefetches. Idempotent.
func (f *articleFeed) stop() { f.cancel() }

// fetchResult is one prefetched article payload (or its fetch error).
type fetchResult struct {
	data []byte
	err  error
}

// prefetchArticles pipelines fetches of the live articles in arts, preserving
// order: the returned channel yields one single-use result channel per live
// article, in article order. depth is read live: at most depth() fetches are
// in flight at once, re-evaluated as fetches complete, so a raised repair
// connection count speeds up a pipeline already running. Completed payloads
// buffered ahead of the consumer are bounded by maxFetchAhead. stop cancels
// outstanding fetches; callers must call it once done (early return or normal
// completion).
func prefetchArticles(ctx context.Context, fetch ArticleFetcher, arts []Article, depth func() int) (slots <-chan chan fetchResult, stop func()) {
	pctx, cancel := context.WithCancel(ctx)
	out := make(chan chan fetchResult, maxFetchAhead)
	// The limiter bounds concurrent fetches at the live depth (clamped to
	// [1, maxFetchAhead]); blocked acquires re-check it on every release.
	lim := NewConnLimiter(func() int {
		d := depth()
		if d <= 0 {
			d = fetchAhead
		}
		return min(d, maxFetchAhead)
	})
	go func() {
		defer close(out)
		for _, a := range arts {
			if a.Dead {
				continue
			}
			release, err := lim.Acquire(pctx)
			if err != nil {
				return
			}
			slot := make(chan fetchResult, 1)
			go func(id string) {
				data, err := fetch.Fetch(pctx, id)
				release()
				slot <- fetchResult{data: data, err: err}
			}(a.MessageID)
			select {
			case out <- slot:
			case <-pctx.Done():
				return
			}
		}
	}()
	return out, cancel
}

// verifyRecovered checks every recovered slice against its IFSC MD5.
func verifyRecovered(plan *Plan, idx *par2.Index, recovered [][]byte, startSlice []int64, missingPos map[int]int) error {
	for global, pos := range missingPos {
		fi := fileForSlice(startSlice, global)
		local := global - int(startSlice[fi])
		checks := idx.SliceChecks[plan.Files[fi].FileID]
		if local >= len(checks) {
			return fmt.Errorf("%w: recovered slice %d outside IFSC range", ErrUnrepairable, global)
		}
		if md5.Sum(recovered[pos]) != checks[local].MD5 {
			return fmt.Errorf("%w: recovered slice %d failed MD5 verification", ErrUnrepairable, global)
		}
	}
	return nil
}

// emitPatches cuts recovered slices back into dead-article payloads and
// stores them atomically.
//
// Only dead articles — the plan's, plus any discovered mid-sweep — get
// patches. Corrupt-but-present slices reclassified during the job are solved
// for (as unknowns) but deliberately NOT patched: their articles are still
// served fine by providers, and the read path only consults the patch store
// when an article is MISSING — a patch for a present article would never be
// read.
func emitPatches(plan *Plan, discovered []DeadArticle, recovered [][]byte, sliceSize int64, startSlice []int64, missingPos map[int]int, store *PatchStore, log *slog.Logger) error {
	deadArticles := make([]DeadArticle, 0, len(plan.DeadArticles)+len(discovered))
	deadArticles = append(deadArticles, plan.DeadArticles...)
	deadArticles = append(deadArticles, discovered...)
	for _, da := range deadArticles {
		payload := make([]byte, da.Size)
		first := startSlice[da.FileIdx] + da.FileStart/sliceSize
		last := startSlice[da.FileIdx] + (da.FileStart+da.Size-1)/sliceSize
		for g := first; g <= last; g++ {
			pos, ok := missingPos[int(g)]
			if !ok {
				return fmt.Errorf("par2repair: internal error: slice %d for article %s not in missing set", g, da.MessageID)
			}
			sliceFileStart := (g - startSlice[da.FileIdx]) * sliceSize
			// Overlap of [sliceFileStart, sliceFileStart+sliceSize) with the
			// article's [da.FileStart, da.FileStart+da.Size).
			from := max(sliceFileStart, da.FileStart)
			to := min(sliceFileStart+sliceSize, da.FileStart+da.Size)
			copy(payload[from-da.FileStart:to-da.FileStart], recovered[pos][from-sliceFileStart:to-sliceFileStart])
		}
		if err := store.Put(da.MessageID, payload); err != nil {
			return fmt.Errorf("par2repair: store patch for %s: %w", da.MessageID, err)
		}
		log.InfoContext(context.Background(), "stored repaired article payload",
			"message_id", da.MessageID, "bytes", len(payload))
	}
	return nil
}

// fileForSlice returns the index of the plan file containing the global slice.
func fileForSlice(startSlice []int64, global int) int {
	for i := len(startSlice) - 1; i >= 0; i-- {
		if int64(global) >= startSlice[i] {
			return i
		}
	}
	return 0
}

// readRangeFrom reads [off, off+n) of a file's concatenated article payloads
// through get (typically an articleFeed).
func readRangeFrom(get func(string) ([]byte, error), f SetFile, off, n int64) ([]byte, error) {
	out := make([]byte, n)
	var artStart int64
	for _, a := range f.Articles {
		artEnd := artStart + a.Size
		if artEnd > off && artStart < off+n {
			data, err := get(a.MessageID)
			if err != nil {
				return nil, err
			}
			from := max(off, artStart)
			to := min(off+n, artEnd)
			// A truncated payload (shorter than the article's declared size)
			// leaves its tail as zeros instead of panicking; the checksum
			// layers above reject whatever the gap damaged.
			if srcFrom := from - artStart; srcFrom < int64(len(data)) {
				copy(out[from-off:to-off], data[srcFrom:min(int64(len(data)), to-artStart)])
			}
		}
		artStart = artEnd
		if artStart >= off+n {
			break
		}
	}
	return out, nil
}
