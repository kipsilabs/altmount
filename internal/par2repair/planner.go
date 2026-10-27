package par2repair

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
)

// ErrUnrepairable marks damage that PAR2 repair cannot or may not fix:
// insufficient recovery slices, policy caps exceeded, or failed verification.
// The caller routes such files to the existing ARR/corruption path.
var ErrUnrepairable = errors.New("par2repair: unrepairable")

// ErrNothingToRepair is returned when no article in the recovery set is dead.
var ErrNothingToRepair = errors.New("par2repair: nothing to repair")

// planMargin is how many recovery rows a plan carries beyond the damage it
// found. The sweep reads every present slice once, and a slice the plan
// thought present can still turn out dead or corrupt; margin rows let those
// be solved without a second full read of the release.
const planMargin = 8

// Article is one usenet article of a recovery-set file.
type Article struct {
	MessageID string
	Size      int64 // decoded payload size
	Dead      bool  // confirmed missing on all providers
}

// SetFile is one recovery-set member resolved to its usenet articles, whose
// concatenated payloads are exactly the file's bytes.
type SetFile struct {
	FileID   [16]byte
	Length   uint64
	Articles []Article
}

// DeadArticle locates one dead article within the recovery set.
type DeadArticle struct {
	FileIdx   int // index into Plan.Files
	ArtIdx    int
	MessageID string
	FileStart int64 // byte offset of the article within its file
	Size      int64
}

// Caps is the repair policy: how much damage a job may fix.
type Caps struct {
	// RecoveryMargin overrides the baseline spare rows; nil or negative keeps eight.
	RecoveryMargin *int
	// MaxRepairRatio caps missing bytes over the damaged files' total bytes.
	MaxRepairRatio float64
	// MaxMemoryBytes bounds the solver's in-heap accumulator memory (k × slice
	// size). Jobs over the bound still run, backed by a disk arena.
	MaxMemoryBytes int64
	// MinReleaseSizeBytes / MaxReleaseSizeBytes bound the size of releases a
	// repair may take on (content bytes, PAR2 files excluded). A repair
	// downloads the whole release once, so these let users skip releases too
	// large to be worth the bandwidth or too small to bother with. Zero means
	// unbounded on that side.
	MinReleaseSizeBytes int64
	MaxReleaseSizeBytes int64
	// ExpectedHiddenArticles is the liveness sample's estimate of dead
	// articles the plan cannot see (the sample found damage but the full
	// release was not STATed). BuildPlan provisions extra margin rows for
	// them so the payload sweep absorbs the discoveries without a replan.
	ExpectedHiddenArticles int
	// VerifySweep builds a plan even when no article is known dead: the
	// trigger knows the release is damaged (corrupt-but-present articles
	// broke import analysis) but not which articles. The plan then carries
	// no missing slices and margin recovery rows only; the job's CRC sweep
	// locates the corrupt slices and absorbs them onto the margin.
	VerifySweep bool
}

// Plan is everything a repair job needs to run: which slices to reconstruct,
// which recovery slices to use, and the article layout of the recovery set.
type Plan struct {
	SliceSize     int
	GlobalSlices  int
	Missing       []int // global slice indices, ascending
	Recovery      []par2.RecoverySliceRef
	SpareRecovery []par2.RecoverySliceRef
	Files         []SetFile // recovery-set order (FileID ascending)
	DeadArticles  []DeadArticle
	// SpillToDisk marks a plan whose solver buffers exceed the memory budget:
	// the job backs accumulators, recovery payloads and recovered slices with
	// a memory-mapped scratch file instead of the heap.
	SpillToDisk bool
}

// BuildPlan maps dead articles to global recovery-set slices and selects
// recovery slices, enforcing the caps. files may arrive in any order but must
// contain every recovery-set member of idx.
func BuildPlan(idx *par2.Index, files []SetFile, caps Caps) (*Plan, error) {
	if idx.SliceSize == 0 || idx.SliceSize%4 != 0 {
		return nil, fmt.Errorf("par2repair: invalid slice size %d", idx.SliceSize)
	}
	sliceSize := int64(idx.SliceSize)

	byID := make(map[[16]byte]SetFile, len(files))
	for _, f := range files {
		byID[f.FileID] = f
	}

	plan := &Plan{SliceSize: int(sliceSize)}
	var startSlice []int64
	var total int64
	for _, id := range idx.RecoveryIDs {
		f, ok := byID[id]
		if !ok {
			fd := idx.Files[id]
			return nil, fmt.Errorf("par2repair: recovery-set member %q (%x) not resolved to usenet articles", fd.Name, id)
		}
		if sum := articleSizeSum(f.Articles); sum != int64(f.Length) {
			return nil, fmt.Errorf("par2repair: file %x: article sizes sum to %d, want %d", id, sum, f.Length)
		}
		plan.Files = append(plan.Files, f)
		startSlice = append(startSlice, total)
		total += (int64(f.Length) + sliceSize - 1) / sliceSize
	}
	plan.GlobalSlices = int(total)

	// Map dead articles to global slice indices.
	missingSet := map[int]bool{}
	var damagedFileBytes, missingArticleBytes int64
	damagedFiles := map[int]bool{}
	for fi, f := range plan.Files {
		var off int64
		for ai, a := range f.Articles {
			if a.Dead {
				first := startSlice[fi] + off/sliceSize
				last := startSlice[fi] + (off+a.Size-1)/sliceSize
				for s := first; s <= last; s++ {
					missingSet[int(s)] = true
				}
				plan.DeadArticles = append(plan.DeadArticles, DeadArticle{
					FileIdx: fi, ArtIdx: ai, MessageID: a.MessageID,
					FileStart: off, Size: a.Size,
				})
				missingArticleBytes += a.Size
				if !damagedFiles[fi] {
					damagedFiles[fi] = true
					damagedFileBytes += int64(f.Length)
				}
			}
			off += a.Size
		}
	}
	if len(missingSet) == 0 && !caps.VerifySweep {
		return nil, ErrNothingToRepair
	}
	for s := range missingSet {
		plan.Missing = append(plan.Missing, s)
	}
	sort.Ints(plan.Missing)

	// Caps. Solver memory scales with whole slices, but the damage ratio must
	// count the bytes actually missing: releases routinely post PAR2 slices far
	// larger than their articles (a 27 MB slice over 1 MB articles is typical),
	// so one dead article marks a whole slice missing. Charging the ratio for
	// the slice would overstate the damage by the slice/article factor and
	// reject releases that are barely damaged.
	k := len(plan.Missing)
	solverBytes := int64(k) * sliceSize
	plan.SpillToDisk = caps.MaxMemoryBytes > 0 && solverBytes > caps.MaxMemoryBytes
	if caps.MaxRepairRatio > 0 && damagedFileBytes > 0 {
		if ratio := float64(missingArticleBytes) / float64(damagedFileBytes); ratio > caps.MaxRepairRatio {
			return nil, fmt.Errorf("%w: damage ratio %.4f exceeds max_repair_ratio %.4f",
				ErrUnrepairable, ratio, caps.MaxRepairRatio)
		}
	}
	if len(idx.Recovery) < k {
		return nil, fmt.Errorf("%w: needs %d recovery slices, set has %d",
			ErrUnrepairable, k, len(idx.Recovery))
	}

	// Choose the k lowest-exponent recovery slices plus up to planMargin
	// margin rows; the rest are spares. Margin rows are folded alongside the
	// chosen ones, so a slice discovered dead or corrupt mid-sweep is absorbed
	// without a second full read of the release, and a singular matrix is
	// retried by row selection instead of a re-sweep. Margin never pushes an
	// in-memory job over the budget and into disk spill — it shrinks first;
	// a plan that spills anyway keeps the full margin (the arena absorbs it).
	recs := make([]par2.RecoverySliceRef, len(idx.Recovery))
	copy(recs, idx.Recovery)
	sort.Slice(recs, func(i, j int) bool { return recs[i].Exponent < recs[j].Exponent })
	margin := planMargin
	if caps.RecoveryMargin != nil && *caps.RecoveryMargin >= 0 {
		margin = min(*caps.RecoveryMargin, len(recs)-k)
	}
	// Verify-only sweeps need insurance to discover damage before they can solve.
	if caps.VerifySweep {
		margin = max(margin, planMargin)
	}
	m := min(margin+hiddenMarginRows(plan.Files, sliceSize, caps.ExpectedHiddenArticles), len(recs)-k)
	if !plan.SpillToDisk && caps.MaxMemoryBytes > 0 {
		for m > 0 && int64(k+m)*sliceSize > caps.MaxMemoryBytes {
			m--
		}
	}
	plan.Recovery = recs[:k+m]
	plan.SpareRecovery = recs[k+m:]

	return plan, nil
}

// maxHiddenMarginRows caps the extra margin the hidden-damage estimate may
// add. Margin rows are fetched recovery payloads — one slice of download
// each — so a sampling estimate must not be able to demand unbounded rows.
// Sized to what maxHiddenAbsorbArticles articles typically span.
const maxHiddenMarginRows = 128

// hiddenMarginRows converts a hidden-dead-article estimate into extra margin
// rows: the slices those articles are expected to touch, plus headroom for
// the estimate's sampling error (~2 standard deviations, Poisson-flavored).
func hiddenMarginRows(files []SetFile, sliceSize int64, hiddenArticles int) int {
	if hiddenArticles <= 0 {
		return 0
	}
	var articles, spanned int64
	for _, f := range files {
		for _, a := range f.Articles {
			articles++
			spanned += a.Size/sliceSize + 1
		}
	}
	if articles == 0 {
		return 0
	}
	expected := float64(hiddenArticles) * float64(spanned) / float64(articles)
	rows := int(math.Ceil(expected + 2*math.Sqrt(expected)))
	return min(rows, maxHiddenMarginRows)
}

func articleSizeSum(arts []Article) int64 {
	var sum int64
	for _, a := range arts {
		sum += a.Size
	}
	return sum
}
