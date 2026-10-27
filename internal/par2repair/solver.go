// Package par2repair reconstructs missing usenet articles from a release's
// PAR2 recovery data, streaming the recovery set from NNTP and persisting
// repaired article payloads to a local patch store.
package par2repair

import (
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"unsafe"

	"github.com/javi11/gopar-turbo/gf16"
	"github.com/javi11/gopar-turbo/gf2p16"
)

// ErrSingularMatrix is returned by Solve when no invertible subset of the
// loaded recovery rows exists — a known flaw of the PAR2 Vandermonde
// construction. Retrying with a different recovery-slice subset may succeed.
var ErrSingularMatrix = errors.New("par2repair: recovery matrix is singular")

// Solver reconstructs missing slices from a single streaming pass over the
// present slices plus the loaded recovery slices.
//
// Math: recovery slice r with exponent e_r satisfies
//
//	R_r = Σ_{j present} g_j^{e_r}·D_j ⊕ Σ_{m missing} g_m^{e_r}·D_m
//
// Each accumulator is seeded with R_r (AddRecovery or SeedRecoveryOwning) and
// every present slice is folded in (FoldPresent); what remains is the
// missing-only combination, solved as a k×k system in Solve.
//
// The GF(2^16) region arithmetic runs on the gf16 backend (ParPar SIMD
// kernels under cgo, pure Go otherwise). Accumulators live in the backend's
// prepared layout for the whole fold and are untransformed once, in Solve.
//
// More recovery rows than missing slices may be loaded (margin rows): every
// row is folded, and a slice discovered missing mid-fold joins the unknowns
// via AddMissing without a second pass. Solve then uses as many rows as there
// are unknowns, choosing a linearly independent subset.
//
// Not safe for concurrent use: all calls must come from one goroutine.
// Close releases the backend contexts when the solver is done.
type Solver struct {
	missing   []int
	exps      []uint32
	acc       [][]byte // prepared-layout accumulators, one per exps entry; nil until seeded
	sliceSize int
	alloc     bufAlloc
	workers   int

	// WorkerLimit, when set, is read live before each fold and caps how many
	// goroutines FoldPresent uses. The fold is an all-cores memory-bandwidth
	// workload proportional to rows × release size; a repair running beside
	// active playback throttles here so streaming is not starved of CPU.
	// Non-positive values (and values above the full width) mean no limit.
	// Set before folding begins; not synchronized against concurrent folds.
	WorkerLimit func() int

	ctx          *gf16.Context   // primary context: Prepare/Finish and single-threaded folds
	wctx         []*gf16.Context // per-worker contexts for the parallel fold (lazy)
	prep         []byte          // first prepared-input buffer
	prepared     [][]byte
	coefficients [][]uint16
	tasks        chan foldTask
	workerWG     sync.WaitGroup
}

// bufAlloc returns a zeroed buffer of n bytes. The heap allocator is the
// default; disk-backed jobs pass an arena allocator instead.
type bufAlloc func(n int) ([]byte, error)

func heapAlloc(n int) ([]byte, error) { return make([]byte, n), nil }

// minParallelFold is the slice size below which folding stays on one
// goroutine. Splitting a small slice costs more in scheduling than the fold
// itself takes.
const minParallelFold = 128 << 10

// accAlign is the alignment given to accumulator buffers carved out of the
// caller's allocator. The gf16 SIMD kernels require aligned prepared buffers;
// page alignment satisfies every backend.
const accAlign = 4096

// NewSolver prepares a solver for the given missing global slice indices and
// the exponents of the recovery slices that will seed the accumulators — at
// least one per missing slice; extra rows are margin for slices discovered
// missing after folding began.
func NewSolver(missingIdx []int, recoveryExp []uint32, sliceSize int) (*Solver, error) {
	return NewSolverAlloc(missingIdx, recoveryExp, sliceSize, heapAlloc)
}

// NewSolverAlloc is NewSolver with the accumulator (and recovered-slice)
// buffers coming from alloc, which must return zeroed memory.
func NewSolverAlloc(missingIdx []int, recoveryExp []uint32, sliceSize int, alloc bufAlloc) (*Solver, error) {
	// Zero missing slices is valid: a verify sweep starts with no known
	// damage and grows the unknowns via AddMissing as corrupt slices surface.
	if len(recoveryExp) < len(missingIdx) {
		return nil, fmt.Errorf("par2repair: need at least one recovery slice per missing slice (missing=%d recovery=%d)",
			len(missingIdx), len(recoveryExp))
	}
	if sliceSize <= 0 || sliceSize%4 != 0 {
		return nil, fmt.Errorf("par2repair: invalid slice size %d", sliceSize)
	}
	ctx, err := gf16.NewContext(sliceSize)
	if err != nil {
		return nil, fmt.Errorf("par2repair: init gf16 context: %w", err)
	}
	return &Solver{
		missing:   slices.Clone(missingIdx),
		exps:      slices.Clone(recoveryExp),
		acc:       make([][]byte, len(recoveryExp)),
		sliceSize: sliceSize,
		alloc:     alloc,
		workers:   runtime.GOMAXPROCS(0),
		ctx:       ctx,
		prep:      ctx.NewBuffer(),
	}, nil
}

// Close releases the backend contexts. The solver must not be used after.
// Callers must close the solver: persistent workers retain their contexts.
func (s *Solver) Close() {
	if s.tasks != nil {
		close(s.tasks)
		s.workerWG.Wait()
		s.tasks = nil
	}
	if s.ctx != nil {
		s.ctx.Close()
		s.ctx = nil
	}
	for _, c := range s.wctx {
		c.Close()
	}
	s.wctx = nil
}

// accumulatorArenaBytes is the number of bytes a single accumulator requests
// from the solver's allocator: the backend's prepared-buffer size plus
// alignment slack. Callers sizing an arena multiply by the recovery row count.
func accumulatorArenaBytes(sliceSize int) (int64, error) {
	ctx, err := gf16.NewContext(sliceSize)
	if err != nil {
		return 0, fmt.Errorf("par2repair: init gf16 context: %w", err)
	}
	defer ctx.Close()
	return int64(ctx.BufSize()) + accAlign, nil
}

// newAcc carves an aligned prepared-layout buffer out of the allocator.
func (s *Solver) newAcc() ([]byte, error) {
	raw, err := s.alloc(s.ctx.BufSize() + accAlign)
	if err != nil {
		return nil, err
	}
	off := int(uintptr(unsafe.Pointer(&raw[0])) & uintptr(accAlign-1))
	if off != 0 {
		off = accAlign - off
	}
	return raw[off : off+s.ctx.BufSize() : off+s.ctx.BufSize()], nil
}

// AddRecovery seeds accumulator i with the i-th recovery slice's payload
// (zero-padded to slice size); the caller's buffer stays usable. Calling it
// again for the same accumulator XORs the new payload in.
func (s *Solver) AddRecovery(i int, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if s.acc[i] == nil {
		buf, err := s.newAcc()
		if err != nil {
			return err
		}
		s.acc[i] = buf
		s.ctx.Prepare(s.acc[i], payload)
		return nil
	}
	s.ctx.Prepare(s.prep, payload)
	s.ctx.MulAdd(s.acc[i], s.prep, 1)
	return nil
}

// SeedRecoveryOwning seeds accumulator i from a payload buffer the caller
// read for this repair alone. The payload is consumed: its contents move into
// the accumulator's prepared layout and nothing may rely on the buffer again.
// The buffer must be exactly one slice — the arithmetic reads whole slices.
func (s *Solver) SeedRecoveryOwning(i int, payload []byte) error {
	if len(payload) != s.sliceSize {
		return fmt.Errorf("par2repair: donated recovery buffer is %d bytes, want %d", len(payload), s.sliceSize)
	}
	if s.acc[i] != nil {
		return fmt.Errorf("par2repair: accumulator %d already seeded", i)
	}
	buf, err := s.newAcc()
	if err != nil {
		return err
	}
	s.acc[i] = buf
	s.ctx.Prepare(s.acc[i], payload)
	return nil
}

// AddMissing reclassifies one more global slice as missing, consuming a
// margin row. Call it instead of FoldPresent when a slice the plan thought
// present turns out unreadable or corrupt mid-fold; the slice must not have
// been folded. Fails when every loaded recovery row is already spoken for.
func (s *Solver) AddMissing(globalIdx int) error {
	if len(s.missing) >= len(s.exps) {
		return fmt.Errorf("par2repair: no spare recovery row for slice %d (missing=%d rows=%d)",
			globalIdx, len(s.missing)+1, len(s.exps))
	}
	s.missing = append(s.missing, globalIdx)
	return nil
}

// FoldPresent folds one present input slice (zero-padded to slice size) into
// every accumulator. Slices may arrive in any order. All accumulators must be
// seeded first.
func (s *Solver) FoldPresent(globalIdx int, slice []byte) {
	s.FoldPresentBatch([]int{globalIdx}, [][]byte{slice})
}

// FoldPresentBatch synchronously folds present slices into every recovery row.
// Indices and inputs must have equal lengths; inputs may be reused on return.
// The caller bounds batch memory. Empty inputs contribute zero.
func (s *Solver) FoldPresentBatch(indices []int, inputs [][]byte) {
	if len(indices) != len(inputs) {
		panic("par2repair: batch indices and inputs differ")
	}
	if len(inputs) == 0 {
		return
	}
	if len(s.prepared) == 0 {
		s.prepared = append(s.prepared, s.prep)
	}
	for len(s.prepared) < len(inputs) {
		s.prepared = append(s.prepared, s.ctx.NewBuffer())
	}
	srcs := s.prepared[:len(inputs)]
	bases := make([]gf2p16.T, len(inputs))
	for i, input := range inputs {
		// Prepare pads partial slices; an empty slice has an all-zero layout.
		if len(input) == 0 {
			clear(srcs[i])
		} else {
			s.ctx.Prepare(srcs[i], input)
		}
		bases[i] = VandermondeBase(indices[i])
	}
	if len(s.coefficients) != len(s.exps) {
		s.coefficients = make([][]uint16, len(s.exps))
	}
	for r, exp := range s.exps {
		if cap(s.coefficients[r]) < len(inputs) {
			s.coefficients[r] = make([]uint16, len(inputs))
		}
		s.coefficients[r] = s.coefficients[r][:len(inputs)]
		for i, g := range bases {
			s.coefficients[r][i] = uint16(g.Pow(exp))
		}
	}
	bufSize := s.ctx.BufSize()
	// Keep useful work per worker; too many tiny ranges lose to dispatch cost.
	workers := min(s.foldWorkers(), max(1, s.sliceSize/minParallelFold))
	if workers > 1 && s.workerContexts(workers) {
		stride := s.ctx.Stride()
		chunk := (bufSize + workers - 1) / workers
		chunk = (chunk + stride - 1) / stride * stride
		var wg sync.WaitGroup
		for from := 0; from < bufSize; from += chunk {
			wg.Add(1)
			s.tasks <- foldTask{srcs: srcs, coeffs: s.coefficients, offset: from, length: min(chunk, bufSize-from), done: &wg}
		}
		wg.Wait()
		return
	}
	s.foldRange(s.ctx, srcs, s.coefficients, 0, bufSize)
}

// foldWorkers is the fold width for the next FoldPresent call: the full
// worker count, capped by the live WorkerLimit when one is set.
func (s *Solver) foldWorkers() int {
	if s.WorkerLimit != nil {
		if l := s.WorkerLimit(); l > 0 && l < s.workers {
			return l
		}
	}
	return s.workers
}

// foldTask describes one independent byte range of a synchronous batch.
type foldTask struct {
	srcs           [][]byte
	coeffs         [][]uint16
	offset, length int
	done           *sync.WaitGroup
}

// workerContexts grows the persistent pool lazily, falling back to the primary
// context if another backend context cannot be created.
func (s *Solver) workerContexts(workers int) bool {
	if s.tasks == nil {
		s.tasks = make(chan foldTask)
	}
	for len(s.wctx) < workers {
		c, err := gf16.NewContext(s.sliceSize)
		if err != nil {
			return false
		}
		s.wctx = append(s.wctx, c)
		tasks := s.tasks
		s.workerWG.Add(1)
		go func() {
			defer s.workerWG.Done()
			for task := range tasks {
				s.foldRange(c, task.srcs, task.coeffs, task.offset, task.length)
				task.done.Done()
			}
		}()
	}
	return true
}

// foldRange applies a batch with one backend call per recovery row and range.
func (s *Solver) foldRange(ctx *gf16.Context, srcs [][]byte, coeffs [][]uint16, offset, length int) {
	for r := range s.acc {
		ctx.MulAddMulti(s.acc[r], srcs, offset, length, coeffs[r])
	}
}

// Solve inverts a k×k system over a linearly independent subset of the loaded
// rows and returns the recovered slices, positionally matching the missing
// indices (NewSolver's, plus AddMissing's in call order).
func (s *Solver) Solve() ([][]byte, error) {
	k := len(s.missing)

	// Coefficient of unknown i in row r.
	coeff := func(r, i int) gf2p16.T {
		return VandermondeBase(s.missing[i]).Pow(s.exps[r])
	}
	rows := selectIndependentRows(len(s.exps), k, coeff)
	if rows == nil {
		return nil, ErrSingularMatrix
	}

	a := gf2p16.NewMatrixFromFunction(k, k, func(r, i int) gf2p16.T {
		return coeff(rows[r], i)
	})
	inv, err := a.Inverse()
	if err != nil {
		return nil, ErrSingularMatrix
	}
	out := make([][]byte, k)
	tmp := s.ctx.NewBuffer()
	for i := range out {
		buf, err := s.alloc(s.sliceSize)
		if err != nil {
			return nil, err
		}
		out[i] = buf
		s.ctx.Mul(tmp, s.acc[rows[0]], uint16(inv.At(i, 0)))
		for r := 1; r < k; r++ {
			s.ctx.MulAdd(tmp, s.acc[rows[r]], uint16(inv.At(i, r)))
		}
		s.ctx.Finish(tmp, out[i])
	}
	return out, nil
}

// selectIndependentRows returns the indices of the first k rows (of n, in
// order, coefficients via coeff) that form a linearly independent set, or nil
// when no such set exists. This is what turns margin rows into singular-matrix
// insurance: a dependent row is skipped instead of failing the solve.
func selectIndependentRows(n, k int, coeff func(r, i int) gf2p16.T) []int {
	var sel []int
	var basis [][]gf2p16.T // reduced selected rows
	var pivot []int        // pivot column of each basis row
	for r := 0; r < n && len(sel) < k; r++ {
		v := make([]gf2p16.T, k)
		for i := range v {
			v[i] = coeff(r, i)
		}
		for b, p := range pivot {
			if v[p] == 0 {
				continue
			}
			f := v[p].Div(basis[b][p])
			for c := range v {
				v[c] = v[c].Minus(f.Times(basis[b][c]))
			}
		}
		pc := -1
		for c, x := range v {
			if x != 0 {
				pc = c
				break
			}
		}
		if pc < 0 {
			continue // dependent on the rows already selected
		}
		sel = append(sel, r)
		basis = append(basis, v)
		pivot = append(pivot, pc)
	}
	if len(sel) < k {
		return nil
	}
	return sel
}

// VandermondeBase returns the j-th PAR2 Vandermonde generator: 2^i in
// GF(2^16), skipping exponents divisible by 3, 5, 17 or 257 (whose elements
// have order below 65535). The table grows on demand and is process-cached.
func VandermondeBase(j int) gf2p16.T {
	basesMu.Lock()
	defer basesMu.Unlock()
	for len(bases) <= j {
		i := nextBaseExp
		nextBaseExp++
		if i%3 == 0 || i%5 == 0 || i%17 == 0 || i%257 == 0 {
			continue
		}
		bases = append(bases, gf2p16.T(2).Pow(uint32(i)))
	}
	return bases[j]
}

var (
	basesMu     sync.Mutex
	bases       []gf2p16.T
	nextBaseExp int
)
