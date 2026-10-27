package par2repair

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	"github.com/javi11/gopar-turbo/gf2p16"
)

// buildRecovery computes recovery slices the spec way (direct sum over all
// slices), independently of Solver's incremental fold.
func buildRecovery(slices [][]byte, exps []uint32, sliceSize int) [][]byte {
	out := make([][]byte, len(exps))
	for i, e := range exps {
		acc := make([]byte, sliceSize)
		for j, sl := range slices {
			gf2p16.MulAndAddByteSliceLE(VandermondeBase(j).Pow(e), sl, acc)
		}
		out[i] = acc
	}
	return out
}

func TestSolverRoundTrip(t *testing.T) {
	const sliceSize, n = 512, 20
	rng := rand.New(rand.NewSource(42))
	slices := make([][]byte, n)
	for j := range slices {
		slices[j] = make([]byte, sliceSize)
		rng.Read(slices[j])
	}
	missing := []int{3, 7, 19}
	exps := []uint32{0, 1, 2}
	rec := buildRecovery(slices, exps, sliceSize)

	s, err := NewSolver(missing, exps, sliceSize)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rec {
		s.AddRecovery(i, r)
	}
	for j, sl := range slices {
		if j == 3 || j == 7 || j == 19 {
			continue
		}
		s.FoldPresent(j, sl)
	}
	got, err := s.Solve()
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range missing {
		if !bytes.Equal(got[i], slices[m]) {
			t.Fatalf("slice %d not recovered", m)
		}
	}
}

func TestSolverSingleMissingHighExponent(t *testing.T) {
	const sliceSize, n = 256, 5
	slices := make([][]byte, n)
	for j := range slices {
		slices[j] = bytes.Repeat([]byte{byte(j + 1)}, sliceSize)
	}
	// Use a non-contiguous exponent, as when earlier volumes are dead.
	exps := []uint32{7}
	rec := buildRecovery(slices, exps, sliceSize)

	s, err := NewSolver([]int{2}, exps, sliceSize)
	if err != nil {
		t.Fatal(err)
	}
	s.AddRecovery(0, rec[0])
	for j, sl := range slices {
		if j != 2 {
			s.FoldPresent(j, sl)
		}
	}
	got, err := s.Solve()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[0], slices[2]) {
		t.Fatal("slice 2 not recovered")
	}
}

func TestSolverKMismatch(t *testing.T) {
	if _, err := NewSolver([]int{1, 2}, []uint32{0}, 512); err == nil {
		t.Fatal("want error: need one recovery slice per missing slice")
	}
	// Zero missing slices is valid: a verify sweep starts empty and grows
	// its unknowns via AddMissing as corrupt slices surface.
	if _, err := NewSolver(nil, nil, 512); err != nil {
		t.Fatalf("zero missing slices must construct a valid solver, got %v", err)
	}
	if _, err := NewSolver([]int{1}, []uint32{0}, 511); err == nil {
		t.Fatal("want error: slice size not a multiple of 4")
	}
}

// TestSolverMarginRowsAndAddMissing loads more recovery rows than there are
// missing slices (margin) and reclassifies one more slice as missing mid-fold,
// as the sweep does when an article dies between planning and sweeping.
func TestSolverMarginRowsAndAddMissing(t *testing.T) {
	const sliceSize, n = 512, 10
	rng := rand.New(rand.NewSource(11))
	slices := make([][]byte, n)
	for j := range slices {
		slices[j] = make([]byte, sliceSize)
		rng.Read(slices[j])
	}
	exps := []uint32{0, 1, 2, 3} // 4 rows for what starts as 1 missing slice
	rec := buildRecovery(slices, exps, sliceSize)

	s, err := NewSolver([]int{2}, exps, sliceSize)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rec {
		if err := s.AddRecovery(i, r); err != nil {
			t.Fatal(err)
		}
	}
	for j, sl := range slices {
		if j == 2 {
			continue
		}
		if j == 5 {
			// Slice 5 turns out missing mid-sweep: absorb it instead of
			// re-sweeping, using one of the margin rows.
			if err := s.AddMissing(5); err != nil {
				t.Fatal(err)
			}
			continue
		}
		s.FoldPresent(j, sl)
	}
	got, err := s.Solve()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("recovered %d slices, want 2", len(got))
	}
	if !bytes.Equal(got[0], slices[2]) || !bytes.Equal(got[1], slices[5]) {
		t.Fatal("margin solve did not recover both slices")
	}
}

// TestSolverAddMissingWithoutSpareRow verifies AddMissing refuses to grow the
// missing set past the loaded recovery rows.
func TestSolverAddMissingWithoutSpareRow(t *testing.T) {
	s, err := NewSolver([]int{0}, []uint32{0}, 512)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddMissing(1); err == nil {
		t.Fatal("want error: no spare accumulator for a second missing slice")
	}
}

// TestSolverSolveSkipsDependentRows gives Solve linearly dependent rows ahead
// of an independent one: it must select an invertible row subset instead of
// failing on the first k rows (the PAR2 Vandermonde singularity case).
func TestSolverSolveSkipsDependentRows(t *testing.T) {
	const sliceSize, n = 256, 8
	rng := rand.New(rand.NewSource(13))
	slices := make([][]byte, n)
	for j := range slices {
		slices[j] = make([]byte, sliceSize)
		rng.Read(slices[j])
	}
	// Rows 0 and 1 share an exponent, so any 2x2 system over them is singular;
	// row 2 provides the independent equation.
	exps := []uint32{1, 1, 5}
	rec := buildRecovery(slices, exps, sliceSize)

	missing := []int{0, 4}
	s, err := NewSolver(missing, exps, sliceSize)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rec {
		if err := s.AddRecovery(i, r); err != nil {
			t.Fatal(err)
		}
	}
	for j, sl := range slices {
		if j != 0 && j != 4 {
			s.FoldPresent(j, sl)
		}
	}
	got, err := s.Solve()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[0], slices[0]) || !bytes.Equal(got[1], slices[4]) {
		t.Fatal("row-selecting solve did not recover the slices")
	}
}

// TestSolverSeedRecoveryOwning donates the recovery payload buffer to the
// solver: seeding consumes it into the accumulator's prepared layout and the
// caller must not rely on it afterwards.
func TestSolverSeedRecoveryOwning(t *testing.T) {
	const sliceSize, n = 512, 6
	rng := rand.New(rand.NewSource(17))
	slices := make([][]byte, n)
	for j := range slices {
		slices[j] = make([]byte, sliceSize)
		rng.Read(slices[j])
	}
	exps := []uint32{0}
	rec := buildRecovery(slices, exps, sliceSize)

	s, err := NewSolver([]int{3}, exps, sliceSize)
	if err != nil {
		t.Fatal(err)
	}
	donated := bytes.Clone(rec[0])
	if err := s.SeedRecoveryOwning(0, donated); err != nil {
		t.Fatal(err)
	}
	for j, sl := range slices {
		if j != 3 {
			s.FoldPresent(j, sl)
		}
	}
	got, err := s.Solve()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[0], slices[3]) {
		t.Fatal("owning-seeded solve did not recover the slice")
	}

	// Wrong-size donations are refused: the arithmetic reads whole slices.
	s2, err := NewSolver([]int{0}, []uint32{0}, 512)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.SeedRecoveryOwning(0, make([]byte, 100)); err == nil {
		t.Fatal("want error: donated buffer must be exactly one slice")
	}
}

// TestSolverLargeSliceRoundTrip exercises the parallel fold path (slices above
// the parallel threshold, with a size that does not divide evenly across
// workers) and must agree with the spec arithmetic byte for byte.
func TestSolverLargeSliceRoundTrip(t *testing.T) {
	const sliceSize = 192<<10 + 4 // above minParallelFold, awkward chunking
	const n = 5
	rng := rand.New(rand.NewSource(19))
	slices := make([][]byte, n)
	for j := range slices {
		slices[j] = make([]byte, sliceSize)
		rng.Read(slices[j])
	}
	missing := []int{1, 4}
	exps := []uint32{0, 1}
	rec := buildRecovery(slices, exps, sliceSize)

	s, err := NewSolver(missing, exps, sliceSize)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, r := range rec {
		if err := s.AddRecovery(i, r); err != nil {
			t.Fatal(err)
		}
	}
	for j, sl := range slices {
		if j != 1 && j != 4 {
			s.FoldPresent(j, sl)
		}
	}
	got, err := s.Solve()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[0], slices[1]) || !bytes.Equal(got[1], slices[4]) {
		t.Fatal("large-slice round trip failed")
	}
}

// TestSolverFoldWorkersHonorsLimit: a live worker limit throttles the
// parallel fold so a repair running beside playback does not saturate every
// core. Non-positive or over-wide limits fall back to the full width.
func TestSolverFoldWorkersHonorsLimit(t *testing.T) {
	s, err := NewSolver([]int{0}, []uint32{0}, 512)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if got := s.foldWorkers(); got != s.workers {
		t.Fatalf("no limit: foldWorkers = %d, want %d", got, s.workers)
	}
	s.WorkerLimit = func() int { return 1 }
	if got := s.foldWorkers(); got != 1 {
		t.Fatalf("limit 1: foldWorkers = %d, want 1", got)
	}
	s.WorkerLimit = func() int { return 0 }
	if got := s.foldWorkers(); got != s.workers {
		t.Fatalf("limit 0 (unset): foldWorkers = %d, want %d", got, s.workers)
	}
	s.WorkerLimit = func() int { return s.workers + 5 }
	if got := s.foldWorkers(); got != s.workers {
		t.Fatalf("limit above width: foldWorkers = %d, want %d", got, s.workers)
	}
}

// TestSolverWorkerLimitedRoundTrip re-runs the parallel-fold round trip under
// a worker limit: throttled folds must stay byte-exact, both on the limited
// parallel path and when the limit collapses the fold to one goroutine.
func TestSolverWorkerLimitedRoundTrip(t *testing.T) {
	const sliceSize = 192<<10 + 4 // above minParallelFold, awkward chunking
	const n = 5
	for _, limit := range []int{1, 2} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			rng := rand.New(rand.NewSource(19))
			slices := make([][]byte, n)
			for j := range slices {
				slices[j] = make([]byte, sliceSize)
				rng.Read(slices[j])
			}
			missing := []int{1, 4}
			exps := []uint32{0, 1}
			rec := buildRecovery(slices, exps, sliceSize)

			s, err := NewSolver(missing, exps, sliceSize)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.WorkerLimit = func() int { return limit }
			for i, r := range rec {
				if err := s.AddRecovery(i, r); err != nil {
					t.Fatal(err)
				}
			}
			for j, sl := range slices {
				if j != 1 && j != 4 {
					s.FoldPresent(j, sl)
				}
			}
			got, err := s.Solve()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got[0], slices[1]) || !bytes.Equal(got[1], slices[4]) {
				t.Fatal("worker-limited round trip failed")
			}
		})
	}
}

func BenchmarkFoldPresent(b *testing.B) {
	const sliceSize = 4 << 20
	slice := make([]byte, sliceSize)
	rand.New(rand.NewSource(23)).Read(slice)
	exps := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
	s, err := NewSolver([]int{0, 1, 2, 3, 4, 5, 6, 7}, exps, sliceSize)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	for i := range exps {
		if err := s.AddRecovery(i, slice); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(sliceSize)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		s.FoldPresent(8+i, slice)
	}
}

func TestSolverBatchRoundTrip(t *testing.T) {
	for _, size := range []int{512, 256 << 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			const n = 9
			data := make([][]byte, n)
			rng := rand.New(rand.NewSource(90))
			for i := range data {
				data[i] = make([]byte, size)
				rng.Read(data[i])
			}
			exps := []uint32{1, 3, 7}
			rec := buildRecovery(data, exps, size)
			s, err := NewSolver([]int{2}, exps, size)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for i := range rec {
				if err := s.AddRecovery(i, rec[i]); err != nil {
					t.Fatal(err)
				}
			}
			limit := 1
			s.WorkerLimit = func() int { return limit }
			s.FoldPresentBatch([]int{0, 1, 3}, [][]byte{data[0], data[1], data[3]})
			if err := s.AddMissing(5); err != nil {
				t.Fatal(err)
			}
			limit = 2
			s.FoldPresentBatch([]int{4, 6, 7}, [][]byte{data[4], data[6], data[7]})
			s.FoldPresentBatch([]int{8, 9}, [][]byte{data[8], nil})
			s.FoldPresentBatch(nil, nil)
			got, err := s.Solve()
			if err != nil {
				t.Fatal(err)
			}
			for i, m := range []int{2, 5} {
				if !bytes.Equal(got[i], data[m]) {
					t.Fatalf("missing slice %d differs", m)
				}
			}
		})
	}
}

func BenchmarkFoldPresentBatch(b *testing.B) {
	const size = 4 << 20
	data := make([][]byte, 4)
	rng := rand.New(rand.NewSource(23))
	for i := range data {
		data[i] = make([]byte, size)
		rng.Read(data[i])
	}
	exps := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
	s, err := NewSolver([]int{0, 1, 2, 3, 4, 5, 6, 7}, exps, size)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	for i := range exps {
		if err := s.AddRecovery(i, data[0]); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(4 * size)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s.FoldPresentBatch([]int{8, 9, 10, 11}, data)
	}
}
