package par2repair

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
)

// mkIndex builds a parsed PAR2 index over the given file contents.
func mkIndex(t *testing.T, sliceSize int, numRecovery int, contents map[string][]byte) *par2.Index {
	t.Helper()
	var entries []par2gen.FileEntry
	for name, c := range contents {
		entries = append(entries, par2gen.FileEntry{Name: name, Content: c})
	}
	set := par2gen.BuildFull(sliceSize, entries, numRecovery)
	streams := []io.Reader{bytes.NewReader(set.Index)}
	for _, v := range set.Volumes {
		streams = append(streams, bytes.NewReader(v))
	}
	idx, err := par2.ParseIndex(streams)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

// mkSetFile builds a SetFile for the named file, splitting its content into
// articles of artSize bytes; the article indices in dead are marked Dead.
func mkSetFile(t *testing.T, idx *par2.Index, name string, size int64, artSize int64, dead ...int) SetFile {
	t.Helper()
	var fileID [16]byte
	found := false
	for id, fd := range idx.Files {
		if fd.Name == name {
			fileID, found = id, true
			break
		}
	}
	if !found {
		t.Fatalf("file %q not in index", name)
	}
	deadSet := map[int]bool{}
	for _, d := range dead {
		deadSet[d] = true
	}
	sf := SetFile{FileID: fileID, Length: uint64(size)}
	for off, i := int64(0), 0; off < size; off, i = off+artSize, i+1 {
		sz := min(artSize, size-off)
		sf.Articles = append(sf.Articles, Article{
			MessageID: fmt.Sprintf("<%s-%d@test>", name, i),
			Size:      sz,
			Dead:      deadSet[i],
		})
	}
	return sf
}

func defaultCaps() Caps {
	return Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}
}

func TestBuildPlanSingleDeadArticle(t *testing.T) {
	// One file: 10240 B, slice 1024 (10 slices), articles of 2048 B.
	// Article 1 dead -> bytes [2048,4096) -> slices 2,3.
	content := bytes.Repeat([]byte{0x5A}, 10240)
	idx := mkIndex(t, 1024, 6, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 10240, 2048, 1)}

	plan, err := BuildPlan(idx, files, defaultCaps())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 3}; !equalInts(plan.Missing, want) {
		t.Fatalf("Missing = %v, want %v", plan.Missing, want)
	}
	if plan.GlobalSlices != 10 {
		t.Fatalf("GlobalSlices = %d", plan.GlobalSlices)
	}
	// 2 missing + up to planMargin extra rows for mid-sweep discoveries.
	if len(plan.Recovery) != 6 || len(plan.SpareRecovery) != 0 {
		t.Fatalf("recovery split = %d/%d, want 6/0", len(plan.Recovery), len(plan.SpareRecovery))
	}
	// Lowest exponents chosen first.
	if plan.Recovery[0].Exponent != 0 || plan.Recovery[1].Exponent != 1 {
		t.Fatalf("recovery exponents = %d,%d", plan.Recovery[0].Exponent, plan.Recovery[1].Exponent)
	}
	if len(plan.DeadArticles) != 1 || plan.DeadArticles[0].FileStart != 2048 {
		t.Fatalf("DeadArticles = %+v", plan.DeadArticles)
	}
}

func TestBuildPlanStraddlingBoundary(t *testing.T) {
	// slice 1024; articles of 1536 B: article 1 covers [1536,3072) ->
	// slices 1,2 (and byte 3071 is inside slice 2).
	content := bytes.Repeat([]byte{0x11}, 6144)
	idx := mkIndex(t, 1024, 4, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 6144, 1536, 1)}

	plan, err := BuildPlan(idx, files, defaultCaps())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{1, 2}; !equalInts(plan.Missing, want) {
		t.Fatalf("Missing = %v, want %v", plan.Missing, want)
	}
}

func TestBuildPlanSecondFileOffset(t *testing.T) {
	// file layout in FileID order is not controllable by name, so compute
	// the expected offset from the plan's own file order.
	a := bytes.Repeat([]byte{0x01}, 4096) // 4 slices at 1024
	b := bytes.Repeat([]byte{0x02}, 4096)
	idx := mkIndex(t, 1024, 6, map[string][]byte{"a.rar": a, "b.rar": b})
	files := []SetFile{
		mkSetFile(t, idx, "a.rar", 4096, 2048),
		mkSetFile(t, idx, "b.rar", 4096, 2048, 0), // b article 0 dead -> b slices 0,1
	}

	plan, err := BuildPlan(idx, files, defaultCaps())
	if err != nil {
		t.Fatal(err)
	}
	// find b.rar's position in the plan's recovery-set order
	bID := files[1].FileID
	start := 0
	for _, f := range plan.Files {
		if f.FileID == bID {
			break
		}
		start += 4 // 4 slices per file
	}
	if want := []int{start, start + 1}; !equalInts(plan.Missing, want) {
		t.Fatalf("Missing = %v, want %v", plan.Missing, want)
	}
	if plan.GlobalSlices != 8 {
		t.Fatalf("GlobalSlices = %d", plan.GlobalSlices)
	}
}

func TestBuildPlanNotEnoughRecovery(t *testing.T) {
	content := bytes.Repeat([]byte{0x33}, 8192)
	idx := mkIndex(t, 1024, 1, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 8192, 2048, 1)} // 2 missing slices, 1 recovery

	_, err := BuildPlan(idx, files, defaultCaps())
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable", err)
	}
}

func TestBuildPlanMemoryCapSpillsToDisk(t *testing.T) {
	content := bytes.Repeat([]byte{0x44}, 8192)
	idx := mkIndex(t, 1024, 4, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 8192, 2048, 1)}

	caps := defaultCaps()
	caps.MaxMemoryBytes = 1024 // 2 slices needed = 2048 B > 1024
	plan, err := BuildPlan(idx, files, caps)
	if err != nil {
		t.Fatalf("over-budget damage must plan a disk-backed repair, got %v", err)
	}
	if !plan.SpillToDisk {
		t.Fatal("plan must spill to disk when accumulators exceed the memory budget")
	}

	caps.MaxMemoryBytes = 64 << 20
	plan, err = BuildPlan(idx, files, caps)
	if err != nil {
		t.Fatal(err)
	}
	if plan.SpillToDisk {
		t.Fatal("plan must stay in memory under the budget")
	}
}

func TestBuildPlanRatioCap(t *testing.T) {
	content := bytes.Repeat([]byte{0x55}, 8192)
	idx := mkIndex(t, 1024, 4, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 8192, 2048, 1)}

	caps := defaultCaps()
	caps.MaxRepairRatio = 0.1 // damage = 2048/8192 = 25% > 10%
	_, err := BuildPlan(idx, files, caps)
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable", err)
	}
}

func TestBuildPlanNothingToRepair(t *testing.T) {
	content := bytes.Repeat([]byte{0x66}, 4096)
	idx := mkIndex(t, 1024, 2, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 4096, 2048)}

	_, err := BuildPlan(idx, files, defaultCaps())
	if !errors.Is(err, ErrNothingToRepair) {
		t.Fatalf("err = %v, want ErrNothingToRepair", err)
	}
}

// A verify sweep: the trigger knows the release is damaged (corrupt-but-
// present articles broke archive analysis at import) but not which articles.
// With VerifySweep set, no dead articles builds a plan with no missing slices
// and margin recovery rows only — the job's CRC sweep locates the corrupt
// slices and absorbs them onto the margin. Without the flag, the no-damage
// short-circuit stays as before.
func TestBuildPlanVerifySweep(t *testing.T) {
	content := bytes.Repeat([]byte{0x66}, 10240)
	idx := mkIndex(t, 1024, 6, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 10240, 2048)} // nothing dead

	caps := defaultCaps()
	caps.VerifySweep = true
	plan, err := BuildPlan(idx, files, caps)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Missing) != 0 {
		t.Fatalf("Missing = %v, want none for a verify sweep", plan.Missing)
	}
	if len(plan.Recovery) == 0 {
		t.Fatal("verify sweep needs margin recovery rows to absorb corrupt slices")
	}
	if len(plan.Recovery)+len(plan.SpareRecovery) != 6 {
		t.Fatalf("recovery split = %d/%d, want all 6 rows accounted for",
			len(plan.Recovery), len(plan.SpareRecovery))
	}

	// Known damage keeps its normal plan regardless of the flag.
	damaged := []SetFile{mkSetFile(t, idx, "a.rar", 10240, 2048, 1)}
	plan, err = BuildPlan(idx, damaged, caps)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Missing) == 0 {
		t.Fatal("dead articles must still produce missing slices under VerifySweep")
	}
}

func TestBuildPlanMissingSetMember(t *testing.T) {
	a := bytes.Repeat([]byte{0x01}, 4096)
	b := bytes.Repeat([]byte{0x02}, 4096)
	idx := mkIndex(t, 1024, 2, map[string][]byte{"a.rar": a, "b.rar": b})
	// only a.rar resolved by the caller
	files := []SetFile{mkSetFile(t, idx, "a.rar", 4096, 2048, 0)}

	_, err := BuildPlan(idx, files, defaultCaps())
	if err == nil || errors.Is(err, ErrNothingToRepair) {
		t.Fatalf("err = %v, want unresolved-member error", err)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The repair cap must measure damage in bytes actually missing, not in
// slice-quantized bytes. Real releases post large PAR2 slices over much
// smaller articles (e.g. a 27 MB slice over 1 MB articles), so one dead
// article marks a whole slice missing and quantized accounting overstates the
// damage by the slice/article ratio — rejecting releases that are barely
// damaged at all.
func TestBuildPlanRatioUsesMissingBytesNotSliceQuantized(t *testing.T) {
	// 65536 B file, 8192 B slices, 256 B articles: one dead article is
	// 0.39% of the file, but it lands in a slice worth 12.5% of it.
	content := bytes.Repeat([]byte{0x66}, 65536)
	idx := mkIndex(t, 8192, 4, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 65536, 256, 3)}

	caps := defaultCaps()
	caps.MaxRepairRatio = 0.05 // above the true 0.39%, below the quantized 12.5%

	plan, err := BuildPlan(idx, files, caps)
	if err != nil {
		t.Fatalf("a file 0.39%% damaged must be repairable under a 5%% cap: %v", err)
	}
	if len(plan.Missing) != 1 {
		t.Fatalf("Missing = %v, want exactly the one slice holding the dead article", plan.Missing)
	}
}

// TestBuildPlanMarginRespectsMemoryBudget shrinks the margin rather than let
// it push an in-memory job over the budget and into disk spill.
func TestBuildPlanMarginRespectsMemoryBudget(t *testing.T) {
	content := bytes.Repeat([]byte{0x5A}, 10240)
	idx := mkIndex(t, 1024, 6, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 10240, 2048, 1)}

	// 2 missing slices of 1024 B fit in 4096 B; margin rows past 4 slices
	// would not. Margin must shrink to 2, not force SpillToDisk.
	plan, err := BuildPlan(idx, files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if plan.SpillToDisk {
		t.Fatal("margin must not force an in-memory job to spill")
	}
	if len(plan.Recovery) != 4 || len(plan.SpareRecovery) != 2 {
		t.Fatalf("recovery split = %d/%d, want 4/2", len(plan.Recovery), len(plan.SpareRecovery))
	}
}

// TestBuildPlanSpillKeepsFullMargin: a plan already over the memory budget is
// disk-backed anyway, so it keeps the full margin.
func TestBuildPlanSpillKeepsFullMargin(t *testing.T) {
	content := bytes.Repeat([]byte{0x5A}, 10240)
	idx := mkIndex(t, 1024, 6, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 10240, 2048, 1)}

	plan, err := BuildPlan(idx, files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.SpillToDisk {
		t.Fatal("fixture must produce a spill plan")
	}
	if len(plan.Recovery) != 6 {
		t.Fatalf("Recovery = %d, want all 6 (full margin on a spill plan)", len(plan.Recovery))
	}
}

// A liveness sample that found hidden damage yields an article estimate
// instead of a full-release STAT. The plan must provision extra margin rows
// for those articles — sized from how many slices a typical article spans —
// so the payload sweep absorbs them without a replan.
func TestBuildPlanProvisionsMarginForHiddenDamage(t *testing.T) {
	// One file: 10240 B, slice 1024 (10 slices), articles of 2048 B.
	// Article 1 dead -> 2 known missing slices. 20 recovery slices available.
	content := bytes.Repeat([]byte{0x5A}, 10240)
	idx := mkIndex(t, 1024, 20, map[string][]byte{"a.rar": content})
	files := []SetFile{mkSetFile(t, idx, "a.rar", 10240, 2048, 1)}

	caps := defaultCaps()
	caps.ExpectedHiddenArticles = 2
	plan, err := BuildPlan(idx, files, caps)
	if err != nil {
		t.Fatal(err)
	}

	// Without the estimate the plan carries k + planMargin = 10 rows; the two
	// expected hidden articles must add rows beyond that.
	if len(plan.Recovery) <= 2+planMargin {
		t.Fatalf("recovery rows = %d, want more than %d (hidden-damage margin missing)",
			len(plan.Recovery), 2+planMargin)
	}
	if got := len(plan.Recovery) + len(plan.SpareRecovery); got != 20 {
		t.Fatalf("rows + spares = %d, want all 20", got)
	}

	// The memory cap still bounds the extra rows: with room for only 4 rows
	// the plan must not grow past it.
	caps.MaxMemoryBytes = 4 * 1024
	plan, err = BuildPlan(idx, files, caps)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Recovery) != 4 {
		t.Fatalf("recovery rows = %d, want 4 (memory cap must still bound hidden-damage margin)",
			len(plan.Recovery))
	}
}

func TestBuildPlanRecoveryMargin(t *testing.T) {
	content := bytes.Repeat([]byte{0x5a}, 16*1024)
	idx := mkIndex(t, 1024, 12, map[string][]byte{"a.rar": content})
	for _, tc := range []struct {
		name   string
		margin *int
		verify bool
		hidden int
		want   int
	}{
		{name: "default", want: 9},
		{name: "zero", margin: new(int), want: 1},
		{name: "two", margin: intPointer(2), want: 3},
		{name: "negative defaults", margin: intPointer(-1), want: 9},
		{name: "verify keeps insurance", margin: new(int), verify: true, want: 8},
		{name: "hidden damage keeps insurance", margin: new(int), hidden: 1, want: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dead []int
			if !tc.verify {
				dead = []int{0}
			}
			files := []SetFile{mkSetFile(t, idx, "a.rar", int64(len(content)), 1024, dead...)}
			caps := defaultCaps()
			caps.RecoveryMargin = tc.margin
			caps.VerifySweep = tc.verify
			caps.ExpectedHiddenArticles = tc.hidden
			plan, err := BuildPlan(idx, files, caps)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Recovery) != tc.want {
				t.Fatalf("rows=%d want=%d", len(plan.Recovery), tc.want)
			}
		})
	}
}

func intPointer(n int) *int { return &n }
