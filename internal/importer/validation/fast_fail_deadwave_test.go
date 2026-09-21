package validation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
)

// A dead post used to be STAT-ed 64 times, three attempts over, then swept per
// file: hundreds of slow 430 lookups left pipelined on the connections, which
// the next import's STATs then queued behind. Eight sampled articles all
// missing is already the verdict.
func TestFastFailReleaseProbeVerdictJudgesDeadPostFromFirstWave(t *testing.T) {
	outcomes := make(map[string][]error, 64)
	for _, seg := range makeTestSegments("seg", 64) {
		outcomes[seg.Id] = []error{nntppool.ErrArticleNotFound}
	}
	client := newScriptedStatClient(outcomes)

	v, err := FastFailReleaseProbeVerdict(context.Background(), probeFile(64), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	if err != nil {
		t.Fatalf("FastFailReleaseProbeVerdict error = %v", err)
	}
	if !v.Missing || !v.Dead {
		t.Fatalf("verdict = %+v, want Missing and Dead", v)
	}
	total := 0
	for _, seg := range makeTestSegments("seg", 64) {
		total += client.callCount(seg.Id)
	}
	if total > probeFirstWave {
		t.Fatalf("STATs issued = %d, want at most the first wave of %d", total, probeFirstWave)
	}
	if len(v.MissingIDs) == 0 {
		t.Fatal("verdict carries no missing ids for the caller to record")
	}
}

func TestFastFailReleaseProbeVerdictHealthyPostChecksWholeSample(t *testing.T) {
	client := newScriptedStatClient(nil)

	v, err := FastFailReleaseProbeVerdict(context.Background(), probeFile(64), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	if err != nil || v.Missing || v.Dead {
		t.Fatalf("verdict = %+v, err = %v, want healthy", v, err)
	}
	total := 0
	for _, seg := range makeTestSegments("seg", 64) {
		total += client.callCount(seg.Id)
	}
	if total != 64 {
		t.Fatalf("STATs issued = %d, want the whole 64-article sample on a healthy post", total)
	}
}

func TestFastFailReleaseProbeVerdictPartialDamageIsNotDead(t *testing.T) {
	client := newScriptedStatClient(map[string][]error{"seg-1": {nntppool.ErrArticleNotFound}})

	v, err := FastFailReleaseProbeVerdict(context.Background(), probeFile(64), fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, time.Time{})
	if err != nil {
		t.Fatalf("FastFailReleaseProbeVerdict error = %v", err)
	}
	if !v.Missing || v.Dead {
		t.Fatalf("verdict = %+v, want Missing but not Dead: one miss of eight is damage to map, not a dead post", v)
	}
}

func TestDeadReleaseResultsMarkEveryFileBroken(t *testing.T) {
	files := []FastFailFile{
		{Filename: "a.part01.rar", Segments: makeTestSegments("a", 3), GroupKey: "a"},
		{Filename: "a.part02.rar", Segments: makeTestSegments("b", 3), GroupKey: "a"},
		{Filename: "a.par2"},
	}
	results := DeadReleaseResults(files, []string{"a-0", "b-2"})
	if len(results) != 3 {
		t.Fatalf("results = %d, want one per file", len(results))
	}
	if !results[0].Broken || !results[1].Broken {
		t.Fatalf("files with segments not marked broken: %+v", results[:2])
	}
	if results[2].Broken {
		t.Fatal("segment-less sidecar marked broken")
	}
	if got := results[0].MissingSegmentIDs; len(got) != 1 || got[0] != "a-0" {
		t.Fatalf("file 0 missing ids = %v, want [a-0]", got)
	}
	if got := results[1].MissingSegmentIDs; len(got) != 1 || got[0] != "b-2" {
		t.Fatalf("file 1 missing ids = %v, want [b-2]", got)
	}
}

// A damaged-not-dead post (first article of every volume gone) reaches the
// per-file sweep. On a provider answering 430s slowly the sweep used to
// dispatch 64 STATs and wait the attempt out — 18 s in the bench — although
// the first dozen misses already condemned the release, and every 430 it left
// in flight slowed the import that followed. The attempt ends as soon as the
// definitive answers prove the post dead, and the first chunk stays small.
func TestFastFailCheckFilesEndsAttemptOnceReleaseIsDead(t *testing.T) {
	var files []FastFailFile
	outcomes := make(map[string][]error, 64)
	slow := make(map[string]time.Duration, 64)
	for i := range 64 {
		segs := makeTestSegments(fmt.Sprintf("v%02d", i), 1)
		files = append(files, FastFailFile{Filename: fmt.Sprintf("vol%02d.mkv", i), Segments: segs})
		outcomes[segs[0].Id] = []error{nntppool.ErrArticleNotFound}
		if i >= 12 {
			slow[segs[0].Id] = 3 * time.Second
		}
	}
	client := newDelayedStatClient(outcomes, nil)
	client.alwaysDelay = slow

	start := time.Now()
	results, err := FastFailCheckFiles(context.Background(), files, fastFailPoolManager{client: client}, 100, 64, 30*time.Second, nil, nil, false, time.Time{})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want the dead-release verdict", err)
	}
	for i, r := range results {
		if !r.Broken {
			t.Fatalf("results[%d] not Broken on a dead release", i)
		}
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("sweep took %s, want the attempt cut once a dozen misses condemned the release", elapsed)
	}
	total := 0
	for i := range 64 {
		total += client.callCount(fmt.Sprintf("v%02d-0", i))
	}
	if total > firstSweepChunk {
		t.Fatalf("STATs issued = %d, want at most the first chunk of %d", total, firstSweepChunk)
	}
}
