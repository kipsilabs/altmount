package bundle

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// heapWorstBundle is the largest-pool shape a bundle can take: 60k pool
// entries of ~220 bytes, one NZB-scheme file reading all of them.
func heapWorstBundle(t *testing.T) (wire []byte, id BundleID, rel contentid.ID, canonical int) {
	t.Helper()
	n := 60000
	pad := strings.Repeat("a", 200)
	pool := make([]string, n)
	segs := make([]Segment, n)
	for i := range pool {
		pool[i] = fmt.Sprintf("%s%08d@h.example", pad, i)
		segs[i] = Segment{Ref: int64(i), Size: 1, Start: 0, End: 0}
	}
	b := &Bundle{Scheme: SchemeNZB, Title: "T", PostedUnix: 1700000000, Pool: pool, ReleaseFiles: []int{n},
		Files: []File{{Name: "m.mkv", Size: int64(n), Segments: segs, FileID: contentid.ID{1}}}, TotalSize: int64(n), ReleaseID: contentid.ID{9}}
	fid, err := contentid.FileID(buildMeta(b, &b.Files[0]))
	if err != nil {
		t.Fatal(err)
	}
	b.Files[0].FileID = fid
	if b.ReleaseID, err = releaseIDOfList(b.Pool, b.ReleaseFiles); err != nil {
		t.Fatal(err)
	}
	raw, err := Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	wire, err = Compress(raw)
	if err != nil {
		t.Fatal(err)
	}
	return wire, ComputeID(raw), b.ReleaseID, len(raw)
}

// TestVerifyPeakHeap pins the memory a single Verify of a worst-case valid
// bundle may use, so the decode path cannot silently regain extra copies.
func TestVerifyPeakHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates hundreds of MB")
	}
	wire, id, rel, canonical := heapWorstBundle(t)
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var (
		mu   sync.Mutex
		peak uint64
	)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			mu.Lock()
			peak = max(peak, m.HeapAlloc)
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()
	if _, err := Verify(wire, id, rel); err != nil {
		t.Fatal(err)
	}
	close(stop)
	<-done
	extra := int64(peak) - int64(base.HeapAlloc)
	t.Logf("canonical %d MB, wire %d KB, peak extra heap %d MB (%.1fx canonical)", canonical>>20, len(wire)>>10, extra>>20, float64(extra)/float64(canonical))
	if limit := int64(canonical) * maxHeapFactorTenths / 10; extra > limit && !raceBuild {
		t.Fatalf("peak extra heap %d MB exceeds %.1fx canonical (%d MB)", extra>>20, float64(maxHeapFactorTenths)/10, limit>>20)
	}
}

// maxHeapFactorTenths is the allowed peak, in tenths of the canonical size.
const maxHeapFactorTenths = 55
