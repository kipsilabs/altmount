package bundlex

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

const segSize = 716800

// marker strings that must never appear in a bundle.
var privateMarkers = []string{
	"/local/store/path", "RCLONE-PASSWORD-MARK", "RCLONE-SALT-MARK", "download-id-MARK", "IndexerMARK",
	"CategoryMARK", "ArrNameMARK", "provider.mark.example", "health-history-MARK", "fingerprint-MARK",
}

func ids(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%06d@news.test", prefix, i)
	}
	return out
}

// plainFile is a healthy, unencrypted file whose segments are all of segs.
func plainFile(name string, segs []string) *metapb.FileMetadata {
	m := &metapb.FileMetadata{
		FileSize: int64(len(segs)) * segSize, SourceNzbPath: "/local/store/path/x.nzbz", StoreRef: "/local/store/path/ref.nzbz",
		Status: metapb.FileStatus_FILE_STATUS_HEALTHY, CreatedAt: 1, ModifiedAt: 2, ReleaseDate: 1700000000,
		OriginalFilename: name, NzbdavId: "download-id-MARK", Password: "", Salt: "",
		HoleProviderFingerprint: "fingerprint-MARK",
	}
	for _, id := range segs {
		m.SegmentData = append(m.SegmentData, &metapb.SegmentData{Id: id, SegmentSize: segSize, StartOffset: 0, EndOffset: segSize - 1})
	}
	return m
}

// fakeLib implements Library and BuildSource over in-memory data.
type fakeLib struct {
	mu       sync.Mutex
	scheme   int
	paths    []string
	files    map[string]*metapb.FileMetadata
	nzb      [][]string
	pwNZB    map[string]string
	reads    int
	readErr  error
	slowRead time.Duration
}

func (f *fakeLib) ReleaseFiles(context.Context, string) (int, []string, error) {
	return f.scheme, f.paths, nil
}

func (f *fakeLib) ReadFile(p string) (*metapb.FileMetadata, error) {
	f.mu.Lock()
	f.reads++
	err, slow := f.readErr, f.slowRead
	m := f.files[p]
	f.mu.Unlock()
	if slow > 0 {
		time.Sleep(slow)
	}
	return m, err
}

func (f *fakeLib) NZBSegmentIDs(string) ([][]string, error) { return f.nzb, nil }

func (f *fakeLib) NZBPassword(p string, _ *metapb.FileMetadata) (string, bool) {
	pw, ok := f.pwNZB[p]
	return pw, ok
}

type patches map[string]bool

func (p patches) Has(id string) bool { return p[id] }

// nzbRelease is a two-file NZB release (one video, one par2 not imported).
func nzbRelease() (*fakeLib, string) {
	video := ids("vid", 20)
	par := ids("par", 3)
	lib := &fakeLib{scheme: database.PeernetSchemeNZB, paths: []string{"movies/R/movie.mkv"},
		files: map[string]*metapb.FileMetadata{"movies/R/movie.mkv": plainFile("movie.mkv", video)},
		nzb:   [][]string{video, par}}
	return lib, releaseHexOfNZB(lib.nzb)
}

func releaseHexOfNZB(files [][]string) string {
	store := &metapb.NzbStore{}
	for _, f := range files {
		fe := &metapb.NzbFileEntry{}
		for _, id := range f {
			fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id})
		}
		store.Files = append(store.Files, fe)
	}
	id, err := contentid.ReleaseID(store)
	if err != nil {
		panic(err)
	}
	return id.String()
}

var posted = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func newTestBuilder(t testing.TB, lib *fakeLib, p PatchIndex) *Builder {
	t.Helper()
	return NewBuilder(BuilderConfig{Source: lib, Library: lib, Patches: p})
}

func (f *fakeLib) setReadErr(err error) { f.mu.Lock(); f.readErr = err; f.mu.Unlock() }
func (f *fakeLib) readCount() int       { f.mu.Lock(); defer f.mu.Unlock(); return f.reads }
