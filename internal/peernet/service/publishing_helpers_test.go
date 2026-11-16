package service

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
)

// pubMeta is a metadata tree with fixed files.
type pubMeta struct {
	mu    sync.Mutex
	files map[string]*metapb.FileMetadata
}

func (m *pubMeta) ReadFileMetadata(p string) (*metapb.FileMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.files[p], nil
}
func (m *pubMeta) ReadStore(string) (*metapb.NzbStore, error)               { return nil, nil }
func (m *pubMeta) ListDirectoryAll(string) ([]fs.FileInfo, []string, error) { return nil, nil, nil }
func (m *pubMeta) SetFileObserver(metadata.FileObserver)                    {}

// fakeBackboneSource counts refreshes.
type fakeBackboneSource struct {
	provider, known []string
	refreshes       atomic.Int32
	refreshErr      error
}

func (f *fakeBackboneSource) ProviderGroups() []string { return f.provider }
func (f *fakeBackboneSource) Known() []string          { return f.known }
func (f *fakeBackboneSource) Refresh(context.Context) error {
	f.refreshes.Add(1)
	return f.refreshErr
}

// exclusionsHolder is a live, mutable exclusion list.
type exclusionsHolder struct {
	mu sync.Mutex
	ex publish.Exclusions
}

func (h *exclusionsHolder) get() publish.Exclusions {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ex
}
func (h *exclusionsHolder) set(e publish.Exclusions) {
	h.mu.Lock()
	h.ex = e
	h.mu.Unlock()
}

type pubFixture struct {
	*instance
	meta    *pubMeta
	bb      *fakeBackboneSource
	excl    *exclusionsHolder
	content *database.PeernetContentRepository
	state   *database.PeernetPublishStateRepository
}

var (
	pubRelease = strings.Repeat("a", 64)
	pubFile    = strings.Repeat("b", 64)
)

const pubPath = "movies/Some.Movie.2020/movie.mkv"

// newPubInstance builds an instance whose service has every publishing
// dependency. mut may adjust the deps (test hooks).
func newPubInstance(t *testing.T, set Settings, mut func(*Deps)) *pubFixture {
	t.Helper()
	dir := t.TempDir()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(dir, "alt.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, d := db.Connection(), db.Dialect()
	f := &pubFixture{
		meta:    &pubMeta{files: map[string]*metapb.FileMetadata{}},
		bb:      &fakeBackboneSource{provider: []string{"Omicron"}, known: []string{"Omicron", "Eweka Internet Services"}},
		excl:    &exclusionsHolder{},
		content: database.NewPeernetContentRepository(conn, d),
		state:   database.NewPeernetPublishStateRepository(conn, d),
	}
	nets := database.NewPeernetNetworkRepository(conn, d)
	recs := database.NewPeernetRecordRepository(conn, d)
	cur := database.NewPeernetSyncCursorRepository(conn, d)
	deps := Deps{
		DataDir: dir, Networks: nets, Peers: database.NewPeernetPeerRepository(conn, d), Records: recs, Cursors: cur,
		Content: f.content, Metadata: f.meta, Settings: set,
		PublishState: f.state, Origins: database.NewPeernetOriginRepository(conn, d),
		Exclusions: f.excl.get, Backbones: f.bb, noContentIndex: true,
		TickInterval: time.Hour, allowLoopback: true, listenHost: "127.0.0.1",
	}
	if mut != nil {
		mut(&deps)
	}
	f.instance = &instance{svc: New(deps), dataDir: dir, meta: nil, db: db, nets: nets, recs: recs, cur: cur}
	return f
}

// seedFile indexes pubPath and gives it metadata and import history.
func (f *pubFixture) seedFile(t *testing.T, mods ...func(*seed)) {
	t.Helper()
	s := &seed{path: pubPath, release: pubRelease, file: pubFile, size: 1 << 30, nzbName: "Some.Movie.2020.1080p.nzb",
		category: "movies", indexer: "IndexerOne", postDate: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	s.sourceNzb = "/nzbs/Some.Movie.2020.1080p.nzb"
	for _, m := range mods {
		m(s)
	}
	ctx := context.Background()
	if s.release != "" || s.file != "" {
		if err := f.content.Upsert(ctx, database.PeernetContentEntry{FilePath: s.path, FileID: s.file, ReleaseID: s.release, ReleaseScheme: s.scheme}); err != nil {
			t.Fatal(err)
		}
	}
	f.meta.mu.Lock()
	f.meta.files[s.path] = &metapb.FileMetadata{FileSize: s.size, SourceNzbPath: s.sourceNzb, Password: s.password, ReleaseDate: s.postDate.Unix()}
	f.meta.mu.Unlock()
	if s.nzbName != "" || s.category != "" || s.indexer != "" {
		_, err := f.db.Connection().ExecContext(ctx,
			`INSERT INTO import_history (nzb_name, file_name, file_size, virtual_path, category, indexer, download_id, metadata)
			 VALUES (?, 'movie.mkv', ?, ?, ?, ?, ?, ?)`,
			s.nzbName, s.size, s.path, nullIfEmpty(s.category), nullIfEmpty(s.indexer), s.downloadID, nullIfEmpty(s.metadata))
		if err != nil {
			t.Fatal(err)
		}
	}
}

type seed struct {
	path, release, file, nzbName, category, indexer, downloadID, metadata string
	sourceNzb, password                                                   string
	size                                                                  int64
	postDate                                                              time.Time
	// scheme is the stored release scheme (0 means nzb when a release is set).
	scheme int
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func healthyNotice() HealthyNotice {
	return HealthyNotice{FilePath: pubPath, CheckedAt: time.Now().Add(-time.Minute),
		TotalSegments: 100, SegmentsChecked: 100, FullCheck: true, MediaHeaderVerified: true, PatchKnown: true}
}

// drain waits until the publishing queue is empty and idle.
func (f *pubFixture) drain(t *testing.T) {
	t.Helper()
	n := f.node(t)
	eventually(t, "publishing queue to drain", func() bool { return n.pipe.idle() })
}

func (f *pubFixture) status(t *testing.T) Status {
	t.Helper()
	st, err := f.svc.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func (f *pubFixture) openNetStatus(t *testing.T) NetworkStatus {
	t.Helper()
	for _, n := range f.status(t).Networks {
		if n.Kind == KindOpen {
			return n
		}
	}
	t.Fatal("no open network")
	return NetworkStatus{}
}

// awaitFirstUpkeep waits for the first background upkeep pass, which starts
// right after the node does and re-announces due records on its own. Tests
// that call reannounce themselves must not overlap with it (the later passes
// are an hour away).
func awaitFirstUpkeep(t *testing.T, f *pubFixture) {
	t.Helper()
	n := f.node(t)
	eventually(t, "first upkeep pass", func() bool { return n.maintRuns.Load() >= 1 })
}
