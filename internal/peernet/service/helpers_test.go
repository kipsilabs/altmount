package service

import (
	"context"
	"io/fs"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// fakeMeta is an empty metadata tree that records the observer.
type fakeMeta struct {
	mu       sync.Mutex
	observer metadata.FileObserver
	sets     int
	walks    int // root directory listings (one per backfill pass)
}

func (f *fakeMeta) ReadFileMetadata(string) (*metapb.FileMetadata, error) { return nil, nil }
func (f *fakeMeta) ReadStore(string) (*metapb.NzbStore, error)            { return nil, nil }
func (f *fakeMeta) ListDirectoryAll(dir string) ([]fs.FileInfo, []string, error) {
	if dir == "" {
		f.mu.Lock()
		f.walks++
		f.mu.Unlock()
	}
	return nil, nil, nil
}
func (f *fakeMeta) SetFileObserver(o metadata.FileObserver) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observer = o
	f.sets++
}
func (f *fakeMeta) hasObserver() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.observer != nil
}

type instance struct {
	svc     *Service
	dataDir string
	meta    *fakeMeta
	db      *database.DB
	nets    *database.PeernetNetworkRepository
	recs    *database.PeernetRecordRepository
	cur     *database.PeernetSyncCursorRepository
}

func newInstance(t *testing.T, set Settings) *instance {
	t.Helper()
	dir := t.TempDir()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(dir, "alt.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, d := db.Connection(), db.Dialect()
	meta := &fakeMeta{}
	nets := database.NewPeernetNetworkRepository(conn, d)
	recs := database.NewPeernetRecordRepository(conn, d)
	cur := database.NewPeernetSyncCursorRepository(conn, d)
	svc := New(Deps{
		DataDir: dir, Networks: nets, Peers: database.NewPeernetPeerRepository(conn, d), Records: recs, Cursors: cur,
		Content: database.NewPeernetContentRepository(conn, d), Metadata: meta, Settings: set,
		TickInterval: time.Hour, allowLoopback: true, listenHost: "127.0.0.1",
	})
	return &instance{svc: svc, dataDir: dir, meta: meta, db: db, nets: nets, recs: recs, cur: cur}
}

func (i *instance) start(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	i.svc.Start(ctx)
	t.Cleanup(func() {
		i.svc.Stop(context.Background())
		cancel()
	})
	return ctx
}

func (i *instance) node(t *testing.T) *node {
	t.Helper()
	n := i.svc.currentNode()
	if n == nil {
		t.Fatal("service is not running")
	}
	return n
}

func (i *instance) port(t *testing.T) int {
	t.Helper()
	st, err := i.svc.Status(context.Background())
	if err != nil || !st.Listening {
		t.Fatalf("not listening: %+v err=%v", st, err)
	}
	return st.ListenPort
}

func (i *instance) addr(t *testing.T) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(i.port(t)))
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (f *fakeMeta) walkCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.walks
}
