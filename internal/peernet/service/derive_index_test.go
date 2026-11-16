package service

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

func hx(c string) string { return strings.Repeat(c, 64) }

func testDB(t *testing.T) *database.DB {
	t.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// A superseded derived release ID takes this node's own records and publish
// state with it, in every network, and nothing else.
func TestContentIndexWithdrawRemovesOwnRecordsAndState(t *testing.T) {
	db := testDB(t)
	conn, d := db.Connection(), db.Dialect()
	recs := database.NewPeernetRecordRepository(conn, d)
	state := database.NewPeernetPublishStateRepository(conn, d)
	ctx := context.Background()
	now := time.Now()
	put := func(nw, rel, hash string, source int) {
		t.Helper()
		_, err := recs.Insert(ctx, database.PeernetRecord{
			NetworkID: nw, Kind: database.PeernetRecordKindRelease, ContentHash: hash, ReleaseID: rel, PublisherNodeID: hx("9"),
			CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Source: source, Title: "t", Payload: []byte("p"),
			ReleaseScheme: database.PeernetSchemeFiles,
		}, database.PeernetRecordLimits{})
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Put(ctx, database.PeernetPublishState{NetworkID: nw, Kind: database.PeernetRecordKindRelease, ReleaseID: rel, PublishedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	put(hx("a"), hx("1"), hx("c"), database.PeernetRecordSourceLocal)
	put(hx("b"), hx("1"), hx("d"), database.PeernetRecordSourceLocal)
	put(hx("a"), hx("2"), hx("e"), database.PeernetRecordSourceLocal)

	c := &contentIndex{deps: Deps{Records: recs, PublishState: state}}
	c.withdraw(ctx, []string{hx("1")})

	for _, nw := range []string{hx("a"), hx("b")} {
		if _, ok, _ := state.Get(ctx, nw, database.PeernetRecordKindRelease, hx("1"), ""); ok {
			t.Errorf("publish state of the superseded release survived in %s", nw[:4])
		}
	}
	if _, ok, _ := state.Get(ctx, hx("a"), database.PeernetRecordKindRelease, hx("2"), ""); !ok {
		t.Error("another release's state was removed")
	}
	pa, _ := recs.ListOwnSince(ctx, hx("a"), 0, 10)
	pb, _ := recs.ListOwnSince(ctx, hx("b"), 0, 10)
	if len(pa.Rows) != 1 || len(pb.Rows) != 0 {
		t.Errorf("own records left: network a %d (want 1), network b %d (want 0)", len(pa.Rows), len(pb.Rows))
	}
}

// Writing metadata for an old-format import gives its files a derived release
// ID without a restart, and growing the import supersedes the earlier ID.
func TestContentIndexDerivesIncrementally(t *testing.T) {
	db := testDB(t)
	repo := database.NewPeernetContentRepository(db.Connection(), db.Dialect())
	ms := metadata.NewMetadataService(t.TempDir())
	deps := Deps{Content: repo, Metadata: NewMetadataHost(ms), deriveDebounce: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	c := startContentIndex(ctx, deps)
	if c == nil {
		t.Fatal("no content index")
	}
	t.Cleanup(func() { cancel(); c.stop() })
	waitFor(t, "the initial pass", func() bool { return !c.running.Load() })

	write := func(p string, ids ...string) {
		t.Helper()
		segs := make([]*metapb.SegmentData, len(ids))
		for i, id := range ids {
			segs[i] = &metapb.SegmentData{Id: id, SegmentSize: 10, EndOffset: 9}
		}
		m := &metapb.FileMetadata{FileSize: 10, SegmentData: segs, SourceNzbPath: "/data/.nzbs/5/x.nzb.gz"}
		if err := ms.WriteFileMetadata(p, m); err != nil {
			t.Fatal(err)
		}
	}
	releaseOf := func(p string) string {
		e, _, _ := repo.GetByPath(ctx, p)
		return e.ReleaseID
	}
	write("c/r/a.mkv", "a1")
	waitFor(t, "first file derived", func() bool { return releaseOf("c/r/a.mkv") != "" })
	first := releaseOf("c/r/a.mkv")
	e, _, _ := repo.GetByPath(ctx, "c/r/a.mkv")
	if e.ReleaseScheme != database.PeernetSchemeFiles {
		t.Fatalf("scheme %d, want files", e.ReleaseScheme)
	}
	write("c/r/b.mkv", "b1")
	waitFor(t, "second file joins the release", func() bool {
		return releaseOf("c/r/b.mkv") != "" && releaseOf("c/r/a.mkv") == releaseOf("c/r/b.mkv")
	})
	if releaseOf("c/r/a.mkv") == first {
		t.Fatal("the release ID did not change when the file set did")
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
