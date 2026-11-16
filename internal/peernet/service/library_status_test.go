package service

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
)

func TestLibraryStatusCountsShareableFiles(t *testing.T) {
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repo := database.NewPeernetContentRepository(db.Connection(), db.Dialect())
	hex := func(c string) string { return strings.Repeat(c, 64) }
	ctx := context.Background()
	for _, e := range []database.PeernetContentEntry{
		{FilePath: "a.mkv", FileID: hex("a"), ReleaseID: hex("1"), ReleaseScheme: database.PeernetSchemeNZB},
		{FilePath: "b.mkv", FileID: hex("b"), ReleaseID: hex("2"), ReleaseScheme: database.PeernetSchemeFiles, GroupKey: hex("d")},
		{FilePath: "c.mkv", FileID: hex("c")},                     // no source NZB in its metadata
		{FilePath: "d.mkv", FileID: hex("d"), GroupKey: hex("e")}, // in a group over the cap
		{FilePath: "e.mkv", FileID: hex("e"), GroupKey: hex("f")}, // waiting for the next derive pass
	} {
		if err := repo.Upsert(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	c := &contentIndex{deps: Deps{Content: repo}}
	c.skipped.Store(2)
	c.oversize.Store(1)
	got := c.library(ctx)
	want := LibraryStatus{Files: 7, WithReleaseID: 2, WithNZBReleaseID: 1, WithDerivedReleaseID: 1, FileIDOnly: 3, NotIndexable: 2,
		NoSourceNZB: 1, GroupTooLarge: 1, Pending: 1}
	if got != want {
		t.Fatalf("library = %+v, want %+v", got, want)
	}
}
