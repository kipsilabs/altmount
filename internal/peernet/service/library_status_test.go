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
		{FilePath: "a.mkv", FileID: hex("a"), ReleaseID: hex("1")},
		{FilePath: "b.mkv", FileID: hex("b"), ReleaseID: hex("1")},
		{FilePath: "c.mkv", FileID: hex("c")},
	} {
		if err := repo.Upsert(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	c := &contentIndex{deps: Deps{Content: repo}}
	c.skipped.Store(2)
	got := c.library(ctx)
	want := LibraryStatus{Files: 5, WithReleaseID: 2, FileIDOnly: 1, NotIndexable: 2}
	if got != want {
		t.Fatalf("library = %+v, want %+v", got, want)
	}
}
