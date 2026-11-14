package bundlex

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

type fakeOrigins struct {
	from map[string]bool
	err  error
}

func (f fakeOrigins) FromNZB(_ context.Context, id string) (bool, error) { return f.from[id], f.err }

// keyedFile is an AES-keyed file whose segments cover its padded size.
func keyedFile(segs []string) *metapb.FileMetadata {
	m := plainFile("movie.mkv", segs)
	m.FileSize -= 5
	m.StoreRef, m.SourceNzbPath = "", ""
	m.Encryption, m.AesKey, m.AesIv = metapb.Encryption_AES, bytes.Repeat([]byte{0x41}, 32), bytes.Repeat([]byte{0x42}, 16)
	return m
}

type pathLib struct{ paths []string }

func (l pathLib) ReleaseFiles(context.Context, string) (int, []string, error) {
	return database.PeernetSchemeFiles, l.paths, nil
}

func fileIDOf(t *testing.T, m *metapb.FileMetadata) string {
	t.Helper()
	id, err := contentid.FileID(m)
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

func TestMetadataSourcePasswordOriginComesFromTheRecordedFlag(t *testing.T) {
	ms := metadata.NewMetadataService(t.TempDir())
	m := keyedFile(ids("k", 3))
	if err := ms.WriteFileMetadata("movies/R/movie.mkv", m); err != nil {
		t.Fatal(err)
	}
	read, err := ms.ReadFileMetadata("movies/R/movie.mkv")
	if err != nil {
		t.Fatal(err)
	}
	flagged := fakeOrigins{from: map[string]bool{fileIDOf(t, read): true}}

	if pw, ok := NewMetadataSource(ms, flagged).NZBPassword("movies/R/movie.mkv", read); !ok || pw != "" {
		t.Fatalf("flagged file: (%q, %v); the password itself is never known", pw, ok)
	}
	if _, ok := NewMetadataSource(ms, fakeOrigins{}).NZBPassword("movies/R/movie.mkv", read); ok {
		t.Fatal("an unflagged file (every pre-existing import) must not count as from the NZB")
	}
	if _, ok := NewMetadataSource(ms, fakeOrigins{from: flagged.from, err: errors.New("db down")}).NZBPassword("p", read); ok {
		t.Fatal("a failed lookup must fail closed")
	}
	if _, ok := NewMetadataSource(ms, nil).NZBPassword("p", read); ok {
		t.Fatal("no origin store: never from the NZB")
	}
	// A move or rename changes the path, not the File ID: the flag follows.
	if err := ms.WriteFileMetadata("tv/Moved/other-name.mkv", m); err != nil {
		t.Fatal(err)
	}
	moved, _ := ms.ReadFileMetadata("tv/Moved/other-name.mkv")
	if _, ok := NewMetadataSource(ms, flagged).NZBPassword("tv/Moved/other-name.mkv", moved); !ok {
		t.Fatal("the flag did not survive a move")
	}
}

func TestKeyedReleaseBuildsOnlyWithTheRecordedFlag(t *testing.T) {
	ms := metadata.NewMetadataService(t.TempDir())
	if err := ms.WriteFileMetadata("movies/R/movie.mkv", keyedFile(ids("k", 3))); err != nil {
		t.Fatal(err)
	}
	read, _ := ms.ReadFileMetadata("movies/R/movie.mkv")
	fid, _ := contentid.ParseID(fileIDOf(t, read))
	rel, err := contentid.ReleaseIDFromFiles([]contentid.ID{fid})
	if err != nil {
		t.Fatal(err)
	}
	build := func(o PasswordOrigins) (*Built, error) {
		b := NewBuilder(BuilderConfig{Source: NewMetadataSource(ms, o), Library: pathLib{paths: []string{"movies/R/movie.mkv"}}, Patches: NoPatches{}})
		return b.Build(context.Background(), rel.String(), "T", posted)
	}
	if _, err := build(fakeOrigins{}); notShareableReason(t, err) != "password_not_from_nzb" {
		t.Fatalf("without the flag: %v", err)
	}
	built, err := build(fakeOrigins{from: map[string]bool{fid.String(): true}})
	if err != nil {
		t.Fatalf("with the flag: %v", err)
	}
	got, err := bundle.Verify(built.Wire, built.ID, rel)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Files) != 1 || !bytes.Equal(got.Files[0].AESKey, bytes.Repeat([]byte{0x41}, 32)) || !bytes.Equal(got.Files[0].AESIV, bytes.Repeat([]byte{0x42}, 16)) {
		t.Fatalf("the key and IV must travel: %+v", got.Files)
	}
	if got.NZBPassword != "" {
		t.Fatal("the password text is not known to the node and must not appear")
	}
}
