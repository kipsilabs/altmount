package service

import (
	"errors"
	"reflect"
	"testing"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

type stubMeta struct {
	m   *metapb.FileMetadata
	err error
}

func (s stubMeta) ReadFileMetadata(string) (*metapb.FileMetadata, error) { return s.m, s.err }

func TestReleaseFileReaderCopiesOnlyTheNarrowFields(t *testing.T) {
	m := &metapb.FileMetadata{FileSize: 12, ReleaseDate: 34, SourceNzbPath: "a/b.nzb", Password: "pw",
		AesKey: []byte("key")}
	got, err := NewReleaseFileReader(stubMeta{m: m}).ReadReleaseFile("x")
	if err != nil || got == nil {
		t.Fatalf("%v %v", got, err)
	}
	want := ReleaseFile{FileSize: 12, ReleaseDate: 34, SourceNzbPath: "a/b.nzb", Password: "pw"}
	if *got != want {
		t.Fatalf("got %+v", *got)
	}
	// A guard against the view growing back into the full record.
	if n := reflect.TypeOf(ReleaseFile{}).NumField(); n != 4 {
		t.Fatalf("ReleaseFile has %d fields; keep it to what release information needs", n)
	}
}

func TestReleaseFileReaderAbsentAndError(t *testing.T) {
	if got, err := NewReleaseFileReader(stubMeta{}).ReadReleaseFile("x"); got != nil || err != nil {
		t.Fatalf("absent: %v %v", got, err)
	}
	boom := errors.New("boom")
	if got, err := NewReleaseFileReader(stubMeta{err: boom}).ReadReleaseFile("x"); got != nil || !errors.Is(err, boom) {
		t.Fatalf("error: %v %v", got, err)
	}
}
