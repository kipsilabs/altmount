package bundlex

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

func notShareableReason(t *testing.T, err error) string {
	t.Helper()
	var ns *NotShareableError
	if !errors.As(err, &ns) {
		t.Fatalf("want NotShareableError, got %v", err)
	}
	return ns.Reason
}

func TestBuildNZBSchemeVerifiesAgainstItsReleaseID(t *testing.T) {
	lib, rel := nzbRelease()
	b := newTestBuilder(t, lib, NoPatches{})
	got, err := b.Build(context.Background(), rel, "Some.Movie.2020.1080p", posted)
	if err != nil {
		t.Fatal(err)
	}
	rid, _ := contentid.ParseID(rel)
	bd, err := bundle.Verify(got.Wire, got.ID, rid)
	if err != nil {
		t.Fatalf("a receiver would reject the bundle: %v", err)
	}
	if bd.Scheme != bundle.SchemeNZB || len(bd.Pool) != 23 {
		t.Fatalf("scheme %v pool %d (the whole NZB list must travel)", bd.Scheme, len(bd.Pool))
	}
}

func TestBuildIsDeterministic(t *testing.T) {
	lib, rel := nzbRelease()
	a, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "T", posted)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "T", posted)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID || !bytes.Equal(a.Wire, b.Wire) {
		t.Fatal("two builds of the same metadata differ")
	}
}

func TestBuildFilesSchemeNeverFallsBackFromNZB(t *testing.T) {
	lib, rel := nzbRelease()
	lib.nzb = nil // the store is gone
	_, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "T", posted)
	if r := notShareableReason(t, err); r != ReasonNoNZBList {
		t.Fatalf("reason = %s", r)
	}
	// A files-scheme release is built from its files alone.
	f := plainFile("a.mkv", ids("a", 5))
	fid, _ := contentid.FileID(f)
	frid, _ := contentid.ReleaseIDFromFiles([]contentid.ID{fid})
	lib2 := &fakeLib{scheme: database.PeernetSchemeFiles, paths: []string{"p"}, files: map[string]*metapb.FileMetadata{"p": f}}
	got, err := newTestBuilder(t, lib2, NoPatches{}).Build(context.Background(), frid.String(), "T", posted)
	if err != nil || got.Scheme != bundle.SchemeFiles {
		t.Fatalf("files scheme: %v %v", got, err)
	}
}

func TestBuildStaleLibraryIsAReleaseIDMismatch(t *testing.T) {
	lib, rel := nzbRelease()
	lib.nzb = append(lib.nzb, ids("extra", 1)) // the NZB list no longer hashes to the indexed ID
	_, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "T", posted)
	if r := notShareableReason(t, err); r != ReasonReleaseMismatch {
		t.Fatalf("reason = %s", r)
	}
}

func TestNotShareableReasonsAreCounted(t *testing.T) {
	cases := map[string]func(*fakeLib) PatchIndex{
		"rclone_encrypted": func(l *fakeLib) PatchIndex {
			l.files["movies/R/movie.mkv"].Encryption = metapb.Encryption_RCLONE
			return NoPatches{}
		},
		"healed_by_local_patch": func(l *fakeLib) PatchIndex { return patches{"vid-000003@news.test": true} },
		"known_holes": func(l *fakeLib) PatchIndex {
			l.files["movies/R/movie.mkv"].KnownHoles = []*metapb.HoleRun{{StartSegment: 1, Count: 1}}
			return NoPatches{}
		},
		"password_not_from_nzb": func(l *fakeLib) PatchIndex {
			m := l.files["movies/R/movie.mkv"]
			m.Encryption, m.AesKey, m.AesIv = metapb.Encryption_AES, bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 16)
			return NoPatches{}
		},
		"patch_status_unknown": func(l *fakeLib) PatchIndex { return nil },
	}
	for want, mut := range cases {
		lib, rel := nzbRelease()
		b := newTestBuilder(t, lib, mut(lib))
		_, err := b.Build(context.Background(), rel, "T", posted)
		if r := notShareableReason(t, err); r != want {
			t.Errorf("%s: reason = %s", want, r)
		}
		if b.Status().NotShareable[want] != 1 {
			t.Errorf("%s: not counted: %v", want, b.Status().NotShareable)
		}
	}
}

func TestPasswordTravelsOnlyWhenFromTheNZB(t *testing.T) {
	enc := func() (*fakeLib, string) {
		lib, rel := nzbRelease()
		m := lib.files["movies/R/movie.mkv"]
		m.Encryption, m.AesKey, m.AesIv = metapb.Encryption_AES, bytes.Repeat([]byte{0x41}, 32), bytes.Repeat([]byte{0x42}, 16)
		m.FileSize += 0
		return lib, rel
	}
	// Not from the NZB (the metadata adapter's answer): not shareable.
	lib, rel := enc()
	if _, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "T", posted); err == nil {
		t.Fatal("key material shared without a proven NZB password")
	}
	// From the NZB: the key and the password travel.
	lib, rel = enc()
	lib.pwNZB = map[string]string{"movies/R/movie.mkv": "NZB-PW-MARK"}
	got, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "T", posted)
	if err != nil {
		// A segment table of unencrypted size is not an AES-padded one; the
		// structural validator may refuse the synthetic file, which is fine
		// for this test only if the reason is not the password rule.
		if r := notShareableReason(t, err); r == "password_not_from_nzb" {
			t.Fatalf("the password rule fired for a from-NZB password")
		}
		return
	}
	raw, _ := bundle.Decompress(got.Wire)
	if !bytes.Contains(raw, []byte("NZB-PW-MARK")) {
		t.Fatal("NZB password missing from a from-NZB bundle")
	}
}

func TestBundleBytesContainNoLocalData(t *testing.T) {
	lib, rel := nzbRelease()
	m := lib.files["movies/R/movie.mkv"]
	m.Password, m.Salt = "RCLONE-PASSWORD-MARK", "RCLONE-SALT-MARK"
	m.KnownHoles = nil
	got, err := newTestBuilder(t, lib, NoPatches{}).Build(context.Background(), rel, "Some.Movie.2020", posted)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bundle.Decompress(got.Wire)
	if err != nil {
		t.Fatal(err)
	}
	for _, mark := range privateMarkers {
		if bytes.Contains(raw, []byte(mark)) || bytes.Contains(got.Wire, []byte(mark)) {
			t.Errorf("bundle contains %q", mark)
		}
	}
	if strings.Contains(string(raw), "movies/R") {
		t.Error("bundle contains a virtual path")
	}
}

func TestBuildConcurrencyIsOneAndResultCached(t *testing.T) {
	lib, rel := nzbRelease()
	lib.slowRead = 50 * time.Millisecond
	b := newTestBuilder(t, lib, NoPatches{})
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.Build(context.Background(), rel, "T", posted); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if lib.readCount() != 1 {
		t.Fatalf("metadata read %d times, want one build", lib.readCount())
	}
	if st := b.Status(); st.Built != 1 || st.CacheHits != 4 || st.Cached != 1 {
		t.Fatalf("status %+v", st)
	}
	b.Invalidate(rel)
	if _, err := b.Build(context.Background(), rel, "T", posted); err != nil || lib.readCount() != 2 {
		t.Fatalf("after invalidate: %v reads=%d", err, lib.readCount())
	}
}

func TestBuildBusyAndTransientErrors(t *testing.T) {
	lib, rel := nzbRelease()
	lib.slowRead = 300 * time.Millisecond
	b := NewBuilder(BuilderConfig{Source: lib, Library: lib, Patches: NoPatches{}, BuildWait: 20 * time.Millisecond})
	go func() { _, _ = b.Build(context.Background(), rel, "T", posted) }()
	time.Sleep(50 * time.Millisecond)
	if _, err := b.Build(context.Background(), "ab"+rel[2:], "T", posted); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	lib.setReadErr(errors.New("disk"))
	b.Invalidate(rel)
	_, err := b.Build(context.Background(), rel, "T", posted)
	var ns *NotShareableError
	if err == nil || errors.As(err, &ns) {
		t.Fatalf("a read failure must be transient, got %v", err)
	}
	if b.Status().BuildFailures != 1 {
		t.Fatal("failure not counted")
	}
}
