package bundle

import (
	"bytes"
	"errors"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

func pack(t testing.TB, b *Bundle) ([]byte, BundleID) {
	t.Helper()
	wire, id, err := Pack(b)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	return wire, id
}

func TestVerifyAcceptsBothSchemes(t *testing.T) {
	for name, b := range map[string]*Bundle{"files": goodBundle(t), "nzb": goodNZBBundle(t)} {
		wire, id := pack(t, b)
		got, err := Verify(wire, id, b.ReleaseID)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.ReleaseID != b.ReleaseID || len(got.Files) != len(b.Files) {
			t.Fatalf("%s: wrong bundle returned", name)
		}
	}
}

func TestPackIDIsHashOfCanonicalBytes(t *testing.T) {
	b := goodBundle(t)
	wire, id := pack(t, b)
	if id != ComputeID(mustEncode(t, b)) {
		t.Fatal("Pack id is not the hash of the canonical bytes")
	}
	raw, err := Decompress(wire)
	if err != nil || !bytes.Equal(raw, mustEncode(t, b)) {
		t.Fatalf("wire does not decompress to the canonical bytes: %v", err)
	}
}

func TestVerifyRejects(t *testing.T) {
	good := goodBundle(t)
	wire, id := pack(t, good)
	other := clone(good)
	other.Title = "Another title"
	otherWire, otherID := pack(t, other)

	badFileID := clone(good)
	badFileID.Files[0].FileID[0] ^= 0xFF
	// keep the files sorted so only the File ID claim is wrong
	badFileID.Files = sortedByID(badFileID.Files)
	badFileWire, badFileBID := pack(t, badFileID)

	// A bundle whose embedded Release ID is the announced one but whose
	// content hashes to something else.
	liar := clone(good)
	liar.Files = liar.Files[:len(liar.Files)-1]
	liar.Pool = nil
	liar = rebuildPoolDropping(t, liar)
	liarWire, liarBID := pack(t, liar)

	nzbStyle := nzbStyleReleaseID(t, good)
	nzbB := goodNZBBundle(t)
	nzbWire, nzbBID := pack(t, nzbB)
	filesStyleOfNZB := testFilesRelease(fileIDs(nzbB))

	junk, _ := Compress([]byte("not a bundle at all"))

	tests := []struct {
		name string
		wire []byte
		bid  BundleID
		rid  contentid.ID
		want error
	}{
		{"substituted bundle", otherWire, id, good.ReleaseID, ErrBundleIDMismatch},
		{"substituted bundle, valid for its own id but not the announced release", otherWire, otherID, contentid.ID{9}, ErrReleaseIDMismatch},
		{"wrong announced release id", wire, id, contentid.ID{9}, ErrReleaseIDMismatch},
		{"files bundle with an nzb-style release id", wire, id, nzbStyle, ErrReleaseIDMismatch},
		{"nzb bundle with a files-style release id", nzbWire, nzbBID, filesStyleOfNZB, ErrReleaseIDMismatch},
		{"wrong file id inside the bundle", badFileWire, badFileBID, good.ReleaseID, ErrFileIDMismatch},
		{"embedded release id differs from what the content hashes to", liarWire, liarBID, liar.ReleaseID, ErrReleaseIDMismatch},
		{"announced bundle id checked before decoding", junk, BundleID{1}, good.ReleaseID, ErrBundleIDMismatch},
		{"junk with its own correct id", junk, ComputeID([]byte("not a bundle at all")), good.ReleaseID, ErrMalformed},
		{"oversized wire", make([]byte, MaxCompressedSize+1), id, good.ReleaseID, ErrTooLarge},
		{"empty wire", nil, id, good.ReleaseID, ErrMalformed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Verify(tc.wire, tc.bid, tc.rid); !errors.Is(err, tc.want) {
				t.Fatalf("Verify = %v, want %v", err, tc.want)
			}
		})
	}
}

func sortedByID(fs []File) []File {
	out := append([]File(nil), fs...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && bytes.Compare(out[j-1].FileID[:], out[j].FileID[:]) > 0; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func fileIDs(b *Bundle) []contentid.ID {
	var ids []contentid.ID
	for _, f := range b.Files {
		ids = append(ids, f.FileID)
	}
	return ids
}

// nzbStyleReleaseID is the nzb-scheme Release ID over the bundle's segments
// laid out one NZB file per bundle file.
func nzbStyleReleaseID(t testing.TB, b *Bundle) contentid.ID {
	t.Helper()
	var files [][]string
	for _, f := range b.Files {
		var ids []string
		for _, s := range f.Segments {
			if s.Ref != GapRef {
				ids = append(ids, b.Pool[s.Ref])
			}
		}
		files = append(files, ids)
	}
	id, err := contentid.ReleaseID(nzbStore(files))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// rebuildPoolDropping keeps b.ReleaseID (now stale) but rebuilds a valid pool
// for the remaining files.
func rebuildPoolDropping(t testing.TB, b *Bundle) *Bundle {
	t.Helper()
	var specs []specFile
	keep := map[string]bool{}
	for _, f := range b.Files {
		keep[f.Name] = true
	}
	for _, s := range testSpecs() {
		if keep[s.name] {
			specs = append(specs, s)
		}
	}
	n := assemble(t, specs)
	n.ReleaseID = b.ReleaseID
	return n
}

func TestVerifyWithCustomFilesReleaseID(t *testing.T) {
	b := goodBundle(t)
	custom := contentid.ID{7, 7, 7}
	b.ReleaseID = custom
	wire, id := pack(t, b)
	called := 0
	opts := Options{FilesReleaseID: func(ids []contentid.ID) contentid.ID {
		called++
		if len(ids) != len(b.Files) {
			t.Errorf("got %d ids", len(ids))
		}
		for i := 1; i < len(ids); i++ {
			if bytes.Compare(ids[i-1][:], ids[i][:]) >= 0 {
				t.Error("ids not sorted ascending")
			}
		}
		return custom
	}}
	if _, err := VerifyWith(wire, id, custom, opts); err != nil || called != 1 {
		t.Fatalf("err %v called %d", err, called)
	}
	if _, err := Verify(wire, id, custom); !errors.Is(err, ErrReleaseIDMismatch) {
		t.Fatalf("default function must not accept the custom id: %v", err)
	}
}

func TestFilesReleaseID(t *testing.T) {
	ids := []contentid.ID{{3}, {1}, {2}}
	want := testFilesRelease(ids)
	if got := FilesReleaseID(ids); got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	if ids[0] != (contentid.ID{3}) {
		t.Fatal("input was reordered")
	}
	if FilesReleaseID([]contentid.ID{{1}}) == FilesReleaseID([]contentid.ID{{2}}) {
		t.Fatal("collision")
	}
}

func TestVerifyCanonical(t *testing.T) {
	b := goodNZBBundle(t)
	raw := mustEncode(t, b)
	if _, err := VerifyCanonical(raw, ComputeID(raw), b.ReleaseID, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyCanonical(raw, BundleID{}, b.ReleaseID, Options{}); !errors.Is(err, ErrBundleIDMismatch) {
		t.Fatal(err)
	}
}

func TestVerifyNZBSchemeRecomputesTheFullList(t *testing.T) {
	// Dropping the non-imported NZB file changes the release id, so a peer
	// cannot trim the list and keep the announced identity.
	b := goodNZBBundle(t)
	trimmed := assembleNZB(t, testSpecs(), nzbLayout()[:5])
	trimmed.ReleaseID = b.ReleaseID
	wire, id := pack(t, trimmed)
	if _, err := Verify(wire, id, b.ReleaseID); !errors.Is(err, ErrReleaseIDMismatch) {
		t.Fatalf("got %v", err)
	}
}
