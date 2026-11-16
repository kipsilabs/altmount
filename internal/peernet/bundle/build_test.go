package bundle

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"google.golang.org/protobuf/proto"
)

func releaseInput(specs []specFile) ReleaseInput {
	in := ReleaseInput{Scheme: SchemeFiles, Title: "Some Release 2024", PostedAt: time.Unix(1700000000, 0)}
	for _, s := range specs {
		in.Files = append(in.Files, s.input())
	}
	return in
}

func TestFromLocalMatchesIndependentAssembly(t *testing.T) {
	got, err := FromLocal(releaseInput(testSpecs()))
	if err != nil {
		t.Fatalf("FromLocal: %v", err)
	}
	if !bytes.Equal(mustEncode(t, got), mustEncode(t, goodBundle(t))) {
		t.Fatal("files scheme: FromLocal differs from the independently assembled bundle")
	}

	in := releaseInput(testSpecs())
	in.Scheme, in.NZBFiles = SchemeNZB, nzbLayout()
	got, err = FromLocal(in)
	if err != nil {
		t.Fatalf("FromLocal nzb: %v", err)
	}
	if !bytes.Equal(mustEncode(t, got), mustEncode(t, goodNZBBundle(t))) {
		t.Fatal("nzb scheme: FromLocal differs from the independently assembled bundle")
	}
}

func TestFromLocalIsInputOrderIndependent(t *testing.T) {
	specs := testSpecs()
	a, err := FromLocal(releaseInput(specs))
	if err != nil {
		t.Fatal(err)
	}
	rev := make([]specFile, len(specs))
	for i := range specs {
		rev[len(specs)-1-i] = specs[i]
	}
	b, err := FromLocal(releaseInput(rev))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mustEncode(t, a), mustEncode(t, b)) {
		t.Fatal("file order changed the bundle")
	}
}

func TestFromLocalNotShareable(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(in *ReleaseInput)
		reason ShareReason
	}{
		{"rclone encrypted file", func(in *ReleaseInput) {
			m := in.Files[0].Meta
			m.Encryption, m.Password, m.Salt = metapb.Encryption_RCLONE, "RCLONEPW", "RCLONESALT"
		}, ReasonRclone},
		{"header encryption", func(in *ReleaseInput) { in.Files[0].Meta.Encryption = metapb.Encryption_HEADERS }, ReasonOtherEncryption},
		{"aes file whose password is not from the nzb", func(in *ReleaseInput) {
			for i := range in.Files {
				if len(in.Files[i].Meta.AesKey) > 0 {
					in.Files[i].PasswordFromNZB = false
				}
			}
		}, ReasonPasswordNotNZB},
		{"nested encrypted outer whose password is not from the nzb", func(in *ReleaseInput) {
			for i := range in.Files {
				if len(in.Files[i].Meta.SharedOuterSources) > 0 && len(in.Files[i].Meta.SharedOuterSources[0].AesKey) > 0 {
					in.Files[i].PasswordFromNZB = false
				}
			}
		}, ReasonPasswordNotNZB},
		{"healed by local par2 patches", func(in *ReleaseInput) { in.Files[0].PatchedLocally = true }, ReasonLocalPatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := releaseInput(testSpecs())
			tc.mutate(&in)
			_, err := FromLocal(in)
			if !errors.Is(err, ErrNotShareable) {
				t.Fatalf("err = %v, want ErrNotShareable", err)
			}
			var ns *NotShareableError
			if !errors.As(err, &ns) || ns.Reason != tc.reason {
				t.Fatalf("reason = %+v, want %s", ns, tc.reason)
			}
		})
	}
}

func TestFromLocalPlainFileNeedsNoPasswordFlag(t *testing.T) {
	in := releaseInput([]specFile{testSpecs()[0]})
	in.Files[0].PasswordFromNZB = false
	if _, err := FromLocal(in); err != nil {
		t.Fatalf("plain file with the flag unset: %v", err)
	}
}

func TestEncodedBundleNeverHoldsLocalSecrets(t *testing.T) {
	in := releaseInput(testSpecs())
	secrets := []string{"RCLONEPW", "RCLONESALT", "/local/store/path.nzbz", "store-ref-abc", "dl-id-123",
		"provider-fingerprint-secret", "usenet.provider.example", "radarr-main", "NZBGeek", "movies-cat"}
	for i := range in.Files {
		m := in.Files[i].Meta
		// A stray password and salt on a non-rclone file must not travel.
		m.Password, m.Salt = "RCLONEPW", "RCLONESALT"
		m.StoreRef = "store-ref-abc"
		m.SourceNzbPath = "/local/store/path.nzbz"
	}
	in.NZBPassword = "nzb-pass"
	b, err := FromLocal(in)
	if err != nil {
		t.Fatal(err)
	}
	raw := mustEncode(t, b)
	wire, err := Compress(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range secrets {
		if bytes.Contains(raw, []byte(s)) {
			t.Errorf("canonical bytes contain %q", s)
		}
	}
	for _, ts := range []int64{1700000001} { // local modified/created time
		if bytes.Contains(raw, appendVarintBytes(ts)) {
			t.Errorf("canonical bytes contain the local timestamp %d", ts)
		}
	}
	if !bytes.Contains(raw, []byte("nzb-pass")) {
		t.Error("the NZB password was dropped")
	}
	_ = wire
}

func appendVarintBytes(v int64) []byte {
	var b []byte
	u := uint64(v)
	for u >= 0x80 {
		b = append(b, byte(u)|0x80)
		u >>= 7
	}
	return append(b, byte(u))
}

func TestFromLocalNames(t *testing.T) {
	base := testSpecs()[0]
	mk := func(name string, id string) specFile {
		f := base
		f.name = name
		f.par2 = nil
		f.segs = []specSeg{full(id+"1@x.test", 1000), full(id+"2@x.test", 1000), full(id+"3@x.test", 500)}
		return f
	}
	in := releaseInput([]specFile{mk("a.mkv", "a"), mk("a.mkv", "b"), mk("A.MKV", "c"), mk("../../etc/x.mkv", "d"), mk("CON", "e")})
	b, err := FromLocal(in)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range b.Files {
		names = append(names, f.Name)
	}
	seen := map[string]bool{}
	for _, n := range names {
		if seen[strings.ToLower(n)] {
			t.Errorf("duplicate name %q in %v", n, names)
		}
		seen[strings.ToLower(n)] = true
		if err := checkFilename(n); err != nil {
			t.Errorf("name %q: %v", n, err)
		}
	}
	for _, want := range []string{"x.mkv", "_CON"} {
		if !seen[strings.ToLower(want)] {
			t.Errorf("missing %q in %v", want, names)
		}
	}
	// Deterministic whatever the input order.
	in2 := releaseInput([]specFile{mk("CON", "e"), mk("../../etc/x.mkv", "d"), mk("A.MKV", "c"), mk("a.mkv", "b"), mk("a.mkv", "a")})
	b2, err := FromLocal(in2)
	if err != nil || !bytes.Equal(mustEncode(t, b), mustEncode(t, b2)) {
		t.Fatalf("name disambiguation depends on input order (err %v)", err)
	}
}

func TestFromLocalNameFallsBackToOriginalFilename(t *testing.T) {
	in := releaseInput([]specFile{testSpecs()[0]})
	in.Files[0].Name = ""
	b, err := FromLocal(in)
	if err != nil || b.Files[0].Name != "movie.mkv" {
		t.Fatalf("%v %v", err, b)
	}
}

func TestFromLocalRejects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(in *ReleaseInput)
		want   error
	}{
		{"nil metadata", func(in *ReleaseInput) { in.Files[0].Meta = nil }, ErrInvalidField},
		{"no files", func(in *ReleaseInput) { in.Files = nil }, ErrInvalidField},
		{"empty title", func(in *ReleaseInput) { in.Title = " \u202E " }, ErrInvalidField},
		{"no post date", func(in *ReleaseInput) { in.PostedAt = time.Time{} }, ErrInvalidField},
		{"unknown scheme", func(in *ReleaseInput) { in.Scheme = 0 }, ErrUnknownScheme},
		{"unresolved segment refs", func(in *ReleaseInput) {
			m := in.Files[0].Meta
			m.SegmentData = nil
			m.SegmentRefs = []*metapb.SegmentRef{{StoreIndex: 0, EndOffset: 9}}
		}, ErrUnresolved},
		{"unresolved segment runs", func(in *ReleaseInput) {
			m := in.Files[0].Meta
			m.SegmentData = nil
			m.SegmentRuns = []*metapb.SegmentRun{{Count: 3}}
		}, ErrUnresolved},
		{"inconsistent local metadata", func(in *ReleaseInput) { in.Files[0].Meta.FileSize++ }, ErrSizeMismatch},
		{"bad message id", func(in *ReleaseInput) { in.Files[0].Meta.SegmentData[0].Id = "a b@x" }, ErrBadMessageID},
		{"empty message id", func(in *ReleaseInput) { in.Files[0].Meta.SegmentData[0].Id = "" }, ErrBadMessageID},
		{"shared outer index out of range", func(in *ReleaseInput) {
			for _, f := range in.Files {
				for _, ns := range f.Meta.NestedSources {
					ns.SharedOuterSourceIndex = 9
				}
			}
		}, ErrOutOfRange},
		{"same file twice", func(in *ReleaseInput) {
			dup := in.Files[0]
			dup.Meta = specFileMetaClone(in.Files[0].Meta)
			dup.Meta.OriginalFilename = "other.mkv"
			dup.Name = "other.mkv"
			in.Files = append(in.Files, dup)
		}, ErrDuplicateFile},
		{"segment missing from the nzb list", func(in *ReleaseInput) {
			in.Scheme, in.NZBFiles = SchemeNZB, nzbLayout()[1:]
		}, ErrSegmentNotInNZB},
		{"nzb scheme without a list", func(in *ReleaseInput) { in.Scheme, in.NZBFiles = SchemeNZB, nil }, ErrBadPool},
		{"bad id in the nzb list", func(in *ReleaseInput) {
			in.Scheme, in.NZBFiles = SchemeNZB, nzbLayout()
			in.NZBFiles[5][0] = "bad id"
		}, ErrBadMessageID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := releaseInput(testSpecs())
			tc.mutate(&in)
			if _, err := FromLocal(in); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func specFileMetaClone(m *metapb.FileMetadata) *metapb.FileMetadata {
	return proto.Clone(m).(*metapb.FileMetadata)
}

func TestFromLocalAcceptsBracketedIDs(t *testing.T) {
	in := releaseInput(testSpecs())
	for _, f := range in.Files {
		for _, s := range f.Meta.SegmentData {
			s.Id = "<" + s.Id + ">"
		}
	}
	b, err := FromLocal(in)
	if err != nil {
		t.Fatal(err)
	}
	want := goodBundle(t)
	if !bytes.Equal(mustEncode(t, b), mustEncode(t, want)) {
		t.Fatal("bracketed ids changed the bundle")
	}
}

func TestFromLocalDoesNotMutateInput(t *testing.T) {
	in := releaseInput(testSpecs())
	before := make([][]byte, len(in.Files))
	for i, f := range in.Files {
		before[i] = []byte(f.Meta.String())
	}
	if _, err := FromLocal(in); err != nil {
		t.Fatal(err)
	}
	for i, f := range in.Files {
		if string(before[i]) != f.Meta.String() {
			t.Fatalf("file %d metadata mutated", i)
		}
	}
}
