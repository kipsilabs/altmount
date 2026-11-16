package contentid

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seg(id string) *metapb.NzbSeg { return &metapb.NzbSeg{Id: id, Bytes: 100} }

func nzbFile(ids ...string) *metapb.NzbFileEntry {
	f := &metapb.NzbFileEntry{Subject: "s"}
	for i, id := range ids {
		s := seg(id)
		s.Number = int32(i + 1)
		f.Segments = append(f.Segments, s)
	}
	return f
}

func store(files ...*metapb.NzbFileEntry) *metapb.NzbStore { return &metapb.NzbStore{Files: files} }

func sd(ids ...string) []*metapb.SegmentData {
	out := make([]*metapb.SegmentData, len(ids))
	for i, id := range ids {
		out[i] = &metapb.SegmentData{Id: id, SegmentSize: 10, EndOffset: 9}
	}
	return out
}

func mustRelease(t *testing.T, s *metapb.NzbStore) ID {
	t.Helper()
	id, err := ReleaseID(s)
	require.NoError(t, err)
	return id
}

func mustFile(t *testing.T, m *metapb.FileMetadata) ID {
	t.Helper()
	id, err := FileID(m)
	require.NoError(t, err)
	return id
}

func TestReleaseID_Properties(t *testing.T) {
	gap := holes.PlaceholderID(2, "salt")
	base := store(nzbFile("a", "b", "c"), nzbFile("d", "e"))

	tests := []struct {
		name string
		a, b *metapb.NzbStore
		same bool
	}{
		{"identical", base, store(nzbFile("a", "b", "c"), nzbFile("d", "e")), true},
		{"local fields ignored", base, &metapb.NzbStore{Files: []*metapb.NzbFileEntry{
			{Subject: "other", Poster: "p", Date: 5, Groups: []string{"x"}, Segments: nzbFile("a", "b", "c").Segments},
			nzbFile("d", "e"),
		}}, true},
		{"segment bytes ignored", base, store(func() *metapb.NzbFileEntry {
			f := nzbFile("a", "b", "c")
			for _, s := range f.Segments {
				s.Bytes = 999
			}
			return f
		}(), nzbFile("d", "e")), true},
		{"gap placeholder excluded", base, store(nzbFile("a", gap, "b", "c"), nzbFile("d", "e")), true},
		{"angle brackets canonicalised", base, store(nzbFile("<a>", "b", "c"), nzbFile("d", "e")), true},
		{"segment order matters", base, store(nzbFile("b", "a", "c"), nzbFile("d", "e")), false},
		{"file order matters", base, store(nzbFile("d", "e"), nzbFile("a", "b", "c")), false},
		{"file boundary matters", base, store(nzbFile("a", "b"), nzbFile("c", "d", "e")), false},
		{"merged files differ", base, store(nzbFile("a", "b", "c", "d", "e")), false},
		{"extra empty file differs", base, store(nzbFile("a", "b", "c"), nzbFile(), nzbFile("d", "e")), false},
		{"different id", base, store(nzbFile("a", "b", "x"), nzbFile("d", "e")), false},
		{"id concatenation ambiguity", store(nzbFile("ab", "c")), store(nzbFile("a", "bc")), false},
		{"id split ambiguity", store(nzbFile("a", "b")), store(nzbFile("ab")), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ia, ib := mustRelease(t, tc.a), mustRelease(t, tc.b)
			if tc.same {
				assert.Equal(t, ia, ib)
			} else {
				assert.NotEqual(t, ia, ib)
			}
		})
	}
}

func TestReleaseID_Deterministic(t *testing.T) {
	s := store(nzbFile("a", "b"))
	assert.Equal(t, mustRelease(t, s), mustRelease(t, s))
}

// layoutBuilder assembles the documented byte layout by hand, independently of
// the implementation, so the package comment and the code cannot drift apart.
type layoutBuilder struct{ b []byte }

func (l *layoutBuilder) u32(v uint32) *layoutBuilder {
	return &layoutBuilder{binary.BigEndian.AppendUint32(l.b, v)}
}

func (l *layoutBuilder) u64(v uint64) *layoutBuilder {
	return &layoutBuilder{binary.BigEndian.AppendUint64(l.b, v)}
}

func (l *layoutBuilder) str(s string) *layoutBuilder {
	return &layoutBuilder{append(l.u32(uint32(len(s))).b, s...)}
}

func (l *layoutBuilder) id() ID { return sha256.Sum256(l.b) }

func TestKnownVectors(t *testing.T) {
	rel := (&layoutBuilder{}).str("altmount-peernet/release/v1").u64(2).
		u64(2).str("a").str("bc").
		u64(1).str("d").id()
	assert.Equal(t, rel, mustRelease(t, store(nzbFile("a", "bc"), nzbFile("<d>"))))

	file := (&layoutBuilder{}).str("altmount-peernet/file/v1").
		u64(2).str("a").str("b").
		u64(2).u64(1).str("n1").u64(0).id()
	m := &metapb.FileMetadata{
		SegmentData: sd("a", "b"),
		NestedSources: []*metapb.NestedSegmentSource{
			{Segments: sd("n1")}, {Segments: sd(holes.PlaceholderID(1, "x"))},
		},
	}
	assert.Equal(t, file, mustFile(t, m))
}

func TestReleaseID_Errors(t *testing.T) {
	_, err := ReleaseID(nil)
	require.ErrorIs(t, err, ErrNoSegments)
	_, err = ReleaseID(store(nzbFile(holes.PlaceholderID(1, "x"))))
	require.ErrorIs(t, err, ErrNoSegments)
	_, err = ReleaseID(store(nzbFile("")))
	require.ErrorIs(t, err, ErrEmptyMessageID)
	_, err = ReleaseID(store(&metapb.NzbFileEntry{Segments: []*metapb.NzbSeg{nil}}))
	require.ErrorIs(t, err, ErrEmptyMessageID)
}

func TestFileID_Properties(t *testing.T) {
	gap := holes.PlaceholderID(1, "s")
	plain := &metapb.FileMetadata{SegmentData: sd("a", "b", "c")}
	nested := func(segs ...[]string) *metapb.FileMetadata {
		m := &metapb.FileMetadata{}
		for _, s := range segs {
			m.NestedSources = append(m.NestedSources, &metapb.NestedSegmentSource{Segments: sd(s...)})
		}
		return m
	}

	tests := []struct {
		name string
		a, b *metapb.FileMetadata
		same bool
	}{
		{"identical", plain, &metapb.FileMetadata{SegmentData: sd("a", "b", "c")}, true},
		{"local fields ignored", plain, &metapb.FileMetadata{
			SegmentData: sd("a", "b", "c"), SourceNzbPath: "/x/y.nzbz", Status: metapb.FileStatus_FILE_STATUS_CORRUPTED,
			CreatedAt: 1, ModifiedAt: 2, Password: "pw", Salt: "salt", Encryption: metapb.Encryption_AES,
			AesKey: []byte{1}, AesIv: []byte{2}, StoreRef: "/s", OriginalFilename: "f.mkv", FileSize: 12345,
			ReleaseDate: 9, NzbdavId: "id", HoleProviderFingerprint: "fp",
			KnownHoles: []*metapb.HoleRun{{StartSegment: 1, Count: 1, RecordedAt: 3}},
			Par2Files:  []*metapb.Par2FileReference{{Filename: "a.par2", SegmentData: sd("p1")}},
		}, true},
		{"segment offsets ignored", plain, func() *metapb.FileMetadata {
			m := &metapb.FileMetadata{SegmentData: sd("a", "b", "c")}
			m.SegmentData[0].StartOffset = 7
			m.SegmentData[1].SegmentSize = 1
			return m
		}(), true},
		{"gap excluded", plain, &metapb.FileMetadata{SegmentData: sd("a", gap, "b", "c")}, true},
		{"order matters", plain, &metapb.FileMetadata{SegmentData: sd("c", "b", "a")}, false},
		{"missing segment", plain, &metapb.FileMetadata{SegmentData: sd("a", "b")}, false},
		{"main vs nested differ", &metapb.FileMetadata{SegmentData: sd("a", "b")}, nested([]string{"a", "b"}), false},
		{"nested boundaries matter", nested([]string{"a", "b"}, []string{"c"}), nested([]string{"a"}, []string{"b", "c"}), false},
		{"nested split vs joined", nested([]string{"a", "b"}), nested([]string{"a"}, []string{"b"}), false},
		{"nested order matters", nested([]string{"a"}, []string{"b"}), nested([]string{"b"}, []string{"a"}), false},
		{"nested keys ignored", nested([]string{"a"}), func() *metapb.FileMetadata {
			m := nested([]string{"a"})
			m.NestedSources[0].AesKey, m.NestedSources[0].AesIv = []byte{1}, []byte{2}
			m.NestedSources[0].InnerOffset = 5
			return m
		}(), true},
		{"main+nested boundary", &metapb.FileMetadata{SegmentData: sd("a"), NestedSources: nested([]string{"b"}).NestedSources},
			&metapb.FileMetadata{SegmentData: sd("a", "b")}, false},
		{"concat ambiguity", &metapb.FileMetadata{SegmentData: sd("ab", "c")}, &metapb.FileMetadata{SegmentData: sd("a", "bc")}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ia, ib := mustFile(t, tc.a), mustFile(t, tc.b)
			if tc.same {
				assert.Equal(t, ia, ib)
			} else {
				assert.NotEqual(t, ia, ib)
			}
		})
	}
}

func TestFileID_FileAndReleaseDomainsSeparated(t *testing.T) {
	r := mustRelease(t, store(nzbFile("a", "b")))
	f := mustFile(t, &metapb.FileMetadata{SegmentData: sd("a", "b")})
	assert.NotEqual(t, r, f)
}

func TestFileID_SharedOuterSources(t *testing.T) {
	expanded := &metapb.FileMetadata{NestedSources: []*metapb.NestedSegmentSource{
		{Segments: sd("o1", "o2"), InnerOffset: 1},
		{Segments: sd("o1", "o2"), InnerOffset: 50},
	}}
	shared := &metapb.FileMetadata{
		SharedOuterSources: []*metapb.NestedSegmentSource{{Segments: sd("o1", "o2")}},
		NestedSources: []*metapb.NestedSegmentSource{
			{SharedOuterSourceIndex: 1, InnerOffset: 1},
			{SharedOuterSourceIndex: 1, InnerOffset: 50},
		},
	}
	before := mustFile(t, shared)
	assert.Equal(t, mustFile(t, expanded), before, "shared layout must hash like the expanded layout")
	assert.Nil(t, shared.NestedSources[0].Segments, "input must not be mutated")

	bad := &metapb.FileMetadata{NestedSources: []*metapb.NestedSegmentSource{{SharedOuterSourceIndex: 3}}}
	_, err := FileID(bad)
	require.ErrorIs(t, err, ErrBadSharedSource)
}

func TestFileID_Errors(t *testing.T) {
	tests := []struct {
		name string
		m    *metapb.FileMetadata
		want error
	}{
		{"nil", nil, ErrNoSegments},
		{"empty", &metapb.FileMetadata{}, ErrNoSegments},
		{"only gaps", &metapb.FileMetadata{SegmentData: sd(holes.PlaceholderID(1, "x"))}, ErrNoSegments},
		{"unresolved refs", &metapb.FileMetadata{SegmentRefs: []*metapb.SegmentRef{{StoreIndex: 0}}}, ErrUnresolved},
		{"unresolved runs", &metapb.FileMetadata{SegmentRuns: []*metapb.SegmentRun{{Count: 2}}}, ErrUnresolved},
		{"unresolved nested", &metapb.FileMetadata{NestedSources: []*metapb.NestedSegmentSource{{SegmentRefs: []*metapb.SegmentRef{{}}}}}, ErrUnresolved},
		{"empty id", &metapb.FileMetadata{SegmentData: sd("")}, ErrEmptyMessageID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FileID(tc.m)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestFileID_Large(t *testing.T) {
	ids := make([]string, 5000)
	for i := range ids {
		ids[i] = fmt.Sprintf("part%d@host", i)
	}
	a := mustFile(t, &metapb.FileMetadata{SegmentData: sd(ids...)})
	ids[4999] = "other"
	assert.NotEqual(t, a, mustFile(t, &metapb.FileMetadata{SegmentData: sd(ids...)}))
}
