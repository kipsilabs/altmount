package bundle

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

type effSeg struct {
	ID               string
	Size, Start, End int64
}

type effNested struct {
	Segs           []effSeg
	Key, IV        string
	Offset, Length int64
	VolumeSize     int64
}

type effPar2 struct {
	Name string
	Size int64
	Segs []effSeg
}

// effective is what the readers act on, with local-only fields left out and
// gap placeholders normalised (their ids are local by design).
type effective struct {
	Name       string
	Size       int64
	Date       int64
	Encryption metapb.Encryption
	Key, IV    string
	Segs       []effSeg
	Nested     []effNested
	Clips      []Clip
	Par2       []effPar2
}

func effSegs(in []*metapb.SegmentData) []effSeg {
	var out []effSeg
	for _, s := range in {
		id := s.Id
		if holes.IsPlaceholderID(id) {
			id = "GAP"
		}
		out = append(out, effSeg{id, s.SegmentSize, s.StartOffset, s.EndOffset})
	}
	return out
}

func effectiveOf(t *testing.T, m *metapb.FileMetadata) effective {
	t.Helper()
	require.NoError(t, expandShared(m))
	e := effective{Name: m.OriginalFilename, Size: m.FileSize, Date: m.ReleaseDate, Encryption: m.Encryption,
		Key: string(m.AesKey), IV: string(m.AesIv), Segs: effSegs(m.SegmentData)}
	for _, ns := range m.NestedSources {
		e.Nested = append(e.Nested, effNested{effSegs(ns.Segments), string(ns.AesKey), string(ns.AesIv), ns.InnerOffset, ns.InnerLength, ns.InnerVolumeSize})
	}
	for _, c := range m.ClipBoundaries {
		e.Clips = append(e.Clips, Clip{c.ByteLen, c.Delta_90K})
	}
	for _, p := range m.Par2Files {
		e.Par2 = append(e.Par2, effPar2{p.Filename, p.FileSize, effSegs(p.SegmentData)})
	}
	return e
}

func TestToLocalRoundTripFieldByField(t *testing.T) {
	b := goodBundle(t)
	loc, err := ToLocal(b, testNow)
	require.NoError(t, err)
	require.Len(t, loc.Files, len(testSpecs()))
	got := map[string]*metapb.FileMetadata{}
	for _, f := range loc.Files {
		got[f.Name] = f.Meta
	}
	for _, s := range testSpecs() {
		m, ok := got[s.name]
		require.True(t, ok, s.name)
		assert.Equal(t, effectiveOf(t, s.meta()), effectiveOf(t, m), s.name)
	}
}

func TestToLocalDropsLocalFieldsAndIsNeverAPath(t *testing.T) {
	loc, err := ToLocal(goodBundle(t), testNow)
	require.NoError(t, err)
	for _, f := range loc.Files {
		m := f.Meta
		assert.Empty(t, m.SourceNzbPath)
		assert.Empty(t, m.StoreRef)
		assert.Empty(t, m.NzbdavId)
		assert.Empty(t, m.Password)
		assert.Empty(t, m.Salt)
		assert.Empty(t, m.KnownHoles)
		assert.Empty(t, m.HoleProviderFingerprint)
		assert.Empty(t, m.SegmentRefs)
		assert.Empty(t, m.SegmentRuns)
		assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, m.Status)
		assert.Equal(t, testNow.Unix(), m.CreatedAt)
		assert.Equal(t, testNow.Unix(), m.ModifiedAt)
		assert.False(t, strings.ContainsAny(f.Name, `/\`), f.Name)
		assert.Equal(t, f.Name, m.OriginalFilename)
	}
}

func TestToLocalPreservesFileIdentity(t *testing.T) {
	b := goodBundle(t)
	loc, err := ToLocal(b, testNow)
	require.NoError(t, err)
	want := map[string]contentid.ID{}
	for _, f := range b.Files {
		want[f.Name] = f.FileID
	}
	for _, f := range loc.Files {
		require.NoError(t, expandShared(f.Meta))
		id, err := contentid.FileID(f.Meta)
		require.NoError(t, err)
		assert.Equal(t, want[f.Name], id, f.Name)
	}
}

func TestToLocalSharesOuterSources(t *testing.T) {
	loc, err := ToLocal(goodBundle(t), testNow)
	require.NoError(t, err)
	for _, f := range loc.Files {
		if f.Name != "nested.mkv" {
			continue
		}
		assert.Len(t, f.Meta.SharedOuterSources, 1)
		assert.Len(t, f.Meta.NestedSources, 2)
		for _, ns := range f.Meta.NestedSources {
			assert.Equal(t, int32(1), ns.SharedOuterSourceIndex)
		}
	}
}

func TestToLocalDoesNotAliasTheBundle(t *testing.T) {
	b := goodBundle(t)
	want := mustEncode(t, b)
	loc, err := ToLocal(b, testNow)
	require.NoError(t, err)
	for _, f := range loc.Files {
		for i := range f.Meta.AesKey {
			f.Meta.AesKey[i] = 0
		}
		for _, o := range f.Meta.SharedOuterSources {
			for i := range o.AesKey {
				o.AesKey[i] = 0
			}
		}
	}
	assert.Equal(t, want, encodeUnchecked(b))
}

func TestToLocalNZBSchemeCarriesTheReleaseList(t *testing.T) {
	loc, err := ToLocal(goodNZBBundle(t), testNow)
	require.NoError(t, err)
	assert.Equal(t, SchemeNZB, loc.Scheme)
	assert.Equal(t, nzbLayout(), loc.NZBFiles)
	loc, err = ToLocal(goodBundle(t), testNow)
	require.NoError(t, err)
	assert.Nil(t, loc.NZBFiles)
}

func TestToLocalRejectsInvalidBundle(t *testing.T) {
	b := goodBundle(t)
	b.Files[0].Size++
	_, err := ToLocal(b, testNow)
	assert.ErrorIs(t, err, ErrSizeMismatch)
	_, err = ToLocal(nil, testNow)
	assert.ErrorIs(t, err, ErrInvalidField)
}

// The converted metadata is read by the real metadata service from inline
// segments alone: no .nzbz store entry is needed.
func TestToLocalIsReadableWithoutAStore(t *testing.T) {
	loc, err := ToLocal(goodBundle(t), testNow)
	require.NoError(t, err)
	svc := metadata.NewMetadataService(t.TempDir())
	want := map[string]contentid.ID{}
	for _, f := range goodBundle(t).Files {
		want[f.Name] = f.FileID
	}
	for _, f := range loc.Files {
		require.NoError(t, svc.WriteFileMetadata("/release/"+f.Name, f.Meta))
		got, err := svc.ReadFileMetadata("/release/" + f.Name)
		require.NoError(t, err)
		require.NotNil(t, got)
		id, err := contentid.FileID(got)
		require.NoError(t, err)
		assert.Equal(t, want[f.Name], id, f.Name)
		assert.Equal(t, f.Meta.FileSize, got.FileSize)
	}
}
