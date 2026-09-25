package availability

import (
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/require"
)

func validMetadata() *metapb.FileMetadata {
	return &metapb.FileMetadata{FileSize: 2, SegmentData: []*metapb.SegmentData{
		{Id: "<one@example>", SegmentSize: 2, StartOffset: 0, EndOffset: 1},
	}}
}

func TestManifestIdentityIsPathIndependentAndPar2Independent(t *testing.T) {
	first := validMetadata()
	first.SourceNzbPath = "/one/path/source.nzb"
	second := validMetadata()
	second.SourceNzbPath = "/another/path/source.nzb"

	got, err := BuildManifestIdentity(first)
	require.NoError(t, err)
	equal, err := BuildManifestIdentity(second)
	require.NoError(t, err)
	require.Equal(t, got, equal)

	first.Par2Files = []*metapb.Par2FileReference{{Filename: "broken.par2", SegmentData: []*metapb.SegmentData{{Id: "raw-par2-id"}}}}
	withPar2, err := BuildManifestIdentity(first)
	require.NoError(t, err)
	require.Equal(t, got, withPar2)

	first.Par2Files[0].Filename = "different.par2"
	changedPar2, err := BuildManifestIdentity(first)
	require.NoError(t, err)
	require.Equal(t, got, changedPar2)

	first.Par2Files[0].SegmentData = []*metapb.SegmentData{{Id: "raw-par2-id", StartOffset: -1}}
	malformedPar2, err := BuildManifestIdentity(first)
	require.NoError(t, err)
	require.Equal(t, got, malformedPar2)

	first.Par2Files = nil
	withoutPar2, err := BuildManifestIdentity(first)
	require.NoError(t, err)
	require.Equal(t, got, withoutPar2)
}

func TestManifestIdentityChangesWhenResolvedSegmentChanges(t *testing.T) {
	first, err := BuildManifestIdentity(validMetadata())
	require.NoError(t, err)
	changed := validMetadata()
	changed.SegmentData[0].Id = "two@example"
	second, err := BuildManifestIdentity(changed)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

func TestManifestIdentityFailsClosedForUnsupportedMetadata(t *testing.T) {
	cases := map[string]func(*metapb.FileMetadata){
		"encryption": func(m *metapb.FileMetadata) { m.Encryption = metapb.Encryption_AES },
		"password":   func(m *metapb.FileMetadata) { m.Password = "secret" },
		"salt":       func(m *metapb.FileMetadata) { m.Salt = "salt" },
		"aes key":    func(m *metapb.FileMetadata) { m.AesKey = []byte("key") },
		"aes iv":     func(m *metapb.FileMetadata) { m.AesIv = []byte("iv") },
		"nested":     func(m *metapb.FileMetadata) { m.NestedSources = []*metapb.NestedSegmentSource{{}} },
		"shared outer": func(m *metapb.FileMetadata) {
			m.SharedOuterSources = []*metapb.NestedSegmentSource{{}}
		},
		"clip boundaries": func(m *metapb.FileMetadata) { m.ClipBoundaries = []*metapb.ClipBoundary{{ByteLen: 1}} },
		"empty":           func(m *metapb.FileMetadata) { m.SegmentData = nil },
		"nil segment":     func(m *metapb.FileMetadata) { m.SegmentData = []*metapb.SegmentData{nil} },
		"empty id":        func(m *metapb.FileMetadata) { m.SegmentData[0].Id = " < > " },
		"negative offset": func(m *metapb.FileMetadata) { m.SegmentData[0].StartOffset = -1 },
		"reversed":        func(m *metapb.FileMetadata) { m.SegmentData[0].StartOffset, m.SegmentData[0].EndOffset = 2, 1 },
		"overflow":        func(m *metapb.FileMetadata) { m.SegmentData[0].EndOffset = 9223372036854775807 },
		"invalid size":    func(m *metapb.FileMetadata) { m.SegmentData[0].SegmentSize = 3 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			metadata := validMetadata()
			mutate(metadata)
			_, err := BuildManifestIdentity(metadata)
			require.Error(t, err)
		})
	}
}
