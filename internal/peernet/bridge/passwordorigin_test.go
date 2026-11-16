package bridge

import (
	"context"
	"errors"
	"testing"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeMeta map[string]*metapb.FileMetadata

func (f fakeMeta) ReadFileMetadata(p string) (*metapb.FileMetadata, error) { return f[p], nil }

type fakeOriginStore struct {
	ids []string
	err error
}

func (f *fakeOriginStore) MarkFromNZB(_ context.Context, ids []string) error {
	f.ids = append(f.ids, ids...)
	return f.err
}

func seg(id string) []*metapb.SegmentData {
	return []*metapb.SegmentData{{Id: id, SegmentSize: 100, EndOffset: 99}}
}

func fileID(t *testing.T, m *metapb.FileMetadata) string {
	t.Helper()
	id, err := contentid.FileID(m)
	require.NoError(t, err)
	return id.String()
}

func TestNZBPasswordRecorder_OnlyKeyedFilesAreRecordedByFileID(t *testing.T) {
	keyed := &metapb.FileMetadata{FileSize: 100, SegmentData: seg("k@x"), Encryption: metapb.Encryption_AES,
		AesKey: make([]byte, 32), AesIv: make([]byte, 16)}
	nested := &metapb.FileMetadata{FileSize: 100, SegmentData: seg("n@x"),
		SharedOuterSources: []*metapb.NestedSegmentSource{{Segments: seg("o@x"), AesKey: make([]byte, 32), AesIv: make([]byte, 16), InnerLength: 100}}}
	plain := &metapb.FileMetadata{FileSize: 100, SegmentData: seg("p@x")}
	store := &fakeOriginStore{}
	NewNZBPasswordRecorder(fakeMeta{"a/k.mkv": keyed, "a/n.mkv": nested, "a/p.mkv": plain}, store).
		RecordNZBPasswordFiles(context.Background(), []string{"a/k.mkv", "a/n.mkv", "a/p.mkv", "a/gone.mkv"})
	assert.ElementsMatch(t, []string{fileID(t, keyed), fileID(t, nested)}, store.ids,
		"keyed files only; unkeyed and missing files have no key to protect")
}

func TestNZBPasswordRecorder_StoreFailureNeverPanicsOrLeaks(t *testing.T) {
	keyed := &metapb.FileMetadata{FileSize: 100, SegmentData: seg("k@x"), AesKey: make([]byte, 32), AesIv: make([]byte, 16)}
	store := &fakeOriginStore{err: errors.New("db down")}
	NewNZBPasswordRecorder(fakeMeta{"k": keyed}, store).RecordNZBPasswordFiles(context.Background(), []string{"k"})
	assert.Len(t, store.ids, 1)
}
