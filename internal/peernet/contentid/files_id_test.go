package contentid

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fid(b byte) ID {
	var id ID
	for i := range id {
		id[i] = b
	}
	return id
}

// referenceFilesID is an independent spelling of the documented byte layout.
func referenceFilesID(sorted ...ID) ID {
	h := sha256.New()
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len("altmount-peernet/release-files/v1")))
	h.Write(l[:])
	h.Write([]byte("altmount-peernet/release-files/v1"))
	for _, id := range sorted {
		h.Write(id[:])
	}
	var out ID
	copy(out[:], h.Sum(nil))
	return out
}

func TestReleaseIDFromFiles_Layout(t *testing.T) {
	got, err := ReleaseIDFromFiles([]ID{fid(3), fid(1), fid(2)})
	require.NoError(t, err)
	assert.Equal(t, referenceFilesID(fid(1), fid(2), fid(3)), got)
}

func TestReleaseIDFromFiles_OrderIndependentAndInputKept(t *testing.T) {
	in := []ID{fid(9), fid(4)}
	a, err := ReleaseIDFromFiles(in)
	require.NoError(t, err)
	b, err := ReleaseIDFromFiles([]ID{fid(4), fid(9)})
	require.NoError(t, err)
	assert.Equal(t, a, b)
	assert.Equal(t, []ID{fid(9), fid(4)}, in, "input slice must not be reordered")
}

func TestReleaseIDFromFiles_DistinctFromOtherKinds(t *testing.T) {
	one, err := ReleaseIDFromFiles([]ID{fid(5)})
	require.NoError(t, err)
	assert.NotEqual(t, fid(5), one)
	two, err := ReleaseIDFromFiles([]ID{fid(5), fid(6)})
	require.NoError(t, err)
	assert.NotEqual(t, one, two)
}

func TestReleaseIDFromFiles_Rejects(t *testing.T) {
	_, err := ReleaseIDFromFiles(nil)
	assert.ErrorIs(t, err, ErrNoFiles)
	_, err = ReleaseIDFromFiles([]ID{fid(1), {}})
	assert.ErrorIs(t, err, ErrInvalidID)
	over := make([]ID, maxIDFiles+1)
	for i := range over {
		binary.BigEndian.PutUint32(over[i][:4], uint32(i+1))
	}
	_, err = ReleaseIDFromFiles(over)
	assert.ErrorIs(t, err, ErrTooManyFiles)
}

func TestReleaseIDFromFiles_CapBoundaryAccepted(t *testing.T) {
	ids := make([]ID, maxIDFiles)
	for i := range ids {
		binary.BigEndian.PutUint32(ids[i][:4], uint32(i+1))
	}
	_, err := ReleaseIDFromFiles(ids)
	assert.NoError(t, err)
}

// A file kept under two folders (the same NZB imported twice) is one file of
// the release: the ID is over the distinct File IDs, so a node holding the
// duplicate derives the same ID as one that does not.
func TestReleaseIDFromFiles_DuplicatesAreOneFile(t *testing.T) {
	once, err := ReleaseIDFromFiles([]ID{fid(1), fid(2)})
	require.NoError(t, err)
	twice, err := ReleaseIDFromFiles([]ID{fid(2), fid(1), fid(2), fid(1), fid(1)})
	require.NoError(t, err)
	assert.Equal(t, once, twice)
	assert.Equal(t, referenceFilesID(fid(1), fid(2)), twice)
}

func TestReleaseIDFromFiles_CapCountsDistinctFiles(t *testing.T) {
	ids := make([]ID, 0, maxIDFiles*2)
	for i := range maxIDFiles {
		var id ID
		binary.BigEndian.PutUint32(id[:4], uint32(i+1))
		ids = append(ids, id, id)
	}
	_, err := ReleaseIDFromFiles(ids)
	assert.NoError(t, err, "10,000 distinct files listed twice is still 10,000 files")
}
