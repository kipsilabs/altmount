package bundleimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func acceptPlain(t *testing.T, plain []byte, name string) error {
	t.Helper()
	e := newEnv(t)
	b := e.mkBundle("st", fileSpec{name: name, plain: plain, segSize: 128})
	_, err := e.imp.Accept(context.Background(), e.req(b))
	if err != nil {
		e.assertNothingBehind()
	}
	return err
}

func TestStructure_MatroskaIndexAtTheTailMustHold(t *testing.T) {
	good := mkvPlain(1024)
	require.NoError(t, acceptPlain(t, good, "a.mkv"))

	t.Run("SeekHead points at the wrong element", func(t *testing.T) {
		bad := mkvPlain(1024)
		// Break the Cues element at the tail (its ID), keep the head intact.
		bad[len(bad)-9] ^= 0xFF
		err := acceptPlain(t, bad, "a.mkv")
		require.Error(t, err)
		assert.Equal(t, ReasonStructureInvalid, ReasonOf(err))
	})
	t.Run("Segment claims more than the file holds", func(t *testing.T) {
		bad := mkvPlain(1024)
		bad[16+7] += 9 // low byte of the Segment size
		err := acceptPlain(t, bad, "a.mkv")
		require.Error(t, err)
		assert.Equal(t, ReasonStructureInvalid, ReasonOf(err))
	})
}

func mp4Box(typ string, payload int) []byte {
	n := 8 + payload
	b := []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	b = append(b, typ...)
	return append(b, make([]byte, payload)...)
}

func TestStructure_MP4BoxChainMustEndAtTheEndOfTheFile(t *testing.T) {
	var good []byte
	good = append(good, mp4Box("ftyp", 16)...)
	good = append(good, mp4Box("moov", 200)...)
	good = append(good, mp4Box("mdat", 600)...)
	copy(good[8:], "isom")
	require.NoError(t, acceptPlain(t, good, "a.mp4"))

	bad := append(append([]byte{}, good...), 1, 2, 3) // trailing bytes beyond the last box
	err := acceptPlain(t, bad, "a.mp4")
	require.Error(t, err)
	assert.Equal(t, ReasonStructureInvalid, ReasonOf(err))

	noMoov := append(append([]byte{}, mp4Box("ftyp", 16)...), mp4Box("mdat", 800)...)
	err = acceptPlain(t, noMoov, "a.mp4")
	require.Error(t, err)
	assert.Equal(t, ReasonStructureInvalid, ReasonOf(err))
}

func TestStructure_TransportStreamNeedsSyncAtTheEnd(t *testing.T) {
	ts := make([]byte, 188*40)
	for k := 0; k < 40; k++ {
		ts[k*188] = 0x47
	}
	require.NoError(t, acceptPlain(t, ts, "a.ts"))
	bad := append([]byte{}, ts...)
	bad[188*39] = 0
	err := acceptPlain(t, bad, "a.ts")
	require.Error(t, err)
	assert.Equal(t, ReasonStructureInvalid, ReasonOf(err))
}
