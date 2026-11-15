package bundleimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/testsupport/mediagen"
)

// A RAR 4 set with obfuscated volume names: no header and no name says which
// volume is which, so only the media itself can bind the order.
func TestParts_ObfuscatedRar4SetOrderComesFromTheMatroskaClusters(t *testing.T) {
	payload := mediagen.MKVClusters(6000, 300)
	for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
		t.Run("honest order/"+scheme.String(), func(t *testing.T) {
			e := newEnv(t)
			s := e.mkStoredSet(setSpec{tag: "ob", kind: "rar4", payload: payload, cuts: evenCuts(len(payload), 5), scheme: scheme, extraVol: -1, obfuscate: true})
			res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.NoError(t, err, "%v", err)
			assert.Equal(t, payload, e.stagedBytes(res.Paths[0]))
		})
		t.Run("swapped middle volumes/"+scheme.String(), func(t *testing.T) {
			e := newEnv(t)
			s := e.mkStoredSet(setSpec{tag: "ob", kind: "rar4", payload: payload, cuts: evenCuts(len(payload), 5), scheme: scheme, extraVol: -1, obfuscate: true})
			swapPartsOf(&s, 1, 2)
			require.NoError(t, e.tryReseal(s.b))
			_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.Error(t, err, "a swap of two middle volumes with obfuscated names was accepted")
			assert.Equal(t, ReasonPartOrder, ReasonOf(err), "%v", err)
			assert.Empty(t, e.rep.got, "the order is a local judgement, nobody is blamed")
			e.assertNothingBehind()
		})
	}
}

func TestClusterTimecodeAt(t *testing.T) {
	mk := mediagen.MKVClusters(3000, 300)
	tc, ok := clusterTimecodeAt(mk[200:])
	assert.True(t, ok)
	assert.Positive(t, tc)
	_, ok = clusterTimecodeAt(make([]byte, 4096))
	assert.False(t, ok, "no cluster in zeros")
	_, ok = clusterTimecodeAt([]byte{0x1F, 0x43, 0xB6, 0x75, 0x84, 0, 0, 0, 0x99, 0x84, 0, 0, 0, 1})
	assert.False(t, ok, "a cluster id not followed by a Timecode is no cluster start")
}
