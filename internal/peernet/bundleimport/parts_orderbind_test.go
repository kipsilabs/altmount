package bundleimport

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/testsupport/mediagen"
)

// The order of the parts of a multi-part file must be bound: by RAR 5 volume
// numbers, by volume names that parse as one numbered set, or by the payload at
// EVERY part boundary. A file nothing binds is declined, never accepted.

// swapParts exchanges parts a and b of the movie (their segments and counts) and
// reseals the bundle.
func swapParts(e *env, s builtSet, a, b int) {
	m := s.movie()
	var blocks [][]bundle.Segment
	at := 0
	for _, n := range m.Parts {
		blocks = append(blocks, append([]bundle.Segment(nil), m.Segments[at:at+n]...))
		at += n
	}
	blocks[a], blocks[b] = blocks[b], blocks[a]
	m.Segments = m.Segments[:0]
	for k := range blocks {
		m.Parts[k] = len(blocks[k])
		m.Segments = append(m.Segments, blocks[k]...)
	}
	e.sealAny(s.b)
}

// obfuscatedRAR4 builds an obfuscated RAR 4 set (random yEnc names: the names
// say nothing, RAR 4 has no volume number) holding payload in n volumes.
func (e *env) obfuscatedRAR4(tag string, payload []byte, n int) builtSet {
	return e.mkStoredSet(setSpec{tag: tag, kind: "rar4", payload: payload, cuts: evenCuts(len(payload), n), scheme: bundle.SchemeNZB, extraVol: -1, obfuscate: true})
}

// Two swapped middle volumes of 30 are caught every time, not by luck: every
// boundary is read.
func TestPartOrder_EveryBoundaryOfAnObfuscatedSetIsChecked(t *testing.T) {
	payload := mediagen.MKVClusters(30*400, 300)
	for run := 0; run < 6; run++ {
		t.Run(fmt.Sprintf("swap %d", run), func(t *testing.T) {
			e := newEnv(t)
			s := e.obfuscatedRAR4("oe", payload, 30)
			swapParts(e, s, 5+run*3, 6+run*3)
			_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.Error(t, err, "swapped middle volumes of an obfuscated set must never pass")
			assert.Equal(t, ReasonPartOrder, ReasonOf(err), "%v", err)
			e.assertNothingBehind()
		})
	}
}

func TestPartOrder_AnHonestObfuscatedSetIsAccepted(t *testing.T) {
	e := newEnv(t)
	payload := mediagen.MKVClusters(30*400, 300)
	s := e.obfuscatedRAR4("oh", payload, 30)
	res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
	require.NoError(t, err, "%v", err)
	assert.Equal(t, payload, e.stagedBytes(res.Paths[0]))
}

// Containers whose parts nothing orders (a transport stream, a Matroska file
// with one giant cluster) are declined when no name or number binds the order.
func TestPartOrder_AnObfuscatedSetNothingOrdersIsDeclined(t *testing.T) {
	cases := map[string][]byte{
		"transport stream":          tsPayload(220),
		"matroska without clusters": mkvPlain(1600),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			s := e.mkStoredSet(setSpec{tag: "ou", kind: "rar4", payload: payload, cuts: evenCuts(len(payload), 4), segSize: 188,
				scheme: bundle.SchemeNZB, extraVol: -1, obfuscate: true})
			_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.Error(t, err)
			assert.Equal(t, ReasonPartOrderUnverifiable, ReasonOf(err), "%v", err)
			assert.Empty(t, e.rep.got, "a local limitation never blames the publisher")
			e.assertNothingBehind()
		})
	}
}

// Every volume has its own random base name ("a1.rar", "b2.rar": each parses as
// a set of its own). That is not a numbering clash: the names say nothing and
// the payload decides.
func TestPartOrder_SingletonVolumeNamesFallThroughToThePayload(t *testing.T) {
	e := newEnv(t)
	payload := mediagen.MKVClusters(8*400, 300)
	s := e.obfuscatedRAR4("sn", payload, 8)
	for v := range s.ids {
		for _, id := range s.ids[v] {
			beh := e.fp.BehaviorFor(id)
			beh.YEnc.FileName = fmt.Sprintf("rnd%x%d.rar", 0x1f3a+v*77, v)
			e.fp.SetBehavior(id, beh)
		}
	}
	res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
	require.NoError(t, err, "honest random names must not be declined as different sets: %v", err)
	assert.Equal(t, payload, e.stagedBytes(res.Paths[0]))

	// And the payload still catches a swap among them.
	e2 := newEnv(t)
	s2 := e2.obfuscatedRAR4("sx", payload, 8)
	for v := range s2.ids {
		for _, id := range s2.ids[v] {
			beh := e2.fp.BehaviorFor(id)
			beh.YEnc.FileName = fmt.Sprintf("rnd%x%d.rar", 0x1f3a+v*77, v)
			e2.fp.SetBehavior(id, beh)
		}
	}
	swapParts(e2, s2, 3, 4)
	_, err = e2.imp.Accept(context.Background(), e2.openReq(s2.b))
	require.Error(t, err)
	assert.Equal(t, ReasonPartOrder, ReasonOf(err), "%v", err)
}

func TestNameOrder_OnlyANumberingClashIsAClash(t *testing.T) {
	v, _ := nameOrder([]string{"a1.rar", "b2.rar", "c3.rar"})
	assert.Equal(t, namesSilent, v, "each name a set of its own")
	v, _ = nameOrder([]string{"x1f3a8c21", "y9d0e47b2", "zz5c1a"})
	assert.Equal(t, namesSilent, v)
	v, _ = nameOrder([]string{"m.part01.rar", "m.part02.rar", "m.part03.rar"})
	assert.Equal(t, namesBind, v)
	v, _ = nameOrder([]string{"m.part01.rar", "m.part03.rar", "m.part02.rar"})
	assert.Equal(t, namesClash, v)
	v, _ = nameOrder([]string{"m.part01.rar", "m.part03.rar"})
	assert.Equal(t, namesClash, v, "a volume is missing")
	v, _ = nameOrder([]string{"m.part01.rar", "m.part02.rar", "qq9z81f3"})
	assert.Equal(t, namesClash, v, "a part outside the numbered set")
	v, _ = nameOrder([]string{"m.part01.rar", "other.part02.rar", "m.part03.rar"})
	assert.Equal(t, namesClash, v, "two sets mixed")
}

// rawSplit builds a file that is a raw split: every part is a whole posted file
// that is not an archive (movie.001, movie.002 ...).
func (e *env) rawSplit(tag string, payload []byte, n int, name func(v int) string) builtSet {
	const seg = 128
	s := builtSet{spec: setSpec{tag: tag, segSize: seg, scheme: bundle.SchemeNZB}}
	b := &bundle.Bundle{Scheme: bundle.SchemeNZB, Title: "Test Release " + tag, PostedUnix: 1_700_000_000}
	movie := bundle.File{Name: "movie.mkv", Size: int64(len(payload))}
	cuts := evenCuts(len(payload), n)
	at := 0
	for v, c := range cuts {
		posted := payload[at : at+c]
		at += c
		var ids []string
		for i := 0; i*seg < len(posted); i++ {
			from, end := i*seg, min((i+1)*seg, len(posted))
			id := fmt.Sprintf("%s-v%d-%03d@test", tag, v, i)
			beh := fakepoolBehavior(posted[from:end], from, len(posted))
			beh.YEnc.FileName = name(v)
			e.fp.SetBehavior(id, beh)
			ids = append(ids, id)
			movie.Segments = append(movie.Segments, bundle.Segment{Ref: int64(len(b.Pool)), Size: int64(end - from), Start: 0, End: int64(end - from - 1)})
			b.Pool = append(b.Pool, id)
		}
		movie.Parts = append(movie.Parts, len(ids))
		b.ReleaseFiles = append(b.ReleaseFiles, len(ids))
		s.ids = append(s.ids, ids)
		s.posted = append(s.posted, posted)
	}
	b.Files = []bundle.File{movie}
	b.TotalSize = movie.Size
	s.b = b
	e.sealAny(b)
	return s
}

// Whole-posted-file parts (a raw split) get the same rule: bound by one naming
// set or by the payload, never by nothing, and never a mix of unrelated posted
// files.
func TestPartOrder_RawSplitPartsAreBoundLikeAnyOther(t *testing.T) {
	payload := mediagen.MKVClusters(8*400, 300)
	numbered := func(v int) string { return fmt.Sprintf("movie.%03d", v+1) }
	random := func(v int) string { return fmt.Sprintf("q%x", 0x7a31c9+v*91) }

	t.Run("numbered set", func(t *testing.T) {
		e := newEnv(t)
		s := e.rawSplit("rs", payload, 8, numbered)
		res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
		require.NoError(t, err, "%v", err)
		assert.Equal(t, payload, e.stagedBytes(res.Paths[0]))
	})
	t.Run("a gap in the numbering", func(t *testing.T) {
		e := newEnv(t)
		s := e.rawSplit("rg", payload, 8, func(v int) string {
			if v >= 3 {
				return fmt.Sprintf("movie.%03d", v+2)
			}
			return numbered(v)
		})
		_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
		require.Error(t, err)
		assert.Equal(t, ReasonPartOrder, ReasonOf(err), "%v", err)
	})
	t.Run("unrelated posted files mixed into one set", func(t *testing.T) {
		e := newEnv(t)
		s := e.rawSplit("rm", payload, 8, func(v int) string {
			if v == 4 {
				return "other.005"
			}
			return numbered(v)
		})
		_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
		require.Error(t, err)
		assert.Equal(t, ReasonPartOrder, ReasonOf(err), "%v", err)
	})
	t.Run("random names are ordered by the payload", func(t *testing.T) {
		e := newEnv(t)
		s := e.rawSplit("rr", payload, 8, random)
		_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
		require.NoError(t, err, "%v", err)
		e2 := newEnv(t)
		s2 := e2.rawSplit("rx", payload, 8, random)
		swapParts(e2, s2, 2, 3)
		_, err = e2.imp.Accept(context.Background(), e2.openReq(s2.b))
		require.Error(t, err)
		assert.Equal(t, ReasonPartOrder, ReasonOf(err), "%v", err)
	})
	t.Run("random names and nothing in the payload", func(t *testing.T) {
		e := newEnv(t)
		ts := tsPayload(200)
		s := e.rawSplit("rt", ts, 4, random)
		s.b.Files[0].Name = "movie.ts"
		e.sealAny(s.b)
		_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
		require.Error(t, err)
		assert.Equal(t, ReasonPartOrderUnverifiable, ReasonOf(err), "%v", err)
	})
}
