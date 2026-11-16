package importer

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/archivehdr/archivetest"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
	"github.com/kipsilabs/altmount/internal/peernet/bundlex"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/mediagen"
	"github.com/kipsilabs/altmount/internal/testsupport/nzbbuild"
)

// Hostile variants of the real pipeline: the honest bundle the real importer,
// metadata and builder produce for a release, with ONE thing mutated, and the
// reason the receiver must give.

type rpPublished struct {
	env  *batteryEnv
	bd   *bundle.Bundle
	data []byte
	path string
}

// rpPublish imports the release with the real importer and returns the verified
// bundle the real builder made for it.
func rpPublish(t *testing.T, tag string, data []byte, build func(post func(string, []byte) nzbbuild.File) []nzbbuild.File) rpPublished {
	t.Helper()
	env := newBatteryEnv(t)
	post := func(name string, d []byte) nzbbuild.File {
		var segs []nzbbuild.Segment
		total := (len(d) + rpSeg - 1) / rpSeg
		for i := 0; i < total; i++ {
			lo, hi := i*rpSeg, min(len(d), (i+1)*rpSeg)
			id := fmt.Sprintf("%s-%s-%03d@hp.test", tag, name, i+1)
			env.client.SetBehavior(id, fakepool.SegmentBehavior{Bytes: d[lo:hi], YEnc: nntppool.YEncMeta{
				FileName: name, FileSize: int64(len(d)), Part: int64(i + 1), Total: int64(total), PartBegin: int64(lo), PartSize: int64(hi - lo)}})
			segs = append(segs, nzbbuild.Segment{ID: id, Bytes: hi - lo})
		}
		return nzbbuild.File{Subject: name, Segments: segs}
	}
	nzb := nzbbuild.Build(build(post)...)
	_, _, err := env.runImport(nzb, "Release.Name.2024.1080p")
	require.NoError(t, err)
	paths := rpVirtualPaths(t, env.metaRoot)
	require.NotEmpty(t, paths)
	src := bundlex.NewMetadataSource(env.svc, rpOrigins{})
	lists, err := src.NZBSegmentIDs(paths[0])
	require.NoError(t, err)
	store := &metapb.NzbStore{}
	for _, l := range lists {
		fe := &metapb.NzbFileEntry{}
		for _, id := range l {
			fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id})
		}
		store.Files = append(store.Files, fe)
	}
	rid, err := contentid.ReleaseID(store)
	require.NoError(t, err)
	built, err := bundlex.NewBuilder(bundlex.BuilderConfig{Source: src, Library: rpLibrary{paths: paths}, Patches: bundlex.NoPatches{}}).
		Build(context.Background(), rid.String(), "Release Name 2024 1080p", time.Unix(1_700_000_000, 0))
	require.NoError(t, err)
	bd, err := bundle.Verify(built.Wire, built.ID, rid)
	require.NoError(t, err)
	return rpPublished{env: env, bd: bd, data: data, path: paths[0]}
}

// rpObfuscateNames makes every posted volume carry a random yEnc file name, as an
// obfuscated release does (the importer already ran on the honest names; the
// receiver reads the articles' own headers).
func rpObfuscateNames(p rpPublished) {
	names := map[string]string{}
	for _, id := range p.bd.Pool {
		beh := p.env.client.BehaviorFor(id)
		n, ok := names[beh.YEnc.FileName]
		if !ok {
			n = fmt.Sprintf("%08x%04x", 0x9c1f3a77+len(names)*104729, 0x5a3c+len(names)*31)
			names[beh.YEnc.FileName] = n
		}
		beh.YEnc.FileName = n
		p.env.client.SetBehavior(id, beh)
	}
}

// rpReseal recomputes the File IDs after a mutation, as an attacker would.
func rpReseal(t *testing.T, bd *bundle.Bundle) {
	t.Helper()
	loc, err := bundle.ToLocal(bd, time.Unix(1_700_000_000, 0))
	require.NoError(t, err)
	for i := range bd.Files {
		id, err := contentid.FileID(loc.Files[i].Meta)
		require.NoError(t, err)
		bd.Files[i].FileID = id
	}
	sort.Slice(bd.Files, func(i, j int) bool { return string(bd.Files[i].FileID[:]) < string(bd.Files[j].FileID[:]) })
}

// rpAccept is the receiver: the real importer over the same fake pool.
func rpAccept(t *testing.T, p rpPublished, bd *bundle.Bundle) (*bundleimport.Result, error) {
	t.Helper()
	libRoot := t.TempDir()
	lib := metadata.NewMetadataService(libRoot)
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	cfg := config.DefaultConfig()
	pm := processorTestPoolManager{client: p.env.client}
	stager := bundleimport.NewFSStager(bundleimport.FSStagerDeps{Pool: pm, Config: func() *config.Config { return cfg }, Root: t.TempDir()})
	state := database.NewPeernetBundleImportRepository(db.Connection(), database.DialectSQLite)
	imp := bundleimport.New(bundleimport.Config{
		Library: lib, Stager: stager, Prober: rpProber{}, Headers: bundleimport.NewPoolHeaderReader(pm),
		State: state, Announcer: rpAnnouncer{}, Reporter: rpReporter{},
		HealthEnabled: func() bool { return true },
	})
	res, err := imp.Accept(context.Background(), bundleimport.Request{Bundle: bd, Target: bundleimport.Target{Dir: "movies/Release.Name.2024.1080p", Category: "movies"}})
	if err != nil {
		// A refused bundle leaves nothing behind: no file, no state row.
		require.Empty(t, rpVirtualPaths(t, libRoot), "a refused bundle must leave no metadata")
		rows, lerr := state.ListAfter(context.Background(), "", 10)
		require.NoError(t, lerr)
		require.Empty(t, rows, "a refused bundle must leave no state row")
	}
	return res, err
}

// rpMutated returns a copy of the bundle with its first file changed by f, and
// resealed as an attacker would.
func rpMutated(t *testing.T, bd *bundle.Bundle, f func(file *bundle.File)) *bundle.Bundle {
	t.Helper()
	h := *bd
	h.Files = append([]bundle.File(nil), bd.Files...)
	file := h.Files[0]
	file.Segments = append([]bundle.Segment(nil), file.Segments...)
	file.Parts = append([]int(nil), file.Parts...)
	f(&file)
	file.Size = 0
	for _, sg := range file.Segments {
		file.Size += sg.End - sg.Start + 1
	}
	h.Files[0] = file
	h.TotalSize = file.Size
	rpReseal(t, &h)
	return &h
}

// partRange returns the index range [from, to) of part k in the file's segments.
func partRange(f *bundle.File, k int) (int, int) {
	at := 0
	for j := 0; j < k; j++ {
		at += f.Parts[j]
	}
	return at, at + f.Parts[k]
}

// Hostile variants built from honest pipeline output: each protection is hit
// with the specific lie it exists for, and the receiver must name the reason and
// leave nothing behind.
func TestRealPipelineHostile_EveryProtectionCatchesItsLie(t *testing.T) {
	data := mediagen.MKVClusters(rpPayload, 20_000)
	p := rpPublish(t, "hv", data, rpRar4Volumes("Movie.2024.mkv", data, 4))
	require.Len(t, p.bd.Files[0].Parts, 4)
	_, err := rpAccept(t, p, p.bd)
	require.NoError(t, err, "the honest bundle is accepted")

	cases := []struct {
		name   string
		reason bundleimport.Reason
		mutate func(t *testing.T) *bundle.Bundle
	}{
		{"yEnc offset lie on the last article of a middle volume", bundleimport.ReasonYencMismatch, func(t *testing.T) *bundle.Bundle {
			f := p.bd.Files[0]
			_, to := partRange(&f, 1)
			id := p.bd.Pool[f.Segments[to-1].Ref]
			beh := p.env.client.BehaviorFor(id)
			t.Cleanup(func() { p.env.client.SetBehavior(id, beh) })
			lie := beh
			lie.YEnc.PartBegin += 7
			p.env.client.SetBehavior(id, lie)
			return p.bd
		}},
		{"part offset lie (a middle part starts later than its archive header says)", bundleimport.ReasonExtentMismatch, func(t *testing.T) *bundle.Bundle {
			return rpMutated(t, p.bd, func(f *bundle.File) {
				from, _ := partRange(f, 1)
				f.Segments[from].Start += 8
			})
		}},
		{"part length lie (a middle part stops short of its archive header's size)", bundleimport.ReasonExtentMismatch, func(t *testing.T) *bundle.Bundle {
			return rpMutated(t, p.bd, func(f *bundle.File) {
				_, to := partRange(f, 1)
				f.Segments[to-1].End -= 1000
			})
		}},
		{"truncated volume tail (the last part loses its end)", bundleimport.ReasonExtentMismatch, func(t *testing.T) *bundle.Bundle {
			return rpMutated(t, p.bd, func(f *bundle.File) {
				_, to := partRange(f, 3)
				f.Segments[to-1].End -= 500
			})
		}},
		{"sliver first part", bundleimport.ReasonExtentMismatch, func(t *testing.T) *bundle.Bundle {
			return rpMutated(t, p.bd, func(f *bundle.File) {
				_, to := partRange(f, 0)
				last := f.Segments[to-1]
				last.Start = last.End - 999
				f.Segments = append([]bundle.Segment{last}, f.Segments[to:]...)
				f.Parts[0] = 1
			})
		}},
		{"part swap with parseable names", bundleimport.ReasonPartOrder, func(t *testing.T) *bundle.Bundle {
			return rpMutated(t, p.bd, func(f *bundle.File) {
				var blocks [][]bundle.Segment
				at := 0
				for _, n := range f.Parts {
					blocks = append(blocks, append([]bundle.Segment(nil), f.Segments[at:at+n]...))
					at += n
				}
				blocks[1], blocks[2] = blocks[2], blocks[1]
				f.Parts[1], f.Parts[2] = f.Parts[2], f.Parts[1]
				f.Segments = nil
				for _, b := range blocks {
					f.Segments = append(f.Segments, b...)
				}
			})
		}},
		{"the file is named as another kind than its archive entry", bundleimport.ReasonExtentMismatch, func(t *testing.T) *bundle.Bundle {
			return rpMutated(t, p.bd, func(f *bundle.File) { f.Name = "Movie.2024.mp4" })
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bd := c.mutate(t)
			_, err := rpAccept(t, p, bd)
			require.Error(t, err)
			assert.Equal(t, c.reason, bundleimport.ReasonOf(err), "%v", err)
		})
	}
}

// An obfuscated RAR 4 set: honest order is accepted, two swapped middle volumes
// are refused (part_order) because the Matroska clusters run backwards.
func TestRealPipelineHostile_ObfuscatedSetOrderFromThePayload(t *testing.T) {
	data := mediagen.MKVClusters(rpPayload, 20_000)
	p := rpPublish(t, "obf", data, rpRar4Volumes("Movie.2024.mkv", data, 5))
	rpObfuscateNames(p)
	require.NotEmpty(t, p.bd.Files[0].Parts, "the real builder wrote a multi-part layout")
	require.Len(t, p.bd.Files[0].Parts, 5)

	_, err := rpAccept(t, p, p.bd)
	require.NoError(t, err, "the honest bundle is accepted")

	hostile := *p.bd
	hostile.Files = append([]bundle.File(nil), p.bd.Files...)
	f := hostile.Files[0]
	var chunks [][]bundle.Segment
	at := 0
	for _, n := range f.Parts {
		chunks = append(chunks, f.Segments[at:at+n])
		at += n
	}
	chunks[1], chunks[2] = chunks[2], chunks[1]
	f.Parts = append([]int(nil), f.Parts...)
	f.Parts[1], f.Parts[2] = f.Parts[2], f.Parts[1]
	f.Segments = nil
	for _, c := range chunks {
		f.Segments = append(f.Segments, c...)
	}
	hostile.Files[0] = f
	rpReseal(t, &hostile)
	_, err = rpAccept(t, p, &hostile)
	require.Error(t, err)
	assert.Equal(t, bundleimport.ReasonPartOrder, bundleimport.ReasonOf(err), "%v", err)
}

// Article sizes that are not one poster's shape are declined before any read.
func TestRealPipelineHostile_ArticleSizesOfOneTableAreNotOneShape(t *testing.T) {
	data := mediagen.MKV(rpPayload)
	p := rpPublish(t, "shp", data, func(post func(string, []byte) nzbbuild.File) []nzbbuild.File {
		return []nzbbuild.File{post("Movie.2024.mkv", data)}
	})
	hostile := *p.bd
	hostile.Files = append([]bundle.File(nil), p.bd.Files...)
	f := hostile.Files[0]
	f.Segments = append([]bundle.Segment(nil), f.Segments...)
	// move 1000 bytes of the usable range from one article to the next and
	// declare the first one that much smaller, as a one-article lie would.
	f.Segments[1].Size -= 1000
	f.Segments[1].End -= 1000
	f.Segments[2].Size += 1000
	f.Segments[2].End += 1000
	hostile.Files[0] = f
	rpReseal(t, &hostile)
	_, err := rpAccept(t, p, &hostile)
	require.Error(t, err)
	assert.Equal(t, bundleimport.ReasonSizeImplausible, bundleimport.ReasonOf(err), "%v", err)
}

// rpSevenZipVolumes posts one stored 7z archive cut raw into n volumes
// (Release.7z.001 ...), as a split 7z release is.
func rpSevenZipVolumes(name string, pl []byte, n int) func(func(string, []byte) nzbbuild.File) []nzbbuild.File {
	return func(post func(string, []byte) nzbbuild.File) []nzbbuild.File {
		a := archivetest.SevenZip(archivetest.SevenZipOpts{}, archivetest.File{Name: name, Data: pl}).Data
		each := (len(a) + n - 1) / n
		var out []nzbbuild.File
		for i := 0; i < n; i++ {
			lo, hi := i*each, min(len(a), (i+1)*each)
			out = append(out, post(fmt.Sprintf("Release.7z.%03d", i+1), a[lo:hi]))
		}
		return out
	}
}

// A split 7z set holding a media file: the honest bundle is accepted and bound
// through the 7z headers; dropping articles from it is refused.
func TestRealPipelineHostile_SplitSevenZipTailDrop(t *testing.T) {
	data := mediagen.MKVClusters(rpPayload, 20_000)
	p := rpPublish(t, "z7", data, rpSevenZipVolumes("Movie.2024.mkv", data, 4))
	f := p.bd.Files[0]
	require.Len(t, f.Parts, 4, "the real builder wrote a four-part layout of the split 7z set")
	res, err := rpAccept(t, p, p.bd)
	require.NoError(t, err, "the honest split 7z bundle is accepted: %v", err)
	require.NotNil(t, res)

	for _, c := range []struct {
		name   string
		part   int
		reason bundleimport.Reason
	}{
		{"a middle part loses its tail", 1, bundleimport.ReasonExtentUnverifiable},
		{"the first part loses its tail", 0, bundleimport.ReasonExtentUnverifiable},
		{"the last part loses its tail", 3, bundleimport.ReasonExtentMismatch},
	} {
		t.Run(c.name, func(t *testing.T) {
			bd := rpMutated(t, p.bd, func(f *bundle.File) {
				_, to := partRange(f, c.part)
				f.Segments = append(f.Segments[:to-3], f.Segments[to:]...)
				f.Parts[c.part] -= 3
			})
			_, err := rpAccept(t, p, bd)
			require.Error(t, err)
			assert.Equal(t, c.reason, bundleimport.ReasonOf(err), "%v", err)
		})
	}
}
