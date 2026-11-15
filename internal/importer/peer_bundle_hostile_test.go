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
	lib := metadata.NewMetadataService(t.TempDir())
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	cfg := config.DefaultConfig()
	pm := processorTestPoolManager{client: p.env.client}
	stager := bundleimport.NewFSStager(bundleimport.FSStagerDeps{Pool: pm, Config: func() *config.Config { return cfg }, Root: t.TempDir()})
	imp := bundleimport.New(bundleimport.Config{
		Library: lib, Stager: stager, Prober: rpProber{}, Headers: bundleimport.NewPoolHeaderReader(pm),
		State: database.NewPeernetBundleImportRepository(db.Connection(), database.DialectSQLite), Announcer: rpAnnouncer{}, Reporter: rpReporter{},
		HealthEnabled: func() bool { return true },
	})
	return imp.Accept(context.Background(), bundleimport.Request{Bundle: bd, Target: bundleimport.Target{Dir: "movies/Release.Name.2024.1080p", Category: "movies"}})
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
