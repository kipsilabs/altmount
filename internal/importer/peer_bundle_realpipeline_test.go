package importer

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
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
	"github.com/stretchr/testify/require"
)

// notShareable lists release types a publisher is allowed to refuse, with the
// reason. A type that is not here must build, verify, be accepted by a
// receiver and read back byte-identically.
var notShareable = map[string]string{}

type rpProber struct{}

func (rpProber) ProbeRelease(context.Context, []bundleimport.ProbeFile) error { return nil }

type rpAnnouncer struct{}

func (rpAnnouncer) AnnounceVerifiedImport(context.Context, bundleimport.Announcement) error {
	return nil
}

type rpReporter struct{}

func (rpReporter) ReportBadBundle(context.Context, bundleimport.Origin, string)      {}
func (rpReporter) ReportVerifiedBundle(context.Context, bundleimport.Origin, string) {}
func (rpReporter) ReportFailedBundle(context.Context, string, string)                {}

type rpOrigins struct{ fromNZB bool }

func (o rpOrigins) FromNZB(context.Context, string) (bool, error) { return o.fromNZB, nil }

type rpLibrary struct{ paths []string }

func (l rpLibrary) ReleaseFiles(context.Context, string) (int, []string, error) {
	return database.PeernetSchemeNZB, l.paths, nil
}

// rpRelease is one release type of the matrix: the posted NZB files and the
// payload (the media file as the library will show it).
type rpRelease struct {
	name     string
	payload  string // library file name of the payload
	data     []byte
	password string
	build    func(post func(name string, data []byte) nzbbuild.File) []nzbbuild.File
}

const (
	rpSeg     = 40_000
	rpPayload = 500_000
)

func rpRar5Volumes(name string, pl []byte, n int) func(func(string, []byte) nzbbuild.File) []nzbbuild.File {
	return func(post func(string, []byte) nzbbuild.File) []nzbbuild.File {
		vol := (len(pl) + n - 1) / n
		var out []nzbbuild.File
		for i := 0; i < n; i++ {
			lo, hi := i*vol, min(len(pl), (i+1)*vol)
			b := archivetest.RAR5Volume(i, archivetest.File{Name: name, Data: pl[lo:hi], SplitBefore: i > 0, SplitAfter: i < n-1, UnpackedSize: int64(len(pl))})
			out = append(out, post(fmt.Sprintf("Release.part%02d.rar", i+1), b.Data))
		}
		return out
	}
}

func rpRar4Volumes(name string, pl []byte, n int) func(func(string, []byte) nzbbuild.File) []nzbbuild.File {
	return func(post func(string, []byte) nzbbuild.File) []nzbbuild.File {
		vol := (len(pl) + n - 1) / n
		var out []nzbbuild.File
		for i := 0; i < n; i++ {
			lo, hi := i*vol, min(len(pl), (i+1)*vol)
			b := archivetest.RAR4Volume(i, i == n-1, archivetest.File{Name: name, Data: pl[lo:hi], SplitBefore: i > 0, SplitAfter: i < n-1, UnpackedSize: int64(len(pl))})
			out = append(out, post(fmt.Sprintf("Release.part%02d.rar", i+1), b.Data))
		}
		return out
	}
}

func rpEncrypted(name string, pl []byte, pw string) func(func(string, []byte) nzbbuild.File) []nzbbuild.File {
	return func(post func(string, []byte) nzbbuild.File) []nzbbuild.File {
		salt := make([]byte, 16)
		iv := []byte("0123456789abcdef")
		key, err := pbkdf2.Key(sha256.New, pw, salt, 1<<15, 32)
		if err != nil {
			panic(err)
		}
		blk, _ := aes.NewCipher(key)
		padded := append([]byte{}, pl...)
		for len(padded)%16 != 0 {
			padded = append(padded, 0)
		}
		cipher.NewCBCEncrypter(blk, iv).CryptBlocks(padded, padded)
		b := archivetest.RAR5(archivetest.File{Name: name, Data: padded, IV: iv, UnpackedSize: int64(len(pl))})
		return []nzbbuild.File{post("Release.rar", b.Data)}
	}
}

func rpReleases() []rpRelease {
	mkv := mediagen.MKV(rpPayload)
	avi := mediagen.AVI(rpPayload)
	plain := func(n string, d []byte) func(func(string, []byte) nzbbuild.File) []nzbbuild.File {
		return func(post func(string, []byte) nzbbuild.File) []nzbbuild.File { return []nzbbuild.File{post(n, d)} }
	}
	return []rpRelease{
		{name: "plain mkv", payload: "Movie.2024.mkv", data: mkv, build: plain("Movie.2024.mkv", mkv)},
		{name: "plain avi (generic container)", payload: "Movie.2024.avi", data: avi, build: plain("Movie.2024.avi", avi)},
		{name: "rar5 single volume stored", payload: "Movie.2024.mkv", data: mkv, build: func(post func(string, []byte) nzbbuild.File) []nzbbuild.File {
			return []nzbbuild.File{post("Release.rar", archivetest.RAR5(archivetest.File{Name: "Movie.2024.mkv", Data: mkv}).Data)}
		}},
		{name: "rar5 multi volume stored (4)", payload: "Movie.2024.mkv", data: mkv, build: rpRar5Volumes("Movie.2024.mkv", mkv, 4)},
		{name: "rar4 single volume stored", payload: "Movie.2024.mkv", data: mkv, build: func(post func(string, []byte) nzbbuild.File) []nzbbuild.File {
			return []nzbbuild.File{post("Release.rar", archivetest.RAR4(archivetest.File{Name: "Movie.2024.mkv", Data: mkv}).Data)}
		}},
		{name: "rar4 multi volume stored (4)", payload: "Movie.2024.mkv", data: mkv, build: rpRar4Volumes("Movie.2024.mkv", mkv, 4)},
		{name: "rar5 encrypted, password in the NZB", payload: "Movie.2024.mkv", data: mkv, password: "fixture-pw-123", build: rpEncrypted("Movie.2024.mkv", mkv, "fixture-pw-123")},
	}
}

func rpVirtualPaths(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	require.NoError(t, filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".meta") {
			rel, _ := filepath.Rel(root, strings.TrimSuffix(p, ".meta"))
			out = append(out, filepath.ToSlash(rel))
		}
		return err
	}))
	return out
}

func TestRealPipelineEveryReleaseTypeIsShareableAndAccepted(t *testing.T) {
	for _, rel := range rpReleases() {
		t.Run(rel.name, func(t *testing.T) {
			ctx := context.Background()
			// Publisher: the real importer over the fake pool.
			env := newBatteryEnv(t)
			post := func(name string, data []byte) nzbbuild.File {
				var segs []nzbbuild.Segment
				total := (len(data) + rpSeg - 1) / rpSeg
				for i := 0; i < total; i++ {
					lo, hi := i*rpSeg, min(len(data), (i+1)*rpSeg)
					id := fmt.Sprintf("%s-%s-%03d@rp.test", strings.ReplaceAll(rel.name, " ", ""), name, i+1)
					env.client.SetBehavior(id, fakepool.SegmentBehavior{Bytes: data[lo:hi], YEnc: nntppool.YEncMeta{
						FileName: name, FileSize: int64(len(data)), Part: int64(i + 1), Total: int64(total), PartBegin: int64(lo), PartSize: int64(hi - lo)}})
					segs = append(segs, nzbbuild.Segment{ID: id, Bytes: hi - lo})
				}
				return nzbbuild.File{Subject: name, Segments: segs}
			}
			nzb := nzbbuild.Build(rel.build(post)...)
			if rel.password != "" {
				nzb.Meta = map[string]string{"password": rel.password}
			}
			_, _, err := env.runImport(nzb, "Release.Name.2024.1080p")
			require.NoError(t, err, "the real importer must import the fixture")
			paths := rpVirtualPaths(t, env.metaRoot)
			require.NotEmpty(t, paths)

			src := bundlex.NewMetadataSource(env.svc, rpOrigins{fromNZB: rel.password != ""})
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

			builder := bundlex.NewBuilder(bundlex.BuilderConfig{Source: src, Library: rpLibrary{paths: paths}, Patches: bundlex.NoPatches{}})
			built, err := builder.Build(ctx, rid.String(), "Release Name 2024 1080p", time.Unix(1_700_000_000, 0))
			if reason, ok := notShareable[rel.name]; ok {
				require.Error(t, err, "documented not shareable: %s", reason)
				return
			}
			require.NoError(t, err, "a release the importer produced must be shareable")
			t.Logf("MATRIX %-40s built=yes scheme=%d wire=%d bytes", rel.name, built.Scheme, len(built.Wire))
			bd, err := bundle.Verify(built.Wire, built.ID, rid)
			require.NoError(t, err)

			// Receiver: real accept over the same fake pool, real stager.
			lib := metadata.NewMetadataService(t.TempDir())
			db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			cfg := config.DefaultConfig()
			pm := processorTestPoolManager{client: env.client}
			stager := bundleimport.NewFSStager(bundleimport.FSStagerDeps{Pool: pm, Config: func() *config.Config { return cfg }, Root: t.TempDir()})
			imp := bundleimport.New(bundleimport.Config{
				Library: lib, Stager: stager, Prober: rpProber{}, Headers: bundleimport.NewPoolHeaderReader(pm),
				State: database.NewPeernetBundleImportRepository(db.Connection(), database.DialectSQLite), Announcer: rpAnnouncer{}, Reporter: rpReporter{},
				HealthEnabled: func() bool { return true },
			})
			res, err := imp.Accept(ctx, bundleimport.Request{Bundle: bd, Target: bundleimport.Target{Dir: "movies/Release.Name.2024.1080p", Category: "movies"}})
			require.NoError(t, err, "a receiver must accept what the publisher announced")
			require.Len(t, res.Paths, 1)
			require.True(t, strings.HasSuffix(res.Paths[0], "/"+rel.payload), "accepted file %q, want name %q", res.Paths[0], rel.payload)

			stage, err := stager.NewStaging()
			require.NoError(t, err)
			defer stage.Close()
			m, err := lib.ReadFileMetadata(res.Paths[0])
			require.NoError(t, err)
			require.NoError(t, stage.WriteFileMetadata(res.Paths[0], m))
			f, err := stage.Opener().OpenFile(ctx, res.Paths[0], os.O_RDONLY, 0)
			require.NoError(t, err)
			defer f.Close()
			got, err := io.ReadAll(f)
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256(rel.data), sha256.Sum256(got), "accepted file bytes differ from the payload")
			t.Logf("MATRIX %-40s accepted=yes bytes-identical=yes size=%d", rel.name, len(got))
		})
	}
}
