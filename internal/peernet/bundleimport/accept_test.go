package bundleimport

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testKey = []byte("0123456789abcdef0123456789abcdef")
	testIV  = []byte("fedcba9876543210")
)

type fakeProber struct {
	err   error
	calls int
	files []ProbeFile
}

func (p *fakeProber) ProbeRelease(_ context.Context, files []ProbeFile) error {
	p.calls++
	p.files = files
	return p.err
}

type fakeAnnouncer struct{ got []Announcement }

func (a *fakeAnnouncer) AnnounceVerifiedImport(_ context.Context, an Announcement) error {
	a.got = append(a.got, an)
	return nil
}

type fakeReporter struct{ got, verified []string }

func (r *fakeReporter) ReportVerifiedBundle(_ context.Context, o Origin, release string) {
	r.verified = append(r.verified, o.PublisherID+"/"+release)
}

func (r *fakeReporter) ReportBadBundle(_ context.Context, o Origin, release string) {
	r.got = append(r.got, o.PublisherID+"/"+release)
}

type env struct {
	t         *testing.T
	libRoot   string
	stageRoot string
	lib       *metadata.MetadataService
	db        *database.DB
	repo      *database.PeernetBundleImportRepository
	fp        *fakepool.Client
	prober    *fakeProber
	ann       *fakeAnnouncer
	rep       *fakeReporter
	healthOn  bool
	imp       *Importer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, libRoot: t.TempDir(), stageRoot: t.TempDir(), fp: fakepool.New(),
		prober: &fakeProber{}, ann: &fakeAnnouncer{}, rep: &fakeReporter{}, healthOn: true}
	e.lib = metadata.NewMetadataService(e.libRoot)
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	e.db = db
	e.repo = database.NewPeernetBundleImportRepository(db.Connection(), database.DialectSQLite)
	cfg := config.DefaultConfig()
	stager := NewFSStager(FSStagerDeps{
		Pool:   &fakePoolManager{client: e.fp},
		Config: func() *config.Config { return cfg },
		Root:   e.stageRoot,
	})
	e.imp = New(Config{Headers: NewPoolHeaderReader(&fakePoolManager{client: e.fp}), Library: e.lib, Stager: stager, Prober: e.prober, State: e.repo, Announcer: e.ann,
		Reporter: e.rep, HealthEnabled: func() bool { return e.healthOn }})
	return e
}

// fileSpec describes one file of a test bundle. The served articles hold plain
// (or its AES-CBC encryption under key); the bundle carries bundleKey.
type fileSpec struct {
	name      string
	plain     []byte
	segSize   int
	key       []byte
	bundleKey []byte
}

// mkvPlain builds a minimal but structurally valid Matroska file of size
// bytes: EBML header, a Segment whose size ends at the file end, a SeekHead
// pointing at Info and at Cues (the last element of the file), a Cluster of
// filler between them.
func mkvPlain(size int) []byte {
	u32 := func(v int) []byte { return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)} }
	seek := func(id []byte, pos int) []byte {
		body := append(append([]byte{0x53, 0xAB, 0x84}, id...), append([]byte{0x53, 0xAC, 0x84}, u32(pos)...)...)
		return append([]byte{0x4D, 0xBB, byte(0x80 | len(body))}, body...)
	}
	info := []byte{0x15, 0x49, 0xA9, 0x66}
	cues := []byte{0x1C, 0x53, 0xBB, 0x6B}
	ebml := append([]byte{0x1A, 0x45, 0xDF, 0xA3, 0x8B, 0x42, 0x82, 0x88}, "matroska"...)
	segStart := len(ebml) + 12
	seekLen := 2 * 17
	seekHead := append([]byte{0x11, 0x4D, 0x9B, 0x74, byte(0x80 | seekLen)}, append(seek(info, seekLen+5), seek(cues, size-segStart-9)...)...)
	out := append([]byte{}, ebml...)
	segSize := size - segStart
	out = append(out, 0x18, 0x53, 0x80, 0x67, 0x01, 0, byte(segSize>>40), byte(segSize>>32), byte(segSize>>24), byte(segSize>>16), byte(segSize>>8), byte(segSize))
	out = append(out, seekHead...)
	out = append(out, append(append([]byte{}, info...), 0x84, 0, 0, 0, 0)...)
	fill := size - len(out) - 6 - 9
	out = append(out, 0x1F, 0x43, 0xB6, 0x75, byte(0x40|fill>>8), byte(fill))
	for k := 0; k < fill; k++ {
		out = append(out, byte((k*7+13)%251))
	}
	out = append(out, append(append([]byte{}, cues...), 0x84, 0, 0, 0, 0)...)
	return out
}

func junkPlain(size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(i%7 + 1)
	}
	return out
}

func cbc(t *testing.T, key, plain []byte) []byte {
	blk, err := aes.NewCipher(key)
	require.NoError(t, err)
	out := make([]byte, len(plain))
	cipher.NewCBCEncrypter(blk, testIV).CryptBlocks(out, plain)
	return out
}

// mkBundle serves the specs' articles from the fake pool and returns a valid
// nzb-scheme bundle (identifiers recomputed from what it describes).
func (e *env) mkBundle(tag string, specs ...fileSpec) *bundle.Bundle {
	e.t.Helper()
	b := &bundle.Bundle{Scheme: bundle.SchemeNZB, Title: "Test Release " + tag, PostedUnix: 1_700_000_000}
	for fi, sp := range specs {
		payload := sp.plain
		var key, iv []byte
		if sp.key != nil {
			payload = cbc(e.t, sp.key, sp.plain)
			key, iv = sp.key, testIV
			if sp.bundleKey != nil {
				key = sp.bundleKey
			}
		}
		var segs []bundle.Segment
		count := 0
		for i := 0; i*sp.segSize < len(payload); i++ {
			end := min((i+1)*sp.segSize, len(payload))
			id := fmt.Sprintf("%s-%d-%03d@test", tag, fi, i)
			e.fp.SetBehavior(id, fakepool.SegmentBehavior{Bytes: payload[i*sp.segSize : end],
				YEnc: nntppool.YEncMeta{PartBegin: int64(i * sp.segSize), PartSize: int64(end - i*sp.segSize), FileSize: int64(len(payload))}})
			segs = append(segs, bundle.Segment{Ref: int64(len(b.Pool)), Size: int64(end - i*sp.segSize), Start: 0, End: int64(end - i*sp.segSize - 1)})
			b.Pool = append(b.Pool, id)
			count++
		}
		b.ReleaseFiles = append(b.ReleaseFiles, count)
		b.Files = append(b.Files, bundle.File{Name: sp.name, Size: int64(len(sp.plain)), Segments: segs, AESKey: key, AESIV: iv})
		b.TotalSize += int64(len(sp.plain))
	}
	for i := range b.Files { // provisional ascending ids so ToLocal validates
		b.Files[i].FileID = contentid.ID{byte(i + 1)}
	}
	b.ReleaseID = contentid.ID{9}
	loc, err := bundle.ToLocal(b, e.imp.cfg.Now())
	require.NoError(e.t, err)
	for i := range b.Files {
		id, err := contentid.FileID(loc.Files[i].Meta)
		require.NoError(e.t, err)
		b.Files[i].FileID = id
	}
	sort.Slice(b.Files, func(i, j int) bool { return string(b.Files[i].FileID[:]) < string(b.Files[j].FileID[:]) })
	store := &metapb.NzbStore{}
	pos := 0
	for _, n := range b.ReleaseFiles {
		fe := &metapb.NzbFileEntry{}
		for _, id := range b.Pool[pos : pos+n] {
			fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id})
		}
		pos += n
		store.Files = append(store.Files, fe)
	}
	rid, err := contentid.ReleaseID(store)
	require.NoError(e.t, err)
	b.ReleaseID = rid
	require.NoError(e.t, bundle.Validate(b))
	return b
}

func (e *env) req(b *bundle.Bundle) Request {
	return Request{Bundle: b, Origin: Origin{NetworkID: strings.Repeat("c", 64), PublisherID: strings.Repeat("d", 64)},
		Target: Target{Dir: "movies/Test.Release", Category: "movies", QueueID: 11}}
}

func (e *env) filesOnDisk(root string) []string {
	var out []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			out = append(out, p)
		}
		return nil
	})
	return out
}

func (e *env) stateRows() int {
	var n int
	require.NoError(e.t, e.db.Connection().QueryRow(`SELECT COUNT(*) FROM peernet_bundle_imports`).Scan(&n))
	return n
}

func (e *env) assertNothingBehind() {
	e.t.Helper()
	assert.Empty(e.t, e.filesOnDisk(e.libRoot), "no metadata file may be written")
	entries, _ := os.ReadDir(e.stageRoot)
	assert.Empty(e.t, entries, "no staging leftovers")
	assert.Zero(e.t, e.stateRows(), "no state row")
	var idx, q, h, fh int
	conn := e.db.Connection()
	_ = conn.QueryRow(`SELECT COUNT(*) FROM peernet_content_index`).Scan(&idx)
	_ = conn.QueryRow(`SELECT COUNT(*) FROM import_queue`).Scan(&q)
	_ = conn.QueryRow(`SELECT COUNT(*) FROM import_history`).Scan(&h)
	_ = conn.QueryRow(`SELECT COUNT(*) FROM file_health`).Scan(&fh)
	assert.Zero(e.t, idx+q+h+fh, "no index, queue, history or health row")
}

func mkvSpec(name string) fileSpec { return fileSpec{name: name, plain: mkvPlain(512), segSize: 128} }

func TestAccept_WritesPendingFilesAndMarksOrigin(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("ok", mkvSpec("movie.mkv"))
	res, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	assert.Equal(t, []string{"movies/Test.Release/movie.mkv"}, res.Paths)
	assert.Equal(t, "movies/Test.Release", res.Dir)
	assert.False(t, res.AlreadyImported)

	meta, err := e.lib.ReadFileMetadata("movies/Test.Release/movie.mkv")
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, int64(512), meta.FileSize)

	row, ok, err := e.repo.Get(context.Background(), "movies/Test.Release/movie.mkv")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, database.PeernetBundleStatePending, row.State)
	assert.Equal(t, strings.Repeat("d", 64), row.PublisherID)
	assert.Equal(t, strings.Repeat("c", 64), row.NetworkID)
	assert.Equal(t, b.ReleaseID.String(), row.ReleaseID)
	assert.Equal(t, int64(11), row.QueueID)
	assert.True(t, row.ArrOwed)
	assert.Equal(t, uint64(1), e.imp.Counters().Accepted)
	entries, _ := os.ReadDir(e.stageRoot)
	assert.Empty(t, entries, "staging removed")
}

func TestAccept_TargetComesFromTheRequestNeverTheBundle(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("tgt", mkvSpec("movie.mkv"))
	b.Title = "movies Evil Title" // the title never steers the folder
	req := e.req(b)
	req.Target.Dir = "tv/Local.Choice"
	res, err := e.imp.Accept(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, []string{"tv/Local.Choice/movie.mkv"}, res.Paths)
}

func TestAccept_StoresNZBPasswordOpaquelyOnTheFiles(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("pw", mkvSpec("movie.mkv"))
	b.NZBPassword = "s3cret pass"
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	meta, err := e.lib.ReadFileMetadata("movies/Test.Release/movie.mkv")
	require.NoError(t, err)
	assert.Equal(t, "s3cret pass", meta.Password)
	assert.Equal(t, metapb.Encryption_NONE, meta.Encryption, "the password is not used to decrypt anything")
}

func TestAccept_RejectsBadBundles(t *testing.T) {
	cases := map[string]struct {
		mut    func(e *env, b *bundle.Bundle, req *Request)
		reason Reason
	}{
		"nil bundle":       {func(e *env, b *bundle.Bundle, r *Request) { r.Bundle = nil }, ReasonInvalid},
		"size mismatch":    {func(e *env, b *bundle.Bundle, r *Request) { b.TotalSize++ }, ReasonInvalid},
		"path separator":   {func(e *env, b *bundle.Bundle, r *Request) { b.Files[0].Name = "a/b.mkv" }, ReasonBadName},
		"dotdot name":      {func(e *env, b *bundle.Bundle, r *Request) { b.Files[0].Name = ".." }, ReasonBadName},
		"empty target":     {func(e *env, b *bundle.Bundle, r *Request) { r.Target.Dir = "" }, ReasonBadTarget},
		"traversal target": {func(e *env, b *bundle.Bundle, r *Request) { r.Target.Dir = "movies/../../x" }, ReasonBadTarget},
		"too large": {func(e *env, b *bundle.Bundle, r *Request) {
			e.imp.cfg.Policy.MaxTotalSize = 100
		}, ReasonTooLarge},
		"too many files":  {func(e *env, b *bundle.Bundle, r *Request) { e.imp.cfg.Policy.MaxFiles = 0 + 1 }, ReasonTooManyFiles},
		"health disabled": {func(e *env, b *bundle.Bundle, r *Request) { e.healthOn = false }, ReasonHealthDisabled},
		"release id wrong": {func(e *env, b *bundle.Bundle, r *Request) {
			b.ReleaseID[0] ^= 0xff
		}, ReasonInvalid},
		"file id wrong": {func(e *env, b *bundle.Bundle, r *Request) { b.Files[0].FileID[5] ^= 0xff }, ReasonInvalid},
		"nzb mismatch": {func(e *env, b *bundle.Bundle, r *Request) {
			r.ExpectedNZB = [][]string{{"other@x"}}
		}, ReasonNZBMismatch},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			b := e.mkBundle("bad", mkvSpec("movie.mkv"), mkvSpec("second.mkv"))
			req := e.req(b)
			tc.mut(e, b, &req)
			_, err := e.imp.Accept(context.Background(), req)
			require.Error(t, err)
			assert.Equal(t, tc.reason, ReasonOf(err), "%v", err)
			assert.Equal(t, uint64(1), e.imp.Counters().Rejected[tc.reason])
			e.assertNothingBehind()
			assert.Empty(t, e.rep.got, "a local rejection never strikes anyone")
		})
	}
}

func TestAccept_NZBCrossCheckAcceptsTheSameList(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("same", mkvSpec("movie.mkv"))
	req := e.req(b)
	req.ExpectedNZB = [][]string{append([]string{}, b.Pool...)}
	_, err := e.imp.Accept(context.Background(), req)
	require.NoError(t, err)
}

func TestAccept_CollisionWithDifferentContentIsRejected(t *testing.T) {
	e := newEnv(t)
	other := e.mkBundle("other", mkvSpec("movie.mkv"))
	_, err := e.imp.Accept(context.Background(), e.req(other))
	require.NoError(t, err)

	b := e.mkBundle("mine", mkvSpec("movie.mkv"), mkvSpec("extra.mkv"))
	_, err = e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonCollision, ReasonOf(err))
	assert.False(t, e.lib.FileExists("movies/Test.Release/extra.mkv"), "nothing of the rejected bundle was written")
	meta, err := e.lib.ReadFileMetadata("movies/Test.Release/movie.mkv")
	require.NoError(t, err)
	assert.Equal(t, other.Pool[0], meta.SegmentData[0].Id, "the existing file is untouched")
}

func TestAccept_SameFileIDAtPathIsIdempotent(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("idem", mkvSpec("movie.mkv"))
	ctx := context.Background()
	_, err := e.imp.Accept(ctx, e.req(b))
	require.NoError(t, err)
	_, err = e.repo.MarkPassed(ctx, "movies/Test.Release/movie.mkv")
	require.NoError(t, err)
	calls := e.prober.calls

	res, err := e.imp.Accept(ctx, e.req(b))
	require.NoError(t, err)
	assert.True(t, res.AlreadyImported)
	assert.Equal(t, calls, e.prober.calls, "no second probe")
	row, _, _ := e.repo.Get(ctx, "movies/Test.Release/movie.mkv")
	assert.Equal(t, database.PeernetBundleStatePassed, row.State, "a passed file is not reset to pending")
	assert.Equal(t, uint64(1), e.imp.Counters().AlreadyImported)
}

func TestAccept_ProbeFailureRejectsWithoutStrikeOrLeftovers(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		reason Reason
	}{
		"missing":      {ErrProbeMissing, ReasonProbeMissing},
		"inconclusive": {ErrProbeInconclusive, ReasonProbeInconclusive},
		"other":        {errors.New("boom"), ReasonProbeInconclusive},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.prober.err = tc.err
			b := e.mkBundle("probe", mkvSpec("movie.mkv"))
			_, err := e.imp.Accept(context.Background(), e.req(b))
			require.Error(t, err)
			assert.Equal(t, tc.reason, ReasonOf(err))
			e.assertNothingBehind()
			assert.Empty(t, e.rep.got, "the publisher may simply have other providers: no strike")
		})
	}
}

func TestAccept_ProbeSeesEveryFilesSegments(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("pf", mkvSpec("movie.mkv"), mkvSpec("second.mkv"))
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	require.Len(t, e.prober.files, 2)
	total := 0
	for _, f := range e.prober.files {
		assert.Len(t, f.SegmentIDs, 4)
		total += len(f.SegmentIDs)
	}
	assert.Equal(t, 8, total)
}

func TestAccept_WrongAESKeyFailsMediaProbeAndLeavesNothing(t *testing.T) {
	e := newEnv(t)
	wrong := []byte("WRONGKEY9876543210WRONGKEY987654")
	b := e.mkBundle("aes", fileSpec{name: "movie.mkv", plain: mkvPlain(512), segSize: 128, key: testKey, bundleKey: wrong})
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonMediaInvalid, ReasonOf(err), "%v", err)
	e.assertNothingBehind()
	assert.Empty(t, e.rep.got)
}

func TestAccept_RightAESKeyPassesMediaProbe(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("aesok", fileSpec{name: "movie.mkv", plain: mkvPlain(512), segSize: 128, key: testKey})
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	assert.True(t, e.lib.FileExists("movies/Test.Release/movie.mkv"))
}

func TestAccept_GarbageMediaFailsProbe(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("junk", fileSpec{name: "movie.mkv", plain: junkPlain(512), segSize: 128})
	_, err := e.imp.Accept(context.Background(), e.req(b))
	assert.Equal(t, ReasonMediaInvalid, ReasonOf(err))
	e.assertNothingBehind()
}

func TestAccept_MissingHeadArticleFailsProbeAsMissing(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("head", mkvSpec("movie.mkv"))
	e.fp.SetBehavior(b.Pool[0], fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	_, err := e.imp.Accept(context.Background(), e.req(b))
	// The first article is always among the yEnc samples, so its absence is
	// now found there (before the media probe would find it).
	assert.Equal(t, ReasonProbeMissing, ReasonOf(err), "%v", err)
	e.assertNothingBehind()
}

func TestAccept_NoVerifiableMediaFileIsRejected(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("doc", fileSpec{name: "readme.txt", plain: junkPlain(256), segSize: 128})
	_, err := e.imp.Accept(context.Background(), e.req(b))
	assert.Equal(t, ReasonNoMediaFile, ReasonOf(err))
	e.assertNothingBehind()
}

func TestAccept_WriteFailureRollsBackEverything(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("wf", mkvSpec("movie.mkv"), mkvSpec("second.mkv"))
	lib := &failingLibrary{Library: e.lib, failOn: 2}
	e.imp.cfg.Library = lib
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonWriteFailed, ReasonOf(err))
	e.assertNothingBehind()
}

type failingLibrary struct {
	Library
	failOn, n int
}

func (l *failingLibrary) WriteFileMetadata(p string, m *metapb.FileMetadata) error {
	l.n++
	if l.n == l.failOn {
		return errors.New("disk full")
	}
	return l.Library.WriteFileMetadata(p, m)
}

func TestAccept_RecordsTheVerifiedReleaseIdentityInTheContentIndex(t *testing.T) {
	e := newEnv(t)
	content := database.NewPeernetContentRepository(e.db.Connection(), database.DialectSQLite)
	e.imp.cfg.Index = content
	b := e.mkBundle("idx", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	res, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	for _, p := range res.Paths {
		row, ok, err := content.GetByPath(context.Background(), p)
		require.NoError(t, err)
		require.True(t, ok, p)
		assert.Equal(t, b.ReleaseID.String(), row.ReleaseID)
		assert.Equal(t, database.PeernetSchemeNZB, row.ReleaseScheme)
		assert.NotEmpty(t, row.GroupKey)
		assert.True(t, contentid.IsVerifiedBundleRow(row))
		meta, err := e.lib.ReadFileMetadata(p)
		require.NoError(t, err)
		fid, err := contentid.FileID(meta)
		require.NoError(t, err)
		assert.Equal(t, fid.String(), row.FileID)
	}
}

func TestAccept_FilesSchemeBundleIsIndexedAsFilesScheme(t *testing.T) {
	e := newEnv(t)
	content := database.NewPeernetContentRepository(e.db.Connection(), database.DialectSQLite)
	e.imp.cfg.Index = content
	b := e.mkBundle("idxf", mkvSpec("a.mkv"))
	b.Scheme = bundle.SchemeFiles
	b.ReleaseFiles = nil
	ids := []contentid.ID{b.Files[0].FileID}
	b.ReleaseID = bundle.FilesReleaseID(ids)
	req := e.req(b)
	_, err := e.imp.Accept(context.Background(), req)
	require.NoError(t, err)
	row, ok, err := content.GetByPath(context.Background(), "movies/Test.Release/a.mkv")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, database.PeernetSchemeFiles, row.ReleaseScheme)
	assert.Equal(t, b.ReleaseID.String(), row.ReleaseID)
}

func TestAccept_WriteFailureLeavesNoIndexRows(t *testing.T) {
	e := newEnv(t)
	e.imp.cfg.Index = database.NewPeernetContentRepository(e.db.Connection(), database.DialectSQLite)
	b := e.mkBundle("idxw", mkvSpec("movie.mkv"), mkvSpec("second.mkv"))
	e.imp.cfg.Library = &failingLibrary{Library: e.lib, failOn: 2}
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	e.assertNothingBehind() // includes the content index
}

func TestAccept_ResultCarriesFileSizesInPathOrder(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("sz", fileSpec{name: "a.mkv", plain: mkvPlain(512), segSize: 128}, fileSpec{name: "b.mkv", plain: mkvPlain(256), segSize: 128})
	res, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	require.Len(t, res.Sizes, len(res.Paths))
	for n, p := range res.Paths {
		meta, err := e.lib.ReadFileMetadata(p)
		require.NoError(t, err)
		assert.Equal(t, meta.FileSize, res.Sizes[n], p)
	}
}
