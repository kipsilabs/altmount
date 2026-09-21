package nzbfilesystem

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/usenet"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const reverifySegmentID = "a@b.example.com"

// writeStreamMetaN writes metadata for a file of n equally sized segments, the
// first of which carries reverifySegmentID. A single missing segment out of n
// stays under every hole threshold, so it classifies as degraded rather than
// failed — which writeStreamMeta's single-segment file cannot do.
func writeStreamMetaN(t *testing.T, ms *metadata.MetadataService, filePath string, n int) []*metapb.SegmentData {
	t.Helper()

	const segSize = 1024
	segs := make([]*metapb.SegmentData, n)
	for i := range segs {
		id := fmt.Sprintf("seg%d@example.com", i)
		if i == 0 {
			id = reverifySegmentID
		}
		segs[i] = &metapb.SegmentData{
			Id:          id,
			SegmentSize: segSize,
			StartOffset: 0,
			EndOffset:   segSize - 1,
		}
	}

	meta := ms.CreateFileMetadata(
		int64(n)*segSize, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		segs, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(filePath, meta))
	return meta.SegmentData
}

// newImportedStreamFile wires an mvf whose file is already imported (health row
// carries a library_path), so the repair branch would move its metadata to the
// corrupted safety folder.
func newImportedStreamFile(
	t *testing.T, ctx context.Context, filePath string,
	repo *database.HealthRepository, db *sql.DB, ms *metadata.MetadataService,
	seg []*metapb.SegmentData, fp *fakepool.Client,
) *MetadataVirtualFile {
	t.Helper()

	_, err := db.Exec(
		`INSERT INTO file_health (file_path, library_path, status, scheduled_check_at) VALUES (?, ?, 'healthy', datetime('now'))`,
		filePath, "/media/library/"+filePath,
	)
	require.NoError(t, err)

	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.MountPath = ""

	mvf := newStreamFailureMVF(ctx, filePath, repo, ms, seg, cfg)
	if fp != nil {
		mvf.poolManager = newFakePoolManager(fp)
	}
	return mvf
}

// A single 430 mid-stream is not proof the article is gone: providers return it
// transiently (backend desync), and the reader never retries an
// ErrArticleNotFound. Condemning on that evidence moved a healthy file's
// metadata into the corrupted folder and made the ARR redownload it, while
// re-importing the very same NZB minutes later verified clean (issue #749).
func TestUpdateFileHealthOnError_TransientMissIsNotCondemned(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()

	filePath := "series/stream.s01e10.mkv"
	seg := writeStreamMeta(t, ms, filePath)

	// The re-check finds the article present: the 430 was transient.
	fp := fakepool.New()
	fp.SetBehavior(reverifySegmentID, fakepool.SegmentBehavior{Bytes: []byte("present")})

	mvf := newImportedStreamFile(t, ctx, filePath, repo, db, ms, seg, fp)
	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
		UnderlyingErr: nntppool.ErrArticleNotFound,
		SegmentID:     reverifySegmentID,
		NoRetry:       true,
		FileOffset:    -1,
	}, true)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusPending, fh.Status,
		"an unconfirmed miss must be handed to the health worker, not repaired and not forgotten")

	original, readErr := ms.ReadFileMetadata(filePath)
	require.NoError(t, readErr)
	assert.NotNil(t, original, "metadata must stay put when the miss is not confirmed")
	assert.False(t, mvf.metadataGone.Load(), "handle must not latch closed on an unconfirmed miss")
}

// The re-check costs a network round-trip while the caller holds mvf.mu, which
// a concurrent foreground read on the same handle would block on. It must
// therefore only run when something destructive would otherwise follow: a
// degraded verdict is zero-filled and still playable, so it must not pay for a
// STAT at all.
func TestUpdateFileHealthOnError_DegradedMissSkipsTheRecheck(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()

	filePath := "series/stream.s01e12.mkv"
	seg := writeStreamMetaN(t, ms, filePath, 512)

	fp := fakepool.New()
	fp.SetBehavior(reverifySegmentID, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	mvf := newImportedStreamFile(t, ctx, filePath, repo, db, ms, seg, fp)
	// A single missing segment in a large file classifies as degraded.
	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
		UnderlyingErr: nntppool.ErrArticleNotFound,
		SegmentID:     reverifySegmentID,
		NoRetry:       true,
		FileOffset:    0,
	}, true)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	require.Equal(t, database.HealthStatusDegraded, fh.Status,
		"precondition: this failure must classify as degraded")
	assert.Zero(t, fp.StatCalls(),
		"a degraded (still playable) failure must not pay for a re-check on the read path")
}

// A re-check that confirms the 430 keeps the existing repair behavior.
func TestUpdateFileHealthOnError_ConfirmedMissStillTriggersRepair(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()

	filePath := "series/stream.s01e11.mkv"
	seg := writeStreamMeta(t, ms, filePath)

	fp := fakepool.New()
	fp.SetBehavior(reverifySegmentID, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	mvf := newImportedStreamFile(t, ctx, filePath, repo, db, ms, seg, fp)
	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
		UnderlyingErr: nntppool.ErrArticleNotFound,
		SegmentID:     reverifySegmentID,
		NoRetry:       true,
		FileOffset:    -1,
	}, true)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusRepairTriggered, fh.Status,
		"a confirmed miss must still trigger the repair")

	original, readErr := ms.ReadFileMetadata(filePath)
	require.NoError(t, readErr)
	assert.Nil(t, original, "a confirmed miss must still move the metadata away")
}

// When the re-check cannot resolve the question — transport error, no pool —
// behaviour is unchanged: the failure is treated as before rather than newly
// suppressing repairs whenever the pool is briefly unhealthy.
func TestUpdateFileHealthOnError_UnresolvedRecheckKeepsPreviousBehaviour(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	tests := []struct {
		name string
		pool func() *fakepool.Client
	}{
		{
			name: "transport error during re-check",
			pool: func() *fakepool.Client {
				fp := fakepool.New()
				fp.SetBehavior(reverifySegmentID, fakepool.SegmentBehavior{Err: errors.New("connection reset")})
				return fp
			},
		},
		{
			name: "no pool configured",
			pool: func() *fakepool.Client { return nil },
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, db, ms := setupStreamHealthEnv(t)
			ctx := context.Background()

			filePath := "series/stream.s01e2" + string(rune('0'+i)) + ".mkv"
			seg := writeStreamMeta(t, ms, filePath)

			mvf := newImportedStreamFile(t, ctx, filePath, repo, db, ms, seg, tt.pool())
			mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
				UnderlyingErr: nntppool.ErrArticleNotFound,
				SegmentID:     reverifySegmentID,
				NoRetry:       true,
				FileOffset:    -1,
			}, true)

			fh, err := repo.GetFileHealth(ctx, filePath)
			require.NoError(t, err)
			require.NotNil(t, fh)
			assert.Equal(t, database.HealthStatusRepairTriggered, fh.Status)
		})
	}
}

// The reader re-checks a miss before it reaches here (see
// internal/usenet/transient_miss_test.go). When it has already answered, this
// handler must reuse that answer rather than pay a second round trip under
// mvf.mu — even though the article now STATs present, because the reader saw
// the miss survive a full confirm-and-retry cycle.
func TestUpdateFileHealthOnError_ReaderVerdictSkipsSecondCheck(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	tests := []struct {
		name    string
		verdict usenet.MissVerdict
	}{
		{"confirmed gone by the reader", usenet.MissConfirmed},
		{"present but unfetchable", usenet.MissUnfetchable},
		{"reader could not resolve it", usenet.MissUnresolved},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, db, ms := setupStreamHealthEnv(t)
			ctx := context.Background()

			filePath := "series/stream.s01e3" + string(rune('0'+i)) + ".mkv"
			seg := writeStreamMeta(t, ms, filePath)

			// Deliberately present: a second check would say "transient" and
			// wrongly defer a miss the reader already proved out.
			fp := fakepool.New()
			fp.SetBehavior(reverifySegmentID, fakepool.SegmentBehavior{Bytes: []byte("present")})

			mvf := newImportedStreamFile(t, ctx, filePath, repo, db, ms, seg, fp)
			mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
				UnderlyingErr: nntppool.ErrArticleNotFound,
				SegmentID:     reverifySegmentID,
				MissVerdict:   tt.verdict,
				NoRetry:       true,
				FileOffset:    -1,
			}, true)

			fh, err := repo.GetFileHealth(ctx, filePath)
			require.NoError(t, err)
			require.NotNil(t, fh)
			assert.Equal(t, database.HealthStatusRepairTriggered, fh.Status,
				"a miss the reader already settled must be repaired, not deferred")
			assert.Zero(t, fp.StatCalls(),
				"the reader's verdict must be reused, not re-asked under mvf.mu")
		})
	}
}

// Failures that reach here without a reader verdict (the crypt reader, older
// call sites) still get their own check — on the priority lane, since a
// normal-lane STAT can queue behind a large BODY for longer than its budget.
func TestUpdateFileHealthOnError_FallbackRecheckUsesPriorityLane(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()

	filePath := "series/stream.s01e40.mkv"
	seg := writeStreamMeta(t, ms, filePath)

	fp := fakepool.New()
	fp.SetBehavior(reverifySegmentID, fakepool.SegmentBehavior{Bytes: []byte("present")})

	mvf := newImportedStreamFile(t, ctx, filePath, repo, db, ms, seg, fp)
	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
		UnderlyingErr: nntppool.ErrArticleNotFound,
		SegmentID:     reverifySegmentID,
		MissVerdict:   usenet.MissUnverified,
		NoRetry:       true,
		FileOffset:    -1,
	}, true)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusPending, fh.Status)
	assert.Equal(t, int64(1), fp.StatPriorityCalls(),
		"the fallback re-check must not queue behind a body fetch")
}
