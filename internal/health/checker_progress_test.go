package health

import (
	"context"
	"github.com/javi11/nntppool/v5"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArticleCheckProgress(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "manual"
		if batch {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			env := newBatchTestEnv(t, t.TempDir(), fakepool.New(), func(c *config.Config) {
				c.Health.CheckAllSegments = ptr(false)
				c.Health.SegmentSamplePercentage = 10
				c.Health.MaxConcurrentSegmentChecks = ptr(1)
			})
			const path = "complete/progress.mkv"
			writeMultiSegmentFile(t, env, path, 10)
			var updates []CheckProgress
			opts := CheckOptions{OnProgress: func(filePath string, p CheckProgress) {
				assert.Equal(t, path, filePath)
				updates = append(updates, p)
			}}
			if batch {
				env.healthChecker.CheckFilesBatch(context.Background(), []string{path}, nil, opts)
			} else {
				env.healthChecker.CheckFile(context.Background(), path, opts)
			}
			require.Len(t, updates, 6)
			for i, p := range updates {
				assert.Equal(t, 10, p.TotalArticles)
				assert.Equal(t, 5, p.ArticlesToCheck)
				assert.Equal(t, i, p.ArticlesChecked)
			}
		})
	}
}

func TestArticleProgressFailuresAndEarlyStop(t *testing.T) {
	for _, tc := range []struct {
		name        string
		providerErr error
		wantChecked int
	}{
		{"unresolved attempts", nntppool.ErrConnectionDied, 3},
		{"early stop", nntppool.ErrArticleNotFound, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fakepool.New()
			env := newBatchTestEnv(t, t.TempDir(), client, func(c *config.Config) {
				c.Health.CheckAllSegments = ptr(true)
				c.Health.MaxConcurrentSegmentChecks = ptr(1)
				c.Health.AcceptableMissingSegmentsPercentage = 0
			})
			const path = "complete/progress.mkv"
			ids := writeMultiSegmentFile(t, env, path, 3)
			for _, id := range ids {
				client.SetBehavior(id, fakepool.SegmentBehavior{Err: tc.providerErr})
			}
			var updates []CheckProgress
			env.healthChecker.CheckFile(context.Background(), path, CheckOptions{OnProgress: func(_ string, p CheckProgress) { updates = append(updates, p) }})
			require.NotEmpty(t, updates)
			assert.Zero(t, updates[0].ArticlesChecked)
			assert.Equal(t, CheckProgress{TotalArticles: 3, ArticlesToCheck: 3, ArticlesChecked: tc.wantChecked}, updates[len(updates)-1])
		})
	}
}

func TestWorkerProgressOwnership(t *testing.T) {
	env := newBatchTestEnv(t, t.TempDir(), fakepool.New())
	const path = "complete/progress.mkv"
	_, release, report := env.hw.beginBatchChecks(context.Background(), []string{path})
	require.Nil(t, env.hw.GetCheckProgress(path))
	report(path, CheckProgress{TotalArticles: 10, ArticlesToCheck: 5, ArticlesChecked: 1})
	snapshot := env.hw.GetCheckProgress(path)
	require.NotNil(t, snapshot)
	snapshot.ArticlesChecked = 100
	assert.Equal(t, 1, env.hw.GetCheckProgress(path).ArticlesChecked)
	release()
	require.Nil(t, env.hw.GetCheckProgress(path))
	_, releaseNew, reportNew := env.hw.beginBatchChecks(context.Background(), []string{path})
	defer releaseNew()
	reportNew(path, CheckProgress{TotalArticles: 20, ArticlesToCheck: 10, ArticlesChecked: 2})
	report(path, CheckProgress{ArticlesChecked: 5})
	assert.Equal(t, 2, env.hw.GetCheckProgress(path).ArticlesChecked)
}

// Observe the start notification at the network boundary, before the sweep
// can complete and emit its existing terminal notification.
type progressObservingClient struct {
	pool.NntpClient
	beforeSweep func()
}

func (c *progressObservingClient) ExistsMany(ctx context.Context, ids []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	c.beforeSweep()
	return c.NntpClient.ExistsMany(ctx, ids, opts)
}

func TestScheduledCheckNotifiesBeforeArticleSweep(t *testing.T) {
	client := &progressObservingClient{NntpClient: fakepool.New()}
	env := newBatchTestEnv(t, t.TempDir(), client)
	const path = "complete/progress.mkv"
	writeHealthyFile(t, env, path)
	insertFileHealth(t, env.db, path, "", 0, 3)
	broadcaster := progress.NewProgressBroadcaster()
	env.hw.progressBroadcaster = broadcaster
	sub, updates := broadcaster.Subscribe()
	defer broadcaster.Unsubscribe(sub)
	observed := false
	client.beforeSweep = func() {
		observed = true
		select {
		case update := <-updates:
			assert.Equal(t, "health_changed", update.Status)
			fh, err := env.healthRepo.GetFileHealth(context.Background(), path)
			require.NoError(t, err)
			assert.Equal(t, "checking", string(fh.Status))
			assert.True(t, env.hw.IsCheckActive(path))
		default:
			t.Error("scheduled checks must notify the Health page before the article sweep")
		}
	}
	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))
	assert.True(t, observed)
}
