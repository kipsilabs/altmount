package health

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/par2repair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAdviser answers every file with one fixed advice and records its use.
type fakeAdviser struct {
	mu      sync.Mutex
	advice  PeerAdvice
	delay   time.Duration
	calls   []string
	applied []PeerAdviceKind
}

func (f *fakeAdviser) PeerAdvice(ctx context.Context, filePath string) PeerAdvice {
	f.mu.Lock()
	f.calls = append(f.calls, filePath)
	d, a := f.delay, f.advice
	f.mu.Unlock()
	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return PeerAdvice{}
		}
	}
	return a
}

func (f *fakeAdviser) PeerAdviceApplied(k PeerAdviceKind) {
	f.mu.Lock()
	f.applied = append(f.applied, k)
	f.mu.Unlock()
}

func (f *fakeAdviser) consulted() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeAdviser) appliedKinds() []PeerAdviceKind {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]PeerAdviceKind(nil), f.applied...)
}

const bigFileSegments = 4096 // 4 MiB in 1 KiB segments

// sampledEnv is a 4096-segment file with a 50 % configured sample.
func sampledEnv(t *testing.T) *holeTestEnv {
	t.Helper()
	env := newHoleTestEnv(t, "peer.mp4", 4*1024*1024, 1024)
	off := false
	env.cfg.Health.CheckAllSegments = &off
	env.cfg.Health.SegmentSamplePercentage = 50
	return env
}

func autoOpts(env *holeTestEnv, status database.HealthStatus) CheckOptions {
	return CheckOptions{PeerFiles: map[string]PeerFileInfo{
		env.filePath: {Status: status, ReleaseDate: time.Now().Add(-400 * 24 * time.Hour)},
	}}
}

func statCount(env *holeTestEnv, run func()) int {
	before := env.fp.StatCalls()
	run()
	return int(env.fp.StatCalls() - before)
}

func TestPeerFloorPercent(t *testing.T) {
	for _, tc := range []struct {
		name              string
		total, configured int
		pct               int
		ok                bool
	}{
		{"large file uses the percentage floor", 4096, 50, PeerFloorPercent, true},
		{"mid file is lifted to the minimum segment count", 500, 50, 2, true},
		{"small file lifted above the percentage", 200, 50, 5, true},
		{"file at the minimum is never reduced", 10, 50, 0, false},
		{"tiny file is never reduced", 3, 100, 0, false},
		{"single segment is never reduced", 1, 50, 0, false},
		{"configured percentage already at the floor", 4096, 2, 0, false},
		{"configured percentage below the floor", 4096, 1, 0, false},
		{"configured percentage below the lifted floor", 200, 4, 0, false},
		{"zero segments", 0, 50, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pct, ok := peerFloorPercent(tc.total, tc.configured)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.pct, pct)
			if ok {
				got := max(tc.total*pct/100, 5)
				assert.GreaterOrEqual(t, got, min(PeerFloorMinSegments, tc.total), "never below the minimum")
				assert.GreaterOrEqual(t, got, 5, "never below the forced first and last segments")
				assert.Less(t, pct, tc.configured, "never more than the normal sample")
			}
		})
	}
}

func TestPeerAdvice_ReduceToFloorLowersTheSample(t *testing.T) {
	env := sampledEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}}
	env.checker.SetPeerAdviser(adv)

	normal := statCount(env, func() {
		require.Equal(t, EventTypeFileHealthy, env.checker.CheckFile(context.Background(), env.filePath).Type)
	})
	assert.Equal(t, 2048, normal, "no peer files: no advice, no reduction")
	assert.Zero(t, adv.consulted())

	var ev HealthEvent
	reduced := statCount(env, func() {
		ev = env.checker.CheckFile(context.Background(), env.filePath, autoOpts(env, database.HealthStatusHealthy))
	})
	require.Equal(t, EventTypeFileHealthy, ev.Type)
	assert.Equal(t, bigFileSegments*PeerFloorPercent/100, reduced)
	assert.Equal(t, reduced, ev.Stats.SegmentsChecked)
	assert.False(t, ev.Stats.FullCheck)
	assert.Equal(t, 1, adv.consulted(), "consulted once per file")
	assert.Equal(t, []PeerAdviceKind{PeerAdviceReduceSample}, adv.appliedKinds())
}

func TestPeerAdvice_ReducedSampleStillCoversFirstAndLastSegments(t *testing.T) {
	for _, idx := range []int{0, 1, 2, bigFileSegments - 2, bigFileSegments - 1} {
		env := sampledEnv(t)
		env.checker.SetPeerAdviser(&fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}})
		env.markSegmentMissing(idx)
		ev := env.checker.CheckFile(context.Background(), env.filePath, autoOpts(env, database.HealthStatusHealthy))
		assert.Equal(t, EventTypeFileCorrupted, ev.Type, "segment %d", idx)
	}
}

// A reduced check that finds a missing segment is the same failed check a
// normal sample would produce.
func TestPeerAdvice_ReducedCheckThatFindsAHoleFailsLikeANormalCheck(t *testing.T) {
	run := func(adv PeerAdviser) HealthEvent {
		env := sampledEnv(t)
		env.markSegmentMissing(0)
		if adv != nil {
			env.checker.SetPeerAdviser(adv)
		}
		return env.checker.CheckFile(context.Background(), env.filePath, autoOpts(env, database.HealthStatusHealthy))
	}
	normal := run(nil)
	reduced := run(&fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}})
	require.Equal(t, EventTypeFileCorrupted, normal.Type)
	assert.Equal(t, normal.Type, reduced.Type)
	assert.Equal(t, normal.Status, reduced.Status)
	require.NotNil(t, normal.Classification)
	require.NotNil(t, reduced.Classification)
	assert.Equal(t, normal.Classification.Verdict, reduced.Classification.Verdict)
	assert.Equal(t, normal.Classification.TotalMissing, reduced.Classification.TotalMissing)
	assert.Equal(t, normal.Classification.LongestRun, reduced.Classification.LongestRun)
	assert.Equal(t, normal.Stats.MissingCount, reduced.Stats.MissingCount)
	assert.Contains(t, reduced.Error.Error(), "checked segments are missing from your Usenet provider")
}

func TestPeerAdvice_ReduceNeverAppliesTo(t *testing.T) {
	type tweak func(env *holeTestEnv, opts *CheckOptions)
	for _, tc := range []struct {
		name  string
		tweak tweak
		// consulted: whether the adviser may be asked at all.
		consulted bool
		want      int
	}{
		{"check_all_segments", func(e *holeTestEnv, _ *CheckOptions) {
			on := true
			e.cfg.Health.CheckAllSegments = &on
		}, false, bigFileSegments},
		{"forced full check", func(_ *holeTestEnv, o *CheckOptions) { o.ForceFullCheck = true }, false, bigFileSegments},
		{"pending first check", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusPending}
			o.CurrentStatus = database.HealthStatusPending
		}, false, 2048},
		{"degraded file", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusDegraded}
		}, false, 2048},
		{"corrupted file", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusCorrupted}
		}, false, 2048},
		{"repair triggered file", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusRepairTriggered}
		}, false, 2048},
		{"manual recheck (no scheduled-cycle info)", func(_ *holeTestEnv, o *CheckOptions) { o.PeerFiles = nil }, false, 2048},
		{"explicit content verification request", func(_ *holeTestEnv, o *CheckOptions) {
			yes := true
			o.VerifyContentOverride = &yes
		}, false, 2048},
		{"file with known holes", func(e *holeTestEnv, _ *CheckOptions) {
			require.NoError(t, e.ms.AddKnownHoles(e.filePath, []holes.Run{{Start: 10, Count: 1}}, e.cfg.ProviderFingerprint()))
		}, false, 2048},
		{"file with a PAR2 patch", func(e *holeTestEnv, _ *CheckOptions) {
			store := par2repair.NewPatchStore(t.TempDir())
			require.NoError(t, store.Put(e.segIDs[100], make([]byte, 1024)))
			e.checker.SetPatchIndex(store)
		}, false, 2047}, // the patched article is not asked of the providers
		{"percentage already at or below the floor", func(e *holeTestEnv, _ *CheckOptions) {
			e.cfg.Health.SegmentSamplePercentage = PeerFloorPercent
		}, true, bigFileSegments * PeerFloorPercent / 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := sampledEnv(t)
			adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}}
			env.checker.SetPeerAdviser(adv)
			opts := autoOpts(env, database.HealthStatusHealthy)
			tc.tweak(env, &opts)
			got := statCount(env, func() { env.checker.CheckFile(context.Background(), env.filePath, opts) })
			assert.InDelta(t, tc.want, got, 1, "random sampling may or may not include the one patched article")
			if tc.consulted {
				assert.Equal(t, 1, adv.consulted())
			} else {
				assert.Zero(t, adv.consulted(), "must not even ask")
			}
			assert.Empty(t, adv.appliedKinds())
		})
	}
}

func TestPeerAdvice_TinyFileIsAlwaysFullyChecked(t *testing.T) {
	env := newHoleTestEnv(t, "tiny.mp4", 8*1024, 1024) // 8 segments
	off := false
	env.cfg.Health.CheckAllSegments = &off
	env.cfg.Health.SegmentSamplePercentage = 50
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}}
	env.checker.SetPeerAdviser(adv)
	got := statCount(env, func() {
		env.checker.CheckFile(context.Background(), env.filePath, autoOpts(env, database.HealthStatusHealthy))
	})
	assert.Equal(t, 5, got, "the sampler's own minimum (first three, last two), exactly as without advice")
	assert.Empty(t, adv.appliedKinds(), "nothing was reduced")
}

func TestPeerAdvice_SlowAdviserFailsOpenWithinTheTimeout(t *testing.T) {
	env := sampledEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}, delay: 5 * time.Second}
	env.checker.SetPeerAdviser(adv)
	start := time.Now()
	var ev HealthEvent
	got := statCount(env, func() {
		ev = env.checker.CheckFile(context.Background(), env.filePath, autoOpts(env, database.HealthStatusHealthy))
	})
	assert.Less(t, time.Since(start), 3*time.Second, "a stuck peer lookup must not stall a check")
	assert.Equal(t, EventTypeFileHealthy, ev.Type)
	assert.Equal(t, 2048, got, "no advice: the normal sample")
}

func TestPeerAdvice_NilAdviserBehavesExactlyAsBefore(t *testing.T) {
	env := sampledEnv(t)
	opts := autoOpts(env, database.HealthStatusHealthy)
	got := statCount(env, func() {
		ev := env.checker.CheckFile(context.Background(), env.filePath, opts)
		assert.Equal(t, EventTypeFileHealthy, ev.Type)
	})
	assert.Equal(t, 2048, got)
	env.checker.SetPeerAdviser(&fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}})
	env.checker.SetPeerAdviser(nil)
	got = statCount(env, func() { env.checker.CheckFile(context.Background(), env.filePath, opts) })
	assert.Equal(t, 2048, got, "removing the adviser restores the default")
}

func TestPeerAdvice_SkipOnceDefersTheCheck(t *testing.T) {
	env := sampledEnv(t)
	until := time.Now().Add(5 * 24 * time.Hour)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: until}}
	env.checker.SetPeerAdviser(adv)

	var ev HealthEvent
	got := statCount(env, func() {
		ev = env.checker.CheckFile(context.Background(), env.filePath, autoOpts(env, database.HealthStatusHealthy))
	})
	require.Equal(t, EventTypeCheckDeferred, ev.Type)
	assert.Zero(t, got, "a deferred check does not touch the providers")
	assert.WithinDuration(t, until, ev.DeferUntil, time.Second, "no later than the evidence allows")
	assert.Equal(t, []PeerAdviceKind{PeerAdviceSkipOnce}, adv.appliedKinds())
}

func TestPeerAdvice_SkipNeverLengthensTheInterval(t *testing.T) {
	env := sampledEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(7 * 24 * time.Hour)}}
	env.checker.SetPeerAdviser(adv)

	// A day-old file is re-checked every 6 hours at most: the deferral may not
	// push the check past what a successful check would schedule.
	opts := CheckOptions{PeerFiles: map[string]PeerFileInfo{
		env.filePath: {Status: database.HealthStatusHealthy, ReleaseDate: time.Now().Add(-26 * time.Hour)},
	}}
	ev := env.checker.CheckFile(context.Background(), env.filePath, opts)
	require.Equal(t, EventTypeCheckDeferred, ev.Type)
	assert.LessOrEqual(t, time.Until(ev.DeferUntil), aggressiveCheckInterval+time.Minute)
	assert.Greater(t, time.Until(ev.DeferUntil), minInterval-time.Minute)

	// A brand-new file's normal interval is the 1 h minimum: the deferral
	// is capped to it.
	opts.PeerFiles[env.filePath] = PeerFileInfo{Status: database.HealthStatusHealthy, ReleaseDate: time.Now().Add(-20 * time.Minute)}
	ev = env.checker.CheckFile(context.Background(), env.filePath, opts)
	require.Equal(t, EventTypeCheckDeferred, ev.Type)
	assert.LessOrEqual(t, time.Until(ev.DeferUntil), minInterval+time.Minute)
}

func TestPeerAdvice_SkipOnlyOncePerRealCheck(t *testing.T) {
	env := sampledEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(5 * 24 * time.Hour)}}
	env.checker.SetPeerAdviser(adv)
	opts := autoOpts(env, database.HealthStatusHealthy)
	info := opts.PeerFiles[env.filePath]
	info.AlreadyDeferred = true
	opts.PeerFiles[env.filePath] = info

	got := statCount(env, func() {
		ev := env.checker.CheckFile(context.Background(), env.filePath, opts)
		assert.Equal(t, EventTypeFileHealthy, ev.Type)
	})
	assert.Equal(t, 2048, got, "the second due check is a normal, full-size check")
	assert.Empty(t, adv.appliedKinds())
	assert.Zero(t, adv.consulted(), "no point asking when the answer cannot be used")
}

func TestPeerAdvice_SkipNeverAppliesTo(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tweak func(env *holeTestEnv, opts *CheckOptions)
	}{
		{"check_all_segments", func(e *holeTestEnv, _ *CheckOptions) {
			on := true
			e.cfg.Health.CheckAllSegments = &on
		}},
		{"forced full check", func(_ *holeTestEnv, o *CheckOptions) { o.ForceFullCheck = true }},
		{"pending first check", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusPending, ReleaseDate: time.Now().Add(-400 * 24 * time.Hour)}
		}},
		{"degraded", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusDegraded, ReleaseDate: time.Now().Add(-400 * 24 * time.Hour)}
		}},
		{"corrupted", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusCorrupted, ReleaseDate: time.Now().Add(-400 * 24 * time.Hour)}
		}},
		{"repair triggered", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusRepairTriggered, ReleaseDate: time.Now().Add(-400 * 24 * time.Hour)}
		}},
		{"manual recheck", func(_ *holeTestEnv, o *CheckOptions) { o.PeerFiles = nil }},
		{"known holes", func(e *holeTestEnv, _ *CheckOptions) {
			require.NoError(t, e.ms.AddKnownHoles(e.filePath, []holes.Run{{Start: 10, Count: 1}}, e.cfg.ProviderFingerprint()))
		}},
		{"patched file", func(e *holeTestEnv, _ *CheckOptions) {
			store := par2repair.NewPatchStore(t.TempDir())
			require.NoError(t, store.Put(e.segIDs[100], make([]byte, 1024)))
			e.checker.SetPatchIndex(store)
		}},
		{"unknown release date", func(e *holeTestEnv, o *CheckOptions) {
			o.PeerFiles[e.filePath] = PeerFileInfo{Status: database.HealthStatusHealthy}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := sampledEnv(t)
			adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(5 * 24 * time.Hour)}}
			env.checker.SetPeerAdviser(adv)
			opts := autoOpts(env, database.HealthStatusHealthy)
			tc.tweak(env, &opts)
			ev := env.checker.CheckFile(context.Background(), env.filePath, opts)
			assert.NotEqual(t, EventTypeCheckDeferred, ev.Type)
			assert.Empty(t, adv.appliedKinds())
		})
	}
}

func TestPeerAdvice_SkipWithAnImplausibleDeadlineIsIgnored(t *testing.T) {
	for _, until := range []time.Time{{}, time.Now().Add(-time.Hour), time.Now().Add(10 * time.Minute)} {
		env := sampledEnv(t)
		env.checker.SetPeerAdviser(&fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: until}})
		ev := env.checker.CheckFile(context.Background(), env.filePath, autoOpts(env, database.HealthStatusHealthy))
		assert.Equal(t, EventTypeFileHealthy, ev.Type, "deadline %v", until)
	}
}

// The batch path (the automatic cycle) consults the adviser per file and
// defers only the files advised.
func TestPeerAdvice_BatchPathDefersOnlyAdvisedFiles(t *testing.T) {
	env := sampledEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(5 * 24 * time.Hour)}}
	env.checker.SetPeerAdviser(adv)
	opts := autoOpts(env, database.HealthStatusHealthy)
	events := env.checker.CheckFilesBatch(context.Background(), []string{env.filePath}, []database.HealthStatus{database.HealthStatusHealthy}, opts)
	require.Len(t, events, 1)
	assert.Equal(t, EventTypeCheckDeferred, events[0].Type)
	assert.Equal(t, 1, adv.consulted())
}
