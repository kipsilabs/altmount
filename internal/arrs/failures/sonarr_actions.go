package failures

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"
)

type actionLock struct {
	gate chan struct{}
	refs int
}

// LockAction serializes all scanner/worker stages for a download. Completion is
// tracked independently per target/stage; waiting operations refresh after entry.
func (t *Tracker) LockAction(ctx context.Context, key string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t == nil || key == "" {
		return func() {}, nil
	}
	t.mu.Lock()
	if t.locks == nil {
		t.locks = make(map[string]*actionLock)
	}
	lock := t.locks[key]
	if lock == nil {
		lock = &actionLock{gate: make(chan struct{}, 1)}
		lock.gate <- struct{}{}
		t.locks[key] = lock
	}
	lock.refs++
	t.mu.Unlock()
	drop := func() {
		t.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(t.locks, key)
		}
		t.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	case <-lock.gate:
		var once sync.Once
		return func() { once.Do(func() { lock.gate <- struct{}{}; drop() }) }, nil
	}
}

// SonarrDownloadKey identifies a failure generation across scanner and worker.
func SonarrDownloadKey(instance, downloadID string) string {
	identity, _ := json.Marshal([2]string{instance, strings.ToLower(strings.TrimSpace(downloadID))})
	return "sonarr-download|" + string(identity)
}

func SonarrDownloadID(key string) string {
	var identity [2]string
	if err := json.Unmarshal([]byte(strings.TrimPrefix(key, "sonarr-download|")), &identity); err != nil {
		return ""
	}
	return identity[1]
}

// BumpOnce counts an episode only once for one failed download generation.
func (t *Tracker) BumpOnce(key, event string) int {
	if t == nil || key == "" {
		return 0
	}
	if event == "" {
		return t.Bump(key)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.events == nil {
		t.events = make(map[string]map[string]struct{})
	}
	if t.events[key] == nil {
		t.events[key] = make(map[string]struct{})
	}
	if _, seen := t.events[key][event]; !seen {
		t.events[key][event] = struct{}{}
		t.counts[key]++
	}
	return t.counts[key]
}

// BeginAction coalesces concurrent producers and completed repeated callbacks.
// finish(false) permits a retry after refreshing external state.
func (t *Tracker) BeginAction(key string) (finish func(bool), acquired bool) {
	if t == nil || key == "" {
		return func(bool) {}, true
	}
	t.mu.Lock()
	if t.actions == nil {
		t.actions = make(map[string]bool)
	}
	if t.completedAt == nil {
		t.completedAt = make(map[string]time.Time)
	}
	// Keep completed callbacks for one day, bounded to 10,000 generations.
	// Pending stages are never evicted by this completion cache.
	now := time.Now()
	for old, at := range t.completedAt {
		if now.Sub(at) > 24*time.Hour {
			delete(t.completedAt, old)
			delete(t.actions, old)
		}
	}
	if len(t.completedAt) >= 10000 {
		var oldest string
		var at time.Time
		for candidate, when := range t.completedAt {
			if oldest == "" || when.Before(at) {
				oldest, at = candidate, when
			}
		}
		delete(t.completedAt, oldest)
		delete(t.actions, oldest)
	}
	if _, exists := t.actions[key]; exists {
		t.mu.Unlock()
		return func(bool) {}, false
	}
	t.actions[key] = false
	t.mu.Unlock()
	var once sync.Once
	return func(success bool) {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			if success {
				t.actions[key] = true
				t.completedAt[key] = time.Now()
			} else {
				delete(t.actions, key)
			}
		})
	}, true
}
