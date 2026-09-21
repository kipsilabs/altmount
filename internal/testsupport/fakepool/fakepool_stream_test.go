package fakepool

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
)

type recordingWriter struct {
	mu     sync.Mutex
	writes [][]byte
}

func (r *recordingWriter) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (r *recordingWriter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.writes)
}

func (r *recordingWriter) all() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []byte
	for _, w := range r.writes {
		out = append(out, w...)
	}
	return out
}

func TestBodyStreamPriorityChunksAndGates(t *testing.T) {
	c := New()
	payload := bytes.Repeat([]byte{7}, 10)
	gate := make(chan struct{})
	c.SetBehavior("a", SegmentBehavior{Bytes: payload, ChunkSize: 4, TailGate: gate})

	w := &recordingWriter{}
	done := make(chan error, 1)
	go func() {
		_, err := c.Fetch(context.Background(), nntppool.Req{
			MessageID: "a", Writer: w, Lane: nntppool.LanePriority,
		})
		done <- err
	}()

	deadline := time.After(2 * time.Second)
	for w.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("no first chunk before the gate opened")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if w.count() != 1 || len(w.all()) != 4 {
		t.Fatalf("writes before gate = %d (%d bytes)", w.count(), len(w.all()))
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(w.all(), payload) {
		t.Fatalf("reassembled %v", w.all())
	}
	if c.BodyStreamPriorityCalls() != 1 {
		t.Fatal("call not counted")
	}
}

func TestBodyStreamPriorityFailAfterFirstChunk(t *testing.T) {
	c := New()
	payload := bytes.Repeat([]byte{9}, 8)
	c.SetBehavior("a", SegmentBehavior{Bytes: payload, ChunkSize: 4, FailAfterFirstChunk: true, FailErr: errors.New("conn died")})

	w := &recordingWriter{}
	_, err := c.Fetch(context.Background(), nntppool.Req{
		MessageID: "a", Writer: w, Lane: nntppool.LanePriority,
	})
	if err == nil || w.count() != 1 {
		t.Fatalf("first call: err=%v writes=%d, want error after one chunk", err, w.count())
	}
	w2 := &recordingWriter{}
	if _, err := c.Fetch(context.Background(), nntppool.Req{
		MessageID: "a", Writer: w2, Lane: nntppool.LanePriority,
	}); err != nil {
		t.Fatalf("second call must succeed: %v", err)
	}
	if w2.count() != 2 || !bytes.Equal(w2.all(), payload) {
		t.Fatalf("second call writes = %d, want 2 full chunks", w2.count())
	}
}
