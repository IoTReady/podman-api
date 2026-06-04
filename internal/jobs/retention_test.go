package jobs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

func TestRunner_StartRetention_PrunesOldTerminalJobs(t *testing.T) {
	m := store.NewMemory()
	ctx := context.Background()

	// One finished job that will be older than the (tiny) retention window.
	j, _ := m.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
	_ = m.Finish(ctx, j.ID, store.JobSucceeded, "")

	r := NewRunner(m, Registry{}, 1)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Retention of 1ns: the already-finished job is immediately "old"; the
	// initial sweep should remove it.
	r.StartRetention(rctx, time.Nanosecond)

	waitFor(t, func() bool {
		_, err := m.GetJob(ctx, j.ID)
		return err == store.ErrNotFound
	})
}

func TestRunner_StartRetention_DisabledByZero(t *testing.T) {
	m := store.NewMemory()
	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	_ = m.Finish(context.Background(), j.ID, store.JobSucceeded, "")

	r := NewRunner(m, Registry{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.StartRetention(ctx, 0) // disabled → no goroutine, nothing pruned

	time.Sleep(50 * time.Millisecond)
	if _, err := m.GetJob(context.Background(), j.ID); err != nil {
		t.Fatalf("job should survive when retention disabled: %v", err)
	}
}
