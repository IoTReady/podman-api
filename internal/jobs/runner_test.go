package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// handlerFunc adapts a func to the Handler interface for tests.
type handlerFunc func(ctx context.Context, job store.Job, jc *JobContext) error

func (f handlerFunc) Run(ctx context.Context, job store.Job, jc *JobContext) error {
	return f(ctx, job, jc)
}

// waitFor polls until cond() or the deadline; fails the test on timeout.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestRunner_RunsHandler_Succeeds(t *testing.T) {
	m := store.NewMemory()
	reg := Registry{"test": handlerFunc(func(ctx context.Context, job store.Job, jc *JobContext) error {
		jc.Step("working", "detail")
		return nil
	})}
	r := NewRunner(m, reg, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)

	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	r.Notify()

	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobSucceeded
	})
	got, _ := m.GetJob(context.Background(), j.ID)
	if len(got.Steps) != 1 || got.Steps[0].Step != "working" {
		t.Fatalf("step not recorded: %+v", got.Steps)
	}
}

func TestRunner_HandlerError_Fails(t *testing.T) {
	m := store.NewMemory()
	reg := Registry{"test": handlerFunc(func(ctx context.Context, job store.Job, jc *JobContext) error {
		return errors.New("kaboom")
	})}
	r := NewRunner(m, reg, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	r.Notify()
	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobFailed
	})
	got, _ := m.GetJob(context.Background(), j.ID)
	if got.Error != "kaboom" {
		t.Fatalf("error not recorded: %q", got.Error)
	}
}

func TestRunner_UnknownKind_Fails(t *testing.T) {
	m := store.NewMemory()
	r := NewRunner(m, Registry{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	j, _ := m.Enqueue(context.Background(), "mystery", json.RawMessage(`{}`), "")
	r.Notify()
	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobFailed
	})
	got, _ := m.GetJob(context.Background(), j.ID)
	if got.Error == "" {
		t.Fatal("expected a 'no handler' error message")
	}
}

func TestRunner_BootRecovery_FailsRunning(t *testing.T) {
	m := store.NewMemory()
	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	_, _, _ = m.ClaimNext(context.Background()) // leave it running (simulate crash)

	r := NewRunner(m, Registry{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)

	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobFailed
	})
}

func TestRunner_CancelStops(t *testing.T) {
	m := store.NewMemory()
	r := NewRunner(m, Registry{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)
	cancel()
	done := make(chan struct{})
	go func() { r.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop after ctx cancel")
	}
}
