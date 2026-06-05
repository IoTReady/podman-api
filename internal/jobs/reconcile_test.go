package jobs

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// fakeReconciler records calls and returns a scripted outcome per job id.
type fakeReconciler struct {
	mu       sync.Mutex
	outcomes map[string]fakeOutcome // by job id
	calls    map[string]int
}

type fakeOutcome struct {
	state    store.JobState
	message  string
	resolved bool
	err      error
}

func (f *fakeReconciler) Reconcile(_ context.Context, job store.Job, _ *JobContext) (store.JobState, string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[job.ID]++
	o := f.outcomes[job.ID]
	return o.state, o.message, o.resolved, o.err
}

func (f *fakeReconciler) callCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

func TestStart_BootTransition(t *testing.T) {
	ctx := context.Background()
	js := store.NewMemory()
	mig, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	evac, _ := js.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	js.ClaimNext(ctx) // mig -> running
	js.ClaimNext(ctx) // evac -> running

	r := NewRunner(js, Registry{}, 2)
	r.SetReconcilers(Reconcilers{"migrate": &fakeReconciler{
		outcomes: map[string]fakeOutcome{mig.ID: {state: store.JobSucceeded, resolved: true}},
		calls:    map[string]int{},
	}})
	runCtx, cancel := context.WithCancel(ctx)
	r.Start(runCtx)
	t.Cleanup(func() { cancel(); r.Wait() })

	// evacuate (no reconciler) was failed at boot immediately.
	if j, _ := js.GetJob(ctx, evac.ID); j.State != store.JobFailed {
		t.Fatalf("evacuate state = %q, want failed", j.State)
	}
	// migrate was moved to reconciling then resolved by the loop's first sweep.
	waitForState(t, js, mig.ID, store.JobSucceeded)
}

func TestReconcileLoop_RetriesInconclusive(t *testing.T) {
	ctx := context.Background()
	js := store.NewMemory()
	mig, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	js.ClaimNext(ctx)

	fr := &fakeReconciler{
		outcomes: map[string]fakeOutcome{mig.ID: {resolved: false}}, // inconclusive forever
		calls:    map[string]int{},
	}
	r := NewRunner(js, Registry{}, 2)
	r.SetReconcilers(Reconcilers{"migrate": fr})
	// Shrink the loop interval for the test.
	r.reconcileInterval = 20 * time.Millisecond
	runCtx, cancel := context.WithCancel(ctx)
	r.Start(runCtx)
	t.Cleanup(func() { cancel(); r.Wait() })

	// Stays reconciling and is retried more than once.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fr.callCount(mig.ID) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fr.callCount(mig.ID) < 2 {
		t.Fatalf("reconciler called %d times, want >= 2 (retry)", fr.callCount(mig.ID))
	}
	if j, _ := js.GetJob(ctx, mig.ID); j.State != store.JobReconciling {
		t.Fatalf("state = %q, want reconciling", j.State)
	}
}

func TestReconcileLoop_CancelWins(t *testing.T) {
	ctx := context.Background()
	js := store.NewMemory()
	mig, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	js.ClaimNext(ctx)
	js.MarkReconciling(ctx, []string{"migrate"})

	// Operator cancels before the loop resolves.
	ok, _ := js.CancelReconciling(ctx, mig.ID)
	if !ok {
		t.Fatal("CancelReconciling failed to set up the test")
	}
	fr := &fakeReconciler{
		outcomes: map[string]fakeOutcome{mig.ID: {state: store.JobFailed, resolved: true}},
		calls:    map[string]int{},
	}
	r := NewRunner(js, Registry{}, 2)
	r.SetReconcilers(Reconcilers{"migrate": fr})
	r.reconcileInterval = 20 * time.Millisecond
	runCtx, cancel := context.WithCancel(ctx)
	r.Start(runCtx)
	t.Cleanup(func() { cancel(); r.Wait() })

	time.Sleep(150 * time.Millisecond) // let a few sweeps run
	if fr.callCount(mig.ID) != 0 {
		t.Fatalf("reconciler called %d times for a canceled job, want 0", fr.callCount(mig.ID))
	}
	if j, _ := js.GetJob(ctx, mig.ID); j.State != store.JobCanceled {
		t.Fatalf("state = %q, want canceled (cancel must win the CAS)", j.State)
	}
}

func waitForState(t *testing.T, js store.JobStore, id string, want store.JobState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if j, _ := js.GetJob(context.Background(), id); j.State == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	j, _ := js.GetJob(context.Background(), id)
	t.Fatalf("job %s state = %q, want %q (timeout)", id, j.State, want)
}
