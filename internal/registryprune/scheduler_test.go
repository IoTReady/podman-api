package registryprune

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

func queuedRuns(t *testing.T, mem *store.Memory) []store.Job {
	t.Helper()
	jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: JobKind, State: store.JobQueued})
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func newScheduler(mem *store.Memory, now time.Time) *Scheduler {
	return &Scheduler{
		Store:    mem,
		Interval: 24 * time.Hour,
		Payload:  func() Payload { return Payload{Policy: DefaultPolicy()} },
		Now:      func() time.Time { return now },
	}
}

// A never-run job is due immediately: a fresh start must not wait a full
// interval before its first pass.
func TestSchedulerEnqueuesWhenNeverRun(t *testing.T) {
	mem := store.NewMemory()
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	s := newScheduler(mem, now)
	s.tick(context.Background())
	jobs := queuedRuns(t, mem)
	if len(jobs) != 1 {
		t.Fatalf("want 1 queued run, got %d", len(jobs))
	}
	var p Payload
	if err := json.Unmarshal(jobs[0].Args, &p); err != nil {
		t.Fatal(err)
	}
	if p.Policy.MaxDeletesPerRepo != DefaultPolicy().MaxDeletesPerRepo {
		t.Fatalf("payload policy not carried: %+v", p.Policy)
	}
}

// In-flight dedup. A registry prune run can take minutes; a second run started
// alongside the first would classify against a catalog the first is mutating.
func TestSchedulerDedupsInFlightRun(t *testing.T) {
	mem := store.NewMemory()
	ctx := context.Background()
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	args, _ := json.Marshal(Payload{Policy: DefaultPolicy()})
	if _, err := mem.Enqueue(ctx, JobKind, args, ""); err != nil {
		t.Fatal(err)
	}
	s := newScheduler(mem, now)
	s.tick(ctx)
	if got := len(queuedRuns(t, mem)); got != 1 {
		t.Fatalf("a queued run must suppress a second enqueue, got %d", got)
	}

	// Same again with a RUNNING job rather than a queued one.
	if _, _, err := mem.ClaimNext(ctx); err != nil {
		t.Fatal(err)
	}
	s.tick(ctx)
	all, _ := mem.ListJobs(ctx, store.JobFilter{Kind: JobKind})
	if len(all) != 1 {
		t.Fatalf("a running run must suppress a second enqueue, got %d jobs", len(all))
	}
}

func TestSchedulerWaitsForInterval(t *testing.T) {
	mem := store.NewMemory()
	ctx := context.Background()
	args, _ := json.Marshal(Payload{Policy: DefaultPolicy()})
	j, _ := mem.Enqueue(ctx, JobKind, args, "")
	mem.ClaimNext(ctx)
	if err := mem.Finish(ctx, j.ID, store.JobSucceeded, ""); err != nil {
		t.Fatal(err)
	}
	done, _ := mem.GetJob(ctx, j.ID)

	s := newScheduler(mem, done.Finished.Add(time.Hour)) // well inside the 24h interval
	s.tick(ctx)
	if got := len(queuedRuns(t, mem)); got != 0 {
		t.Fatalf("must not re-enqueue inside the interval, got %d", got)
	}

	s.Now = func() time.Time { return done.Finished.Add(25 * time.Hour) }
	s.tick(ctx)
	if got := len(queuedRuns(t, mem)); got != 1 {
		t.Fatalf("must enqueue once the interval has elapsed, got %d", got)
	}
}

// Failure backoff: a persistently failing run (unreachable registry) must not
// be re-enqueued every tick, which would flood the job store.
func TestSchedulerBacksOffAfterFailure(t *testing.T) {
	mem := store.NewMemory()
	ctx := context.Background()
	args, _ := json.Marshal(Payload{Policy: DefaultPolicy()})
	j, _ := mem.Enqueue(ctx, JobKind, args, "")
	mem.ClaimNext(ctx)
	if err := mem.Finish(ctx, j.ID, store.JobFailed, "registry unreachable"); err != nil {
		t.Fatal(err)
	}
	done, _ := mem.GetJob(ctx, j.ID)

	s := newScheduler(mem, done.Finished.Add(time.Minute))
	s.tick(ctx)
	if got := len(queuedRuns(t, mem)); got != 0 {
		t.Fatalf("must back off right after a failure, got %d", got)
	}

	s.Now = func() time.Time { return done.Finished.Add(failureBackoff + time.Minute) }
	s.tick(ctx)
	if got := len(queuedRuns(t, mem)); got != 1 {
		t.Fatalf("must retry once the backoff has elapsed, got %d", got)
	}
}

// Start runs an immediate first pass before the ticker's first fire.
func TestSchedulerStartRunsImmediateFirstPass(t *testing.T) {
	mem := store.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	s := newScheduler(mem, time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC))
	s.Start(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(queuedRuns(t, mem)) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	s.Wait()
	if got := len(queuedRuns(t, mem)); got != 1 {
		t.Fatalf("Start must run an immediate first pass, got %d queued", got)
	}
}

// Interval<=0 disables the scheduler entirely rather than enqueuing on every
// tick.
func TestSchedulerZeroIntervalIsInert(t *testing.T) {
	mem := store.NewMemory()
	s := newScheduler(mem, time.Now())
	s.Interval = 0
	s.tick(context.Background())
	if got := len(queuedRuns(t, mem)); got != 0 {
		t.Fatalf("Interval<=0 must not enqueue, got %d", got)
	}
}
