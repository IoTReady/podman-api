package registryprune

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

func onDemandScheduler(st store.JobStore) *Scheduler {
	return &Scheduler{
		Store:    st,
		Interval: 24 * time.Hour,
		Now:      time.Now,
		Payload:  func() Payload { return Payload{Policy: DefaultPolicy(), DryRun: true} },
	}
}

func TestEnqueueNow_Enqueues(t *testing.T) {
	mem := store.NewMemory()
	s := onDemandScheduler(mem)

	job, err := s.EnqueueNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if job.ID == "" {
		t.Fatal("want a job")
	}
	if job.Kind != JobKind {
		t.Fatalf("kind = %q, want %q", job.Kind, JobKind)
	}
}

// The whole reason the route exists. The interval gate is backed by PERSISTED
// job history, so it survives a restart: without EnqueueNow, a second run
// inside 24h can only be had by editing -registry-prune-interval and
// restarting — the one knob nobody should improvise with on the day of the
// first real delete. #220's rollout is a sequence of on-demand runs.
func TestEnqueueNow_IgnoresTheIntervalGate(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemory()
	s := onDemandScheduler(mem)

	first, err := s.EnqueueNow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mem.ClaimNext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := mem.Finish(ctx, first.ID, store.JobSucceeded, ""); err != nil {
		t.Fatal(err)
	}
	// A tick right now is gated: the newest success is far younger than 24h.
	s.tick(ctx)
	if n := countJobs(t, mem); n != 1 {
		t.Fatalf("tick enqueued despite the interval gate: %d jobs", n)
	}
	// The on-demand trigger is not.
	if _, err := s.EnqueueNow(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countJobs(t, mem); n != 2 {
		t.Fatalf("EnqueueNow did not enqueue: %d jobs", n)
	}
}

// The guard the route exposes rather than bypasses. Two runs classifying the
// same catalog while each mutates it is the failure this design is shaped
// around.
func TestEnqueueNow_RefusesWhileARunIsInFlight(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemory()
	s := onDemandScheduler(mem)

	if _, err := s.EnqueueNow(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := s.EnqueueNow(ctx) // the first is still queued
	if !errors.Is(err, ErrRunInFlight) {
		t.Fatalf("err = %v, want ErrRunInFlight", err)
	}
	if n := countJobs(t, mem); n != 1 {
		t.Fatalf("a second run was enqueued anyway: %d jobs", n)
	}
}

// A running (not merely queued) job counts too.
func TestEnqueueNow_RefusesWhileARunIsRunning(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemory()
	s := onDemandScheduler(mem)

	if _, err := s.EnqueueNow(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := mem.ClaimNext(ctx); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if _, err := s.EnqueueNow(ctx); !errors.Is(err, ErrRunInFlight) {
		t.Fatalf("err = %v, want ErrRunInFlight", err)
	}
}

// An unconfigured scheduler must refuse rather than enqueue a job whose payload
// would abort at validatePolicy.
func TestEnqueueNow_RefusesWhenUnconfigured(t *testing.T) {
	if _, err := (&Scheduler{Store: store.NewMemory()}).EnqueueNow(context.Background()); err == nil {
		t.Fatal("want an error with no Payload")
	}
	if _, err := (&Scheduler{Payload: func() Payload { return Payload{} }}).EnqueueNow(context.Background()); err == nil {
		t.Fatal("want an error with no Store")
	}
}

// Concurrent triggers must not both slip past the in-flight scan. Without the
// lock the check and the enqueue are two steps and both callers can pass the
// first before either performs the second.
func TestEnqueueNow_ConcurrentTriggersEnqueueOnce(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemory()
	s := onDemandScheduler(mem)

	const n = 8
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			<-start
			_, err := s.EnqueueNow(ctx)
			errs <- err
		}()
	}
	close(start)
	ok := 0
	for i := 0; i < n; i++ {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, ErrRunInFlight):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 {
		t.Fatalf("%d callers enqueued, want exactly 1", ok)
	}
	if got := countJobs(t, mem); got != 1 {
		t.Fatalf("%d jobs enqueued, want 1", got)
	}
}

func countJobs(t *testing.T, st store.JobStore) int {
	t.Helper()
	js, err := st.ListJobs(context.Background(), store.JobFilter{Kind: JobKind, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return len(js)
}

// tick-vs-EnqueueNow is the pairing that actually occurs in production: the
// ticker is live for the whole time the route is reachable. The committed
// self-vs-self race test does not cover it, and removing tick()'s half of the
// lock survives that test — and the entire suite — while failing this one on
// the first trial.
func TestTickAndEnqueueNowDoNotBothEnqueue(t *testing.T) {
	for trial := 0; trial < 200; trial++ {
		ctx := context.Background()
		mem := store.NewMemory()
		s := onDemandScheduler(mem)
		// Nothing has ever run, so tick()'s interval gate is open and both
		// paths are genuinely eligible to enqueue at the same instant.
		start := make(chan struct{})
		done := make(chan struct{}, 2)
		go func() { <-start; s.tick(ctx); done <- struct{}{} }()
		go func() { <-start; _, _ = s.EnqueueNow(ctx); done <- struct{}{} }()
		close(start)
		<-done
		<-done
		if n := countJobs(t, mem); n != 1 {
			t.Fatalf("trial %d: %d registry-prune jobs enqueued, want exactly 1", trial, n)
		}
	}
}

// A caller waiting on the enqueue lock must give up when its request does. The
// critical section performs store I/O, and this route is operator-facing during
// an incident: a wedged job store must produce a timely error, not a hanging
// connection.
func TestEnqueueNow_HonoursContextCancellationWhileWaiting(t *testing.T) {
	mem := store.NewMemory()
	s := onDemandScheduler(mem)

	// Hold the lock from another goroutine.
	if err := s.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.release()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := s.EnqueueNow(ctx)
		errCh <- err
	}()
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("EnqueueNow ignored its cancelled context and kept waiting on the lock")
	}
	if n := countJobs(t, mem); n != 0 {
		t.Fatalf("%d jobs enqueued by a cancelled caller", n)
	}
}

// A typed-nil *Scheduler behind the api.RegistryPruner interface must answer an
// error, not panic the request handler. h.pruner == nil is false for such a
// value, so the nil check in the route cannot catch it.
func TestEnqueueNow_NilReceiverIsAnErrorNotAPanic(t *testing.T) {
	var s *Scheduler
	if _, err := s.EnqueueNow(context.Background()); err == nil {
		t.Fatal("want an error from a nil *Scheduler")
	}
}
