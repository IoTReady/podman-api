// Package jobs runs queued jobs from a store.JobStore through registered
// per-kind handlers, on a bounded background worker pool.
package jobs

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// DefaultWorkers is the worker-pool size when NewRunner is given workers <= 0.
// Raised from 4 to 8 to give plain jobs headroom: a parent evacuate occupies one
// worker for its whole fan-out, so a few concurrent evacuates could otherwise
// starve migrate/other jobs. This raises the starvation threshold; a structural
// fix (separate orchestration pool) remains a future option (#54).
const DefaultWorkers = 8

// pollInterval is the safety-net wake even without a Notify (e.g. after a
// restart that left queued jobs).
const pollInterval = 5 * time.Second

// Handler executes one job of a given kind. It should honour ctx for cancellation
// and report progress via jc.Step. Returning a non-nil error fails the job.
type Handler interface {
	Run(ctx context.Context, job store.Job, jc *JobContext) error
}

// Registry maps a job kind to its handler.
type Registry map[string]Handler

// JobContext is the handler's progress channel back to the store.
type JobContext struct {
	store store.JobStore
	id    string
}

// NewJobContext builds a JobContext for a job id. Exposed so handlers can be
// exercised in tests without the full runner.
func NewJobContext(js store.JobStore, id string) *JobContext {
	return &JobContext{store: js, id: id}
}

// Step records a progress entry. It is best-effort: a store error is logged, not
// returned, so progress logging never fails the job.
func (jc *JobContext) Step(step, detail string) {
	if err := jc.store.AppendStep(context.Background(), jc.id, store.JobStep{
		TS: time.Now(), Step: step, Detail: detail,
	}); err != nil {
		log.Printf("jobs: append step to %s failed: %v", jc.id, err)
	}
}

// Runner drains queued jobs and dispatches them to handlers.
type Runner struct {
	store    store.JobStore
	handlers Registry
	workers  int
	poke     chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
	inflight map[string]*inflightJob
}

// inflightJob tracks a currently-running job so an operator request can cancel
// its handler context. canceled distinguishes an operator cancel from a
// shutdown/ctx cancel: only Cancel sets it, so a job interrupted by daemon
// shutdown still records failed, not canceled.
type inflightJob struct {
	cancel   context.CancelFunc
	canceled bool
}

// NewRunner builds a runner. workers <= 0 uses DefaultWorkers.
// The handler registry must not be modified after this call.
func NewRunner(js store.JobStore, h Registry, workers int) *Runner {
	if workers <= 0 {
		workers = DefaultWorkers
	}
	return &Runner{store: js, handlers: h, workers: workers, poke: make(chan struct{}, 1), inflight: map[string]*inflightJob{}}
}

// Notify wakes a worker to check for new work; call after an Enqueue. Non-blocking.
// One Notify is sufficient for a batch of enqueues — a woken worker drains the
// queue exhaustively before sleeping again.
func (r *Runner) Notify() {
	select {
	case r.poke <- struct{}{}:
	default:
	}
}

// Cancel signals an in-flight job to stop. Returns true if the job was found
// running — its handler context is cancelled and it will finish as canceled;
// false if no such job is currently running (queued/terminal jobs are handled by
// the caller via the store).
func (r *Runner) Cancel(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.inflight[id]
	if !ok {
		return false
	}
	entry.canceled = true
	entry.cancel()
	return true
}

// Start reaps crash-interrupted jobs, then launches the worker pool. It returns
// immediately; the pool runs until ctx is cancelled. Use Wait to block for exit.
// It must be called exactly once.
func (r *Runner) Start(ctx context.Context) {
	if n, err := r.store.FailRunning(ctx, "interrupted by daemon restart"); err != nil {
		log.Printf("jobs: boot recovery failed: %v", err)
	} else if n > 0 {
		log.Printf("jobs: marked %d interrupted job(s) failed on startup", n)
	}
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go r.worker(ctx)
	}
}

// Wait blocks until all workers have exited (after ctx cancellation).
func (r *Runner) Wait() { r.wg.Wait() }

// retentionInterval is how often StartRetention sweeps after its initial run.
const retentionInterval = time.Hour

// StartRetention periodically prunes terminal jobs older than retention. It is a
// no-op when retention <= 0. It sweeps once immediately, then every
// retentionInterval, until ctx is cancelled. It is tracked by the runner's
// WaitGroup, so Wait blocks for it too.
func (r *Runner) StartRetention(ctx context.Context, retention time.Duration) {
	if retention <= 0 {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		sweep := func() {
			n, err := r.store.PruneJobs(ctx, time.Now().Add(-retention))
			if err != nil {
				log.Printf("jobs: retention sweep failed: %v", err)
				return
			}
			if n > 0 {
				log.Printf("jobs: retention pruned %d terminal job(s)", n)
			}
		}
		sweep() // prompt first pass so a restart cleans up immediately
		t := time.NewTicker(retentionInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sweep()
			}
		}
	}()
}

func (r *Runner) worker(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		// Drain everything currently claimable.
		for {
			if ctx.Err() != nil {
				return
			}
			job, ok, err := r.store.ClaimNext(ctx)
			if err != nil {
				// Back off briefly so a persistent store error can't spin the
				// worker hot when pokes keep arriving.
				log.Printf("jobs: claim error: %v", err)
				time.Sleep(200 * time.Millisecond)
				break
			}
			if !ok {
				break
			}
			r.run(ctx, job)
		}
		select {
		case <-ctx.Done():
			return
		case <-r.poke:
		case <-t.C:
		}
	}
}

// finishTimeout bounds the terminal-state write so a slow/contended store can't
// hang the worker, while keeping the write independent of the runner's
// (cancellable) lifecycle context.
const finishTimeout = 5 * time.Second

// finish writes the terminal state on a fresh short-lived context, logging (not
// returning) a store error. It deliberately does NOT use the runner's lifecycle
// context: at shutdown that context is cancelled, and a completed job must still
// record its true terminal state. Reap-on-boot remains the fallback only for a
// true process kill in the narrow window between handler-return and this write.
func (r *Runner) finish(id string, state store.JobState, errMsg string) {
	ctx, cancel := context.WithTimeout(context.Background(), finishTimeout)
	defer cancel()
	if err := r.store.Finish(ctx, id, state, errMsg); err != nil {
		log.Printf("jobs: finish %s failed: %v", id, err)
	}
}

func (r *Runner) run(ctx context.Context, job store.Job) {
	h, ok := r.handlers[job.Kind]
	if !ok {
		r.finish(job.ID, store.JobFailed, "no handler for kind "+job.Kind)
		return
	}

	jctx, cancel := context.WithCancel(ctx)
	entry := &inflightJob{cancel: cancel}
	r.mu.Lock()
	r.inflight[job.ID] = entry
	r.mu.Unlock()

	err := h.Run(jctx, job, &JobContext{store: r.store, id: job.ID})

	r.mu.Lock()
	canceled := entry.canceled
	delete(r.inflight, job.ID)
	r.mu.Unlock()
	cancel()

	switch {
	case canceled:
		r.finish(job.ID, store.JobCanceled, "canceled by operator")
	case err != nil:
		r.finish(job.ID, store.JobFailed, err.Error())
	default:
		r.finish(job.ID, store.JobSucceeded, "")
	}
}
