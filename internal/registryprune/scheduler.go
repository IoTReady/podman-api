package registryprune

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// TickInterval is how often the scheduler re-evaluates whether a run is due. It
// is the granularity of the interval and backoff gates, not the run cadence
// itself (that is Scheduler.Interval).
const TickInterval = time.Minute

// activeScanLimit bounds how many recent registry-prune jobs are scanned to
// find in-flight work and the last terminal outcome. Active jobs are always
// among the newest, so this is ample.
const activeScanLimit = 500

// failureBackoff is how long to wait after a failed run before re-enqueuing, so
// a persistently failing registry is not retried every tick (which would flood
// the job store — and a registry prune's failure mode, an unreachable registry,
// is exactly the persistent kind).
const failureBackoff = time.Hour

// Scheduler enqueues registry-prune jobs on a schedule. Store/Now are injected
// so the tick logic is unit-testable without real time.
//
// Unlike prune.Scheduler there is no per-host dimension: there is one registry,
// so dedup, backoff and the interval gate are all global.
type Scheduler struct {
	Store store.JobStore
	// Interval is the run cadence. Zero or negative disables the scheduler
	// entirely (it never enqueues), rather than meaning "every tick".
	Interval time.Duration
	// Payload is evaluated once per enqueue. (The server does not re-parse
	// flags on SIGHUP — only hosts and the operator file are reloaded — so this
	// is a seam for a future reload, not one today.)
	Payload func() Payload
	Now     func() time.Time

	wg sync.WaitGroup
	// enqueueMu makes the in-flight check and the enqueue that follows it one
	// atomic step. Without it two callers — a tick and an on-demand POST, or
	// two POSTs — can both pass scanJobs before either has written a row, and
	// the concurrency guard this whole design leans on evaporates under exactly
	// the load it exists for.
	enqueueMu sync.Mutex
}

// ErrRunInFlight is returned by EnqueueNow when a run is already queued or
// running. Two concurrent runs would each classify the whole catalog from a
// listing the other is mutating.
var ErrRunInFlight = errors.New("a registry prune run is already queued or running")

// Start launches the ticker loop until ctx is cancelled. An immediate first
// pass runs before the ticker, so a control plane that has been down past its
// interval prunes without waiting a full tick. Use Wait to block until the loop
// has exited after cancellation.
func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(TickInterval)
		defer t.Stop()
		runTick := func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("registryprune: scheduler tick panicked: %v", r)
				}
			}()
			s.tick(ctx)
		}
		runTick() // prompt first pass
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runTick()
			}
		}
	}()
}

// Wait blocks until the scheduler goroutine has exited (after its ctx is
// cancelled). Mirrors jobs.Runner so callers can drain cleanly on shutdown.
func (s *Scheduler) Wait() { s.wg.Wait() }

func (s *Scheduler) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// EnqueueNow enqueues a run immediately, bypassing the interval and backoff
// gates but NOT the in-flight check. It is the on-demand trigger behind
// POST /registry/prune.
//
// Skipping the interval gate is the entire point. That gate is backed by
// PERSISTED job history — the newest succeeded run being younger than Interval
// returns early — so it survives a restart, and without this method the only
// way to get a second run inside 24h is to edit -registry-prune-interval and
// restart, which is precisely the knob nobody should be improvising with on the
// day of the first real delete. #220's rollout is a sequence of on-demand runs
// (dry run, read the steps, Stage-A-only real run, verify, then Stage B) and
// each step must not cost a day.
//
// The in-flight check is NOT skipped, and is taken under the same lock the
// ticker uses: a manual enqueue that raced past it would put two runs on the
// same catalog, each reasoning from a listing the other is mutating.
func (s *Scheduler) EnqueueNow(ctx context.Context) (store.Job, error) {
	if s.Payload == nil || s.Store == nil {
		return store.Job{}, errors.New("registry prune scheduler is not configured")
	}
	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	if inflight, _, _ := s.scanJobs(ctx); inflight {
		return store.Job{}, ErrRunInFlight
	}
	args, err := json.Marshal(s.Payload())
	if err != nil {
		return store.Job{}, fmt.Errorf("marshal registry-prune payload: %w", err)
	}
	job, err := s.Store.Enqueue(ctx, JobKind, args, "")
	if err != nil {
		return store.Job{}, fmt.Errorf("enqueue registry-prune: %w", err)
	}
	log.Printf("registryprune: enqueued an on-demand registry prune run (job %s)", job.ID)
	return job, nil
}

// tick enqueues a run if one is due and none is in flight.
func (s *Scheduler) tick(ctx context.Context) {
	if s.Interval <= 0 || s.Payload == nil {
		return
	}
	// Same lock as EnqueueNow, so the scan and the enqueue below cannot
	// interleave with an on-demand trigger.
	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	inflight, lastSuccess, lastFailure := s.scanJobs(ctx)
	if inflight {
		// A registry prune classifies the whole catalog; a second concurrent
		// run would reason from a listing the first one is mutating.
		return
	}
	now := s.now()

	// Error backoff, cleared by a later success.
	if !lastFailure.IsZero() && now.Sub(lastFailure) < failureBackoff {
		if lastSuccess.IsZero() || lastFailure.After(lastSuccess) {
			return
		}
	}
	// A never-run scheduler is due immediately.
	if !lastSuccess.IsZero() && now.Sub(lastSuccess) < s.Interval {
		return
	}

	args, err := json.Marshal(s.Payload())
	if err != nil {
		log.Printf("registryprune: marshal payload: %v", err)
		return
	}
	if _, err := s.Store.Enqueue(ctx, JobKind, args, ""); err != nil {
		log.Printf("registryprune: enqueue failed: %v", err)
		return
	}
	log.Printf("registryprune: enqueued a registry prune run")
}

// scanJobs reports whether a run is queued or running, and the most recent
// succeeded/failed finish times. On a store error it reports in-flight — fail
// safe toward not enqueuing.
func (s *Scheduler) scanJobs(ctx context.Context) (inflight bool, lastSuccess, lastFailure time.Time) {
	jobs, err := s.Store.ListJobs(ctx, store.JobFilter{Kind: JobKind, Limit: activeScanLimit})
	if err != nil {
		log.Printf("registryprune: list jobs failed (assuming a run is in flight): %v", err)
		return true, time.Time{}, time.Time{}
	}
	for _, j := range jobs {
		switch j.State {
		case store.JobQueued, store.JobRunning:
			inflight = true
		case store.JobSucceeded:
			if j.Finished.After(lastSuccess) {
				lastSuccess = j.Finished
			}
		case store.JobFailed:
			if j.Finished.After(lastFailure) {
				lastFailure = j.Finished
			}
		}
	}
	return inflight, lastSuccess, lastFailure
}
