# Job-runner robustness — design (#54)

**Status:** approved
**Date:** 2026-06-04
**Issue:** #54 (migrate/evacuate hardening umbrella) — operational + capability sub-items
**Batch:** job-runner robustness (worker headroom + job cancellation)

## Goal

Harden the jobs subsystem with two changes shipped as one PR:

1. **Worker headroom** — reduce the chance a parent `evacuate` job starves plain
   jobs of a worker slot.
2. **Job cancellation** — let an operator stop a queued or running job, recording
   a distinct `canceled` terminal state.

Out of scope (parked): bind-mount / host-path support (now its own low-priority
issue, #73); the structural two-pool orchestration split (a future option, see
Feature 1).

## Background

The jobs runner (`internal/jobs/runner.go`) is one shared pool of
`DefaultWorkers = 4`. Each worker loops `ClaimNext` (claims the oldest queued job
*regardless of kind*) → run handler → `finish`. A parent `evacuate` handler
(`internal/evacuate/handler.go`) blocks on `wg.Wait()` for its entire fan-out,
holding its worker the whole time; its child migrations run as in-handler
goroutines (`StartChild`, never claimed by the pool). So enough concurrent
evacuates occupy every worker and starve plain migrate/other jobs. The handler's
own doc comment already flags "give the runner headroom or a separate
orchestration pool."

Cancellation does not exist today: no terminal state, no store method, no route,
no in-memory handle on a running job's context (only the runner's lifecycle ctx,
cancelled at shutdown, can interrupt a handler).

The store (`internal/store/jobs.go`) has states `queued/running/succeeded/failed`;
`state` is free `TEXT` (no `CHECK` constraint, so a new state needs no schema
migration). All writes are serialized by an in-process mutex (added in #70), so
write concurrency across workers is effectively 1.

## Feature 1 — worker headroom

Chosen approach: **add headroom** (not a structural pool split). It raises the
starvation threshold rather than eliminating it; the spec states this honestly,
and the two-pool option stays recorded in #54 for later.

Changes:

- `internal/jobs/runner.go`: raise `DefaultWorkers` from `4` to `8`.
- `cmd/podman-api/main.go`: new flag **`-job-workers`** (default
  `jobs.DefaultWorkers`, clamped to `>= 1`), passed to `NewRunner` in place of
  the hardcoded `jobs.DefaultWorkers`.
- `internal/store/sqlite.go`: raise **`maxOpenConns` from 8 to 12** and update its
  comment. Rationale: the pool is kept above the worker count so concurrent
  `GET /jobs` readers are not starved while a worker holds the single write
  connection. Writes are mutex-serialized, so this is purely reader headroom.
  (Note for operators: a very large `-job-workers` may warrant a correspondingly
  larger pool; the default pairing of 8 workers / 12 conns is the supported
  baseline.)
- `internal/evacuate/handler.go`: update the doc comment — a parent evacuate still
  occupies one worker for its whole fan-out; headroom raises the threshold; the
  structural fix (separate orchestration pool) remains a future option.

## Feature 2 — job cancellation

A distinct `canceled` terminal state, an in-memory cancel handle in the runner,
and a `POST /jobs/{id}/cancel` route.

### State

`internal/store/jobs.go`: add

```go
JobCanceled JobState = "canceled"
```

`Finish` accepts `JobCanceled` as a valid terminal state (extend the guard in
both `*SQLite.Finish` and `*Memory.Finish`, which currently reject anything other
than succeeded/failed). No schema change (`state` is free `TEXT`).

### Store method

Add to the `JobStore` interface and both implementations:

```go
// CancelQueued atomically transitions a still-queued job to canceled (setting
// finished). Returns true if it transitioned; false if the job was not in the
// queued state (already claimed, terminal, or absent).
CancelQueued(ctx context.Context, id string) (bool, error)
```

- `*SQLite`: `UPDATE jobs SET state='canceled', finished=? WHERE id=? AND state='queued'`,
  run through the same write-mutex/retry path as other writes; return
  `RowsAffected() > 0`.
- `*Memory`: under its mutex, if the job exists and is `queued`, set
  `state=canceled`, `finished=now`, return true; else false.

Terminal predicates updated to include `canceled`:

- `*SQLite.PruneJobs`: both `state IN (...)` clauses become
  `('succeeded','failed','canceled')`.
- `*Memory`: the `terminal` helper includes `JobCanceled`.

`FailRunning` is unaffected — a canceled job is not in `running`.

### Runner

`internal/jobs/runner.go` gains an in-flight registry:

```go
mu       sync.Mutex
inflight map[string]*inflightJob

type inflightJob struct {
    cancel   context.CancelFunc
    canceled bool
}
```

- `run(ctx, job)`: derive `jctx, cancel := context.WithCancel(ctx)`; register
  `inflight[job.ID] = &inflightJob{cancel: cancel}` under `mu`; run the handler
  with `jctx`; after it returns, under `mu` read the `canceled` flag and delete
  the entry. Terminal state: if `canceled` → `finish(JobCanceled)` **regardless**
  of the handler's return value; else the existing succeeded/failed logic.
  (`cancel()` is also called via the registry on a cancel request; the deferred
  delete is the cleanup.)
- New method:

```go
// Cancel signals an in-flight job to stop. Returns true if the job was found
// running (its handler context is cancelled and it will finish as canceled);
// false if no such job is currently running.
func (r *Runner) Cancel(id string) bool
```

  Under `mu`: if `inflight[id]` exists, set `canceled = true`, call its `cancel()`,
  return true; else false.

This makes `*Runner` satisfy a small interface consumed by the API:

```go
type JobCanceller interface{ Cancel(id string) bool }
```

### API

`internal/api`: `handlers` gains a `canceller JobCanceller` field; `NewRouter`
gains the parameter; `main` passes the runner.

New route (guarded by `instances:write`, consistent with `POST /migrate` and
`POST /evacuate`; existing `instances:*` keys already cover it — no orphan
`jobs:write` scope):

```
POST /jobs/{id}/cancel
```

Handler `cancelJob`:

1. `h.jobs == nil` → `501` (jobs store disabled).
2. `GetJob(id)`; `ErrNotFound` → `404`.
3. State is terminal (`succeeded`/`failed`/`canceled`) → `409` ("job already
   terminal").
4. State is `running` → `h.canceller.Cancel(id)`:
   - true → `202` + job view (cancellation is async; the handler unwinds and the
     terminal write lands shortly after).
   - false (raced to finish) → re-`GetJob`; terminal → `409`.
5. State is `queued` → `CancelQueued(id)`:
   - true → `202` + job view.
   - false (raced into running) → `h.canceller.Cancel(id)`; true → `202`; else
     `409`.

Response: `202 Accepted` with the current `jobView`. Running cancels are
asynchronous, so the returned state may still be `running`.

### Parent/child propagation

Cancelling a running parent `evacuate` cancels its handler context, which is
shared by the in-handler child migrations. Each child's `migrate` is already
ctx-aware: it rolls back (source left intact — guaranteed by the existing migrate
rollback) and returns a `context.Canceled`-wrapped error.

`internal/evacuate/handler.go` `runChild`: when `errors.Is(migErr,
context.Canceled)`, finish the child as `store.JobCanceled` instead of
`store.JobFailed`; otherwise unchanged. The parent itself is finished `canceled`
by the runner (its `canceled` flag is set), regardless of the aggregated error it
returns.

## Error handling & edge cases

- **queued↔running race at cancel time.** Handled by the fallthroughs in API
  steps 4 and 5 (try the other path once before concluding terminal). The window
  is tiny and documented.
- **Crash after cancel-request, before the terminal write.** The job is still
  `running` on disk; boot recovery (`FailRunning`) marks it
  `failed("interrupted by daemon restart")`. Acceptable — it was interrupted —
  and documented.
- **Retention.** `canceled` is terminal, so `PruneJobs` reaps it like
  succeeded/failed.
- **Cancel safety.** A running migrate cancelled mid-copy rolls back and leaves
  the source intact (existing guarantee); cancellation never causes data loss.

## Testing (TDD)

Store (both `*SQLite` and `*Memory`):
- `TestCancelQueued` — queued→canceled; no-op (false) when running/terminal/absent.
- `TestFinish_AcceptsCanceled` — `Finish(..., JobCanceled, ...)` succeeds.
- `TestPruneJobs_IncludesCanceled` — a canceled terminal job is pruned.

Runner:
- `TestRunner_CancelRunning` — a handler that blocks on ctx observes cancellation
  and the job ends `canceled`, not `failed`.
- `TestRunner_CancelUnknown` — `Cancel` of an unknown id returns false.

API:
- `TestCancelJob` table — `501` (jobs disabled), `404` (absent), `409` (terminal),
  `202` (queued), `202` (running), and the race fallthrough — using a fake
  canceller.

Evacuate:
- `TestEvacuate_CancelMarksChildrenCanceled` — a cancelled parent records its
  in-flight children as `canceled`.

Wiring:
- `-job-workers` flag default + clamp.
- `openapi_test` updated for the new route and the `canceled` state.

## Documentation

- `api/openapi.yaml`: add `POST /jobs/{id}/cancel` and the `canceled` enum value
  to the job-state schema.
- Wiki `Operating.md`: job cancellation (verb, scope, async-for-running, the
  boot-race caveat, migrate rollback safety) and a note that headroom raises but
  does not remove the evacuate starvation threshold.
- Wiki `Deploying.md`: the `-job-workers` flag in the Run example.
- `README.md`: `-job-workers` flag bullet.
