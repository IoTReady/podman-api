# Boot-time reconciliation of interrupted migrates — Design

**Issue:** #54 (migrate/evacuate hardening backlog) — sub-item *"No auto-resume after a daemon restart"*.

**Status:** Approved design. Scope is **reconciliation (option B)**, not resume-and-continue (option A, deferred).

## Problem

When the daemon restarts, `Runner.Start` calls `store.FailRunning`, which flips
**every** `running` job to `failed` ("interrupted by daemon restart"). Queued jobs
already survive a restart untouched and are claimed after boot, so they are not the
problem. The gap is the **in-flight** jobs — and the only in-flight job kinds are
`migrate` and the parent `evacuate` that fans out child migrates.

A `migrate` is a destructive, multi-phase, partially-completed operation with no
persisted checkpoint:

1. load source spec
2. preflight destination
3. **Stop source**
4. `migratePostStop`: provision per-host secrets → copy + verify volumes → Apply on dest → verify healthy
5. on success: **reap source** (irreversible commit)
6. on failure: roll back (restart source, reap dest)

A crash can land anywhere in there, leaving an ambiguous on-host state (source
stopped with no dest; partial dest; healthy dest with source not yet reaped). Today
the job is simply marked `failed` and the operator must inspect both hosts and clean
up by hand. Naïve re-enqueue is unsafe: a re-run's own preflight trips
`instance_exists` if Apply already ran, stranding a stopped source and an orphaned
dest.

## Goal

On boot, drive each interrupted migrate to a **consistent end-state** automatically —
roll forward to completion when the destination is already healthy, otherwise roll
back to the pre-migrate state — without ever destroying the only surviving copy of
the data. Resume-and-continue (continuing a migrate from its interrupted phase, and
re-spawning an evacuate's un-started children) is explicitly out of scope.

## Decisions (locked during brainstorming)

1. **Policy:** roll-forward when safe, else roll-back.
2. **Boot timing:** asynchronous; retry until the host is reachable (no boot stall, nothing force-failed for a transient outage). Introduces a non-terminal `reconciling` state.
3. **Evacuate parent:** always marked `failed`; its child migrates are reconciled individually. The parent is *not* resumed. The operator re-issues evacuate, which naturally only re-moves instances still on the source (already-migrated ones are gone from `from_host`).
4. **Escape hatch:** `POST /jobs/{id}/cancel` is extended to transition a `reconciling` job → `canceled` (operator gives up; cleans up manually).

## Architecture

A **`Reconciler` registry parallel to the existing `Handler` registry.** The jobs
runner stays kind-agnostic; all host logic lives in the `migrate` package. This
mirrors the established `jobs.Handler` / `jobs.Registry` pattern.

```go
// internal/jobs/runner.go
type Reconciler interface {
    // Reconcile drives an interrupted job to a consistent state. It returns the
    // terminal state to record. resolved=false means inconclusive (e.g. a host is
    // unreachable) — the job is left reconciling and retried on the next sweep.
    // err is for unexpected/logged failures; an inconclusive result is not an error.
    Reconcile(ctx context.Context, job store.Job, jc *JobContext) (state store.JobState, resolved bool, err error)
}

type Reconcilers map[string]Reconciler // keyed by job kind
```

The runner gains a `reconcilers Reconcilers` field, set via an exported
`SetReconcilers(Reconcilers)` method called before `Start` (this keeps
`NewRunner`'s existing `(js, h, workers)` signature stable; a nil/empty map means
no reconcilers and exactly today's behaviour). `main` builds the map with
`migrate.Reconciler{Svc: svc}` for kind `"migrate"` and calls
`runner.SetReconcilers(...)`. `evacuate` is **not** in the map, so the parent
falls through to `failed` (decision 3).

### Boot transition

`Runner.Start` replaces the single `FailRunning` call with two ordered store calls:

```go
recon := r.reconcilableKinds()                  // keys of r.reconcilers
if n, err := r.store.MarkReconciling(ctx, recon); err == nil && n > 0 {
    log.Printf("jobs: marked %d interrupted job(s) reconciling on startup", n)
}
if n, err := r.store.FailRunning(ctx, "interrupted by daemon restart"); err == nil && n > 0 {
    log.Printf("jobs: marked %d interrupted job(s) failed on startup", n)
}
```

Order matters: `MarkReconciling` runs first, so those rows are no longer `running`
and `FailRunning` only catches the rest (parent evacuate, unknown kinds). When the
reconciler map is empty, `MarkReconciling` is a no-op over an empty kind list and the
behaviour is exactly today's.

### The reconcile loop

A new goroutine, tracked by the runner's `WaitGroup` (same shape as
`StartRetention`): an immediate first sweep, then every `reconcileInterval` until
ctx is cancelled.

```go
const reconcileInterval = 30 * time.Second
```

Each sweep lists `reconciling` jobs (`ListJobs(JobFilter{State: JobReconciling})`)
and processes them with **bounded concurrency** (bounded by the runner's worker
count), so one unreachable host does not block other jobs' reconciliation. Per job:

- look up the reconciler by `job.Kind` (skip + log if somehow absent);
- call `Reconcile`;
- **resolved** → `ResolveReconciling(id, state, msg)` (CAS, below);
- **inconclusive** (`resolved == false`) → log, leave `reconciling`; retried next sweep ("retry until reachable").

The loop goroutine is launched inside `Start`, immediately after the boot
transition, and only when `len(r.reconcilers) > 0`.

### Cancel race (compare-and-swap)

Both the loop and an operator `cancel` can try to leave `reconciling`. Two
conditional store methods, each guarded `WHERE id=? AND state='reconciling'`:

- `ResolveReconciling(ctx, id, state, msg) (bool, error)` — loop records `succeeded`/`failed`.
- `CancelReconciling(ctx, id) (bool, error)` — API records `canceled`.

Whoever writes first wins; the other's conditional UPDATE affects 0 rows and
no-ops. If the operator cancels first, the loop's resolve is ignored (cancel
respected) and the host state is left as-is for manual cleanup. If the loop
finishes first, cancel no-ops and the endpoint returns the already-terminal state.

## The reconcile algorithm (migrate)

`migrate.Reconciler.Reconcile` unmarshals the job's `Args` into a
`MigrateRequest` (same shape the handler uses), takes `migrateLock(Template, Slug)`
(serialises against any re-issued migrate of the same instance), then inspects both
hosts and decides:

| Destination | Source | Action | Result |
|---|---|---|---|
| exists & **healthy** (`waitRunning` passes) | present | roll forward: reap source (`Delete{PruneVolumes,PruneSecrets}`) | `succeeded` |
| exists & healthy | already gone | nothing to do (commit had finished) | `succeeded` |
| absent or **unhealthy** | present | roll back: `Start` source, reap dest | `failed` |
| absent or **unhealthy** | **already gone** | **do NOT reap dest** — it is the only copy; leave in place | `failed` (needs-attention message) |
| **inconclusive** (host unreachable) | — | leave `reconciling`, retry next sweep | (`reconciling`) |

The fourth row is the safety rule: the source is only ever reaped *after* the dest
verified healthy, so "source gone + dest now unhealthy" is a post-commit
degradation — rolling back would destroy the only copy. We never do that; we record
`failed` with a clear message and leave the dest for the operator.

All actions reuse existing primitives (`waitRunning`, `Start`,
`Delete{PruneVolumes,PruneSecrets}`), so reconciliation is consistent with the
in-handler rollback/commit by construction. Inspection that returns an unreachable /
transient error from any host → `resolved=false` (inconclusive). Each branch records
a job step via `jc.Step`: `reconcile-roll-forward`, `reconcile-roll-back`,
`reconcile-orphan-dest`, `reconcile-inconclusive`.

"Healthy" is determined with the same `waitRunning` check the handler uses
(pod + every container `Running`, and every container declaring a healthcheck
`healthy`), bounded by the existing `-migrate-verify-timeout`. A dest that never
becomes healthy within the window is treated as unhealthy → roll back.

## Store changes

New state, non-terminal:

```go
const JobReconciling JobState = "reconciling"
```

Three new methods on the `JobStore` interface (implemented by `*SQLite` and `*Memory`):

```go
// MarkReconciling moves running jobs whose kind is in kinds → reconciling
// (boot recovery). Returns the count moved. Empty kinds → no-op, returns 0.
MarkReconciling(ctx context.Context, kinds []string) (int, error)

// ResolveReconciling transitions a reconciling job → state (succeeded/failed),
// setting finished + error. CAS: only affects a row currently in reconciling.
// Returns true if it transitioned.
ResolveReconciling(ctx context.Context, id string, state JobState, errMsg string) (bool, error)

// CancelReconciling transitions a reconciling job → canceled, setting finished.
// CAS: only affects a row currently in reconciling. Returns true if it transitioned.
CancelReconciling(ctx context.Context, id string) (bool, error)
```

`PruneJobs` already deletes only `succeeded`/`failed`, so `reconciling` is excluded
from retention automatically — no change. `ResolveReconciling` rejects any state
other than `succeeded`/`failed` (programming-error guard, like `Finish`).

SQL sketch:

```sql
-- MarkReconciling (kinds expanded to placeholders)
UPDATE jobs SET state='reconciling' WHERE state='running' AND kind IN (?, ...);
-- ResolveReconciling
UPDATE jobs SET state=?, error=?, finished=? WHERE id=? AND state='reconciling';
-- CancelReconciling
UPDATE jobs SET state='canceled', finished=? WHERE id=? AND state='reconciling';
```

## API changes

- `POST /jobs/{id}/cancel` gains a branch: when the job is neither inflight
  (`Runner.Cancel` returns false) nor `queued` (`CancelQueued` returns false), try
  `CancelReconciling`; on success return the same shape as a queued cancel
  (`canceled`). Only then fall through to the existing `job_terminal` /
  `job_not_cancelable` handling.
- OpenAPI: add `reconciling` to the job-state enum; document the state (a job whose
  in-flight migrate was interrupted by a restart and is being driven to a consistent
  state) and that cancelling a `reconciling` job means "give up; the source/dest are
  left as-is for manual cleanup".

## Scope guards (YAGNI)

- `reconcileInterval` is a const; a `-job-reconcile-interval` flag is a trivial later add.
- No resume-and-continue (option A); no re-spawning of an evacuate's un-started children.
- No new metrics (#52 OTel counters for reconciliation outcomes are a noted future).
- Evacuate registers no reconciler; the parent is always `failed`.

## Testing

- **store** (sqlite + memory):
  - `MarkReconciling` moves only `running` rows of a matching kind, returns the count, leaves other states/kinds untouched; empty kinds → 0.
  - `ResolveReconciling` / `CancelReconciling` only transition from `reconciling` (CAS proof: a no-longer-reconciling row → `false`, 0 rows); `ResolveReconciling` rejects non-terminal states.
- **migrate reconciler** (fake podman client + memory store): all five matrix rows, including
  - inconclusive (host inspect returns unreachable → `resolved=false`, no host mutation);
  - the safety row (dest unhealthy + source gone → `failed`, **assert no Delete issued on the dest**).
- **runner**: boot transition (reconcilable kind → `reconciling`, other kind → `failed`) with a fake reconciler; the loop resolves resolvable jobs and retries inconclusive ones; cancel-vs-loop CAS (operator cancel wins → stays `canceled`).
- **API**: `reconciling` appears in job listing / state enum; cancel of a `reconciling` job → `canceled`.

## Documentation

Wiki **Operating** gains a new *"Auto-reconciliation after restart"* section: what
boot recovery does now, the `reconciling` state, the roll-forward/roll-back
semantics and the never-destroy-the-only-copy safety rule, retry-until-reachable, and
the cancel escape hatch.
