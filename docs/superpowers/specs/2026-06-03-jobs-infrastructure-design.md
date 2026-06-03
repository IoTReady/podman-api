# Phase 3: Jobs infrastructure — design

**Date:** 2026-06-03
**Status:** Approved (brainstorm)
**Tracking:** Forgejo #32 (part of milestone #29). Also closes #42.
**Umbrella:** `docs/superpowers/specs/2026-06-03-migrate-evacuate-design.md`
**Builds on:** #31 (state store foundation, merged PR #40)

## Goal

Add the asynchronous job machinery that `migrate` (#34) and `evacuate` (#35)
will run on: a durable `jobs` table, a background runner that executes queued
jobs through registered per-kind handlers, read-only `GET /jobs` endpoints, and
crash-safe boot recovery. This phase ships **no real job kinds** — migrate and
evacuate register their handlers in later phases; #32 is exercised with a fake
handler in tests.

Jobs require the store, so the whole subsystem is gated behind `-state-db`: with
it off, the runner never starts and the job endpoints return `501 Not
Implemented` (matching the umbrella's opt-in contract).

This phase also resolves **#42**: now that a background runner reads/writes the
DB while the API reads it, raise the SQLite connection pool and add a
`busy_timeout`.

## Decisions locked in this brainstorm

| Decision | Choice |
| --- | --- |
| SQLite concurrency (#42) | **Raise pool to 4 + `PRAGMA busy_timeout=5000`** — real reader concurrency under WAL; competing writer waits, not errors |
| Runner location | New **`internal/jobs`** package (orchestration); job persistence stays in `internal/store` (data) |
| JobStore vs Store | **Separate `JobStore` interface**, both implemented by the one `*SQLite`; consumers depend on the narrow view they need |
| Real job kinds in #32 | **None** — migrate/evacuate register handlers in #34/#35; tested here with a fake handler |
| Worker count | **Fixed 4** (named const); a flag can come later (YAGNI) |
| Shutdown of in-flight jobs | **Cancel, don't block** — interrupted job stays `running`, reaped to `failed` on next boot |
| Job args encryption | **Plaintext** — args carry no secrets (secrets live encrypted in `specs`) |
| Job-create endpoint | **None in #32** — jobs are created by migrate/evacuate POSTs in #34/#35 (`jobs:write` lands with them) |

## Package layout

| Package | Change |
| --- | --- |
| `internal/store` | `jobs` table; `Job`/`JobState`/`JobStep`/`JobFilter` types; `JobStore` interface + SQLite impl; the #42 pool/`busy_timeout` change; `Memory` extended to implement `JobStore` too (test double) |
| `internal/jobs` (new) | `Runner` (worker pool + lifecycle), `Handler` interface + `Registry`, `JobContext` (progress API) |
| `internal/api` | `GET /jobs`, `GET /jobs/{id}`; `jobs:read` scope; `jobView`; `501` when disabled |
| `cmd/podman-api` | build the runner when the store is enabled; start before serving, stop on shutdown |

`jobs` imports `store`. Later, `instance`/main import `jobs` to register the
migrate/evacuate handlers — no import cycle (`store ← jobs ← instance/main`).

## Data model

### `jobs` table

```sql
CREATE TABLE IF NOT EXISTS jobs (
  id        TEXT PRIMARY KEY,            -- sortable, time-prefixed
  kind      TEXT NOT NULL,               -- "migrate" | "evacuate" | test kinds
  args      TEXT NOT NULL,               -- JSON request body (NOT secret)
  state     TEXT NOT NULL,               -- queued | running | succeeded | failed
  steps     TEXT NOT NULL DEFAULT '[]',  -- JSON array of {ts, step, detail}
  parent_id TEXT,                        -- nullable; evacuate→child migrate linkage
  error     TEXT,                        -- failure message
  created   INTEGER NOT NULL,            -- unix seconds (enqueue time)
  started   INTEGER,                     -- nullable
  finished  INTEGER                      -- nullable
);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);
```

Added to the existing schema init in `OpenSQLite` (alongside `specs`). Bump
`PRAGMA user_version` to `2`.

**Args-plaintext invariant.** `args` is stored unencrypted. Migrate/evacuate args
are `{from_host, to_host, template, slug, parameters?}` (and the evacuate map) —
none are secrets; the per-instance secrets live encrypted in the `specs` table.
#34/#35 must never place a secret value in `args`. Documented here as a binding
invariant.

### Types (`internal/store`)

```go
type JobState string

const (
    JobQueued    JobState = "queued"
    JobRunning   JobState = "running"
    JobSucceeded JobState = "succeeded"
    JobFailed    JobState = "failed"
)

type JobStep struct {
    TS     time.Time `json:"ts"`
    Step   string    `json:"step"`
    Detail string    `json:"detail,omitempty"`
}

type Job struct {
    ID       string
    Kind     string
    Args     json.RawMessage // opaque to the store; handlers unmarshal their own shape
    State    JobState
    Steps    []JobStep
    ParentID string // "" if none
    Error    string
    Created  time.Time
    Started  time.Time // zero until claimed
    Finished time.Time // zero until done
}

type JobFilter struct {
    State JobState // "" = any
    Kind  string   // "" = any
}
```

### `JobStore` interface (`internal/store`)

```go
type JobStore interface {
    // Enqueue inserts a new queued job, generating its ID. parentID is "" for
    // top-level jobs.
    Enqueue(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error)
    GetJob(ctx context.Context, id string) (Job, error)        // ErrNotFound when absent
    ListJobs(ctx context.Context, f JobFilter) ([]Job, error)  // newest first
    // ClaimNext atomically transitions the oldest queued job to running and
    // returns it. ok=false when there is nothing to claim.
    ClaimNext(ctx context.Context) (job Job, ok bool, err error)
    AppendStep(ctx context.Context, id string, step JobStep) error
    // Finish sets the terminal state (succeeded/failed), finished timestamp, and
    // error message (empty for success).
    Finish(ctx context.Context, id string, state JobState, errMsg string) error
    // FailRunning marks every job still in `running` as failed with reason.
    // Called once at startup to reap jobs interrupted by a crash/restart.
    FailRunning(ctx context.Context, reason string) (int, error)
}
```

`ErrNotFound` (already defined for specs) is reused.

**Job IDs** are sortable and time-prefixed: `hex(unixNano)` + `-` + `hex(6
random bytes)` (from `crypto/rand`). Fixed-width hex of the nanosecond timestamp
sorts lexicographically by creation time; the random suffix avoids collisions.

**Atomic claim.** `ClaimNext` is a single statement so concurrent workers never
double-claim:

```sql
UPDATE jobs SET state='running', started=?
WHERE id = (SELECT id FROM jobs WHERE state='queued' ORDER BY created, id LIMIT 1)
  AND state='queued'
RETURNING id, kind, args, state, steps, parent_id, error, created, started, finished;
```

The subquery picks the oldest `queued` job; the outer `AND state='queued'` guard
ensures that if another worker claimed that row in the race window, this update
affects 0 rows (no `RETURNING` row → `ok=false`, the worker simply retries).
`RETURNING` (SQLite ≥3.35, supported by `modernc.org/sqlite`) fetches the claimed
row in the same statement. A `-race` test runs two goroutines claiming
concurrently over N queued jobs and asserts each is claimed exactly once.

## Runner (`internal/jobs`)

```go
type Handler interface {
    Run(ctx context.Context, job store.Job, jc *JobContext) error
}

type Registry map[string]Handler // kind → handler

// JobContext is the handler's progress channel back to the store.
type JobContext struct {
    store store.JobStore
    id    string
}
// Step appends a progress entry (best-effort persist; a store error is logged,
// not returned — progress logging must not fail the job).
func (jc *JobContext) Step(step, detail string)

type Runner struct {
    store    store.JobStore
    handlers Registry
    workers  int
    poke     chan struct{}
}

const DefaultWorkers = 4

func NewRunner(js store.JobStore, h Registry, workers int) *Runner
// Start reaps interrupted jobs, then launches the worker pool. It returns
// immediately; the pool runs until ctx is cancelled.
func (r *Runner) Start(ctx context.Context)
// Notify wakes a worker to check for new work (called after an Enqueue so the
// job is picked up without waiting for the poll tick).
func (r *Runner) Notify()
```

**Worker loop.** Each of `workers` goroutines loops: `ClaimNext` → resolve
`handlers[job.Kind]` → run it with a `JobContext` and a context derived from the
runner's lifecycle ctx → `Finish(succeeded)` on nil, `Finish(failed, err.Error())`
on error. An unregistered kind → `Finish(failed, "no handler for kind <kind>")`
(no panic). When `ClaimNext` returns `ok=false`, the worker blocks on the `poke`
channel or a periodic ticker (~5s); the ticker is the safety net for a missed
poke or a post-restart queue.

**Boot recovery.** `Start` calls `FailRunning("interrupted by daemon restart")`
**before** launching workers, so a job left `running` by a previous crash becomes
`failed`. There is no auto-resume — the operator re-issues the operation (migrate
is rollback-safe: the source is untouched until the destination verifies).

**Shutdown.** Main cancels the runner's context on SIGINT/SIGTERM. Workers stop
claiming and an in-flight handler's ctx is cancelled. Shutdown does **not** block
waiting for long handlers to unwind; an interrupted job stays `running` and is
reaped to `failed` on the next boot (the same path as a crash). This is a
deliberate contract, documented at the call site.

## API

| Method | Path | Scope | Result |
| --- | --- | --- | --- |
| GET | `/jobs?state=&kind=` | `jobs:read` | list, newest first |
| GET | `/jobs/{id}` | `jobs:read` | one job, `404` if absent |

- A new **`jobs:read`** scope (wildcard `jobs:*` already works via the existing
  `HasScope`).
- `handlers` gains a `jobs store.JobStore` field; `NewRouter` gains a `jobs
  store.JobStore` parameter. A **nil** JobStore (store disabled) makes both
  endpoints return `501 Not Implemented`.
- `jobView` maps `Job` → JSON: `id, kind, state, args, steps:[{ts,step,detail}],
  parent_id?, error?, created, started?, finished?`. Timestamps RFC3339; nullable
  ones (`parent_id`, `error`, `started`, `finished`) omitted when empty/zero.
- No job-create endpoint in this phase.

## #42 fix (`internal/store/sqlite.go`)

In `OpenSQLite`:

- `db.SetMaxOpenConns(4)` and `db.SetMaxIdleConns(4)` (was `1`/`1`).
- Add `PRAGMA busy_timeout = 5000` (5s) after opening.
- Update the WAL comment: with >1 connection the reader concurrency is now
  realized (API reads while the runner writes); the busy-timeout makes a
  contending writer wait instead of failing with `database is locked`.

`const maxOpenConns = 4` as a named constant. Existing spec tests are unaffected
(they don't depend on the pool size). The atomic-claim statement keeps job
claiming correct under the concurrent pool.

## Backend wiring (`cmd/podman-api`)

`OpenSQLite` returns the single `*SQLite` that satisfies both `store.Store` and
`store.JobStore`. To keep `main` holding one handle while consumers depend on
narrow views, add a combined interface:

```go
// DB is the full backend: spec store + job store + closer. main holds one of
// these; instance.Service takes the Store view, the runner takes the JobStore
// view.
type DB interface {
    Store
    JobStore
    io.Closer
}
```

`openStore` returns `(store.DB, error)` (was `(store.Store, error)`); `nil` when
disabled. `main`:

1. `db, err := openStore(...)` — fatal on error (unchanged refuse-to-start).
2. If `db != nil`:
   - `defer db.Close()`.
   - `svc.SetStore(db)` (Store view).
   - `runner := jobs.NewRunner(db, registry, jobs.DefaultWorkers)` and
     `runner.Start(ctx)` where `ctx` is cancelled by the shutdown handler. The
     registry is **empty** in #32 (no kinds yet); migrate/evacuate add entries in
     #34/#35.
   - Pass `db` (JobStore view) to `api.NewRouter`.
3. If `db == nil`: pass `nil` JobStore to the router (→ `501`), don't start the
   runner.

The runner's lifecycle context is cancelled in the existing SIGINT/SIGTERM
shutdown goroutine, before/alongside `srv.Shutdown`.

## Testing (TDD)

**`internal/store` — JobStore on real temp SQLite**
- `Enqueue` creates a `queued` job with a generated id and `created` set.
- `GetJob` round-trips; missing id → `ErrNotFound`.
- `ListJobs` filters by state and kind; returns newest first.
- `ClaimNext` flips the oldest `queued`→`running`, sets `started`, returns it;
  returns `ok=false` on an empty queue.
- **Concurrency:** two goroutines calling `ClaimNext` over N queued jobs claim
  each job exactly once (run under `-race`).
- `AppendStep` appends a step (visible via `GetJob`).
- `Finish` sets terminal state, `finished`, and error (empty on success).
- `FailRunning` flips all `running`→`failed`, returns the count, leaves other
  states untouched.

**`internal/jobs` — Runner with an in-memory JobStore double**
- A fake handler that succeeds → job reaches `succeeded`.
- A handler returning an error → `failed` with that message.
- A job whose kind has no registered handler → `failed "no handler for kind …"`.
- `jc.Step(...)` entries are persisted (asserted via `GetJob`).
- Boot recovery: a pre-seeded `running` job is `failed` after `Start`.
- Cancelling the runner ctx stops the workers cleanly (no goroutine leak — the
  test waits for Start's workers to exit).
- `store.Memory` is extended to implement `JobStore` (in-memory jobs map) so the
  runner is unit-testable without disk; it keeps the existing `PutErr`/`DeleteErr`
  hooks pattern and adds the job methods.

**`internal/api`**
- `GET /jobs` and `/jobs/{id}` JSON shape; `state`/`kind` filters; `404` for an
  unknown id.
- **Store disabled:** a nil JobStore → both endpoints return `501`.
- `jobs:read` scope is enforced (a key lacking it → `403`, via the existing auth
  test pattern).

**`cmd/podman-api`**
- The runner is constructed and started only when the store is enabled; the
  wiring compiles and the pool/`busy_timeout` are applied (covered by the store
  tests plus an `openStore`-returns-`DB` check).

## Out of scope for this phase

- No `migrate`/`evacuate` handlers or POST endpoints (later phases register
  handlers into the runner's registry).
- No `jobs:write` scope (lands with the create endpoints in #34/#35).
- No job retention/pruning (history grows unbounded for now; a retention policy
  is a future concern).
- No per-job cancellation endpoint, no retry endpoint (YAGNI until asked).
- No configurable worker count flag (fixed `DefaultWorkers`).
