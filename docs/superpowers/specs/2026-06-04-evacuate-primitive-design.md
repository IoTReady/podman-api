# Evacuate primitive (POST /evacuate) — design

**Issue:** #35 (Phase 6 of #29). **Depends on:** #34 (migrate primitive).

## Goal

Add a server-side `evacuate` primitive so a client can clear every instance off
one host in a single call. The daemon fans the work out into one **child migrate
job** per instance, runs them with bounded concurrency, and reports an aggregate
result on a **parent job**. Placement stays client policy: the request supplies
the `slug → destination host` map; the daemon executes it.

Evacuate composes the existing `migrate` primitive — it adds orchestration, not a
new movement mechanism.

## Request / API

```
POST /evacuate            (guard: instances:write)
{
  "from_host": "hostA",
  "map": { "acme": "hostB", "globex": "hostC" }
}
→ 202 { "job_id": "<parent job id>" }
```

- `map` is `slug → to_host`. It carries **no** template and **no** parameters:
  the daemon resolves the template from the stored spec, and `migrate` already
  merges the stored spec's parameters. The map is placement only.
- Job args (parent and children) are plaintext and carry **no secrets**, as with
  every other job. Secrets stay encrypted in the `specs` table and are re-read by
  `Migrate` via `GetSpec`.

### Handler behaviour (`internal/api/evacuate.go`)

1. `h.jobs == nil` (store/jobs disabled) → `501 not_implemented`.
2. Body fails to decode → `400 invalid_body`.
3. `svc.ResolveEvacuation(ctx, req)` for synchronous fast-fail validation
   (discarding the resolved moves) → on error, `WriteError` maps it (see
   *Validation*).
4. Marshal the request as the parent job's args, `Enqueue("evacuate", args, "")`,
   return `202 { job_id }`.

The handler never spawns children itself — it only enqueues the parent. The
shared job runner claims the parent and runs the evacuate handler, which does the
fan-out.

## Discovery & validation

`svc.ResolveEvacuation(ctx, req) ([]instance.MigrateRequest, error)` is the single
planning function. It is called twice: by the POST handler (fast-fail, result
discarded) and by the parent job handler at execution time (state may have
drifted between enqueue and run, so it re-validates).

Steps:

1. `from_host` must be a known host → else `ErrUnknownHost`.
2. Store must be enabled → else `ErrStoreDisabled`.
3. Enumerate the host's instances with the new `Store.ListSpecKeys(host)`
   returning `[]SpecKey{Template, Slug}` (no secret decryption — planning needs
   only the keys).
4. Build a `slug → template` index from the keys. If a slug maps to **more than
   one template** on this host, the slug-keyed map cannot express it →
   `ErrInvalidEvacuation` (`400`, message names the slug).
5. **Strict bijection** between the host's instances and `map`:
   - every enumerated instance's slug must be present in `map` (else
     `ErrInvalidEvacuation`, "no destination for slug X") — prevents silently
     leaving an instance behind, which would defeat "evacuate";
   - every `map` key must match an enumerated instance (else
     `ErrInvalidEvacuation`, "no such instance: slug X") — catches typos.
   - An empty host with an empty `map` is a valid no-op (zero moves).
6. For each `(template, slug)` build a `MigrateRequest{FromHost: from_host,
   ToHost: map[slug], Template, Slug, Parameters: nil}`. Each `to_host` must be a
   known host (`ErrUnknownHost`) and `!= from_host` (`ErrSameHost`).

Returns the moves slice (stable order: sorted by slug, so child-job ordering and
test assertions are deterministic).

### HTTP mapping (`internal/api/errors.go`)

- `ErrUnknownHost` → existing mapping (unchanged).
- `ErrSameHost` → `400 invalid_request` (existing).
- `ErrStoreDisabled` → `501 not_implemented` (existing).
- `ErrInvalidEvacuation` (new sentinel) → `400 invalid_request`. All
  bijection/ambiguity failures wrap this with a specific message.

## Execution — parent self-drives children

`internal/evacuate/handler.go`:

```go
type Handler struct {
    Svc  *instance.Service
    Jobs store.JobStore
}
var _ jobs.Handler = (*Handler)(nil)

// evacuateConcurrency bounds how many child migrations run at once. migrate is
// heavy (stop + volume cold-copy + apply + verify), so default low. var (not
// const) so same-package tests can shorten/lengthen it.
var evacuateConcurrency = 2
```

`Run(ctx, job, jc)`:

1. Unmarshal `job.Args` → `EvacuateRequest` (wrap "decode evacuate args: %w").
2. `moves, err := h.Svc.ResolveEvacuation(ctx, req)`; on error return it (parent
   fails — e.g. a host went away after enqueue).
3. Fan out with **`sync.WaitGroup` + a buffered semaphore channel** of size
   `evacuateConcurrency`. **Not** `errgroup` with `SetLimit`: errgroup cancels
   the group on the first error, but a sibling failure must **not** abort the
   other migrations (mirrors bulk-ops).
4. Per move (in its own goroutine, gated by the semaphore):
   - `child, err := h.Jobs.StartChild(ctx, "migrate", marshal(move), job.ID)` —
     new store method that inserts a job **already in `running`** with
     `parent_id = job.ID` and `started = now`. Because it is never `queued`, the
     runner's `ClaimNext` (which claims only `queued`) cannot race it.
   - `cjc := jobs.NewJobContext(h.Jobs, child.ID)` — the migrate's progress steps
     land on the child job.
   - `err := h.Svc.Migrate(ctx, move, cjc.Step)`.
   - `h.Jobs.Finish(ctx, child.ID, succeeded|failed, errMsg)`.
   - record the outcome (slug, err) into a results slice (index-addressed, no
     shared-mutation race).
   - best-effort parent step: `jc.Step("migrate", "<slug> → <to_host>: ok|FAILED: …")`.
5. `wg.Wait()`, then aggregate: count failures.
   - all succeeded → return `nil` (runner marks the parent `succeeded`).
   - otherwise return an error
     `"<n>/<total> migrations failed: <slug>: <err>; <slug>: <err>"` (runner marks
     the parent `failed`, message stored in the job's `error` column). A final
     `jc.Step("summary", "<ok> ok, <failed> failed")` is recorded regardless.

If a `StartChild`/`Finish` store call itself errors, that child is treated as a
failure and surfaced in the aggregate (the migration may still have run, but we
could not record it — fail loud).

### Why this is deadlock-free

Each evacuate consumes exactly **one** pool worker (the parent). Children run as
goroutines *inside* the parent and need no pool worker. With a default pool of 4,
`K` concurrent evacuates use `K` workers and the rest queue — there is no path
where a waiting parent starves the children it is waiting on (the classic
parent-blocks-a-worker-pool deadlock that the alternative "enqueue children +
poll the shared pool" design risks).

## Observability

- Children are normal job rows, visible in `GET /jobs` already.
- Add `ParentID` to `store.JobFilter` and a `?parent_id=<id>` query param to
  `GET /jobs` so a client polling a `failed` parent can list its children and see
  which migrations failed and their per-child step trail. The parent's own
  `error` string also carries the per-child summary, so drill-down is a
  convenience, not a requirement.

## New surface (summary)

| Area | Addition |
|------|----------|
| `internal/store/store.go` | `SpecKey{Template, Slug}` type; `Store.ListSpecKeys(ctx, host) ([]SpecKey, error)` |
| `internal/store/jobs.go` | `JobStore.StartChild(ctx, kind, args, parentID) (Job, error)`; `JobFilter.ParentID` field |
| `internal/store/sqlite.go` | impl `ListSpecKeys` (`SELECT template, slug …`), `StartChild` (insert `running`), `parent_id` predicate in `ListJobs` |
| `internal/store/memory.go` | impl `ListSpecKeys`, `StartChild`, `parent_id` filter |
| `internal/instance/evacuate.go` | `EvacuateRequest`; `ResolveEvacuation`; `ErrInvalidEvacuation` |
| `internal/evacuate/handler.go` | `Handler{Svc, Jobs}`; `Run` (fan-out, bounded concurrency, child lifecycle, aggregate) |
| `internal/api/evacuate.go` | `POST /evacuate` handler |
| `internal/api/router.go` | route `POST /evacuate` (guard `instances:write`) |
| `internal/api/jobs.go` | `?parent_id=` query param → `JobFilter.ParentID` |
| `internal/api/errors.go` | `ErrInvalidEvacuation` → `400 invalid_request` |
| `cmd/podman-api/main.go` | register `"evacuate": &evacuate.Handler{Svc: svc, Jobs: db}` |
| `api/openapi.yaml` | `/evacuate` path, `EvacuateRequest` schema, `?parent_id` on `/jobs`, error code enum |

## Testing (TDD)

- **Store**: `ListSpecKeys` returns keys for a host and only that host, no
  secrets; `StartChild` inserts a `running` job with `parent_id` that `ClaimNext`
  does **not** claim; `ListJobs` honours `ParentID` filter.
- **ResolveEvacuation**: happy path returns sorted moves; unknown from_host →
  `ErrUnknownHost`; store disabled → `ErrStoreDisabled`; unmapped instance →
  `ErrInvalidEvacuation`; extra map key → `ErrInvalidEvacuation`; ambiguous slug
  → `ErrInvalidEvacuation`; dest == from_host → `ErrSameHost`; unknown dest →
  `ErrUnknownHost`; empty host + empty map → empty slice, no error.
- **Handler**: parent spawns N children with `parent_id` set; aggregate state =
  succeeded when all succeed. One child fails (fake migrate error) → siblings
  still run (assert all N children reached a terminal state) and parent returns a
  `failed` error naming the failed slug. Bounded concurrency: with
  `evacuateConcurrency = 1`, observe serialized execution (a counter/max-in-flight
  probe) — adversarial check that the bound is real.
- **API**: `POST /evacuate` with jobs disabled → `501`; malformed body → `400`;
  validation failure → `4xx` and **no** job enqueued; success → `202 { job_id }`
  and a parent job row exists. `GET /jobs?parent_id=<id>` returns only that
  parent's children.
- **OpenAPI**: `/evacuate` present in the spot-checked path list.

## Out of scope

- No daemon-side placement/scheduler — the client supplies the map (milestone
  decision).
- No automatic resume after a daemon restart — boot recovery marks an in-flight
  parent and its children `failed`; the client re-issues evacuate (the strict
  bijection means an already-moved instance is simply gone from `from_host`, so a
  re-issued evacuate naturally covers only what remains).
- No per-evacuate concurrency override in the request — `evacuateConcurrency` is a
  package default; making it configurable is a later concern (YAGNI).
- Application-readiness verification beyond `migrate`'s existing "all containers
  Running" gate (inherited limitation, documented in the migrate spec).
