# Evacuate dry-run / plan preview — design

**Issue:** #54 (migrate/evacuate hardening umbrella) — "No dry-run / plan preview
for evacuate" sub-item.

**Status:** approved design.

## Goal

Let an operator ask *"what would this evacuate do, and would it actually
succeed?"* without enqueuing a job or mutating anything. The preview resolves the
evacuate map into a per-instance move list (the static plan) **and** runs the
live destination preflight checks per move, reporting — for every instance —
whether the destination would currently accept it.

This closes the gap where a misconfigured destination (host port already taken,
a per-host secret not yet seeded, a draining target, an instance already present)
is invisible until the evacuate is running and fails mid-job.

## Scope

- **Evacuate only.** A single `migrate` already fast-fails synchronously via
  `CheckMigratable`, and one instance's live-preflight failure surfaces quickly
  as a job failure, so the multi-instance "know before you commit" payoff is an
  evacuate concern. A migrate preview is out of scope.
- **No new state.** The preview enqueues no job and writes nothing. There is no
  job row to poll, prune, or cancel.
- **Read-only.** `PlanEvacuation` only reads stored specs and inspects hosts.

## Architecture & data flow

```
POST /evacuate/plan   (scope: instances:read)
        │  body: {from_host, map}        (same shape as POST /evacuate; "concurrency" ignored)
        ▼
  handlers.evacuatePlan
        │  svc.PlanEvacuation(ctx, req)
        ▼
  instance.Service.PlanEvacuation
        │  1. ResolveEvacuation(req)  ── static plan + map validation
        │        └─ bad map → ErrInvalidEvacuation / ErrUnknownHost / ErrStoreDisabled / ErrSameHost → 4xx (no body plan)
        │  2. for each resolved move (sorted by slug):
        │        load spec + template + eff params
        │        run the shared preflight predicates, COLLECTING all issues
        │        → PlannedMove{ok, issues[]}
        ▼
  EvacuationPlan{from_host, moves[]}  → 200 JSON
```

**Core invariant:** a request that yields a `200` plan is exactly a request the
real `POST /evacuate` would accept — identical static validation, identical 4xx
mapping. Once the map fully resolves, the `200` body reports, per instance,
whether the *live* destination state would let the move through.

## Reusing the preflight checks (no drift)

Today `Service.preflightDest` is **fail-fast**: it returns on the first blocking
condition. A preview needs *all* problems for *every* move, so the check logic is
refactored into small, individually-callable predicates that **both** paths
compose:

- The fail-fast `Migrate` execution path keeps returning the first error (order
  and behavior preserved).
- The new collect-all plan path runs every predicate and gathers issues.

This keeps the preview and the executor provably in sync — a preview that lies is
worse than no preview. The four checks extracted from `preflightDest`:

1. **destination draining** — `hostCfg.Drain`.
2. **instance already exists** — `PodInspect(dest, podName)` finds a pod.
3. **per-host secret missing** — `SecretInspect(dest, name)` → `ErrNotFound` for
   any `tmpl.Meta.Secrets.PerHostReferenced`.
4. **host-port conflict** — required host ports (rendered from the template with
   effective params) intersect `PortsInUse(dest)`.

## Report schema

New types in `internal/instance`:

```go
// EvacuationPlan is the result of PlanEvacuation: the resolved per-instance
// moves plus, for each, whether the destination would currently accept it.
type EvacuationPlan struct {
    FromHost string
    Moves    []PlannedMove
}

type PlannedMove struct {
    Slug     string
    Template string
    ToHost   string
    OK       bool        // true iff Issues is empty
    Issues   []PlanIssue
}

type PlanIssue struct {
    Code    string // stable machine code (see table)
    Message string // human detail (the conflicting port, the secret name, ...)
}
```

### Issue codes

| Code | Source check | Meaning |
|---|---|---|
| `destination_draining` | `hostCfg.Drain` | dest host is draining; evacuate would refuse it |
| `instance_exists` | `PodInspect` finds a pod | an instance with this name is already on the dest |
| `host_secret_missing` | `SecretInspect` → NotFound | a required per-host secret isn't seeded on the dest |
| `port_conflict` | `PortsInUse` ∩ required | a host port the pod binds is already taken |
| `check_error` | any inspect call errors for a non-blocking reason (host unreachable, decrypt fail, render error, ...) | the check was **inconclusive** — the preview cannot vouch for this move |

`check_error` is deliberately distinct from a blocking issue: an unreachable
destination makes a move *inconclusive*, not *known-bad*. The operator sees the
preview could not vouch for it rather than a false "all clear."

### JSON response

```json
{
  "from_host": "node-a",
  "moves": [
    {"slug": "acme", "template": "postgres", "to_host": "node-b", "ok": true, "issues": []},
    {"slug": "globex", "template": "redis", "to_host": "node-c", "ok": false,
     "issues": [
       {"code": "port_conflict", "message": "host port 6379 already in use on node-c"},
       {"code": "host_secret_missing", "message": "required host secret missing: redis-auth"}
     ]}
  ]
}
```

Moves are sorted by slug (deterministic, matching `ResolveEvacuation`). There is
**no** top-level summary/count field — the client can trivially count
`ok == false` moves, and a redundant summary invites drift. (Easy to add later if
requested.)

## Error semantics

### Request-level failures → 4xx/5xx, no plan body

These mean a plan cannot even be produced; they mirror the real `POST /evacuate`
exactly so the two endpoints stay consistent.

| Condition | Status | Code |
|---|---|---|
| store disabled (`h.jobs == nil` / `ErrStoreDisabled`) | `501` | `jobs_disabled` (reuses `errJobsDisabled`) |
| malformed JSON body | `400` | `invalid_body` |
| unknown `from_host` (`ErrUnknownHost`) | `404` | (existing mapping) |
| bad map — unmapped/extra/ambiguous slug, unknown dest, same-host (`ErrInvalidEvacuation` / `ErrSameHost`) | `400` | `invalid_request` |

If the real evacuate would 4xx, the plan 4xxes identically. Only once the map
fully resolves does the endpoint return `200`.

### Move-level conditions → 200 body, never an HTTP error

- Blocking conditions (draining, exists, secret-missing, port-conflict) →
  `ok:false` + issue(s).
- Inconclusive checks (host unreachable, `GetSpec` decrypt failure, render or
  `PortsInUse` error) → `ok:false` + a `check_error` issue. **One unreachable
  destination does not blank the whole preview** — every other move is still
  reported.

### Concurrency

The live checks run **sequentially**, in slug-sorted order (deterministic). Each
move's preflight is a handful of fast inspect calls and an evacuate is typically a
modest instance count. Bounded-concurrent probing is a possible later
optimization, explicitly out of scope for v1. The request body's `concurrency`
field (which only affects the real fan-out) is **ignored** by the plan, consistent
with `ResolveEvacuation`.

## API

- **Route:** `POST /evacuate/plan`, scope `instances:read` (read-only — a key
  without write scope can preview).
- **Request body:** identical to `POST /evacuate` (`from_host`, `map`,
  `concurrency` ignored).
- **Responses:** `200` `EvacuationPlan`; `400` invalid body / bad map; `404`
  unknown `from_host`; `501` store disabled.
- **OpenAPI:** add the path and the `EvacuationPlan` / `PlannedMove` / `PlanIssue`
  schemas; document the issue-code set.

## Testing strategy

All tests use the existing `internal/podman/fake` and in-memory store — no real
podman host (so `make test` covers it; integration suite unaffected).

**1. Service — `internal/instance/evacuate_plan_test.go` (bulk):**
- All moves clean → every `PlannedMove.OK == true`, no issues.
- Each blocking condition in isolation → its exact issue code: draining dest,
  instance exists, missing per-host secret, port conflict.
- **Multiple issues on one move** (port conflict *and* missing secret) — proves
  collect-all, not fail-fast.
- **Mixed plan** — some ok, some blocked — all reported, sorted by slug.
- **`check_error`** — a destination inspect returns a non-NotFound error →
  `ok:false` + `check_error`, and other moves still reported.
- Map-level failures still return the sentinel errors (`ErrInvalidEvacuation`,
  `ErrUnknownHost`, `ErrSameHost`, `ErrStoreDisabled`) — i.e. `PlanEvacuation`
  defers to `ResolveEvacuation` for static validation.

**2. Shared-predicate refactor regression:** existing `migrate` /
`preflightDest` tests stay green (fail-fast order preserved) — the guard that the
extraction did not change execution behavior.

**3. API — extend `internal/api/evacuate_test.go`:**
- `501` store disabled; `400` invalid body / bad map; `404` unknown host.
- `200` happy path → response JSON shape matches schema, moves sorted.
- `200` with a blocked move → correct `ok:false` + issue codes in the body.
- Scope guard: route requires `instances:read` (a key lacking it is rejected; a
  read-only key suffices).

**4. OpenAPI — extend `internal/api/openapi_test.go`:** add `/evacuate/plan` to
the path spot-check slice.

## Documentation

Wiki `Operating.md`: add a "Previewing an evacuate" subsection under
"Migrating & evacuating instances" — the verb, the read scope, the issue-code
meanings, and the "200 plan == a request the real evacuate would accept"
guarantee. No `Deploying.md` change (no new flag).

## Out of scope / future

- Migrate preview (single move; already fast-fails synchronously).
- Bounded-concurrent live probing.
- Readiness / volume-size cost estimates (heavier source inspection).
- A top-level summary/count field in the response.
