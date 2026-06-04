# Phase 5: migrate primitive — design

**Date:** 2026-06-03
**Status:** Approved (brainstorm)
**Tracking:** Forgejo #34 (part of milestone #29).
**Umbrella:** `docs/superpowers/specs/2026-06-03-migrate-evacuate-design.md`
**Builds on:** #31 state store (PR #40), #32 jobs runner (PR #43), #33 volume cold-copy (PR #46) — all merged.

## Goal

Move a running instance from one host to another with its data, as a single
asynchronous job. `POST /migrate` enqueues the work and returns `202 {job_id}`;
the job loads the stored spec, copies the instance's volumes host-to-host,
re-applies the spec on the destination (decrypting secrets from the store),
verifies the destination is healthy, and only then deletes the source. Any
failure before the destination verifies rolls back — the source is restarted and
the half-built destination is reaped — so **the source is the source of truth
until the destination is proven healthy**.

This is the milestone's first real job kind: it registers a handler into the
runner's (previously empty) `Registry` and is the first writer of jobs via an
HTTP endpoint.

## Decisions locked in this brainstorm

| Decision | Choice |
| --- | --- |
| Orchestration placement | **`Service.Migrate(ctx, req, step func(step, detail string)) error`** in `internal/instance` — reuses render/secrets/`Apply`/`Delete`/`CopyVolume`/`Start`/`Stop`/`PortsInUse` and the existing fake+Memory test harness. A thin `internal/migrate.Handler` adapts it to `jobs.Handler`. The `step` callback keeps `instance` free of a `jobs` import. |
| Missing spec | **Require the spec in the store → `404`**. Adopting un-stored legacy instances is out of scope (never-stored secrets can't migrate faithfully); operator re-Applies via the stateful path first, then migrates. |
| Validation boundary | **Hybrid.** `POST` synchronously validates request shape, known hosts/template, `from != to`, and **spec-exists (→404)**, then enqueues. Dest preflight (ports/secrets/drain/no-clobber) plus all mutations and rollback run **in the job** (surfaced as job state/steps). |
| Dest volume creation | **New `podman.Client.VolumeCreate(ctx, hostID, name)`** — `CopyVolume` (#33) needs the dest volume to pre-exist, and copy must precede `Apply` so `play kube` reuses the volumes. This is the dest-creation responsibility #33 deferred to #34. |
| Job args | **Plaintext** `{from_host,to_host,template,slug,parameters}` — **no secrets**. The handler re-reads decrypted secrets from the store via `GetSpec` (consistent with #32: secrets live only in the encrypted `specs` table). |
| Endpoint scope | **`instances:write`** (migrate is an instance mutation). No new scope; `GET /jobs` stays `jobs:read`. |
| Store host move | **No new store method.** `Apply(dest)` `PutSpec`s the dest row; `Delete(source)` `DeleteSpec`s the old row. Net effect: the spec ends up only at the destination. |
| Parameters override | Effective params = **`merge(storedSpec.Parameters, req.Parameters)`** (request wins) — e.g. remap a host port that's taken on the destination. Used for both the port preflight and the dest `Apply`. |

## Architecture

```
POST /migrate ─► api.handlers.migrate
                   │  validate (shape, hosts, template, from!=to, GetSpec exists→404)
                   │  jobs.Enqueue("migrate", argsJSON, "")   (runner polls ≤5s)
                   └─► 202 {job_id}

runner (Registry{"migrate": migrate.Handler}) drains the queue
   └─► migrate.Handler.Run(ctx, job, jc)
          unmarshal job.Args ─► instance.MigrateRequest
          svc.Migrate(ctx, req, jc.Step)
```

`Service.Migrate` is the unit under test (fake podman client + `Memory` store).
The `migrate.Handler` is a ~15-line adapter. The API handler is a thin
validate-enqueue-notify shell.

### New/changed components

| Component | File | Responsibility |
| --- | --- | --- |
| `VolumeCreate` | `internal/podman/client.go`, `real.go`, `fake/fake.go` | create an empty named volume on a host (real via `volumes.Create` binding; fake marks it present) |
| `MigrateRequest` | `internal/instance/migrate.go` (new) | `{FromHost,ToHost,Template,Slug string; Parameters map[string]any}` (json `from_host,to_host,template,slug,parameters`) |
| `Service.Migrate` | `internal/instance/migrate.go` (new) | the full algorithm below, with a best-effort `step` progress callback |
| `ErrPortConflict` | `internal/instance/service.go` (errors block) | dest host-port already in use |
| `migrate.Handler` | `internal/migrate/handler.go` (new) | `jobs.Handler` adapter: unmarshal args → `svc.Migrate(…, jc.Step)` |
| `migrate` API handler | `internal/api/migrate.go` (new) + route in `router.go` | `POST /migrate`, synchronous validation, enqueue, 202 |
| handler registration | `cmd/podman-api/main.go` | register the `migrate` handler in the `Registry` before `runner.Start` |

## API surface

`POST /migrate` (scope `instances:write`)

```json
{ "from_host": "h1", "to_host": "h2", "template": "postgres", "slug": "db1",
  "parameters": { "host_port": 5544 } }
```

Synchronous responses:

| Condition | Status | Code |
| --- | --- | --- |
| malformed JSON | 400 | `invalid_body` |
| `from_host == to_host` | 400 | `invalid_request` |
| unknown host (`from` or `to`) | 404 | `unknown_host` |
| unknown template | 404 | `unknown_template` |
| spec absent in store | 404 | `not_found` |
| store disabled (no `-state-db`) | 501 | `not_implemented` |
| accepted | 202 | body `{ "job_id": "<id>" }` |

On accept: `job, _ := jobs.Enqueue(ctx, "migrate", args, "")`;
`WriteJSON(202, {job_id: job.ID})`. The runner picks the job up on its next 5s
poll — fine for an operation that takes seconds-to-minutes. An explicit
`runner.Notify()` would shave that latency but is **deferred**: it would mean
threading the notifier through `NewRouter`'s ~10 call sites for no correctness
gain. Noted as a trivial future optimisation.

`migrate` requires the store (it reads specs); like the jobs endpoints it
returns `501` when the store is disabled — the API handler holds `jobs` (nil when
disabled) and the `Service` holds `store` (nil when disabled); guard on both.

## Migrate algorithm (`Service.Migrate`)

Effective params: `eff = merge(spec.Parameters, req.Parameters)` (request wins).

**Locking.** `Migrate` takes a single **migrate-scoped** lock keyed on the
instance identity only, via a sentinel "host" that no real host id collides with
(`s.instanceLock("\x00migrate", tmpl, slug)`). It deliberately does **not** take
the per-host instance locks, because `Apply`/`Delete`/`Start`/`Stop` each acquire
`instanceLock(host,tmpl,slug)` internally and Go's `sync.Mutex` is non-reentrant —
holding it here would self-deadlock. The migrate-scoped lock serialises two
migrates of the *same* instance (so the loser sees the source already gone and
fails cleanly in preflight, rather than its rollback reaping the winner's
freshly-built destination); the sub-operations' own per-host locks still guard
against concurrent plain `Apply`/`Delete` on either endpoint.

1. **Load** — `spec, err := store.GetSpec(from, tmpl, slug)`; on `ErrNotFound`
   return it (already 404'd at POST; this guards a race). `step("load", …)`.
2. **Preflight dest** — all fail-fast, *before* touching the source:
   - dest host known and **not draining** → else `ErrHostDraining`.
   - **no existing instance** for `(tmpl,slug)` on dest (`PodInspect` must be
     `ErrNotFound`) → else `ErrInstanceExists` (rollback would otherwise clobber
     a real instance).
   - **per-host secrets present** on dest: for each `Meta.Secrets.PerHostReferenced`,
     `client.SecretInspect(to, name)` → else `ErrHostSecretMissing`.
   - **required host ports free**: `render(template, eff)`, parse
     `spec.containers[].ports[].hostPort`, diff against `PortsInUse(to)` → else
     `ErrPortConflict`.

   `step("preflight", …)`.
3. **Stop source** — `Stop(from, tmpl, slug)` to quiesce (not remove).
   `step("stop-source", …)`. From here, failures roll back.
4. **Copy volumes** — `vols := InstanceVolumes(from, tmpl, slug)`; for each:
   `client.VolumeCreate(to, vol.Name)` then `CopyVolume(from, to, vol.Name)`.
   `step("copy-volume", name)` per volume.
5. **Apply dest** — `Apply(to, ApplyRequest{Template:tmpl, Slug:slug,
   Parameters:eff, Secrets:spec.Secrets}, ApplyOptions{Replace:false})`.
   `play kube` reuses the pre-copied named volumes; Apply pushes per-instance
   secrets and `PutSpec(to,…)`. `step("apply-dest", …)`.
6. **Verify** — poll `PodInspect(to, tmpl, slug)` until status `Running` or
   `verifyTimeout` (consts: `verifyTimeout = 60s`, `verifyInterval = 2s`).
   Timeout/unhealthy → rollback. `step("verify", …)`.
7. **Commit** — `Delete(from, tmpl, slug, DeleteOptions{PruneVolumes:true,
   PruneSecrets:true})` removes source pod + volumes + per-instance secrets and
   `DeleteSpec(from,…)`. `step("commit", …)`. Done.

### Rollback (failure in steps 4–6)

`step("rollback", reason)`, then:
1. `Start(from, tmpl, slug)` — bring the source pod back up.
2. `Delete(to, tmpl, slug, DeleteOptions{PruneVolumes:true, PruneSecrets:true})`
   — reap the half-built dest: pod, the volumes we created/copied, per-instance
   secrets, and the dest spec if `Apply` wrote it.

Per-host secrets on the destination are **never** created or deleted by migrate
(preflight only checks they exist). The source's spec is only removed in step 7,
so a rollback leaves the store consistent (spec still at source). Rollback errors
are appended as steps; the job's terminal error is the original failure.

On daemon crash mid-job the row is left `running` and reaped to `failed` on boot
(existing #32 behaviour); migrate is **not** auto-resumed — the source is
untouched until step 6, so the operator re-issues it. (If a crash happens between
steps 3 and 7, the source pod may be left stopped; re-issuing migrate restarts it
on the happy path's commit, or the operator starts it. This is acceptable for v1
and noted as a known edge.)

## VolumeCreate primitive

`Client.VolumeCreate(ctx, hostID, name string) error`

- **Real:** `volumes.Create(c, types.VolumeCreateOptions{Name: name}, nil)` over
  the cached connection context (same pattern as the #33 integration test).
  Creating an already-existing volume: podman returns success/no-op for an
  existing name in practice; treat an "exists" error as success so step 4 is
  idempotent on re-issue. Other errors propagate.
- **Fake:** marks the volume present (`hostVolumes[host][name]`), idempotent.

## Errors and status mapping

New: `instance.ErrPortConflict` → `409 / port_conflict` (added to the API
`classify` switch). Reused: `ErrHostDraining` (423), `ErrHostSecretMissing` (422),
`ErrInstanceExists` (409), `ErrUnknownHost`/`ErrUnknownTemplate`/`store.ErrNotFound`
(404). Inside the job these become the job's `error` string and `failed` state;
synchronously at POST they become HTTP statuses.

## Testing (TDD)

Unit (pure-Go: fake podman client + `Memory` store, the existing instance
harness; no build tags):

1. **Happy path.** Seed spec + a volume with bytes at source. `Migrate`. Assert:
   source pod/volumes/per-instance-secrets gone and `GetSpec(from)` →
   `ErrNotFound`; dest pod `Running`, dest volume has the copied bytes,
   `GetSpec(to)` present; the `step` callback recorded the expected sequence.
2. **Rollback at each mutation point.** Inject failure via fake hooks at copy,
   apply, and verify; assert source pod restarted and intact (`GetSpec(from)`
   present, pod `Running`), dest fully reaped (no pod, no volumes, no dest spec),
   and `Migrate` returns the injected error.
3. **Preflight fail-fast (source untouched).** dest port conflict →
   `ErrPortConflict`; missing per-host secret → `ErrHostSecretMissing`; dest
   already has the instance → `ErrInstanceExists`; dest draining →
   `ErrHostDraining`. Each asserts the source pod was never stopped.
4. **API handler.** missing spec → 404; `from==to` → 400; unknown host → 404;
   store disabled → 501; happy POST → 202 with a `job_id` and a `migrate` job
   enqueued (assert via the `JobStore`); `notify` invoked.
5. **VolumeCreate (fake)** — creates/idempotent; **(real, `integration` tag)** —
   extend the #33 volume integration test: `VolumeCreate` then export/import.

Integration (real podman, `integration` tag): covered by the `VolumeCreate`
addition; the full migrate path is exercised at the unit level against the fake
(a live two-host migrate integration test is out of scope for this phase — the
CI runner is single-host).

## Out of scope (deferred)

- **Legacy adoption** of un-stored instances (require spec → 404).
- **`CopyVolume` stream timeout / cancellation** and **source-vs-dest error-locus**
  distinction — the two follow-ups from #33; migrate runs under the job with no
  per-stream deadline yet.
- **Evacuate** (#35) — builds on migrate; `parent_id` child jobs land there.
- **Auto-resume** of a crashed migrate (reaped to `failed`; operator re-issues).
- **Live two-host migrate integration test** (single-host CI).
- **Application-readiness verification.** Verify is a *liveness* gate — it waits
  until the destination pod and all its containers report `Running`, then commits
  by destroying the source (pod + volumes + per-instance secrets). It does not
  probe in-container application health, so a process that is up but not yet
  serving is treated as healthy. It also assumes templates have no init/one-shot
  containers (which would legitimately report non-Running); the current templates
  are single app-container. A per-template readiness probe before the
  irreversible source delete is a follow-up.

## Files touched

| File | Change |
| --- | --- |
| `internal/podman/client.go` | `VolumeCreate` on the `Client` interface |
| `internal/podman/real.go` | real `VolumeCreate` via `volumes.Create` |
| `internal/podman/fake/fake.go` | fake `VolumeCreate` (+ idempotent), hooks for migrate failure injection if needed |
| `internal/instance/migrate.go` | new: `MigrateRequest`, `Service.Migrate`, port-extraction helper, verify-poll consts |
| `internal/instance/service.go` | `ErrPortConflict` |
| `internal/migrate/handler.go` | new: `jobs.Handler` adapter |
| `internal/api/migrate.go` | new: `POST /migrate` handler |
| `internal/api/router.go` | add the `POST /migrate` route |
| `internal/api/errors.go` | `ErrPortConflict` → 409, `ErrSameHost` → 400 |
| `cmd/podman-api/main.go` | register `migrate` handler in `Registry` before `runner.Start` |
| `internal/instance/migrate_test.go`, `internal/api/migrate_test.go`, `internal/podman/fake` + integration tests | tests above |
