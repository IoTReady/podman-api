# Host-health automation: scheduled podman prune + disk cleanup (#59)

**Date:** 2026-06-04
**Status:** Approved (brainstorm) — pending spec review
**Author:** Tej + Claude (brainstorm)
**Issue:** #59 · Part of the open-core PaaS roadmap (`2026-06-04-podman-paas-roadmap-design.md`, Phase 0)

## 1. Context

`podman-api` orchestrates stateful pods across one or more Podman hosts. Hosts already
in production accumulate dangling image layers, exited containers, build cache, and
unused volumes; left unattended a host fills its container-storage partition and deploys
start failing. Phase 0 of the roadmap pulls forward a **headless, scheduled cleanup**
capability that keeps hosts tidy on a safe policy. The UI surfaces it later (Slice 3);
this work is headless-first.

**Success:** hosts self-clean on a schedule and don't fill disks, and they **never** reap
in-use resources.

## 2. Goals / Non-goals

**Goals**
- A scheduled, per-host cleanup that runs `podman` prune operations on a safe policy.
- Configurable interval **and** disk high-water threshold (either fires a run).
- Per-host policy overrides over global flag defaults.
- Every run is recorded as a job (auditable, queryable via the jobs API, UI-ready).
- Dry-run mode for safe rollout.

**Non-goals (YAGNI)**
- No UI (Slice 3).
- No alerting/thresholds-as-alerts (commercial tier).
- No cross-host coordination — each host self-cleans independently.
- No log/metrics retention policy changes (separate concern).

## 3. Architecture

A new **`prune` job kind** executed by the existing `jobs.Runner`, fed by a **per-host
scheduler** goroutine. The whole feature is **gated on `-state-db`** (no store → no
scheduler), exactly like migrate/evacuate, because runs are persisted as jobs.

```
prune scheduler (ticks)
  └─ per host: due-by-interval (last prune from store) OR over-threshold (HostInfo.Disk)
               AND no in-flight prune/migrate/evacuate job for the host
       └─ enqueue { kind:"prune", host, policy snapshot }
              └─ jobs.Runner ─▶ prune.Handler
                     ├─ ImagePrune(dangling)         [default]
                     ├─ ImagePrune(all-unused)       [opt-in]
                     ├─ ContainerPrune (exited)       [opt-in]
                     ├─ BuildCachePrune               [opt-in]
                     └─ VolumePrune(protect-filter)   [opt-in]
                            └─ store: job + per-scope steps (reclaimed bytes)
                            └─ obs: prune_* metrics
                                   └─ jobs API / UI (Slice 3)
```

### 3.1 Why reuse the jobs runner
- Persisted job records give auditability, per-run progress/error, and a queryable history
  the Slice-3 UI reads for free.
- The runner's bounded worker pool, cancellation, and retention already exist.
- `jobs.Runner.StartRetention(ctx, interval)` is the precedent for a periodic background
  loop driven by a ticker.

## 4. Components

### 4.1 `podman.Client` prune methods (interface + `real` + `fake`)
New methods, each returning a `PruneReport`:

```go
// PruneReport summarizes one prune operation.
type PruneReport struct {
    Items     []string // ids/names removed (or, in dry-run, would-be-removed)
    Reclaimed int64    // bytes freed (or reclaimable, in dry-run)
}

ImagePrune(ctx context.Context, hostID string, all bool) (PruneReport, error)
ContainerPrune(ctx context.Context, hostID string) (PruneReport, error)
BuildCachePrune(ctx context.Context, hostID string) (PruneReport, error)
VolumePrune(ctx context.Context, hostID string, filters map[string][]string) (PruneReport, error)
```

- `ImagePrune(all=false)` removes dangling layers; `all=true` also removes tagged images
  unused by any container (libpod image prune `-a`).
- `VolumePrune` takes libpod filters; the handler passes a **label filter** so volumes
  bearing the protect label are skipped (see §6).
- The implementer **verifies the exact libpod v5 binding signatures** (`images.Prune`,
  `containers.Prune`, `volumes.Prune`, build-cache prune) against the vendored
  `podman/v5/pkg/bindings` before finalizing — names/return shapes here are the intent,
  not a guarantee.

The `fake` records each call (host, scope, filters) and returns canned reports so the
handler and scheduler are testable without a real host.

### 4.2 `PrunePolicy` (config)
Parsed from each `hosts/*.yaml` under a `prune:` key, merged over global flag defaults
(per-host fields that are unset inherit the global default):

```yaml
# hosts/web1.yaml
prune:
  enabled: true            # default: false (global -prune-enabled)
  interval: 12h            # default: 24h
  disk_threshold_pct: 70   # default: 85; 0 disables the threshold trigger
  scope: [dangling]        # default: [dangling]; others opt-in (see §5)
  dry_run: false           # default: false
```

A small `config` type + parse/merge function; lives alongside `internal/config/hosts.go`.
Unknown scope tokens are a **load-time error** (fail fast, consistent with hosts parsing).

### 4.3 Scheduler (`internal/prune/scheduler.go`)
- A single goroutine started from `main` (inside the `db != nil` block).
- Ticks on a base interval (e.g. 1m, ≤ the smallest meaningful granularity); on each tick,
  for every host with `enabled`:
  - **Interval gate:** `now - lastPrune(host) >= policy.interval`, where `lastPrune` is the
    completion time of the most recent terminal `prune` job for that host (read from the
    store; falls back to "never" → due). Reading from the store means a restart does **not**
    trigger an immediate prune storm and survives process restarts without new state.
  - **Threshold gate:** `HostInfo.Disk.Used% >= policy.disk_threshold_pct` (skip if
    threshold is 0). Disk info is fetched best-effort; a host whose info errors is skipped
    and logged, never blocks others.
  - **In-flight guard:** skip if the host already has a queued/running `prune` **or**
    `migrate`/`evacuate` job (closes the window where volume prune could reap a migration's
    temporarily-detached volume; also dedups overlapping ticks).
  - If a gate fires and the guard is clear → enqueue a `prune` job whose payload carries a
    **snapshot of the resolved policy** (so a mid-flight config reload can't change a
    running job's behavior).
- Clock and store are injected (interfaces) so the scheduler is unit-testable without real
  time or a real DB.

### 4.4 Prune handler (`internal/prune/handler.go`)
- Implements `jobs.Handler` for kind `"prune"`.
- Reads the policy snapshot from the job payload; executes the enabled scopes in a fixed,
  safe order (images → containers → build cache → volumes), recording one `jc.Step` per
  scope with the reclaimed byte count and item count.
- **Honors `ctx` cancellation** between scopes (and relies on binding ctx for in-flight
  calls) so shutdown/`StartRetention`-style cancellation is clean.
- **Dry-run:** instead of removing, reports reclaimable bytes (via `system df` /
  binding dry-run where available) and records steps prefixed `dry-run:` — removes nothing.
- A scope error is recorded as a failed step; the handler continues remaining scopes and
  returns a non-nil error at the end if any scope failed (partial-progress is preserved in
  steps, the job is marked failed so it's visible).

### 4.5 Metrics (`internal/obs`)
New instruments (Prometheus now, OTel-shaped per the observability seam):
- `prune_runs_total{host,result}` counter
- `prune_reclaimed_bytes_total{host,scope}` counter
- `prune_failures_total{host,scope}` counter
- `prune_last_run_timestamp{host}` gauge

### 4.6 Wiring (`cmd/podman-api/main.go`)
- New flags (global defaults):
  - `-prune-enabled` (bool, default `false`)
  - `-prune-interval` (duration, default `24h`)
  - `-prune-disk-threshold` (int 0–100, default `85`; 0 disables the threshold gate)
  - `-prune-scope` (comma list, default `dangling`)
  - `-prune-dry-run` (bool, default `false`)
- Register the `"prune"` handler in the existing `jobs.Registry`.
- Start the scheduler inside the `db != nil` block, after `runner.Start`, cancelled by the
  same `runnerCtx`.

## 5. Cleanup scope (defaults vs opt-in)

| Scope token   | Operation                                  | Default |
|---------------|--------------------------------------------|---------|
| `dangling`    | `ImagePrune(all=false)` — dangling layers  | **ON**  |
| `all-images`  | `ImagePrune(all=true)` — unused tagged imgs| opt-in  |
| `containers`  | `ContainerPrune` — exited containers       | opt-in  |
| `build-cache` | `BuildCachePrune`                          | opt-in  |
| `volumes`     | `VolumePrune` (protect-filtered)           | opt-in  |

All operations rely on podman prune's **safe-by-default** semantics (only
dangling/unused) and **never force-remove**. Only `dangling` runs unless an operator opts
into more.

## 6. Safety (the "never touch in-use resources" mandate)

1. **Safe-by-default podman semantics:** prune only removes dangling/unused objects; no
   `force`. A running pod's images/volumes/containers are never candidates.
2. **In-flight guard:** the scheduler will not enqueue a prune for a host with an active
   `prune`/`migrate`/`evacuate` job — the key mitigation against reaping a migration's
   transiently-detached volume.
3. **Volume protect-label:** `volumes` scope passes a label filter so volumes carrying the
   managed protect label (e.g. `podman-api.protect=true`, exact key TBD-in-plan against the
   labels the service already stamps) are excluded. Volumes are opt-in regardless.
4. **Dry-run:** lets operators observe what a policy would reclaim before enabling removal.
5. **Opt-in feature:** disabled by default — existing hosts don't start auto-deleting on
   upgrade; the operator turns it on deliberately.

## 7. Error handling

- Host info / connectivity error on a tick → that host is skipped and logged; other hosts
  proceed; no job enqueued.
- Enqueue failure → logged, retried next tick (idempotent: the in-flight guard prevents
  duplicates once one is queued).
- Per-scope prune error → recorded as a failed step; remaining scopes still run; job ends
  failed. The job record + steps make the partial outcome auditable.
- Scheduler goroutine panics are contained (recover + log) so one bad tick never kills the
  loop.

## 8. Testing

- **`podman.fake`:** record prune calls (host/scope/filters), return canned `PruneReport`s,
  injectable errors per scope.
- **Handler tests:** scope ordering; only-enabled-scopes-run; per-scope step + reclaimed
  bytes recorded; dry-run removes nothing and reports reclaimable; ctx-cancel stops between
  scopes; one scope failing → job failed but others ran.
- **Scheduler tests (injected clock + fake store):** interval-due fires; not-yet-due skips;
  threshold-trigger fires below interval; threshold=0 disables; in-flight prune/migrate/
  evacuate suppresses enqueue; disabled host skipped; host-info error skips only that host;
  no-prune-storm-on-restart (lastPrune read from store).
- **Config tests:** yaml `prune:` parse; per-host override × global-flag-default merge;
  unknown scope token errors at load.
- All via `make test` (remote-client build tags). `gofmt -l .` empty, `go vet` clean.

## 9. Rollout

Ship headless. Operators enable per host (or globally) once they've watched a `dry_run`
run report sane reclaimable numbers. Slice 3 adds the UI surface over the existing job
records and metrics — no schema change needed then.
