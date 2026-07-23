# Background Inventory Poller — Design

**Date:** 2026-07-23
**Status:** Approved
**Repo:** OSS `github.com/iotready/podman-api` (UI, service, podman client, and
server wiring all live here; the commercial `podman-api-pro` repo consumes this
via a version bump only — no pro code change).
**Related:** `2026-07-22-ui-speed-and-caching-design.md` (introduced the lazy 3s
`instanceCache`, which this design builds on).

## Problem

The operator UI feels slow, worst on the host with the most instances
(engine-1). Two root causes:

1. **SSH chattiness.** Inspecting a host issues **one SSH round-trip per
   container** (`internal/podman/real.go` `PodList`/`PodInspect` call
   `containers.Inspect` per container). Cost scales with instance count, so
   engine-1 pays the most on every page load.

2. **Dead-host stall.** The UI dashboard fans out to every host with **no
   per-host timeout** (`internal/ui/handlers_hosts.go`), and the per-operation
   timeout is 10 minutes (`real.go` `callTimeout`). One unreachable host (e.g.
   engine-2 "connection refused") can drag the whole dashboard. Refresh errors
   are never cached, so every request re-attempts the dead host.

The existing `instanceCache` (`internal/instance/instancecache.go`) is a 3s,
**lazy**, pull-based cache over `ListAllInstances` only. Nothing proactively
refreshes it, its TTL is short, and it covers neither host status nor the
dead-host case.

## Goal

Make reads instant and resilient by keeping a **warm cache** filled by a
background poller, and let the UI **paint immediately and self-correct**
(progressive enhancement). Accept brief, clearly-labeled staleness in list views
(freshness contract "A": cache-always, self-correcting).

Non-goals: persisting observed state to disk; inventory history; changing the
desired-state store; optimizing instance-detail `Get` beyond progressive
enhancement.

## Freshness contract (decided)

- **Cache-always:** the request path serves whatever the poller last wrote and
  never blocks on live podman when a warm entry exists.
- **Cold miss only** (fresh boot, or immediately after a mutation invalidates the
  entry) does a single synchronous live fetch, guarded by the existing in-flight
  dedup.
- **Mutations invalidate** as they do today (`invalidateInstances`), so
  post-action views re-fetch fresh.
- **Staleness is visible:** the UI shows "updated Ns ago" and an "unreachable —
  last seen Ns ago" badge.

## Architecture

```
server.go RunWithFlags
  └─ inventory.Poller (new, started under runnerCtx)      every INVENTORY_REFRESH_INTERVAL
        └─ per-host goroutine, bounded by INVENTORY_REFRESH_TIMEOUT
              └─ Service.RefreshHost(ctx, host)           live fetch → cache put
                    └─ instanceCache.put(host, entry)     data + fetchedAt + reachable + lastErr

UI / JSON read path (existing callers unchanged)
  └─ Service.ListAllInstances(ctx, host)                  cache-always
  └─ Service.ListAllInstancesWithMeta(ctx, host)          new sibling → (data, Freshness)
        └─ cold miss → one synchronous live fetch (in-flight deduped)
```

### Component 1 — Cache model & freshness (`internal/instance/instancecache.go`)

Each per-host entry gains:

- `data []Observed` — last successful inventory.
- `fetchedAt time.Time` — when that data was captured.
- `reachable bool` + `lastErr error` — outcome of the most recent refresh.

Behavior:

- **`put(host, data, fetchedAt)`** — new method the poller uses to populate the
  cache proactively (today only `get` populates lazily). Sets `reachable=true`,
  clears `lastErr`.
- **Serve-last-known-good on error:** a new `putError(host, err)` keeps the
  previous `data`/`fetchedAt`, sets `reachable=false`, records `lastErr`. Reads
  return stale-but-real data plus freshness — never an empty error page, never a
  re-stall on the dead host.
- **Cache-always reads:** a warm entry is returned directly; no live fetch.
- **Cold miss** (no entry, or invalidated): fall back to the existing
  synchronous fetch with in-flight dedup, then populate.
- Existing `invalidateInstances` semantics unchanged.

A `Freshness` value `{FetchedAt time.Time, Reachable bool, Age() time.Duration}`
is returned alongside data so callers can render the cue.

### Component 2 — Service surface (`internal/instance/service.go`)

- **`RefreshHost(ctx, host) error`** — runs `listAllInstancesLive` and `put`s the
  result; on error calls `putError`. This is the only proactive cache populator.
  Returns the error for the poller to log, but the cache already holds
  last-known-good.
- **`ListAllInstancesWithMeta(ctx, host) ([]Observed, Freshness, error)`** — the
  meta-returning sibling of `ListAllInstances`. `ListAllInstances` keeps its
  signature and delegates (discarding meta) so existing callers are untouched.

**Scope of "keep warm" per host:** the instance list (`ListAllInstances`), which
backs the dashboard counts, the host page, and `HostCounts`. **Reachability comes
for free** from this refresh — a successful refresh sets `reachable=true`, a
failed one `reachable=false` — so the dashboard/host-page get liveness + freshness
without a second probe. Explicitly **out of scope for v1** (kept live-on-demand,
possible follow-on): host *load* (cpu/mem/loadavg via `HostLoad`), podman
`Version`, and instance-detail `Get` (single pod, cheap) — the latter handled by
UI progressive enhancement rather than the poller. This keeps the poller and
cache to a single data shape (`[]Observed`) with no second cache.

### Component 3 — Background poller (`internal/inventory`)

A `Poller` type started in `server.go` under `runnerCtx`, mirroring
`internal/prune/scheduler.go`:

- `wg.Add(1)`; goroutine with `time.NewTicker(interval)`.
- **Immediate first pass** on start so the cache is warm within one cycle of boot.
- `select` on `ctx.Done()` / `ticker.C`.
- **Panic-recover per tick** so one bad host can't kill the loop.
- `Wait()` for clean shutdown.
- Each tick loads the current host set (`hostsHolder.Load()`) and fans out **one
  goroutine per host**, each calling `Service.RefreshHost` under its **own
  per-host timeout** (`INVENTORY_REFRESH_TIMEOUT`, default 20s < the 30s cadence)
  so a hung host can't bleed into the next cycle.
- **Log once per state transition** (reachable↔unreachable) to avoid the log spam
  the ingress loop produces.
- `interval == 0` disables the poller entirely (escape hatch → today's lazy
  behavior).

### Component 4 — UI progressive enhancement (`internal/ui/`)

- **Instant paint from warm cache.** `/ui` (dashboard) and `/ui/hosts/{host}`
  render immediately from cache — no blocking on podman.
- **Self-correcting fragment.** The data region becomes an htmx fragment with
  `hx-trigger="every 10s"`; cache-only reads are cheap, so polling faster than
  the 30s server cadence makes the freshness cue tick and picks up each refresh
  promptly. It swaps itself in place.
- **Cold-cache skeleton.** If a host's entry is empty (just booted), render a
  lightweight skeleton/"loading…" state; the htmx poll fills it once the first
  refresh lands. First paint is never blocked on podman.
- **Freshness cue.** A subtle "updated Ns ago" line, and an "unreachable — last
  seen Ns ago" badge when `reachable=false`, fed by the `Freshness` struct.
- **Dashboard fan-out bound.** Add the per-host timeout the dashboard currently
  lacks (the JSON `/hosts` path already has 5s), relevant only on cold start
  since warm reads are instant.

### Component 5 — Config (env-only; OSS server owns the flag set)

| Env | Default | Meaning |
|---|---|---|
| `INVENTORY_REFRESH_INTERVAL` | `30s` | poll cadence per host; `0` disables the poller |
| `INVENTORY_REFRESH_TIMEOUT` | `20s` | per-host bound per refresh |

Both are Go durations, validated at startup (fail-fast, like the S3 duration
knobs). Parsed OSS-side; the pro repo remains env-only with no new flags.

## Error handling

- Dead host → `reachable=false`, keep last-known-good, log once per transition.
- Cold miss on a dead host (no prior data) → the existing error surface,
  unchanged (nothing to serve yet).
- Poller per-host panic → recovered and logged; loop continues.

## Testing (TDD)

- **Cache:** `put` stores data + freshness; `putError` preserves prior data and
  flips `reachable`; cold miss still fetches; in-flight dedup intact;
  `invalidate` still clears.
- **Poller:** one tick refreshes all hosts; a failing host neither blocks others
  nor panics the loop; immediate-first-pass warms the cache; `Wait()` returns on
  ctx cancel. Uses `internal/podman/fake`.
- **Service:** `ListAllInstancesWithMeta` returns warm data without hitting
  podman; returns stale data + `Reachable=false` after a failed refresh.
- **UI:** host page renders from a pre-warmed cache without a live call; renders
  the skeleton on cold cache; shows the unreachable badge. Follows the existing
  `render_test.go` / handler-test patterns.

## Rollout

1. Land in OSS behind the new env knobs (default-on at 30s).
2. Tag an OSS release.
3. `make bump V=<tag>` in podman-api-pro, build, deploy to engine-infra.
4. No pro code change; the two env knobs may optionally be set in the
   `podman-api` user service environment.
