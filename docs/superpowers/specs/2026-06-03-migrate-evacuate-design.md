# Migrate / Evacuate: stateful controller design

**Date:** 2026-06-03
**Status:** Approved (brainstorm) — umbrella design + Phase 1 spec
**Tracking:** to be filed on Forgejo (milestone)

## Problem

The CMS that drives podman-api currently has to orchestrate cross-host moves by
hand: flip `drain` on the source host, re-`Apply` each instance on a target host,
poll until healthy, then `Delete` on the source — and unwind partial failures
itself. We want `migrate` and `evacuate` as **server-side primitives** so the
client gets simpler. Two facts make this non-trivial today:

1. **Secrets are write-only.** `podman.Secret` carries only `{Name, CreatedAt}`;
   the daemon writes per-instance secrets to a host and zeroes its copy. It
   cannot read them back.
2. **The daemon is stateless about specs.** A running instance is just a labelled
   Pod (`podman-api/template`, `podman-api/slug`). The original `parameters` and
   `secrets` are not persisted anywhere recoverable.

You cannot move what you cannot describe. So `migrate`/`evacuate` require a
durable record of desired state.

## Decision summary (from brainstorm)

| Decision | Choice |
| --- | --- |
| Spec source at migrate time | **Daemon persists specs** in a store |
| Store technology | **SQLite** (embedded; matches single-daemon deploy) |
| Secrets in store | **Encrypted at rest** (AES-256-GCM, daemon key) → name-only migrate |
| Volume data | **Cold copy** (stop, export\|import, start) |
| Execution model | **Async jobs + poll** (`jobs` table, background runner) |
| Evacuate placement | **Client supplies `slug→host` map** (placement is client policy) |
| Store activation | **Opt-in** via `-state-db`; off = today's stateless behavior |
| Host load API | **New**: expose hosts + container count + load for client placement |

## Architecture shift

podman-api goes from **stateless proxy** to **stateful controller**. The
`instance.Service` gains a second collaborator beside the podman `Client`:

- **`podman.Client`** — the *actuator* (does things to hosts). Unchanged in role.
- **`Store`** — the *desired-state record* (SQLite behind a `Store` interface). New.

The existing layering (`api → instance.Service → podman.Client`) is preserved;
the store is a second dependency of the Service, not a new tier or service.

**Opt-in.** A `-state-db <path>` flag enables the store. Without it the daemon
behaves exactly as today, and `POST /migrate`, `POST /evacuate`, and `GET /jobs`
return `501 Not Implemented`. Users who don't need migration carry no new state,
key management, or attack surface.

## Data model (SQLite)

### `specs` — desired state
Written on `Apply`, removed on `Delete`. One row per instance.

| column | notes |
| --- | --- |
| `host`, `template`, `slug` | composite primary key |
| `parameters` | JSON |
| `secrets` | AES-256-GCM ciphertext blob (nonce-prefixed) |
| `created`, `updated` | timestamps |

Secret values are encrypted with a daemon key loaded from `-spec-key-file`
(32 bytes, base64 or raw), read once at startup. The daemon **refuses to start**
if `-state-db` is set without a readable key. (There is no SIGHUP key hot-reload
— see the Phase 2 design and issue #41: changing the key can't re-encrypt
existing rows, so a runtime swap would silently break them.)

### `jobs` — async operations
| column | notes |
| --- | --- |
| `id` | sortable, generated server-side (crypto/rand-backed, time-prefixed) |
| `kind` | `migrate` \| `evacuate` |
| `args` | JSON (request body) |
| `state` | `queued` \| `running` \| `succeeded` \| `failed` |
| `steps` | JSON array of `{ts, step, detail}` progress log |
| `parent_id` | set for child migrate jobs spawned by an evacuate |
| `error` | failure message |
| `started`, `finished` | timestamps |

On daemon boot, jobs left in `running` (process crashed mid-op) are marked
`failed` with a "interrupted by restart" reason — we do not auto-resume a
partially-applied migration; the operator re-issues it (the migrate algorithm is
idempotent-safe because the source is untouched until dest verifies).

### Threat-model note
Enabling the store means the daemon host now holds **encrypted** tenant secrets.
DB compromise **and** key compromise together = leak; today neither exists. This
is the explicit cost of name-only migrate. Mitigations: key file `0600`, separate
from the DB; key never logged; consider a future KMS/age backend behind the same
seam.

## New mechanisms

### Volume cold-copy
Two new `podman.Client` methods:

```go
VolumeExport(ctx context.Context, host, name string) (io.ReadCloser, error)
VolumeImport(ctx context.Context, host, name string, r io.Reader) error
```

The daemon streams source→dest through an `io.Pipe`, so volume data transits the
daemon host's **network** (two SSH conns) but not its disk. Wired in `real.go`
(libpod) and `fake`.

### Job runner
A background worker drains the `jobs` queue, executes each job's algorithm, and
writes `steps`/`state` updates to the row. Bounded concurrency. One runner per
daemon.

## API surface

| Method | Path | Body | Result |
| --- | --- | --- | --- |
| GET | `/hosts` | — | hosts + drain + counts + load (Phase 1) |
| GET | `/hosts/{id}` | — | one host, same shape |
| POST | `/migrate` | `{from_host, to_host, template, slug, parameters?}` | `202 {job_id}` |
| POST | `/evacuate` | `{from_host, map:{slug:to_host,...}}` | `202 {job_id}` (parent) |
| GET | `/jobs/{id}` | — | job state + steps |
| GET | `/jobs` | `?state=&kind=` | list |

`migrate`'s optional `parameters` override merges onto the stored spec — e.g. to
remap a host port for the new home if the original is taken on the dest.

## Migrate algorithm (one job)

1. Load spec from store. `404` if absent (see *Legacy adoption*).
2. Pre-check dest: required host ports free, per-host secrets present, host not
   draining. Fail fast otherwise.
3. **Stop** source pod (quiesce data).
4. For each named volume: `export(src) | import(dst)` streamed through the daemon.
5. **Apply** spec on dest — reuses copied volumes; pushes secrets decrypted from
   the store.
6. **Verify** dest pod healthy (poll `PodInspect` until Running, bounded).
7. **Delete** source (pod + volumes + secrets); update the store row's `host`.

**Rollback.** Any failure *before* step 6 verification → leave the source intact
(restart it if step 3 stopped it), reap the half-built dest, job = `failed`. The
source remains the source of truth until the dest verifies healthy. After step 7
the move is committed.

## Evacuate algorithm

Parent job validates `from_host` and every dest in `map`, then enqueues one child
migrate job per `slug` (bounded concurrency). Children run independently; partial
failure does **not** abort siblings — mirrors the existing bulk-ops contract. The
parent aggregates child states and is `succeeded` only if all children succeed,
else `failed` with per-child detail.

## Legacy adoption

Instances created *before* the store exists have no stored spec and cannot be
migrated. The adoption path is to **re-`Apply` once** (the client supplies the
spec, which populates the store). Secrets cannot be back-read from a host, so
there is no automatic backfill. Documented, not coded around.

## Decomposition (each phase = its own spec → plan → PR)

1. **Host inventory + load API** *(this doc details it below)* — independent,
   read-only; unblocks the client's evacuate-placement role.
2. **State store foundation** — `Store` interface + SQLite impl, encrypted spec
   persist on Apply/Delete, `-state-db` / `-spec-key-file` flags, key loading.
3. **Jobs infrastructure** — `jobs` table, background runner, `GET /jobs`, crash
   handling.
4. **Volume copy primitive** — `VolumeExport`/`VolumeImport` in Client + real.go
   + fake, host-to-host streaming.
5. **migrate** — orchestrate the algorithm over 2–4. `POST /migrate`.
6. **evacuate** — parent/child over the client map. `POST /evacuate`.

Phases 1 and 2 are independent and can land in either order; 3 depends on 2;
4 is independent; 5 depends on 2+3+4; 6 depends on 5.

---

# Phase 1 spec: Host inventory + load API

## Goal

Give the client the data it needs to choose evacuate targets: for every host,
its drain state, instance/container counts, and current load. Read-only,
store-independent, no new flags.

## Client interface addition

```go
// HostInfo is a point-in-time resource snapshot for a host.
type HostInfo struct {
    CPUs        int      // logical CPUs (libpod info)
    MemTotal    int64    // bytes (libpod info)
    MemFree     int64    // bytes (libpod info)
    MemUsedPct  float64  // derived
    CPUPct      *float64 // libpod info CPUUtilization when present, else nil
    LoadAvg     [3]float64 // 1/5/15-min, from /proc/loadavg over SSH
    Disk        DiskUsage  // from `podman system df`
    ContainerCount int     // total containers across managed pods
}

type DiskUsage struct {
    Total, Used, Free, Reclaimable int64 // bytes
}

HostInfo(ctx context.Context, host string) (HostInfo, error)
```

### Metric sources
- **CPUs, MemTotal, MemFree, CPUPct** — single libpod `info` call. `CPUPct` is
  version-dependent; emit `null` (omit) when the field is absent rather than
  faking a value.
- **LoadAvg** — read `/proc/loadavg` over an **SSH exec channel**. The current
  connection is an SSH-tunnelled libpod *socket*; reading loadavg needs a second
  `ssh.Session` on the same client. This is the one metric outside libpod and the
  main implementation risk in this phase. If the host connection is a local unix
  socket (no SSH), read the daemon host's own `/proc/loadavg`. **Open
  implementation detail:** confirm `real.go` retains the `*ssh.Client` (not just
  the tunnelled `net.Conn`) so a session can be opened; if not, the connection
  layer needs a small extension. Resolve in the Phase 1 plan.
- **Disk** — `podman system df` (a second libpod call). Maps total/used/free and
  reclaimable (images + dangling volumes).
- **ContainerCount** — sum of containers across `podman-api`-managed pods
  (reuse `ListAllInstances` / pod inspect), or a filtered container count.

All sub-fetches are best-effort: a failure to read one metric (e.g. loadavg on a
restricted host) populates the rest and marks that metric absent, rather than
failing the whole call. Reachability failure (host down) still returns an error.

## Service method

```go
// HostLoad returns a HostInfo snapshot, or ErrUnknownHost.
func (s *Service) HostLoad(ctx context.Context, host string) (podman.HostInfo, error)
```

## API shape

`GET /hosts/{id}` response gains a `load` object and `container_count`
(`instance_count`, `draining`, reachability already present):

```json
{
  "id": "h1",
  "draining": false,
  "instance_count": 7,
  "container_count": 12,
  "reachable": true,
  "load": {
    "cpus": 8,
    "mem_total": 16728002560,
    "mem_free": 4012300000,
    "mem_used_pct": 76.0,
    "cpu_pct": 34.0,
    "loadavg": [0.81, 1.04, 0.97],
    "disk": { "total": 0, "used": 0, "free": 0, "reclaimable": 0 }
  }
}
```

`GET /hosts` returns an array of the same shape. Load fetching is per-host and
can be slow (SSH round-trips); fetch concurrently across hosts and apply a short
per-host timeout so one unreachable host doesn't stall the list. Absent metrics
serialize as `null` / omitted.

## Testing (TDD)

- `fake` client gains a settable `HostInfo` (and error hook) so the Service and
  API layers are unit-testable without a real host.
- Service: unknown host → `ErrUnknownHost`; happy path returns the snapshot;
  per-metric degradation (one sub-fetch errors → field absent, call succeeds).
- API: `/hosts/{id}` and `/hosts` JSON shape, `load` serialization with
  present/absent `cpu_pct`, concurrent list with one slow/unreachable host.
- `real.go`: loadavg parsing and `system df` mapping covered by the
  podman-in-podman integration job; the SSH-exec path is integration-only.

## Out of scope for Phase 1
- No historical/time-series load (point-in-time only).
- No placement/scheduling logic in the daemon (client owns it).
- No store, jobs, or new flags.
