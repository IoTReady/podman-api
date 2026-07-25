# Container Liveness & Restart Metrics — Design

**Date:** 2026-07-25
**Status:** Approved
**Issue:** OSS `IoTReady/podman-api#192` (all code) — downstream
`IoTReady/podman-api-pro#57` (Grafana rules + scrape config only)
**Repo:** OSS `github.com/iotready/podman-api`. The commercial `podman-api-pro`
repo consumes this via a version bump; it contributes no Go code, only
operational configuration on engine-infra.
**Related:** `2026-07-23-background-inventory-poller-design.md` (introduced the
warm `instanceCache` and the poller this design reads from).

## Problem

`/metrics` exposes only request and job counters (`podman_api_requests_*`,
`podman_api_jobs_*`). Nothing describes the state of the things podman-api
exists to manage. Downstream that means:

- The `container-restart-loop` Grafana rule has been **paused** since 2026-07-25
  because no restart signal exists anywhere in the stack. Container logs carry
  no lifecycle events, and the journal on the managed hosts is volatile and
  records no podman restart events.
- There is no `up`-style per-instance signal. Issue #55 removed two scrape jobs
  that had tried to synthesise one by scraping the application's own
  (nonexistent) `/metrics`; 30 of 44 targets had been permanently red as a
  result.

The data already exists and is already gathered on a schedule.
`internal/inventory.Poller` calls `Service.RefreshHost` per tick (default 30s),
which sweeps the host and stores `[]instance.Observed` — carrying
`RestartCount`, `Status`, `Health` and `Ready` per container — in the warm
per-host `instanceCache`. It is simply never exported.

## Goal

Export per-container and per-host state from the existing metrics endpoint,
sourced from the warm cache at zero additional podman cost, with an explicit
staleness signal so consumers can tell "the container is down" from "this data
is ten minutes old".

**Non-goals.** Application-level liveness (a process can be `running` and still
hung — see Limitations); metric history or persistence; changing the poller's
cadence or the desired-state store; any new extension seam.

## Approach

A Prometheus **collector** that renders series from the cache on each scrape.

Two alternatives were rejected:

- **Push into a `GaugeVec` from `RefreshHost`.** Simpler, but every deleted or
  renamed instance leaves a permanently-frozen series unless the caller
  remembers to `DeleteLabelValues` correctly. That is the same failure mode as
  the orphaned alert rule in pro#54 — a metric that keeps reporting long after
  the thing it describes is gone.
- **Live sweep on scrape.** Accurate to the millisecond, but a sweep carrying a
  20s per-host timeout on the scrape path means scrape timeouts during exactly
  the host outage the metrics are meant to observe.

A collector avoids both: it never issues a podman call, so it cannot block, and
it renders present state, so stale series cannot accumulate.

## Components

### 1. `internal/instance/instancecache.go` — non-fetching snapshot

Every existing read path (`get`, `getWithMeta`) performs a blocking fetch on a
cold miss. The collector must never do that. Add:

```go
// HostInventory is a point-in-time view of one host's cached inventory.
type HostInventory struct {
    Observed  []Observed
    FetchedAt time.Time
    Reachable bool
    HasData   bool
}

// snapshot returns the current contents of the cache without fetching.
func (c *instanceCache) snapshot() map[string]HostInventory
```

It takes `c.mu`, copies the map, and returns. The `Observed` slices are shared
read-only, consistent with how cached slices are already treated by callers.

### 2. `internal/instance/service.go` — exported accessor

```go
func (s *Service) InventorySnapshot() map[string]HostInventory
```

A thin pass-through to `s.instCache.snapshot()`.

### 3. `internal/obs/inventory.go` — the collector

```go
type InventorySource interface {
    InventorySnapshot() map[string]instance.HostInventory
}

type InventoryCollector struct {
    src   InventorySource
    hosts func() []string
    now   func() time.Time // injectable for tests
}

func NewInventoryCollector(reg prometheus.Registerer, src InventorySource, hosts func() []string) *InventoryCollector
func (c *InventoryCollector) Describe(ch chan<- *prometheus.Desc)
func (c *InventoryCollector) Collect(ch chan<- prometheus.Metric)
```

`hosts` is the **same `hostsFn` the poller uses**, so a host that has never been
polled still reports `host_reachable 0` rather than being silently absent from
the metric set entirely. `Collect` iterates `hosts()`, looks each up in the
snapshot, and emits.

`internal/obs` importing `internal/instance` introduces no cycle — `instance`
does not import `obs` (job metrics reach it through the `jobs.Metrics`
interface).

### 4. `server/server.go` — wiring

Register the collector inside the existing `if *inventoryInterval > 0` block,
alongside `invPoller`, reusing that block's `hostsFn` closure. With the poller
disabled the cache is lazily populated and a snapshot would be misleading, so
the metrics are not registered at all rather than reporting zeros.

## Metrics

```
podman_api_container_restarts_total{host,template,slug,container}   counter
podman_api_container_running{host,template,slug,container}          1 if status=="running", else 0
podman_api_container_healthy{host,template,slug,container}          1 healthy / 0 unhealthy|starting / -1 no healthcheck
podman_api_instance_ready{host,template,slug}                       1 if Observed.Ready
podman_api_host_reachable{host}                                     1 if last refresh succeeded
podman_api_inventory_age_seconds{host}                              now - FetchedAt
```

Semantics that callers must know:

- **`restarts_total` resets on redeploy.** It is podman's `RestartCount`, which
  returns to 0 when `kube play` recreates the pod. Prometheus treats this as a
  counter reset and `increase()`/`rate()` handle it correctly, but a redeploy
  registers as a small restart burst. Alert thresholds must tolerate that.
- **`container_healthy` is `-1` for containers with no healthcheck.** Sampled on
  engine-infra 2026-07-25: 24/24 containers declare none, so the metric carries
  no signal today. It is near-free to export and becomes useful when
  healthchecks land in the templates.
- **A host with no cached data** (`HasData == false` — never polled, or a failed
  cold start) emits `host_reachable 0` and nothing else. No phantom container
  series, and no `inventory_age_seconds`: `FetchedAt` is zero, and an "age" of
  57 years would be worse than an absent series.
- **`inventory_age_seconds` is emitted while a host is unreachable**, measured
  from the last *successful* fetch, so it grows for the duration of an outage.
  This is the case `host_reachable` alone cannot express: data is being served,
  and it is getting older.

Cardinality, measured against the live fleet on 2026-07-25 rather than
estimated: **202 containers across 92 instances on 3 managed hosts**, giving
~704 series (3 per container, 1 per instance, 2 per host). An earlier draft of
this document guessed ~250 from a sample of engine-infra alone, which
undercounted engine-1 (127 containers) by a wide margin. Cardinality scales
linearly with containers, which is the right bound for this data, but size the
Prometheus retention accordingly.

## Error handling

The collector cannot fail: it performs no I/O and takes only a mutex already
held for microseconds elsewhere. A panic in `Collect` would be caught by
`promhttp`'s default `HTTPErrorOnError` behaviour and surface as a 500 on
`/metrics` — acceptable, and preferable to a partial scrape being silently
accepted.

Label values come from podman pod labels (`template`, `slug`) and container
names, all of which are constrained by the template validator; no sanitisation
is needed beyond what already exists.

## Testing

Unit tests in `internal/obs`, driven by a fake `InventorySource` — the whole
point of the seam is that no podman host is required.

1. **Happy path.** Two hosts, mixed container states; assert exact series and
   values via `testutil.CollectAndCompare`.
2. **Unreachable host.** `Reachable: false, HasData: true`; assert container
   series still emitted from last-known-good data *and* `host_reachable 0`, and
   that `inventory_age_seconds` grows with the injected clock.
3. **Cold host.** `HasData: false`; assert only `host_reachable 0` and no
   container series.
4. **Unpolled host.** Host present in `hosts()` but absent from the snapshot;
   assert `host_reachable 0` rather than nothing.
5. **Deleted instance.** Snapshot changes between two `Collect` calls; assert
   the removed instance's series are gone — the property that motivated the
   collector over a `GaugeVec`.
6. **No healthcheck.** `Health == ""` yields `-1`, not `0`.

Plus a cache-level test that `snapshot()` does not fetch on a cold miss (pass a
fetch func that fails the test if called), and a `server` wiring test that the
collector is absent when `-inventory-refresh-interval=0`.

## Downstream: engine-infra operations (pro#57)

No pro Go code. Two operational changes and four Grafana rules.

**The endpoint is not where pro#57 assumed.** `/metrics` is not mounted on the
API port — `api.NewRouter` is passed `nil` for its metrics handler. It lives on
a separate listener at `-metrics-addr`, currently `127.0.0.1:9090`.
`http://100.64.0.9:8080/metrics` returns 404.

**Port collision.** podman-api's metrics bind `127.0.0.1:9090` while
Prometheus's own web UI binds `100.64.0.9:9090`, and `prometheus.yml` already
carries `job_name: prometheus` targeting the latter. They coexist only by
virtue of different interfaces. Move podman-api to `127.0.0.1:9101` in the
systemd unit before adding a scrape job, so the config is unambiguous to read.
Prometheus runs `hostNetwork: true` and can scrape loopback directly, which
keeps the endpoint off the tailnet — this matters, because `/metrics` is
mounted outside the auth guard.

Grafana rules, all in `alerts.yml`:

```
container-restart-loop   increase(podman_api_container_restarts_total[15m]) > 3   for 5m
container-down           podman_api_container_running == 0                        for 5m
instance-not-ready       podman_api_instance_ready == 0                           for 10m
host-inventory-stale     podman_api_host_reachable == 0
                         or podman_api_inventory_age_seconds > 300                for 5m
```

The first three are gated on `podman_api_host_reachable == 1` so that a host
outage fires `host-inventory-stale` once rather than a storm of false container
alerts derived from last-known-good data. `container-restart-loop` keeps its
existing uid and is unpaused — changing a uid orphans the rule (see
`grafana-provisioning-orphan-rules`).

The 300s staleness threshold is ten missed poller ticks at the default 30s
cadence.

## Limitations

**This does not close the gap that hid `dev.encube`.** That container was
`running`, `restart_count: 0`, and `ready` — while accepting TCP connections and
answering none. Process liveness is not application liveness. Closing that
requires probing the application, which is pro#58 (generate blackbox targets
from podman-api ingress). This design deliberately reports what podman knows,
and no more; claiming otherwise would be worse than the current silence.
