# Per-container resource metrics + volume usage

**Issue:** IoTReady/podman-api#209 (downstream tracking: `tej/podman-api-pro#11`)
**Date:** 2026-07-28
**Builds on:** #192 / `docs/superpowers/sdd/2026-07-25-container-liveness-metrics/`

## Problem

The inventory collector answers "is it up" — `podman_api_container_running`,
`_restarts_total`, `_healthy`, `podman_api_instance_ready`. It answers nothing
about how hard anything is working. There is no CPU, memory, network, block-IO
or disk figure anywhere in the binary, so a container that is up but thrashing,
or a volume quietly filling a host, is invisible until something falls over.

## Decisions taken before design

Two choices narrow the scope and are settled:

- **Dashboards are Grafana's, not the binary's.** The fleet already runs
  Prometheus and Grafana on engine-infra, scraping podman-api as
  `job="podman-api"`. This change exposes series; it adds no charting to the
  HTMX UI. The roadmap's "zero backend" in-binary dashboards remain unbuilt and
  out of scope here.
- **Collection extends podman-api's own poller** rather than deploying
  `prometheus-podman-exporter` per host. One scrape target, and the
  `host`/`template`/`slug` labels the four existing Grafana alert rules already
  key on come for free — an exporter would label by container and pod name and
  need relabelling to recover them.

## Metric surface

All series carry `{host, template, slug, container}` unless noted.

| Metric | Type | Source field |
|---|---|---|
| `podman_api_container_cpu_seconds_total` | counter | `CPUNano` / 1e9 |
| `podman_api_container_memory_bytes` | gauge | `MemUsage` |
| `podman_api_container_memory_limit_bytes` | gauge | `MemLimit` |
| `podman_api_container_network_receive_bytes_total` | counter | Σ `Network[*].RxBytes` |
| `podman_api_container_network_transmit_bytes_total` | counter | Σ `Network[*].TxBytes` |
| `podman_api_container_block_read_bytes_total` | counter | `BlockInput` |
| `podman_api_container_block_write_bytes_total` | counter | `BlockOutput` |
| `podman_api_container_processes` | gauge | `PIDs` |
| `podman_api_volume_size_bytes{host,template,slug,volume}` | gauge | `system df` volume `Size` |
| `podman_api_volume_usage_age_seconds{host}` | gauge | age of the last successful df walk |

Three deliberate choices in that table:

**Podman's percentages are not exported.** `define.ContainerStats` offers `CPU`,
`AvgCPU` and `MemPerc`. For a non-streaming call these are averaged against
container start time, so a container that was busy at boot and idle since reads
as permanently hot. Exporting the cumulative counter and letting Grafana
`rate()` it is both correct and the Prometheus idiom.

**`memory_limit_bytes` is omitted when `MemLimit` is 0**, rather than emitting a
zero. Most of our containers declare no limit; a literal 0 would make every
`memory_bytes / limit_bytes` panel read as infinite saturation. An absent series
is the honest representation of "no limit declared".

**The counters reset when a container is recreated**, exactly as
`podman_api_container_restarts_total` already does — a redeploy starts `CPUNano`
at zero again. `rate()`/`increase()` handle resets; the behaviour is stated on
each metric's Help string so nobody reads a redeploy as a counter bug.

Cardinality: roughly 150 managed containers fleet-wide × 7 container series ≈ 1k
new series, plus one per volume. Negligible for this Prometheus.

## Architecture

### Two samplers, two cadences

*Fast — on the existing 30s inventory tick.* A new client method issues one
non-streaming `containers.Stats` call per host, which returns every container on
that host in a single response.

```go
// podman.Client
ContainerStats(ctx context.Context, hostID string) ([]ContainerStats, error)
```

*Slow — its own cadence, default 1h.* A new client method issues one
`system.DiskUsage` call per host and takes `Volumes[].VolumeName` / `.Size`.

```go
// podman.Client
VolumeUsage(ctx context.Context, hostID string) (map[string]int64, error)
```

Both map podman's types into local value types in `internal/podman/types.go`,
following the existing `Pod`/`Volume` mapping pattern — nothing from
`libpod/define` escapes the podman package.

### Caching and the no-I/O-at-scrape rule

Each sampler writes into its own cache in `internal/instance`, mirroring
`instanceCache`:

```go
func (s *Service) RefreshHostStats(ctx context.Context, host string) error
func (s *Service) RefreshHostVolumeUsage(ctx context.Context, host string) error
func (s *Service) StatsSnapshot() map[string]HostStats
func (s *Service) VolumeUsageSnapshot() map[string]HostVolumeUsage
```

Two new collectors in `internal/obs` render those snapshots at scrape time. This
preserves the property `InventoryCollector` was built around and states in its
own doc comment: **a scrape performs no podman I/O**, so an unreachable or slow
host can never stall Prometheus, and a deleted instance's series disappear on the
next poll instead of freezing at their last value.

### Failure isolation

`inventory.Poller` gains two optional refresher fields; nil means that sampler is
disabled. A stats or df failure is logged on transition (same
`logTransition` discipline — no per-tick flooding) and leaves the affected series
absent until the next success.

**Neither sampler may influence `podman_api_host_reachable`.** All four
Infrastructure Alerts rules (`container-restart-loop`, `container-down`,
`instance-not-ready`, `host-inventory-stale`) gate on `host_reachable == 1`. A
slow `system df` that flipped that gate would silence real alerts across the
whole host — the failure mode is strictly worse than the metric it was trying to
collect. Reachability remains defined solely by the inventory refresh.

The slow sampler runs on its own goroutine with its own timeout, so its worst
case is a stale `volume_size_bytes` and a climbing `volume_usage_age_seconds` —
never a delayed inventory tick.

### Attribution

`containers.Stats` reports container **names**, not template/slug. The inventory
snapshot already holds every managed container name per host, so the stats
collector joins on `(host, container name)` and drops anything it cannot
attribute — containers on the host that podman-api does not manage. Names are
unique per host, so the join is exact and costs no extra podman calls.

Volumes need one small change. `listAllInstancesLive` currently passes `nil` for
volumes, so the warm cache holds no volume list at all and df sizes would have
nothing to join against. The sweep will populate `Observed.Volumes` with **names
only**, constructed from the template meta as `<template>-<slug>-<volume>`
exactly as `Service.Get` does. This is pure string construction against
already-loaded template data — no podman call, no added sweep cost. Sizes stay
absent on that path; they come from the df cache.

## Configuration

| Flag | Default | Notes |
|---|---|---|
| `-container-stats` | `true` | One extra call per host per inventory tick. Only meaningful with the poller enabled; with `-inventory-refresh-interval=0` there is no tick and no stats. |
| `-volume-usage-interval` | `1h` | `0` disables the sampler entirely (collector absent, not empty). |
| `-volume-usage-timeout` | `5m` | Per-host timeout for one df walk. |

## Risk: `system df` is slow

Podman's `/system/df` walks images, containers and volumes; on a large store it
can take minutes. This is the one genuine cost in the change, and three things
contain it: it runs hourly, not per-tick; it is isolated on its own goroutine
with its own timeout; and `podman_api_volume_usage_age_seconds` exists precisely
so a wedged walk is *visible* rather than silently frozen at its last value.

Before enabling it fleet-wide, time one call by hand on engine-1 (the host with
the largest store) and record the figure. If it exceeds the timeout there,
`-volume-usage-interval` ships defaulting to `0` (disabled) and the cadence
question reopens. The fast sampler does not depend on the slow one, so that
outcome delays only volume usage.

## Testing

- `internal/podman/fake` gains both methods.
- Collector tests via `prometheus/testutil.CollectAndCompare`, following
  `internal/obs/inventory_test.go`. Cases: normal render; a container present in
  stats but absent from inventory (dropped); a container in inventory with no
  stats (series absent, not zero); `MemLimit == 0` (limit series omitted);
  multi-interface network summing.
- Cache tests mirroring `instancecache_test.go`, including the generation-guard
  behaviour on concurrent refresh.
- A poller test asserting that a stats refresh error leaves the host's
  reachability — and therefore `podman_api_host_reachable` — at 1.
- Integration tests for the two new client methods against real podman,
  following the existing `internal/podman/real_*_integration_test.go` files.

## Downstream (podman-api-pro)

- `make bump V=<tag>` once released.
- A `Fleet resources` Grafana dashboard: CPU rate, memory (and memory-vs-limit
  where a limit exists), network, block IO, volume size — each `topk`, with
  `host`/`template`/`slug` template variables. Dashboard JSON versioned under
  `deploy/grafana/dashboards/` with the `podman unshare` copy-in documented,
  following the `alerts.yml` precedent.
- **No new alert rules in this change.** Sensible CPU/memory/volume-growth
  thresholds need a week of real data; guessing them now yields either noise or
  rules that never fire. Follow-up issue once the dashboard has history.

## Out of scope

- In-binary UI charts (the roadmap's "zero backend" dashboards).
- The OTLP push toggle for hosted/commercial egress.
- The always-zero `SizeBytes` in `Real.VolumeInspect` — the df cache could
  eventually feed the instance-detail Size column, but that is a UI change and
  belongs to its own issue.
