# Container Resource Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export per-container CPU/memory/network/block-IO series and per-volume disk usage from podman-api's Prometheus listener, so Grafana can graph fleet resource use alongside the existing liveness metrics.

**Architecture:** Two samplers write into two caches inside `internal/instance`; two new collectors in `internal/obs` render those caches at scrape time. Container stats piggyback on the existing 30s inventory poller tick (one `containers.Stats` call per host); volume usage runs on its own hourly goroutine (one `system.DiskUsage` call per host). A scrape never performs podman I/O.

**Tech Stack:** Go 1.23+, `github.com/containers/podman/v5` bindings, `prometheus/client_golang`.

**Spec:** `docs/superpowers/specs/2026-07-28-container-resource-metrics-design.md`
**Issue:** IoTReady/podman-api#209

## Global Constraints

- **Build tags are mandatory.** Always build and test via the Makefile (`make test`, `make vet`) — a plain `go build` cannot satisfy the podman v5 CGO graph-driver/gpgme deps. Never invoke `go test ./...` directly.
- **`make vet` must be clean** (gofmt check + `go vet`) before every commit.
- **Nothing from `libpod/define` or `pkg/domain/entities` may escape `internal/podman`.** Map into local value types in `internal/podman/types.go`, following the existing `Pod`/`Volume`/`HostInfo` pattern.
- **Neither new sampler may influence `podman_api_host_reachable`.** All four Grafana Infrastructure Alerts rules gate on it; a sampler failure that flipped it would silence real alerts host-wide. Reachability stays defined solely by the inventory refresh.
- **Collectors do no podman I/O.** They read a cached snapshot only, exactly as `obs.InventoryCollector` does.
- **Metric naming:** `podman_api_` prefix, `_total` suffix on counters, `_bytes`/`_seconds` units in the name.
- Commit messages end with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.

---

## File Structure

**Create:**
- `internal/instance/statscache.go` — both caches (container stats + volume usage) and their `HostStats`/`HostVolumeUsage` snapshot types.
- `internal/instance/statscache_test.go`
- `internal/obs/stats.go` — `StatsCollector` (container resource series).
- `internal/obs/stats_test.go`
- `internal/obs/volumeusage.go` — `VolumeUsageCollector` (volume size + walk age).
- `internal/obs/volumeusage_test.go`
- `internal/podman/real_stats_integration_test.go`

**Modify:**
- `internal/podman/types.go` — `ContainerStats` value type.
- `internal/podman/client.go` — two new interface methods.
- `internal/podman/real.go` — `ContainerStats`, `VolumeUsage`, `mapContainerStats`.
- `internal/podman/real_pure_test.go` — `mapContainerStats` unit test.
- `internal/podman/fake/fake.go` — fake implementations + test hooks.
- `internal/instance/service.go` — cache fields, constructor wiring, four public methods, volume-name population in the sweep.
- `internal/inventory/poller.go` — optional stats refresher on the tick; standalone volume-usage loop.
- `server/server.go` — flags, sampler wiring, collector registration.

---

### Task 1: `podman.ContainerStats` type and `Real.ContainerStats`

**Files:**
- Modify: `internal/podman/types.go`, `internal/podman/client.go`, `internal/podman/real.go`, `internal/podman/real_pure_test.go`
- Modify: `internal/podman/fake/fake.go`

**Interfaces:**
- Produces: `podman.ContainerStats` struct; `Client.ContainerStats(ctx context.Context, hostID string) ([]ContainerStats, error)`; `fake.Fake` fields `ContainerStatsVal map[string][]podman.ContainerStats`, `ContainerStatsErr error`, `ContainerStatsCalls int`.

- [ ] **Step 1: Write the failing test**

Add to `internal/podman/real_pure_test.go`:

```go
func TestMapContainerStats(t *testing.T) {
	in := define.ContainerStats{
		Name:        "engine-valvo-engine",
		CPUNano:     12_500_000_000,
		MemUsage:    734003200,
		MemLimit:    2147483648,
		BlockInput:  4096,
		BlockOutput: 8192,
		PIDs:        37,
		Network: map[string]define.ContainerNetworkStats{
			"eth0": {RxBytes: 100, TxBytes: 200},
			"eth1": {RxBytes: 5, TxBytes: 7},
		},
	}
	got := mapContainerStats(in)
	want := ContainerStats{
		Name: "engine-valvo-engine", CPUNano: 12_500_000_000,
		MemUsageBytes: 734003200, MemLimitBytes: 2147483648,
		NetRxBytes: 105, NetTxBytes: 207,
		BlockReadBytes: 4096, BlockWriteBytes: 8192, PIDs: 37,
	}
	if got != want {
		t.Fatalf("mapContainerStats = %+v, want %+v", got, want)
	}
}
```

Add the `define` import (`"github.com/containers/podman/v5/libpod/define"`) to that file if absent.

- [ ] **Step 2: Run the test and verify it fails**

Run: `make test 2>&1 | grep -A5 TestMapContainerStats`
Expected: compile failure — `undefined: mapContainerStats`.

- [ ] **Step 3: Add the value type**

In `internal/podman/types.go`, after the `Volume` type:

```go
// ContainerStats is a point-in-time resource sample for one container, mapped
// from libpod's define.ContainerStats.
//
// The cumulative fields (CPUNano, the byte counters) reset to zero when the
// container is recreated, exactly as RestartCount does — a redeploy reads as a
// counter reset, which Prometheus rate()/increase() handle.
//
// Podman's own CPU/AvgCPU/MemPerc percentages are deliberately NOT carried:
// for a non-streaming call they are averaged against container start time, so a
// container busy at boot and idle since reads as permanently hot. Export the
// counters and let PromQL derive rates.
type ContainerStats struct {
	Name            string
	CPUNano         uint64 // cumulative CPU time, nanoseconds
	MemUsageBytes   uint64
	MemLimitBytes   uint64 // 0 when the container declares no limit
	NetRxBytes      uint64 // summed across interfaces
	NetTxBytes      uint64
	BlockReadBytes  uint64
	BlockWriteBytes uint64
	PIDs            uint64
}
```

- [ ] **Step 4: Add the mapper**

In `internal/podman/real.go`, near the other mapping helpers:

```go
func mapContainerStats(s define.ContainerStats) ContainerStats {
	out := ContainerStats{
		Name:            s.Name,
		CPUNano:         s.CPUNano,
		MemUsageBytes:   s.MemUsage,
		MemLimitBytes:   s.MemLimit,
		BlockReadBytes:  s.BlockInput,
		BlockWriteBytes: s.BlockOutput,
		PIDs:            s.PIDs,
	}
	for _, n := range s.Network {
		out.NetRxBytes += n.RxBytes
		out.NetTxBytes += n.TxBytes
	}
	return out
}
```

- [ ] **Step 5: Run the test and verify it passes**

Run: `make test 2>&1 | grep -A5 TestMapContainerStats`
Expected: PASS (or no failure lines).

- [ ] **Step 6: Add the interface method and the real implementation**

In `internal/podman/client.go`, in the `Client` interface after the Volumes block:

```go
	// Stats
	// ContainerStats returns a single non-streaming resource sample for every
	// container on the host. It is one call per host; the caller attributes
	// samples to instances by container name.
	ContainerStats(ctx context.Context, hostID string) ([]ContainerStats, error)
```

In `internal/podman/real.go`:

```go
// ContainerStats issues one non-streaming stats call and returns the first
// (and only) report. Passing nil containers asks podman for every container on
// the host, so a fleet sample costs one round trip per host.
func (r *Real) ContainerStats(ctx context.Context, id string) ([]ContainerStats, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()
	stream := false
	all := true
	ch, err := containers.Stats(c, nil, &containers.StatsOptions{Stream: &stream, All: &all})
	if err != nil {
		return nil, err
	}
	select {
	case <-c.Done():
		return nil, c.Err()
	case rep, ok := <-ch:
		if !ok {
			return nil, nil
		}
		if rep.Error != nil {
			return nil, rep.Error
		}
		out := make([]ContainerStats, 0, len(rep.Stats))
		for _, s := range rep.Stats {
			out = append(out, mapContainerStats(s))
		}
		return out, nil
	}
}
```

- [ ] **Step 7: Implement it on the fake**

In `internal/podman/fake/fake.go`, add to the `Fake` struct near the `HostInfo*` hooks:

```go
	// ContainerStatsVal is returned by ContainerStats, keyed by host ID.
	ContainerStatsVal map[string][]podman.ContainerStats
	// ContainerStatsErr, if non-nil, makes ContainerStats return this error.
	ContainerStatsErr error
	// ContainerStatsCalls counts ContainerStats invocations.
	ContainerStatsCalls int
```

and the method:

```go
func (f *Fake) ContainerStats(_ context.Context, h string) ([]podman.ContainerStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ContainerStatsCalls++
	if f.ContainerStatsErr != nil {
		return nil, f.ContainerStatsErr
	}
	return f.ContainerStatsVal[h], nil
}
```

- [ ] **Step 8: Verify the whole package builds and tests pass**

Run: `make test && make vet`
Expected: PASS, vet clean. (If any other `podman.Client` implementation exists in the tree, the compiler will name it — implement the method there too.)

- [ ] **Step 9: Commit**

```bash
git add internal/podman/
git commit -m "feat(podman): ContainerStats client method (#209)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: `Real.VolumeUsage`

**Files:**
- Modify: `internal/podman/client.go`, `internal/podman/real.go`, `internal/podman/fake/fake.go`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `Client.VolumeUsage(ctx context.Context, hostID string) (map[string]int64, error)`; `fake.Fake` fields `VolumeUsageVal map[string]map[string]int64`, `VolumeUsageErr error`, `VolumeUsageCalls int`.

- [ ] **Step 1: Write the failing test**

Create `internal/podman/fake/fake_volumeusage_test.go`:

```go
package fake

import (
	"context"
	"errors"
	"testing"
)

func TestFakeVolumeUsage(t *testing.T) {
	f := New()
	f.VolumeUsageVal = map[string]map[string]int64{"h1": {"engine-valvo-data": 4096}}
	got, err := f.VolumeUsage(context.Background(), "h1")
	if err != nil {
		t.Fatalf("VolumeUsage: %v", err)
	}
	if got["engine-valvo-data"] != 4096 {
		t.Fatalf("size = %d, want 4096", got["engine-valvo-data"])
	}
	if f.VolumeUsageCalls != 1 {
		t.Fatalf("calls = %d, want 1", f.VolumeUsageCalls)
	}

	f.VolumeUsageErr = errors.New("boom")
	if _, err := f.VolumeUsage(context.Background(), "h1"); err == nil {
		t.Fatal("want error when VolumeUsageErr set")
	}
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `make test 2>&1 | grep -A5 TestFakeVolumeUsage`
Expected: compile failure — `f.VolumeUsage undefined`.

- [ ] **Step 3: Add the interface method**

In `internal/podman/client.go`, directly under the `ContainerStats` entry added in Task 1:

```go
	// VolumeUsage returns each volume's on-disk size in bytes, keyed by volume
	// name, from one `system df` call. Podman walks images, containers and
	// volumes to answer this, so it can take minutes on a large store — callers
	// must run it on a slow cadence with its own timeout, never on a hot path.
	VolumeUsage(ctx context.Context, hostID string) (map[string]int64, error)
```

- [ ] **Step 4: Implement it on Real and the fake**

In `internal/podman/real.go` (the `system` package is already imported — `HostInfo` calls `system.DiskUsage`):

```go
func (r *Real) VolumeUsage(ctx context.Context, id string) (map[string]int64, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()
	df, err := system.DiskUsage(c, &system.DiskOptions{})
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(df.Volumes))
	for _, v := range df.Volumes {
		out[v.VolumeName] = v.Size
	}
	return out, nil
}
```

In `internal/podman/fake/fake.go`, next to the Task 1 hooks:

```go
	// VolumeUsageVal is returned by VolumeUsage, keyed by host ID then volume name.
	VolumeUsageVal map[string]map[string]int64
	// VolumeUsageErr, if non-nil, makes VolumeUsage return this error.
	VolumeUsageErr error
	// VolumeUsageCalls counts VolumeUsage invocations.
	VolumeUsageCalls int
```

```go
func (f *Fake) VolumeUsage(_ context.Context, h string) (map[string]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.VolumeUsageCalls++
	if f.VolumeUsageErr != nil {
		return nil, f.VolumeUsageErr
	}
	return f.VolumeUsageVal[h], nil
}
```

- [ ] **Step 5: Run the tests and verify they pass**

Run: `make test && make vet`
Expected: PASS, vet clean.

- [ ] **Step 6: Commit**

```bash
git add internal/podman/
git commit -m "feat(podman): VolumeUsage client method via system df (#209)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Populate volume names in the inventory sweep

The warm cache currently holds no volume list at all — `listAllInstancesLive` passes `nil` for volumes — so volume sizes would have nothing to attribute to. Fill in names only, constructed from template meta exactly as `Service.Get` does (`<template>-<slug>-<volume>`). Pure string work: no podman call, no added sweep cost. Sizes stay zero on this path; they come from the usage cache.

**Files:**
- Modify: `internal/instance/service.go` (inside `listAllInstancesLive`, the per-pod loop around line 797)
- Test: `internal/instance/service_test.go` (or the existing test file covering `ListAllInstances` — find it with `grep -rln "ListAllInstances" internal/instance/*_test.go`)

**Interfaces:**
- Produces: `Observed.Volumes[].Name` populated on the sweep path, `SizeBytes` left 0.

- [ ] **Step 1: Write the failing test**

Add to the test file that already exercises `ListAllInstances`:

```go
func TestListAllInstancesPopulatesVolumeNames(t *testing.T) {
	// Reuse this file's existing helper for building a Service with a fake
	// client, a template declaring a volume named "data", and one played pod.
	// Then:
	got, err := svc.ListAllInstances(context.Background(), "h1")
	if err != nil {
		t.Fatalf("ListAllInstances: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("instances = %d, want 1", len(got))
	}
	if len(got[0].Volumes) != 1 {
		t.Fatalf("volumes = %+v, want 1", got[0].Volumes)
	}
	if want := "tmpl-slug-data"; got[0].Volumes[0].Name != want {
		t.Fatalf("volume name = %q, want %q", got[0].Volumes[0].Name, want)
	}
	if got[0].Volumes[0].SizeBytes != 0 {
		t.Fatalf("size = %d, want 0 (sweep does not price volumes)", got[0].Volumes[0].SizeBytes)
	}
}
```

Match the template ID and slug to whatever the surrounding tests' fixture uses; the assertion is `<template>-<slug>-<declared volume name>`.

- [ ] **Step 2: Run the test and verify it fails**

Run: `make test 2>&1 | grep -A8 TestListAllInstancesPopulatesVolumeNames`
Expected: FAIL — `volumes = [], want 1`.

- [ ] **Step 3: Implement**

In `internal/instance/service.go`, inside the per-template goroutine in `listAllInstancesLive`, replace the `Normalize(p, tmplID, slug, nil, ...)` call:

```go
			for _, p := range pods {
				slug := p.Labels["podman-api/slug"]
				ss := vals[store.SpecKey{Template: tmplID, Slug: slug}]
				// Names only, derived from template meta — the same construction
				// Get uses. No podman call: the sweep must not pay for volume
				// inspection, and sizes come from the volume-usage cache.
				var vols []podman.Volume
				for _, v := range t.Meta.Volumes {
					vols = append(vols, podman.Volume{Name: tmplID + "-" + slug + "-" + v.Name})
				}
				obs := Normalize(p, tmplID, slug, vols, secretEnvs, ss.vals)
				part = append(part, applySecretRedaction(obs, ss, sweepErr))
			}
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `make test && make vet`
Expected: PASS, vet clean. If a UI or API test now asserts an empty volume list from the sweep path, update that assertion — populated names are the intended new behaviour.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/
git commit -m "feat(instance): populate volume names on the inventory sweep (#209)

Names are constructed from template meta, so attribution for volume-usage
metrics costs no podman call. Sizes stay 0 on this path.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Stats and volume-usage caches

**Files:**
- Create: `internal/instance/statscache.go`, `internal/instance/statscache_test.go`
- Modify: `internal/instance/service.go` (struct fields ~line 95, `NewService` ~line 118)

**Interfaces:**
- Consumes: `podman.ContainerStats` (Task 1), `Client.ContainerStats` / `Client.VolumeUsage` (Tasks 1–2).
- Produces:
  - `instance.HostStats{Containers map[string]podman.ContainerStats; FetchedAt time.Time}`
  - `instance.HostVolumeUsage{Sizes map[string]int64; FetchedAt time.Time}`
  - `(*Service).RefreshHostStats(ctx context.Context, host string) error`
  - `(*Service).StatsSnapshot() map[string]HostStats`
  - `(*Service).RefreshHostVolumeUsage(ctx context.Context, host string) error`
  - `(*Service).VolumeUsageSnapshot() map[string]HostVolumeUsage`

- [ ] **Step 1: Write the failing tests**

Create `internal/instance/statscache_test.go`:

```go
package instance

import (
	"context"
	"errors"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func statsService(f *fake.Fake) *Service {
	return NewService(f, []config.Host{{ID: "h1"}})
}

func TestRefreshHostStatsPopulatesSnapshot(t *testing.T) {
	f := fake.New()
	f.ContainerStatsVal = map[string][]podman.ContainerStats{
		"h1": {{Name: "engine-valvo-engine", CPUNano: 5, MemUsageBytes: 9}},
	}
	svc := statsService(f)
	if err := svc.RefreshHostStats(context.Background(), "h1"); err != nil {
		t.Fatalf("RefreshHostStats: %v", err)
	}
	snap := svc.StatsSnapshot()
	got, ok := snap["h1"].Containers["engine-valvo-engine"]
	if !ok {
		t.Fatalf("container absent from snapshot: %+v", snap)
	}
	if got.CPUNano != 5 || got.MemUsageBytes != 9 {
		t.Fatalf("stats = %+v", got)
	}
	if snap["h1"].FetchedAt.IsZero() {
		t.Fatal("FetchedAt not stamped")
	}
}

// A failed stats refresh must DROP the host's samples, not keep them. A frozen
// cumulative counter reads as "container went idle", which is a lie; an absent
// series is honest.
func TestRefreshHostStatsDropsOnError(t *testing.T) {
	f := fake.New()
	f.ContainerStatsVal = map[string][]podman.ContainerStats{
		"h1": {{Name: "c", CPUNano: 5}},
	}
	svc := statsService(f)
	if err := svc.RefreshHostStats(context.Background(), "h1"); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	f.ContainerStatsErr = errors.New("boom")
	if err := svc.RefreshHostStats(context.Background(), "h1"); err == nil {
		t.Fatal("want error")
	}
	if _, ok := svc.StatsSnapshot()["h1"]; ok {
		t.Fatal("stale stats retained after a failed refresh")
	}
}

// Volume usage is the opposite: sizes change slowly and the walk is hourly, so
// a single failure keeps the last value and lets the age metric report the gap.
func TestRefreshHostVolumeUsageKeepsLastOnError(t *testing.T) {
	f := fake.New()
	f.VolumeUsageVal = map[string]map[string]int64{"h1": {"engine-valvo-data": 4096}}
	svc := statsService(f)
	if err := svc.RefreshHostVolumeUsage(context.Background(), "h1"); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	f.VolumeUsageErr = errors.New("boom")
	if err := svc.RefreshHostVolumeUsage(context.Background(), "h1"); err == nil {
		t.Fatal("want error")
	}
	if got := svc.VolumeUsageSnapshot()["h1"].Sizes["engine-valvo-data"]; got != 4096 {
		t.Fatalf("size = %d, want the retained 4096", got)
	}
}

func TestSnapshotsAreCopies(t *testing.T) {
	f := fake.New()
	f.ContainerStatsVal = map[string][]podman.ContainerStats{"h1": {{Name: "c"}}}
	svc := statsService(f)
	if err := svc.RefreshHostStats(context.Background(), "h1"); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	snap := svc.StatsSnapshot()
	delete(snap, "h1")
	if _, ok := svc.StatsSnapshot()["h1"]; !ok {
		t.Fatal("mutating a snapshot mutated the cache")
	}
}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `make test 2>&1 | grep -A5 "RefreshHostStats\|VolumeUsageSnapshot"`
Expected: compile failure — undefined methods.

- [ ] **Step 3: Write the caches**

Create `internal/instance/statscache.go`:

```go
package instance

import (
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/podman"
)

// HostStats is one host's most recent container resource sample, keyed by
// container name. Read by the metrics collector at scrape time.
type HostStats struct {
	Containers map[string]podman.ContainerStats
	FetchedAt  time.Time
}

// HostVolumeUsage is one host's most recent volume sizing, keyed by volume name.
type HostVolumeUsage struct {
	Sizes     map[string]int64
	FetchedAt time.Time
}

// statsCache holds the latest container sample per host.
//
// A failed refresh DROPS the host's entry rather than keeping the last one.
// These are cumulative counters: a frozen value reads as a container that went
// idle, which is worse than an absent series. Volume usage takes the opposite
// choice — see volumeUsageCache.
type statsCache struct {
	mu   sync.Mutex
	data map[string]HostStats
}

func newStatsCache() *statsCache {
	return &statsCache{data: map[string]HostStats{}}
}

func (c *statsCache) put(host string, s HostStats) {
	c.mu.Lock()
	c.data[host] = s
	c.mu.Unlock()
}

func (c *statsCache) drop(host string) {
	c.mu.Lock()
	delete(c.data, host)
	c.mu.Unlock()
}

func (c *statsCache) snapshot() map[string]HostStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]HostStats, len(c.data))
	for h, s := range c.data {
		out[h] = s
	}
	return out
}

// volumeUsageCache holds the latest volume sizing per host.
//
// Unlike statsCache it KEEPS the last value through a failed walk: sizes change
// slowly, the walk is hourly, and podman_api_volume_usage_age_seconds already
// reports how stale the figure is. Dropping would punch hour-wide holes in a
// gauge for one transient error.
type volumeUsageCache struct {
	mu   sync.Mutex
	data map[string]HostVolumeUsage
}

func newVolumeUsageCache() *volumeUsageCache {
	return &volumeUsageCache{data: map[string]HostVolumeUsage{}}
}

func (c *volumeUsageCache) put(host string, u HostVolumeUsage) {
	c.mu.Lock()
	c.data[host] = u
	c.mu.Unlock()
}

func (c *volumeUsageCache) snapshot() map[string]HostVolumeUsage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]HostVolumeUsage, len(c.data))
	for h, u := range c.data {
		out[h] = u
	}
	return out
}
```

The maps *inside* a returned entry are shared and must be treated read-only by callers, exactly as `instanceCache.snapshot` documents for its `Observed` slices.

- [ ] **Step 4: Wire the caches into Service**

In `internal/instance/service.go`, add to the `Service` struct next to `instCache`:

```go
	statsCache *statsCache       // per-host container resource samples (poller-fed)
	volCache   *volumeUsageCache // per-host volume sizing (slow sampler-fed)
```

In `NewService`, next to `s.instCache = newInstanceCache(3 * time.Second)`:

```go
	s.statsCache = newStatsCache()
	s.volCache = newVolumeUsageCache()
```

Then add the four public methods at the end of `service.go`, next to `InventorySnapshot`:

```go
// RefreshHostStats samples every container's resource usage on host and stores
// it for the metrics collector. One podman call per host.
//
// On error the host's samples are dropped, so its series go absent rather than
// freezing at their last value. The caller must NOT treat a failure here as the
// host being unreachable — reachability is the inventory refresh's to decide.
func (s *Service) RefreshHostStats(ctx context.Context, host string) error {
	st, err := s.client.ContainerStats(ctx, host)
	if err != nil {
		s.statsCache.drop(host)
		return err
	}
	m := make(map[string]podman.ContainerStats, len(st))
	for _, c := range st {
		m[c.Name] = c
	}
	s.statsCache.put(host, HostStats{Containers: m, FetchedAt: time.Now()})
	return nil
}

// StatsSnapshot returns the cached container samples for every host. Never
// fetches: it runs on the scrape path.
func (s *Service) StatsSnapshot() map[string]HostStats { return s.statsCache.snapshot() }

// RefreshHostVolumeUsage sizes every volume on host via one `system df` call.
// Podman walks the whole store to answer, so this belongs on a slow cadence
// with its own timeout. On error the previous sizing is retained.
func (s *Service) RefreshHostVolumeUsage(ctx context.Context, host string) error {
	sizes, err := s.client.VolumeUsage(ctx, host)
	if err != nil {
		return err
	}
	s.volCache.put(host, HostVolumeUsage{Sizes: sizes, FetchedAt: time.Now()})
	return nil
}

// VolumeUsageSnapshot returns the cached volume sizing for every host. Never
// fetches: it runs on the scrape path.
func (s *Service) VolumeUsageSnapshot() map[string]HostVolumeUsage {
	return s.volCache.snapshot()
}
```

- [ ] **Step 5: Run the tests and verify they pass**

Run: `make test && make vet`
Expected: PASS, vet clean.

- [ ] **Step 6: Commit**

```bash
git add internal/instance/
git commit -m "feat(instance): caches for container stats and volume usage (#209)

Stats drop on a failed refresh (a frozen counter lies); volume sizing keeps
its last value and lets the age metric report the gap.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: `obs.StatsCollector`

**Files:**
- Create: `internal/obs/stats.go`, `internal/obs/stats_test.go`

**Interfaces:**
- Consumes: `instance.HostStats` and `(*Service).StatsSnapshot()` (Task 4); `obs.InventorySource` (existing, `internal/obs/inventory.go`).
- Produces: `obs.StatsSource` interface; `obs.NewStatsCollector(reg prometheus.Registerer, stats StatsSource, inv InventorySource) *StatsCollector`.

Note the test helpers `gathered`, `want` and `absent` already exist in `internal/obs/inventory_test.go` (same package) — use them, do not redefine them.

- [ ] **Step 1: Write the failing tests**

Create `internal/obs/stats_test.go`:

```go
package obs

import (
	"testing"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeStats struct {
	snap map[string]instance.HostStats
}

func (f *fakeStats) StatsSnapshot() map[string]instance.HostStats { return f.snap }

// inv builds an inventory snapshot with one instance owning the named containers.
func statsInv(host, tmpl, slug string, containers ...string) *fakeInventory {
	o := instance.Observed{Template: tmpl, Slug: slug}
	for _, c := range containers {
		o.Containers = append(o.Containers, instance.ObservedContainer{Name: c})
	}
	return &fakeInventory{snap: map[string]instance.HostInventory{
		host: {Observed: []instance.Observed{o}, HasData: true, Reachable: true},
	}}
}

func newStatsReg(t *testing.T, st StatsSource, inv InventorySource) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	NewStatsCollector(reg, st, inv)
	return reg
}

func TestStatsCollectorRendersSeries(t *testing.T) {
	st := &fakeStats{snap: map[string]instance.HostStats{
		"h1": {Containers: map[string]podman.ContainerStats{
			"engine-valvo-engine": {
				Name: "engine-valvo-engine", CPUNano: 2_500_000_000,
				MemUsageBytes: 100, MemLimitBytes: 400,
				NetRxBytes: 7, NetTxBytes: 9,
				BlockReadBytes: 11, BlockWriteBytes: 13, PIDs: 3,
			},
		}},
	}}
	got := gathered(t, newStatsReg(t, st, statsInv("h1", "engine", "valvo", "engine-valvo-engine")))
	const l = "{container=engine-valvo-engine,host=h1,slug=valvo,template=engine}"
	want(t, got, "podman_api_container_cpu_seconds_total"+l, 2.5)
	want(t, got, "podman_api_container_memory_bytes"+l, 100)
	want(t, got, "podman_api_container_memory_limit_bytes"+l, 400)
	want(t, got, "podman_api_container_network_receive_bytes_total"+l, 7)
	want(t, got, "podman_api_container_network_transmit_bytes_total"+l, 9)
	want(t, got, "podman_api_container_block_read_bytes_total"+l, 11)
	want(t, got, "podman_api_container_block_write_bytes_total"+l, 13)
	want(t, got, "podman_api_container_processes"+l, 3)
}

// A container podman-api does not manage (no inventory entry) must not produce
// series — there is no template/slug to label it with.
func TestStatsCollectorDropsUnattributable(t *testing.T) {
	st := &fakeStats{snap: map[string]instance.HostStats{
		"h1": {Containers: map[string]podman.ContainerStats{
			"some-random-container": {Name: "some-random-container", CPUNano: 1},
		}},
	}}
	got := gathered(t, newStatsReg(t, st, statsInv("h1", "engine", "valvo", "engine-valvo-engine")))
	absent(t, got, "podman_api_container_cpu_seconds_total")
}

// A managed container with no sample yet yields no series at all — never a zero,
// which would read as a genuinely idle container.
func TestStatsCollectorAbsentWhenNoSample(t *testing.T) {
	st := &fakeStats{snap: map[string]instance.HostStats{}}
	got := gathered(t, newStatsReg(t, st, statsInv("h1", "engine", "valvo", "engine-valvo-engine")))
	absent(t, got, "podman_api_container_cpu_seconds_total")
}

// MemLimit 0 means "no limit declared"; emitting 0 would make every saturation
// panel read as fully saturated.
func TestStatsCollectorOmitsZeroMemLimit(t *testing.T) {
	st := &fakeStats{snap: map[string]instance.HostStats{
		"h1": {Containers: map[string]podman.ContainerStats{
			"engine-valvo-engine": {Name: "engine-valvo-engine", MemUsageBytes: 100, MemLimitBytes: 0},
		}},
	}}
	got := gathered(t, newStatsReg(t, st, statsInv("h1", "engine", "valvo", "engine-valvo-engine")))
	want(t, got, "podman_api_container_memory_bytes{container=engine-valvo-engine,host=h1,slug=valvo,template=engine}", 100)
	absent(t, got, "podman_api_container_memory_limit_bytes")
}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `make test 2>&1 | grep -A5 TestStatsCollector`
Expected: compile failure — `undefined: NewStatsCollector`.

- [ ] **Step 3: Implement the collector**

Create `internal/obs/stats.go`:

```go
package obs

import (
	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

// StatsSource supplies the latest per-host container resource samples.
// Implemented by *instance.Service.
type StatsSource interface {
	StatsSnapshot() map[string]instance.HostStats
}

const resetNote = " Resets to 0 when the pod is recreated, so a redeploy appears as a counter reset."

var (
	descCPUSeconds = prometheus.NewDesc(
		"podman_api_container_cpu_seconds_total",
		"Cumulative CPU time consumed by the container, in seconds."+resetNote,
		containerLabels, nil)
	descMemBytes = prometheus.NewDesc(
		"podman_api_container_memory_bytes",
		"Container memory usage in bytes.",
		containerLabels, nil)
	descMemLimitBytes = prometheus.NewDesc(
		"podman_api_container_memory_limit_bytes",
		"Container memory limit in bytes. Absent when the container declares no limit.",
		containerLabels, nil)
	descNetRxBytes = prometheus.NewDesc(
		"podman_api_container_network_receive_bytes_total",
		"Bytes received by the container, summed across interfaces."+resetNote,
		containerLabels, nil)
	descNetTxBytes = prometheus.NewDesc(
		"podman_api_container_network_transmit_bytes_total",
		"Bytes transmitted by the container, summed across interfaces."+resetNote,
		containerLabels, nil)
	descBlockRead = prometheus.NewDesc(
		"podman_api_container_block_read_bytes_total",
		"Bytes read from block devices by the container."+resetNote,
		containerLabels, nil)
	descBlockWrite = prometheus.NewDesc(
		"podman_api_container_block_write_bytes_total",
		"Bytes written to block devices by the container."+resetNote,
		containerLabels, nil)
	descProcesses = prometheus.NewDesc(
		"podman_api_container_processes",
		"Number of processes running in the container.",
		containerLabels, nil)
)

// StatsCollector renders per-container resource samples at scrape time.
//
// Like InventoryCollector it does no podman I/O — it reads two cached
// snapshots. The inventory snapshot is what supplies template/slug: podman
// reports stats by container name only, so a sample whose container is not in
// the inventory is dropped rather than emitted under a guessed identity.
type StatsCollector struct {
	stats StatsSource
	inv   InventorySource
}

func NewStatsCollector(reg prometheus.Registerer, stats StatsSource, inv InventorySource) *StatsCollector {
	c := &StatsCollector{stats: stats, inv: inv}
	reg.MustRegister(c)
	return c
}

func (c *StatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descCPUSeconds
	ch <- descMemBytes
	ch <- descMemLimitBytes
	ch <- descNetRxBytes
	ch <- descNetTxBytes
	ch <- descBlockRead
	ch <- descBlockWrite
	ch <- descProcesses
}

func (c *StatsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.stats.StatsSnapshot()
	for host, inv := range c.inv.InventorySnapshot() {
		hs, ok := stats[host]
		if !ok {
			continue
		}
		for _, o := range inv.Observed {
			for _, ct := range o.Containers {
				s, ok := hs.Containers[ct.Name]
				if !ok {
					continue // managed but unsampled: absent beats a false zero
				}
				lv := []string{host, o.Template, o.Slug, ct.Name}
				g := func(d *prometheus.Desc, v float64) {
					ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, lv...)
				}
				cnt := func(d *prometheus.Desc, v float64) {
					ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, lv...)
				}
				cnt(descCPUSeconds, float64(s.CPUNano)/1e9)
				g(descMemBytes, float64(s.MemUsageBytes))
				if s.MemLimitBytes > 0 {
					g(descMemLimitBytes, float64(s.MemLimitBytes))
				}
				cnt(descNetRxBytes, float64(s.NetRxBytes))
				cnt(descNetTxBytes, float64(s.NetTxBytes))
				cnt(descBlockRead, float64(s.BlockReadBytes))
				cnt(descBlockWrite, float64(s.BlockWriteBytes))
				g(descProcesses, float64(s.PIDs))
			}
		}
	}
}
```

`containerLabels` is already declared in `internal/obs/inventory.go` (same package) — reuse it, do not redeclare.

- [ ] **Step 4: Run the tests and verify they pass**

Run: `make test && make vet`
Expected: PASS, vet clean.

- [ ] **Step 5: Commit**

```bash
git add internal/obs/
git commit -m "feat(obs): container resource metrics collector (#209)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: `obs.VolumeUsageCollector`

**Files:**
- Create: `internal/obs/volumeusage.go`, `internal/obs/volumeusage_test.go`

**Interfaces:**
- Consumes: `instance.HostVolumeUsage` and `(*Service).VolumeUsageSnapshot()` (Task 4); `Observed.Volumes[].Name` populated by Task 3.
- Produces: `obs.VolumeUsageSource` interface; `obs.NewVolumeUsageCollector(reg prometheus.Registerer, src VolumeUsageSource, inv InventorySource) *VolumeUsageCollector` with an overridable `now func() time.Time` field, matching `InventoryCollector`.

- [ ] **Step 1: Write the failing tests**

Create `internal/obs/volumeusage_test.go`:

```go
package obs

import (
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeVolUsage struct {
	snap map[string]instance.HostVolumeUsage
}

func (f *fakeVolUsage) VolumeUsageSnapshot() map[string]instance.HostVolumeUsage { return f.snap }

func volInv(host, tmpl, slug string, vols ...string) *fakeInventory {
	o := instance.Observed{Template: tmpl, Slug: slug}
	for _, v := range vols {
		o.Volumes = append(o.Volumes, instance.ObservedVolume{Name: v})
	}
	return &fakeInventory{snap: map[string]instance.HostInventory{
		host: {Observed: []instance.Observed{o}, HasData: true, Reachable: true},
	}}
}

func newVolReg(t *testing.T, src VolumeUsageSource, inv InventorySource) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := NewVolumeUsageCollector(reg, src, inv)
	c.now = func() time.Time { return testNow }
	return reg
}

func TestVolumeUsageCollectorRendersSizeAndAge(t *testing.T) {
	src := &fakeVolUsage{snap: map[string]instance.HostVolumeUsage{
		"h1": {
			Sizes:     map[string]int64{"engine-valvo-data": 4096},
			FetchedAt: testNow.Add(-90 * time.Second),
		},
	}}
	got := gathered(t, newVolReg(t, src, volInv("h1", "engine", "valvo", "engine-valvo-data")))
	want(t, got, "podman_api_volume_size_bytes{host=h1,slug=valvo,template=engine,volume=engine-valvo-data}", 4096)
	want(t, got, "podman_api_volume_usage_age_seconds{host=h1}", 90)
}

// A volume podman-api does not manage must not be emitted — there is no
// instance to attribute it to.
func TestVolumeUsageCollectorDropsUnattributable(t *testing.T) {
	src := &fakeVolUsage{snap: map[string]instance.HostVolumeUsage{
		"h1": {Sizes: map[string]int64{"someones-other-volume": 1}, FetchedAt: testNow},
	}}
	got := gathered(t, newVolReg(t, src, volInv("h1", "engine", "valvo", "engine-valvo-data")))
	absent(t, got, "podman_api_volume_size_bytes")
}

// Age is the whole point of the metric: a wedged walk must be visible, so the
// age series is emitted for a host that has been sampled even if no volume of
// its own matched.
func TestVolumeUsageCollectorAgeWithoutSizes(t *testing.T) {
	src := &fakeVolUsage{snap: map[string]instance.HostVolumeUsage{
		"h1": {Sizes: map[string]int64{}, FetchedAt: testNow.Add(-time.Hour)},
	}}
	got := gathered(t, newVolReg(t, src, volInv("h1", "engine", "valvo", "engine-valvo-data")))
	want(t, got, "podman_api_volume_usage_age_seconds{host=h1}", 3600)
}

// A host that has never been sampled has no age to report — emitting 0 would
// read as a walk that just completed.
func TestVolumeUsageCollectorAbsentWhenNeverSampled(t *testing.T) {
	src := &fakeVolUsage{snap: map[string]instance.HostVolumeUsage{}}
	got := gathered(t, newVolReg(t, src, volInv("h1", "engine", "valvo", "engine-valvo-data")))
	absent(t, got, "podman_api_volume_usage_age_seconds")
	absent(t, got, "podman_api_volume_size_bytes")
}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `make test 2>&1 | grep -A5 TestVolumeUsageCollector`
Expected: compile failure — `undefined: NewVolumeUsageCollector`.

- [ ] **Step 3: Implement the collector**

Create `internal/obs/volumeusage.go`:

```go
package obs

import (
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

// VolumeUsageSource supplies the latest per-host volume sizing.
// Implemented by *instance.Service.
type VolumeUsageSource interface {
	VolumeUsageSnapshot() map[string]instance.HostVolumeUsage
}

var (
	descVolumeSize = prometheus.NewDesc(
		"podman_api_volume_size_bytes",
		"On-disk size of a managed volume, from podman system df.",
		[]string{"host", "template", "slug", "volume"}, nil)
	descVolumeUsageAge = prometheus.NewDesc(
		"podman_api_volume_usage_age_seconds",
		"Seconds since the last successful volume-usage walk for this host. Absent until the host has been walked successfully at least once; a climbing value means the walk is wedged or slower than its cadence.",
		[]string{"host"}, nil)
)

// VolumeUsageCollector renders volume sizes at scrape time, attributing each
// volume to its instance via the inventory snapshot. Does no podman I/O.
type VolumeUsageCollector struct {
	src VolumeUsageSource
	inv InventorySource
	now func() time.Time
}

func NewVolumeUsageCollector(reg prometheus.Registerer, src VolumeUsageSource, inv InventorySource) *VolumeUsageCollector {
	c := &VolumeUsageCollector{src: src, inv: inv, now: time.Now}
	reg.MustRegister(c)
	return c
}

func (c *VolumeUsageCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descVolumeSize
	ch <- descVolumeUsageAge
}

func (c *VolumeUsageCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.src.VolumeUsageSnapshot()
	now := c.now()
	inv := c.inv.InventorySnapshot()
	for host, u := range snap {
		if !u.FetchedAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(descVolumeUsageAge, prometheus.GaugeValue,
				now.Sub(u.FetchedAt).Seconds(), host)
		}
		for _, o := range inv[host].Observed {
			for _, v := range o.Volumes {
				size, ok := u.Sizes[v.Name]
				if !ok {
					continue
				}
				ch <- prometheus.MustNewConstMetric(descVolumeSize, prometheus.GaugeValue,
					float64(size), host, o.Template, o.Slug, v.Name)
			}
		}
	}
}
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `make test && make vet`
Expected: PASS, vet clean.

- [ ] **Step 5: Commit**

```bash
git add internal/obs/
git commit -m "feat(obs): volume usage metrics collector (#209)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: Poller and server wiring

**Files:**
- Modify: `internal/inventory/poller.go`
- Test: `internal/inventory/poller_test.go`
- Modify: `server/server.go` (flags ~line 90, poller block ~line 259, `registerInventoryMetrics` ~line 476)

**Interfaces:**
- Consumes: `(*Service).RefreshHostStats`, `RefreshHostVolumeUsage`, `StatsSnapshot`, `VolumeUsageSnapshot` (Task 4); `obs.NewStatsCollector` (Task 5); `obs.NewVolumeUsageCollector` (Task 6).
- Produces: `inventory.StatsRefresher` and `inventory.VolumeUsageRefresher` interfaces; `Poller.Stats` field; `Poller.StartVolumeUsage(ctx, hostsFn, interval, timeout)`.

- [ ] **Step 1: Write the failing tests**

Add to `internal/inventory/poller_test.go` (reuse whatever fake `Refresher` that file already defines; the fake below is written standalone — if a suitable one exists, use it and drop the duplicate):

```go
type statsRec struct {
	mu    sync.Mutex
	hosts []string
	err   error
}

func (s *statsRec) RefreshHostStats(_ context.Context, host string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hosts = append(s.hosts, host)
	return s.err
}

func (s *statsRec) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.hosts)
}

func TestPollerRefreshesStatsOnTick(t *testing.T) {
	inv := &recordingRefresher{} // the file's existing fake
	st := &statsRec{}
	p := &Poller{Svc: inv, Stats: st, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1", "h2"} })
	waitFor(t, func() bool { return st.calls() == 2 }) // helper: poll until true or fail after ~2s
	cancel()
	p.Wait()
}

// The four Grafana alert rules gate on podman_api_host_reachable. A stats
// failure must never look like an unreachable host.
func TestPollerStatsErrorDoesNotAffectReachability(t *testing.T) {
	inv := &recordingRefresher{}
	st := &statsRec{err: errors.New("stats boom")}
	p := &Poller{Svc: inv, Stats: st, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, func() bool { return st.calls() == 1 })
	cancel()
	p.Wait()
	if inv.errFor("h1") != nil {
		t.Fatal("stats failure leaked into the inventory refresh result")
	}
}

// A nil Stats field means the sampler is off; the tick must still run.
func TestPollerWithoutStatsRefresher(t *testing.T) {
	inv := &recordingRefresher{}
	p := &Poller{Svc: inv, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, func() bool { return inv.calls() >= 1 })
	cancel()
	p.Wait()
}
```

Adapt `recordingRefresher`/`waitFor` to the helpers already in that file; if none exist, write them locally (a mutex-guarded call recorder and a 2-second poll loop calling `t.Fatal` on timeout).

- [ ] **Step 2: Run the tests and verify they fail**

Run: `make test 2>&1 | grep -A5 TestPoller`
Expected: compile failure — `unknown field Stats`.

- [ ] **Step 3: Extend the poller**

In `internal/inventory/poller.go`, add the interfaces next to `Refresher`:

```go
// StatsRefresher samples one host's container resource usage.
// Implemented by *instance.Service.RefreshHostStats.
type StatsRefresher interface {
	RefreshHostStats(ctx context.Context, host string) error
}

// VolumeUsageRefresher sizes one host's volumes.
// Implemented by *instance.Service.RefreshHostVolumeUsage.
type VolumeUsageRefresher interface {
	RefreshHostVolumeUsage(ctx context.Context, host string) error
}
```

Add the field to `Poller`:

```go
	// Stats, when non-nil, samples container resource usage on every tick,
	// alongside the inventory refresh. Its failures are logged and otherwise
	// ignored: reachability is the inventory refresh's to decide, and the
	// Grafana alert rules all gate on podman_api_host_reachable.
	Stats StatsRefresher
```

In `tick`, inside the per-host goroutine, after `p.logTransition(host, err)`:

```go
			if p.Stats != nil {
				sctx, scancel := context.WithTimeout(ctx, p.Timeout)
				if serr := p.Stats.RefreshHostStats(sctx, host); serr != nil {
					p.logStatsTransition(host, serr)
				} else {
					p.logStatsTransition(host, nil)
				}
				scancel()
			}
```

Add a `statsState map[string]bool` field alongside `state`, initialised in `Start` next to `p.state`, and a `logStatsTransition` that mirrors `logTransition` exactly (log only on change, so a persistently failing sampler does not flood):

```go
// logStatsTransition logs stats-sampler failures only when the outcome changes,
// mirroring logTransition. Kept separate from the inventory state so a stats
// failure can never be mistaken for — or influence — host reachability.
func (p *Poller) logStatsTransition(host string, err error) {
	ok := err == nil
	p.mu.Lock()
	prev, seen := p.statsState[host]
	p.statsState[host] = ok
	p.mu.Unlock()
	switch {
	case !seen && !ok:
		log.Printf("inventory: host %s container stats unavailable: %v", host, err)
	case seen && prev && !ok:
		log.Printf("inventory: host %s container stats unavailable: %v", host, err)
	case seen && !prev && ok:
		log.Printf("inventory: host %s container stats available again", host)
	}
}
```

Extend `pruneState` to prune `statsState` with the same `keep` set.

Then add the standalone slow loop:

```go
// StartVolumeUsage runs a separate, much slower loop that sizes each host's
// volumes. It is deliberately NOT part of tick(): podman's system df walks the
// whole store and can take minutes, so it must never delay an inventory
// refresh. Failures leave the previous sizing in place; staleness surfaces as
// podman_api_volume_usage_age_seconds rather than as a gap.
func (p *Poller) StartVolumeUsage(ctx context.Context, hostsFn func() []string, r VolumeUsageRefresher, interval, timeout time.Duration) {
	if r == nil || interval <= 0 {
		return
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		run := func() {
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("inventory: volume usage tick panicked: %v", rec)
				}
			}()
			var wg sync.WaitGroup
			for _, h := range hostsFn() {
				wg.Add(1)
				go func(host string) {
					defer wg.Done()
					hctx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()
					if err := r.RefreshHostVolumeUsage(hctx, host); err != nil {
						log.Printf("inventory: host %s volume usage walk failed: %v", host, err)
					}
				}(h)
			}
			wg.Wait()
		}
		run()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				run()
			}
		}
	}()
}
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `make test && make vet`
Expected: PASS, vet clean.

- [ ] **Step 5: Wire the server**

In `server/server.go`, add to the flag block near `inventoryTimeout` (~line 91):

```go
		containerStats      = fs.Bool("container-stats", true, "sample per-container CPU/memory/network/block-IO on each inventory tick and export them as Prometheus metrics; requires the inventory poller")
		volumeUsageInterval = fs.Duration("volume-usage-interval", time.Hour, "cadence for the per-host volume sizing walk (podman system df); 0 disables it")
		volumeUsageTimeout  = fs.Duration("volume-usage-timeout", 5*time.Minute, "per-host timeout for one volume sizing walk")
```

In the `if *inventoryInterval > 0 {` block, set the field before `invPoller.Start`:

```go
		invPoller = &inventory.Poller{Svc: svc, Interval: *inventoryInterval, Timeout: *inventoryTimeout}
		if *containerStats {
			invPoller.Stats = svc
		}
		invPoller.Start(runnerCtx, hostIDs)
		invPoller.StartVolumeUsage(runnerCtx, hostIDs, svc, *volumeUsageInterval, *volumeUsageTimeout)
```

and after `registerInventoryMetrics(...)`:

```go
		if *containerStats {
			obs.NewStatsCollector(prometheus.DefaultRegisterer, svc, svc)
			log.Printf("container stats sampling enabled")
		}
		if *volumeUsageInterval > 0 {
			obs.NewVolumeUsageCollector(prometheus.DefaultRegisterer, svc, svc)
			log.Printf("volume usage sampling enabled (interval %s, per-host timeout %s)", *volumeUsageInterval, *volumeUsageTimeout)
		}
```

Both collectors register only inside the poller block, so with the poller disabled they are absent entirely rather than empty — matching how `registerInventoryMetrics` already behaves.

- [ ] **Step 6: Verify the binary builds and starts**

Run: `make build && ./bin/podman-api --help 2>&1 | grep -E "container-stats|volume-usage"`
Expected: all three flags listed with the documented defaults.

- [ ] **Step 7: Run the full suite**

Run: `make test && make vet`
Expected: PASS, vet clean.

- [ ] **Step 8: Commit**

```bash
git add internal/inventory/ server/
git commit -m "feat(server): wire container stats and volume usage samplers (#209)

Stats sample on the existing inventory tick; volume sizing runs on its own
hourly loop so a slow system df can never delay an inventory refresh. A stats
failure never touches host reachability.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 8: Integration test against real podman

**Files:**
- Create: `internal/podman/real_stats_integration_test.go`

**Interfaces:**
- Consumes: `Real.ContainerStats`, `Real.VolumeUsage` (Tasks 1–2).

First read an existing integration test (`internal/podman/real_integration_test.go`) and copy its build tag, skip guard, and host/container setup helpers verbatim — this task must not invent a second harness.

- [ ] **Step 1: Write the test**

```go
// Same build tag and skip guard as real_integration_test.go.

func TestContainerStatsIntegration(t *testing.T) {
	// Set up a Real client and start a pod using this file's existing helper,
	// then:
	stats, err := r.ContainerStats(ctx, hostID)
	if err != nil {
		t.Fatalf("ContainerStats: %v", err)
	}
	var found bool
	for _, s := range stats {
		if s.Name == containerName {
			found = true
			if s.MemUsageBytes == 0 {
				t.Errorf("MemUsageBytes = 0 for a running container")
			}
		}
	}
	if !found {
		t.Fatalf("container %q absent from stats: %+v", containerName, stats)
	}
}

func TestVolumeUsageIntegration(t *testing.T) {
	// Create a volume with this file's existing helper, then:
	sizes, err := r.VolumeUsage(ctx, hostID)
	if err != nil {
		t.Fatalf("VolumeUsage: %v", err)
	}
	if _, ok := sizes[volName]; !ok {
		t.Fatalf("volume %q absent from df: %+v", volName, sizes)
	}
}
```

Note `MemUsageBytes` is asserted non-zero but `CPUNano` is not: a container that has just started can legitimately report zero CPU time, and asserting otherwise makes the test flaky.

- [ ] **Step 2: Run the integration tests**

Run: the same command the repo's other integration tests use (check the Makefile for an `integration` target or the build tag's `-tags` value).
Expected: PASS on a machine with podman; skipped elsewhere.

- [ ] **Step 3: Run the full suite and commit**

```bash
make test && make vet
git add internal/podman/
git commit -m "test(podman): integration coverage for ContainerStats and VolumeUsage (#209)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 9: Documentation

**Files:**
- Modify: `CLAUDE.md` (the metrics section), `README.md` (flags/metrics documentation if it lists them — check with `grep -n "inventory-refresh-interval" README.md docs/*.md`)

- [ ] **Step 1: Document the metrics and flags**

Add to the observability section of `CLAUDE.md`, after the existing liveness metric list:

```markdown
**Resource metrics (#209).** With the inventory poller enabled, the same
listener also exports per-container resource usage —
`podman_api_container_cpu_seconds_total`, `_memory_bytes`,
`_memory_limit_bytes`, `_network_receive_bytes_total`,
`_network_transmit_bytes_total`, `_block_read_bytes_total`,
`_block_write_bytes_total`, `_processes` — all labelled
`{host,template,slug,container}`. Sampled by one `containers.Stats` call per
host on each poller tick; disable with `-container-stats=false`.

The cumulative series reset when a pod is recreated, exactly as
`podman_api_container_restarts_total` does; use `rate()`/`increase()`.
`_memory_limit_bytes` is **absent**, not zero, for a container with no declared
limit — so a `memory_bytes / limit_bytes` panel silently drops unlimited
containers rather than showing them at 100%.

Podman's own CPU/memory *percentages* are not exported: for a non-streaming
stats call they are averaged against container start time, so a container busy
at boot and idle since would read as permanently hot.

**Volume usage** runs separately, on `-volume-usage-interval` (default `1h`, `0`
disables) with `-volume-usage-timeout` (default `5m`), because podman's
`system df` walks the whole store and can take minutes.
`podman_api_volume_size_bytes{host,template,slug,volume}` carries the size and
`podman_api_volume_usage_age_seconds{host}` how stale it is — a climbing age is
how a wedged walk becomes visible, since the last good sizing is deliberately
retained through a failure rather than dropped.

Neither sampler affects `podman_api_host_reachable`: the four Grafana
Infrastructure Alerts rules gate on it, so a slow `system df` must never be able
to silence them.
```

- [ ] **Step 2: Verify no other doc contradicts this**

Run: `grep -rn "container-stats\|volume-usage\|_cpu_seconds_total" README.md docs/ CLAUDE.md`
Expected: only the text just added, plus the spec.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: container resource + volume usage metrics (#209)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Post-implementation (not subagent tasks — the operator runs these)

1. **Time `system df` on engine-1 by hand** before trusting the default:
   `ssh engine-1 "time podman system df -v >/dev/null"`. If it exceeds the 5m
   timeout, ship `-volume-usage-interval=0` and reopen the cadence question. The
   container-stats sampler does not depend on it.
2. Open the PR (`forgejo pr create IoTReady/podman-api --head=feat/209-container-resource-metrics --base=main`), merge, tag a release.
3. In `podman-api-pro`: `make bump V=<tag>`, deploy to engine-infra, then build
   the `Fleet resources` Grafana dashboard and version its JSON under
   `deploy/grafana/dashboards/`.
