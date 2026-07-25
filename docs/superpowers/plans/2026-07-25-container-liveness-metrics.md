# Container Liveness & Restart Metrics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export per-container and per-host state from podman-api's `/metrics`, sourced from the existing warm inventory cache, then unpause and extend the Grafana rules that had no signal to run on.

**Architecture:** A `prometheus.Collector` in `internal/obs` renders series from `internal/instance`'s warm cache on each scrape. No podman I/O on the scrape path, so a scrape cannot block behind an unreachable host; and because it renders present state rather than accumulating into a `GaugeVec`, a deleted instance's series disappear on the next poll. An explicit staleness pair (`host_reachable`, `inventory_age_seconds`) lets consumers distinguish "container is down" from "this data is ten minutes old".

**Tech Stack:** Go 1.22+, `github.com/prometheus/client_golang` v1.23.2, Prometheus, Grafana unified alerting (file provisioning), rootless podman on AlmaLinux.

**Spec:** `docs/superpowers/specs/2026-07-25-container-liveness-metrics-design.md`
**Issues:** OSS `IoTReady/podman-api#192` (Tasks 1–5), pro `IoTReady/podman-api-pro#57` (Tasks 6–9)

## Global Constraints

- **Two repos.** Tasks 1–5 are in `/home/tej/projects/podman-api` (OSS, branch `feat/192-container-liveness-metrics`, already created). Tasks 6–9 are in `/home/tej/projects/podman-api-pro` and on the `engine-infra` host.
- **`main` is PR-only in both repos.** Never commit or push directly to `main`. Branch protection and a `pre-push` hook enforce this server-side, admins included.
- **Never use a `replace` directive** in committed `podman-api-pro` code.
- **Always build and test via the `Makefile`**, never bare `go build ./...` — the CGO build tags (`containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper`) are required and a plain build fails on a clean machine.
- **`make vet` must be clean** (it runs `gofmt` check + `go vet`) before any commit is considered done.
- **Forge is Forgejo, not GitHub.** `gh` does not work. Use `forgejo issue …` / `forgejo pr create …`. The `forgejo` CLI has **no `--body-file` flag**; write the body to a file and pass `--body="$(cat file)"`.
- **Never use backticks inside a `--body="…"` shell argument** — they are command substitution and will silently mangle the posted text. Write the body to a file first.
- **Never reuse a release tag.** Tags are cached immutably by `proxy.golang.org`. If a tag is wrong, bump to the next version.
- Prometheus datasource uid: `PBFA97CFB590B2093`. Loki datasource uid: `P8E80F9AEF21F6940`.
- Grafana provisioning paths on engine-infra are owned by a subuid — every read or write needs `podman unshare`.
- **Never change an existing Grafana rule's `uid`.** A changed uid orphans the old rule permanently (it keeps evaluating and cannot be deleted from the UI). Change `title` instead.

## File Structure

| File | Responsibility |
|---|---|
| `internal/instance/instancecache.go` (modify) | Add `HostInventory` type and a non-fetching `snapshot()` |
| `internal/instance/instancecache_test.go` (modify) | Prove `snapshot()` never fetches |
| `internal/instance/service.go` (modify) | Export `InventorySnapshot()` |
| `internal/obs/inventory.go` (create) | `InventorySource` interface, metric descriptors, `InventoryCollector` |
| `internal/obs/inventory_test.go` (create) | Collector behaviour across reachable / stale / cold / unpolled / deleted |
| `server/server.go` (modify) | `registerInventoryMetrics` helper + call site inside the poller block |
| `server/server_test.go` (modify) | Helper returns nil when the poller is disabled |
| `README.md` (modify) | Document the six new metrics |

---

### Task 1: Non-fetching inventory snapshot

The collector must read the cache without ever triggering a live podman sweep — every existing read path (`get`, `getWithMeta`) fetches on a cold miss, which on the scrape path would mean a 20s stall during exactly the outage you want to observe.

**Files:**
- Modify: `internal/instance/instancecache.go`
- Modify: `internal/instance/service.go`
- Test: `internal/instance/instancecache_test.go`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `instance.HostInventory` struct with fields `Observed []Observed`, `FetchedAt time.Time`, `Reachable bool`, `HasData bool`
  - `func (s *Service) InventorySnapshot() map[string]HostInventory`

- [ ] **Step 1: Write the failing tests**

Append to `internal/instance/instancecache_test.go`:

```go
func TestInstanceCache_SnapshotNeverFetchesOnColdMiss(t *testing.T) {
	c := newInstanceCache(time.Minute)
	// Sanity: the ordinary read path *does* fetch on a cold miss. The whole
	// point of snapshot() is that it does not, so assert both halves here.
	var fetched bool
	if _, err := c.get("h1", func() ([]Observed, error) {
		fetched = true
		return []Observed{{Slug: "a"}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !fetched {
		t.Fatal("get() did not fetch on a cold miss; test premise is wrong")
	}

	c2 := newInstanceCache(time.Minute)
	if got := c2.snapshot(); len(got) != 0 {
		t.Fatalf("cold snapshot = %v, want empty and no fetch", got)
	}
	// A snapshot must not have populated the cache as a side effect either.
	if len(c2.data) != 0 {
		t.Fatalf("snapshot populated the cache: %v", c2.data)
	}
}

func TestInstanceCache_SnapshotReportsEntries(t *testing.T) {
	c := newInstanceCache(time.Minute)
	at := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	c.put("h1", c.gen["h1"], []Observed{{Template: "t", Slug: "a"}}, at)

	snap := c.snapshot()
	e, ok := snap["h1"]
	if !ok {
		t.Fatal("h1 missing from snapshot")
	}
	if !e.HasData || !e.Reachable {
		t.Fatalf("h1 = %+v, want HasData and Reachable", e)
	}
	if !e.FetchedAt.Equal(at) {
		t.Fatalf("FetchedAt = %v, want %v", e.FetchedAt, at)
	}
	if len(e.Observed) != 1 || e.Observed[0].Slug != "a" {
		t.Fatalf("Observed = %+v, want one instance with slug a", e.Observed)
	}
}

func TestInstanceCache_SnapshotKeepsDataWhenUnreachable(t *testing.T) {
	c := newInstanceCache(time.Minute)
	at := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	c.put("h1", c.gen["h1"], []Observed{{Template: "t", Slug: "a"}}, at)
	c.markUnreachable("h1", c.gen["h1"])

	e := c.snapshot()["h1"]
	if e.Reachable {
		t.Fatal("Reachable = true, want false after markUnreachable")
	}
	if !e.HasData || len(e.Observed) != 1 {
		t.Fatalf("e = %+v, want last-known-good data retained", e)
	}
	if !e.FetchedAt.Equal(at) {
		t.Fatalf("FetchedAt = %v, want the last successful fetch time %v", e.FetchedAt, at)
	}
}

func TestInstanceCache_SnapshotIsACopy(t *testing.T) {
	c := newInstanceCache(time.Minute)
	c.put("h1", c.gen["h1"], []Observed{{Slug: "a"}}, time.Now())
	snap := c.snapshot()
	delete(snap, "h1")
	if _, ok := c.snapshot()["h1"]; !ok {
		t.Fatal("mutating the returned map mutated the cache")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestInstanceCache_Snapshot -v`

Expected: FAIL — `c.snapshot undefined (type *instanceCache has no field or method snapshot)`.

- [ ] **Step 3: Add `HostInventory` and `snapshot()`**

Append to `internal/instance/instancecache.go`:

```go
// HostInventory is a point-in-time view of one host's cached inventory, as read
// by the metrics collector. It mirrors instEntry, which is unexported.
type HostInventory struct {
	Observed  []Observed
	FetchedAt time.Time
	Reachable bool
	HasData   bool
}

// snapshot returns the cache's current contents without fetching. Every other
// read path blocks on a live sweep for a cold host; the metrics collector runs
// on the scrape path and must never do that. The returned map is a fresh copy;
// the Observed slices inside it are shared and read-only, as elsewhere.
func (c *instanceCache) snapshot() map[string]HostInventory {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]HostInventory, len(c.data))
	for host, e := range c.data {
		out[host] = HostInventory{
			Observed:  e.obs,
			FetchedAt: e.fetchedAt,
			Reachable: e.reachable,
			HasData:   e.hasData,
		}
	}
	return out
}
```

Append to `internal/instance/service.go` (next to `RefreshHost`):

```go
// InventorySnapshot returns the warm cache's current contents for every host
// that has an entry, without triggering a fetch. It is meaningful only when the
// inventory poller is running — with the lazy cache, entries appear only after a
// read, so a snapshot would under-report.
func (s *Service) InventorySnapshot() map[string]HostInventory {
	return s.instCache.snapshot()
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /home/tej/projects/podman-api && make test`

Expected: PASS, whole suite green.

- [ ] **Step 5: Verify formatting and vet are clean**

Run: `cd /home/tej/projects/podman-api && make vet`

Expected: no output, exit 0.

- [ ] **Step 6: Commit**

```bash
cd /home/tej/projects/podman-api
git add internal/instance/instancecache.go internal/instance/instancecache_test.go internal/instance/service.go
git commit -m "feat(instance): non-fetching inventory snapshot for metrics (#192)

The metrics collector runs on the scrape path and must never trigger a live
podman sweep, which every existing cache read path does on a cold miss.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: The inventory collector

**Files:**
- Create: `internal/obs/inventory.go`
- Test: `internal/obs/inventory_test.go`

**Interfaces:**
- Consumes: `instance.HostInventory`, `Service.InventorySnapshot()` from Task 1.
- Produces:
  - `obs.InventorySource` interface — `InventorySnapshot() map[string]instance.HostInventory`
  - `func NewInventoryCollector(reg prometheus.Registerer, src InventorySource, hosts func() []string) *InventoryCollector`

- [ ] **Step 1: Write the failing tests**

Create `internal/obs/inventory_test.go`:

```go
package obs

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeInventory struct{ snap map[string]instance.HostInventory }

func (f *fakeInventory) InventorySnapshot() map[string]instance.HostInventory { return f.snap }

var testNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// gathered flattens a registry into "name{k=v,...}" -> value so tests can assert
// on exact series without depending on collector emission order.
func gathered(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			parts := make([]string, 0, len(m.Label))
			for _, lp := range m.Label {
				parts = append(parts, lp.GetName()+"="+lp.GetValue())
			}
			sort.Strings(parts)
			key := mf.GetName() + "{" + strings.Join(parts, ",") + "}"
			switch {
			case m.Gauge != nil:
				out[key] = m.Gauge.GetValue()
			case m.Counter != nil:
				out[key] = m.Counter.GetValue()
			}
		}
	}
	return out
}

func newTestCollector(t *testing.T, src InventorySource, hosts ...string) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := NewInventoryCollector(reg, src, func() []string { return hosts })
	c.now = func() time.Time { return testNow }
	return reg
}

func want(t *testing.T, got map[string]float64, key string, val float64) {
	t.Helper()
	v, ok := got[key]
	if !ok {
		t.Fatalf("missing series %s (got %v)", key, got)
	}
	if v != val {
		t.Fatalf("%s = %v, want %v", key, v, val)
	}
}

func absent(t *testing.T, got map[string]float64, prefix string) {
	t.Helper()
	for k := range got {
		if strings.HasPrefix(k, prefix) {
			t.Fatalf("series %s present, want absent", k)
		}
	}
}

func TestInventoryCollector_HappyPath(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"engine-1": {
			HasData:   true,
			Reachable: true,
			FetchedAt: testNow.Add(-15 * time.Second),
			Observed: []instance.Observed{{
				Template: "engine", Slug: "razorpay", Ready: true,
				Containers: []instance.ObservedContainer{
					{Name: "app", Status: "running", RestartCount: 2, Health: "healthy"},
					{Name: "litestream", Status: "exited", RestartCount: 7, Health: "unhealthy"},
				},
			}},
		},
	}}
	got := gathered(t, newTestCollector(t, src, "engine-1"))

	want(t, got, `podman_api_host_reachable{host=engine-1}`, 1)
	want(t, got, `podman_api_inventory_age_seconds{host=engine-1}`, 15)
	want(t, got, `podman_api_instance_ready{host=engine-1,slug=razorpay,template=engine}`, 1)
	want(t, got, `podman_api_container_restarts_total{container=app,host=engine-1,slug=razorpay,template=engine}`, 2)
	want(t, got, `podman_api_container_running{container=app,host=engine-1,slug=razorpay,template=engine}`, 1)
	want(t, got, `podman_api_container_healthy{container=app,host=engine-1,slug=razorpay,template=engine}`, 1)
	want(t, got, `podman_api_container_restarts_total{container=litestream,host=engine-1,slug=razorpay,template=engine}`, 7)
	want(t, got, `podman_api_container_running{container=litestream,host=engine-1,slug=razorpay,template=engine}`, 0)
	want(t, got, `podman_api_container_healthy{container=litestream,host=engine-1,slug=razorpay,template=engine}`, 0)
}

func TestInventoryCollector_NoHealthcheckIsMinusOne(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow, Observed: []instance.Observed{{
			Template: "t", Slug: "s", Ready: true,
			Containers: []instance.ObservedContainer{{Name: "c", Status: "running", Health: ""}},
		}}},
	}}
	got := gathered(t, newTestCollector(t, src, "h1"))
	want(t, got, `podman_api_container_healthy{container=c,host=h1,slug=s,template=t}`, -1)
}

func TestInventoryCollector_StartingHealthIsZero(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow, Observed: []instance.Observed{{
			Template: "t", Slug: "s",
			Containers: []instance.ObservedContainer{{Name: "c", Status: "running", Health: "starting"}},
		}}},
	}}
	got := gathered(t, newTestCollector(t, src, "h1"))
	want(t, got, `podman_api_container_healthy{container=c,host=h1,slug=s,template=t}`, 0)
	want(t, got, `podman_api_instance_ready{host=h1,slug=s,template=t}`, 0)
}

func TestInventoryCollector_UnreachableHostStillEmitsLastKnownGood(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {
			HasData: true, Reachable: false,
			FetchedAt: testNow.Add(-10 * time.Minute),
			Observed: []instance.Observed{{
				Template: "t", Slug: "s", Ready: true,
				Containers: []instance.ObservedContainer{{Name: "c", Status: "running"}},
			}},
		},
	}}
	got := gathered(t, newTestCollector(t, src, "h1"))

	want(t, got, `podman_api_host_reachable{host=h1}`, 0)
	want(t, got, `podman_api_inventory_age_seconds{host=h1}`, 600)
	want(t, got, `podman_api_container_running{container=c,host=h1,slug=s,template=t}`, 1)
}

func TestInventoryCollector_ColdHostEmitsOnlyReachable(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: false, Reachable: false},
	}}
	got := gathered(t, newTestCollector(t, src, "h1"))

	want(t, got, `podman_api_host_reachable{host=h1}`, 0)
	absent(t, got, "podman_api_inventory_age_seconds")
	absent(t, got, "podman_api_container_")
	absent(t, got, "podman_api_instance_ready")
}

func TestInventoryCollector_UnpolledHostReportsUnreachable(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{}}
	got := gathered(t, newTestCollector(t, src, "never-polled"))
	want(t, got, `podman_api_host_reachable{host=never-polled}`, 0)
	absent(t, got, "podman_api_container_")
}

func TestInventoryCollector_DeletedInstanceSeriesDisappear(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow, Observed: []instance.Observed{{
			Template: "t", Slug: "gone",
			Containers: []instance.ObservedContainer{{Name: "c", Status: "running"}},
		}}},
	}}
	reg := newTestCollector(t, src, "h1")
	if _, ok := gathered(t, reg)[`podman_api_container_running{container=c,host=h1,slug=gone,template=t}`]; !ok {
		t.Fatal("series missing before deletion")
	}

	src.snap = map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow},
	}
	absent(t, gathered(t, reg), "podman_api_container_")
}

func TestInventoryCollector_DuplicateHostsDoNotBreakGather(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow},
	}}
	got := gathered(t, newTestCollector(t, src, "h1", "h1"))
	want(t, got, `podman_api_host_reachable{host=h1}`, 1)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/obs/ -run TestInventoryCollector -v`

Expected: FAIL — `undefined: InventorySource`, `undefined: NewInventoryCollector`.

- [ ] **Step 3: Write the collector**

Create `internal/obs/inventory.go`:

```go
package obs

import (
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

// InventorySource supplies the current warm-cache inventory snapshot.
// Implemented by *instance.Service.
type InventorySource interface {
	InventorySnapshot() map[string]instance.HostInventory
}

var (
	containerLabels = []string{"host", "template", "slug", "container"}
	instanceLabels  = []string{"host", "template", "slug"}

	descContainerRestarts = prometheus.NewDesc(
		"podman_api_container_restarts_total",
		"Container restart count reported by podman. Resets to 0 when the pod is recreated, so a redeploy appears as a counter reset.",
		containerLabels, nil)
	descContainerRunning = prometheus.NewDesc(
		"podman_api_container_running",
		"1 if the container status is running, 0 otherwise.",
		containerLabels, nil)
	descContainerHealthy = prometheus.NewDesc(
		"podman_api_container_healthy",
		"1 if the container healthcheck reports healthy, 0 if unhealthy or starting, -1 if the container declares no healthcheck.",
		containerLabels, nil)
	descInstanceReady = prometheus.NewDesc(
		"podman_api_instance_ready",
		"1 if every container in the instance reports a healthy or absent healthcheck.",
		instanceLabels, nil)
	descHostReachable = prometheus.NewDesc(
		"podman_api_host_reachable",
		"1 if the most recent inventory refresh for this host succeeded.",
		[]string{"host"}, nil)
	descInventoryAge = prometheus.NewDesc(
		"podman_api_inventory_age_seconds",
		"Seconds since the last successful inventory refresh for this host. Absent until the host has been polled successfully at least once.",
		[]string{"host"}, nil)
)

// InventoryCollector renders per-container and per-host state from the warm
// inventory cache at scrape time.
//
// It is a collector rather than a set of pushed gauges for two reasons: it does
// no podman I/O, so a scrape can never block behind an unreachable host; and it
// renders present state, so a deleted or renamed instance's series disappear on
// the next poll instead of freezing at their last value forever.
type InventoryCollector struct {
	src   InventorySource
	hosts func() []string
	now   func() time.Time
}

// NewInventoryCollector builds the collector and registers it on reg.
//
// hosts must be the same host-list function the inventory poller uses. A host
// that has never been polled then still reports podman_api_host_reachable 0
// rather than vanishing from the metric set, which would read as "fine".
func NewInventoryCollector(reg prometheus.Registerer, src InventorySource, hosts func() []string) *InventoryCollector {
	c := &InventoryCollector{src: src, hosts: hosts, now: time.Now}
	reg.MustRegister(c)
	return c
}

func (c *InventoryCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descContainerRestarts
	ch <- descContainerRunning
	ch <- descContainerHealthy
	ch <- descInstanceReady
	ch <- descHostReachable
	ch <- descInventoryAge
}

func (c *InventoryCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.src.InventorySnapshot()
	now := c.now()
	seen := make(map[string]bool, len(snap))
	for _, host := range c.hosts() {
		if seen[host] {
			continue // a duplicated host id would emit duplicate series and fail Gather
		}
		seen[host] = true

		inv, ok := snap[host]
		ch <- prometheus.MustNewConstMetric(descHostReachable, prometheus.GaugeValue, boolValue(ok && inv.Reachable), host)
		if !ok || !inv.HasData {
			continue
		}
		if !inv.FetchedAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(descInventoryAge, prometheus.GaugeValue, now.Sub(inv.FetchedAt).Seconds(), host)
		}
		for _, o := range inv.Observed {
			ch <- prometheus.MustNewConstMetric(descInstanceReady, prometheus.GaugeValue, boolValue(o.Ready), host, o.Template, o.Slug)
			for _, ct := range o.Containers {
				ch <- prometheus.MustNewConstMetric(descContainerRestarts, prometheus.CounterValue, float64(ct.RestartCount), host, o.Template, o.Slug, ct.Name)
				ch <- prometheus.MustNewConstMetric(descContainerRunning, prometheus.GaugeValue, boolValue(strings.EqualFold(ct.Status, "running")), host, o.Template, o.Slug, ct.Name)
				ch <- prometheus.MustNewConstMetric(descContainerHealthy, prometheus.GaugeValue, healthValue(ct.Health), host, o.Template, o.Slug, ct.Name)
			}
		}
	}
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// healthValue maps a podman healthcheck status onto a gauge. The -1 case is
// deliberately distinct from 0: a container with no healthcheck declared is not
// the same as one that is failing its healthcheck.
func healthValue(h string) float64 {
	switch {
	case h == "":
		return -1
	case strings.EqualFold(h, "healthy"):
		return 1
	default:
		return 0
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /home/tej/projects/podman-api && make test`

Expected: PASS, whole suite green.

- [ ] **Step 5: Verify formatting and vet are clean**

Run: `cd /home/tej/projects/podman-api && make vet`

Expected: no output, exit 0.

- [ ] **Step 6: Commit**

```bash
cd /home/tej/projects/podman-api
git add internal/obs/inventory.go internal/obs/inventory_test.go
git commit -m "feat(obs): per-container liveness and restart collector (#192)

Renders container/instance/host state from the warm inventory cache on
scrape. A collector rather than pushed gauges so deleted instances stop
reporting, and so a scrape never blocks behind an unreachable host.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Server wiring

The collector is registered only when the background poller runs. With the poller off the cache is populated lazily by reads, so a snapshot would report a subset of hosts as cold — worse than no metric at all.

**Files:**
- Modify: `server/server.go` (the `if *inventoryInterval > 0` block, currently around lines 259–272)
- Test: `server/server_test.go`

**Interfaces:**
- Consumes: `obs.NewInventoryCollector`, `obs.InventorySource` (Task 2); `*instance.Service` satisfies `obs.InventorySource` via Task 1.
- Produces: `registerInventoryMetrics(reg prometheus.Registerer, src obs.InventorySource, hosts func() []string, pollerEnabled bool) *obs.InventoryCollector`

- [ ] **Step 1: Write the failing test**

Append to `server/server_test.go`:

```go
type stubInventory struct{}

func (stubInventory) InventorySnapshot() map[string]instance.HostInventory { return nil }

func TestRegisterInventoryMetrics_NilWhenPollerDisabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	got := registerInventoryMetrics(reg, stubInventory{}, func() []string { return nil }, false)
	if got != nil {
		t.Fatal("collector registered with the poller disabled; the warm cache is not being filled")
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(mfs) != 0 {
		t.Fatalf("registry has %d metric families, want 0", len(mfs))
	}
}

func TestRegisterInventoryMetrics_RegistersWhenPollerEnabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	got := registerInventoryMetrics(reg, stubInventory{}, func() []string { return []string{"h1"} }, true)
	if got == nil {
		t.Fatal("collector not registered with the poller enabled")
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sawReachable bool
	for _, mf := range mfs {
		if mf.GetName() == "podman_api_host_reachable" {
			sawReachable = true
		}
	}
	if !sawReachable {
		t.Fatal("podman_api_host_reachable missing after registration")
	}
}
```

Ensure `server/server_test.go` imports `"github.com/iotready/podman-api/internal/instance"` and `"github.com/prometheus/client_golang/prometheus"` (add only if not already present).

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./server/ -run TestRegisterInventoryMetrics -v`

Expected: FAIL — `undefined: registerInventoryMetrics`.

- [ ] **Step 3: Add the helper and wire the call site**

Append to `server/server.go` (near `buildJobRegistry`):

```go
// registerInventoryMetrics registers the per-container inventory collector, but
// only when the background poller is running. With the poller disabled the warm
// cache is filled lazily by reads, so a snapshot would report never-read hosts
// as cold — a metric that lies is worse than one that is absent.
func registerInventoryMetrics(reg prometheus.Registerer, src obs.InventorySource, hosts func() []string, pollerEnabled bool) *obs.InventoryCollector {
	if !pollerEnabled {
		return nil
	}
	return obs.NewInventoryCollector(reg, src, hosts)
}
```

Then replace the existing poller block in `RunWithFlags` — currently:

```go
	var invPoller *inventory.Poller
	if *inventoryInterval > 0 {
		svc.EnableWarmInventory()
		invPoller = &inventory.Poller{Svc: svc, Interval: *inventoryInterval, Timeout: *inventoryTimeout}
		invPoller.Start(runnerCtx, func() []string {
			hs := *hostsHolder.Load()
			ids := make([]string, len(hs))
			for i, h := range hs {
				ids[i] = h.ID
			}
			return ids
		})
		log.Printf("inventory poller enabled (interval %s, per-host timeout %s)", *inventoryInterval, *inventoryTimeout)
	}
```

with:

```go
	var invPoller *inventory.Poller
	if *inventoryInterval > 0 {
		svc.EnableWarmInventory()
		hostIDs := func() []string {
			hs := *hostsHolder.Load()
			ids := make([]string, len(hs))
			for i, h := range hs {
				ids[i] = h.ID
			}
			return ids
		}
		invPoller = &inventory.Poller{Svc: svc, Interval: *inventoryInterval, Timeout: *inventoryTimeout}
		invPoller.Start(runnerCtx, hostIDs)
		// Same host list as the poller, so a host that has never been polled
		// still reports host_reachable 0 instead of being silently absent.
		registerInventoryMetrics(prometheus.DefaultRegisterer, svc, hostIDs, true)
		log.Printf("inventory poller enabled (interval %s, per-host timeout %s)", *inventoryInterval, *inventoryTimeout)
	}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /home/tej/projects/podman-api && make test`

Expected: PASS, whole suite green.

- [ ] **Step 5: Document the metrics in the README**

Find the section of `README.md` that describes `-metrics-addr` or `/metrics`. Add this table immediately after it (adjust the surrounding prose to match the README's existing voice):

```markdown
When the background inventory poller is enabled (`-inventory-refresh-interval`,
default 30s), `/metrics` additionally exposes per-container state rendered from
the warm inventory cache:

| Metric | Labels | Meaning |
|---|---|---|
| `podman_api_container_restarts_total` | host, template, slug, container | podman's restart count. Resets to 0 when the pod is recreated, so a redeploy appears as a counter reset. |
| `podman_api_container_running` | host, template, slug, container | 1 if the container status is `running`. |
| `podman_api_container_healthy` | host, template, slug, container | 1 healthy, 0 unhealthy or starting, **-1 when the container declares no healthcheck**. |
| `podman_api_instance_ready` | host, template, slug | 1 if every container reports a healthy or absent healthcheck. |
| `podman_api_host_reachable` | host | 1 if the most recent inventory refresh succeeded. |
| `podman_api_inventory_age_seconds` | host | Seconds since the last *successful* refresh. |

The last two are not optional decoration. When a host becomes unreachable the
cache keeps serving last-known-good data, so `podman_api_container_running`
stays at 1 for the duration of the outage. Any alert built on the container
metrics should be gated on `podman_api_host_reachable == 1`, with a separate
alert on staleness.
```

- [ ] **Step 6: Verify formatting and vet are clean**

Run: `cd /home/tej/projects/podman-api && make vet && make build`

Expected: no output from vet, and `bin/podman-api` built.

- [ ] **Step 7: Commit**

```bash
cd /home/tej/projects/podman-api
git add server/server.go server/server_test.go README.md
git commit -m "feat(server): register inventory collector when the poller runs (#192)

Registered only alongside the poller: with the poller off the cache is
filled lazily by reads, so a snapshot would report unread hosts as cold.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Manual end-to-end verification against a real host

Unit tests use a fake source. This proves the wiring produces real series from a real podman sweep before anything is tagged.

**Files:** none (verification only).

**Interfaces:**
- Consumes: `bin/podman-api` built in Task 3.

- [ ] **Step 1: Run the built binary against the live hosts on a spare port**

The live service owns `100.64.0.9:8080` and `127.0.0.1:9090`; this test instance must not collide with either. Run from the OSS repo:

```bash
cd /home/tej/projects/podman-api
make build          # the dev machine is linux/amd64, same as engine-infra
scp bin/podman-api engine-infra:/tmp/podman-api-192
ssh engine-infra '/tmp/podman-api-192 \
  -addr=127.0.0.1:8099 \
  -metrics-addr=127.0.0.1:9199 \
  -hosts-dir=$HOME/.config/podman-api/hosts \
  -keys-file=$HOME/.config/podman-api/keys.yaml \
  -state-db=/tmp/probe-192.db \
  -inventory-refresh-interval=10s > /tmp/probe-192.log 2>&1 &
  echo started'
```

Note this writes a throwaway state DB at `/tmp/probe-192.db`; it must not point at the live `~/.local/share/podman-api/state.db`.

- [ ] **Step 2: Wait for the first poll, then scrape**

```bash
ssh engine-infra 'until curl -sf http://127.0.0.1:9199/metrics | grep -q podman_api_host_reachable; do sleep 3; done; \
  curl -s http://127.0.0.1:9199/metrics | grep -E "^podman_api_(container|instance|host_reachable|inventory_age)" | sort | head -40'
```

Expected: `podman_api_host_reachable` = 1 for engine-infra/engine-1/engine-2, non-zero `podman_api_inventory_age_seconds`, and `podman_api_container_running 1` for real template/slug pairs.

- [ ] **Step 3: Cross-check the counts against the API**

```bash
ssh engine-infra 'curl -s http://127.0.0.1:9199/metrics | grep -c "^podman_api_container_running"'
```

Compare with the live API's container count across all hosts (sum of `containers` across `GET /hosts/{host}/instances` for each host). They must match. If they do not, stop and investigate before proceeding — a mismatch means the collector is dropping or duplicating instances.

- [ ] **Step 4: Confirm `container_healthy` is -1, as the spec predicts**

```bash
ssh engine-infra 'curl -s http://127.0.0.1:9199/metrics | grep "^podman_api_container_healthy" | awk "{print \$2}" | sort | uniq -c'
```

Expected: all `-1` (no template declares a healthcheck today). Any other value is fine too — just record what was observed.

- [ ] **Step 5: Tear down the probe**

```bash
ssh engine-infra 'pkill -f podman-api-192; rm -f /tmp/podman-api-192 /tmp/probe-192.db* /tmp/probe-192.log; echo cleaned'
```

Verify the live service is untouched: `ssh engine-infra 'systemctl --user is-active podman-api'` → `active`.

- [ ] **Step 6: Record the observed output in the issue**

Write the observed metric sample to a file and post it (never inline backticks in `--body=`):

```bash
cd /tmp/claude-1001/-home-tej-projects-podman-api-pro/0901bed7-6516-47a5-8fea-574c817516d7/scratchpad
# write verify-192.md with the metric sample and the count cross-check
forgejo issue comment IoTReady/podman-api 192 --body="$(cat verify-192.md)"
```

---

### Task 5: OSS PR, merge, and release tag

**Files:** none (release process).

**Interfaces:**
- Produces: OSS tag `v1.0.23`, consumed by Task 6.

- [ ] **Step 1: Push the branch and open the PR**

```bash
cd /home/tej/projects/podman-api
git push -u origin feat/192-container-liveness-metrics
```

Write the PR body to `scratchpad/pr-192.md` describing: the collector approach and why not a `GaugeVec`; the six metrics; the `RestartCount`-resets-on-redeploy caveat; the `healthy == -1` caveat; and the Task 4 verification output. Then:

```bash
forgejo pr create IoTReady/podman-api --title="feat(obs): per-container liveness and restart metrics (#192)" --head=feat/192-container-liveness-metrics --base=main --body="$(cat scratchpad/pr-192.md)"
```

- [ ] **Step 2: Merge after review**

```bash
forgejo pr merge IoTReady/podman-api <PR#> --method=squash
```

- [ ] **Step 3: Publish to GitHub and tag**

```bash
cd /home/tej/projects/podman-api
git switch main && git pull --ff-only origin main
git push github main
git tag v1.0.23 && git push origin v1.0.23 && git push github v1.0.23
```

The pre-push hook runs `make test` before the GitHub push. If it fails, stop — do not force.

- [ ] **Step 4: Close the OSS issue**

```bash
forgejo issue close IoTReady/podman-api 192
```

---

### Task 6: Bump the commercial module

**Files:**
- Modify: `/home/tej/projects/podman-api-pro/go.mod`, `go.sum`

**Interfaces:**
- Consumes: OSS tag `v1.0.23` from Task 5.

- [ ] **Step 1: Branch and bump**

```bash
cd /home/tej/projects/podman-api-pro
git switch -c chore/57-bump-oss-v1.0.23
make bump V=v1.0.23
```

- [ ] **Step 2: Build and test**

```bash
cd /home/tej/projects/podman-api-pro && make build && make test && make vet
```

Expected: all green. `GOPRIVATE` is set in the Makefile so the fresh tag resolves without waiting on the module proxy.

- [ ] **Step 3: Commit and open the PR**

```bash
cd /home/tej/projects/podman-api-pro
git add go.mod go.sum
git commit -m "chore: bump OSS dep to v1.0.23 (container liveness metrics)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
git push -u origin chore/57-bump-oss-v1.0.23
forgejo pr create tej/podman-api-pro --title="chore: bump OSS dep to v1.0.23 (container liveness metrics)" --head=chore/57-bump-oss-v1.0.23 --base=main --body="Brings in the per-container inventory collector from IoTReady/podman-api#192. No pro code change. Unblocks the Grafana rules in #57."
```

- [ ] **Step 4: Merge**

```bash
forgejo pr merge tej/podman-api-pro <PR#> --method=squash
```

---

### Task 7: Deploy and move the metrics listener off port 9090

podman-api's metrics currently bind `127.0.0.1:9090` while Prometheus's own web UI binds `100.64.0.9:9090`. They coexist only by virtue of different interfaces, and `prometheus.yml` already has a `job_name: prometheus` pointing at the latter. Adding a second 9090 target would be genuinely ambiguous to read.

**Files:**
- Modify: `~/.config/systemd/user/podman-api.service` on engine-infra (path confirmed via `systemctl --user cat podman-api`)

**Interfaces:**
- Consumes: the merged pro `main` from Task 6.
- Produces: podman-api metrics reachable at `127.0.0.1:9101` on engine-infra.

- [ ] **Step 1: Back up the unit file**

```bash
ssh engine-infra 'systemctl --user cat podman-api | head -1'   # confirm the unit path
ssh engine-infra 'cp ~/.config/systemd/user/podman-api.service ~/podman-api.service.bak-$(date +%Y%m%d-%H%M%S) && ls -la ~/podman-api.service.bak-*'
```

- [ ] **Step 2: Build and stage the new binary**

```bash
cd /home/tej/projects/podman-api-pro
git switch main && git pull --ff-only origin main
make build-linux
scp bin/podman-api-pro engine-infra:~/.local/bin/podman-api.new
```

- [ ] **Step 3: Change the metrics address**

```bash
ssh engine-infra "sed -i 's|-metrics-addr=127.0.0.1:9090|-metrics-addr=127.0.0.1:9101|' ~/.config/systemd/user/podman-api.service && grep -o 'metrics-addr=[^ ]*' ~/.config/systemd/user/podman-api.service"
```

Expected output: `metrics-addr=127.0.0.1:9101`

- [ ] **Step 4: Swap the binary and restart**

```bash
ssh engine-infra 'mv ~/.local/bin/podman-api.new ~/.local/bin/podman-api && chmod +x ~/.local/bin/podman-api && systemctl --user daemon-reload && systemctl --user restart podman-api'
ssh engine-infra 'until systemctl --user is-active --quiet podman-api; do sleep 2; done; systemctl --user is-active podman-api'
```

- [ ] **Step 5: Verify the new metrics are live and nothing regressed**

```bash
ssh engine-infra 'curl -s http://127.0.0.1:9101/metrics | grep -c "^podman_api_container_running"'
ssh engine-infra 'curl -s http://127.0.0.1:9101/metrics | grep "^podman_api_host_reachable"'
ssh engine-infra 'curl -s -o /dev/null -w "old-port:%{http_code}\n" http://127.0.0.1:9090/metrics'   # expect a connection failure, not 200
```

Then confirm the control plane itself still works:

```bash
set -a; . ~/.config/podman-api/pro.env; set +a
curl -s -o /dev/null -w "healthz:%{http_code}\n" "$PODMAN_API_ADDR/healthz"
curl -s -H "Authorization: Bearer $PODMAN_API_TOKEN" "$PODMAN_API_ADDR/hosts/engine-infra/instances" | head -c 200
```

If the service fails to start, restore: `ssh engine-infra 'cp ~/podman-api.service.bak-* ~/.config/systemd/user/podman-api.service && systemctl --user daemon-reload && systemctl --user restart podman-api'`.

---

### Task 8: Add the Prometheus scrape job

**Files:**
- Modify: `~/.local/share/containers/storage/volumes/prometheus-main-data/_data/prometheus.yml` on engine-infra (via `podman unshare`)

**Interfaces:**
- Consumes: metrics on `127.0.0.1:9101` from Task 7.
- Produces: `job="podman-api"` series in Prometheus, consumed by Task 9.

- [ ] **Step 1: Back up the config**

```bash
ssh engine-infra 'podman unshare cp ~/.local/share/containers/storage/volumes/prometheus-main-data/_data/prometheus.yml ~/prometheus.yml.bak-$(date +%Y%m%d-%H%M%S) && ls -la ~/prometheus.yml.bak-*'
```

- [ ] **Step 2: Append the scrape job**

Write this script locally to `scratchpad/add_podman_api_job.py`, then run it as `ssh engine-infra "podman unshare python3 -" < scratchpad/add_podman_api_job.py`. (Do not paste heredocs with parentheses over ssh — quoting breaks. `podman unshare` is required because `_data` is subuid-owned.)

```python
p = "/home/almalinux/.local/share/containers/storage/volumes/prometheus-main-data/_data/prometheus.yml"
s = open(p).read()

assert "job_name: podman-api" not in s, "job already present"

job = """
  # podman-api's own metrics endpoint. Bound to loopback (the /metrics route is
  # mounted outside the auth guard, so it must not reach the tailnet); Prometheus
  # runs hostNetwork: true and can reach it. Port 9101, not 9090: Prometheus's own
  # web UI already owns 100.64.0.9:9090.
  - job_name: podman-api
    scrape_interval: 30s
    static_configs:
      - targets: ["127.0.0.1:9101"]
        labels:
          host: engine-infra
"""

open(p, "w").write(s.rstrip("\n") + "\n" + job)
print("added job_name: podman-api")
```

Note the `host: engine-infra` label describes where the *control plane* runs; the per-host series carry their own `host` label from the collector, which Prometheus keeps because scrape labels do not override metric labels — verify this in Step 4.

- [ ] **Step 3: Hot-reload Prometheus**

```bash
ssh engine-infra 'curl -s -o /dev/null -w "reload:%{http_code}\n" -X POST http://100.64.0.9:9090/-/reload'
```

Expected: `reload:200` (the `--web.enable-lifecycle` flag was added in #56).

- [ ] **Step 4: Verify targets and that the `host` label survived**

```bash
ssh engine-infra 'curl -s "http://100.64.0.9:9090/api/v1/targets?state=active" | python3 -c "
import sys,json,collections
d=json.load(sys.stdin)[\"data\"][\"activeTargets\"]
c=collections.Counter((t[\"labels\"][\"job\"], t[\"health\"]) for t in d)
print(dict(c)); print(\"total\", len(d))
"'
```

Expected: `("podman-api","up"): 1`, and every other job still up.

```bash
ssh engine-infra 'curl -s --get http://100.64.0.9:9090/api/v1/query --data-urlencode "query=count by (host) (podman_api_container_running)" | python3 -m json.tool | head -30'
```

Expected: one entry per managed host (`engine-infra`, `engine-1`, `engine-2`) with the collector's own `host` label — **not** all collapsed onto `engine-infra`. If they are collapsed, the scrape-level `host: engine-infra` label is the cause: remove it from the job and reload.

---

### Task 9: Unpause and extend the Grafana rules

**Files:**
- Modify: `~/.local/share/containers/storage/volumes/grafana-main-data/_data/provisioning/alerting/alerts.yml` on engine-infra (via `podman unshare`)

**Interfaces:**
- Consumes: `job="podman-api"` series from Task 8.

- [ ] **Step 1: Back up and pull down the current file**

```bash
ssh engine-infra 'podman unshare cp ~/.local/share/containers/storage/volumes/grafana-main-data/_data/provisioning/alerting/alerts.yml ~/alerts.yml.bak-$(date +%Y%m%d-%H%M%S)'
ssh engine-infra 'podman unshare cat ~/.local/share/containers/storage/volumes/grafana-main-data/_data/provisioning/alerting/alerts.yml' > scratchpad/alerts.yml
```

- [ ] **Step 2: Validate every expression against live Prometheus before writing anything**

This step is not optional. The #54 rewrite found two real bugs this way — a window shorter than the data's cadence, and a feedback loop — that no amount of reading would have caught.

```bash
for q in \
  'increase(podman_api_container_restarts_total[15m]) > 3 and on(host) podman_api_host_reachable == 1' \
  'podman_api_container_running == 0 and on(host) podman_api_host_reachable == 1' \
  'podman_api_instance_ready == 0 and on(host) podman_api_host_reachable == 1' \
  'podman_api_host_reachable == 0 or podman_api_inventory_age_seconds > 300'; do
  echo "--- $q"
  ssh engine-infra "curl -s --get http://100.64.0.9:9090/api/v1/query --data-urlencode 'query=$q' | python3 -c \"import sys,json; d=json.load(sys.stdin); print(d['status'], len(d['data']['result']))\""
done
```

Expected: `success` for all four. The first three should return **0** results on a healthy fleet; the fourth should return 0. Any non-zero result is a real finding — investigate it and report it before writing the rules, do not tune the threshold to silence it.

- [ ] **Step 3: Edit `scratchpad/alerts.yml`**

Replace the whole `container-restart-loop` block (the `# PAUSED (2026-07-25, #54)` comment through the end of its `annotations:`) with the four rules below. They go in the `Infrastructure Alerts` group, where `container-restart-loop` already lives. Keep the uid `container-restart-loop` unchanged — changing a uid orphans the rule permanently.

```yaml
      # Unpaused 2026-07-25 (#57 / OSS #192). Now backed by
      # podman_api_container_restarts_total from podman-api's inventory
      # collector rather than log lines. Note that podman resets RestartCount to
      # 0 when a pod is recreated, so a redeploy reads as a counter reset;
      # increase() handles that, and the >3 threshold tolerates the resulting
      # small burst. Gated on host_reachable so a host outage fires
      # host-inventory-stale once instead of a storm of false container alerts
      # off last-known-good cache data.
      - uid: container-restart-loop
        title: Container Restart Loop Detected
        condition: C
        data:
          - refId: A
            relativeTimeRange:
              from: 900
              to: 0
            datasourceUid: PBFA97CFB590B2093
            model:
              expr: 'increase(podman_api_container_restarts_total[15m]) and on(host) podman_api_host_reachable == 1'
              instant: true
              refId: A
          - refId: B
            datasourceUid: __expr__
            model:
              type: reduce
              expression: A
              reducer: max
              refId: B
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              expression: B
              conditions:
                - evaluator:
                    params: [3]
                    type: gt
              refId: C
        noDataState: OK
        execErrState: OK
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Container {{ $labels.container }} in {{ $labels.template }}/{{ $labels.slug }} is restart-looping"
          description: "{{ $labels.container }} in {{ $labels.template }}/{{ $labels.slug }} on {{ $labels.host }} restarted {{ $values.B }} times in the last 15 minutes."

      - uid: container-down
        title: Container Not Running
        condition: C
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: PBFA97CFB590B2093
            model:
              expr: 'podman_api_container_running and on(host) podman_api_host_reachable == 1'
              instant: true
              refId: A
          - refId: B
            datasourceUid: __expr__
            model:
              type: reduce
              expression: A
              reducer: last
              refId: B
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              expression: B
              conditions:
                - evaluator:
                    params: [1]
                    type: lt
              refId: C
        noDataState: OK
        execErrState: OK
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Container {{ $labels.container }} in {{ $labels.template }}/{{ $labels.slug }} is not running"
          description: "podman reports {{ $labels.container }} in {{ $labels.template }}/{{ $labels.slug }} on {{ $labels.host }} as not running. Check: podman pod ps on that host."

      - uid: instance-not-ready
        title: Instance Not Ready
        condition: C
        data:
          - refId: A
            relativeTimeRange:
              from: 600
              to: 0
            datasourceUid: PBFA97CFB590B2093
            model:
              expr: 'podman_api_instance_ready and on(host) podman_api_host_reachable == 1'
              instant: true
              refId: A
          - refId: B
            datasourceUid: __expr__
            model:
              type: reduce
              expression: A
              reducer: last
              refId: B
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              expression: B
              conditions:
                - evaluator:
                    params: [1]
                    type: lt
              refId: C
        noDataState: OK
        execErrState: OK
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Instance {{ $labels.template }}/{{ $labels.slug }} is not ready"
          description: "{{ $labels.template }}/{{ $labels.slug }} on {{ $labels.host }} has a container failing its healthcheck."

      # Load-bearing. When a host goes unreachable the inventory cache keeps
      # serving last-known-good data, so every container metric above stays at
      # its last value and the gated rules go quietly Normal. This is the rule
      # that says the data itself stopped being trustworthy. 300s is ten missed
      # poller ticks at the default 30s cadence.
      - uid: host-inventory-stale
        title: Host Inventory Stale
        condition: C
        data:
          - refId: A
            relativeTimeRange:
              from: 600
              to: 0
            datasourceUid: PBFA97CFB590B2093
            model:
              expr: 'clamp_max(podman_api_inventory_age_seconds, 3600) + 3600 * (podman_api_host_reachable == bool 0)'
              instant: true
              refId: A
          - refId: B
            datasourceUid: __expr__
            model:
              type: reduce
              expression: A
              reducer: last
              refId: B
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              expression: B
              conditions:
                - evaluator:
                    params: [300]
                    type: gt
              refId: C
        noDataState: Alerting
        execErrState: Alerting
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Inventory for {{ $labels.host }} is stale"
          description: "podman-api has not successfully refreshed {{ $labels.host }} recently. Container metrics for this host are last-known-good and must not be trusted. Check the podman-api log on engine-infra."
```

Note on `host-inventory-stale`: a host that is cold (never polled) emits `host_reachable 0` and **no** `inventory_age_seconds`, so the expression adds a 3600 penalty for unreachable hosts to make that case alert too. Verify this specific expression in Step 4 before trusting it.

- [ ] **Step 4: Re-validate the `host-inventory-stale` expression against live data**

```bash
ssh engine-infra "curl -s --get http://100.64.0.9:9090/api/v1/query --data-urlencode 'query=clamp_max(podman_api_inventory_age_seconds, 3600) + 3600 * (podman_api_host_reachable == bool 0)' | python3 -m json.tool | head -40"
```

Expected: one value per host, each a small number (roughly the poller interval) on a healthy fleet. If a host is missing from the result entirely, the `+` is dropping it (Prometheus `+` is an inner join) — in that case use `(podman_api_host_reachable == 0) or (podman_api_inventory_age_seconds > 300)` as a classic condition instead and re-verify.

- [ ] **Step 5: Push the file back and restart Grafana**

```bash
cd /tmp/claude-1001/-home-tej-projects-podman-api-pro/0901bed7-6516-47a5-8fea-574c817516d7/scratchpad
scp alerts.yml engine-infra:/tmp/alerts.yml
ssh engine-infra 'podman unshare cp /tmp/alerts.yml ~/.local/share/containers/storage/volumes/grafana-main-data/_data/provisioning/alerting/alerts.yml && rm /tmp/alerts.yml'
ssh engine-infra 'podman restart grafana-main-grafana'
ssh engine-infra 'until curl -sf -o /dev/null http://100.64.0.9:3000/api/health; do sleep 3; done; curl -s http://100.64.0.9:3000/api/health'
```

Grafana needs a restart to re-read provisioning; it has no hot reload for alert rules.

- [ ] **Step 6: Verify the rules load with no evaluation errors**

Grafana's HTTP API returns 401 (the `GF_SECURITY_ADMIN_PASSWORD` env var is stale — Grafana honours it only on first boot). Check the log and the DB instead.

```bash
ssh engine-infra 'podman logs --since 3m grafana-main-grafana 2>&1 | grep -Ei "error|failed to evaluate" | head -20'
```

Expected: no `Failed to evaluate rule` lines for any of the four uids.

```bash
ssh engine-infra 'podman unshare cp ~/.local/share/containers/storage/volumes/grafana-main-data/_data/grafana.db /tmp/g.db && chmod 644 /tmp/g.db'
ssh engine-infra 'python3 -c "
import sqlite3
c=sqlite3.connect(\"/tmp/g.db\")
for uid,title,paused in c.execute(\"select uid,title,is_paused from alert_rule where uid in (\\\"container-restart-loop\\\",\\\"container-down\\\",\\\"instance-not-ready\\\",\\\"host-inventory-stale\\\")\"):
    print(uid, repr(title), \"paused=\",paused)
"'
ssh engine-infra 'rm -f /tmp/g.db'
```

Expected: all four present, `container-restart-loop` with `paused=0`.

- [ ] **Step 7: Confirm no orphan rules were created**

```bash
ssh engine-infra 'podman unshare cp ~/.local/share/containers/storage/volumes/grafana-main-data/_data/grafana.db /tmp/g.db && chmod 644 /tmp/g.db'
ssh engine-infra 'python3 -c "
import sqlite3
c=sqlite3.connect(\"/tmp/g.db\")
print([r[0] for r in c.execute(\"select uid from alert_rule order by uid\")])
"'
ssh engine-infra 'rm -f /tmp/g.db'
```

Cross-check every uid against `grep "uid:" scratchpad/alerts.yml`. Any uid in the DB but not in the file is an orphan — retire it with a `deleteRules:` tombstone, never by deleting the block alone.

- [ ] **Step 8: Close pro#57**

Write the closing comment to `scratchpad/close-57.md` covering: what shipped, the OSS tag, the port move and why, the four rules and their gating, the observed metric counts, and the explicit limitation that this does not detect a hung-but-running container (that remains #58). Then:

```bash
cd /tmp/claude-1001/-home-tej-projects-podman-api-pro/0901bed7-6516-47a5-8fea-574c817516d7/scratchpad
forgejo issue comment tej/podman-api-pro 57 --body="$(cat close-57.md)"
forgejo issue close tej/podman-api-pro 57
```

- [ ] **Step 9: Update CLAUDE.md and memory**

Add a short section to `/home/tej/projects/podman-api-pro/CLAUDE.md` under the deploy notes recording that podman-api's metrics listener is `127.0.0.1:9101` (not 9090, which is Prometheus's own UI) and that the container/instance metrics require the inventory poller to be enabled. Commit it on a branch and open a PR — `main` is PR-only.

Update `~/.claude/projects/-home-tej-projects-podman-api-pro/memory/monitoring-stack-topology.md` with the new scrape job and port, since that memory currently describes the pre-#57 topology.

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| Non-fetching `snapshot()` + `HostInventory` | 1 |
| `Service.InventorySnapshot()` | 1 |
| `InventoryCollector`, six metrics, semantics | 2 |
| Server wiring gated on the poller | 3 |
| Test list (happy / unreachable / cold / unpolled / deleted / no-healthcheck) | 2 (all six, plus duplicate-host) |
| Cache test that `snapshot()` never fetches | 1 |
| Server test that the collector is absent with the poller off | 3 |
| `-metrics-addr` port move off 9090 | 7 |
| Prometheus scrape job | 8 |
| Four Grafana rules, gated, uid preserved | 9 |
| Limitation (hung-but-running not covered) | 9 Step 8 |

**Type consistency:** `HostInventory` fields (`Observed`, `FetchedAt`, `Reachable`, `HasData`) are used identically in Tasks 1, 2 and 3. `InventorySource` is defined in Task 2 and consumed in Task 3. `registerInventoryMetrics` has the same four-parameter signature in its definition and both tests. Metric names are identical in the collector, the tests, the README, the scrape verification and the Grafana rules.

**Known risk carried deliberately:** the `host-inventory-stale` expression uses a Prometheus `+` between two metrics with different label sets, which is an inner join and may drop cold hosts. Task 9 Step 4 verifies this against live data and gives the exact fallback. This is flagged rather than guessed because a staleness rule that silently drops the host it was written for is the precise failure mode this whole change exists to eliminate.
