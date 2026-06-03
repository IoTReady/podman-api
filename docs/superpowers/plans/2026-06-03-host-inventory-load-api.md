# Host Inventory + Load API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose per-host drain state, instance/container counts, and current resource load (CPU, memory, CPU%, disk, load average) on `GET /hosts` and `GET /hosts/{id}` so the client can choose evacuate placement.

**Architecture:** Add a `HostInfo(ctx, hostID)` method to the `podman.Client` interface (host resource snapshot from libpod `info` + `system df` + a best-effort SSH read of `/proc/loadavg`). Surface it through a thin `instance.Service.HostLoad`, plus a `HostCounts` helper that derives instance+container counts from the existing `ListAllInstances`. The API `hostView` gains `container_count` and a `load` object; `listHosts` fetches hosts concurrently with a per-host timeout so one slow host can't stall the list.

**Tech Stack:** Go, libpod bindings (`pkg/bindings/system`), `golang.org/x/crypto/ssh` for the loadavg exec, the in-memory `fake` client for unit tests, `httptest` for the API tests. Build/test with the repo's remote-client tags (`make test`).

This plan implements Phase 1 of the migrate/evacuate milestone (issue #30, tracker #29). Spec: `docs/superpowers/specs/2026-06-03-migrate-evacuate-design.md` (Phase 1 section).

---

## File structure

- `internal/podman/types.go` — **Modify.** Add `HostInfo` and `DiskUsage` value types.
- `internal/podman/client.go` — **Modify.** Add `HostInfo(ctx, hostID) (HostInfo, error)` to the `Client` interface.
- `internal/podman/fake/fake.go` — **Modify.** Add `HostInfoVal` / `HostInfoErr` hooks and the `HostInfo` method.
- `internal/instance/service.go` — **Modify.** Add `HostLoad` and `HostCounts`.
- `internal/instance/service_more_test.go` — **Modify.** Unit tests for `HostLoad` / `HostCounts`.
- `internal/api/hosts.go` — **Modify.** Enrich `hostView` (`container_count` + `load`); add `loadView`; parallelize `listHosts`.
- `internal/api/coverage_test.go` — **Modify.** API tests for the new JSON shape and concurrent list.
- `internal/podman/real.go` — **Modify.** Implement `HostInfo` (libpod info + df + loadavg). Integration-tested.
- `internal/podman/real_hostinfo_integration_test.go` — **Create.** Integration test for the real `HostInfo`.
- `api/openapi.yaml` — **Modify.** Document `container_count` + `load` on the host schema.

The unit-testable surface (types, fake, service, api) is fully TDD'd in Tasks 1–7. `real.go` (libpod + SSH) is integration-only — Task 8 — matching the repo's existing split (real.go is covered by the podman-in-podman CI job, not unit tests).

---

## Task 1: HostInfo / DiskUsage value types

**Files:**
- Modify: `internal/podman/types.go`

- [ ] **Step 1: Add the types**

Append to `internal/podman/types.go` (after the `Secret` type):

```go
// HostInfo is a point-in-time resource snapshot for a host, sourced from
// libpod `info` + `system df` plus a best-effort read of /proc/loadavg.
// Pointer fields are nil when the underlying source does not report them, so
// an absent metric serializes as null rather than a misleading zero.
type HostInfo struct {
	CPUs       int      // logical CPUs
	MemTotal   int64    // bytes
	MemFree    int64    // bytes
	MemUsedPct float64  // derived: (MemTotal-MemFree)/MemTotal*100, 0 if MemTotal==0
	CPUPct     *float64 // busy percent (user+system); nil when libpod omits CPUUtilization
	LoadAvg    *[3]float64 // 1/5/15-min; nil when unavailable
	Disk       DiskUsage
}

// DiskUsage describes the host's container-storage partition (graphroot).
type DiskUsage struct {
	Total       int64 // bytes (graphroot partition size)
	Used        int64 // bytes
	Free        int64 // bytes (Total-Used)
	Reclaimable int64 // bytes reclaimable from dangling volumes (system df)
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/`
Expected: builds clean (no usages yet).

- [ ] **Step 3: Commit**

```bash
git add internal/podman/types.go
git commit -m "feat(podman): HostInfo + DiskUsage value types (#30)"
```

---

## Task 2: Add HostInfo to the Client interface + fake

This task changes the `Client` interface, so the `fake` (and `real`) must implement the new method or nothing compiles. We do fake here; real is Task 8. Until Task 8, `real.go` won't satisfy the interface — so we keep the interface change and the fake together in one commit and let `real.go` lag is NOT acceptable (build breaks). Therefore: add a temporary stub on `real.go` in this task too, replaced for real in Task 8.

**Files:**
- Modify: `internal/podman/client.go`
- Modify: `internal/podman/fake/fake.go`
- Modify: `internal/podman/real.go` (temporary stub)

- [ ] **Step 1: Write the failing fake test**

Add to `internal/podman/fake/` a new file `fake_hostinfo_test.go`:

```go
package fake

import (
	"context"
	"errors"
	"testing"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeHostInfo_ReturnsSetValue(t *testing.T) {
	f := New()
	cpu := 42.0
	f.HostInfoVal = podman.HostInfo{CPUs: 8, MemTotal: 100, MemFree: 25, MemUsedPct: 75, CPUPct: &cpu}

	got, err := f.HostInfo(context.Background(), "h1")
	require.NoError(t, err)
	assert.Equal(t, 8, got.CPUs)
	assert.Equal(t, 75.0, got.MemUsedPct)
	require.NotNil(t, got.CPUPct)
	assert.Equal(t, 42.0, *got.CPUPct)
}

func TestFakeHostInfo_Error(t *testing.T) {
	f := New()
	f.HostInfoErr = errors.New("boom")
	_, err := f.HostInfo(context.Background(), "h1")
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/fake/ -run TestFakeHostInfo -v`
Expected: COMPILE FAIL — `f.HostInfoVal undefined`, `f.HostInfo undefined`.

- [ ] **Step 3: Add the interface method**

In `internal/podman/client.go`, add a new section after the `// Health` block:

```go
	// Host
	HostInfo(ctx context.Context, hostID string) (HostInfo, error)
```

- [ ] **Step 4: Add fake hooks + method**

In `internal/podman/fake/fake.go`, add two fields to the `Fake` struct (next to the other hooks, after `PodInspectErr`):

```go
	// HostInfoVal is returned by HostInfo when HostInfoErr is nil.
	HostInfoVal podman.HostInfo
	// HostInfoErr, if non-nil, makes HostInfo return this error.
	HostInfoErr error
```

Add the method (place it next to `UsedHostPorts`):

```go
func (f *Fake) HostInfo(_ context.Context, _ string) (podman.HostInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.HostInfoErr != nil {
		return podman.HostInfo{}, f.HostInfoErr
	}
	return f.HostInfoVal, nil
}
```

- [ ] **Step 5: Add a temporary real.go stub**

In `internal/podman/real.go`, add (it will be replaced in Task 8):

```go
// HostInfo is implemented in Task 8; temporary stub to satisfy the interface.
func (r *Real) HostInfo(ctx context.Context, id string) (HostInfo, error) {
	return HostInfo{}, fmt.Errorf("HostInfo not yet implemented")
}
```

(`fmt` is already imported in real.go.)

- [ ] **Step 6: Run the fake test to verify it passes**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/fake/ -run TestFakeHostInfo -v`
Expected: PASS. Also run `make build` — Expected: builds clean (interface satisfied by both fake and real).

- [ ] **Step 7: Commit**

```bash
git add internal/podman/client.go internal/podman/fake/fake.go internal/podman/real.go
git commit -m "feat(podman): HostInfo on Client interface + fake hook (#30)"
```

---

## Task 3: Service.HostLoad

**Files:**
- Modify: `internal/instance/service.go`
- Modify: `internal/instance/service_more_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/instance/service_more_test.go`:

```go
func TestService_HostLoad_UnknownHost(t *testing.T) {
	svc, _ := newSvc(t)
	_, err := svc.HostLoad(context.Background(), "nope")
	assert.ErrorIs(t, err, ErrUnknownHost)
}

func TestService_HostLoad_PassesThrough(t *testing.T) {
	svc, f := newSvc(t)
	f.HostInfoVal = podman.HostInfo{CPUs: 4, MemTotal: 200, MemFree: 50, MemUsedPct: 75}
	got, err := svc.HostLoad(context.Background(), "h1")
	require.NoError(t, err)
	assert.Equal(t, 4, got.CPUs)
	assert.Equal(t, 75.0, got.MemUsedPct)
}
```

Tests are `package instance` (white-box), so `newSvc`, `pgApply`, `ApplyOptions`, `ErrUnknownHost` are unqualified. `newSvc(t)` returns `(*Service, *fake.Fake)` wired with host `h1` and the postgres template. `service_more_test.go` already imports `podman` (used by `AddVolume`); if not, add `"github.com/iotready/podman-api/internal/podman"`.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestService_HostLoad -v`
Expected: COMPILE FAIL — `svc.HostLoad undefined`.

- [ ] **Step 3: Implement HostLoad**

Add to `internal/instance/service.go` (next to `PortsInUse`):

```go
// HostLoad returns a point-in-time resource snapshot for a host.
func (s *Service) HostLoad(ctx context.Context, host string) (podman.HostInfo, error) {
	if _, ok := s.host(host); !ok {
		return podman.HostInfo{}, ErrUnknownHost
	}
	return s.client.HostInfo(ctx, host)
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestService_HostLoad -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/service.go internal/instance/service_more_test.go
git commit -m "feat(instance): HostLoad service method (#30)"
```

---

## Task 4: Service.HostCounts (instance + container counts in one pass)

`hostView` currently calls `InstanceCount` (which calls `ListAllInstances` and discards the slice). We add `HostCounts` that calls `ListAllInstances` once and returns both the instance count and the total container count, so the API gets `container_count` without a second backend sweep. `InstanceCount` stays for existing callers.

**Files:**
- Modify: `internal/instance/service.go`
- Modify: `internal/instance/service_more_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/instance/service_more_test.go`:

```go
func TestService_HostCounts(t *testing.T) {
	svc, _ := newSvc(t)
	// Apply two instances of the postgres template (each pod has 1 container).
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("a"), ApplyOptions{}))
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("b"), ApplyOptions{}))

	instances, containers, err := svc.HostCounts(context.Background(), "h1")
	require.NoError(t, err)
	assert.Equal(t, 2, instances)
	assert.Equal(t, instances, containers) // postgres template = 1 container per pod
}

func TestService_HostCounts_UnknownHost(t *testing.T) {
	svc, _ := newSvc(t)
	_, _, err := svc.HostCounts(context.Background(), "nope")
	assert.ErrorIs(t, err, ErrUnknownHost)
}
```

`newSvc`, `pgApply`, `ApplyOptions`, `ErrUnknownHost` are unqualified (`package instance`). The container assertion uses `instances == containers` so it holds for any single-container template rather than hard-coding 2; if `pgTemplate` later gains a sidecar, only the equality (not a literal) needs revisiting.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestService_HostCounts -v`
Expected: COMPILE FAIL — `svc.HostCounts undefined`.

- [ ] **Step 3: Implement HostCounts**

Add to `internal/instance/service.go` (next to `InstanceCount`):

```go
// HostCounts returns the number of managed instances and the total number of
// their containers on a host, in a single ListAllInstances sweep.
func (s *Service) HostCounts(ctx context.Context, host string) (instances, containers int, err error) {
	all, err := s.ListAllInstances(ctx, host)
	if err != nil {
		return 0, 0, err
	}
	for _, obs := range all {
		containers += len(obs.Containers)
	}
	return len(all), containers, nil
}
```

(`ListAllInstances` already returns `ErrUnknownHost` for an unknown host, so the unknown-host test passes without an extra check.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestService_HostCounts -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/service.go internal/instance/service_more_test.go
git commit -m "feat(instance): HostCounts (instances + containers in one sweep) (#30)"
```

---

## Task 5: API — loadView + enrich hostView

**Files:**
- Modify: `internal/api/hosts.go`
- Modify: `internal/api/coverage_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/api/coverage_test.go`. The API tests are `package api`; `newSrvFull(t)` returns `(*httptest.Server, string /*token*/, *fake.Fake)` and `authedReq(t, srv, tok, method, path)` issues a request (the fake returned IS the one wired into the server, and the fixture host id is `h1`):

```go
func TestGetHost_IncludesCountsAndLoad(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	cpu := 33.0
	la := [3]float64{0.5, 0.7, 0.9}
	f.HostInfoVal = podman.HostInfo{
		CPUs: 8, MemTotal: 1000, MemFree: 250, MemUsedPct: 75,
		CPUPct: &cpu, LoadAvg: &la,
		Disk: podman.DiskUsage{Total: 100, Used: 60, Free: 40, Reclaimable: 5},
	}

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	load, ok := body["load"].(map[string]any)
	require.True(t, ok, "load object present")
	assert.Equal(t, float64(8), load["cpus"])
	assert.Equal(t, 75.0, load["mem_used_pct"])
	assert.Equal(t, 33.0, load["cpu_pct"])
	assert.Equal(t, []any{0.5, 0.7, 0.9}, load["loadavg"])
	assert.Contains(t, body, "container_count")
}

func TestGetHost_LoadOmitsAbsentMetrics(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.HostInfoVal = podman.HostInfo{CPUs: 2, MemTotal: 10, MemFree: 5, MemUsedPct: 50} // CPUPct + LoadAvg nil

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1")
	defer resp.Body.Close()
	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	load := body["load"].(map[string]any)
	_, hasCPU := load["cpu_pct"]
	_, hasLA := load["loadavg"]
	assert.False(t, hasCPU, "cpu_pct omitted when nil")
	assert.False(t, hasLA, "loadavg omitted when nil")
}
```

Add `"github.com/iotready/podman-api/internal/podman"` to the imports of `coverage_test.go` if not already present (`json`, `http` already are).

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestGetHost_Includes -v`
Expected: FAIL — `body["load"]` absent (`ok` false).

- [ ] **Step 3: Implement loadView + enrich hostView**

In `internal/api/hosts.go`, replace the instance-count block in `hostView` (currently lines ~51-55):

```go
	if reachable {
		if n, err := h.svc.InstanceCount(r.Context(), host.ID); err == nil {
			entry["instance_count"] = n
		}
	}
```

with:

```go
	if reachable {
		if ic, cc, err := h.svc.HostCounts(r.Context(), host.ID); err == nil {
			entry["instance_count"] = ic
			entry["container_count"] = cc
		}
		if info, err := h.svc.HostLoad(r.Context(), host.ID); err == nil {
			entry["load"] = loadView(info)
		}
	}
```

Add `loadView` to the bottom of `internal/api/hosts.go`:

```go
// loadView renders a HostInfo as the canonical JSON load object. Pointer
// metrics absent from the source are omitted entirely (null-by-omission).
func loadView(info podman.HostInfo) map[string]any {
	m := map[string]any{
		"cpus":         info.CPUs,
		"mem_total":    info.MemTotal,
		"mem_free":     info.MemFree,
		"mem_used_pct": info.MemUsedPct,
		"disk": map[string]any{
			"total":       info.Disk.Total,
			"used":        info.Disk.Used,
			"free":        info.Disk.Free,
			"reclaimable": info.Disk.Reclaimable,
		},
	}
	if info.CPUPct != nil {
		m["cpu_pct"] = *info.CPUPct
	}
	if info.LoadAvg != nil {
		m["loadavg"] = []float64{info.LoadAvg[0], info.LoadAvg[1], info.LoadAvg[2]}
	}
	return m
}
```

Add the import `"github.com/iotready/podman-api/internal/podman"` to `internal/api/hosts.go` (the package currently imports `config` and `instance`; add `podman`).

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestGetHost_ -v`
Expected: PASS.

- [ ] **Step 5: Run the full api + instance suites (no regressions)**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ ./internal/instance/`
Expected: PASS. (The existing `hostView` tests that asserted `instance_count` still pass; `container_count` is additive.)

- [ ] **Step 6: Commit**

```bash
git add internal/api/hosts.go internal/api/coverage_test.go
git commit -m "feat(api): container_count + load object on host view (#30)"
```

---

## Task 6: Parallelize listHosts with per-host timeout

`hostView` does several SSH round-trips per host (Ping, Version, HostCounts, HostLoad). Done sequentially across N hosts, `GET /hosts` is O(N × round-trip). Fan out across hosts concurrently with a bounded per-host timeout so one unreachable host can't stall the whole list.

**Files:**
- Modify: `internal/api/hosts.go`
- Modify: `internal/api/coverage_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/api/coverage_test.go`:

```go
func TestListHosts_ReturnsAllWithLoad(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.HostInfoVal = podman.HostInfo{CPUs: 8, MemTotal: 1000, MemFree: 100, MemUsedPct: 90}

	resp := authedReq(t, srv, tok, "GET", "/hosts")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.NotEmpty(t, body)
	for _, h := range body {
		if h["status"] == "ok" {
			load, ok := h["load"].(map[string]any)
			require.True(t, ok, "reachable host has load")
			assert.Equal(t, float64(8), load["cpus"])
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails (or passes trivially) — confirm intent**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestListHosts_ReturnsAllWithLoad -v`
Expected: This may PASS against the current sequential `listHosts` (the fake is instant). That's acceptable — the test pins the observable contract (all hosts, load present). The parallelization in Step 3 is a non-behavioral refactor verified by this test staying green. If it fails, fix the JSON shape first.

- [ ] **Step 3: Parallelize listHosts**

Replace `listHosts` in `internal/api/hosts.go`:

```go
func (h *handlers) listHosts(w http.ResponseWriter, r *http.Request) {
	hosts := h.svc.Hosts()
	out := make([]map[string]any, len(hosts))
	const perHostTimeout = 5 * time.Second
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Add(1)
		go func(i int, host config.Host) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), perHostTimeout)
			defer cancel()
			out[i] = h.hostViewCtx(ctx, host)
		}(i, host)
	}
	wg.Wait()
	WriteJSON(w, http.StatusOK, out)
}
```

Refactor `hostView(r *http.Request, host)` to `hostViewCtx(ctx context.Context, host config.Host)` (take a context instead of the request), and update the existing `getHost` caller to pass `r.Context()`:

In `getHost`, change `h.hostView(r, host)` to `h.hostViewCtx(r.Context(), host)`. Inside `hostViewCtx`, replace every `r.Context()` with `ctx`.

Add imports to `internal/api/hosts.go`: `"context"`, `"sync"`, `"time"`.

- [ ] **Step 4: Run the test + full api suite**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -v`
Expected: PASS (including the pre-existing `getHost`/`listHosts` tests after the `hostViewCtx` rename).

- [ ] **Step 5: Run the race detector on the api package**

Run: `go test -race -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/`
Expected: PASS, no data race. (Each goroutine writes a distinct `out[i]`; the fake is mutex-guarded.)

- [ ] **Step 6: Commit**

```bash
git add internal/api/hosts.go internal/api/coverage_test.go
git commit -m "perf(api): fetch hosts concurrently with per-host timeout (#30)"
```

---

## Task 7: gofmt + vet + full unit suite checkpoint

**Files:** none (verification only).

- [ ] **Step 1: Format and vet**

Run:
```bash
gofmt -l internal/ cmd/
go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...
```
Expected: `gofmt -l` prints nothing; `go vet` clean. If `gofmt -l` lists files, run `gofmt -w` on them and re-commit.

- [ ] **Step 2: Full unit suite**

Run: `make test`
Expected: PASS.

- [ ] **Step 3: Commit any formatting fixes (if needed)**

```bash
git add -A && git commit -m "style: gofmt (#30)" || echo "nothing to format"
```

---

## Task 8: real.go HostInfo (libpod info + df + loadavg) — integration

This is the only non-unit-testable task. It replaces the Task 2 stub with the real implementation and is covered by the podman-in-podman integration job (build tag `integration`), consistent with the rest of `real.go`.

**Files:**
- Modify: `internal/podman/real.go`
- Create: `internal/podman/real_hostinfo_integration_test.go`

- [ ] **Step 1: Replace the stub with the real implementation**

In `internal/podman/real.go`, replace the Task 2 stub `HostInfo` with:

```go
// HostInfo returns a point-in-time resource snapshot for a host. CPU/mem/disk
// come from libpod `info`; reclaimable from `system df`; loadavg is a
// best-effort read of /proc/loadavg (the one metric libpod does not expose).
// Any sub-metric that cannot be obtained is left at its zero/nil value rather
// than failing the whole call; only a failed `info` call (host unreachable)
// returns an error.
func (r *Real) HostInfo(ctx context.Context, id string) (HostInfo, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return HostInfo{}, err
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return HostInfo{}, err
	}
	out := HostInfo{}
	if info.Host != nil {
		out.CPUs = info.Host.CPUs
		out.MemTotal = info.Host.MemTotal
		out.MemFree = info.Host.MemFree
		if info.Host.MemTotal > 0 {
			out.MemUsedPct = float64(info.Host.MemTotal-info.Host.MemFree) / float64(info.Host.MemTotal) * 100
		}
		if u := info.Host.CPUUtilization; u != nil {
			busy := u.UserPercent + u.SystemPercent
			out.CPUPct = &busy
		}
	}
	// Disk: graphroot partition from Store, reclaimable from system df.
	out.Disk.Total = int64(info.Store.GraphRootAllocated)
	out.Disk.Used = int64(info.Store.GraphRootUsed)
	out.Disk.Free = out.Disk.Total - out.Disk.Used
	if df, err := system.DiskUsage(c, &system.DiskOptions{}); err == nil {
		var reclaimable int64
		for _, v := range df.Volumes {
			reclaimable += v.ReclaimableSize
		}
		out.Disk.Reclaimable = reclaimable
	}
	// Loadavg: best-effort; nil on any failure.
	if la := r.hostLoadAvg(id); la != nil {
		out.LoadAvg = la
	}
	return out, nil
}
```

Add the import `"github.com/iotready/podman-api/internal/podman"`? No — real.go IS package `podman`, so `HostInfo`, `DiskUsage` are local types. No new podman import. `system` is already imported.

- [ ] **Step 2: Add the loadavg helper**

Add to `internal/podman/real.go`:

```go
// hostLoadAvg reads /proc/loadavg for a host, returning the 1/5/15-minute
// averages, or nil if it cannot be read. For a unix (local) host it reads the
// daemon's own /proc/loadavg; for an SSH host it execs `cat /proc/loadavg`
// over a short-lived SSH session using the same identity podman uses. Any
// error yields nil so the metric is simply absent.
func (r *Real) hostLoadAvg(id string) *[3]float64 {
	h, ok := r.hosts[id]
	if !ok {
		return nil
	}
	var raw string
	if h.Addr == "unix" {
		b, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return nil
		}
		raw = string(b)
	} else {
		out, err := sshReadLoadAvg(h)
		if err != nil {
			return nil
		}
		raw = out
	}
	return parseLoadAvg(raw)
}

// parseLoadAvg extracts the first three space-separated floats from a
// /proc/loadavg line ("0.42 0.37 0.31 1/512 12345").
func parseLoadAvg(raw string) *[3]float64 {
	fields := strings.Fields(raw)
	if len(fields) < 3 {
		return nil
	}
	var la [3]float64
	for i := 0; i < 3; i++ {
		v, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return nil
		}
		la[i] = v
	}
	return &la
}
```

(`os`, `strings`, `strconv` are already imported in real.go.)

- [ ] **Step 3: Add the SSH exec helper in a new build-tagged-free file**

The SSH client is only used here. Add to `internal/podman/real.go` (or a sibling `real_ssh.go` in package `podman`):

```go
import (
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// sshReadLoadAvg dials the host over SSH and reads /proc/loadavg. It reuses the
// host's configured key (h.SSHKey) and the user's known_hosts for verification
// — the same trust the libpod SSH connection relies on.
func sshReadLoadAvg(h config.Host) (string, error) {
	user, addr := splitUserHost(h.Addr) // "user@host[:port]" -> user, "host:port"
	auth := []ssh.AuthMethod{}
	if h.SSHKey != "" {
		key, err := os.ReadFile(h.SSHKey)
		if err != nil {
			return "", err
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return "", err
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	hostKeyCb, err := knownhosts.New(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
	if err != nil {
		return "", err
	}
	cfg := &ssh.ClientConfig{
		User:            user,
		Auth:            auth,
		HostKeyCallback: hostKeyCb,
		Timeout:         5 * time.Second,
	}
	conn, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	sess, err := conn.NewSession()
	if err != nil {
		return "", err
	}
	defer sess.Close()
	out, err := sess.Output("cat /proc/loadavg")
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// splitUserHost parses "user@host" or "user@host:port" into (user, "host:port"),
// defaulting to port 22.
func splitUserHost(addr string) (user, hostport string) {
	at := strings.IndexByte(addr, '@')
	if at >= 0 {
		user = addr[:at]
		addr = addr[at+1:]
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "22")
	}
	return user, addr
}
```

Verify `golang.org/x/crypto/ssh` and `.../knownhosts` are available: run `go list -m golang.org/x/crypto`. They are transitive deps of podman; if `go build` reports them missing from `go.mod`, run `go get golang.org/x/crypto/ssh` and `go mod tidy` with the build tags.

- [ ] **Step 4: Write the integration test**

Create `internal/podman/real_hostinfo_integration_test.go`:

```go
//go:build integration

package podman

import (
	"context"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Uses the same local-unix-socket fixture as the other integration tests
// (see real_integration_test.go for the host-config helper).
func TestRealHostInfo_LocalSocket(t *testing.T) {
	host := integrationHost(t) // existing helper returning a unix-socket config.Host
	r, err := NewReal([]config.Host{host})
	require.NoError(t, err)

	info, err := r.HostInfo(context.Background(), host.ID)
	require.NoError(t, err)
	assert.Greater(t, info.CPUs, 0)
	assert.Greater(t, info.MemTotal, int64(0))
	assert.GreaterOrEqual(t, info.MemUsedPct, 0.0)
	// unix host => loadavg read from local /proc/loadavg
	require.NotNil(t, info.LoadAvg)
	assert.GreaterOrEqual(t, info.LoadAvg[0], 0.0)
}
```

NOTE: `integrationHost(t)` stands for whatever fixture the existing integration tests use to produce a working local `config.Host` (grep `internal/podman/real_integration_test.go` for the actual helper and match its name/signature).

- [ ] **Step 5: Build with both tag sets**

Run:
```bash
make build
go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper integration" ./internal/podman/
```
Expected: both clean. (Unit `make test` does not run the integration file.)

- [ ] **Step 6: Run the integration test (requires a local podman socket)**

Run: `make test-integration` (or `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper integration" ./internal/podman/ -run TestRealHostInfo -v`)
Expected: PASS on a host with a running podman. On a machine without podman, this is expected to be run by the CI integration job — note in the PR that it was validated there.

- [ ] **Step 7: Commit**

```bash
git add internal/podman/real.go internal/podman/real_ssh.go internal/podman/real_hostinfo_integration_test.go
git commit -m "feat(podman): real HostInfo via libpod info + df + /proc/loadavg (#30)"
```

---

## Task 9: OpenAPI doc + README touch

**Files:**
- Modify: `api/openapi.yaml`

- [ ] **Step 1: Document the new fields**

In `api/openapi.yaml`, find the host response schema used by `GET /hosts/{host}` and `GET /hosts`. Add to its properties:

```yaml
        container_count:
          type: integer
          description: Total containers across podman-api-managed pods on this host.
        load:
          type: object
          description: Point-in-time host resource snapshot. Present only when the host is reachable.
          properties:
            cpus: { type: integer }
            mem_total: { type: integer, format: int64, description: bytes }
            mem_free: { type: integer, format: int64, description: bytes }
            mem_used_pct: { type: number }
            cpu_pct: { type: number, description: "busy %, omitted when unavailable" }
            loadavg:
              type: array
              items: { type: number }
              minItems: 3
              maxItems: 3
              description: "1/5/15-minute load average; omitted when unavailable"
            disk:
              type: object
              properties:
                total: { type: integer, format: int64 }
                used: { type: integer, format: int64 }
                free: { type: integer, format: int64 }
                reclaimable: { type: integer, format: int64 }
```

(Match the existing indentation/style of the file. If the host schema is inline rather than a named component, add the same properties inline.)

- [ ] **Step 2: Validate the YAML parses**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run OpenAPI` (if an openapi-serving test exists), otherwise: `python3 -c "import yaml,sys; yaml.safe_load(open('api/openapi.yaml'))" && echo OK`.
Expected: OK / PASS.

- [ ] **Step 3: Commit**

```bash
git add api/openapi.yaml
git commit -m "docs(api): document container_count + load on host schema (#30)"
```

---

## Task 10: Open the PR

- [ ] **Step 1: Push and open PR**

```bash
git push -u origin feat/30-host-load
forgejo pr create tej/podman-api --head=feat/30-host-load --base=main \
  --title="feat(api): host inventory + load API (#30)" \
  --body="Phase 1 of #29. Adds container_count + a load object (cpus, mem, cpu%, disk, loadavg) to GET /hosts and /hosts/{id} so the client can compute evacuate placement.

- podman.Client gains HostInfo; fake gets a settable hook.
- Service.HostLoad + HostCounts (instances+containers in one sweep).
- hostView enriched; listHosts fetches hosts concurrently with a per-host timeout.
- real.go: libpod info + system df + best-effort /proc/loadavg (loadavg via local read for unix hosts, SSH exec for remote). Integration-tested.

Closes #30.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

- [ ] **Step 2: Watch CI**

Run: `forgejo actions list tej/podman-api --limit 3`
Expected: the push/PR runs go green (lint/test/build + integration). Report status.

---

## Self-review notes

- **Spec coverage:** CPUs+mem (Task 8 info.Host), cpu_pct (Task 8 CPUUtilization), disk (Task 8 Store + df), loadavg (Task 8 helper), container_count (Task 4), `/hosts` + `/hosts/{id}` shape (Task 5), concurrent list with timeout (Task 6), fake hook + per-metric absence + unknown-host (Tasks 2–5), real.go integration (Task 8), out-of-scope items untouched. All Phase 1 spec bullets map to a task.
- **Deviation from spec:** spec wrote `LoadAvg [3]float64` and `CPUPct *float64`; plan uses `LoadAvg *[3]float64` too, so both version-dependent metrics degrade by null-omission rather than reporting a misleading zero. `ContainerCount` moved out of `HostInfo` into a derived `HostCounts` (it's a podman-api concept, not a host resource — cleaner separation; matches how `instance_count` already works).
- **Risk:** the SSH-exec loadavg (Task 8) is the one genuinely new mechanism. It is isolated in `sshReadLoadAvg`, best-effort (nil on any error), and integration-only. Tasks 1–7 deliver the full feature minus remote loadavg and are independently mergeable if Task 8 needs iteration.
