# Background Inventory Poller Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep each host's inventory cache warm via a background poller so the operator UI/JSON reads serve instantly and a dead host never stalls the dashboard, with the UI painting immediately and self-correcting via htmx polling.

**Architecture:** Extend the existing per-host `instanceCache` with freshness metadata and a "warm" mode (entries never expire while a poller refreshes them; a failed refresh keeps last-known-good marked unreachable). A new `internal/inventory.Poller` (mirroring `internal/prune.Scheduler`) refreshes every host on an interval. The UI host page renders from the warm cache and self-corrects through a pollable htmx fragment showing a freshness cue.

**Tech Stack:** Go stdlib (`net/http.ServeMux`, `html/template`, `time`, `sync`, `context`); htmx (already vendored); `internal/podman/fake` for tests. Build tags required on every go command: `containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper`.

## Global Constraints

- **OSS repo only** (`github.com/iotready/podman-api`). No pro changes.
- **Always use the Makefile or the build tags.** Bare `go test`/`go build` fail on a clean machine. Use: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" <pkg>` or `make test` / `make vet` / `make build`.
- **TDD:** write the failing test first, watch it fail, implement minimally, watch it pass, commit.
- **Config idiom is CLI flags** (`fs.Duration`), not env — this is an OSS feature.
- **Freshness contract "A" (cache-always):** warm entries are served without touching podman; a failed refresh serves last-known-good marked unreachable; mutations invalidate as today.
- Branch: `feat/background-inventory-poller` (already created off `main`).
- Commit after each task. Co-author trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## File Structure

- `internal/instance/instancecache.go` — MODIFY: add freshness fields, `Freshness` type, warm mode, `getWithMeta`, `put`, `markUnreachable`, `beginRefresh`, `setWarm`; remove the old `get`.
- `internal/instance/instancecache_test.go` — MODIFY/CREATE: cache-level tests.
- `internal/instance/service.go` — MODIFY: add `RefreshHost`, `ListAllInstancesWithMeta`, `EnableWarmInventory`; repoint `ListAllInstances` at `getWithMeta`.
- `internal/instance/service_refresh_test.go` — CREATE: service-level wiring tests.
- `internal/inventory/poller.go` — CREATE: the `Poller` + `Refresher` interface.
- `internal/inventory/poller_test.go` — CREATE: poller tests with a fake refresher.
- `server/server.go` — MODIFY: two new flags, poller construction/start, `Wait()` on shutdown.
- `internal/ui/ui.go` — MODIFY: register the fragment route.
- `internal/ui/handlers_hosts.go` — MODIFY: `hostInstancesData` helper, `hostInstances` rework, new `hostInstancesFragment`, dashboard per-host timeout.
- `internal/ui/templates/host-instances.html` — MODIFY: split into `host-instances` (polling shell) + `host-instances-body` (freshness cue + table/skeleton).
- `internal/ui/handlers_hosts_test.go` — MODIFY: tests for fragment route + freshness markers + dashboard timeout.

---

### Task 1: Cache freshness model + warm mode

**Files:**
- Modify: `internal/instance/instancecache.go` (full rewrite of the 95-line file)
- Test: `internal/instance/instancecache_test.go`

**Interfaces:**
- Produces (for Task 2):
  - `type Freshness struct { FetchedAt time.Time; Reachable bool; HasData bool }` with method `Age() time.Duration`.
  - `(*instanceCache).getWithMeta(host string, fetch func() ([]Observed, error)) ([]Observed, Freshness, error)`
  - `(*instanceCache).beginRefresh(host string) uint64`
  - `(*instanceCache).put(host string, gen uint64, obs []Observed, at time.Time)`
  - `(*instanceCache).markUnreachable(host string, gen uint64)`
  - `(*instanceCache).setWarm(v bool)`
  - Existing `invalidate(host string)` unchanged.

- [ ] **Step 1: Write the failing tests**

Add to `internal/instance/instancecache_test.go` (create the file if it does not exist; keep the existing `package instance` and any current tests):

```go
package instance

import (
	"errors"
	"testing"
	"time"
)

func TestCacheWarmServesWithoutFetchAndIgnoresTTL(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }

	// Poller populates the entry.
	gen := c.beginRefresh("h")
	c.put("h", gen, []Observed{{Template: "t", Slug: "a"}}, base)

	// A much later read (well past ttl) still serves the warm entry with no fetch.
	c.now = func() time.Time { return base.Add(time.Hour) }
	fetched := false
	obs, fresh, err := c.getWithMeta("h", func() ([]Observed, error) {
		fetched = true
		return nil, nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if fetched {
		t.Fatal("warm hit must not fetch")
	}
	if len(obs) != 1 || obs[0].Slug != "a" {
		t.Fatalf("obs = %+v", obs)
	}
	if !fresh.Reachable || !fresh.HasData || !fresh.FetchedAt.Equal(base) {
		t.Fatalf("fresh = %+v", fresh)
	}
}

func TestCacheColdMissFetchesAndPopulates(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }

	calls := 0
	obs, fresh, err := c.getWithMeta("h", func() ([]Observed, error) {
		calls++
		return []Observed{{Slug: "x"}}, nil
	})
	if err != nil || calls != 1 || len(obs) != 1 {
		t.Fatalf("cold miss: obs=%+v calls=%d err=%v", obs, calls, err)
	}
	if !fresh.HasData || !fresh.Reachable {
		t.Fatalf("fresh = %+v", fresh)
	}
	// Second read is now a warm hit (no fetch).
	_, _, _ = c.getWithMeta("h", func() ([]Observed, error) {
		calls++
		return nil, nil
	})
	if calls != 1 {
		t.Fatalf("expected warm hit, calls=%d", calls)
	}
}

func TestCacheMarkUnreachableKeepsLastKnownGood(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }
	gen := c.beginRefresh("h")
	c.put("h", gen, []Observed{{Slug: "a"}}, base)

	// A later refresh fails.
	gen2 := c.beginRefresh("h")
	c.markUnreachable("h", gen2)

	obs, fresh, err := c.getWithMeta("h", func() ([]Observed, error) {
		t.Fatal("must not fetch when last-known-good exists")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(obs) != 1 || obs[0].Slug != "a" {
		t.Fatalf("obs = %+v (should keep last-known-good)", obs)
	}
	if fresh.Reachable {
		t.Fatal("fresh.Reachable should be false after markUnreachable")
	}
	if !fresh.HasData {
		t.Fatal("fresh.HasData should stay true (we still have data)")
	}
}

func TestCacheColdFetchErrorReturnsErrNoData(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	boom := errors.New("connection refused")
	obs, fresh, err := c.getWithMeta("h", func() ([]Observed, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if obs != nil || fresh.HasData || fresh.Reachable {
		t.Fatalf("cold error should be empty+unreachable: obs=%+v fresh=%+v", obs, fresh)
	}
}

func TestCachePutSkippedAfterInvalidate(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }

	// Refresh begins, captures gen.
	gen := c.beginRefresh("h")
	// A mutation invalidates before the refresh stores its (now stale) result.
	c.invalidate("h")
	c.put("h", gen, []Observed{{Slug: "stale"}}, base)

	// Entry must be cold (the stale put was dropped), so the next read fetches.
	fetched := false
	c.getWithMeta("h", func() ([]Observed, error) {
		fetched = true
		return []Observed{{Slug: "fresh"}}, nil
	})
	if !fetched {
		t.Fatal("stale put should have been dropped, forcing a cold fetch")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestCache' ./internal/instance/`
Expected: FAIL / build error — `getWithMeta`, `Freshness`, `put`, `markUnreachable`, `beginRefresh`, `setWarm` undefined.

- [ ] **Step 3: Rewrite `internal/instance/instancecache.go`**

Replace the entire file with:

```go
package instance

import (
	"sync"
	"time"
)

// instanceCache is a per-host cache over the (expensive) live podman sweep done
// by ListAllInstances. Two modes:
//
//   - lazy (default): entries expire after ttl; a stale read re-fetches. This is
//     the behaviour when no inventory poller runs (ttl>0) or caching is off
//     (ttl==0).
//   - warm (setWarm(true)): entries never expire on age — a background poller
//     keeps them fresh via put(); reads always serve the entry when present. A
//     failed refresh marks the entry unreachable but keeps its last-known-good
//     data. Cold misses still fetch synchronously (deduped) so the very first
//     read per host, or a read right after invalidate(), returns real data.
//
// Concurrent misses for the same host collapse into a single fetch (in-flight
// de-dup). Cached slices are treated read-only by callers.
type instanceCache struct {
	ttl  time.Duration
	warm bool
	now  func() time.Time

	mu       sync.Mutex
	data     map[string]instEntry
	inflight map[string]*instCall
	gen      map[string]uint64
}

type instEntry struct {
	obs       []Observed
	fetchedAt time.Time
	reachable bool
	hasData   bool
}

type instCall struct {
	wg    sync.WaitGroup
	obs   []Observed
	fresh Freshness
	err   error
}

// Freshness describes how current a returned inventory snapshot is. HasData is
// false only on a cold, never-populated (or failed cold) host; Reachable is
// false when the most recent refresh failed but stale data is still being
// served.
type Freshness struct {
	FetchedAt time.Time
	Reachable bool
	HasData   bool
}

// Age is how long ago the served snapshot was captured. Zero when there is no
// data.
func (f Freshness) Age() time.Duration {
	if f.FetchedAt.IsZero() {
		return 0
	}
	return time.Since(f.FetchedAt)
}

func newInstanceCache(ttl time.Duration) *instanceCache {
	return &instanceCache{
		ttl:      ttl,
		now:      time.Now,
		data:     make(map[string]instEntry),
		inflight: make(map[string]*instCall),
		gen:      make(map[string]uint64),
	}
}

// setWarm switches the cache into warm mode (see type doc). Called once at
// startup when the inventory poller is enabled, before traffic is served.
func (c *instanceCache) setWarm(v bool) {
	c.mu.Lock()
	c.warm = v
	c.mu.Unlock()
}

// getWithMeta is the cache-always read path. A present entry is returned
// directly when warm, or when still within ttl (lazy fallback). On a cold miss
// it performs the blocking fetch, de-duplicating concurrent callers, stores a
// successful result, and returns freshness metadata.
func (c *instanceCache) getWithMeta(host string, fetch func() ([]Observed, error)) ([]Observed, Freshness, error) {
	c.mu.Lock()
	if e, ok := c.data[host]; ok && (c.warm || (c.ttl > 0 && c.now().Sub(e.fetchedAt) < c.ttl)) {
		c.mu.Unlock()
		return e.obs, Freshness{FetchedAt: e.fetchedAt, Reachable: e.reachable, HasData: e.hasData}, nil
	}
	if call, ok := c.inflight[host]; ok {
		c.mu.Unlock()
		call.wg.Wait()
		return call.obs, call.fresh, call.err
	}
	call := &instCall{}
	call.wg.Add(1)
	c.inflight[host] = call
	gen := c.gen[host]
	c.mu.Unlock()

	obs, err := fetch()

	c.mu.Lock()
	delete(c.inflight, host)
	now := c.now()
	if err == nil && c.gen[host] == gen {
		c.data[host] = instEntry{obs: obs, fetchedAt: now, reachable: true, hasData: true}
	}
	call.obs = obs
	call.err = err
	if err == nil {
		call.fresh = Freshness{FetchedAt: now, Reachable: true, HasData: true}
	} else {
		call.fresh = Freshness{Reachable: false, HasData: false}
	}
	c.mu.Unlock()
	call.wg.Done()
	return call.obs, call.fresh, call.err
}

// beginRefresh captures the current generation for a host so a following put or
// markUnreachable can detect an invalidate() that raced with the live fetch.
func (c *instanceCache) beginRefresh(host string) uint64 {
	c.mu.Lock()
	g := c.gen[host]
	c.mu.Unlock()
	return g
}

// put stores a successful background refresh, guarded by gen so a refresh that
// raced with an invalidate() is dropped (read-your-writes for the mutator).
func (c *instanceCache) put(host string, gen uint64, obs []Observed, at time.Time) {
	c.mu.Lock()
	if c.gen[host] == gen {
		c.data[host] = instEntry{obs: obs, fetchedAt: at, reachable: true, hasData: true}
	}
	c.mu.Unlock()
}

// markUnreachable flags the host's last-known-good entry as stale after a failed
// refresh, preserving its data so reads still serve it. No-op when the entry is
// gone (cold — nothing to serve) or a mutation intervened (gen changed).
func (c *instanceCache) markUnreachable(host string, gen uint64) {
	c.mu.Lock()
	if e, ok := c.data[host]; ok && c.gen[host] == gen {
		e.reachable = false
		c.data[host] = e
	}
	c.mu.Unlock()
}

// invalidate drops the cached instance list for a host so the next read
// re-sweeps, and bumps the generation so an in-flight refresh's put is dropped.
func (c *instanceCache) invalidate(host string) {
	c.mu.Lock()
	delete(c.data, host)
	c.gen[host]++
	c.mu.Unlock()
}
```

- [ ] **Step 4: Repoint the old `get` caller (compile check)**

The old `get` method is removed. Find any remaining callers:

Run: `cd /home/tej/projects/podman-api && grep -rn '\.instCache\.get\b\|instCache.get(' internal/`
Expected: the only reference is in `service.go` `ListAllInstances` — Task 2 repoints it. If any OTHER caller exists, note it; it must move to `getWithMeta`. (There should be none.)

- [ ] **Step 5: Run the cache tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestCache' ./internal/instance/`
Expected: PASS. (The package as a whole will not build until Task 2 repoints `ListAllInstances`; run the package build in Task 2.)

- [ ] **Step 6: Commit**

```bash
cd /home/tej/projects/podman-api
git add internal/instance/instancecache.go internal/instance/instancecache_test.go
git commit -m "feat(instance): warm-mode inventory cache with freshness + last-known-good

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Service surface — RefreshHost, ListAllInstancesWithMeta, EnableWarmInventory

**Files:**
- Modify: `internal/instance/service.go` (`ListAllInstances` at ~line 594; add new methods nearby)
- Test: `internal/instance/service_refresh_test.go` (create)

**Interfaces:**
- Consumes (from Task 1): `getWithMeta`, `beginRefresh`, `put`, `markUnreachable`, `setWarm`, `Freshness`.
- Produces (for Tasks 3, 4, 5):
  - `(*Service).RefreshHost(ctx context.Context, host string) error`
  - `(*Service).ListAllInstancesWithMeta(ctx context.Context, host string) ([]Observed, Freshness, error)`
  - `(*Service).EnableWarmInventory()`
  - `(*Service).ListAllInstances` keeps its existing signature `(ctx, host) ([]Observed, error)`.

- [ ] **Step 1: Write the failing test**

Create `internal/instance/service_refresh_test.go`:

```go
package instance

import (
	"context"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func newRefreshSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	fc := fake.New()
	svc := NewService(fc, []config.Host{{ID: "h1"}})
	mem := store.NewMemory()
	if err := mem.PutTemplate(context.Background(), store.Template{
		Meta: render.Meta{ID: "tmpl-a"},
		Body: "apiVersion: v1\nkind: Pod\nmetadata:\n  name: tmpl-a\n",
	}); err != nil {
		t.Fatal(err)
	}
	svc.SetStore(mem)
	fc.AddPod("h1", podman.Pod{
		ID:   "tmpl-a-h1-x",
		Name: "tmpl-a-h1-x",
		Labels: map[string]string{
			"podman-api/template": "tmpl-a",
			"podman-api/slug":     "x",
		},
		Status: "Running",
	})
	return svc, fc
}

func TestRefreshHostWarmsCache(t *testing.T) {
	svc, _ := newRefreshSvc(t)
	svc.EnableWarmInventory()

	if err := svc.RefreshHost(context.Background(), "h1"); err != nil {
		t.Fatalf("RefreshHost: %v", err)
	}

	obs, fresh, err := svc.ListAllInstancesWithMeta(context.Background(), "h1")
	if err != nil {
		t.Fatalf("ListAllInstancesWithMeta: %v", err)
	}
	if len(obs) != 1 || obs[0].Slug != "x" {
		t.Fatalf("obs = %+v", obs)
	}
	if !fresh.Reachable || !fresh.HasData {
		t.Fatalf("fresh = %+v", fresh)
	}
}

func TestRefreshHostUnknownHostErrors(t *testing.T) {
	svc, _ := newRefreshSvc(t)
	if err := svc.RefreshHost(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for unknown host")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestRefreshHost' ./internal/instance/`
Expected: FAIL / build error — `EnableWarmInventory`, `RefreshHost`, `ListAllInstancesWithMeta` undefined.

- [ ] **Step 3: Add the service methods**

In `internal/instance/service.go`, replace the existing `ListAllInstances` method (currently at ~line 594-598) with the following block (keeps `ListAllInstances`, adds the three new methods; `time` and `context` are already imported):

```go
// ListAllInstances returns every podman-api-managed pod on a host across all
// known templates. The result is the union of List(host, t) for each catalog
// template id, so a pod for a template the daemon doesn't know about is
// silently omitted.
func (s *Service) ListAllInstances(ctx context.Context, host string) ([]Observed, error) {
	obs, _, err := s.instCache.getWithMeta(host, func() ([]Observed, error) {
		return s.listAllInstancesLive(ctx, host)
	})
	return obs, err
}

// ListAllInstancesWithMeta is ListAllInstances plus freshness metadata (when the
// data was captured and whether the host was reachable on the last refresh), for
// UI callers that render a staleness cue.
func (s *Service) ListAllInstancesWithMeta(ctx context.Context, host string) ([]Observed, Freshness, error) {
	return s.instCache.getWithMeta(host, func() ([]Observed, error) {
		return s.listAllInstancesLive(ctx, host)
	})
}

// EnableWarmInventory switches the instance cache into warm mode so entries are
// served without expiry and kept fresh by the inventory poller. Call once at
// startup, before serving traffic, when the poller is enabled.
func (s *Service) EnableWarmInventory() { s.instCache.setWarm(true) }

// RefreshHost performs a live inventory sweep of host and stores it in the warm
// cache. On failure it marks the host's last-known-good entry unreachable
// (keeping the data) and returns the error for the caller to log. This is the
// only proactive cache populator; the background poller calls it per tick.
func (s *Service) RefreshHost(ctx context.Context, host string) error {
	gen := s.instCache.beginRefresh(host)
	obs, err := s.listAllInstancesLive(ctx, host)
	if err != nil {
		s.instCache.markUnreachable(host, gen)
		return err
	}
	s.instCache.put(host, gen, obs, time.Now())
	return nil
}
```

- [ ] **Step 4: Build the package and run the tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/`
Expected: PASS (both the new tests and the whole existing `instance` package now compile and pass).

- [ ] **Step 5: Commit**

```bash
cd /home/tej/projects/podman-api
git add internal/instance/service.go internal/instance/service_refresh_test.go
git commit -m "feat(instance): RefreshHost + ListAllInstancesWithMeta + warm-mode toggle

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Background inventory poller

**Files:**
- Create: `internal/inventory/poller.go`
- Test: `internal/inventory/poller_test.go`

**Interfaces:**
- Consumes (from Task 2): `*instance.Service` satisfies `Refresher` via `RefreshHost`.
- Produces (for Task 4):
  - `type Refresher interface { RefreshHost(ctx context.Context, host string) error }`
  - `type Poller struct { Svc Refresher; Interval, Timeout time.Duration }`
  - `(*Poller).Start(ctx context.Context, hostsFn func() []string)`
  - `(*Poller).Wait()`

- [ ] **Step 1: Write the failing test**

Create `internal/inventory/poller_test.go`:

```go
package inventory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeRefresher struct {
	mu     sync.Mutex
	calls  map[string]int
	failOn map[string]bool
}

func newFakeRefresher() *fakeRefresher {
	return &fakeRefresher{calls: map[string]int{}, failOn: map[string]bool{}}
}

func (f *fakeRefresher) RefreshHost(ctx context.Context, host string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[host]++
	if f.failOn[host] {
		return errors.New("unreachable")
	}
	return nil
}

func (f *fakeRefresher) count(host string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[host]
}

func TestPollerImmediateFirstPassRefreshesAllHosts(t *testing.T) {
	f := newFakeRefresher()
	f.failOn["dead"] = true
	p := &Poller{Svc: f, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())

	p.Start(ctx, func() []string { return []string{"a", "b", "dead"} })

	// The immediate first pass should refresh every host within a moment; a
	// failing host must not block the others or panic the loop.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.count("a") >= 1 && f.count("b") >= 1 && f.count("dead") >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if f.count("a") == 0 || f.count("b") == 0 || f.count("dead") == 0 {
		t.Fatalf("not all hosts refreshed: a=%d b=%d dead=%d",
			f.count("a"), f.count("b"), f.count("dead"))
	}

	cancel()
	p.Wait() // must return promptly after cancel
}

func TestPollerStopsOnContextCancel(t *testing.T) {
	f := newFakeRefresher()
	p := &Poller{Svc: f, Interval: 10 * time.Millisecond, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"a"} })

	time.Sleep(50 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after cancel")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/inventory/`
Expected: FAIL / build error — package `inventory` / `Poller` does not exist.

- [ ] **Step 3: Create `internal/inventory/poller.go`**

```go
// Package inventory keeps each host's cached instance inventory warm by
// refreshing it on a schedule, so UI/API reads are served without a live podman
// sweep and an unreachable host never stalls a request.
package inventory

import (
	"context"
	"log"
	"sync"
	"time"
)

// Refresher refreshes one host's cached inventory. Implemented by
// *instance.Service.RefreshHost.
type Refresher interface {
	RefreshHost(ctx context.Context, host string) error
}

// Poller periodically refreshes every host's inventory into the service cache.
// It mirrors internal/prune.Scheduler: an immediate first pass so a fresh start
// warms within one cycle, per-tick panic recovery, a per-host timeout so one
// hung host can't bleed into the next cycle, and Wait() for clean shutdown.
type Poller struct {
	Svc      Refresher
	Interval time.Duration
	Timeout  time.Duration

	mu    sync.Mutex
	state map[string]bool // host -> last-known reachable, for transition logging
	wg    sync.WaitGroup
}

// Start launches the ticker loop until ctx is cancelled. hostsFn returns the
// current host ids on each tick (so SIGHUP host reloads are picked up).
func (p *Poller) Start(ctx context.Context, hostsFn func() []string) {
	p.mu.Lock()
	if p.state == nil {
		p.state = map[string]bool{}
	}
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		t := time.NewTicker(p.Interval)
		defer t.Stop()
		runTick := func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("inventory: poller tick panicked: %v", r)
				}
			}()
			p.tick(ctx, hostsFn())
		}
		runTick() // prompt first pass so a fresh start doesn't wait a full tick
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runTick()
			}
		}
	}()
}

// Wait blocks until the poller goroutine has exited after ctx cancellation.
func (p *Poller) Wait() { p.wg.Wait() }

// tick refreshes every host concurrently, each under its own timeout.
func (p *Poller) tick(ctx context.Context, hosts []string) {
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			hctx, cancel := context.WithTimeout(ctx, p.Timeout)
			defer cancel()
			err := p.Svc.RefreshHost(hctx, host)
			p.logTransition(host, err)
		}(h)
	}
	wg.Wait()
}

// logTransition logs only when a host's reachability changes (or is first seen
// unreachable), so a persistently-down host doesn't flood the log every tick.
func (p *Poller) logTransition(host string, err error) {
	reachable := err == nil
	p.mu.Lock()
	prev, seen := p.state[host]
	p.state[host] = reachable
	p.mu.Unlock()

	switch {
	case !seen && !reachable:
		log.Printf("inventory: host %s unreachable: %v", host, err)
	case seen && prev && !reachable:
		log.Printf("inventory: host %s unreachable: %v", host, err)
	case seen && !prev && reachable:
		log.Printf("inventory: host %s reachable again", host)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass (with the race detector)**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -race ./internal/inventory/`
Expected: PASS, no data races.

- [ ] **Step 5: Commit**

```bash
cd /home/tej/projects/podman-api
git add internal/inventory/
git commit -m "feat(inventory): background poller keeping the instance cache warm

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Server wiring + flags

**Files:**
- Modify: `server/server.go` (flags near line 84-92; wiring after the prune block ~line 253; shutdown near `pruneSched.Wait()` ~line 394; import block)

**Interfaces:**
- Consumes (from Tasks 2, 3): `svc.EnableWarmInventory()`, `inventory.Poller`, `svc.RefreshHost` (via the `Refresher` interface).

- [ ] **Step 1: Add the import**

In `server/server.go`, add to the import block (alongside the other `github.com/iotready/podman-api/internal/...` imports):

```go
	"github.com/iotready/podman-api/internal/inventory"
```

- [ ] **Step 2: Add the two flags**

In the flag declaration block (near lines 84-92, beside `pruneInterval`/`ingressInterval`), add:

```go
		inventoryInterval = fs.Duration("inventory-refresh-interval", 30*time.Second, "background inventory refresh cadence per host; 0 disables the poller (falls back to the lazy 3s cache)")
		inventoryTimeout  = fs.Duration("inventory-refresh-timeout", 20*time.Second, "per-host timeout for one background inventory refresh")
```

- [ ] **Step 3: Wire the poller**

In `RunWithFlags`, immediately AFTER the `if *pruneEnabled { ... }` block (ends ~line 253) and before the `if c.backupScheduler != nil {` block, add:

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

- [ ] **Step 4: Wait on shutdown**

Find the shutdown drain where `pruneSched.Wait()` is called (~line 393-394):

```go
		if pruneSched != nil {
			pruneSched.Wait()
		}
```

Add immediately after it:

```go
		if invPoller != nil {
			invPoller.Wait()
		}
```

- [ ] **Step 5: Build and vet**

Run: `cd /home/tej/projects/podman-api && make build && make vet`
Expected: both succeed, no output from vet.

- [ ] **Step 6: Run the server package tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./server/`
Expected: PASS (existing server tests still green; the poller defaults on at 30s and must not break startup).

- [ ] **Step 7: Commit**

```bash
cd /home/tej/projects/podman-api
git add server/server.go
git commit -m "feat(server): start the inventory poller (flags: -inventory-refresh-interval/-timeout)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: UI host page — freshness cue + pollable fragment

**Files:**
- Modify: `internal/ui/handlers_hosts.go` (`hostInstances`; add `hostInstancesData`, `hostInstancesFragment`)
- Modify: `internal/ui/ui.go` (register `GET /ui/hosts/{host}/fragment`)
- Modify: `internal/ui/templates/host-instances.html` (split into shell + body blocks)
- Test: `internal/ui/handlers_hosts_test.go`

**Interfaces:**
- Consumes (from Task 2): `svc.ListAllInstancesWithMeta(ctx, host) ([]instance.Observed, instance.Freshness, error)`.
- Produces: route `GET /ui/hosts/{host}/fragment` rendering the `host-instances-body` block.

**Note on rendering:** `u.render` returns a bare block for HX requests (`HX-Request: true`) and the layout-wrapped page otherwise (see `internal/ui/render.go`). The htmx poll sends `HX-Request`, so the fragment handler naturally returns just the body block.

- [ ] **Step 1: Write the failing tests**

Add to `internal/ui/handlers_hosts_test.go` (imports `strings`, `net/http` already present; `uiWithService`/`authedGet` helpers already exist):

```go
func TestHostPageHasPollingFragmentAndFreshnessCue(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui/hosts/edge-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	// The data region polls the fragment endpoint.
	if !strings.Contains(body, `hx-get="/ui/hosts/edge-1/fragment"`) {
		t.Errorf("host page missing polling fragment hx-get\n%s", body)
	}
	if !strings.Contains(body, `hx-trigger="every 10s"`) {
		t.Errorf("host page missing 10s poll trigger\n%s", body)
	}
	// Freshness cue is present on a reachable host (edge-1 is up in the fake).
	if !strings.Contains(body, "updated") {
		t.Errorf("host page missing freshness cue\n%s", body)
	}
}

func TestHostInstancesFragmentReturnsBareBody(t *testing.T) {
	u := uiWithService(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	r := httptest.NewRequest("GET", "/ui/hosts/edge-1/fragment", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	r.Header.Set("HX-Request", "true") // htmx poll
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "<!DOCTYPE html>") {
		t.Errorf("fragment must not include the layout\n%s", body)
	}
	if !strings.Contains(body, `class="tbl"`) {
		t.Errorf("fragment should contain the instance table\n%s", body)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestHostPageHasPollingFragment|TestHostInstancesFragment' ./internal/ui/`
Expected: FAIL — route `/ui/hosts/{host}/fragment` unregistered (404) and the polling attributes absent.

- [ ] **Step 3: Rework the host handlers**

In `internal/ui/handlers_hosts.go`, ensure the imports include `context` and `errors` and the `instance` package (do NOT add `time` yet — nothing in this task uses it; Task 6 adds it):

```go
import (
	"context"
	"errors"
	"net/http"
	"sort"
	"sync"

	"github.com/iotready/podman-api/internal/instance"
)
```

Replace the existing `hostInstances` function with:

```go
// hostInstancesData builds the view model shared by the full host page and its
// pollable fragment. It never returns a hard error for an unreachable host —
// stale data (or an empty "unreachable" panel) is surfaced instead, per the
// warm-cache serve-last-known-good contract. A genuinely unknown host still
// errors so the caller can 404.
func (u *UI) hostInstancesData(ctx context.Context, host string) (map[string]any, error) {
	obs, fresh, err := u.cfg.Svc.ListAllInstancesWithMeta(ctx, host)
	if err != nil && errors.Is(err, instance.ErrUnknownHost) {
		return nil, err
	}
	return map[string]any{
		"Host":        host,
		"ActiveHost":  host,
		"Instances":   obs,
		"AgeSeconds":  int(fresh.Age().Seconds()),
		"Unreachable": !fresh.Reachable,
		"Cold":        !fresh.HasData,
	}, nil
}

func (u *UI) hostInstances(w http.ResponseWriter, r *http.Request) {
	data, err := u.hostInstancesData(r.Context(), r.PathValue("host"))
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "host-instances", u.pageData(data))
}

// hostInstancesFragment renders just the instance table + freshness cue for the
// htmx poll on the host page. render() returns a bare block for HX requests.
func (u *UI) hostInstancesFragment(w http.ResponseWriter, r *http.Request) {
	data, err := u.hostInstancesData(r.Context(), r.PathValue("host"))
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "host-instances-body", u.pageData(data))
}
```


- [ ] **Step 4: Register the fragment route**

In `internal/ui/ui.go` `Handler()`, immediately after the `mux.Handle("GET /ui/hosts/{host}", guard(u.hostInstances))` line, add:

```go
	mux.Handle("GET /ui/hosts/{host}/fragment", guard(u.hostInstancesFragment))
```

- [ ] **Step 5: Split the template**

Replace the entire contents of `internal/ui/templates/host-instances.html` with:

```html
{{define "host-instances"}}
<div class="phead"><div><h1>{{.Host}}</h1><div class="sub">{{len .Instances}} instance(s)</div></div></div>
<div class="card">
  <div class="card-h">
    <span class="t">{{.Host}} · instances</span>
    <span class="sp"></span>
    <a class="btn primary sm" href="/ui/hosts/{{.Host}}/deploy" hx-get="/ui/hosts/{{.Host}}/deploy" hx-target="#main" hx-push-url="true" hx-disabled-elt="this">+ Deploy</a>
  </div>
  <div id="hi-body" hx-get="/ui/hosts/{{.Host}}/fragment" hx-trigger="every 10s" hx-swap="innerHTML">
    {{template "host-instances-body" .}}
  </div>
</div>
{{end}}

{{define "host-instances-body"}}
<div class="freshness sub">
  {{if .Unreachable}}
    <span class="badge warn"><span class="dot"></span>unreachable{{if not .Cold}} — last seen {{.AgeSeconds}}s ago{{end}}</span>
  {{else if not .Cold}}
    <span class="mut">updated {{.AgeSeconds}}s ago</span>
  {{end}}
</div>
{{if .Cold}}
  <div class="skeleton mut">Loading inventory…</div>
{{else}}
  <table class="tbl">
    <thead><tr><th>Template / Slug</th><th>Status</th></tr></thead>
    <tbody>
    {{range .Instances}}
      <tr hx-get="/ui/hosts/{{$.Host}}/instances/{{.Template}}/{{.Slug}}" hx-target="#main" hx-push-url="true" preload="mouseover">
        <td class="idcell" data-label="Template/Slug"><span class="tmpl">{{.Template}}</span> <span class="mut">/</span> <span class="slug">{{.Slug}}</span></td>
        <td data-label="Status">
          {{if eq .Pod.Status "Running"}}
            {{if .Ready}}<span class="badge ok"><span class="dot"></span>{{.Pod.Status}}</span>{{else}}<span class="badge warn"><span class="dot"></span>{{.Pod.Status}}</span>{{end}}
          {{else}}
            <span class="badge muted"><span class="dot"></span>{{.Pod.Status}}</span>
          {{end}}
          <span class="go">→</span>
        </td>
      </tr>
    {{end}}
    </tbody>
  </table>
{{end}}
{{end}}
```

- [ ] **Step 6: Run the UI tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/`
Expected: PASS — the two new tests plus all existing UI tests (including the drift/render tests). If an existing test asserted the old single-block `host-instances` markup, update it to match the new structure (the instance rows and status badges are unchanged, only wrapped in `#hi-body`).

- [ ] **Step 7: Commit**

```bash
cd /home/tej/projects/podman-api
git add internal/ui/handlers_hosts.go internal/ui/ui.go internal/ui/templates/host-instances.html internal/ui/handlers_hosts_test.go
git commit -m "feat(ui): host page paints from warm cache + self-correcting htmx fragment

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Dashboard per-host timeout bound

**Files:**
- Modify: `internal/ui/handlers_hosts.go` (`dashboard`)
- Test: `internal/ui/handlers_hosts_test.go`

**Interfaces:**
- Consumes: `svc.ListAllInstances` (warm cache).

**Why:** The dashboard fans out one goroutine per host with no per-host timeout, so a cold/unreachable host can stall the whole page up to the 10-minute op timeout. Warm reads are instant; this only bounds the cold/dead case (the JSON `/hosts` path already bounds at 5s).

- [ ] **Step 1: Write the failing test**

Add to `internal/ui/handlers_hosts_test.go`:

```go
func TestDashboardBoundsPerHostFetch(t *testing.T) {
	// A dashboard render must complete quickly even if a host's fetch would
	// otherwise hang far longer than the per-host bound. We assert the bound
	// constant is small; the render itself is exercised by existing dashboard
	// tests. This guards against the bound being removed or set too high.
	if dashboardHostTimeout <= 0 || dashboardHostTimeout > 10*time.Second {
		t.Fatalf("dashboardHostTimeout = %s, want a small positive bound", dashboardHostTimeout)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestDashboardBoundsPerHostFetch' ./internal/ui/`
Expected: FAIL / build error — `dashboardHostTimeout` undefined.

- [ ] **Step 3: Add the bound and apply it**

In `internal/ui/handlers_hosts.go`, add the constant above `dashboard` (and confirm `context` and `time` are imported — `context` from Task 5, add `time` now):

```go
// dashboardHostTimeout bounds each per-host instance fetch on the dashboard
// fan-out so one cold or unreachable host can't stall the whole page (warm
// reads return instantly; this only bites on a cold/dead host). Mirrors the
// JSON /hosts path's 5s per-host bound.
const dashboardHostTimeout = 5 * time.Second
```

In the `dashboard` goroutine, replace the `ListAllInstances(r.Context(), id)` call with a per-host timeout context. The goroutine body becomes:

```go
			go func(i int, id string) {
				defer wg.Done()
				hctx, cancel := context.WithTimeout(r.Context(), dashboardHostTimeout)
				defer cancel()
				n := 0
				if obs, err := u.cfg.Svc.ListAllInstances(hctx, id); err == nil {
					n = len(obs)
				}
				summaries[i] = hostSummary{ID: id, Instances: n}
			}(i, h.ID)
```

- [ ] **Step 4: Run the UI tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/`
Expected: PASS (the new bound test plus all existing dashboard tests).

- [ ] **Step 5: Commit**

```bash
cd /home/tej/projects/podman-api
git add internal/ui/handlers_hosts.go internal/ui/handlers_hosts_test.go
git commit -m "feat(ui): bound dashboard per-host fetch so a dead host can't stall the page

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Full test suite**

Run: `cd /home/tej/projects/podman-api && make test`
Expected: all packages PASS.

- [ ] **Step 2: Vet + build**

Run: `cd /home/tej/projects/podman-api && make vet && make build`
Expected: vet clean, binary builds.

- [ ] **Step 3: Race check on the new concurrent code**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -race ./internal/inventory/ ./internal/instance/ ./internal/ui/`
Expected: PASS, no races.

- [ ] **Step 4: Confirm the full branch diff is coherent**

Run: `cd /home/tej/projects/podman-api && git log --oneline main..HEAD && git diff --stat main..HEAD`
Expected: 6 feature commits (Tasks 1-6) touching only the files listed in this plan.

---

## Post-plan (out of scope for the task loop; done by the orchestrator after review)

OSS release + pro rollout, following the standard pipeline:
1. OSS PR (`feat/background-inventory-poller` → `main`), CI green, merge.
2. Tag next OSS release; push tag to Forgejo + GitHub.
3. `make bump V=<tag>` in `podman-api-pro`, `make build && make test`, PR, merge.
4. `make build-linux`, back up the live binary, deploy to engine-infra, restart, verify.
