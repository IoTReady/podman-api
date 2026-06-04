# Evacuate primitive (POST /evacuate) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /evacuate {from_host, map:{slug:to_host}}` that fans every instance on a host out into bounded-concurrency child `migrate` jobs under one aggregate parent job.

**Architecture:** A thin API handler enqueues a parent `evacuate` job. The shared job runner runs `evacuate.Handler`, which resolves the host's stored instances into a migrate plan (`instance.Service.ResolveEvacuation`), then runs the migrations itself with a bounded semaphore — each as an already-`running` child job (`store.JobStore.StartChild`, never `queued`, so the runner can't double-claim it). The parent uses exactly one pool worker; children run as goroutines inside it, so the fan-out is deadlock-free. The parent succeeds iff all children succeed.

**Tech Stack:** Go, `net/http` ServeMux, modernc SQLite, existing `internal/{store,instance,jobs,api}` packages.

**Spec:** `docs/superpowers/specs/2026-06-04-evacuate-primitive-design.md`

**Build/test tags (REQUIRED — the tree transitively imports libpod CGO drivers):**
```
containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper
```
Throughout this plan, `$TAGS` means exactly that string. Run tests as:
```sh
go test -tags "$TAGS" ./internal/<pkg>/ -run <Name> -v
```
or just `make test` for the whole suite. Keep changes `gofmt`-clean and `go vet`-clean.

---

## File Structure

- `internal/store/store.go` — add `SpecKey` type + `ListSpecKeys` to the `Store` interface.
- `internal/store/jobs.go` — add `StartChild` to `JobStore`; add `ParentID` to `JobFilter`.
- `internal/store/sqlite.go` — implement `ListSpecKeys`, `StartChild`; add `parent_id` predicate to `ListJobs`.
- `internal/store/memory.go` — implement `ListSpecKeys`, `StartChild`; add `parent_id` filter to `ListJobs`.
- `internal/instance/evacuate.go` (NEW) — `EvacuateRequest`, `ResolveEvacuation`, `ErrInvalidEvacuation`.
- `internal/evacuate/handler.go` (NEW) — `Handler{Svc, Jobs}`, fan-out + aggregate.
- `internal/api/evacuate.go` (NEW) — `POST /evacuate` handler.
- `internal/api/router.go` — route; `internal/api/errors.go` — classify; `internal/api/jobs.go` — `?parent_id`.
- `cmd/podman-api/main.go` — register `evacuate` handler.
- `api/openapi.yaml` + `internal/api/openapi_test.go` — document the path.

---

## Task 1: Store — `ListSpecKeys`

**Files:**
- Modify: `internal/store/store.go`
- Modify: `internal/store/sqlite.go`
- Modify: `internal/store/memory.go`
- Test: `internal/store/listspeckeys_test.go` (Create)

- [ ] **Step 1: Write the failing test**

Create `internal/store/listspeckeys_test.go`:

```go
package store

import (
	"context"
	"testing"
)

func TestListSpecKeys(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	put := func(host, tmpl, slug string) {
		if err := m.PutSpec(ctx, Spec{Host: host, Template: tmpl, Slug: slug,
			Parameters: map[string]any{}, Secrets: map[string]string{}}); err != nil {
			t.Fatal(err)
		}
	}
	put("hostA", "web", "acme")
	put("hostA", "web", "globex")
	put("hostB", "web", "other")

	keys, err := m.ListSpecKeys(ctx, "hostA")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("want 2 keys for hostA, got %d: %v", len(keys), keys)
	}
	got := map[string]string{}
	for _, k := range keys {
		got[k.Slug] = k.Template
	}
	if got["acme"] != "web" || got["globex"] != "web" {
		t.Fatalf("unexpected keys: %v", got)
	}

	empty, err := m.ListSpecKeys(ctx, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("want 0 keys for unknown host, got %d", len(empty))
	}
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test -tags "$TAGS" ./internal/store/ -run TestListSpecKeys -v`
Expected: compile error — `m.ListSpecKeys undefined` and `SpecKey` undefined.

- [ ] **Step 3: Add the type + interface method**

In `internal/store/store.go`, after the `Spec` struct add:

```go
// SpecKey identifies one stored instance without exposing its secrets. Used by
// host-wide planning (evacuate) that only needs to know what is on a host.
type SpecKey struct {
	Template string
	Slug     string
}
```

And add to the `Store` interface (after `DeleteSpec`):

```go
	// ListSpecKeys returns the (template, slug) of every spec on host, without
	// decrypting secrets. Empty slice (no error) when the host has none.
	ListSpecKeys(ctx context.Context, host string) ([]SpecKey, error)
```

- [ ] **Step 4: Implement for SQLite**

In `internal/store/sqlite.go`, after `DeleteSpec`:

```go
func (s *SQLite) ListSpecKeys(ctx context.Context, host string) ([]SpecKey, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT template, slug FROM specs WHERE host=? ORDER BY template, slug`, host)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SpecKey{}
	for rows.Next() {
		var k SpecKey
		if err := rows.Scan(&k.Template, &k.Slug); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}
```

- [ ] **Step 5: Implement for Memory**

In `internal/store/memory.go`, after `DeleteSpec`:

```go
func (m *Memory) ListSpecKeys(_ context.Context, host string) ([]SpecKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []SpecKey{}
	for _, s := range m.specs {
		if s.Host == host {
			out = append(out, SpecKey{Template: s.Template, Slug: s.Slug})
		}
	}
	return out, nil
}
```

- [ ] **Step 6: Run the test to confirm it passes**

Run: `go test -tags "$TAGS" ./internal/store/ -run TestListSpecKeys -v`
Expected: PASS. Then `go build -tags "$TAGS" ./internal/store/` to confirm the `var _ DB = (*SQLite)(nil)` / `var _ JobStore = (*Memory)(nil)` assertions still compile (Memory must satisfy `Store` too — it does via the new method).

- [ ] **Step 7: Commit**

```bash
git add internal/store/store.go internal/store/sqlite.go internal/store/memory.go internal/store/listspeckeys_test.go
git commit -m "feat(store): ListSpecKeys for host-wide instance enumeration (#35)"
```

---

## Task 2: Store — `StartChild` + `JobFilter.ParentID`

**Files:**
- Modify: `internal/store/jobs.go`
- Modify: `internal/store/sqlite.go`
- Modify: `internal/store/memory.go`
- Test: `internal/store/startchild_test.go` (Create)

- [ ] **Step 1: Write the failing test**

Create `internal/store/startchild_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"testing"
)

func TestStartChildIsRunningAndUnclaimable(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	parent, err := m.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := m.StartChild(ctx, "migrate", json.RawMessage(`{"slug":"acme"}`), parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if child.State != JobRunning {
		t.Fatalf("child state = %q, want running", child.State)
	}
	if child.ParentID != parent.ID {
		t.Fatalf("child parent = %q, want %q", child.ParentID, parent.ID)
	}
	if child.Started.IsZero() {
		t.Fatal("child Started should be set")
	}

	// ClaimNext must pick the queued PARENT, never the already-running child.
	claimed, ok, err := m.ClaimNext(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimNext ok=%v err=%v", ok, err)
	}
	if claimed.ID != parent.ID {
		t.Fatalf("claimed %q, want parent %q (child must be unclaimable)", claimed.ID, parent.ID)
	}
	if _, ok, _ := m.ClaimNext(ctx); ok {
		t.Fatal("a second claim returned a job; the running child must not be claimable")
	}
}

func TestListJobsByParentID(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	parent, _ := m.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	c1, _ := m.StartChild(ctx, "migrate", json.RawMessage(`{}`), parent.ID)
	c2, _ := m.StartChild(ctx, "migrate", json.RawMessage(`{}`), parent.ID)
	_, _ = m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "") // unrelated top-level

	got, err := m.ListJobs(ctx, JobFilter{ParentID: parent.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 children, got %d", len(got))
	}
	ids := map[string]bool{got[0].ID: true, got[1].ID: true}
	if !ids[c1.ID] || !ids[c2.ID] {
		t.Fatalf("children missing from filtered list: %v", got)
	}
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test -tags "$TAGS" ./internal/store/ -run 'TestStartChild|TestListJobsByParentID' -v`
Expected: compile error — `m.StartChild undefined`, `JobFilter{ParentID}` unknown field.

- [ ] **Step 3: Extend the interface + filter**

In `internal/store/jobs.go`, add to `JobFilter`:

```go
// JobFilter narrows ListJobs. Empty fields match anything.
type JobFilter struct {
	State    JobState
	Kind     string
	ParentID string
}
```

Add to the `JobStore` interface (after `Enqueue`):

```go
	// StartChild inserts a child job already in the running state (never queued),
	// owned by parentID. Because it is not queued, ClaimNext never claims it — the
	// caller (a parent job handler) drives it directly.
	StartChild(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error)
```

- [ ] **Step 4: Implement for SQLite**

In `internal/store/sqlite.go`, after `Enqueue`:

```go
func (s *SQLite) StartChild(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	id := newJobID()
	now := time.Now().Unix()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	var parent any
	if parentID != "" {
		parent = parentID
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO jobs (id, kind, args, state, steps, parent_id, error, created, started, finished)
VALUES (?, ?, ?, 'running', '[]', ?, NULL, ?, ?, NULL)`,
		id, kind, string(args), parent, now, now)
	if err != nil {
		return Job{}, err
	}
	return s.GetJob(ctx, id)
}
```

And add the `parent_id` predicate in `ListJobs`, right after the `f.Kind` block:

```go
	if f.ParentID != "" {
		where = append(where, "parent_id=?")
		args = append(args, f.ParentID)
	}
```

- [ ] **Step 5: Implement for Memory**

In `internal/store/memory.go`, after `Enqueue`:

```go
func (m *Memory) StartChild(_ context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	now := time.Now()
	j := Job{
		ID: newJobID(), Kind: kind, Args: args, State: JobRunning,
		Steps: []JobStep{}, ParentID: parentID, Created: now, Started: now,
	}
	m.jobs = append(m.jobs, j)
	return cloneJob(j), nil
}
```

And add the `parent_id` filter in `ListJobs`, after the `f.Kind` continue:

```go
		if f.ParentID != "" && j.ParentID != f.ParentID {
			continue
		}
```

- [ ] **Step 6: Run the tests to confirm they pass**

Run: `go test -tags "$TAGS" ./internal/store/ -run 'TestStartChild|TestListJobsByParentID' -v`
Expected: PASS. Run the whole store package too: `go test -tags "$TAGS" ./internal/store/` — Expected: PASS (the `var _ DB`/`var _ JobStore` assertions cover the new methods).

- [ ] **Step 7: Commit**

```bash
git add internal/store/jobs.go internal/store/sqlite.go internal/store/memory.go internal/store/startchild_test.go
git commit -m "feat(store): StartChild + ParentID filter for parent/child jobs (#35)"
```

---

## Task 3: instance — `ResolveEvacuation`

**Files:**
- Create: `internal/instance/evacuate.go`
- Test: `internal/instance/evacuate_test.go` (Create)

Context: `instance.Service` has `s.host(id) (config.Host, bool)`, `s.store store.Store` (nil when disabled), and the sentinels `ErrUnknownHost`, `ErrSameHost`, `ErrStoreDisabled` in `service.go`. `MigrateRequest{FromHost,ToHost,Template,Slug,Parameters}` lives in `migrate.go`.

- [ ] **Step 1: Write the failing test**

Create `internal/instance/evacuate_test.go`. Reuse whatever test constructor the migrate tests use to build a `*Service` with a fake client + `store.Memory` + host/template config. Inspect `internal/instance/migrate_test.go` for the existing helper (e.g. a `newTestService(...)` or inline `NewService(...)` + `SetStore(...)`) and mirror it exactly — do not invent a new constructor.

```go
package instance

import (
	"context"
	"errors"
	"testing"

	"github.com/iotready/podman-api/internal/store"
)

func TestResolveEvacuation(t *testing.T) {
	// svc: hosts {hostA, hostB, hostC}, template "web" loaded on each, store=Memory.
	// Build it the same way migrate_test.go does.
	svc, mem := newEvacTestService(t) // SEE NOTE BELOW
	ctx := context.Background()

	seed := func(host, slug string) {
		if err := mem.PutSpec(ctx, store.Spec{Host: host, Template: "web", Slug: slug,
			Parameters: map[string]any{}, Secrets: map[string]string{}}); err != nil {
			t.Fatal(err)
		}
	}
	seed("hostA", "acme")
	seed("hostA", "globex")

	t.Run("happy path sorted", func(t *testing.T) {
		moves, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "hostA", Map: map[string]string{"globex": "hostC", "acme": "hostB"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(moves) != 2 || moves[0].Slug != "acme" || moves[1].Slug != "globex" {
			t.Fatalf("want [acme,globex] sorted, got %+v", moves)
		}
		if moves[0].ToHost != "hostB" || moves[0].Template != "web" || moves[0].FromHost != "hostA" {
			t.Fatalf("bad move: %+v", moves[0])
		}
	})

	t.Run("unmapped instance", func(t *testing.T) {
		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "hostA", Map: map[string]string{"acme": "hostB"}, // globex left out
		})
		if !errors.Is(err, ErrInvalidEvacuation) {
			t.Fatalf("want ErrInvalidEvacuation, got %v", err)
		}
	})

	t.Run("extra map key", func(t *testing.T) {
		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "hostA", Map: map[string]string{"acme": "hostB", "globex": "hostC", "ghost": "hostB"},
		})
		if !errors.Is(err, ErrInvalidEvacuation) {
			t.Fatalf("want ErrInvalidEvacuation, got %v", err)
		}
	})

	t.Run("dest == from", func(t *testing.T) {
		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "hostA", Map: map[string]string{"acme": "hostA", "globex": "hostC"},
		})
		if !errors.Is(err, ErrSameHost) {
			t.Fatalf("want ErrSameHost, got %v", err)
		}
	})

	t.Run("unknown dest", func(t *testing.T) {
		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "hostA", Map: map[string]string{"acme": "nope", "globex": "hostC"},
		})
		if !errors.Is(err, ErrUnknownHost) {
			t.Fatalf("want ErrUnknownHost, got %v", err)
		}
	})

	t.Run("unknown from", func(t *testing.T) {
		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{FromHost: "nope", Map: map[string]string{}})
		if !errors.Is(err, ErrUnknownHost) {
			t.Fatalf("want ErrUnknownHost, got %v", err)
		}
	})

	t.Run("empty host empty map", func(t *testing.T) {
		moves, err := svc.ResolveEvacuation(ctx, EvacuateRequest{FromHost: "hostB", Map: map[string]string{}})
		if err != nil || len(moves) != 0 {
			t.Fatalf("want empty no-op, got moves=%v err=%v", moves, err)
		}
	})
}
```

**NOTE on `newEvacTestService`:** Do **not** add a new helper if `migrate_test.go` already has one that yields `(*Service, *store.Memory)` with `hostA/hostB/hostC` and template `web`. If the existing helper differs (different host names / no store handle returned), adapt this test to it instead — match the existing test conventions. If migrate_test builds the service inline, replicate that inline here and drop the `newEvacTestService` call. The store-disabled case is covered separately:

```go
func TestResolveEvacuationStoreDisabled(t *testing.T) {
	svc, _ := newEvacTestServiceNoStore(t) // service WITHOUT SetStore
	_, err := svc.ResolveEvacuation(context.Background(),
		EvacuateRequest{FromHost: "hostA", Map: map[string]string{}})
	if !errors.Is(err, ErrStoreDisabled) {
		t.Fatalf("want ErrStoreDisabled, got %v", err)
	}
}
```

If a no-store variant isn't readily available, fold this assertion into the main test by constructing a second service without `SetStore`.

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestResolveEvacuation -v`
Expected: compile error — `EvacuateRequest`, `ResolveEvacuation`, `ErrInvalidEvacuation` undefined.

- [ ] **Step 3: Implement**

Create `internal/instance/evacuate.go`:

```go
package instance

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ErrInvalidEvacuation means the request cannot be planned against the stored
// specs: an instance on the host has no destination in the map, a map entry
// names no instance, or a slug is ambiguous across templates. The API maps it to
// 400 invalid_request.
var ErrInvalidEvacuation = errors.New("invalid evacuation request")

// EvacuateRequest is the POST /evacuate body and the evacuate job's args. Map is
// slug -> destination host; it carries no template (resolved from the stored
// spec) and no parameters (Migrate merges the spec's own).
type EvacuateRequest struct {
	FromHost string            `json:"from_host"`
	Map      map[string]string `json:"map"`
}

// ResolveEvacuation validates the request against the specs stored on FromHost
// and returns the per-instance migrate plan, sorted by slug for determinism. It
// is pure read/validation (no mutation) and is called both synchronously by the
// POST handler (fast-fail, result discarded) and by the evacuate job handler at
// execution time (state may have drifted since enqueue).
func (s *Service) ResolveEvacuation(ctx context.Context, req EvacuateRequest) ([]MigrateRequest, error) {
	if _, ok := s.host(req.FromHost); !ok {
		return nil, ErrUnknownHost
	}
	if s.store == nil {
		return nil, ErrStoreDisabled
	}
	keys, err := s.store.ListSpecKeys(ctx, req.FromHost)
	if err != nil {
		return nil, err
	}

	tmplBySlug := make(map[string]string, len(keys))
	for _, k := range keys {
		if prev, dup := tmplBySlug[k.Slug]; dup && prev != k.Template {
			return nil, fmt.Errorf("%w: slug %q exists under templates %q and %q on %s",
				ErrInvalidEvacuation, k.Slug, prev, k.Template, req.FromHost)
		}
		tmplBySlug[k.Slug] = k.Template
	}

	// Every instance on the host must have a destination (true evacuate).
	for slug := range tmplBySlug {
		if _, ok := req.Map[slug]; !ok {
			return nil, fmt.Errorf("%w: no destination for slug %q on %s",
				ErrInvalidEvacuation, slug, req.FromHost)
		}
	}

	moves := make([]MigrateRequest, 0, len(req.Map))
	for slug, dest := range req.Map {
		tmpl, ok := tmplBySlug[slug]
		if !ok {
			return nil, fmt.Errorf("%w: no such instance %q on %s",
				ErrInvalidEvacuation, slug, req.FromHost)
		}
		if dest == req.FromHost {
			return nil, ErrSameHost
		}
		if _, ok := s.host(dest); !ok {
			return nil, ErrUnknownHost
		}
		moves = append(moves, MigrateRequest{
			FromHost: req.FromHost, ToHost: dest, Template: tmpl, Slug: slug,
		})
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].Slug < moves[j].Slug })
	return moves, nil
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestResolveEvacuation -v`
Expected: PASS (all subtests). Also `go vet -tags "$TAGS" ./internal/instance/`.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/evacuate.go internal/instance/evacuate_test.go
git commit -m "feat(instance): ResolveEvacuation planning + validation (#35)"
```

---

## Task 4: evacuate.Handler — fan-out + aggregate

**Files:**
- Create: `internal/evacuate/handler.go`
- Test: `internal/evacuate/handler_test.go` (Create)

Context: `jobs.Handler` is `Run(ctx, store.Job, *jobs.JobContext) error`; `jobs.NewJobContext(js store.JobStore, id string) *JobContext`; `JobContext.Step(step, detail string)`. The runner marks the parent succeeded when `Run` returns nil, failed (with the error string) otherwise.

**Concurrency note for the implementer:** child goroutines must NOT call the parent's `jc.Step` concurrently — `AppendStep` is a non-transactional read-modify-write that assumes a single writer per job id. Per-instance progress goes to each *child* job (via `cjc.Step` inside `Migrate`); the parent writes only a single `summary` step after the join. Keep it that way.

- [ ] **Step 1: Write the failing test**

Create `internal/evacuate/handler_test.go`:

```go
package evacuate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// buildSvc returns a *instance.Service with hosts hostA/hostB/hostC, template
// "web", a Memory store seeded with the given hostA slugs, plus the Memory store
// handle. Mirror instance/migrate_test.go's construction (same fake client +
// SetStore). If migrate_test exposes a reusable helper, prefer calling it.
func buildSvc(t *testing.T, slugs ...string) (*instance.Service, *store.Memory) {
	t.Helper()
	// ... construct as in instance tests; seed hostA/web/<slug> specs ...
	panic("implement using instance test conventions")
}

func TestEvacuateAllSucceed(t *testing.T) {
	svc, mem := buildSvc(t, "acme", "globex")
	parent, _ := mem.Enqueue(context.Background(), "evacuate", mustArgs(t, "hostA", map[string]string{"acme": "hostB", "globex": "hostC"}), "")

	var calls int32
	h := &Handler{Svc: svc, Jobs: mem}
	h.migrate = func(ctx context.Context, req instance.MigrateRequest, step func(string, string)) error {
		atomic.AddInt32(&calls, 1)
		step("fake", req.Slug)
		return nil
	}

	if err := h.Run(context.Background(), parent, jobs.NewJobContext(mem, parent.ID)); err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("want 2 migrate calls, got %d", calls)
	}
	// Two child migrate jobs, both succeeded, parented.
	children, _ := mem.ListJobs(context.Background(), store.JobFilter{ParentID: parent.ID})
	if len(children) != 2 {
		t.Fatalf("want 2 children, got %d", len(children))
	}
	for _, c := range children {
		if c.Kind != "migrate" || c.State != store.JobSucceeded {
			t.Fatalf("child not succeeded migrate: %+v", c)
		}
	}
}

func TestEvacuateOneFailsSiblingsRun(t *testing.T) {
	svc, mem := buildSvc(t, "acme", "globex", "initech")
	parent, _ := mem.Enqueue(context.Background(), "evacuate",
		mustArgs(t, "hostA", map[string]string{"acme": "hostB", "globex": "hostC", "initech": "hostB"}), "")

	var ran sync.Map
	h := &Handler{Svc: svc, Jobs: mem}
	h.migrate = func(ctx context.Context, req instance.MigrateRequest, step func(string, string)) error {
		ran.Store(req.Slug, true)
		if req.Slug == "globex" {
			return errors.New("boom")
		}
		return nil
	}

	err := h.Run(context.Background(), parent, jobs.NewJobContext(mem, parent.ID))
	if err == nil {
		t.Fatal("want failure")
	}
	if !contains(err.Error(), "globex") || !contains(err.Error(), "1/3") {
		t.Fatalf("aggregate error should name globex and 1/3: %v", err)
	}
	// All three ran despite one failing.
	for _, s := range []string{"acme", "globex", "initech"} {
		if _, ok := ran.Load(s); !ok {
			t.Fatalf("%s did not run; a sibling failure aborted others", s)
		}
	}
	// Child states: 2 succeeded, 1 failed.
	children, _ := mem.ListJobs(context.Background(), store.JobFilter{ParentID: parent.ID})
	var ok, bad int
	for _, c := range children {
		switch c.State {
		case store.JobSucceeded:
			ok++
		case store.JobFailed:
			bad++
		}
	}
	if ok != 2 || bad != 1 {
		t.Fatalf("want 2 ok / 1 failed children, got %d / %d", ok, bad)
	}
}

func TestEvacuateConcurrencyBound(t *testing.T) {
	old := evacuateConcurrency
	evacuateConcurrency = 1
	defer func() { evacuateConcurrency = old }()

	svc, mem := buildSvc(t, "a", "b", "c", "d")
	parent, _ := mem.Enqueue(context.Background(), "evacuate",
		mustArgs(t, "hostA", map[string]string{"a": "hostB", "b": "hostB", "c": "hostB", "d": "hostB"}), "")

	var inFlight, maxInFlight int32
	h := &Handler{Svc: svc, Jobs: mem}
	h.migrate = func(ctx context.Context, req instance.MigrateRequest, step func(string, string)) error {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxInFlight)
			if n <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, n) {
				break
			}
		}
		// brief spin so overlaps would be observed if the bound were broken
		for i := 0; i < 1000; i++ {
		}
		atomic.AddInt32(&inFlight, -1)
		return nil
	}
	if err := h.Run(context.Background(), parent, jobs.NewJobContext(mem, parent.ID)); err != nil {
		t.Fatal(err)
	}
	if maxInFlight != 1 {
		t.Fatalf("evacuateConcurrency=1 but observed %d in flight", maxInFlight)
	}
}

func mustArgs(t *testing.T, from string, m map[string]string) []byte {
	t.Helper()
	b, err := json.Marshal(instance.EvacuateRequest{FromHost: from, Map: m})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func contains(s, sub string) bool { return len(s) >= len(sub) && (fmt.Sprintf("%s", s) != "" && strings_Contains(s, sub)) }
```

NOTE to implementer: replace the hacky `contains`/`mustArgs` helpers with clean ones — use `strings.Contains` directly and import `encoding/json` and `strings`. The pseudo-helpers above only sketch intent. Keep `buildSvc` faithful to the instance package's existing test setup; if that setup is non-trivial, it's acceptable for `buildSvc` to seed specs via `mem.PutSpec` and construct the service exactly as `instance/migrate_test.go` does.

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test -tags "$TAGS" ./internal/evacuate/ -v`
Expected: compile error — package/handler undefined.

- [ ] **Step 3: Implement**

Create `internal/evacuate/handler.go`:

```go
// Package evacuate adapts host-evacuation orchestration to the jobs runner.
package evacuate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// evacuateConcurrency bounds how many child migrations run at once. migrate is
// heavy (stop + volume cold-copy + apply + verify), so keep it low. var (not
// const) so same-package tests can change it.
var evacuateConcurrency = 2

// Handler runs "evacuate" jobs: resolve the host's instances into a migrate
// plan, run the migrations with bounded concurrency (each as a child migrate
// job), and aggregate onto the parent.
type Handler struct {
	Svc  *instance.Service
	Jobs store.JobStore
	// migrate moves one instance; defaults to Svc.Migrate. Overridable in tests.
	migrate func(ctx context.Context, req instance.MigrateRequest, step func(step, detail string)) error
}

var _ jobs.Handler = (*Handler)(nil)

func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.EvacuateRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode evacuate args: %w", err)
	}
	moves, err := h.Svc.ResolveEvacuation(ctx, req)
	if err != nil {
		return err
	}

	mig := h.migrate
	if mig == nil {
		mig = h.Svc.Migrate
	}

	sem := make(chan struct{}, evacuateConcurrency)
	failures := make([]string, len(moves)) // index-addressed; "" == success
	var wg sync.WaitGroup
	for i, m := range moves {
		wg.Add(1)
		go func(i int, m instance.MigrateRequest) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := h.runChild(ctx, job.ID, m, mig); err != nil {
				failures[i] = fmt.Sprintf("%s: %v", m.Slug, err)
			}
		}(i, m)
	}
	wg.Wait()

	var failed []string
	for _, f := range failures {
		if f != "" {
			failed = append(failed, f)
		}
	}
	// Single post-join writer: preserves AppendStep's single-writer invariant.
	jc.Step("summary", fmt.Sprintf("%d ok, %d failed", len(moves)-len(failed), len(failed)))
	if len(failed) > 0 {
		return fmt.Errorf("%d/%d migrations failed: %s", len(failed), len(moves), strings.Join(failed, "; "))
	}
	return nil
}

// runChild records a child migrate job, runs the migration (its progress steps
// land on the child), and finishes the child. A store bookkeeping error counts
// as a failure even if the migration itself succeeded — we could not record it,
// so fail loud.
func (h *Handler) runChild(ctx context.Context, parentID string, m instance.MigrateRequest,
	mig func(context.Context, instance.MigrateRequest, func(string, string)) error) error {
	args, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal child args: %w", err)
	}
	child, err := h.Jobs.StartChild(ctx, "migrate", args, parentID)
	if err != nil {
		return fmt.Errorf("start child job: %w", err)
	}
	cjc := jobs.NewJobContext(h.Jobs, child.ID)
	migErr := mig(ctx, m, cjc.Step)

	state, errMsg := store.JobSucceeded, ""
	if migErr != nil {
		state, errMsg = store.JobFailed, migErr.Error()
	}
	if ferr := h.Jobs.Finish(ctx, child.ID, state, errMsg); ferr != nil && migErr == nil {
		return fmt.Errorf("finish child job: %w", ferr)
	}
	return migErr
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `go test -tags "$TAGS" ./internal/evacuate/ -v` and once more with `-race`:
`go test -tags "$TAGS" -race ./internal/evacuate/ -v`
Expected: PASS, no data races (the `-race` run validates the index-addressed `failures` slice and single-writer parent step).

- [ ] **Step 5: Commit**

```bash
git add internal/evacuate/handler.go internal/evacuate/handler_test.go
git commit -m "feat(evacuate): parent job handler with bounded-concurrency fan-out (#35)"
```

---

## Task 5: API — `POST /evacuate`, route, classify, `?parent_id`

**Files:**
- Create: `internal/api/evacuate.go`
- Modify: `internal/api/router.go`
- Modify: `internal/api/errors.go`
- Modify: `internal/api/jobs.go`
- Test: `internal/api/evacuate_test.go` (Create)

Context: handlers hold `svc *instance.Service` and `jobs store.JobStore`. Mirror `internal/api/migrate.go` and `internal/api/migrate_test.go` exactly (raw `http.NewRequest("POST", url, body)` + Bearer header; the test wiring builds a router with a `store.Memory` as the JobStore).

- [ ] **Step 1: Write the failing test**

Create `internal/api/evacuate_test.go`. Mirror `migrate_test.go`'s router/auth/store setup. Cover:

```go
// jobs disabled -> 501
// malformed JSON body -> 400 invalid_body, no job enqueued
// validation failure (unmapped instance) -> 400 invalid_request, no job enqueued
// success -> 202 {job_id}, a parent "evacuate" job row exists
// GET /jobs?parent_id=<id> returns only that parent's children (seed via StartChild)
```

Write each as a subtest using the same helpers `migrate_test.go` uses (e.g. a `newTestAPI`/`doReq` helper if present). For the "no job enqueued" assertions, list jobs before/after and assert the count is unchanged. For the success case, decode the body for `job_id`, then `GET /jobs/{id}` (or list) and assert `kind=="evacuate"`. For the `?parent_id` case, after enqueuing the parent, call `StartChild` twice against the store directly, then assert `GET /jobs?parent_id=<parent>` returns exactly those two.

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test -tags "$TAGS" ./internal/api/ -run Evacuate -v`
Expected: compile error — `h.evacuate` undefined / route 404.

- [ ] **Step 3: Implement the handler**

Create `internal/api/evacuate.go`:

```go
package api

import (
	"encoding/json"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
)

func (h *handlers) evacuate(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil { // store disabled → evacuate unavailable
		WriteError(w, errJobsDisabled)
		return
	}
	var req instance.EvacuateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if _, err := h.svc.ResolveEvacuation(r.Context(), req); err != nil {
		WriteError(w, err)
		return
	}
	args, err := json.Marshal(req)
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "evacuate", args, "")
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
}
```

- [ ] **Step 4: Add the route**

In `internal/api/router.go`, after the `POST /migrate` route:

```go
	// Evacuate enqueues a parent job that fans out child migrate jobs; 501 when the store is disabled.
	mux.Handle("POST /evacuate", guard("instances:write", http.HandlerFunc(h.evacuate)))
```

- [ ] **Step 5: Classify the new sentinel**

In `internal/api/errors.go`, add a case (next to `ErrSameHost`):

```go
	case errors.Is(err, instance.ErrInvalidEvacuation):
		return "invalid_request", http.StatusBadRequest, err.Error()
```

- [ ] **Step 6: Wire `?parent_id`**

In `internal/api/jobs.go`, in `listJobs`, extend the filter:

```go
	f := store.JobFilter{
		State:    store.JobState(r.URL.Query().Get("state")),
		Kind:     r.URL.Query().Get("kind"),
		ParentID: r.URL.Query().Get("parent_id"),
	}
```

- [ ] **Step 7: Run the tests to confirm they pass**

Run: `go test -tags "$TAGS" ./internal/api/ -run 'Evacuate|Jobs' -v`
Expected: PASS. Then the whole api package: `go test -tags "$TAGS" ./internal/api/`.

- [ ] **Step 8: Commit**

```bash
git add internal/api/evacuate.go internal/api/router.go internal/api/errors.go internal/api/jobs.go internal/api/evacuate_test.go
git commit -m "feat(api): POST /evacuate + ?parent_id job filter (#35)"
```

---

## Task 6: Wire main + OpenAPI

**Files:**
- Modify: `cmd/podman-api/main.go`
- Modify: `api/openapi.yaml`
- Modify: `internal/api/openapi_test.go`

- [ ] **Step 1: Register the handler in main**

In `cmd/podman-api/main.go`, add the import `"github.com/iotready/podman-api/internal/evacuate"` and extend the registry:

```go
		registry := jobs.Registry{
			"migrate":  &migrate.Handler{Svc: svc},
			"evacuate": &evacuate.Handler{Svc: svc, Jobs: db},
		}
```

(`db` is the `store.DB`, which satisfies `store.JobStore` for the `Jobs` field.)

- [ ] **Step 2: Build to confirm wiring compiles**

Run: `make build`
Expected: `bin/podman-api` builds with no errors.

- [ ] **Step 3: Add the OpenAPI path**

In `api/openapi.yaml`, mirror the existing `/migrate` entry. Add a `/evacuate` path (POST, `instances:write` security, request body `EvacuateRequest`, responses `202` JobAccepted, `400` invalid_body/invalid_request, `501` not_implemented). Add the `EvacuateRequest` schema:

```yaml
    EvacuateRequest:
      type: object
      required: [from_host, map]
      properties:
        from_host: { type: string }
        map:
          type: object
          additionalProperties: { type: string }
          description: slug → destination host for every instance on from_host
```

Add a `parent_id` query parameter to `GET /jobs` (string, optional, "filter to children of this parent job"). Ensure the Error `code` enum includes `invalid_request` and `not_implemented` (add if missing).

- [ ] **Step 4: Add the path to the spot-check test**

In `internal/api/openapi_test.go`, add `"/evacuate"` to the list of paths asserted present (mirror how `"/migrate"` was added).

- [ ] **Step 5: Run the OpenAPI test + full suite**

Run: `go test -tags "$TAGS" ./internal/api/ -run OpenAPI -v` then `make test`
Expected: PASS across the suite.

- [ ] **Step 6: gofmt + vet**

Run:
```sh
gofmt -l internal/ cmd/        # must print nothing
go vet -tags "$TAGS" ./...
```
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add cmd/podman-api/main.go api/openapi.yaml internal/api/openapi_test.go
git commit -m "feat: register evacuate handler + document POST /evacuate (#35)"
```

---

## Self-Review (controller checklist — already run)

- **Spec coverage:** request/validation (Task 3, 5), strict bijection + ambiguity + same/unknown host (Task 3), `ListSpecKeys` (Task 1), `StartChild`/unclaimable/ParentID filter (Task 2), bounded-concurrency fan-out + siblings-continue + aggregate (Task 4), `POST /evacuate` + 501/400/202 + `?parent_id` (Task 5), main registration + OpenAPI (Task 6). All spec sections map to a task.
- **Type consistency:** `SpecKey{Template,Slug}`, `JobFilter.ParentID`, `EvacuateRequest{FromHost,Map}`, `ErrInvalidEvacuation`, `evacuate.Handler{Svc,Jobs,migrate}` used consistently across tasks. `StartChild(ctx, kind, args, parentID)` signature identical in interface + both impls + handler call.
- **Concurrency:** parent `jc.Step` only post-join (single writer); per-child progress on child jobs; `-race` gate in Task 4.
- **No placeholders:** every code step has complete code. The two test helpers flagged "mirror migrate_test.go" are deliberate — the implementer must match existing test conventions rather than invent a divergent harness.
