# Evacuate dry-run / plan preview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only `POST /evacuate/plan` that resolves an evacuate map into a per-instance move list and runs the live destination preflight checks for each, reporting per move whether the destination would currently accept it — without enqueuing a job or mutating anything.

**Architecture:** `Service.preflightDest` is refactored into a collect-all `preflightIssues` that both the fail-fast `Migrate` executor and a new `Service.PlanEvacuation` share (no drift). `PlanEvacuation` defers to the existing `ResolveEvacuation` for static map validation, then runs `preflightIssues` per resolved move, classifying each returned error into a stable `PlanIssue` code. A thin API handler serializes the result.

**Tech Stack:** Go (standard `net/http` ServeMux), existing `internal/podman/fake` + `internal/store.Memory` for tests, testify.

**Build/test tags (REQUIRED — CGO drivers):** every `go` command below uses
```
TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"
```
Prefer `make build` / `make test`; for single packages use `go test -tags "$TAGS" ./path/...`.

**Spec:** `docs/superpowers/specs/2026-06-04-evacuate-plan-preview-design.md`

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `internal/instance/migrate.go` | extract `preflightIssues` (collect-all), rewire `preflightDest` to it | Modify |
| `internal/instance/evacuate_plan.go` | `EvacuationPlan` / `PlannedMove` / `PlanIssue` types, issue-code consts, `PlanEvacuation`, `classifyPlanIssue` | Create |
| `internal/instance/evacuate_plan_test.go` | service-level tests for `PlanEvacuation` + `preflightIssues` | Create |
| `internal/api/evacuate.go` | append `evacuatePlan` handler | Modify |
| `internal/api/router.go` | register `POST /evacuate/plan` (scope `instances:read`) | Modify |
| `internal/api/evacuate_test.go` | API-level tests for the new route | Modify |
| `api/openapi.yaml` | `/evacuate/plan` path + `EvacuationPlan`/`PlannedMove`/`PlanIssue` schemas | Modify |
| `internal/api/openapi_test.go` | add `/evacuate/plan` to the path spot-check slice | Modify |

---

## Task 1: Extract collect-all `preflightIssues` (shared by executor and preview)

**Why:** `preflightDest` returns on the first problem. The preview needs *all* problems for *every* move, so the check logic must become collect-all while the executor keeps its fail-fast semantics and existing sentinel errors.

**Files:**
- Modify: `internal/instance/migrate.go:116-155` (the `preflightDest` function)
- Test: `internal/instance/evacuate_plan_test.go` (new file — first test lives here)

- [ ] **Step 1: Write the failing test**

Create `internal/instance/evacuate_plan_test.go` with this content (a fixture template that declares BOTH a per-host secret and a host port, so one destination yields two issues):

```go
package instance

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// secretAndPortTemplate declares both a per-host secret and a fixed hostPort, so
// a single destination can surface two distinct preflight issues at once.
func secretAndPortTemplate() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "needs-both",
			Parameters: render.Parameters{Required: []string{"slug", "image"}},
			Secrets:    render.Secrets{PerHostReferenced: []string{"shared-token"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: needs-both-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
      ports:
        - hostPort: 9090
          containerPort: 80
`,
		Source: "needs-both.yaml",
	}
}

func TestPreflightIssues_CollectsAll(t *testing.T) {
	ctx := context.Background()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	f := fake.New()
	mem := store.NewMemory()
	svc := NewService(f, hosts, []config.Template{secretAndPortTemplate()})
	svc.SetStore(mem)
	// Occupy port 9090 on the destination so the port check fails; leave the
	// per-host secret "shared-token" absent so the secret check also fails.
	f.AddPod("h2", podman.Pod{Name: "occupier", Status: "Running",
		Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 9090}}}}})

	tmpl := secretAndPortTemplate()
	eff := map[string]any{"slug": "x", "image": "img"}
	errs := svc.preflightIssues(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-both", Slug: "x"}, tmpl, eff)

	require.Len(t, errs, 2, "expected both the missing-secret and port-conflict issues")
	var sawSecret, sawPort bool
	for _, e := range errs {
		if errors.Is(e, ErrHostSecretMissing) {
			sawSecret = true
		}
		if errors.Is(e, ErrPortConflict) {
			sawPort = true
		}
	}
	assert.True(t, sawSecret, "missing per-host secret not reported")
	assert.True(t, sawPort, "port conflict not reported")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestPreflightIssues_CollectsAll -v`
Expected: FAIL — compile error `svc.preflightIssues undefined`.

- [ ] **Step 3: Add `preflightIssues` and rewire `preflightDest`**

In `internal/instance/migrate.go`, replace the entire existing `preflightDest` function (lines 116-155, the block beginning `// preflightDest runs all fail-fast destination checks (no mutation).`) with:

```go
// preflightIssues runs every destination preflight check and returns all
// problems found, in check order. A nil/empty result means the destination
// would currently accept the instance. Each error is either a sentinel-wrapped
// blocking condition (ErrHostDraining / ErrInstanceExists / ErrHostSecretMissing
// / ErrPortConflict) or an infrastructure error that made a check inconclusive.
// preflightDest (fail-fast executor path) and PlanEvacuation (collect-all preview
// path) both build on it, so the preview and the executor never disagree.
func (s *Service) preflightIssues(ctx context.Context, req MigrateRequest, tmpl config.Template, eff map[string]any) []error {
	var issues []error
	hostCfg, _ := s.host(req.ToHost)
	if hostCfg.Drain {
		issues = append(issues, ErrHostDraining)
	}
	// An infra error here usually means the host is unreachable, which would
	// fail every subsequent check too — report it once and stop, mirroring the
	// executor's original fail-fast behaviour.
	if _, err := s.client.PodInspect(ctx, req.ToHost, podName(req.Template, req.Slug)); err == nil {
		issues = append(issues, ErrInstanceExists)
	} else if !errors.Is(err, podman.ErrNotFound) {
		return append(issues, fmt.Errorf("inspect dest pod: %w", err))
	}
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, req.ToHost, name); err != nil {
			if errors.Is(err, podman.ErrNotFound) {
				issues = append(issues, fmt.Errorf("%w: %s", ErrHostSecretMissing, name))
			} else {
				issues = append(issues, fmt.Errorf("inspect host secret %q: %w", name, err))
			}
		}
	}
	want, err := s.requiredHostPorts(tmpl, eff)
	if err != nil {
		return append(issues, err)
	}
	if len(want) > 0 {
		used, err := s.PortsInUse(ctx, req.ToHost)
		if err != nil {
			return append(issues, fmt.Errorf("ports in use: %w", err))
		}
		busy := map[int]bool{}
		for _, p := range used {
			busy[p.HostPort] = true
		}
		for _, p := range want {
			if busy[p] {
				issues = append(issues, fmt.Errorf("%w: %d", ErrPortConflict, p))
			}
		}
	}
	return issues
}

// preflightDest runs all fail-fast destination checks (no mutation), returning
// the first blocking condition or infrastructure error encountered, in check
// order. It is the executor's guard; PlanEvacuation uses preflightIssues to
// collect every problem instead.
func (s *Service) preflightDest(ctx context.Context, req MigrateRequest, tmpl config.Template, eff map[string]any) error {
	if errs := s.preflightIssues(ctx, req, tmpl, eff); len(errs) > 0 {
		return errs[0]
	}
	return nil
}
```

Note: the order of appends (drain → instance-exists → secrets → ports) preserves `preflightDest`'s original first-error result, so `errs[0]` matches the prior return value for every fail-fast test.

- [ ] **Step 4: Run the new test and the existing migrate regression tests**

Run: `go test -tags "$TAGS" ./internal/instance/ -run 'TestPreflightIssues_CollectsAll|TestMigrate_PreflightFailFast_SourceUntouched|TestMigrate_HappyPath|TestCheckMigratable_Errors' -v`
Expected: PASS (new test passes; all four existing preflight/migrate tests stay green — proves the refactor preserved fail-fast behaviour).

- [ ] **Step 5: Commit**

```bash
git add internal/instance/migrate.go internal/instance/evacuate_plan_test.go
git commit -m "refactor(instance): extract collect-all preflightIssues (#54)"
```

---

## Task 2: `PlanEvacuation` + plan types

**Files:**
- Create: `internal/instance/evacuate_plan.go`
- Test: `internal/instance/evacuate_plan_test.go` (append tests)

- [ ] **Step 1: Write the failing tests**

Append to `internal/instance/evacuate_plan_test.go`:

```go
// newPlanSvc builds a service with the postgres + web (port) + host-secret
// templates and a destination set, returning the service, fake, and store.
func newPlanSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/a"},
		{ID: "h2", Addr: "unix", Socket: "/b"},
		{ID: "h3", Addr: "unix", Socket: "/c"},
		{ID: "draining", Addr: "unix", Socket: "/d", Drain: true},
	}
	f := fake.New()
	mem := store.NewMemory()
	svc := NewService(f, hosts, []config.Template{pgTemplate(), portTemplate(), templateWithHostSecret()})
	svc.SetStore(mem)
	return svc, f, mem
}

func TestPlanEvacuation_AllClean(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
	seedSpec(t, mem, "h1", "postgres", "db2", map[string]any{"slug": "db2", "image": "x", "port": 5432, "db": "d", "user": "u"})

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2", "db2": "h3"}})
	require.NoError(t, err)
	assert.Equal(t, "h1", plan.FromHost)
	require.Len(t, plan.Moves, 2)
	// Sorted by slug.
	assert.Equal(t, "db1", plan.Moves[0].Slug)
	assert.Equal(t, "db2", plan.Moves[1].Slug)
	for _, m := range plan.Moves {
		assert.True(t, m.OK, "move %s should be clean", m.Slug)
		assert.Empty(t, m.Issues)
	}
}

func TestPlanEvacuation_BlockingConditions(t *testing.T) {
	ctx := context.Background()

	t.Run("destination draining", func(t *testing.T) {
		svc, _, mem := newPlanSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
		plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "draining"}})
		require.NoError(t, err)
		require.Len(t, plan.Moves, 1)
		assert.False(t, plan.Moves[0].OK)
		require.Len(t, plan.Moves[0].Issues, 1)
		assert.Equal(t, "destination_draining", plan.Moves[0].Issues[0].Code)
	})

	t.Run("instance already exists on destination", func(t *testing.T) {
		svc, f, mem := newPlanSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
		f.AddPod("h2", podman.Pod{Name: "postgres-db1", Status: "Running"})
		plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
		require.NoError(t, err)
		require.Len(t, plan.Moves[0].Issues, 1)
		assert.Equal(t, "instance_exists", plan.Moves[0].Issues[0].Code)
	})

	t.Run("missing per-host secret", func(t *testing.T) {
		svc, _, mem := newPlanSvc(t)
		seedSpec(t, mem, "h1", "needs-host-secret", "s1", map[string]any{"slug": "s1", "image": "x"})
		plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"s1": "h2"}})
		require.NoError(t, err)
		require.Len(t, plan.Moves[0].Issues, 1)
		assert.Equal(t, "host_secret_missing", plan.Moves[0].Issues[0].Code)
	})

	t.Run("host port conflict", func(t *testing.T) {
		svc, f, mem := newPlanSvc(t)
		seedSpec(t, mem, "h1", "web", "w1", map[string]any{"slug": "w1", "image": "x"})
		f.AddPod("h2", podman.Pod{Name: "other", Status: "Running",
			Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 8080}}}}})
		plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"w1": "h2"}})
		require.NoError(t, err)
		require.Len(t, plan.Moves[0].Issues, 1)
		assert.Equal(t, "port_conflict", plan.Moves[0].Issues[0].Code)
	})
}

func TestPlanEvacuation_MixedPlan_AllReportedSorted(t *testing.T) {
	svc, f, mem := newPlanSvc(t)
	ctx := context.Background()
	// "alpha" is clean to h2; "beta" hits a port conflict on h3.
	seedSpec(t, mem, "h1", "postgres", "alpha", map[string]any{"slug": "alpha", "image": "x", "port": 5432, "db": "d", "user": "u"})
	seedSpec(t, mem, "h1", "web", "beta", map[string]any{"slug": "beta", "image": "x"})
	f.AddPod("h3", podman.Pod{Name: "other", Status: "Running",
		Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 8080}}}}})

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"alpha": "h2", "beta": "h3"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves, 2)
	assert.Equal(t, "alpha", plan.Moves[0].Slug)
	assert.True(t, plan.Moves[0].OK)
	assert.Equal(t, "beta", plan.Moves[1].Slug)
	assert.False(t, plan.Moves[1].OK, "a problematic move must not stop the others from being reported")
	require.Len(t, plan.Moves[1].Issues, 1)
	assert.Equal(t, "port_conflict", plan.Moves[1].Issues[0].Code)
}

func TestPlanEvacuation_InconclusiveCheck(t *testing.T) {
	svc, f, mem := newPlanSvc(t)
	ctx := context.Background()
	seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
	// A non-NotFound PodInspect error makes the destination check inconclusive.
	f.PodInspectErr = errors.New("dial tcp: connection refused")

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves[0].Issues, 1)
	assert.Equal(t, "check_error", plan.Moves[0].Issues[0].Code)
	assert.False(t, plan.Moves[0].OK)
}

func TestPlanEvacuation_StaticValidationErrors(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
	seedSpec(t, mem, "h1", "postgres", "db2", map[string]any{"slug": "db2", "image": "x", "port": 5432, "db": "d", "user": "u"})

	// Unknown from_host.
	_, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "ghost", Map: map[string]string{}})
	assert.ErrorIs(t, err, ErrUnknownHost)
	// Unmapped instance (db2) -> invalid evacuation.
	_, err = svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
	assert.ErrorIs(t, err, ErrInvalidEvacuation)
	// Same-host destination.
	_, err = svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h1", "db2": "h2"}})
	assert.ErrorIs(t, err, ErrSameHost)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestPlanEvacuation -v`
Expected: FAIL — compile error `svc.PlanEvacuation undefined` and `EvacuationPlan`/`PlannedMove` undefined.

- [ ] **Step 3: Create `internal/instance/evacuate_plan.go`**

```go
package instance

import (
	"context"
	"errors"
)

// Plan issue codes. Stable strings exposed in the POST /evacuate/plan response
// and documented in the OpenAPI spec.
const (
	codeDestinationDraining = "destination_draining"
	codeInstanceExists      = "instance_exists"
	codeHostSecretMissing   = "host_secret_missing"
	codePortConflict        = "port_conflict"
	codeCheckError          = "check_error"
)

// EvacuationPlan is the result of PlanEvacuation: the resolved per-instance
// moves plus, for each, whether the destination would currently accept it.
type EvacuationPlan struct {
	FromHost string        `json:"from_host"`
	Moves    []PlannedMove `json:"moves"`
}

// PlannedMove is one instance's planned move and its preflight verdict.
type PlannedMove struct {
	Slug     string      `json:"slug"`
	Template string      `json:"template"`
	ToHost   string      `json:"to_host"`
	OK       bool        `json:"ok"` // true iff Issues is empty
	Issues   []PlanIssue `json:"issues"`
}

// PlanIssue is a single reason a move is not clean: a blocking destination
// condition or an inconclusive (check_error) check.
type PlanIssue struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// PlanEvacuation previews an evacuation without mutating anything or enqueuing a
// job. It defers to ResolveEvacuation for static map validation (returning the
// same sentinel errors the real POST /evacuate would), then runs the live
// destination preflight checks per resolved move, collecting every problem. A
// move with no issues would currently be accepted by the destination.
func (s *Service) PlanEvacuation(ctx context.Context, req EvacuateRequest) (EvacuationPlan, error) {
	moves, err := s.ResolveEvacuation(ctx, req)
	if err != nil {
		return EvacuationPlan{}, err
	}
	plan := EvacuationPlan{FromHost: req.FromHost, Moves: make([]PlannedMove, 0, len(moves))}
	for _, m := range moves {
		plan.Moves = append(plan.Moves, s.planMove(ctx, m))
	}
	return plan, nil
}

// planMove runs the live preflight for one resolved move and classifies the
// result. Spec-load / template-lookup failures are reported as check_error so
// the move is still surfaced rather than blanking the whole plan.
func (s *Service) planMove(ctx context.Context, m MigrateRequest) PlannedMove {
	pm := PlannedMove{Slug: m.Slug, Template: m.Template, ToHost: m.ToHost, Issues: []PlanIssue{}}
	tmpl, err := s.lookup(m.ToHost, m.Template)
	if err != nil {
		pm.Issues = append(pm.Issues, PlanIssue{Code: codeCheckError, Message: err.Error()})
		return pm
	}
	spec, err := s.store.GetSpec(ctx, m.FromHost, m.Template, m.Slug)
	if err != nil {
		pm.Issues = append(pm.Issues, PlanIssue{Code: codeCheckError, Message: err.Error()})
		return pm
	}
	eff := mergeParams(spec.Parameters, m.Parameters)
	eff["slug"] = m.Slug // canonical slug always wins; pod name must match podName()
	for _, e := range s.preflightIssues(ctx, m, tmpl, eff) {
		pm.Issues = append(pm.Issues, classifyPlanIssue(e))
	}
	pm.OK = len(pm.Issues) == 0
	return pm
}

// classifyPlanIssue maps a preflight error to a stable plan issue code. Anything
// that is not a known blocking sentinel is an inconclusive check_error.
func classifyPlanIssue(err error) PlanIssue {
	switch {
	case errors.Is(err, ErrHostDraining):
		return PlanIssue{Code: codeDestinationDraining, Message: err.Error()}
	case errors.Is(err, ErrInstanceExists):
		return PlanIssue{Code: codeInstanceExists, Message: err.Error()}
	case errors.Is(err, ErrHostSecretMissing):
		return PlanIssue{Code: codeHostSecretMissing, Message: err.Error()}
	case errors.Is(err, ErrPortConflict):
		return PlanIssue{Code: codePortConflict, Message: err.Error()}
	default:
		return PlanIssue{Code: codeCheckError, Message: err.Error()}
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/instance/ -run 'TestPlanEvacuation|TestPreflightIssues' -v`
Expected: PASS (all `TestPlanEvacuation_*` subtests and `TestPreflightIssues_CollectsAll`).

- [ ] **Step 5: Commit**

```bash
git add internal/instance/evacuate_plan.go internal/instance/evacuate_plan_test.go
git commit -m "feat(instance): PlanEvacuation preview with per-move preflight (#54)"
```

---

## Task 3: API handler + route

**Files:**
- Modify: `internal/api/evacuate.go` (append `evacuatePlan`)
- Modify: `internal/api/router.go:91` (register route after the `POST /evacuate` line)
- Test: `internal/api/evacuate_test.go` (append tests)

- [ ] **Step 1a: Widen `newEvacSrv` to also return the fake**

The blocked-move test needs to pre-place a pod on a destination, which requires the `*fake.Fake` the server was built on. `newEvacSrv` currently returns `(srv, tok, mem)`; change it to also return the fake.

In `internal/api/evacuate_test.go`, change the `newEvacSrv` signature and body (lines 21-38) so it constructs the fake as a local and returns it:

```go
func newEvacSrv(t *testing.T) (*httptest.Server, string, *store.Memory, *fake.Fake) {
	t.Helper()
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "jobs:*"}}}
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/x"},
		{ID: "h2", Addr: "unix", Socket: "/y"},
		{ID: "h3", Addr: "unix", Socket: "/z"},
	}
	mem := store.NewMemory()
	f := fake.New()
	svc := instance.NewService(f, hosts, []config.Template{migrateTmpl()})
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok, mem, f
}
```

Then fix the five existing callers (the fake is unused in each, so use `_`):

| Test | From | To |
|---|---|---|
| `TestEvacuate_API_Success` | `srv, tok, mem := newEvacSrv(t)` | `srv, tok, mem, _ := newEvacSrv(t)` |
| `TestEvacuate_API_MalformedBody` | `srv, tok, _ := newEvacSrv(t)` | `srv, tok, _, _ := newEvacSrv(t)` |
| `TestEvacuate_API_ValidationFailsNoJob` | `srv, tok, mem := newEvacSrv(t)` | `srv, tok, mem, _ := newEvacSrv(t)` |
| `TestEvacuate_API_UnknownDestHost_400` | `srv, tok, mem := newEvacSrv(t)` | `srv, tok, mem, _ := newEvacSrv(t)` |
| `TestJobs_API_ParentIDFilter` | `srv, tok, mem := newEvacSrv(t)` | `srv, tok, mem, _ := newEvacSrv(t)` |

Add `"github.com/iotready/podman-api/internal/podman"` to the test file's import block (used by the blocked-move test below).

- [ ] **Step 1b: Append the new failing tests**

Append to `internal/api/evacuate_test.go`:

```go
func postEvacuatePlan(t *testing.T, srv *httptest.Server, tok string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", srv.URL+"/evacuate/plan", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

type planResp struct {
	FromHost string `json:"from_host"`
	Moves    []struct {
		Slug   string `json:"slug"`
		ToHost string `json:"to_host"`
		OK     bool   `json:"ok"`
		Issues []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"issues"`
	} `json:"moves"`
}

func TestEvacuatePlan_API_Success(t *testing.T) {
	srv, tok, mem, _ := newEvacSrv(t)
	seedEvac(t, mem, "db1")
	seedEvac(t, mem, "db2")

	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2", "db2": "h3"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out planResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "h1", out.FromHost)
	require.Len(t, out.Moves, 2)
	assert.Equal(t, "db1", out.Moves[0].Slug) // sorted
	assert.Equal(t, "db2", out.Moves[1].Slug)
	for _, m := range out.Moves {
		assert.True(t, m.OK)
		assert.Empty(t, m.Issues)
	}
}

func TestEvacuatePlan_API_BlockedMove(t *testing.T) {
	srv, tok, mem, f := newEvacSrv(t)
	seedEvac(t, mem, "db1")
	// Pre-place the instance on the destination so the move is blocked.
	f.AddPod("h2", podman.Pod{Name: "postgres-db1", Status: "Running"})

	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out planResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Moves, 1)
	assert.False(t, out.Moves[0].OK)
	require.Len(t, out.Moves[0].Issues, 1)
	assert.Equal(t, "instance_exists", out.Moves[0].Issues[0].Code)
}

func TestEvacuatePlan_API_MalformedBody(t *testing.T) {
	srv, tok, _, _ := newEvacSrv(t)
	req, _ := http.NewRequest("POST", srv.URL+"/evacuate/plan", bytes.NewBufferString("{not json"))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEvacuatePlan_API_BadMap_400(t *testing.T) {
	srv, tok, mem, _ := newEvacSrv(t)
	seedEvac(t, mem, "db1")
	seedEvac(t, mem, "db2")
	// db2 unmapped -> invalid_request, same as the real evacuate.
	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2"},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEvacuatePlan_API_UnknownHost_404(t *testing.T) {
	srv, tok, _, _ := newEvacSrv(t)
	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "ghost", Map: map[string]string{},
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestEvacuatePlan_API_StoreDisabled_501(t *testing.T) {
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "jobs:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	svc := instance.NewService(fake.New(), hosts, []config.Template{migrateTmpl()})
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)

	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{FromHost: "h1", Map: map[string]string{}})
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

func TestEvacuatePlan_API_RequiresReadScope(t *testing.T) {
	// A key WITHOUT instances:read is rejected (hosts:read only -> 403 forbidden).
	hash, err := config.HashToken("t")
	require.NoError(t, err)
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	mem := store.NewMemory()
	svc := instance.NewService(fake.New(), hosts, []config.Template{migrateTmpl()})
	svc.SetStore(mem)

	keys := []config.APIKey{{ID: "noscope", SecretHash: hash, Scopes: []string{"hosts:read"}}}
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)

	resp := postEvacuatePlan(t, srv, "t", instance.EvacuateRequest{FromHost: "h1", Map: map[string]string{}})
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "$TAGS" ./internal/api/ -run TestEvacuatePlan -v`
Expected: FAIL — route returns 404 (not registered) / handler undefined / compile errors from the signature change until the handler + route exist.

- [ ] **Step 3: Add the handler**

Append to `internal/api/evacuate.go`:

```go
// evacuatePlan previews an evacuation: it resolves the map and runs the live
// destination preflight checks per instance, returning a per-move report. It is
// read-only — no job is enqueued and nothing is mutated. Map-level validation
// errors return the same 4xx the real POST /evacuate would.
func (h *handlers) evacuatePlan(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil { // store disabled → evacuate unavailable
		WriteError(w, errJobsDisabled)
		return
	}
	var req instance.EvacuateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	plan, err := h.svc.PlanEvacuation(r.Context(), req)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, plan)
}
```

- [ ] **Step 4: Register the route**

In `internal/api/router.go`, immediately after the `POST /evacuate` registration (line 91), add:

```go
	// Evacuate plan is a read-only dry-run: resolve the map and run live
	// destination preflight checks, returning a per-move report. No job, no
	// mutation; 501 when the store is disabled.
	mux.Handle("POST /evacuate/plan", guard("instances:read", http.HandlerFunc(h.evacuatePlan)))
```

(The more specific `/evacuate/plan` pattern takes precedence over `/evacuate` in Go's ServeMux; `POST /evacuate` matches only the exact path.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/api/ -run 'TestEvacuatePlan|TestEvacuate_API|TestJobs_API_ParentIDFilter' -v`
Expected: PASS (new plan tests pass; the refactored existing evacuate tests stay green).

- [ ] **Step 6: Commit**

```bash
git add internal/api/evacuate.go internal/api/router.go internal/api/evacuate_test.go
git commit -m "feat(api): POST /evacuate/plan dry-run endpoint (#54)"
```

---

## Task 4: OpenAPI documentation

**Files:**
- Modify: `api/openapi.yaml` (add path after the `/evacuate` block ~line 794; add schemas after `EvacuateRequest` ~line 270)
- Test: `internal/api/openapi_test.go:40-52` (add `/evacuate/plan` to the `want` slice)

- [ ] **Step 1: Add the path spot-check assertion (failing test)**

In `internal/api/openapi_test.go`, add `"/evacuate/plan",` to the `want` slice (right after `"/evacuate",` near line 50):

```go
		"/evacuate",
		"/evacuate/plan",
		"/migrate",
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/api/ -run TestOpenAPI -v`
Expected: FAIL — the spec has no `/evacuate/plan` path yet.

- [ ] **Step 3: Add the path to `api/openapi.yaml`**

Insert immediately after the `/evacuate` block (after its `x-required-scope: instances:write` line, ~line 794, before `/migrate:`):

```yaml
  /evacuate/plan:
    post:
      tags: [migrate]
      summary: preview an evacuation without running it (requires -state-db)
      description: >-
        Read-only dry-run of POST /evacuate. Resolves the map against the stored
        specs on from_host (identical static validation, so a request that would
        4xx on /evacuate 4xxes here identically), then runs the live destination
        preflight checks for every instance and returns a per-move report. No job
        is enqueued and nothing is mutated. Each move is ok=true only when its
        issues list is empty; a non-empty list names the blocking conditions
        (destination_draining, instance_exists, host_secret_missing,
        port_conflict) or an inconclusive check (check_error, e.g. the
        destination was unreachable). Requires only a read scope.
      requestBody:
        required: true
        content: {application/json: {schema: {$ref: '#/components/schemas/EvacuateRequest'}}}
      responses:
        '200':
          description: evacuation plan
          content:
            application/json:
              schema: {$ref: '#/components/schemas/EvacuationPlan'}
        '400': {$ref: '#/components/responses/BadRequest'}
        '404': {$ref: '#/components/responses/NotFound'}
        '501':
          description: state store not enabled (-state-db not set)
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Error'}
      x-required-scope: instances:read
```

- [ ] **Step 4: Add the schemas to `api/openapi.yaml`**

Immediately after the `EvacuateRequest:` schema block (it begins ~line 270 under `components: schemas:`; insert after its final property), add:

```yaml
    EvacuationPlan:
      type: object
      description: Result of POST /evacuate/plan — the resolved moves and their preflight verdicts.
      required: [from_host, moves]
      properties:
        from_host: {type: string}
        moves:
          type: array
          items: {$ref: '#/components/schemas/PlannedMove'}
    PlannedMove:
      type: object
      required: [slug, template, to_host, ok, issues]
      properties:
        slug: {type: string}
        template: {type: string}
        to_host: {type: string}
        ok:
          type: boolean
          description: true iff issues is empty (the destination would currently accept the move).
        issues:
          type: array
          items: {$ref: '#/components/schemas/PlanIssue'}
    PlanIssue:
      type: object
      required: [code, message]
      properties:
        code:
          type: string
          enum: [destination_draining, instance_exists, host_secret_missing, port_conflict, check_error]
        message: {type: string}
```

- [ ] **Step 5: Run the spec tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/api/ -run TestOpenAPI -v`
Expected: PASS (the `/evacuate/plan` path is found; the spec still parses).

- [ ] **Step 6: Commit**

```bash
git add api/openapi.yaml internal/api/openapi_test.go
git commit -m "docs(openapi): document POST /evacuate/plan (#54)"
```

---

## Task 5: Wiki documentation (published post-merge)

**Note:** the wiki is a **separate repo** (`/tmp/pa-wiki`, `podman-api.wiki.git`) with no PR flow. This task is applied and pushed there **after the PR merges**, not committed in this worktree. It is listed here so the spec's documentation requirement is not lost. No `Deploying.md` change (no new flag).

- [ ] **Step 1: Add a "Previewing an evacuate" subsection to `Operating.md`**

In `/tmp/pa-wiki/Operating.md`, under "Migrating & evacuating instances", insert a subsection **before** "### Cancelling a job":

```markdown
### Previewing an evacuate

```sh
curl -sS -X POST https://api.example/evacuate/plan \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"from_host":"node-a","map":{"acme":"node-b","globex":"node-c"}}'
# -> 200 {plan}
```

`POST /evacuate/plan` (scope **`instances:read`** — a read-only key works) is a
**dry-run**: it resolves the map and runs the same live destination checks the
real move runs, but enqueues nothing and changes nothing. It answers *"would this
evacuate succeed?"* before you commit.

The static validation is **identical** to `POST /evacuate`, so a request that
would `4xx` there `4xx`es here the same way (unmapped/extra/ambiguous slug,
unknown destination → `400`; unknown `from_host` → `404`; no state store →
`501`). Once the map resolves you get `200` with one entry per instance:

```json
{
  "from_host": "node-a",
  "moves": [
    {"slug":"acme","template":"postgres","to_host":"node-b","ok":true,"issues":[]},
    {"slug":"globex","template":"redis","to_host":"node-c","ok":false,
     "issues":[{"code":"port_conflict","message":"required host port already in use: 6379"}]}
  ]
}
```

A move is `ok` only when `issues` is empty. Issue codes:

- `destination_draining` — the destination host is draining; evacuate refuses it.
- `instance_exists` — an instance with this name is already on the destination.
- `host_secret_missing` — a required per-host secret is not seeded on the destination.
- `port_conflict` — a host port the pod binds is already taken on the destination.
- `check_error` — the check was **inconclusive** (e.g. the destination was
  unreachable); the preview cannot vouch for the move, treat it as not-ready.

A blocked plan does not stop you fixing the named problems (seed the secret, free
the port, pick another destination) and re-previewing until every move is `ok`.
```

- [ ] **Step 2: Publish (post-merge)**

```bash
cd /tmp/pa-wiki && git add Operating.md && git commit -m "Operating: document evacuate plan preview (#54)" && git push
```

---

## Task 6: Final verification

- [ ] **Step 1: Format and vet**

Run (from the worktree root):
```bash
gofmt -l internal/ cmd/ && go vet -tags "$TAGS" ./internal/instance/... ./internal/api/...
```
Expected: `gofmt -l` prints nothing; `go vet` is silent.

- [ ] **Step 2: Build**

Run: `make build`
Expected: builds `bin/podman-api` with no errors.

- [ ] **Step 3: Full unit suite for the touched packages**

Run: `go test -tags "$TAGS" ./internal/instance/... ./internal/api/...`
Expected: `ok` for both packages.

- [ ] **Step 4: Commit any formatting fixups (if gofmt changed files)**

```bash
git add -A && git commit -m "chore: gofmt (#54)" || echo "nothing to commit"
```
