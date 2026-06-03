# Migrate Primitive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /migrate` that moves a running instance (spec + volumes + secrets) host-to-host as an async job, with verify-then-commit and rollback-on-failure.

**Architecture:** Core algorithm is `Service.Migrate(ctx, req, step)` in `internal/instance` (reuses `Apply`/`Delete`/`Start`/`Stop`/`CopyVolume`/`PortsInUse` + render). A thin `internal/migrate.Handler` adapts it to `jobs.Handler`. `POST /migrate` validates synchronously, enqueues a `"migrate"` job, notifies the runner, returns `202 {job_id}`. A new `podman.Client.VolumeCreate` lets migrate create dest volumes before copying so `play kube` reuses them.

**Tech Stack:** Go, podman v5 bindings, `internal/jobs` runner, `internal/store` (SQLite/Memory), testify. Unit tests run with the standard CGO build tags (`make test`); the real-client `VolumeCreate` test is behind the `integration` tag.

**Spec:** `docs/superpowers/specs/2026-06-03-migrate-primitive-design.md`

---

## Conventions for every task

- Build the whole tree with `make build` (carries the required CGO tags). Plain `go build ./...` fails.
- Run unit tests with the tags. For a single package:
  `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestX`
  or just `make test` for the whole suite. `internal/instance`, `internal/api`, `internal/podman`, and `internal/podman/fake` all need the tags (they transitively import libpod).
- gofmt-clean (`gofmt -l .` empty) and `go vet` clean.
- Conventional commits; end each commit body with exactly:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

## File structure

| File | Responsibility | Change |
| --- | --- | --- |
| `internal/podman/client.go` | `Client` interface | add `VolumeCreate` |
| `internal/podman/real.go` | libpod client | `VolumeCreate` via `volumes.Create` |
| `internal/podman/fake/fake.go` | in-memory client | `VolumeCreate`; test helpers `AddPod`, `PlayKubePodStatus` |
| `internal/podman/fake/fake_volcreate_test.go` | fake unit test | NEW |
| `internal/instance/service.go` | Service | add `ErrPortConflict`; `migrateLock` helper |
| `internal/instance/migrate.go` | migrate core | NEW: `MigrateRequest`, `Service.CheckMigratable`, `Service.Migrate`, `requiredHostPorts`, verify-poll, `verifyTimeout`/`verifyInterval` vars |
| `internal/instance/migrate_test.go` | migrate unit tests | NEW |
| `internal/migrate/handler.go` | jobs adapter | NEW: `Handler` implementing `jobs.Handler` |
| `internal/migrate/handler_test.go` | adapter test | NEW |
| `internal/api/migrate.go` | HTTP handler | NEW: `POST /migrate` |
| `internal/api/migrate_test.go` | api test | NEW |
| `internal/api/router.go` | routing | add the `POST /migrate` route (no signature change) |
| `internal/api/errors.go` | error mapping | `ErrPortConflict` → 409, `ErrSameHost` → 400 |
| `cmd/podman-api/main.go` | wiring | register `migrate` handler in the `Registry` |
| `internal/podman/real_volcopy_integration_test.go` | integration | extend with `VolumeCreate` |

Adding `VolumeCreate` to the `Client` interface forces both `*Real` (`real.go`) and `*Fake` (`fake.go`) to implement it (compile-time `var _ Client`/`var _ podman.Client` assertions) — so Task 1 lands interface + both impls together.

---

## Task 1: VolumeCreate primitive (+ fake test helpers)

**Files:**
- Modify: `internal/podman/client.go` (Volumes block, ~line 29)
- Modify: `internal/podman/real.go` (after `VolumeImport`)
- Modify: `internal/podman/fake/fake.go` (methods + test helpers)
- Test: `internal/podman/fake/fake_volcreate_test.go` (new)

- [ ] **Step 1: Write the failing fake test**

Create `internal/podman/fake/fake_volcreate_test.go`:

```go
package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestFake_VolumeCreate(t *testing.T) {
	f := New()
	ctx := context.Background()

	require.NoError(t, f.VolumeCreate(ctx, "h1", "vol"))
	v, err := f.VolumeInspect(ctx, "h1", "vol")
	require.NoError(t, err)
	assert.Equal(t, "vol", v.Name)

	// Idempotent: creating an existing volume is not an error.
	require.NoError(t, f.VolumeCreate(ctx, "h1", "vol"))
}
```

- [ ] **Step 2: Run it, confirm it fails to compile** (`f.VolumeCreate undefined`):

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/fake/ -run TestFake_VolumeCreate`

- [ ] **Step 3: Add the interface method**

In `internal/podman/client.go`, in the `// Volumes` block (after `VolumeImport`):

```go
	// VolumeCreate creates an empty named volume on host. Creating a volume that
	// already exists is a no-op (no error).
	VolumeCreate(ctx context.Context, hostID, name string) error
```

- [ ] **Step 4: Implement the real method**

In `internal/podman/real.go`, add the import `"github.com/containers/podman/v5/pkg/domain/entities/types"` to the podman bindings import group, then add after `VolumeImport`:

```go
// VolumeCreate creates an empty named volume. An already-existing name is
// treated as success so migrate's create-then-copy step is idempotent on retry.
func (r *Real) VolumeCreate(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	if _, err := volumes.Create(c, types.VolumeCreateOptions{Name: name}, nil); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		return err
	}
	return nil
}
```

(`volumes` and `strings` are already imported in real.go.)

- [ ] **Step 5: Implement the fake method + test helpers**

In `internal/podman/fake/fake.go`:

Add the `VolumeCreate` method after `VolumeImport`:

```go
func (f *Fake) VolumeCreate(_ context.Context, h, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostVolumes(h)[name]; !ok {
		f.hostVolumes(h)[name] = podman.Volume{Name: name}
	}
	return nil
}
```

Add the `PlayKubePodStatus` hook field to the `Fake` struct's hook section:

```go
	// PlayKubePodStatus overrides the status assigned to pods created by
	// PlayKube. Empty means "Running". Lets a test force a played pod to stay
	// un-healthy so the migrate verify-poll times out.
	PlayKubePodStatus string
```

In `PlayKube`, replace the two `Status: "Running"` assignments (the container status and the pod status) so the **pod** status honours the hook (leave the container status as `"Running"`):

```go
		podStatus := "Running"
		if f.PlayKubePodStatus != "" {
			podStatus = f.PlayKubePodStatus
		}
		pods[head.Metadata.Name] = podman.Pod{
			ID: head.Metadata.Name, Name: head.Metadata.Name,
			Status: podStatus, Created: time.Now(),
			Containers: cs, Labels: head.Metadata.Labels,
		}
```

Add an `AddPod` test helper next to `AddVolume`:

```go
// AddPod seeds a pod on a host (with whatever container ports it carries), so a
// test can occupy host ports or pre-place an instance. Test-only.
func (f *Fake) AddPod(host string, p podman.Pod) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hostPods(host)[p.Name] = p
}
```

- [ ] **Step 6: Run the fake test, confirm PASS**

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/fake/ -run TestFake_VolumeCreate`

- [ ] **Step 7: `make build` (whole tree compiles, incl. real.go).**

- [ ] **Step 8: gofmt + commit**

```bash
gofmt -w internal/podman/client.go internal/podman/real.go internal/podman/fake/fake.go internal/podman/fake/fake_volcreate_test.go
git add internal/podman/client.go internal/podman/real.go internal/podman/fake/fake.go internal/podman/fake/fake_volcreate_test.go
git commit -m "feat(podman): VolumeCreate primitive (#34)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: MigrateRequest, ErrPortConflict, preflight + sync validation

This task adds the request type, the synchronous validator the API will call, the dest preflight checks, and the port helper — but **not** the mutation steps yet. `Service.Migrate` is built up to "preflight passes, then return nil" so the preflight is independently testable.

**Files:**
- Modify: `internal/instance/service.go` (errors block ~lines 20-26: add `ErrPortConflict`, `ErrSameHost`)
- Create: `internal/instance/migrate.go`
- Test: `internal/instance/migrate_test.go` (new)

- [ ] **Step 1: Add the new sentinel errors**

In `internal/instance/service.go`, inside the `var (… )` errors block (after `ErrHostDraining`):

```go
	ErrPortConflict      = errors.New("required host port already in use")
	ErrSameHost          = errors.New("source and destination host are the same")
```

- [ ] **Step 2: Write the failing preflight/validation tests**

Create `internal/instance/migrate_test.go`:

```go
package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// portTemplate is a fixture whose rendered Pod declares a fixed hostPort, used
// to exercise the dest port-conflict preflight.
func portTemplate() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "web",
			Parameters: render.Parameters{Required: []string{"slug", "image"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: web-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
      ports:
        - hostPort: 8080
          containerPort: 80
`,
		Source: "web.yaml",
	}
}

// newMigrateSvc builds a two-host Service with a Memory store, the pg + port
// fixtures, and returns the fake + store for seeding/asserting.
func newMigrateSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/x"},
		{ID: "h2", Addr: "unix", Socket: "/y"},
		{ID: "draining", Addr: "unix", Socket: "/z", Drain: true},
	}
	f := fake.New()
	mem := store.NewMemory()
	svc := NewService(f, hosts, []config.Template{pgTemplate(), portTemplate()})
	svc.SetStore(mem)
	return svc, f, mem
}

func seedSpec(t *testing.T, mem *store.Memory, host, tmpl, slug string, params map[string]any) {
	t.Helper()
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: host, Template: tmpl, Slug: slug, Parameters: params,
	}))
}

func TestCheckMigratable_Errors(t *testing.T) {
	svc, _, mem := newMigrateSvc(t)
	ctx := context.Background()
	seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})

	// same host
	err := svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "h1", Template: "postgres", Slug: "db1"})
	require.ErrorIs(t, err, ErrSameHost)
	// unknown from-host
	err = svc.CheckMigratable(ctx, MigrateRequest{FromHost: "nope", ToHost: "h2", Template: "postgres", Slug: "db1"})
	require.ErrorIs(t, err, ErrUnknownHost)
	// unknown to-host
	err = svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "nope", Template: "postgres", Slug: "db1"})
	require.ErrorIs(t, err, ErrUnknownHost)
	// unknown template
	err = svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "ghost", Slug: "db1"})
	require.ErrorIs(t, err, ErrUnknownTemplate)
	// missing spec
	err = svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "absent"})
	require.ErrorIs(t, err, store.ErrNotFound)
	// happy
	require.NoError(t, svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}))
}

func TestMigrate_PreflightFailFast_SourceUntouched(t *testing.T) {
	ctx := context.Background()

	t.Run("dest draining", func(t *testing.T) {
		svc, f, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
		f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "draining", Template: "postgres", Slug: "db1"}, nil)
		require.ErrorIs(t, err, ErrHostDraining)
		p, _ := f.PodInspect(ctx, "h1", "postgres-db1")
		assert.Equal(t, "Running", p.Status) // source never stopped
	})

	t.Run("dest already has instance", func(t *testing.T) {
		svc, f, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
		f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
		f.AddPod("h2", podman.Pod{Name: "postgres-db1", Status: "Running"})
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, nil)
		require.ErrorIs(t, err, ErrInstanceExists)
		p, _ := f.PodInspect(ctx, "h1", "postgres-db1")
		assert.Equal(t, "Running", p.Status)
	})

	t.Run("port conflict on dest", func(t *testing.T) {
		svc, f, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "web", "w1", map[string]any{"slug": "w1", "image": "x"})
		f.AddPod("h1", podman.Pod{Name: "web-w1", Status: "Running"})
		// Occupy port 8080 on dest with an unrelated pod.
		f.AddPod("h2", podman.Pod{Name: "other", Status: "Running",
			Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 8080}}}}})
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "web", Slug: "w1"}, nil)
		require.ErrorIs(t, err, ErrPortConflict)
		p, _ := f.PodInspect(ctx, "h1", "web-w1")
		assert.Equal(t, "Running", p.Status)
	})

	t.Run("missing per-host secret on dest", func(t *testing.T) {
		// Add a template that references a per-host secret.
		hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
		f := fake.New()
		mem := store.NewMemory()
		svc := NewService(f, hosts, []config.Template{templateWithHostSecret()})
		svc.SetStore(mem)
		seedSpec(t, mem, "h1", "needs-host-secret", "s1", map[string]any{"slug": "s1", "image": "x"})
		f.AddPod("h1", podman.Pod{Name: "needs-host-secret-s1", Status: "Running"})
		// dest h2 lacks "shared-pull-token".
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1"}, nil)
		require.ErrorIs(t, err, ErrHostSecretMissing)
		p, _ := f.PodInspect(ctx, "h1", "needs-host-secret-s1")
		assert.Equal(t, "Running", p.Status)
	})
}
```

- [ ] **Step 3: Run, confirm it fails to compile** (`MigrateRequest`/`CheckMigratable`/`Migrate` undefined):

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run 'TestCheckMigratable|TestMigrate_Preflight'`

- [ ] **Step 4: Create `internal/instance/migrate.go` with the type, validator, preflight, and a Migrate that stops after preflight**

```go
package instance

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
)

// verify-poll knobs; vars (not consts) so same-package tests can shorten them.
var (
	verifyTimeout  = 60 * time.Second
	verifyInterval = 2 * time.Second
)

// MigrateRequest is the POST /migrate body and the migrate job's args.
type MigrateRequest struct {
	FromHost   string         `json:"from_host"`
	ToHost     string         `json:"to_host"`
	Template   string         `json:"template"`
	Slug       string         `json:"slug"`
	Parameters map[string]any `json:"parameters"`
}

// migrateLock serialises migrates of the same instance without colliding with
// the per-host instance locks taken by Apply/Delete/Start/Stop (which would
// self-deadlock, sync.Mutex being non-reentrant). The sentinel "host" cannot
// collide with any real host id.
func (s *Service) migrateLock(tmpl, slug string) *sync.Mutex {
	return s.instanceLock("\x00migrate", tmpl, slug)
}

// CheckMigratable runs the cheap synchronous validation the POST handler needs:
// distinct known hosts, known template, and an existing stored spec. It performs
// no mutation. Returns typed errors the API maps to HTTP statuses.
func (s *Service) CheckMigratable(ctx context.Context, req MigrateRequest) error {
	if req.FromHost == req.ToHost {
		return ErrSameHost
	}
	if _, ok := s.host(req.FromHost); !ok {
		return ErrUnknownHost
	}
	if _, ok := s.host(req.ToHost); !ok {
		return ErrUnknownHost
	}
	if _, err := s.lookup(req.ToHost, req.Template); err != nil {
		return err // ErrUnknownHost/ErrUnknownTemplate
	}
	if s.store == nil {
		return fmt.Errorf("migrate requires the state store")
	}
	if _, err := s.store.GetSpec(ctx, req.FromHost, req.Template, req.Slug); err != nil {
		return err // store.ErrNotFound
	}
	return nil
}

// requiredHostPorts renders the template with eff params and returns the host
// ports its Pod(s) bind.
func (s *Service) requiredHostPorts(tmpl config.Template, params map[string]any) ([]int, error) {
	rendered, err := render.Render(rawTemplate(tmpl), params)
	if err != nil {
		return nil, fmt.Errorf("render: %w", err)
	}
	var ports []int
	for _, doc := range splitYAMLDocs(rendered) {
		var d struct {
			Kind string `yaml:"kind"`
			Spec struct {
				Containers []struct {
					Ports []struct {
						HostPort int `yaml:"hostPort"`
					} `yaml:"ports"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		}
		if yaml.Unmarshal([]byte(doc), &d) != nil || d.Kind != "Pod" {
			continue
		}
		for _, c := range d.Spec.Containers {
			for _, p := range c.Ports {
				if p.HostPort > 0 {
					ports = append(ports, p.HostPort)
				}
			}
		}
	}
	return ports, nil
}

func splitYAMLDocs(s string) []string {
	return strings.Split(s, "\n---\n")
}

// preflightDest runs all fail-fast destination checks (no mutation).
func (s *Service) preflightDest(ctx context.Context, req MigrateRequest, tmpl config.Template, eff map[string]any) error {
	hostCfg, _ := s.host(req.ToHost)
	if hostCfg.Drain {
		return ErrHostDraining
	}
	// No existing instance at dest (else rollback could clobber it).
	if _, err := s.client.PodInspect(ctx, req.ToHost, podName(req.Template, req.Slug)); err == nil {
		return ErrInstanceExists
	} else if !errors.Is(err, podman.ErrNotFound) {
		return fmt.Errorf("inspect dest pod: %w", err)
	}
	// Per-host secrets present on dest.
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, req.ToHost, name); err != nil {
			if errors.Is(err, podman.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrHostSecretMissing, name)
			}
			return fmt.Errorf("inspect host secret %q: %w", name, err)
		}
	}
	// Required host ports free on dest.
	want, err := s.requiredHostPorts(tmpl, eff)
	if err != nil {
		return err
	}
	if len(want) > 0 {
		used, err := s.PortsInUse(ctx, req.ToHost)
		if err != nil {
			return fmt.Errorf("ports in use: %w", err)
		}
		busy := map[int]bool{}
		for _, p := range used {
			busy[p.HostPort] = true
		}
		for _, p := range want {
			if busy[p] {
				return fmt.Errorf("%w: %d", ErrPortConflict, p)
			}
		}
	}
	return nil
}

// Migrate moves an instance from one host to another. See the migrate spec.
// step is a best-effort progress callback (may be nil).
func (s *Service) Migrate(ctx context.Context, req MigrateRequest, step func(step, detail string)) error {
	if step == nil {
		step = func(string, string) {}
	}
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	tmpl, err := s.lookup(req.ToHost, req.Template)
	if err != nil {
		return err
	}
	if s.store == nil {
		return fmt.Errorf("migrate requires the state store")
	}
	spec, err := s.store.GetSpec(ctx, req.FromHost, req.Template, req.Slug)
	if err != nil {
		return err
	}
	step("load", req.FromHost+"/"+req.Template+"/"+req.Slug)

	eff := mergeParams(spec.Parameters, req.Parameters)
	if err := s.preflightDest(ctx, req, tmpl, eff); err != nil {
		return err
	}
	step("preflight", req.ToHost)
	return nil // mutation steps added in Task 3
}

// mergeParams returns a new map: base overlaid by override (override wins).
func mergeParams(base, override map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}
```

Add the missing imports to `migrate.go`: it also needs `"strings"`, `"sync"`, and `"github.com/iotready/podman-api/internal/config"`. Final import block:

```go
import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
)
```

- [ ] **Step 5: Run the Task-2 tests, confirm PASS**

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run 'TestCheckMigratable|TestMigrate_Preflight'`
Expected: PASS. (`store.NewMemory` exists; confirm the constructor name with `grep "func NewMemory" internal/store/memory.go` — if it differs, adjust the fixture.)

- [ ] **Step 6: gofmt + commit**

```bash
gofmt -w internal/instance/service.go internal/instance/migrate.go internal/instance/migrate_test.go
git add internal/instance/service.go internal/instance/migrate.go internal/instance/migrate_test.go
git commit -m "feat(instance): migrate request, validation, and dest preflight (#34)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Migrate mutation happy path (stop → copy → apply → verify → commit)

**Files:**
- Modify: `internal/instance/migrate.go` (replace the `return nil // mutation steps added in Task 3` tail; add a verify helper)
- Modify: `internal/instance/migrate_test.go` (add the happy-path test)

- [ ] **Step 1: Write the failing happy-path test** (append to `migrate_test.go`):

```go
func TestMigrate_HappyPath(t *testing.T) {
	svc, f, mem := newMigrateSvc(t)
	ctx := context.Background()
	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}

	// Seed a running source instance: spec + pod + a data volume with bytes.
	seedSpec(t, mem, "h1", "postgres", "db1", params)
	f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
	f.SetVolumeData("h1", "postgres-db1-data", []byte("PGDATA"))

	var steps []string
	err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"},
		func(s, _ string) { steps = append(steps, s) })
	require.NoError(t, err)

	// Source gone.
	_, err = f.PodInspect(ctx, "h1", "postgres-db1")
	require.ErrorIs(t, err, podman.ErrNotFound)
	_, err = mem.GetSpec(ctx, "h1", "postgres", "db1")
	require.ErrorIs(t, err, store.ErrNotFound)
	assert.Nil(t, f.VolumeData("h1", "postgres-db1-data"))

	// Dest running with copied volume bytes + stored spec.
	p, err := f.PodInspect(ctx, "h2", "postgres-db1")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status)
	assert.Equal(t, []byte("PGDATA"), f.VolumeData("h2", "postgres-db1-data"))
	_, err = mem.GetSpec(ctx, "h2", "postgres", "db1")
	require.NoError(t, err)

	assert.Equal(t, []string{"load", "preflight", "stop-source", "copy-volume", "apply-dest", "verify", "commit"}, steps)
}
```

(`f.VolumeData`, `f.SetVolumeData` are the #33 helpers. The source `Delete` prunes the source volume → `VolumeData("h1", …)` becomes nil.)

- [ ] **Step 2: Run it, confirm it fails** (steps stop at `preflight`; dest not built):

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestMigrate_HappyPath`

- [ ] **Step 3: Replace the preflight-only tail of `Migrate` with the full happy path**

In `migrate.go`, replace:

```go
	step("preflight", req.ToHost)
	return nil // mutation steps added in Task 3
```

with:

```go
	step("preflight", req.ToHost)

	// From here the source is mutated; failures before verify roll back.
	if err := s.Stop(ctx, req.FromHost, req.Template, req.Slug); err != nil {
		return fmt.Errorf("stop source: %w", err)
	}
	step("stop-source", req.FromHost)

	if err := s.migratePostStop(ctx, req, eff, spec.Secrets, step); err != nil {
		step("rollback", err.Error())
		_ = s.Start(ctx, req.FromHost, req.Template, req.Slug)
		_ = s.Delete(ctx, req.ToHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true})
		return err
	}

	// Verified healthy: dest is now truth. Commit by reaping the source.
	if err := s.Delete(ctx, req.FromHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); err != nil {
		return fmt.Errorf("commit (delete source): %w", err)
	}
	step("commit", req.FromHost)
	return nil
}

// migratePostStop runs the steps that mutate the destination: copy volumes,
// apply the spec, verify health. Any error here is rolled back by the caller.
func (s *Service) migratePostStop(ctx context.Context, req MigrateRequest, eff map[string]any, secrets map[string]string, step func(step, detail string)) error {
	vols, err := s.InstanceVolumes(ctx, req.FromHost, req.Template, req.Slug)
	if err != nil {
		return fmt.Errorf("list source volumes: %w", err)
	}
	for _, v := range vols {
		if err := s.client.VolumeCreate(ctx, req.ToHost, v.Name); err != nil {
			return fmt.Errorf("create dest volume %q: %w", v.Name, err)
		}
		if err := s.CopyVolume(ctx, req.FromHost, req.ToHost, v.Name); err != nil {
			return fmt.Errorf("copy volume %q: %w", v.Name, err)
		}
		step("copy-volume", v.Name)
	}

	if err := s.Apply(ctx, req.ToHost, ApplyRequest{
		Template: req.Template, Slug: req.Slug, Parameters: eff, Secrets: secrets,
	}, ApplyOptions{Replace: false}); err != nil {
		return fmt.Errorf("apply on dest: %w", err)
	}
	step("apply-dest", req.ToHost)

	if err := s.waitRunning(ctx, req.ToHost, req.Template, req.Slug); err != nil {
		return fmt.Errorf("verify dest: %w", err)
	}
	step("verify", req.ToHost)
	return nil
}

// waitRunning polls the dest pod until Running, bounded by verifyTimeout and the
// caller's context.
func (s *Service) waitRunning(ctx context.Context, host, tmpl, slug string) error {
	deadline := time.Now().Add(verifyTimeout)
	ticker := time.NewTicker(verifyInterval)
	defer ticker.Stop()
	for {
		p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
		if err == nil && p.Status == "Running" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pod %s not running within %s", podName(tmpl, slug), verifyTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
```

Note: `Apply` on the dest pushes per-instance secrets and `PutSpec`s the dest row; `play kube` reuses the volumes created above. `Delete(from, prune)` removes the source pod + volumes + per-instance secrets and `DeleteSpec`s the source row — completing the host move with no new store method.

- [ ] **Step 4: Run the happy-path test, confirm PASS**

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestMigrate_HappyPath`

- [ ] **Step 5: gofmt + commit**

```bash
gofmt -w internal/instance/migrate.go internal/instance/migrate_test.go
git add internal/instance/migrate.go internal/instance/migrate_test.go
git commit -m "feat(instance): migrate stop/copy/apply/verify/commit happy path (#34)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Rollback on failure (copy / apply / verify)

The rollback wiring already exists in `Migrate` (the `migratePostStop` error branch). This task **verifies** it with injected failures. No production change expected unless a test reveals a gap.

**Files:**
- Modify: `internal/instance/migrate_test.go` (add rollback tests)

- [ ] **Step 1: Write the failing rollback tests** (append to `migrate_test.go`):

```go
func TestMigrate_Rollback(t *testing.T) {
	ctx := context.Background()
	base := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}

	// assertRolledBack: source restored & intact, dest fully reaped.
	assertRolledBack := func(t *testing.T, f *fake.Fake, mem *store.Memory) {
		t.Helper()
		p, err := f.PodInspect(ctx, "h1", "postgres-db1")
		require.NoError(t, err)
		assert.Equal(t, "Running", p.Status)               // source restarted
		_, err = mem.GetSpec(ctx, "h1", "postgres", "db1") // source spec intact
		require.NoError(t, err)
		_, err = f.PodInspect(ctx, "h2", "postgres-db1") // dest pod reaped
		require.ErrorIs(t, err, podman.ErrNotFound)
		_, err = mem.GetSpec(ctx, "h2", "postgres", "db1") // dest spec reaped
		require.ErrorIs(t, err, store.ErrNotFound)
		assert.Nil(t, f.VolumeData("h2", "postgres-db1-data")) // dest volume reaped
	}

	seed := func(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
		svc, f, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", base)
		f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
		f.SetVolumeData("h1", "postgres-db1-data", []byte("PGDATA"))
		return svc, f, mem
	}

	t.Run("copy fails", func(t *testing.T) {
		svc, f, mem := seed(t)
		f.ImportErr = assertErr // volume import fails → copy fails
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, nil)
		require.Error(t, err)
		assertRolledBack(t, f, mem)
	})

	t.Run("apply fails", func(t *testing.T) {
		svc, f, mem := seed(t)
		f.PlayKubeErr = assertErr // dest play kube fails → apply fails
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, nil)
		require.Error(t, err)
		assertRolledBack(t, f, mem)
	})

	t.Run("verify fails", func(t *testing.T) {
		svc, f, mem := seed(t)
		// Force the dest pod to never reach Running, and shrink the poll window.
		f.PlayKubePodStatus = "Exited"
		restore := setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)
		defer restore()
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, nil)
		require.Error(t, err)
		assertRolledBack(t, f, mem)
	})
}
```

Add these test-only helpers to `migrate_test.go` (top, after imports):

```go
var assertErr = errors.New("injected failure")

// setVerifyKnobs temporarily shrinks the verify-poll timing for tests.
func setVerifyKnobs(timeout, interval time.Duration) func() {
	ot, oi := verifyTimeout, verifyInterval
	verifyTimeout, verifyInterval = timeout, interval
	return func() { verifyTimeout, verifyInterval = ot, oi }
}
```

Add `"errors"` and `"time"` to the `migrate_test.go` import block.

- [ ] **Step 2: Run, confirm behavior**

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestMigrate_Rollback`

If all three pass, the rollback wiring from Task 3 is correct. If a test fails, fix `Migrate`'s rollback branch in `migrate.go` (do NOT weaken the test) — e.g. ensure `Start(from)` and the dest `Delete(prune)` both run and the source spec is never touched pre-commit. Re-run until green.

- [ ] **Step 3: Run the whole instance package under -race** (the verify poll uses a goroutine-free ticker, but confirm):

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -race ./internal/instance/`

- [ ] **Step 4: commit**

```bash
gofmt -w internal/instance/migrate_test.go internal/instance/migrate.go
git add internal/instance/migrate_test.go internal/instance/migrate.go
git commit -m "test(instance): migrate rolls back on copy/apply/verify failure (#34)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Jobs adapter (`internal/migrate.Handler`)

**Files:**
- Create: `internal/migrate/handler.go`
- Test: `internal/migrate/handler_test.go`

- [ ] **Step 1: Write the failing adapter test**

Create `internal/migrate/handler_test.go`:

```go
package migrate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func TestHandler_Run_MigratesAndRecordsSteps(t *testing.T) {
	ctx := context.Background()
	f := fake.New()
	mem := store.NewMemory()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	svc := instance.NewService(f, hosts, []config.Template{pgFixture()})
	svc.SetStore(mem)

	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}
	require.NoError(t, mem.PutSpec(ctx, store.Spec{Host: "h1", Template: "postgres", Slug: "db1", Parameters: params}))
	f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})

	args, _ := json.Marshal(instance.MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"})
	job, err := mem.Enqueue(ctx, "migrate", args, "")
	require.NoError(t, err)

	h := &Handler{Svc: svc}
	jc := jobs.NewJobContext(mem, job.ID) // see Step 3 for this helper
	require.NoError(t, h.Run(ctx, job, jc))

	// Migration happened.
	p, err := f.PodInspect(ctx, "h2", "postgres-db1")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status)

	// Progress steps were recorded on the job.
	got, err := mem.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, got.Steps)
}

// pgFixture mirrors the instance package's postgres test template.
func pgFixture() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "postgres",
			Parameters: render.Parameters{Required: []string{"slug", "image", "port", "db", "user"}},
			Volumes:    []render.Volume{{Name: "data"}},
		},
		Body: "apiVersion: v1\nkind: Pod\nmetadata:\n  name: postgres-{{.slug}}\nspec:\n  containers:\n    - name: db\n      image: {{.image}}\n",
		Source: "postgres.yaml",
	}
}
```

Add `"github.com/iotready/podman-api/internal/render"` to the test imports (used by `pgFixture`).

- [ ] **Step 2: Confirm `JobContext` is constructible from outside `jobs`**

`JobContext` has unexported fields. Check whether a constructor exists:
`grep -n "func NewJobContext\|type JobContext" internal/jobs/runner.go`

If there is **no** `NewJobContext`, add one to `internal/jobs/runner.go` (it's a legitimate, small enabling change so handlers can be unit-tested outside the runner):

```go
// NewJobContext builds a JobContext for a job id. Exposed so handlers can be
// exercised in tests without the full runner.
func NewJobContext(js store.JobStore, id string) *JobContext {
	return &JobContext{store: js, id: id}
}
```

(If the field names differ, match them — read the struct first.)

- [ ] **Step 3: Create `internal/migrate/handler.go`**

```go
// Package migrate adapts the instance migrate algorithm to the jobs runner.
package migrate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Handler runs "migrate" jobs by delegating to instance.Service.Migrate.
type Handler struct {
	Svc *instance.Service
}

// Run unmarshals the job args into a MigrateRequest and performs the migration,
// reporting progress through the job context.
func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.MigrateRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode migrate args: %w", err)
	}
	return h.Svc.Migrate(ctx, req, jc.Step)
}

// Ensure Handler satisfies the runner contract.
var _ jobs.Handler = (*Handler)(nil)
```

- [ ] **Step 4: Run the adapter test, confirm PASS**

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/migrate/`

- [ ] **Step 5: gofmt + commit**

```bash
gofmt -w internal/migrate/handler.go internal/migrate/handler_test.go internal/jobs/runner.go
git add internal/migrate/handler.go internal/migrate/handler_test.go internal/jobs/runner.go
git commit -m "feat(migrate): jobs.Handler adapter for migrate (#34)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: HTTP endpoint + handler registration

**Files:**
- Create: `internal/api/migrate.go`
- Test: `internal/api/migrate_test.go`
- Modify: `internal/api/router.go` (add the `POST /migrate` route — no signature change)
- Modify: `internal/api/errors.go` (`ErrPortConflict`, `ErrSameHost` mapping)
- Modify: `cmd/podman-api/main.go` (register the `migrate` handler in the `Registry`)

> **Note on `notify`:** we intentionally do NOT wire `runner.Notify()` here. The
> runner polls the queue every 5s, which is fine for migrate; threading a
> notifier through `NewRouter`'s ~10 call sites is deferred (see spec). So
> `NewRouter`'s signature is unchanged.

- [ ] **Step 1: Map the new errors in `internal/api/errors.go`**

`classify` returns **three** values `(code string, status int, msg string)`. Add these cases to the switch (before the `default`):

```go
	case errors.Is(err, instance.ErrPortConflict):
		return "port_conflict", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrSameHost):
		return "invalid_request", http.StatusBadRequest, err.Error()
```

- [ ] **Step 2: Add the route in `internal/api/router.go`**

The `handlers` struct already has `svc` and `jobs`. Register the route next to the jobs routes (scope `instances:write`):

```go
	mux.Handle("POST /migrate", guard("instances:write", http.HandlerFunc(h.migrate)))
```

No struct or `NewRouter` signature change.

- [ ] **Step 3: Create `internal/api/migrate.go`**

```go
package api

import (
	"encoding/json"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
)

func (h *handlers) migrate(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil { // store disabled → migrate unavailable
		WriteError(w, errJobsDisabled)
		return
	}
	var req instance.MigrateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if err := h.svc.CheckMigratable(r.Context(), req); err != nil {
		WriteError(w, err)
		return
	}
	args, err := json.Marshal(req)
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "migrate", args, "")
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
}
```

Confirm `errJobsDisabled`, `WriteJSON`, `WriteError`, `ErrorBody`, and `Enqueue`'s
signature by reading `internal/api/jobs.go`, `internal/api/errors.go`, and
`internal/store/jobs.go`. `errJobsDisabled` already maps to 501. `ErrorBody`'s
fields are `Code`/`Message` (see `errors.go`).

- [ ] **Step 4: Write the API test**

The existing api tests post bodies with raw `http.NewRequest` + a `Bearer` header
(see `internal/api/bulk_test.go` / `instances_test.go`); `authedReq` is body-less,
so do NOT use it here. Build a store-enabled harness modeled on `newSrvWithJobs`
(`internal/api/jobs_test.go`) but with an `instances:write` token, a postgres
template, and the store wired onto the Service.

Create `internal/api/migrate_test.go`:

```go
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func migrateTmpl() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "postgres",
			Parameters: render.Parameters{Required: []string{"slug", "image", "port", "db", "user"}},
			Volumes:    []render.Volume{{Name: "data"}},
		},
		Body:   "apiVersion: v1\nkind: Pod\nmetadata:\n  name: postgres-{{.slug}}\nspec:\n  containers:\n    - name: db\n      image: {{.image}}\n",
		Source: "postgres.yaml",
	}
}

// newMigrateSrv builds a store-enabled server whose token carries instances:write.
func newMigrateSrv(t *testing.T) (*httptest.Server, string, *fake.Fake, *store.Memory) {
	t.Helper()
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	f := fake.New()
	mem := store.NewMemory()
	svc := instance.NewService(f, hosts, []config.Template{migrateTmpl()})
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok, f, mem
}

func postMigrate(t *testing.T, srv *httptest.Server, tok string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", srv.URL+"/migrate", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestMigrate_API(t *testing.T) {
	srv, tok, f, mem := newMigrateSrv(t)
	ctx := context.Background()
	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}
	require.NoError(t, mem.PutSpec(ctx, store.Spec{Host: "h1", Template: "postgres", Slug: "db1", Parameters: params}))
	f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})

	// happy → 202 with job_id, and a migrate job is enqueued
	resp := postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"})
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var acc struct{ JobID string `json:"job_id"` }
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&acc))
	require.NotEmpty(t, acc.JobID)
	job, err := mem.GetJob(ctx, acc.JobID)
	require.NoError(t, err)
	assert.Equal(t, "migrate", job.Kind)

	// same host → 400
	resp = postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "h1", ToHost: "h1", Template: "postgres", Slug: "db1"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// unknown host → 404
	resp = postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "nope", ToHost: "h2", Template: "postgres", Slug: "db1"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// missing spec → 404
	resp = postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "absent"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestMigrate_API_StoreDisabled_501(t *testing.T) {
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	svc := instance.NewService(fake.New(), hosts, []config.Template{migrateTmpl()})
	// no SetStore: store disabled; pass nil jobs to NewRouter so h.jobs == nil.
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil))
	t.Cleanup(srv.Close)

	resp := postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"})
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}
```

(`store.Memory` implements both `Store` and `JobStore`, so passing `mem` as the
`jobs` arg to `NewRouter` and calling `mem.GetJob`/`mem.Enqueue` both work.
Confirm `GetJob` exists on `store.Memory` — `grep "func (m \*Memory) GetJob" internal/store/memory.go`.)

- [ ] **Step 5: Confirm the test fails** (no route / no `migrate.go`), then run after implementing:

`go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestMigrate`

- [ ] **Step 6: Wire `cmd/podman-api/main.go`**

In the store-enabled branch where the runner is built with `jobs.Registry{}`,
register the migrate handler:

```go
registry := jobs.Registry{
	"migrate": &migrate.Handler{Svc: svc},
}
runner := jobs.NewRunner(db, registry, jobs.DefaultWorkers)
runner.Start(runnerCtx)
```

Add `"github.com/iotready/podman-api/internal/migrate"` to main's imports. The
`NewRouter(...)` call is unchanged (still `api.NewRouter(svc, jobStore, keyStore, combined, nil)`).

- [ ] **Step 7: Build + run**

```bash
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestMigrate
make build
```

- [ ] **Step 8: gofmt + commit**

```bash
gofmt -w internal/api/migrate.go internal/api/router.go internal/api/errors.go internal/api/migrate_test.go cmd/podman-api/main.go
git add internal/api/migrate.go internal/api/router.go internal/api/errors.go internal/api/migrate_test.go cmd/podman-api/main.go
git commit -m "feat(api): POST /migrate enqueues a migrate job (#34)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```


## Task 7: Real-client VolumeCreate integration test

**Files:**
- Modify: `internal/podman/real_volcopy_integration_test.go`

- [ ] **Step 1: Replace the direct `volumes.Create` calls with the new client method**

The #33 integration test (`TestReal_VolumeCopy_LocalOnly`) currently creates the src/dst volumes via `volumes.Create(conn, …)`. Switch them to exercise the new client method instead, so `VolumeCreate` gets real coverage:

```go
	for _, name := range []string{src, dst} {
		_ = c.VolumeRemove(ctx, "local", name, true) // clear any leftover
		require.NoError(t, c.VolumeCreate(ctx, "local", name))
	}
```

Then drop the now-unused `volumes`/`types` imports if nothing else in the file uses them (the compiler will tell you).

- [ ] **Step 2: Add an idempotency assertion** (right after the create loop):

```go
	// VolumeCreate is idempotent — creating an existing volume is not an error.
	require.NoError(t, c.VolumeCreate(ctx, "local", src))
```

- [ ] **Step 3: Confirm it compiles under the integration tag**

`go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper integration" ./internal/podman/`

- [ ] **Step 4: Run if a podman v5 socket is available** (best-effort; CI runs it on `quay.io/podman/stable`):

`make test-integration` — if it fails only because the local daemon predates podman v5 (no export/import/idempotent-create), that's an environment limitation, not a defect; report it. Do not weaken the test.

- [ ] **Step 5: gofmt + commit**

```bash
gofmt -w internal/podman/real_volcopy_integration_test.go
git add internal/podman/real_volcopy_integration_test.go
git commit -m "test(podman): exercise VolumeCreate in the volume integration test (#34)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Wrap-up (after all tasks)

- [ ] `make test` green; `gofmt -l .` empty; `go vet` (with tags) clean; `make build` ok.
- [ ] Open one PR for #34: `forgejo pr create tej/podman-api --head=feat/34-migrate --base=main --title="migrate primitive (POST /migrate) (#34)" --body=...` — link the spec/plan, note it closes #34, registers the first real job kind, and that the two #33 follow-ups (stream timeout, error-locus) remain open for a later pass.

---

## Self-review notes

- **Spec coverage:** `VolumeCreate` (Task 1) ✔; `MigrateRequest`/sync validation/preflight incl. ports-secrets-drain-no-clobber (Task 2) ✔; stop/copy/apply/verify/commit + store host-move via Apply+Delete (Task 3) ✔; rollback on copy/apply/verify (Task 4) ✔; `jobs.Handler` adapter (Task 5) ✔; `POST /migrate` 202 + hybrid sync validation (404/400/501) + main registration + `ErrPortConflict`→409 (Task 6) ✔; integration coverage for `VolumeCreate` (Task 7) ✔; migrate-scoped lock (Task 2 `migrateLock`, used in Task 3's `Migrate`) ✔; plaintext args / secrets-from-store (Task 5 handler unmarshals args, Task 3 reads `spec.Secrets`) ✔. `notify` is deferred (runner polls ≤5s) — no `NewRouter` change.
- **Type consistency:** `MigrateRequest{FromHost,ToHost,Template,Slug,Parameters}` is identical across instance, migrate adapter, and api. `Service.Migrate(ctx, MigrateRequest, func(string,string))`, `Service.CheckMigratable(ctx, MigrateRequest) error`, `Handler{Svc}` are used consistently. `NewRouter`'s signature is unchanged. `verifyTimeout`/`verifyInterval` are package vars overridden via `setVerifyKnobs`.
- **Verified against the code while planning:** `store.NewMemory()` exists; `classify` returns `(code, status, msg)` (3 values — snippets match); `NewRouter` is unchanged so its existing call sites are untouched; api POST-with-body tests use raw `http.NewRequest`+`Bearer` (not `authedReq`); `store.Memory` implements both `Store` and `JobStore`. **Still confirm while implementing:** `jobs.NewJobContext` does not exist yet — Task 5 adds it (the `JobContext` fields are `store store.JobStore` and `id string`); `store.Memory.GetJob` exists.
- **Placeholder scan:** no TBD/TODO; every code step has complete code. The few "confirm the name" notes point at real names to read, not missing logic.
