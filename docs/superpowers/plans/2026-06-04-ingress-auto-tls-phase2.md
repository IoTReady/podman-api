# Ingress + auto-TLS — Phase 2 (Caddy controller) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the Phase 1 ingress foundations into a working per-host Caddy reverse proxy that terminates TLS via HTTP-01 ACME and routes each instance's domains to its pod over a shared podman network — reconciled automatically on every deploy/delete.

**Architecture:** Per-host managed Caddy (topology B, `docs/superpowers/specs/2026-06-04-ingress-auto-tls-design.md`). A `CaddyController` ensures a shared ingress network and a Caddy system pod on each host (publishing :80/:443, config+data on named volumes), then on every instance change it derives the host's routes from the store, renders a Caddyfile (Phase 1's `ingress.RenderCaddyfile`), copies it into the running Caddy container, and `caddy reload`s — all over the existing podman socket. App pods join the ingress network and expose no host ports; the route backend is `<pod-name>:<container-port>` (the spike confirmed pod names resolve via aardvark and are globally unique). Reconcile runs inline on create/delete/upgrade and on a periodic drift-correction loop, serialized per host.

**Tech Stack:** Go, `github.com/containers/podman/v5 v5.8.2` bindings, `modernc.org/sqlite`, `gopkg.in/yaml.v3`, `testify/require`, Caddy v2.

**Build/test note (from CLAUDE.md):** all builds/tests need the remote-client tags. Every command below assumes:
```sh
export TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"
```
Run the full suite with `make test`; targeted runs use `go test -tags "$TAGS" <pkg> -run <name> -v`.

**Spike inputs applied (`docs/superpowers/specs/2026-06-04-ingress-spike-findings.md`):**
- Backend = `<pod-name>:<container-port>`; pod name = `<template>-<slug>` (globally unique). Bare container names collide and must not be used.
- Caddy data volume preserves the cert + ACME account across restarts — no re-issue.
- `podman cp` + `caddy reload` over the socket apply config with zero downtime.
- **Port-binding decision: rootless + sysctl.** The Caddy pod runs in the same rootless podman podman-api already drives; the host must set `net.ipv4.ip_unprivileged_port_start` ≤ 80 (persisted) so the pod can publish :80/:443. This is a provisioning prerequisite recorded in Task 10 + the wiki.

**Collision note (parallel work on #54):** the only behavioural change to a shared file is one new line in `internal/instance/service.go` (`Apply`) plus a new field/setter, and a *variadic* extension to `PlayKube` (so existing callers/tests are untouched). Everything else is new files. If a merge conflict with #54 arises it will be localized to those few lines.

---

## Scope of this plan

**In this plan:** the podman client primitives (network ensure, exec, copy-into-container, network-aware play), the `ingress.Controller` + `CaddyController`, route derivation with host-wide uniqueness + "ingress-required" enforcement, Service wiring, `main()` flags + periodic reconcile, and an integration test.

**Not in this plan:** the `DNSProvider` automation seam (operator manages DNS for v1 — a no-op is documented in the design, no code needed until automation is wanted), and the operator wiki page (published separately, outside the PR flow — Task 10 lists exactly what it must say).

## File structure

| File | Responsibility | Task |
|---|---|---|
| `internal/podman/client.go` (modify) | Extend `PlayKube` signature; add `NetworkEnsure`, `ContainerExec`, `CopyToContainer`; add `ExecResult` | 1–4 |
| `internal/podman/real.go` (modify) | Real libpod implementations of the above | 1–4 |
| `internal/podman/fake/fake.go` (modify) | Fake implementations + call recording | 1–4 |
| `internal/podman/fake/fake_ingress_test.go` (create) | Fake-behaviour tests for the new methods | 1–4 |
| `internal/ingress/controller.go` (create) | `Controller` interface, `Disabled` no-op, `CaddyController`, `Config`, `TemplateIngress` | 5 |
| `internal/ingress/routes.go` (create) | `deriveRoutes` (enumerate specs → `[]Route`, enforce uniqueness + ingress-required) | 6 |
| `internal/ingress/routes_test.go` (create) | Route-derivation tests | 6 |
| `internal/ingress/caddypod.go` (create) | Caddy pod kube-YAML builder + `ensureProxy` | 7 |
| `internal/ingress/caddypod_test.go` (create) | EnsureProxy tests | 7 |
| `internal/ingress/reconcile.go` (create) | `Reconcile` (lock → derive → ensure → render → cp → validate → reload) | 8 |
| `internal/ingress/reconcile_test.go` (create) | Reconcile tests | 8 |
| `internal/instance/service.go` (modify) | `ingress` field + `SetIngress`; attach network in `Apply`; reconcile after create/delete; reject domains when disabled | 9 |
| `internal/instance/service_ingress_test.go` (create) | Service-level ingress wiring tests | 9 |
| `internal/cmd/podman-api/main.go` (modify) | `-ingress-*` flags, controller construction, periodic reconcile loop, shutdown | 10 |
| `internal/ingress/integration_test.go` (create) | `integration`-tagged end-to-end (Pebble ACME) | 11 |

---

## Task 1: Make `PlayKube` network-aware (variadic)

App pods must join the ingress network at creation so aardvark resolves them by pod name. `play.KubeOptions` (alias of `kube.PlayOptions`) carries `Network *[]string`. A **variadic** parameter keeps every existing caller and test compiling unchanged.

**Files:**
- Modify: `internal/podman/client.go:14`, `internal/podman/real.go:141`, `internal/podman/fake/fake.go:159`
- Test: `internal/podman/fake/fake_ingress_test.go` (create)

- [ ] **Step 1: Write the failing test**

Create `internal/podman/fake/fake_ingress_test.go`:
```go
package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlayKubeRecordsNetworks(t *testing.T) {
	f := New()
	yaml := "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web-a\nspec:\n  containers:\n    - name: web\n      image: nginx\n"
	require.NoError(t, f.PlayKube(context.Background(), "h1", yaml, false, "podman-api-ingress"))
	require.Len(t, f.PlayCalls, 1)
	require.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/podman/fake/ -run TestPlayKubeRecordsNetworks -v`
Expected: FAIL — `PlayKube` takes 4 args (no variadic), `f.PlayCalls` undefined (compile error).

- [ ] **Step 3: Update the interface**

In `internal/podman/client.go`, change the `PlayKube` line (currently line 14):
```go
	PlayKube(ctx context.Context, hostID, yaml string, replace bool, networks ...string) error
```

- [ ] **Step 4: Update the real implementation**

In `internal/podman/real.go`, change `PlayKube`'s signature and set the option (replace the body at lines 141–163):
```go
func (r *Real) PlayKube(ctx context.Context, id, raw string, replace bool, networks ...string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "play-kube-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(raw); err != nil {
		return err
	}
	tmp.Close()

	opts := &play.KubeOptions{}
	if replace {
		t := true
		opts.Replace = &t
	}
	if len(networks) > 0 {
		opts.Network = &networks
	}
	_, err = play.Kube(c, tmp.Name(), opts)
	return err
}
```

- [ ] **Step 5: Update the fake + add call recording**

In `internal/podman/fake/fake.go`, add a `PlayCall` type and a `PlayCalls` field, and record in `PlayKube`. Add near the other hook fields in the `Fake` struct (after `PodListErr error` around line 69):
```go
	// PlayCalls records every PlayKube invocation (host, replace, networks).
	PlayCalls []PlayCall
```
Add the type below the `Fake` struct (after line 90):
```go
// PlayCall records one PlayKube invocation for assertions.
type PlayCall struct {
	Host     string
	Replace  bool
	Networks []string
}
```
Change the `PlayKube` signature (line 159) and record the call as the first statement inside the lock:
```go
func (f *Fake) PlayKube(_ context.Context, hostID, raw string, replace bool, networks ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PlayCalls = append(f.PlayCalls, PlayCall{Host: hostID, Replace: replace, Networks: networks})
	if f.PlayKubeErr != nil {
		return f.PlayKubeErr
	}
```
(Leave the rest of `PlayKube`'s body unchanged.)

- [ ] **Step 6: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/podman/... -run 'TestPlayKube|PlayKube' -v`
Expected: PASS (new test + existing PlayKube tests still green, since variadic is source-compatible).

- [ ] **Step 7: Commit**

```sh
git add internal/podman/client.go internal/podman/real.go internal/podman/fake/fake.go internal/podman/fake/fake_ingress_test.go
git commit -m "feat(podman): network-aware PlayKube (variadic networks) (#60)"
```

---

## Task 2: `NetworkEnsure`

Idempotent network creation. The real impl uses `network.CreateWithOptions` with `IgnoreIfExists` so concurrent ensures don't race.

**Files:**
- Modify: `internal/podman/client.go`, `internal/podman/real.go`, `internal/podman/fake/fake.go`
- Test: `internal/podman/fake/fake_ingress_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/podman/fake/fake_ingress_test.go`:
```go
func TestNetworkEnsureRecords(t *testing.T) {
	f := New()
	require.NoError(t, f.NetworkEnsure(context.Background(), "h1", "podman-api-ingress"))
	require.NoError(t, f.NetworkEnsure(context.Background(), "h1", "podman-api-ingress")) // idempotent
	require.Equal(t, []string{"podman-api-ingress", "podman-api-ingress"}, f.NetworkEnsureCalls["h1"])
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/podman/fake/ -run TestNetworkEnsureRecords -v`
Expected: FAIL — `NetworkEnsure` undefined.

- [ ] **Step 3: Add to the interface**

In `internal/podman/client.go`, add a `// Networks` group after the `// Volumes` block (after line 39):
```go
	// Networks
	// NetworkEnsure creates the named network if absent; creating one that
	// already exists is a no-op (no error).
	NetworkEnsure(ctx context.Context, hostID, name string) error
```

- [ ] **Step 4: Add the real implementation**

In `internal/podman/real.go`, add the imports `network "github.com/containers/podman/v5/pkg/bindings/network"` and `nettypes "go.podman.io/common/libnetwork/types"` to the import block, then add the method (place it after `PlayKube`):
```go
func (r *Real) NetworkEnsure(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	ignore := true
	_, err = network.CreateWithOptions(c, &nettypes.Network{Name: name},
		&network.ExtraCreateOptions{IgnoreIfExists: &ignore})
	return err
}
```

- [ ] **Step 5: Add the fake implementation**

In `internal/podman/fake/fake.go`, add a field to the `Fake` struct:
```go
	// NetworkEnsureCalls records, per host, the network names ensured.
	NetworkEnsureCalls map[string][]string
	// NetworkEnsureErr, if non-nil, makes NetworkEnsure fail.
	NetworkEnsureErr error
```
Initialize the map in `New()` (add to the returned literal):
```go
		NetworkEnsureCalls: map[string][]string{},
```
Add the method (near the volume methods):
```go
func (f *Fake) NetworkEnsure(_ context.Context, host, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.NetworkEnsureErr != nil {
		return f.NetworkEnsureErr
	}
	f.NetworkEnsureCalls[host] = append(f.NetworkEnsureCalls[host], name)
	return nil
}
```

- [ ] **Step 6: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/podman/fake/ -run TestNetworkEnsureRecords -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```sh
git add internal/podman/client.go internal/podman/real.go internal/podman/fake/fake.go internal/podman/fake/fake_ingress_test.go
git commit -m "feat(podman): NetworkEnsure (idempotent network create) (#60)"
```

---

## Task 3: `ContainerExec`

Run a command in a running container and capture its exit code + combined output (used for `caddy validate` and `caddy reload`).

**Files:**
- Modify: `internal/podman/client.go`, `internal/podman/real.go`, `internal/podman/fake/fake.go`
- Test: `internal/podman/fake/fake_ingress_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/podman/fake/fake_ingress_test.go`:
```go
func TestContainerExecHookAndRecord(t *testing.T) {
	f := New()
	f.ExecFunc = func(host, container string, cmd []string) (podman.ExecResult, error) {
		return podman.ExecResult{ExitCode: 0, Output: "ok"}, nil
	}
	res, err := f.ContainerExec(context.Background(), "h1", "caddy", []string{"caddy", "reload"})
	require.NoError(t, err)
	require.Equal(t, 0, res.ExitCode)
	require.Equal(t, "ok", res.Output)
	require.Len(t, f.ExecCalls, 1)
	require.Equal(t, []string{"caddy", "reload"}, f.ExecCalls[0].Cmd)
}
```
Add the import `"github.com/iotready/podman-api/internal/podman"` to the test file's import block (alongside `context`, `testing`, and `require`).

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/podman/fake/ -run TestContainerExecHookAndRecord -v`
Expected: FAIL — `ContainerExec`, `ExecResult`, `ExecFunc`, `ExecCalls` undefined.

- [ ] **Step 3: Add to the interface + result type**

In `internal/podman/client.go`, add a `// Exec` group after the `// Networks` block:
```go
	// Exec
	// ContainerExec runs cmd in the named running container and returns its
	// exit code and combined stdout+stderr. A non-zero exit code is NOT an
	// error; only a transport/podman failure returns a non-nil error.
	ContainerExec(ctx context.Context, hostID, container string, cmd []string) (ExecResult, error)
```
Add the result type near `PruneReport` (after line 78):
```go
// ExecResult is the outcome of ContainerExec.
type ExecResult struct {
	ExitCode int
	Output   string // combined stdout+stderr
}
```

- [ ] **Step 4: Add the real implementation**

In `internal/podman/real.go`, add imports `"bytes"`, `handlers "github.com/containers/podman/v5/pkg/api/handlers"`, and `dockerContainer "github.com/docker/docker/api/types/container"`, then add:
```go
func (r *Real) ContainerExec(ctx context.Context, id, container string, cmd []string) (ExecResult, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return ExecResult{}, err
	}
	sessionID, err := containers.ExecCreate(c, container, &handlers.ExecCreateConfig{
		ExecOptions: dockerContainer.ExecOptions{
			Cmd:          cmd,
			AttachStdout: true,
			AttachStderr: true,
		},
	})
	if err != nil {
		return ExecResult{}, mapNotFound(err)
	}
	var buf bytes.Buffer
	var w io.Writer = &buf
	attach := true
	if err := containers.ExecStartAndAttach(c, sessionID, &containers.ExecStartAndAttachOptions{
		OutputStream: &w,
		ErrorStream:  &w,
		AttachOutput: &attach,
		AttachError:  &attach,
	}); err != nil {
		return ExecResult{}, err
	}
	ins, err := containers.ExecInspect(c, sessionID, &containers.ExecInspectOptions{})
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{ExitCode: ins.ExitCode, Output: buf.String()}, nil
}
```
(`io` is already imported in `real.go`; `mapNotFound` already exists there.)

- [ ] **Step 5: Add the fake implementation**

In `internal/podman/fake/fake.go`, add fields:
```go
	// ExecFunc, if set, produces the ContainerExec result for tests. Default
	// (nil) returns ExitCode 0, empty output.
	ExecFunc func(host, container string, cmd []string) (podman.ExecResult, error)
	// ExecCalls records every ContainerExec invocation.
	ExecCalls []ExecCall
```
Add the type:
```go
// ExecCall records one ContainerExec invocation for assertions.
type ExecCall struct {
	Host      string
	Container string
	Cmd       []string
}
```
Add the method:
```go
func (f *Fake) ContainerExec(_ context.Context, host, container string, cmd []string) (podman.ExecResult, error) {
	f.mu.Lock()
	f.ExecCalls = append(f.ExecCalls, ExecCall{Host: host, Container: container, Cmd: cmd})
	fn := f.ExecFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(host, container, cmd)
	}
	return podman.ExecResult{}, nil
}
```

- [ ] **Step 6: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/podman/fake/ -run TestContainerExecHookAndRecord -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```sh
git add internal/podman/client.go internal/podman/real.go internal/podman/fake/fake.go internal/podman/fake/fake_ingress_test.go
git commit -m "feat(podman): ContainerExec with exit code + combined output (#60)"
```

---

## Task 4: `CopyToContainer`

Write a single in-memory file into a directory inside a running container (the new Caddyfile into `/etc/caddy`).

**Files:**
- Modify: `internal/podman/client.go`, `internal/podman/real.go`, `internal/podman/fake/fake.go`
- Test: `internal/podman/fake/fake_ingress_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/podman/fake/fake_ingress_test.go`:
```go
func TestCopyToContainerRecords(t *testing.T) {
	f := New()
	require.NoError(t, f.CopyToContainer(context.Background(), "h1", "caddy", "/etc/caddy", "Caddyfile", []byte("hello")))
	require.Len(t, f.CopyCalls, 1)
	got := f.CopyCalls[0]
	require.Equal(t, "caddy", got.Container)
	require.Equal(t, "/etc/caddy", got.DestDir)
	require.Equal(t, "Caddyfile", got.Name)
	require.Equal(t, []byte("hello"), got.Content)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/podman/fake/ -run TestCopyToContainerRecords -v`
Expected: FAIL — `CopyToContainer`, `CopyCalls` undefined.

- [ ] **Step 3: Add to the interface**

In `internal/podman/client.go`, extend the `// Exec` group (or add a `// Copy` line) after `ContainerExec`:
```go
	// CopyToContainer writes content as a single file `name` into directory
	// `destDir` inside the running container (e.g. destDir="/etc/caddy",
	// name="Caddyfile"). destDir must already exist in the container.
	CopyToContainer(ctx context.Context, hostID, container, destDir, name string, content []byte) error
```

- [ ] **Step 4: Add the real implementation**

In `internal/podman/real.go`, add the import `"archive/tar"` (`bytes` was added in Task 3), then:
```go
func (r *Real) CopyToContainer(ctx context.Context, id, container, destDir, name string, content []byte) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(content)),
	}); err != nil {
		return err
	}
	if _, err := tw.Write(content); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	// CopyFromArchive copies INTO the container (PUT /containers/{id}/archive).
	copyFn, err := containers.CopyFromArchive(c, container, destDir, &tarBuf)
	if err != nil {
		return mapNotFound(err)
	}
	return copyFn()
}
```

- [ ] **Step 5: Add the fake implementation**

In `internal/podman/fake/fake.go`, add fields:
```go
	// CopyCalls records every CopyToContainer invocation.
	CopyCalls []CopyCall
	// CopyErr, if non-nil, makes CopyToContainer fail.
	CopyErr error
```
Add the type:
```go
// CopyCall records one CopyToContainer invocation for assertions.
type CopyCall struct {
	Host      string
	Container string
	DestDir   string
	Name      string
	Content   []byte
}
```
Add the method:
```go
func (f *Fake) CopyToContainer(_ context.Context, host, container, destDir, name string, content []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.CopyErr != nil {
		return f.CopyErr
	}
	cp := make([]byte, len(content))
	copy(cp, content)
	f.CopyCalls = append(f.CopyCalls, CopyCall{Host: host, Container: container, DestDir: destDir, Name: name, Content: cp})
	return nil
}
```

- [ ] **Step 6: Run to verify pass + confirm fake still satisfies the interface**

Run: `go test -tags "$TAGS" ./internal/podman/... -v`
Expected: PASS. The compile-time guard `var _ podman.Client = (*Fake)(nil)` (fake.go:483) now covers all four new methods.

- [ ] **Step 7: Commit**

```sh
git add internal/podman/client.go internal/podman/real.go internal/podman/fake/fake.go internal/podman/fake/fake_ingress_test.go
git commit -m "feat(podman): CopyToContainer (single file into running container) (#60)"
```

---

## Task 5: Ingress `Controller` interface, no-op, and `CaddyController` skeleton

Define the controller contract and the types Service + main depend on. The no-op `Disabled` lets Service always hold a non-nil controller.

**Files:**
- Create: `internal/ingress/controller.go`
- Test: `internal/ingress/controller_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/ingress/controller_test.go`:
```go
package ingress

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDisabledControllerIsNoOp(t *testing.T) {
	var c Controller = Disabled{}
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestDisabledControllerIsNoOp -v`
Expected: FAIL — `Controller`, `Disabled` undefined.

- [ ] **Step 3: Implement the skeleton**

Create `internal/ingress/controller.go`:
```go
package ingress

import (
	"context"
	"sync"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// Controller reconciles a host's ingress (Caddy) state with the store.
type Controller interface {
	// Reconcile makes the host's Caddy proxy match the routes derived from the
	// store: ensures the network + Caddy pod exist, renders the Caddyfile, and
	// applies it (zero-downtime reload). Safe to call repeatedly; serialized
	// per host.
	Reconcile(ctx context.Context, host string) error
}

// Disabled is the no-op Controller used when ingress is turned off. Reconcile
// does nothing so the rest of the system can call it unconditionally.
type Disabled struct{}

func (Disabled) Reconcile(context.Context, string) error { return nil }

// TemplateIngress is a template's ingress declaration, supplied to the
// controller so it can compute backends without importing the template loader.
// Only templates whose meta declares ingress: appear in the controller's map.
type TemplateIngress struct {
	Container string // declared HTTP container name (informational)
	Port      int    // container port the backend points at
}

// Config holds the operator-set knobs for the Caddy controller.
type Config struct {
	Network    string // shared ingress network name (e.g. "podman-api-ingress")
	CaddyImage string // e.g. "docker.io/library/caddy:2"
	ACMEEmail  string // ACME account email for the global Caddyfile block
}

// CaddyController is the production Controller. It drives a per-host Caddy pod
// over the existing podman socket.
type CaddyController struct {
	client    podman.Client
	store     store.Store
	templates map[string]TemplateIngress // template id -> ingress decl
	cfg       Config

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-host serialization
}

// NewCaddyController builds a controller. templates must include an entry for
// every template that declares ingress: in its meta.
func NewCaddyController(client podman.Client, st store.Store, templates map[string]TemplateIngress, cfg Config) *CaddyController {
	return &CaddyController{
		client:    client,
		store:     st,
		templates: templates,
		cfg:       cfg,
		locks:     map[string]*sync.Mutex{},
	}
}

// hostLock returns the per-host mutex, creating it on first use.
func (c *CaddyController) hostLock(host string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.locks[host]
	if !ok {
		m = &sync.Mutex{}
		c.locks[host] = m
	}
	return m
}

// Compile-time guarantees.
var (
	_ Controller = Disabled{}
	_ Controller = (*CaddyController)(nil)
)
```

- [ ] **Step 4: Run to verify it fails to compile (Reconcile not yet on CaddyController)**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestDisabledControllerIsNoOp -v`
Expected: FAIL — `*CaddyController does not implement Controller (missing Reconcile)`. This is expected; `Reconcile` lands in Task 8. Temporarily comment out the `_ Controller = (*CaddyController)(nil)` line to get a green checkpoint, with a `// TODO(task 8): re-enable once Reconcile lands` note, OR proceed knowing Tasks 6–8 complete the type. **Choose:** leave the guard commented with the TODO so this task commits green.

- [ ] **Step 5: Comment the pending guard and re-run**

Edit the guard block to:
```go
var (
	_ Controller = Disabled{}
	// _ Controller = (*CaddyController)(nil) // TODO(task 8): re-enable once Reconcile lands
)
```
Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestDisabledControllerIsNoOp -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```sh
git add internal/ingress/controller.go internal/ingress/controller_test.go
git commit -m "feat(ingress): Controller interface, Disabled no-op, CaddyController skeleton (#60)"
```

---

## Task 6: Route derivation

Enumerate a host's instances, enforce the two design rules (a domain'd instance's template **must** declare `ingress:`; **host-wide domain uniqueness**), and build `[]Route` with `Backend = <pod-name>:<port>`.

**Files:**
- Create: `internal/ingress/routes.go`
- Test: `internal/ingress/routes_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/ingress/routes_test.go`:
```go
package ingress

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/store"
)

// memStore is a minimal in-memory store.Store for route-derivation tests.
type memStore struct{ specs []store.Spec }

func (m *memStore) PutSpec(context.Context, store.Spec) error { return nil }
func (m *memStore) GetSpec(_ context.Context, host, tmpl, slug string) (store.Spec, error) {
	for _, s := range m.specs {
		if s.Host == host && s.Template == tmpl && s.Slug == slug {
			return s, nil
		}
	}
	return store.Spec{}, store.ErrNotFound
}
func (m *memStore) DeleteSpec(context.Context, string, string, string) error { return nil }
func (m *memStore) ListSpecKeys(_ context.Context, host string) ([]store.SpecKey, error) {
	var out []store.SpecKey
	for _, s := range m.specs {
		if s.Host == host {
			out = append(out, store.SpecKey{Template: s.Template, Slug: s.Slug})
		}
	}
	return out, nil
}

func newCtl(specs []store.Spec, tmpls map[string]TemplateIngress) *CaddyController {
	return NewCaddyController(nil, &memStore{specs: specs}, tmpls, Config{})
}

func TestDeriveRoutesHappyPath(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}, Updated: time.Now()},
		{Host: "h1", Template: "postgres", Slug: "db", Domains: nil}, // no domains -> skipped
	}
	c := newCtl(specs, map[string]TemplateIngress{"web": {Container: "web", Port: 8080}})
	routes, err := c.deriveRoutes(context.Background(), "h1")
	require.NoError(t, err)
	require.Equal(t, []Route{{Domain: "blog.example.com", Backend: "web-blog:8080"}}, routes)
}

func TestDeriveRoutesRejectsDomainsWithoutIngress(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "postgres", Slug: "db", Domains: []string{"db.example.com"}},
	}
	c := newCtl(specs, map[string]TemplateIngress{}) // postgres declares no ingress
	_, err := c.deriveRoutes(context.Background(), "h1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "declares no ingress")
}

func TestDeriveRoutesRejectsDuplicateDomain(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "web", Slug: "a", Domains: []string{"x.example.com"}},
		{Host: "h1", Template: "web", Slug: "b", Domains: []string{"x.example.com"}},
	}
	c := newCtl(specs, map[string]TemplateIngress{"web": {Port: 8080}})
	_, err := c.deriveRoutes(context.Background(), "h1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "claimed by")
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestDeriveRoutes -v`
Expected: FAIL — `deriveRoutes` undefined.

- [ ] **Step 3: Implement**

Create `internal/ingress/routes.go`:
```go
package ingress

import (
	"context"
	"fmt"
	"sort"
	"strconv"
)

// podName mirrors instance.podName: an instance's pod is "<template>-<slug>",
// which is globally unique and the name aardvark resolves on the network. The
// route backend points at the pod name (NOT a bare container name, which is not
// unique across pods).
func podName(template, slug string) string { return template + "-" + slug }

// deriveRoutes builds the host's ingress routes from the store. It enforces two
// design rules:
//   - an instance carrying domains whose template declares no ingress: is an
//     operator error (rejected), and
//   - a domain may be claimed by at most one instance on the host.
//
// Instances without domains are skipped. The returned slice is domain-sorted
// for a stable Caddyfile.
func (c *CaddyController) deriveRoutes(ctx context.Context, host string) ([]Route, error) {
	keys, err := c.store.ListSpecKeys(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("ingress: list specs for %s: %w", host, err)
	}
	owner := map[string]string{} // domain -> "template/slug"
	var routes []Route
	for _, k := range keys {
		sp, err := c.store.GetSpec(ctx, host, k.Template, k.Slug)
		if err != nil {
			return nil, fmt.Errorf("ingress: get spec %s/%s: %w", k.Template, k.Slug, err)
		}
		if len(sp.Domains) == 0 {
			continue
		}
		ti, ok := c.templates[k.Template]
		if !ok {
			return nil, fmt.Errorf("ingress: instance %s/%s has domains but template %q declares no ingress", k.Template, k.Slug, k.Template)
		}
		backend := podName(k.Template, k.Slug) + ":" + strconv.Itoa(ti.Port)
		for _, d := range sp.Domains {
			if prev, dup := owner[d]; dup {
				return nil, fmt.Errorf("ingress: domain %q claimed by both %s and %s/%s", d, prev, k.Template, k.Slug)
			}
			owner[d] = k.Template + "/" + k.Slug
			routes = append(routes, Route{Domain: d, Backend: backend})
		}
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].Domain < routes[j].Domain })
	return routes, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestDeriveRoutes -v`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```sh
git add internal/ingress/routes.go internal/ingress/routes_test.go
git commit -m "feat(ingress): derive host routes with uniqueness + ingress-required checks (#60)"
```

---

## Task 7: Caddy pod + `ensureProxy`

Ensure the network and Caddy system pod exist. Seed the config volume with an initial Caddyfile via `VolumeImport` (so Caddy boots with a valid config — no chicken-and-egg), then play the pod on the ingress network publishing :80/:443.

**Files:**
- Create: `internal/ingress/caddypod.go`
- Test: `internal/ingress/caddypod_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/ingress/caddypod_test.go`:
```go
package ingress

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman/fake"
)

func TestEnsureProxyCreatesWhenAbsent(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, &memStore{}, nil, Config{
		Network: "podman-api-ingress", CaddyImage: "docker.io/library/caddy:2", ACMEEmail: "ops@example.com",
	})
	created, err := c.ensureProxy(context.Background(), "h1", "{\n\temail ops@example.com\n}\n\n")
	require.NoError(t, err)
	require.True(t, created)
	// network ensured
	require.Equal(t, []string{"podman-api-ingress"}, f.NetworkEnsureCalls["h1"])
	// pod played on the ingress network
	require.Len(t, f.PlayCalls, 1)
	require.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
	// config volume seeded with the initial Caddyfile (tar round-trips through the fake)
	require.Contains(t, string(f.VolumeData("h1", caddyConfigVolume)), "email ops@example.com")
}

func TestEnsureProxyNoopWhenPresent(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, &memStore{}, nil, Config{Network: "n", CaddyImage: "img"})
	_, err := c.ensureProxy(context.Background(), "h1", "{\n}\n\n")
	require.NoError(t, err)
	// second call sees the existing pod and creates nothing new
	plays := len(f.PlayCalls)
	created, err := c.ensureProxy(context.Background(), "h1", "{\n}\n\n")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, plays, len(f.PlayCalls), "no second play")
}

func TestCaddyPodYAMLShape(t *testing.T) {
	y := caddyPodYAML("docker.io/library/caddy:2")
	require.Contains(t, y, "name: "+caddyPodName)
	require.Contains(t, y, "image: docker.io/library/caddy:2")
	require.Contains(t, y, "hostPort: 80")
	require.Contains(t, y, "hostPort: 443")
	require.Contains(t, y, "claimName: "+caddyConfigVolume)
	require.Contains(t, y, "claimName: "+caddyDataVolume)
	require.True(t, strings.Contains(y, "kind: Pod"))
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run 'EnsureProxy|CaddyPodYAML' -v`
Expected: FAIL — `ensureProxy`, `caddyPodYAML`, and the `caddy*` consts undefined.

- [ ] **Step 3: Implement**

Create `internal/ingress/caddypod.go`:
```go
package ingress

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/iotready/podman-api/internal/podman"
)

const (
	// caddyPodName is the globally-unique name of the per-host Caddy system pod.
	// It cannot collide with app pods, which are "<template>-<slug>".
	caddyPodName = "podman-api-ingress-caddy"
	// caddyConfigDir is where the Caddyfile lives inside the container.
	caddyConfigDir = "/etc/caddy"
	// caddyConfigFile is the Caddyfile name inside caddyConfigDir.
	caddyConfigFile = "Caddyfile"
	// caddyConfigVolume / caddyDataVolume are the named volumes backing the
	// config dir and the ACME/cert data (data must persist across restarts).
	caddyConfigVolume = "podman-api-caddy-config"
	caddyDataVolume   = "podman-api-caddy-data"
)

// caddyPodYAML renders the kube manifest for the Caddy system pod. PVC
// claimNames map to podman named volumes. hostPort publishes :80/:443 to the
// host (rootless: requires net.ipv4.ip_unprivileged_port_start <= 80).
func caddyPodYAML(image string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  labels:
    podman-api.ingress: caddy
spec:
  containers:
    - name: caddy
      image: %s
      args: ["caddy", "run", "--config", "%s/%s", "--adapter", "caddyfile"]
      ports:
        - containerPort: 80
          hostPort: 80
        - containerPort: 443
          hostPort: 443
      volumeMounts:
        - name: config
          mountPath: %s
        - name: data
          mountPath: /data
  volumes:
    - name: config
      persistentVolumeClaim:
        claimName: %s
    - name: data
      persistentVolumeClaim:
        claimName: %s
`, caddyPodName, image, caddyConfigDir, caddyConfigFile, caddyConfigDir, caddyConfigVolume, caddyDataVolume)
}

// tarFile builds an uncompressed tar containing one file at `name`.
func tarFile(name string, content []byte) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(content); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// ensureProxy makes the network + Caddy pod exist on host. When it creates the
// pod it seeds the config volume with initialCaddyfile (so Caddy boots with a
// valid config) and returns created=true; when the pod already exists it does
// nothing and returns created=false. Reconcile uses created to decide whether a
// live cp+reload is needed.
func (c *CaddyController) ensureProxy(ctx context.Context, host, initialCaddyfile string) (bool, error) {
	if _, err := c.client.PodInspect(ctx, host, caddyPodName); err == nil {
		return false, nil // already present
	} else if !errors.Is(err, podman.ErrNotFound) {
		return false, fmt.Errorf("ingress: inspect caddy pod: %w", err)
	}
	if err := c.client.NetworkEnsure(ctx, host, c.cfg.Network); err != nil {
		return false, fmt.Errorf("ingress: ensure network: %w", err)
	}
	if err := c.client.VolumeCreate(ctx, host, caddyConfigVolume); err != nil {
		return false, fmt.Errorf("ingress: create config volume: %w", err)
	}
	if err := c.client.VolumeCreate(ctx, host, caddyDataVolume); err != nil {
		return false, fmt.Errorf("ingress: create data volume: %w", err)
	}
	// Seed the config volume so Caddy's `caddy run --config` finds a valid file
	// on first boot. VolumeImport unpacks an uncompressed tar into the volume.
	seed, err := tarFile(caddyConfigFile, []byte(initialCaddyfile))
	if err != nil {
		return false, err
	}
	if err := c.client.VolumeImport(ctx, host, caddyConfigVolume, seed); err != nil {
		return false, fmt.Errorf("ingress: seed config volume: %w", err)
	}
	if err := c.client.PlayKube(ctx, host, caddyPodYAML(c.cfg.CaddyImage), false, c.cfg.Network); err != nil {
		return false, fmt.Errorf("ingress: play caddy pod: %w", err)
	}
	return true, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run 'EnsureProxy|CaddyPodYAML' -v`
Expected: PASS.

> Note: the fake's `VolumeImport` requires the volume to exist first (it returns `ErrNotFound` otherwise). `ensureProxy` calls `VolumeCreate` before `VolumeImport`, so the seed succeeds in the fake and on a real host alike.

- [ ] **Step 5: Commit**

```sh
git add internal/ingress/caddypod.go internal/ingress/caddypod_test.go
git commit -m "feat(ingress): Caddy system pod + ensureProxy with seeded config volume (#60)"
```

---

## Task 8: `Reconcile`

Tie it together: serialize per host, derive routes, ensure the proxy (seeding the rendered config), and — for an already-running Caddy — copy the new Caddyfile in, `caddy validate`, then `caddy reload` (zero downtime).

**Files:**
- Create: `internal/ingress/reconcile.go`
- Modify: `internal/ingress/controller.go` (re-enable the compile-time guard)
- Test: `internal/ingress/reconcile_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/ingress/reconcile_test.go`:
```go
package ingress

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func webSpecStore() *memStore {
	return &memStore{specs: []store.Spec{
		{Host: "h1", Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}},
	}}
}

func TestReconcileFreshHostSeedsAndPlays(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, webSpecStore(), map[string]TemplateIngress{"web": {Port: 8080}},
		Config{Network: "podman-api-ingress", CaddyImage: "docker.io/library/caddy:2", ACMEEmail: "ops@example.com"})

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	// Fresh pod: config seeded with the rendered route, NO live reload yet.
	require.Contains(t, string(f.VolumeData("h1", caddyConfigVolume)), "blog.example.com")
	require.Contains(t, string(f.VolumeData("h1", caddyConfigVolume)), "reverse_proxy web-blog:8080")
	require.Empty(t, f.ExecCalls, "fresh pod boots from the seeded volume; no reload")
}

func TestReconcileExistingHostCopiesAndReloads(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, webSpecStore(), map[string]TemplateIngress{"web": {Port: 8080}},
		Config{Network: "n", CaddyImage: "img", ACMEEmail: "ops@example.com"})
	require.NoError(t, c.Reconcile(context.Background(), "h1")) // creates the pod

	require.NoError(t, c.Reconcile(context.Background(), "h1")) // pod now exists -> cp + reload

	require.NotEmpty(t, f.CopyCalls)
	last := f.CopyCalls[len(f.CopyCalls)-1]
	require.Equal(t, caddyConfigDir, last.DestDir)
	require.Equal(t, caddyConfigFile, last.Name)
	require.Contains(t, string(last.Content), "reverse_proxy web-blog:8080")
	// validate then reload
	require.GreaterOrEqual(t, len(f.ExecCalls), 2)
	require.Contains(t, f.ExecCalls[len(f.ExecCalls)-2].Cmd, "validate")
	require.Contains(t, f.ExecCalls[len(f.ExecCalls)-1].Cmd, "reload")
}

func TestReconcileFailsWhenValidateFails(t *testing.T) {
	f := fake.New()
	f.ExecFunc = func(_, _ string, cmd []string) (podman.ExecResult, error) {
		for _, a := range cmd {
			if a == "validate" {
				return podman.ExecResult{ExitCode: 1, Output: "adapt: bad config"}, nil
			}
		}
		return podman.ExecResult{}, nil
	}
	c := NewCaddyController(f, webSpecStore(), map[string]TemplateIngress{"web": {Port: 8080}},
		Config{Network: "n", CaddyImage: "img"})
	require.NoError(t, c.Reconcile(context.Background(), "h1")) // create
	err := c.Reconcile(context.Background(), "h1")              // cp + failing validate
	require.Error(t, err)
	require.Contains(t, err.Error(), "validate")
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestReconcile -v`
Expected: FAIL — `Reconcile` undefined on `*CaddyController`.

- [ ] **Step 3: Implement**

Create `internal/ingress/reconcile.go`:
```go
package ingress

import (
	"context"
	"fmt"
)

// Reconcile makes host's Caddy proxy match the store-derived routes. It is
// serialized per host and safe to call repeatedly.
func (c *CaddyController) Reconcile(ctx context.Context, host string) error {
	lock := c.hostLock(host)
	lock.Lock()
	defer lock.Unlock()

	routes, err := c.deriveRoutes(ctx, host)
	if err != nil {
		return err
	}
	caddyfile, err := RenderCaddyfile(c.cfg.ACMEEmail, routes)
	if err != nil {
		return fmt.Errorf("ingress: render caddyfile: %w", err)
	}

	created, err := c.ensureProxy(ctx, host, caddyfile)
	if err != nil {
		return err
	}
	if created {
		// The fresh pod boots reading the seeded Caddyfile; nothing to reload.
		return nil
	}

	// Existing pod: push the new config and reload with no downtime.
	if err := c.client.CopyToContainer(ctx, host, caddyContainer, caddyConfigDir, caddyConfigFile, []byte(caddyfile)); err != nil {
		return fmt.Errorf("ingress: copy caddyfile: %w", err)
	}
	cfgPath := caddyConfigDir + "/" + caddyConfigFile
	if res, err := c.client.ContainerExec(ctx, host, caddyContainer,
		[]string{"caddy", "validate", "--config", cfgPath, "--adapter", "caddyfile"}); err != nil {
		return fmt.Errorf("ingress: exec validate: %w", err)
	} else if res.ExitCode != 0 {
		return fmt.Errorf("ingress: caddy validate failed (exit %d): %s", res.ExitCode, res.Output)
	}
	if res, err := c.client.ContainerExec(ctx, host, caddyContainer,
		[]string{"caddy", "reload", "--config", cfgPath}); err != nil {
		return fmt.Errorf("ingress: exec reload: %w", err)
	} else if res.ExitCode != 0 {
		return fmt.Errorf("ingress: caddy reload failed (exit %d): %s", res.ExitCode, res.Output)
	}
	return nil
}
```

Add the `caddyContainer` constant to `internal/ingress/caddypod.go`'s const block (the container name inside the Caddy pod — the YAML names it `caddy`):
```go
	// caddyContainer is the container name `podman kube play` assigns from the
	// pod spec's container `name: caddy`.
	caddyContainer = "caddy"
```

- [ ] **Step 4: Re-enable the compile-time guard**

In `internal/ingress/controller.go`, restore the guard commented in Task 5:
```go
var (
	_ Controller = Disabled{}
	_ Controller = (*CaddyController)(nil)
)
```

- [ ] **Step 5: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/ingress/ -v`
Expected: PASS (all ingress tests, Phase 1 + Phase 2).

> Real-host note: `podman kube play` may name the container `<pod>-caddy` rather than `caddy`. The integration test (Task 11) confirms the actual name against a live host; if it differs, resolve it dynamically via `PodInspect(host, caddyPodName).Containers[0].Name` in Reconcile. Unit tests use the fake, which names it `caddy` from the YAML, so they stay valid either way.

- [ ] **Step 6: Commit**

```sh
git add internal/ingress/reconcile.go internal/ingress/caddypod.go internal/ingress/controller.go internal/ingress/reconcile_test.go
git commit -m "feat(ingress): Reconcile — derive, ensure, render, cp+validate+reload (#60)"
```

---

## Task 9: Wire the controller into the instance Service

Attach app pods to the ingress network at create; reconcile after every create/delete; and reject `domains` when ingress is disabled.

**Files:**
- Modify: `internal/instance/service.go`
- Test: `internal/instance/service_ingress_test.go` (create)

- [ ] **Step 1: Write the failing tests**

Create `internal/instance/service_ingress_test.go`. Mirror the construction the existing `service_test.go` uses (inspect that file for the exact `NewService`/template-fixture helpers; the snippet below assumes a `web` template whose meta declares `ingress: {container: web, port: 8080}` and a fake client):
```go
package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/ingress"
	"github.com/iotready/podman-api/internal/podman/fake"
)

// recordingCtl is a Controller that records the hosts it reconciled.
type recordingCtl struct{ hosts []string }

func (r *recordingCtl) Reconcile(_ context.Context, host string) error {
	r.hosts = append(r.hosts, host)
	return nil
}

func TestApplyRejectsDomainsWhenIngressDisabled(t *testing.T) {
	svc := newServiceWithWebTemplate(t, fake.New()) // helper from service_test.go style
	err := svc.Apply(context.Background(), "h1",
		ApplyRequest{Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}},
		ApplyOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingress is disabled")
}

func TestApplyAttachesNetworkAndReconcilesWhenEnabled(t *testing.T) {
	f := fake.New()
	svc := newServiceWithWebTemplate(t, f)
	rec := &recordingCtl{}
	svc.SetIngress(rec, "podman-api-ingress")

	require.NoError(t, svc.Apply(context.Background(), "h1",
		ApplyRequest{Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}},
		ApplyOptions{}))

	// app pod joined the ingress network
	require.Len(t, f.PlayCalls, 1)
	require.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
	// reconcile fired for the host
	require.Equal(t, []string{"h1"}, rec.hosts)
}
```
> If `service_test.go` has no reusable `newServiceWithWebTemplate` helper, add one in this file that builds a `config.Template` with `Meta.Ingress = &render.Ingress{Container: "web", Port: 8080}` and a body that renders a `kind: Pod` with `metadata.name: web-{{.slug}}` — match the fixture shape already used by the existing tests. Requires `-state-db` behaviour: the existing helper must call `svc.SetStore(...)` with an in-memory/sqlite store so `Apply` persists the spec (ingress derivation reads it). Use the same store the other Service tests use.

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/instance/ -run 'IngressDisabled|AttachesNetwork' -v`
Expected: FAIL — `SetIngress` undefined; no network attached; no rejection.

- [ ] **Step 3: Add the field, setter, and helpers**

In `internal/instance/service.go`, add the import `"github.com/iotready/podman-api/internal/ingress"`. Add two fields to the `Service` struct (after `store store.Store` ~line 60):
```go
	ingress    ingress.Controller // never nil; ingress.Disabled{} when off
	ingressNet string             // shared ingress network; "" when ingress disabled
```
In `NewService` (after the struct literal is built, before `return s`), default the controller so it is never nil:
```go
	s.ingress = ingress.Disabled{}
```
Add the setter next to `SetStore` (~line 99):
```go
// SetIngress enables ingress reconciliation. network is the shared podman
// network app pods join; passing a real controller marks ingress enabled so
// Apply will accept domains. Call with ingress.Disabled{} and "" to disable.
func (s *Service) SetIngress(c ingress.Controller, network string) {
	s.ingress = c
	s.ingressNet = network
}

// ingressEnabled reports whether a real ingress controller is wired.
func (s *Service) ingressEnabled() bool { return s.ingressNet != "" }
```

- [ ] **Step 4: Enforce, attach, and reconcile in `Apply`**

In `internal/instance/service.go`'s `Apply`, near the top (right after the template is resolved and `req` validated, before rendering), reject domains when disabled:
```go
	if len(req.Domains) > 0 && !s.ingressEnabled() {
		return fmt.Errorf("instance %s/%s declares domains but ingress is disabled", req.Template, req.Slug)
	}
```
Where `Apply` currently calls `PlayKube` (line 235), compute the networks and pass them. Replace that one line:
```go
	var networks []string
	if s.ingressEnabled() && tmpl.Meta.Ingress != nil {
		networks = []string{s.ingressNet}
	}
	if err := s.client.PlayKube(ctx, host, yaml, opts.Replace, networks...); err != nil {
		return fmt.Errorf("play kube: %w", err)
	}
```
(`tmpl` is the resolved `config.Template` already in scope in `Apply`; confirm the local variable name when implementing and use it.) After the `PutSpec` block succeeds (end of `Apply`, before `return nil`), reconcile:
```go
	if s.ingressEnabled() {
		if err := s.ingress.Reconcile(ctx, host); err != nil {
			return fmt.Errorf("ingress reconcile: %w", err)
		}
	}
	return nil
```

- [ ] **Step 5: Reconcile in `Delete`**

In `Delete` (~line 391), after the `DeleteSpec` block succeeds and before the final `return`, add the same reconcile (so a removed instance's routes drop out). Place it before the `if !podExisted ...` early-return logic so a successful delete always reconciles:
```go
	if s.ingressEnabled() {
		if err := s.ingress.Reconcile(ctx, host); err != nil {
			return fmt.Errorf("ingress reconcile: %w", err)
		}
	}
```
> `Upgrade` calls `Apply` with `Replace: true`, so the Apply hook covers upgrades — no separate change needed.

- [ ] **Step 6: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/instance/ -v`
Expected: PASS (new tests + existing Service tests, which use the default `Disabled` controller and so attach no network and skip reconcile).

- [ ] **Step 7: Commit**

```sh
git add internal/instance/service.go internal/instance/service_ingress_test.go
git commit -m "feat(instance): attach ingress network, reconcile on apply/delete, reject domains when disabled (#60)"
```

---

## Task 10: `main()` flags, controller wiring, and periodic reconcile

Add the `-ingress-*` flags, build the controller when enabled (requires `-state-db`), wire it into the Service, run a periodic per-host drift-correction loop, and stop it cleanly on shutdown.

**Files:**
- Modify: `internal/cmd/podman-api/main.go`

- [ ] **Step 1: Add the flags**

In `internal/cmd/podman-api/main.go`, add to the `var (...)` flag block (after the `prune*` flags ~line 56):
```go
		ingressEnabled  = flag.Bool("ingress-enabled", false, "enable per-host Caddy ingress + auto-TLS (requires -state-db)")
		ingressNetwork  = flag.String("ingress-network", "podman-api-ingress", "shared podman network app pods join for ingress")
		ingressImage    = flag.String("ingress-caddy-image", "docker.io/library/caddy:2", "Caddy image for the per-host ingress pod")
		ingressACME     = flag.String("ingress-acme-email", "", "ACME account email for Let's Encrypt (required when -ingress-enabled)")
		ingressInterval = flag.Duration("ingress-reconcile-interval", 5*time.Minute, "periodic ingress drift-correction interval per host; 0 disables the periodic loop")
```

- [ ] **Step 2: Validate flags after `flag.Parse()`**

After `flag.Parse()` and the existing store/prune validation, add:
```go
	if *ingressEnabled {
		if *stateDB == "" {
			log.Fatalf("ingress: -ingress-enabled requires -state-db (routes are derived from the desired-state store)")
		}
		if *ingressACME == "" {
			log.Fatalf("ingress: -ingress-enabled requires -ingress-acme-email")
		}
	}
```

- [ ] **Step 3: Build the controller and wire it into the Service**

After the store is opened and `svc.SetStore(db)` is called (the `if db != nil { ... }` block ~line 113–122), add (inside that block, since ingress needs the store):
```go
		if *ingressEnabled {
			tmplIngress := map[string]ingress.TemplateIngress{}
			for _, t := range tmpls {
				if t.Meta.Ingress != nil {
					tmplIngress[t.Meta.ID] = ingress.TemplateIngress{
						Container: t.Meta.Ingress.Container,
						Port:      t.Meta.Ingress.Port,
					}
				}
			}
			ctl := ingress.NewCaddyController(client, db, tmplIngress, ingress.Config{
				Network:    *ingressNetwork,
				CaddyImage: *ingressImage,
				ACMEEmail:  *ingressACME,
			})
			svc.SetIngress(ctl, *ingressNetwork)
			ingressCtl = ctl // captured for the periodic loop below
		}
```
Declare `var ingressCtl *ingress.CaddyController` before the `if db != nil` block so it is visible to the periodic loop, and add the import `"github.com/iotready/podman-api/internal/ingress"`. `tmpls` is the loaded template slice already in scope (it was passed to `instance.NewService`).

- [ ] **Step 4: Start the periodic reconcile loop**

After the router/server is constructed and before the blocking serve, add a goroutine that reconciles every known host on a ticker (mirrors the prune scheduler pattern; uses the cancelable runner context). Use the existing hosts holder and a context that the shutdown handler cancels:
```go
	ingressLoopDone := make(chan struct{})
	if ingressCtl != nil && *ingressInterval > 0 {
		go func() {
			defer close(ingressLoopDone)
			t := time.NewTicker(*ingressInterval)
			defer t.Stop()
			for {
				select {
				case <-runnerCtx.Done(): // the same ctx cancelRunner() cancels
					return
				case <-t.C:
					for _, h := range *hostsHolder.Load() {
						if err := ingressCtl.Reconcile(runnerCtx, h.ID); err != nil {
							log.Printf("ingress: periodic reconcile %s failed: %v", h.ID, err)
						}
					}
				}
			}
		}()
	} else {
		close(ingressLoopDone)
	}
```
> Match the actual names in `main.go` for the cancelable context (`runnerCtx`/`cancelRunner`) and the hosts holder (`hostsHolder`); the explore confirmed both exist. If the hosts holder stores `[]config.Host`, `h.ID` is the host identifier — confirm the field name (`ID`) against `config.Host`.

- [ ] **Step 5: Wait for the loop on shutdown**

In the shutdown goroutine (after `cancelRunner()` and the existing `pruneSched.Wait()` ~line 259), add:
```go
		<-ingressLoopDone
```

- [ ] **Step 6: Build + vet + smoke-run help**

Run:
```sh
make build
go vet -tags "$TAGS" ./...
./bin/podman-api -h 2>&1 | grep ingress
```
Expected: build succeeds; vet clean; `-h` lists the five `-ingress-*` flags.

- [ ] **Step 7: Document the provisioning prerequisite**

Add a short note to `README.md` under the operations/quick-reference section (match the existing prune note's style):
```markdown
### Ingress + auto-TLS (optional)

`-ingress-enabled` runs a per-host Caddy pod that terminates TLS (HTTP-01 ACME)
and reverse-proxies each instance's `domains` to its pod over the shared
`-ingress-network`. Requires `-state-db` and `-ingress-acme-email`. Apps join the
ingress network and publish no host ports; only Caddy publishes :80/:443.

**Rootless prerequisite:** the host must allow rootless binding of privileged
ports — set `net.ipv4.ip_unprivileged_port_start=80` persistently (e.g. a
`/etc/sysctl.d/` drop-in), or Caddy cannot publish :80/:443. See the wiki
"Provisioning a Podman Host" page.
```

- [ ] **Step 8: Commit**

```sh
git add internal/cmd/podman-api/main.go README.md
git commit -m "feat(cmd): -ingress-* flags, controller wiring, periodic reconcile (#60)"
```

> **Wiki (published separately, not in this PR):** the "Provisioning a Podman Host" page must add the `net.ipv4.ip_unprivileged_port_start=80` sysctl drop-in step; a new "Ingress + auto-TLS" section on the Operating page must document the five flags, the DNS-points-to-host requirement, the shared network, and that certs persist on the `podman-api-caddy-data` volume.

---

## Task 11: Integration test (Pebble ACME), behind the `integration` tag

Prove end-to-end on a real podman host: deploying a web instance with a domain produces a live HTTPS route. Use Pebble (a local ACME test server) to avoid Let's Encrypt rate limits.

**Files:**
- Create: `internal/ingress/integration_test.go`

- [ ] **Step 1: Write the integration test**

Create `internal/ingress/integration_test.go`. Follow the conventions of the existing `internal/podman/real_*_integration_test.go` files (same `//go:build integration` tag, the same host/identity env vars they read, and `t.Skip` when unset):
```go
//go:build integration

package ingress_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/ingress"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// TestIngressEndToEnd deploys a web pod with a domain on a real host and asserts
// Reconcile produces a live route. Set PODMAN_API_IT_HOST (and the identity vars
// the other integration tests use) plus INGRESS_IT_DOMAIN (a domain whose DNS
// points at the host) to run. Uses Pebble for ACME if INGRESS_IT_ACME_DIR_URL is
// set; otherwise validates routing only (skips cert assertion).
func TestIngressEndToEnd(t *testing.T) {
	host := os.Getenv("PODMAN_API_IT_HOST")
	domain := os.Getenv("INGRESS_IT_DOMAIN")
	if host == "" || domain == "" {
		t.Skip("set PODMAN_API_IT_HOST and INGRESS_IT_DOMAIN to run")
	}
	// Build the real client exactly as the other integration tests do
	// (mirror real_pods_integration_test.go's setup helper).
	client := newITClient(t) // helper mirroring the existing integration tests

	st, err := store.OpenSQLite(":memory:", testKey(t)) // match store test constructor
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.PutSpec(context.Background(), store.Spec{
		Host: host, Template: "web", Slug: "it", Domains: []string{domain},
	}))

	ctl := ingress.NewCaddyController(client, st,
		map[string]ingress.TemplateIngress{"web": {Container: "web", Port: 80}},
		ingress.Config{Network: "podman-api-ingress-it", CaddyImage: "docker.io/library/caddy:2", ACMEEmail: "it@example.com"})

	// Deploy a dummy web pod named web-it on the ingress network.
	const webPod = `apiVersion: v1
kind: Pod
metadata:
  name: web-it
spec:
  containers:
    - name: web
      image: docker.io/library/nginx:alpine
`
	require.NoError(t, client.PlayKube(context.Background(), host, webPod, true, "podman-api-ingress-it"))
	t.Cleanup(func() { _ = client.PodRemove(context.Background(), host, "web-it", true) })
	t.Cleanup(func() { _ = client.PodRemove(context.Background(), host, "podman-api-ingress-caddy", true) })

	require.NoError(t, ctl.Reconcile(context.Background(), host))

	// Give Caddy a moment, then assert the route serves (cert assertion only
	// when an ACME dir URL is configured; HTTP-01 against public LE needs real
	// DNS + open :80/:443 as the spike confirmed).
	time.Sleep(5 * time.Second)
	// ... HTTP GET https://domain, assert 200 (or via `caddy` exec to localhost) ...
}
```

- [ ] **Step 2: Verify it compiles under the integration tag (without a host)**

Run: `go test -tags "$TAGS integration" ./internal/ingress/ -run TestIngressEndToEnd -v`
Expected: the test compiles and SKIPs (env vars unset). If `newITClient`/`testKey` helpers don't exist, define them in this file by copying the setup from `internal/podman/real_pods_integration_test.go` and `internal/store/sqlite_test.go`.

- [ ] **Step 3: (Manual, optional) Run against the real host**

With `*.podman.iotready.com` pointing at the host (already configured per the spike) and the rootless sysctl set:
```sh
export PODMAN_API_IT_HOST=<host-id> INGRESS_IT_DOMAIN=it.podman.iotready.com
# plus the identity env vars the other integration tests read
go test -tags "$TAGS integration" ./internal/ingress/ -run TestIngressEndToEnd -v
```
Expected: route live; with public DNS + LE, HTTPS returns 200 with a valid cert (mirrors spike A2).

- [ ] **Step 4: Commit**

```sh
git add internal/ingress/integration_test.go
git commit -m "test(ingress): integration end-to-end (real host, behind integration tag) (#60)"
```

---

## Final gate (after all tasks)

- [ ] Full suite + vet + fmt:
```sh
make test
go vet -tags "$TAGS" ./...
gofmt -l .   # must print nothing
```
- [ ] Dispatch a final holistic code review over the whole branch.
- [ ] Then run superpowers:finishing-a-development-branch to open the PR against `main` (target the existing #60 issue; one PR per issue).

---

## Self-review notes (spec + Phase-2-outline coverage)

- **Phase-2 outline item 1** (NetworkEnsure/ContainerExec/CopyToContainer + Real/Mock) → Tasks 1–4. Binding signatures pinned against v5.8.2 (`network.CreateWithOptions`+`ExtraCreateOptions.IgnoreIfExists`; `containers.ExecCreate`/`ExecStartAndAttach`/`ExecInspect` with `handlers.ExecCreateConfig`/`dockerContainer.ExecOptions`, exit code via `InspectExecSession.ExitCode`; `containers.CopyFromArchive` copies INTO the container).
- **item 2** (IngressController + CaddyController EnsureProxy/Apply) → Tasks 5, 7, 8.
- **item 3** (route derivation, Backend=`<pod-name>:<port>`, host-wide uniqueness + ingress-required) → Task 6.
- **item 4** (attach app pods to the ingress network, no host port) → Task 1 (variadic PlayKube) + Task 9 (Apply passes the network only for ingress templates). App templates already publish no host ports.
- **item 5** (per-host reconcile inline + periodic, serialized) → Task 8 (`hostLock`), Task 9 (inline on apply/delete), Task 10 (periodic loop).
- **item 6** (DNSProvider no-op seam) → out of scope by design (operator-managed DNS for v1); documented, no code.
- **item 7** (`main()` flags + reject domains when disabled) → Tasks 9 + 10.
- **item 8** (integration test behind the `integration` tag) → Task 11.
- **item 9** (rootless port-start provisioning prerequisite) → Task 10 (README + wiki note); decision recorded: rootless + sysctl.
- **Defense in depth:** `RenderCaddyfile` (Phase 1) re-validates every domain/backend, so a derivation bug cannot inject Caddyfile directives — `deriveRoutes` feeds it, never bypasses it.

**Type consistency check:** `Route{Domain, Backend}` (Phase 1) is produced by `deriveRoutes` (Task 6) and consumed by `RenderCaddyfile` (Task 8). `Controller.Reconcile(ctx, host)` is defined in Task 5 and implemented in Task 8; `Disabled` and `*CaddyController` both satisfy it (guard re-enabled in Task 8). `TemplateIngress{Container, Port}` (Task 5) is built in `main()` from `render.Ingress{Container, Port}` (Phase 1 meta) in Task 10 and read in Task 6. `ExecResult{ExitCode, Output}` (Task 3) is returned by `ContainerExec` and checked in `Reconcile` (Task 8). `caddyContainer`/`caddyConfigDir`/`caddyConfigFile`/`caddy{Config,Data}Volume`/`caddyPodName` consts (Task 7) are used across Tasks 7–8. `SetIngress(Controller, network)` (Task 9) is called in Task 10. All consistent.
