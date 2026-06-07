# Healthchecks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gate deploys and starts on container healthcheck readiness, expose health status in API responses and the UI.

**Architecture:** Extract `waitReady` from the existing migrate `waitRunning` into a shared helper; call it after `Apply` and `Start`; surface health in `Observed` via new `Health`, `Ready`, and `Warnings` fields; add a `-deploy-verify-timeout` flag (default 30s, 0 = disabled).

**Tech Stack:** Go, net/http, htmx (UI templates only), testify

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `internal/instance/ready.go` | Create | `waitReady`, `errReadyTimeout`, `deployVerifyTimeout`, `SetDeployVerifyTimeout` |
| `internal/instance/ready_test.go` | Create | Unit tests for `waitReady` |
| `internal/instance/migrate.go` | Modify | `waitRunning` delegates to `waitReady`; rollback `Start` call updated |
| `internal/instance/migrate_test.go` | Modify | `setVerifyKnobs` also resets `deployVerifyTimeout` |
| `internal/instance/observed.go` | Modify | Add `Health`, `Ready`, `Warnings` fields; update `Normalize` |
| `internal/instance/observed_test.go` | Modify | Tests for health propagation and `Ready` aggregation |
| `internal/instance/service.go` | Modify | Add `ApplyAndObserve`; change `Start` to return `(Observed, error)` |
| `internal/instance/service_test.go` | Modify | Tests for `ApplyAndObserve` and updated `Start` |
| `internal/api/instances.go` | Modify | `createInstance`/`applyInstance` use `ApplyAndObserve`; `startInstance` returns JSON |
| `internal/ui/handlers_deploy.go` | Modify | `deployCreate` uses `ApplyAndObserve`, surfaces warnings as Notice |
| `internal/ui/handlers_instances.go` | Modify | `lifecycle` handles `Start`'s new signature, surfaces warnings as Notice |
| `cmd/podman-api/main.go` | Modify | Add `-deploy-verify-timeout` flag, call `SetDeployVerifyTimeout` |
| `internal/ui/templates/host-instances.html` | Modify | Aggregate health dot per instance |
| `internal/ui/templates/instance-detail.html` | Modify | Per-container health column; readiness warning notice |

---

## Task 1: Extract `waitReady` into `internal/instance/ready.go`

**Files:**
- Create: `internal/instance/ready.go`
- Modify: `internal/instance/migrate.go` (lines 365–383)
- Modify: `internal/instance/migrate_test.go` (lines 22–31)
- Create: `internal/instance/ready_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/instance/ready_test.go`:

```go
package instance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func readySvc(t *testing.T, f *fake.Fake) *Service {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := NewService(f, hosts)
	svc.SetStore(seedStore(t, webTemplate()))
	return svc
}

func TestWaitReady_NilWhenReady(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-ok", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "healthy"}}})
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "ok", 50*time.Millisecond))
}

func TestWaitReady_TimeoutSentinel(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-bad", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "bad", 50*time.Millisecond)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errReadyTimeout), "expected errReadyTimeout, got %v", err)
}

func TestWaitReady_ContextCancel(t *testing.T) {
	defer setVerifyKnobs(200*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-slow", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := readySvc(t, f).waitReady(ctx, "h1", "web", "slow", 200*time.Millisecond)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWaitReady_ZeroTimeout(t *testing.T) {
	// timeout=0 means disabled: must return nil immediately without polling
	f := fake.New() // no pods added — any poll would fail
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "x", 0))
}

func TestWaitReady_NoHealthcheck(t *testing.T) {
	// Container with no declared healthcheck (Health=="") is ready when Running
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-nohc", Status: "Running",
		Containers: []podman.Container{{Status: "Running"}}}) // Health==""
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "nohc", 50*time.Millisecond))
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
make test 2>&1 | grep -A3 "ready_test"
```

Expected: compilation error — `waitReady`, `errReadyTimeout` undefined.

- [ ] **Step 3: Create `internal/instance/ready.go`**

```go
package instance

import (
	"context"
	"errors"
	"time"
)

var errReadyTimeout = errors.New("readiness timeout")

// deployVerifyTimeout is the readiness wait applied after Apply and Start.
// Vars (not consts) so same-package tests can shorten them via setVerifyKnobs.
var deployVerifyTimeout = 30 * time.Second

// SetDeployVerifyTimeout configures the readiness wait applied after Apply and
// Start. No-op for d <= 0. Called once at startup from -deploy-verify-timeout.
func SetDeployVerifyTimeout(d time.Duration) {
	if d > 0 {
		deployVerifyTimeout = d
	}
}

// waitReady polls until podReady returns true, or timeout elapses, or ctx is
// cancelled. Returns nil on success, errReadyTimeout on timeout, ctx.Err() on
// cancellation. timeout==0 disables the wait and returns nil immediately.
// A transient PodInspect error during polling is not fatal (the pod is Running;
// the error is logged implicitly by continuing the loop).
func (s *Service) waitReady(ctx context.Context, host, tmpl, slug string, timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(verifyInterval)
	defer ticker.Stop()
	for {
		p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
		if err == nil && podReady(p) {
			return nil
		}
		if time.Now().After(deadline) {
			return errReadyTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
```

- [ ] **Step 4: Update `waitRunning` in `migrate.go` to delegate (lines 365–383)**

Replace the body of `waitRunning`:

```go
// waitRunning polls the dest pod until Running, bounded by verifyTimeout and the
// caller's context.
func (s *Service) waitRunning(ctx context.Context, host, tmpl, slug string) error {
	if err := s.waitReady(ctx, host, tmpl, slug, verifyTimeout); err != nil {
		if errors.Is(err, errReadyTimeout) {
			return fmt.Errorf("pod %s not running within %s", podName(tmpl, slug), verifyTimeout)
		}
		return err
	}
	return nil
}
```

- [ ] **Step 5: Update `setVerifyKnobs` in `migrate_test.go` (lines 22–31)**

```go
// setVerifyKnobs temporarily shrinks the verify-poll timing for tests.
func setVerifyKnobs(timeout, interval time.Duration) func() {
	ot, oi, od := verifyTimeout, verifyInterval, deployVerifyTimeout
	verifyTimeout, verifyInterval, deployVerifyTimeout = timeout, interval, timeout
	return func() { verifyTimeout, verifyInterval, deployVerifyTimeout = ot, oi, od }
}
```

- [ ] **Step 6: Run tests**

```
make test 2>&1 | grep -E "FAIL|PASS|ready_test"
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add internal/instance/ready.go internal/instance/ready_test.go \
        internal/instance/migrate.go internal/instance/migrate_test.go
git commit -m "feat(65): extract waitReady; waitRunning delegates to it"
```

---

## Task 2: Add `Health`, `Ready`, `Warnings` to `Observed`

**Files:**
- Modify: `internal/instance/observed.go`
- Modify: `internal/instance/observed_test.go`

- [ ] **Step 1: Write failing tests**

Add to `internal/instance/observed_test.go` (after the existing `TestNormalize`):

```go
func TestNormalize_HealthPropagation(t *testing.T) {
	p := podman.Pod{
		Status: "Running",
		Containers: []podman.Container{
			{Name: "app", Image: "nginx", Status: "Running", Health: "healthy"},
			{Name: "sidecar", Image: "alpine", Status: "Running", Health: ""},
		},
	}
	obs := Normalize(p, "web", "s1", nil, nil)

	require.Len(t, obs.Containers, 2)
	assert.Equal(t, "healthy", obs.Containers[0].Health)
	assert.Equal(t, "", obs.Containers[1].Health)
}

func TestNormalize_ReadyAggregation(t *testing.T) {
	tests := []struct {
		name       string
		containers []podman.Container
		wantReady  bool
	}{
		{
			"all healthy",
			[]podman.Container{
				{Status: "Running", Health: "healthy"},
				{Status: "Running", Health: "healthy"},
			},
			true,
		},
		{
			"one still starting",
			[]podman.Container{
				{Status: "Running", Health: "healthy"},
				{Status: "Running", Health: "starting"},
			},
			false,
		},
		{
			"one unhealthy",
			[]podman.Container{{Status: "Running", Health: "unhealthy"}},
			false,
		},
		{
			"no healthchecks declared — ready when Running",
			[]podman.Container{
				{Status: "Running"},
				{Status: "Running"},
			},
			true,
		},
		{
			"mixed declared and undeclared — only declared gates Ready",
			[]podman.Container{
				{Status: "Running", Health: "healthy"},
				{Status: "Running"},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := podman.Pod{Status: "Running", Containers: tt.containers}
			obs := Normalize(p, "web", "s1", nil, nil)
			assert.Equal(t, tt.wantReady, obs.Ready)
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
make test 2>&1 | grep -E "FAIL|obs.Ready|obs.Containers\[0\].Health"
```

Expected: compilation error — `obs.Ready` and `Health` undefined.

- [ ] **Step 3: Update `internal/instance/observed.go`**

Add `Health` to `ObservedContainer`, and `Ready`/`Warnings` to `Observed`:

```go
package instance

import (
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/podman"
)

// Observed is the JSON shape returned for an instance.
type Observed struct {
	Template   string              `json:"template"`
	Slug       string              `json:"slug"`
	Ready      bool                `json:"ready"`
	Pod        ObservedPod         `json:"pod"`
	Containers []ObservedContainer `json:"containers"`
	Volumes    []ObservedVolume    `json:"volumes,omitempty"`
	EnvSummary map[string]string   `json:"env_summary,omitempty"`
	Warnings   []string            `json:"warnings,omitempty"`
}

type ObservedPod struct {
	ID      string    `json:"id,omitempty"`
	Status  string    `json:"status"`
	Created time.Time `json:"created,omitempty"`
}

type ObservedContainer struct {
	Name         string                `json:"name"`
	Image        string                `json:"image"`
	ImageTag     string                `json:"image_tag,omitempty"`
	Status       string                `json:"status"`
	Health       string                `json:"health,omitempty"`
	StartedAt    time.Time             `json:"started_at,omitempty"`
	RestartCount int                   `json:"restart_count"`
	Ports        []ObservedPortMapping `json:"ports,omitempty"`
}

type ObservedPortMapping struct {
	HostIP        string `json:"host_ip,omitempty"`
	HostPort      int    `json:"host_port"`
	ContainerPort int    `json:"container_port"`
	Protocol      string `json:"protocol,omitempty"`
}

type ObservedVolume struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

// Normalize builds Observed from a Pod + the volumes the API thinks the
// instance owns. Env vars whose names appear in secretEnvs (the set derived
// from the template's secretKeyRef blocks) are dropped from env_summary so
// secret material never returns to the CMS. A defensive substring check on
// SECRET also catches anything not anchored to a known template.
func Normalize(p podman.Pod, template, slug string, vols []podman.Volume, secretEnvs map[string]bool) Observed {
	out := Observed{
		Template: template,
		Slug:     slug,
		Pod:      ObservedPod{ID: p.ID, Status: p.Status, Created: p.Created},
	}
	ready := true
	for _, c := range p.Containers {
		oc := ObservedContainer{
			Name: c.Name, Image: c.Image, ImageTag: c.ImageTag,
			Status: c.Status, Health: c.Health,
			StartedAt: c.StartedAt, RestartCount: c.RestartCount,
		}
		for _, port := range c.Ports {
			oc.Ports = append(oc.Ports, ObservedPortMapping{
				HostIP: port.HostIP, HostPort: port.HostPort,
				ContainerPort: port.ContainerPort, Protocol: port.Protocol,
			})
		}
		out.Containers = append(out.Containers, oc)
		if c.Health != "" && c.Health != "healthy" {
			ready = false
		}
	}
	out.Ready = ready
	for _, v := range vols {
		out.Volumes = append(out.Volumes, ObservedVolume{Name: v.Name, SizeBytes: v.SizeBytes})
	}

	// EnvSummary takes the union of non-secret env vars across containers.
	out.EnvSummary = map[string]string{}
	for _, c := range p.Containers {
		for k, v := range c.Env {
			if secretEnvs[k] || strings.Contains(strings.ToUpper(k), "SECRET") {
				continue
			}
			out.EnvSummary[k] = v
		}
	}
	if len(out.EnvSummary) == 0 {
		out.EnvSummary = nil
	}
	return out
}
```

- [ ] **Step 4: Run tests**

```
make test 2>&1 | grep -E "FAIL|PASS|observed_test"
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/instance/observed.go internal/instance/observed_test.go
git commit -m "feat(65): add Health, Ready, Warnings to Observed; populate in Normalize"
```

---

## Task 3: Add `ApplyAndObserve` and update `Start` signature

**Files:**
- Modify: `internal/instance/service.go`
- Modify: `internal/instance/migrate.go` (rollback `Start` call)
- Modify: `internal/instance/service_test.go`

- [ ] **Step 1: Write failing tests**

Add to `internal/instance/service_test.go` (after existing tests):

These tests go in `internal/instance/service_test.go` which is `package instance` — no package prefix on `ApplyRequest`, `ApplyOptions`, etc. Use `newSvcWith` with an inline single-host slice.

```go
func TestApplyAndObserve_ReadyOnSuccess(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.PlayKubeContainerHealth = "healthy"
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.ApplyAndObserve(context.Background(), "h1", ApplyRequest{
		Template:   "web",
		Slug:       "s1",
		Parameters: map[string]any{"slug": "s1", "image": "nginx"},
	}, ApplyOptions{})
	require.NoError(t, err)
	assert.True(t, obs.Ready)
	assert.Empty(t, obs.Warnings)
}

func TestApplyAndObserve_WarningOnTimeout(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.PlayKubeContainerHealth = "starting"
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.ApplyAndObserve(context.Background(), "h1", ApplyRequest{
		Template:   "web",
		Slug:       "s1",
		Parameters: map[string]any{"slug": "s1", "image": "nginx"},
	}, ApplyOptions{})
	require.NoError(t, err)
	assert.False(t, obs.Ready)
	require.Len(t, obs.Warnings, 1)
	assert.Contains(t, obs.Warnings[0], "readiness timeout")
}

func TestStart_ReadyOnSuccess(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-s1", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "healthy"}}})
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.Start(context.Background(), "h1", "web", "s1")
	require.NoError(t, err)
	assert.True(t, obs.Ready)
	assert.Empty(t, obs.Warnings)
}

func TestStart_WarningOnTimeout(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-s1", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.Start(context.Background(), "h1", "web", "s1")
	require.NoError(t, err)
	assert.False(t, obs.Ready)
	require.Len(t, obs.Warnings, 1)
	assert.Contains(t, obs.Warnings[0], "readiness timeout")
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
make test 2>&1 | grep -E "FAIL|ApplyAndObserve|undefined"
```

Expected: compilation error — `ApplyAndObserve` undefined, `Start` wrong return type.

- [ ] **Step 3: Add `ApplyAndObserve` to `internal/instance/service.go`**

Add this method after `Apply` (around line 289):

```go
// ApplyAndObserve creates or replaces an instance (via Apply), waits for
// container healthchecks to pass (up to deployVerifyTimeout), then returns the
// observed state. On readiness timeout the operation still succeeds but
// Observed.Warnings carries a human-readable message.
func (s *Service) ApplyAndObserve(ctx context.Context, host string, req ApplyRequest, opts ApplyOptions) (Observed, error) {
	if err := s.Apply(ctx, host, req, opts); err != nil {
		return Observed{}, err
	}
	readyErr := s.waitReady(ctx, host, req.Template, req.Slug, deployVerifyTimeout)
	obs, err := s.Get(ctx, host, req.Template, req.Slug)
	if err != nil {
		return Observed{}, err
	}
	if errors.Is(readyErr, errReadyTimeout) {
		obs.Warnings = append(obs.Warnings, fmt.Sprintf(
			"readiness timeout: healthcheck did not pass within %s — the app may still be initialising",
			deployVerifyTimeout,
		))
	}
	return obs, nil
}
```

- [ ] **Step 4: Change `Start` signature in `internal/instance/service.go` (line 548)**

Replace:
```go
func (s *Service) Start(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodStart)
}
```

With:
```go
// Start starts a stopped instance and waits for container healthchecks to pass
// (up to deployVerifyTimeout). On readiness timeout the call still succeeds and
// Observed.Warnings carries a human-readable message.
func (s *Service) Start(ctx context.Context, host, tmpl, slug string) (Observed, error) {
	if err := s.lifecycle(ctx, host, tmpl, slug, s.client.PodStart); err != nil {
		return Observed{}, err
	}
	readyErr := s.waitReady(ctx, host, tmpl, slug, deployVerifyTimeout)
	obs, err := s.Get(ctx, host, tmpl, slug)
	if err != nil {
		return Observed{}, err
	}
	if errors.Is(readyErr, errReadyTimeout) {
		obs.Warnings = append(obs.Warnings, fmt.Sprintf(
			"readiness timeout: healthcheck did not pass within %s — the app may still be initialising",
			deployVerifyTimeout,
		))
	}
	return obs, nil
}
```

- [ ] **Step 5: Fix rollback `Start` call in `migrate.go` (line 257)**

Replace:
```go
if rerr := s.Start(rbctx, req.FromHost, req.Template, req.Slug); rerr != nil {
    step("rollback-restore-failed", rerr.Error())
}
```

With:
```go
if _, rerr := s.Start(rbctx, req.FromHost, req.Template, req.Slug); rerr != nil {
    step("rollback-restore-failed", rerr.Error())
}
```

- [ ] **Step 6: Run tests**

```
make test 2>&1 | grep -E "FAIL|PASS"
```

Expected: all tests pass. (Callers of `Start` that haven't been updated yet will produce compile errors — fix them in Tasks 4–6 first if needed, or run `go build ./...` to confirm.)

```
go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./... 2>&1
```

Expected: compile errors naming the remaining callers (`startInstance`, `lifecycle` in UI) — that's correct; fix them in the next tasks.

- [ ] **Step 7: Commit after Tasks 4–6 compile cleanly** (defer this commit)

---

## Task 4: Update API handlers

**Files:**
- Modify: `internal/api/instances.go`

- [ ] **Step 1: Update `createInstance` to use `ApplyAndObserve`**

In `createInstance` (around lines 54–79), replace:
```go
if err := h.svc.Apply(r.Context(), host, req, opts); err != nil {
    WriteError(w, err)
    return
}
obs, err := h.svc.Get(r.Context(), host, req.Template, req.Slug)
if err != nil {
    WriteError(w, err)
    return
}
WriteJSON(w, http.StatusCreated, obs)
```

With:
```go
obs, err := h.svc.ApplyAndObserve(r.Context(), host, req, opts)
if err != nil {
    WriteError(w, err)
    return
}
WriteJSON(w, http.StatusCreated, obs)
```

- [ ] **Step 2: Update `applyInstance` to use `ApplyAndObserve`**

In `applyInstance` (around lines 81–119), replace:
```go
opts := instance.ApplyOptions{Replace: true, SkipPull: queryBool(r, "skip_pull")}
if err := h.svc.Apply(r.Context(), host, req, opts); err != nil {
    WriteError(w, err)
    return
}
obs, err := h.svc.Get(r.Context(), host, req.Template, req.Slug)
if err != nil {
    WriteError(w, err)
    return
}
WriteJSON(w, http.StatusOK, obs)
```

With:
```go
opts := instance.ApplyOptions{Replace: true, SkipPull: queryBool(r, "skip_pull")}
obs, err := h.svc.ApplyAndObserve(r.Context(), host, req, opts)
if err != nil {
    WriteError(w, err)
    return
}
WriteJSON(w, http.StatusOK, obs)
```

- [ ] **Step 3: Update `startInstance` to use new `Start` signature**

Replace the entire `startInstance` function (lines 156–166):

```go
func (h *handlers) startInstance(w http.ResponseWriter, r *http.Request) {
	tmpl, slug := r.PathValue("template"), r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	obs, err := h.svc.Start(r.Context(), r.PathValue("host"), tmpl, slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
}
```

- [ ] **Step 4: Build to verify compile**

```
go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/... 2>&1
```

Expected: no errors.

---

## Task 5: Update UI deploy handler

**Files:**
- Modify: `internal/ui/handlers_deploy.go`

- [ ] **Step 1: Update `deployCreate` to use `ApplyAndObserve` and surface warnings**

Replace the entire `deployCreate` function (lines 247–276):

```go
func (u *UI) deployCreate(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	params, secrets := formValues(r.PostForm)
	req := instance.ApplyRequest{
		Template:   r.FormValue("template"),
		Slug:       r.FormValue("slug"),
		Parameters: params,
		Secrets:    secrets,
	}
	obs, applyErr := u.cfg.Svc.ApplyAndObserve(r.Context(), host, req, instance.ApplyOptions{Replace: false})
	if applyErr != nil {
		data, derr := u.deployFormData(r, host, req.Template, req.Slug, typedValues(r.PostForm), false)
		if derr != nil {
			u.renderError(w, r, derr)
			return
		}
		data["Error"] = applyErr.Error()
		u.render(w, r, errorStatus(applyErr), "deploy-form", u.pageData(data))
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	if len(obs.Warnings) > 0 {
		data["Notice"] = strings.Join(obs.Warnings, "; ")
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}
```

Make sure `"strings"` is in the import block at the top of `handlers_deploy.go` (it already is).

- [ ] **Step 2: Build to verify compile**

```
go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... 2>&1
```

Expected: still a compile error in `handlers_instances.go` (Start call). That's Task 6.

---

## Task 6: Update UI lifecycle handler

**Files:**
- Modify: `internal/ui/handlers_instances.go`

- [ ] **Step 1: Update `lifecycle` to handle `Start`'s new signature**

Replace the entire `lifecycle` function (lines 50–99):

```go
// lifecycle dispatches start/stop/restart/delete, then re-renders the instance
// detail (or the host instance list, after a delete). Upgrade is NOT handled
// here — it is a separate form flow.
func (u *UI) lifecycle(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	action := r.PathValue("action")
	ctx := r.Context()

	var (
		err    error
		notice string
	)
	switch action {
	case "start":
		var obs instance.Observed
		obs, err = u.cfg.Svc.Start(ctx, host, tmpl, slug)
		if err == nil && len(obs.Warnings) > 0 {
			notice = strings.Join(obs.Warnings, "; ")
		}
	case "stop":
		err = u.cfg.Svc.Stop(ctx, host, tmpl, slug)
	case "restart":
		err = u.cfg.Svc.Restart(ctx, host, tmpl, slug)
	case "delete":
		err = u.cfg.Svc.Delete(ctx, host, tmpl, slug, instance.DeleteOptions{})
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	if err != nil {
		obs, gerr := u.cfg.Svc.Get(ctx, host, tmpl, slug)
		if gerr != nil {
			u.renderError(w, r, err)
			return
		}
		data := u.instanceView(ctx, host, obs)
		data["ActionError"] = err.Error()
		u.render(w, r, errorStatus(err), "instance-detail", u.pageData(data))
		return
	}
	if action == "delete" {
		obs, lerr := u.cfg.Svc.ListAllInstances(ctx, host)
		if lerr != nil {
			u.renderError(w, r, lerr)
			return
		}
		u.render(w, r, http.StatusOK, "host-instances", u.pageData(map[string]any{"Host": host, "Instances": obs}))
		return
	}
	obs, gerr := u.cfg.Svc.Get(ctx, host, tmpl, slug)
	if gerr != nil {
		u.renderError(w, r, gerr)
		return
	}
	data := u.instanceView(ctx, host, obs)
	if notice != "" {
		data["Notice"] = notice
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}
```

Add `"strings"` to the import block if not already present.

- [ ] **Step 2: Full build and tests**

```
go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./... 2>&1
```

Expected: no compile errors.

```
make test 2>&1 | grep -E "FAIL|ok"
```

Expected: all pass.

- [ ] **Step 3: Commit Tasks 3–6 together**

```bash
git add internal/instance/service.go internal/instance/migrate.go \
        internal/instance/service_test.go \
        internal/api/instances.go \
        internal/ui/handlers_deploy.go internal/ui/handlers_instances.go
git commit -m "feat(65): ApplyAndObserve + Start readiness wait; wire into API and UI handlers"
```

---

## Task 7: Add `-deploy-verify-timeout` flag

**Files:**
- Modify: `cmd/podman-api/main.go`

- [ ] **Step 1: Add the flag declaration** (near line 54, alongside `migrateVerifyTimeout`)

Add this line after the `migrateVerifyTimeout` flag declaration:

```go
deployVerifyTimeout = flag.Duration("deploy-verify-timeout", 30*time.Second, "how long to wait for container healthchecks to pass after deploy or start (0 = disabled)")
```

- [ ] **Step 2: Call `SetDeployVerifyTimeout` after `SetVerifyTimeout`** (around line 118)

Add after `instance.SetVerifyTimeout(*migrateVerifyTimeout)`:

```go
instance.SetDeployVerifyTimeout(*deployVerifyTimeout)
```

- [ ] **Step 3: Build to verify**

```
go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -o bin/podman-api ./cmd/podman-api 2>&1
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add cmd/podman-api/main.go
git commit -m "feat(65): add -deploy-verify-timeout flag (default 30s)"
```

---

## Task 8: Health dot on instance list

**Files:**
- Modify: `internal/ui/templates/host-instances.html`

- [ ] **Step 1: Add health dot column**

Replace the entire file content:

```html
{{define "host-instances"}}
<div class="host-head">
  <strong>{{.Host}}</strong> · {{len .Instances}} instances
  <a class="pure-button" href="/ui/hosts/{{.Host}}/deploy" hx-get="/ui/hosts/{{.Host}}/deploy" hx-target="#main" hx-push-url="true">+ Deploy</a>
</div>
<table class="pure-table">
  <tbody>
  {{range .Instances}}
    <tr class="clickable" hx-get="/ui/hosts/{{$.Host}}/instances/{{.Template}}/{{.Slug}}" hx-target="#main" hx-push-url="true">
      <td>{{.Template}} / {{.Slug}}</td>
      <td>
        {{.Pod.Status}}
        {{if eq .Pod.Status "Running"}}
          {{if .Ready}}<span class="health-dot healthy" title="healthy">●</span>{{else}}<span class="health-dot starting" title="not yet healthy">●</span>{{end}}
        {{end}}
      </td>
    </tr>
  {{end}}
  </tbody>
</table>
{{end}}
```

- [ ] **Step 2: Add CSS for health dots**

Check `internal/ui/static/style.css` for existing status chip styles, then add:

```css
.health-dot { font-size: 0.85em; margin-left: 4px; }
.health-dot.healthy { color: #2ecc71; }
.health-dot.starting { color: #f39c12; }
.health-dot.unhealthy { color: #e74c3c; }
```

Find the right file with:
```
grep -rn "pure-table\|status" internal/ui/static/ | head -5
```

Then append the CSS to that file.

- [ ] **Step 3: Commit**

```bash
git add internal/ui/templates/host-instances.html internal/ui/static/
git commit -m "feat(65): health dot on instance list"
```

---

## Task 9: Health column and warnings on instance detail

**Files:**
- Modify: `internal/ui/templates/instance-detail.html`

- [ ] **Step 1: Update the containers section with health column**

Replace line 17 (the containers range):
```html
{{range .Inst.Containers}}<div>{{.Name}} — {{.Image}} — {{.Status}} (restarts {{.RestartCount}})</div>{{end}}
```

With:
```html
{{range .Inst.Containers}}
<div class="container-row">
  <span>{{.Name}}</span>
  <span>{{.Image}}</span>
  <span>{{.Status}} (restarts {{.RestartCount}})</span>
  <span>
    {{if eq .Health "healthy"}}<span class="health-dot healthy" title="healthy">● healthy</span>
    {{else if eq .Health "starting"}}<span class="health-dot starting" title="starting">● starting</span>
    {{else if eq .Health "unhealthy"}}<span class="health-dot unhealthy" title="unhealthy">● unhealthy</span>
    {{else}}—{{end}}
  </span>
</div>
{{end}}
```

- [ ] **Step 2: Verify Notice is already wired**

Check line 3 of `instance-detail.html` — the Notice div already exists:
```html
{{if .Notice}}<div class="notice">{{.Notice}}</div>{{end}}
```

No changes needed; the deploy and start handlers already pass `data["Notice"]`.

- [ ] **Step 3: Run full test suite**

```
make test 2>&1 | grep -E "FAIL|ok"
```

Expected: all pass.

- [ ] **Step 4: Build**

```
make build 2>&1
```

Expected: `bin/podman-api` produced, no errors.

- [ ] **Step 5: Commit**

```bash
git add internal/ui/templates/instance-detail.html
git commit -m "feat(65): per-container health column on instance detail"
```

---

## Self-Review Checklist

Run after all tasks:

- [ ] `gofmt -l .` outputs nothing (no unformatted files)
- [ ] `go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...` outputs nothing
- [ ] `make test` — all pass
- [ ] `make build` — produces `bin/podman-api`
- [ ] Spec coverage:
  - Apply blocks on readiness → `ApplyAndObserve` ✓
  - Start blocks on readiness → `Start` signature change ✓
  - Timeout warns, not fails → `obs.Warnings` ✓
  - Health in GET responses → `Normalize` populates `Health` ✓
  - UI list dot → `host-instances.html` ✓
  - UI detail health column → `instance-detail.html` ✓
  - `-deploy-verify-timeout` flag → `main.go` ✓
  - `timeout=0` disables wait → `waitReady(timeout==0)` returns nil ✓
