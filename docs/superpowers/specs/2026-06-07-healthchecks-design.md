# Generic App-Readiness Healthchecks — Design

**Issue:** #65  
**Date:** 2026-06-07  
**Tier:** OSS

---

## Problem

Container `Running` status does not mean the app is serving. Deploys and starts
currently return immediately after `PlayKube`/`PodStart` succeeds, leaving the
caller with a pod that may be in the healthcheck `starting` or `unhealthy` state.
The readiness verification path already exists in `migrate.go` (`waitRunning`,
`podReady`) but is not connected to regular deploys or starts.

**Gap:** #54 `waitRunning` noted this; #65 closes it for all lifecycle operations
and surfaces health status in the API and UI.

---

## Success Criteria

- `Apply` (deploy) blocks until every container that declares a healthcheck
  reports `healthy`, or until `-deploy-verify-timeout` elapses.
- `Start` (restart a stopped instance) applies the same readiness wait.
- On timeout, the operation still succeeds (pod is Running) but the response
  carries a `warnings` field with a human-readable message.
- Container health status is visible in GET `/instances` responses.
- The UI shows an aggregated red/green dot per instance on the list page and
  per-container health on the instance detail page.

---

## Architecture & Data Flow

```
Apply / Start
      |
      v
PlayKube / PodStart
      |
      v
waitReady(ctx, host, tmpl, slug, deployVerifyTimeout)
  ├── polls podReady every 2s (existing function, unchanged)
  ├── nil        → proceed normally
  └── timeout    → attach warnings=["readiness timeout: ..."]
      |
      v
buildObserved(...)
  ├── ObservedContainer.Health  populated from podman inspect
  └── Observed.Ready            aggregated bool
      |
      v
HTTP 200 / 201
  { ...observed..., "warnings": ["..."] }   // warnings omitted when empty
```

`waitReady` is extracted from `migrate.go`'s `waitRunning` into a shared helper.
Both the migrate path and deploy/start use it; each path passes its own timeout.
`podReady` is unchanged — containers without a declared healthcheck (`Health == ""`)
are gated on `Status == "Running"` only.

---

## Data Model

### `internal/instance/observed.go`

```go
type ObservedContainer struct {
    Name   string `json:"name"`
    Image  string `json:"image"`
    Status string `json:"status"`
    Health string `json:"health,omitempty"` // "healthy"/"unhealthy"/"starting"/""
}

type Observed struct {
    Template   string              `json:"template"`
    Slug       string              `json:"slug"`
    Ready      bool                `json:"ready"`              // aggregated healthcheck status
    Pod        ObservedPod         `json:"pod"`
    Containers []ObservedContainer `json:"containers"`
    Volumes    []ObservedVolume    `json:"volumes,omitempty"`
    EnvSummary map[string]string   `json:"env_summary,omitempty"`
    Warnings   []string            `json:"warnings,omitempty"` // readiness timeout message
}
```

**`Ready` semantics:** `true` when every container with `Health != ""` reports
`"healthy"`. Containers that declare no healthcheck don't affect `Ready` — they
only require `Status == "Running"`. An instance with no healthchecks at all has
`Ready: true` once it is Running.

`Warnings` and `Health` are `omitempty` so existing clients are unaffected when
everything succeeds.

---

## Service Layer

### `internal/instance/ready.go` (new file)

Extract and generalize `waitRunning` from `migrate.go`:

```go
var errReadyTimeout = errors.New("readiness timeout")

func (s *Service) waitReady(ctx context.Context, host, tmpl, slug string, timeout time.Duration) error {
    if timeout == 0 {
        return nil // disabled
    }
    deadline := time.Now().Add(timeout)
    ticker := time.NewTicker(verifyInterval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
            if err == nil && podReady(p) {
                return nil
            }
            if time.Now().After(deadline) {
                return errReadyTimeout
            }
        }
    }
}
```

`migrate.go`'s `waitRunning` becomes:

```go
func (s *Service) waitRunning(ctx context.Context, host, tmpl, slug string) error {
    return s.waitReady(ctx, host, tmpl, slug, verifyTimeout)
}
```

### `Apply` and `Start`

`Start` currently returns `error`; this changes to `(*Observed, error)` so it can
carry warnings. The API handler for the start lifecycle route changes to return the
observed snapshot (consistent with Apply). `Stop` and `Restart` are not changed —
readiness is only relevant when bringing a pod up.

After the podman operation, call `waitReady` with `deployVerifyTimeout`. On
`errReadyTimeout`, populate `Warnings` on the returned `Observed`; on any other
inspect error during polling, log and proceed normally (pod is running, a transient
inspect failure is not fatal). Context cancellation propagates as an error.

`timeout=0` skips the wait entirely, preserving current behaviour for any operator
that sets `-deploy-verify-timeout=0`.

The `Warnings` slice is populated before calling `buildObserved` so it is present
on the same response object returned to the caller.

### `buildObserved` (updated)

Populate `ObservedContainer.Health` from `podman.Container.Health` (already
available via `enrichContainer`). Compute `Observed.Ready` by checking all
containers with `Health != ""`.

---

## Configuration

New flag in `cmd/podman-api/main.go`:

```go
flag.DurationVar(&cfg.DeployVerifyTimeout, "deploy-verify-timeout", 30*time.Second,
    "how long to wait for container healthchecks to pass after deploy or start (0 = disabled)")
```

Existing `-migrate-verify-timeout` is unchanged. The two timeouts are independent
because migrations tolerate longer waits (DB WAL replay, cache priming) while
deploys should be snappier.

---

## UI

### Instance list (`instances.html`)

Aggregate health dot immediately after the existing status chip:

| Condition | Dot | Meaning |
|-----------|-----|---------|
| `ready: true` | ● green | all declared healthchecks passing (or none declared) |
| `ready: false`, pod Running | ● amber | pod up, healthcheck not yet passing |
| pod not Running | — | status chip already conveys this |

Data comes from `Observed.Ready` in the existing GET response — no extra API call.

### Instance detail (`instance-detail.html`)

Per-container health column in the containers table:

| Container | Image | Status | Health |
|-----------|-------|--------|--------|
| app | nginx:1.25 | Running | ● healthy |
| db | postgres:16 | Running | ● starting |
| sidecar | alpine | Running | — |

`—` when `Health` is empty (no healthcheck declared). Colors: green=`healthy`,
amber=`starting`, red=`unhealthy`.

### Deploy response warning

When `warnings` is non-empty in the deploy response, append to the success flash:
> "Instance deployed. Note: healthcheck did not pass within 30s — the app may still be initialising."

---

## Testing

### `internal/instance/ready_test.go` (new)

- `waitReady` returns `nil` when pod becomes healthy before deadline
- `waitReady` returns `errReadyTimeout` when deadline passes
- `waitReady` returns `ctx.Err()` on context cancellation
- `waitReady` with `timeout=0` returns immediately without polling
- Containers without healthchecks (`Health == ""`) do not block readiness

### `internal/instance/service_test.go` additions

- `Apply` with healthy container → `Ready: true`, no warnings
- `Apply` with timeout → `Ready: false`, `Warnings` non-empty
- `Start` with healthy container → `Ready: true`, no warnings
- `Start` with timeout → `Ready: false`, `Warnings` non-empty

### `internal/instance/observed_test.go` (new or extended)

- `buildObserved` propagates `Health` from `podman.Container`
- `Ready` aggregation: all healthy → true; any unhealthy/starting → false; no healthchecks → true

Existing migrate tests are unaffected — they call `waitRunning` which delegates
to `waitReady` with `verifyTimeout`.

The existing `fake.PlayKubeContainerHealth` field already supports driving health
states in tests; no new fake infrastructure needed.

---

## Out of Scope

- SQLite-specific health (WAL lag, replication) — commercial tier (#65 note)
- Configurable per-template readiness timeouts — a future improvement
- Automatic rollback on readiness failure — out of scope; deploy warns, not fails
