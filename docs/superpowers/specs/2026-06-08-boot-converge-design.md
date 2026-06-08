# Boot Converge: Instance Spec Reconciliation on Startup

**Issue:** #125
**Date:** 2026-06-08
**Status:** Approved

## Problem

Managed instances deployed via `podman play kube` do not survive a host reboot.
Only `podman-api.service` is wired into systemd; workload pods have no per-pod
systemd units. `restartPolicy: Always` recovers container crashes, but a host
reboot leaves pods down until podman-api re-converges the stored desired state
— which it currently does not do.

## Solution: One-shot boot converge of stored specs

After startup, podman-api asynchronously reconciles every stored instance spec
against real host state. If a pod is missing, it is re-created from the stored
template + parameters.

### Method

**`Service.ReconcileSpecsOnHost(ctx, hostID) error`** in a new file
`internal/instance/spec_reconcile.go`.

For each spec key on the host:

1. **`PodInspect`** — if the pod exists and is running, skip (already converged).
2. **Load spec** (`store.GetSpec`) and template (`store.GetTemplate`).
3. **Template deleted** → log warning, skip (spec row kept; operator deletes via API).
4. **Spec corrupt / undecryptable** → log error, skip (operator fixes key or cleans up).
5. **Apply defaults** to stored parameters (`render.ApplyDefaults`).
6. **Validate** with `ValidateAllowMissingSecrets` (a stored spec may lack a secret
   the template now requires — the pod was running before the template gained it).
7. **Render YAML** (`render.RenderBody`).
8. **Create per-instance secrets** on host (deterministic names, remove+create).
9. **Ensure ingress network** if template declares ingress.
10. **`PlayKube`** — `replace=false` when the pod is absent, `replace=true`
    when the pod exists but is not running (Exited/Stopped/Paused), so podman
    replaces the stale pod rather than returning "pod already exists".
11. **`PutSpec`** (upsert — updates timestamp).
12. **`Ingress.Reconcile`** if any instance was reconverged (once per host,
    not once per instance).

### Startup wiring

In `cmd/podman-api/main.go`, just before `ListenAndServe`, a goroutine is
launched that fans out per-host to `ReconcileSpecsOnHost`. Results are
logged. The HTTP server starts immediately — converge is async.

### Error tolerance (all errors are non-fatal — log + continue)

| Error | Handling |
|-------|----------|
| Host unreachable | Warn, skip that host entirely |
| Template deleted | Warn, skip that instance |
| Spec corrupt | Error, skip that instance |
| Secrets undecryptable | Error, skip (wrong/missing key — operator must restart with correct key) |
| Render / PlayKube failure | Error, skip (pod stays down; operator can `PUT` to re-apply) |

### Not included

- **No periodic loop.** This is a one-shot boot converge. The existing periodic
  ingress loop handles Caddy drift. A periodic pod-spec drift loop is a separate
  feature.
- **No image pull.** Images should already be cached on the host. Skipping the
  pull makes boot converge fast.
- **No secret key pre-check.** If secrets are undecryptable, it shows up at
  step 2's `GetSpec` and is handled as a per-instance error.

### Testing

New test file `internal/instance/spec_reconcile_test.go` using `fake.Fake` and
`store.Memory`:

- Running pod → no PlayKube call
- Missing pod → PlayKube called with correct YAML
- Template deleted → warning, no PlayKube
- Host unreachable → warning, no PlayKube
- Spec with secrets → secrets created on host before PlayKube