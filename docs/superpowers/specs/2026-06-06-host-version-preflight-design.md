# Host podman-version preflight (issue #85)

Date: 2026-06-06
Issue: #85 — managed podman hosts need ≥ 5.6.0 for volume import/export API (migrate)

## Problem

Cold-copy migrate/evacuate streams volumes through libpod's `GET /volumes/{name}/export`
and `volumes.Import` (`internal/podman/real.go`), which first shipped in podman 5.6.0.
Against an older host the export request 404s mid-migration — a confusing late failure.
Nothing today checks a host's podman version at registration: hosts load from YAML at
startup (`config.LoadHosts` → `podman.NewReal`) and connections open lazily, so the first
signal of an unsupported host is an operation failing.

## Decision summary

- **Floor**: podman ≥ **5.6.0**, constant in `internal/podman`.
- **Boot policy**: fail boot on a reachable host below the floor; tolerate unreachable
  hosts with a logged warning (transient network must not take the daemon down).
- **Deferred enforcement**: hosts that skipped the boot check are version-verified on
  first successful connect; below-floor hosts fail operations with a typed error.
- **Diagnostics exempt**: `Ping`/`Version` bypass enforcement so `GET /hosts` can still
  display the actual (unsupported) version.
- **Error surface**: sentinel `podman.ErrHostVersionUnsupported`, classified by the API
  layer as code `host_version_unsupported`, HTTP 422 (consistent with
  `host_secret_missing`).

## Design

### 1. Version floor — `internal/podman` (new `version.go`)

- `const MinPodmanVersion = "5.6.0"` with a rationale comment pointing at the volume
  export/import bindings and #85.
- `var ErrHostVersionUnsupported = errors.New("host podman version unsupported")`.
- `checkVersion(hostID, v string) error` parses with `github.com/blang/semver/v4`
  (`semver.ParseTolerant` — podman reports suffixed versions like `5.8.2-dev`; promote
  the dep from indirect to direct). Below floor → error wrapping
  `ErrHostVersionUnsupported` with host ID, found version, and floor, e.g.
  `host "test-1": podman 5.4.2 < minimum 5.6.0 (volume export/import requires >= 5.6.0)`.
  Unparseable version → error wrapping the sentinel (fail closed: the floor cannot be
  confirmed).

### 2. Boot preflight — `(*Real).Preflight(ctx) error`

For each registered host, with a short per-host timeout (~10s):

- Connect + fetch version (`system.Info`).
- **Unreachable** → `log.Printf` warning, continue; host stays unverified.
- **Below floor / unparseable** → return the `checkVersion` error; `main.go` treats it
  as fatal (`log.Fatalf`), so the daemon refuses to start.
- **Pass** → mark host verified.

Wired in `cmd/podman-api/main.go` immediately after `podman.NewReal(hosts)`.

Timeout caveat: `ctxFor` deliberately roots cached connections at
`context.Background()` (per-request cancellation must not kill them), so the boot
timeout cannot simply be a ctx passed through `ctxFor` — that would cache a
connection rooted in a soon-cancelled ctx. Bound the preflight attempt externally
(e.g. run connect+check in a goroutine and select on a timer); on timeout treat the
host as unreachable (warn, continue) and leave any late-arriving connection to be
cached or discarded safely.

### 3. Lazy enforcement — verified-gated connection path

`Real` gains a `verified map[string]bool` guarded by the existing `mu`.

- `ctxFor` stays the raw connect path (unchanged semantics).
- New `opCtxFor(parent, id)` = `ctxFor` + ensure-verified: if the host is not yet
  verified, fetch version over the (cached) connection, run `checkVersion`, and mark
  verified on pass. On fail, return the typed error **without** marking verified — the
  connection ctx stays cached, so a host whose podman is upgraded in place starts
  passing without a daemon restart.
- All operation methods (`PlayKube`, volume export/import, pod ops, …) switch from
  `ctxFor` to `opCtxFor`. `Ping` and `Version` keep raw `ctxFor` (diagnostics exempt).
- Version check is at most one extra `system.Info` round-trip per host per process
  lifetime (plus retries while a host is failing the check).

### 4. API error mapping — `internal/api/errors.go`

`classify()` maps `podman.ErrHostVersionUnsupported` → code `host_version_unsupported`,
HTTP 422. Operations against a sneaked-in old host (unreachable at boot, recovered
later) surface a clear setup error instead of a libpod 404.

### 5. Tests

- `checkVersion` table test: below floor (`5.4.2`), at floor (`5.6.0`), above (`5.8.2`),
  tolerant parse (`5.8.2-dev`), garbage (`""`, `"abc"`) → error wrapping sentinel.
- Verified-set behaviour unit-tested at the `Real` level where possible without a live
  socket; connection-dependent paths (`Preflight`, `opCtxFor` round-trip) are covered by
  the integration suite against a real podman host.
- `internal/podman/fake`: `Version()` returns a configurable `VersionStr` (default
  remains `fake-1.0` for existing tests) so API-level tests can assert the
  `host_version_unsupported` → 422 mapping end-to-end through `classify()`.

### 6. Wiki — "Supported host OS" on *Provisioning a Podman Host*

Published directly to the wiki repo (no PR flow):

- One-line floor rationale (volume export/import needs podman ≥ 5.6.0, link #85).
- Distro table from the issue: AlmaLinux 10 / Rocky 10 / Fedora ✅; Ubuntu 24.04,
  Debian 13, Rocky/RHEL 9 ❌ on stock repos.
- Recommend AlmaLinux 10 (Rocky 10 second choice); note the openSUSE OBS escape hatch
  for Debian/Ubuntu and why it's discouraged for a managed fleet.
- Note the daemon behaviour: refuses to boot against a reachable host below 5.6.0;
  unreachable hosts are warned and re-checked on first use.

## Out of scope

- `version_status` field in `GET /hosts` (log-only warnings for now).
- Migrate-preflight duplication of the check (the connection layer already covers it).
- Quarantine/drain semantics for unsupported hosts.
