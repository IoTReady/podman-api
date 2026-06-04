# Migrate Safety-Before-Reap Design

**Issue:** #54 (migrate/evacuate hardening backlog) — *Correctness/verification* group.

**Goal:** A migrate must not destroy the source until the destination is *verifiably*
good. Today the verify step only checks pod/container liveness and the volume copy
is unverified, so an app that is up-but-not-serving, or a silently truncated/corrupt
volume copy, can be committed and the source reaped — losing data. This adds an
application-readiness gate and a volume-copy integrity check, both gating the
destructive commit.

## Scope

Two #54 sub-items, tightly coupled around the migrate "verify" step:

1. **Application-readiness gate.** `waitRunning` only checks every container is
   `Running` before reaping the source. A container can be up before the app serves
   (DB replaying WAL, etc.). Gate the commit on container *health*, not just liveness.
2. **Volume-copy integrity.** The cold-copy export→import has no verification that the
   destination matches the source before the source is reaped.

Both failures occur inside the migrate *job*, so they surface as a job failure (with
rollback), never as the POST status. Evacuate inherits the behavior for free — its
children are migrates.

### Explicitly out of scope (YAGNI)

- **No per-request override** for either toggle. Global flags only, mirroring how
  `evacuateConcurrency` began before it grew a per-request override. Can be added later.
- **No configurable readiness probe** (httpGet/tcpSocket/exec). Readiness rides
  entirely on template-declared container healthchecks — podman-native, zero new
  config surface.

## Architecture

Three components, in dependency order:

```
podman.Container.Health  ──►  podReady() / waitRunning()        (readiness gate)
                              (instance/migrate.go)

archive/tar manifest     ──►  CopyVolume() returns src manifest  (integrity)
                              re-export dest, compare
                              (instance/service.go + migrate.go)
```

### Component 1 — Container health in the podman layer

- Add `Health string` to `podman.Container` (`internal/podman/types.go`):
  - `""` — the container declares **no** healthcheck.
  - `"healthy"` / `"unhealthy"` / `"starting"` — podman's reported health status.
- Populate in `enrichContainer` (`internal/podman/real.go`), which already calls
  `containers.Inspect`:
  ```go
  if ins.State != nil && ins.State.Health != nil {
      c.Health = ins.State.Health.Status
  }
  ```
  **Zero extra API calls** — the inspect report is already fetched for image/ports/env.
- The fake client (`internal/podman/fake`) gains a way to set per-container health so
  the gate is unit-testable.

### Component 2 — Application-readiness gate (`internal/instance/migrate.go`)

- Replace `allContainersRunning(p)` with `podReady(p podman.Pod) bool`:
  - every container is `Running`, **and**
  - every container that *declares* a healthcheck (`Health != ""`) reports
    `Health == "healthy"`.
- Containers with no declared healthcheck keep today's liveness-only behavior. The
  gate is therefore **opt-in by the template** and fully backward compatible: an
  instance with no healthchecks behaves exactly as before.
- `"starting"` (still inside the healthcheck `start_period`) counts as *not ready* —
  keep polling. A container stuck `unhealthy`/`starting` past the timeout makes
  `waitRunning` return its existing timeout error → migrate rolls back (restart
  source, reap partial dest) → **source preserved**. This is the safety win.
- App readiness (WAL replay, cache warm-up) can exceed today's 60s. Expose the
  existing `verifyTimeout` var as a flag `-migrate-verify-timeout` (default `60s`).
  `verifyInterval` stays an internal constant.

### Component 3 — Volume-copy integrity (`internal/instance/service.go` + `migrate.go`)

- New manifest type (in `internal/instance`):
  ```go
  type fileInfo struct {
      typ    byte   // tar.Header.Typeflag
      size   int64  // regular files only
      sha256 string // hex; regular files only
      link   string // symlink/hardlink target only
  }
  type Manifest map[string]fileInfo // key: path.Clean(tar.Header.Name)
  ```
- `buildManifest(r io.Reader) (Manifest, error)` parses a tar stream with
  `archive/tar`:
  - key on `path.Clean(hdr.Name)`;
  - regular files: record `size` + streaming `sha256` of contents;
  - symlinks/hardlinks: record `link` (Linkname);
  - directories: recorded with type only (presence matters, no content);
  - **ignore** mtime/uid/gid/mode/pax-records — podman export need not preserve those
    identically across hosts.
  - Sorted-map equality makes the comparison **order/layout independent**.
- `CopyVolume` tees the source export through the manifest builder *during* the copy,
  returning the source `Manifest`:
  ```go
  func (s *Service) CopyVolume(ctx, fromHost, toHost, name string) (Manifest, error)
  ```
  Implementation: the existing copy goroutine writes source bytes to an
  `io.MultiWriter(importPipeWriter, manifestPipeWriter)`; a second goroutine runs
  `buildManifest` on the manifest pipe's read end. One source read; sha256 overlaps
  the network-bound copy. The only caller is `migratePostStop`, so the signature
  change is contained.
- `volumeManifest(ctx, host, name) (Manifest, error)` helper: export a host's volume
  and build its manifest. Used for the **dest re-export** (the one extra read).
- `migratePostStop` flow per volume becomes:
  ```
  VolumeCreate(dest, v)
  srcManifest = CopyVolume(src, dest, v)      // tee'd during copy
  if verifyVolumes {
      dstManifest = volumeManifest(dest, v)   // one extra dest read
      if !srcManifest.Equal(dstManifest) { return ErrVolumeIntegrity(v, firstDiff) }
      step("verify-volume", v)
  }
  ```
  A mismatch returns a wrapped `ErrVolumeIntegrity` naming the volume and the first
  differing path. The caller's existing rollback path restarts the source and reaps
  the partial dest, so the **source is left intact**.
- Toggle: `-migrate-verify-volumes` bool flag, **default `true`** (this is the safety
  batch). When `false`, `migratePostStop` skips the dest re-export and compare; the
  source manifest is still built (cheap, overlaps the copy) but unused.

## Data flow (commit gate)

```
stop source
  └─ per volume: create dest, copy (tee→srcManifest), [re-export dest→dstManifest, compare]
  └─ apply spec on dest
  └─ waitRunning: poll until podReady (Running + declared healthchecks healthy), bounded by -migrate-verify-timeout
  └─ COMMIT: reap source (detached ctx)        ← only reached if every gate above passed
any failure before COMMIT → rollback: restart source, reap partial dest (source intact)
```

## Error handling

- `ErrVolumeIntegrity` — new sentinel in `internal/instance`, wrapped with volume name
  + first differing path. Occurs in the job → job failure with per-step detail. No new
  `internal/api/errors.go` mapping (it never reaches a synchronous POST).
- Readiness timeout reuses the existing `waitRunning` timeout error path → job failure
  → rollback.
- A failed dest re-export (`volumeManifest` error) is treated like any other
  post-stop failure → rollback. Never reap on an unverifiable copy.

## Configuration

| Flag | Default | Meaning |
|------|---------|---------|
| `-migrate-verify-timeout` | `60s` | Max wait in `waitRunning` for the dest pod to become ready (running + declared healthchecks healthy). |
| `-migrate-verify-volumes` | `true` | Verify each copied volume's content manifest against the source before reaping. `false` disables the dest re-export+compare. |

Both wired in `cmd/podman-api/main.go`. No API request fields change; `api/openapi.yaml`
unchanged.

## Testing

Unit (no real podman):

- `podReady` table: no-healthcheck-all-running → ready; healthcheck healthy → ready;
  one `unhealthy` → not ready; one `starting` → not ready; mixed declared/undeclared.
- `waitRunning` against a fake whose `PodInspect` returns evolving health
  (`starting`→`healthy` becomes ready; stays `unhealthy` → timeout error). Shorten
  `verifyTimeout`/`verifyInterval` via the package vars.
- `buildManifest`: crafted tar → expected manifest; same content in different entry
  order → equal manifests; truncated content / changed byte / missing file → unequal;
  symlink target recorded; mtime/uid differences ignored.
- `CopyVolume` returns a source manifest matching the bytes streamed (fake export);
  copy still succeeds and import receives the full stream.
- `migratePostStop` integrity mismatch (fake where dest export differs from source) →
  returns `ErrVolumeIntegrity`; caller rollback restarts source and reaps dest.
- `Manifest.Equal` + first-diff helper unit test.

Fake client additions: per-container `Health`; per-volume content so
export/import/round-trip and manifest comparison are exercisable in-memory.

## Documentation

- `README.md`: add `-migrate-verify-timeout` and `-migrate-verify-volumes` to the flag
  list.
- Wiki `Deploying.md`: add both flags to the migrate/evacuate line of the Run example.
- Wiki `Operating.md` "Migrating & evacuating instances": rewrite the current
  *documented-limitation* note about the readiness gate into the real behavior
  (commit waits for declared healthchecks; tune with `-migrate-verify-timeout`); add a
  short note that volume copies are content-verified before the source is reaped, and
  that `-migrate-verify-volumes=false` opts out.
