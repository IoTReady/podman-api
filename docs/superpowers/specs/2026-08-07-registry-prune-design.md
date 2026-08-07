# First-party registry prune (#219 / #220) — design

**Date:** 2026-08-07
**Status:** proposed, awaiting review
**Supersedes the premise of:** issue #219 as filed (see "Correcting the record")

## Goal

Move container-registry garbage collection out of `registry-gc.sh` — a hand-installed
script on an unmanaged host, driven by a bare systemd timer — and into the podman-api
control plane as a managed, observable job. Both stages: manifest deletion (v2 API) and
blob reclamation (registry `garbage-collect`).

## Correcting the record

Issue #219 states that `registry-gc.sh`'s notion of "in use" is "a heuristic (tag-name
pattern matching)". **That is no longer true** — it was true before the pro #64/#65/#66
remediation. The script today:

- queries the control plane (`GET /hosts`, then `/hosts/<host>/instances`),
- takes every container's `image_tag`, resolving tag-form refs to digests with
  `HEAD /v2/<repo>/manifests/<tag>`,
- protects those digests **globally across all repos**,
- **fails closed** if the control plane is unreachable or the fleet-wide in-use count
  is zero,
- re-checks the protected set immediately before each `DELETE`,
- classifies every repo before deleting anything (the #64 fix),
- keeps unrecognised tags by default (the #66 fix).

So the case for this work is **not correctness**. It is operability, plus one real
correctness gap (below).

### The evidence that operability is the problem

The 2026-08-02 scheduled run **aborted**: `engine` produced 274 delete candidates against
a fixed `MAX_DELETES_PER_REPO=100` tripwire. Because the tripwire aborts the *whole run*,
every other repo also got zero deletions. Nothing surfaced this. GC has not run since;
`/srv/registry` is **71G**. A dry-run on 2026-08-07 found only **2** delete candidates,
so the condition cleared itself — by an unattributed mechanism (`engine` went from 988
tags on 2026-08-06 to 570 on 2026-08-07, and the ~274 `SHA-ORPHAN` tags are gone, but no
successful timer run occurred in that window). A week of silent non-execution, and a
fleet-wide state change nobody observed, are both exactly what a job record, metrics, and
alerting exist to prevent.

### The one real correctness gap

The script protects digests of **currently running containers**. That is narrower than
*recreatability*, which is what the #64 incident actually destroyed: 11 of 24 Engine
instances survived with running containers but could never have been recreated, because
their manifests were gone. An instance that is stopped, or whose stored spec pins a
digest nothing is currently running, is **not protected today**.

**This design widens the in-use set to the union of observed running digests and stored
spec image refs resolved to digests.**

## Prerequisites (infrastructure, not code)

Both must land before the feature can run in production. Neither is a code change.

### P1 — podman upgrade on otp-infra-1

`internal/podman/version.go` sets `MinPodmanVersion = "5.6.0"`, and `opCtxFor` gates every
operation through `ensureVerified` (`real.go:235`). Only diagnostics (`Ping`, `Version`,
`HostInfo`) bypass it, so an unsupported host renders in `GET /hosts` and fails every
actual operation with `ErrHostVersionUnsupported`.

**otp-infra-1 runs podman 4.9.3.** It must be upgraded to ≥ 5.6.0. `ensureVerified`
caches only successes ("a host whose podman is upgraded in place starts passing without a
restart"), so this does not need to be sequenced against a control-plane restart.

This is an OS-level upgrade on the host running the fleet's only registry. It should be
done deliberately, with the registry's data volume backed up, not as step zero of a
feature branch.

### P2 — host registration

- Generate a dedicated keypair on engine-infra; authorise it for `tej@otp-infra-1`
  (today: no matching key, host key not trusted, `ssh engine-infra "ssh tej@otp-infra-1"`
  fails).
- Add `hosts/otp-infra-1.yaml` matching the engine-1/engine-2 pattern
  (`addr`, `socket: /run/user/<uid>/podman/podman.sock`, `ssh_key`).
- **Pin `prune.enabled: false` explicitly.** It is already off by default
  (`-prune-enabled` defaults false and the live `ExecStart` passes no `-prune-*` flags),
  but image-prune scope on the host holding the only registry must not be one fleet-wide
  flag away from running.
- Reload with SIGHUP (`systemctl --user reload podman-api`); no restart needed.

`tej`'s podman socket is already enabled and lingering is on. No hand-started container on
that host carries a `podman-api/template` label, so the inventory poller ignores them for
instances, metrics, UI, and alerting.

## Architecture

Mirrors `internal/prune` (the host-cleanup scheduler) throughout: ticker-driven scheduler →
resolved policy → job payload → `jobs.Handler` → progress as `jc.Step` rows → generic job
list/detail UI + Prometheus metrics.

```
registryprune.Scheduler (ticker)
        │  enqueue "registry-prune" {Policy}
        ▼
jobs.Runner ──► registryprune.Handler
                     │
                     ├─ 1. build in-use set  ── inventory cache + spec store + registry
                     ├─ 2. classify every repo  (no deletes yet)
                     ├─ 3. tripwire check per repo
                     ├─ 4. Stage A: DELETE manifests by digest
                     └─ 5. Stage B: blob GC  (stop registry → one-shot pod → restart)
```

### Why not systemd control

The blob-GC step as scripted runs `systemctl --user stop registry.service`, and the quadlet
carries `Restart=always`, so a podman-level stop would have systemd restart the registry
*mid-GC* — the exact race that corrupts blobs.

podman-api's client is libpod's REST API tunnelled over SSH to a podman socket. **There is
no shell on the other end**, so there is no `systemctl`. Adding one would mean giving the
control plane a general remote-command-execution channel: a large, permanent security
surface in the OSS core, added to solve one host's lifecycle problem.

**Instead:** the registry becomes a podman-api-managed instance rather than a quadlet.
Lifecycle control is then native, and systemd leaves the loop entirely.

### Why kube play for the one-shot GC

Verified against the code, not assumed:

- `PlayKube(ctx, hostID, yaml string, replace bool, networks ...string) error` takes raw
  YAML (`internal/podman/client.go:16`).
- The core never sets or overrides `restartPolicy` — a manifest-declared
  `restartPolicy: Never` reaches podman untouched.
- `hostPath` passes through; `internal/render/validate.go` checks only the
  parameter/secret allow-list, never the manifest schema. The pro repo's OpenVPN injector
  already proves the shape works live through this path.
- Rootless podman maps container UID 0 to the invoking host user, which is why the existing
  registry quadlet — same user, same `/srv/registry` mount — writes to it today. A GC pod
  running as container-root inherits that mapping.

So the GC step is a pod manifest, not a new `podman run` primitive.

## Components

### 1. `imgregistry.Delete` (new)

```go
Delete(ctx context.Context, repo, digest string) error
```

- Registry v2 `DELETE /v2/<repo>/manifests/<digest>` — **by digest, never by tag**.
- Reuses `ValidRepoName`/`ValidRef`; a ref that is not a digest is rejected before any
  request is built.
- Returns `ErrNotFound` for a genuine 404 and `ErrUnreachable` otherwise. **These must
  never be collapsed** — "unreachable" must not read as "already gone."
- **Bypasses the TTL cache.** `NewCachingClient` caches `Tags()` for 5 minutes; a delete
  decision must not be made from a cached listing, and a delete must invalidate it.

### 2. Exit-code observability (new)

`PlayKube` returns as soon as the pod *starts*. Nothing in the codebase awaits completion,
`podman.Container` has no `ExitCode` field, and `enrichContainer` never reads
`ins.State.ExitCode` even though libpod provides it.

Without this, a GC job can only report that it managed to start a container — it would
report success for a GC that failed. Add:

- `ExitCode int` and `Exited bool` on `podman.Container`, populated in `enrichContainer`.
- A bounded wait helper that polls container state to completion with a timeout, returning
  the exit code.

### 3. In-use digest set

The safety-critical component. Union of two sources:

**(a) Observed** — `Observed.Containers[].Image` from the warm inventory cache
(`ListAllInstancesWithMeta`), which is a resolved digest from a live `podman inspect`,
not a spec guess.

**(b) Desired** — every stored `Spec.Parameters` image-bearing value (`image`, `pg_image`,
and any other `*_image`), resolved tag→digest through the registry. This is the
recreatability half the script lacks.

**Fail-closed rules, all mandatory:**

| Condition | Behaviour |
|---|---|
| Any host's `Freshness.Reachable` is false | Abort the run. A stale snapshot under-reports what is in use. |
| Any host's snapshot older than a configured max age | Abort the run. |
| Fleet-wide in-use digest count is zero | Abort the run (matches the script). |
| A spec's image ref cannot be resolved to a digest | Abort the run — not "skip it". |
| Registry unreachable at any point | Abort the run. |

Protection is keyed **by digest across all repos**, matching the script: blobs are shared,
and this is the conservative direction.

### 4. Classification policy

Ported from the script, kept as data, not code, so it is reviewable and testable:

- Name-exact protected: `latest`, `e2e`, `dev`, `main`, `deps-cache`, `scanners-cache`.
- CalVer: `^[0-9]{4}\.[0-9]{2}\.[0-9]{2}$`.
- Per-repo extras: currently `otp: ^(runtime-base|valvo-fork-v1)`.
- Any digest shared with a protected tag.
- `feat-*` newer than the retention window (default 30d), aged from the config blob's
  `.created`.
- Unrecognised tags are **kept** by default; deleting them stays opt-in.

Deletable classes remain exactly two (plus opt-in unrecognised): bare-hex tags whose digest
is not `:latest`'s, and `feat-*` past retention.

**One deliberate divergence.** The script's per-repo tripwire aborts the *entire run*. That
is what silently disabled GC fleet-wide for a week. Here, a repo exceeding its threshold is
**skipped and recorded** — as a job step, a log line, and a Prometheus counter — while every
other repo proceeds. Safety for the anomalous repo is preserved; a single noisy repo no
longer takes down GC for the other 27.

### 5. Stage B — blob GC

1. Stop the registry instance (native, now that it is managed).
2. `PlayKube` a one-shot pod: `registry:2`, `restartPolicy: Never`, `hostPath` mount of
   `/srv/registry`, command `garbage-collect --delete-untagged /etc/docker/registry/config.yml`.
3. Await completion; read the exit code (component 2).
4. Restart the registry instance — **in a deferred path, so it comes back even if GC fails.**
5. Record before/after volume size.

Stage B is independently gated: a `--no-gc` equivalent runs Stage A only, leaving blobs
recoverable. This mirrors the script's own recommendation for first live runs.

### 6. Surfacing

- Job steps per phase, as `internal/prune` does.
- Prometheus: run outcome, manifests deleted, bytes reclaimed, repos skipped by tripwire.
- Dry-run mode producing the full Keep/Delete classification with **zero** deletes.
- Alerting on run failure — the gap that let 2026-08-02 pass unnoticed.

## Testing

Follows existing conventions: `httptest.NewServer` for `imgregistry.Delete` itself;
a hand-written `imgregistry.Client` fake for handler-level tests; `store.Memory` and
`internal/podman/fake` for job and scheduler wiring.

Non-negotiable cases:

- Every fail-closed rule above, each asserted to abort with **zero** delete calls.
- A digest in use only via a stored spec (nothing running) is never deleted — the
  recreatability regression test.
- `ErrUnreachable` is never treated as "already deleted".
- Tripwire skips one repo and still processes the rest.
- Dry-run issues no DELETE and no PlayKube.
- Stage B restarts the registry even when GC exits non-zero.

## Rollout (this is #220)

1. P1, P2 (podman upgrade, host registration).
2. Migrate the registry quadlet to a managed instance.
3. Ship the job with the scheduler **disabled**; run dry-run against the live registry;
   diff its classification against `registry-gc.sh --dry-run`. They should agree except
   where this design deliberately protects *more*.
4. Live Stage-A-only run. Verify.
5. Enable Stage B. Verify reclaimed space.
6. Only then disable `registry-gc.timer`, remove the script from otp-infra-1 and from
   `IoTReady/engine`, and note the retirement date in pro's CLAUDE.md (the incident
   history stays as record).

Do not disable the old timer before step 5 passes. It is currently the only thing that
garbage-collects the registry at all.

## Out of scope

- Registry auth beyond the existing `none`/basic support.
- Any UI for *editing* the classification policy; it stays configuration.
- Multi-registry support. One registry, as today.
