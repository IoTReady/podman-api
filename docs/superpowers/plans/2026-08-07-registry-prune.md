# Registry prune (#219) implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** A first-party, in-use-aware registry garbage-collection job in podman-api — manifest deletion plus blob reclamation — replacing `registry-gc.sh`.

**Architecture:** Mirrors `internal/prune` end to end: ticker scheduler → policy → job payload → `jobs.Handler` → `jc.Step` progress rows → generic job UI + Prometheus metrics. New package `internal/registryprune`.

**Spec:** `docs/superpowers/specs/2026-08-07-registry-prune-design.md` — read it before Task 1.

**Tech stack:** Go, stdlib + existing deps only. No new module dependencies.

## Global Constraints

- **Build/test with `make`, never bare `go build ./...`.** The required tags are
  `containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper`;
  without them the build dies on `btrfs/version.h`. Use `make build`, `make test`, `make vet`.
  `make vet` (gofmt check + `go vet`) must be clean before any commit.
- **`main` is PR-only.** Work happens on `feat/219-registry-prune` in the worktree at
  `.worktrees/219-registry-prune`. Never commit to `main`.
- **This is the OSS repo.** No commercial logic. Nothing may import `podman-api-pro`.
- **Fail closed, always.** Every ambiguity in the in-use computation resolves toward *not*
  deleting. `ErrUnreachable` must never be treated as "already gone".
- **Delete by digest, never by tag.** Registry v2 `DELETE /v2/<repo>/manifests/<digest>`.
- **No new module deps.** Test with `httptest`, hand-written fakes, `store.Memory`,
  `internal/podman/fake` — the conventions already in this repo.
- Tests must be verified RED before the implementation lands (TDD). A test that passes
  against unwritten code is not evidence.

---

### Task 1: `imgregistry.Delete`

**Files:**
- Modify: `internal/imgregistry/client.go` (interface + `HTTPClient` impl)
- Modify: `internal/imgregistry/cache.go` (pass-through + invalidation)
- Test: `internal/imgregistry/client_test.go`, `internal/imgregistry/cache_test.go`

**Interfaces — Produces:**
```go
// on Client
Delete(ctx context.Context, repo, digest string) error
```

**Requirements:**
- Issues `DELETE /v2/<repo>/manifests/<digest>`. Success is HTTP 202 (per the v2 spec)
  — accept 2xx.
- Validates `repo` with `ValidRepoName` and `digest` with `ValidRef` **and** additionally
  rejects anything that is not digest-form (`sha256:…`). A tag must never reach the wire:
  deleting by tag deletes the manifest for every tag sharing that digest.
- 404 → `ErrNotFound`. Anything else non-2xx, or a transport error → `ErrUnreachable`.
- `CachingClient.Delete` delegates to the inner client and, on success, **invalidates the
  cached `Tags()` entry for that repo**. A stale listing after a delete would make a
  subsequent run reason from a tag that no longer exists.

**Tests (all RED first):**
1. Success path: asserts method `DELETE`, exact path, 202 handled as success.
2. 404 → `errors.Is(err, ErrNotFound)`.
3. 500 → `errors.Is(err, ErrUnreachable)`; explicitly assert it is NOT `ErrNotFound`.
4. Tag-form ref (`v1.2.3`) is rejected **without any HTTP request being made** (assert
   request count == 0).
5. Path-traversal ref (`../../foo`) rejected, zero requests.
6. Cache invalidation: populate `Tags()`, `Delete()`, assert the next `Tags()` re-fetches.

**Commit:** `feat(imgregistry): add Delete for registry v2 manifests`

---

### Task 2: container exit-code observability

**Files:**
- Modify: `internal/podman/types.go` (`Container` struct)
- Modify: `internal/podman/real.go` (`enrichContainer`, new wait helper)
- Modify: `internal/podman/client.go` (interface), `internal/podman/fake/fake.go`
- Test: `internal/podman/` unit tests

**Interfaces — Produces:**
```go
// on podman.Container
ExitCode int
Exited   bool

// on podman.Client
WaitForPodCompletion(ctx context.Context, hostID, podName string, timeout time.Duration) (exitCode int, err error)
```

**Why this exists:** `PlayKube` returns as soon as the pod *starts*. Without an exit code, a
GC job reports success for a GC that failed. Do not skip it.

**Requirements:**
- `enrichContainer` populates `ExitCode`/`Exited` from `ins.State.ExitCode` /
  `ins.State.Running` (libpod already provides both; today they are dropped).
- `WaitForPodCompletion` polls container state until every container has exited or the
  timeout elapses. Returns the **first non-zero** exit code if any, else 0.
- Timeout elapsing is an error, not a zero exit code.
- Add the method to the `fake` implementation so downstream tests can drive it.

**Tests:** exited container reports its code; running container is `Exited: false`;
wait returns first non-zero; wait times out as an error, distinguishable from success.

**Commit:** `feat(podman): expose container exit codes and pod completion wait`

---

### Task 3: in-use digest set

**Files:**
- Create: `internal/registryprune/inuse.go`, `internal/registryprune/inuse_test.go`

**Interfaces — Consumes:** `imgregistry.Client` (Task 1). **Produces:**
```go
type InUseSet struct{ Digests map[string]struct{} }  // digest -> present, across all repos

type InUseSource interface {
    ListAllInstancesWithMeta(ctx context.Context, host string) (/* existing return */)
}

func BuildInUseSet(ctx context.Context, ...) (InUseSet, error)
var ErrUnsafeToPrune = errors.New("unsafe to prune")
```

**This is the safety-critical component.** Union of two sources:

**(a) Observed** — `Observed.Containers[].Image` from the warm inventory cache. Already a
resolved digest from a live `podman inspect`.

**(b) Desired** — every image-bearing value in each stored `Spec.Parameters` (`image`,
`pg_image`, and any key matching `*_image`), resolved tag→digest through the registry.
**This half does not exist in `registry-gc.sh` and is the recreatability fix.**

Protection is keyed by digest **across all repos** — blobs are shared; this is the
conservative direction.

**Fail-closed rules — every one returns `ErrUnsafeToPrune` and aborts the run:**

| Condition | Behaviour |
|---|---|
| Any host `Freshness.Reachable == false` | abort |
| Any host snapshot older than `maxSnapshotAge` | abort |
| Fleet-wide in-use digest count is zero | abort |
| A spec image ref cannot be resolved to a digest | abort — never "skip it" |
| Registry returns `ErrUnreachable` at any point | abort |

**Tests (each asserting the abort produces ZERO delete calls):** one per table row, plus:
a digest present only via a stored spec (nothing running) is in the set — the
recreatability regression test, and the single most important test in this plan;
digest-pinned refs are used as-is without a registry round-trip; a repo with no live
instances does not by itself trip the zero-count rule (only fleet-wide zero does).

**Commit:** `feat(registryprune): build fail-closed in-use digest set`

---

### Task 4: classification policy

**Files:**
- Create: `internal/registryprune/policy.go`, `internal/registryprune/policy_test.go`

**Interfaces — Produces:**
```go
type Class string // "protected" | "calver" | "repo-protected" | "shares-protected" |
                  // "in-use" | "feat-recent" | "sha-is-latest" | "unclassified" |
                  // "sha-orphan" | "feat-stale"

type Policy struct {
    ProtectedExact   []string
    CalVer           *regexp.Regexp
    ExtraPerRepo     map[string]*regexp.Regexp
    RetentionDays    int
    MaxDeletesPerRepo int
    DeleteUnrecognised bool
}
func DefaultPolicy() Policy
func Classify(repo string, tg imgregistry.TagGroup, inUse InUseSet, protectedDigests map[string]struct{}, p Policy, now time.Time) Class
func Deletable(c Class, p Policy) bool
```

**Defaults ported verbatim from `registry-gc.sh`:**
- Exact: `latest`, `e2e`, `dev`, `main`, `deps-cache`, `scanners-cache`
- CalVer: `^[0-9]{4}\.[0-9]{2}\.[0-9]{2}$`
- Per-repo extras: `otp` → `^(runtime-base|valvo-fork-v1)`
- Retention: 30 days, aged from the config blob's `.created`
- `MaxDeletesPerRepo`: 100

**Rules:**
- Deletable classes are exactly `sha-orphan` and `feat-stale`, plus `unclassified` **only**
  when `DeleteUnrecognised` is true.
- `unclassified` is kept by default. This is the #66 fix; do not "simplify" it away.
- Any digest shared with a protected tag is `shares-protected`.
- `in-use` is checked before every delete rule, so a dry-run never misreports why a tag
  survived.
- Age-unknown fails toward deletion for `feat-*` only (matching the script). Document it.

**Tests:** table-driven, one case per class; the `otp` per-repo extra; a bare-hex tag whose
digest equals `:latest` is `sha-is-latest`, not `sha-orphan`; `unclassified` is not
deletable unless opted in.

**Commit:** `feat(registryprune): port classification policy`

---

### Task 5: job handler + scheduler (Stage A)

**Files:**
- Create: `internal/registryprune/handler.go`, `scheduler.go`, and tests
- Test: `internal/registryprune/handler_test.go`, `scheduler_test.go`

**Interfaces — Consumes:** Tasks 1, 3, 4. **Produces:** job kind `"registry-prune"`.

**Requirements:**
- `Handler.Run(ctx, job, jc)` unmarshals `Payload{Policy, DryRun, SkipBlobGC}`.
- Order, non-negotiable: build in-use set → classify **every** repo → tripwire check →
  only then delete. Classification of all repos before any deletion is the #64 fix.
- **Tripwire divergence from the script:** a repo exceeding `MaxDeletesPerRepo` is
  **skipped and recorded** (job step + log + metric); every other repo proceeds. The
  script's whole-run abort is what disabled GC for all 28 repos for a week.
- Dedup deletes per repo+digest.
- **Backstop re-check** of the in-use and protected sets immediately before each DELETE.
- Dry-run: full classification, `jc.Step` output, **zero** DELETE and zero PlayKube.
- Scheduler mirrors `prune.Scheduler`: ticker, immediate first pass, in-flight dedup by
  scanning recent jobs, failure backoff, `Now func() time.Time` test seam.

**Tests:** dry-run issues no deletes; tripwire skips one repo and the others still process;
in-use digest is never deleted even if classified deletable (backstop); `ErrUnsafeToPrune`
from Task 3 aborts with zero deletes; scheduler dedups an in-flight run.

**Commit:** `feat(registryprune): add prune job handler and scheduler`

---

### Task 6: Stage B — blob GC

**Files:**
- Create: `internal/registryprune/blobgc.go`, `blobgc_test.go`

**Interfaces — Consumes:** Task 2 (`WaitForPodCompletion`).

**Requirements:**
- Build a pod manifest: image `registry:2`, `restartPolicy: Never`, `hostPath` volume
  mounting the registry storage dir at `/var/lib/registry`, command
  `garbage-collect --delete-untagged /etc/docker/registry/config.yml`.
- Sequence: stop the registry instance → `PlayKube` the one-shot pod → await completion →
  read exit code → **restart the registry instance in a deferred path so it comes back even
  if GC fails or panics**.
- Non-zero GC exit is a job failure, but the registry must still be running afterwards.
- Record before/after storage size as job steps.
- Gated by `SkipBlobGC` (the `--no-gc` equivalent) and skipped entirely on dry-run.

**Tests:** registry is restarted when GC exits non-zero; registry is restarted when the wait
times out; `SkipBlobGC` plays no pod; dry-run plays no pod; the generated manifest contains
`restartPolicy: Never` and the hostPath mount (assert on the YAML).

**Commit:** `feat(registryprune): add blob garbage-collection stage`

---

### Task 7: wiring, metrics, surfacing

**Files:**
- Modify: `server/server.go` (flags, job registry, scheduler start)
- Modify: `internal/obs/` (metrics)
- Test: existing server wiring tests

**Requirements:**
- Flags, all defaulting to **off**: `-registry-prune-enabled` (default false),
  `-registry-prune-interval`, `-registry-prune-dry-run`, `-registry-prune-storage-path`,
  `-registry-prune-max-deletes-per-repo`.
- Feature entirely absent when the registry client is nil or the flag is false — same
  pattern as the registry browser: routes 404, scheduler never starts.
- Register `"registry-prune"` in `buildJobRegistry`.
- Prometheus: run outcome, manifests deleted, bytes reclaimed, repos skipped by tripwire.
- No reconciler — a failed run is simply retried by the scheduler, as with `prune`.

**Tests:** disabled by default; nil registry client means no scheduler; job kind registered.

**Commit:** `feat(server): wire registry prune scheduler and metrics`

---

## Not in this plan (deliberately)

- **P1/P2 infrastructure** — the podman 4.9.3→≥5.6.0 upgrade on otp-infra-1 and host
  registration. Production changes, done with the operator, not on a feature branch.
- **Migrating the registry quadlet to a managed instance** — a production migration,
  sequenced during rollout.
- **Retiring `registry-gc.sh`** — that is #220, gated on this shipping and a verified live
  run. Do not touch the timer.
