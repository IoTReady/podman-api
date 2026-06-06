# #114 — Atomic load+apply via `applyLocked`

**Issue:** tej/podman-api#114 (follow-up from #92 / PR #112 review).

**Goal:** Close the read-modify-write race in `RotateInstanceSecrets` and
`UpgradeImage`: today they `GetSpec` *outside* `Apply`'s per-instance lock, so two
concurrent re-applies of the same instance can interleave and silently drop one
update (a lost rotation is worse than a stale image).

## Problem

```
spec := GetSpec(...)            // outside the lock
merged := overlay(spec.Secrets) // or params, for UpgradeImage
Apply(..., Replace=true)        // takes s.instanceLock(host,tmpl,slug) internally
```

B can read the spec before A's `Apply` commits, then re-apply carrying A's stale
value → A's change is undone. `Apply`'s `instanceLock` is a non-reentrant
`sync.Mutex`, so naively wrapping `GetSpec`→`Apply` in the same lock deadlocks.

## Design

Extract a lock-free core and share it.

### `applyLocked`

`applyLocked(ctx, host, req, opts) error` = the **entire current `Apply` body minus
lock acquisition** (template lookup, `ApplyDefaults`, validate, secrets-need-key
check, `validateIngress`, per-host-secret precheck, drain/exists checks, render,
pull, secret push, `PlayKube`, `PutSpec` under `tmplMu.RLock`, ingress reconcile).
It assumes the caller holds the `instanceLock` (and the `hostLock` when the request
carries domains).

### `Apply` (public; contract unchanged)

```go
func (s *Service) Apply(ctx, host, req, opts) error {
    if len(req.Domains) > 0 {
        hl := s.hostLock(host); hl.Lock(); defer hl.Unlock()
    }
    lock := s.instanceLock(host, req.Template, req.Slug)
    lock.Lock(); defer lock.Unlock()
    return s.applyLocked(ctx, host, req, opts)
}
```

Moving the pre-lock validation into `applyLocked` (now under the lock) is benign —
those steps are fast and side-effect-free.

### `RotateInstanceSecrets` / `UpgradeImage`

```go
lock := s.instanceLock(host, tmpl, slug)
lock.Lock(); defer lock.Unlock()
spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
// ... ErrNotFound → ErrInstanceNotFound; else wrap ...
merged := overlay(spec)               // secrets overlay / params image override
return s.applyLocked(ctx, host, ApplyRequest{...}, ApplyOptions{Replace:true, AllowMissingSecrets:true})
```

Load+apply is now atomic under one acquisition: a second rotation blocks on the
`instanceLock` until the first commits (`PutSpec`) and releases, then reads the
committed value before overlaying its own. No lost update. The empty-`newSecrets`
guard in `RotateInstanceSecrets` and the empty-`image` guard in `UpgradeImage` stay
*before* the lock (cheap rejects, nothing to serialize).

### Why no `hostLock` in rotate/upgrade (and why that is correct)

`hostLock` serializes the host-wide domain-uniqueness check + claim across
*different* instances (#82). Rotate/upgrade re-apply the instance's **own** domains;
`validateIngress` already skips the instance's own spec row, so they can never create
a new cross-instance claim. A concurrent deploy of another instance for the same
domain still observes the present spec row and is rejected. Rotate/upgrade therefore
acquire a strict **subset** of `Apply`'s locks (instanceLock only) → no new deadlock
cycle, no lock-order inversion (Apply always takes hostLock *before* instanceLock; a
holder of only instanceLock never reaches for hostLock).

## Tests (`internal/instance/service_test.go`)

A deterministic atomicity test using a gated podman-client decorator that blocks
`PlayKube` (after an initial deploy) and signals when reached:

```go
type gatedClient struct {
    podman.Client
    armed   atomic.Bool
    reached chan struct{} // buffered (cap 2): each gated PlayKube entry signals once
    release chan struct{} // closed by the test to let gated PlayKube proceed
}
func (g *gatedClient) PlayKube(ctx context.Context, host, yaml string, replace bool, networks ...string) error {
    if g.armed.Load() {
        g.reached <- struct{}{}
        <-g.release
    }
    return g.Client.PlayKube(ctx, host, yaml, replace, networks...)
}
```

Flow (`twoSecretTemplate`, instance deployed with `password`/`token`):

1. Deploy with the gate disarmed.
2. `g.armed.Store(true)`.
3. Goroutine A: `RotateInstanceSecrets(password=A)`. A reads the spec, enters the
   gated `PlayKube` (holding the instanceLock), signals `reached`, blocks on `release`.
4. `<-g.reached` (A is parked).
5. Goroutine B: `RotateInstanceSecrets(token=B)`. On the fixed code B blocks on the
   instanceLock (no `GetSpec` yet); on the old code B reads the *pre-commit* spec and
   reaches its own gated `PlayKube`.
6. `select { case <-g.reached: case <-time.After(~300ms): }` — old code: B's second
   `reached` arrives (B has already read the stale spec → lost update is now baked in);
   fixed code: times out (B blocked on the lock, has not read).
7. `close(g.release)`; wait for both goroutines.
8. Assert the final stored spec carries **both** `password=A` **and** `token=B`.

On the pre-fix code B read the spec before A committed, so whichever rotation commits
last carries the other's *original* value → the final spec is missing one update
(deterministic lost update; the assertion fails). On the fixed code B cannot read
until A commits, so it overlays onto A's committed value and both survive. Run under
`-race`.

Keep the existing `RotateInstanceSecrets` / `UpgradeImage` behavioral tests green
(overlay/reapply, empty rejected, not-found, corrupt propagates, AllowMissingSecrets).
Add a sibling deadlock-guard assertion is unnecessary — the suite running to
completion is the deadlock guard.

## Out of scope

- The `Apply`-takes-an-overlay-closure alternative (rejected: less direct than
  extracting the shared core).
- #113 — separate PR (#115).
