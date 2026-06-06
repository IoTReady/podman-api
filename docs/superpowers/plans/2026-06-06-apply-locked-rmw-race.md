# #114 Atomic load+apply via `applyLocked` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `RotateInstanceSecrets` and `UpgradeImage` load+apply atomically under
the per-instance lock by extracting a lock-free `applyLocked` core, closing the
read-modify-write race that can silently drop a concurrent rotation.

**Architecture:** `Apply` keeps its public contract but becomes a thin wrapper that
acquires the hostLock (domains only) + instanceLock and delegates to a new private
`applyLocked` (the current `Apply` body minus lock acquisition). Rotate/upgrade take
the instanceLock themselves, then `GetSpec`→merge→`applyLocked` — so the read and the
re-apply happen under one acquisition. Rotate/upgrade take NO hostLock (they re-apply
their own domains and cannot create a new cross-instance claim), keeping the lock set
a subset of `Apply`'s → no deadlock.

**Tech Stack:** Go 1.22, `sync.Mutex`. Build/test with the remote-client tags
(`make test`, or `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs
exclude_graphdriver_devicemapper" ./internal/instance/ -run <Pat> -v`). Bare
`go test ./...` fails (CGO graphdrivers).

All work is in `internal/instance/service.go` and `internal/instance/service_test.go`.

---

### Task 1: Extract `applyLocked`; make `Apply` a thin locking wrapper

Pure refactor — NO behavior change. The existing instance-package suite is the
regression guard (it must stay 100% green).

**Files:**
- Modify: `internal/instance/service.go` (`Apply`)

- [ ] **Step 1: Confirm the baseline is green**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ 2>&1 | tail -3`
Expected: `ok`.

- [ ] **Step 2: Rename the method and strip lock acquisition**

In `internal/instance/service.go`:

1. Rename `func (s *Service) Apply(ctx context.Context, host string, req ApplyRequest, opts ApplyOptions) error` to `func (s *Service) applyLocked(...)` with the same parameters.

2. DELETE the two lock-acquisition blocks from the renamed `applyLocked` body — the hostLock block:
```go
	if len(req.Domains) > 0 {
		hl := s.hostLock(host)
		hl.Lock()
		defer hl.Unlock()
	}
```
and the instanceLock block:
```go
	lock := s.instanceLock(host, req.Template, req.Slug)
	lock.Lock()
	defer lock.Unlock()
```
Leave everything else in the body byte-for-byte unchanged (validate, secrets-need-key check, validateIngress, per-host-secret precheck, drain/exists, render, pull, secret push, PlayKube, PutSpec under tmplMu.RLock, ingress reconcile). Note the leading template lookup / `ApplyDefaults` / `validate` / secrets-need-key check now run under the caller's lock — that is intended and benign (fast, side-effect-free).

3. Give `applyLocked` a doc comment stating its precondition:
```go
// applyLocked is the lock-free core of Apply: it performs the full create/replace
// (validate → play → persist → ingress) assuming the caller already holds the
// per-instance lock (and, for domain-carrying requests, the per-host lock). It
// exists so the read-modify-write callers (RotateInstanceSecrets, UpgradeImage)
// can hold one lock across GetSpec + re-apply without re-entering Apply's lock
// (which is non-reentrant). Apply is the public, lock-acquiring entry point.
```

- [ ] **Step 3: Add the thin `Apply` wrapper**

Immediately above `applyLocked`, add the public `Apply` (keep its original doc
comment — move it here):

```go
// Apply creates or replaces an instance. If opts.Replace is false and the pod
// exists, returns ErrInstanceExists. Unless opts.SkipPull is set, every container
// image referenced in the rendered Pod spec is pulled before the manifest is
// played. Apply acquires the per-host lock (domain-carrying requests only, taken
// before the instance lock — a consistent order so the two never deadlock) and the
// per-instance lock, then runs applyLocked.
func (s *Service) Apply(ctx context.Context, host string, req ApplyRequest, opts ApplyOptions) error {
	if len(req.Domains) > 0 {
		hl := s.hostLock(host)
		hl.Lock()
		defer hl.Unlock()
	}
	lock := s.instanceLock(host, req.Template, req.Slug)
	lock.Lock()
	defer lock.Unlock()
	return s.applyLocked(ctx, host, req, opts)
}
```

(Avoid duplicating the long lock-rationale comments — `applyLocked`'s body already
carries the domain-claim/ordering commentary near `validateIngress`/`PutSpec`; the
short note above is enough.)

- [ ] **Step 4: Build + run the full instance suite (regression guard)**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -count=1 2>&1 | tail -3`
Expected: `ok` — every existing test (Apply, migrate, evacuate, reconcile, rotate, upgrade) still passes. A failure means the refactor changed behavior; fix it before committing.

- [ ] **Step 5: gofmt + commit**

```bash
gofmt -w internal/instance/service.go
cd /home/tej/projects/podman-api/.worktrees/feat-114-applylocked && git add internal/instance/service.go && git commit -m "instance: extract lock-free applyLocked from Apply (#114)"
```

---

### Task 2: Make rotate/upgrade lock across GetSpec+apply; add the atomicity test

**Files:**
- Modify: `internal/instance/service.go` (`RotateInstanceSecrets`, `UpgradeImage`)
- Test: `internal/instance/service_test.go`

- [ ] **Step 1: Write the failing atomicity test**

In `internal/instance/service_test.go`, add the gated client + test. Add `"sync/atomic"` and `"time"` to the import block.

```go
// gatedClient wraps a podman.Client and, once armed, blocks every PlayKube until
// the test closes release — signalling each entry on reached (buffered so a second
// concurrent entry never blocks). It lets the test interleave two rotations of one
// instance deterministically.
type gatedClient struct {
	podman.Client
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (g *gatedClient) PlayKube(ctx context.Context, host, yaml string, replace bool, networks ...string) error {
	if g.armed.Load() {
		g.reached <- struct{}{}
		<-g.release
	}
	return g.Client.PlayKube(ctx, host, yaml, replace, networks...)
}

// TestRotateInstanceSecrets_ConcurrentRotationsDoNotLoseUpdates proves load+apply
// is atomic under one lock: two concurrent rotations of *different* secrets on the
// same instance must both survive. On the pre-fix code (GetSpec outside Apply's
// lock) the second rotation reads the spec before the first commits and re-applies
// a stale value, dropping one update.
func TestRotateInstanceSecrets_ConcurrentRotationsDoNotLoseUpdates(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	g := &gatedClient{Client: fake.New(), reached: make(chan struct{}, 2), release: make(chan struct{})}
	svc, mem := newSvcWith(t, g, hosts, twoSecretTemplate())
	ctx := context.Background()

	// Deploy with the gate disarmed.
	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template:   "twosec",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "img:1"},
		Secrets:    map[string]string{"password": "p", "token": "t"},
	}, ApplyOptions{Replace: true}))

	g.armed.Store(true)

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	// A rotates password, parks in PlayKube holding the instance lock.
	go func() { errA <- svc.RotateInstanceSecrets(ctx, "h1", "twosec", "demo", map[string]string{"password": "A"}) }()
	<-g.reached
	// B rotates token. Fixed code: B blocks on the instance lock (no GetSpec yet).
	// Old code: B reads the pre-commit spec and reaches its own gated PlayKube.
	go func() { errB <- svc.RotateInstanceSecrets(ctx, "h1", "twosec", "demo", map[string]string{"token": "B"}) }()
	select {
	case <-g.reached: // old code: B already read the stale spec
	case <-time.After(300 * time.Millisecond): // fixed code: B is blocked on the lock
	}
	close(g.release)
	require.NoError(t, <-errA)
	require.NoError(t, <-errB)

	got, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	assert.Equal(t, "A", got.Secrets["password"], "password rotation lost (RMW race)")
	assert.Equal(t, "B", got.Secrets["token"], "token rotation lost (RMW race)")
}
```

- [ ] **Step 2: Run it to verify it fails on current rotate**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestRotateInstanceSecrets_ConcurrentRotationsDoNotLoseUpdates -race -v`
Expected: FAIL — the final spec is missing one of `password=A` / `token=B` (RotateInstanceSecrets still does `GetSpec` outside the lock, via `Apply`).

- [ ] **Step 3: Lock across GetSpec+apply in `RotateInstanceSecrets`**

In `internal/instance/service.go` `RotateInstanceSecrets`, after the empty-`newSecrets`
guard, acquire the instance lock, then read + merge + `applyLocked` (replacing the
final `s.Apply(...)` call):

```go
	if len(newSecrets) == 0 {
		return errors.New("no secrets to rotate")
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()
	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return fmt.Errorf("load spec: %w", err)
	}
	merged := maps.Clone(spec.Secrets)
	if merged == nil {
		merged = map[string]string{}
	}
	for k, v := range newSecrets {
		merged[k] = v
	}
	return s.applyLocked(ctx, host, ApplyRequest{
		Template:   tmpl,
		Slug:       slug,
		Parameters: spec.Parameters,
		Secrets:    merged,
		Domains:    spec.Domains,
	}, ApplyOptions{Replace: true, AllowMissingSecrets: true})
```

Update the method doc comment to note that the load and re-apply happen atomically
under the per-instance lock (no read-modify-write race with a concurrent
rotation/upgrade of the same instance).

- [ ] **Step 4: Lock across GetSpec+apply in `UpgradeImage`**

In `UpgradeImage`, after the empty-`image` guard, do the same shape (instance lock →
GetSpec → params clone+image → `applyLocked`):

```go
	if image == "" {
		return errors.New("upgrade requires an image")
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()
	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return fmt.Errorf("load spec: %w", err)
	}
	params := maps.Clone(spec.Parameters)
	if params == nil {
		params = map[string]any{}
	}
	params["image"] = image
	return s.applyLocked(ctx, host, ApplyRequest{
		Template:   tmpl,
		Slug:       slug,
		Parameters: params,
		Secrets:    spec.Secrets,
		Domains:    spec.Domains,
	}, ApplyOptions{Replace: true, AllowMissingSecrets: true})
```

Update its doc comment with the same atomicity note.

- [ ] **Step 5: Run the atomicity test (now PASS) + the full instance suite**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -race -count=1 2>&1 | tail -4`
Expected: `ok` — the atomicity test passes (both updates survive), and every existing
rotate/upgrade/apply/migrate test still passes (the suite completing also proves the
new lock ordering does not deadlock).

- [ ] **Step 6: gofmt + commit**

```bash
gofmt -w internal/instance/service.go internal/instance/service_test.go
cd /home/tej/projects/podman-api/.worktrees/feat-114-applylocked && git add internal/instance/ && git commit -m "instance: rotate/upgrade load+apply atomically under the instance lock (#114)"
```

---

### Final verification (after all tasks)

- [ ] `gofmt -l internal/` is empty (excluding `.html`).
- [ ] `go vet` (with tags) clean on `./internal/instance/`.
- [ ] `make build` succeeds.
- [ ] `make test` passes (full unit suite); also run the instance package with `-race`.
