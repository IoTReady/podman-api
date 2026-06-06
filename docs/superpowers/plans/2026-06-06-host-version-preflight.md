# Host podman-version preflight (#85) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refuse to operate against managed podman hosts below podman 5.6.0 (the floor for the volume export/import API that migrate needs), with a fail-fast boot preflight and lazy first-connect enforcement.

**Architecture:** A version floor (`checkVersion`) lives in `internal/podman`. Boot calls `(*Real).Preflight`: reachable host below floor → fatal; unreachable → warn + defer. All operation methods go through a new `opCtxFor` that verifies a host's version once per process on first connect; diagnostics (`Ping`/`Version`/`HostInfo`) stay on the raw path so `GET /hosts` can still show an unsupported host's version. The API maps the new sentinel to `host_version_unsupported` / 422.

**Tech Stack:** Go, `github.com/blang/semver/v4` (already in module graph via podman), libpod bindings, testify.

**Spec:** `docs/superpowers/specs/2026-06-06-host-version-preflight-design.md`

**Branch/worktree:** `feat/85-host-version-preflight` under `.worktrees/` (per CLAUDE.md). All `make` commands run from the worktree root. Build/test always via `make` (carries the remote-client build tags).

---

### Task 1: Version floor — `checkVersion`, constant, sentinel

**Files:**
- Create: `internal/podman/version.go`
- Test: `internal/podman/version_test.go`
- Modify: `go.mod` (promote `blang/semver/v4` to direct)

- [ ] **Step 1: Write the failing test**

Create `internal/podman/version_test.go`:

```go
package podman

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckVersion(t *testing.T) {
	cases := []struct {
		name    string
		version string
		wantErr bool
	}{
		{"below floor", "5.4.2", true},
		{"at floor", "5.6.0", false},
		{"above floor", "5.8.2", false},
		{"dev suffix above floor", "5.8.2-dev", false},
		{"v prefix tolerated", "v5.7.0", false},
		{"pre-release of floor is below floor", "5.6.0-rc1", true},
		{"empty", "", true},
		{"garbage", "abc", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkVersion("h1", c.version)
			if c.wantErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrHostVersionUnsupported),
					"must wrap sentinel, got: %v", err)
				assert.Contains(t, err.Error(), `host "h1"`)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/ -run TestCheckVersion -v`
Expected: FAIL — `undefined: checkVersion`, `undefined: ErrHostVersionUnsupported`

- [ ] **Step 3: Write the implementation**

Create `internal/podman/version.go`:

```go
package podman

import (
	"errors"
	"fmt"

	"github.com/blang/semver/v4"
)

// MinPodmanVersion is the floor for managed hosts. Cold-copy migrate streams
// volumes through libpod's GET /volumes/{name}/export and volumes.Import,
// which first shipped in podman 5.6.0 — on older hosts the export 404s
// mid-migration. The preflight turns that into a clear setup error (#85).
const MinPodmanVersion = "5.6.0"

var minPodmanVersion = semver.MustParse(MinPodmanVersion)

// ErrHostVersionUnsupported marks a host whose podman is below
// MinPodmanVersion (or whose version cannot be parsed, which fails closed).
var ErrHostVersionUnsupported = errors.New("host podman version unsupported")

// checkVersion enforces MinPodmanVersion against a host-reported version
// string. Parsing is tolerant ("v" prefixes, partial versions) because podman
// builds report suffixed versions like "5.8.2-dev"; per semver, a pre-release
// of the floor (5.6.0-rc1) sorts below the floor and is rejected.
func checkVersion(hostID, version string) error {
	v, err := semver.ParseTolerant(version)
	if err != nil {
		return fmt.Errorf("host %q: cannot parse podman version %q (floor %s unconfirmed): %w",
			hostID, version, MinPodmanVersion, ErrHostVersionUnsupported)
	}
	if v.LT(minPodmanVersion) {
		return fmt.Errorf("host %q: podman %s < minimum %s (volume export/import requires >= %s): %w",
			hostID, version, MinPodmanVersion, MinPodmanVersion, ErrHostVersionUnsupported)
	}
	return nil
}
```

Promote the dep to direct (it is already pinned indirect at v4.0.0):

```bash
go get github.com/blang/semver/v4@v4.0.0
```

Verify `go.mod` diff is just the `// indirect` marker moving/dropping — nothing else should change.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/ -run TestCheckVersion -v`
Expected: PASS (all subtests)

- [ ] **Step 5: Commit**

```bash
git add internal/podman/version.go internal/podman/version_test.go go.mod go.sum
git commit -m "feat(85): podman version floor checkVersion + ErrHostVersionUnsupported"
```

---

### Task 2: `Real` plumbing — verified set, version probe, `opCtxFor`

**Files:**
- Modify: `internal/podman/real.go` (struct at :45-50, `NewReal` at :54-66, `Version` at :134-144, new funcs after `ctxFor` :123)
- Test: `internal/podman/version_test.go` (append)

- [ ] **Step 1: Write the failing tests**

Append to `internal/podman/version_test.go` (internal package — privates are reachable; this mirrors `real_pure_test.go`'s no-socket style by pre-seeding the ctx cache and stubbing the probe):

```go
import (
	"context"
	// ... add to existing imports

	"github.com/iotready/podman-api/internal/config"
)

// stubReal returns a Real with one host "h1" whose connection is pre-cached
// (no dial) and whose version probe is stubbed.
func stubReal(t *testing.T, probe func(context.Context) (string, error)) *Real {
	t.Helper()
	r, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	require.NoError(t, err)
	r.ctx["h1"] = context.Background()
	r.versionProbe = probe
	return r
}

func TestOpCtxFor_RefusesOldVersion(t *testing.T) {
	r := stubReal(t, func(context.Context) (string, error) { return "5.4.2", nil })
	_, err := r.opCtxFor(context.Background(), "h1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrHostVersionUnsupported))
}

func TestOpCtxFor_VerifiesOncePerHost(t *testing.T) {
	calls := 0
	r := stubReal(t, func(context.Context) (string, error) { calls++; return "5.8.2", nil })
	for i := 0; i < 3; i++ {
		_, err := r.opCtxFor(context.Background(), "h1")
		require.NoError(t, err)
	}
	assert.Equal(t, 1, calls, "version probed once, then cached")
}

func TestOpCtxFor_InPlaceUpgradeRecovers(t *testing.T) {
	calls := 0
	r := stubReal(t, func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "5.4.2", nil // old at first
		}
		return "5.8.2", nil // host upgraded in place
	})
	_, err := r.opCtxFor(context.Background(), "h1")
	require.Error(t, err)
	_, err = r.opCtxFor(context.Background(), "h1")
	require.NoError(t, err, "no daemon restart needed after host upgrade")
}

func TestOpCtxFor_ProbeErrorIsNotVerified(t *testing.T) {
	calls := 0
	r := stubReal(t, func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "", errors.New("transient")
		}
		return "5.8.2", nil
	})
	_, err := r.opCtxFor(context.Background(), "h1")
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrHostVersionUnsupported),
		"transient probe failure is not a version verdict")
	_, err = r.opCtxFor(context.Background(), "h1")
	require.NoError(t, err)
}

func TestVersion_BypassesEnforcement(t *testing.T) {
	r := stubReal(t, func(context.Context) (string, error) { return "5.4.2", nil })
	v, err := r.Version(context.Background(), "h1")
	require.NoError(t, err, "diagnostics stay readable on unsupported hosts")
	assert.Equal(t, "5.4.2", v)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/ -run 'TestOpCtxFor|TestVersion_Bypasses' -v`
Expected: FAIL — `undefined: r.opCtxFor`, `undefined: r.versionProbe`

- [ ] **Step 3: Implement**

In `internal/podman/real.go`:

(a) Extend the struct (`real.go:45-50`):

```go
type Real struct {
	hosts map[string]config.Host

	mu       sync.Mutex
	ctx      map[string]context.Context // hostID -> connection-bearing ctx
	verified map[string]bool            // hostID -> passed the MinPodmanVersion check

	// versionProbe overrides the version lookup in tests; nil means
	// system.Info over the supplied connection ctx.
	versionProbe func(context.Context) (string, error)
}
```

(b) Initialise in `NewReal` (`real.go:55`):

```go
	r := &Real{hosts: map[string]config.Host{}, ctx: map[string]context.Context{}, verified: map[string]bool{}}
```

(c) Add after `ctxFor` (`real.go:123`):

```go
// probeVersion fetches the podman version over an established connection ctx.
func (r *Real) probeVersion(c context.Context) (string, error) {
	if r.versionProbe != nil {
		return r.versionProbe(c)
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return "", err
	}
	return info.Version.Version, nil
}

// ensureVerified enforces MinPodmanVersion once per host per process. On
// failure the host stays unverified (and the connection stays cached), so a
// host whose podman is upgraded in place starts passing without a restart.
// The probe runs outside the mutex; a concurrent duplicate probe is harmless.
func (r *Real) ensureVerified(c context.Context, id string) error {
	r.mu.Lock()
	ok := r.verified[id]
	r.mu.Unlock()
	if ok {
		return nil
	}
	v, err := r.probeVersion(c)
	if err != nil {
		return fmt.Errorf("verify host %q podman version: %w", id, err)
	}
	if err := checkVersion(id, v); err != nil {
		return err
	}
	r.mu.Lock()
	r.verified[id] = true
	r.mu.Unlock()
	return nil
}

// opCtxFor is ctxFor plus the MinPodmanVersion gate. Every operation method
// uses it; diagnostics (Ping, Version, HostInfo) use raw ctxFor so GET /hosts
// can still display an unsupported host's version (#85).
func (r *Real) opCtxFor(parent context.Context, id string) (context.Context, error) {
	c, err := r.ctxFor(parent, id)
	if err != nil {
		return nil, err
	}
	if err := r.ensureVerified(c, id); err != nil {
		return nil, err
	}
	return c, nil
}
```

(d) DRY `Version` (`real.go:134-144`) onto the probe:

```go
func (r *Real) Version(ctx context.Context, id string) (string, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return "", err
	}
	return r.probeVersion(c)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/ -run 'TestOpCtxFor|TestVersion_Bypasses|TestCheckVersion' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/podman/real.go internal/podman/version_test.go
git commit -m "feat(85): verified-gated opCtxFor enforces version floor on first connect"
```

---

### Task 3: Switch operation methods to `opCtxFor`

**Files:**
- Modify: `internal/podman/real.go` (all operation methods)

- [ ] **Step 1: Mechanical switch**

Every operation method currently calls `r.ctxFor(ctx, id)`. Switch them ALL, then revert the three diagnostics. From the worktree root:

```bash
sed -i 's/r\.ctxFor(ctx, id)/r.opCtxFor(ctx, id)/g' internal/podman/real.go
```

Then revert to `r.ctxFor(ctx, id)` (raw path) in exactly these methods — they feed `GET /hosts` diagnostics and must keep working against an unsupported host:
- `Ping` (~real.go:125)
- `Version` (~real.go:134)
- `HostInfo` (~real.go:728)
- `hostLoadAvg` (~real.go:777)

- [ ] **Step 2: Verify the split**

```bash
grep -n 'r\.ctxFor(ctx, id)' internal/podman/real.go
```

Expected: exactly 4 matches, inside `Ping`, `Version`, `HostInfo`, `hostLoadAvg`. Everything else (`PlayKube`, `Pod*`, `Secret*`, `Volume*`, `NetworkEnsure`, `ContainerExec`, `CopyToContainer`, `ContainerLogs`, `ImagePull`, `UsedHostPorts`, `ImagePrune`, `ContainerPrune`, `BuildCachePrune`, `VolumePrune`) must show `r.opCtxFor(ctx, id)`.

Note: integration tests (`real_prune_integration_test.go:73,136`) call `c.ctxFor(...)` directly — leave them as-is; they bypass enforcement deliberately.

- [ ] **Step 3: Build and run the package suite**

Run: `make build && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/ -v -count=1`
Expected: build OK, unit tests PASS (integration tests skip without a podman host).

- [ ] **Step 4: Commit**

```bash
git add internal/podman/real.go
git commit -m "feat(85): route all podman operations through the version-gated opCtxFor"
```

---

### Task 4: Boot preflight — `(*Real).Preflight`

**Files:**
- Modify: `internal/podman/real.go` (add `Preflight` after `opCtxFor`; add `"log"` and ensure `"time"` in imports — `time` is already imported)
- Test: `internal/podman/version_test.go` (append)

- [ ] **Step 1: Write the failing tests**

Append to `internal/podman/version_test.go`:

```go
import (
	"time"
	// ... add to existing imports
)

func TestPreflight_FatalOnReachableOldHost(t *testing.T) {
	r := stubReal(t, func(context.Context) (string, error) { return "5.4.2", nil })
	err := r.Preflight(context.Background())
	require.Error(t, err, "reachable host below floor must refuse boot")
	assert.True(t, errors.Is(err, ErrHostVersionUnsupported))
}

func TestPreflight_MarksVerified_NoReprobe(t *testing.T) {
	calls := 0
	r := stubReal(t, func(context.Context) (string, error) { calls++; return "5.8.2", nil })
	require.NoError(t, r.Preflight(context.Background()))
	_, err := r.opCtxFor(context.Background(), "h1")
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "preflight pass is cached; first op does not re-probe")
}

func TestPreflight_ToleratesUnreachable(t *testing.T) {
	// No pre-seeded ctx: ctxFor dials the (nonexistent) unix socket and fails.
	r, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: "/nonexistent/podman.sock"}})
	require.NoError(t, err)
	require.NoError(t, r.Preflight(context.Background()),
		"unreachable host is a warning, not a boot failure")
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.False(t, r.verified["h1"], "unreachable host stays unverified")
}

func TestPreflight_TimeoutDefers(t *testing.T) {
	old := preflightTimeout
	preflightTimeout = 50 * time.Millisecond
	defer func() { preflightTimeout = old }()

	r := stubReal(t, func(context.Context) (string, error) {
		time.Sleep(500 * time.Millisecond)
		return "5.4.2", nil
	})
	require.NoError(t, r.Preflight(context.Background()),
		"slow host is treated as unreachable at boot")
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.False(t, r.verified["h1"])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/ -run TestPreflight -v`
Expected: FAIL — `undefined: r.Preflight`, `undefined: preflightTimeout`

- [ ] **Step 3: Implement**

Add to `internal/podman/real.go` (after `opCtxFor`); add `"log"` to imports:

```go
// preflightTimeout bounds each host's boot-time connect+version probe.
// var (not const) so tests can shrink it.
var preflightTimeout = 10 * time.Second

// Preflight enforces MinPodmanVersion at boot. A reachable host below the
// floor returns an error (main treats it as fatal — the daemon refuses to
// start). An unreachable or slow host is logged and left unverified; the
// check re-runs on its first successful connect (opCtxFor), so a down-at-boot
// old host still cannot sneak in. See #85.
func (r *Real) Preflight(ctx context.Context) error {
	for id := range r.hosts {
		if err := r.preflightHost(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *Real) preflightHost(ctx context.Context, id string) error {
	type result struct {
		version string
		err     error
	}
	ch := make(chan result, 1)
	// The dial cannot take a deadline: ctxFor deliberately roots cached
	// connections at context.Background() (per-request cancellation must not
	// kill them), so the attempt is bounded externally. On timeout the
	// goroutine is abandoned; if it completes later it merely caches a usable
	// connection for first use.
	go func() {
		c, err := r.ctxFor(ctx, id)
		if err != nil {
			ch <- result{err: err}
			return
		}
		v, err := r.probeVersion(c)
		ch <- result{version: v, err: err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			log.Printf("preflight: host %q unreachable, podman version check deferred to first use: %v", id, res.err)
			return nil
		}
		if err := checkVersion(id, res.version); err != nil {
			return err
		}
		r.mu.Lock()
		r.verified[id] = true
		r.mu.Unlock()
		log.Printf("preflight: host %q podman %s ok (>= %s)", id, res.version, MinPodmanVersion)
		return nil
	case <-time.After(preflightTimeout):
		log.Printf("preflight: host %q did not answer within %s, podman version check deferred to first use", id, preflightTimeout)
		return nil
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/podman/ -run TestPreflight -v -count=1`
Expected: PASS (4 tests; `ToleratesUnreachable` may take a moment dialing the dead socket)

- [ ] **Step 5: Commit**

```bash
git add internal/podman/real.go internal/podman/version_test.go
git commit -m "feat(85): boot preflight — fatal on old reachable hosts, defer unreachable"
```

---

### Task 5: Wire preflight into main

**Files:**
- Modify: `cmd/podman-api/main.go:104-107`

- [ ] **Step 1: Wire it**

After `podman.NewReal(hosts)` (`main.go:104-107`), add:

```go
	client, err := podman.NewReal(hosts)
	if err != nil {
		log.Fatalf("podman: %v", err)
	}
	// Refuse to boot against a reachable host below the podman version floor;
	// unreachable hosts are warned and re-checked on first use (#85).
	if err := client.Preflight(context.Background()); err != nil {
		log.Fatalf("podman: %v", err)
	}
```

(`context` is already imported in main.go.)

- [ ] **Step 2: Build**

Run: `make build`
Expected: builds clean to `bin/podman-api`.

- [ ] **Step 3: Commit**

```bash
git add cmd/podman-api/main.go
git commit -m "feat(85): run host version preflight at boot"
```

---

### Task 6: API error mapping + fake version knob

**Files:**
- Modify: `internal/api/errors.go:66-67` (new classify arm)
- Modify: `internal/api/coverage_test.go:198-215` (extend table)
- Modify: `internal/podman/fake/fake.go:498` (configurable version)

- [ ] **Step 1: Write the failing test**

In `internal/api/coverage_test.go`, extend the `TestClassify_RemainingSentinels` table (line ~204) with:

```go
		{podman.ErrHostVersionUnsupported, "host_version_unsupported", http.StatusUnprocessableEntity},
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestClassify_RemainingSentinels -v`
Expected: FAIL — classified as `internal` / 500, not `host_version_unsupported` / 422.

- [ ] **Step 3: Implement**

In `internal/api/errors.go`, add an arm to `classify()` directly after the `podman.ErrNotFound` case (line 66-67):

```go
	case errors.Is(err, podman.ErrHostVersionUnsupported):
		return "host_version_unsupported", http.StatusUnprocessableEntity, err.Error()
```

In `internal/podman/fake/fake.go`: add a field to the hooks section of the `Fake` struct (near `Unknown`, ~line 60):

```go
	// VersionStr overrides the version Version reports; empty means "fake-1.0".
	VersionStr string
```

and replace the `Version` method (line 498):

```go
func (f *Fake) Version(_ context.Context, _ string) (string, error) {
	if f.VersionStr != "" {
		return f.VersionStr, nil
	}
	return "fake-1.0", nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ ./internal/podman/... -count=1`
Expected: PASS (fake default unchanged, so no existing test moves).

- [ ] **Step 5: Commit**

```bash
git add internal/api/errors.go internal/api/coverage_test.go internal/podman/fake/fake.go
git commit -m "feat(85): classify host_version_unsupported as 422; fake version knob"
```

---

### Task 7: Full verification + PR

**Files:** none (verification only)

- [ ] **Step 1: Full gate**

```bash
gofmt -l .          # must print nothing
go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...
make build
make test
```

Expected: gofmt empty, vet clean, build OK, full unit suite PASS.

- [ ] **Step 2: Push branch and open PR (Forgejo, not gh)**

```bash
git push -u origin feat/85-host-version-preflight
forgejo pr create tej/podman-api --title="feat: host podman-version preflight (floor 5.6.0) (#85)" --head=feat/85-host-version-preflight --base=main --body="Implements the provisioning preflight from #85: boot fails fast on reachable hosts below podman 5.6.0 (volume export/import floor); unreachable hosts are warned and version-gated on first connect via opCtxFor. Diagnostics (Ping/Version/HostInfo) stay exempt so GET /hosts shows the offending version. API maps ErrHostVersionUnsupported -> host_version_unsupported / 422.

Spec: docs/superpowers/specs/2026-06-06-host-version-preflight-design.md"
```

---

### Task 8: Wiki — "Supported host OS" section (publish directly; no PR flow)

**Files:**
- Wiki repo: `ssh://git@git.iotready.com:2222/tej/podman-api.wiki.git`, page `Provisioning-a-Podman-Host.md`

Do this AFTER the code PR merges (the section documents shipped behaviour).

- [ ] **Step 1: Clone the wiki and inspect the page**

```bash
git clone ssh://git@git.iotready.com:2222/tej/podman-api.wiki.git /tmp/podman-api.wiki
ls /tmp/podman-api.wiki   # confirm the provisioning page's exact filename
```

- [ ] **Step 2: Add the section**

Insert near the top of the provisioning page (after any intro, before the step-by-step instructions), adapting heading level to the page's existing style:

```markdown
## Supported host OS

Managed hosts must run **podman ≥ 5.6.0**: cold-copy migrate/evacuate streams
volumes through the libpod `volume export`/`import` API, which first shipped in
podman 5.6.0 ([#85](https://git.iotready.com/tej/podman-api/issues/85)). The
daemon enforces this — it **refuses to boot** against a reachable host below the
floor, and hosts that were unreachable at boot are version-checked on first use
(operations fail with `host_version_unsupported` instead of a confusing 404).

| Distro | podman (stock repos) | Supported? |
|---|---|---|
| **AlmaLinux 10** (recommended) | 5.6.0 → 5.8.2 (AppStream) | ✅ |
| **Rocky Linux 10** | 5.6.0 → 5.8.2 (AppStream) | ✅ |
| Fedora 41/42 | 5.6.2 / 5.8.2 | ✅ |
| Ubuntu 24.04 LTS | 4.9.3 | ❌ |
| Debian 13 (trixie) | 5.4.2 | ❌ |
| Rocky Linux 9 / RHEL 9.x | ~5.4 | ❌ |

**Recommendation: AlmaLinux 10.** Both RHEL-10 rebuilds ship podman 5.8.2 (exact
parity with the daemon's pinned bindings); AlmaLinux wins on independent security
cadence, foundation governance, and broader hardware support. Rocky 10 is a fine
second choice if exact 1:1 RHEL parity is ever required.

**Escape hatch (discouraged):** Debian/Ubuntu can reach the floor via the
upstream openSUSE OBS podman repo, but that adds per-host third-party repo
maintenance we don't want on a managed fleet. Prefer reinstalling on AlmaLinux 10.
```

- [ ] **Step 3: Publish**

```bash
cd /tmp/podman-api.wiki
git add -A && git commit -m "Provisioning: add Supported host OS section (podman >= 5.6.0 floor, #85)"
git push
```

- [ ] **Step 4: Close out issue #85**

The PR merge + wiki section complete the issue's remaining checkboxes:

```bash
forgejo issue close tej/podman-api 85
```

(Or comment first summarising the preflight + wiki change, then close.)
