# Migrate Safety-Before-Reap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a migrate refuse to reap the source until the destination is verifiably good — gate the commit on container *health* (not just liveness) and on a *content-manifest* match between the copied volume and the source.

**Architecture:** Add a `Health` field to the podman container model (free — the inspect data is already fetched), make `waitRunning` wait for declared healthchecks to report `healthy`, and add a tar-content-manifest comparison that re-exports source and dest after the copy and aborts (rolling back) on any difference. Both gates sit in the existing migrate verify path, so evacuate inherits them. Two flags tune timeout and toggle volume verification.

**Tech Stack:** Go, `archive/tar`, `crypto/sha256`, `modernc.org/sqlite` (unaffected), podman v5 bindings.

**Working directory:** all paths are relative to the worktree `/home/tej/projects/podman-api/.worktrees/54-safety-before-reap`. Build/test ONLY with the remote-client tags via the Makefile: `make test`. A raw `go test ./...` fails on this machine (CGO drivers).

**Spec:** `docs/superpowers/specs/2026-06-04-migrate-safety-before-reap-design.md`

---

## File Structure

- `internal/podman/types.go` — add `Container.Health`.
- `internal/podman/real.go` — populate `Health` in `enrichContainer`.
- `internal/podman/real_pure_test.go` — pure test for the mapping.
- `internal/podman/fake/fake.go` — `PlayKubeContainerHealth` + `ImportTransform` test hooks.
- `internal/instance/migrate.go` — `podReady`, `waitRunning` health gate, `SetVerifyTimeout`, volume-verify branch in `migratePostStop`.
- `internal/instance/service.go` — `ErrVolumeIntegrity`, `verifyVolumes` field + default + `SetVerifyVolumes`, `volumeManifest` helper.
- `internal/instance/manifest.go` (new) — `Manifest`, `fileInfo`, `buildManifest`, `firstDiff`.
- `internal/instance/manifest_test.go` (new) — manifest unit tests + `tarBytes` helper.
- `internal/instance/migrate_test.go` — readiness tests; integrity rollback test; update existing volume seeds to tar.
- `cmd/podman-api/main.go` — `-migrate-verify-timeout` / `-migrate-verify-volumes` flags + wiring.
- `README.md` — document both flags.
- Wiki (separate repo `/tmp/pa-wiki`) — Operating + Deploying updates (final task).

---

## Task 1: Container health in the podman layer

**Files:**
- Modify: `internal/podman/types.go` (the `Container` struct, ~line 15-25)
- Modify: `internal/podman/real.go` (`enrichContainer`, ~line 188-223)
- Test: `internal/podman/real_pure_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/podman/real_pure_test.go` (and add `"github.com/containers/podman/v5/libpod/define"` to its imports):

```go
func TestEnrichContainer_Health(t *testing.T) {
	t.Run("healthcheck status copied", func(t *testing.T) {
		var c Container
		enrichContainer(&c, &define.InspectContainerData{
			State: &define.InspectContainerState{
				Health: &define.HealthCheckResults{Status: "healthy"},
			},
		})
		if c.Health != "healthy" {
			t.Fatalf("Health = %q, want %q", c.Health, "healthy")
		}
	})

	t.Run("no healthcheck leaves Health empty", func(t *testing.T) {
		var c Container
		enrichContainer(&c, &define.InspectContainerData{State: &define.InspectContainerState{}})
		if c.Health != "" {
			t.Fatalf("Health = %q, want empty", c.Health)
		}
	})
}
```

- [ ] **Step 2: Run it; verify it fails to compile**

Run: `make test 2>&1 | grep -A3 podman`
Expected: build failure — `c.Health undefined` (field not yet on `Container`).

- [ ] **Step 3: Add the `Health` field**

In `internal/podman/types.go`, add the field to `Container` immediately after `Status string`:

```go
	Status       string
	// Health is the container's healthcheck status: "" when the container
	// declares no healthcheck, otherwise "healthy" / "unhealthy" / "starting".
	Health       string
```

- [ ] **Step 4: Populate it in `enrichContainer`**

In `internal/podman/real.go`, inside `enrichContainer`, add right after the existing `if ins.State != nil && !ins.State.StartedAt.IsZero() { ... }` block:

```go
	if ins.State != nil && ins.State.Health != nil {
		c.Health = ins.State.Health.Status
	}
```

- [ ] **Step 5: Run the test; verify it passes**

Run: `make test 2>&1 | tail -20`
Expected: PASS (whole suite still green — `Health` defaults to `""` everywhere else).

- [ ] **Step 6: Commit**

```bash
git add internal/podman/types.go internal/podman/real.go internal/podman/real_pure_test.go
git commit -m "feat(podman): expose container healthcheck status (#54)"
```

---

## Task 2: Application-readiness gate in waitRunning

**Files:**
- Modify: `internal/instance/migrate.go` (`waitRunning` ~line 252, `allContainersRunning` ~line 276; add `SetVerifyTimeout`)
- Modify: `internal/podman/fake/fake.go` (add `PlayKubeContainerHealth`)
- Test: `internal/instance/migrate_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/instance/migrate_test.go`:

```go
func TestPodReady(t *testing.T) {
	tests := []struct {
		name string
		pod  podman.Pod
		want bool
	}{
		{"pod not running", podman.Pod{Status: "Exited", Containers: []podman.Container{{Status: "Running"}}}, false},
		{"container not running", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Exited"}}}, false},
		{"no healthcheck, all running", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running"}}}, true},
		{"healthcheck healthy", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running", Health: "healthy"}}}, true},
		{"healthcheck unhealthy", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running", Health: "unhealthy"}}}, false},
		{"healthcheck still starting", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running", Health: "starting"}}}, false},
		{"mixed declared and undeclared", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running", Health: "healthy"}, {Status: "Running"}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podReady(tt.pod); got != tt.want {
				t.Fatalf("podReady = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWaitRunning_HealthGate(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	svc, f, _ := newMigrateSvc(t)
	ctx := context.Background()

	t.Run("ready when declared healthcheck is healthy", func(t *testing.T) {
		f.AddPod("h2", podman.Pod{Name: "web-ok", Status: "Running",
			Containers: []podman.Container{{Status: "Running", Health: "healthy"}}})
		require.NoError(t, svc.waitRunning(ctx, "h2", "web", "ok"))
	})

	t.Run("times out while a healthcheck stays unhealthy", func(t *testing.T) {
		f.AddPod("h2", podman.Pod{Name: "web-bad", Status: "Running",
			Containers: []podman.Container{{Status: "Running", Health: "unhealthy"}}})
		err := svc.waitRunning(ctx, "h2", "web", "bad")
		require.Error(t, err)
		assert.ErrorContains(t, err, "not running")
	})
}
```

- [ ] **Step 2: Run; verify it fails**

Run: `make test 2>&1 | grep -A3 instance`
Expected: build failure — `podReady` undefined.

- [ ] **Step 3: Replace `allContainersRunning` with `podReady` and use it in `waitRunning`**

In `internal/instance/migrate.go`:

(a) In `waitRunning`, change the success check from
```go
		if err == nil && p.Status == "Running" && allContainersRunning(p) {
```
to
```go
		if err == nil && podReady(p) {
```

(b) Replace the whole `allContainersRunning` function with:
```go
// podReady reports whether the pod is up and serving: the pod is Running, every
// container is Running, and every container that declares a healthcheck reports
// "healthy". Containers with no declared healthcheck (Health == "") are gated on
// liveness alone, so an instance without healthchecks behaves exactly as before.
// "starting" (still inside the healthcheck start_period) counts as not ready.
func podReady(p podman.Pod) bool {
	if p.Status != "Running" {
		return false
	}
	for _, c := range p.Containers {
		if c.Status != "Running" {
			return false
		}
		if c.Health != "" && c.Health != "healthy" {
			return false
		}
	}
	return true
}
```

(c) Add the exported timeout setter near the `verifyTimeout`/`verifyInterval` var block (after the `var (...)` at ~line 23):
```go
// SetVerifyTimeout overrides the maximum time waitRunning waits for the
// destination to become ready before the migrate fails (and rolls back).
// No-op for d <= 0. Called once at startup from the -migrate-verify-timeout flag.
func SetVerifyTimeout(d time.Duration) {
	if d > 0 {
		verifyTimeout = d
	}
}
```

- [ ] **Step 4: Add the fake health hook**

In `internal/podman/fake/fake.go`:

(a) Add a field next to `PlayKubeContainerStatus`:
```go
	// PlayKubeContainerHealth sets the Health status of containers created by
	// PlayKube (default "" = no healthcheck declared). Lets a test drive the
	// migrate readiness gate.
	PlayKubeContainerHealth string
```

(b) In `PlayKube`, set it on each played container — change the container append to include `Health`:
```go
			cs = append(cs, podman.Container{
				Name: c.Name, Image: c.Image, ImageTag: c.Image,
				Status: cstatus, Health: f.PlayKubeContainerHealth, StartedAt: time.Now(),
			})
```

- [ ] **Step 5: Run; verify pass**

Run: `make test 2>&1 | tail -20`
Expected: PASS, whole suite green.

- [ ] **Step 6: Commit**

```bash
git add internal/instance/migrate.go internal/podman/fake/fake.go internal/instance/migrate_test.go
git commit -m "feat(migrate): gate verify on container health, not just liveness (#54)"
```

---

## Task 3: Volume-copy content manifest

**Files:**
- Create: `internal/instance/manifest.go`
- Create: `internal/instance/manifest_test.go`
- Modify: `internal/instance/service.go` (`ErrVolumeIntegrity`, `verifyVolumes` field + default + setter, `volumeManifest`)
- Test: `internal/instance/manifest_test.go`

- [ ] **Step 1: Write the failing manifest tests**

Create `internal/instance/manifest_test.go`:

```go
package instance

import (
	"archive/tar"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tarBytes builds an uncompressed tar (one regular file per map entry) for use
// as fake volume contents. Map iteration order varies, which also exercises the
// manifest's order-independence.
func tarBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg,
		}))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

func TestBuildManifest(t *testing.T) {
	a, err := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"f1": "hello", "dir/f2": "world"})))
	require.NoError(t, err)
	require.Len(t, a, 2)
	assert.Equal(t, int64(5), a["f1"].size)
	assert.NotEmpty(t, a["f1"].sha256)
}

func TestManifest_FirstDiff(t *testing.T) {
	base, _ := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"f1": "hello", "dir/f2": "world"})))

	// Same content, different write order -> equal.
	same, _ := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"dir/f2": "world", "f1": "hello"})))
	_, ok := base.firstDiff(same)
	assert.True(t, ok, "identical content must compare equal")

	// Changed content -> differs at that path.
	changed, _ := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"f1": "HELLO", "dir/f2": "world"})))
	diff, ok := base.firstDiff(changed)
	assert.False(t, ok)
	assert.Equal(t, "f1", diff)

	// Missing file -> differs.
	missing, _ := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"f1": "hello"})))
	_, ok = base.firstDiff(missing)
	assert.False(t, ok)
}

func TestBuildManifest_EmptyStream(t *testing.T) {
	m, err := buildManifest(bytes.NewReader(nil))
	require.NoError(t, err)
	assert.Empty(t, m)
}

func TestBuildManifest_NotTar(t *testing.T) {
	_, err := buildManifest(bytes.NewReader([]byte("this is definitely not a tar archive")))
	require.Error(t, err)
}
```

- [ ] **Step 2: Run; verify it fails**

Run: `make test 2>&1 | grep -A3 instance`
Expected: build failure — `buildManifest` / `firstDiff` undefined.

- [ ] **Step 3: Implement the manifest**

Create `internal/instance/manifest.go`:

```go
package instance

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path"
	"sort"
)

// fileInfo is the content fingerprint of one tar entry. It deliberately omits
// mtime/uid/gid/mode so a volume compares equal across hosts that don't preserve
// those identically.
type fileInfo struct {
	typ    byte   // tar.Header.Typeflag
	size   int64  // regular files only
	sha256 string // hex sha256 of contents; regular files only
	link   string // symlink/hardlink target only
}

// Manifest fingerprints a volume's tar export, keyed by cleaned path.
type Manifest map[string]fileInfo

// buildManifest parses an uncompressed tar stream (as produced by VolumeExport)
// into a Manifest. It always drains r to EOF — even after a parse error — so a
// writer feeding r through a pipe can never block on a short read.
func buildManifest(r io.Reader) (Manifest, error) {
	m := Manifest{}
	err := parseTar(r, m)
	io.Copy(io.Discard, r) //nolint:errcheck // best-effort drain so a tee'd writer never blocks
	return m, err
}

func parseTar(r io.Reader, m Manifest) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fi := fileInfo{typ: hdr.Typeflag}
		switch hdr.Typeflag {
		case tar.TypeReg:
			h := sha256.New()
			n, err := io.Copy(h, tr)
			if err != nil {
				return err
			}
			fi.size = n
			fi.sha256 = hex.EncodeToString(h.Sum(nil))
		case tar.TypeSymlink, tar.TypeLink:
			fi.link = hdr.Linkname
		}
		m[path.Clean(hdr.Name)] = fi
	}
}

// firstDiff returns ("", true) when the two manifests are equal, otherwise
// (path, false) naming the first path (sorted) that is present on only one side
// or whose content differs. fileInfo is comparable, so == covers all fields.
func (m Manifest) firstDiff(other Manifest) (string, bool) {
	seen := map[string]bool{}
	var keys []string
	for k := range m {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for k := range other {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		a, oka := m[k]
		b, okb := other[k]
		if oka != okb || a != b {
			return k, false
		}
	}
	return "", true
}
```

- [ ] **Step 4: Add the service plumbing**

In `internal/instance/service.go`:

(a) Add the sentinel inside the existing `var (...)` block of errors:
```go
	ErrVolumeIntegrity   = errors.New("volume copy failed integrity check")
```

(b) Add `verifyVolumes bool` to the `Service` struct (after the `store` field):
```go
	store         store.Store
	verifyVolumes bool // verify each migrated volume's content before reaping the source
```

(c) In `NewService`, set the default in the struct literal:
```go
	s := &Service{
		client:        client,
		templates:     map[string]config.Template{},
		secretEnvs:    map[string]map[string]bool{},
		locks:         map[string]*sync.Mutex{},
		verifyVolumes: true,
	}
```

(d) Add the setter and the manifest helper (place near `CopyVolume`, ~line 557):
```go
// SetVerifyVolumes toggles post-copy volume integrity verification during
// migrate. Default true; set false (via -migrate-verify-volumes=false) to skip
// the extra source+dest re-export per volume.
func (s *Service) SetVerifyVolumes(v bool) { s.verifyVolumes = v }

// volumeManifest exports a host's volume and fingerprints its tar stream.
func (s *Service) volumeManifest(ctx context.Context, host, name string) (Manifest, error) {
	rc, err := s.client.VolumeExport(ctx, host, name)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return buildManifest(rc)
}
```

- [ ] **Step 5: Run; verify pass**

Run: `make test 2>&1 | tail -20`
Expected: PASS. (The verify branch isn't wired into migrate yet — that's Task 4 — so existing migrate tests with non-tar volume bytes still pass here.)

- [ ] **Step 6: Commit**

```bash
git add internal/instance/manifest.go internal/instance/manifest_test.go internal/instance/service.go
git commit -m "feat(migrate): tar content-manifest primitives for volume integrity (#54)"
```

---

## Task 4: Wire integrity into the migrate commit path

**Files:**
- Modify: `internal/instance/migrate.go` (`migratePostStop`, ~line 221-248)
- Modify: `internal/podman/fake/fake.go` (`ImportTransform` hook + `VolumeImport`)
- Test: `internal/instance/migrate_test.go` (new mismatch test; update existing seeds to tar)

- [ ] **Step 1: Add the fake import-corruption hook**

In `internal/podman/fake/fake.go`:

(a) Add a field near `ImportErr`:
```go
	// ImportTransform, if non-nil, rewrites the bytes VolumeImport stores on the
	// destination — lets a test simulate a lossy/corrupting copy so the source
	// and dest manifests diverge.
	ImportTransform func(host, name string, in []byte) []byte
```

(b) In `VolumeImport`, after `data, err := io.ReadAll(r)` (and its error check) and before taking the lock, add:
```go
	if f.ImportTransform != nil {
		data = f.ImportTransform(h, name, data)
	}
```

- [ ] **Step 2: Write the failing integrity-rollback test**

Add to `internal/instance/migrate_test.go`:

```go
func TestMigrate_VolumeIntegrityMismatch_RollsBack(t *testing.T) {
	ctx := context.Background()
	svc, f, mem := newMigrateSvc(t)
	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}
	require.NoError(t, mem.PutSpec(ctx, store.Spec{
		Host: "h1", Template: "postgres", Slug: "db1",
		Parameters: params, Secrets: map[string]string{"password": "p"},
	}))
	f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
	f.SetVolumeData("h1", "postgres-db1-data", tarBytes(t, map[string]string{"PG_VERSION": "16"}))
	// Destination receives different content than the source -> manifests differ.
	f.ImportTransform = func(_, _ string, _ []byte) []byte {
		return tarBytes(t, map[string]string{"PG_VERSION": "CORRUPT"})
	}

	err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, nil)
	require.ErrorIs(t, err, ErrVolumeIntegrity)

	// Rolled back: source restarted & intact, dest reaped (pod was never even applied).
	p, perr := f.PodInspect(ctx, "h1", "postgres-db1")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)
	_, gerr := mem.GetSpec(ctx, "h1", "postgres", "db1")
	require.NoError(t, gerr)
	_, derr := f.PodInspect(ctx, "h2", "postgres-db1")
	require.ErrorIs(t, derr, podman.ErrNotFound)
}
```

- [ ] **Step 3: Run; verify it fails**

Run: `make test 2>&1 | grep -A6 IntegrityMismatch`
Expected: FAIL — migrate currently succeeds (no verify wired), so `require.ErrorIs(..., ErrVolumeIntegrity)` fails.

- [ ] **Step 4: Wire the verify branch into `migratePostStop`**

In `internal/instance/migrate.go`, replace the volume loop in `migratePostStop` (the `for _, v := range vols { ... }` block) with:

```go
	for _, v := range vols {
		if err := s.client.VolumeCreate(ctx, req.ToHost, v.Name); err != nil {
			return fmt.Errorf("create dest volume %q: %w", v.Name, err)
		}
		if err := s.CopyVolume(ctx, req.FromHost, req.ToHost, v.Name); err != nil {
			return fmt.Errorf("copy volume %q: %w", v.Name, err)
		}
		step("copy-volume", v.Name)
		if s.verifyVolumes {
			src, err := s.volumeManifest(ctx, req.FromHost, v.Name)
			if err != nil {
				return fmt.Errorf("verify volume %q: re-export source: %w", v.Name, err)
			}
			dst, err := s.volumeManifest(ctx, req.ToHost, v.Name)
			if err != nil {
				return fmt.Errorf("verify volume %q: re-export dest: %w", v.Name, err)
			}
			if diff, ok := src.firstDiff(dst); !ok {
				return fmt.Errorf("%w: volume %q differs at %q", ErrVolumeIntegrity, v.Name, diff)
			}
			step("verify-volume", v.Name)
		}
	}
```

- [ ] **Step 5: Update existing migrate tests to seed tar (now that verify is on by default)**

In `internal/instance/migrate_test.go`:

(a) `TestMigrate_HappyPath` — replace the volume seed:
```go
	f.SetVolumeData("h1", "postgres-db1-data", []byte("PGDATA"))
```
with:
```go
	srcTar := tarBytes(t, map[string]string{"PG_VERSION": "16"})
	f.SetVolumeData("h1", "postgres-db1-data", srcTar)
```
then replace the dest-bytes assertion:
```go
	assert.Equal(t, []byte("PGDATA"), f.VolumeData("h2", "postgres-db1-data"))
```
with:
```go
	assert.Equal(t, srcTar, f.VolumeData("h2", "postgres-db1-data"))
```
and replace the steps assertion:
```go
	assert.Equal(t, []string{"load", "preflight", "stop-source", "copy-volume", "apply-dest", "verify", "commit"}, steps)
```
with (adds `verify-volume` after `copy-volume`):
```go
	assert.Equal(t, []string{"load", "preflight", "stop-source", "copy-volume", "verify-volume", "apply-dest", "verify", "commit"}, steps)
```

(b) `TestMigrate_Rollback` — in the `seed` closure, replace:
```go
		f.SetVolumeData("h1", "postgres-db1-data", []byte("PGDATA"))
```
with:
```go
		f.SetVolumeData("h1", "postgres-db1-data", tarBytes(t, map[string]string{"PG_VERSION": "16"}))
```
(The "apply fails" / "verify fails" subtests copy successfully, so the source is re-exported for the manifest — it must be valid tar.)

- [ ] **Step 6: Run; verify all pass**

Run: `make test 2>&1 | tail -25`
Expected: PASS — `TestMigrate_VolumeIntegrityMismatch_RollsBack`, `TestMigrate_HappyPath`, `TestMigrate_Rollback` (all subtests), and the whole suite green.

- [ ] **Step 7: Commit**

```bash
git add internal/instance/migrate.go internal/podman/fake/fake.go internal/instance/migrate_test.go
git commit -m "feat(migrate): verify volume content before reaping the source (#54)"
```

---

## Task 5: Flags + docs

**Files:**
- Modify: `cmd/podman-api/main.go` (flag declarations near line 41-42; wiring after `instance.NewService`, ~line 85)
- Modify: `README.md` (flag list after line 205)

- [ ] **Step 1: Add the flags**

In `cmd/podman-api/main.go`, in the `flag.*` declaration block (with `jobsRetention`/`evacConc`), add:
```go
		migrateVerifyTimeout = flag.Duration("migrate-verify-timeout", 60*time.Second, "max wait for a migrated instance to become ready (running + declared healthchecks healthy) before reaping the source")
		migrateVerifyVolumes = flag.Bool("migrate-verify-volumes", true, "verify each copied volume's content against the source before reaping the source (adds a re-export of source and dest per volume); false disables it")
```
Ensure `"time"` is in the import block (add it if absent).

- [ ] **Step 2: Wire them after the service is built**

In `cmd/podman-api/main.go`, immediately after `svc := instance.NewService(client, hosts, tmpls)`:
```go
	instance.SetVerifyTimeout(*migrateVerifyTimeout)
	svc.SetVerifyVolumes(*migrateVerifyVolumes)
```

- [ ] **Step 3: Build to verify wiring compiles**

Run: `make build`
Expected: builds `bin/podman-api` with no errors.

- [ ] **Step 4: Document the flags in README**

In `README.md`, after the `-evacuate-concurrency` bullet (line 205), add:
```markdown
- **`-migrate-verify-timeout <dur>`** — how long a migrate waits for the destination instance to become ready before it gives up and rolls back (default `60s`). Readiness means the pod and every container are `Running` **and** every container that declares a healthcheck reports `healthy`; raise this for apps with a slow warm-up (DB WAL replay, cache priming). Containers without a healthcheck are gated on liveness alone.
- **`-migrate-verify-volumes`** — verify each copied volume's contents (a sorted path→size+sha256 manifest) against the source before the source is reaped; a mismatch fails the move and rolls back (default `true`). Set `=false` to skip the extra source+dest re-export per volume on very large volumes.
```

- [ ] **Step 5: Verify formatting and vet**

Run: `gofmt -l . && go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`
Expected: no output from `gofmt -l`; `go vet` clean.

- [ ] **Step 6: Commit**

```bash
git add cmd/podman-api/main.go README.md
git commit -m "feat(migrate): -migrate-verify-timeout and -migrate-verify-volumes flags (#54)"
```

---

## Task 6: Wiki docs (separate repo — final, after review)

**Files:** `/tmp/pa-wiki/Operating.md`, `/tmp/pa-wiki/Deploying.md` (the `podman-api.wiki.git` repo — no PR flow; push directly).

This task touches a separate repository and has no Go tests. Do it after the code is reviewed/merged (mirrors the prior batch's flow). Steps:

- [ ] **Step 1: Deploying.md** — in the Run example, extend the migrate/evacuate / jobs comment lines to mention the two new flags:
```
# jobs: -jobs-retention=168h  -evacuate-concurrency=2
# migrate verify: -migrate-verify-timeout=60s (raise for slow app warm-up)  -migrate-verify-volumes=true (content-check copies)
```

- [ ] **Step 2: Operating.md** — in "Migrating & evacuating instances", rewrite the readiness *documented-limitation* note into real behavior: the commit now waits (up to `-migrate-verify-timeout`, default 60s) for every container that declares a healthcheck to report `healthy`, not just `Running`; containers without a healthcheck are still gated on liveness only. Add a sentence that each copied volume's content is verified against the source before the source is reaped (a manifest of path→size+sha256), that a mismatch rolls the move back, and that `-migrate-verify-volumes=false` opts out for very large volumes.

- [ ] **Step 3: Commit & push the wiki repo**
```bash
cd /tmp/pa-wiki && git add Operating.md Deploying.md && git commit -m "docs: migrate readiness gate + volume integrity verification (#54)" && git push
```

---

## Done criteria

- `make test` green (new: `TestEnrichContainer_Health`, `TestPodReady`, `TestWaitRunning_HealthGate`, `TestBuildManifest*`, `TestManifest_FirstDiff`, `TestMigrate_VolumeIntegrityMismatch_RollsBack`; updated: `TestMigrate_HappyPath`, `TestMigrate_Rollback`).
- `make build` succeeds; `gofmt -l .` empty; `go vet` clean.
- Migrate waits for declared healthchecks before committing; a never-healthy instance rolls back with the source intact.
- A corrupted volume copy fails the move with `ErrVolumeIntegrity` and the source is preserved.
- Both flags documented (README) and, post-merge, in the wiki.
