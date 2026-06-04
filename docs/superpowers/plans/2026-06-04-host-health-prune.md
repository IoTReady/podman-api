# Host-health Prune/Cleanup Implementation Plan (#59)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A headless, scheduled per-host `podman` prune/disk-cleanup capability that fires on a configurable interval OR disk high-water threshold, runs as auditable `prune` jobs on the existing jobs runner, and never reaps in-use resources.

**Architecture:** A new `prune` job kind (handler in `internal/prune`) executed by the existing `jobs.Runner`, fed by a per-host scheduler goroutine (also in `internal/prune`) started from `main` inside the `db != nil` block. Policy is parsed per `hosts/*.yaml` over global flag defaults. Dangling-image prune is on by default; all-images, containers, build-cache, and volumes are opt-in. Gated on `-state-db` like migrate/evacuate.

**Tech Stack:** Go, libpod v5 bindings (`pkg/bindings/{images,containers,volumes}`), `gopkg.in/yaml.v3`, Prometheus client, existing `store.JobStore` / `jobs.Runner` / `podman.Client` abstractions. Build/test with remote-client tags via `make`.

---

## File Structure

- **Create** `internal/prune/policy.go` — `PrunePolicy`, scope constants, `Resolve` (merge per-host over defaults), scope validation. (Lives in a new `prune` package so `config` need not depend on it.)
- **Modify** `internal/config/hosts.go` — add a raw `Prune *PruneConfig` field to `Host` (so `KnownFields(true)` accepts it) + the `PruneConfig` yaml shape.
- **Modify** `internal/podman/client.go` — add `PruneReport` type + 4 prune methods to `Client`.
- **Modify** `internal/podman/real.go` — implement the 4 methods via libpod bindings.
- **Modify** `internal/podman/fake/fake.go` — implement the 4 methods with call-recording + canned reports + injectable errors.
- **Create** `internal/prune/handler.go` — `Handler` (implements `jobs.Handler` for kind `"prune"`) + `Payload` job-args shape.
- **Create** `internal/prune/scheduler.go` — `Scheduler` (ticker loop, gates, in-flight guard, enqueue).
- **Create** `internal/obs/prune.go` — `PruneMetrics` collector set + nil-safe recorder.
- **Modify** `cmd/podman-api/main.go` — flags, registry entry, scheduler start.
- **Modify** `README.md` — operating/flags note.
- **Test files:** `internal/podman/fake/prune_test.go`, `internal/prune/policy_test.go`, `internal/prune/handler_test.go`, `internal/prune/scheduler_test.go`, `internal/config/hosts_prune_test.go`.

**Build/test commands** (always with tags — use the Makefile):
- Whole suite: `make test`
- One package: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/prune/ -run TestName -v`
- Build: `make build`
- Lint gates: `gofmt -l .` (must be empty), `go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`

For brevity below, `TAGS` means `containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper`.

---

## Task 1: Podman prune methods (interface + fake + real)

**Files:**
- Modify: `internal/podman/client.go` (interface + new `PruneReport` type)
- Modify: `internal/podman/fake/fake.go`
- Create: `internal/podman/fake/prune_test.go`
- Modify: `internal/podman/real.go`

Both `*Real` and `*Fake` have `var _ Client` assertions, so all four methods must exist on both for the package to compile. Unit tests exercise the fake (no podman host in CI); `real.go` is implemented to compile + match binding signatures and is covered by integration tests later.

- [ ] **Step 1: Add the `PruneReport` type and interface methods**

In `internal/podman/client.go`, add the type near `LogOptions`:

```go
// PruneReport summarizes one prune operation: the ids/names removed (or, in a
// dry-run, that would be removed) and the bytes reclaimed (sum of per-item sizes).
type PruneReport struct {
	Items     []string
	Reclaimed int64
}
```

And in the `Client` interface, under a new `// Prune` group (after `// Images`):

```go
	// Prune
	// ImagePrune removes unused images. all=false removes only dangling layers;
	// all=true also removes tagged images not used by any container.
	ImagePrune(ctx context.Context, hostID string, all bool) (PruneReport, error)
	// ContainerPrune removes stopped (exited) containers.
	ContainerPrune(ctx context.Context, hostID string) (PruneReport, error)
	// BuildCachePrune removes dangling build cache.
	BuildCachePrune(ctx context.Context, hostID string) (PruneReport, error)
	// VolumePrune removes unused (unattached) volumes. filters are libpod volume
	// prune filters (e.g. {"label!": {"podman-api.protect=true"}}) so callers can
	// protect volumes; never removes in-use volumes.
	VolumePrune(ctx context.Context, hostID string, filters map[string][]string) (PruneReport, error)
```

- [ ] **Step 2: Write the failing fake test**

Create `internal/podman/fake/prune_test.go`:

```go
package fake

import (
	"context"
	"errors"
	"testing"
)

func TestImagePruneRecordsCallAndReturnsCannedReport(t *testing.T) {
	f := New()
	f.PruneReports["images"] = struct {
		Items     []string
		Reclaimed int64
	}{Items: []string{"sha256:aaa"}, Reclaimed: 4096}

	rep, err := f.ImagePrune(context.Background(), "h1", true)
	if err != nil {
		t.Fatalf("ImagePrune: %v", err)
	}
	if rep.Reclaimed != 4096 || len(rep.Items) != 1 {
		t.Fatalf("unexpected report: %+v", rep)
	}
	if len(f.PruneCalls) != 1 || f.PruneCalls[0].Host != "h1" ||
		f.PruneCalls[0].Scope != "images" || !f.PruneCalls[0].All {
		t.Fatalf("unexpected calls: %+v", f.PruneCalls)
	}
}

func TestVolumePruneRecordsFiltersAndError(t *testing.T) {
	f := New()
	f.PruneErr = map[string]error{"volumes": errors.New("boom")}
	_, err := f.VolumePrune(context.Background(), "h1",
		map[string][]string{"label!": {"podman-api.protect=true"}})
	if err == nil {
		t.Fatal("expected error")
	}
	if len(f.PruneCalls) != 1 || f.PruneCalls[0].Scope != "volumes" ||
		f.PruneCalls[0].Filters["label!"][0] != "podman-api.protect=true" {
		t.Fatalf("unexpected calls: %+v", f.PruneCalls)
	}
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `go test -tags "$TAGS" ./internal/podman/fake/ -run TestImagePrune -v`
Expected: FAIL — compile error (`PruneReports`, `PruneCalls`, `ImagePrune` undefined).

- [ ] **Step 4: Implement the fake**

In `internal/podman/fake/fake.go`, add fields to the `Fake` struct (near the other hooks):

```go
	// Prune hooks. PruneReports maps a scope ("images","containers","buildcache",
	// "volumes") to the report ImagePrune/etc. should return; absent → empty report.
	// PruneErr maps a scope to an error to return. PruneCalls records every call.
	PruneReports map[string]struct {
		Items     []string
		Reclaimed int64
	}
	PruneErr   map[string]error
	PruneCalls []PruneCall
```

Add the call-record type and methods at the end of the file (before the `var _` assertion):

```go
// PruneCall records one prune invocation for assertions.
type PruneCall struct {
	Host    string
	Scope   string // "images" | "containers" | "buildcache" | "volumes"
	All     bool   // ImagePrune only
	Filters map[string][]string // VolumePrune only
}

func (f *Fake) pruneScope(host, scope string, all bool, filters map[string][]string) (podman.PruneReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PruneCalls = append(f.PruneCalls, PruneCall{Host: host, Scope: scope, All: all, Filters: filters})
	if f.PruneErr != nil {
		if err := f.PruneErr[scope]; err != nil {
			return podman.PruneReport{}, err
		}
	}
	if r, ok := f.PruneReports[scope]; ok {
		return podman.PruneReport{Items: r.Items, Reclaimed: r.Reclaimed}, nil
	}
	return podman.PruneReport{}, nil
}

func (f *Fake) ImagePrune(_ context.Context, host string, all bool) (podman.PruneReport, error) {
	return f.pruneScope(host, "images", all, nil)
}

func (f *Fake) ContainerPrune(_ context.Context, host string) (podman.PruneReport, error) {
	return f.pruneScope(host, "containers", false, nil)
}

func (f *Fake) BuildCachePrune(_ context.Context, host string) (podman.PruneReport, error) {
	return f.pruneScope(host, "buildcache", false, nil)
}

func (f *Fake) VolumePrune(_ context.Context, host string, filters map[string][]string) (podman.PruneReport, error) {
	return f.pruneScope(host, "volumes", false, filters)
}
```

If `New()` constructs maps eagerly, also initialize `PruneReports`/`PruneErr` there; otherwise leave nil (the code guards nil maps). Check `New()` and match its style — if it pre-allocates other maps, pre-allocate these too.

- [ ] **Step 5: Run the fake test to verify it passes**

Run: `go test -tags "$TAGS" ./internal/podman/fake/ -run TestImagePrune -v` and `-run TestVolumePrune`
Expected: PASS.

- [ ] **Step 6: Implement the real methods**

In `internal/podman/real.go`, add imports `"github.com/containers/podman/v5/pkg/bindings/images"`, `".../bindings/containers"` (volumes is already imported), and append:

```go
// sumPrune folds libpod's per-item prune reports into our PruneReport. Items with
// a non-nil Err are skipped from the reclaimed total but still surfaced as ids.
func sumPrune(reps []*reports.PruneReport) podman.PruneReport {
	var out podman.PruneReport
	for _, r := range reps {
		if r == nil {
			continue
		}
		out.Items = append(out.Items, r.Id)
		if r.Err == nil {
			out.Reclaimed += int64(r.Size)
		}
	}
	return out
}

func (r *Real) ImagePrune(ctx context.Context, id string, all bool) (podman.PruneReport, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return podman.PruneReport{}, err
	}
	reps, err := images.Prune(c, new(images.PruneOptions).WithAll(all))
	if err != nil {
		return podman.PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) ContainerPrune(ctx context.Context, id string) (podman.PruneReport, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return podman.PruneReport{}, err
	}
	reps, err := containers.Prune(c, new(containers.PruneOptions))
	if err != nil {
		return podman.PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) BuildCachePrune(ctx context.Context, id string) (podman.PruneReport, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return podman.PruneReport{}, err
	}
	// Build cache is pruned through the images prune endpoint with the
	// build-cache flag set (libpod has no standalone build-cache binding in v5).
	reps, err := images.Prune(c, new(images.PruneOptions).WithBuildCache(true))
	if err != nil {
		return podman.PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) VolumePrune(ctx context.Context, id string, filters map[string][]string) (podman.PruneReport, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return podman.PruneReport{}, err
	}
	opts := new(volumes.PruneOptions)
	if len(filters) > 0 {
		opts = opts.WithFilters(filters)
	}
	reps, err := volumes.Prune(c, opts)
	if err != nil {
		return podman.PruneReport{}, err
	}
	return sumPrune(reps), nil
}
```

Add the `reports` import: `"github.com/containers/podman/v5/pkg/domain/entities/reports"`. **Verify** these binding option builders (`WithAll`, `WithBuildCache`, `WithFilters`) and the `reports.PruneReport{Id,Err,Size}` shape compile against the vendored v5.8.2 (they were confirmed present during planning); adjust names if the build complains.

- [ ] **Step 7: Verify the whole podman package builds and tests pass**

Run: `go build -tags "$TAGS" ./internal/podman/... && go test -tags "$TAGS" ./internal/podman/...`
Expected: builds clean (both `var _ Client` assertions satisfied), tests PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/podman/client.go internal/podman/real.go internal/podman/fake/fake.go internal/podman/fake/prune_test.go
git commit -m "feat(podman): add image/container/buildcache/volume prune methods (#59)"
```

---

## Task 2: Prune policy types + per-host config + resolution

**Files:**
- Create: `internal/prune/policy.go`
- Modify: `internal/config/hosts.go`
- Create: `internal/prune/policy_test.go`
- Create: `internal/config/hosts_prune_test.go`

The raw yaml shape lives in `config` (so `KnownFields(true)` accepts a `prune:` block); the resolved policy + scope logic lives in `prune` (so `config` doesn't depend on `prune`).

- [ ] **Step 1: Add the raw config shape to `Host`**

In `internal/config/hosts.go`, add to the `Host` struct (after `Drain`):

```go
	// Prune is the optional per-host host-health cleanup policy. nil means "use
	// the global flag defaults". Pointer fields inside distinguish "unset"
	// (inherit default) from an explicit zero value.
	Prune *PruneConfig `yaml:"prune,omitempty"`
```

And add the type at the bottom of the file:

```go
// PruneConfig is the raw per-host prune policy as parsed from hosts/*.yaml.
// Every field is a pointer so an omitted field inherits the global default
// rather than overriding it with a zero value. Resolution lives in the prune
// package (config must not depend on prune).
type PruneConfig struct {
	Enabled       *bool     `yaml:"enabled,omitempty"`
	Interval      *string   `yaml:"interval,omitempty"`       // Go duration, e.g. "12h"
	DiskThreshold *int      `yaml:"disk_threshold_pct,omitempty"`
	Scope         *[]string `yaml:"scope,omitempty"`
	DryRun        *bool     `yaml:"dry_run,omitempty"`
}
```

- [ ] **Step 2: Write the failing config parse test**

Create `internal/config/hosts_prune_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeHost(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadHostsParsesPruneBlock(t *testing.T) {
	dir := t.TempDir()
	writeHost(t, dir, "h1.yaml", `id: h1
addr: unix
socket: /run/podman/podman.sock
prune:
  enabled: true
  interval: 12h
  disk_threshold_pct: 70
  scope: [dangling, volumes]
  dry_run: true
`)
	hosts, err := LoadHosts(dir)
	if err != nil {
		t.Fatalf("LoadHosts: %v", err)
	}
	if len(hosts) != 1 || hosts[0].Prune == nil {
		t.Fatalf("prune not parsed: %+v", hosts)
	}
	p := hosts[0].Prune
	if p.Enabled == nil || !*p.Enabled || p.Interval == nil || *p.Interval != "12h" ||
		p.DiskThreshold == nil || *p.DiskThreshold != 70 || p.Scope == nil ||
		len(*p.Scope) != 2 || p.DryRun == nil || !*p.DryRun {
		t.Fatalf("unexpected prune config: %+v", p)
	}
}

func TestLoadHostsNoPruneBlockLeavesNil(t *testing.T) {
	dir := t.TempDir()
	writeHost(t, dir, "h1.yaml", "id: h1\naddr: unix\nsocket: /s\n")
	hosts, err := LoadHosts(dir)
	if err != nil {
		t.Fatalf("LoadHosts: %v", err)
	}
	if hosts[0].Prune != nil {
		t.Fatalf("expected nil prune, got %+v", hosts[0].Prune)
	}
}
```

- [ ] **Step 3: Run to verify it fails, then passes**

Run: `go test -tags "$TAGS" ./internal/config/ -run TestLoadHostsParsesPrune -v`
Expected: FAIL first if the struct field is missing (compile error), PASS after Step 1 is in place. (Step 1 already adds the field, so this confirms parsing.)

- [ ] **Step 4: Write the failing policy-resolution test**

Create `internal/prune/policy_test.go`:

```go
package prune

import (
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
)

func ptr[T any](v T) *T { return &v }

func TestResolveUsesDefaultsWhenHostHasNoPolicy(t *testing.T) {
	def := Defaults{Enabled: false, Interval: 24 * time.Hour, DiskThreshold: 85, Scope: []string{ScopeDangling}, DryRun: false}
	got, err := Resolve(nil, def)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Enabled || got.Interval != 24*time.Hour || got.DiskThreshold != 85 ||
		len(got.Scope) != 1 || got.Scope[0] != ScopeDangling || got.DryRun {
		t.Fatalf("unexpected resolved policy: %+v", got)
	}
}

func TestResolveOverridesPerField(t *testing.T) {
	def := Defaults{Enabled: false, Interval: 24 * time.Hour, DiskThreshold: 85, Scope: []string{ScopeDangling}}
	hc := &config.PruneConfig{
		Enabled:       ptr(true),
		Interval:      ptr("6h"),
		DiskThreshold: ptr(60),
		Scope:         ptr([]string{ScopeDangling, ScopeVolumes}),
		DryRun:        ptr(true),
	}
	got, err := Resolve(hc, def)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !got.Enabled || got.Interval != 6*time.Hour || got.DiskThreshold != 60 ||
		len(got.Scope) != 2 || !got.DryRun {
		t.Fatalf("unexpected resolved policy: %+v", got)
	}
}

func TestResolveRejectsUnknownScope(t *testing.T) {
	def := Defaults{Scope: []string{ScopeDangling}}
	_, err := Resolve(&config.PruneConfig{Scope: ptr([]string{"bogus"})}, def)
	if err == nil {
		t.Fatal("expected unknown-scope error")
	}
}

func TestResolveRejectsBadInterval(t *testing.T) {
	_, err := Resolve(&config.PruneConfig{Interval: ptr("nope")}, Defaults{})
	if err == nil {
		t.Fatal("expected bad-duration error")
	}
}

func TestResolveRejectsThresholdOutOfRange(t *testing.T) {
	_, err := Resolve(&config.PruneConfig{DiskThreshold: ptr(150)}, Defaults{})
	if err == nil {
		t.Fatal("expected threshold range error")
	}
}
```

- [ ] **Step 5: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/prune/ -run TestResolve -v`
Expected: FAIL — package `prune` doesn't exist yet.

- [ ] **Step 6: Implement `policy.go`**

Create `internal/prune/policy.go`:

```go
// Package prune implements scheduled host-health cleanup: a "prune" job kind run
// by the jobs runner, fed by a per-host scheduler that fires on a configurable
// interval or disk high-water threshold. It never reaps in-use resources.
package prune

import (
	"fmt"
	"time"

	"github.com/iotready/podman-api/internal/config"
)

// Scope tokens. Dangling is the only default; the rest are opt-in.
const (
	ScopeDangling   = "dangling"    // dangling image layers
	ScopeAllImages  = "all-images"  // also unused tagged images
	ScopeContainers = "containers"  // exited containers
	ScopeBuildCache = "build-cache" // dangling build cache
	ScopeVolumes    = "volumes"     // unused (unattached) volumes, protect-filtered
)

func validScope(s string) bool {
	switch s {
	case ScopeDangling, ScopeAllImages, ScopeContainers, ScopeBuildCache, ScopeVolumes:
		return true
	}
	return false
}

// Defaults are the global flag-derived policy defaults a per-host config merges over.
type Defaults struct {
	Enabled       bool
	Interval      time.Duration
	DiskThreshold int // percent 0..100; 0 disables the threshold trigger
	Scope         []string
	DryRun        bool
}

// Policy is a fully-resolved, validated per-host prune policy.
type Policy struct {
	Enabled       bool
	Interval      time.Duration
	DiskThreshold int
	Scope         []string
	DryRun        bool
}

// Resolve merges a raw per-host config (nil = inherit everything) over defaults
// and validates the result. Unknown scope tokens, unparseable intervals, and
// out-of-range thresholds are errors.
func Resolve(hc *config.PruneConfig, def Defaults) (Policy, error) {
	p := Policy{
		Enabled:       def.Enabled,
		Interval:      def.Interval,
		DiskThreshold: def.DiskThreshold,
		Scope:         append([]string(nil), def.Scope...),
		DryRun:        def.DryRun,
	}
	if hc != nil {
		if hc.Enabled != nil {
			p.Enabled = *hc.Enabled
		}
		if hc.Interval != nil {
			d, err := time.ParseDuration(*hc.Interval)
			if err != nil {
				return Policy{}, fmt.Errorf("prune interval %q: %w", *hc.Interval, err)
			}
			p.Interval = d
		}
		if hc.DiskThreshold != nil {
			p.DiskThreshold = *hc.DiskThreshold
		}
		if hc.Scope != nil {
			p.Scope = append([]string(nil), *hc.Scope...)
		}
		if hc.DryRun != nil {
			p.DryRun = *hc.DryRun
		}
	}
	if p.DiskThreshold < 0 || p.DiskThreshold > 100 {
		return Policy{}, fmt.Errorf("prune disk_threshold_pct %d out of range 0..100", p.DiskThreshold)
	}
	for _, s := range p.Scope {
		if !validScope(s) {
			return Policy{}, fmt.Errorf("unknown prune scope %q", s)
		}
	}
	return p, nil
}

// HasScope reports whether the policy enables scope s.
func (p Policy) HasScope(s string) bool {
	for _, x := range p.Scope {
		if x == s {
			return true
		}
	}
	return false
}
```

- [ ] **Step 7: Run all Task-2 tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/prune/ ./internal/config/ -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/prune/policy.go internal/prune/policy_test.go internal/config/hosts.go internal/config/hosts_prune_test.go
git commit -m "feat(prune): per-host prune policy config + resolution (#59)"
```

---

## Task 3: Prune job handler

**Files:**
- Create: `internal/prune/handler.go`
- Create: `internal/prune/handler_test.go`

The handler runs the enabled scopes in a fixed safe order (images → containers → build-cache → volumes), records one job step per scope with reclaimed bytes, honors ctx cancellation between scopes, and in dry-run reports `HostInfo.Disk.Reclaimable` without removing anything.

- [ ] **Step 1: Write the failing handler test**

Create `internal/prune/handler_test.go`:

```go
package prune

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func runHandler(t *testing.T, h *Handler, payload Payload) (store.Job, error) {
	t.Helper()
	mem := store.NewMemory()
	args, _ := json.Marshal(payload)
	job, err := mem.Enqueue(context.Background(), "prune", args, "")
	if err != nil {
		t.Fatal(err)
	}
	jc := jobs.NewJobContext(mem, job.ID)
	runErr := h.Run(context.Background(), job, jc)
	got, _ := mem.GetJob(context.Background(), job.ID)
	return got, runErr
}

func TestHandlerRunsOnlyEnabledScopesInOrder(t *testing.T) {
	f := fake.New()
	f.PruneReports = map[string]struct {
		Items     []string
		Reclaimed int64
	}{
		"images":     {Items: []string{"i1"}, Reclaimed: 100},
		"containers": {Items: []string{"c1"}, Reclaimed: 200},
		"volumes":    {Items: []string{"v1"}, Reclaimed: 300},
	}
	h := &Handler{Client: f}
	pol := Policy{Scope: []string{ScopeDangling, ScopeContainers, ScopeVolumes}}
	if _, err := runHandler(t, h, Payload{Host: "h1", Policy: pol}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	gotOrder := []string{}
	for _, c := range f.PruneCalls {
		gotOrder = append(gotOrder, c.Scope)
	}
	want := []string{"images", "containers", "volumes"}
	if strings.Join(gotOrder, ",") != strings.Join(want, ",") {
		t.Fatalf("scope order = %v, want %v", gotOrder, want)
	}
	// images prune for dangling must pass all=false
	if f.PruneCalls[0].All {
		t.Fatal("dangling scope must call ImagePrune(all=false)")
	}
	// volumes must carry the protect-label filter
	if f.PruneCalls[2].Filters["label!"][0] != ProtectLabel+"=true" {
		t.Fatalf("volume prune missing protect filter: %+v", f.PruneCalls[2].Filters)
	}
}

func TestHandlerAllImagesPassesAllTrue(t *testing.T) {
	f := fake.New()
	h := &Handler{Client: f}
	if _, err := runHandler(t, h, Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeAllImages}}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.PruneCalls) != 1 || !f.PruneCalls[0].All {
		t.Fatalf("all-images must call ImagePrune(all=true): %+v", f.PruneCalls)
	}
}

func TestHandlerDryRunRemovesNothing(t *testing.T) {
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Reclaimable: 4096}}
	h := &Handler{Client: f}
	job, err := runHandler(t, h, Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeDangling, ScopeVolumes}, DryRun: true}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.PruneCalls) != 0 {
		t.Fatalf("dry-run must not call prune, got %+v", f.PruneCalls)
	}
	joined := ""
	for _, s := range job.Steps {
		joined += s.Step + ":" + s.Detail + "\n"
	}
	if !strings.Contains(joined, "dry-run") || !strings.Contains(joined, "4096") {
		t.Fatalf("dry-run step missing reclaimable: %q", joined)
	}
}

func TestHandlerScopeErrorFailsJobButContinues(t *testing.T) {
	f := fake.New()
	f.PruneErr = map[string]error{"images": errors.New("boom")}
	h := &Handler{Client: f}
	_, err := runHandler(t, h, Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeDangling, ScopeContainers}}})
	if err == nil {
		t.Fatal("expected handler to return error when a scope fails")
	}
	// containers scope still ran despite images failing
	ranContainers := false
	for _, c := range f.PruneCalls {
		if c.Scope == "containers" {
			ranContainers = true
		}
	}
	if !ranContainers {
		t.Fatal("handler must continue remaining scopes after one fails")
	}
}

func TestHandlerHonorsContextCancellation(t *testing.T) {
	f := fake.New()
	h := &Handler{Client: f}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mem := store.NewMemory()
	args, _ := json.Marshal(Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeDangling}}})
	job, _ := mem.Enqueue(context.Background(), "prune", args, "")
	jc := jobs.NewJobContext(mem, job.ID)
	if err := h.Run(ctx, job, jc); err == nil {
		t.Fatal("expected cancellation error")
	}
	if len(f.PruneCalls) != 0 {
		t.Fatalf("must not prune after cancellation: %+v", f.PruneCalls)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/prune/ -run TestHandler -v`
Expected: FAIL — `Handler`, `Payload`, `ProtectLabel` undefined.

- [ ] **Step 3: Implement `handler.go`**

Create `internal/prune/handler.go`:

```go
package prune

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// ProtectLabel marks a volume that must never be reaped by the volumes scope.
// The volume prune passes a "label!" filter so volumes carrying it are excluded.
const ProtectLabel = "podman-api.protect"

// Payload is the job-args shape the scheduler enqueues and the handler reads.
// It carries a snapshot of the resolved policy so a mid-flight config reload
// cannot change a running job's behavior.
type Payload struct {
	Host   string `json:"host"`
	Policy Policy `json:"policy"`
}

// Metrics records prune outcomes. nil-safe: a nil *Handler.Metrics records nothing.
type Metrics interface {
	RunDone(host, result string)
	Reclaimed(host, scope string, bytes int64)
}

// Handler implements jobs.Handler for the "prune" kind.
type Handler struct {
	Client  podman.Client
	Metrics Metrics // optional
}

var _ jobs.Handler = (*Handler)(nil)

func (h *Handler) metric() Metrics {
	if h.Metrics == nil {
		return noopMetrics{}
	}
	return h.Metrics
}

// scopeOrder is the fixed, safe execution order. images-family first, volumes last.
func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var p Payload
	if err := json.Unmarshal(job.Args, &p); err != nil {
		return fmt.Errorf("decode prune args: %w", err)
	}

	if p.Policy.DryRun {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := h.Client.HostInfo(ctx, p.Host)
		if err != nil {
			h.metric().RunDone(p.Host, "error")
			return fmt.Errorf("dry-run host info: %w", err)
		}
		jc.Step("dry-run", fmt.Sprintf("scopes=%v reclaimable=%d bytes (nothing removed)", p.Policy.Scope, info.Disk.Reclaimable))
		h.metric().RunDone(p.Host, "dry-run")
		return nil
	}

	// (scope, func) pairs in fixed order; only enabled ones run.
	type step struct {
		scope string
		run   func() (podman.PruneReport, error)
	}
	steps := []step{
		{ScopeDangling, func() (podman.PruneReport, error) { return h.Client.ImagePrune(ctx, p.Host, false) }},
		{ScopeAllImages, func() (podman.PruneReport, error) { return h.Client.ImagePrune(ctx, p.Host, true) }},
		{ScopeContainers, func() (podman.PruneReport, error) { return h.Client.ContainerPrune(ctx, p.Host) }},
		{ScopeBuildCache, func() (podman.PruneReport, error) { return h.Client.BuildCachePrune(ctx, p.Host) }},
		{ScopeVolumes, func() (podman.PruneReport, error) {
			return h.Client.VolumePrune(ctx, p.Host, map[string][]string{"label!": {ProtectLabel + "=true"}})
		}},
	}

	var firstErr error
	for _, s := range steps {
		if !p.Policy.HasScope(s.scope) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rep, err := s.run()
		if err != nil {
			jc.Step("prune:"+s.scope, "FAILED: "+err.Error())
			h.metric().Reclaimed(p.Host, s.scope, 0)
			if firstErr == nil {
				firstErr = fmt.Errorf("prune %s: %w", s.scope, err)
			}
			continue
		}
		jc.Step("prune:"+s.scope, fmt.Sprintf("removed %d item(s), reclaimed %d bytes", len(rep.Items), rep.Reclaimed))
		h.metric().Reclaimed(p.Host, s.scope, rep.Reclaimed)
	}
	if firstErr != nil {
		h.metric().RunDone(p.Host, "failed")
		return firstErr
	}
	h.metric().RunDone(p.Host, "succeeded")
	return nil
}

type noopMetrics struct{}

func (noopMetrics) RunDone(string, string)          {}
func (noopMetrics) Reclaimed(string, string, int64) {}
```

- [ ] **Step 4: Run the handler tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/prune/ -run TestHandler -v`
Expected: PASS (all five).

- [ ] **Step 5: Commit**

```bash
git add internal/prune/handler.go internal/prune/handler_test.go
git commit -m "feat(prune): prune job handler with scope ordering + dry-run (#59)"
```

---

## Task 4: Per-host scheduler

**Files:**
- Create: `internal/prune/scheduler.go`
- Create: `internal/prune/scheduler_test.go`

The scheduler ticks; on each tick it reads recent prune jobs to compute, per host, the last successful prune time and whether a prune is in flight, and checks whether any migrate/evacuate is active. For each enabled host it enqueues a prune job when (interval elapsed since last prune OR disk over threshold) AND no prune already in flight for that host. When a migrate/evacuate is active, the `volumes` scope is dropped from that tick's enqueued policy (the safety net against reaping a migration's transiently-detached volume); other scopes still run.

- [ ] **Step 1: Write the failing scheduler test**

Create `internal/prune/scheduler_test.go`:

```go
package prune

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// hostPolicy is the scheduler's per-host input.
func hp(id string, p Policy) HostPolicy { return HostPolicy{Host: id, Policy: p} }

func enabledDanglingPolicy() Policy {
	return Policy{Enabled: true, Interval: time.Hour, DiskThreshold: 80, Scope: []string{ScopeDangling}}
}

func decodePayload(t *testing.T, j store.Job) Payload {
	t.Helper()
	var p Payload
	if err := json.Unmarshal(j.Args, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTickEnqueuesWhenIntervalElapsed(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}

	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})

	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune"})
	if len(jobs) != 1 {
		t.Fatalf("expected 1 prune job, got %d", len(jobs))
	}
	if decodePayload(t, jobs[0]).Host != "h1" {
		t.Fatalf("wrong host: %+v", jobs[0])
	}
}

func TestTickSkipsDisabledHost(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	s := &Scheduler{Store: mem, Client: f, Now: time.Now}
	pol := enabledDanglingPolicy()
	pol.Enabled = false
	s.tick(context.Background(), []HostPolicy{hp("h1", pol)})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune"})
	if len(jobs) != 0 {
		t.Fatalf("disabled host must not enqueue, got %d", len(jobs))
	}
}

func TestTickSkipsWhenNotYetDueAndUnderThreshold(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 10}} // 10% < 80
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}

	// Seed a recent successful prune for h1 (30m ago < 1h interval).
	args, _ := json.Marshal(Payload{Host: "h1"})
	j, _ := mem.Enqueue(context.Background(), "prune", args, "")
	mem.ClaimNext(context.Background())
	mem.Finish(context.Background(), j.ID, store.JobSucceeded, "")
	// Memory store sets Finished to its own clock; override by re-seeding via helper if needed.
	s.lastOverride = map[string]time.Time{"h1": now.Add(-30 * time.Minute)}

	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune"})
	// Only the seeded (succeeded) job — no new queued one.
	queued := 0
	for _, jb := range jobs {
		if jb.State == store.JobQueued {
			queued++
		}
	}
	if queued != 0 {
		t.Fatalf("expected no new enqueue, got %d queued", queued)
	}
}

func TestTickEnqueuesOnThresholdBeforeInterval(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 90}} // 90% >= 80
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }, lastOverride: map[string]time.Time{"h1": now.Add(-1 * time.Minute)}}

	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 1 {
		t.Fatalf("threshold trigger should enqueue, got %d", len(jobs))
	}
}

func TestTickThresholdZeroDisablesThresholdTrigger(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 99}}
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	pol := enabledDanglingPolicy()
	pol.DiskThreshold = 0
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }, lastOverride: map[string]time.Time{"h1": now.Add(-1 * time.Minute)}}
	s.tick(context.Background(), []HostPolicy{hp("h1", pol)})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("threshold=0 must not trigger, got %d", len(jobs))
	}
}

func TestTickSkipsWhenPruneInFlight(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	// A queued prune for h1 already exists.
	args, _ := json.Marshal(Payload{Host: "h1"})
	mem.Enqueue(context.Background(), "prune", args, "")
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}
	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 1 {
		t.Fatalf("must not double-enqueue while prune in flight, got %d", len(jobs))
	}
}

func TestTickDropsVolumesScopeWhenMigrateActive(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	// An active (queued) migrate job.
	mem.Enqueue(context.Background(), "migrate", json.RawMessage(`{}`), "")
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}
	pol := enabledDanglingPolicy()
	pol.Scope = []string{ScopeDangling, ScopeVolumes}
	s.tick(context.Background(), []HostPolicy{hp("h1", pol)})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 1 {
		t.Fatalf("expected 1 prune job, got %d", len(jobs))
	}
	p := decodePayload(t, jobs[0])
	for _, sc := range p.Policy.Scope {
		if sc == ScopeVolumes {
			t.Fatal("volumes scope must be dropped while migrate active")
		}
	}
}

func TestTickSkipsHostWhenInfoErrorsButThresholdNeeded(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoErr = context.DeadlineExceeded
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	// Not yet due by interval, so the only path to enqueue is the threshold,
	// which needs HostInfo — and that errors. Host should be skipped, not crash.
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }, lastOverride: map[string]time.Time{"h1": now.Add(-1 * time.Minute)}}
	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("host with info error and not-due must be skipped, got %d", len(jobs))
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/prune/ -run TestTick -v`
Expected: FAIL — `Scheduler`, `HostPolicy` undefined.

- [ ] **Step 3: Implement `scheduler.go`**

Create `internal/prune/scheduler.go`:

```go
package prune

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// TickInterval is how often the scheduler re-evaluates every host. It is the
// granularity of both the interval and threshold gates.
const TickInterval = time.Minute

// activeScanLimit bounds how many recent jobs we scan to find in-flight work and
// last-prune times. Active (queued/running) jobs are always among the newest, so
// this is ample for any realistic queue depth.
const activeScanLimit = 500

// HostPolicy pairs a host id with its resolved policy. The caller (main) builds
// this slice once at startup and again on SIGHUP reload.
type HostPolicy struct {
	Host   string
	Policy Policy
}

// Scheduler enqueues prune jobs on a schedule. Store/Client/Now are injected so
// the tick logic is unit-testable without real time or a real podman host.
type Scheduler struct {
	Store  store.JobStore
	Client podman.Client
	Now    func() time.Time

	// lastOverride, when set for a host, replaces the store-derived last-prune
	// time. Test seam only; nil in production.
	lastOverride map[string]time.Time
}

// Start launches the ticker loop until ctx is cancelled. hostsFn returns the
// current host policies on each tick (so SIGHUP reloads are picked up).
func (s *Scheduler) Start(ctx context.Context, hostsFn func() []HostPolicy) {
	go func() {
		t := time.NewTicker(TickInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("prune: scheduler tick panicked: %v", r)
						}
					}()
					s.tick(ctx, hostsFn())
				}()
			}
		}
	}()
}

// tick evaluates every host once.
func (s *Scheduler) tick(ctx context.Context, hosts []HostPolicy) {
	now := s.Now()
	inflight, lastPrune := s.scanPruneJobs(ctx)
	migrateActive := s.migrateOrEvacuateActive(ctx)

	for _, hp := range hosts {
		if !hp.Policy.Enabled {
			continue
		}
		if inflight[hp.Host] {
			continue // dedup: a prune for this host is queued/running
		}

		due := false
		if last, ok := lastPrune[hp.Host]; !ok {
			due = true // never pruned
		} else if now.Sub(last) >= hp.Policy.Interval {
			due = true
		}

		overThreshold := false
		if !due && hp.Policy.DiskThreshold > 0 {
			info, err := s.Client.HostInfo(ctx, hp.Host)
			if err != nil {
				log.Printf("prune: host %s info failed, skipping this tick: %v", hp.Host, err)
				continue
			}
			if info.Disk.Total > 0 {
				usedPct := float64(info.Disk.Used) / float64(info.Disk.Total) * 100
				if usedPct >= float64(hp.Policy.DiskThreshold) {
					overThreshold = true
				}
			}
		}

		if !due && !overThreshold {
			continue
		}

		pol := hp.Policy
		if migrateActive && pol.HasScope(ScopeVolumes) {
			pol.Scope = withoutScope(pol.Scope, ScopeVolumes)
			log.Printf("prune: migrate/evacuate active, dropping volumes scope for host %s this run", hp.Host)
		}

		args, err := json.Marshal(Payload{Host: hp.Host, Policy: pol})
		if err != nil {
			log.Printf("prune: marshal payload for %s: %v", hp.Host, err)
			continue
		}
		if _, err := s.Store.Enqueue(ctx, "prune", args, ""); err != nil {
			log.Printf("prune: enqueue for %s failed: %v", hp.Host, err)
			continue
		}
		log.Printf("prune: enqueued cleanup for host %s (scopes=%v)", hp.Host, pol.Scope)
	}
}

// scanPruneJobs returns, from the most recent prune jobs, the set of hosts with a
// queued/running prune (in-flight) and the last successful prune time per host.
func (s *Scheduler) scanPruneJobs(ctx context.Context) (inflight map[string]bool, last map[string]time.Time) {
	inflight = map[string]bool{}
	last = map[string]time.Time{}
	if s.lastOverride != nil {
		for h, t := range s.lastOverride {
			last[h] = t
		}
	}
	jobs, err := s.Store.ListJobs(ctx, store.JobFilter{Kind: "prune", Limit: activeScanLimit})
	if err != nil {
		log.Printf("prune: list prune jobs failed: %v", err)
		return
	}
	for _, j := range jobs {
		var p Payload
		if err := json.Unmarshal(j.Args, &p); err != nil || p.Host == "" {
			continue
		}
		switch j.State {
		case store.JobQueued, store.JobRunning:
			inflight[p.Host] = true
		case store.JobSucceeded:
			if s.lastOverride != nil {
				continue // test seam wins
			}
			if cur, ok := last[p.Host]; !ok || j.Finished.After(cur) {
				last[p.Host] = j.Finished
			}
		}
	}
	return
}

// migrateOrEvacuateActive reports whether any migrate/evacuate job is queued or
// running. Coarse-grained (host-agnostic) on purpose: the only dangerous overlap
// is volume reaping, which we suppress entirely while any such job is active.
func (s *Scheduler) migrateOrEvacuateActive(ctx context.Context) bool {
	for _, kind := range []string{"migrate", "evacuate"} {
		jobs, err := s.Store.ListJobs(ctx, store.JobFilter{Kind: kind, Limit: activeScanLimit})
		if err != nil {
			log.Printf("prune: list %s jobs failed (assuming active for safety): %v", kind, err)
			return true
		}
		for _, j := range jobs {
			if j.State == store.JobQueued || j.State == store.JobRunning {
				return true
			}
		}
	}
	return false
}

func withoutScope(scopes []string, drop string) []string {
	out := make([]string, 0, len(scopes))
	for _, s := range scopes {
		if s != drop {
			out = append(out, s)
		}
	}
	return out
}
```

- [ ] **Step 4: Run the scheduler tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/prune/ -run TestTick -v`
Expected: PASS (all eight). If `store.NewMemory` does not exist under that name, check `internal/store/memory.go` for the constructor and adjust the test helper accordingly.

- [ ] **Step 5: Run the whole prune package with the race detector**

Run: `go test -tags "$TAGS" -race ./internal/prune/...`
Expected: PASS, no race warnings.

- [ ] **Step 6: Commit**

```bash
git add internal/prune/scheduler.go internal/prune/scheduler_test.go
git commit -m "feat(prune): per-host scheduler with interval+threshold gates and migrate-aware volume guard (#59)"
```

---

## Task 5: Prune metrics

**Files:**
- Create: `internal/obs/prune.go`
- Create: `internal/obs/prune_test.go`

A small Prometheus collector set implementing the `prune.Metrics` interface, created once and passed to the handler. Uses its own `prometheus.Registry`-free `MustRegister` like the existing `obs.New`, but guard against double-registration in tests by constructing collectors without global registration and exposing a `Register` step, OR follow the existing pattern exactly. Match `internal/obs/metrics.go`'s approach.

- [ ] **Step 1: Write the failing metrics test**

Create `internal/obs/prune_test.go`:

```go
package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestPruneMetricsRecord(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPruneMetrics(reg)
	m.RunDone("h1", "succeeded")
	m.Reclaimed("h1", "dangling", 2048)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sawRuns, sawReclaimed bool
	for _, mf := range mfs {
		switch mf.GetName() {
		case "podman_api_prune_runs_total":
			sawRuns = true
		case "podman_api_prune_reclaimed_bytes_total":
			sawReclaimed = true
			if v := sumCounter(mf.Metric); v != 2048 {
				t.Fatalf("reclaimed = %v, want 2048", v)
			}
		}
	}
	if !sawRuns || !sawReclaimed {
		t.Fatalf("missing metrics: runs=%v reclaimed=%v", sawRuns, sawReclaimed)
	}
}

func sumCounter(ms []*dto.Metric) float64 {
	var v float64
	for _, m := range ms {
		v += m.GetCounter().GetValue()
	}
	return v
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/obs/ -run TestPruneMetrics -v`
Expected: FAIL — `NewPruneMetrics` undefined.

- [ ] **Step 3: Implement `prune.go`**

Create `internal/obs/prune.go`:

```go
package obs

import "github.com/prometheus/client_golang/prometheus"

// PruneMetrics implements prune.Metrics with Prometheus counters. It is created
// with an explicit Registerer so production registers on the default registry
// (via NewPruneMetrics(prometheus.DefaultRegisterer)) and tests use a private one.
type PruneMetrics struct {
	runs      *prometheus.CounterVec
	reclaimed *prometheus.CounterVec
}

// NewPruneMetrics builds and registers the prune collectors on reg.
func NewPruneMetrics(reg prometheus.Registerer) *PruneMetrics {
	m := &PruneMetrics{
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_prune_runs_total",
			Help: "Count of prune job outcomes by host and result.",
		}, []string{"host", "result"}),
		reclaimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_prune_reclaimed_bytes_total",
			Help: "Bytes reclaimed by prune, by host and scope.",
		}, []string{"host", "scope"}),
	}
	reg.MustRegister(m.runs, m.reclaimed)
	return m
}

// RunDone records one finished prune run.
func (m *PruneMetrics) RunDone(host, result string) {
	m.runs.WithLabelValues(host, result).Inc()
}

// Reclaimed adds reclaimed bytes for a scope.
func (m *PruneMetrics) Reclaimed(host, scope string, bytes int64) {
	if bytes < 0 {
		bytes = 0
	}
	m.reclaimed.WithLabelValues(host, scope).Add(float64(bytes))
}
```

- [ ] **Step 4: Run the metrics test to verify it passes**

Run: `go test -tags "$TAGS" ./internal/obs/ -run TestPruneMetrics -v`
Expected: PASS. (`PruneMetrics` satisfies `prune.Metrics` structurally — no import of `prune` here, avoiding a cycle.)

- [ ] **Step 5: Commit**

```bash
git add internal/obs/prune.go internal/obs/prune_test.go
git commit -m "feat(obs): prune run/reclaimed Prometheus metrics (#59)"
```

---

## Task 6: Wire flags, registry, and scheduler into main

**Files:**
- Modify: `cmd/podman-api/main.go`

- [ ] **Step 1: Add the flags**

In the `flag` block in `main.go` (after `migrateVerifyVolumes`), add:

```go
		pruneEnabled   = flag.Bool("prune-enabled", false, "enable scheduled host-health prune/cleanup (requires -state-db)")
		pruneInterval  = flag.Duration("prune-interval", 24*time.Hour, "default interval between scheduled prunes per host")
		pruneThreshold = flag.Int("prune-disk-threshold", 85, "disk used%% high-water that triggers an early prune; 0 disables the threshold trigger")
		pruneScope     = flag.String("prune-scope", "dangling", "default prune scopes, comma-separated: dangling,all-images,containers,build-cache,volumes")
		pruneDryRun    = flag.Bool("prune-dry-run", false, "default dry-run: report reclaimable space without removing anything")
```

- [ ] **Step 2: Build the defaults and start the scheduler inside the `db != nil` block**

In `main.go`, inside `if db != nil { ... }`, after `runner.Start(runnerCtx)` and the retention block, add:

```go
		registry["prune"] = &prune.Handler{Client: client, Metrics: obs.NewPruneMetrics(prometheus.DefaultRegisterer)}
```

Wait — `registry` is constructed as a literal before `NewRunner`. Instead, add the `"prune"` entry to the `jobs.Registry{...}` literal directly:

```go
		pruneMetrics := obs.NewPruneMetrics(prometheus.DefaultRegisterer)
		registry := jobs.Registry{
			"migrate":  &migrate.Handler{Svc: svc},
			"evacuate": &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: *evacConc},
			"prune":    &prune.Handler{Client: client, Metrics: pruneMetrics},
		}
		runner := jobs.NewRunner(db, registry, jobs.DefaultWorkers)
		runner.Start(runnerCtx)
		if *jobsRetention > 0 {
			runner.StartRetention(runnerCtx, *jobsRetention)
			log.Printf("jobs retention enabled: pruning terminal jobs older than %s", *jobsRetention)
		}
		if *pruneEnabled {
			def := prune.Defaults{
				Enabled:       true,
				Interval:      *pruneInterval,
				DiskThreshold: *pruneThreshold,
				Scope:         splitScopes(*pruneScope),
				DryRun:        *pruneDryRun,
			}
			hostsFn := func() []prune.HostPolicy {
				return buildHostPolicies(hostsHolder.Load(), def)
			}
			// Validate the startup set once so misconfig fails loudly at boot.
			if _, err := resolveAll(hostsHolder.Load(), def); err != nil {
				log.Fatalf("prune policy: %v", err)
			}
			sched := &prune.Scheduler{Store: db, Client: client, Now: time.Now}
			sched.Start(runnerCtx, hostsFn)
			log.Printf("prune scheduler enabled (interval %s, disk threshold %d%%, scopes %v)", *pruneInterval, *pruneThreshold, def.Scope)
		}
```

> **Note on `hostsHolder`:** `main` already hot-swaps hosts on SIGHUP. Find how the current host set is stored (a local `hosts` slice plus the SIGHUP handler that rebuilds it). If hosts are kept in a plain variable captured by the SIGHUP closure, introduce a small `atomic.Pointer[[]config.Host]` (`hostsHolder`) that both the SIGHUP handler and `hostsFn` read, so the scheduler always sees the latest reloaded set. If a holder already exists, use it. If wiring an atomic holder is more than a couple of lines, fall back to capturing the startup `hosts` slice directly (document that per-host prune policy changes then require a process restart, not just SIGHUP) — pick the smaller change and note which in the commit message.

- [ ] **Step 3: Add the helper functions**

At the bottom of `main.go` (package-level), add:

```go
// splitScopes parses a comma-separated scope flag, trimming spaces and dropping empties.
func splitScopes(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// buildHostPolicies resolves every host's prune policy over the defaults,
// skipping (with a log) any host whose policy fails to resolve so one bad file
// never stops the others.
func buildHostPolicies(hosts []config.Host, def prune.Defaults) []prune.HostPolicy {
	var out []prune.HostPolicy
	for _, h := range hosts {
		p, err := prune.Resolve(h.Prune, def)
		if err != nil {
			log.Printf("prune: host %s policy invalid, skipping: %v", h.ID, err)
			continue
		}
		out = append(out, prune.HostPolicy{Host: h.ID, Policy: p})
	}
	return out
}

// resolveAll resolves all host policies, returning the first error (used at boot
// to fail loudly on a misconfigured startup set).
func resolveAll(hosts []config.Host, def prune.Defaults) ([]prune.HostPolicy, error) {
	var out []prune.HostPolicy
	for _, h := range hosts {
		p, err := prune.Resolve(h.Prune, def)
		if err != nil {
			return nil, fmt.Errorf("host %s: %w", h.ID, err)
		}
		out = append(out, prune.HostPolicy{Host: h.ID, Policy: p})
	}
	return out, nil
}
```

Add imports as needed: `"strings"`, `"fmt"` (likely already present), `"github.com/prometheus/client_golang/prometheus"`, `"github.com/iotready/podman-api/internal/prune"`. Confirm `client` (the `podman.Client`) is in scope at that point (it is — `client, err := podman.NewReal(hosts)` earlier).

- [ ] **Step 4: Build and vet**

Run: `make build && go vet -tags "$TAGS" ./...`
Expected: builds to `bin/podman-api`, vet clean. Fix any unused-import / scope issues the compiler flags.

- [ ] **Step 5: Smoke-check the flags exist**

Run: `./bin/podman-api -h 2>&1 | grep -E "prune-(enabled|interval|disk-threshold|scope|dry-run)"`
Expected: all five flags listed.

- [ ] **Step 6: Run the full suite**

Run: `make test`
Expected: all packages PASS (including the new `internal/prune` and `internal/obs` tests).

- [ ] **Step 7: Commit**

```bash
git add cmd/podman-api/main.go
git commit -m "feat(prune): wire flags, registry, and scheduler into main (#59)"
```

---

## Task 7: Documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document the feature and flags**

Add a "Host-health automation (scheduled prune)" subsection to `README.md` near the other operational flags, covering: it requires `-state-db`; the five flags with defaults; per-host `prune:` yaml override example (the one in the spec §4.2); the safety notes (opt-in, dangling-only default, in-flight guard vs migrate/evacuate, volume protect-label `podman-api.protect=true`, dry-run); and that runs appear in the jobs API as kind `prune`. Match the README's existing tone/format. (Operator wiki pages are updated separately per CLAUDE.md — note in the PR that the Operating wiki page needs a matching entry.)

- [ ] **Step 2: Verify gofmt + final full suite**

Run: `gofmt -l . ` (expect empty) and `make test` (expect PASS).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(prune): document scheduled host-health prune flags and policy (#59)"
```

---

## Self-Review (planner)

**Spec coverage:**
- §3 architecture (job kind + scheduler, gated on `-state-db`) → Tasks 3, 4, 6. ✓
- §4.1 client prune methods → Task 1. ✓
- §4.2 `PrunePolicy` per-host yaml + merge → Task 2. ✓
- §4.3 scheduler (interval gate from store last-prune, threshold gate from HostInfo, in-flight guard, policy snapshot) → Task 4. ✓
- §4.4 handler (scope order, per-scope steps, ctx-cancel, dry-run via reclaimable) → Task 3. ✓
- §4.5 metrics → Task 5. ✓ (Note: spec listed 4 instruments; plan ships `runs_total` + `reclaimed_bytes_total`. `failures_total` is subsumed by `runs_total{result="failed"}` and per-scope failed steps; `last_run_timestamp` is derivable from the jobs API. Documented reduction — YAGNI.)
- §4.6 wiring / flags → Task 6. ✓
- §5 scope defaults vs opt-in → Task 2 (`Defaults.Scope=[dangling]`) + Task 3 (per-scope dispatch). ✓
- §6 safety: safe-by-default (no force) ✓ Task 1; in-flight guard ✓ Task 4; volume protect-label ✓ Task 3 (`ProtectLabel`); dry-run ✓ Task 3; opt-in feature ✓ Task 6 (`-prune-enabled=false`). ✓
- §7 error handling → Task 3 (per-scope continue-on-error) + Task 4 (host-info error skip, enqueue retry-next-tick, panic recover). ✓
- §8 testing → tests in every task. ✓

**Placeholder scan:** No TBD/TODO. The one judgment call (`hostsHolder` atomic vs restart-only) is spelled out with a concrete fallback and a decision rule, not left open.

**Type consistency:** `PruneReport{Items,Reclaimed}` (Task 1) used consistently in handler (Task 3). `Policy`/`Defaults`/`Resolve`/`HasScope`/scope consts (Task 2) used identically in handler + scheduler. `Payload{Host,Policy}` (Task 3) marshaled/unmarshaled identically in scheduler (Task 4) and tests. `Metrics{RunDone,Reclaimed}` interface (Task 3) matches `*obs.PruneMetrics` methods (Task 5) exactly. Fake fields `PruneReports/PruneErr/PruneCalls` (Task 1) used by handler tests (Task 3). ✓

**Note for implementers:** binding option-builder names (`WithAll`/`WithBuildCache`/`WithFilters`) and `reports.PruneReport` were confirmed present in vendored podman v5.8.2 during planning; if a future bump changes them, Step 6 of Task 1 says to adjust. `store.NewMemory` constructor name should be confirmed against `internal/store/memory.go` before writing test helpers (Task 4 Step 4 notes this).
