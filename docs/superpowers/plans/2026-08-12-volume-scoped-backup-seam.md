# Volume-Scoped Backup Seam Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a backup capture a subset of an instance's volumes, honour `backup: none` as a real veto, and make a partial restore incapable of deleting data it has no copy of.

**Architecture:** Three layers change in a safety-forced order. First `Restore` stops tearing down volumes it cannot recreate — this must land *before* anything can produce a narrower backup. Then the runner honours the `none` marker (which itself makes backups narrower). Then an optional volume scope is threaded from the extension seam and the HTTP route down through the job args into the runner, validated before any pod is stopped.

**Tech Stack:** Go 1.x, `testify` (`assert`/`require`), the in-package `internal/podman/fake` client, `internal/store.Memory`.

**Spec:** `docs/superpowers/specs/2026-08-12-volume-scoped-backup-seam-design.md`

## Global Constraints

- Build and test with the module's tags: `make test` runs `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`. A bare `go test ./...` fails to build on a clean machine. Same for `make vet`.
- Volume scope is always expressed in the template's **declared short names** (`sites`), never podman's full `<template>-<slug>-<volume>` names. The core resolves via the existing `volumeName(tmpl, slug, short)` helper — never by string concatenation.
- The only marker literal the core interprets is the exact string `none`. Every other marker value stays opaque and belongs to the commercial layer.
- A volume declaring **no** `backup:` field is still exported. Only an explicit `none` vetoes.
- `make vet` must be clean before every commit.
- Work on branch `feat/volume-scoped-backup-249`. `main` is PR-only.

---

### Task 1: Restore removes only the volumes it can recreate

Lands first on purpose. Task 2 makes backups narrower than the whole instance; if this task were second, there would be a window where restoring one deletes unbacked-up volumes permanently.

**Files:**
- Modify: `internal/instance/backup.go` — `Restore` (the `Delete` call, ~line 284) and `restoreVolume` (~line 337)
- Test: `internal/instance/backup_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: no new exported names. Behaviour relied on by Tasks 2-4: a restore touches exactly `b.Volumes` and no other volume.

- [ ] **Step 1: Write the failing test**

Add to `internal/instance/backup_test.go`:

```go
// TestRestore_LeavesVolumesOutsideTheBackupAlone locks the invariant that a
// restore may never delete a volume it has no copy of. Before this, Restore
// tore down every DECLARED volume via PruneVolumes:true and recreated only the
// ones the backup recorded — harmless while every backup held every volume, and
// data loss the moment one is narrower.
//
// The volume must be DECLARED by the template to reproduce: pruneInstanceResources
// derives what to remove from t.Meta.Volumes, so an undeclared volume was never
// at risk and would make this test pass vacuously. The scenario here is the
// ordinary one that gets there today — a template that gained a volume after the
// backup was taken.
func TestRestore_LeavesVolumesOutsideTheBackupAlone(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	require.Equal(t, "pg-a-data", b.Volumes[0].Name)

	// The template gains a volume AFTER the backup, and the instance populates
	// it. The backup has no copy of it.
	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "extra"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-extra", tarBytes(t, map[string]string{"keep": "me"}))

	require.NoError(t, svc.Restore(ctx, RestoreRequest{BackupID: req.BackupID}, nil))

	assert.NotEmpty(t, f.VolumeData("h1", "pg-a-extra"),
		"restore deleted a declared volume the backup had no copy of")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run TestRestore_LeavesVolumesOutsideTheBackupAlone -v`

Expected: FAIL — `restore deleted a volume that was not in the backup` (the assertion on `VolumeData` finds nothing, because `PruneVolumes: true` removed it).

- [ ] **Step 3: Stop the teardown from pruning volumes**

In `Restore`, change the teardown call. Replace:

```go
	if err := s.Delete(ctx, b.Host, b.Template, b.Slug, DeleteOptions{PruneVolumes: true}); err != nil && !errors.Is(err, ErrInstanceNotFound) {
		return fmt.Errorf("teardown: %w", err)
	}
```

with:

```go
	// PruneVolumes is deliberately false: a backup may cover fewer volumes than
	// the instance declares (a scoped backup, or one whose `none`-marked volumes
	// were vetoed), and a pruning teardown would delete those and never put them
	// back. Each volume the backup DOES carry is removed by restoreVolume
	// immediately before it is recreated, so an import never merges into stale
	// content. Removing the pod first is what makes those volumes unreferenced.
	if err := s.Delete(ctx, b.Host, b.Template, b.Slug, DeleteOptions{PruneVolumes: false}); err != nil && !errors.Is(err, ErrInstanceNotFound) {
		return fmt.Errorf("teardown: %w", err)
	}
```

- [ ] **Step 4: Remove each restored volume before recreating it**

In `restoreVolume`, insert before the existing `VolumeCreate` call:

```go
	// Remove before create: VolumeCreate is idempotent, so without this an
	// import would merge into whatever the old volume still held. ErrNotFound is
	// ordinary — a DR rebuild restores onto a host with no such volume yet.
	if err := s.client.VolumeRemove(ctx, b.Host, bv.Name, true); err != nil && !errors.Is(err, podman.ErrNotFound) {
		return fmt.Errorf("remove before restore: %w", err)
	}
	if err := s.client.VolumeCreate(ctx, b.Host, bv.Name); err != nil {
		return fmt.Errorf("create: %w", err)
	}
```

(The `VolumeCreate` block already exists — add the `VolumeRemove` block above it, do not duplicate `VolumeCreate`.)

- [ ] **Step 5: Run the new test and the whole backup suite**

Run: `cd ~/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run 'TestRestore|TestBackup' -v`

Expected: PASS, including the pre-existing `TestRestore_HappyPath` and `TestRestore_VerifyMismatchFails` — the whole-instance path must be unchanged.

- [ ] **Step 6: Run the full suite and vet**

Run: `cd ~/projects/podman-api && make test && make vet`
Expected: all packages PASS, vet silent.

- [ ] **Step 7: Commit**

```bash
cd ~/projects/podman-api
git add internal/instance/backup.go internal/instance/backup_test.go
git commit -m "fix(restore): never delete a volume the backup cannot recreate (#249)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: The runner honours `backup: none`, and says what it skipped

**Files:**
- Modify: `internal/instance/service.go` — add `BackupMarkerNone` to the `var` block at ~line 35-45
- Modify: `internal/instance/backup.go` — `Backup`'s export loop (~lines 126-150)
- Modify: `internal/instance/service_test.go` — `pgTemplate()` volume fixture (~line 64)
- Modify: `internal/api/backups_test.go` — `backupTmpl()` volume fixture (~line 50)
- Test: `internal/instance/backup_test.go`

**Interfaces:**
- Consumes: Task 1's guarantee that a narrower backup restores safely.
- Produces: `instance.BackupMarkerNone` (exported `const`, value `"none"`) — Task 3 uses it in `CheckBackupable` and `backupctl`.

**Fixture note:** `pgTemplate()` today declares its single volume as `{Name: "data", Backup: "none"}`. Under this task that volume would stop being backed up and every existing backup test would fail. The fixture is changed to a realistic two-volume shape instead — a marked `data` and a vetoed `logs` — which keeps `TestBackup_HappyPath`'s `Len(b.Volumes, 1)` true for the right reason.

- [ ] **Step 1: Change the shared template fixture**

In `internal/instance/service_test.go`, replace the `Volumes:` line in `pgTemplate()`:

```go
			Volumes: []render.Volume{{Name: "data", Backup: "none"}},
```

with:

```go
			Volumes: []render.Volume{
				{Name: "data", Backup: "s3; interval=24h"},
				{Name: "logs", Backup: "none"},
			},
```

Apply the identical change to `backupTmpl()` in `internal/api/backups_test.go`, whose `data` volume is also declared `Backup: "none"` today — leaving it would make every API-level backup exercise a no-op backup that captures nothing:

```go
			Volumes: []render.Volume{
				{Name: "data", Backup: "s3; interval=24h"},
				{Name: "logs", Backup: "none"},
			},
```

- [ ] **Step 2: Write the failing tests**

Add to `internal/instance/backup_test.go`:

```go
// TestBackup_SkipsNoneMarkedVolumes proves `backup: none` is a veto rather than
// documentation. The `logs` volume exists on the host and is exported today.
func TestBackup_SkipsNoneMarkedVolumes(t *testing.T) {
	svc, f, _, blob := newBackupSvc(t)
	ctx := context.Background()
	f.SetVolumeData("h1", "pg-a-logs", tarBytes(t, map[string]string{"app.log": "noise"}))

	req := newBackupReq()
	var steps []string
	require.NoError(t, svc.Backup(ctx, req, recordSteps(&steps)))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	assert.Equal(t, "pg-a-data", b.Volumes[0].Name)

	_, err = blob.Get(ctx, "h1/pg/a/"+req.BackupID+"/pg-a-logs.tar")
	assert.ErrorIs(t, err, fs.ErrNotExist, "a none-marked volume must write no blob")

	assert.Contains(t, steps, "skip-volume")
}

// TestBackup_ExportsUnmarkedVolumes locks the other half of the rule: only an
// explicit `none` vetoes. A volume with no backup: field at all is still
// captured — reading "unmarked" as "excluded" would silently shrink every
// existing backup in the fleet.
func TestBackup_ExportsUnmarkedVolumes(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	names := []string{}
	for _, v := range b.Volumes {
		names = append(names, v.Name)
	}
	assert.ElementsMatch(t, []string{"pg-a-data", "pg-a-cache"}, names)
}

// TestBackup_EmitsExportStepBeforeExporting is the #135 fix: the step trail must
// name the volume being exported for the DURATION of the export, not only once
// it finishes. Without it the whole multi-minute stop window reads as dead air
// between `stop` and the first sign of progress, and a live job is
// indistinguishable from a hung one.
func TestBackup_EmitsExportStepBeforeExporting(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()

	var steps []string
	f.ExportReader = func(_, name string) io.ReadCloser {
		// Assert mid-export: by the time bytes are being read, the step must
		// already have been emitted.
		assert.Contains(t, steps, "export-volume",
			"export-volume must be emitted BEFORE the export begins")
		return io.NopCloser(bytes.NewReader(tarBytes(t, map[string]string{"x": "1"})))
	}

	require.NoError(t, svc.Backup(ctx, newBackupReq(), recordSteps(&steps)))
	assert.Contains(t, steps, "export-volume-done")
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd ~/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run 'TestBackup_SkipsNoneMarkedVolumes|TestBackup_ExportsUnmarkedVolumes|TestBackup_EmitsExportStepBeforeExporting' -v`

Expected: all three FAIL. `SkipsNoneMarked` fails on `Len(b.Volumes, 1)` (gets 2); `ExportsUnmarked` passes trivially today but must still be present as a regression lock; `EmitsExportStepBefore` fails inside `ExportReader` on the `Contains` assertion.

- [ ] **Step 4: Add the marker constant**

In `internal/instance/service.go`, add to the same `var`/`const` region as the `Err*` values (a new `const` block directly beneath them):

```go
// BackupMarkerNone is the one marker literal the core interprets. A volume
// declaring `backup: none` is never exported by a backup, on any path. Every
// other marker string stays opaque — the grammar (cadence, mode) belongs to a
// commercial BackupScheduler, and the core ascribes it no meaning.
const BackupMarkerNone = "none"
```

- [ ] **Step 5: Rewrite the export loop**

In `internal/instance/backup.go`, replace the block that runs from `vols, err := s.InstanceVolumes(...)` through the end of the `for _, v := range vols` loop with:

```go
	// Template metadata is needed before the volume loop: it carries both the
	// `none` veto and the declared exclude patterns. Declared exclude patterns
	// are keyed by the template's short volume name; InstanceVolumes returns
	// podman's full <template>-<slug>-<vol> names. Resolve through the same
	// volumeName() the pod manifest uses, so the two cannot drift.
	tpl, err := s.lookup(ctx, req.Host, req.Template)
	if err != nil {
		restart()
		return fail(fmt.Errorf("lookup template: %w", err))
	}
	type volMeta struct {
		marker  string
		exclude []string
	}
	declared := map[string]volMeta{}
	for _, v := range tpl.Meta.Volumes {
		declared[volumeName(req.Template, req.Slug, v.Name)] = volMeta{marker: v.Backup, exclude: v.Exclude}
	}

	vols, err := s.InstanceVolumes(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		restart()
		return fail(fmt.Errorf("list volumes: %w", err))
	}

	var bvols []store.BackupVolume
	for _, v := range vols {
		m := declared[v.Name]
		if m.marker == BackupMarkerNone {
			// State the absence rather than leaving it to be inferred from a
			// backup that silently lacks a volume.
			step("skip-volume", v.Name+" (backup: none)")
			continue
		}
		// Emitted BEFORE the export so a multi-minute volume shows the phase in
		// progress rather than nothing until it returns (#135).
		step("export-volume", v.Name)
		bv, err := s.backupVolume(ctx, req, v.Name, m.exclude)
		if err != nil {
			restart()
			return fail(fmt.Errorf("backup volume %q: %w", v.Name, err))
		}
		bvols = append(bvols, bv)
		step("export-volume-done", fmt.Sprintf("%s (%d bytes)", v.Name, bv.SizeBytes))
	}
```

- [ ] **Step 6: Run the tests**

Run: `cd ~/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -v`

Expected: PASS. If `TestBackup_HappyPath` fails on `assert.Contains(t, steps, "export-volume")`, that assertion is still correct — the step name is unchanged, only its position moved.

- [ ] **Step 7: Full suite and vet**

Run: `cd ~/projects/podman-api && make test && make vet`
Expected: all PASS, vet silent.

- [ ] **Step 8: Commit**

```bash
cd ~/projects/podman-api
git add internal/instance/backup.go internal/instance/service.go internal/instance/service_test.go internal/instance/backup_test.go
git commit -m "feat(backup): honour backup: none, emit export steps before exporting (#249, #135)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Volume scope through the seam, the job args and validation

**Files:**
- Modify: `extension/backup.go` — add `BackupOptions`, change `EnqueueBackup`, update doc comments
- Modify: `internal/instance/backup.go` — `BackupRequest.Volumes`, `CheckBackupable` signature + validation, scope filter in the export loop
- Modify: `internal/instance/service.go` — add `ErrInvalidBackupScope`
- Modify: `internal/api/errors.go` — map it to 400
- Modify: `internal/api/backups.go` — pass `nil` scope for now (Task 4 wires the body)
- Modify: `internal/backupctl/controller.go` — `Service` interface signature, `EnqueueBackup` signature, drop `none` from `backupMarkers`
- Test: `internal/instance/backup_test.go`, `internal/backupctl/controller_test.go`

**Interfaces:**
- Consumes: `instance.BackupMarkerNone` (Task 2).
- Produces:
  - `extension.BackupOptions{ Volumes []string }`
  - `extension.BackupController.EnqueueBackup(ctx context.Context, host, template, slug string, opts BackupOptions) (jobID string, err error)`
  - `instance.BackupRequest.Volumes []string` (JSON key `volumes`, `omitempty`)
  - `instance.Service.CheckBackupable(ctx context.Context, host, tmpl, slug string, volumes []string) error`
  - `instance.ErrInvalidBackupScope`
  - Task 4 consumes all of these; Task 6 consumes `extension.BackupOptions`.

- [ ] **Step 1: Write the failing tests**

Add to `internal/instance/backup_test.go`:

```go
func TestCheckBackupable_RejectsUndeclaredVolume(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	err := svc.CheckBackupable(context.Background(), "h1", "pg", "a", []string{"sitez"})
	assert.ErrorIs(t, err, ErrInvalidBackupScope)
}

func TestCheckBackupable_RejectsNoneMarkedVolume(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	err := svc.CheckBackupable(context.Background(), "h1", "pg", "a", []string{"logs"})
	assert.ErrorIs(t, err, ErrInvalidBackupScope)
}

func TestCheckBackupable_AcceptsEmptyAndDeclaredScope(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	ctx := context.Background()
	assert.NoError(t, svc.CheckBackupable(ctx, "h1", "pg", "a", nil))
	assert.NoError(t, svc.CheckBackupable(ctx, "h1", "pg", "a", []string{"data"}))
}

// TestBackup_ScopeNarrowsTheExportSet: with two exportable volumes present, a
// scope of {data} captures only data.
func TestBackup_ScopeNarrowsTheExportSet(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))

	req := newBackupReq()
	req.Volumes = []string{"data"}
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	assert.Equal(t, "pg-a-data", b.Volumes[0].Name)
}

// TestBackup_ScopedToAnAbsentVolumeSucceeds: a DECLARED volume that does not
// exist on the host yet is skipped, not an error — a brand-new instance must
// not fail its first backup for it.
func TestBackup_ScopedToAnAbsentVolumeSucceeds(t *testing.T) {
	svc, _, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	// Note: no SetVolumeData for pg-a-cache — it is declared but absent.

	req := newBackupReq()
	req.Volumes = []string{"cache"}
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State)
	assert.Empty(t, b.Volumes)
}

// TestBackup_InvalidScopeNeverStopsThePod: validation is synchronous and runs
// before the stop, so a scheduler bug or a typo cannot cost an outage.
func TestBackup_InvalidScopeNeverStopsThePod(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()

	req := newBackupReq()
	req.Volumes = []string{"nope"}
	err := svc.Backup(ctx, req, nil)
	assert.ErrorIs(t, err, ErrInvalidBackupScope)

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status, "an invalid scope must not stop the pod")
}
```

Add to `internal/backupctl/controller_test.go`:

```go
// TestBackupMarkers_DropsNoneMarkedVolumes: the core now interprets `none`, so
// a vetoed volume must not be projected to a scheduler that would then have to
// re-derive the same veto — and an instance whose ONLY marked volume is `none`
// is not backup-eligible at all.
func TestListBackupInstances_DropsNoneMarkedVolumes(t *testing.T) {
	svc := &fakeSvc{
		hosts: []config.Host{{ID: "h1"}},
		instances: map[string][]instance.Observed{
			"h1": {
				{Template: "web", Slug: "a"},
				{Template: "vetoed", Slug: "b"},
			},
		},
		templates: map[string]store.Template{
			"web": tmpl("web",
				render.Volume{Name: "sites", Backup: "s3; interval=24h"},
				render.Volume{Name: "logs", Backup: "none"},
				render.Volume{Name: "cache"}),
			// Every marked volume is vetoed: not backup-eligible at all.
			"vetoed": tmpl("vetoed", render.Volume{Name: "logs", Backup: "none"}),
		},
	}
	c := &Controller{Svc: svc}

	got, err := c.ListBackupInstances(context.Background())
	require.NoError(t, err)

	require.Len(t, got, 1, "an instance whose only marker is `none` is not eligible")
	assert.Equal(t, "web", got[0].Template)
	require.Len(t, got[0].Volumes, 1, "a `none` marker must not be projected")
	assert.Equal(t, "sites", got[0].Volumes[0].Name)
	assert.Equal(t, "s3; interval=24h", got[0].Volumes[0].Backup)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ ./internal/backupctl/ 2>&1 | head -40`

Expected: compile failure — `CheckBackupable` takes 4 args, `BackupRequest` has no `Volumes`, `ErrInvalidBackupScope` undefined. That is the correct first failure.

- [ ] **Step 3: Add the error**

In `internal/instance/service.go`, alongside the other `ErrBackup*` values:

```go
	ErrInvalidBackupScope  = errors.New("backup scope names a volume that cannot be backed up")
```

In `internal/api/errors.go`, add a case above the `ErrBackupsDisabled` case:

```go
	case errors.Is(err, instance.ErrInvalidBackupScope):
		return "invalid_backup_scope", http.StatusBadRequest, err.Error()
```

- [ ] **Step 4: Change the extension seam**

In `extension/backup.go`, add above `BackupController`:

```go
// BackupOptions narrows what a backup job captures. It is a struct rather than
// a bare parameter because further knobs are planned (a per-volume mode, an
// opaque instance id): growing a struct is additive, growing a parameter list
// breaks the interface again each time.
type BackupOptions struct {
	// Volumes lists the template's declared (short) volume names to snapshot,
	// e.g. ["sites"]. Empty means every declared volume not marked `none`.
	//
	// Naming a volume the template does not declare, or one marked `none`,
	// fails the call — it never silently degrades to a smaller backup.
	Volumes []string
}
```

Replace the `EnqueueBackup` declaration and its doc comment with:

```go
	// EnqueueBackup enqueues a backup job for one instance over the same path
	// the HTTP POST .../backup handler uses, returning the new job id. opts.Volumes
	// narrows what is captured; an empty scope means every declared volume not
	// marked `none`.
	//
	// It is authoritative for in-flight dedupe: if a backup job for this
	// instance is already queued, running, or reconciling, it enqueues nothing
	// and returns an empty jobID with a nil error. Dedupe is per instance, not
	// per volume — a scoped and an unscoped backup stop the same pod.
	EnqueueBackup(ctx context.Context, host, template, slug string, opts BackupOptions) (jobID string, err error)
```

And amend `BackupVolumeMarker`'s doc comment to record the one interpreted literal:

```go
// BackupVolumeMarker pairs a volume name with its raw backup marker. The core
// interprets exactly one literal — `none`, meaning never back this volume up,
// which is filtered out before projection so it never reaches a scheduler.
// Every other value is opaque and belongs to the commercial marker grammar.
```

- [ ] **Step 5: Thread the scope through the runner**

In `internal/instance/backup.go`:

```go
type BackupRequest struct {
	BackupID string   `json:"backup_id"`
	Host     string   `json:"host"`
	Template string   `json:"template"`
	Slug     string   `json:"slug"`
	// Volumes is the declared (short) volume names to capture. Empty means
	// every declared volume not marked `none`. Persisted in the job args so a
	// backup interrupted by a daemon restart reconciles with the scope it
	// started with.
	Volumes []string `json:"volumes,omitempty"`
}
```

Replace `CheckBackupable` with:

```go
// CheckBackupable runs the cheap synchronous validation the POST handler
// needs: known host, known template, stored spec present, blob store wired,
// and — when volumes is non-empty — that every named volume is declared by the
// template and not vetoed by a `none` marker.
//
// Scope validation is synchronous and upfront so a typo or a misconfigured
// scheduler fails the request outright rather than stopping a pod and
// producing a green, empty backup.
func (s *Service) CheckBackupable(ctx context.Context, host, tmpl, slug string, volumes []string) error {
	if s.blobs == nil {
		return ErrBackupsDisabled
	}
	t, err := s.lookup(ctx, host, tmpl)
	if err != nil {
		return err
	}
	if _, err := s.store.GetSpec(ctx, host, tmpl, slug); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return err
	}
	if len(volumes) == 0 {
		return nil
	}
	declared := make(map[string]string, len(t.Meta.Volumes))
	for _, v := range t.Meta.Volumes {
		declared[v.Name] = v.Backup
	}
	for _, name := range volumes {
		marker, ok := declared[name]
		if !ok {
			return fmt.Errorf("%w: %q is not declared by template %s", ErrInvalidBackupScope, name, tmpl)
		}
		if marker == BackupMarkerNone {
			return fmt.Errorf("%w: %q is marked `backup: none`", ErrInvalidBackupScope, name)
		}
	}
	return nil
}
```

Update `Backup`'s own call: `if err := s.CheckBackupable(ctx, req.Host, req.Template, req.Slug, req.Volumes); err != nil {`.

In the export loop from Task 2, add the scope filter — build the set just above the loop:

```go
	// Scope in declared short names, resolved through the same volumeName() the
	// pod manifest uses. Empty scope means "everything not vetoed".
	scope := make(map[string]bool, len(req.Volumes))
	for _, short := range req.Volumes {
		scope[volumeName(req.Template, req.Slug, short)] = true
	}
```

and inside the loop, directly after the `BackupMarkerNone` check:

```go
		if len(scope) > 0 && !scope[v.Name] {
			continue
		}
```

(No `skip-volume` step for an out-of-scope volume: the caller chose the scope and already knows. `skip-volume` is reserved for the veto, which the caller did not choose.)

- [ ] **Step 6: Update the two call sites**

`internal/api/backups.go` — `postBackup`, pass an explicit nil scope (Task 4 replaces it):

```go
	if err := h.svc.CheckBackupable(r.Context(), host, tmpl, slug, nil); err != nil {
```

`internal/backupctl/controller.go` — the `Service` interface method, `EnqueueBackup`, and `backupMarkers`:

```go
	CheckBackupable(ctx context.Context, host, template, slug string, volumes []string) error
```

```go
func (c *Controller) EnqueueBackup(ctx context.Context, host, template, slug string, opts extension.BackupOptions) (string, error) {
	inFlight, err := c.backupInFlight(ctx, host, template, slug)
	if err != nil {
		return "", err
	}
	if inFlight {
		return "", nil
	}
	if err := c.Svc.CheckBackupable(ctx, host, template, slug, opts.Volumes); err != nil {
		return "", err
	}
	req := instance.BackupRequest{
		BackupID: store.NewBackupID(), Host: host, Template: template, Slug: slug,
		Volumes: opts.Volumes,
	}
	args, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	job, err := c.Jobs.Enqueue(ctx, "backup", args, "")
	if err != nil {
		return "", err
	}
	return job.ID, nil
}
```

In `backupMarkers`, skip the veto:

```go
	for _, v := range t.Meta.Volumes {
		// `none` is the one marker the core interprets: the volume is never
		// backed up, so projecting it would only make a scheduler re-derive the
		// same veto — and an instance whose only marked volume is `none` is not
		// backup-eligible at all.
		if v.Backup == "" || v.Backup == instance.BackupMarkerNone {
			continue
		}
		markers = append(markers, extension.BackupVolumeMarker{Name: v.Name, Backup: v.Backup})
	}
```

- [ ] **Step 7: Run the tests**

Run: `cd ~/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ ./internal/backupctl/ ./internal/api/ -v 2>&1 | tail -40`

Expected: PASS. Fix any other `CheckBackupable`/`EnqueueBackup` call site the compiler names — including test fakes implementing the `backupctl.Service` interface.

- [ ] **Step 8: Full suite and vet**

Run: `cd ~/projects/podman-api && make test && make vet`
Expected: all PASS, vet silent.

- [ ] **Step 9: Commit**

```bash
cd ~/projects/podman-api
git add extension/backup.go internal/instance/ internal/backupctl/ internal/api/
git commit -m "feat(backup): volume-scoped backups through the extension seam (#249)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: The HTTP route accepts an optional scope

**Files:**
- Modify: `internal/api/backups.go` — `postBackup`
- Test: `internal/api/backups_test.go`

**Interfaces:**
- Consumes: `instance.BackupRequest.Volumes`, `instance.Service.CheckBackupable(..., volumes)` (Task 3).
- Produces: `POST /hosts/{host}/instances/{template}/{slug}/backup` accepting `{"volumes": ["data"]}`.

- [ ] **Step 1: Write the failing tests**

Note the route is `/backup`, **singular** — `POST /hosts/{host}/instances/{template}/{slug}/backup`. `backupReq` (a nil-body helper) and `postJSON` (from `internal/api/coverage_test.go`, same package) are both already available.

Add to `internal/api/backups_test.go`:

```go
// TestAPI_PostBackup_NoBodyIsUnscoped is the wire-compatibility lock: every
// client that exists today sends no body at all, and must keep meaning "every
// backupable volume".
func TestAPI_PostBackup_NoBodyIsUnscoped(t *testing.T) {
	srv, tok, _, mem, _ := newBackupSrv(t)
	ctx := context.Background()

	resp := backupReq(t, "POST", srv.URL+"/hosts/h1/instances/postgres/a/backup", tok)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var acc struct {
		JobID string `json:"job_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&acc))

	job, err := mem.GetJob(ctx, acc.JobID)
	require.NoError(t, err)
	var args instance.BackupRequest
	require.NoError(t, json.Unmarshal(job.Args, &args))
	assert.Empty(t, args.Volumes)
}

func TestAPI_PostBackup_ScopedBody(t *testing.T) {
	srv, tok, _, mem, _ := newBackupSrv(t)
	ctx := context.Background()

	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances/postgres/a/backup",
		`{"volumes":["data"]}`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var acc struct {
		JobID string `json:"job_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&acc))

	job, err := mem.GetJob(ctx, acc.JobID)
	require.NoError(t, err)
	var args instance.BackupRequest
	require.NoError(t, json.Unmarshal(job.Args, &args))
	assert.Equal(t, []string{"data"}, args.Volumes)
}

func TestAPI_PostBackup_InvalidScopeIs400(t *testing.T) {
	srv, tok, _, mem, _ := newBackupSrv(t)
	ctx := context.Background()

	for _, body := range []string{`{"volumes":["nope"]}`, `{"volumes":["logs"]}`} {
		resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances/postgres/a/backup", body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
		var eb ErrorBody
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&eb))
		_ = resp.Body.Close()
		assert.Equal(t, "invalid_backup_scope", eb.Code, body)
	}

	jobs, err := mem.ListJobs(ctx, store.JobFilter{Kind: "backup", Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, jobs, "a rejected scope must enqueue nothing")
}

func TestAPI_PostBackup_MalformedBodyIs400(t *testing.T) {
	srv, tok, _, _, _ := newBackupSrv(t)

	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances/postgres/a/backup", `{`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var eb ErrorBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&eb))
	assert.Equal(t, "invalid_request", eb.Code)
}
```

If `store.JobFilter` with an empty `State` does not list across all states in this codebase, assert instead that `mem.ListJobs` with `State: store.JobQueued` is empty — read `internal/store/memory.go`'s `ListJobs` before choosing.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestAPI_PostBackup -v`

Expected: the scoped and 400 cases FAIL (the handler ignores the body entirely today).

- [ ] **Step 3: Decode the optional body**

In `internal/api/backups.go`, replace the head of `postBackup` (from the `host, tmpl, slug :=` line through the `req := instance.BackupRequest{...}` line) with:

```go
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")

	// The body is optional: every client predating volume scoping sends none,
	// and an absent body must keep meaning "every backupable volume". io.EOF is
	// therefore the ordinary case, not an error.
	var body struct {
		Volumes []string `json:"volumes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_request", Message: "body must be a JSON object"})
		return
	}

	if err := h.svc.CheckBackupable(r.Context(), host, tmpl, slug, body.Volumes); err != nil {
		WriteError(w, err)
		return
	}
	req := instance.BackupRequest{
		BackupID: store.NewBackupID(), Host: host, Template: tmpl, Slug: slug,
		Volumes: body.Volumes,
	}
```

Add `"errors"` and `"io"` to the file's imports.

- [ ] **Step 4: Run the tests**

Run: `cd ~/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/ -run TestAPI_PostBackup -v`
Expected: PASS.

- [ ] **Step 5: Full suite and vet**

Run: `cd ~/projects/podman-api && make test && make vet`
Expected: all PASS, vet silent.

- [ ] **Step 6: Commit**

```bash
cd ~/projects/podman-api
git add internal/api/backups.go internal/api/backups_test.go
git commit -m "feat(api): optional volume scope on POST .../backup (#249)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: Documentation, PR and release tag

**Files:**
- Modify: `docs/wiki-drafts/Backing-up-and-Restoring.md`

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: a tagged OSS release for Task 6's `make bump`.

- [ ] **Step 1: Document the three behaviour changes**

Read `docs/wiki-drafts/Backing-up-and-Restoring.md` in full first, then add a section covering, in the reader's likely order of need:

1. `backup: none` is now a veto on every path — scheduled and manual alike. A volume with no `backup:` field is still backed up; only the explicit literal is a veto.
2. `POST .../backup` (singular) accepts an optional `{"volumes": ["sites"]}`. Names are the template's declared short names. An undeclared or `none`-marked name returns 400 `invalid_backup_scope` and stops no pod.
3. A restore now removes and recreates **only** the volumes its backup contains. Volumes outside the backup keep their live content. State plainly that this makes a scoped restore partial by design, and that a restore can no longer delete data it has no copy of.

Also state what has **not** changed: a snapshot still stops the pod, so a scoped backup shortens the outage in proportion to the bytes it drops but does not remove it.

- [ ] **Step 2: Full suite and vet one final time**

Run: `cd ~/projects/podman-api && make test && make vet && make build`
Expected: all PASS, vet silent, binary builds.

- [ ] **Step 3: Commit and push**

```bash
cd ~/projects/podman-api
git add docs/wiki-drafts/Backing-up-and-Restoring.md
git commit -m "docs: volume scoping, the none veto, and partial restore (#249)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
git push -u origin feat/volume-scoped-backup-249
```

- [ ] **Step 4: Open the PR**

```bash
forgejo pr create IoTReadyNext/podman-api \
  --title="feat(backup): volume-scoped backups and the none veto (#249, #135)" \
  --head=feat/volume-scoped-backup-249 --base=main \
  --body="Implements docs/superpowers/specs/2026-08-12-volume-scoped-backup-seam-design.md.

Closes #249. Fixes the #135 step-label half.

BREAKING: extension.BackupController.EnqueueBackup gains a BackupOptions
parameter. podman-api-pro's scheduler must be updated at the same bump.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

- [ ] **Step 5: Merge and tag**

After review and merge, tag a release from `main`. Determine the next patch version from `forgejo release list IoTReadyNext/podman-api` (or `git tag --sort=-v:refname | head -1`) and tag the increment. Confirm the tag reaches **public GitHub**, not only Forgejo — `make bump` in pro resolves it from GitHub via `GOPRIVATE`.

---

### Task 6: The commercial scheduler passes its snapshot scope

Without this the OSS work is inert: nothing ever supplies a scope.

**Files (in `~/projects/podman-api-pro`):**
- Modify: `go.mod` / `go.sum` via `make bump`
- Modify: whichever file implements `extension.BackupScheduler` and calls `EnqueueBackup` — locate with `grep -rn "EnqueueBackup" --include=*.go .`
- Test: that file's `_test.go` sibling

**Interfaces:**
- Consumes: `extension.BackupOptions`, the new `EnqueueBackup` signature (Task 3), at the tag from Task 5.
- Produces: nothing downstream.

- [ ] **Step 1: Bump the OSS dependency**

```bash
cd ~/projects/podman-api-pro
git checkout -b feat/volume-scoped-backup-249
make bump V=<the tag from Task 5 Step 5>
make build
```

Expected: `make build` **fails** to compile at the `EnqueueBackup` call site. That is the coordinated break working as designed.

- [ ] **Step 2: Write the failing test**

In the scheduler's test file, assert that enqueuing for an instance whose markers are `sites` (`s3; interval=24h`, i.e. `mode=snapshot` by default) and `archives` (`s3; mode=incremental`) passes `BackupOptions{Volumes: []string{"sites"}}` — the incremental volume is already covered continuously by the parquet-sync sidecar and must not be snapshotted a second time. Follow the fake `BackupController` pattern already in that file.

- [ ] **Step 3: Run it to verify it fails**

Run: `cd ~/projects/podman-api-pro && make test`
Expected: FAIL (compile error at the call site, then the assertion once it compiles).

- [ ] **Step 4: Pass the snapshot-marked volumes**

At the `EnqueueBackup` call site, collect the volume names whose parsed marker is `mode=snapshot` (the default when `mode` is absent) and pass them as `extension.BackupOptions{Volumes: names}`. Volumes parsed as `mode=incremental` are excluded — they are the parquet-sync sidecar's, not the snapshot runner's. A volume whose marker fails to parse is already logged and skipped by the existing parser; leave that behaviour alone.

- [ ] **Step 5: Run the tests**

Run: `cd ~/projects/podman-api-pro && make test && make vet && make build`
Expected: all PASS, vet silent, binary builds.

- [ ] **Step 6: Commit and open the PR**

```bash
cd ~/projects/podman-api-pro
git add -A
git commit -m "feat(scheduler): pass snapshot-marked volumes as the backup scope (#135)

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
git push -u origin feat/volume-scoped-backup-249
forgejo pr create IoTReadyNext/podman-api-pro \
  --title="feat(scheduler): volume-scoped backups (#135)" \
  --head=feat/volume-scoped-backup-249 --base=main \
  --body="Bumps the OSS dependency to pick up the volume-scoped EnqueueBackup seam
and passes only the mode=snapshot volumes.

The Frappe fleet's \`sites\` marker stays \`none\` — P1 shortens the snapshot
outage but does not remove it. Re-enabling those is P2's call.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

- [ ] **Step 7: Do NOT deploy or re-enable Frappe backups**

Deployment to engine-infra and any change to the Frappe templates' `sites` marker are explicitly out of scope. P1 ships the mechanism; P2 decides when the fleet uses it.

---

## Spec coverage

| Spec decision | Task |
|---|---|
| D1 — core learns only `none`; runner + marker projection | 2, 3 |
| D2 — `BackupOptions`, short names, request/job/API plumbing | 3, 4 |
| D3 — reject a bad scope before the stop | 3 |
| D4 — restore removes only what it can recreate | 1 |
| D5 — step before export, `export-volume-done`, `skip-volume` | 2 |
| Non-goal: outage, Frappe re-enable, #250 | 5 (documented), 6 Step 7 |
