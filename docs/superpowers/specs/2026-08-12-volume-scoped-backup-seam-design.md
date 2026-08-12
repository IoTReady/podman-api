# Volume-scoped backup seam

**Date:** 2026-08-12
**Status:** approved
**Issues:** #249 (both halves), #135 (step labels) · pro #135, pro #153
**Programme:** P1 of `podman-api-pro`'s
`docs/superpowers/specs/2026-08-11-backup-programme-design.md`

## Problem

`extension.BackupController` offers no way to back up less than a whole
instance, and the snapshot runner behind it exports every declared volume
regardless of its marker.

`BackupController.EnqueueBackup`'s own doc comment says it enqueues a job
"snapshotting all of its **backup-marked** volumes". That is not what happens.
Measured on a completed job for `vedanta/frappe-otp/godesi`
(2026-08-11T01:17:59Z → 01:26:24Z):

| volume | marker | exported |
|---|---|---|
| `frappe-otp-godesi-sites` | `s3; interval=24h` | 1.84 GB |
| `frappe-otp-godesi-logs` | `none` | 88 MB |
| `frappe-otp-godesi-duckdb` | `s3; mode=incremental` | 622 MB |

2.55 GB exported against 1.84 GB actually snapshot-marked. `logs` is an explicit
"do not back this up" and was tarred anyway; `duckdb` already has a continuous
sidecar pushing it to the same bucket, so its 622 MB is written twice by two
mechanisms.

The whole export happens with the pod stopped — `frappe-otp/valj` (3.8 GB
`sites`) was down ~13 minutes, `godesi` ~8.5 — which is why the commercial side
set `sites` to `none` on both Frappe templates and left 14 production sites with
no scheduled off-host backup at all.

A third, smaller defect compounds it: `step("export-volume", name)` is emitted
*after* an export returns, so the entire stop window reads as dead air between
`stop` and the first sign of progress. A live 13-minute job is indistinguishable
from a hung one via the exact mechanism operators are told to use (#135).

## Goal

The runner honours the markers it is already given, and a caller that knows only
one volume needs snapshotting can say so. P1 does **not** eliminate the outage —
it narrows what the stop window has to cover and makes the doc comment true.
Taking the outage to zero is P2's `filesync` mode.

## Decisions

### D1 — The core learns exactly one marker literal: `none`

Today the seam documents that the core "ascribes no meaning to the marker string
beyond *non-empty == marked for backup*". This design breaks that, minimally and
deliberately: the exact literal `none` means **do not back this volume up**.
Every other string stays opaque and belongs to the commercial marker grammar.

That single exception is what turns `backup: none` from documentation into a
veto, and it is the smallest semantic footprint that closes #249's first half.
Two places honour it:

- `Service.Backup` skips `none`-marked volumes when building the export set.
- `backupctl.backupMarkers` stops projecting them, so an instance whose only
  marked volume is `none` drops off `ListBackupInstances` entirely rather than
  being handed to a scheduler that must then re-derive the same veto.

A volume declaring **no** `backup:` field at all is unaffected — still exported.
Only an explicit `none` is a veto. This matters: reading "unmarked" as "excluded"
would silently shrink every existing backup in the fleet, which is a data-loss
shape, not a scoping improvement.

The seam's doc comment is updated to state the one exception rather than left
claiming an opacity that is no longer true.

### D2 — Scope travels in an options struct

```go
// BackupOptions narrows what a backup job captures.
type BackupOptions struct {
    // Volumes lists the template's declared (short) volume names to snapshot,
    // e.g. ["sites"]. Empty means every declared volume not marked `none`.
    Volumes []string
}

EnqueueBackup(ctx context.Context, host, template, slug string, opts BackupOptions) (jobID string, err error)
```

A struct rather than a bare `volumes []string` parameter because more knobs are
already scheduled by the programme: P2 adds a mode, P3 adds the opaque instance
id. Growing a struct is additive; growing a parameter list is another breaking
change each time.

This **is** a compile-breaking change to a published extension interface. That is
intended — `podman-api-pro`'s scheduler fails to build at `make bump`, which is
the correct way for a contract change to surface, rather than a silently ignored
new field.

**Scope is expressed in declared short names** (`sites`), matching what
`BackupVolumeMarker.Name` already hands the scheduler. The core resolves them to
podman's full `<template>-<slug>-<volume>` names through the same `volumeName()`
the pod manifest uses, so the two cannot drift — the pattern #248 established for
exclude patterns.

Carried through three places:

- `instance.BackupRequest` gains `Volumes []string`, so a job's args survive a
  daemon restart and `ReconcileBackup` sees the same scope the run started with.
- `POST /hosts/{host}/instances/{template}/{slug}/backups` accepts an optional
  JSON body `{"volumes": ["sites"]}`. An absent or empty body means today's
  behaviour, so every existing client is untouched.
- `backupctl.Controller.EnqueueBackup` passes it straight through.

Dedupe is unchanged: `backupInFlight` still matches on the host/template/slug
triple, so a scoped and an unscoped backup for one instance still collide.
Correct — both stop the same pod.

### D3 — A bad scope is rejected before the pod stops

`CheckBackupable` gains the volume list and rejects a scope that names:

- a volume the template does not declare (typo, or a template edit that renamed
  it), or
- a volume marked `none`.

Both fail the synchronous POST with 400 and fail a scheduler's `EnqueueBackup`
call, so neither ever reaches a `Stop`. The alternative — intersecting the
request with what is exportable and backing up whatever survives — was rejected:
a scheduler misconfigured to name the wrong volume would then produce a fleet of
green, empty backups, which is #104's failure mode one level up.

One case is tolerated rather than rejected: a **declared** volume that does not
yet exist on the host. `InstanceVolumes` already skips those (a declared volume
may legitimately not have been created yet), and a backup of a brand-new instance
must not fail for it.

Asking for a `none`-marked volume is an error rather than an honoured veto so
that "I asked for X and got a backup without X" is impossible. A caller wanting
"everything backupable" passes an empty scope, which is exactly that.

### D4 — Restore stops tearing down what it cannot put back

This is the hazard scoping introduces, and it does not exist today.

`Restore` currently calls `Delete(..., DeleteOptions{PruneVolumes: true})`, which
removes **every** volume of the instance, then recreates only the volumes the
backup recorded. That is correct today only because every backup happens to
contain every volume. The moment a backup is scoped, restoring a `sites`-only
backup of `godesi` would delete `logs` and `duckdb` and never put them back.

The teardown becomes `PruneVolumes: false` plus an explicit removal of exactly
the volumes named in `b.Volumes`, immediately before each is recreated from its
blob. For a whole-instance backup the outcome is identical to today — it simply
stops being accidental. For a scoped one, unscoped volumes keep their live
content and the pod is re-applied around them.

The one behaviour genuinely lost: `PruneVolumes: true` would also have reaped a
volume the template declares but the backup lacks. Under the new rule such a
volume survives. That is the intended semantic — a restore may no longer delete
data it has no copy of.

No schema change. `store.Backup.Volumes` already records what a backup contains
and the API already exposes it, so "this backup covers `sites` only" is
answerable before restoring.

### D5 — The step trail shows the stop window and says what it skipped

- Emit `export-volume` **before** each export begins, not after. This is the
  #135 fix: the operator sees which volume the job is on for the duration rather
  than nothing until it finishes.
- Emit `export-volume-done` after, carrying the byte count.
- Emit `skip-volume` for each `none`-marked volume, so an absence from the backup
  is stated rather than inferred.

## Non-goals

- **The outage.** A `sites`-only snapshot still stops the pod for most of the
  8-13 minutes, because `sites` is the bulk of the bytes. P1 removes the
  `logs`/`duckdb` overhead (28% on `godesi`) and the double-copy; P2's `filesync`
  removes the stop.
- **Re-enabling the Frappe fleet's scheduled backups.** They stay `none` until
  P2. The stopgap on `dev` covers those 14 sites meanwhile.
- **#250** (promote a surviving hardlink whose exclude-dropped target left it
  dangling). Independent of scoping, and it needs buffering or a second pass in
  the riskiest function on the #248 branch. Its own PR.
- **Per-volume restore granularity beyond what a backup contains.** Restore's
  scope is the backup's scope; there is no route to restore a subset of a
  backup's own volumes.

## Components

| Unit | Change |
|---|---|
| `extension/backup.go` | `BackupOptions`; `EnqueueBackup` signature; doc comments state the `none` exception |
| `internal/instance/backup.go` | `BackupRequest.Volumes`; `CheckBackupable` validation; export-set construction honouring `none` + scope; scoped restore teardown; step trail |
| `internal/backupctl/controller.go` | pass `opts` through; drop `none` from `backupMarkers`; `Service` interface's `CheckBackupable` signature |
| `internal/api/backups.go` | optional request body, absent == unscoped |
| `podman-api-pro` (after tag) | scheduler passes its `mode=snapshot` volumes |

## Testing

TDD, per the repo's habit — each behaviour gets its failing test first.

- **Export set:** a template with `sites` (marked), `logs` (`none`) and an
  unmarked `cache` volume yields `{sites, cache}` unscoped; `{sites}` when scoped
  to it; and the `none` volume never appears under any scope.
- **Validation:** an undeclared name and a `none`-marked name each fail
  `CheckBackupable` and produce 400 from the handler, with the pod never stopped
  (assert against the fake's call log, not just the error).
- **Declared-but-absent volume:** scoped to it, the backup succeeds and records
  no blob for it.
- **Scoped restore:** a `{sites}` backup restored against an instance holding
  `{sites, logs}` removes and recreates only `sites`; `logs` is never passed to
  `VolumeRemove`. The whole-instance case is asserted byte-for-byte unchanged
  against the pre-P1 expectation.
- **Reconcile:** a scoped backup interrupted mid-run reconciles from its job args
  with the same scope.
- **Marker projection:** an instance whose only marked volume is `none` is absent
  from `ListBackupInstances`.
- **Step trail:** `export-volume` precedes the export call; `skip-volume` names
  each vetoed volume.
- **Wire compatibility:** a `POST /backups` with no body behaves exactly as
  before.

## Risks

| Risk | Mitigation |
|---|---|
| Breaking the published `EnqueueBackup` signature strands pro until it is bumped. | Intended and coordinated: OSS tag, then `make bump` in pro in the same session. The break is at compile time, not runtime. |
| Reading `none` in the core erodes the seam's "markers are commercial policy" boundary. | Exactly one literal, stated in the doc comment, with every other string still opaque. Any second literal is a design change, not an increment. |
| A scoped backup's restore is partial and an operator may not notice. | `b.Volumes` is already in the API response; D5's step trail names skipped volumes at backup time; D4 guarantees a partial restore can never *delete* what it cannot restore. |
| The scoped teardown misses a case `PruneVolumes: true` covered. | The lost case is enumerated in D4 and is the intended semantic; the whole-instance path is locked by a test asserting the pre-P1 volume set. |
