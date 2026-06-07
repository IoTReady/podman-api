# On-Demand Volume Backup + One-Click Restore (#66) — Design

**Date:** 2026-06-06
**Issue:** #66 (tier/oss, slice-3)
**Status:** Implemented — amendments recorded below where implementation diverged from the original plan.

## Scope

OSS on-demand primitive only (per the 2026-06-06 open-core split on #66):

- `POST` backup of an instance's volumes + one-click in-place restore,
  verified with the existing sha256 manifest machinery (same as
  migrate/evacuate).
- **Out of scope** (commercial, #107/#68): scheduled backups, S3/offsite
  targets, retention policy, PITR/Litestream-grade replication.

## Decisions

| Question | Decision |
|---|---|
| Artifact storage | Pluggable `BlobStore` interface; one OSS impl: local directory on the API server (`-backup-dir`). The interface is the seam where #107's S3 backend slots in. |
| Granularity | **Per-instance**: one backup captures *all* volumes of an instance at the same stopped moment. Volumes are separate artifacts internally. |
| Snapshot consistency | **Stop → export → restart.** Guaranteed-consistent (SQLite-safe); reuses migrate's quiesce pattern. Live/zero-downtime backup is deliberately left to the commercial PITR tier. |
| Restore scope | **In-place only**: the instance must exist on its original host. DR restore-to-another-host is a possible follow-up, not OSS v1. |
| Lifecycle | **Manual list/delete only.** No automatic pruning, no keep-last-N — retention *policy* is commercial (#107). Operator owns the disk. |
| Spec capture | **Volumes only.** Parameters/secrets/domains are not captured; restore keeps the current spec. The image ref at backup time is recorded as an informational hint. |

## Architecture

Two new job kinds — `backup` and `restore` — registered with the existing
jobs runner (`internal/jobs`), each with a reconciler so interrupted runs
fail clean and restart the instance if it was left stopped.

New package `internal/backup`:

### BlobStore

**Amendment:** `Put` returns a `BlobWriter` (not `io.WriteCloser`); `Delete`
(per-key) is replaced by `DeleteAll(ctx, prefix)` which removes every blob
under the directory-like prefix in one call. `Get` returns an error satisfying
`errors.Is(err, fs.ErrNotExist)` for missing blobs. The actual interface as
implemented:

```go
type BlobWriter interface {
    io.Writer
    Commit() error  // makes the blob visible atomically; only call on clean write
    Abort() error   // discards the temp file; key stays absent
}

type BlobStore interface {
    Put(ctx context.Context, key string) (BlobWriter, error)
    Get(ctx context.Context, key string) (io.ReadCloser, error)
    // DeleteAll removes every blob under the directory-like prefix (no-op if absent).
    DeleteAll(ctx context.Context, prefix string) error
}
```

The two-phase write (`Commit`/`Abort`) is the contract that a failed backup
never leaves a partial blob that looks complete. Only `Commit` makes the blob
visible; a crash or explicit `Abort` leaves no visible artifact.

`DeleteAll` replaces per-key `Delete` because a single prefix call covers the
whole backup artifact directory including any `.tmp-*` partial files left by a
process crash — no need to enumerate individual keys.

OSS impl `LocalDir` (`internal/backup`), rooted at `-backup-dir` (default
`<state-db-dir>/backups`). Writes go via `os.CreateTemp` in the key's
directory, then `os.Rename` into place. `Commit` fsyncs the file, closes it,
renames it, and then fsyncs the parent directory so the rename itself is
durable across a crash immediately after it.

### Backup flow (job kind `backup`)

0. The backup ID is generated at enqueue time and passed in the job args —
   that is how `POST .../backup` can return `{job_id, backup_id}` before
   the job runs.
1. Acquire the per-instance apply-lock (serializes against
   restore/migrate/delete on the same instance).
2. Insert `backups` row in state `creating`.
3. `Stop` the instance.
4. For each volume: `VolumeExport` teed into (a) the blob store and
   (b) `buildManifest` (`internal/instance/manifest.go`) in one pass.
5. Update the row: per-volume manifests + sizes, state `complete`.
6. `Start` the instance — **only if it was running before the backup began.**
   A deliberately-stopped instance stays stopped after a successful (or failed)
   backup. This is different from the boot reconciler (see below).

**Amendment — restart semantics:** the design said "restart in a deferred
step"; the implementation makes it conditional. `Backup` records `wasRunning`
before step 3 and only calls `Start` if that flag is true. This preserves the
operator's intent when backing up an already-stopped instance.

No second read-back verify at backup time: the manifest is built from the
same bytes written to the blob; real verification happens at restore.

### Restore flow (job kind `restore`)

1. `CheckRestorable` runs synchronously at enqueue time (and again under
   the lock inside the job): validates backup row is `complete`, host is
   known and **not draining**, instance spec exists. The drain check is
   upfront so a draining host can't fail the job after teardown. (**Amendment.**)
2. Acquire the per-instance apply-lock.
3. Re-check `CheckRestorable` under the lock (guards against a concurrent
   delete racing the lock acquire).
4. `Delete(PruneVolumes: true)` the instance — tears down containers and
   volumes. Per-instance secrets are kept; `Apply` below re-pushes them
   from the spec. A missing pod is tolerated (already-gone is idempotent).
5. For each volume: create volume → `Get` blob → `VolumeImport` → re-export
   → `buildManifest` → `firstDiff` against the stored manifest (exactly
   migrate's verify step). Verify always runs regardless of
   `-migrate-verify-volumes` — blobs have no live source to compare against.
6. Re-`Apply` the current spec to recreate containers and start the
   instance; wait for it to become healthy.
7. **Post-teardown failure handling:** if any step after teardown (5 or 6)
   fails, the caller re-persists the spec row on a detached context before
   returning the error. The teardown above deleted the spec row; without
   this re-persist a failed restore would leave the instance spec-less and
   `CheckRestorable` would refuse to retry (it requires the spec). This
   preserves desired state without implementing rollback. (**Amendment.**)
8. Verify failure ⇒ job fails before declaring success; instance is left
   down with the spec preserved so the restore is retryable.

Data flows through the API server (as it already does for migrate), and
now also rests there via the blob store.

## Reconciler behaviour

**Amendment — restore job is NOT reconcilable.**

The spec said both job kinds would have reconcilers. Only `backup` has one.
`restore` is not registered as a reconcilable kind:

- An interrupted **backup** row is in `creating` state. The reconciler marks
  it `failed`, calls `DeleteAll` to remove any partial blobs, and restarts
  the instance unconditionally (post-crash the prior run-state is
  unknowable; errs toward availability — a deliberately-stopped instance
  interrupted mid-backup may come back running). `resolved=false` only when
  the host is unreachable; it is retried on the next sweep.

- An interrupted **restore** job fails via boot recovery (the jobs runner
  marks it `failed` at start). The operator re-runs the restore by POSTing
  a new restore job — this is safe because the blob is unmodified and the
  spec was preserved. The restore job is idempotent from the blob (teardown
  is idempotent; `DeleteAll` is idempotent). There is no automated reconcile
  to avoid ambiguity about which volumes were partially written.

In `buildJobRegistry` (cmd/podman-api/main.go):
- `jobs.Registry` includes both `"backup"` and `"restore"` handlers.
- `jobs.Reconcilers` includes `"backup"` only (no `"restore"` entry).

## Data model

New table in the state DB (`internal/store/sqlite.go` `schemaSQL`,
`CREATE TABLE IF NOT EXISTS` like the rest):

```sql
CREATE TABLE backups (
  id        TEXT PRIMARY KEY,        -- backup ID, e.g. bk_<ulid-ish>
  host      TEXT NOT NULL,
  template  TEXT NOT NULL,
  slug      TEXT NOT NULL,
  state     TEXT NOT NULL,           -- creating | complete | failed
  volumes   TEXT NOT NULL,           -- JSON: [{name, size_bytes, manifest}]
  image     TEXT NOT NULL DEFAULT '',-- image ref at backup time (informational)
  created   INTEGER NOT NULL,
  finished  INTEGER
);
CREATE INDEX IF NOT EXISTS backups_instance ON backups(host, template, slug);
```

- Per-volume **manifests live in the row** (JSON `path → {sha256, size,
  type}`), not in the blob store: metadata survives hand-deleted blob
  files, and restore verifies without trusting the artifact it is
  verifying.
- `state` makes half-written backups visible and non-restorable; the
  reconciler marks interrupted `creating` rows `failed`.
- Rows are **not** deleted when the instance is deleted — backups outlive
  instances by design (though OSS restore needs the instance redeployed
  first). `DELETE /backups/{id}` removes blobs, then the row.

**Amendment — schema migration:** the `backups` table and `backups_instance`
index are added as the **user_version 7** migration step in `migrateSchema`
(`internal/store/sqlite.go`). A fresh database gets it via `schemaSQL` (the
`CREATE TABLE IF NOT EXISTS` catch-all); an existing database at v6 or below
receives it via the incremental migration path. The migration is idempotent
(`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`).

### Blob layout

```
<backup-dir>/<host>/<template>/<slug>/<backup-id>/<volume-name>.tar
```

Plain uncompressed tar — what `VolumeExport` emits. No recompression CPU;
streams symmetric with import.

## API

Existing route/guard patterns (`internal/api/router.go`):

| Route | Scope | Behavior |
|---|---|---|
| `POST /hosts/{host}/instances/{template}/{slug}/backup` | `instances:write` | enqueue `backup` job → `{job_id, backup_id}` |
| `GET  /hosts/{host}/instances/{template}/{slug}/backups` | `instances:read` | list backups for the instance, newest first |
| `POST /backups/{id}/restore` | `instances:write` | enqueue `restore` job → `{job_id}` |
| `DELETE /backups/{id}` | `instances:write` | synchronous: delete blobs, then row; 409 if busy |

Progress/steps via the existing `GET /jobs/{id}`.

**Amendment — list pagination:** the list endpoint takes `?limit=` only
(absent or `<=0` → default 100, values above 1000 clamped to 1000, newest
first). No cursor. The cursor-based pagination spec was not implemented.

**Amendment — DELETE busy gate:** the 409 is returned if a **backup** job OR a
**restore** job targeting the same backup ID is in flight (not just restore).
The gate is job-based, not row-state-based — a crashed daemon can leave a
`creating` row with no live job, and that row must stay deletable.
`BackupDeletable` checks `JobQueued | JobRunning | JobReconciling` states for
both job kinds.

## Error handling

New sentinels mapped in `internal/api/errors.go`:

| Sentinel | Status | Code |
|---|---|---|
| `ErrBackupNotFound` | 404 | `backup_not_found` |
| `ErrBackupNotRestorable` (state ≠ `complete`, or blob missing) | 422 | `backup_not_restorable` |
| `ErrBackupBusy` (delete during in-flight backup **or** restore) | 409 | `backup_busy` |
| `ErrBackupsDisabled` (blob store not wired, e.g. embedded without `SetBlobStore`) | 501 | `not_implemented` |

**Amendment:** `ErrBackupBusy` covers both a backup job and a restore job in
flight (the spec said "restore" only). See the DELETE discussion above.

Existing mappings reused: restore onto a missing instance → 404
`instance_not_found`; draining host → 423 `host_draining`; old podman
host → 422 `host_version_unsupported` (volume export/import is already
gated by the 5.6.0 floor, #85).

Concurrency: the per-instance apply-lock (`migrateLock`) serializes backup,
restore, migrate, and delete on the same instance.

## Admin UI

Instance detail page (`internal/ui`):

- **Back up now** button → enqueues, links to the job detail page.
- Backups list (id, created, image hint, size, state) with per-row
  **Restore** (confirm dialog — "overwrites current data") and **Delete**
  buttons.
- New `handlers_backups.go`, same htmx pattern as existing handlers.

## Testing

TDD throughout, per house style:

- **Unit:** blob store (temp-file/rename atomicity; partial writes
  invisible), backup/restore handlers against the fake podman client
  (the fake already has `VolumeExport`/`VolumeImport`), manifest-verify
  failure path, reconciler marks interrupted backups `failed` and
  restarts the instance.
- **Store:** backups table CRUD + state transitions.
- **API:** route/error-mapping tests in the existing table-driven style.
- **Integration** (`make test-integration`): real round-trip — deploy,
  write data, backup, mutate data, restore, assert original data is back
  and the instance is running.

## Existing machinery reused

| Component | Location |
|---|---|
| Volume export/import | `internal/podman/real.go` (VolumeExport/VolumeImport) |
| Manifest build/verify | `internal/instance/manifest.go` (`buildManifest`, `firstDiff`) |
| Quiesce + verify pattern | `internal/instance/migrate.go` |
| Jobs runner, steps, reconcilers | `internal/jobs`, `internal/store/jobs.go` |
| State DB schema | `internal/store/sqlite.go` |
| Error classification | `internal/api/errors.go` |
| Version floor (5.6.0) | `internal/podman/version.go` (#85) |
| Volume `Backup` meta field | `internal/render/meta.go` (placeholder, unused) |
