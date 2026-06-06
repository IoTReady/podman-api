# On-Demand Volume Backup + One-Click Restore (#66) — Design

**Date:** 2026-06-06
**Issue:** #66 (tier/oss, slice-3)
**Status:** Approved design, pre-implementation

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

```go
type BlobStore interface {
    Put(ctx context.Context, key string) (io.WriteCloser, error) // streamed write
    Get(ctx context.Context, key string) (io.ReadCloser, error)
    Delete(ctx context.Context, key string) error
}
```

OSS impl `localDir`, rooted at `-backup-dir` (default
`<state-db-dir>/backups`). Writes go via temp-file + rename so partial
writes never look like complete backups.

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
6. `Start` the instance — in a deferred step, so a failed backup never
   leaves the instance down.

No second read-back verify at backup time: the manifest is built from the
same bytes written to the blob; real verification happens at restore.

### Restore flow (job kind `restore`)

1. Acquire the per-instance apply-lock.
2. Refuse unless the backup row is `complete` and blobs exist
   (`backup_not_restorable`).
3. `Stop` the instance; tear down containers (volumes cannot be removed
   while referenced); remove + recreate volumes.
4. For each volume: `Get` blob → `VolumeImport` → re-export →
   `buildManifest` → `firstDiff` against the stored manifest (exactly
   migrate's verify step).
5. Re-run the deploy machinery to recreate containers; start.
6. Verify failure ⇒ job fails *before* declaring success; instance is left
   stopped with a clear error (imported data may be partial — that is why
   we verify).

Data flows through the API server (as it already does for migrate), and
now also rests there via the blob store.

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
| `GET  /hosts/{host}/instances/{template}/{slug}/backups` | `instances:read` | list backups for the instance (paginated, newest first) |
| `POST /backups/{id}/restore` | `instances:write` | enqueue `restore` job → `{job_id}` |
| `DELETE /backups/{id}` | `instances:write` | synchronous: delete blobs, then row; 409 if a restore job is running against it |

Progress/steps via the existing `GET /jobs/{id}`.

## Error handling

New sentinels mapped in `internal/api/errors.go`:

| Sentinel | Status | Code |
|---|---|---|
| `ErrBackupNotFound` | 404 | `backup_not_found` |
| `ErrBackupNotRestorable` (state ≠ `complete`, or blob missing) | 422 | `backup_not_restorable` |
| `ErrBackupBusy` (delete during in-flight restore) | 409 | `backup_busy` |

Existing mappings reused: restore onto a missing instance → 404
`instance_not_found`; old podman host → 422 `host_version_unsupported`
(volume export/import is already gated by the 5.6.0 floor, #85).

Concurrency: the per-instance apply-lock serializes backup, restore,
migrate, and delete on the same instance.

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
