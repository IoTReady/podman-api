# Backing up and Restoring

On-demand volume backup and in-place restore for pod instances managed by
podman-api. Introduced in #66 (OSS primitive).

---

## What a backup is

A **backup** captures every Podman volume attached to one instance at the
same stopped moment — a consistent, SQLite-safe snapshot:

1. The instance is stopped.
2. Each volume is exported as a plain uncompressed tar (`podman volume export`),
   streamed through the API server, and written to a local artifact file.
3. A sha256 manifest (path → hash + size for every file in the tar) is built
   from the same bytes in one pass and stored in the state DB alongside the
   backup record.
4. The instance is restarted — **only if it was running before the backup
   began**. A deliberately-stopped instance stays stopped after the backup
   completes or fails.

**Downtime note.** The instance is unavailable for the duration of the export
(step 2). Export time is proportional to the total volume data. Live/zero-
downtime backup is out of scope for the OSS tier.

**Race warning.** Starting the instance manually while a backup job is running
can capture a live (possibly inconsistent) volume export — let backup jobs
finish before issuing lifecycle actions.

Parameters, secrets, and domains are **not** captured — the backup is volumes
only. The container image reference at backup time is recorded as an
informational hint. Restore re-applies the instance's current spec (whatever
parameters and secrets are in the state store at restore time).

---

## Where artifacts live

Artifact files are written to the local filesystem of the API server under
`-backup-dir` (flag; default `<state-db dir>/backups`).

Layout:

```
<backup-dir>/<host>/<template>/<slug>/<backup-id>/<volume-name>.tar
```

Example:

```
/var/lib/podman-api/backups/prod-1/postgres/my-db/bk_01J4XY.../data.tar
```

Each `.tar` file is written via a temp file (`os.CreateTemp`) and renamed
atomically into place only when the write is clean (`Commit`). A partial write
is never visible as a complete backup. A process crash during export leaves a
`.tmp-*` file in the backup's directory; it is cleaned up automatically when
the backup is deleted (`DeleteAll` walks the whole directory prefix).

**sha256 manifests are stored in the state DB** (the `backups` table, per-volume
JSON field), not in the artifact files. This means:
- A hand-deleted artifact file does not corrupt the DB record — the backup
  transitions to `backup_not_restorable` on the next restore attempt.
- Restore verifies the imported data against the stored manifest, which it
  does not trust the artifact to supply.

---

## Requirements

- podman-api with `-state-db` set (always on in practice — the template
  catalog requires it).
- Podman **>= 5.6.0** on every managed host (`podman volume export/import`
  was stabilised there; the daemon enforces this as a boot-time preflight,
  per #85).
- Disk space on the API server host: roughly equal to the total uncompressed
  volume data per backup, multiplied by however many backups you keep. No
  automatic pruning — operator owns the disk.

---

## API walk-through

All requests require a bearer token with the appropriate scope.

### Trigger a backup

```sh
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  https://podman-api.example.com/hosts/prod-1/instances/postgres/my-db/backup
```

Scope: `instances:write`

The backup is **enqueued as an async job**. The response carries both the job
ID (for polling) and the backup ID (available immediately, before the job runs):

```json
{
  "job_id": "01J4XY...",
  "backup_id": "bk_01J4XY..."
}
```

Poll for completion:

```sh
curl -s -H "Authorization: Bearer $TOKEN" \
  https://podman-api.example.com/jobs/01J4XY...
```

A completed job looks like:

```json
{
  "id": "01J4XY...",
  "kind": "backup",
  "state": "succeeded",
  "steps": [
    {"name": "load",         "detail": "prod-1/postgres/my-db"},
    {"name": "stop",         "detail": "prod-1"},
    {"name": "export-volume","detail": "data"},
    {"name": "restart",      "detail": "prod-1"},
    {"name": "complete",     "detail": "bk_01J4XY..."}
  ]
}
```

### List backups

```sh
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://podman-api.example.com/hosts/prod-1/instances/postgres/my-db/backups?limit=10"
```

Scope: `instances:read`

Returns newest-first. `?limit=` is the only pagination parameter; absent or
`<=0` defaults to 100, clamped at 1000 maximum:

```json
{
  "backups": [
    {
      "id":       "bk_01J4XY...",
      "host":     "prod-1",
      "template": "postgres",
      "slug":     "my-db",
      "state":    "complete",
      "image":    "docker.io/library/postgres:16",
      "volumes":  [{"name": "data", "size_bytes": 52428800}],
      "created":  "2026-06-06T10:00:00Z",
      "finished": "2026-06-06T10:01:23Z"
    }
  ]
}
```

`state` is one of `creating`, `complete`, or `failed`. Only `complete` backups
are restorable.

### Restore from a backup

```sh
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  https://podman-api.example.com/backups/bk_01J4XY.../restore
```

Scope: `instances:write`

The restore is enqueued as an async job:

```json
{"job_id": "01J4ZZ..."}
```

Poll `GET /jobs/01J4ZZ...` for progress. A successful restore job steps
through: `load` → `teardown` → `restore-volume` (one step per volume) →
`apply` → `verify`.

The endpoint validates synchronously before enqueuing:
- The backup exists and is in `complete` state.
- The backup's host is known and not draining (a 423 is returned if it is).
- The instance spec exists in the state store.

A draining host is refused **synchronously** (before teardown) so the job
cannot be left in a half-restored state on a host that is being evacuated.

### Delete a backup

```sh
curl -s -X DELETE \
  -H "Authorization: Bearer $TOKEN" \
  https://podman-api.example.com/backups/bk_01J4XY...
```

Scope: `instances:write`

Synchronously removes the artifact files (the whole `<backup-id>/` directory),
then the DB row. Returns `204` on success.

Returns `409 backup_busy` if a backup job **or** a restore job targeting this
backup is currently queued, running, or reconciling.

---

## Admin UI flow

The instance detail page (`/ui/hosts/{host}/instances/{template}/{slug}`)
shows a **BACKUPS** section with:

- **Back up now** button — triggers the confirmation dialog ("The instance is
  stopped for the duration of the backup"), then enqueues a backup job and
  displays a notice banner with the job ID.
- A list of existing backups (ID, timestamp, state, image hint, per-volume
  size).
  - **Restore** button (only on `complete` rows) — triggers the confirmation
    dialog ("This stops the instance and OVERWRITES its current data"), then
    enqueues a restore job and re-renders the page with a notice.
  - **Delete** button — removes the backup after a browser confirm prompt.

All UI actions are HTMX requests to `/ui/...` endpoints that mirror the API
behaviour (same validation, same job enqueue).

---

## Restore semantics

Restore is **in-place** and **destructive**:

1. The instance is stopped.
2. The pod and all its volumes are torn down (`Delete` with `PruneVolumes`).
   Per-instance and host-scoped secrets are kept — `Apply` re-pushes them
   from the stored spec.
3. Each volume is recreated and imported from the backup's artifact file.
4. The content of every restored volume is **verified** against the sha256
   manifest stored in the DB. Verification always runs (unlike migrate, where
   it can be disabled). A mismatch fails the job before declaring success.
5. `Apply` re-runs the current spec (current parameters, secrets, domains)
   to recreate containers and start the instance.
6. The job waits until every container is `Running` and every declared
   healthcheck reports `healthy` before succeeding.

**There is no rollback.** A failure after step 2 (teardown) leaves the
instance **down** with volumes partially restored. The spec row is
preserved — the restore can be retried by submitting another `POST
/backups/{id}/restore` request. The job error names the failed step so you
know which volume or which apply phase to investigate.

The instance is left down (not auto-restarted) on failure. This is
intentional: an import error or verify mismatch means the data is suspect;
bringing the instance up against suspect data would hide the problem.

---

## Deleting backups: busy gate

`DELETE /backups/{id}` returns `409 backup_busy` if any of the following
active jobs targets the same backup ID:

- A `backup` job (the backup is still being written)
- A `restore` job (a restore is in progress from this backup)

The gate is job-based, not row-state-based. A crashed daemon can leave a
`creating` row with no live job, and that row must remain deletable. After the
next boot the row is failed by the boot reconciler and no active job references
it, so `DELETE` proceeds normally.

---

## Failure and interruption semantics

### Backup interrupted mid-export (daemon crash or SIGTERM)

The job runner marks in-flight jobs `failed` at boot. The `backup` kind has
a reconciler (`ReconcileBackup`) that runs at boot for any `creating`-state
backup row:

1. Marks the row `failed` (CAS — if the row is already `complete`, work
   finished and only the job's terminal write was lost, so no cleanup is
   needed).
2. Calls `DeleteAll` on the backup's artifact prefix to remove any partial
   `.tar` or `.tmp-*` files.
3. Attempts to restart the instance (unconditionally — post-crash the prior
   run-state is unknowable; reconcile errs toward availability). A
   deliberately-stopped instance interrupted mid-backup **may come back
   running** after a daemon crash.
4. If the host is no longer in the config, the backup is still marked failed
   and partial blobs cleaned; the restart is skipped and the reconciler
   resolves terminal (no retry loop).

### Restore interrupted mid-restore (daemon crash or SIGTERM)

The `restore` kind does **not** have an automated reconciler. The boot runner
marks the job `failed`. The operator re-runs the restore by submitting a new
`POST /backups/{id}/restore`. The blob is unmodified and the spec was
preserved, so the retry is safe and idempotent.

---

## Out of scope (OSS v1)

The following are **not implemented** in this release and are planned for the
commercial tier (#107) or future slices:

- **Scheduled backups** — no keep-last-N, no cron-triggered backups. Trigger
  via API or UI only.
- **Retention policy** — no automatic pruning. Operator manages disk.
- **Offsite / S3 targets** — artifacts are local to the API server filesystem
  only. The `BlobStore` interface is the seam where an S3/offsite backend
  slots in (#107).
- **PITR / Litestream-grade replication** — no continuous or incremental
  backup. Each backup is a full stop-and-export snapshot.
- **Restore to a different host** — restore requires the instance on its
  original host. DR restore-to-another-host is a possible follow-on.
