# Backing up and Restoring

On-demand volume backup and in-place restore for pod instances managed by
podman-api. Introduced in #66 (OSS primitive).

---

## What a backup is

A **backup** captures the Podman volumes attached to one instance — by
default every declared volume not marked `backup: none`, or a narrower
explicit scope naming a subset of them — at the same stopped moment: a
consistent, SQLite-safe snapshot:

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

## Excluding paths from a volume's backup

A template may declare glob patterns on a volume that keep matching paths out
of that volume's backup tar entirely:

```yaml
volumes:
  - name: sites
    backup: "s3; interval=24h"
    exclude:
      - "*/private/backups/**"
```

Patterns are [doublestar](https://github.com/bmatcuk/doublestar/v4) globs —
`**` spans directory separators — matched against each tar entry's cleaned
path relative to the volume root. A template with a pattern that fails any of
the following checks is rejected at registration, not at backup time, since
the only runtime signal for a pattern that matches nothing is a silent
`excluded.entries: 0` in a backup row nobody reads:

- Must be relative: no leading `/`.
- No `..` segment.
- Must already be `path.Clean`-equivalent to itself: no leading `./`, no
  trailing `/`, no internal `//`. Patterns are matched against `path.Clean`ed
  tar entry names, so an unclean pattern can never match anything.
- For a `/**`-suffixed pattern, the stripped prefix must not itself end in a
  wildcard segment (`*` or `**`) while also containing a `**` segment
  somewhere in it (e.g. `a/**/**`, `**/**`, `**/*/**`, `a/**/*/**`). In that
  shape the prefix matches every entry the full pattern matches, so the
  directory-contents rescue below fires unconditionally and the whole
  pattern silently drops nothing at all, files included. `a/**`, `a/*/**`,
  and `**/b/**` are all fine — none of their stripped prefixes end in a
  wildcard segment while also containing a `**`. A bare `**` (no `/**`
  suffix to strip) is unaffected by this check and legitimately matches,
  and so drops, every entry.

A pattern never drops a directory's own tar entry, no matter which form it
takes or how it matches — only files and links are ever removed. A directory
can be emptied but not removed: `*/private/backups/**` drops everything
*under* `private/backups`, so it restores as an empty directory, and naming
the directory outright (`*/private/backups`) is a no-op — the directory entry
still ships, exactly as if no pattern had matched it. This is deliberate:
restore recreates a directory implicitly from the paths beneath it, so a tar
that omitted a directory entry its children still need would produce a
re-export with a key the stored manifest lacks, and that failure surfaces
only in restore's integrity check — after the instance has already been torn
down for the restore.

A hardlink whose target was excluded is dropped along with it, otherwise the
archive would fail to import. Symlinks are not resolved this way: a symlink
pointing at an excluded path survives in the backup and comes back dangling,
exactly as it would on the source filesystem.

**Exclusion applies to backups only.** Instance rename, host migration, and
volume copy always move every byte — they remove the source once the copy
lands, so nothing can be safely left behind.

What each pattern set actually excluded is recorded per backup and surfaced
in the [list-backups API](#list-backups) response (`excluded.entries: 0`
against a non-empty pattern list means the patterns matched nothing that
backup — usually a stale or misspelled pattern, not a failure; a new
instance legitimately has nothing to exclude yet).

---

## Scoping a backup to specific volumes

`POST .../backup` accepts an optional JSON body naming which of the
template's declared volumes to capture:

```sh
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"volumes": ["sites"]}' \
  https://podman-api.example.com/hosts/prod-1/instances/frappe-otp/acme/backup
```

Names are the template's declared **short** volume names (`sites`, not the
resolved `frappe-otp-acme-sites` podman name). An absent body means "every
backupable volume" — the same behaviour as before this existed, so no
existing caller has to change.

Scope is validated **synchronously, before anything is enqueued**: naming a
volume the template doesn't declare, or one the template marks `backup:
none`, returns `400` with `{"code": "invalid_backup_scope", ...}` and stops
no pod. A scoped backup does not silently degrade to a smaller one on a typo
— it fails the request outright.

Three shapes are deliberately **not** read as "back up everything":

- `{"volumes": []}` — an explicitly empty scope is rejected
  (`invalid_backup_scope`). Omit the body entirely to mean "everything".
- `{"volume": ["sites"]}` — an unknown field (the singular typo, say) is
  rejected `400 invalid_request` rather than decoding to an empty scope.
- An explicit scope that names only volumes which do not exist on the host
  fails the **job**: this is knowable only once the run resolves what exists,
  so the backup row is marked `failed`, any partial artifacts are removed and
  the instance is restarted. A caller that asked for something specific and
  got nothing must not be handed a green backup row. (An **unscoped** backup
  of an instance whose volumes have not been created yet still succeeds, with
  an empty `volumes` list — that is a brand-new instance's first backup.)

**This does not shrink the outage to zero.** A snapshot backup still stops
the whole pod for the duration of the export — scoping only reduces how many
bytes are copied while it's down, in proportion to what you drop. Excluding
a large, rarely-changed volume from the scope shortens the stop; it does not
remove it.

### The `none` veto

A template volume can declare `backup: none` to opt permanently out of being
backed up, on every path — an explicit scope in the POST body, the default
"every volume", and a commercial scheduler's cadence-driven trigger all
honour it identically. It is the **one** marker literal the core itself
interprets; every other non-empty value (`s3; interval=6h`, a Litestream DB
path, …) is opaque grammar owned by the commercial tier.

**Only the literal `none` vetoes.** A volume with no `backup:` field at all
is not opted out — it is still backed up in full by an unscoped request; the
absence of a marker carries no meaning to the core beyond "no commercial
scheduler will pick this volume up on its own." Don't read a missing
`backup:` field as "excluded" — that's what `none` is for.

**The spelling is exact, and enforced at registration.** The comparison is
plain string equality everywhere it is consumed, so `None`, `NONE` or
`"none "` would veto *nothing* — silently, with the volume exported into
every blob and no error to read. Template registration therefore **rejects**
a marker that differs from `none` only by case or surrounding whitespace,
naming the exact literal required. It is not quietly corrected: an author who
wrote `None` believed they were vetoing a volume and is told they were not.

A vetoed volume named explicitly in a scope request is rejected at the door
(`invalid_backup_scope`, above). A vetoed volume left out of an unscoped
request is simply skipped, and the job's step trail says so — see below.

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
    {"name": "load",              "detail": "prod-1/postgres/my-db"},
    {"name": "stop",              "detail": "prod-1"},
    {"name": "export-volume",     "detail": "data"},
    {"name": "export-volume-done","detail": "data (52428800 bytes)"},
    {"name": "restart",           "detail": "prod-1"},
    {"name": "complete",          "detail": "bk_01J4XY..."}
  ]
}
```

`export-volume` is emitted **before** that volume's export begins, so a
multi-minute volume shows as in-progress in the step trail rather than
nothing appearing until it finishes; `export-volume-done` follows with the
byte count once the copy completes.

Two different steps record a volume the backup does **not** contain, with no
`export-volume` pair — they are distinct on purpose, because after the fact
the step trail is the only place the two can be told apart:

- `skip-volume` (detail `"<name> (backup: none)"`) — the template **vetoes**
  that volume with `backup: none`. It would not be captured by any backup.
- `skip-volume-scope` (detail `"<name> (not in the requested scope)"`) — the
  volume is backup-able, but this request named a narrower scope. A later
  unscoped backup captures it normally.

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
      "volumes":  [
        {"name": "data", "size_bytes": 52428800},
        {
          "name": "sites", "size_bytes": 8912896000,
          "excluded": {"patterns": ["*/private/backups/**"], "entries": 8, "bytes": 3627000000}
        }
      ],
      "created":  "2026-06-06T10:00:00Z",
      "finished": "2026-06-06T10:01:23Z"
    }
  ]
}
```

`state` is one of `creating`, `complete`, or `failed`. Only `complete` backups
are restorable.

`excluded` is present only on a volume whose template declared `exclude:`
patterns for that backup — `data` above was exported in full and carries no
`excluded` key at all, which is how a partial backup is distinguished from a
complete one. When present, `patterns` is the list applied, `entries` the
count of tar entries it dropped, and `bytes` their total uncompressed size.

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
    dialog, which names the volumes this backup actually contains and warns
    that every other volume keeps its current data, then
    enqueues a restore job and re-renders the page with a notice.
  - **Delete** button — removes the backup after a browser confirm prompt.

All UI actions are HTMX requests to `/ui/...` endpoints that mirror the API
behaviour (same validation, same job enqueue).

---

## Restore semantics

Restore is **in-place** and **destructive** — but only for the volumes the
backup actually covers:

1. The instance is stopped.
2. The pod is torn down (`Delete` with `PruneVolumes: false` — volumes are
   *not* bulk-removed here). Per-instance and host-scoped secrets are kept —
   `Apply` re-pushes them from the stored spec.
3. Each volume **the backup contains** is removed and recreated immediately
   before its own import, one at a time, then imported from the backup's
   artifact file. A volume the instance has today that the backup does not
   cover — because it was scoped out at backup time, or vetoed by `backup:
   none` — is never touched: it is not removed, not recreated, and keeps
   whatever live content it had going into the restore.
4. The content of every restored volume is **verified** against the sha256
   manifest stored in the DB. Verification always runs (unlike migrate, where
   it can be disabled). A mismatch fails the job before declaring success.
5. `Apply` re-runs the current spec (current parameters, secrets, domains)
   to recreate containers and start the instance.
6. The job waits until every container is `Running` and every declared
   healthcheck reports `healthy` before succeeding.

**A scoped restore is partial by design.** Restoring from a backup that only
covers `sites` leaves every other volume exactly as it was before the
restore ran — that's the point of the scope, not a limitation of it. The
practical corollary: a restore can no longer delete data it has no copy of.
Before this, tearing down the pod with `PruneVolumes` removed every volume
the instance declared, including ones the backup being restored from never
captured — so restoring a scoped or `none`-vetoed backup used to destroy
data with no way to bring it back. That hole is closed; check *which*
volumes a backup covers (the `volumes` list in its
[list-backups](#list-backups) row) before relying on a restore to put
everything back the way it was.

> ⚠️ **A partial restore can leave an instance internally inconsistent, and
> nothing detects it.** The volumes a backup covers go back to the moment the
> backup was taken; every other volume stays at **today**. For anything whose
> state is split across two volumes that must agree, that is a broken instance,
> not a partial one:
>
> - A Postgres data directory restored to yesterday beside a WAL volume left at
>   today either refuses to start on an invalid checkpoint record, or replays
>   stale WAL over the restored directory.
> - An application database restored to yesterday beside an uploads/files volume
>   left at today has rows referencing files that do not exist, and files no row
>   knows about.
>
> This is the direct cost of the guarantee above — a restore may never delete a
> volume it has no copy of — and it is the right trade, because the alternative
> destroys data outright. But it means `backup: none` and an explicit scope are
> decisions about **restorability**, not just about backup size. Do not mark a
> volume `none` (or scope it out) if the instance's correctness depends on it
> agreeing with a volume that *is* backed up. Where two volumes must move
> together, back them up together.
>
> Before restoring, check the `volumes` list on the backup row
> ([list-backups](#list-backups)) against the volumes the instance declares. If
> the backup covers fewer, decide explicitly what happens to the rest —
> restoring is not that decision.

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
