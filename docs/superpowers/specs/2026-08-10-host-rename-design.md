# Host rename (#246)

## Problem

There is no supported way to rename a managed host — the `id` in `hosts/<name>.yaml`.
The API only exposes instance-level rename
(`POST /hosts/{host}/instances/{template}/{slug}/rename`); nothing mutates a host's
identity. `config.LoadHosts` keys/dedupes purely by the YAML `id` field (not filename),
so today the only lever is hand-editing that field — which is unsafe on a host with live
instances:

- `specs` (PK `host,template,slug`) and `host_secrets` (PK `host,name`) in the SQLite store
  are keyed by the config `id`. Editing it out from under the store orphans every existing
  instance/secret row — reconciliation can no longer find desired state for pods that are
  still running.
- `backups` (same key shape) is keyed the same way; existing backup rows become unreachable
  by the new id.
- The in-memory host list (`instance.Service.hosts`) only picks up a changed `id` on
  restart or `SIGHUP` (`config.LoadHosts` reload in `server/server.go`), so there's a window
  where neither the old nor the new id resolves correctly.

Not in scope for this pass (tracked as follow-ups, not solved here):

- S3 on-demand snapshot-backup blob keys (`internal/instance/backup.go`, commercial S3
  client lives in podman-api-pro) are prefixed by host id and are **not** re-keyed. This is
  a known, documented gap.
- Prometheus/Grafana `host` labels are in-memory/time-series; a rename just starts a new
  series. No action needed, continuity loss is acceptable.
- Litestream/parquet-sync sidecars key their own S3 paths off the sidecar's OS hostname
  (`os.Hostname()`), not the config `id` — already decoupled, unaffected by this change.

## Goal

A single API call that atomically (from the operator's perspective) renames a host: updates
the host's config file, migrates its store rows, and makes the new id live immediately with
no process restart and no `SIGHUP` required.

## Design

### New scope

`hosts:write`, following the existing `<resource>:write` convention (`instances:write`,
`templates:write`, `secrets:write`).

### New route

```
POST /hosts/{host}/rename
Body: {"new_id": "<new host id>"}
```

Guarded by `hosts:write`. Lives in `internal/api/hosts.go` alongside the other host
handlers.

Validation, in order:

1. `new_id` is non-empty and passes the same id-shape validation already used for host ids
   elsewhere (alnum + hyphen; reuse whatever validator `render`/`config` already applies to
   host ids — if none exists today, add the minimal one: `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`).
2. `{host}` (the path value) must resolve against the live host list
   (`instance.Service.Hosts()`) — 404 `unknown_host` if not.
3. `new_id != host` — 400 `invalid_body` (no-op rename).
4. `new_id` must not already be a live host id — 409 `conflict`.

### Store layer: `store.RenameHost(ctx, oldID, newID string) error`

New method on the `Store` interface (SQLite implementation in `internal/store/sqlite.go`;
add a no-op-safe implementation to `store.Memory` used by tests too). One transaction:

```sql
-- refuse to silently merge into an id that already has rows (defensive; the API layer's
-- "must not already be a live host id" check covers the common case, this covers stale/
-- orphaned rows under an id no host currently claims)
SELECT COUNT(*) FROM specs WHERE host = ?newID
-- if > 0: abort, return ErrHostRenameConflict

UPDATE specs        SET host = ?newID WHERE host = ?oldID;
UPDATE host_secrets  SET host = ?newID WHERE host = ?oldID;
UPDATE backups       SET host = ?newID WHERE host = ?oldID;
```

All three in one `BEGIN`/`COMMIT` so a mid-transaction failure leaves the DB exactly as it
was (SQLite transaction, not best-effort).

### Config layer: `config.RenameHostFile(dir, oldID, newID string) error`

New function in `internal/config/hosts.go`. Does **not** re-marshal the YAML (that would
drop comments/formatting in a real hosts file). Instead:

1. Re-scan `dir` for the `*.yaml` file whose parsed `id` equals `oldID` (reuses the same
   scan `LoadHosts` does).
2. Read the raw file bytes. Find the line matching `^id:\s*oldID\s*$` (exact, anchored).
   If not found — fail closed (`fmt.Errorf`), don't guess at a fuzzy replace.
3. Replace that one line with `id: newID`, write to a temp file in the same directory, then
   `os.Rename` over the original (atomic on the same filesystem).
4. Return the original file's full byte content to the caller so the handler can revert it
   verbatim if the subsequent DB transaction fails.

The filename itself is never changed — `LoadHosts` never inspects it, and rename-vs-copy of
the file adds risk (a stray old-named file with new `id` inside) for no benefit. An operator
who wants filename/id to match can rename the file by hand separately.

### Handler sequence (`internal/api/hosts.go: renameHost`)

```
1. validate (above)
2. preflight the store's refusals read-only (store.CheckHostRename via
   svc.CanRenameHost) -> 409 here, before anything has been written
3. original, err := config.RenameHostFile(hostsDir, oldID, newID)
   -> 500 on failure, nothing has changed yet
4. err := store.RenameHost(ctx, oldID, newID)
   -> on failure: write `original` bytes back to the file, atomically (temp file +
      os.Rename, same as the forward write) and best-effort; log if that also
      fails — this is the one state where operator intervention may be needed, return 500
5. live-swap the new host list EVERYWHERE (see correction below)
6. 200 {"old_id": oldID, "new_id": newID}
```

**Correction (final whole-branch review).** Step 5 was written as
`svc.SetHosts(hosts)` alone. That is incomplete: the service's host map is only one
of three places a host list lives. `podman.Real` keeps its own `hosts` map, mutated
solely by `Real.SetHosts` — without that call, every podman operation against the new
id fails `unknown host` (while the old id keeps resolving) even though
`GET /hosts/{new}` answers 200 from service metadata. And `server/server.go`'s
`hostsHolder`, read by the periodic ingress reconcile, the prune scheduler's policies
and the boot-converge goroutine, is refreshed by neither. SIGHUP has always done all
three; the rename must too. As implemented: `Service.RenameHost` does
`svc.SetHosts` + `client.SetHosts` and evicts the per-host caches keyed by the old id
(`instCache`/`statsCache`/`volCache` — otherwise the old id is exported as a metric
for the process's lifetime), and the handler then calls a server-supplied hosts
reloader (`api.WithHostsReloader`) that re-reads `hosts/*.yaml` and runs the same
`applyHosts` helper SIGHUP does. Step 2 is likewise a correction: rewriting the config
file before asking whether the store would accept the rename made the most common
refusal (`host_has_backups`) destructive.

**Correction (PR review, concurrency finding #2).** Steps 3 and 4 above read as two
independently-taken steps in the handler, and that is how they were first built — the
config rewrite in `renameHost`, unsynchronized, and the store migration behind
`Service.RenameHost`'s host lock. Two mutation points, one lock. Two concurrent
`POST /hosts/h1/rename` (`h1->h2` and `h1->h3`) then both read `id: h1` before either
writes, so one write is silently lost; both call `RenameHost`, one wins the host lock
and commits, and the loser — which now correctly fails the inside-the-lock
`checkRenameHosts` with `ErrUnknownHost` — runs its step-4 revert with the `id: h1`
bytes IT captured, *after* the winner committed. The config file then says `h1`
permanently while the store and the live host set say `h2`: a silent split-brain
reachable from two ordinary requests, no crash required.

Steps 3 and 4 are therefore one step, inside one lock acquisition.
`Service.RenameHost` takes the rewrite as a parameter —
`RenameHost(ctx, oldID, newID string, rewriteFile func() (revert func() error, err error))` —
and runs it after the post-lock checks, before the store migration, calling `revert`
itself if the migration fails, all before releasing the lock. The loser of a race
fails the check and never rewrites anything, so there is nothing stale to revert.
Step 2 (`CanRenameHost`) stays where it is, outside the lock, and stays advisory: it
buys a fast, specific `host_has_backups` without a destructive rewrite, and is never
the source of truth — the post-lock check and the store's own transaction are.

`hostsDir` needs to reach the handler — plumb it through the same way other `main`-supplied
dependencies reach `internal/api` handlers today (a field on the `handlers`/server-options
struct, set once in `server/server.go` from the existing `*hostsDir` flag value already read
at startup).

### Known accepted races (v1, documented not solved)

- **In-flight jobs/locks against the host during rename.** `instance.Service` keys its
  per-host mutexes (`hostLocks`), its per-instance mutexes (`locks`, keyed
  `host|template|slug`) and its instance/stats/volume caches by host id string, so a rename
  mid-operation means a lock held under the old key and a new operation's lock under the new
  key don't exclude each other for that one transition.

  **Applies against instances that already exist are now enforced, not just documented.**
  `Service.RenameHost` takes `hostLock(oldID)` and then the `instanceLock` of every key
  `ListSpecKeys(oldID)` returns — sorted by (template, slug), and after the host lock, so the
  order matches `Apply`'s own hostLock-before-instanceLock order and no cycle is possible —
  and holds all of them across the entire body (store migration, host-list swap, podman
  client propagation, cache eviction). Without this, an `Apply` for **any** instance on the
  host, domain-carrying or not, could land its `PutSpec(host: oldID, …)` after the
  `UPDATE specs SET host = newID` had run, permanently orphaning that spec row under an id no
  live host claims: the instance becomes unreachable through the API while its pod keeps
  running. Note this deliberately does **not** make `hostLock` unconditional in `Apply` —
  non-domain applies for different instances on one host stay concurrent with each other.

  **Residual gap (not closed):** an `Apply` for a `(template, slug)` that did not yet exist
  when `RenameHost` called `ListSpecKeys`. `instanceLock` lazily creates a mutex for any key,
  so such an apply takes a brand-new lock that `RenameHost` never knew to acquire and is
  therefore not serialized against the rename — it can still orphan its spec under `oldID`.
  This is strictly narrower than the bug above (the two calls must interleave inside the
  single `ListSpecKeys` snapshot window, *and* only for an instance being created for the
  first time, versus any existing instance at any point during the whole rename), but it is
  real. Still-unenforced beyond that: in-flight backups/migrates against the host. Continue
  to avoid renaming a host with work in flight against it.
- **S3 snapshot-backup blob re-keying** — explicitly out of scope, tracked as a follow-up in
  #246 itself. Existing on-demand backups remain reachable only under a `backups` row now
  pointing at the new host id but an S3 key prefix still under the old — i.e. after a rename,
  existing backups become **unreachable** via the API's backup-restore route (the row says
  `newID` but the blob lives at `oldID/...`) until a follow-up ships. This must be called out
  loudly in the response and in docs, not silently swallowed.

  Given that consequence, the initial implementation should refuse to rename a host that has
  any backup rows at all, rather than silently breaking restore. (Zero-backup hosts — the
  common case for a first release of this feature — rename cleanly.) A host with backups
  requires the S3 re-key follow-up before it can be renamed. This is a v1 scope-narrowing,
  not in the original ask, added here because "silently orphan backups" is worse than
  "refuse and say why."

### Testing

- Store: `TestRenameHost` — migrates all three tables, rejects when `newID` already has
  rows, rejects when `oldID` has none (nothing to rename), transactional (inject a failure
  mid-way if the test harness allows, otherwise verify all-or-nothing via the SQL alone).
- Store: rename refused when `oldID` has backup rows (the v1 scope-narrowing above).
- Config: `TestRenameHostFile` — happy path preserves comments/formatting of untouched
  lines, fails closed when the `id:` line isn't found verbatim, fails when no file matches
  `oldID`.
- API: `TestRenameHost` handler tests — 404 unknown host, 400 same id, 409 id collision,
  happy path returns 200 and a subsequent `GET /hosts/{new_id}` succeeds while
  `GET /hosts/{old_id}` 404s, with no restart between the two calls (proves the `SetHosts`
  swap, not just the file write).
- Live validation (this pass, per user request): rename `engine-2` → `engine-2-rntest` and
  back on the real fleet, verifying instances/ready-state are unaffected throughout and the
  new id is queryable immediately after the API call with no `SIGHUP`.
