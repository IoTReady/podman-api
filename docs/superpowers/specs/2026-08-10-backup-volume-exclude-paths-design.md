# Per-volume backup exclude paths (#248)

## Problem

`instance.backupVolume` exports a whole volume. There is no way to declare
"back this volume up, but not that subtree", so a volume that mixes durable
data with regenerable junk ships both on every snapshot.

The motivating case is the Frappe fleet (podman-api-pro#153). Those templates
declare a `pre_backup` hook that runs `bench --site … backup`, writing a
database dump into `<site>/private/backups/` inside the very volume the
snapshot is about to export. Frappe keeps several generations there. So each
snapshot carries two to four copies of the same database, plus — on sites
migrated with `--with-files` — a `-private-files.tar` duplicating a
`private/files` tree the same tar is already copying uncompressed.

Measured across 14 Frappe instances on `vedanta`, 2026-08-10:

| | size |
|---|---:|
| `sites` volume content, all 14 | 17.5 G |
| of which `<site>/private/backups/` | **10.6 G (60%)** |
| `private/files` + `public/files` (must keep) | 6.65 G |
| newest DB dump per site (must keep, one each) | 3.7 G |
| `assets/`, `logs/`, `locks/`, `site_config.json` | ~0.2 G |

`balco` is the clearest single case — a 4.36 G snapshot holding:

```
20260810_001509-…-database.sql.gz      957M
20260810_001509-…-private-files.tar    712M   duplicates private/files
20260810_153948-…-database.sql.gz      978M
20260810_155648-…-database.sql.gz      980M
```

Three dumps of one database, and a files tar of a tree the snapshot copies
anyway. With `S3_BACKUP_KEEP_LAST=7` the redundancy is ~74 G in the bucket.

It also costs availability, not just storage. A `mode=snapshot` backup is a
full pod stop → export → restart (podman-api-pro#135), and the stop window is
sized by total volume content. `balco` is offline for an export that is 60%
redundant.

## Goal

A template can declare, per volume, path patterns to omit from that volume's
backup tar — without changing what any other code path exports, and without a
volume that declares nothing behaving differently in any respect.

## Non-goals

- **Reducing what `pre_backup` writes.** The dumps are not pure waste: the
  shared MariaDB instance backing these sites has its own `data` volume marked
  `backup: none`, so the `bench backup` dump inside `sites` is the only backup
  of that database that exists. Exactly one dump per snapshot is load-bearing.
  Backing up MariaDB itself is a separate problem.
- **Include-lists.** See "Rejected alternatives".
- **Per-instance overrides.** Which subtrees of a volume are junk is a property
  of the application, which is a property of the template.
- **Filtering restore.** Restore imports whatever the tar holds.

## Design

### 1. Declaration

`render.Volume` (`internal/render/meta.go:66`) gains one field:

```go
type Volume struct {
	Name    string   `yaml:"name" json:"name"`
	Backup  string   `yaml:"backup,omitempty" json:"backup,omitempty"`
	Exclude []string `yaml:"exclude,omitempty" json:"exclude,omitempty"`
}
```

```yaml
volumes:
  - name: sites
    backup: "s3; interval=24h"
    exclude:
      - "*/private/backups/**"
```

Patterns are globs matched against each tar entry's `path.Clean`ed name,
relative to the volume root — the same normalisation `parseTar` already
applies. `**` must span separators, which `path.Match` cannot express — so this
needs either a small hand-rolled matcher or a new dependency
(`github.com/bmatcuk/doublestar`; the module has neither it nor any other glob
library today). Prefer the dependency: subtree matching is easy to get subtly
wrong, and this one decides what does not get backed up.

**`exclude` is a sibling of `backup:`, not part of its grammar.** The `backup:`
marker string is parsed commercially (`backupmarker` in podman-api-pro) and
projected verbatim by the core through the `WithBackupScheduler` seam. The
exclude patterns must be read by the core's own export loop, so folding them
into the marker would mean the core parsing a grammar it does not own. A
separate typed field keeps the tier boundary intact.

**Validation at template registration** (alongside the existing volume checks):
each pattern must compile, must be non-empty, must not start with `/`, and must
not contain a `..` segment — so a pattern cannot be read as host-absolute or
escape the volume root. An invalid pattern rejects the template, rather than
being skipped at backup time where nobody would see it.

### 2. Directory-entry convention

`*/private/backups/**` matches the directory's *contents*; the directory entry
itself does not match, so it restores as an empty directory. `*/private/backups`
(no trailing `/**`) matches the directory entry and drops it entirely.

Both are legitimate and the difference matters — an app that expects a
directory to exist and does not create it will fail on a restored volume. For
Frappe either works (`bench backup` creates the directory), but the docs must
state the distinction rather than leave it to be discovered.

### 3. The filter

Today (`instance.backupVolume`, `internal/instance/backup.go:171`):

```
VolumeExport ──TeeReader──> blob writer          (byte-for-byte copy)
                    └──────> buildManifest       (parses + sha256s every entry)
```

With patterns declared:

```
VolumeExport ──> tar.Reader ──filter──> tar.Writer ──> blob writer
                                  └───> Manifest
```

**When `exclude` is empty the current code path runs unchanged.** This is the
central safety property of the change: the copy stays a byte-for-byte
`TeeReader` for every template that has not opted in, so the new code is
unreachable for all existing data, and any regression it carries is scoped to
volumes someone deliberately annotated.

Three correctness constraints on the re-encode, each with a test:

- **Header fidelity.** `tar.Writer` re-emits headers rather than copying bytes.
  Long names, PAX records and xattrs must survive: pass the `*tar.Header`
  through untouched, and set `hdr.Format` explicitly rather than letting Go
  re-select a format per entry.
- **Hardlinks.** A `TypeLink` entry whose target was excluded produces an
  archive that fails to import. A link is excluded when its own path matches
  **or** its `Linkname` resolves to an excluded path.
- **Cost.** Negligible. Every byte is already read and hashed to build the
  manifest; the filter adds a write, not a parse.

Name the predicate something distinct from the existing `excludePath` in
`internal/instance/manifest.go:140`. That one omits Litestream shadow-WAL paths
from **fingerprint comparison** (#142) while the bytes still ship — a different
concept at a nearly identical name. Suggest `contentFilter` / `filterEntry` for
the new one, and a comment on each pointing at the other.

### 4. Recording what was dropped

`store.BackupVolume` gains:

```go
type ExcludedPaths struct {
	Patterns []string `json:"patterns"`
	Entries  int      `json:"entries"`
	Bytes    int64    `json:"bytes"`
}
```

Persisted with the manifest and returned by `GET /backups`:

```json
"volumes": [{
  "name": "frappe-otp-balco-sites",
  "size_bytes": 2300000000,
  "excluded": {"patterns": ["*/private/backups/**"], "entries": 8, "bytes": 3627000000}
}]
```

This is what makes a partial backup distinguishable from a complete one — the
restore path can state what was omitted, and `entries: 0` against a non-empty
pattern list is a visible signal that a pattern is stale or misspelled.

A zero-match pattern deliberately does **not** fail the backup: a brand-new
instance legitimately has nothing to exclude yet, and failing a good backup for
a cosmetic reason is worse than the drift it would catch.

### 5. Scope: the backup path only

`VolumeExport` has four callers. Only one filters:

| call site | behaviour |
|---|---|
| `internal/instance/backup.go:172` (backup) | **filtered** |
| `internal/instance/rename.go:219` (rename) | unfiltered |
| `internal/instance/service.go:1519` (copy) | unfiltered |
| `internal/instance/service.go:1537` (migrate) | unfiltered |

Rename, copy and migrate move an instance and **remove the source**. A dropped
path there has no second copy to recover from, unlike a backup, where the live
volume still holds everything. A migrated host keeping its `private/backups` is
wasteful and harmless; a migrated host silently losing a subtree is not.

Add a comment at each of the three unfiltered call sites saying so, because the
obvious future tidy-up is to share the filtered helper across all four.

## Testing

- Round-trip: build a tar with nested dirs, long (>100 char) names, a symlink,
  a hardlink, and xattrs; filter with no patterns; assert the output is
  byte-identical to the input.
- Filtering: assert excluded entries are absent, surviving entries are
  bit-identical, and the manifest covers exactly the survivors.
- Hardlink whose target is excluded: assert the link is dropped too and the
  result imports cleanly.
- Directory entry vs contents: `dir/**` keeps the empty dir; `dir` drops it.
- Accounting: `Entries`/`Bytes` match what was dropped; a non-matching pattern
  yields `entries: 0` and a successful backup.
- Validation: absolute pattern, `..` segment, and uncompilable pattern each
  reject the template.
- Integration on a real Frappe volume: snapshot `sites` with and without the
  pattern, restore both, diff the trees.

## Rollout

1. Implement + merge + tag in this repo.
2. `make bump` in podman-api-pro.
3. Add `exclude:` to `templates/frappe-otp.yaml` and `frappe-bbmeat.yaml`,
   re-register. Templates change **last** — a template declaring a field the
   deployed core does not know is rejected by validation.

podman-api-pro#153 also proposes an interim `pre_backup` prune (append a delete
of older dump sets to the hook). That ships without waiting for this and does
not conflict; once `exclude:` is live the prune is redundant but harmless.

Expected effect on the measured fleet: ~17.5 G → ~10.5 G per cycle, ~50 G
across the retention window, and a proportionally shorter stop window.

## Rejected alternatives

**Include-lists (whitelist).** Precise, and would give the smallest possible
tar. Rejected because it fails closed: a directory nobody thought to list is
silently absent from every snapshot until someone needs it at restore time.
Exclude-lists fail open — a new path is captured by default, and a stale
pattern costs storage rather than data. The same asymmetry rules out supporting
both.

**Prune inside `pre_backup`.** Zero code, ships immediately, and is worth doing
as an interim (above). Rejected as the durable fix because it is per-template
shell that does not generalise — the next template with a junk directory writes
its own `rm` line — and it cannot touch anything the application holds open.

**Split the subtrees into their own volumes.** Mount `private/files` and
`public/files` as separate volumes and mark only those for backup. Needs no
core change, but requires a per-instance data migration whose failure mode is
silent: this exact pattern has already misfired on the pro side, where every
`duckdb` volume on `vedanta` is empty because the copy step was never scripted,
and the backups succeed carrying nothing. It also converts recovery from a
volume restore into a `bench restore` runbook that does not exist.

**Filtering in podman.** `/volumes/{name}/export` takes no filter options, and
this needs no upstream change: the tar is already fully decoded in-process.
