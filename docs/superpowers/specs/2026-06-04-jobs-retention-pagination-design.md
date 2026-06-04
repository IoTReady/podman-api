# Jobs: pagination + retention (#45, #51)

**Goal:** Bound the unbounded `jobs` table and `GET /jobs` response — add
cursor pagination with a default page size, and an opt-in age-based retention
sweep that prunes terminal jobs while keeping parent/child relationships intact.

Closes #45 and #51 (duplicates). Also folds in the two minor notes from #45:
a connection-pool bump and nanosecond job timestamps.

## Background

`store.JobStore.ListJobs` / `GET /jobs` have no `LIMIT` and never prune, so the
table and the response grow without bound. Evacuate makes it worse — one parent
plus N child rows per call. Job IDs are time-prefixed and globally unique
(`<unixnano>-<rand>`), and the listing is ordered `created DESC, id DESC`, which
makes the id a natural, stable pagination cursor.

## Components & data flow

### A. Nanosecond job timestamps (`internal/store/sqlite.go`)

Job `created` / `started` / `finished` are stored via `time.Now().Unix()`
(second granularity), so durations and FIFO tiebreaks are coarse. Switch the
**jobs** table to `UnixNano()` on write and `time.Unix(0, n)` on read. The
`specs` table is unchanged (out of scope). Bump `PRAGMA user_version` to 3.

Pre-upgrade terminal job rows hold second-scale integers; read back as nanos
they render near the Unix epoch until the retention sweep removes them. This is
acceptable: the store is opt-in, jobs are ephemeral (reaped on boot, pruned by
retention), and only terminal leftovers from a prior version are affected. No
data migration is performed; this limitation is documented here and in the PR.

### B. Connection pool (`internal/store/sqlite.go`)

Raise `maxOpenConns` from 4 to 8 so `GET /jobs` reads keep headroom over the
4-worker writer pool under saturation (the note from #45). `SetMaxIdleConns`
tracks `maxOpenConns`. The comment references `jobs.DefaultWorkers` so the
relationship is explicit.

### C. Pagination (`internal/store`, `internal/api`)

`JobFilter` gains two fields:

```go
type JobFilter struct {
	State    JobState
	Kind     string
	ParentID string
	Limit    int    // <=0 → DefaultJobLimit; clamped to MaxJobLimit
	Before   string // cursor: return jobs with id < Before (the prior page's last id)
}
```

Constants in `internal/store`:

```go
const DefaultJobLimit = 100
const MaxJobLimit      = 1000
```

A shared helper clamps the limit so the cap holds regardless of caller:

```go
func clampJobLimit(n int) int {
	if n <= 0 { return DefaultJobLimit }
	if n > MaxJobLimit { return MaxJobLimit }
	return n
}
```

- **SQLite `ListJobs`**: when `Before != ""` add `id < ?` to the WHERE clause;
  order by **`id DESC` alone**; append `LIMIT ?` with the clamped value. The id
  is fixed-width (`%016x-%x` — 16 hex of unix-nanos, a dash, 12 hex of random),
  so its lexicographic order is a total order on creation time. The cursor key
  (`id`) MUST equal the sort key: `created` and the id's time-prefix come from
  two separate clock reads (`newJobID` vs the row's `created`), so ordering by
  `created` while cursoring on `id` lets concurrent inserts skip a row between
  pages. Ordering by id alone makes `id < before` a correct, stable cursor.
- **Memory `ListJobs`**: same semantics — filter (state/kind/parent_id and
  `id >= Before`), then **sort by `id` descending** (matching SQLite's total
  order, since append order can diverge from id order under concurrent
  enqueues), then take the clamped limit.

**API (`internal/api/jobs.go`)**: `listJobs` parses `?limit=` and `?before=`.
A non-integer `limit` returns `400 invalid_query` ("limit must be an
integer" — matching the package's query-param error idiom); otherwise the parsed
value (including 0 / negative) is passed through
and the store clamps it. `before` is passed verbatim. **The response stays a
bare JSON array** — no shape change. A client pages by passing the last id it
received as the next `before`, and stops when it gets fewer than `limit` rows.
The default limit now bounds the previously-unbounded endpoint.

### D. Retention (`internal/store`, `internal/jobs`, `cmd/podman-api`)

New `JobStore` method:

```go
// PruneJobs deletes terminal (succeeded/failed) jobs finished before olderThan,
// preserving parent/child integrity: a parent row is removed only when it has
// no surviving child. Returns the number of rows deleted.
PruneJobs(ctx context.Context, olderThan time.Time) (int, error)
```

- **SQLite**: a single transaction with two deletes (cutoff =
  `olderThan.UnixNano()`):
  1. Delete terminal **children** finished before the cutoff:
     `DELETE FROM jobs WHERE parent_id IS NOT NULL AND state IN ('succeeded','failed') AND finished IS NOT NULL AND finished < ?`
  2. Delete terminal jobs finished before the cutoff that are **not** referenced
     as a parent by any surviving row:
     `DELETE FROM jobs WHERE state IN ('succeeded','failed') AND finished IS NOT NULL AND finished < ? AND id NOT IN (SELECT parent_id FROM jobs WHERE parent_id IS NOT NULL)`

  Step 1 clears old children first; step 2 then removes old parents/top-level
  jobs whose children are all gone, while a still-running or recent child keeps
  its (old, terminal) parent alive — no orphans. Return the summed
  `RowsAffected`.
- **Memory**: mirror the semantics over the slice (compute the surviving-parent
  set, delete old terminal children, then old terminal non-referenced jobs).

**Runner (`internal/jobs/runner.go`)**:

```go
const retentionInterval = time.Hour

// StartRetention runs a periodic prune of terminal jobs older than retention.
// It is a no-op when retention <= 0. It sweeps once immediately, then every
// retentionInterval, and exits when ctx is cancelled. Tracked by the runner's
// WaitGroup so Wait() includes it.
func (r *Runner) StartRetention(ctx context.Context, retention time.Duration)
```

The goroutine calls `r.store.PruneJobs(ctx, time.Now().Add(-retention))`; a
prune error is logged, not fatal; a non-zero delete count is logged.

**main (`cmd/podman-api/main.go`)**: a new flag

```go
jobsRetention = flag.Duration("jobs-retention", 0,
	"if >0, prune terminal jobs older than this (e.g. 168h); 0 disables")
```

When `db != nil` and `*jobsRetention > 0`, call
`runner.StartRetention(runnerCtx, *jobsRetention)` after `runner.Start` and log
the configured retention.

### E. Docs

- `api/openapi.yaml`: document `limit` and `before` query params on `GET /jobs`.
- `README.md`: note the default page size + `before` cursor on the jobs row, and
  the `-jobs-retention` flag.
- Wiki `Operating.md`: pagination + retention paragraph — pushed directly after
  merge (the wiki has no PR flow), consistent with prior practice.

## Testing

**Store (`internal/store`)** — SQLite and Memory, table-driven where natural:
- `ListJobs` returns at most the limit; omitted/zero/negative limit → 100; limit
  > 1000 → 1000.
- `before` cursor: with N>limit jobs, page 1 returns the newest `limit`; passing
  the last id as `before` returns the next page; no overlap, no gaps.
- `PruneJobs`: old terminal jobs deleted; recent terminal and any non-terminal
  jobs kept; an old terminal **parent** with a surviving (recent or running)
  child is **not** deleted; when the child is also old+terminal both go; returns
  the correct count.
- Nanosecond round-trip: a job's `Created`/`Started`/`Finished` preserve
  sub-second precision through write/read.

**Runner (`internal/jobs`)**:
- `StartRetention` with a tiny retention and a context-cancel sweeps at least
  once and removes an old terminal job; `retention <= 0` starts nothing.

**API (`internal/api`)**:
- `?limit=1` returns one element; `?before=<id>` returns the following page;
  `?limit=abc` → `400 invalid_query`.
- `GET /jobs` with no params returns a bare array bounded by the default limit.

## Out of scope

- `specs`-table timestamp granularity (unchanged).
- Count-based retention (age-based only).
- A response envelope / `next_before` field (bare array preserved).
- Backfilling/migrating pre-upgrade second-scale job timestamps.
