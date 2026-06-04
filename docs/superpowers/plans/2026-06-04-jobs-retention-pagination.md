# Jobs pagination + retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the `jobs` table and `GET /jobs` response with cursor pagination (default page size) and an opt-in age-based retention sweep that preserves parent/child integrity.

**Architecture:** Job timestamps move to nanosecond precision; `JobFilter` gains `Limit`/`Before` (cursor = job id) with store-side clamping; a new `PruneJobs` store method deletes old terminal jobs children-before-parents; a runner goroutine sweeps periodically; `main` gates it behind a `-jobs-retention` flag.

**Tech Stack:** Go, `modernc.org/sqlite`, standard `context`/`testing`. **All `go` commands MUST carry the build tags** (CGO drivers are excluded by them). Use this exact prefix everywhere:

```
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...
```

The `internal/store` package is pure Go, but passing the tags is harmless and keeps commands uniform. Run all commands from the worktree root `/home/tej/projects/podman-api/.worktrees/45-jobs-retention`.

**Conventions:** `gofmt -l .` must be empty, `go vet` clean. Forge is Forgejo (not GitHub). Test helpers already exist in `internal/store`: `openJobStore(t)` (in `jobs_test.go`) returns a `*SQLite`; `store.NewMemory()` returns a `*Memory`.

---

### Task 1: Nanosecond job timestamps (SQLite)

Switch the **jobs** table's `created`/`started`/`finished` to nanosecond precision. The `specs` table is untouched.

**Files:**
- Modify: `internal/store/sqlite.go` (the `Enqueue`, `StartChild`, `ClaimNext`, `Finish`, `FailRunning` writes use `time.Now().Unix()`; `scanJob` reads with `time.Unix(...)`; `OpenSQLite` sets `user_version`)
- Test: `internal/store/jobs_nanos_test.go` (Create)

- [ ] **Step 1: Write the failing test**

Create `internal/store/jobs_nanos_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestJobs_NanosecondTimestamps(t *testing.T) {
	s := openJobStore(t)
	ctx := context.Background()

	before := time.Now()
	j, err := s.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	// Created must carry sub-second precision: truncating to the second would
	// usually lose the nanosecond remainder. Assert it is not second-aligned OR
	// at least within a tight window of wall clock (ns round-trip preserved).
	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Created.Before(before.Truncate(time.Second)) || got.Created.After(time.Now().Add(time.Second)) {
		t.Fatalf("created %v out of expected window", got.Created)
	}
	// The decisive check: a value stored as UnixNano and read with Unix(0, n)
	// keeps nanosecond resolution. Stored as seconds, the sub-second part is 0.
	if got.Created.Nanosecond() == 0 && before.Nanosecond() != 0 {
		t.Fatalf("created lost sub-second precision: %v (nsec=0)", got.Created)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestJobs_NanosecondTimestamps ./internal/store/`
Expected: FAIL — `created lost sub-second precision ... (nsec=0)` (timestamps are currently second-granular).

- [ ] **Step 3: Switch job writes/reads to nanoseconds**

In `internal/store/sqlite.go`:

In `scanJob`, change the three reads from `time.Unix(x, 0)` to `time.Unix(0, x)`:

```go
	j.Created = time.Unix(0, created)
	if started.Valid {
		j.Started = time.Unix(0, started.Int64)
	}
	if finished.Valid {
		j.Finished = time.Unix(0, finished.Int64)
	}
```

In `Enqueue`, `StartChild`, `ClaimNext`, `Finish`, and `FailRunning`, change every `now := time.Now().Unix()` used for a **jobs** column to `now := time.Now().UnixNano()`. (Each of those five methods has exactly one such `now`. Do NOT touch `PutSpec`/`GetSpec`/`DeleteSpec`, which use the specs table.)

In `OpenSQLite`, bump the version stamp:

```go
	if _, err := db.Exec(`PRAGMA user_version = 3`); err != nil {
```

Add a short comment above the `scanJob` time reads:

```go
	// Job timestamps are stored as Unix nanoseconds (see Enqueue/Finish) for
	// sub-second durations and FIFO tiebreaks.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestJobs_NanosecondTimestamps ./internal/store/`
Expected: PASS

- [ ] **Step 5: Run the whole store package, then commit**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/`
Expected: PASS (existing job/spec tests unaffected — they don't assert second-granularity).

```bash
gofmt -l internal/store/
git add internal/store/sqlite.go internal/store/jobs_nanos_test.go
git commit -m "feat(store): nanosecond job timestamps (#45)"
```

---

### Task 2: Connection-pool headroom

Raise the SQLite pool above the worker count so reads aren't starved by writers.

**Files:**
- Modify: `internal/store/sqlite.go` (the `maxOpenConns` const ~line 44 and its comment)

- [ ] **Step 1: Bump the constant**

In `internal/store/sqlite.go`, change:

```go
// maxOpenConns bounds the SQLite connection pool. >1 enables WAL reader
// concurrency (API reads while the job runner writes); a competing writer waits
// up to busy_timeout rather than failing with "database is locked".
const maxOpenConns = 4
```

to:

```go
// maxOpenConns bounds the SQLite connection pool. WAL allows many concurrent
// readers + one writer; setting the pool above jobs.DefaultWorkers (4) leaves
// read headroom so GET /jobs is not starved when every worker is writing. A
// competing writer waits up to busy_timeout rather than failing with
// "database is locked".
const maxOpenConns = 8
```

(There is no import cycle risk — this is a comment reference, not code. Do not import the jobs package.)

- [ ] **Step 2: Verify build + store tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
gofmt -l internal/store/
git add internal/store/sqlite.go
git commit -m "perf(store): raise SQLite pool above worker count (#45)"
```

---

### Task 3: Pagination fields + clamp in the store layer

Add `Limit`/`Before` to `JobFilter`, the limit constants, the clamp helper, and wire them through both `ListJobs` implementations.

**Files:**
- Modify: `internal/store/jobs.go` (the `JobFilter` struct ~line 43-48; add constants + helper)
- Modify: `internal/store/sqlite.go` (`ListJobs` ~line 276-310)
- Modify: `internal/store/memory.go` (`ListJobs` ~line 121-139)
- Test: `internal/store/jobs_pagination_test.go` (Create)

- [ ] **Step 1: Write the failing test**

Create `internal/store/jobs_pagination_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"testing"
)

// enqueueN inserts n jobs and returns them newest-last.
func enqueueN(t *testing.T, js JobStore, n int) []Job {
	t.Helper()
	ctx := context.Background()
	var jobs []Job
	for i := 0; i < n; i++ {
		j, err := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, j)
	}
	return jobs
}

func testPaginationOn(t *testing.T, js JobStore) {
	ctx := context.Background()
	created := enqueueN(t, js, 5) // oldest..newest

	// limit caps the page
	page1, err := js.ListJobs(ctx, JobFilter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(page1))
	}
	// newest first
	if page1[0].ID != created[4].ID || page1[1].ID != created[3].ID {
		t.Fatalf("page1 order wrong: %s,%s", page1[0].ID, page1[1].ID)
	}

	// before cursor returns the following page with no overlap
	page2, err := js.ListJobs(ctx, JobFilter{Limit: 2, Before: page1[1].ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].ID != created[2].ID || page2[1].ID != created[1].ID {
		t.Fatalf("page2 wrong: %+v", []string{page2[0].ID, page2[1].ID})
	}

	// zero/negative limit → default (all 5 here, well under DefaultJobLimit)
	all, err := js.ListJobs(ctx, JobFilter{Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Fatalf("default-limit len = %d, want 5", len(all))
	}
}

func TestPagination_SQLite(t *testing.T) { testPaginationOn(t, openJobStore(t)) }
func TestPagination_Memory(t *testing.T) { testPaginationOn(t, NewMemory()) }

func TestClampJobLimit(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, DefaultJobLimit}, {-5, DefaultJobLimit}, {50, 50},
		{MaxJobLimit, MaxJobLimit}, {MaxJobLimit + 1, MaxJobLimit},
	}
	for _, c := range cases {
		if got := clampJobLimit(c.in); got != c.want {
			t.Errorf("clampJobLimit(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestPagination|TestClampJobLimit' ./internal/store/`
Expected: FAIL to **compile** — `JobFilter` has no `Limit`/`Before`, `clampJobLimit`/`DefaultJobLimit`/`MaxJobLimit` undefined.

- [ ] **Step 3: Add fields, constants, and clamp helper**

In `internal/store/jobs.go`, replace the `JobFilter` struct:

```go
// JobFilter narrows ListJobs. Empty fields match anything. Limit/Before paginate
// the result (ordered newest-first); Before is the id of the previous page's
// last row (a cursor), returning only rows older than it.
type JobFilter struct {
	State    JobState
	Kind     string
	ParentID string
	Limit    int    // <=0 → DefaultJobLimit; values above MaxJobLimit are clamped
	Before   string // cursor: return jobs with id < Before
}

// Job listing page-size bounds.
const (
	DefaultJobLimit = 100
	MaxJobLimit     = 1000
)

// clampJobLimit applies the default/maximum page-size policy.
func clampJobLimit(n int) int {
	if n <= 0 {
		return DefaultJobLimit
	}
	if n > MaxJobLimit {
		return MaxJobLimit
	}
	return n
}
```

- [ ] **Step 4: Wire SQLite `ListJobs`**

In `internal/store/sqlite.go`, update `ListJobs` to add the `Before` predicate and `LIMIT`:

```go
func (s *SQLite) ListJobs(ctx context.Context, f JobFilter) ([]Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs`
	var where []string
	var args []any
	if f.State != "" {
		where = append(where, "state=?")
		args = append(args, string(f.State))
	}
	if f.Kind != "" {
		where = append(where, "kind=?")
		args = append(args, f.Kind)
	}
	if f.ParentID != "" {
		where = append(where, "parent_id=?")
		args = append(args, f.ParentID)
	}
	if f.Before != "" {
		where = append(where, "id < ?")
		args = append(args, f.Before)
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created DESC, id DESC LIMIT ?"
	args = append(args, clampJobLimit(f.Limit))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
```

- [ ] **Step 5: Wire Memory `ListJobs`**

In `internal/store/memory.go`, update `ListJobs`:

```go
func (m *Memory) ListJobs(_ context.Context, f JobFilter) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	limit := clampJobLimit(f.Limit)
	out := []Job{}
	for i := len(m.jobs) - 1; i >= 0; i-- { // newest first
		j := m.jobs[i]
		if f.State != "" && j.State != f.State {
			continue
		}
		if f.Kind != "" && j.Kind != f.Kind {
			continue
		}
		if f.ParentID != "" && j.ParentID != f.ParentID {
			continue
		}
		if f.Before != "" && j.ID >= f.Before {
			continue
		}
		out = append(out, cloneJob(j))
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
```

Note: Memory appends in insertion order (newest last), and ids are
monotonically increasing, so iterating from the tail yields newest-first and the
`id >= Before` skip is the cursor — matching SQLite.

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestPagination|TestClampJobLimit' ./internal/store/`
Expected: PASS (both SQLite and Memory).

- [ ] **Step 7: Run the whole store package, then commit**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/`
Expected: PASS

```bash
gofmt -l internal/store/
git add internal/store/jobs.go internal/store/sqlite.go internal/store/memory.go internal/store/jobs_pagination_test.go
git commit -m "feat(store): paginate ListJobs with limit + before cursor (#45)"
```

---

### Task 4: `PruneJobs` retention in the store layer

Add `PruneJobs` to the `JobStore` interface and both backends, preserving parent/child integrity.

**Files:**
- Modify: `internal/store/jobs.go` (add `PruneJobs` to the `JobStore` interface, after `FailRunning`)
- Modify: `internal/store/sqlite.go` (add `PruneJobs` method)
- Modify: `internal/store/memory.go` (add `PruneJobs` method)
- Test: `internal/store/jobs_prune_test.go` (Create)

- [ ] **Step 1: Write the failing test**

Create `internal/store/jobs_prune_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// finishJob marks a job terminal so it is prune-eligible.
func finishJob(t *testing.T, js JobStore, id string) {
	t.Helper()
	if err := js.Finish(context.Background(), id, JobSucceeded, ""); err != nil {
		t.Fatal(err)
	}
}

func testPruneOn(t *testing.T, js JobStore) {
	ctx := context.Background()

	// Case 1: a standalone terminal job is pruned; cutoff in the future so it
	// counts as "old".
	old, _ := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
	finishJob(t, js, old.ID)

	// Case 2: a still-queued (non-terminal) job is kept.
	queued, _ := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")

	// Case 3: a terminal parent whose child is still running must be kept.
	parent, _ := js.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	finishJob(t, js, parent.ID)
	child, _ := js.StartChild(ctx, "migrate", json.RawMessage(`{}`), parent.ID) // running

	n, err := js.PruneJobs(ctx, time.Now().Add(time.Hour)) // everything finished is "old"
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d, want 1 (only the standalone terminal job)", n)
	}
	if _, err := js.GetJob(ctx, old.ID); err != ErrNotFound {
		t.Fatalf("standalone job should be gone, err=%v", err)
	}
	if _, err := js.GetJob(ctx, queued.ID); err != nil {
		t.Fatalf("queued job should survive: %v", err)
	}
	if _, err := js.GetJob(ctx, parent.ID); err != nil {
		t.Fatalf("parent with running child should survive: %v", err)
	}

	// Now finish the child and prune again: both parent and child go.
	finishJob(t, js, child.ID)
	n, err = js.PruneJobs(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("pruned %d, want 2 (parent + child)", n)
	}
	if _, err := js.GetJob(ctx, parent.ID); err != ErrNotFound {
		t.Fatalf("parent should be gone, err=%v", err)
	}
	if _, err := js.GetJob(ctx, child.ID); err != ErrNotFound {
		t.Fatalf("child should be gone, err=%v", err)
	}

	// Recent terminal jobs (cutoff in the past) are kept.
	recent, _ := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
	finishJob(t, js, recent.ID)
	n, err = js.PruneJobs(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pruned %d, want 0 (nothing older than the past cutoff)", n)
	}
	if _, err := js.GetJob(ctx, recent.ID); err != nil {
		t.Fatalf("recent job should survive: %v", err)
	}
}

func TestPrune_SQLite(t *testing.T) { testPruneOn(t, openJobStore(t)) }
func TestPrune_Memory(t *testing.T) { testPruneOn(t, NewMemory()) }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestPrune ./internal/store/`
Expected: FAIL to compile — `PruneJobs` undefined on both stores.

- [ ] **Step 3: Add `PruneJobs` to the interface**

In `internal/store/jobs.go`, add to the `JobStore` interface right after the `FailRunning` line:

```go
	// PruneJobs deletes terminal (succeeded/failed) jobs finished before
	// olderThan, preserving parent/child integrity: a parent row is deleted only
	// when it has no surviving child. Returns the number of rows deleted.
	PruneJobs(ctx context.Context, olderThan time.Time) (int, error)
```

(`time` is already imported in jobs.go.)

- [ ] **Step 4: Implement SQLite `PruneJobs`**

In `internal/store/sqlite.go`, add after `FailRunning`:

```go
func (s *SQLite) PruneJobs(ctx context.Context, olderThan time.Time) (int, error) {
	cutoff := olderThan.UnixNano()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	// 1) Old terminal children first, so their parents can then be considered.
	rc, err := tx.ExecContext(ctx, `
DELETE FROM jobs
WHERE parent_id IS NOT NULL AND state IN ('succeeded','failed')
  AND finished IS NOT NULL AND finished < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	nChild, err := rc.RowsAffected()
	if err != nil {
		return 0, err
	}

	// 2) Old terminal jobs not referenced as a parent by any surviving row.
	rp, err := tx.ExecContext(ctx, `
DELETE FROM jobs
WHERE state IN ('succeeded','failed') AND finished IS NOT NULL AND finished < ?
  AND id NOT IN (SELECT parent_id FROM jobs WHERE parent_id IS NOT NULL)`, cutoff)
	if err != nil {
		return 0, err
	}
	nParent, err := rp.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int(nChild + nParent), nil
}
```

- [ ] **Step 5: Implement Memory `PruneJobs`**

In `internal/store/memory.go`, add after `FailRunning`:

```go
func (m *Memory) PruneJobs(_ context.Context, olderThan time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	terminal := func(j Job) bool { return j.State == JobSucceeded || j.State == JobFailed }
	old := func(j Job) bool {
		return !j.Finished.IsZero() && j.Finished.Before(olderThan)
	}

	// Pass 1: delete old terminal children.
	kept := m.jobs[:0:0]
	deleted := 0
	for _, j := range m.jobs {
		if j.ParentID != "" && terminal(j) && old(j) {
			deleted++
			continue
		}
		kept = append(kept, j)
	}
	m.jobs = kept

	// Pass 2: delete old terminal jobs not referenced as a parent by survivors.
	referenced := map[string]bool{}
	for _, j := range m.jobs {
		if j.ParentID != "" {
			referenced[j.ParentID] = true
		}
	}
	kept = m.jobs[:0:0]
	for _, j := range m.jobs {
		if terminal(j) && old(j) && !referenced[j.ID] {
			deleted++
			continue
		}
		kept = append(kept, j)
	}
	m.jobs = kept
	return deleted, nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestPrune ./internal/store/`
Expected: PASS (both backends).

- [ ] **Step 7: Run the whole store package, then commit**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/`
Expected: PASS

```bash
gofmt -l internal/store/
git add internal/store/jobs.go internal/store/sqlite.go internal/store/memory.go internal/store/jobs_prune_test.go
git commit -m "feat(store): PruneJobs retention preserving parent/child (#45)"
```

---

### Task 5: Retention sweep in the runner

Add `StartRetention` to the runner — a periodic, WaitGroup-tracked prune.

**Files:**
- Modify: `internal/jobs/runner.go` (add the `retentionInterval` const and `StartRetention` method; `time`/`context`/`log` are already imported)
- Test: `internal/jobs/retention_test.go` (Create)

- [ ] **Step 1: Write the failing test**

Create `internal/jobs/retention_test.go`:

```go
package jobs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

func TestRunner_StartRetention_PrunesOldTerminalJobs(t *testing.T) {
	m := store.NewMemory()
	ctx := context.Background()

	// One finished job that will be older than the (tiny) retention window.
	j, _ := m.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
	_ = m.Finish(ctx, j.ID, store.JobSucceeded, "")

	r := NewRunner(m, Registry{}, 1)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Retention of 1ns: the already-finished job is immediately "old"; the
	// initial sweep should remove it.
	r.StartRetention(rctx, time.Nanosecond)

	waitFor(t, func() bool {
		_, err := m.GetJob(ctx, j.ID)
		return err == store.ErrNotFound
	})
}

func TestRunner_StartRetention_DisabledByZero(t *testing.T) {
	m := store.NewMemory()
	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	_ = m.Finish(context.Background(), j.ID, store.JobSucceeded, "")

	r := NewRunner(m, Registry{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.StartRetention(ctx, 0) // disabled → no goroutine, nothing pruned

	time.Sleep(50 * time.Millisecond)
	if _, err := m.GetJob(context.Background(), j.ID); err != nil {
		t.Fatalf("job should survive when retention disabled: %v", err)
	}
}
```

(`waitFor` already exists in `runner_test.go` in the same package.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestRunner_StartRetention ./internal/jobs/`
Expected: FAIL to compile — `StartRetention` undefined.

- [ ] **Step 3: Implement `StartRetention`**

In `internal/jobs/runner.go`, add after the `Wait` method:

```go
// retentionInterval is how often StartRetention sweeps after its initial run.
const retentionInterval = time.Hour

// StartRetention periodically prunes terminal jobs older than retention. It is a
// no-op when retention <= 0. It sweeps once immediately, then every
// retentionInterval, until ctx is cancelled. It is tracked by the runner's
// WaitGroup, so Wait blocks for it too.
func (r *Runner) StartRetention(ctx context.Context, retention time.Duration) {
	if retention <= 0 {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		sweep := func() {
			n, err := r.store.PruneJobs(ctx, time.Now().Add(-retention))
			if err != nil {
				log.Printf("jobs: retention sweep failed: %v", err)
				return
			}
			if n > 0 {
				log.Printf("jobs: retention pruned %d terminal job(s)", n)
			}
		}
		sweep() // prompt first pass so a restart cleans up immediately
		t := time.NewTicker(retentionInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sweep()
			}
		}
	}()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestRunner_StartRetention ./internal/jobs/`
Expected: PASS

- [ ] **Step 5: Run the whole jobs package (with -race), then commit**

Run: `go test -race -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/jobs/`
Expected: PASS, no race warnings.

```bash
gofmt -l internal/jobs/
git add internal/jobs/runner.go internal/jobs/retention_test.go
git commit -m "feat(jobs): periodic retention sweep (#45)"
```

---

### Task 6: API pagination params

Parse `?limit=` / `?before=` in `listJobs` and reject non-integer limits.

**Files:**
- Modify: `internal/api/jobs.go` (the `listJobs` handler ~line 58-80; add `strconv` import)
- Test: `internal/api/jobs_pagination_test.go` (Create)

The harness already exists in `internal/api/jobs_test.go`: `newSrvWithJobs(t, js store.JobStore) (*httptest.Server, string)` builds a test server (key scope `jobs:read`, sufficient for `GET /jobs`) wired to the given job store and returns `(srv, token)`; `authedReq(t, srv, tok, method, path) *http.Response` makes an authenticated request. Use these directly. The error body shape is `{"code": "...", "message": "..."}` (`ErrorBody` in `errors.go`).

- [ ] **Step 1: Write the failing test**

Create `internal/api/jobs_pagination_test.go`:

```go
package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/iotready/podman-api/internal/store"
)

func TestJobs_Pagination(t *testing.T) {
	js := store.NewMemory()
	ctx := context.Background()
	var ids []string // oldest..newest
	for i := 0; i < 3; i++ {
		j, err := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	srv, tok := newSrvWithJobs(t, js)

	decode := func(resp *http.Response) []map[string]any {
		t.Helper()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status %d: %s", resp.StatusCode, b)
		}
		var list []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
			t.Fatal(err)
		}
		return list
	}

	// limit caps the page; newest first.
	p1 := decode(authedReq(t, srv, tok, "GET", "/jobs?limit=1"))
	if len(p1) != 1 || p1[0]["id"] != ids[2] {
		t.Fatalf("page1: %+v", p1)
	}

	// before cursor returns the next page.
	p2 := decode(authedReq(t, srv, tok, "GET", "/jobs?limit=1&before="+ids[2]))
	if len(p2) != 1 || p2[0]["id"] != ids[1] {
		t.Fatalf("page2: %+v", p2)
	}

	// no params → bare array bounded by the default limit (3 here).
	all := decode(authedReq(t, srv, tok, "GET", "/jobs"))
	if len(all) != 3 {
		t.Fatalf("all: %+v", all)
	}

	// non-integer limit → 400 invalid_query.
	resp := authedReq(t, srv, tok, "GET", "/jobs?limit=abc")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad limit status = %d, want 400", resp.StatusCode)
	}
	var eb map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&eb); err != nil {
		t.Fatal(err)
	}
	if eb["code"] != "invalid_query" {
		t.Fatalf("error code = %v, want invalid_query", eb["code"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'Pagination' ./internal/api/`
Expected: FAIL — limit/before are ignored (no clamping/cursor) and `?limit=abc` returns 200, not 400.

NOTE: the API error idiom in this package is `WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "...", Message: "..."})` (see `internal/api/logs.go`, `migrate.go`). Bad query params use code `invalid_query`. There is no `APIError` type — use `ErrorBody` directly as shown in Step 3.

- [ ] **Step 3: Parse the params**

In `internal/api/jobs.go`, add `"strconv"` to the imports, and update `listJobs`:

```go
func (h *handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	// Empty state/kind/parent_id query params become zero-value filter fields,
	// which the store treats as "match all".
	f := store.JobFilter{
		State:    store.JobState(r.URL.Query().Get("state")),
		Kind:     r.URL.Query().Get("kind"),
		ParentID: r.URL.Query().Get("parent_id"),
		Before:   r.URL.Query().Get("before"),
	}
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_query", Message: "limit must be an integer"})
			return
		}
		f.Limit = n // store clamps to [1, MaxJobLimit]
	}
	jobs, err := h.jobs.ListJobs(r.Context(), f)
	if err != nil {
		WriteError(w, err)
		return
	}
	out := make([]jobView, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toJobView(j))
	}
	WriteJSON(w, http.StatusOK, out)
}
```

The `ErrorBody{Code: "invalid_query", ...}` literal above IS the real package idiom (confirmed against `internal/api/logs.go` and `migrate.go` — `WriteJSON` + `ErrorBody`, no `APIError` type). `ErrorBody` is already defined in `internal/api/errors.go` in this same package, so no extra import is needed for it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'Pagination' ./internal/api/`
Expected: PASS

- [ ] **Step 5: Run the whole api package, then commit**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/`
Expected: PASS

```bash
gofmt -l internal/api/
git add internal/api/jobs.go internal/api/jobs_pagination_test.go
git commit -m "feat(api): paginate GET /jobs with limit + before (#45)"
```

---

### Task 7: Wire the `-jobs-retention` flag

Expose retention as an operator flag and start the sweep.

**Files:**
- Modify: `cmd/podman-api/main.go` (the flag block ~line 33-40; the runner-start block ~line 93-104)

- [ ] **Step 1: Add the flag**

In `cmd/podman-api/main.go`, in the `flag.*` var block, add after `specKeyFile`:

```go
		jobsRetention = flag.Duration("jobs-retention", 0, "if >0, prune terminal jobs older than this (e.g. 168h); 0 disables")
```

- [ ] **Step 2: Start the sweep**

In the `if db != nil {` block, after `runner.Start(runnerCtx)`:

```go
		runner.Start(runnerCtx)
		if *jobsRetention > 0 {
			runner.StartRetention(runnerCtx, *jobsRetention)
			log.Printf("jobs retention enabled: pruning terminal jobs older than %s", *jobsRetention)
		}
		log.Printf("desired-state store enabled: %s (job runner started)", *stateDB)
```

- [ ] **Step 3: Build and run cmd tests**

Run: `go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -o /tmp/podman-api ./cmd/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./cmd/podman-api/`
Expected: build succeeds; tests PASS.

- [ ] **Step 4: Verify the flag is registered**

Run: `/tmp/podman-api -h 2>&1 | grep jobs-retention`
Expected: shows `-jobs-retention duration` with the help text.

- [ ] **Step 5: Commit**

```bash
gofmt -l cmd/
git add cmd/podman-api/main.go
git commit -m "feat(cmd): -jobs-retention flag to enable the prune sweep (#45)"
```

---

### Task 8: Docs — OpenAPI + README

Document the new query params and flag.

**Files:**
- Modify: `api/openapi.yaml` (the `GET /jobs` operation parameters)
- Modify: `README.md` (jobs row in the API quick reference; flags/state-store note)

- [ ] **Step 1: Inspect current docs**

Run: `grep -n "/jobs" api/openapi.yaml` and read the `GET /jobs` operation, noting how `state`/`kind`/`parent_id` query params are declared. Run `grep -n "parent_id\|/jobs\|state-db\|spec-key-file" README.md` to find the jobs reference rows and the flags section.

- [ ] **Step 2: Add OpenAPI params**

In `api/openapi.yaml`, under the `GET /jobs` operation's `parameters:` list (alongside the existing `state`/`kind`/`parent_id`), add:

```yaml
        - in: query
          name: limit
          schema: { type: integer, default: 100, minimum: 1, maximum: 1000 }
          description: Max jobs to return (newest first). Defaults to 100; values above 1000 are clamped.
        - in: query
          name: before
          schema: { type: string }
          description: Cursor — return only jobs older than this job id (pass the previous page's last id). Page until fewer than `limit` rows are returned.
```

Match the exact indentation/style of the surrounding parameter entries in the file.

- [ ] **Step 3: Update README**

In `README.md`, update the `GET /jobs` quick-reference row to mention pagination, e.g. append to its description: `(paginated: ?limit= default 100, ?before=<id> cursor)`. In the state-store / flags description (near `-state-db` / `-spec-key-file`), add a line:

```
- `-jobs-retention <dur>` — when set (e.g. `168h`), a background sweep prunes terminal jobs older than the duration, keeping parent/child families intact. Default `0` (disabled; jobs accumulate until manual cleanup).
```

Match the README's existing list/format style.

- [ ] **Step 4: Verify OpenAPI still parses (spot-check test)**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run OpenAPI ./internal/api/`
Expected: PASS (the openapi spot-check test, if present, still finds `/jobs`).

- [ ] **Step 5: Commit**

```bash
git add api/openapi.yaml README.md
git commit -m "docs: pagination params + -jobs-retention flag (#45)"
```

---

### Task 9: Full verification

**Files:** none (verification only).

- [ ] **Step 1: Full test suite**

Run: `make test`
Expected: every package `ok`.

- [ ] **Step 2: Race check on the changed packages**

Run: `go test -race -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ ./internal/jobs/ ./internal/api/`
Expected: PASS, no race warnings. (Note: `internal/store/TestSQLite_ClaimNext_NoDoubleClaim` is a known pre-existing flaky test under load — if it alone fails, re-run it in isolation to confirm it is not a regression from this work.)

- [ ] **Step 2: gofmt + vet**

Run: `gofmt -l . && go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`
Expected: gofmt prints nothing; vet is silent.

---

## Self-Review

- **Spec coverage:** A=Task 1, B=Task 2, C=Tasks 3+6, D=Tasks 4+5+7, E=Task 8. All spec sections map to tasks. ✓
- **Placeholders:** Task 6 deliberately leaves the API test harness to be matched against the existing package idiom (the error-type literal is flagged as illustrative with explicit instructions to use the real `errors.go` constructor) — this is necessary because the exact `APIError` shape must be read from the codebase, not guessed. Everything else is concrete. ✓
- **Type consistency:** `JobFilter.Limit int`/`Before string`, `clampJobLimit`, `DefaultJobLimit`/`MaxJobLimit`, `PruneJobs(ctx, time.Time) (int, error)`, `StartRetention(ctx, time.Duration)`, `retentionInterval` are used identically across tasks. ✓
