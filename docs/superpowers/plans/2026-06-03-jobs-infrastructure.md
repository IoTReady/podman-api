# Jobs Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an async job subsystem — a `jobs` table + `JobStore`, a background `Runner` with a per-kind `Handler` registry, read-only `GET /jobs` endpoints, and crash-safe boot recovery — that `migrate`/`evacuate` (#34/#35) will run on. Also resolves #42 (raise the SQLite pool + add `busy_timeout`).

**Architecture:** Job persistence lives in `internal/store` (new `jobs` table on the existing SQLite file, a `JobStore` interface implemented by the same `*SQLite`, and a combined `store.DB` interface). A new `internal/jobs` package holds the `Runner` (bounded worker pool) and `Handler`/`Registry`/`JobContext`. The API gains `jobs:read` endpoints that return `501` when the store is disabled. The whole subsystem is gated behind `-state-db`. No real job kinds ship in this phase — migrate/evacuate register handlers later; #32 is tested with a fake handler.

**Tech Stack:** Go, `modernc.org/sqlite` (pure-Go), `database/sql`, Go 1.22 `net/http` mux.

**Spec:** `docs/superpowers/specs/2026-06-03-jobs-infrastructure-design.md`

---

## Conventions for every task

- **Build tags.** `internal/store` and `internal/jobs` are pure Go (plain `go test ./internal/store/ ./internal/jobs/` — no tags). `internal/api`, `cmd/podman-api`, and `internal/instance` import podman, so their tests need:
  ```sh
  export TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"
  ```
- `gofmt -w` every file; `gofmt -l .` must be empty. `go vet -tags "$TAGS" ./...` clean.
- **Commit trailer** on every commit:
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  ```
- Forgejo repo `tej/podman-api`; no `gh`.

## File structure

| File | Responsibility | Task |
| --- | --- | --- |
| `internal/store/jobs.go` | `Job`/`JobState`/`JobStep`/`JobFilter` types, `JobStore` + `DB` interfaces, `newJobID` | 1 |
| `internal/store/jobs_test.go` | id-generator + (later) sqlite jobstore tests | 1,3 |
| `internal/store/sqlite.go` | #42 pool/`busy_timeout`; `jobs` table in schema; `user_version=2`; SQLite `JobStore` methods | 2,3 |
| `internal/store/sqlite_concurrency_test.go` | schema-present + concurrent-access test | 2 |
| `internal/store/memory.go` | `Memory` extended to implement `JobStore` | 4 |
| `internal/store/memory_jobs_test.go` | memory jobstore tests | 4 |
| `internal/jobs/runner.go` | `Runner`, `Handler`, `Registry`, `JobContext` | 5 |
| `internal/jobs/runner_test.go` | runner behaviour tests | 5 |
| `internal/api/jobs.go` | `listJobs`/`getJob` handlers, `jobView`, `errJobsDisabled` | 6 |
| `internal/api/jobs_test.go` | jobs endpoint tests | 6 |
| `internal/api/router.go` | `jobs` param + routes | 6 |
| `internal/api/errors.go` | classify `store.ErrNotFound`→404, `errJobsDisabled`→501 | 6 |
| `cmd/podman-api/main.go` | `openStore`→`store.DB`; build+start runner; pass JobStore to router | 7 |
| `api/openapi.yaml`, `README.md` | document `/jobs` + `jobs:read` | 8 |

---

## Task 1: Store job types, interfaces, and ID generator

**Files:**
- Create: `internal/store/jobs.go`
- Create: `internal/store/jobs_test.go`

- [ ] **Step 1: Write the failing test** (`internal/store/jobs_test.go`)

```go
package store

import (
	"regexp"
	"testing"
)

func TestNewJobID_FormatAndUnique(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{16}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := newJobID()
		if !re.MatchString(id) {
			t.Fatalf("id %q does not match expected format", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/ -run TestNewJobID -v`
Expected: FAIL — `undefined: newJobID`.

- [ ] **Step 3: Write the implementation** (`internal/store/jobs.go`)

```go
package store

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// JobState is the lifecycle state of a job row.
type JobState string

const (
	JobQueued    JobState = "queued"
	JobRunning   JobState = "running"
	JobSucceeded JobState = "succeeded"
	JobFailed    JobState = "failed"
)

// JobStep is one progress entry recorded by a handler.
type JobStep struct {
	TS     time.Time `json:"ts"`
	Step   string    `json:"step"`
	Detail string    `json:"detail,omitempty"`
}

// Job is one row of the jobs table.
type Job struct {
	ID       string
	Kind     string
	Args     json.RawMessage // opaque to the store; handlers unmarshal their own shape
	State    JobState
	Steps    []JobStep
	ParentID string // "" if none
	Error    string
	Created  time.Time
	Started  time.Time // zero until claimed
	Finished time.Time // zero until done
}

// JobFilter narrows ListJobs. Empty fields match anything.
type JobFilter struct {
	State JobState
	Kind  string
}

// JobStore persists and dispenses jobs. Implemented by *SQLite and *Memory.
type JobStore interface {
	// Enqueue inserts a new queued job, generating its ID. parentID is "" for
	// top-level jobs.
	Enqueue(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error)
	GetJob(ctx context.Context, id string) (Job, error) // ErrNotFound when absent
	ListJobs(ctx context.Context, f JobFilter) ([]Job, error)
	// ClaimNext atomically transitions the oldest queued job to running and
	// returns it. ok=false when there is nothing to claim.
	ClaimNext(ctx context.Context) (job Job, ok bool, err error)
	AppendStep(ctx context.Context, id string, step JobStep) error
	// Finish sets the terminal state, finished timestamp, and error (empty for success).
	Finish(ctx context.Context, id string, state JobState, errMsg string) error
	// FailRunning marks every job still in running as failed with reason; returns
	// the count. Called once at startup to reap crash-interrupted jobs.
	FailRunning(ctx context.Context, reason string) (int, error)
}

// DB is the full backend: spec store + job store + closer. main holds one of
// these; instance.Service takes the Store view, the runner takes the JobStore view.
type DB interface {
	Store
	JobStore
	io.Closer
}

// newJobID returns a sortable, time-prefixed unique id: 16 hex digits of the
// creation time in unix-nanoseconds, a dash, then 6 random bytes (12 hex).
func newJobID() string {
	var b [6]byte
	_, _ = io.ReadFull(rand.Reader, b[:])
	return fmt.Sprintf("%016x-%x", uint64(time.Now().UnixNano()), b)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/store/ -run TestNewJobID -v`
Expected: PASS.

- [ ] **Step 5: Verify the whole store package still builds** (the new interfaces aren't implemented yet, that's fine — they're just type decls)

Run: `go test ./internal/store/ 2>&1 | tail -3`
Expected: existing tests PASS (nothing implements `JobStore`/`DB` yet, but that's not a compile error — interfaces need no implementors to compile).

- [ ] **Step 6: Commit**

```bash
gofmt -w internal/store/jobs.go internal/store/jobs_test.go
git add internal/store/jobs.go internal/store/jobs_test.go
git commit -m "feat(store): job types, JobStore/DB interfaces, sortable job id (#32)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: #42 pool/busy_timeout + jobs schema

**Files:**
- Modify: `internal/store/sqlite.go`
- Create: `internal/store/sqlite_concurrency_test.go`

- [ ] **Step 1: Write the failing test** (`internal/store/sqlite_concurrency_test.go`)

```go
package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

func TestSQLite_JobsTableExists(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	var name string
	err := s.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'`).Scan(&name)
	if err != nil {
		t.Fatalf("jobs table not found: %v", err)
	}
	if name != "jobs" {
		t.Fatalf("got table %q", name)
	}
}

func TestSQLite_BusyTimeoutSet(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	var ms int
	if err := s.db.QueryRow(`PRAGMA busy_timeout`).Scan(&ms); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if ms != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", ms)
	}
}

func TestSQLite_ConcurrentSpecWrites_NoLockError(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "state.db")
	s, err := OpenSQLite(db, NewKeyStore(testKey(0x11)))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sp := sampleSpec()
			sp.Slug = string(rune('a' + i))
			if err := s.PutSpec(ctx, sp); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent PutSpec failed (likely 'database is locked'): %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/ -run 'TestSQLite_JobsTableExists|TestSQLite_BusyTimeoutSet' -v`
Expected: FAIL — jobs table missing / busy_timeout is 0 (default).

- [ ] **Step 3: Edit `internal/store/sqlite.go`**

(a) Add the jobs table to the `schemaSQL` const. Replace the whole const with:
```go
const schemaSQL = `
CREATE TABLE IF NOT EXISTS specs (
  host       TEXT NOT NULL,
  template   TEXT NOT NULL,
  slug       TEXT NOT NULL,
  parameters TEXT NOT NULL,
  secrets    BLOB NOT NULL,
  created    INTEGER NOT NULL,
  updated    INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug)
);
CREATE TABLE IF NOT EXISTS jobs (
  id        TEXT PRIMARY KEY,
  kind      TEXT NOT NULL,
  args      TEXT NOT NULL,
  state     TEXT NOT NULL,
  steps     TEXT NOT NULL DEFAULT '[]',
  parent_id TEXT,
  error     TEXT,
  created   INTEGER NOT NULL,
  started   INTEGER,
  finished  INTEGER
);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);`
```
Note: `db.Exec(schemaSQL)` already runs multiple statements in one call (modernc supports it — the existing single-table version is one statement; multiple is fine).

(b) Add a named const near the top of the file (after the imports):
```go
// maxOpenConns bounds the SQLite connection pool. >1 enables WAL reader
// concurrency (API reads while the job runner writes); a competing writer waits
// up to busy_timeout rather than failing with "database is locked".
const maxOpenConns = 4
```

(c) In `OpenSQLite`, replace the pool + comment block:
```go
	// SQLite is single-writer; cap the pool to one connection to avoid
	// "database is locked" under concurrent Apply/Delete.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// WAL mode: cheaper commits and fewer fsyncs than the default rollback
	// journal. Its cross-connection read-while-write concurrency needs >1 open
	// connection; with MaxOpenConns(1) that benefit is not yet realized — the
	// jobs phase (#32) will raise the cap and add a busy_timeout (see #42).
	if _, err := db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		_ = db.Close()
		return nil, err
	}
```
with:
```go
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns)
	// WAL + busy_timeout: WAL gives many concurrent readers + one writer (the
	// API reads /jobs while the background runner writes); busy_timeout makes a
	// competing writer wait rather than failing with "database is locked".
	if _, err := db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		_ = db.Close()
		return nil, err
	}
```

(d) Bump the user_version. Replace:
```go
	if _, err := db.Exec(`PRAGMA user_version = 1`); err != nil {
```
with:
```go
	if _, err := db.Exec(`PRAGMA user_version = 2`); err != nil {
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store/ -v 2>&1 | tail -15`
Expected: the 3 new tests PASS; all pre-existing store tests still PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/store/sqlite.go internal/store/sqlite_concurrency_test.go
git add internal/store/sqlite.go internal/store/sqlite_concurrency_test.go
git commit -m "feat(store): jobs table + raise pool with busy_timeout (closes #42) (#32)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: SQLite JobStore methods

**Files:**
- Modify: `internal/store/sqlite.go` (append the JobStore methods + a row scanner)
- Modify: `internal/store/jobs_test.go` (append the JobStore tests)

- [ ] **Step 1: Write the failing tests** (append to `internal/store/jobs_test.go`)

```go
import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)
// NOTE: merge these into the existing import block at the top of jobs_test.go.

func openJobStore(t *testing.T) *SQLite {
	t.Helper()
	return openTestStore(t, NewKeyStore(testKey(0x11)))
}

func TestSQLite_Enqueue_Get(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, err := s.Enqueue(ctx, "migrate", json.RawMessage(`{"from":"h1"}`), "")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if j.ID == "" || j.State != JobQueued || j.Created.IsZero() {
		t.Fatalf("bad enqueued job: %+v", j)
	}
	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Kind != "migrate" || string(got.Args) != `{"from":"h1"}` || got.State != JobQueued {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestSQLite_GetJob_Missing(t *testing.T) {
	if _, err := openJobStore(t).GetJob(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestSQLite_ListJobs_FilterAndOrder(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	a, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	b, _ := s.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	_ = a
	all, err := s.ListJobs(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) != 2 || all[0].ID != b.ID {
		t.Fatalf("expected newest-first, got %d jobs head=%v", len(all), all[0].ID)
	}
	mig, _ := s.ListJobs(ctx, JobFilter{Kind: "migrate"})
	if len(mig) != 1 || mig[0].Kind != "migrate" {
		t.Fatalf("kind filter failed: %+v", mig)
	}
}

func TestSQLite_ClaimNext_AndEmpty(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	if _, ok, err := s.ClaimNext(ctx); err != nil || ok {
		t.Fatalf("empty claim: ok=%v err=%v", ok, err)
	}
	first, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	j, ok, err := s.ClaimNext(ctx)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if j.ID != first.ID || j.State != JobRunning || j.Started.IsZero() {
		t.Fatalf("bad claimed job: %+v", j)
	}
	if _, ok, _ := s.ClaimNext(ctx); ok {
		t.Fatal("second claim should find nothing (only running left)")
	}
}

func TestSQLite_ClaimNext_NoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	const n = 20
	for i := 0; i < n; i++ {
		if _, err := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), ""); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, ok, err := s.ClaimNext(ctx)
				if err != nil || !ok {
					return
				}
				mu.Lock()
				claimed[j.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(claimed) != n {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), n)
	}
	for id, c := range claimed {
		if c != 1 {
			t.Fatalf("job %s claimed %d times", id, c)
		}
	}
}

func TestSQLite_AppendStep_Finish_FailRunning(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = s.ClaimNext(ctx)
	if err := s.AppendStep(ctx, j.ID, JobStep{TS: time.Unix(100, 0), Step: "stop", Detail: "src"}); err != nil {
		t.Fatalf("AppendStep: %v", err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if len(got.Steps) != 1 || got.Steps[0].Step != "stop" {
		t.Fatalf("step not recorded: %+v", got.Steps)
	}
	if err := s.Finish(ctx, j.ID, JobSucceeded, ""); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	got, _ = s.GetJob(ctx, j.ID)
	if got.State != JobSucceeded || got.Finished.IsZero() {
		t.Fatalf("finish not applied: %+v", got)
	}

	// FailRunning reaps a second job left running.
	j2, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = s.ClaimNext(ctx)
	n, err := s.FailRunning(ctx, "interrupted")
	if err != nil || n != 1 {
		t.Fatalf("FailRunning n=%d err=%v", n, err)
	}
	got2, _ := s.GetJob(ctx, j2.ID)
	if got2.State != JobFailed || got2.Error != "interrupted" {
		t.Fatalf("FailRunning did not mark job: %+v", got2)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/ -run 'TestSQLite_Enqueue|TestSQLite_GetJob|TestSQLite_ListJobs|TestSQLite_ClaimNext|TestSQLite_AppendStep' -v`
Expected: FAIL — `s.Enqueue undefined` etc.

- [ ] **Step 3: Append the JobStore implementation to `internal/store/sqlite.go`**

Add these imports to `sqlite.go`'s import block if missing: `strings`. (`context`, `database/sql`, `encoding/json`, `errors`, `time` are already imported.)

```go
const jobColumns = `id, kind, args, state, steps, parent_id, error, created, started, finished`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(sc rowScanner) (Job, error) {
	var (
		j                 Job
		args, steps       string
		parent, errMsg    sql.NullString
		created           int64
		started, finished sql.NullInt64
	)
	if err := sc.Scan(&j.ID, &j.Kind, &args, &j.State, &steps, &parent, &errMsg, &created, &started, &finished); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, err
	}
	j.Args = json.RawMessage(args)
	if err := json.Unmarshal([]byte(steps), &j.Steps); err != nil {
		return Job{}, err
	}
	j.ParentID = parent.String
	j.Error = errMsg.String
	j.Created = time.Unix(created, 0)
	if started.Valid {
		j.Started = time.Unix(started.Int64, 0)
	}
	if finished.Valid {
		j.Finished = time.Unix(finished.Int64, 0)
	}
	return j, nil
}

func (s *SQLite) Enqueue(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	id := newJobID()
	now := time.Now().Unix()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	var parent any
	if parentID != "" {
		parent = parentID
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO jobs (id, kind, args, state, steps, parent_id, error, created, started, finished)
VALUES (?, ?, ?, 'queued', '[]', ?, NULL, ?, NULL, NULL)`,
		id, kind, string(args), parent, now)
	if err != nil {
		return Job{}, err
	}
	return s.GetJob(ctx, id)
}

func (s *SQLite) GetJob(ctx context.Context, id string) (Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=?`, id)
	return scanJob(row)
}

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
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created DESC, id DESC"
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

func (s *SQLite) ClaimNext(ctx context.Context) (Job, bool, error) {
	now := time.Now().Unix()
	row := s.db.QueryRowContext(ctx, `
UPDATE jobs SET state='running', started=?
WHERE id = (SELECT id FROM jobs WHERE state='queued' ORDER BY created, id LIMIT 1)
  AND state='queued'
RETURNING `+jobColumns, now)
	j, err := scanJob(row)
	if errors.Is(err, ErrNotFound) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	return j, true, nil
}

func (s *SQLite) AppendStep(ctx context.Context, id string, step JobStep) error {
	// Only the worker running this job appends steps, so there is no concurrent
	// AppendStep for the same id — a read-modify-write is safe.
	var steps string
	if err := s.db.QueryRowContext(ctx, `SELECT steps FROM jobs WHERE id=?`, id).Scan(&steps); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	var arr []JobStep
	if err := json.Unmarshal([]byte(steps), &arr); err != nil {
		return err
	}
	arr = append(arr, step)
	b, err := json.Marshal(arr)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE jobs SET steps=? WHERE id=?`, string(b), id)
	return err
}

func (s *SQLite) Finish(ctx context.Context, id string, state JobState, errMsg string) error {
	now := time.Now().Unix()
	var e any
	if errMsg != "" {
		e = errMsg
	}
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET state=?, error=?, finished=? WHERE id=?`,
		string(state), e, now, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) FailRunning(ctx context.Context, reason string) (int, error) {
	now := time.Now().Unix()
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET state='failed', error=?, finished=? WHERE state='running'`,
		reason, now)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}
```

- [ ] **Step 4: Run tests to verify they pass (including the race detector for the claim test)**

Run: `go test ./internal/store/ -v 2>&1 | tail -20`
Then: `go test -race ./internal/store/ -run TestSQLite_ClaimNext_NoDoubleClaim -v`
Expected: all PASS; the concurrent-claim test passes under `-race`.

- [ ] **Step 5: Confirm `*SQLite` satisfies the interfaces** — add a compile-time assertion at the bottom of `sqlite.go`:

```go
var _ DB = (*SQLite)(nil)
```

Run: `go build ./internal/store/`
Expected: compiles (proves `*SQLite` implements `Store`, `JobStore`, and `io.Closer`).

- [ ] **Step 6: Commit**

```bash
gofmt -w internal/store/sqlite.go internal/store/jobs_test.go
git add internal/store/sqlite.go internal/store/jobs_test.go
git commit -m "feat(store): SQLite JobStore (enqueue/claim/finish/recover) (#32)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: In-memory JobStore (test double)

**Files:**
- Modify: `internal/store/memory.go`
- Create: `internal/store/memory_jobs_test.go`

- [ ] **Step 1: Write the failing tests** (`internal/store/memory_jobs_test.go`)

```go
package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestMemory_Jobs_EnqueueClaimFinish(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	j, err := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	if err != nil || j.State != JobQueued {
		t.Fatalf("enqueue: %+v err=%v", j, err)
	}
	c, ok, err := m.ClaimNext(ctx)
	if err != nil || !ok || c.ID != j.ID || c.State != JobRunning {
		t.Fatalf("claim: %+v ok=%v err=%v", c, ok, err)
	}
	if _, ok, _ := m.ClaimNext(ctx); ok {
		t.Fatal("nothing left to claim")
	}
	if err := m.AppendStep(ctx, j.ID, JobStep{Step: "x"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Finish(ctx, j.ID, JobSucceeded, ""); err != nil {
		t.Fatalf("finish: %v", err)
	}
	got, _ := m.GetJob(ctx, j.ID)
	if got.State != JobSucceeded || len(got.Steps) != 1 {
		t.Fatalf("final: %+v", got)
	}
}

func TestMemory_Jobs_GetMissing(t *testing.T) {
	if _, err := NewMemory().GetJob(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestMemory_Jobs_FailRunning(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	j, _ := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = m.ClaimNext(ctx)
	n, err := m.FailRunning(ctx, "boom")
	if err != nil || n != 1 {
		t.Fatalf("failrunning n=%d err=%v", n, err)
	}
	got, _ := m.GetJob(ctx, j.ID)
	if got.State != JobFailed || got.Error != "boom" {
		t.Fatalf("not failed: %+v", got)
	}
}

func TestMemory_Jobs_ListFilterNewestFirst(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	_, _ = m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	b, _ := m.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	all, _ := m.ListJobs(ctx, JobFilter{})
	if len(all) != 2 || all[0].ID != b.ID {
		t.Fatalf("newest-first failed: %+v", all)
	}
	ev, _ := m.ListJobs(ctx, JobFilter{Kind: "evacuate"})
	if len(ev) != 1 {
		t.Fatalf("filter failed: %+v", ev)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/ -run TestMemory_Jobs -v`
Expected: FAIL — `m.Enqueue undefined`.

- [ ] **Step 3: Extend `internal/store/memory.go`**

Add a `jobs` slice (ordered by insertion so "newest first" = reverse) and a monotonic counter field to the `Memory` struct, and the JobStore methods. Add imports `encoding/json` and `time`.

Change the struct and constructor:
```go
type Memory struct {
	mu    sync.Mutex
	specs map[string]Spec
	jobs  []Job // insertion order; newest last

	PutErr    error
	DeleteErr error
}

func NewMemory() *Memory {
	return &Memory{specs: map[string]Spec{}}
}
```

Append the JobStore methods (all guard `m.mu`):
```go
func (m *Memory) Enqueue(_ context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	j := Job{
		ID: newJobID(), Kind: kind, Args: args, State: JobQueued,
		Steps: []JobStep{}, ParentID: parentID, Created: time.Now(),
	}
	m.jobs = append(m.jobs, j)
	return j, nil
}

func (m *Memory) GetJob(_ context.Context, id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		if j.ID == id {
			return j, nil
		}
	}
	return Job{}, ErrNotFound
}

func (m *Memory) ListJobs(_ context.Context, f JobFilter) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Job{}
	for i := len(m.jobs) - 1; i >= 0; i-- { // newest first
		j := m.jobs[i]
		if f.State != "" && j.State != f.State {
			continue
		}
		if f.Kind != "" && j.Kind != f.Kind {
			continue
		}
		out = append(out, j)
	}
	return out, nil
}

func (m *Memory) ClaimNext(_ context.Context) (Job, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs { // oldest first
		if m.jobs[i].State == JobQueued {
			m.jobs[i].State = JobRunning
			m.jobs[i].Started = time.Now()
			return m.jobs[i], true, nil
		}
	}
	return Job{}, false, nil
}

func (m *Memory) AppendStep(_ context.Context, id string, step JobStep) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id {
			m.jobs[i].Steps = append(m.jobs[i].Steps, step)
			return nil
		}
	}
	return ErrNotFound
}

func (m *Memory) Finish(_ context.Context, id string, state JobState, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id {
			m.jobs[i].State = state
			m.jobs[i].Error = errMsg
			m.jobs[i].Finished = time.Now()
			return nil
		}
	}
	return ErrNotFound
}

func (m *Memory) FailRunning(_ context.Context, reason string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for i := range m.jobs {
		if m.jobs[i].State == JobRunning {
			m.jobs[i].State = JobFailed
			m.jobs[i].Error = reason
			m.jobs[i].Finished = time.Now()
			n++
		}
	}
	return n, nil
}
```

Add a compile-time assertion at the bottom of `memory.go`:
```go
var _ JobStore = (*Memory)(nil)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store/ -v 2>&1 | tail -20`
Expected: the new memory-jobs tests PASS; all prior store tests still PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/store/memory.go internal/store/memory_jobs_test.go
git add internal/store/memory.go internal/store/memory_jobs_test.go
git commit -m "feat(store): in-memory JobStore test double (#32)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Runner, Handler, Registry, JobContext

**Files:**
- Create: `internal/jobs/runner.go`
- Create: `internal/jobs/runner_test.go`

This package is pure Go (no podman import) — tests run with plain `go test ./internal/jobs/`.

- [ ] **Step 1: Write the failing tests** (`internal/jobs/runner_test.go`)

```go
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// handlerFunc adapts a func to the Handler interface for tests.
type handlerFunc func(ctx context.Context, job store.Job, jc *JobContext) error

func (f handlerFunc) Run(ctx context.Context, job store.Job, jc *JobContext) error {
	return f(ctx, job, jc)
}

// waitFor polls until cond() or the deadline; fails the test on timeout.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestRunner_RunsHandler_Succeeds(t *testing.T) {
	m := store.NewMemory()
	reg := Registry{"test": handlerFunc(func(ctx context.Context, job store.Job, jc *JobContext) error {
		jc.Step("working", "detail")
		return nil
	})}
	r := NewRunner(m, reg, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)

	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	r.Notify()

	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobSucceeded
	})
	got, _ := m.GetJob(context.Background(), j.ID)
	if len(got.Steps) != 1 || got.Steps[0].Step != "working" {
		t.Fatalf("step not recorded: %+v", got.Steps)
	}
}

func TestRunner_HandlerError_Fails(t *testing.T) {
	m := store.NewMemory()
	reg := Registry{"test": handlerFunc(func(ctx context.Context, job store.Job, jc *JobContext) error {
		return errors.New("kaboom")
	})}
	r := NewRunner(m, reg, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	r.Notify()
	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobFailed
	})
	got, _ := m.GetJob(context.Background(), j.ID)
	if got.Error != "kaboom" {
		t.Fatalf("error not recorded: %q", got.Error)
	}
}

func TestRunner_UnknownKind_Fails(t *testing.T) {
	m := store.NewMemory()
	r := NewRunner(m, Registry{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	j, _ := m.Enqueue(context.Background(), "mystery", json.RawMessage(`{}`), "")
	r.Notify()
	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobFailed
	})
	got, _ := m.GetJob(context.Background(), j.ID)
	if got.Error == "" {
		t.Fatal("expected a 'no handler' error message")
	}
}

func TestRunner_BootRecovery_FailsRunning(t *testing.T) {
	m := store.NewMemory()
	// Seed a job stuck in running (simulate a crash mid-flight).
	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	_, _, _ = m.ClaimNext(context.Background())

	r := NewRunner(m, Registry{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)

	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobFailed
	})
}

func TestRunner_CancelStops(t *testing.T) {
	m := store.NewMemory()
	r := NewRunner(m, Registry{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)
	cancel()
	done := make(chan struct{})
	go func() { r.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop after ctx cancel")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/jobs/ -v`
Expected: FAIL — package/`NewRunner` undefined.

- [ ] **Step 3: Write `internal/jobs/runner.go`**

```go
// Package jobs runs queued jobs from a store.JobStore through registered
// per-kind handlers, on a bounded background worker pool.
package jobs

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// DefaultWorkers is the worker-pool size when NewRunner is given workers <= 0.
const DefaultWorkers = 4

// pollInterval is the safety-net wake even without a Notify (e.g. after a
// restart that left queued jobs).
const pollInterval = 5 * time.Second

// Handler executes one job of a given kind. It should honour ctx for cancellation
// and report progress via jc.Step. Returning a non-nil error fails the job.
type Handler interface {
	Run(ctx context.Context, job store.Job, jc *JobContext) error
}

// Registry maps a job kind to its handler.
type Registry map[string]Handler

// JobContext is the handler's progress channel back to the store.
type JobContext struct {
	store store.JobStore
	id    string
}

// Step records a progress entry. It is best-effort: a store error is logged, not
// returned, so progress logging never fails the job.
func (jc *JobContext) Step(step, detail string) {
	if err := jc.store.AppendStep(context.Background(), jc.id, store.JobStep{
		TS: time.Now(), Step: step, Detail: detail,
	}); err != nil {
		log.Printf("jobs: append step to %s failed: %v", jc.id, err)
	}
}

// Runner drains queued jobs and dispatches them to handlers.
type Runner struct {
	store    store.JobStore
	handlers Registry
	workers  int
	poke     chan struct{}
	wg       sync.WaitGroup
}

// NewRunner builds a runner. workers <= 0 uses DefaultWorkers.
func NewRunner(js store.JobStore, h Registry, workers int) *Runner {
	if workers <= 0 {
		workers = DefaultWorkers
	}
	return &Runner{store: js, handlers: h, workers: workers, poke: make(chan struct{}, 1)}
}

// Notify wakes a worker to check for new work; call after an Enqueue. Non-blocking.
func (r *Runner) Notify() {
	select {
	case r.poke <- struct{}{}:
	default:
	}
}

// Start reaps crash-interrupted jobs, then launches the worker pool. It returns
// immediately; the pool runs until ctx is cancelled. Use Wait to block for exit.
func (r *Runner) Start(ctx context.Context) {
	if n, err := r.store.FailRunning(ctx, "interrupted by daemon restart"); err != nil {
		log.Printf("jobs: boot recovery failed: %v", err)
	} else if n > 0 {
		log.Printf("jobs: marked %d interrupted job(s) failed on startup", n)
	}
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go r.worker(ctx)
	}
}

// Wait blocks until all workers have exited (after ctx cancellation).
func (r *Runner) Wait() { r.wg.Wait() }

func (r *Runner) worker(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		// Drain everything currently claimable.
		for {
			if ctx.Err() != nil {
				return
			}
			job, ok, err := r.store.ClaimNext(ctx)
			if err != nil {
				log.Printf("jobs: claim error: %v", err)
				break
			}
			if !ok {
				break
			}
			r.run(ctx, job)
		}
		select {
		case <-ctx.Done():
			return
		case <-r.poke:
		case <-t.C:
		}
	}
}

func (r *Runner) run(ctx context.Context, job store.Job) {
	h, ok := r.handlers[job.Kind]
	if !ok {
		_ = r.store.Finish(ctx, job.ID, store.JobFailed, "no handler for kind "+job.Kind)
		return
	}
	jc := &JobContext{store: r.store, id: job.ID}
	if err := h.Run(ctx, job, jc); err != nil {
		_ = r.store.Finish(ctx, job.ID, store.JobFailed, err.Error())
		return
	}
	_ = r.store.Finish(ctx, job.ID, store.JobSucceeded, "")
}
```

- [ ] **Step 4: Run tests to verify they pass (with the race detector)**

Run: `go test -race ./internal/jobs/ -v`
Expected: all 5 tests PASS, no data races.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/jobs/runner.go internal/jobs/runner_test.go
git add internal/jobs/runner.go internal/jobs/runner_test.go
git commit -m "feat(jobs): background runner + handler registry + JobContext (#32)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: API — GET /jobs, GET /jobs/{id}, jobs:read, 501-when-disabled

**Files:**
- Create: `internal/api/jobs.go`
- Create: `internal/api/jobs_test.go`
- Modify: `internal/api/router.go` (handlers field + `NewRouter` param + routes)
- Modify: `internal/api/errors.go` (classify `store.ErrNotFound`, `errJobsDisabled`)
- Modify: `internal/api/router_test.go` and `internal/api/instances_test.go` (NewRouter call sites)

All api tests use `-tags "$TAGS"`.

- [ ] **Step 1: Write the failing tests** (`internal/api/jobs_test.go`)

```go
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// newSrvWithJobs builds a server whose key has jobs:read and whose router is
// wired to the given JobStore (nil = store disabled).
func newSrvWithJobs(t *testing.T, js store.JobStore) (*httptest.Server, string) {
	t.Helper()
	hash, tok := testKeyPair(t) // defined in instances_test.go helpers
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"jobs:read"}}}
	svc := instance.NewService(fake.New(), []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}, nil)
	srv := httptest.NewServer(NewRouter(svc, js, auth.NewKeyStore(keys), nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestJobs_ListAndGet(t *testing.T) {
	js := store.NewMemory()
	j, _ := js.Enqueue(context.Background(), "migrate", json.RawMessage(`{"from":"h1"}`), "")
	srv, tok := newSrvWithJobs(t, js)

	resp := authedReq(t, srv, tok, "GET", "/jobs")
	if resp.Code != http.StatusOK {
		t.Fatalf("list status %d", resp.Code)
	}
	var list []map[string]any
	_ = json.Unmarshal(resp.Body.Bytes(), &list)
	if len(list) != 1 || list[0]["kind"] != "migrate" {
		t.Fatalf("list body: %s", resp.Body.String())
	}

	resp = authedReq(t, srv, tok, "GET", "/jobs/"+j.ID)
	if resp.Code != http.StatusOK {
		t.Fatalf("get status %d", resp.Code)
	}
	var one map[string]any
	_ = json.Unmarshal(resp.Body.Bytes(), &one)
	if one["id"] != j.ID || one["state"] != "queued" {
		t.Fatalf("get body: %s", resp.Body.String())
	}
}

func TestJobs_Filter(t *testing.T) {
	js := store.NewMemory()
	_, _ = js.Enqueue(context.Background(), "migrate", json.RawMessage(`{}`), "")
	_, _ = js.Enqueue(context.Background(), "evacuate", json.RawMessage(`{}`), "")
	srv, tok := newSrvWithJobs(t, js)
	resp := authedReq(t, srv, tok, "GET", "/jobs?kind=evacuate")
	var list []map[string]any
	_ = json.Unmarshal(resp.Body.Bytes(), &list)
	if len(list) != 1 || list[0]["kind"] != "evacuate" {
		t.Fatalf("filter body: %s", resp.Body.String())
	}
}

func TestJobs_NotFound(t *testing.T) {
	srv, tok := newSrvWithJobs(t, store.NewMemory())
	resp := authedReq(t, srv, tok, "GET", "/jobs/missing")
	if resp.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.Code)
	}
}

func TestJobs_DisabledReturns501(t *testing.T) {
	srv, tok := newSrvWithJobs(t, nil) // store disabled
	resp := authedReq(t, srv, tok, "GET", "/jobs")
	if resp.Code != http.StatusNotImplemented {
		t.Fatalf("want 501, got %d", resp.Code)
	}
	resp = authedReq(t, srv, tok, "GET", "/jobs/x")
	if resp.Code != http.StatusNotImplemented {
		t.Fatalf("want 501 for get, got %d", resp.Code)
	}
}
```

NOTE: `authedReq` and a hash/token helper already exist in the api test package. If a `testKeyPair(t) (hash, token string)` helper does not already exist, the implementer must check `instances_test.go` for how `newSrvFull` derives its hash+token and reuse that exact mechanism (e.g. `config.HashToken`); do not invent a new auth path. Adjust `newSrvWithJobs` to match the existing helper names.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags "$TAGS" ./internal/api/ -run TestJobs -v`
Expected: FAIL — `NewRouter` arg count / `listJobs` undefined.

- [ ] **Step 3: Add the jobs handlers** (`internal/api/jobs.go`)

```go
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// errJobsDisabled is returned by the jobs endpoints when no JobStore is wired
// (the -state-db store is off). It classifies to 501 Not Implemented.
var errJobsDisabled = errors.New("jobs store not enabled (set -state-db)")

type stepView struct {
	TS     string `json:"ts"`
	Step   string `json:"step"`
	Detail string `json:"detail,omitempty"`
}

type jobView struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	State    string          `json:"state"`
	Args     json.RawMessage `json:"args"`
	Steps    []stepView      `json:"steps"`
	ParentID string          `json:"parent_id,omitempty"`
	Error    string          `json:"error,omitempty"`
	Created  string          `json:"created"`
	Started  string          `json:"started,omitempty"`
	Finished string          `json:"finished,omitempty"`
}

func toJobView(j store.Job) jobView {
	v := jobView{
		ID: j.ID, Kind: j.Kind, State: string(j.State), Args: j.Args,
		Steps: []stepView{}, ParentID: j.ParentID, Error: j.Error,
		Created: j.Created.UTC().Format(time.RFC3339),
	}
	if v.Args == nil {
		v.Args = json.RawMessage("null")
	}
	for _, s := range j.Steps {
		v.Steps = append(v.Steps, stepView{
			TS: s.TS.UTC().Format(time.RFC3339), Step: s.Step, Detail: s.Detail,
		})
	}
	if !j.Started.IsZero() {
		v.Started = j.Started.UTC().Format(time.RFC3339)
	}
	if !j.Finished.IsZero() {
		v.Finished = j.Finished.UTC().Format(time.RFC3339)
	}
	return v
}

func (h *handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	f := store.JobFilter{
		State: store.JobState(r.URL.Query().Get("state")),
		Kind:  r.URL.Query().Get("kind"),
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

func (h *handlers) getJob(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	j, err := h.jobs.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, toJobView(j))
}
```

- [ ] **Step 4: Wire the router** (`internal/api/router.go`)

(a) Add import `"github.com/iotready/podman-api/internal/store"`.

(b) Change the `handlers` struct:
```go
type handlers struct {
	svc  *instance.Service
	jobs store.JobStore // nil = store disabled → /jobs endpoints return 501
}
```

(c) Change `NewRouter`'s signature and the `h` literal:
```go
func NewRouter(svc *instance.Service, jobs store.JobStore, keys *auth.KeyStore, audit func(http.Handler) http.Handler, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	h := &handlers{svc: svc, jobs: jobs}
```

(d) Add the routes (after the Volumes block, before `return mux`):
```go
	// Jobs (read). 501 when the store is disabled.
	mux.Handle("GET /jobs", guard("jobs:read", http.HandlerFunc(h.listJobs)))
	mux.Handle("GET /jobs/{id}", guard("jobs:read", http.HandlerFunc(h.getJob)))
```

- [ ] **Step 5: Classify the new errors** (`internal/api/errors.go`)

In `classify`, add cases (import `store` if not already imported there). Place the `errJobsDisabled` case and a `store.ErrNotFound` case before the `default`:
```go
	case errors.Is(err, errJobsDisabled):
		return "not_implemented", http.StatusNotImplemented, err.Error()
	case errors.Is(err, store.ErrNotFound):
		return "not_found", http.StatusNotFound, err.Error()
```
Add `"github.com/iotready/podman-api/internal/store"` to `errors.go` imports if missing.

- [ ] **Step 6: Fix the other `NewRouter` call sites**

`internal/api/router.go` callers in tests and main must pass the new `jobs` arg. Update:
- `internal/api/router_test.go` — both `NewRouter(svc, auth.NewKeyStore(keys), ...)` calls become `NewRouter(svc, nil, auth.NewKeyStore(keys), ...)`.
- `internal/api/instances_test.go` line ~45 (`newSrvFull`) — `NewRouter(svc, auth.NewKeyStore(keys), nil, nil)` becomes `NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil)`.

(`cmd/podman-api/main.go` is updated in Task 7.)

- [ ] **Step 7: Run tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/api/ -v 2>&1 | tail -25`
Expected: the new `TestJobs_*` PASS; all pre-existing api tests still PASS.

- [ ] **Step 8: Commit**

```bash
gofmt -w internal/api/
git add internal/api/jobs.go internal/api/jobs_test.go internal/api/router.go internal/api/errors.go internal/api/router_test.go internal/api/instances_test.go
git commit -m "feat(api): GET /jobs[/{id}] with jobs:read; 501 when store off (#32)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: main wiring — runner lifecycle + DB handle

**Files:**
- Modify: `cmd/podman-api/main.go`
- Modify: `cmd/podman-api/main_test.go`

All cmd tests use `-tags "$TAGS"`.

- [ ] **Step 1: Update the failing tests** (`cmd/podman-api/main_test.go`)

The `openStore` return type changes from `store.Store` to `store.DB`. Update the four `TestOpenStore_*` tests: the only change is the type the `st` variable holds (still nil-checks and a PutSpec round-trip, which `store.DB` supports via its embedded `Store`). They should compile and pass unchanged in behaviour — but to assert the job side too, extend `TestOpenStore_Enabled` to also enqueue:

```go
func TestOpenStore_Enabled(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	st, err := openStore(db, writeKey(t))
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	if st == nil {
		t.Fatal("enabled store should return a non-nil store")
	}
	if err := st.PutSpec(context.Background(), storeSpecFixture()); err != nil {
		t.Fatalf("PutSpec via returned store: %v", err)
	}
	if _, err := st.Enqueue(context.Background(), "migrate", nil, ""); err != nil {
		t.Fatalf("Enqueue via returned DB: %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./cmd/podman-api/ -run TestOpenStore_Enabled -v`
Expected: FAIL — `st.Enqueue undefined` (openStore still returns `store.Store`).

- [ ] **Step 3: Change `openStore` to return `store.DB`** (`cmd/podman-api/main.go`)

```go
func openStore(stateDB, keyFile string) (store.DB, error) {
	if stateDB == "" {
		return nil, nil
	}
	if keyFile == "" {
		return nil, fmt.Errorf("-state-db requires -spec-key-file")
	}
	key, err := store.LoadKeyFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("spec key: %w", err)
	}
	st, err := store.OpenSQLite(stateDB, store.NewKeyStore(key))
	if err != nil {
		return nil, fmt.Errorf("state db: %w", err)
	}
	return st, nil
}
```
(The doc comment above it stays; `*store.SQLite` satisfies `store.DB`.)

- [ ] **Step 4: Wire the runner + router in `main`** (`cmd/podman-api/main.go`)

Add imports: `"context"` (already imported) and `"github.com/iotready/podman-api/internal/jobs"`.

Replace the existing store-wiring block:
```go
	specStore, err := openStore(*stateDB, *specKeyFile)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	if specStore != nil {
		if closer, ok := specStore.(interface{ Close() error }); ok {
			defer closer.Close()
		}
		svc.SetStore(specStore)
		log.Printf("desired-state store enabled: %s", *stateDB)
	}
```
with:
```go
	db, err := openStore(*stateDB, *specKeyFile)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	// runnerCtx is cancelled by the shutdown handler to stop the job runner.
	runnerCtx, cancelRunner := context.WithCancel(context.Background())
	defer cancelRunner()
	var jobStore store.JobStore
	if db != nil {
		defer db.Close()
		svc.SetStore(db)
		jobStore = db
		// Registry is empty in this phase; migrate/evacuate register handlers later.
		runner := jobs.NewRunner(db, jobs.Registry{}, jobs.DefaultWorkers)
		runner.Start(runnerCtx)
		log.Printf("desired-state store enabled: %s (job runner started)", *stateDB)
	}
```

- [ ] **Step 5: Pass the JobStore to the router** (`cmd/podman-api/main.go`)

Change:
```go
	router := api.NewRouter(svc, keyStore, combined, nil)
```
to:
```go
	router := api.NewRouter(svc, jobStore, keyStore, combined, nil)
```

- [ ] **Step 6: Cancel the runner on shutdown** (`cmd/podman-api/main.go`)

In the SIGINT/SIGTERM goroutine, cancel the runner alongside the HTTP shutdown. Change:
```go
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
```
to:
```go
		<-sig
		cancelRunner() // stop the job runner; in-flight jobs are reaped on next boot
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
```

- [ ] **Step 7: Run tests + builds**

Run:
```bash
go test -tags "$TAGS" ./cmd/podman-api/ -v
go build -tags "$TAGS" ./...
```
Expected: openStore tests PASS; whole tree builds.

- [ ] **Step 8: Commit**

```bash
gofmt -w cmd/podman-api/main.go cmd/podman-api/main_test.go
git add cmd/podman-api/main.go cmd/podman-api/main_test.go
git commit -m "feat(cmd): start job runner when store enabled; wire JobStore to API (#32)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Document the endpoints (OpenAPI + README)

**Files:**
- Modify: `api/openapi.yaml`
- Modify: `README.md`

- [ ] **Step 1: Add the jobs paths + schema to `api/openapi.yaml`**

Find the `paths:` section and add two paths consistent with the existing style (reuse the existing security scheme / bearer auth declaration). Add under `paths:`:
```yaml
  /jobs:
    get:
      summary: List async jobs (migrate/evacuate). 501 when the state store is disabled.
      operationId: listJobs
      parameters:
        - in: query
          name: state
          schema: { type: string, enum: [queued, running, succeeded, failed] }
        - in: query
          name: kind
          schema: { type: string }
      responses:
        "200":
          description: jobs, newest first
          content:
            application/json:
              schema:
                type: array
                items: { $ref: "#/components/schemas/Job" }
        "501": { description: state store disabled }
  /jobs/{id}:
    get:
      summary: Get one job by id. 501 when the state store is disabled.
      operationId: getJob
      parameters:
        - in: path
          name: id
          required: true
          schema: { type: string }
      responses:
        "200":
          description: the job
          content:
            application/json:
              schema: { $ref: "#/components/schemas/Job" }
        "404": { description: not found }
        "501": { description: state store disabled }
```
And under `components: schemas:` add:
```yaml
    Job:
      type: object
      properties:
        id: { type: string }
        kind: { type: string }
        state: { type: string, enum: [queued, running, succeeded, failed] }
        args: {}
        steps:
          type: array
          items:
            type: object
            properties:
              ts: { type: string, format: date-time }
              step: { type: string }
              detail: { type: string }
        parent_id: { type: string }
        error: { type: string }
        created: { type: string, format: date-time }
        started: { type: string, format: date-time }
        finished: { type: string, format: date-time }
```
If the existing routes declare a `security:` block per-operation, mirror it on both new operations (scope `jobs:read`).

- [ ] **Step 2: Verify the spec still serves** — the spec is embedded and served at `/openapi.yaml`; just confirm it's valid YAML:

Run: `go test -tags "$TAGS" ./internal/api/ 2>&1 | tail -3` (the openapi-serving test, if any, still passes) and `python3 -c "import yaml,sys; yaml.safe_load(open('api/openapi.yaml'))" && echo OK`
Expected: `OK`, api tests green.

- [ ] **Step 3: Add a short README note**

In the Security model / state-store section (where `-state-db` is documented), add a sentence and bullet:
> When the store is enabled, async operations (future migrate/evacuate) are tracked as **jobs**. Read them at:
> - `GET /jobs?state=&kind=` and `GET /jobs/{id}` — scope `jobs:read`. Returns `501` when `-state-db` is not set.

Also add `jobs:read` to the scopes list if the README enumerates scopes.

- [ ] **Step 4: Commit**

```bash
git add api/openapi.yaml README.md
git commit -m "docs(api): document /jobs endpoints + jobs:read scope (#32)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] **Pure-Go packages:** `go test -race ./internal/store/ ./internal/jobs/` — all PASS, no races.
- [ ] **Full suite with tags:** `go test -tags "$TAGS" ./...` — all PASS.
- [ ] **gofmt + vet:** `gofmt -l .` empty; `go vet -tags "$TAGS" ./...` clean.
- [ ] **Build:** `make build` produces `bin/podman-api`.
- [ ] **Opt-in default intact:** with no `-state-db`, the runner doesn't start and `GET /jobs` returns `501`.

Then use **superpowers:finishing-a-development-branch** to open the PR against `main` (one PR for #32). On merge: check off the Phase 3 box on #29, and close #42 (the pool/busy_timeout fix shipped here).
