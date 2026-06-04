# Job-Runner Robustness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the jobs subsystem with worker headroom (so a parent evacuate can't starve plain jobs) and operator-initiated job cancellation recorded as a distinct `canceled` terminal state.

**Architecture:** A new `JobCanceled` terminal state and a `CancelQueued` store method handle queued jobs; an in-memory cancel registry in the `Runner` cancels running jobs' handler contexts; a `POST /jobs/{id}/cancel` route drives both. Worker headroom is just a higher default worker count made flag-configurable, plus a matching SQLite pool bump. A cancelled parent evacuate propagates cancellation to its in-flight children.

**Tech Stack:** Go (build tags `containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper`), `modernc.org/sqlite`, `net/http`, stdlib `testing` (store/jobs packages) + testify (api package).

**Build/test:** Always use the Makefile (it carries the required tags). `make test` runs unit tests; `make build` builds the binary. A bare `go test ./...` fails on missing CGO headers — do not use it.

**Worktree:** All work happens in `.worktrees/54-job-runner-robustness` on branch `feat/54-job-runner-robustness`. The spec lives at `docs/superpowers/specs/2026-06-04-job-runner-robustness-design.md`.

---

### Task 1: `canceled` terminal state — store

**Files:**
- Modify: `internal/store/jobs.go` (add the state const)
- Modify: `internal/store/memory.go:178-193` (`Finish` guard)
- Modify: `internal/store/sqlite.go:472-496` (`Finish` guard)
- Test: `internal/store/memory_jobs_test.go`, `internal/store/jobs_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/store/memory_jobs_test.go`:

```go
func TestMemory_Finish_AcceptsCanceled(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	j, _ := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	if _, _, err := m.ClaimNext(ctx); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := m.Finish(ctx, j.ID, JobCanceled, "canceled by operator"); err != nil {
		t.Fatalf("Finish canceled: %v", err)
	}
	got, _ := m.GetJob(ctx, j.ID)
	if got.State != JobCanceled || got.Error != "canceled by operator" || got.Finished.IsZero() {
		t.Fatalf("bad canceled job: %+v", got)
	}
}
```

Append to `internal/store/jobs_test.go`:

```go
func TestSQLite_Finish_AcceptsCanceled(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	if _, _, err := s.ClaimNext(ctx); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.Finish(ctx, j.ID, JobCanceled, "canceled by operator"); err != nil {
		t.Fatalf("Finish canceled: %v", err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.State != JobCanceled || got.Error != "canceled by operator" || got.Finished.IsZero() {
		t.Fatalf("bad canceled job: %+v", got)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test 2>&1 | grep -E "JobCanceled|Finish_AcceptsCanceled|FAIL|undefined"`
Expected: compile failure — `undefined: JobCanceled`.

- [ ] **Step 3: Add the state constant**

In `internal/store/jobs.go`, extend the state block (currently lines 15-20):

```go
const (
	JobQueued    JobState = "queued"
	JobRunning   JobState = "running"
	JobSucceeded JobState = "succeeded"
	JobFailed    JobState = "failed"
	JobCanceled  JobState = "canceled"
)
```

- [ ] **Step 4: Accept `JobCanceled` in both `Finish` guards**

`internal/store/memory.go`, change the guard in `Finish` (line 179):

```go
	if state != JobSucceeded && state != JobFailed && state != JobCanceled {
		return fmt.Errorf("store: Finish: invalid terminal state %q", state)
	}
```

`internal/store/sqlite.go`, change the guard in `Finish` (line 473):

```go
	if state != JobSucceeded && state != JobFailed && state != JobCanceled {
		return fmt.Errorf("store.Finish: invalid terminal state %q", state)
	}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test 2>&1 | grep -E "ok\s+github.com/iotready/podman-api/internal/store|FAIL"`
Expected: store package `ok`.

- [ ] **Step 6: Commit**

```bash
git add internal/store/jobs.go internal/store/memory.go internal/store/sqlite.go internal/store/memory_jobs_test.go internal/store/jobs_test.go
git commit -m "feat(store): add canceled terminal job state (#54)"
```

---

### Task 2: `CancelQueued` store method

**Files:**
- Modify: `internal/store/jobs.go` (add to `JobStore` interface, after `FailRunning`, ~line 92)
- Modify: `internal/store/memory.go` (new method)
- Modify: `internal/store/sqlite.go` (new method)
- Test: `internal/store/memory_jobs_test.go`, `internal/store/jobs_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/store/memory_jobs_test.go`:

```go
func TestMemory_CancelQueued(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()

	// queued -> canceled (true)
	q, _ := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	ok, err := m.CancelQueued(ctx, q.ID)
	if err != nil || !ok {
		t.Fatalf("cancel queued: ok=%v err=%v", ok, err)
	}
	got, _ := m.GetJob(ctx, q.ID)
	if got.State != JobCanceled || got.Finished.IsZero() {
		t.Fatalf("queued not canceled: %+v", got)
	}

	// running -> false (not queued)
	r, _ := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = m.ClaimNext(ctx)
	if ok, _ := m.CancelQueued(ctx, r.ID); ok {
		t.Fatal("running job should not CancelQueued")
	}

	// absent -> false
	if ok, _ := m.CancelQueued(ctx, "nope"); ok {
		t.Fatal("absent job should not CancelQueued")
	}
}
```

Append to `internal/store/jobs_test.go`:

```go
func TestSQLite_CancelQueued(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)

	q, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	ok, err := s.CancelQueued(ctx, q.ID)
	if err != nil || !ok {
		t.Fatalf("cancel queued: ok=%v err=%v", ok, err)
	}
	got, _ := s.GetJob(ctx, q.ID)
	if got.State != JobCanceled || got.Finished.IsZero() {
		t.Fatalf("queued not canceled: %+v", got)
	}

	r, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	if _, _, err := s.ClaimNext(ctx); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if ok, _ := s.CancelQueued(ctx, r.ID); ok {
		t.Fatal("running job should not CancelQueued")
	}

	if ok, _ := s.CancelQueued(ctx, "nope"); ok {
		t.Fatal("absent job should not CancelQueued")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test 2>&1 | grep -E "CancelQueued|undefined|FAIL"`
Expected: compile failure — `CancelQueued` undefined.

- [ ] **Step 3: Add the interface method**

In `internal/store/jobs.go`, add to the `JobStore` interface immediately after the `FailRunning` method declaration (line ~92):

```go
	// CancelQueued atomically transitions a still-queued job to canceled (setting
	// finished). Returns true if it transitioned; false if the job was not in the
	// queued state (already claimed, terminal, or absent).
	CancelQueued(ctx context.Context, id string) (bool, error)
```

- [ ] **Step 4: Implement on `*Memory`**

In `internal/store/memory.go`, add after `FailRunning` (after line 208):

```go
func (m *Memory) CancelQueued(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id && m.jobs[i].State == JobQueued {
			m.jobs[i].State = JobCanceled
			m.jobs[i].Finished = time.Now()
			return true, nil
		}
	}
	return false, nil
}
```

- [ ] **Step 5: Implement on `*SQLite`**

In `internal/store/sqlite.go`, add after `FailRunning` (after line 514):

```go
func (s *SQLite) CancelQueued(ctx context.Context, id string) (bool, error) {
	now := time.Now().UnixNano()
	var n int64
	err := s.write(ctx, func() error {
		res, e := s.db.ExecContext(ctx,
			`UPDATE jobs SET state='canceled', finished=? WHERE id=? AND state='queued'`, now, id)
		if e != nil {
			return e
		}
		n, e = res.RowsAffected()
		return e
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test 2>&1 | grep -E "ok\s+github.com/iotready/podman-api/internal/store|FAIL"`
Expected: store package `ok`.

- [ ] **Step 7: Commit**

```bash
git add internal/store/jobs.go internal/store/memory.go internal/store/sqlite.go internal/store/memory_jobs_test.go internal/store/jobs_test.go
git commit -m "feat(store): CancelQueued — atomically cancel a queued job (#54)"
```

---

### Task 3: Retention prunes canceled jobs

**Files:**
- Modify: `internal/store/memory.go:214` (terminal helper in `PruneJobs`)
- Modify: `internal/store/sqlite.go:529,542` (two `state IN (...)` clauses in `PruneJobs`)
- Test: `internal/store/jobs_prune_test.go`

- [ ] **Step 1: Write the failing test**

First read `internal/store/jobs_prune_test.go` to reuse its existing helpers (it has an `openJobStore`-style setup and helpers to age a job). Append a test that mirrors the existing prune tests but finishes the job as `JobCanceled`:

```go
func TestSQLite_PruneJobs_IncludesCanceled(t *testing.T) {
	ctx := context.Background()
	js := openJobStore(t)

	old, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	if _, _, err := js.ClaimNext(ctx); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := js.Finish(ctx, old.ID, JobCanceled, "canceled by operator"); err != nil {
		t.Fatalf("finish: %v", err)
	}

	// Prune everything finished before "now + 1h" — the canceled job qualifies.
	n, err := js.PruneJobs(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 pruned, got %d", n)
	}
	if _, err := js.GetJob(ctx, old.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canceled job not pruned: %v", err)
	}
}
```

> Note: if `jobs_prune_test.go` already imports `time`/`errors`/`json`, do not re-add them. If it lacks a local SQLite opener, use `openJobStore(t)` (defined in `jobs_test.go`, same package).

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test 2>&1 | grep -E "PruneJobs_IncludesCanceled|FAIL"`
Expected: FAIL — `want 1 pruned, got 0` (canceled not yet treated as terminal).

- [ ] **Step 3: Update the SQLite prune clauses**

In `internal/store/sqlite.go` `PruneJobs`, change both `state IN ('succeeded','failed')` clauses (lines 529 and 542) to:

```go
state IN ('succeeded','failed','canceled')
```

(Line 529 is in the child-delete query; line 542 is in the parent-delete query. Both must be updated.)

- [ ] **Step 4: Update the Memory terminal helper**

In `internal/store/memory.go` `PruneJobs`, change the `terminal` helper (line 214):

```go
	terminal := func(j Job) bool {
		return j.State == JobSucceeded || j.State == JobFailed || j.State == JobCanceled
	}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test 2>&1 | grep -E "ok\s+github.com/iotready/podman-api/internal/store|FAIL"`
Expected: store package `ok`.

- [ ] **Step 6: Commit**

```bash
git add internal/store/sqlite.go internal/store/memory.go internal/store/jobs_prune_test.go
git commit -m "feat(store): retention prunes canceled jobs (#54)"
```

---

### Task 4: Runner cancellation registry

**Files:**
- Modify: `internal/jobs/runner.go` (`Runner` struct, `NewRunner`, `run`, new `Cancel`)
- Test: `internal/jobs/runner_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/jobs/runner_test.go`:

```go
func TestRunner_CancelRunning(t *testing.T) {
	m := store.NewMemory()
	started := make(chan struct{})
	reg := Registry{"test": handlerFunc(func(ctx context.Context, job store.Job, jc *JobContext) error {
		close(started)
		<-ctx.Done() // block until cancelled
		return ctx.Err()
	})}
	r := NewRunner(m, reg, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)

	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	r.Notify()
	<-started // handler is now running and registered

	if !r.Cancel(j.ID) {
		t.Fatal("Cancel returned false for a running job")
	}
	waitFor(t, func() bool {
		got, _ := m.GetJob(context.Background(), j.ID)
		return got.State == store.JobCanceled
	})
}

func TestRunner_CancelUnknown(t *testing.T) {
	r := NewRunner(store.NewMemory(), Registry{}, 1)
	if r.Cancel("nope") {
		t.Fatal("Cancel of unknown id returned true")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test 2>&1 | grep -E "CancelRunning|CancelUnknown|undefined|FAIL"`
Expected: compile failure — `r.Cancel` undefined.

- [ ] **Step 3: Add the registry to the `Runner` struct**

In `internal/jobs/runner.go`, extend the `Runner` struct (lines 53-59) and add the entry type just below it:

```go
// Runner drains queued jobs and dispatches them to handlers.
type Runner struct {
	store    store.JobStore
	handlers Registry
	workers  int
	poke     chan struct{}
	wg       sync.WaitGroup

	mu       sync.Mutex
	inflight map[string]*inflightJob
}

// inflightJob tracks a currently-running job so an operator request can cancel
// its handler context. canceled distinguishes an operator cancel from a
// shutdown/ctx cancel: only Cancel sets it, so a job interrupted by daemon
// shutdown still records failed, not canceled.
type inflightJob struct {
	cancel   context.CancelFunc
	canceled bool
}
```

- [ ] **Step 4: Initialise the map in `NewRunner`**

In `internal/jobs/runner.go` `NewRunner` (lines 63-68), add `inflight`:

```go
	return &Runner{
		store:    js,
		handlers: h,
		workers:  workers,
		poke:     make(chan struct{}, 1),
		inflight: map[string]*inflightJob{},
	}
```

- [ ] **Step 5: Wire registration + canceled-aware finish into `run`**

Replace the body of `run` (lines 186-198) with:

```go
func (r *Runner) run(ctx context.Context, job store.Job) {
	h, ok := r.handlers[job.Kind]
	if !ok {
		r.finish(job.ID, store.JobFailed, "no handler for kind "+job.Kind)
		return
	}

	jctx, cancel := context.WithCancel(ctx)
	entry := &inflightJob{cancel: cancel}
	r.mu.Lock()
	r.inflight[job.ID] = entry
	r.mu.Unlock()

	err := h.Run(jctx, job, &JobContext{store: r.store, id: job.ID})

	r.mu.Lock()
	canceled := entry.canceled
	delete(r.inflight, job.ID)
	r.mu.Unlock()
	cancel()

	switch {
	case canceled:
		r.finish(job.ID, store.JobCanceled, "canceled by operator")
	case err != nil:
		r.finish(job.ID, store.JobFailed, err.Error())
	default:
		r.finish(job.ID, store.JobSucceeded, "")
	}
}
```

- [ ] **Step 6: Add the `Cancel` method**

In `internal/jobs/runner.go`, add after `Notify` (after line 78):

```go
// Cancel signals an in-flight job to stop. Returns true if the job was found
// running — its handler context is cancelled and it will finish as canceled;
// false if no such job is currently running (queued/terminal jobs are handled by
// the caller via the store).
func (r *Runner) Cancel(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.inflight[id]
	if !ok {
		return false
	}
	entry.canceled = true
	entry.cancel()
	return true
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `make test 2>&1 | grep -E "ok\s+github.com/iotready/podman-api/internal/jobs|FAIL"`
Expected: jobs package `ok`. (`TestRunner_FinishSurvivesCancelledRunnerCtx` must still pass — a shutdown-cancelled ctx with `canceled=false` finishes succeeded.)

- [ ] **Step 8: Run with the race detector**

Run: `make test 2>&1 | grep -E "DATA RACE|FAIL" || echo "no races"`

> If `make test` does not enable `-race`, run the package directly:
> `go test -race -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/jobs/...`
Expected: no data races (the `inflight` map is guarded by `mu`).

- [ ] **Step 9: Commit**

```bash
git add internal/jobs/runner.go internal/jobs/runner_test.go
git commit -m "feat(jobs): runner cancellation registry + Cancel (#54)"
```

---

### Task 5: `POST /jobs/{id}/cancel` route

**Files:**
- Modify: `internal/api/router.go` (`NewRouter` signature, `handlers` struct, route registration)
- Modify: `internal/api/jobs.go` (`JobCanceller` interface, `cancelJob` handler)
- Modify: all `NewRouter` callers (add the new arg) — see Step 4
- Test: `internal/api/jobs_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/api/jobs_test.go`. This adds a fake canceller and a server helper whose key carries `instances:write` (the cancel route's scope):

```go
type fakeCanceller struct{ running map[string]bool }

func (f fakeCanceller) Cancel(id string) bool { return f.running[id] }

// newSrvWithCancel wires a router with the given store + canceller and a key
// scoped instances:write (the cancel route's required scope).
func newSrvWithCancel(t *testing.T, js store.JobStore, c JobCanceller) (*httptest.Server, string) {
	t.Helper()
	tok := "t"
	hash, err := config.HashToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:write", "jobs:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, js, auth.NewKeyStore(keys), nil, c))
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestCancelJob(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled returns 501", func(t *testing.T) {
		srv, tok := newSrvWithCancel(t, nil, fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/x/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotImplemented {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("absent returns 404", func(t *testing.T) {
		srv, tok := newSrvWithCancel(t, store.NewMemory(), fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/nope/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("terminal returns 409", func(t *testing.T) {
		js := store.NewMemory()
		j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
		_, _, _ = js.ClaimNext(ctx)
		_ = js.Finish(ctx, j.ID, store.JobSucceeded, "")
		srv, tok := newSrvWithCancel(t, js, fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("queued returns 202 and becomes canceled", func(t *testing.T) {
		js := store.NewMemory()
		j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
		srv, tok := newSrvWithCancel(t, js, fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status %d", resp.StatusCode)
		}
		got, _ := js.GetJob(ctx, j.ID)
		if got.State != store.JobCanceled {
			t.Fatalf("state %q", got.State)
		}
	})

	t.Run("running returns 202 via canceller", func(t *testing.T) {
		js := store.NewMemory()
		j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
		_, _, _ = js.ClaimNext(ctx) // now running
		srv, tok := newSrvWithCancel(t, js, fakeCanceller{running: map[string]bool{j.ID: true}})
		resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("running but canceller does not have it returns 409", func(t *testing.T) {
		js := store.NewMemory()
		j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
		_, _, _ = js.ClaimNext(ctx) // running on disk, but canceller has nothing (raced to finish)
		srv, tok := newSrvWithCancel(t, js, fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test 2>&1 | grep -E "JobCanceller|newSrvWithCancel|undefined|FAIL"`
Expected: compile failure — `JobCanceller` undefined and `NewRouter` arg count mismatch.

- [ ] **Step 3: Add the `JobCanceller` interface, struct field, and handler**

In `internal/api/jobs.go`, add the interface near the top (after the `errJobsDisabled` var, line 15):

```go
// JobCanceller cancels an in-flight (running) job. Implemented by *jobs.Runner.
// Nil when the job runner is not wired (no -state-db), in which case the cancel
// endpoint is unreachable anyway (the jobs-disabled guard precedes it).
type JobCanceller interface {
	Cancel(id string) bool
}
```

Add the `cancelJob` handler at the end of `internal/api/jobs.go`:

```go
func (h *handlers) cancelJob(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	id := r.PathValue("id")
	j, err := h.jobs.GetJob(r.Context(), id)
	if err != nil {
		WriteError(w, err) // store.ErrNotFound -> 404
		return
	}

	switch j.State {
	case store.JobSucceeded, store.JobFailed, store.JobCanceled:
		WriteJSON(w, http.StatusConflict,
			ErrorBody{Code: "job_terminal", Message: "job is already in a terminal state"})
		return

	case store.JobRunning:
		if h.canceller != nil && h.canceller.Cancel(id) {
			h.writeAcceptedJob(w, r, id)
			return
		}
		// Raced to finish (or no canceller): re-check and report terminal.
		WriteJSON(w, http.StatusConflict,
			ErrorBody{Code: "job_terminal", Message: "job is no longer running"})
		return

	default: // queued
		ok, err := h.jobs.CancelQueued(r.Context(), id)
		if err != nil {
			WriteError(w, err)
			return
		}
		if ok {
			h.writeAcceptedJob(w, r, id)
			return
		}
		// Raced into running between GetJob and CancelQueued: try the canceller.
		if h.canceller != nil && h.canceller.Cancel(id) {
			h.writeAcceptedJob(w, r, id)
			return
		}
		WriteJSON(w, http.StatusConflict,
			ErrorBody{Code: "job_terminal", Message: "job could not be canceled"})
	}
}

// writeAcceptedJob re-reads the job and returns it with 202. Running cancels are
// asynchronous, so the returned state may still be "running".
func (h *handlers) writeAcceptedJob(w http.ResponseWriter, r *http.Request, id string) {
	j, err := h.jobs.GetJob(r.Context(), id)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, toJobView(j))
}
```

- [ ] **Step 4: Change `NewRouter` to take a canceller, store it, register the route**

In `internal/api/router.go`:

- Extend the signature (line 15) and `handlers` literal (line 17):

```go
func NewRouter(svc *instance.Service, jobs store.JobStore, keys *auth.KeyStore, audit func(http.Handler) http.Handler, metricsHandler http.Handler, canceller JobCanceller) http.Handler {
	mux := http.NewServeMux()
	h := &handlers{svc: svc, jobs: jobs, canceller: canceller}
```

- Add the `canceller` field to the `handlers` struct (currently `internal/api/router.go:97-99`, fields `svc`, `jobs`):

```go
type handlers struct {
	svc       *instance.Service
	jobs      store.JobStore
	canceller JobCanceller
}
```

- Register the route alongside the other `/jobs` routes (after line 83):

```go
	mux.Handle("POST /jobs/{id}/cancel", guard("instances:write", http.HandlerFunc(h.cancelJob)))
```

Now update every other `NewRouter` caller to pass the new final argument. Test callers pass `nil`:

- `internal/api/jobs_test.go:31` (`newSrvWithJobs`): `NewRouter(svc, js, auth.NewKeyStore(keys), nil, nil, nil)`
- `internal/api/templates_test.go:30`: append `, nil`
- `internal/api/router_test.go:29` and `:70`: append `, nil`
- `internal/api/migrate_test.go:46` and `:106`: append `, nil`
- `internal/api/instances_test.go:45`: append `, nil`
- `internal/api/evacuate_test.go:35` and `:126`: append `, nil`
- `internal/api/hosts_test.go:36` and `:56`: append `, nil`
- `internal/api/secrets_test.go:24`: append `, nil`
- `cmd/podman-api/e2e_integration_test.go:80`: append `, nil`
- `cmd/podman-api/main.go:144`: handled in Task 6 (passes the real runner).

> Tip to find any you missed: `grep -rn "NewRouter(" --include=*.go . | grep -v "func NewRouter"` — every line must have 6 args.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test 2>&1 | grep -E "ok\s+github.com/iotready/podman-api/internal/api|FAIL|too many arguments|not enough arguments"`
Expected: api package `ok`, no arg-count errors. (`cmd/podman-api` may still fail to compile until Task 6 updates `main.go:144` — that is expected and fixed next.)

- [ ] **Step 6: Commit**

```bash
git add internal/api/router.go internal/api/jobs.go internal/api/jobs_test.go internal/api/templates_test.go internal/api/router_test.go internal/api/migrate_test.go internal/api/instances_test.go internal/api/evacuate_test.go internal/api/hosts_test.go internal/api/secrets_test.go cmd/podman-api/e2e_integration_test.go
git commit -m "feat(api): POST /jobs/{id}/cancel route (#54)"
```

---

### Task 6: Wire the canceller + worker headroom

**Files:**
- Modify: `cmd/podman-api/main.go` (new flag, hoist canceller, pass to `NewRouter` and `NewRunner`)
- Modify: `internal/jobs/runner.go:15` (`DefaultWorkers` 4 → 8)
- Modify: `internal/store/sqlite.go:40-46` (`maxOpenConns` 8 → 12 + comment)

- [ ] **Step 1: Raise the default worker count**

In `internal/jobs/runner.go`, change line 15:

```go
// DefaultWorkers is the worker-pool size when NewRunner is given workers <= 0.
// Raised from 4 to 8 to give plain jobs headroom: a parent evacuate occupies one
// worker for its whole fan-out, so a few concurrent evacuates could otherwise
// starve migrate/other jobs. This raises the starvation threshold; a structural
// fix (separate orchestration pool) remains a future option (#54).
const DefaultWorkers = 8
```

- [ ] **Step 2: Raise the SQLite connection pool to keep read headroom**

In `internal/store/sqlite.go`, change `maxOpenConns` (lines 43-46) and its comment:

```go
// maxOpenConns bounds the SQLite connection pool. WAL allows many concurrent
// readers + one writer; setting the pool above the job worker count
// (jobs.DefaultWorkers, 8) leaves read headroom so GET /jobs is not starved when
// a worker holds the write connection. A competing writer waits up to
// busy_timeout rather than failing with "database is locked". Operators raising
// -job-workers well above the default may want a correspondingly larger pool.
const maxOpenConns = 12
```

- [ ] **Step 3: Add the `-job-workers` flag**

In `cmd/podman-api/main.go`, add to the flag block (after line 42, near `evacConc`):

```go
		jobWorkers = flag.Int("job-workers", jobs.DefaultWorkers, "size of the background job worker pool (<=0 uses the built-in default)")
```

> `jobs` is already imported in `main.go` (it references `jobs.Registry` / `jobs.NewRunner`).

- [ ] **Step 4: Hoist the canceller and use the flag**

In `cmd/podman-api/main.go`, declare a canceller var before the `if db != nil` block. Change lines 99-115. Add `var canceller api.JobCanceller` next to `var jobStore store.JobStore`, set it inside the block, and pass `*jobWorkers` to `NewRunner`:

```go
	var jobStore store.JobStore
	var canceller api.JobCanceller
	if db != nil {
		defer db.Close()
		svc.SetStore(db)
		jobStore = db
		registry := jobs.Registry{
			"migrate":  &migrate.Handler{Svc: svc},
			"evacuate": &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: *evacConc},
		}
		runner := jobs.NewRunner(db, registry, *jobWorkers)
		canceller = runner
		runner.Start(runnerCtx)
		if *jobsRetention > 0 {
			runner.StartRetention(runnerCtx, *jobsRetention)
			log.Printf("jobs retention enabled: pruning terminal jobs older than %s", *jobsRetention)
		}
		log.Printf("desired-state store enabled: %s (job runner started, %d workers)", *stateDB, *jobWorkers)
	}
```

- [ ] **Step 5: Pass the canceller to `NewRouter`**

In `cmd/podman-api/main.go`, change line 144:

```go
	router := api.NewRouter(svc, jobStore, keyStore, combined, nil, canceller)
```

> `api` is already imported. When `db == nil`, `canceller` is nil and `jobStore` is nil, so the cancel route returns 501 — consistent.

- [ ] **Step 6: Build and run the full suite**

Run: `make build && make test 2>&1 | tail -20`
Expected: build succeeds; all packages `ok`, including `cmd/podman-api`.

- [ ] **Step 7: Commit**

```bash
git add cmd/podman-api/main.go internal/jobs/runner.go internal/store/sqlite.go
git commit -m "feat(jobs): -job-workers flag + raised default headroom; wire canceller (#54)"
```

---

### Task 7: Propagate cancellation to evacuate children

**Files:**
- Modify: `internal/evacuate/handler.go:134-150` (`runChild`)
- Test: `internal/evacuate/handler_test.go`

- [ ] **Step 1: Write the failing test**

This uses the package's existing `buildSvc` / `mustArgs` helpers (in `handler_test.go`) — the same pattern as `TestEvacuateAllSucceed`. The injected migrate returns `context.Canceled` directly (so `errors.Is` matches with no new import). Append:

```go
func TestEvacuate_CancelMarksChildrenCanceled(t *testing.T) {
	ctx := context.Background()
	svc, mem := buildSvc(t, "acme")
	parent, _ := mem.Enqueue(ctx, "evacuate", mustArgs(t, "hostA",
		map[string]string{"acme": "hostB"}), "")

	h := &Handler{Svc: svc, Jobs: mem}
	h.migrate = func(_ context.Context, req instance.MigrateRequest, step func(string, string)) error {
		return context.Canceled // parent canceled -> child rolled back on shared ctx
	}

	// Run returns an error (the move "failed" from the aggregate's view); we only
	// assert the child's recorded state here.
	_ = h.Run(ctx, parent, jobs.NewJobContext(mem, parent.ID))

	children, _ := mem.ListJobs(ctx, store.JobFilter{ParentID: parent.ID})
	if len(children) != 1 {
		t.Fatalf("want 1 child, got %d", len(children))
	}
	if children[0].State != store.JobCanceled {
		t.Fatalf("child state = %q, want canceled", children[0].State)
	}
}
```

> `context`, `instance`, `jobs`, `store`, `errors` are already imported in `handler_test.go`; this test adds no new imports.

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test 2>&1 | grep -E "CancelMarksChildrenCanceled|FAIL"`
Expected: FAIL — child state is `failed`, not `canceled`.

- [ ] **Step 3: Branch on `context.Canceled` in `runChild`**

In `internal/evacuate/handler.go`, update the terminal-state selection in `runChild` (lines 136-139). Add the `errors` import if not present, then:

```go
	state, errMsg := store.JobSucceeded, ""
	if migErr != nil {
		state, errMsg = store.JobFailed, migErr.Error()
		if errors.Is(migErr, context.Canceled) {
			// Parent was canceled: the child rolled back (source intact) on the
			// shared context. Record it as canceled, not failed.
			state = store.JobCanceled
		}
	}
```

> `context` is already imported. Add `"errors"` to the import block if absent.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test 2>&1 | grep -E "ok\s+github.com/iotready/podman-api/internal/evacuate|FAIL"`
Expected: evacuate package `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/evacuate/handler.go internal/evacuate/handler_test.go
git commit -m "feat(evacuate): record canceled children when parent is canceled (#54)"
```

---

### Task 8: OpenAPI + README

**Files:**
- Modify: `api/openapi.yaml` (job-state enum ×2, new `/jobs/{id}/cancel` path)
- Modify: `internal/api/openapi_test.go` (add cancel path to the spot-check list)
- Modify: `README.md` (`-job-workers` bullet)

- [ ] **Step 1: Add the cancel path to the OpenAPI spot-check (failing test first)**

In `internal/api/openapi_test.go`, add `"/jobs/{id}/cancel"` to the `want` slice (the list of required paths, around line 47-58):

```go
		"/migrate",
		"/jobs/{id}/cancel",
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test 2>&1 | grep -E "spec missing path|FAIL"`
Expected: FAIL — `spec missing path "/jobs/{id}/cancel"`.

- [ ] **Step 3: Add `canceled` to both job-state enums**

In `api/openapi.yaml`:
- Line 346 (the `Job` schema `state` enum): `enum: [queued, running, succeeded, failed, canceled]`
- Line 836 (the `GET /jobs` `state` query-param enum): `enum: [queued, running, succeeded, failed, canceled]`

- [ ] **Step 4: Add the cancel path**

In `api/openapi.yaml`, after the `/jobs/{id}` block (after line 891), add:

```yaml

  /jobs/{id}/cancel:
    post:
      tags: [jobs]
      summary: cancel a queued or running job (requires -state-db)
      description: >
        Cancels a queued job immediately (it transitions to `canceled`) or
        signals a running job to stop. Cancellation of a running job is
        asynchronous — the handler unwinds (a migrate rolls back, leaving the
        source intact) and the terminal `canceled` state is recorded shortly
        after, so the 202 body may still show `running`. A parent evacuate's
        in-flight children are canceled with it.
      parameters:
        - name: id
          in: path
          required: true
          schema: {type: string}
      responses:
        '202':
          description: cancellation accepted (queued -> canceled, or running -> stopping)
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Job'}
        '404': {$ref: '#/components/responses/NotFound'}
        '409': {$ref: '#/components/responses/Conflict'}
        '501':
          description: state store not enabled (-state-db not set)
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Error'}
      x-required-scope: instances:write
```

- [ ] **Step 5: Run to verify the spec test passes**

Run: `make test 2>&1 | grep -E "ok\s+github.com/iotready/podman-api/internal/api|FAIL"`
Expected: api package `ok`.

- [ ] **Step 6: Add the README flag bullet**

In `README.md`, after line 205 (`-evacuate-concurrency`), add:

```markdown
- **`-job-workers <n>`** — size of the background job worker pool (default `8`). A parent evacuate occupies one worker for its entire fan-out, so this is the headroom that keeps a few concurrent evacuates from starving plain migrate/other jobs; raise it if you run many concurrent evacuates. Values `<=0` fall back to the built-in default.
```

Also add a cancellation note after the jobs-watching section if a natural spot exists (optional; the wiki carries the operator-facing detail).

- [ ] **Step 7: Final gofmt + vet + full suite**

Run:
```bash
gofmt -l . && go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./... ; make test 2>&1 | tail -15
```
Expected: `gofmt -l .` prints nothing; `go vet` clean; all packages `ok`.

- [ ] **Step 8: Commit**

```bash
git add api/openapi.yaml internal/api/openapi_test.go README.md
git commit -m "docs(api): document POST /jobs/{id}/cancel + canceled state, -job-workers (#54)"
```

---

## Post-implementation

- Wiki (`Operating.md` job cancellation + headroom note; `Deploying.md` + Run-example `-job-workers`) is a separate repo (`/tmp/pa-wiki`, no PR flow) — update it during post-merge housekeeping, as in the prior batch.
- The structural two-pool orchestration fix stays recorded in #54 as a future option; bind-mount support is now #73.

## Notes for the implementer

- **Build/test only via the Makefile** (it carries the required tags). A bare `go test ./...` / `go build ./...` fails on missing CGO headers.
- **TDD discipline:** write the test, watch it fail for the stated reason, implement minimally, watch it pass, commit. One commit per task.
- **Store tests use stdlib `testing`** (`t.Fatalf`); the **api package uses testify**. Match the file you are editing.
- The `canceled` flag in the runner is set **only** by `Cancel`, never by ctx cancellation — that is what keeps a shutdown-interrupted job recording `failed` (and the existing `TestRunner_FinishSurvivesCancelledRunnerCtx` green).
