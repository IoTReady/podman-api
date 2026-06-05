# Boot-time Reconciliation of Interrupted Migrates — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On daemon restart, automatically drive each interrupted `migrate` job to a consistent end-state (roll forward if the destination is already healthy, else roll back), instead of blindly marking it `failed`.

**Architecture:** A `Reconciler` registry parallel to the existing `jobs.Handler` registry keeps the runner kind-agnostic. At boot the runner moves interrupted `running` jobs of reconcilable kinds to a new non-terminal `reconciling` state and fails the rest; a background loop then drives each `reconciling` job through its registered reconciler, retrying until hosts are reachable. The migrate reconcile algorithm lives in `instance.Service` (reusing `waitRunning`/`Start`/`Delete`/`migrateLock`); a thin `migrate.Reconciler` adapter wires it to the runner.

**Tech Stack:** Go, SQLite (`modernc.org/sqlite`), the project's remote-client build tags.

**Build/test note:** Always use the tags. Single-package run:
```sh
TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"
go test -tags "$TAGS" ./internal/store/ -run TestX -v
```
Keep `gofmt -l .` empty and `go vet` clean.

**Design reference:** `docs/superpowers/specs/2026-06-05-migrate-auto-reconcile-design.md`

---

## File Structure

- `internal/store/jobs.go` — add `JobReconciling` constant; add `MarkReconciling`, `ResolveReconciling`, `CancelReconciling` to the `JobStore` interface (Task 1, 2).
- `internal/store/sqlite.go` — SQLite implementations of the three new methods (Task 1, 2).
- `internal/store/memory.go` — Memory implementations of the three new methods (Task 1, 2).
- `internal/store/reconcile_test.go` — **new**, table tests over sqlite+memory for the three methods (Task 1, 2).
- `internal/jobs/runner.go` — `Reconciler` interface, `Reconcilers` type, `SetReconcilers`, `reconcilableKinds`, boot transition, reconcile loop (Task 3).
- `internal/jobs/reconcile_test.go` — **new**, runner boot + loop tests with a fake reconciler (Task 3).
- `internal/instance/reconcile.go` — **new**, `Service.ReconcileMigrate` algorithm + `destState`/`sourcePresent` helpers (Task 4).
- `internal/instance/reconcile_test.go` — **new**, the five-row decision matrix against the fake client (Task 4).
- `internal/migrate/reconciler.go` — **new**, `migrate.Reconciler` adapter implementing `jobs.Reconciler` (Task 5).
- `internal/migrate/reconciler_test.go` — **new**, adapter mapping test (Task 5).
- `cmd/podman-api/main.go` — build the reconcilers map and call `runner.SetReconcilers` (Task 5).
- `internal/api/jobs.go` — cancel branch for a `reconciling` job (Task 6).
- `internal/api/jobs_test.go` — cancel-of-reconciling test (Task 6).
- `internal/api/openapi.yaml` — add `reconciling` to the job-state enum + doc (Task 6).

---

## Task 1: Store — `reconciling` state + `MarkReconciling`

**Files:**
- Modify: `internal/store/jobs.go` (add constant + interface method)
- Modify: `internal/store/sqlite.go` (implement)
- Modify: `internal/store/memory.go` (implement)
- Test: `internal/store/reconcile_test.go` (create)

- [ ] **Step 1: Write the failing test**

Create `internal/store/reconcile_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"testing"
)

// jobStores returns the two JobStore implementations under test, each seeded
// independently. The factory lets each subtest get a fresh store.
func jobStores(t *testing.T) map[string]func() JobStore {
	t.Helper()
	return map[string]func() JobStore{
		"memory": func() JobStore { return NewMemory() },
		"sqlite": func() JobStore { return openTestStore(t, NewKeyStore(testKey(0x11))) },
	}
}

func TestMarkReconciling(t *testing.T) {
	for name, mk := range jobStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			js := mk()

			// A running migrate, a running evacuate, and a queued migrate.
			mig, err := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
			if err != nil {
				t.Fatal(err)
			}
			evac, err := js.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
			if err != nil {
				t.Fatal(err)
			}
			queued, err := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
			if err != nil {
				t.Fatal(err)
			}
			// Claim the first two so they are running; leave `queued` queued.
			if _, _, err := js.ClaimNext(ctx); err != nil { // claims mig (oldest)
				t.Fatal(err)
			}
			if _, _, err := js.ClaimNext(ctx); err != nil { // claims evac
				t.Fatal(err)
			}

			n, err := js.MarkReconciling(ctx, []string{"migrate"})
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Fatalf("MarkReconciling moved %d jobs, want 1", n)
			}

			assertState(t, js, mig.ID, JobReconciling)  // running migrate -> reconciling
			assertState(t, js, evac.ID, JobRunning)     // running evacuate untouched
			assertState(t, js, queued.ID, JobQueued)    // queued migrate untouched
		})
	}
}

func TestMarkReconciling_EmptyKinds(t *testing.T) {
	for name, mk := range jobStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			js := mk()
			if _, err := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), ""); err != nil {
				t.Fatal(err)
			}
			if _, _, err := js.ClaimNext(ctx); err != nil {
				t.Fatal(err)
			}
			n, err := js.MarkReconciling(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("MarkReconciling(nil) moved %d, want 0", n)
			}
		})
	}
}

func assertState(t *testing.T, js JobStore, id string, want JobState) {
	t.Helper()
	j, err := js.GetJob(context.Background(), id)
	if err != nil {
		t.Fatalf("GetJob %s: %v", id, err)
	}
	if j.State != want {
		t.Fatalf("job %s state = %q, want %q", id, j.State, want)
	}
}
```

Note: `openTestStore(t, ks)`, `NewKeyStore`, `testKey`, and `NewMemory()` already exist in this package (see `internal/store/sqlite_test.go` and `internal/store/jobs_test.go`, which uses `openTestStore(t, NewKeyStore(testKey(0x11)))`).

- [ ] **Step 2: Run test to verify it fails (compile error: undefined JobReconciling / MarkReconciling)**

Run: `go test -tags "$TAGS" ./internal/store/ -run TestMarkReconciling -v`
Expected: FAIL — `undefined: JobReconciling`, `js.MarkReconciling undefined`.

- [ ] **Step 3: Add the constant + interface method**

In `internal/store/jobs.go`, add to the state block:

```go
const (
	JobQueued      JobState = "queued"
	JobRunning     JobState = "running"
	JobReconciling JobState = "reconciling"
	JobSucceeded   JobState = "succeeded"
	JobFailed      JobState = "failed"
	JobCanceled    JobState = "canceled"
)
```

In the `JobStore` interface, after `FailRunning`, add:

```go
	// MarkReconciling moves every running job whose kind is in kinds to the
	// reconciling state (non-terminal); returns the count moved. Called once at
	// startup, before FailRunning, so reconcilable kinds are recovered rather than
	// failed. An empty kinds slice is a no-op returning 0.
	MarkReconciling(ctx context.Context, kinds []string) (int, error)
```

- [ ] **Step 4: Implement in SQLite**

In `internal/store/sqlite.go`, after `FailRunning`:

```go
func (s *SQLite) MarkReconciling(ctx context.Context, kinds []string) (int, error) {
	if len(kinds) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(kinds))
	args := make([]any, len(kinds))
	for i, k := range kinds {
		placeholders[i] = "?"
		args[i] = k
	}
	query := `UPDATE jobs SET state='reconciling' WHERE state='running' AND kind IN (` +
		strings.Join(placeholders, ",") + `)`
	var n int64
	err := s.write(ctx, func() error {
		res, e := s.db.ExecContext(ctx, query, args...)
		if e != nil {
			return e
		}
		n, e = res.RowsAffected()
		return e
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}
```

Ensure `strings` is imported in `sqlite.go` (it is used elsewhere; if not, add it).

- [ ] **Step 5: Implement in Memory**

In `internal/store/memory.go`, after `FailRunning`:

```go
func (m *Memory) MarkReconciling(_ context.Context, kinds []string) (int, error) {
	if len(kinds) == 0 {
		return 0, nil
	}
	want := make(map[string]bool, len(kinds))
	for _, k := range kinds {
		want[k] = true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for i := range m.jobs {
		if m.jobs[i].State == JobRunning && want[m.jobs[i].Kind] {
			m.jobs[i].State = JobReconciling
			n++
		}
	}
	return n, nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/store/ -run TestMarkReconciling -v`
Expected: PASS (both `memory` and `sqlite` subtests).

- [ ] **Step 7: Commit**

```bash
git add internal/store/jobs.go internal/store/sqlite.go internal/store/memory.go internal/store/reconcile_test.go
git commit -m "feat(store): reconciling state + MarkReconciling (#54)"
```

---

## Task 2: Store — `ResolveReconciling` + `CancelReconciling` (CAS)

**Files:**
- Modify: `internal/store/jobs.go` (interface)
- Modify: `internal/store/sqlite.go` (implement)
- Modify: `internal/store/memory.go` (implement)
- Test: `internal/store/reconcile_test.go` (append)

- [ ] **Step 1: Write the failing test**

Append to `internal/store/reconcile_test.go`:

```go
func TestResolveReconciling(t *testing.T) {
	for name, mk := range jobStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			js := mk()
			j, err := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := js.ClaimNext(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := js.MarkReconciling(ctx, []string{"migrate"}); err != nil {
				t.Fatal(err)
			}

			ok, err := js.ResolveReconciling(ctx, j.ID, JobSucceeded, "")
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("ResolveReconciling returned false, want true")
			}
			assertState(t, js, j.ID, JobSucceeded)

			// CAS: a second resolve no-ops (no longer reconciling).
			ok, err = js.ResolveReconciling(ctx, j.ID, JobFailed, "late")
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("second ResolveReconciling returned true, want false")
			}
			assertState(t, js, j.ID, JobSucceeded) // unchanged
		})
	}
}

func TestResolveReconciling_RejectsNonTerminal(t *testing.T) {
	js := NewMemory()
	if _, err := js.ResolveReconciling(context.Background(), "x", JobRunning, ""); err == nil {
		t.Fatal("ResolveReconciling(running) returned nil error, want rejection")
	}
}

func TestCancelReconciling(t *testing.T) {
	for name, mk := range jobStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			js := mk()
			j, err := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := js.ClaimNext(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := js.MarkReconciling(ctx, []string{"migrate"}); err != nil {
				t.Fatal(err)
			}

			ok, err := js.CancelReconciling(ctx, j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("CancelReconciling returned false, want true")
			}
			assertState(t, js, j.ID, JobCanceled)

			// CAS: resolving after cancel no-ops (cancel wins).
			ok, err = js.ResolveReconciling(ctx, j.ID, JobFailed, "loop")
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("ResolveReconciling after cancel returned true, want false")
			}
			assertState(t, js, j.ID, JobCanceled)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TAGS" ./internal/store/ -run 'TestResolveReconciling|TestCancelReconciling' -v`
Expected: FAIL — `js.ResolveReconciling undefined`, `js.CancelReconciling undefined`.

- [ ] **Step 3: Add interface methods**

In `internal/store/jobs.go`, after `MarkReconciling`:

```go
	// ResolveReconciling transitions a reconciling job to a terminal state
	// (succeeded or failed), setting finished + error. Compare-and-swap: it
	// affects only a row currently in reconciling, so it no-ops (returns false) if
	// an operator cancel already moved it. Passing any non-terminal state is a
	// programming error.
	ResolveReconciling(ctx context.Context, id string, state JobState, errMsg string) (bool, error)
	// CancelReconciling transitions a reconciling job to canceled, setting
	// finished. Compare-and-swap: affects only a row currently in reconciling,
	// returning false otherwise. Used by the cancel endpoint as the escape hatch.
	CancelReconciling(ctx context.Context, id string) (bool, error)
```

- [ ] **Step 4: Implement in SQLite**

In `internal/store/sqlite.go`, after `MarkReconciling`:

```go
func (s *SQLite) ResolveReconciling(ctx context.Context, id string, state JobState, errMsg string) (bool, error) {
	if state != JobSucceeded && state != JobFailed {
		return false, fmt.Errorf("store.ResolveReconciling: invalid terminal state %q", state)
	}
	now := time.Now().UnixNano()
	var e any
	if errMsg != "" {
		e = errMsg
	}
	var n int64
	err := s.write(ctx, func() error {
		res, err := s.db.ExecContext(ctx,
			`UPDATE jobs SET state=?, error=?, finished=? WHERE id=? AND state='reconciling'`,
			string(state), e, now, id)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *SQLite) CancelReconciling(ctx context.Context, id string) (bool, error) {
	now := time.Now().UnixNano()
	var n int64
	err := s.write(ctx, func() error {
		res, err := s.db.ExecContext(ctx,
			`UPDATE jobs SET state='canceled', finished=? WHERE id=? AND state='reconciling'`, now, id)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
```

- [ ] **Step 5: Implement in Memory**

In `internal/store/memory.go`, after `MarkReconciling`:

```go
func (m *Memory) ResolveReconciling(_ context.Context, id string, state JobState, errMsg string) (bool, error) {
	if state != JobSucceeded && state != JobFailed {
		return false, fmt.Errorf("store: ResolveReconciling: invalid terminal state %q", state)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id && m.jobs[i].State == JobReconciling {
			m.jobs[i].State = state
			m.jobs[i].Error = errMsg
			m.jobs[i].Finished = time.Now()
			return true, nil
		}
	}
	return false, nil
}

func (m *Memory) CancelReconciling(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id && m.jobs[i].State == JobReconciling {
			m.jobs[i].State = JobCanceled
			m.jobs[i].Finished = time.Now()
			return true, nil
		}
	}
	return false, nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/store/ -run 'TestResolveReconciling|TestCancelReconciling' -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/store/jobs.go internal/store/sqlite.go internal/store/memory.go internal/store/reconcile_test.go
git commit -m "feat(store): CAS ResolveReconciling + CancelReconciling (#54)"
```

---

## Task 3: Runner — Reconciler registry, boot transition, reconcile loop

**Files:**
- Modify: `internal/jobs/runner.go`
- Test: `internal/jobs/reconcile_test.go` (create)

- [ ] **Step 1: Write the failing test**

Create `internal/jobs/reconcile_test.go`:

```go
package jobs

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// fakeReconciler records calls and returns a scripted outcome per job id.
type fakeReconciler struct {
	mu       sync.Mutex
	outcomes map[string]fakeOutcome // by job id
	calls    map[string]int
}

type fakeOutcome struct {
	state    store.JobState
	resolved bool
	err      error
}

func (f *fakeReconciler) Reconcile(_ context.Context, job store.Job, _ *JobContext) (store.JobState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[job.ID]++
	o := f.outcomes[job.ID]
	return o.state, o.resolved, o.err
}

func (f *fakeReconciler) callCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

func TestStart_BootTransition(t *testing.T) {
	ctx := context.Background()
	js := store.NewMemory()
	mig, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	evac, _ := js.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	js.ClaimNext(ctx) // mig -> running
	js.ClaimNext(ctx) // evac -> running

	r := NewRunner(js, Registry{}, 2)
	r.SetReconcilers(Reconcilers{"migrate": &fakeReconciler{
		outcomes: map[string]fakeOutcome{mig.ID: {store.JobSucceeded, true, nil}},
		calls:    map[string]int{},
	}})
	runCtx, cancel := context.WithCancel(ctx)
	r.Start(runCtx)
	t.Cleanup(func() { cancel(); r.Wait() })

	// evacuate (no reconciler) was failed at boot immediately.
	if j, _ := js.GetJob(ctx, evac.ID); j.State != store.JobFailed {
		t.Fatalf("evacuate state = %q, want failed", j.State)
	}
	// migrate was moved to reconciling then resolved by the loop's first sweep.
	waitForState(t, js, mig.ID, store.JobSucceeded)
}

func TestReconcileLoop_RetriesInconclusive(t *testing.T) {
	ctx := context.Background()
	js := store.NewMemory()
	mig, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	js.ClaimNext(ctx)

	fr := &fakeReconciler{
		outcomes: map[string]fakeOutcome{mig.ID: {"", false, nil}}, // inconclusive forever
		calls:    map[string]int{},
	}
	r := NewRunner(js, Registry{}, 2)
	r.SetReconcilers(Reconcilers{"migrate": fr})
	// Shrink the loop interval for the test.
	r.reconcileInterval = 20 * time.Millisecond
	runCtx, cancel := context.WithCancel(ctx)
	r.Start(runCtx)
	t.Cleanup(func() { cancel(); r.Wait() })

	// Stays reconciling and is retried more than once.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fr.callCount(mig.ID) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fr.callCount(mig.ID) < 2 {
		t.Fatalf("reconciler called %d times, want >= 2 (retry)", fr.callCount(mig.ID))
	}
	if j, _ := js.GetJob(ctx, mig.ID); j.State != store.JobReconciling {
		t.Fatalf("state = %q, want reconciling", j.State)
	}
}

func TestReconcileLoop_CancelWins(t *testing.T) {
	ctx := context.Background()
	js := store.NewMemory()
	mig, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	js.ClaimNext(ctx)
	js.MarkReconciling(ctx, []string{"migrate"})

	// Operator cancels before the loop resolves.
	ok, _ := js.CancelReconciling(ctx, mig.ID)
	if !ok {
		t.Fatal("CancelReconciling failed to set up the test")
	}
	fr := &fakeReconciler{
		outcomes: map[string]fakeOutcome{mig.ID: {store.JobFailed, true, nil}},
		calls:    map[string]int{},
	}
	r := NewRunner(js, Registry{}, 2)
	r.SetReconcilers(Reconcilers{"migrate": fr})
	r.reconcileInterval = 20 * time.Millisecond
	runCtx, cancel := context.WithCancel(ctx)
	r.Start(runCtx)
	t.Cleanup(func() { cancel(); r.Wait() })

	time.Sleep(150 * time.Millisecond) // let a few sweeps run
	if j, _ := js.GetJob(ctx, mig.ID); j.State != store.JobCanceled {
		t.Fatalf("state = %q, want canceled (cancel must win the CAS)", j.State)
	}
}

func waitForState(t *testing.T, js store.JobStore, id string, want store.JobState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if j, _ := js.GetJob(context.Background(), id); j.State == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	j, _ := js.GetJob(context.Background(), id)
	t.Fatalf("job %s state = %q, want %q (timeout)", id, j.State, want)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TAGS" ./internal/jobs/ -run 'TestStart_BootTransition|TestReconcileLoop' -v`
Expected: FAIL — `undefined: Reconcilers`, `r.SetReconcilers undefined`, `r.reconcileInterval undefined`.

- [ ] **Step 3: Add the Reconciler interface + Reconcilers type**

In `internal/jobs/runner.go`, after the `Handler`/`Registry` declarations:

```go
// Reconciler drives a job that was interrupted by a daemon restart toward a
// consistent state. It is registered per kind, parallel to Handler. resolved
// reports whether a terminal state was reached: resolved=false means the attempt
// was inconclusive (e.g. a host was unreachable) and the job should stay
// reconciling and be retried on the next sweep. A non-nil err is logged and
// treated as inconclusive.
type Reconciler interface {
	Reconcile(ctx context.Context, job store.Job, jc *JobContext) (state store.JobState, resolved bool, err error)
}

// Reconcilers maps a job kind to its reconciler.
type Reconcilers map[string]Reconciler
```

- [ ] **Step 4: Add fields + SetReconcilers + default interval**

In the `Runner` struct, add fields:

```go
type Runner struct {
	store             store.JobStore
	handlers          Registry
	reconcilers       Reconcilers
	reconcileInterval time.Duration
	workers           int
	poke              chan struct{}
	wg                sync.WaitGroup
	mu                sync.Mutex
	inflight          map[string]*inflightJob
}
```

Add the default interval constant near `pollInterval`:

```go
// reconcileInterval is how often the reconcile loop re-sweeps reconciling jobs,
// retrying those whose hosts were unreachable on the previous pass.
const defaultReconcileInterval = 30 * time.Second
```

In `NewRunner`, initialise the interval (leave reconcilers nil until set):

```go
	return &Runner{
		store:             js,
		handlers:          h,
		reconcileInterval: defaultReconcileInterval,
		workers:           workers,
		poke:              make(chan struct{}, 1),
		inflight:          map[string]*inflightJob{},
	}
```

Add the setter and a helper after `NewRunner`:

```go
// SetReconcilers registers the per-kind reconcilers used for boot recovery. Call
// before Start; the map must not be modified afterwards. A kind with a registered
// reconciler is moved to reconciling (and later resolved) on restart instead of
// being failed.
func (r *Runner) SetReconcilers(rec Reconcilers) { r.reconcilers = rec }

// reconcilableKinds returns the kinds that have a registered reconciler.
func (r *Runner) reconcilableKinds() []string {
	if len(r.reconcilers) == 0 {
		return nil
	}
	kinds := make([]string, 0, len(r.reconcilers))
	for k := range r.reconcilers {
		kinds = append(kinds, k)
	}
	return kinds
}
```

- [ ] **Step 5: Rewrite the boot transition in Start + launch the loop**

Replace the body of `Start` (the `FailRunning` block) with:

```go
func (r *Runner) Start(ctx context.Context) {
	if n, err := r.store.MarkReconciling(ctx, r.reconcilableKinds()); err != nil {
		log.Printf("jobs: boot reconcile mark failed: %v", err)
	} else if n > 0 {
		log.Printf("jobs: marked %d interrupted job(s) reconciling on startup", n)
	}
	if n, err := r.store.FailRunning(ctx, "interrupted by daemon restart"); err != nil {
		log.Printf("jobs: boot recovery failed: %v", err)
	} else if n > 0 {
		log.Printf("jobs: marked %d interrupted job(s) failed on startup", n)
	}
	if len(r.reconcilers) > 0 {
		r.wg.Add(1)
		go r.reconcileLoop(ctx)
	}
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go r.worker(ctx)
	}
}
```

- [ ] **Step 6: Implement the reconcile loop**

Add to `internal/jobs/runner.go` (near `worker`):

```go
// reconcileLoop periodically drives reconciling jobs to a terminal state via
// their registered reconciler, retrying any left inconclusive. It runs until ctx
// is cancelled. An immediate first sweep cleans up promptly after a restart.
func (r *Runner) reconcileLoop(ctx context.Context) {
	defer r.wg.Done()
	r.reconcileSweep(ctx)
	t := time.NewTicker(r.reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.reconcileSweep(ctx)
		}
	}
}

// reconcileSweep processes every reconciling job once, with concurrency bounded
// by the worker count so one unreachable host cannot block the others.
func (r *Runner) reconcileSweep(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	jobsToDo, err := r.store.ListJobs(ctx, store.JobFilter{State: store.JobReconciling, Limit: store.MaxJobLimit})
	if err != nil {
		log.Printf("jobs: reconcile list failed: %v", err)
		return
	}
	sem := make(chan struct{}, r.workers)
	var wg sync.WaitGroup
	for _, job := range jobsToDo {
		rec, ok := r.reconcilers[job.Kind]
		if !ok {
			log.Printf("jobs: no reconciler for reconciling job %s (kind %s)", job.ID, job.Kind)
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(job store.Job, rec Reconciler) {
			defer wg.Done()
			defer func() { <-sem }()
			r.reconcileOne(ctx, job, rec)
		}(job, rec)
	}
	wg.Wait()
}

// reconcileOne runs a single job's reconciler and records the outcome via CAS.
func (r *Runner) reconcileOne(ctx context.Context, job store.Job, rec Reconciler) {
	state, resolved, err := rec.Reconcile(ctx, job, &JobContext{store: r.store, id: job.ID})
	if err != nil {
		log.Printf("jobs: reconcile %s errored (will retry): %v", job.ID, err)
		return
	}
	if !resolved {
		return // inconclusive — retried next sweep
	}
	if _, err := r.store.ResolveReconciling(ctx, job.ID, state, reconcileErrMsg(state)); err != nil {
		log.Printf("jobs: reconcile resolve %s failed: %v", job.ID, err)
	}
}

func reconcileErrMsg(state store.JobState) string {
	if state == store.JobFailed {
		return "reconciled after daemon restart: rolled back"
	}
	return ""
}
```

Note: the reconciler emits the precise per-branch step (`reconcile-roll-forward`, etc.) and detailed message via `jc.Step`; `reconcileErrMsg` is only the short top-level `error` text for a failed job.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/jobs/ -run 'TestStart_BootTransition|TestReconcileLoop' -race -v`
Expected: PASS (all three). Then run the whole package to confirm no regression:
Run: `go test -tags "$TAGS" ./internal/jobs/ -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/jobs/runner.go internal/jobs/reconcile_test.go
git commit -m "feat(jobs): reconciler registry, boot transition, reconcile loop (#54)"
```

---

## Task 4: instance — `ReconcileMigrate` algorithm

**Files:**
- Create: `internal/instance/reconcile.go`
- Test: `internal/instance/reconcile_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/instance/reconcile_test.go`. The test builds a `Service` over the fake podman client and drives all five matrix rows. Use `SetVerifyTimeout` so the unhealthy path returns fast.

```go
package instance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// reconcileSvc builds a Service with two hosts (h1 source, h2 dest) and template
// "web", backed by a fake client and a memory store. The verify timeout is set to
// a sub-tick value so the present-but-unhealthy path returns on waitRunning's first
// iteration instead of blocking on its 2s poll ticker; the healthy path returns
// immediately (podReady) and is unaffected.
func reconcileSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	SetVerifyTimeout(time.Nanosecond)
	t.Cleanup(func() { SetVerifyTimeout(60 * time.Second) })
	fc := fake.New()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/a"},
		{ID: "h2", Addr: "unix", Socket: "/b"},
	}
	tmpls := []config.Template{{Meta: render.Meta{ID: "web"}}}
	svc := NewService(fc, hosts, tmpls)
	svc.SetStore(store.NewMemory())
	return svc, fc
}

// healthyPod returns a Running pod with one Running container and no healthcheck.
func healthyPod(name string) podman.Pod {
	return podman.Pod{Name: name, Status: "Running", Containers: []podman.Container{{Status: "Running"}}}
}

func unhealthyPod(name string) podman.Pod {
	return podman.Pod{Name: name, Status: "Degraded", Containers: []podman.Container{{Status: "Exited"}}}
}

func req() MigrateRequest {
	return MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "web", Slug: "x"}
}

func TestReconcileMigrate_RollForward(t *testing.T) {
	svc, fc := reconcileSvc(t)
	fc.AddPod("h1", healthyPod("web-x")) // source present
	fc.AddPod("h2", healthyPod("web-x")) // dest healthy

	resolved, ok, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || !ok {
		t.Fatalf("got resolved=%v ok=%v, want true/true (roll forward)", resolved, ok)
	}
	// Source reaped.
	if _, err := fc.PodInspect(context.Background(), "h1", "web-x"); !errors.Is(err, podman.ErrNotFound) {
		t.Fatalf("source still present after roll-forward: %v", err)
	}
}

func TestReconcileMigrate_RollForward_SourceGone(t *testing.T) {
	svc, fc := reconcileSvc(t)
	fc.AddPod("h2", healthyPod("web-x")) // dest healthy, no source

	resolved, ok, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || !ok {
		t.Fatalf("got resolved=%v ok=%v, want true/true (already committed)", resolved, ok)
	}
}

func TestReconcileMigrate_RollBack(t *testing.T) {
	svc, fc := reconcileSvc(t)
	fc.AddPod("h1", healthyPod("web-x")) // source present (stopped or running)
	fc.AddPod("h2", unhealthyPod("web-x")) // dest unhealthy

	resolved, ok, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (roll back)", resolved, ok)
	}
	// Dest reaped.
	if _, err := fc.PodInspect(context.Background(), "h2", "web-x"); !errors.Is(err, podman.ErrNotFound) {
		t.Fatalf("dest still present after roll-back: %v", err)
	}
}

func TestReconcileMigrate_DestAbsent_SourcePresent_RollsBack(t *testing.T) {
	svc, fc := reconcileSvc(t)
	fc.AddPod("h1", healthyPod("web-x")) // source present, dest absent

	resolved, ok, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (roll back, nothing to reap)", resolved, ok)
	}
}

func TestReconcileMigrate_OrphanDest_SourceGone_NeverReaps(t *testing.T) {
	svc, fc := reconcileSvc(t)
	fc.AddPod("h2", unhealthyPod("web-x")) // dest unhealthy, source gone

	resolved, ok, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (orphan dest)", resolved, ok)
	}
	// Safety: dest must NOT be reaped — it is the only copy.
	if _, err := fc.PodInspect(context.Background(), "h2", "web-x"); err != nil {
		t.Fatalf("dest was removed in orphan case (data loss): %v", err)
	}
}

func TestReconcileMigrate_Inconclusive_DestUnreachable(t *testing.T) {
	svc, fc := reconcileSvc(t)
	fc.PodInspectErr = errors.New("dial tcp: connection refused") // all inspects fail

	resolved, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resolved {
		t.Fatal("got resolved=true, want false (host unreachable -> inconclusive)")
	}
}
```

Notes for the implementer (all verified against the current tree):
- `fake.New()`, `fc.AddPod(host, pod)`, `fc.PodInspectErr` exist in `internal/podman/fake/fake.go`.
- `SetVerifyTimeout`, `NewService(client, hosts, tmpls)`, `(*Service).SetStore(store.Store)`, `podName`, `waitRunning`, `Start`, `Delete`, `DeleteOptions`, `MigrateRequest` all exist in `internal/instance`.
- Hosts are keyed by `config.Host.ID`; templates by `config.Template.Meta.ID`. `podName(tmpl, slug)` is `tmpl + "-" + slug`, so `MigrateRequest.Template = "web"`, `Slug = "x"` → pod `web-x`.
- Both `Start` and `Delete` call `s.lookup(host, tmpl)`, which requires the host **and** template to be registered — hence both hosts and template "web" above. `Delete` tolerates an absent pod (idempotent), so the roll-back-with-absent-dest case works.
- `podman.Container` liveness field is `Status`; health is `Health` (empty = no healthcheck).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestReconcileMigrate -v`
Expected: FAIL — `svc.ReconcileMigrate undefined`.

- [ ] **Step 3: Implement the algorithm**

Create `internal/instance/reconcile.go`:

```go
package instance

import (
	"context"
	"errors"

	"github.com/iotready/podman-api/internal/podman"
)

// destStatus is the destination's reconcile-relevant state.
type destStatus int

const (
	destHealthy destStatus = iota
	destUnhealthy
	destAbsent
	destUnreachable
)

// ReconcileMigrate drives a migrate that was interrupted by a daemon restart to a
// consistent state, inspecting the real host state rather than trusting any
// persisted progress. It returns:
//
//	resolved=false  inconclusive (a host was unreachable) — caller retries later
//	resolved=true, succeeded=true   rolled forward (or the commit had finished)
//	resolved=true, succeeded=false  rolled back, or the dest is an orphan left in place
//
// step is a best-effort progress callback (may be nil). It reuses the same
// primitives as Migrate (waitRunning/Start/Delete) and takes migrateLock so it
// cannot race a re-issued migrate of the same instance.
func (s *Service) ReconcileMigrate(ctx context.Context, req MigrateRequest, step func(step, detail string)) (resolved, succeeded bool, err error) {
	if step == nil {
		step = func(string, string) {}
	}
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	ds := s.destState(ctx, req.ToHost, req.Template, req.Slug)
	if ds == destUnreachable {
		step("reconcile-inconclusive", req.ToHost+" unreachable")
		return false, false, nil
	}

	srcPresent, srcReachable := s.sourcePresent(ctx, req.FromHost, req.Template, req.Slug)
	if !srcReachable {
		step("reconcile-inconclusive", req.FromHost+" unreachable")
		return false, false, nil
	}

	// Mutations run on a detached context so a sweep/shutdown cancellation cannot
	// strand a half-finished compensation, mirroring Migrate's rollback/commit.
	mctx := context.WithoutCancel(ctx)

	if ds == destHealthy {
		// Roll forward: dest is truth. Reap the source if it still exists.
		if srcPresent {
			if derr := s.Delete(mctx, req.FromHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); derr != nil {
				step("reconcile-inconclusive", "reap source: "+derr.Error())
				return false, false, nil
			}
		}
		step("reconcile-roll-forward", req.ToHost)
		return true, true, nil
	}

	// dest absent or unhealthy.
	if srcPresent {
		// Roll back: restore source, reap any partial dest.
		if rerr := s.Start(mctx, req.FromHost, req.Template, req.Slug); rerr != nil {
			step("reconcile-inconclusive", "restore source: "+rerr.Error())
			return false, false, nil
		}
		if derr := s.Delete(mctx, req.ToHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); derr != nil {
			step("reconcile-inconclusive", "reap dest: "+derr.Error())
			return false, false, nil
		}
		step("reconcile-roll-back", req.FromHost)
		return true, false, nil
	}

	// Source gone and dest not healthy: never destroy the only copy. Leave the
	// dest in place for the operator and record a needs-attention failure.
	step("reconcile-orphan-dest", req.ToHost+" left in place; source already removed")
	return true, false, nil
}

// (no fmt import: error context is carried via step details and wrapped errors
// from the reused Start/Delete primitives.)

// destState classifies the destination, distinguishing absent from unreachable
// and giving a present-but-not-yet-ready dest the verify window to become healthy.
func (s *Service) destState(ctx context.Context, host, tmpl, slug string) destStatus {
	if _, err := s.client.PodInspect(ctx, host, podName(tmpl, slug)); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return destAbsent
		}
		return destUnreachable
	}
	if err := s.waitRunning(ctx, host, tmpl, slug); err == nil {
		return destHealthy
	}
	// Not healthy within the window. Distinguish a genuine unhealthy dest from a
	// host that dropped mid-wait (which must be inconclusive, not a false roll-back).
	if _, err := s.client.PodInspect(ctx, host, podName(tmpl, slug)); err != nil && !errors.Is(err, podman.ErrNotFound) {
		return destUnreachable
	}
	return destUnhealthy
}

// sourcePresent reports whether the source instance exists and whether the host
// was reachable for the check.
func (s *Service) sourcePresent(ctx context.Context, host, tmpl, slug string) (present, reachable bool) {
	_, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
	if err == nil {
		return true, true
	}
	if errors.Is(err, podman.ErrNotFound) {
		return false, true
	}
	return false, false
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestReconcileMigrate -v`
Expected: PASS (all six).

- [ ] **Step 5: Commit**

```bash
git add internal/instance/reconcile.go internal/instance/reconcile_test.go
git commit -m "feat(instance): ReconcileMigrate roll-forward/roll-back algorithm (#54)"
```

---

## Task 5: migrate adapter + main wiring

**Files:**
- Create: `internal/migrate/reconciler.go`
- Test: `internal/migrate/reconciler_test.go`
- Modify: `cmd/podman-api/main.go`

- [ ] **Step 1: Write the failing test**

Create `internal/migrate/reconciler_test.go`. It checks the adapter maps the algorithm's `(resolved, succeeded)` to the right `store.JobState`, including bad args. Use a tiny in-package fake of the algorithm by exercising the real `instance.Service` over a fake client (mirror Task 4's setup at a minimal level), or — simpler — assert the mapping for the bad-args path which needs no hosts:

```go
package migrate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

func TestReconciler_BadArgs_Fails(t *testing.T) {
	r := &Reconciler{Svc: &instance.Service{}}
	state, resolved, err := r.Reconcile(context.Background(),
		store.Job{ID: "j1", Kind: "migrate", Args: json.RawMessage(`not json`)},
		jobs.NewJobContext(store.NewMemory(), "j1"))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !resolved || state != store.JobFailed {
		t.Fatalf("got state=%q resolved=%v, want failed/true", state, resolved)
	}
}

func TestReconciler_SatisfiesInterface(t *testing.T) {
	var _ jobs.Reconciler = (*Reconciler)(nil)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TAGS" ./internal/migrate/ -run TestReconciler -v`
Expected: FAIL — `undefined: Reconciler`.

- [ ] **Step 3: Implement the adapter**

Create `internal/migrate/reconciler.go`:

```go
package migrate

import (
	"context"
	"encoding/json"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Reconciler adapts instance.Service.ReconcileMigrate to the jobs runner's
// Reconciler contract, recovering migrate jobs interrupted by a daemon restart.
type Reconciler struct {
	Svc *instance.Service
}

// Reconcile decodes the job args and drives the interrupted migrate to a
// consistent state. Unparseable args are a permanent failure (the job cannot be
// acted on); an inconclusive host check leaves the job reconciling for retry.
func (r *Reconciler) Reconcile(ctx context.Context, job store.Job, jc *jobs.JobContext) (store.JobState, bool, error) {
	var req instance.MigrateRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		jc.Step("reconcile-bad-args", err.Error())
		return store.JobFailed, true, nil
	}
	resolved, ok, err := r.Svc.ReconcileMigrate(ctx, req, jc.Step)
	if err != nil {
		return "", false, err
	}
	if !resolved {
		return "", false, nil
	}
	if ok {
		return store.JobSucceeded, true, nil
	}
	return store.JobFailed, true, nil
}

// Ensure Reconciler satisfies the runner contract.
var _ jobs.Reconciler = (*Reconciler)(nil)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -tags "$TAGS" ./internal/migrate/ -run TestReconciler -v`
Expected: PASS.

- [ ] **Step 5: Wire it in main**

In `cmd/podman-api/main.go`, immediately after `runner := jobs.NewRunner(db, registry, workers)` and before `runner.Start(runnerCtx)`, add:

```go
		runner.SetReconcilers(jobs.Reconcilers{
			"migrate": &migrate.Reconciler{Svc: svc},
		})
```

The `migrate` package is already imported (it provides `migrate.Handler`). Confirm with `grep '"github.com/iotready/podman-api/internal/migrate"' cmd/podman-api/main.go`.

- [ ] **Step 6: Build + run the package tests**

Run: `make build`
Expected: builds `bin/podman-api` with no error.
Run: `go test -tags "$TAGS" ./internal/migrate/ ./internal/jobs/ -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/migrate/reconciler.go internal/migrate/reconciler_test.go cmd/podman-api/main.go
git commit -m "feat(migrate): reconciler adapter + wire into runner (#54)"
```

---

## Task 6: API — cancel a reconciling job + OpenAPI

**Files:**
- Modify: `internal/api/jobs.go`
- Test: `internal/api/jobs_test.go`
- Modify: `internal/api/openapi.yaml`

- [ ] **Step 1: Write the failing test**

Append to `internal/api/jobs_test.go` a test that a `reconciling` job cancels to `canceled`. Reuse the existing test harness (`newSrvWithCancel`, the `fakeCanceller`). Seed a reconciling job directly in the memory store:

```go
func TestCancelJob_Reconciling(t *testing.T) {
	js := store.NewMemory()
	ctx := context.Background()
	j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	js.ClaimNext(ctx)
	js.MarkReconciling(ctx, []string{"migrate"})

	srv, base := newSrvWithCancel(t, js, fakeCanceller{running: map[string]bool{}})
	defer srv.Close()

	resp, err := http.Post(base+"/jobs/"+j.ID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if jb, _ := js.GetJob(ctx, j.ID); jb.State != store.JobCanceled {
		t.Fatalf("state = %q, want canceled", jb.State)
	}
}
```

Match the import block and the exact helper signatures already in `internal/api/jobs_test.go` (e.g. `newSrvWithCancel` returns `(*httptest.Server, string)`; `fakeCanceller` has a `running map[string]bool`). Adjust the seeding/post if the existing helpers differ.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TAGS" ./internal/api/ -run TestCancelJob_Reconciling -v`
Expected: FAIL — the handler currently returns a 409 (`reconciling` falls into the `default` branch → `CancelQueued` returns false → `job_not_cancelable`).

- [ ] **Step 3: Add the reconciling branch to cancelJob**

In `internal/api/jobs.go` `cancelJob`, add a case before the `default` (queued) case:

```go
	case store.JobReconciling:
		ok, err := h.jobs.CancelReconciling(r.Context(), id)
		if err != nil {
			WriteError(w, err)
			return
		}
		if ok {
			h.writeAcceptedJob(w, r, id)
			return
		}
		// Lost the CAS to the reconcile loop — it just resolved. Report accurately.
		h.writeCancelConflict(w, r, id)
		return
```

`h.jobs` is a `store.JobStore`, which now has `CancelReconciling` (Task 2), so no interface change is needed here.

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -tags "$TAGS" ./internal/api/ -run TestCancelJob -v`
Expected: PASS (the new test and the existing cancel tests).

- [ ] **Step 5: Add `reconciling` to the OpenAPI job-state enum**

In `internal/api/openapi.yaml`, find the job `state` enum (used by the Job schema; grep `enum:` near `queued`) and add `reconciling`:

```yaml
        state:
          type: string
          enum: [queued, running, reconciling, succeeded, failed, canceled]
          description: >
            Job lifecycle state. `reconciling` is a non-terminal state entered
            only at startup: a migrate that was in flight when the daemon
            restarted is being driven to a consistent end state (rolled forward
            if its destination is already healthy, otherwise rolled back).
            Cancelling a reconciling job records `canceled` and leaves the source
            and destination as-is for manual cleanup.
```

Match the surrounding indentation/style of the file (the exact enum line may already be inline — just insert `reconciling` after `running` and add/extend the description).

- [ ] **Step 6: Verify OpenAPI parses + full build**

Run: `make build`
Expected: builds cleanly. If the repo has an OpenAPI lint/validate target or test (grep for `openapi` in the Makefile / `internal/api`), run it; expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/api/jobs.go internal/api/jobs_test.go internal/api/openapi.yaml
git commit -m "feat(api): cancel a reconciling job + openapi enum (#54)"
```

---

## Final verification (after all tasks)

- [ ] **gofmt + vet + full test suite**

```bash
gofmt -l .                 # expect: empty
go vet -tags "$TAGS" ./...
make test                  # full unit suite, expect all ok
```

- [ ] **Confirm the boot back-compat path**

`go test -tags "$TAGS" ./internal/jobs/ -v` includes the pre-existing runner tests; they must still pass (with no reconcilers registered, `MarkReconciling(nil)` is a no-op and behaviour matches the old `FailRunning`-only path).

---

## Out of scope (documented in the spec)

- Resume-and-continue (continuing a migrate from its interrupted phase; re-spawning an evacuate's un-started children).
- A `-job-reconcile-interval` flag (interval is a const for now).
- Reconciliation metrics (#52 OTel).
- Wiki page — published during post-merge housekeeping per repo convention.
