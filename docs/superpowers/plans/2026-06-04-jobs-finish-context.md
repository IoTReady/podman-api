# Jobs finish-context fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `jobs.Runner.finish` record a job's terminal state on a fresh short-lived context so a job that completes at shutdown is never mislabelled `failed` by boot recovery.

**Architecture:** Drop the cancellable `ctx` parameter from `Runner.finish` and derive a `context.WithTimeout(context.Background(), 5s)` inside it. A context-honoring test double (embedding `*store.Memory`) reproduces the shutdown race that the plain Memory store cannot.

**Tech Stack:** Go, standard `context`/`testing`. Build/test tags: `containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper`.

---

### Task 1: Bound `finish()` to a fresh context

**Files:**
- Modify: `internal/jobs/runner.go` (the `finish` method ~line 130-137 and its three call sites in `run` ~line 139-151)
- Test: `internal/jobs/runner_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/jobs/runner_test.go`:

```go
// ctxAwareStore wraps a Memory store and fails Finish when the supplied context
// is already cancelled, reproducing the SQLite behaviour the plain Memory store
// (which ignores ctx) cannot.
type ctxAwareStore struct {
	*store.Memory
}

func (c ctxAwareStore) Finish(ctx context.Context, id string, state store.JobState, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.Memory.Finish(ctx, id, state, errMsg)
}

func TestRunner_FinishSurvivesCancelledRunnerCtx(t *testing.T) {
	m := store.NewMemory()
	cs := ctxAwareStore{m}
	reg := Registry{"test": handlerFunc(func(ctx context.Context, job store.Job, jc *JobContext) error {
		return nil // succeeds
	})}
	r := NewRunner(cs, reg, 1)

	j, _ := m.Enqueue(context.Background(), "test", json.RawMessage(`{}`), "")
	// Claim it so run() has a running job to finish.
	claimed, ok, _ := m.ClaimNext(context.Background())
	if !ok || claimed.ID != j.ID {
		t.Fatalf("claim failed: ok=%v", ok)
	}

	// A cancelled runner context must NOT prevent the terminal-state write.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.run(ctx, claimed)

	got, _ := m.GetJob(context.Background(), j.ID)
	if got.State != store.JobSucceeded {
		t.Fatalf("want succeeded, got %q (err=%q)", got.State, got.Error)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd internal/jobs && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestRunner_FinishSurvivesCancelledRunnerCtx ./...`
Expected: FAIL — `want succeeded, got "running"` (the cancelled ctx makes the double's `Finish` return `context.Canceled`, so the state is never written).

- [ ] **Step 3: Implement the fix**

In `internal/jobs/runner.go`, replace the `finish` method:

```go
// finishTimeout bounds the terminal-state write so a slow/contended store
// can't hang the worker, while keeping the write independent of the runner's
// (cancellable) lifecycle context.
const finishTimeout = 5 * time.Second

// finish writes the terminal state on a fresh short-lived context, logging (not
// returning) a store error. It deliberately does NOT use the runner's lifecycle
// context: at shutdown that context is cancelled, and a completed job must still
// record its true terminal state. Reap-on-boot remains the fallback only for a
// true process kill between handler-return and this write.
func (r *Runner) finish(id string, state store.JobState, errMsg string) {
	ctx, cancel := context.WithTimeout(context.Background(), finishTimeout)
	defer cancel()
	if err := r.store.Finish(ctx, id, state, errMsg); err != nil {
		log.Printf("jobs: finish %s failed: %v", id, err)
	}
}
```

Update the three call sites in `run` to drop the `ctx` argument:

```go
func (r *Runner) run(ctx context.Context, job store.Job) {
	h, ok := r.handlers[job.Kind]
	if !ok {
		r.finish(job.ID, store.JobFailed, "no handler for kind "+job.Kind)
		return
	}
	jc := &JobContext{store: r.store, id: job.ID}
	if err := h.Run(ctx, job, jc); err != nil {
		r.finish(job.ID, store.JobFailed, err.Error())
		return
	}
	r.finish(job.ID, store.JobSucceeded, "")
}
```

`run` keeps its `ctx` parameter — it is still passed to the handler (`h.Run`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd internal/jobs && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`
Expected: PASS — the new test and all existing runner tests.

- [ ] **Step 5: Verify gofmt/vet clean, then commit**

```bash
gofmt -l internal/jobs/      # must print nothing
go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/jobs/
git add internal/jobs/runner.go internal/jobs/runner_test.go
git commit -m "fix(jobs): record terminal state on a fresh context (#44)"
```

---

## Self-Review

- **Spec coverage:** the spec's single requirement (finish on a background-derived context, drop ctx param, test via context-honoring double) maps to Task 1. ✓
- **Placeholders:** none. ✓
- **Type consistency:** `finish(id, state, errMsg)` signature is used consistently at all three call sites; `ctxAwareStore` embeds `*store.Memory` so it satisfies `store.JobStore`. ✓
