# Job Observability Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Prometheus metrics (counters, histograms, gauges) for job lifecycle (migrate/evacuate/all kinds) following the existing `PruneMetrics` pattern.

**Architecture:** New `obs.JobMetrics` struct implements a `jobs.Metrics` interface. The runner's `run()` method instruments core lifecycle centrally; migrate/evacuate handlers track specific events (rollbacks, child failures). Wired through `buildJobRegistry` in main.go.

**Tech Stack:** Go, `prometheus/client_golang`, existing `internal/obs/` patterns.

---

### Task 1: Create `jobs.Metrics` interface

**Files:**
- Create: `internal/jobs/metrics.go`

- [ ] **Step 1: Write the interface file**

Create `internal/jobs/metrics.go` with the Metrics interface that covers runner and handler instrumentation:

```go
package jobs

import "time"

// Metrics records job lifecycle events. Implementations must be safe for
// concurrent use. A nil *Metrics is valid (all methods are no-ops).
type Metrics interface {
	JobStarted(kind string)
	JobFinished(kind, result string)
	ObserveDuration(kind string, d time.Duration)
	JobEnqueued(kind string)
	Rollback(kind string)
	ChildFailure(kind string)
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/jobs/metrics.go
git commit -m "feat(52): add jobs.Metrics interface for job observability"
```

---

### Task 2: Create `obs.JobMetrics` implementation

**Files:**
- Create: `internal/obs/job.go`
- Create: `internal/obs/job_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/obs/job_test.go`:

```go
package obs

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestJobMetricsRecord(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewJobMetrics(reg)

	m.JobEnqueued("migrate")
	m.JobStarted("migrate")
	m.JobFinished("migrate", "succeeded")
	m.ObserveDuration("migrate", 2*time.Second)
	m.Rollback("migrate")
	m.ChildFailure("evacuate")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		check func(metrics []*dto.MetricFamily)
	}{
		{"enqueued", func(ms []*dto.MetricFamily) {
			for _, mf := range ms {
				if mf.GetName() == "podman_api_jobs_enqueued" {
					if got := sumGauge(mf.Metric); got != 1 {
						t.Fatalf("enqueued = %v, want 1", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_enqueued")
		}},
		{"jobs_total", func(ms []*dto.MetricFamily) {
			for _, mf := range ms {
				if mf.GetName() == "podman_api_jobs_total" {
					if got := sumCounter(mf.Metric); got != 1 {
						t.Fatalf("jobs_total = %v, want 1", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_total")
		}},
		{"duration", func(ms []*dto.MetricFamily) {
			for _, mf := range ms {
				if mf.GetName() == "podman_api_job_duration_seconds" {
					if got := sumHistogram(mf.Metric); got != 1 {
						t.Fatalf("duration observations = %v, want 1", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_job_duration_seconds")
		}},
		{"in_flight", func(ms []*dto.MetricFamily) {
			for _, mf := range ms {
				if mf.GetName() == "podman_api_jobs_in_flight" {
					if got := sumGauge(mf.Metric); got != 1 {
						t.Fatalf("in_flight = %v, want 1", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_in_flight")
		}},
		{"rollbacks", func(ms []*dto.MetricFamily) {
			for _, mf := range ms {
				if mf.GetName() == "podman_api_jobs_rollbacks_total" {
					if got := sumCounter(mf.Metric); got != 1 {
						t.Fatalf("rollbacks = %v, want 1", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_rollbacks_total")
		}},
		{"child_failures", func(ms []*dto.MetricFamily) {
			for _, mf := range ms {
				if mf.GetName() == "podman_api_jobs_child_failures_total" {
					if got := sumCounter(mf.Metric); got != 1 {
						t.Fatalf("child_failures = %v, want 1", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_child_failures_total")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.check)
	}
}

func sumGauge(ms []*dto.Metric) float64 {
	var v float64
	for _, m := range ms {
		v += m.GetGauge().GetValue()
	}
	return v
}

func sumHistogram(ms []*dto.Metric) float64 {
	var v float64
	for _, m := range ms {
		v += float64(m.GetHistogram().GetSampleCount())
	}
	return v
}
```

Add `sumGauge` and `sumHistogram` alongside the existing `sumCounter` in `prune_test.go`. Actually no — keep them in the same test file since they're package-private. Or better, add them to `job_test.go` since `prune_test.go` already has `sumCounter`.

The `sumCounter` function is already in `prune_test.go`. We need `sumGauge` and `sumHistogram` in `job_test.go`.

- [ ] **Step 2: Run to confirm it fails**

```bash
cd /home/tej/projects/podman-api && make test 2>&1 | head -20
```

Expected: build failure — `NewJobMetrics` undefined.

- [ ] **Step 3: Write the implementation**

Create `internal/obs/job.go`:

```go
package obs

import (
	"time"

	"github.com/iotready/podman-api/internal/jobs"
	"github.com/prometheus/client_golang/prometheus"
)

// Compile-time check: *JobMetrics implements jobs.Metrics.
var _ jobs.Metrics = (*JobMetrics)(nil)

// JobMetrics implements jobs.Metrics with Prometheus collectors. Created with
// an explicit Registerer so production registers on the default registry
// (NewJobMetrics(prometheus.DefaultRegisterer)) and tests use a private one.
type JobMetrics struct {
	enqueued      *prometheus.GaugeVec   // kind — current queued count
	jobs          *prometheus.CounterVec // kind, result — started/succeeded/failed/canceled
	duration      *prometheus.HistogramVec // kind
	inFlight      *prometheus.GaugeVec   // kind
	rollbacks     *prometheus.CounterVec // kind
	childFailures *prometheus.CounterVec // kind
}

// NewJobMetrics builds and registers the collectors on reg.
func NewJobMetrics(reg prometheus.Registerer) *JobMetrics {
	m := &JobMetrics{
		enqueued: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "podman_api_jobs_enqueued",
			Help: "Current number of queued (not yet claimed) jobs by kind.",
		}, []string{"kind"}),
		jobs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_jobs_total",
			Help: "Count of job lifecycle events by kind and result (started/succeeded/failed/canceled).",
		}, []string{"kind", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "podman_api_job_duration_seconds",
			Help:    "Job handler duration by kind.",
			Buckets: prometheus.DefBuckets,
		}, []string{"kind"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "podman_api_jobs_in_flight",
			Help: "Number of jobs currently being handled by kind.",
		}, []string{"kind"}),
		rollbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_jobs_rollbacks_total",
			Help: "Count of migrate rollbacks by kind.",
		}, []string{"kind"}),
		childFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_jobs_child_failures_total",
			Help: "Count of evacuate child failures by kind.",
		}, []string{"kind"}),
	}
	reg.MustRegister(m.enqueued, m.jobs, m.duration, m.inFlight, m.rollbacks, m.childFailures)
	return m
}

// JobStarted records a job being claimed and started.
func (m *JobMetrics) JobStarted(kind string) {
	m.jobs.WithLabelValues(kind, "started").Inc()
	m.enqueued.WithLabelValues(kind).Dec()
	m.inFlight.WithLabelValues(kind).Inc()
}

// JobFinished records a terminal job outcome (succeeded/failed/canceled).
func (m *JobMetrics) JobFinished(kind, result string) {
	m.jobs.WithLabelValues(kind, result).Inc()
	m.inFlight.WithLabelValues(kind).Dec()
}

// ObserveDuration records a job handler's wall-clock duration.
func (m *JobMetrics) ObserveDuration(kind string, d time.Duration) {
	m.duration.WithLabelValues(kind).Observe(d.Seconds())
}

// JobEnqueued records a new job being enqueued.
func (m *JobMetrics) JobEnqueued(kind string) {
	m.enqueued.WithLabelValues(kind).Inc()
}

// Rollback records a migrate rollback event.
func (m *JobMetrics) Rollback(kind string) {
	m.rollbacks.WithLabelValues(kind).Inc()
}

// ChildFailure records an evacuate child migration failure.
func (m *JobMetrics) ChildFailure(kind string) {
	m.childFailures.WithLabelValues(kind).Inc()
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/tej/projects/podman-api && make test 2>&1 | grep -E '(PASS|FAIL|ok)'
```

Expected: `ok  github.com/iotready/podman-api/internal/obs`

- [ ] **Step 5: Commit**

```bash
git add internal/obs/job.go internal/obs/job_test.go
git commit -m "feat(52): add JobMetrics implementing jobs.Metrics"
```

---

### Task 3: Instrument the runner with metrics

**Files:**
- Modify: `internal/jobs/runner.go`
- Modify: `internal/jobs/runner_test.go`

- [ ] **Step 1: Add `Metrics` field and noopMetrics to runner.go**

After the `inflightJob` type (around line 101), add:

```go
// noopMetrics is a nil-safe default for *Runner.Metrics.
type noopMetrics struct{}

func (noopMetrics) JobStarted(string)              {}
func (noopMetrics) JobFinished(string, string)      {}
func (noopMetrics) ObserveDuration(string, time.Duration) {}
func (noopMetrics) JobEnqueued(string)              {}
func (noopMetrics) Rollback(string)                 {}
func (noopMetrics) ChildFailure(string)             {}
```

Add `Metrics` field to the `Runner` struct (in the field list, around line 87):

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
	Metrics           Metrics // optional; nil-safe
}
```

Add a `metric()` helper method:

```go
func (r *Runner) metric() Metrics {
	if r.Metrics == nil {
		return noopMetrics{}
	}
	return r.Metrics
}
```

- [ ] **Step 2: Instrument the `run()` method**

Replace the `run` method (lines 362-391) with:

```go
func (r *Runner) run(ctx context.Context, job store.Job) {
	h, ok := r.handlers[job.Kind]
	if !ok {
		r.finish(job.ID, store.JobFailed, "no handler for kind "+job.Kind)
		return
	}

	r.metric().JobStarted(job.Kind)
	defer func(start time.Time, kind string) {
		r.metric().ObserveDuration(kind, time.Since(start))
	}(time.Now(), job.Kind)

	jctx, cancel := context.WithCancel(ctx)
	entry := &inflightJob{cancel: cancel}
	r.mu.Lock()
	r.inflight[job.ID] = entry
	r.mu.Unlock()

	err := h.Run(jctx, job, NewJobContext(r.store, job.ID))

	r.mu.Lock()
	canceled := entry.canceled
	delete(r.inflight, job.ID)
	r.mu.Unlock()
	cancel()

	switch {
	case canceled:
		r.finish(job.ID, store.JobCanceled, "canceled by operator")
		r.metric().JobFinished(job.Kind, "canceled")
	case err != nil:
		r.finish(job.ID, store.JobFailed, err.Error())
		r.metric().JobFinished(job.Kind, "failed")
	default:
		r.finish(job.ID, store.JobSucceeded, "")
		r.metric().JobFinished(job.Kind, "succeeded")
	}
}
```

Note: `JobStarted` is called before the handler runs; `JobFinished` is called after `finish()` (store write). The `ObserveDuration` is called via deferred closure so it captures total time including the store write if needed. Actually — let me place `JobFinished` AFTER `finish()` so the duration covers the full lifecycle. The duration defer captures the start time set right after `JobStarted`.

Wait, I placed the defer right after `JobStarted` — it fires at function exit which includes the finish() call. That's correct — duration covers handler run + store finish write.

- [ ] **Step 3: Instrument the `finish()` method — no changes needed**

The finish method doesn't need changes — the `run()` method already calls `r.metric().JobFinished()` after `r.finish()`.

- [ ] **Step 4: Add `reconcileOne` instrumentation**

Add `JobStarted`/`JobFinished` calls in `reconcileOne` (line 283-309):

```go
func (r *Runner) reconcileOne(ctx context.Context, job store.Job, rec Reconciler) {
	jctx, cancel := context.WithCancel(ctx)
	entry := &inflightJob{cancel: cancel}
	r.mu.Lock()
	r.inflight[job.ID] = entry
	r.mu.Unlock()

	state, message, resolved, err := rec.Reconcile(jctx, job, NewJobContext(r.store, job.ID))

	r.mu.Lock()
	delete(r.inflight, job.ID)
	r.mu.Unlock()
	cancel()

	if err != nil {
		log.Printf("jobs: reconcile %s errored (will retry): %v", job.ID, err)
		return
	}
	if !resolved {
		return
	}
	if _, err := r.store.ResolveReconciling(ctx, job.ID, state, message); err != nil {
		log.Printf("jobs: reconcile resolve %s failed: %v", job.ID, err)
	}
}
```

Reconciler doesn't get metric instrumentation — reconcile is a recovery path, not a "job run" in the conventional sense, and adding metrics here would conflate "reconciled" with "started" counts. Skip it per YAGNI.

- [ ] **Step 5: Add runner metric tests to runner_test.go**

Read the existing runner_test.go first, then add test cases for metrics. Add a test at the end of the file:

```go
func TestRunnerMetrics(t *testing.T) {
	t.Parallel()
	js := store.NewMemStore()
	m := &mockMetrics{}
	r := NewRunner(js, Registry{"test": &okHandler{}}, 1)
	r.Metrics = m

	_, err := js.Enqueue(context.Background(), "test", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	r.Notify()
	// Run one worker iteration
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r.run(ctx, store.Job{ID: "test-job", Kind: "test"})

	if !m.started {
		t.Error("JobStarted not called")
	}
	if m.finishedResult != "succeeded" {
		t.Errorf("JobFinished result = %q, want %q", m.finishedResult, "succeeded")
	}
	if m.durationKind != "test" {
		t.Errorf("ObserveDuration kind = %q, want %q", m.durationKind, "test")
	}
}
```

And add a mock Metrics implementation at the bottom of runner_test.go:

```go
type mockMetrics struct {
	started        bool
	finishedResult string
	durationKind   string
	finishedKind   string
}

func (m *mockMetrics) JobStarted(kind string)              { m.started = true }
func (m *mockMetrics) JobFinished(kind, result string)      { m.finishedKind, m.finishedResult = kind, result }
func (m *mockMetrics) ObserveDuration(kind string, d time.Duration) { m.durationKind = kind }
func (m *mockMetrics) JobEnqueued(string)                   {}
func (m *mockMetrics) Rollback(string)                      {}
func (m *mockMetrics) ChildFailure(string)                  {}
```

- [ ] **Step 6: Run tests to verify**

```bash
cd /home/tej/projects/podman-api && go test ./internal/jobs/ -run TestRunnerMetrics -v
```

Expected: PASS

Then run full test suite:

```bash
cd /home/tej/projects/podman-api && make test 2>&1 | grep -E '(PASS|FAIL|ok)'
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add internal/jobs/runner.go internal/jobs/runner_test.go
git commit -m "feat(52): instrument job runner with lifecycle metrics"
```

---

### Task 4: Instrument migrate handler for rollback detection

**Files:**
- Modify: `internal/migrate/handler.go`
- (No test changes needed — existing handler tests pass since Metrics is optional)

- [ ] **Step 1: Add Metrics field to migrate Handler**

Add the field to the Handler struct and detect rollback events via the step wrapper:

```go
package migrate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Handler runs "migrate" jobs by delegating to instance.Service.Migrate.
type Handler struct {
	Svc     *instance.Service
	Metrics jobs.Metrics // optional; nil-safe
}

// Run unmarshals the job args into a MigrateRequest and performs the migration,
// reporting progress through the job context. If the migration rolls back, the
// rollback is recorded in Metrics.
func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.MigrateRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode migrate args: %w", err)
	}

	var rolledBack bool
	wrappedStep := func(step, detail string) {
		if step == "rollback" {
			rolledBack = true
		}
		jc.Step(step, detail)
	}

	err := h.Svc.Migrate(ctx, req, wrappedStep)
	if rolledBack && h.Metrics != nil {
		h.Metrics.Rollback("migrate")
	}
	return err
}
```

- [ ] **Step 2: Run tests**

```bash
cd /home/tej/projects/podman-api && go test ./internal/migrate/ -v 2>&1 | grep -E '(PASS|FAIL|ok)'
```

Expected: all pass (Metrics field is nil → no-op)

- [ ] **Step 3: Commit**

```bash
git add internal/migrate/handler.go
git commit -m "feat(52): instrument migrate handler for rollback metrics"
```

---

### Task 5: Instrument evacuate handler for child failure tracking

**Files:**
- Modify: `internal/evacuate/handler.go`
- (No test changes needed — existing handler tests pass since Metrics is optional)

- [ ] **Step 1: Add Metrics field to evacuate Handler**

Add the field and track child failures in `runChild`. The field goes in the Handler struct and `Run` passes it through. Since the handler is constructed directly in tests and production, we add the field:

```go
type Handler struct {
	Svc         *instance.Service
	Jobs        store.JobStore
	Concurrency int
	Metrics     jobs.Metrics // optional; nil-safe
	migrate     func(ctx context.Context, req instance.MigrateRequest, step func(step, detail string)) error
}
```

No change needed to `Run()`. Only `runChild()` is modified — find the line after the `migErr != nil` check where the state/errMsg is set (around line 138), and add:

In `runChild`, after the state determination and before the Finish call, add:

```go
	state, errMsg := store.JobSucceeded, ""
	if migErr != nil {
		if h.Metrics != nil {
			h.Metrics.ChildFailure("evacuate")
		}
		state, errMsg = store.JobFailed, migErr.Error()
		if errors.Is(migErr, context.Canceled) {
			state = store.JobCanceled
		}
	}
```

Wait, looking at the handler more carefully, `h.Metrics` won't be accessible from `runChild` since it's a method on `*Handler`. Yes it will — `runChild` is a method on `*Handler`:

```go
func (h *Handler) runChild(ctx context.Context, parentID string, m instance.MigrateRequest,
	mig func(context.Context, instance.MigrateRequest, func(string, string)) error) error {
```

So `h.Metrics` is accessible. Good.

- [ ] **Step 2: Run tests**

```bash
cd /home/tej/projects/podman-api && go test ./internal/evacuate/ -v 2>&1 | grep -E '(PASS|FAIL|ok)'
```

Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add internal/evacuate/handler.go
git commit -m "feat(52): instrument evacuate handler for child failure metrics"
```

---

### Task 6: Wire JobMetrics through main.go

**Files:**
- Modify: `cmd/podman-api/main.go`

- [ ] **Step 1: Create JobMetrics and pass to buildJobRegistry**

Around line 182 in main.go where `pruneMetrics` is created, add `jobMetrics`:

```go
jobMetrics := obs.NewJobMetrics(prometheus.DefaultRegisterer)
```

Change the `buildJobRegistry` call (line 183) to pass `jobMetrics`:

```go
registry, reconcilers := buildJobRegistry(svc, client, db, *evacConc, pruneMetrics, jobMetrics)
```

After `runner := jobs.NewRunner(db, registry, workers)` (line 188), add:

```go
runner.Metrics = jobMetrics
```

Update the `buildJobRegistry` function signature and body (line 432):

```go
func buildJobRegistry(svc *instance.Service, client podman.Client, db store.DB, evacConc int, pruneMetrics *obs.PruneMetrics, jobMetrics *obs.JobMetrics) (jobs.Registry, jobs.Reconcilers) {
	reg := jobs.Registry{
		"migrate":  &migrate.Handler{Svc: svc, Metrics: jobMetrics},
		"evacuate": &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: evacConc, Metrics: jobMetrics},
		"prune":    &prune.Handler{Client: client, Jobs: db, Metrics: pruneMetrics},
		"backup":   &backuppkg.Handler{Svc: svc},
		"restore":  &backuppkg.RestoreHandler{Svc: svc},
	}
	recs := jobs.Reconcilers{
		"migrate": &migrate.Reconciler{Svc: svc},
		"backup":  &backuppkg.Reconciler{Svc: svc},
	}
	return reg, recs
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd /home/tej/projects/podman-api && make build 2>&1
```

Expected: binary compiles to `bin/podman-api`

- [ ] **Step 3: Commit**

```bash
git add cmd/podman-api/main.go
git commit -m "feat(52): wire JobMetrics through registerJobRegistry and runner"
```

---

### Task 7: Integration — run full test suite

- [ ] **Step 1: Run all unit tests**

```bash
cd /home/tej/projects/podman-api && make test 2>&1
```

Expected: all tests pass, `gofmt` is clean, `go vet` is clean.

- [ ] **Step 2: Run gofmt check**

```bash
cd /home/tej/projects/podman-api && gofmt -l . 2>&1
```

Expected: empty output (no formatting issues)

- [ ] **Step 3: Run go vet**

```bash
cd /home/tej/projects/podman-api && go vet ./... 2>&1
```

Expected: clean (no output or only vendor warnings)

- [ ] **Step 4: Commit any fixes**

If any issues found in steps 1-3, fix and commit.

---

### Task 8: Create PR branch and push

- [ ] **Step 1: Create branch from current worktree**

```bash
cd /home/tej/projects/podman-api && git checkout -b feat/52-job-observability
```

- [ ] **Step 2: Push the branch**

```bash
cd /home/tej/projects/podman-api && git push origin feat/52-job-observability
```

- [ ] **Step 3: Create PR**

```bash
cd /home/tej/projects/podman-api && forgejo pr create tej/podman-api --title="feat(52): job observability — migrate/evacuate/metrics" --head=feat/52-job-observability --base=main --body="Implements Prometheus metrics for job lifecycle following the established PruneMetrics pattern.

- Counters: jobs enqueued/succeeded/failed/canceled by kind; migrate rollbacks; evacuate child failures
- Histogram: job duration by kind
- Gauges: in-flight jobs, queued depth by kind

Closes #52"
```