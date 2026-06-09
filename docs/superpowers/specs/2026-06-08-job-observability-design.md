# Job Observability Metrics

**Issue:** #52
**Date:** 2026-06-08

## Summary

Add Prometheus metrics for the job system (migrate/evacuate + all job kinds) 
following the established `PruneMetrics` pattern in `internal/obs/`. Core 
lifecycle metrics are instrumented centrally in the job runner; specific 
events (rollbacks, child failures) are instrumented at the handler level.

## Metrics

| Metric | Type | Labels | Source |
|--------|------|--------|--------|
| `podman_api_jobs_total` | Counter | `kind`, `result` | Runner `run()` |
| `podman_api_job_duration_seconds` | Histogram | `kind` | Runner `run()` |
| `podman_api_jobs_in_flight` | Gauge | `kind` | Runner `run()` (+1), `finish()` (-1) |
| `podman_api_jobs_queued` | Gauge | `kind` | Enqueue callers |
| `podman_api_jobs_rollbacks_total` | Counter | `kind` | Migrate handler |
| `podman_api_jobs_child_failures_total` | Counter | `kind` | Evacuate handler |

`result` values for `jobs_total`: `started`, `succeeded`, `failed`, `canceled`.

## Architecture

### 1. `internal/obs/job.go` — `JobMetrics` struct

Follows `PruneMetrics` exactly:

```go
type JobMetrics struct {
    jobs          *prometheus.CounterVec   // kind, result
    duration      *prometheus.HistogramVec // kind
    inFlight      *prometheus.GaugeVec     // kind
    queued        *prometheus.GaugeVec     // kind
    rollbacks     *prometheus.CounterVec   // kind
    childFailures *prometheus.CounterVec   // kind
}

func NewJobMetrics(reg prometheus.Registerer) *JobMetrics
func (m *JobMetrics) JobStarted(kind string)
func (m *JobMetrics) JobFinished(kind, result string)
func (m *JobMetrics) ObserveDuration(kind string, d time.Duration)
func (m *JobMetrics) RecordQueued(kind string, n int)     // for periodic sampling
func (m *JobMetrics) Rollback(kind string)
func (m *JobMetrics) ChildFailure(kind string)
```

### 2. Runner instrumentation (`internal/jobs/runner.go`)

The `run()` method is the single chokepoint for all job lifecycle:

```go
type Runner struct {
    // ... existing fields ...
    Metrics *JobMetrics // optional, nil-safe
}
```

**`run()` changes:**
- Before `h.Run()`: `JobStarted(kind)` + inFlight gauge +1 + record start time
- After `finish()`: `JobFinished(kind, result)` + inFlight gauge -1 + `ObserveDuration(kind, ...)`

### 3. Handler-level instrumentation

**Migrate handler** (`internal/migrate/handler.go`): Wraps `jc.Step` to detect
the `"rollback"` step label. When Svc.Migrate emits a rollback step, the
handler increments `Metrics.Rollback("migrate")` before returning the error.

**Evacuate handler** (`internal/evacuate/handler.go`): In `runChild()`, when
`migErr != nil`, increments `Metrics.ChildFailure("evacuate")` before
returning.

### 4. Wiring (`cmd/podman-api/main.go`)

```go
jobMetrics := obs.NewJobMetrics(prometheus.DefaultRegisterer)
registry, reconcilers := buildJobRegistry(svc, client, db, *evacConc, pruneMetrics, jobMetrics)
```

`buildJobRegistry` passes `jobMetrics` to:
- `jobs.NewRunner` (now accepts optional metrics)
- `migrate.Handler{Metrics: jobMetrics}`
- `evacuate.Handler{Metrics: jobMetrics}`

## Changes per file

| File | Change |
|------|--------|
| `internal/obs/job.go` | **New** — `JobMetrics` struct with 6 collectors |
| `internal/obs/job_test.go` | **New** — test recording + gathering |
| `internal/jobs/runner.go` | Add `Metrics` field; instrument `run()` and `finish()` |
| `internal/jobs/runner_test.go` | Test runner metrics |
| `internal/migrate/handler.go` | Add `Metrics` field; detect rollback via step wrapper |
| `internal/evacuate/handler.go` | Add `Metrics` field; track child failures |
| `cmd/podman-api/main.go` | Create `jobMetrics`, wire through `buildJobRegistry` |

## Non-goals

- OTel pipeline (doesn't exist yet — issue #63/#64 scope)
- Per-instance breakdown of migrate duration (would leak cardinality 
  through instance slugs; the `kind` label gives aggregate signal)
- Changing the `prune.Metrics` interface (unrelated)