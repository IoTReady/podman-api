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
	enqueued      *prometheus.GaugeVec    // kind — current queued count
	jobs          *prometheus.CounterVec  // kind, result — started/succeeded/failed/canceled
	duration      *prometheus.HistogramVec // kind
	inFlight      *prometheus.GaugeVec    // kind
	rollbacks     *prometheus.CounterVec  // kind
	childFailures *prometheus.CounterVec  // kind
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