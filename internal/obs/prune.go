package obs

import "github.com/prometheus/client_golang/prometheus"

// PruneMetrics implements the prune.Metrics interface (structurally — obs must
// not import prune) with Prometheus counters. Created with an explicit
// Registerer so production registers on the default registry
// (NewPruneMetrics(prometheus.DefaultRegisterer)) and tests use a private one.
type PruneMetrics struct {
	runs      *prometheus.CounterVec
	reclaimed *prometheus.CounterVec
}

// NewPruneMetrics builds and registers the prune collectors on reg.
func NewPruneMetrics(reg prometheus.Registerer) *PruneMetrics {
	m := &PruneMetrics{
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_prune_runs_total",
			Help: "Count of prune job outcomes by host and result.",
		}, []string{"host", "result"}),
		reclaimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_prune_reclaimed_bytes_total",
			Help: "Bytes reclaimed by prune, by host and scope.",
		}, []string{"host", "scope"}),
	}
	reg.MustRegister(m.runs, m.reclaimed)
	return m
}

// RunDone records one finished prune run.
func (m *PruneMetrics) RunDone(host, result string) {
	m.runs.WithLabelValues(host, result).Inc()
}

// Reclaimed adds reclaimed bytes for a scope.
func (m *PruneMetrics) Reclaimed(host, scope string, bytes int64) {
	if bytes < 0 {
		bytes = 0
	}
	m.reclaimed.WithLabelValues(host, scope).Add(float64(bytes))
}
