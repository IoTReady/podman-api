package obs

import "github.com/prometheus/client_golang/prometheus"

// RegistryPruneMetrics implements the registryprune.Metrics interface
// (structurally — obs must not import registryprune) with Prometheus
// collectors. Created with an explicit Registerer so production registers on
// the default registry and tests use a private one.
//
// There is no "host" label anywhere here, unlike PruneMetrics: there is exactly
// one container registry, so every series is fleet-global.
type RegistryPruneMetrics struct {
	runs    *prometheus.CounterVec
	deleted *prometheus.CounterVec
	// reclaimed is a CounterVec with NO labels, not a plain Counter. A plain
	// Counter is exported the moment it is registered, so it would read 0 from
	// process start — which is exactly the confusion the handler goes out of its
	// way to avoid by not calling BytesReclaimed on an unmeasured run. With
	// -registry-prune-registry-container empty (the default) nothing ever
	// measures, and a plain Counter would sit flat at zero forever, indis-
	// tinguishable from "GC ran and freed nothing". A label-less Vec has no
	// child until first use, so the series is ABSENT until a real measurement
	// lands.
	reclaimed *prometheus.CounterVec
	skipped   *prometheus.CounterVec
	// candidates is a gauge, not a counter: the useful question about a
	// tripwire skip is "how far over the threshold was it", which is a level,
	// not a rate. The counter beside it is what alerting fires on.
	candidates *prometheus.GaugeVec
}

// NewRegistryPruneMetrics builds and registers the registry-prune collectors on
// reg.
func NewRegistryPruneMetrics(reg prometheus.Registerer) *RegistryPruneMetrics {
	m := &RegistryPruneMetrics{
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_registry_prune_runs_total",
			Help: "Count of registry prune job outcomes by result.",
		}, []string{"result"}),
		deleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_registry_prune_manifests_deleted_total",
			Help: "Registry manifests deleted by repository.",
		}, []string{"repo"}),
		reclaimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_registry_prune_reclaimed_bytes_total",
			Help: "Bytes reclaimed by registry blob garbage collection. A floor, not a measurement: the registry is serving pushes again by the time the after-size is read. ABSENT until a run measures it — an absent series means blob GC is off or sizing is unconfigured, never that nothing was reclaimed.",
		}, []string{}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_registry_prune_repos_skipped_total",
			Help: "Count of repositories skipped by the per-repo delete tripwire.",
		}, []string{"repo"}),
		candidates: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "podman_api_registry_prune_skipped_candidates",
			Help: "Delete candidates in the most recent tripwire skip, by repository.",
		}, []string{"repo"}),
	}
	reg.MustRegister(m.runs, m.deleted, m.reclaimed, m.skipped, m.candidates)
	return m
}

// RunDone records one terminal run outcome ("succeeded", "failed", "dry-run",
// "aborted").
func (m *RegistryPruneMetrics) RunDone(result string) {
	m.runs.WithLabelValues(result).Inc()
}

// ManifestsDeleted adds n manifests removed from repo.
func (m *RegistryPruneMetrics) ManifestsDeleted(repo string, n int) {
	if n < 0 {
		n = 0
	}
	m.deleted.WithLabelValues(repo).Add(float64(n))
}

// BytesReclaimed adds the bytes blob GC freed on disk. The handler calls this
// only when the measurement was real, so an absent sample means "not measured",
// never "nothing reclaimed".
func (m *RegistryPruneMetrics) BytesReclaimed(bytes int64) {
	if bytes < 0 {
		bytes = 0
	}
	m.reclaimed.WithLabelValues().Add(float64(bytes))
}

// RepoSkipped records a repo skipped by the per-repo tripwire. This is the
// counter that would have surfaced the 2026-08-02 incident — one repo's 274
// candidates against a threshold of 100 — within a tick instead of a week.
func (m *RegistryPruneMetrics) RepoSkipped(repo string, candidates int) {
	m.skipped.WithLabelValues(repo).Inc()
	if candidates < 0 {
		candidates = 0
	}
	m.candidates.WithLabelValues(repo).Set(float64(candidates))
}
