package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gatherByName(t *testing.T, reg *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*dto.MetricFamily{}
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func TestRegistryPruneMetricsRecordEveryOutcome(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewRegistryPruneMetrics(reg)

	m.RunDone("succeeded")
	m.RunDone("aborted")
	m.ManifestsDeleted("engine", 3)
	m.ManifestsDeleted("engine", 2)
	m.BytesReclaimed(1024)
	m.RepoSkipped("otp", 274)

	got := gatherByName(t, reg)
	for name, want := range map[string]float64{
		"podman_api_registry_prune_runs_total":              2,
		"podman_api_registry_prune_manifests_deleted_total": 5,
		"podman_api_registry_prune_reclaimed_bytes_total":   1024,
		"podman_api_registry_prune_repos_skipped_total":     1,
	} {
		mf, ok := got[name]
		if !ok {
			t.Fatalf("missing metric %s", name)
		}
		if v := sumCounter(mf.Metric); v != want {
			t.Errorf("%s = %v, want %v", name, v, want)
		}
	}
	mf, ok := got["podman_api_registry_prune_skipped_candidates"]
	if !ok {
		t.Fatal("missing podman_api_registry_prune_skipped_candidates")
	}
	if v := sumGauge(mf.Metric); v != 274 {
		t.Errorf("skipped candidates = %v, want 274", v)
	}
}

// A negative value must clamp rather than panic: a counter decrement panics,
// and taking the whole prune job down over a metric would be worse than the
// bad number.
func TestRegistryPruneMetricsClampNegatives(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewRegistryPruneMetrics(reg)
	m.BytesReclaimed(-5)
	m.ManifestsDeleted("engine", -2)

	got := gatherByName(t, reg)
	for _, name := range []string{
		"podman_api_registry_prune_reclaimed_bytes_total",
		"podman_api_registry_prune_manifests_deleted_total",
	} {
		if mf, ok := got[name]; ok {
			if v := sumCounter(mf.Metric); v != 0 {
				t.Errorf("%s after a negative = %v, want 0", name, v)
			}
		}
	}
}
