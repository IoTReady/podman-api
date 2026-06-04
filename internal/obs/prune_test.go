package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestPruneMetricsRecord(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPruneMetrics(reg)
	m.RunDone("h1", "succeeded")
	m.Reclaimed("h1", "dangling", 2048)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sawRuns, sawReclaimed bool
	for _, mf := range mfs {
		switch mf.GetName() {
		case "podman_api_prune_runs_total":
			sawRuns = true
			if v := sumCounter(mf.Metric); v != 1 {
				t.Fatalf("runs = %v, want 1", v)
			}
		case "podman_api_prune_reclaimed_bytes_total":
			sawReclaimed = true
			if v := sumCounter(mf.Metric); v != 2048 {
				t.Fatalf("reclaimed = %v, want 2048", v)
			}
		}
	}
	if !sawRuns || !sawReclaimed {
		t.Fatalf("missing metrics: runs=%v reclaimed=%v", sawRuns, sawReclaimed)
	}
}

func TestPruneMetricsReclaimedClampsNegative(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPruneMetrics(reg)
	m.Reclaimed("h1", "dangling", -100) // negative must clamp to 0, not panic/decrement

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "podman_api_prune_reclaimed_bytes_total" {
			if v := sumCounter(mf.Metric); v != 0 {
				t.Fatalf("reclaimed after negative = %v, want 0", v)
			}
		}
	}
}

func sumCounter(ms []*dto.Metric) float64 {
	var v float64
	for _, m := range ms {
		v += m.GetCounter().GetValue()
	}
	return v
}
