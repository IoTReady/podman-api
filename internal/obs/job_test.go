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

	// After the sequence above:
	//   enqueued =  1 (enqueued) - 1 (started decrements) = 0
	//   jobs_total = 1 (started) + 1 (succeeded) = 2
	//   duration   = 1 observation
	//   in_flight  = 1 (started) - 1 (finished) = 0
	//   rollbacks = 1
	//   child_failures = 1
	tests := []struct {
		name  string
		check func(t *testing.T)
	}{
		{"enqueued", func(t *testing.T) {
			for _, mf := range mfs {
				if mf.GetName() == "podman_api_jobs_enqueued" {
					if got := sumGauge(mf.Metric); got != 0 {
						t.Fatalf("enqueued = %v, want 0 (after start finishes)", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_enqueued")
		}},
		{"jobs_total", func(t *testing.T) {
			for _, mf := range mfs {
				if mf.GetName() == "podman_api_jobs_total" {
					if got := sumCounter(mf.Metric); got != 2 {
						t.Fatalf("jobs_total = %v, want 2 (started+succeeded)", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_total")
		}},
		{"duration", func(t *testing.T) {
			for _, mf := range mfs {
				if mf.GetName() == "podman_api_job_duration_seconds" {
					if got := sumHistogram(mf.Metric); got != 1 {
						t.Fatalf("duration observations = %v, want 1", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_job_duration_seconds")
		}},
		{"in_flight", func(t *testing.T) {
			for _, mf := range mfs {
				if mf.GetName() == "podman_api_jobs_in_flight" {
					if got := sumGauge(mf.Metric); got != 0 {
						t.Fatalf("in_flight = %v, want 0 (after finished)", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_in_flight")
		}},
		{"rollbacks", func(t *testing.T) {
			for _, mf := range mfs {
				if mf.GetName() == "podman_api_jobs_rollbacks_total" {
					if got := sumCounter(mf.Metric); got != 1 {
						t.Fatalf("rollbacks = %v, want 1", got)
					}
					return
				}
			}
			t.Fatal("missing podman_api_jobs_rollbacks_total")
		}},
		{"child_failures", func(t *testing.T) {
			for _, mf := range mfs {
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