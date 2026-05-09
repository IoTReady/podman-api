package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors. New() registers them with the
// default registry; call Handler() to expose /metrics.
type Metrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

func New() *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "podman_api_requests_total",
			Help: "Total HTTP requests by method, route template, and status.",
		}, []string{"method", "route", "status"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "podman_api_request_duration_seconds",
			Help:    "Request duration by method and route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
	prometheus.MustRegister(m.requests, m.latency)
	return m
}

// Handler is the http.Handler for the /metrics endpoint.
func (m *Metrics) Handler() http.Handler { return promhttp.Handler() }
