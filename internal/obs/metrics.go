package obs

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors. New registers them with the
// Registerer it is given; call Handler() to expose /metrics.
type Metrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec

	gatherer prometheus.Gatherer
}

// New builds and registers the request-metrics collectors on reg, and serves
// /metrics from gatherer. Production wiring passes
// prometheus.DefaultRegisterer/prometheus.DefaultGatherer, which are backed
// by the same underlying registry; tests pass a private
// prometheus.NewRegistry() for both, so repeated construction in the same
// process (e.g. under `go test -count>1`) does not panic with "duplicate
// metrics collector registration attempted" against the shared global
// registry. reg and gatherer must refer to the same registry, or Handler
// will serve metrics registered elsewhere.
func New(reg prometheus.Registerer, gatherer prometheus.Gatherer) *Metrics {
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
		gatherer: gatherer,
	}
	reg.MustRegister(m.requests, m.latency)
	return m
}

// Handler is the http.Handler for the /metrics endpoint. It serves from the
// Gatherer passed to New, not the global default, so it stays correct when
// New is called with a non-default Registerer/Gatherer pair (e.g. in tests).
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.gatherer, promhttp.HandlerOpts{})
}

// Middleware returns an HTTP middleware that records request count and duration.
// The route label uses r.Pattern (the registered mux pattern, available in
// Go 1.22+) to avoid label cardinality explosion from path parameters.
// Falls back to "_other" when the pattern is not populated.
func (m *Metrics) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			route := r.Pattern
			if route == "" {
				route = "_other"
			}
			d := time.Since(start).Seconds()
			m.requests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
			m.latency.WithLabelValues(r.Method, route).Observe(d)
		})
	}
}
