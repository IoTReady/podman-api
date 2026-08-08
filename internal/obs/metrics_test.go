package obs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetrics_RecordsRequest(t *testing.T) {
	// A private registry, not prometheus.DefaultRegisterer: constructing
	// Metrics against the global default would panic on a second call in
	// the same process (e.g. under `go test -count>1`), which is exactly
	// what regressed here (#230).
	reg := prometheus.NewRegistry()
	m := New(reg, reg)
	mw := m.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/something", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	// Read /metrics output via m.Handler() itself now that it serves the
	// registry it was actually constructed with, not the package-global
	// default.
	mreq := httptest.NewRequest("GET", "/metrics", nil)
	mrr := httptest.NewRecorder()
	m.Handler().ServeHTTP(mrr, mreq)
	body := mrr.Body.String()
	require.True(t, strings.Contains(body, "podman_api_requests_total"), "metrics body should contain requests counter")
	require.True(t, strings.Contains(body, "podman_api_request_duration_seconds"), "metrics body should contain duration histogram")
}

// TestNew_MultipleConstructionsDoNotPanic is the direct regression test for
// #230: obs.New used to register its collectors on the package-global
// default Prometheus registry, so a second construction anywhere in the
// same process panicked with "duplicate metrics collector registration
// attempted" — making `go test -count>1` (and any other process that builds
// more than one server, e.g. multiple subtests) unusable for this package.
func TestNew_MultipleConstructionsDoNotPanic(t *testing.T) {
	// Each construction uses its own registry, mirroring how a real test
	// binary run with -count=3 re-executes this test function fresh each
	// time: nothing here shares state across iterations, the way the old
	// New() shared the package-global default registry across every call in
	// the process regardless of which test constructed it.
	require.NotPanics(t, func() {
		reg1 := prometheus.NewRegistry()
		reg2 := prometheus.NewRegistry()
		reg3 := prometheus.NewRegistry()
		New(reg1, reg1)
		New(reg2, reg2)
		New(reg3, reg3)
	})
}
