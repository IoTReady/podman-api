package obs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetrics_RecordsRequest(t *testing.T) {
	m := New()
	mw := m.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/something", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	// Read /metrics output.
	mreq := httptest.NewRequest("GET", "/metrics", nil)
	mrr := httptest.NewRecorder()
	m.Handler().ServeHTTP(mrr, mreq)
	body := mrr.Body.String()
	require.True(t, strings.Contains(body, "podman_api_requests_total"), "metrics body should contain requests counter")
	require.True(t, strings.Contains(body, "podman_api_request_duration_seconds"), "metrics body should contain duration histogram")
}
