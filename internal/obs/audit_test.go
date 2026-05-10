package obs

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAudit_LogsStateChangingRequests(t *testing.T) {
	var buf bytes.Buffer
	mw := NewAuditMiddleware(&buf)

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, m := range []string{"POST", "PUT", "DELETE"} {
		req := httptest.NewRequest(m, "/x", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}

	out := buf.String()
	require.NotEmpty(t, out, "expected audit lines for state-changing requests")
	for _, m := range []string{`"method":"POST"`, `"method":"PUT"`, `"method":"DELETE"`} {
		assert.Contains(t, out, m)
	}
}

func TestAudit_SkipsReadOnly(t *testing.T) {
	var buf bytes.Buffer
	mw := NewAuditMiddleware(&buf)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	req := httptest.NewRequest("GET", "/x", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.True(t, strings.TrimSpace(buf.String()) == "")
}

func TestAudit_IncludesPathFields(t *testing.T) {
	var buf bytes.Buffer
	mw := NewAuditMiddleware(&buf)

	mux := http.NewServeMux()
	mux.Handle("DELETE /hosts/{host}/instances/{template}/{slug}", mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, _ := http.NewRequest("DELETE", srv.URL+"/hosts/h1/instances/postgres/demo", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	out := buf.String()
	assert.Contains(t, out, `"host":"h1"`)
	assert.Contains(t, out, `"template":"postgres"`)
	assert.Contains(t, out, `"slug":"demo"`)
	assert.Contains(t, out, `"status":204`)
	assert.NotContains(t, out, `"error"`)
}

func TestAudit_ErrorField(t *testing.T) {
	var buf bytes.Buffer
	mw := NewAuditMiddleware(&buf)

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	req := httptest.NewRequest("DELETE", "/x", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	out := buf.String()
	assert.Contains(t, out, `"error":"http 404"`)
}
