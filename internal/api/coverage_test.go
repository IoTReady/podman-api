package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
)

// postJSON issues an authed request carrying a JSON body and returns the response.
func postJSON(t *testing.T, srv *httptest.Server, tok, method, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// --- GET /hosts/{host} ------------------------------------------------------

func TestGetHost_FoundAndUnknown(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "h1", got["id"])
	// fake.Ping never errors, so the host is reachable and reports a version.
	assert.Equal(t, "ok", got["status"])
	assert.Equal(t, "fake-1.0", got["podman_version"])

	resp = authedReq(t, srv, tok, "GET", "/hosts/nope")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"unknown_host"`)
}

// --- GET /hosts/{host}/ports-in-use -----------------------------------------

func TestPortsInUse_OKAndUnknownHost(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/ports-in-use")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// No pods bound any ports, so the result is an empty JSON array (not null).
	var ports []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ports))
	assert.Empty(t, ports)

	resp = authedReq(t, srv, tok, "GET", "/hosts/nope/ports-in-use")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"unknown_host"`)
}

// --- GET /templates/{id} ----------------------------------------------------

func TestGetTemplate_FoundUnknownAndInvalid(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	resp := authedReq(t, srv, tok, "GET", "/templates/app")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "app", got["id"])

	resp = authedReq(t, srv, tok, "GET", "/templates/missing")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"unknown_template"`)

	// Uppercase fails the DNS-label allowlist before any lookup.
	resp = authedReq(t, srv, tok, "GET", "/templates/BAD")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"invalid_parameters"`)
}

// --- GET /templates/{id}/render ---------------------------------------------

func TestRenderTemplate_OKUnknownAndInvalid(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	resp := authedReq(t, srv, tok, "GET", "/templates/app/render?slug=hello&image=i:1")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/yaml", resp.Header.Get("Content-Type"))
	out := bodyString(t, resp)
	assert.Contains(t, out, "name: app-hello")
	assert.Contains(t, out, "image: i:1")

	resp = authedReq(t, srv, tok, "GET", "/templates/missing/render")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/templates/BAD/render")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- listInstances: ListAllInstances path -----------------------------------

func TestListInstances_NoTemplateFilter(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	// No ?template= → ListAllInstances across every known template. Empty host
	// yields an empty array.
	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/instances")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Empty(t, got)

	// Unknown host is a 404 through the same branch.
	resp = authedReq(t, srv, tok, "GET", "/hosts/nope/instances")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Invalid template filter is rejected at the edge.
	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances?template=BAD")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- malformed JSON bodies --------------------------------------------------

func TestHandlers_RejectMalformedJSON(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"create", "POST", "/hosts/h1/instances"},
		{"apply", "PUT", "/hosts/h1/instances/app/hello"},
		{"upgrade", "POST", "/hosts/h1/instances/app/hello/upgrade"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := postJSON(t, srv, tok, c.method, c.path, "{not json")
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, bodyString(t, resp), `"code":"invalid_body"`)
		})
	}
}

// --- applyInstance: URL/body mismatch ---------------------------------------

func TestApplyInstance_URLBodyMismatch(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	// Body template disagrees with the URL template.
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/hello",
		`{"template":"other","slug":"hello"}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), "template in URL does not match body")

	// Body slug disagrees with the URL slug.
	resp = postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/hello",
		`{"template":"app","slug":"other"}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), "slug in URL does not match body")
}

// --- error helpers ----------------------------------------------------------

func TestWriteErrorWithDetails(t *testing.T) {
	rr := httptest.NewRecorder()
	WriteErrorWithDetails(rr, instance.ErrUnknownHost, map[string]any{"host": "h9"})
	require.Equal(t, http.StatusNotFound, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, `"code":"unknown_host"`)
	assert.Contains(t, body, `"host":"h9"`)
}

// TestClassify_RemainingSentinels exercises the classify branches not covered
// by errors_test.go's table.
func TestClassify_RemainingSentinels(t *testing.T) {
	cases := []struct {
		err  error
		code string
		stat int
	}{
		{instance.ErrImagePull, "upstream_error", http.StatusBadGateway},
		{instance.ErrHostDraining, "host_draining", http.StatusLocked},
		{podman.ErrNotFound, "instance_not_found", http.StatusNotFound},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		WriteError(rr, c.err)
		assert.Equal(t, c.stat, rr.Code, c.code)
		assert.Contains(t, rr.Body.String(), `"code":"`+c.code+`"`)
	}
}

// bodyString drains and returns the response body as a string.
func bodyString(t *testing.T, resp *http.Response) string {
	t.Helper()
	var buf bytes.Buffer
	_, err := buf.ReadFrom(resp.Body)
	require.NoError(t, err)
	return buf.String()
}
