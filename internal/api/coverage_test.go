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
	"github.com/iotready/podman-api/internal/store"
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
		{store.ErrSecretsUndecryptable, "secrets_undecryptable", http.StatusUnprocessableEntity},
		{podman.ErrHostVersionUnsupported, "host_version_unsupported", http.StatusUnprocessableEntity},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		WriteError(rr, c.err)
		assert.Equal(t, c.stat, rr.Code, c.code)
		assert.Contains(t, rr.Body.String(), `"code":"`+c.code+`"`)
	}
}

// --- GET /hosts/{host}: counts + load ----------------------------------------

func TestGetHost_IncludesCountsAndLoad(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	cpu := 33.0
	la := [3]float64{0.5, 0.7, 0.9}
	f.HostInfoVal = podman.HostInfo{
		CPUs: 8, MemTotal: 1000, MemFree: 250, MemUsedPct: 75,
		CPUPct: &cpu, LoadAvg: &la,
		Disk: podman.DiskUsage{Total: 100, Used: 60, Free: 40, Reclaimable: 5},
	}
	resp := authedReq(t, srv, tok, "GET", "/hosts/h1")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	load, ok := body["load"].(map[string]any)
	require.True(t, ok, "load object present")
	assert.Equal(t, float64(8), load["cpus"])
	assert.Equal(t, 75.0, load["mem_used_pct"])
	assert.Equal(t, 33.0, load["cpu_pct"])
	assert.Equal(t, []any{0.5, 0.7, 0.9}, load["loadavg"])
	assert.Contains(t, body, "container_count")
}

func TestGetHost_LoadOmitsAbsentMetrics(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.HostInfoVal = podman.HostInfo{CPUs: 2, MemTotal: 10, MemFree: 5, MemUsedPct: 50} // CPUPct + LoadAvg nil
	resp := authedReq(t, srv, tok, "GET", "/hosts/h1")
	defer resp.Body.Close()
	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	load := body["load"].(map[string]any)
	_, hasCPU := load["cpu_pct"]
	_, hasLA := load["loadavg"]
	assert.False(t, hasCPU, "cpu_pct omitted when nil")
	assert.False(t, hasLA, "loadavg omitted when nil")
}

// --- GET /hosts: list includes load ------------------------------------------

func TestListHosts_ReturnsAllWithLoad(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.HostInfoVal = podman.HostInfo{CPUs: 8, MemTotal: 1000, MemFree: 100, MemUsedPct: 90}
	resp := authedReq(t, srv, tok, "GET", "/hosts")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.NotEmpty(t, body)
	for _, hh := range body {
		if hh["status"] == "ok" {
			load, ok := hh["load"].(map[string]any)
			require.True(t, ok, "reachable host has load")
			assert.Equal(t, float64(8), load["cpus"])
		}
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
