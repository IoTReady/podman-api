package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bulkPost(t *testing.T, srv, tok, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest("POST", srv+"/hosts/h1/bulk", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	return resp.StatusCode, got
}

func TestBulk_HappyPath_PerOpResults(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	// Create three instances first.
	for _, slug := range []string{"aa", "bb", "cc"} {
		body := fmt.Sprintf(`{"template":"app","slug":"%s","parameters":{"slug":"%s","image":"i:1"},"secrets":{"auth_secret":"s"}}`, slug, slug)
		req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
	}

	bulkBody := `{"ops":[
		{"action":"stop","template":"app","slug":"aa"},
		{"action":"stop","template":"app","slug":"bb"},
		{"action":"restart","template":"app","slug":"cc"}
	]}`
	status, got := bulkPost(t, srv.URL, tok, bulkBody)
	require.Equal(t, http.StatusOK, status)

	results := got["results"].([]any)
	require.Len(t, results, 3)
	for i, r := range results {
		m := r.(map[string]any)
		assert.Equal(t, float64(i), m["index"], "result index must mirror input position")
		assert.Equal(t, float64(204), m["status"], "happy-path op must report 204")
	}
}

func TestBulk_PartialFailure_DoesNotAbortBatch(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	// Create one valid instance; a second op references a non-existent slug.
	body := `{"template":"app","slug":"alive","parameters":{"slug":"alive","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	bulkBody := `{"ops":[
		{"action":"restart","template":"app","slug":"alive"},
		{"action":"restart","template":"app","slug":"never-existed"},
		{"action":"stop","template":"app","slug":"alive"}
	]}`
	status, got := bulkPost(t, srv.URL, tok, bulkBody)
	require.Equal(t, http.StatusOK, status, "batch returns 200 even if individual ops fail")

	results := got["results"].([]any)
	require.Len(t, results, 3)
	assert.Equal(t, float64(204), results[0].(map[string]any)["status"])
	assert.Equal(t, float64(404), results[1].(map[string]any)["status"], "missing instance must return 404, not abort the batch")
	assert.Equal(t, "instance_not_found", results[1].(map[string]any)["code"])
	assert.Equal(t, float64(204), results[2].(map[string]any)["status"], "third op must still run after the failure")
}

func TestBulk_RejectsEmpty(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	status, got := bulkPost(t, srv.URL, tok, `{"ops":[]}`)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "invalid_body", got["code"])
}

func TestBulk_RejectsOversize(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	var ops []string
	for i := 0; i < 101; i++ {
		ops = append(ops, fmt.Sprintf(`{"action":"stop","template":"app","slug":"s%02d"}`, i))
	}
	status, got := bulkPost(t, srv.URL, tok, `{"ops":[`+strings.Join(ops, ",")+`]}`)
	assert.Equal(t, http.StatusRequestEntityTooLarge, status)
	assert.Equal(t, "too_many_ops", got["code"])
}

func TestBulk_ValidatesEachOpIndependently(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	bulkBody := `{"ops":[
		{"action":"stop","template":"app","slug":"BAD"},
		{"action":"unknown","template":"app","slug":"ok"},
		{"action":"stop","template":"app","slug":"missing"}
	]}`
	status, got := bulkPost(t, srv.URL, tok, bulkBody)
	require.Equal(t, http.StatusOK, status)
	results := got["results"].([]any)
	assert.Equal(t, "invalid_parameters", results[0].(map[string]any)["code"])
	assert.Equal(t, "invalid_action", results[1].(map[string]any)["code"])
	assert.Equal(t, "instance_not_found", results[2].(map[string]any)["code"])
}

func TestListInstances_NoTemplateFilterListsAll(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	for _, slug := range []string{"a1", "b2"} {
		body := fmt.Sprintf(`{"template":"app","slug":"%s","parameters":{"slug":"%s","image":"i:1"},"secrets":{"auth_secret":"s"}}`, slug, slug)
		req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
	}

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/instances")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Len(t, out, 2, "GET /instances without ?template returns all managed pods")
}
