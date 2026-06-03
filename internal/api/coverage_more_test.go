package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- lifecycle handlers: service-error branch -------------------------------

// Acting on an instance that does not exist drives each lifecycle handler's
// WriteError branch (the fake returns not-found, which the service maps to
// instance_not_found).
func TestLifecycle_GhostInstanceIsNotFound(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	for _, action := range []string{"start", "stop", "restart"} {
		t.Run(action, func(t *testing.T) {
			resp := authedReq(t, srv, tok, "POST", "/hosts/h1/instances/app/ghost/"+action)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
			assert.Contains(t, bodyString(t, resp), `"code":"instance_not_found"`)
		})
	}
}

// --- instanceVolumes --------------------------------------------------------

func TestInstanceVolumes_EmptyAndUnknownHost(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	// The "app" template declares no volumes, so a valid instance path yields
	// an empty array.
	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/any/volumes")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var vols []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&vols))
	assert.Empty(t, vols)

	// Unknown host fails the lookup → WriteError branch.
	resp = authedReq(t, srv, tok, "GET", "/hosts/nope/instances/app/any/volumes")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"unknown_host"`)
}

// --- deleteVolume -----------------------------------------------------------

func TestDeleteVolume_IdempotentAndUnknownHost(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	// Removing an absent volume is idempotent: 204, not 404.
	resp := authedReq(t, srv, tok, "DELETE", "/hosts/h1/volumes/ghost")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Unknown host is the error branch.
	resp = authedReq(t, srv, tok, "DELETE", "/hosts/nope/volumes/ghost")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"unknown_host"`)
}

// --- putSecret: body validation branches ------------------------------------

func TestPutSecret_BodyValidation(t *testing.T) {
	srv, tok := newSrvWithSecrets(t)

	// Malformed JSON.
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/secrets/s1", "{nope")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"invalid_body"`)

	// Well-formed JSON but empty value.
	resp = postJSON(t, srv, tok, "PUT", "/hosts/h1/secrets/s1", `{"value":""}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), "value is required")
}

// --- listSecrets/deleteSecret: unknown host ---------------------------------

func TestSecrets_UnknownHost(t *testing.T) {
	srv, tok := newSrvWithSecrets(t)

	resp := authedReq(t, srv, tok, "GET", "/hosts/nope/secrets")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"unknown_host"`)

	resp = authedReq(t, srv, tok, "DELETE", "/hosts/nope/secrets/s1")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"unknown_host"`)
}

// --- bulk: unknown host drives bulkClassify's unknown_host branch -----------

func TestBulk_UnknownHostPerOp(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	resp := postJSON(t, srv, tok, "POST", "/hosts/nope/bulk",
		`{"ops":[{"action":"stop","template":"app","slug":"aa"}]}`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	results := got["results"].([]any)
	require.Len(t, results, 1)
	assert.Equal(t, "unknown_host", results[0].(map[string]any)["code"])
	assert.Equal(t, float64(http.StatusNotFound), results[0].(map[string]any)["status"])
}
