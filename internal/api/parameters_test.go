package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// pro#74: the PATCH route changes a parameter while the instance's sealed
// per-instance secret stays sealed — the request carries no secrets at all.
func TestPatchParameters_ReusesSealedSecret(t *testing.T) {
	srv, tok, f := newSrvFull(t)

	create := `{"template":"app","slug":"cfg","parameters":{"slug":"cfg","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/cfg", create)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/parameters",
		`{"parameters":{"image":"i:2"}}`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var pulledNew bool
	for _, p := range f.PullCalls {
		if p.Host == "h1" && p.Image == "i:2" {
			pulledNew = true
		}
	}
	assert.True(t, pulledNew, "the changed parameter must reach the applied manifest, got %+v", f.PullCalls)
}

func TestPatchParameters_RejectsEmptyAndAbsent(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	create := `{"template":"app","slug":"cfg","parameters":{"slug":"cfg","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/cfg", create)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, body := range []string{`{}`, `{"parameters":{}}`} {
		resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/parameters", body)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body %s must be rejected", body)
		assert.Contains(t, bodyString(t, resp), `"code":"invalid_body"`)
		resp.Body.Close()
	}
}

// The route is a write operation and must be guarded as one.
func TestPatchParameters_RequiresWriteScope(t *testing.T) {
	tok := "ro"
	hash, _ := config.HashToken(tok)
	srv := newSrvWithKeys(t, []config.APIKey{
		{ID: "ro", SecretHash: hash, Scopes: []string{"instances:read"}},
	})

	resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/parameters",
		`{"parameters":{"image":"i:2"}}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}
