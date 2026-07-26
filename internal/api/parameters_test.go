package api

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// pro#74: the PATCH route changes a parameter while the instance's sealed
// per-instance secret stays sealed — the request carries no secrets at all.
// This asserts both halves: the new parameter reaches the applied manifest
// (image pulled), AND the sealed secret's value actually survives the re-apply
// unchanged. AllowMissingSecrets means the first assertion alone would pass
// identically even if the secret had been silently dropped, so it is not
// sufficient on its own.
func TestPatchParameters_ReusesSealedSecret(t *testing.T) {
	srv, tok, f := newSrvFull(t)

	create := `{"template":"app","slug":"cfg","parameters":{"slug":"cfg","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/cfg", create)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// f.SecretData is an append/overwrite-only log of every SecretCreate call —
	// SecretRemove (unlike the real backend) never prunes it, so the setup PUT's
	// write above would still be sitting there even if the PATCH dropped the
	// secret entirely. Clear it immediately before the PATCH so the assertion
	// below can only pass if UpdateInstanceParameters actually re-creates the
	// secret from the stored spec.
	secretName := "app-cfg-auth_secret"
	delete(f.SecretData["h1"], secretName)

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

	// The sealed secret must survive the re-apply with its original value: the
	// PATCH request carried no secrets at all, so this is only possible if
	// UpdateInstanceParameters reused spec.Secrets from the store.
	raw, ok := f.SecretData["h1"][secretName]
	require.True(t, ok, "sealed secret %q must be re-created on the host by the PATCH", secretName)
	assert.Contains(t, string(raw), base64.StdEncoding.EncodeToString([]byte("s")),
		"sealed secret value must be unchanged")
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

// pro#74 fix 1 (API layer): slug drives metadata.name/secretKeyRef/claimName in
// the rendered manifest, so accepting it here would replay the manifest
// against a different pod than the one this route's path (and lock) name.
// Rejected before it ever reaches the service layer.
func TestPatchParameters_RejectsSlug(t *testing.T) {
	srv, tok, f := newSrvFull(t)

	create := `{"template":"app","slug":"cfg","parameters":{"slug":"cfg","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/cfg", create)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/parameters",
		`{"parameters":{"slug":"other","image":"i:2"}}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body := bodyString(t, resp)
	assert.Contains(t, body, `"code":"invalid_body"`)
	assert.Contains(t, body, "slug")

	// Nothing should have been applied: no pull for the rejected request's image.
	for _, p := range f.PullCalls {
		assert.NotEqual(t, "i:2", p.Image, "a rejected slug change must not reach the applied manifest")
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
