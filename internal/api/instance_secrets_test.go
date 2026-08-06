package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// newSrvSecrets is newSrvFull plus the backing store, so a test can grow the
// template's declared per-instance secrets after the instance was deployed —
// the situation #207 is about (a template gains secrets an existing sealed
// instance has never been given).
func newSrvSecrets(t *testing.T) (*httptest.Server, string, *fake.Fake, *store.Memory) {
	t.Helper()
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "secrets:*", "hosts:read", "templates:*"}}}

	tmpl := store.Template{
		Meta: render.Meta{
			ID: "app",
			Parameters: []render.ParamDef{
				{Name: "slug", Type: "string", Required: true},
				{Name: "image", Type: "string", Required: true},
			},
			Secrets: render.Secrets{PerInstance: []string{"auth_secret"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: app-{{.slug}}
  labels:
    podman-api/template: app
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
`,
		Origin: "seed",
	}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(context.Background(), tmpl))
	svc := instance.NewService(f, hosts)
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil, "", nil))
	t.Cleanup(srv.Close)
	return srv, tok, f, mem
}

// declareSecrets rewrites the "app" template's per-instance secret list, standing
// in for the template edit that introduces a new secret to an already-deployed
// instance.
func declareSecrets(t *testing.T, mem *store.Memory, names ...string) {
	t.Helper()
	tmpl, err := mem.GetTemplate(context.Background(), "app")
	require.NoError(t, err)
	tmpl.Meta.Secrets.PerInstance = names
	require.NoError(t, mem.PutTemplate(context.Background(), tmpl))
}

// deployWithSecret creates app/cfg holding one sealed secret, then clears the
// fake's secret log so later assertions can only see writes the route under test
// performed. (SecretRemove never prunes f.SecretData, so without the clear the
// setup write would still be there even if the PATCH dropped the secret.)
func deployWithSecret(t *testing.T, srv *httptest.Server, tok string, f *fake.Fake) {
	t.Helper()
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/cfg",
		`{"template":"app","slug":"cfg","parameters":{"slug":"cfg","image":"i:1"},"secrets":{"auth_secret":"s"}}`)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	delete(f.SecretData["h1"], "app-cfg-auth_secret")
}

// #207: the whole point of the route — add a brand-new secret to an instance
// whose existing secret's plaintext the operator cannot read back. The new
// secret must reach the host AND the untouched sealed one must survive with its
// original value, which is only possible if the route merged into the stored
// spec rather than replacing it.
func TestPatchSecrets_AddsNewSecretKeepingSealedOnes(t *testing.T) {
	srv, tok, f, mem := newSrvSecrets(t)
	deployWithSecret(t, srv, tok, f)
	declareSecrets(t, mem, "auth_secret", "vpn_password")

	resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/secrets",
		`{"secrets":{"vpn_password":"p"}}`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, bodyString(t, resp))

	added, ok := f.SecretData["h1"]["app-cfg-vpn_password"]
	require.True(t, ok, "the new secret must be created on the host, got %v", f.SecretData["h1"])
	assert.Contains(t, string(added), base64.StdEncoding.EncodeToString([]byte("p")))

	kept, ok := f.SecretData["h1"]["app-cfg-auth_secret"]
	require.True(t, ok, "the pre-existing sealed secret must be re-created by the re-apply")
	assert.Contains(t, string(kept), base64.StdEncoding.EncodeToString([]byte("s")),
		"the sealed secret's value must be unchanged — the request never carried it")
}

// A name already sealed is overwritten (rotation), not merged into or ignored.
func TestPatchSecrets_OverwritesExistingSecret(t *testing.T) {
	srv, tok, f, _ := newSrvSecrets(t)
	deployWithSecret(t, srv, tok, f)

	resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/secrets",
		`{"secrets":{"auth_secret":"rotated"}}`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, bodyString(t, resp))

	raw := f.SecretData["h1"]["app-cfg-auth_secret"]
	assert.Contains(t, string(raw), base64.StdEncoding.EncodeToString([]byte("rotated")))
}

// Secrets are request-only: no route ever hands plaintext back, and the 200 body
// here is the same Observed as everywhere else.
func TestPatchSecrets_ResponseNeverEchoesPlaintext(t *testing.T) {
	srv, tok, f, mem := newSrvSecrets(t)
	deployWithSecret(t, srv, tok, f)
	declareSecrets(t, mem, "auth_secret", "vpn_password")

	resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/secrets",
		`{"secrets":{"vpn_password":"leakcanary"}}`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, bodyString(t, resp), "leakcanary")
}

// An empty or absent map would restart the instance for no change; reject it
// before the host is touched, exactly as .../parameters does.
func TestPatchSecrets_RejectsEmptyAndAbsent(t *testing.T) {
	srv, tok, f, _ := newSrvSecrets(t)
	deployWithSecret(t, srv, tok, f)

	for _, body := range []string{`{}`, `{"secrets":{}}`} {
		resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/secrets", body)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body %s must be rejected", body)
		assert.Contains(t, bodyString(t, resp), `"code":"invalid_body"`)
		resp.Body.Close()
	}
}

// A blank value would silently wipe a sealed secret whose plaintext nobody can
// recover, and this route cannot delete a secret anyway — so an empty value is a
// mistake, not an instruction. Reject it before the host is touched.
func TestPatchSecrets_RejectsBlankValue(t *testing.T) {
	srv, tok, f, _ := newSrvSecrets(t)
	deployWithSecret(t, srv, tok, f)

	resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/secrets",
		`{"secrets":{"auth_secret":""}}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"invalid_body"`)

	_, wrote := f.SecretData["h1"]["app-cfg-auth_secret"]
	assert.False(t, wrote, "a rejected request must not re-apply the instance")
}

// A name the template does not declare is rejected by render validation (400
// invalid_parameters) and leaves the stored spec untouched.
func TestPatchSecrets_RejectsUndeclaredName(t *testing.T) {
	srv, tok, f, mem := newSrvSecrets(t)
	deployWithSecret(t, srv, tok, f)

	resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/secrets",
		`{"secrets":{"bogus":"v"}}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), "unknown secret")

	spec, err := mem.GetSpec(context.Background(), "h1", "app", "cfg")
	require.NoError(t, err)
	assert.NotContains(t, spec.Secrets, "bogus", "a rejected secret must not be persisted")
}

func TestPatchSecrets_UnknownInstanceNotFound(t *testing.T) {
	srv, tok, _, _ := newSrvSecrets(t)

	resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/ghost/secrets",
		`{"secrets":{"auth_secret":"s"}}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// The route is a write operation and must be guarded as one.
func TestPatchSecrets_RequiresWriteScope(t *testing.T) {
	tok := "ro"
	hash, _ := config.HashToken(tok)
	srv := newSrvWithKeys(t, []config.APIKey{
		{ID: "ro", SecretHash: hash, Scopes: []string{"instances:read"}},
	})

	resp := postJSON(t, srv, tok, "PATCH", "/hosts/h1/instances/app/cfg/secrets",
		`{"secrets":{"auth_secret":"s"}}`)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}
