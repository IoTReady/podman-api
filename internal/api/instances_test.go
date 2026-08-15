package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// newTestServer builds the shared "app" fixture (parameters slug+image, a
// single per-instance secret auth_secret) behind an httptest.Server guarded
// by the given API keys, and returns the fake podman backend alongside it so
// callers can assert on what got applied.
func newTestServer(t *testing.T, keys []config.APIKey) (*httptest.Server, *fake.Fake) {
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
	return srv, f
}

func newSrvFull(t *testing.T) (*httptest.Server, string, *fake.Fake) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "secrets:*", "hosts:read", "templates:*"}}}
	srv, f := newTestServer(t, keys)
	return srv, tok, f
}

// newSrvWithKeys builds the same fixture as newSrvFull but with a caller-
// supplied key set, for tests that need a restricted-scope token (e.g.
// asserting a write route 403s a read-only key).
func newSrvWithKeys(t *testing.T, keys []config.APIKey) *httptest.Server {
	srv, _ := newTestServer(t, keys)
	return srv
}

func TestApplyAndGetInstance(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/hello")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "app", got["template"])
	assert.Equal(t, "hello", got["slug"])
}

// TestApplyInstance_UnknownField asserts PUT .../instances/{template}/{slug}
// rejects an unknown field with a 400 rather than silently ignoring it
// (#254).
func TestApplyInstance_UnknownField(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	body := `{"template":"app","slug":"hello","paramaters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// #201 (API edge): the path/body slug reconciliation above never looked at
// parameters["slug"], which is what actually renders metadata.name. A
// disagreeing parameters.slug is rejected with the same 400 invalid_body shape
// PATCH .../parameters uses, on both deploy routes, before the host is touched.
// (applyLocked pins the canonical slug regardless — this is the friendly half.)
func TestDeployRoutes_RejectMismatchedSlugParameter(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
	}{
		{
			name: "put", method: "PUT", path: "/hosts/h1/instances/app/hello",
			body: `{"template":"app","slug":"hello","parameters":{"slug":"other","image":"i:1"},"secrets":{"auth_secret":"s"}}`,
		},
		{
			name: "post", method: "POST", path: "/hosts/h1/instances",
			body: `{"template":"app","slug":"hello","parameters":{"slug":"other","image":"i:1"},"secrets":{"auth_secret":"s"}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, tok, f := newSrvFull(t)
			resp := postJSON(t, srv, tok, tc.method, tc.path, tc.body)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, bodyString(t, resp), "slug in parameters does not match")
			assert.Empty(t, f.PlayCalls, "a rejected body must not reach the host")
		})
	}
}

// A parameters.slug that agrees with the path is the normal case (the bundled
// templates declare slug) and must still be accepted.
func TestDeployRoutes_AcceptMatchingSlugParameter(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/hello", body)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// The GET body carries the stored render parameters so a client can read a
// shared value back before a read-modify-write (#200). The per-instance secret
// must not appear anywhere in that body.
func TestGetInstance_ReturnsStoredParameters(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s3cr3t"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/hello")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	params, ok := got["parameters"].(map[string]any)
	require.True(t, ok, "parameters must be present: %s", raw)
	assert.Equal(t, "i:1", params["image"])
	assert.Equal(t, "hello", params["slug"])
	assert.NotContains(t, string(raw), "s3cr3t", "no secret material in the GET body")
}

// A pod with no stored spec omits the field entirely rather than emitting null.
func TestGetInstance_NoStoredSpec_OmitsParameters(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.AddPod("h1", podman.Pod{
		Name:   "app-orphan",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "app", "podman-api/slug": "orphan"},
		Containers: []podman.Container{
			{Name: "app", Status: "Running"},
		},
	})

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/orphan")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	_, present := got["parameters"]
	assert.False(t, present, "parameters is omitted, not null, with no stored spec")
}

func TestCreateConflict(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestDeleteInstance(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest("DELETE", srv.URL+"/hosts/h1/instances/app/hello", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/hello")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCreateInstanceRejectsBadDomain(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"ab","parameters":{"slug":"ab","image":"i:1"},"secrets":{"auth_secret":"s"},"domains":["NOT A DOMAIN"]}`
	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "invalid_domains", got["code"])
}

func TestApplyInstanceRejectsBadDomain(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"ab","parameters":{"slug":"ab","image":"i:1"},"secrets":{"auth_secret":"s"},"domains":["NOT A DOMAIN"]}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/ab", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "invalid_domains", got["code"])
}

func TestRenameInstance(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body = `{"new_slug":"world"}`
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/hello/rename", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "world", got["slug"])

	// Old slug should be gone; new slug should be found.
	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/hello")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/world")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRenameInstanceRejectsSameSlug(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body = `{"new_slug":"hello"}`
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/hello/rename", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "invalid_request", got["code"])
}

func TestRenameInstanceRejectsExistingSlug(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body = `{"template":"app","slug":"world","parameters":{"slug":"world","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ = http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/world", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body = `{"new_slug":"world"}`
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/hello/rename", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "instance_already_exists", got["code"])
}

func TestRenameInstanceRejectsInvalidNewSlug(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"hello","parameters":{"slug":"hello","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/hello", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, bad := range []string{"", "UPPERCASE", "has space", "-leading-hyphen", "trailing-hyphen-", "x"} {
		req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/hello/rename", bytes.NewBufferString(`{"new_slug":"`+bad+`"}`))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		if bad == "" {
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "empty new_slug should be rejected")
		} else {
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "new_slug %q should be rejected", bad)
		}
	}
}
