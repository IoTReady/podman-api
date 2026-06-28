package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

func newSrvFull(t *testing.T) (*httptest.Server, string, *fake.Fake) {
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
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil, ""))
	t.Cleanup(srv.Close)
	return srv, tok, f
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
