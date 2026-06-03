package api

import (
	"bytes"
	"encoding/json"
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
)

func newSrvFull(t *testing.T) (*httptest.Server, string, *fake.Fake) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "secrets:*", "hosts:read"}}}
	tmpls := []config.Template{
		{Meta: render.Meta{
			ID:         "app",
			Parameters: render.Parameters{Required: []string{"slug", "image"}},
			Secrets:    render.Secrets{PerInstance: []string{"auth_secret"}},
		}, Body: `apiVersion: v1
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
`},
	}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc := instance.NewService(f, hosts, tmpls)
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil))
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
