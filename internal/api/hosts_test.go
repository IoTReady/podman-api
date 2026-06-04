package api

import (
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
)

func authedReq(t *testing.T, srv *httptest.Server, tok, method, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestListHosts(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:read"}}}
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/x", Labels: map[string]string{"env": "dev"}},
	}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil))
	defer srv.Close()

	resp := authedReq(t, srv, tok, "GET", "/hosts")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 1)
	assert.Equal(t, "h1", got[0]["id"])
	assert.Equal(t, "unix", got[0]["addr"])
}

func TestHostHealthz(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil))
	defer srv.Close()

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/healthz")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/nope/healthz")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
