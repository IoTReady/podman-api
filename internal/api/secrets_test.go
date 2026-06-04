package api

import (
	"bytes"
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

func newSrvWithSecrets(t *testing.T) (*httptest.Server, string) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"secrets:read", "secrets:write"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestPutAndDeleteSecret(t *testing.T) {
	srv, tok := newSrvWithSecrets(t)

	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/secrets/s1", bytes.NewBufferString(`{"value":"v1"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/secrets")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest("DELETE", srv.URL+"/hosts/h1/secrets/s1", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}
