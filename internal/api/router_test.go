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
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func newServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	tok := "test-tok"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:read", "instances:*", "secrets:*"}}}

	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())

	r := NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestRouter_HealthzNoAuth(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRouter_MetricsNotMountedWhenNil(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	resp.Body.Close()
	// /metrics must 404 on the main listener unless an operator opted in by
	// passing a non-nil metricsHandler (and binding -metrics-addr in main).
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRouter_HostsRequiresAuth(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/hosts")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestRouter_AuditCapturesKeyID(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "audited-key", SecretHash: hash, Scopes: []string{"secrets:write"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())

	var buf bytes.Buffer
	auditMW := obs.NewAuditMiddleware(&buf)
	r := NewRouter(svc, nil, auth.NewKeyStore(keys), auditMW, nil, nil, "")
	srv := httptest.NewServer(r)
	defer srv.Close()

	// DELETE /hosts/{host}/secrets/{name} is idempotent — returns 204 regardless.
	// It triggers the audit middleware because it is a state-changing method.
	req, _ := http.NewRequest("DELETE", srv.URL+"/hosts/h1/secrets/none", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	out := buf.String()
	require.Contains(t, out, `"key_id":"audited-key"`, "audit log must include the matched key id; got: %s", out)
}
