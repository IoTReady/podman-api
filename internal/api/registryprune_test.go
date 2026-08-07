package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/registryprune"
	"github.com/iotready/podman-api/internal/store"
	"github.com/stretchr/testify/require"
)

type fakePruner struct {
	job   store.Job
	err   error
	calls int
}

func (f *fakePruner) EnqueueNow(context.Context) (store.Job, error) {
	f.calls++
	return f.job, f.err
}

// newPruneTestServer wires a router with the given pruner (nil = not
// configured) and returns tokens for a write-scoped and a read-only key.
func newPruneTestServer(t *testing.T, p RegistryPruner) (*httptest.Server, string, string) {
	t.Helper()
	wHash, _ := config.HashToken("w")
	rHash, _ := config.HashToken("r")
	keys := []config.APIKey{
		{ID: "kw", SecretHash: wHash, Scopes: []string{"instances:write"}},
		{ID: "kr", SecretHash: rHash, Scopes: []string{"instances:read"}},
	}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts)
	mem := store.NewMemory()
	svc.SetStore(mem)
	var opts []RouterOption
	if p != nil {
		opts = append(opts, WithRegistryPruner(p))
	}
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil, "", nil, opts...))
	t.Cleanup(srv.Close)
	return srv, "w", "r"
}

func postPrune(t *testing.T, srv *httptest.Server, tok string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/registry/prune", nil)
	require.NoError(t, err)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestEnqueueRegistryPrune_Accepted(t *testing.T) {
	p := &fakePruner{job: store.Job{ID: "j1"}}
	srv, wtok, _ := newPruneTestServer(t, p)

	resp := postPrune(t, srv, wtok)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "j1", body["job_id"])
	require.Equal(t, 1, p.calls)
}

// Absent, not disabled: the same shape as the browse routes with no registry
// client. A server that never enabled the feature must not answer 500 or 202.
func TestEnqueueRegistryPrune_NotConfiguredIs404(t *testing.T) {
	srv, wtok, _ := newPruneTestServer(t, nil)
	resp := postPrune(t, srv, wtok)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// The route enqueues a job that deletes manifests. instances:read must not be
// enough, and an unauthenticated call must not reach the pruner at all.
func TestEnqueueRegistryPrune_RequiresWriteScope(t *testing.T) {
	p := &fakePruner{job: store.Job{ID: "j1"}}
	srv, _, rtok := newPruneTestServer(t, p)

	resp := postPrune(t, srv, rtok)
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	anon := postPrune(t, srv, "")
	defer anon.Body.Close()
	require.Equal(t, http.StatusUnauthorized, anon.StatusCode)

	require.Equal(t, 0, p.calls, "an unauthorised caller must never reach the pruner")
}

// 409, not 202-with-the-running-job: "your run started" and "someone else's run
// is still going" are different answers, and collapsing them would let an
// operator believe a fresh run began on their new configuration.
func TestEnqueueRegistryPrune_InFlightIs409(t *testing.T) {
	p := &fakePruner{err: registryprune.ErrRunInFlight}
	srv, wtok, _ := newPruneTestServer(t, p)

	resp := postPrune(t, srv, wtok)
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestEnqueueRegistryPrune_OtherErrorIs500(t *testing.T) {
	p := &fakePruner{err: errors.New("store unavailable")}
	srv, wtok, _ := newPruneTestServer(t, p)

	resp := postPrune(t, srv, wtok)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// GET must not enqueue. A prune is a mutation, and a route that answered a
// browser's GET would be one bookmark away from an unscheduled delete run.
func TestEnqueueRegistryPrune_GetIsNotAllowed(t *testing.T) {
	p := &fakePruner{job: store.Job{ID: "j1"}}
	srv, wtok, _ := newPruneTestServer(t, p)

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/registry/prune", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+wtok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusAccepted, resp.StatusCode)
	require.Equal(t, 0, p.calls)
}

// The concrete scheduler must satisfy the route's interface, or the wiring in
// server.go cannot compile — asserted here so the coupling is checked in the
// package that defines the contract.
var _ RegistryPruner = (*registryprune.Scheduler)(nil)
