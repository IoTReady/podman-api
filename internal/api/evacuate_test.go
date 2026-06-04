package api

import (
	"bytes"
	"context"
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
	"github.com/iotready/podman-api/internal/store"
)

func newEvacSrv(t *testing.T) (*httptest.Server, string, *store.Memory) {
	t.Helper()
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "jobs:*"}}}
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/x"},
		{ID: "h2", Addr: "unix", Socket: "/y"},
		{ID: "h3", Addr: "unix", Socket: "/z"},
	}
	mem := store.NewMemory()
	svc := instance.NewService(fake.New(), hosts, []config.Template{migrateTmpl()})
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok, mem
}

func postEvacuate(t *testing.T, srv *httptest.Server, tok string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", srv.URL+"/evacuate", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func seedEvac(t *testing.T, mem *store.Memory, slug string) {
	t.Helper()
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "postgres", Slug: slug,
		Parameters: map[string]any{}, Secrets: map[string]string{},
	}))
}

func TestEvacuate_API_Success(t *testing.T) {
	srv, tok, mem := newEvacSrv(t)
	ctx := context.Background()
	seedEvac(t, mem, "db1")
	seedEvac(t, mem, "db2")

	resp := postEvacuate(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2", "db2": "h3"},
	})
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var acc struct {
		JobID string `json:"job_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&acc))
	require.NotEmpty(t, acc.JobID)
	job, err := mem.GetJob(ctx, acc.JobID)
	require.NoError(t, err)
	assert.Equal(t, "evacuate", job.Kind)
}

func TestEvacuate_API_MalformedBody(t *testing.T) {
	srv, tok, _ := newEvacSrv(t)
	req, _ := http.NewRequest("POST", srv.URL+"/evacuate", bytes.NewBufferString("{not json"))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEvacuate_API_ValidationFailsNoJob(t *testing.T) {
	srv, tok, mem := newEvacSrv(t)
	ctx := context.Background()
	seedEvac(t, mem, "db1")
	seedEvac(t, mem, "db2")
	// db2 has no destination -> invalid_request, and NO job may be enqueued.
	resp := postEvacuate(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2"},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	all, err := mem.ListJobs(ctx, store.JobFilter{})
	require.NoError(t, err)
	assert.Empty(t, all, "no job should be enqueued when validation fails")
}

func TestEvacuate_API_UnknownDestHost_400(t *testing.T) {
	srv, tok, mem := newEvacSrv(t)
	ctx := context.Background()
	seedEvac(t, mem, "db1")
	// db1 mapped to a host that doesn't exist -> bad map content -> 400, no job.
	// (A bad map must not yield 404; that status is reserved for an unknown
	// from_host, the resource being operated on.)
	resp := postEvacuate(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "ghost"},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	all, err := mem.ListJobs(ctx, store.JobFilter{})
	require.NoError(t, err)
	assert.Empty(t, all, "no job should be enqueued when validation fails")
}

func TestEvacuate_API_StoreDisabled_501(t *testing.T) {
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "jobs:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	svc := instance.NewService(fake.New(), hosts, []config.Template{migrateTmpl()})
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)

	resp := postEvacuate(t, srv, tok, instance.EvacuateRequest{FromHost: "h1", Map: map[string]string{}})
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

func TestJobs_API_ParentIDFilter(t *testing.T) {
	srv, tok, mem := newEvacSrv(t)
	ctx := context.Background()
	parent, err := mem.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	require.NoError(t, err)
	_, err = mem.StartChild(ctx, "migrate", json.RawMessage(`{}`), parent.ID)
	require.NoError(t, err)
	_, err = mem.StartChild(ctx, "migrate", json.RawMessage(`{}`), parent.ID)
	require.NoError(t, err)
	_, err = mem.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "") // unrelated top-level
	require.NoError(t, err)

	req, _ := http.NewRequest("GET", srv.URL+"/jobs?parent_id="+parent.ID, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out []struct {
		ID       string `json:"id"`
		ParentID string `json:"parent_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Len(t, out, 2)
	for _, j := range out {
		assert.Equal(t, parent.ID, j.ParentID)
	}
}
