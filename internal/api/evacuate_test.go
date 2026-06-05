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
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func newEvacSrv(t *testing.T) (*httptest.Server, string, *store.Memory, *fake.Fake) {
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
	f := fake.New()
	svc := instance.NewService(f, hosts, []config.Template{migrateTmpl()})
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok, mem, f
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
	srv, tok, mem, _ := newEvacSrv(t)
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
	srv, tok, _, _ := newEvacSrv(t)
	req, _ := http.NewRequest("POST", srv.URL+"/evacuate", bytes.NewBufferString("{not json"))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEvacuate_API_ValidationFailsNoJob(t *testing.T) {
	srv, tok, mem, _ := newEvacSrv(t)
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
	srv, tok, mem, _ := newEvacSrv(t)
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
	srv, tok, mem, _ := newEvacSrv(t)
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

func postEvacuatePlan(t *testing.T, srv *httptest.Server, tok string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", srv.URL+"/evacuate/plan", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

type planResp struct {
	FromHost string `json:"from_host"`
	Moves    []struct {
		Slug   string `json:"slug"`
		ToHost string `json:"to_host"`
		OK     bool   `json:"ok"`
		Issues []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"issues"`
		Provisions []string `json:"provisions"`
	} `json:"moves"`
}

func seedEvacWithParams(t *testing.T, mem *store.Memory, slug string) {
	t.Helper()
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "postgres", Slug: slug,
		Parameters: map[string]any{"image": "postgres:15", "port": 5432, "db": "app", "user": "app"},
		Secrets:    map[string]string{},
	}))
}

func TestEvacuatePlan_API_Success(t *testing.T) {
	srv, tok, mem, _ := newEvacSrv(t)
	seedEvacWithParams(t, mem, "db1")
	seedEvacWithParams(t, mem, "db2")

	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2", "db2": "h3"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out planResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "h1", out.FromHost)
	require.Len(t, out.Moves, 2)
	assert.Equal(t, "db1", out.Moves[0].Slug) // sorted
	assert.Equal(t, "db2", out.Moves[1].Slug)
	for _, m := range out.Moves {
		assert.True(t, m.OK)
		assert.Empty(t, m.Issues)
	}
}

func TestEvacuatePlan_API_BlockedMove(t *testing.T) {
	srv, tok, mem, f := newEvacSrv(t)
	seedEvacWithParams(t, mem, "db1")
	// Pre-place the instance on the destination so the move is blocked.
	f.AddPod("h2", podman.Pod{Name: "postgres-db1", Status: "Running"})

	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out planResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Moves, 1)
	assert.False(t, out.Moves[0].OK)
	require.Len(t, out.Moves[0].Issues, 1)
	assert.Equal(t, "instance_exists", out.Moves[0].Issues[0].Code)
}

func TestEvacuatePlan_API_MalformedBody(t *testing.T) {
	srv, tok, _, _ := newEvacSrv(t)
	req, _ := http.NewRequest("POST", srv.URL+"/evacuate/plan", bytes.NewBufferString("{not json"))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEvacuatePlan_API_BadMap_400(t *testing.T) {
	srv, tok, mem, _ := newEvacSrv(t)
	seedEvac(t, mem, "db1")
	seedEvac(t, mem, "db2")
	// db2 unmapped -> invalid_request, same as the real evacuate.
	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2"},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEvacuatePlan_API_UnknownHost_404(t *testing.T) {
	srv, tok, _, _ := newEvacSrv(t)
	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "ghost", Map: map[string]string{},
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestEvacuatePlan_API_StoreDisabled_501(t *testing.T) {
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*", "jobs:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	svc := instance.NewService(fake.New(), hosts, []config.Template{migrateTmpl()})
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)

	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{FromHost: "h1", Map: map[string]string{}})
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

func TestEvacuatePlan_API_RequiresReadScope(t *testing.T) {
	// A key WITHOUT instances:read is rejected (hosts:read only -> 403 forbidden).
	hash, err := config.HashToken("t")
	require.NoError(t, err)
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	mem := store.NewMemory()
	svc := instance.NewService(fake.New(), hosts, []config.Template{migrateTmpl()})
	svc.SetStore(mem)

	keys := []config.APIKey{{ID: "noscope", SecretHash: hash, Scopes: []string{"hosts:read"}}}
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)

	resp := postEvacuatePlan(t, srv, "t", instance.EvacuateRequest{FromHost: "h1", Map: map[string]string{}})
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// provisions is always present in the response (empty when nothing to provision).
func TestEvacuatePlan_API_ProvisionsSerialized(t *testing.T) {
	srv, tok, mem, _ := newEvacSrv(t)
	seedEvacWithParams(t, mem, "db1")
	resp := postEvacuatePlan(t, srv, tok, instance.EvacuateRequest{
		FromHost: "h1", Map: map[string]string{"db1": "h2"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out planResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Moves, 1)
	assert.NotNil(t, out.Moves[0].Provisions, "provisions must serialize as [], not null")
	assert.Empty(t, out.Moves[0].Provisions)
}
