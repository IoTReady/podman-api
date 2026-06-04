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
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func migrateTmpl() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "postgres",
			Parameters: render.Parameters{Required: []string{"slug", "image", "port", "db", "user"}},
			Volumes:    []render.Volume{{Name: "data"}},
		},
		Body:   "apiVersion: v1\nkind: Pod\nmetadata:\n  name: postgres-{{.slug}}\nspec:\n  containers:\n    - name: db\n      image: {{.image}}\n",
		Source: "postgres.yaml",
	}
}

func newMigrateSrv(t *testing.T) (*httptest.Server, string, *fake.Fake, *store.Memory) {
	t.Helper()
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	f := fake.New()
	mem := store.NewMemory()
	svc := instance.NewService(f, hosts, []config.Template{migrateTmpl()})
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok, f, mem
}

func postMigrate(t *testing.T, srv *httptest.Server, tok string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", srv.URL+"/migrate", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestMigrate_API(t *testing.T) {
	srv, tok, f, mem := newMigrateSrv(t)
	ctx := context.Background()
	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}
	require.NoError(t, mem.PutSpec(ctx, store.Spec{Host: "h1", Template: "postgres", Slug: "db1", Parameters: params}))
	f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})

	resp := postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"})
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var acc struct {
		JobID string `json:"job_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&acc))
	require.NotEmpty(t, acc.JobID)
	job, err := mem.GetJob(ctx, acc.JobID)
	require.NoError(t, err)
	assert.Equal(t, "migrate", job.Kind)

	resp = postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "h1", ToHost: "h1", Template: "postgres", Slug: "db1"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp = postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "nope", ToHost: "h2", Template: "postgres", Slug: "db1"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp = postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "absent"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestMigrate_API_MalformedBody(t *testing.T) {
	srv, tok, _, _ := newMigrateSrv(t)
	req, _ := http.NewRequest("POST", srv.URL+"/migrate", bytes.NewBufferString("{not json"))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestMigrate_API_StoreDisabled_501(t *testing.T) {
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	svc := instance.NewService(fake.New(), hosts, []config.Template{migrateTmpl()})
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil))
	t.Cleanup(srv.Close)

	resp := postMigrate(t, srv, tok, instance.MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"})
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}
