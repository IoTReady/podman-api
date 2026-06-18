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
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func catalogTmpl() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:      "app",
			Display: render.Display{Name: "App", Category: "web"},
			Parameters: []render.ParamDef{
				{Name: "slug", Type: "string", Required: true},
			},
		},
		Body:   "kind: Pod\nname: app-{{.slug}}\n",
		Origin: "seed",
	}
}

// newSrvWithTmpl builds a server with one seeded template ("app") and a key
// holding templates:read + templates:write + instances:* (so instance-on-host
// references can be created for the delete-in-use test).
func newSrvWithTmpl(t *testing.T) (*httptest.Server, string, *store.Memory, *fake.Fake) {
	t.Helper()
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"templates:read", "templates:write", "instances:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(context.Background(), catalogTmpl()))
	f := fake.New()
	svc := instance.NewService(f, hosts)
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok, mem, f
}

func doReq(t *testing.T, srv *httptest.Server, tok, method, path, body string) *http.Response {
	t.Helper()
	var r *http.Request
	var err error
	if body == "" {
		r, err = http.NewRequest(method, srv.URL+path, nil)
	} else {
		r, err = http.NewRequest(method, srv.URL+path, bytes.NewBufferString(body))
		r.Header.Set("Content-Type", "application/json")
	}
	require.NoError(t, err)
	r.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(r)
	require.NoError(t, err)
	return resp
}

func TestListTemplates(t *testing.T) {
	srv, tok, _, _ := newSrvWithTmpl(t)
	resp := authedReq(t, srv, tok, "GET", "/templates")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 1)
	assert.Equal(t, "app", got[0]["id"])
	assert.Equal(t, "kind: Pod\nname: app-{{.slug}}\n", got[0]["body"])
	assert.Equal(t, "seed", got[0]["origin"])

	params, ok := got[0]["parameters"].([]any)
	require.True(t, ok, "parameters must be a list")
	require.Len(t, params, 1)
	p0 := params[0].(map[string]any)
	assert.Equal(t, "slug", p0["name"])
	assert.Equal(t, "string", p0["type"])
	assert.Equal(t, true, p0["required"])
}

func TestGetTemplate_NotFound(t *testing.T) {
	srv, tok, _, _ := newSrvWithTmpl(t)
	resp := authedReq(t, srv, tok, "GET", "/templates/nope")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRenderTemplate(t *testing.T) {
	srv, tok, _, _ := newSrvWithTmpl(t)
	resp := authedReq(t, srv, tok, "GET", "/templates/app/render?slug=hello")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)
	assert.Contains(t, string(body[:n]), "name: app-hello")
}

func TestRenderTemplate_AppliesDefaults(t *testing.T) {
	// A render preview that omits a defaulted parameter must succeed with the
	// default filled in, matching what an actual deploy would render (#61).
	srv, tok, mem, _ := newSrvWithTmpl(t)
	require.NoError(t, mem.PutTemplate(context.Background(), store.Template{
		Meta: render.Meta{
			ID: "defaulted",
			Parameters: []render.ParamDef{
				{Name: "slug", Type: "string", Required: true},
				{Name: "tag", Type: "string", Default: "latest"},
			},
		},
		Body:   "kind: Pod\nname: d-{{.slug}}\nimage: nginx:{{.tag}}\n",
		Origin: "seed",
	}))

	resp := authedReq(t, srv, tok, "GET", "/templates/defaulted/render?slug=hi")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)
	out := string(body[:n])
	assert.Contains(t, out, "name: d-hi")
	assert.Contains(t, out, "image: nginx:latest")
}

func TestDeleteTemplate_UnknownIs404(t *testing.T) {
	// OpenAPI documents 404 for an unknown id even though store deletes are
	// absent-OK (#61).
	srv, tok, _, _ := newSrvWithTmpl(t)
	resp := doReq(t, srv, tok, "DELETE", "/templates/nope", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCreateTemplate_AndConflict(t *testing.T) {
	srv, tok, _, _ := newSrvWithTmpl(t)
	body := `{"id":"redis","body":"kind: Pod\nname: redis-{{.slug}}\n","parameters":[{"name":"slug","type":"string","required":true}]}`

	resp := doReq(t, srv, tok, "POST", "/templates", body)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	resp.Body.Close()
	assert.Equal(t, "redis", got["id"])
	assert.Equal(t, "user", got["origin"])

	resp = doReq(t, srv, tok, "POST", "/templates", body)
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestCreateTemplate_InvalidBody(t *testing.T) {
	srv, tok, _, _ := newSrvWithTmpl(t)
	// References an undeclared parameter -> dry-run render fails (missingkey=error).
	body := `{"id":"broken","body":"name: {{.undeclared}}\n"}`
	resp := doReq(t, srv, tok, "POST", "/templates", body)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestUpdateTemplate(t *testing.T) {
	srv, tok, _, _ := newSrvWithTmpl(t)
	body := `{"body":"kind: Pod\nname: app2-{{.slug}}\n","parameters":[{"name":"slug","type":"string","required":true}]}`
	resp := doReq(t, srv, tok, "PUT", "/templates/app", body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	resp.Body.Close()
	assert.Equal(t, "app", got["id"])
	assert.Contains(t, got["body"], "app2-")
	// Origin is preserved from the seed.
	assert.Equal(t, "seed", got["origin"])
}

func TestCloneTemplate(t *testing.T) {
	srv, tok, _, _ := newSrvWithTmpl(t)
	resp := doReq(t, srv, tok, "POST", "/templates/app/clone", `{"new_id":"appcopy"}`)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	resp.Body.Close()
	assert.Equal(t, "appcopy", got["id"])
	assert.Equal(t, "user", got["origin"])
}

func TestDeleteTemplate_InUseThenForce(t *testing.T) {
	srv, tok, mem, _ := newSrvWithTmpl(t)
	// Record an instance referencing the template.
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "app", Slug: "i1",
		Parameters: map[string]any{"slug": "i1"},
	}))

	resp := doReq(t, srv, tok, "DELETE", "/templates/app", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	resp = doReq(t, srv, tok, "DELETE", "/templates/app?force=true", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/templates/app")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCreateTemplate_PreBackupRoundTrip(t *testing.T) {
	srv, tok, _, _ := newSrvWithTmpl(t)
	body := `{"id":"frappe","body":"kind: Pod\nname: frappe-{{.slug}}\n","parameters":[{"name":"slug","type":"string","required":true}],"pre_backup":{"container":"app","command":"bench --site {{.slug}} backup"}}`

	resp := doReq(t, srv, tok, "POST", "/templates", body)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	resp = authedReq(t, srv, tok, "GET", "/templates/frappe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))

	pb, ok := got["pre_backup"].(map[string]any)
	require.True(t, ok, "pre_backup must be an object in GET response")
	assert.Equal(t, "app", pb["container"])
	assert.Equal(t, "bench --site {{.slug}} backup", pb["command"])
}

func TestTemplateWriteRejectsReadOnlyKey(t *testing.T) {
	tok := "ro"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "ro", SecretHash: hash, Scopes: []string{"templates:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(context.Background(), catalogTmpl()))
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)

	// Read is allowed.
	resp := authedReq(t, srv, tok, "GET", "/templates")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Write is forbidden (403).
	resp = doReq(t, srv, tok, "POST", "/templates", `{"id":"x2y","body":"kind: Pod\n"}`)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}
