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
	"github.com/iotready/podman-api/internal/render"
)

func newSrvWithTmpl(t *testing.T) (*httptest.Server, string) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:read"}}}
	tmpls := []config.Template{
		{Meta: render.Meta{
			ID:         "app",
			Parameters: render.Parameters{Required: []string{"slug"}},
		}, Body: "kind: Pod\nname: app-{{.slug}}\n", Source: "app.yaml"},
	}
	svc := instance.NewService(fake.New(), nil, tmpls)
	srv := httptest.NewServer(NewRouter(svc, auth.NewKeyStore(keys), nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestListTemplates(t *testing.T) {
	srv, tok := newSrvWithTmpl(t)
	resp := authedReq(t, srv, tok, "GET", "/templates")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 1)
	assert.Equal(t, "app", got[0]["id"])
}

func TestRenderTemplate(t *testing.T) {
	srv, tok := newSrvWithTmpl(t)
	resp := authedReq(t, srv, tok, "GET", "/templates/app/render?slug=hello")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)
	assert.Contains(t, string(body[:n]), "name: app-hello")
}
