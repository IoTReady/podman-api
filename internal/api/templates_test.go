package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			ID:         "x",
			Parameters: render.Parameters{Required: []string{"slug"}},
		}, Body: "kind: Pod\nname: x-{{.slug}}\n", Source: "x.yaml"},
	}
	svc := instance.NewService(fake.New(), nil, tmpls)
	srv := httptest.NewServer(NewRouter(svc, keys))
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
	assert.Equal(t, "x", got[0]["id"])
}

func TestRenderTemplate(t *testing.T) {
	srv, tok := newSrvWithTmpl(t)
	resp := authedReq(t, srv, tok, "GET", "/templates/x/render?slug=hello")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)
	assert.Contains(t, string(body[:n]), "name: x-hello")
}
