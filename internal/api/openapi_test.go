package api

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestOpenAPI_ServedAndParseable(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/openapi.yaml")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "spec must be served without auth")
	assert.Contains(t, resp.Header.Get("Content-Type"), "yaml")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body)

	// The spec must be valid YAML, declare OpenAPI 3.x, and document every
	// path the router actually exposes — otherwise the published contract
	// drifts from the running code.
	var doc struct {
		OpenAPI string         `yaml:"openapi"`
		Info    map[string]any `yaml:"info"`
		Paths   map[string]any `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(body, &doc))
	assert.True(t, len(doc.OpenAPI) >= 3 && doc.OpenAPI[0] == '3', "openapi: must be 3.x, got %q", doc.OpenAPI)
	assert.NotEmpty(t, doc.Info["title"])

	// Spot-check the paths that callers most commonly need to construct
	// requests against. If any go missing the spec is silently broken.
	for _, want := range []string{
		"/healthz",
		"/hosts",
		"/hosts/{host}",
		"/hosts/{host}/instances",
		"/hosts/{host}/instances/{template}/{slug}",
		"/hosts/{host}/instances/{template}/{slug}/start",
		"/hosts/{host}/instances/{template}/{slug}/upgrade",
		"/hosts/{host}/instances/{template}/{slug}/upgrade-image",
		"/hosts/{host}/instances/{template}/{slug}/parameters",
		"/hosts/{host}/instances/{template}/{slug}/secrets",
		"/hosts/{host}/instances/{template}/{slug}/logs",
		"/hosts/{host}/bulk",
		"/evacuate",
		"/evacuate/plan",
		"/migrate",
		"/hosts/{host}/instances/{template}/{slug}/backup",
		"/hosts/{host}/instances/{template}/{slug}/backups",
		"/backups/{id}/restore",
		"/hosts/{host}/instances/{template}/{slug}/restore",
		"/backups/{id}",
		"/jobs/{id}/cancel",
		"/hosts/{host}/secrets/{name}",
		"/hosts/{host}/volumes/{name}",
		"/templates",
		"/templates/{id}",
		"/templates/{id}/render",
		"/openapi.yaml",
	} {
		_, ok := doc.Paths[want]
		assert.Truef(t, ok, "spec missing path %q", want)
	}
}

// TestOpenAPI_BackupPathDocumentsScopeBodyAndBadRequest goes past path
// presence, which is all the check above does — and which is why the optional
// `{"volumes": [...]}` body and the 400 the handler can now return went
// undocumented for a whole branch. A generated client built from a spec missing
// the body cannot express a scoped backup at all, and treats the 400 as an
// undocumented status.
func TestOpenAPI_BackupPathDocumentsScopeBodyAndBadRequest(t *testing.T) {
	srv, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/openapi.yaml")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var doc struct {
		Paths map[string]struct {
			Post struct {
				RequestBody struct {
					Content map[string]struct {
						Schema struct {
							Properties map[string]struct {
								Type string `yaml:"type"`
							} `yaml:"properties"`
						} `yaml:"schema"`
					} `yaml:"content"`
				} `yaml:"requestBody"`
				Responses map[string]any `yaml:"responses"`
			} `yaml:"post"`
		} `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(body, &doc))

	p, ok := doc.Paths["/hosts/{host}/instances/{template}/{slug}/backup"]
	require.True(t, ok, "spec missing the backup path")

	schema := p.Post.RequestBody.Content["application/json"].Schema
	vols, ok := schema.Properties["volumes"]
	require.True(t, ok, "backup requestBody must document the optional \"volumes\" scope")
	assert.Equal(t, "array", vols.Type)

	for _, status := range []string{"202", "400", "404", "501"} {
		_, ok := p.Post.Responses[status]
		assert.Truef(t, ok, "backup path missing documented response %q", status)
	}
}
