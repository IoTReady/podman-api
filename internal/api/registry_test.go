package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

type fakeRegistry struct {
	catalog []string
	tags    map[string][]imgregistry.TagGroup
}

func (f *fakeRegistry) Catalog(ctx context.Context) ([]string, error) { return f.catalog, nil }

func (f *fakeRegistry) Tags(ctx context.Context, repo string) ([]imgregistry.TagGroup, error) {
	groups, ok := f.tags[repo]
	if !ok {
		return nil, fmt.Errorf("repo not found: %s", repo)
	}
	return groups, nil
}

func (f *fakeRegistry) Manifest(ctx context.Context, repo, ref string) (imgregistry.Manifest, error) {
	return imgregistry.Manifest{}, nil
}

// newRegistryTestServer mirrors newTestServer/newSrvFull (instances_test.go)
// but wires a registry client, which those shared helpers' fixed NewRouter
// call does not accept an option for.
func newRegistryTestServer(t *testing.T, reg imgregistry.Client) (*httptest.Server, string) {
	t.Helper()
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	mem := store.NewMemory()
	svc := instance.NewService(f, hosts)
	svc.SetStore(mem)
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil, "", reg))
	t.Cleanup(srv.Close)
	return srv, tok
}

func authedRegistryGet(t *testing.T, srv *httptest.Server, tok, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestListRepos(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{catalog: []string{"engine", "otp"}})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "engine")
	assert.Contains(t, string(body), "otp")
}

func TestListRepos_RegistryDisabled(t *testing.T) {
	srv, tok := newRegistryTestServer(t, nil)
	resp := authedRegistryGet(t, srv, tok, "/registry/repos")
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestListRepoTags(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	srv, tok := newRegistryTestServer(t, &fakeRegistry{
		tags: map[string][]imgregistry.TagGroup{
			"engine": {{Digest: "sha256:abc", Tags: []string{"latest", "v1"}, Created: created, Size: 100}},
		},
	})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/engine")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "sha256:abc")
}

func TestListRepoTags_UnknownRepo(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{tags: map[string][]imgregistry.TagGroup{}})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/nope")
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
