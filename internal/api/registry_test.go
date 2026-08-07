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
	// manifests keys on "repo/ref"; a miss falls back to unreachableErr (if
	// set) or imgregistry.ErrNotFound, mirroring tags below.
	manifests map[string]imgregistry.Manifest
	// unreachableErr, when set, is what an unknown repo/ref resolves to
	// instead of imgregistry.ErrNotFound — simulating a registry that is
	// reachable-but-broken (transport failure, non-404 non-2xx) rather than
	// one that cleanly answered "no such repo."
	unreachableErr error
}

func (f *fakeRegistry) Catalog(ctx context.Context) ([]string, error) { return f.catalog, nil }

// ResolveTags is the drop-reporting surface; this fake never drops a tag, so
// Tags and ResolveTags agree by construction.
func (f *fakeRegistry) ResolveTags(ctx context.Context, repo string) (imgregistry.TagListing, error) {
	groups, err := f.Tags(ctx, repo)
	if err != nil {
		return imgregistry.TagListing{}, err
	}
	return imgregistry.TagListing{Groups: groups}, nil
}

func (f *fakeRegistry) Tags(ctx context.Context, repo string) ([]imgregistry.TagGroup, error) {
	groups, ok := f.tags[repo]
	if !ok {
		if f.unreachableErr != nil {
			return nil, fmt.Errorf("list tags for %s: %w", repo, f.unreachableErr)
		}
		return nil, fmt.Errorf("repo not found: %s: %w", repo, imgregistry.ErrNotFound)
	}
	return groups, nil
}

// TagCount is unused by internal/api (tag count is a UI-only column) but
// required to satisfy imgregistry.Client.
func (f *fakeRegistry) TagCount(ctx context.Context, repo string) (int, error) {
	groups, ok := f.tags[repo]
	if !ok {
		if f.unreachableErr != nil {
			return 0, fmt.Errorf("list tags for %s: %w", repo, f.unreachableErr)
		}
		return 0, fmt.Errorf("repo not found: %s: %w", repo, imgregistry.ErrNotFound)
	}
	n := 0
	for _, g := range groups {
		n += len(g.Tags)
	}
	return n, nil
}

func (f *fakeRegistry) Manifest(ctx context.Context, repo, ref string) (imgregistry.Manifest, error) {
	m, ok := f.manifests[repo+"/"+ref]
	if !ok {
		if f.unreachableErr != nil {
			return imgregistry.Manifest{}, fmt.Errorf("manifest %s/%s: %w", repo, ref, f.unreachableErr)
		}
		return imgregistry.Manifest{}, fmt.Errorf("manifest %s/%s: not found: %w", repo, ref, imgregistry.ErrNotFound)
	}
	return m, nil
}

// Delete is unused by internal/api's routes (no delete route exists yet) but
// required to satisfy imgregistry.Client.
func (f *fakeRegistry) Delete(ctx context.Context, repo, digest string) error {
	return nil
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

// TestListRepoTags_RegistryUnreachable is the Important-3 fix: an error that
// is NOT the registry's own 404 (a transport failure, a non-404 non-2xx, an
// undecodable body — imgregistry.ErrUnreachable) must surface as 502
// registry_unreachable, never as 404 not_found. Collapsing the two used to
// tell an operator "this repo doesn't exist" when the real problem was that
// the registry could not be reached at all.
func TestListRepoTags_RegistryUnreachable(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{
		tags:           map[string][]imgregistry.TagGroup{},
		unreachableErr: imgregistry.ErrUnreachable,
	})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/engine")
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "registry_unreachable")
}

// TestListRepoTags_NamespacedRepo is the Critical-2 fix: a repo name legally
// containing "/" (e.g. "iotready/engine") must be addressable through the
// {repo...} multi-segment wildcard route — a {repo} (one segment) route
// 404s it outright.
func TestListRepoTags_NamespacedRepo(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	srv, tok := newRegistryTestServer(t, &fakeRegistry{
		tags: map[string][]imgregistry.TagGroup{
			"iotready/engine": {{Digest: "sha256:abc", Tags: []string{"latest"}, Created: created}},
		},
	})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/iotready/engine")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "sha256:abc")
}

// TestListRepoTags_InvalidRepoName exercises the purpose-built
// imgregistry.ValidRepoName validator (replacing validName, which rejected
// "/", ".", and single-character components — all legal in a real registry
// repo name) at the API edge.
func TestListRepoTags_InvalidRepoName(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{tags: map[string][]imgregistry.TagGroup{}})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/Not_Valid!")
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestGetManifest exercises the manifest lookup folded into the same
// {repo...} route via "?manifest=<ref>" (Critical-2's routing restructure:
// Go's mux requires the multi-segment wildcard to be the route's last
// segment, ruling out a separate .../manifests/{ref} path).
func TestGetManifest(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{
		manifests: map[string]imgregistry.Manifest{
			"engine/latest": {Digest: "sha256:manifestdigest", Size: 42},
		},
	})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/engine?manifest=latest")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "sha256:manifestdigest")
}

func TestGetManifest_NotFound(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{manifests: map[string]imgregistry.Manifest{}})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/engine?manifest=nope")
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetManifest_RegistryUnreachable(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{
		manifests:      map[string]imgregistry.Manifest{},
		unreachableErr: imgregistry.ErrUnreachable,
	})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/engine?manifest=latest")
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "registry_unreachable")
}

// TestGetManifest_NamespacedRepo confirms the manifest branch of
// getRepoOrManifest also works for a "/"-containing repo.
func TestGetManifest_NamespacedRepo(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{
		manifests: map[string]imgregistry.Manifest{
			"iotready/engine/latest": {Digest: "sha256:nsdigest"},
		},
	})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/iotready/engine?manifest=latest")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "sha256:nsdigest")
}

// TestGetManifest_InvalidRef guards against an unvalidated ref reaching
// Client.Manifest, which interpolates it unescaped into a registry request
// path — a ref like "../../../etc/passwd" must be rejected here rather than
// forwarded to the upstream registry.
func TestGetManifest_InvalidRef(t *testing.T) {
	srv, tok := newRegistryTestServer(t, &fakeRegistry{manifests: map[string]imgregistry.Manifest{}})
	resp := authedRegistryGet(t, srv, tok, "/registry/repos/engine?manifest=..%2F..%2F..%2Fetc%2Fpasswd")
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "invalid_parameters")
}
