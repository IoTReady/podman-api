package ui

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

type fakeRegistryUI struct {
	catalog   []string
	tags      []imgregistry.TagGroup
	tagCounts map[string]int
}

func (f *fakeRegistryUI) Catalog(ctx context.Context) ([]string, error) { return f.catalog, nil }
func (f *fakeRegistryUI) Tags(ctx context.Context, repo string) ([]imgregistry.TagGroup, error) {
	return f.tags, nil
}

// tagCounts, when non-nil, is what TagCount returns per repo; a repo absent
// from the map returns an error, letting a test drive the "count failed to
// resolve" cell.
func (f *fakeRegistryUI) TagCount(ctx context.Context, repo string) (int, error) {
	if f.tagCounts == nil {
		return 0, nil
	}
	n, ok := f.tagCounts[repo]
	if !ok {
		return 0, fmt.Errorf("repo not found: %s: %w", repo, imgregistry.ErrNotFound)
	}
	return n, nil
}

func (f *fakeRegistryUI) Manifest(ctx context.Context, repo, ref string) (imgregistry.Manifest, error) {
	return imgregistry.Manifest{}, nil
}

// uiWithRegistry mirrors uiWithService (handlers_hosts_test.go) but also
// wires an imgregistry.Client, since the shared helper takes no such option.
func uiWithRegistry(t *testing.T, reg imgregistry.Client) *UI {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	svc := instance.NewService(fc, hosts)
	svc.SetStore(store.NewMemory())
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:      svc,
		Auth:     NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
		Registry: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestUI_RegistryRepos_Disabled(t *testing.T) {
	u := uiWithRegistry(t, nil)
	w := authedGet(t, u, "/ui/registry")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body = %s", w.Code, w.Body.String())
	}
}

func TestUI_RegistryRepos_ListsCatalog(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{catalog: []string{"engine", "otp"}})
	w := authedGet(t, u, "/ui/registry")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "engine") {
		t.Fatalf("body missing engine: %s", w.Body.String())
	}
}

func TestUI_RegistryTags_GroupsRenderTogether(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	u := uiWithRegistry(t, &fakeRegistryUI{
		tags: []imgregistry.TagGroup{{Digest: "sha256:abc", Tags: []string{"latest", "v1"}, Created: created}},
	})
	w := authedGet(t, u, "/ui/registry/engine")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "latest") || !strings.Contains(w.Body.String(), "v1") {
		t.Fatalf("body missing tags: %s", w.Body.String())
	}
}

// TestUI_RegistryRepos_LinksNamespacedRepoCorrectly is part of the Critical-2
// fix: a repo name containing "/" (e.g. "iotready/engine") must render an
// href that html/template does not mangle and that the {repo...} route below
// can actually resolve.
func TestUI_RegistryRepos_LinksNamespacedRepoCorrectly(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{catalog: []string{"iotready/engine"}})
	w := authedGet(t, u, "/ui/registry")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `href="/ui/registry/iotready/engine"`) {
		t.Fatalf("expected an href to the namespaced repo, got: %s", w.Body.String())
	}
}

// TestUI_RegistryTags_NamespacedRepo is the UI-side half of Critical 2: the
// {repo...} multi-segment wildcard route must resolve a "/"-containing repo
// name, where the old {repo} (one-segment) route 404d it.
func TestUI_RegistryTags_NamespacedRepo(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	u := uiWithRegistry(t, &fakeRegistryUI{
		tags: []imgregistry.TagGroup{{Digest: "sha256:abc", Tags: []string{"latest"}, Created: created}},
	})
	w := authedGet(t, u, "/ui/registry/iotready/engine")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "latest") {
		t.Fatalf("body missing tags: %s", w.Body.String())
	}
}

// TestUI_RegistryTags_InvalidRepoNameIs400 is the Important-6 fix: the UI
// handler must apply the same repo-name validator as the API edge before the
// value ever reaches u.cfg.Registry.Tags.
func TestUI_RegistryTags_InvalidRepoNameIs400(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{tags: []imgregistry.TagGroup{}})
	w := authedGet(t, u, "/ui/registry/Not_Valid!")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
}

func TestUI_RegistryTags_PickerFragmentOmitsPageChrome(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	u := uiWithRegistry(t, &fakeRegistryUI{
		tags: []imgregistry.TagGroup{{Digest: "sha256:abc", Tags: []string{"latest"}, Created: created}},
	})
	w := authedGet(t, u, "/ui/registry/engine?picker=1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Use") {
		t.Fatalf("expected picker fragment with a Use button: %s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "All repositories") {
		t.Fatalf("picker fragment should not include the full page's back-link chrome: %s", w.Body.String())
	}
}
