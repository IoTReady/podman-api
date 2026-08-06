package ui

import (
	"context"
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
	catalog []string
	tags    []imgregistry.TagGroup
}

func (f *fakeRegistryUI) Catalog(ctx context.Context) ([]string, error) { return f.catalog, nil }
func (f *fakeRegistryUI) Tags(ctx context.Context, repo string) ([]imgregistry.TagGroup, error) {
	return f.tags, nil
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
