package ui

import (
	"context"
	"fmt"
	"html"
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
	tagsErr   error // when set, Tags returns this error instead of tags
	tagCounts map[string]int
	manifests map[string]imgregistry.Manifest
}

func (f *fakeRegistryUI) Catalog(ctx context.Context) ([]string, error) { return f.catalog, nil }

// ResolveTags is the drop-reporting surface; this fake never drops a tag, so
// Tags and ResolveTags agree by construction.
func (f *fakeRegistryUI) ResolveTags(ctx context.Context, repo string) (imgregistry.TagListing, error) {
	groups, err := f.Tags(ctx, repo)
	if err != nil {
		return imgregistry.TagListing{}, err
	}
	return imgregistry.TagListing{Groups: groups}, nil
}

func (f *fakeRegistryUI) Tags(ctx context.Context, repo string) ([]imgregistry.TagGroup, error) {
	if f.tagsErr != nil {
		return nil, f.tagsErr
	}
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
	m, ok := f.manifests[repo+"/"+ref]
	if !ok {
		return imgregistry.Manifest{}, fmt.Errorf("manifest %s/%s: not found: %w", repo, ref, imgregistry.ErrNotFound)
	}
	return m, nil
}

// Delete is unused by internal/ui's routes (no delete route exists yet) but
// required to satisfy imgregistry.Client.
func (f *fakeRegistryUI) Delete(ctx context.Context, repo, digest string) error {
	return nil
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

// TestUI_RegistryRepos_ShowsTagCounts confirms the list page renders a real
// per-repo tag count on first paint (fanned out concurrently), not a
// placeholder the browser has to fill in.
func TestUI_RegistryRepos_ShowsTagCounts(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		catalog:   []string{"engine", "otp"},
		tagCounts: map[string]int{"engine": 737, "otp": 13},
	})
	w := authedGet(t, u, "/ui/registry")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "737") || !strings.Contains(body, "13") {
		t.Fatalf("body missing tag counts: %s", body)
	}
}

// TestUI_RegistryRepos_CountFailureDoesNotFailPage: one repo whose count
// cannot be resolved must render as unknown, not take down the whole list.
func TestUI_RegistryRepos_CountFailureDoesNotFailPage(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		catalog:   []string{"engine", "broken"},
		tagCounts: map[string]int{"engine": 737}, // "broken" absent -> error
	})
	w := authedGet(t, u, "/ui/registry")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 despite one repo's count failing; body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "broken") {
		t.Fatalf("the failing repo should still be listed: %s", body)
	}
	if !strings.Contains(body, "737") {
		t.Fatalf("the working repo's count should still render: %s", body)
	}
}

// TestUI_RegistryRepos_LazyLoadsStats confirms each row asks for its own
// stats fragment after load, rather than the page paying for every repo's
// full Tags() resolution up front.
func TestUI_RegistryRepos_LazyLoadsStats(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		catalog:   []string{"engine"},
		tagCounts: map[string]int{"engine": 737},
	})
	w := authedGet(t, u, "/ui/registry")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "/ui/registry/engine?stats=1") {
		t.Fatalf("expected a lazy stats hx-get for engine: %s", body)
	}
	if !strings.Contains(body, `hx-trigger="load"`) {
		t.Fatalf("expected the stats cell to load on page load: %s", body)
	}
}

// TestUI_RegistryTags_StatsFragment renders just the size/last-pushed cell
// for one repo, with no full-page chrome.
func TestUI_RegistryTags_StatsFragment(t *testing.T) {
	created := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	u := uiWithRegistry(t, &fakeRegistryUI{
		tags: []imgregistry.TagGroup{
			{Digest: "sha256:new", Tags: []string{"latest"}, Created: created, Size: 1000},
			{Digest: "sha256:old", Tags: []string{"v1"}, Created: older, Size: 2000},
		},
	})
	w := authedGet(t, u, "/ui/registry/engine?stats=1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	// Total size across both groups: 3000 bytes -> "2.9 KB" via formatBytes
	// (which uses 1024-based divisors but "KB"/"MB" suffixes, not "KiB").
	if !strings.Contains(body, "2.9 KB") {
		t.Fatalf("expected a formatted total size of 2.9 KB: %s", body)
	}
	// Newest Created is the one shown.
	if !strings.Contains(body, "2026-03-01") {
		t.Fatalf("expected the newest push date: %s", body)
	}
	if strings.Contains(body, "All repositories") {
		t.Fatalf("stats fragment must not carry full-page chrome: %s", body)
	}
}

// TestUI_RegistryTags_StatsFragment_TagsErrorRendersOKWithMarker is the
// Important-2 fix: a repo whose Tags() call fails must not leave the list
// page's lazy-loaded stats cell stuck on "loading…" forever. HTMX does not
// swap a non-2xx response by default (the shared u.renderError path used to
// handle this branch and returns one), so the fragment must render 200 with
// a visible failure marker instead.
func TestUI_RegistryTags_StatsFragment_TagsErrorRendersOKWithMarker(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		tagsErr: fmt.Errorf("registry unreachable: %w", imgregistry.ErrUnreachable),
	})
	w := authedGet(t, u, "/ui/registry/engine?stats=1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 even though Tags() failed (HTMX won't swap a non-2xx): body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "—") {
		t.Fatalf("expected a visible failure marker in the stats cell: %s", body)
	}
	if strings.Contains(body, "card") {
		t.Fatalf("stats cell must not render full error-card chrome: %s", body)
	}
}

// manifestDigestA and manifestDigestB stand in for real sha256 digests in
// tests below. imgregistry.ValidRef's digestRe demands >=32 hex characters
// after the "algo:" prefix (it's the same validator guarding the JSON API
// edge), so a short human-readable stand-in like "sha256:abc" or
// "sha256:idx" is itself an invalid ref and would 400 before ever reaching
// the fake — these are padded out to a valid length while keeping a
// recognizable prefix for the substring assertions below.
var (
	manifestDigestA = "sha256:abc" + strings.Repeat("0", 32-len("abc"))
	manifestDigestB = "sha256:1d0" + strings.Repeat("0", 32-len("1d0"))
)

// TestUI_RegistryManifest_ShowsLayersAndPullRef is the detail view's reason
// to exist: one digest's layers, config digest, and a copy-pasteable
// repo@digest reference.
func TestUI_RegistryManifest_ShowsLayersAndPullRef(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		manifests: map[string]imgregistry.Manifest{
			"engine/" + manifestDigestA: {
				Digest:       manifestDigestA,
				ConfigDigest: "sha256:cfg",
				Size:         3000,
				Layers: []imgregistry.Layer{
					{Digest: "sha256:layer1", Size: 1000},
					{Digest: "sha256:layer2", Size: 2000},
				},
			},
		},
	})
	w := authedGet(t, u, "/ui/registry/engine?manifest="+manifestDigestA)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{"sha256:layer1", "sha256:layer2", "sha256:cfg", "engine@sha256:abc"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q: %s", want, body)
		}
	}
}

// TestUI_RegistryManifest_MultiArchRendersNote: a manifest list has no
// layers of its own (imgregistry.Manifest leaves Layers empty for an index),
// so the view must say so rather than render an empty table that reads as
// broken.
func TestUI_RegistryManifest_MultiArchRendersNote(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		manifests: map[string]imgregistry.Manifest{
			"engine/" + manifestDigestB: {Digest: manifestDigestB}, // no layers, no config
		},
	})
	w := authedGet(t, u, "/ui/registry/engine?manifest="+manifestDigestB)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "No layers") {
		t.Fatalf("expected a no-layers note for a manifest list: %s", w.Body.String())
	}
}

// TestUI_RegistryManifest_InvalidRefIs400 mirrors the API edge: an
// unvalidated ref reaches a client that interpolates it into a registry
// request path.
func TestUI_RegistryManifest_InvalidRefIs400(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{manifests: map[string]imgregistry.Manifest{}})
	w := authedGet(t, u, "/ui/registry/engine?manifest=..%2F..%2Fetc%2Fpasswd")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
	}
}

// TestUI_RegistryTags_DigestLinksToDetail confirms the tag table's digest is
// a way into the detail view, not just a string — and that the link it
// produces actually resolves, not merely that it contains the right
// substring. It uses manifestDigestA (not a short fixture like "sha256:abc")
// because imgregistry.ValidRef demands >=32 hex characters after the
// "algo:" prefix; a shorter digest would produce a link that 400s, which is
// exactly the gap this test exists to close — the original version asserted
// only "manifest=sha256" appeared in the body, which is also true of a link
// to a reference nothing can ever resolve.
func TestUI_RegistryTags_DigestLinksToDetail(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		tags: []imgregistry.TagGroup{
			{Digest: manifestDigestA, Tags: []string{"latest"}, Created: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
		manifests: map[string]imgregistry.Manifest{
			"engine/" + manifestDigestA: {
				Digest:       manifestDigestA,
				ConfigDigest: "sha256:cfg",
				Size:         1000,
				Layers:       []imgregistry.Layer{{Digest: "sha256:layer1", Size: 1000}},
			},
		},
	})
	w := authedGet(t, u, "/ui/registry/engine")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()

	href := extractHref(t, body, "manifest=")
	w2 := authedGet(t, u, href)
	if w2.Code != http.StatusOK {
		t.Fatalf("following the tag table's digest link: status = %d, body = %s", w2.Code, w2.Body.String())
	}
	detailBody := w2.Body.String()
	if !strings.Contains(detailBody, "sha256:layer1") {
		t.Fatalf("detail view did not render the expected layer: %s", detailBody)
	}
	if !strings.Contains(detailBody, "engine@"+manifestDigestA) {
		t.Fatalf("detail view did not render the expected pull ref: %s", detailBody)
	}
}

// extractHref finds the first href="..." attribute in body whose value
// contains marker, and returns the (HTML-unescaped) value. t.Fatal's if none
// is found.
func extractHref(t *testing.T, body, marker string) string {
	t.Helper()
	const attr = `href="`
	idx := 0
	for {
		i := strings.Index(body[idx:], attr)
		if i == -1 {
			t.Fatalf("no href attribute found containing %q in: %s", marker, body)
		}
		start := idx + i + len(attr)
		end := strings.Index(body[start:], `"`)
		if end == -1 {
			t.Fatalf("unterminated href attribute in: %s", body)
		}
		val := body[start : start+end]
		if strings.Contains(val, marker) {
			return html.UnescapeString(val)
		}
		idx = start + end
	}
}

// TestUI_RegistryNavItem_HasIconAndActiveState guards the sidebar link's
// parity with its neighbours (Jobs, Tokens): an icon, and the active-state
// highlight that depends on the handler setting ActivePage. Both were missing
// when the page first shipped, and neither is visible from any other test —
// the registry page tests assert on #main content, not the surrounding nav.
func TestUI_RegistryNavItem_HasIconAndActiveState(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{catalog: []string{"engine"}})
	body := authedGet(t, u, "/ui/registry").Body.String()

	nav := body[strings.Index(body, `href="/ui/registry"`):]
	if end := strings.Index(nav, "</a>"); end != -1 {
		nav = nav[:end]
	}
	if !strings.Contains(nav, `class="ico"`) {
		t.Errorf("registry nav link has no icon, unlike every other nav item: %s", nav)
	}
	if !strings.Contains(nav, `class="active"`) {
		t.Errorf("registry nav link is not highlighted while on a registry page: %s", nav)
	}
	if !strings.Contains(nav, `preload="mouseover"`) {
		t.Errorf("registry nav link lacks preload, unlike its neighbours: %s", nav)
	}
}

// TestUI_RegistryPages_UseTheRealTableClass is a regression test for the
// defect that made these pages look unstyled: they were written against
// class="table", which has no rules in app.css at all, while every other
// page uses class="tbl" (padding, borders, header styling, and the
// responsive card-stacking mode keyed off data-label).
func TestUI_RegistryPages_UseTheRealTableClass(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	u := uiWithRegistry(t, &fakeRegistryUI{
		catalog: []string{"engine"},
		tags: []imgregistry.TagGroup{
			{Digest: manifestDigestA, Tags: []string{"latest"}, Created: created, Size: 10},
		},
		manifests: map[string]imgregistry.Manifest{
			"engine/" + manifestDigestA: {
				Digest: manifestDigestA,
				Layers: []imgregistry.Layer{{Digest: "sha256:layer", Size: 10}},
			},
		},
	})
	for _, path := range []string{
		"/ui/registry",
		"/ui/registry/engine",
		"/ui/registry/engine?manifest=" + manifestDigestA,
	} {
		body := authedGet(t, u, path).Body.String()
		if !strings.Contains(body, `class="tbl"`) {
			t.Errorf("%s: expected the styled table class, got none: %s", path, body)
		}
		if strings.Contains(body, `class="table"`) {
			t.Errorf("%s: uses class=\"table\", which has no CSS rules", path)
		}
		if !strings.Contains(body, "data-label=") {
			t.Errorf("%s: table cells lack data-label, breaking the responsive layout", path)
		}
	}
}
