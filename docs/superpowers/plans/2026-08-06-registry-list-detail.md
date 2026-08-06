# Registry List + Detail View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich the registry repo list with tag count (instant) plus size/last-pushed (lazy-loaded per row), and add a per-digest manifest detail view showing layers, config digest, and a copy-pasteable pull reference.

**Architecture:** Adds one cheap method (`TagCount`) to `imgregistry.Client` that wraps the existing private `listTags` with no manifest/blob resolution. The UI's repo-list handler fans `TagCount` out concurrently across repos (mirroring `dashboard`'s existing per-host fan-out), and each rendered row lazy-loads its expensive size/last-pushed stats via HTMX. Both the stats fragment and the new detail view fold onto the existing `GET /ui/registry/{repo...}` route as query-parameter branches (`?stats=1`, `?manifest=<ref>`), matching the convention already used by `?picker=1` there and `?manifest=<ref>` on the JSON API's own route.

**Tech Stack:** Go stdlib (`sync`, `context`), existing `internal/ui` html/template + HTMX conventions, existing `internal/imgregistry` client.

## Global Constraints

- Build tags are required for anything importing `internal/podman` (which includes `internal/api`, `internal/ui`, `server`): use `make build`/`make vet`/`make test`, or `-tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"` explicitly. `internal/imgregistry` itself has no such dependency — its own package tests run untagged.
- `TagCount` must NOT resolve manifests or config blobs. Its whole purpose is being cheap enough to call for every repo on one page load; a version that resolves manifests would reintroduce the ~21s-per-repo cost that made this design necessary.
- New UI views fold onto the existing `GET /ui/registry/{repo...}` route as query-parameter branches. Do NOT add new mux paths — Go 1.22's `{repo...}` wildcard must be a route's last segment, which is why the JSON API already folds `?manifest=<ref>` this way rather than using `.../manifests/{ref}`.
- `repo` must be validated with `imgregistry.ValidRepoName` and `ref` with `imgregistry.ValidRef` before reaching the client, on every branch — both validators already exist and are shared with the API edge.
- Adding a method to the `imgregistry.Client` interface breaks every implementer until updated. Three exist: `imgregistry.HTTPClient`, `internal/api/registry_test.go`'s `fakeRegistry`, and `internal/ui/handlers_registry_test.go`'s `fakeRegistryUI`.
- No "used by these instances" feature — deliberately deferred to OSS #219 (in-use-aware prune), which must build the same fleet-wide in-use-digest scan. Do not build a second, cruder version here.

---

### Task 1: `TagCount` on the registry client

**Files:**
- Modify: `internal/imgregistry/client.go`
- Modify: `internal/imgregistry/client_test.go`
- Modify: `internal/api/registry_test.go` (interface ripple only — add the method to `fakeRegistry`)
- Modify: `internal/ui/handlers_registry_test.go` (interface ripple only — add the method to `fakeRegistryUI`)

**Interfaces:**
- Consumes: the existing private `func (c *HTTPClient) listTags(ctx context.Context, repo string) ([]string, error)` in `client.go`.
- Produces: `TagCount(ctx context.Context, repo string) (int, error)` on the `Client` interface and on `HTTPClient`; the same method on both test doubles.

- [ ] **Step 1: Write the failing test**

Append to `internal/imgregistry/client_test.go`:

```go
// TestHTTPClient_TagCount_DoesNotResolveManifests is the load-bearing
// property of TagCount: the repo list calls it once per repo on a single
// page load, so it must cost exactly one tags-list call. A version that
// resolved manifests (as Tags does) would reintroduce the ~21s-per-repo
// cost this method exists to avoid.
func TestHTTPClient_TagCount_DoesNotResolveManifests(t *testing.T) {
	var manifestCalls, blobCalls int64
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["latest","v1","v2"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&manifestCalls, 1)
		w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/v2/engine/blobs/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&blobCalls, 1)
		w.Write([]byte(`{}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	n, err := c.TagCount(context.Background(), "engine")
	if err != nil {
		t.Fatalf("TagCount: %v", err)
	}
	if n != 3 {
		t.Fatalf("TagCount = %d, want 3", n)
	}
	if got := atomic.LoadInt64(&manifestCalls); got != 0 {
		t.Fatalf("TagCount made %d manifest requests, want 0 — it must not resolve manifests", got)
	}
	if got := atomic.LoadInt64(&blobCalls); got != 0 {
		t.Fatalf("TagCount made %d blob requests, want 0 — it must not resolve config blobs", got)
	}
}

// TestHTTPClient_TagCount_PaginatesLikeListTags confirms TagCount inherits
// listTags' Link-header pagination rather than counting only the first page.
func TestHTTPClient_TagCount_PaginatesLikeListTags(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("last") == "" {
			w.Header().Set("Link", `</v2/engine/tags/list?n=100&last=a>; rel="next"`)
			w.Write([]byte(`{"name":"engine","tags":["a","b"]}`))
			return
		}
		w.Write([]byte(`{"name":"engine","tags":["c"]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	n, err := c.TagCount(context.Background(), "engine")
	if err != nil {
		t.Fatalf("TagCount: %v", err)
	}
	if n != 3 {
		t.Fatalf("TagCount = %d, want 3 (2 on page 1 + 1 on page 2)", n)
	}
}
```

`atomic`, `context`, `net/http`, `net/http/httptest`, and `testing` are already imported in this file (phase 1's concurrency test added `sync/atomic`). Verify with `head -20 internal/imgregistry/client_test.go` and add anything missing.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /home/tej/projects/podman-api && go test ./internal/imgregistry/... -run TestHTTPClient_TagCount -v`
Expected: FAIL to compile — `c.TagCount` undefined.

- [ ] **Step 3: Implement `TagCount`**

In `internal/imgregistry/client.go`, add the method to the `Client` interface:

```go
// Client is the read-only surface this package exposes.
type Client interface {
	Catalog(ctx context.Context) ([]string, error)
	Tags(ctx context.Context, repo string) ([]TagGroup, error)
	TagCount(ctx context.Context, repo string) (int, error)
	Manifest(ctx context.Context, repo, ref string) (Manifest, error)
}
```

And add the implementation next to `Tags` (immediately after `Tags`'s closing brace, before `listTags`):

```go
// TagCount returns how many tags repo has, without resolving any of them.
// Deliberately NOT implemented as len(Tags(...)): Tags resolves every tag's
// manifest and every unique digest's config blob (~21s for the fleet's
// "engine" repo), while the repo-list page calls this once per repo on a
// single page load. One tags-list call — paginated, but nothing more — is
// the whole point.
//
// Note this counts tags, not unique digests, so it can exceed len(Tags(...))
// when several tags share a digest. That is the intended meaning for a
// "how many tags does this repo have" column.
func (c *HTTPClient) TagCount(ctx context.Context, repo string) (int, error) {
	tags, err := c.listTags(ctx, repo)
	if err != nil {
		return 0, err
	}
	return len(tags), nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd /home/tej/projects/podman-api && go test ./internal/imgregistry/... -run TestHTTPClient_TagCount -v`
Expected: PASS (both tests).

- [ ] **Step 5: Add the method to both test doubles (interface ripple)**

In `internal/api/registry_test.go`, add to `fakeRegistry` (after its `Tags` method) — `internal/api` has no route that calls this, but the double must satisfy the interface:

```go
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
```

In `internal/ui/handlers_registry_test.go`, add to `fakeRegistryUI` (after its `Tags` method). This one IS exercised by Task 2's tests, so it needs per-repo control and an error path:

```go
// tagCounts, when non-nil, is what TagCount returns per repo; a repo absent
// from the map returns an error, letting a test drive the "count failed to
// resolve" cell.
// (add this field to the fakeRegistryUI struct alongside catalog/tags)

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
```

Add the `tagCounts map[string]int` field to the `fakeRegistryUI` struct, and add `"fmt"` to that file's imports if absent.

- [ ] **Step 6: Verify the whole tree builds and tests pass**

Run: `cd /home/tej/projects/podman-api && gofmt -l . && make build && make vet && make test`
Expected: `gofmt -l .` prints nothing; build, vet, and the full test suite all clean.

- [ ] **Step 7: Commit**

```bash
git add internal/imgregistry/client.go internal/imgregistry/client_test.go internal/api/registry_test.go internal/ui/handlers_registry_test.go
git commit -m "feat(imgregistry): add cheap TagCount for the repo-list page"
```

---

### Task 2: Repo list — concurrent tag counts + lazy stats fragment

**Files:**
- Modify: `internal/ui/handlers_registry.go`
- Modify: `internal/ui/templates/registry-repos.html`
- Create: `internal/ui/templates/registry-repo-stats.html`
- Modify: `internal/ui/handlers_registry_test.go`

**Interfaces:**
- Consumes: `imgregistry.Client.TagCount` (Task 1), `imgregistry.Client.Tags`, `imgregistry.ValidRepoName`, `formatBytes` (already registered in `ui.go`'s template FuncMap).
- Produces: a `repoSummary{Name string; TagCount int; CountOK bool}` view model rendered by `registry-repos`; a `?stats=1` branch on `registryTags` rendering the `registry-repo-stats` block.

- [ ] **Step 1: Write the failing tests**

Append to `internal/ui/handlers_registry_test.go`:

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -run TestUI_Registry -v`
Expected: the four new tests FAIL (no counts rendered, no `?stats=1` link, `?stats=1` renders the full tag table instead of a fragment).

- [ ] **Step 3: Rewrite `registryRepos` to fan out tag counts**

Replace `registryRepos` in `internal/ui/handlers_registry.go`:

```go
// repoCountTimeout bounds one repo's TagCount during the list page's
// fan-out. Mirrors hostFetchTimeout's role for the dashboard's per-host
// fan-out: one unreachable repo must not stall the whole page.
const repoCountTimeout = 5 * time.Second

// repoSummary is one row of the registry list page. CountOK distinguishes
// "this repo has zero tags" from "we could not find out", which the template
// renders differently — collapsing them would report an unreachable repo as
// empty, the same class of misleading signal the API's 404-vs-502 split
// exists to prevent.
type repoSummary struct {
	Name     string
	TagCount int
	CountOK  bool
}

func (u *UI) registryRepos(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Registry == nil {
		http.NotFound(w, r)
		return
	}
	repos, err := u.cfg.Registry.Catalog(r.Context())
	if err != nil {
		u.renderError(w, r, err)
		return
	}

	// Fan out the (cheap, one-call) tag counts concurrently — same pattern
	// as dashboard's per-host fan-out in handlers_hosts.go. Each goroutine
	// writes only its own slice index, so no synchronization beyond the
	// WaitGroup is needed. Size and last-pushed are NOT fetched here: they
	// need full Tags() resolution, which the template lazy-loads per row.
	summaries := make([]repoSummary, len(repos))
	var wg sync.WaitGroup
	for i, repo := range repos {
		wg.Add(1)
		go func(i int, repo string) {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(r.Context(), repoCountTimeout)
			defer cancel()
			s := repoSummary{Name: repo}
			if n, err := u.cfg.Registry.TagCount(rctx, repo); err == nil {
				s.TagCount, s.CountOK = n, true
			}
			summaries[i] = s
		}(i, repo)
	}
	wg.Wait()

	u.render(w, r, http.StatusOK, "registry-repos", u.pageData(map[string]any{
		"Repos": summaries,
	}))
}
```

Add `"sync"` to the file's imports. Drop the previous handler's vestigial `"ActiveHost": ""` key — no host is being highlighted on this page.

- [ ] **Step 4: Add the `?stats=1` branch to `registryTags`**

In the same file, inside `registryTags`, immediately after the existing `Tags()` call and its error check (so the branch reuses that already-fetched result), add — placing it BEFORE the existing `?picker=1` branch:

```go
	if r.URL.Query().Get("stats") == "1" {
		var total int64
		var newest time.Time
		for _, g := range groups {
			total += g.Size
			if g.Created.After(newest) {
				newest = g.Created
			}
		}
		u.render(w, r, http.StatusOK, "registry-repo-stats", u.pageData(map[string]any{
			"TotalSize": total,
			"Newest":    newest,
		}))
		return
	}
```

- [ ] **Step 5: Update the list template**

Replace `internal/ui/templates/registry-repos.html`:

```html
{{define "registry-repos"}}
<h2>Registry</h2>
<div class="card"><div class="card-b">
  <table class="table">
    <thead><tr><th>Repository</th><th>Tags</th><th>Size / last pushed</th></tr></thead>
    <tbody>
    {{range .Repos}}
      <tr>
        <td><a href="/ui/registry/{{.Name}}" hx-get="/ui/registry/{{.Name}}" hx-target="#main" hx-push-url="true">{{.Name}}</a></td>
        <td>{{if .CountOK}}{{.TagCount}}{{else}}<span class="subtitle">?</span>{{end}}</td>
        <td hx-get="/ui/registry/{{.Name}}?stats=1" hx-trigger="load" hx-swap="innerHTML"><span class="subtitle">loading…</span></td>
      </tr>
    {{else}}
      <tr><td colspan="3" class="subtitle">No repositories found.</td></tr>
    {{end}}
    </tbody>
  </table>
</div></div>
{{end}}
```

- [ ] **Step 6: Create the stats fragment template**

Create `internal/ui/templates/registry-repo-stats.html`:

```html
{{define "registry-repo-stats"}}{{formatBytes .TotalSize}}{{if not .Newest.IsZero}} <span class="subtitle">· {{.Newest.Format "2006-01-02"}}</span>{{end}}{{end}}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -v`
Expected: all PASS, including the four new tests and the pre-existing registry tests.

- [ ] **Step 8: Verify the whole tree**

Run: `cd /home/tej/projects/podman-api && gofmt -l . && make build && make vet && make test`
Expected: all clean.

- [ ] **Step 9: Commit**

```bash
git add internal/ui/handlers_registry.go internal/ui/handlers_registry_test.go internal/ui/templates/registry-repos.html internal/ui/templates/registry-repo-stats.html
git commit -m "feat(ui): show tag counts and lazy-loaded size/date on the registry list"
```

---

### Task 3: Manifest detail view

**Files:**
- Modify: `internal/ui/handlers_registry.go`
- Create: `internal/ui/templates/registry-manifest.html`
- Modify: `internal/ui/templates/registry-tags.html`
- Modify: `internal/ui/handlers_registry_test.go`

**Interfaces:**
- Consumes: `imgregistry.Client.Manifest`, `imgregistry.ValidRef`, `imgregistry.ValidRepoName`, `formatBytes`.
- Produces: a `?manifest=<ref>` branch on `registryTags` rendering the `registry-manifest` block.

- [ ] **Step 1: Write the failing tests**

Append to `internal/ui/handlers_registry_test.go`. Note `fakeRegistryUI.Manifest` currently returns a zero `Manifest` unconditionally — give it a map first so a test can drive real content. Add a `manifests map[string]imgregistry.Manifest` field to the struct (keyed `"repo/ref"`, mirroring `internal/api`'s `fakeRegistry`) and replace its `Manifest` method with:

```go
func (f *fakeRegistryUI) Manifest(ctx context.Context, repo, ref string) (imgregistry.Manifest, error) {
	m, ok := f.manifests[repo+"/"+ref]
	if !ok {
		return imgregistry.Manifest{}, fmt.Errorf("manifest %s/%s: not found: %w", repo, ref, imgregistry.ErrNotFound)
	}
	return m, nil
}
```

Then the tests:

```go
// TestUI_RegistryManifest_ShowsLayersAndPullRef is the detail view's reason
// to exist: one digest's layers, config digest, and a copy-pasteable
// repo@digest reference.
func TestUI_RegistryManifest_ShowsLayersAndPullRef(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		manifests: map[string]imgregistry.Manifest{
			"engine/sha256:abc": {
				Digest:       "sha256:abc",
				ConfigDigest: "sha256:cfg",
				Size:         3000,
				Layers: []imgregistry.Layer{
					{Digest: "sha256:layer1", Size: 1000},
					{Digest: "sha256:layer2", Size: 2000},
				},
			},
		},
	})
	w := authedGet(t, u, "/ui/registry/engine?manifest=sha256:abc")
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
			"engine/sha256:idx": {Digest: "sha256:idx"}, // no layers, no config
		},
	})
	w := authedGet(t, u, "/ui/registry/engine?manifest=sha256:idx")
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
// a way into the detail view, not just a string.
func TestUI_RegistryTags_DigestLinksToDetail(t *testing.T) {
	u := uiWithRegistry(t, &fakeRegistryUI{
		tags: []imgregistry.TagGroup{
			{Digest: "sha256:abc", Tags: []string{"latest"}, Created: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
	})
	w := authedGet(t, u, "/ui/registry/engine")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	// html/template URL-encodes the digest's ":" in an href context.
	if !strings.Contains(w.Body.String(), "manifest=sha256") {
		t.Fatalf("expected the digest to link to its detail view: %s", w.Body.String())
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -run TestUI_Registry -v`
Expected: the four new tests FAIL.

- [ ] **Step 3: Add the `?manifest=` branch**

In `registryTags` (`internal/ui/handlers_registry.go`), add this branch **before** the `Tags()` call and its `context.WithTimeout` — a single manifest lookup needs neither the whole repo's resolution nor the long tags timeout — but **after** the existing `ValidRepoName` check:

```go
	// A single-digest detail view needs one Manifest lookup, not the whole
	// repo's Tags() resolution — so this branch returns before that cost is
	// paid. Mirrors the JSON API's own ?manifest=<ref> fold on the same
	// route shape (internal/api/registry.go), which exists because Go's
	// {repo...} wildcard must be a route's last segment.
	if ref := r.URL.Query().Get("manifest"); ref != "" {
		if !imgregistry.ValidRef(ref) {
			http.Error(w, "invalid manifest reference", http.StatusBadRequest)
			return
		}
		m, err := u.cfg.Registry.Manifest(r.Context(), repo, ref)
		if err != nil {
			u.renderError(w, r, err)
			return
		}
		u.render(w, r, http.StatusOK, "registry-manifest", u.pageData(map[string]any{
			"Repo":     repo,
			"Ref":      ref,
			"Manifest": m,
		}))
		return
	}
```

- [ ] **Step 4: Create the detail template**

Create `internal/ui/templates/registry-manifest.html`:

```html
{{define "registry-manifest"}}
<h2>{{.Repo}}</h2>
<p class="subtitle"><a href="/ui/registry/{{.Repo}}" hx-get="/ui/registry/{{.Repo}}" hx-target="#main" hx-push-url="true">&larr; {{.Repo}} tags</a></p>
<div class="card"><div class="card-b">
  <div class="field">
    <label>Pull reference</label>
    <code>{{.Repo}}@{{.Manifest.Digest}}</code>
  </div>
  <div class="field">
    <label>Digest</label>
    <code>{{.Manifest.Digest}}</code>
  </div>
  {{if .Manifest.ConfigDigest}}
  <div class="field">
    <label>Config digest</label>
    <code>{{.Manifest.ConfigDigest}}</code>
  </div>
  {{end}}
  <div class="field">
    <label>Total size</label>
    {{formatBytes .Manifest.Size}}
  </div>
</div></div>
<div class="card"><div class="card-b">
  <table class="table">
    <thead><tr><th>Layer</th><th>Size</th></tr></thead>
    <tbody>
    {{range .Manifest.Layers}}
      <tr><td><code>{{.Digest}}</code></td><td>{{formatBytes .Size}}</td></tr>
    {{else}}
      <tr><td colspan="2" class="subtitle">No layers — this is a multi-arch manifest list; each platform entry has its own layers.</td></tr>
    {{end}}
    </tbody>
  </table>
</div></div>
{{end}}
```

- [ ] **Step 5: Link the tag table's digest to the detail view**

In `internal/ui/templates/registry-tags.html`, replace the digest cell:

```html
        <td><code>{{.Digest}}</code></td>
```

with:

```html
        <td><a href="/ui/registry/{{$.Repo}}?manifest={{.Digest}}" hx-get="/ui/registry/{{$.Repo}}?manifest={{.Digest}}" hx-target="#main" hx-push-url="true"><code>{{.Digest}}</code></a></td>
```

(`$.Repo` reaches the page-level repo from inside the `range` over `.Groups`.)

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -v`
Expected: all PASS.

- [ ] **Step 7: Verify the whole tree**

Run: `cd /home/tej/projects/podman-api && gofmt -l . && make build && make vet && make test`
Expected: all clean.

- [ ] **Step 8: Commit**

```bash
git add internal/ui/handlers_registry.go internal/ui/handlers_registry_test.go internal/ui/templates/registry-manifest.html internal/ui/templates/registry-tags.html
git commit -m "feat(ui): add per-digest manifest detail view"
```

---

## After all tasks land

Open a PR against `main` in `IoTReadyNext/podman-api` referencing
`docs/superpowers/specs/2026-08-06-registry-list-detail-design.md`. Deploying
follows the established chain (tag an OSS release, `make bump` in
`podman-api-pro`, `make build-linux`, scp to engine-infra, restart the user
service) — out of scope for this plan.
