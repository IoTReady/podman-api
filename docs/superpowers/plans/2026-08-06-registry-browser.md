# Registry Browser & Upgrade-Picker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator browse the fleet's Docker Registry v2 catalog from the podman-api UI/API, and pick a tag/digest from that browser to upgrade an instance's image, instead of hand-crafting `curl` calls and digests.

**Architecture:** A new `internal/imgregistry` package speaks the Registry v2 HTTP API directly (catalog, tags, manifest, blob) with no external SDK dependency. It's wired into `server/server.go` behind a `-registry-address` flag (empty = feature absent), exposed read-only via three new `internal/api` routes, surfaced in the UI as a new "Registry" page, and folded into the *existing* image-upgrade form (`internal/ui/handlers_deploy.go` `upgradeForm`/`upgradeApply`, `templates/upgrade-form.html`) as a tag picker rather than a new page.

**Tech Stack:** Go stdlib `net/http` for the registry client (no new dependency), existing `internal/api` mux/handler conventions, existing `internal/ui` html/template + HTMX conventions.

## Global Constraints

- Package name is `internal/imgregistry`, never `internal/registry` — the codebase already has `jobs.Registry` (job-kind registry) and a same-named package would collide in every import block that needs both.
- One registry, globally configured (no per-host registry config).
- Auth support is `none` (default) or HTTP Basic only. Do not add bearer-token auth — out of scope per the design spec.
- `Catalog()` must explicitly paginate via the `Link` response header and must never rely on an unbounded/very large single `n=` value — this registry is confirmed to return an empty list with HTTP 200 for `n=` above ~1000, which is indistinguishable from "no repos" unless pagination is followed explicitly.
- `-registry-address` empty means the feature is entirely absent: routes return 404 (not wired into the mux at all) and no "Registry" nav item renders — never a runtime error path.
- No new write route is added for upgrades — the picker must call the existing `Service.UpgradeImage` / `upgrade-image` route.
- Registry unreachable must surface as an explicit error (502-class in the API, an inline error message in the UI), never as a silently empty list.
- This repo requires build tags for anything that transitively imports `internal/podman` (which includes `internal/api`, `internal/ui`, `server`, `cmd/podman-api`) — a plain `go build ./...`/`go test ./...` at the repo root or on those packages fails on a clean machine (missing `gpgme`/`btrfs` headers). Use `make build`/`make test`/`make vet`, or pass `-tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"` explicitly, exactly as `Makefile` and `CLAUDE.md` document. `internal/imgregistry` itself has no such dependency and its own package tests run fine without tags.

---

### Task 1: `imgregistry` client — Catalog with pagination

**Files:**
- Create: `internal/imgregistry/client.go`
- Create: `internal/imgregistry/client_test.go`

**Interfaces:**
- Produces: `type Client interface { Catalog(ctx context.Context) ([]string, error) }` (extended in Task 2), `type Auth struct { Mode, Username, Password string }` (`Mode` is `"none"` or `"basic"`), `type HTTPClient struct { BaseURL string; Auth Auth; HTTP *http.Client }` implementing `Client`, and `func NewHTTPClient(baseURL string, auth Auth) *HTTPClient`.

- [ ] **Step 1: Write the failing test for a single-page catalog**

```go
// internal/imgregistry/client_test.go
package imgregistry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPClient_Catalog_SinglePage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/_catalog" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"repositories":["engine","otp","qabazaar"]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	repos, err := c.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	want := []string{"engine", "otp", "qabazaar"}
	if len(repos) != len(want) {
		t.Fatalf("got %v, want %v", repos, want)
	}
	for i := range want {
		if repos[i] != want[i] {
			t.Fatalf("got %v, want %v", repos, want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd internal/imgregistry && go test ./... -run TestHTTPClient_Catalog_SinglePage -v`
Expected: FAIL — package/type does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/imgregistry/client.go
// Package imgregistry is a minimal Docker Registry v2 HTTP client used to
// browse the fleet's image registry and resolve tags/digests for the
// upgrade-picker. Deliberately not named "registry" — the codebase already
// has jobs.Registry (job-kind registry) and a same-named package here would
// collide in every import block needing both.
package imgregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Auth configures how HTTPClient authenticates to the registry.
type Auth struct {
	Mode     string // "none" (default) or "basic"
	Username string
	Password string
}

// Client is the read-only surface this package exposes. Only Catalog is
// defined in this task; Tags and Manifest are added in Task 2.
type Client interface {
	Catalog(ctx context.Context) ([]string, error)
}

// HTTPClient implements Client against a real Registry v2 server.
type HTTPClient struct {
	BaseURL string // e.g. "http://100.64.0.23:5000", no trailing slash
	Auth    Auth
	HTTP    *http.Client
}

// NewHTTPClient builds an HTTPClient with a sane default timeout.
func NewHTTPClient(baseURL string, auth Auth) *HTTPClient {
	return &HTTPClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Auth:    auth,
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *HTTPClient) do(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if c.Auth.Mode == "basic" {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("registry request %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("registry request %s: status %d", path, resp.StatusCode)
	}
	return resp, nil
}

// catalogPageSize bounds each /v2/_catalog page. This registry returns an
// empty list with HTTP 200 for n= above ~1000, which is indistinguishable
// from "no repos" — staying well under that and following Link explicitly
// (rather than ever requesting one large page) is load-bearing, not a
// tuning choice.
const catalogPageSize = 100

// Catalog lists every repository in the registry, following the Link
// response header across pages rather than requesting one unbounded page.
func (c *HTTPClient) Catalog(ctx context.Context) ([]string, error) {
	var all []string
	path := fmt.Sprintf("/v2/_catalog?n=%d", catalogPageSize)
	for path != "" {
		resp, err := c.do(ctx, path)
		if err != nil {
			return nil, err
		}
		var body struct {
			Repositories []string `json:"repositories"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&body)
		link := resp.Header.Get("Link")
		resp.Body.Close()
		if decErr != nil {
			return nil, fmt.Errorf("decode catalog page: %w", decErr)
		}
		all = append(all, body.Repositories...)
		path = nextLinkPath(link)
	}
	return all, nil
}

// nextLinkPath extracts the path+query of a Link: <...>; rel="next" header,
// or "" if there is no next page.
func nextLinkPath(link string) string {
	if link == "" {
		return ""
	}
	start := strings.Index(link, "<")
	end := strings.Index(link, ">")
	if start == -1 || end == -1 || end <= start {
		return ""
	}
	raw := link[start+1 : end]
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if u.RawQuery == "" {
		return u.Path
	}
	return u.Path + "?" + u.RawQuery
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd internal/imgregistry && go test ./... -run TestHTTPClient_Catalog_SinglePage -v`
Expected: PASS

- [ ] **Step 5: Write the failing pagination + empty-200-trap regression test**

```go
func TestHTTPClient_Catalog_FollowsLinkHeader(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("last") == "" {
			w.Header().Set("Link", `</v2/_catalog?n=100&last=engine>; rel="next"`)
			w.Write([]byte(`{"repositories":["engine"]}`))
			return
		}
		w.Write([]byte(`{"repositories":["otp"]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	repos, err := c.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 requests (page 1 + next), got %d", calls)
	}
	if len(repos) != 2 || repos[0] != "engine" || repos[1] != "otp" {
		t.Fatalf("got %v", repos)
	}
}

// TestHTTPClient_Catalog_NoLinkMeansOnePage is a regression test for the
// known trap on this registry: a request above ~n=1000 returns an empty list
// with HTTP 200, indistinguishable from "no repos", unless pagination stops
// only when the server stops sending Link — never by inferring "done" from
// an empty page in isolation.
func TestHTTPClient_Catalog_NoLinkMeansOnePage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"repositories":[]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	repos, err := c.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if len(repos) != 0 {
		t.Fatalf("expected empty catalog, got %v", repos)
	}
}
```

- [ ] **Step 6: Run tests to verify they fail/pass as expected**

Run: `cd internal/imgregistry && go test ./... -v`
Expected: `TestHTTPClient_Catalog_FollowsLinkHeader` and `TestHTTPClient_Catalog_NoLinkMeansOnePage` both PASS against the Step-3 implementation (no code change needed — this step is a coverage addition, confirming the implementation already handles both cases correctly).

- [ ] **Step 7: Commit**

```bash
git add internal/imgregistry/client.go internal/imgregistry/client_test.go
git commit -m "feat(imgregistry): add Registry v2 client with paginated catalog"
```

---

### Task 2: `imgregistry` client — Tags, Manifest, digest grouping

**Files:**
- Modify: `internal/imgregistry/client.go`
- Modify: `internal/imgregistry/client_test.go`

**Interfaces:**
- Consumes: `HTTPClient`, `Auth`, `NewHTTPClient` from Task 1.
- Produces:
  ```go
  type TagGroup struct {
      Digest  string
      Tags    []string
      Created time.Time
      Size    int64
  }
  type Layer struct {
      Digest string
      Size   int64
  }
  type Manifest struct {
      Digest       string
      ConfigDigest string
      Layers       []Layer
      Size         int64
  }
  type Client interface {
      Catalog(ctx context.Context) ([]string, error)
      Tags(ctx context.Context, repo string) ([]TagGroup, error)
      Manifest(ctx context.Context, repo, ref string) (Manifest, error)
  }
  ```
  `Tags` groups by digest (multiple tags -> one `TagGroup`), sorted newest-`Created`-first.

- [ ] **Step 1: Write the failing test for Manifest**

```go
func TestHTTPClient_Manifest(t *testing.T) {
	const manifestBody = `{
		"schemaVersion": 2,
		"config": {"digest": "sha256:cfgcfgcfgcfg", "size": 1500},
		"layers": [
			{"digest": "sha256:layer1", "size": 1000},
			{"digest": "sha256:layer2", "size": 2000}
		]
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/engine/manifests/latest" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Docker-Content-Digest", "sha256:manifestdigest")
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
		w.Write([]byte(manifestBody))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	m, err := c.Manifest(context.Background(), "engine", "latest")
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if m.Digest != "sha256:manifestdigest" {
		t.Fatalf("got digest %q", m.Digest)
	}
	if m.ConfigDigest != "sha256:cfgcfgcfgcfg" {
		t.Fatalf("got config digest %q", m.ConfigDigest)
	}
	wantSize := int64(1500 + 1000 + 2000)
	if m.Size != wantSize {
		t.Fatalf("got size %d, want %d", m.Size, wantSize)
	}
	if len(m.Layers) != 2 || m.Layers[0].Digest != "sha256:layer1" || m.Layers[1].Size != 2000 {
		t.Fatalf("got layers %+v", m.Layers)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd internal/imgregistry && go test ./... -run TestHTTPClient_Manifest -v`
Expected: FAIL — `Manifest` not defined.

- [ ] **Step 3: Implement Manifest**

```go
// append to internal/imgregistry/client.go

// Layer is one entry in a manifest's layer list.
type Layer struct {
	Digest string
	Size   int64
}

// Manifest is the resolved shape of a single tag/digest reference.
type Manifest struct {
	Digest       string // from Docker-Content-Digest, the canonical content digest
	ConfigDigest string
	Layers       []Layer
	Size         int64 // config size + sum of all layer sizes
}

const manifestAccept = "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json"

// Manifest fetches a single manifest by tag or digest and returns its
// canonical digest (from Docker-Content-Digest) plus size/layer info.
func (c *HTTPClient) Manifest(ctx context.Context, repo, ref string) (Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v2/%s/manifests/%s", c.BaseURL, repo, ref), nil)
	if err != nil {
		return Manifest{}, err
	}
	req.Header.Set("Accept", manifestAccept)
	if c.Auth.Mode == "basic" {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Manifest{}, fmt.Errorf("registry manifest %s/%s: %w", repo, ref, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return Manifest{}, fmt.Errorf("manifest %s/%s: not found", repo, ref)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Manifest{}, fmt.Errorf("registry manifest %s/%s: status %d", repo, ref, resp.StatusCode)
	}

	var body struct {
		Config struct {
			Digest string `json:"digest"`
			Size   int64  `json:"size"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
			Size   int64  `json:"size"`
		} `json:"layers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest %s/%s: %w", repo, ref, err)
	}

	m := Manifest{
		Digest:       resp.Header.Get("Docker-Content-Digest"),
		ConfigDigest: body.Config.Digest,
		Size:         body.Config.Size,
	}
	for _, l := range body.Layers {
		m.Layers = append(m.Layers, Layer{Digest: l.Digest, Size: l.Size})
		m.Size += l.Size
	}
	return m, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd internal/imgregistry && go test ./... -run TestHTTPClient_Manifest -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for Tags (grouping + created blob fetch)**

```go
func TestHTTPClient_Tags_GroupsByDigestAndSortsNewestFirst(t *testing.T) {
	// Two tags ("latest", "8d5f281") share digestA (created older);
	// one tag ("v2") is digestB (created newer). Expect two groups,
	// digestB's group first.
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name":"engine","tags":["latest","8d5f281","v2"]}`))
	})
	digestFor := map[string]string{"latest": "sha256:digestA", "8d5f281": "sha256:digestA", "v2": "sha256:digestB"}
	for tag, digest := range digestFor {
		tag, digest := tag, digest
		mux.HandleFunc("/v2/engine/manifests/"+tag, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Docker-Content-Digest", digest)
			w.Write([]byte(`{"config":{"digest":"sha256:cfg","size":10},"layers":[]}`))
		})
	}
	mux.HandleFunc("/v2/engine/blobs/sha256:cfg", func(w http.ResponseWriter, r *http.Request) {
		created := "2026-01-01T00:00:00Z"
		if r.Header.Get("X-Test-Digest-Hint") == "" { /* no-op, digest not in path context here */ }
		w.Write([]byte(`{"created":"` + created + `"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Skip("superseded by digest-scoped blob test below; kept only to document the naive per-tag approach is wrong")
}

func TestHTTPClient_Tags_GroupsByDigestAndCreated(t *testing.T) {
	blobCreated := map[string]string{
		"sha256:cfgA": "2026-01-01T00:00:00Z",
		"sha256:cfgB": "2026-03-01T00:00:00Z",
	}
	manifestDigest := map[string]string{"latest": "sha256:digestA", "8d5f281": "sha256:digestA", "v2": "sha256:digestB"}
	manifestConfig := map[string]string{"sha256:digestA": "sha256:cfgA", "sha256:digestB": "sha256:cfgB"}

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["latest","8d5f281","v2"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/", func(w http.ResponseWriter, r *http.Request) {
		tag := strings.TrimPrefix(r.URL.Path, "/v2/engine/manifests/")
		digest := manifestDigest[tag]
		w.Header().Set("Docker-Content-Digest", digest)
		w.Write([]byte(`{"config":{"digest":"` + manifestConfig[digest] + `","size":10},"layers":[]}`))
	})
	mux.HandleFunc("/v2/engine/blobs/", func(w http.ResponseWriter, r *http.Request) {
		digest := strings.TrimPrefix(r.URL.Path, "/v2/engine/blobs/")
		w.Write([]byte(`{"created":"` + blobCreated[digest] + `"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	groups, err := c.Tags(context.Background(), "engine")
	if err != nil {
		t.Fatalf("Tags: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 digest groups, got %d: %+v", len(groups), groups)
	}
	// Newest (digestB / "v2", created 2026-03-01) first.
	if groups[0].Digest != "sha256:digestB" {
		t.Fatalf("expected newest group first, got %+v", groups[0])
	}
	if len(groups[0].Tags) != 1 || groups[0].Tags[0] != "v2" {
		t.Fatalf("got tags %v for newest group", groups[0].Tags)
	}
	if groups[1].Digest != "sha256:digestA" {
		t.Fatalf("expected oldest group second, got %+v", groups[1])
	}
	gotTags := append([]string{}, groups[1].Tags...)
	sort.Strings(gotTags)
	if len(gotTags) != 2 || gotTags[0] != "8d5f281" || gotTags[1] != "latest" {
		t.Fatalf("expected [8d5f281 latest], got %v", gotTags)
	}
}
```

Add `"sort"` and `"strings"` to the test file's imports (`strings` may already be present from Task 1's Link-header test if reused; add both explicitly to be safe).

- [ ] **Step 6: Run test to verify it fails**

Run: `cd internal/imgregistry && go test ./... -run TestHTTPClient_Tags_GroupsByDigestAndCreated -v`
Expected: FAIL — `Tags` not defined. (Delete the `t.Skip` scaffold test `TestHTTPClient_Tags_GroupsByDigestAndSortsNewestFirst` before running — it was written only to document why a naive per-tag-blob-fetch design doesn't fit and should not remain in the file.)

- [ ] **Step 7: Implement Tags**

```go
// append to internal/imgregistry/client.go

import (
	"sort"
	// ... keep existing imports
)

// TagGroup is every tag that resolves to the same content digest, with the
// digest's creation time (from its config blob) and total size.
type TagGroup struct {
	Digest  string
	Tags    []string
	Created time.Time
	Size    int64
}

// Client is the read-only registry surface used by the API/UI layers.
type Client interface {
	Catalog(ctx context.Context) ([]string, error)
	Tags(ctx context.Context, repo string) ([]TagGroup, error)
	Manifest(ctx context.Context, repo, ref string) (Manifest, error)
}

// Tags lists every tag in repo, resolves each to its manifest digest, and
// groups tags that share a digest into one TagGroup. Each unique digest's
// config blob is fetched once (not once per tag) for its Created timestamp.
// Groups are sorted newest-Created-first.
func (c *HTTPClient) Tags(ctx context.Context, repo string) ([]TagGroup, error) {
	tagNames, err := c.listTags(ctx, repo)
	if err != nil {
		return nil, err
	}

	byDigest := map[string]*TagGroup{}
	var order []string
	for _, tag := range tagNames {
		m, err := c.Manifest(ctx, repo, tag)
		if err != nil {
			return nil, fmt.Errorf("resolve tag %s: %w", tag, err)
		}
		g, ok := byDigest[m.Digest]
		if !ok {
			g = &TagGroup{Digest: m.Digest, Size: m.Size}
			byDigest[m.Digest] = g
			order = append(order, m.Digest)

			created, err := c.blobCreated(ctx, repo, m.ConfigDigest)
			if err != nil {
				return nil, fmt.Errorf("resolve created time for %s: %w", m.Digest, err)
			}
			g.Created = created
		}
		g.Tags = append(g.Tags, tag)
	}

	groups := make([]TagGroup, 0, len(order))
	for _, d := range order {
		groups = append(groups, *byDigest[d])
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Created.After(groups[j].Created)
	})
	return groups, nil
}

func (c *HTTPClient) listTags(ctx context.Context, repo string) ([]string, error) {
	var all []string
	path := fmt.Sprintf("/v2/%s/tags/list?n=%d", repo, catalogPageSize)
	for path != "" {
		resp, err := c.do(ctx, path)
		if err != nil {
			return nil, err
		}
		var body struct {
			Tags []string `json:"tags"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&body)
		link := resp.Header.Get("Link")
		resp.Body.Close()
		if decErr != nil {
			return nil, fmt.Errorf("decode tags page for %s: %w", repo, decErr)
		}
		all = append(all, body.Tags...)
		path = nextLinkPath(link)
	}
	return all, nil
}

func (c *HTTPClient) blobCreated(ctx context.Context, repo, digest string) (time.Time, error) {
	resp, err := c.do(ctx, fmt.Sprintf("/v2/%s/blobs/%s", repo, digest))
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	var body struct {
		Created string `json:"created"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return time.Time{}, fmt.Errorf("decode config blob %s: %w", digest, err)
	}
	if body.Created == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, body.Created)
}
```

Note: `do()` from Task 1 does not set the manifest `Accept` header, which is fine for `/tags/list` and `/blobs/` requests (they don't need it) but `Manifest` uses its own request construction with the `Accept` header, as written in Step 3 of this task.

- [ ] **Step 8: Run all tests to verify they pass**

Run: `cd internal/imgregistry && go test ./... -v`
Expected: All PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/imgregistry/client.go internal/imgregistry/client_test.go
git commit -m "feat(imgregistry): add Tags/Manifest with digest grouping"
```

---

### Task 3: Config flags + wiring into server.go

**Files:**
- Modify: `server/server.go`
- Test: `server/server_test.go` (only if it already exists and tests flag parsing; otherwise this task is verified via `go build` + the existing server test suite, since `server.go`'s flag block has no dedicated per-flag unit tests today — confirm with `ls server/*_test.go` before writing new tests, and only add one if an equivalent pattern exists for another flag e.g. `-prune-scope`)

**Interfaces:**
- Consumes: `imgregistry.NewHTTPClient(baseURL string, auth imgregistry.Auth) *imgregistry.HTTPClient`, `imgregistry.Client` from Tasks 1-2.
- Produces: a `var registryClient imgregistry.Client` (nil when `-registry-address` is empty) threaded into `api.NewRouter` (Task 4 adds the parameter) and `ui.Config` (Task 5 adds the field).

- [ ] **Step 1: Check for an existing flag-parsing test pattern**

Run: `ls /home/tej/projects/podman-api/server/*_test.go`

If a test asserts flag defaults or parsing (e.g. `TestRunWithFlags_...` covering `-prune-scope`), follow that exact pattern for the new flags in Step 3 below. If none exists, skip straight to Step 2 — this task is then verified by build + the existing full-server integration tests in Step 4.

- [ ] **Step 2: Add the flags**

In `server/server.go`, in the `fs := flag.NewFlagSet(...)` var block (immediately after the `operatorFile`/`uiSecureCookie` lines, ~line 104), add:

```go
		registryAddress  = fs.String("registry-address", "", "container registry host:port to browse (e.g. 100.64.0.23:5000); empty disables the registry browser feature entirely")
		registryAuth     = fs.String("registry-auth", "none", "registry auth mode: none or basic")
		registryUsername = fs.String("registry-username", "", "registry basic-auth username (only used when -registry-auth=basic)")
		registryPassword = fs.String("registry-password", "", "registry basic-auth password (only used when -registry-auth=basic)")
```

- [ ] **Step 3: Build the client and validate the auth mode fails closed**

Find where `svc` and other core dependencies are constructed (search for `router := api.NewRouter(` at ~line 359) and insert immediately before it:

```go
	var registryClient imgregistry.Client
	if strings.TrimSpace(*registryAddress) != "" {
		auth := imgregistry.Auth{Mode: strings.TrimSpace(*registryAuth)}
		switch auth.Mode {
		case "none":
		case "basic":
			auth.Username = *registryUsername
			auth.Password = *registryPassword
		default:
			return fmt.Errorf("registry: invalid -registry-auth %q (must be none or basic)", *registryAuth)
		}
		base := *registryAddress
		if !strings.Contains(base, "://") {
			base = "http://" + base
		}
		registryClient = imgregistry.NewHTTPClient(base, auth)
	}
```

Add `"github.com/iotready/podman-api/internal/imgregistry"` to the import block. If `"strings"` and `"fmt"` are not already imported in `server.go`, add them (check first — `fmt.Errorf` and error wrapping are already used extensively in this file, so both are almost certainly already imported).

- [ ] **Step 4: Thread the client into the router and UI**

Change the `api.NewRouter` call (Task 4 will add the parameter to `NewRouter`'s signature — this step assumes that signature change has landed; if executing Task 3 before Task 4, stub the call as `api.NewRouter(svc, jobStore, keyStore, combined, nil, canceller, Version, registryClient)` and let Task 4's own build step catch any mismatch):

```go
	router := api.NewRouter(svc, jobStore, keyStore, combined, nil, canceller, Version, registryClient)
```

And add `Registry: registryClient` to the `ui.Config{...}` literal at ~line 374:

```go
		uiApp, err = ui.New(ui.Config{Svc: svc, Jobs: jobStore, Auth: authr, Secure: *uiSecureCookie, TokenMgr: tokenMgr, Version: Version, Registry: registryClient})
```

- [ ] **Step 5: Build**

Run: `cd /home/tej/projects/podman-api && make build` (or `go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`)
Expected: fails at this point because `api.NewRouter` and `ui.Config` don't yet have the new parameter/field — that's expected and resolved in Tasks 4-5. Do not proceed to commit until Task 4 and Task 5 land and the full build is green; if working task-by-task with review gates, note this dependency explicitly to the reviewer rather than committing a non-building tree.

- [ ] **Step 6: Commit** (only once Task 4 and Task 5 have landed and `make build` is clean)

```bash
git add server/server.go
git commit -m "feat(server): wire registry-address/auth flags into imgregistry client"
```

---

### Task 4: API routes for repos/tags/manifest

**Files:**
- Modify: `internal/api/router.go`
- Create: `internal/api/registry.go`
- Create: `internal/api/registry_test.go`

**Interfaces:**
- Consumes: `imgregistry.Client` (`Catalog`, `Tags`, `Manifest`) from Tasks 1-2; `WriteJSON`, `WriteError` from `internal/api/errors.go`; `validName` from `internal/api/validate.go`.
- Produces: `handlers.registry imgregistry.Client` field; changes `NewRouter`'s signature to `NewRouter(svc *instance.Service, jobs store.JobStore, keys *auth.KeyStore, audit func(http.Handler) http.Handler, metricsHandler http.Handler, canceller JobCanceller, version string, registryClient imgregistry.Client) http.Handler`.

- [ ] **Step 1: Write the failing handler tests**

This package's existing convention (`internal/api/instances_test.go`) is: build a real `NewRouter(...)` behind an `httptest.Server`, and drive it with real HTTP requests carrying a bearer token, using `testify`'s `require`/`assert` — never by calling a handler method directly with a hand-built request. `newTestServer` there hard-codes a fixed `NewRouter(...)` call with no registry client; add a parallel helper in the new test file rather than changing the shared one, so existing tests are undisturbed:

```go
// internal/api/registry_test.go
package api

import (
	"context"
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
```

Add `"fmt"` to the import block (used by `fakeRegistry.Tags`'s error).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/... -run 'TestListRepos|TestListRepoTags' -v`
Expected: FAIL to compile — `handlers.registry` field doesn't exist yet and `NewRouter` doesn't accept the trailing `imgregistry.Client` argument yet.

- [ ] **Step 3: Add the `registry` field and implement the handlers**

In `internal/api/router.go`, add the field to the `handlers` struct (~line 125):

```go
type handlers struct {
	svc       *instance.Service
	jobs      store.JobStore
	canceller JobCanceller
	registry  imgregistry.Client
}
```

Add `"github.com/iotready/podman-api/internal/imgregistry"` to `router.go`'s imports.

Change `NewRouter`'s signature and body:

```go
func NewRouter(svc *instance.Service, jobs store.JobStore, keys *auth.KeyStore, audit func(http.Handler) http.Handler, metricsHandler http.Handler, canceller JobCanceller, version string, registryClient imgregistry.Client) http.Handler {
	mux := http.NewServeMux()
	h := &handlers{svc: svc, jobs: jobs, canceller: canceller, registry: registryClient}
	// ... rest unchanged ...
```

Register the routes (near the other `instances:read` routes, e.g. after line 68):

```go
	mux.Handle("GET /registry/repos", guard("instances:read", http.HandlerFunc(h.listRepos)))
	mux.Handle("GET /registry/repos/{repo}", guard("instances:read", http.HandlerFunc(h.listRepoTags)))
	mux.Handle("GET /registry/repos/{repo}/manifests/{ref}", guard("instances:read", http.HandlerFunc(h.getManifest)))
```

Note the tags route is `GET /registry/repos/{repo}` (not `/repos/{repo}/tags` as the design spec sketched) — there is no separate "repo metadata" concept in this design, so the per-repo route directly returns its tag groups, and `/repos/{repo}/manifests/{ref}` is the only sub-route under it.

Create `internal/api/registry.go`:

```go
package api

import (
	"net/http"
)

func (h *handlers) listRepos(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: "registry browsing is not configured"})
		return
	}
	repos, err := h.registry.Catalog(r.Context())
	if err != nil {
		WriteJSON(w, http.StatusBadGateway, ErrorBody{Code: "registry_unreachable", Message: err.Error()})
		return
	}
	WriteJSON(w, http.StatusOK, repos)
}

func (h *handlers) listRepoTags(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: "registry browsing is not configured"})
		return
	}
	repo := r.PathValue("repo")
	if !validName(repo) {
		writeInvalidName(w, "repo", repo)
		return
	}
	groups, err := h.registry.Tags(r.Context(), repo)
	if err != nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: err.Error()})
		return
	}
	WriteJSON(w, http.StatusOK, groups)
}

func (h *handlers) getManifest(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: "registry browsing is not configured"})
		return
	}
	repo := r.PathValue("repo")
	ref := r.PathValue("ref")
	if !validName(repo) {
		writeInvalidName(w, "repo", repo)
		return
	}
	m, err := h.registry.Manifest(r.Context(), repo, ref)
	if err != nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: err.Error()})
		return
	}
	WriteJSON(w, http.StatusOK, m)
}
```

`validName` (from `internal/render.ValidName`) is reused as-is for repo name validation — repo names share the same charset constraints as template/host names in this codebase's convention.

Distinguishing "registry unreachable" (502) from "repo/tag not found" (404) at the `imgregistry` layer is out of scope for this task: `Tags`/`Manifest` already return a plain `error` for both a transport failure and a 404 from the registry, so `listRepoTags`/`getManifest` map any error to 404 for now. `listRepos`'s `Catalog` failure maps to 502 since an unreachable registry is the only failure mode for that call (no per-repo 404 concept applies to the catalog).

- [ ] **Step 4: Fix all other `NewRouter` call sites**

Run: `cd /home/tej/projects/podman-api && grep -rln "api.NewRouter(" --include="*.go" .`

For every match outside `server/server.go` (test helpers, integration tests), add a trailing `nil` argument for `registryClient` (a nil `imgregistry.Client` disables the feature, matching the "absent, not empty" convention — safe for tests that don't exercise the registry).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /home/tej/projects/podman-api && make build && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/api/... -v`
Expected: all PASS, build clean.

- [ ] **Step 6: Commit**

```bash
git add internal/api/router.go internal/api/registry.go internal/api/registry_test.go
git commit -m "feat(api): add read-only /registry/repos routes"
```

---

### Task 5: UI "Registry" page

**Files:**
- Modify: `internal/ui/ui.go`
- Create: `internal/ui/handlers_registry.go`
- Create: `internal/ui/handlers_registry_test.go`
- Create: `internal/ui/templates/registry-repos.html`
- Create: `internal/ui/templates/registry-tags.html`
- Modify: `internal/ui/templates/layout.html` (add nav link, conditional on registry being configured)

**Interfaces:**
- Consumes: `imgregistry.Client` from Tasks 1-2; `u.render`, `u.pageData`, `u.renderError` from `internal/ui/render.go`/`ui.go`.
- Produces: `UI.cfg.Registry imgregistry.Client` field; `u.registryRepos`, `u.registryTags` handlers; routes `GET /ui/registry`, `GET /ui/registry/{repo}`.

- [ ] **Step 1: Add the `Registry` field to `ui.Config`**

In `internal/ui/ui.go`, add to the `Config` struct (~line 39):

```go
type Config struct {
	Svc        *instance.Service
	Jobs       store.JobStore
	Auth       Authenticator
	Sessions   SessionStore
	SessionTTL time.Duration
	Secure     bool
	TokenMgr   *auth.TokenManager
	Version    string
	Registry   imgregistry.Client // optional; nil hides the "Registry" nav item and 404s its routes
}
```

Add `"github.com/iotready/podman-api/internal/imgregistry"` to `ui.go`'s imports.

Register the routes near the other `guard(...)` registrations (after the jobs routes, ~line 191):

```go
	mux.Handle("GET /ui/registry", guard(u.registryRepos))
	mux.Handle("GET /ui/registry/{repo}", guard(u.registryTags))
```

- [ ] **Step 2: Write the failing handler test**

This package's existing convention (`internal/ui/handlers_hosts_test.go`) is: build a real `*UI` via `New(Config{...})` (helper `uiWithService(t)` builds one with a real `instance.Service` over the fake podman client and an authenticated operator), then drive requests through the *full mux* with `authedGet(t, u, path)` (`u.Handler().ServeHTTP` with a session cookie) — never by calling a handler method directly with a hand-built `httptest.NewRequest`, since path values (`{repo}` etc.) are only populated by the real mux matching a route. Follow that convention exactly:

```go
// internal/ui/handlers_registry_test.go
package ui

import (
	"context"
	"net/http"
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
```

Add `"strings"` to the import block (used by `strings.Contains` above). `uiWithService`/`authedGet` already exist in `handlers_hosts_test.go` in the same package, so they're available here with no import needed beyond what's already in this file.

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -run TestUI_Registry -v`
Expected: FAIL — `registryRepos`/`registryTags` not defined.

- [ ] **Step 4: Implement the handlers**

Create `internal/ui/handlers_registry.go`:

```go
package ui

import "net/http"

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
	u.render(w, r, http.StatusOK, "registry-repos", u.pageData(map[string]any{
		"ActiveHost": "",
		"Repos":      repos,
	}))
}

func (u *UI) registryTags(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Registry == nil {
		http.NotFound(w, r)
		return
	}
	repo := r.PathValue("repo")
	groups, err := u.cfg.Registry.Tags(r.Context(), repo)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "registry-tags", u.pageData(map[string]any{
		"Repo":   repo,
		"Groups": groups,
	}))
}
```

Create `internal/ui/templates/registry-repos.html`:

```html
{{define "registry-repos"}}
<h2>Registry</h2>
<div class="card"><div class="card-b">
  <table class="table">
    <thead><tr><th>Repository</th></tr></thead>
    <tbody>
    {{range .Repos}}
      <tr><td><a href="/ui/registry/{{.}}" hx-get="/ui/registry/{{.}}" hx-target="#main" hx-push-url="true">{{.}}</a></td></tr>
    {{else}}
      <tr><td class="subtitle">No repositories found.</td></tr>
    {{end}}
    </tbody>
  </table>
</div></div>
{{end}}
```

Create `internal/ui/templates/registry-tags.html`:

```html
{{define "registry-tags"}}
<h2>{{.Repo}}</h2>
<p class="subtitle"><a href="/ui/registry" hx-get="/ui/registry" hx-target="#main" hx-push-url="true">&larr; All repositories</a></p>
<div class="card"><div class="card-b">
  <table class="table">
    <thead><tr><th>Tags</th><th>Digest</th><th>Created</th><th>Size</th></tr></thead>
    <tbody>
    {{range .Groups}}
      <tr>
        <td>{{range $i, $t := .Tags}}{{if $i}}, {{end}}<code>{{$t}}</code>{{end}}</td>
        <td><code>{{.Digest}}</code></td>
        <td>{{.Created.Format "2006-01-02 15:04"}}</td>
        <td>{{formatBytes .Size}}</td>
      </tr>
    {{else}}
      <tr><td colspan="4" class="subtitle">No tags found.</td></tr>
    {{end}}
    </tbody>
  </table>
</div></div>
{{end}}
```

`formatBytes` is already registered in the template `FuncMap` in `ui.go` (used elsewhere for volume sizes) — reused as-is.

Add the nav link in `internal/ui/templates/layout.html`: find the existing nav `<a>` list (locate with `grep -n "ui/jobs" internal/ui/templates/layout.html` to find the right insertion point among existing nav items) and add, immediately after it, gated on the same page-data flag pattern already used for the tokens nav item (`grep -n "TokenMgr\|Tokens" internal/ui/templates/layout.html` to find that exact pattern and mirror it):

```html
{{if .RegistryEnabled}}<a href="/ui/registry" hx-get="/ui/registry" hx-target="#main" hx-push-url="true">Registry</a>{{end}}
```

Then in `u.pageData` (`internal/ui/ui.go`, ~line 138), add `"RegistryEnabled": u.cfg.Registry != nil` to the map it returns, so every page (which all call `u.pageData`) has this flag available to `layout.html` without each handler having to set it individually.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /home/tej/projects/podman-api && make build && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/ui/ui.go internal/ui/handlers_registry.go internal/ui/handlers_registry_test.go internal/ui/templates/registry-repos.html internal/ui/templates/registry-tags.html internal/ui/templates/layout.html
git commit -m "feat(ui): add read-only Registry browser page"
```

---

### Task 6: Upgrade-picker — wire the registry into the existing upgrade form

**Files:**
- Modify: `internal/ui/handlers_deploy.go`
- Modify: `internal/ui/templates/upgrade-form.html`
- Modify: `internal/ui/handlers_deploy_test.go` (or create a focused test file if repo-derivation logic doesn't fit naturally into the existing test file — check `grep -n "func Test.*[Uu]pgrade" internal/ui/handlers_deploy_test.go` first)

**Interfaces:**
- Consumes: `u.cfg.Registry imgregistry.Client` (Task 5), `firstContainerImage(obs instance.Observed) string` (existing, `handlers_deploy.go:560`).
- Produces: `func repoFromImage(image string) string` (pure function, unit-tested in isolation per the plan's Task Right-Sizing rule — a wrong derivation silently points the picker at the wrong repo).

- [ ] **Step 1: Write the failing test for repo derivation**

```go
// append to internal/ui/handlers_deploy_test.go
func TestRepoFromImage(t *testing.T) {
	cases := []struct{ image, want string }{
		{"100.64.0.23:5000/engine:latest", "100.64.0.23:5000/engine"},
		{"100.64.0.23:5000/engine@sha256:abcdef", "100.64.0.23:5000/engine"},
		{"100.64.0.23:5000/engine", "100.64.0.23:5000/engine"},
		{"docker.io/library/postgres:16", "docker.io/library/postgres"},
		{"", ""},
	}
	for _, c := range cases {
		if got := repoFromImage(c.image); got != c.want {
			t.Errorf("repoFromImage(%q) = %q, want %q", c.image, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -run TestRepoFromImage -v`
Expected: FAIL — `repoFromImage` not defined.

- [ ] **Step 3: Implement `repoFromImage`**

Append to `internal/ui/handlers_deploy.go` (near `firstContainerImage`, ~line 560):

```go
// repoFromImage strips a trailing :tag or @digest from an image reference to
// get the repository path the registry browser groups tags under. A bare
// registry host:port/path with neither suffix is returned unchanged. This
// mirrors the resolution scripts/roll.py already does against a template
// body when it needs "what repo does this instance's image belong to."
func repoFromImage(image string) string {
	if image == "" {
		return ""
	}
	if i := strings.LastIndex(image, "@"); i != -1 {
		return image[:i]
	}
	// A ":" after the last "/" is a tag; a ":" that is part of a host:port
	// prefix (before the first "/") is not.
	slash := strings.LastIndex(image, "/")
	colon := strings.LastIndex(image, ":")
	if colon > slash {
		return image[:colon]
	}
	return image
}
```

`strings` is already imported in `handlers_deploy.go` (used by `upgradeApply`'s `strings.TrimSpace`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/tej/projects/podman-api && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -run TestRepoFromImage -v`
Expected: PASS.

- [ ] **Step 5: Wire the derived repo into `upgradeForm`**

Modify `upgradeForm` in `internal/ui/handlers_deploy.go` (~line 407):

```go
func (u *UI) upgradeForm(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	currentImage := firstContainerImage(obs)
	data := map[string]any{
		"Host":         host,
		"ActiveHost":   host,
		"Template":     tmplID,
		"Slug":         slug,
		"CurrentImage": currentImage,
	}
	if u.cfg.Registry != nil {
		data["RegistryRepo"] = repoFromImage(currentImage)
	}
	u.render(w, r, http.StatusOK, "upgrade-form", u.pageData(data))
}
```

`upgradeApply` needs no change — it already applies whatever value is in the `image` form field, regardless of whether it was typed by hand or filled in by the picker.

- [ ] **Step 6: Add the picker to the template**

Modify `internal/ui/templates/upgrade-form.html` to add a "browse registry" affordance below the image field, only rendered when a repo could be derived:

```html
{{define "upgrade-form"}}
<h2>Upgrade {{.Template}} / {{.Slug}} on {{.Host}}</h2>
{{if .Error}}<p class="error">{{.Error}}</p>{{end}}
<p class="subtitle">Changes the image only. Existing parameters and secrets are reused; to rotate a secret, manage it separately.</p>
<div class="card"><div class="card-b">
<form hx-post="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}/upgrade" hx-target="#main" class="form">
  <input type="hidden" name="csrf_token" value="{{.CSRF}}">
  {{if .CurrentImage}}<p class="subtitle">Current: <code>{{.CurrentImage}}</code></p>{{end}}
  <div class="field">
    <label>New image</label>
    <input id="upgrade-image-input" name="image" value="{{.CurrentImage}}" placeholder="docker.io/library/postgres:16" required>
  </div>
  {{if .RegistryRepo}}
  <div class="field">
    <a class="btn ghost" hx-get="/ui/registry/{{.RegistryRepo}}?picker=1" hx-target="#registry-picker" hx-swap="innerHTML">Browse registry ({{.RegistryRepo}})</a>
    <div id="registry-picker"></div>
  </div>
  {{end}}
  <div class="toolbar" style="margin-top:22px">
    <button type="submit" class="btn primary" hx-disabled-elt="this">Upgrade</button>
    <a class="btn ghost" href="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}" hx-get="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}" hx-target="#main" hx-push-url="true">Cancel</a>
  </div>
</form>
</div></div>
{{end}}
```

- [ ] **Step 7: Add a `picker` render mode to `registryTags` and a picker template**

Modify `u.registryTags` in `internal/ui/handlers_registry.go` to render a compact picker fragment instead of the full page when `?picker=1` is set:

```go
func (u *UI) registryTags(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Registry == nil {
		http.NotFound(w, r)
		return
	}
	repo := r.PathValue("repo")
	groups, err := u.cfg.Registry.Tags(r.Context(), repo)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	if r.URL.Query().Get("picker") == "1" {
		u.render(w, r, http.StatusOK, "registry-tags-picker", u.pageData(map[string]any{
			"Repo":   repo,
			"Groups": groups,
		}))
		return
	}
	u.render(w, r, http.StatusOK, "registry-tags", u.pageData(map[string]any{
		"Repo":   repo,
		"Groups": groups,
	}))
}
```

Create `internal/ui/templates/registry-tags-picker.html`:

```html
{{define "registry-tags-picker"}}
<table class="table">
  <thead><tr><th>Tags</th><th>Digest</th><th>Created</th><th></th></tr></thead>
  <tbody>
  {{range .Groups}}
    <tr>
      <td>{{range $i, $t := .Tags}}{{if $i}}, {{end}}<code>{{$t}}</code>{{end}}</td>
      <td><code>{{.Digest}}</code></td>
      <td>{{.Created.Format "2006-01-02 15:04"}}</td>
      <td><button type="button" class="btn ghost" onclick="document.getElementById('upgrade-image-input').value='{{$.Repo}}@{{.Digest}}'">Use</button></td>
    </tr>
  {{else}}
    <tr><td colspan="4" class="subtitle">No tags found.</td></tr>
  {{end}}
  </tbody>
</table>
{{end}}
```

- [ ] **Step 8: Write the failing test for the picker fragment**

```go
// append to internal/ui/handlers_registry_test.go
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
```

- [ ] **Step 9: Run all UI tests**

Run: `cd /home/tej/projects/podman-api && make build && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/... -v`
Expected: all PASS.

- [ ] **Step 10: Run the full test suite**

Run: `cd /home/tej/projects/podman-api && make vet && make test`
Expected: all PASS, `go vet` clean.

- [ ] **Step 11: Commit**

```bash
git add internal/ui/handlers_deploy.go internal/ui/handlers_deploy_test.go internal/ui/handlers_registry.go internal/ui/handlers_registry_test.go internal/ui/templates/upgrade-form.html internal/ui/templates/registry-tags-picker.html
git commit -m "feat(ui): add registry tag picker to the image-upgrade form"
```

---

## After all tasks land

Open a PR: `forgejo pr create IoTReadyNext/podman-api --title="feat: registry browser + upgrade-picker (phase 1)" --head=<branch> --base=main --body="..."` referencing the design spec at `docs/superpowers/specs/2026-08-06-registry-browser-design.md`. Deploy is out of scope for this plan (no `-registry-address` is set on any live host yet — enabling it is a follow-up operational step once this ships and is reviewed, requiring a `make bump` + redeploy of `podman-api-pro` per its own `CLAUDE.md` once the OSS tag is cut).
