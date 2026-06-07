# Live Log Tail in UI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a dedicated live log-tail page to the admin UI, wired to a session-authenticated SSE stream, with container selector and follow/pause controls.

**Architecture:** A new `handlers_logs.go` adds `logsPage` (dedicated log page, static or SSE mode) and `logsStream` (session-scoped SSE, HTML-escapes log content). The htmx SSE extension handles browser-side streaming. Follow/pause is pure htmx navigation (`?follow=true/false`). Container selection is a `<select>` that navigates to the same page preserving follow state.

**Tech Stack:** Go 1.22 (`net/http`), htmx 2.x + htmx-ext-sse 2.x, pure-css, Go `html/template`, Go `embed`

---

## File Map

| Action | File |
|--------|------|
| Create | `internal/ui/handlers_logs.go` |
| Create | `internal/ui/templates/logs-page.html` |
| Create | `internal/ui/static/htmx-ext-sse.min.js` |
| Modify | `internal/ui/ui.go` (route registration) |
| Modify | `internal/ui/templates/instance-detail.html` (Logs link) |
| Delete | `internal/ui/templates/logs.html` |
| Modify | `internal/ui/handlers_instances.go` (remove `logsTail`) |
| Modify | `internal/ui/handlers_instances_test.go` (replace `TestLogsTailRendersLines`) |

---

## Task 1: Bundle htmx-ext-sse

**Files:**
- Create: `internal/ui/static/htmx-ext-sse.min.js`

The existing `embed.go` uses `//go:embed static/*` — any file placed in `internal/ui/static/` is automatically included. No code changes needed.

- [ ] **Step 1: Download htmx-ext-sse**

```bash
curl -L https://cdn.jsdelivr.net/npm/htmx-ext-sse@2.2.2/sse.min.js \
     -o internal/ui/static/htmx-ext-sse.min.js
```

- [ ] **Step 2: Verify the file is non-empty and starts with the extension code**

```bash
head -c 100 internal/ui/static/htmx-ext-sse.min.js
```

Expected: should start with something like `(function(e){"use strict";` (minified JS, not an error page)

- [ ] **Step 3: Verify the build still compiles**

```bash
make build
```

Expected: `bin/podman-api` built successfully, no errors.

- [ ] **Step 4: Commit**

```bash
git add internal/ui/static/htmx-ext-sse.min.js
git commit -m "feat(64): bundle htmx-ext-sse 2.2.2"
```

---

## Task 2: Create stub `logs-page.html`

**Files:**
- Create: `internal/ui/templates/logs-page.html`

The `render` function validates that a template block exists before rendering. Creating a stub now means the template set compiles and test helpers (`New()`) succeed throughout development.

- [ ] **Step 1: Create the stub**

Create `internal/ui/templates/logs-page.html`:

```html
{{define "logs-page"}}
<div>logs-page stub</div>
{{end}}
```

- [ ] **Step 2: Verify all existing tests still pass**

```bash
make test
```

Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/ui/templates/logs-page.html
git commit -m "feat(64): stub logs-page template"
```

---

## Task 3: `handlers_logs.go` — types and `resolveContainerSuffix`

**Files:**
- Create: `internal/ui/handlers_logs.go`
- Create (test): `internal/ui/handlers_logs_test.go`

`resolveContainerSuffix` is a pure function shared by both `logsPage` and `logsStream`. Testing it directly keeps the handler tests simpler.

- [ ] **Step 1: Write the failing test**

Create `internal/ui/handlers_logs_test.go`:

```go
package ui

import (
	"testing"

	"github.com/iotready/podman-api/internal/podman"
)

func TestResolveContainerSuffix(t *testing.T) {
	containers := []podman.Container{
		{Name: "postgres-main-db"},
		{Name: "postgres-main-sidecar"},
	}
	if got := resolveContainerSuffix("postgres", "main", containers); got != "db" {
		t.Fatalf("got %q, want %q", got, "db")
	}
}

func TestResolveContainerSuffixSkipsNonPrefixed(t *testing.T) {
	containers := []podman.Container{
		{Name: "pause"},
		{Name: "infra"},
		{Name: "postgres-main-app"},
	}
	if got := resolveContainerSuffix("postgres", "main", containers); got != "app" {
		t.Fatalf("got %q, want %q", got, "app")
	}
}

func TestResolveContainerSuffixNoMatch(t *testing.T) {
	containers := []podman.Container{
		{Name: "pause"},
	}
	if got := resolveContainerSuffix("postgres", "main", containers); got != "" {
		t.Fatalf("got %q, want empty string", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" \
    ./internal/ui/ -run TestResolveContainer -v
```

Expected: FAIL — `resolveContainerSuffix undefined`

- [ ] **Step 3: Create `handlers_logs.go` with the types and helper**

Create `internal/ui/handlers_logs.go`:

```go
package ui

import (
	"strings"

	"github.com/iotready/podman-api/internal/podman"
)

type containerOpt struct {
	Name   string // full container name, e.g. "postgres-main-db"
	Suffix string // suffix after "{tmpl}-{slug}-", e.g. "db"
}

// resolveContainerSuffix returns the first container whose name starts with
// "{tmpl}-{slug}-", stripping the prefix to get the suffix Svc.Logs expects.
// Returns "" if no container matches.
func resolveContainerSuffix(tmpl, slug string, containers []podman.Container) string {
	prefix := tmpl + "-" + slug + "-"
	for _, c := range containers {
		if strings.HasPrefix(c.Name, prefix) {
			return strings.TrimPrefix(c.Name, prefix)
		}
	}
	return ""
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" \
    ./internal/ui/ -run TestResolveContainer -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ui/handlers_logs.go internal/ui/handlers_logs_test.go
git commit -m "feat(64): containerOpt type + resolveContainerSuffix helper"
```

---

## Task 4: `logsPage` — redirect when container absent, replace route

**Files:**
- Modify: `internal/ui/handlers_logs.go`
- Modify: `internal/ui/ui.go`
- Modify: `internal/ui/handlers_instances_test.go`

When `?container=` is absent the handler fetches the instance, picks the first matching container, and issues a `302` to the canonical URL. This is also where we swap the route registration from `logsTail` → `logsPage`.

- [ ] **Step 1: Write the failing redirect test in `handlers_logs_test.go`**

Add to `internal/ui/handlers_logs_test.go`:

```go
func TestLogsPageRedirectsWhenContainerAbsent(t *testing.T) {
	u := uiWithSeededInstance(t)
	// uiWithSeededInstance seeds container "postgres-main-db"; suffix = "db"
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs")
	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302 redirect", w.Code)
	}
	loc := w.Header().Get("Location")
	if !strings.Contains(loc, "container=db") {
		t.Errorf("Location %q should contain container=db", loc)
	}
	if !strings.Contains(loc, "follow=true") {
		t.Errorf("Location %q should contain follow=true", loc)
	}
}
```

Replace the import block at the top of `handlers_logs_test.go` with the complete set (adds `net/http` and `strings` to what Task 3 established):

```go
import (
	"net/http"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/podman"
)
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" \
    ./internal/ui/ -run TestLogsPageRedirects -v
```

Expected: FAIL — the route still points to `logsTail` which returns 200, not 302.

- [ ] **Step 3: Add `logsPage` redirect path to `handlers_logs.go`**

Add these imports and the handler to `internal/ui/handlers_logs.go`:

```go
import (
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/podman"
)

func (u *UI) logsPage(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")

	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}

	prefix := tmpl + "-" + slug + "-"
	var containers []containerOpt
	for _, c := range obs.Containers {
		if strings.HasPrefix(c.Name, prefix) {
			containers = append(containers, containerOpt{
				Name:   c.Name,
				Suffix: strings.TrimPrefix(c.Name, prefix),
			})
		}
	}

	container := r.URL.Query().Get("container")
	if container == "" {
		if len(containers) == 0 {
			u.render(w, r, http.StatusOK, "logs-page", u.pageData(map[string]any{
				"Host": host, "Template": tmpl, "Slug": slug,
				"Container": "", "Containers": containers, "Follow": false, "Lines": nil,
			}))
			return
		}
		http.Redirect(w, r,
			"/ui/hosts/"+host+"/instances/"+tmpl+"/"+slug+"/logs"+
				"?container="+containers[0].Suffix+"&follow=true",
			http.StatusFound)
		return
	}

	follow := r.URL.Query().Get("follow") == "true"

	if follow {
		u.render(w, r, http.StatusOK, "logs-page", u.pageData(map[string]any{
			"Host": host, "Template": tmpl, "Slug": slug,
			"Container": container, "Containers": containers, "Follow": true, "Lines": nil,
		}))
		return
	}

	ch, err := u.cfg.Svc.Logs(r.Context(), host, tmpl, slug, container, podman.LogOptions{Tail: 200})
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	var lines []string
	for ln := range ch {
		lines = append(lines, ln.Line)
	}
	u.render(w, r, http.StatusOK, "logs-page", u.pageData(map[string]any{
		"Host": host, "Template": tmpl, "Slug": slug,
		"Container": container, "Containers": containers, "Follow": false, "Lines": lines,
	}))
}
```

- [ ] **Step 4: Replace the `logsTail` route with `logsPage` in `ui.go`**

In `internal/ui/ui.go`, find:

```go
mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/logs", guard(u.logsTail))
```

Replace with:

```go
mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/logs", guard(u.logsPage))
```

- [ ] **Step 5: Update `TestLogsTailRendersLines` to test redirect instead**

In `internal/ui/handlers_instances_test.go`, replace:

```go
func TestLogsTailRendersLines(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "database system is ready") {
		t.Error("logs view should show the seeded log line")
	}
}
```

With:

```go
func TestLogsRouteRedirectsToFirstContainer(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs")
	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302 redirect to canonical logs URL", w.Code)
	}
}
```

- [ ] **Step 6: Run all tests to verify they pass**

```bash
make test
```

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/ui/handlers_logs.go internal/ui/handlers_logs_test.go \
        internal/ui/ui.go internal/ui/handlers_instances_test.go
git commit -m "feat(64): logsPage handler + route, redirect when container absent"
```

---

## Task 5: `logsPage` — static render and follow render tests

**Files:**
- Modify: `internal/ui/handlers_logs_test.go`

The handler is already fully implemented; now write tests for the static and follow render paths to lock in their behaviour.

- [ ] **Step 1: Add static render test**

Add to `internal/ui/handlers_logs_test.go`:

```go
func TestLogsPageStaticRenderShowsLines(t *testing.T) {
	u := uiWithSeededInstance(t)
	// ?container=db&follow=false — static 200-line snapshot
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs?container=db&follow=false")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "database system is ready") {
		t.Error("static logs page should contain the seeded log line")
	}
}

func TestLogsPageFollowRenderHasSSEConnect(t *testing.T) {
	u := uiWithSeededInstance(t)
	// ?container=db&follow=true — SSE streaming mode
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs?container=db&follow=true")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "sse-connect") {
		t.Error("follow mode page should contain sse-connect attribute")
	}
}

func TestLogsPageNotFoundReturns404(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/ghost/logs?container=db")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown slug", w.Code)
	}
}
```

- [ ] **Step 2: Run tests to verify they currently fail**

```bash
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" \
    ./internal/ui/ -run "TestLogsPageStatic|TestLogsPageFollow|TestLogsPageNotFound" -v
```

Expected:
- `TestLogsPageStaticRenderShowsLines` — PASS (handler works, stub template renders)
- `TestLogsPageFollowRenderHasSSEConnect` — FAIL (stub template has no `sse-connect`)
- `TestLogsPageNotFoundReturns404` — PASS

The follow test will become green once the full template is in place (Task 8). Leave it failing for now and move on.

- [ ] **Step 3: Run full suite to confirm no regressions**

```bash
make test
```

Expected: all previously passing tests still pass; `TestLogsPageFollowRenderHasSSEConnect` fails (expected — template is still a stub).

- [ ] **Step 4: Commit**

```bash
git add internal/ui/handlers_logs_test.go
git commit -m "test(64): logsPage static, follow, and not-found tests"
```

---

## Task 6: `logsStream` handler

**Files:**
- Modify: `internal/ui/handlers_logs.go`
- Modify: `internal/ui/ui.go`
- Modify: `internal/ui/handlers_logs_test.go`

- [ ] **Step 1: Write failing tests**

Add to `internal/ui/handlers_logs_test.go`:

```go
func TestLogsStreamSSEHeaders(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs/stream?container=db")
	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if !strings.Contains(w.Body.String(), "event: log") {
		t.Error("SSE stream should contain 'event: log' lines")
	}
}

func TestLogsStreamHTMLEscapesLogLines(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	fc.AddPod("edge-1", podman.Pod{
		Name: "postgres-main",
		Containers: []podman.Container{
			{Name: "postgres-main-db", Image: "postgres:16", Status: "Running"},
		},
	})
	fc.LogLines = []podman.LogLine{{Container: "postgres-main-db", Line: `<script>alert(1)</script>`}}
	mem := store.NewMemory()
	_ = mem.PutTemplate(context.Background(), store.Template{Meta: render.Meta{ID: "postgres"}})
	svc := instance.NewService(fc, hosts)
	svc.SetStore(mem)
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}

	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs/stream?container=db")
	body := w.Body.String()
	if strings.Contains(body, "<script>") {
		t.Error("log line must be HTML-escaped; raw <script> tag found in SSE output")
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Error("log line should appear as HTML-escaped &lt;script&gt; in SSE output")
	}
}
```

Replace the import block at the top of `handlers_logs_test.go` with the final complete set (adds `context`, `config`, `instance`, `fake`, `render`, `store` to what earlier tasks established):

```go
import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" \
    ./internal/ui/ -run "TestLogsStream" -v
```

Expected: FAIL — route does not exist yet.

- [ ] **Step 3: Add `logsStream` to `handlers_logs.go`**

Add these imports to `handlers_logs.go` (adjust the existing import block):

```go
import (
	"fmt"
	"html"
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/podman"
)
```

Add the handler:

```go
func (u *UI) logsStream(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")

	container := r.URL.Query().Get("container")
	if container == "" {
		obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		container = resolveContainerSuffix(tmpl, slug, obs.Containers)
		if container == "" {
			http.Error(w, "no containers", http.StatusBadRequest)
			return
		}
	}

	ch, err := u.cfg.Svc.Logs(r.Context(), host, tmpl, slug, container, podman.LogOptions{Follow: true, Tail: 100})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	for {
		select {
		case <-r.Context().Done():
			return
		case line, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: log\ndata: <span class=\"line\">%s</span>\n\n",
				html.EscapeString(line.Line))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}
```

- [ ] **Step 4: Register `logsStream` route in `ui.go`**

In `internal/ui/ui.go`, after the `logsPage` route line, add:

```go
mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/logs/stream", guard(u.logsStream))
```

- [ ] **Step 5: Run stream tests to verify they pass**

```bash
go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" \
    ./internal/ui/ -run "TestLogsStream" -v
```

Expected: both PASS.

- [ ] **Step 6: Run full suite**

```bash
make test
```

Expected: all previously passing tests still pass.

- [ ] **Step 7: Commit**

```bash
git add internal/ui/handlers_logs.go internal/ui/ui.go internal/ui/handlers_logs_test.go
git commit -m "feat(64): logsStream SSE handler + route"
```

---

## Task 7: Full `logs-page.html` template

**Files:**
- Modify: `internal/ui/templates/logs-page.html`

Replace the stub with the complete template. After this task `TestLogsPageFollowRenderHasSSEConnect` (from Task 5) becomes green.

- [ ] **Step 1: Replace `logs-page.html` with the full template**

Overwrite `internal/ui/templates/logs-page.html`:

```html
{{define "logs-page"}}
<div class="inst-head">
  <strong>{{.Template}} / {{.Slug}}</strong> — Logs
  <a class="pure-button"
     href="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}"
     hx-get="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}"
     hx-target="#main" hx-push-url="true">← Instance</a>
</div>
<div class="log-controls">
  <select name="container"
          hx-get="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}/logs"
          hx-target="#main" hx-push-url="true"
          hx-trigger="change" hx-include="[name='follow']">
    {{range .Containers}}
    <option value="{{.Suffix}}" {{if eq .Suffix $.Container}}selected{{end}}>{{.Name}}</option>
    {{end}}
  </select>
  <input type="hidden" name="follow" value="{{.Follow}}">
  {{if .Follow}}
  <button hx-get="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}/logs?container={{.Container}}&follow=false"
          hx-target="#main" hx-push-url="true">Pause</button>
  {{else}}
  <button hx-get="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}/logs?container={{.Container}}&follow=true"
          hx-target="#main" hx-push-url="true">Follow</button>
  {{end}}
</div>
{{if .Follow}}
<script src="/ui/static/htmx-ext-sse.min.js" defer></script>
<pre id="log-lines"
     hx-ext="sse"
     sse-connect="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}/logs/stream?container={{.Container}}"
     sse-swap="log"
     hx-swap="beforeend"></pre>
<script>
(function(){
  var pre = document.getElementById('log-lines');
  document.body.addEventListener('htmx:sseMessage', function() {
    while (pre.children.length > 1000) { pre.removeChild(pre.firstChild); }
    pre.scrollTop = pre.scrollHeight;
  });
})();
</script>
{{else}}
<pre id="log-lines">{{range .Lines}}{{.}}
{{end}}</pre>
{{end}}
{{end}}
```

- [ ] **Step 2: Run the full test suite**

```bash
make test
```

Expected: ALL tests pass, including `TestLogsPageFollowRenderHasSSEConnect` which was failing since Task 5.

- [ ] **Step 3: Commit**

```bash
git add internal/ui/templates/logs-page.html
git commit -m "feat(64): full logs-page template with SSE connect + container selector"
```

---

## Task 8: Update `instance-detail.html`, delete `logs.html`

**Files:**
- Modify: `internal/ui/templates/instance-detail.html`
- Delete: `internal/ui/templates/logs.html`
- Modify: `internal/ui/handlers_instances.go` (remove `logsTail`)
- Modify: `internal/ui/handlers_instances_test.go` (remove unused import if needed)

- [ ] **Step 1: Replace the logs button in `instance-detail.html`**

In `internal/ui/templates/instance-detail.html`, find:

```html
<button hx-get="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/logs" hx-target="#logs">View logs</button>
<pre id="logs"></pre>
```

Replace with:

```html
<a class="pure-button"
   href="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/logs?follow=true"
   hx-get="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/logs?follow=true"
   hx-target="#main" hx-push-url="true">Logs</a>
```

- [ ] **Step 2: Delete `logs.html`**

```bash
rm internal/ui/templates/logs.html
```

- [ ] **Step 3: Remove `logsTail` from `handlers_instances.go`**

In `internal/ui/handlers_instances.go`, delete the entire `logsTail` function (lines 103–144) including its comment block. Also remove the `podman` import if it is no longer used after this deletion.

After deletion, the file should end at the closing brace of the `lifecycle` function. Verify there are no remaining references to `logsTail` anywhere:

```bash
grep -rn "logsTail" internal/ui/
```

Expected: no output.

- [ ] **Step 4: Run full test suite**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 5: Confirm `gofmt` and `go vet` are clean**

```bash
gofmt -l internal/ui/ && \
go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/
```

Expected: no output from either command.

- [ ] **Step 6: Commit**

```bash
git add internal/ui/templates/instance-detail.html \
        internal/ui/handlers_instances.go \
        internal/ui/handlers_instances_test.go
git rm internal/ui/templates/logs.html
git commit -m "feat(64): wire instance-detail Logs nav link; remove logsTail"
```

---

## Task 9: Final verification

- [ ] **Step 1: Run full test suite**

```bash
make test
```

Expected: all tests PASS, zero failures.

- [ ] **Step 2: Build**

```bash
make build
```

Expected: `bin/podman-api` built successfully.

- [ ] **Step 3: gofmt + vet clean**

```bash
gofmt -l . && \
go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...
```

Expected: no output from either command.

- [ ] **Step 4: Final commit (if any uncommitted changes)**

```bash
git status
```

If clean: done. If there are uncommitted changes, commit them:

```bash
git add -p
git commit -m "feat(64): live log tail in UI — complete"
```
