# Live Log Tail in UI — Design Spec

**Issue:** #64  
**Date:** 2026-06-07  
**Tier:** OSS

---

## Overview

Wire the instance-detail page to a dedicated live log-tail view using the htmx SSE extension. The API SSE endpoint already exists (bearer-scoped); this adds a session-scoped UI variant and a dedicated log page with container selector and follow/pause.

---

## Routes

Two new UI routes, both behind `guard` (session cookie, read-only GET, no CSRF required):

```
GET /ui/hosts/{host}/instances/{template}/{slug}/logs
    → logsPage   — dedicated log page; static 200-line snapshot when ?follow=false (default),
                   streaming when ?follow=true
GET /ui/hosts/{host}/instances/{template}/{slug}/logs/stream
    → logsStream — SSE handler; session-auth; calls Svc.Logs with Follow:true, Tail:100
```

The existing API route (`GET /hosts/{host}/instances/{template}/{slug}/logs`) is unchanged — it remains bearer-scoped and serves API clients.

---

## Backend

### `internal/ui/handlers_logs.go` (new file)

**`logsPage`**

1. Fetch the instance via `Svc.Get` to populate the container list.
2. Strip the `{tmpl}-{slug}-` prefix from each container name to get the suffix `Svc.Logs` expects (e.g. `myapp-prod-web` → `web`).
3. Resolve `?container=` from the query; if absent, pick the first container and HTTP 302 to the canonical URL with `?container=X&follow=true` (landing directly in streaming mode).
4. When `?follow=false` (or `?follow=` absent when `?container=` is already present): drain `Svc.Logs(..., LogOptions{Tail: 200})` into a `[]string` and render the static template variant.
5. When `?follow=true`: render the SSE template variant (no drain needed).

**`logsStream`**

1. Resolve the container suffix the same way as `logsPage` (query param or first container).
2. Call `Svc.Logs(..., LogOptions{Follow: true, Tail: 100})`.
3. Set `Content-Type: text/event-stream`, `Cache-Control: no-cache`, flush on each line.
4. For each line received: HTML-escape the content and emit `event: log\ndata: <span class="line">ESCAPED</span>\n\n`.
5. Return when the request context is cancelled (client disconnect) or the channel closes.

### Template data

```go
type logPageData struct {
    Host, Template, Slug string
    Container  string         // selected container suffix
    Containers []containerOpt // {Name: "myapp-prod-web", Suffix: "web"}
    Follow     bool
    Lines      []string       // populated when !Follow
}

type containerOpt struct {
    Name   string // full container name
    Suffix string // argument Svc.Logs expects
}
```

---

## Frontend

### `htmx-ext-sse.min.js`

Bundled into `internal/ui/static/` and embedded via `embed.go`. Loaded only on the log page via a `<script>` tag in the template — not in the global layout.

### `logs-page.html` template (new file; `logs.html` is deleted)

```
{{define "logs-page"}}
  header: template/slug breadcrumb + back link to instance detail
  container <select> with hx-get + hx-push-url + hx-trigger="change"
    hidden input name="follow" to preserve follow state on container switch
  follow/pause button (htmx navigation)
  <pre id="log-lines"> — SSE-connected when following, static when paused
  <script> — line cap + auto-scroll (only rendered when following)
{{end}}
```

#### Container selector

```html
<select name="container"
        hx-get="/ui/hosts/{{.Host}}/instances/{{.Template}}/{{.Slug}}/logs"
        hx-target="#main" hx-push-url="true"
        hx-trigger="change" hx-include="[name='follow']">
  {{range .Containers}}
  <option value="{{.Suffix}}" {{if eq .Suffix $.Container}}selected{{end}}>{{.Name}}</option>
  {{end}}
</select>
<input type="hidden" name="follow" value="{{.Follow}}">
```

Changing the container navigates to the same page with the new `?container=X`, preserving the current follow/pause state.

#### Follow / Pause

```html
{{if .Follow}}
<button hx-get="...?container={{.Container}}&follow=false"
        hx-target="#main" hx-push-url="true">Pause</button>
{{else}}
<button hx-get="...?container={{.Container}}&follow=true"
        hx-target="#main" hx-push-url="true">Follow</button>
{{end}}
```

Clicking Pause navigates to the static snapshot, removing the `sse-connect` element and closing the SSE connection. Clicking Follow reloads in streaming mode. Accumulated live lines are lost on pause (the static snapshot shows the last 200 lines from the server). Acceptable trade-off for the initial implementation.

#### SSE-connected pre (follow mode)

```html
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
    while (pre.children.length > 1000) pre.removeChild(pre.firstChild);
    pre.scrollTop = pre.scrollHeight;
  });
})();
</script>
```

Line cap: 1000 `<span>` children, oldest trimmed from the top. Auto-scroll: on every SSE message, scroll the `<pre>` to the bottom.

#### Static pre (pause mode)

```html
<pre id="log-lines">{{range .Lines}}{{.}}
{{end}}</pre>
```

---

## Instance Detail Change

`instance-detail.html`: remove the inline `hx-get` "View logs" button and `<pre id="logs">`. Replace with a navigation link that lands in follow mode:

```html
<a class="pure-button"
   href="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/logs?follow=true"
   hx-get="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/logs?follow=true"
   hx-target="#main" hx-push-url="true">Logs</a>
```

The `logs.html` template (`{{define "logs"}}`) is deleted — it is no longer referenced after this change.

---

## Out of Scope

- **OTel log-sink seam**: the roadmap item (§3.6 of the PaaS spec) for commercial log archive/query is deferred. This ticket only wires the live UI tail. The sink seam ships separately as part of the observability roadmap (#68).
- **Log query / filter / search**: commercial tier (#68).
- **Multi-instance log aggregation**: not in scope.

---

## Testing

- Unit tests for `logsPage` handler: container resolution, redirect to canonical URL, static render, error cases.
- Unit tests for `logsStream` handler: SSE header assertions, HTML escaping of log content, context cancellation.
- Existing `logsTail` UI handler is removed; its tests are replaced by the new handler tests.
- No integration tests (SSE streaming requires a real podman host; covered manually against host 148).
