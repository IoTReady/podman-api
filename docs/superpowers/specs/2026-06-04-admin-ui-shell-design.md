# Admin UI shell (HTMX + PureCSS, embedded) — Design

**Date:** 2026-06-04
**Status:** Approved (brainstorm complete; ready for implementation plan)
**Author:** Tej + Claude (brainstorm)
**Issue:** #62 (Slice 1 — the deploy loop)
**Roadmap:** `docs/superpowers/specs/2026-06-04-podman-paas-roadmap-design.md` §4 (Slice 1), §3.4 (seams)

## 1. Context

`podman-api` is a server-rendered-free JSON orchestrator today: bearer-token auth with
scopes, an `instance.Service` over libpod bindings, an async jobs runner, and an encrypted
SQLite spec store. It has **no UI**. This is the first frontend in the repo.

This phase delivers the **admin UI shell**: single-operator login, a host list, instance
list/detail with lifecycle actions, and a working deploy form — server-rendered with
**HTMX + PureCSS** from `embed.FS`, on the existing process and port, talking directly to
`instance.Service`. No SPA, no separate process, no client build pipeline.

It is one of three Slice-1 siblings: **#60** ingress + auto-TLS (Phase-1 foundations merged),
**#61** app catalog + deploy wizard, and **#62** (this) the shell. The shell deploys from the
templates already loaded; the curated catalog and richer multi-step wizard are **#61**, layered
on top of the generic form built here.

**Success (from #62):** the full deploy loop is driveable from the browser, no CLI.

## 2. Scope

**In scope**
- Single-operator login (password → signed session cookie) behind a pluggable `Authenticator`.
- App shell: persistent left-sidebar layout (host switcher pinned), HTMX main-panel swaps.
- Host list / host overview (instance count, reachability).
- Instance list per host; instance detail (pod/container status, ports, volumes, env summary).
- Lifecycle actions: Start, Stop, Restart, **Upgrade**, Delete — in-place HTMX POSTs.
- Static logs tail (last N lines).
- Generic deploy form generated from a template's declared `render.Meta`.
- Seams: `Authenticator`, `SessionStore`, `Identity{Subject, Scopes}`.

**Out of scope (deferred, with owning issue)**
- Curated app catalog + multi-step wizard — **#61**.
- Live log streaming — **#64**.
- Per-container metrics / dashboards — **#63**.
- Domain entry / ingress wiring in the deploy form — **#60 / #61**.
- Multi-user RBAC — Slice 4 (the `Authenticator`/`Identity` seam makes it additive).

## 3. Architecture

### 3.1 Package

New package **`internal/ui`**, structured like `internal/api` but rendering HTML:
- owns the `embed.FS` (templates + vendored static assets),
- parses templates once at construction,
- holds the session/auth seam and the HTTP handlers,
- depends on `*instance.Service` and `store.JobStore` — the same objects `internal/api`
  handlers wrap.

### 3.2 Backend access — in-process

UI handlers call `*instance.Service` (and `store.JobStore`) **directly** and render templates
from the results. No HTTP loopback to our own JSON API. The JSON API and the UI are two thin
presentation layers over one service — single source of truth for business logic.

### 3.3 Mounting

UI mounts under a **`/ui` prefix on the main listener** (no new port; no collision with the
API's verb-prefixed routes like `GET /hosts`):

| Route | Purpose |
|---|---|
| `GET /` | 302 → `/ui` |
| `GET /ui` | dashboard (host list) |
| `GET /ui/login`, `POST /ui/login` | login form + submit |
| `POST /ui/logout` | clear session |
| `GET /ui/hosts/{host}` | instance list (fragment or full page) |
| `GET /ui/hosts/{host}/instances/{tmpl}/{slug}` | instance detail |
| `GET /ui/hosts/{host}/deploy` | deploy form |
| `POST /ui/hosts/{host}/deploy` | create instance |
| `POST /ui/hosts/{host}/instances/{tmpl}/{slug}/{action}` | lifecycle (start/stop/restart/upgrade/delete) |
| `GET /ui/hosts/{host}/instances/{tmpl}/{slug}/logs` | static logs tail |
| `GET /ui/jobs`, `GET /ui/jobs/{id}` | jobs list/detail (read) |
| `GET /ui/static/...` | vendored assets |

The router is composed in `cmd/podman-api/main.go`: the UI handler is mounted onto the same
`http.ServeMux` as the API, or wrapped around it. **The UI mounts only when an operator
credential is configured** (`-operator-file`); absent ⇒ UI off, logged at startup. This keeps
the new surface opt-in and prevents exposing it credential-less.

### 3.4 Auth seam (single-operator now → RBAC later)

```go
// Authenticator verifies a login and yields an Identity. The single-operator
// implementation checks a bcrypt password hash; a future commercial implementation
// supplies multi-user RBAC behind the same interface.
type Authenticator interface {
    Authenticate(user, password string) (Identity, error)
}

// Identity is the authenticated subject and its scopes, carried in the request
// context. Single-operator yields one fixed subject with the full scope set;
// RBAC later yields per-user subjects and scopes.
type Identity struct {
    Subject string
    Scopes  []string
}
```

- **Single-operator impl** verifies the submitted password against an argon2id hash, reusing
  `config.HashToken` / `config.VerifyToken` (the same PHC-format scheme used for API tokens).
- **Credential provisioning:** new `-operator-file` flag → a YAML file holding `password_hash:`
  (and optionally `username:`, default `operator`). Reloaded on **SIGHUP** alongside
  `keys.yaml` and `hosts/*.yaml`, following the existing atomic-swap pattern; a bad reload is
  logged and the previous value retained. The existing `podman-api hash-token <plaintext>`
  subcommand prints a PHC hash usable for `password_hash:` (no new subcommand needed).

### 3.5 Sessions

```go
// SessionStore persists active sessions. In-process now (a map); swappable later
// without touching handlers.
type SessionStore interface {
    Create(id Identity) (token string, err error)
    Lookup(token string) (Identity, bool)
    Delete(token string)
}
```

- On successful login: create a session keyed by a cryptographically-random token, set as an
  **`HttpOnly; Secure; SameSite=Lax`** cookie with a sliding expiry.
- Default impl is an in-process map guarded by a mutex; process restart ⇒ re-login (acceptable
  for a single operator).
- A `requireSession` middleware wraps `/ui/*` (except login/static): missing/invalid session →
  302 to `/ui/login`. The resolved `Identity` is placed in the request context.

### 3.6 CSRF

State-changing requests (`POST`) are protected by **`SameSite=Lax` cookies + a per-session
CSRF token** embedded in forms (hidden field) and sent by HTMX on AJAX POSTs (via an
`hx-headers` / configured header). The middleware rejects POSTs whose token doesn't match the
session. `GET` is always safe.

## 4. Rendering

### 4.1 Templates & partials

`internal/ui/templates/` (embedded):
- **`layout.html`** — chrome: sidebar (host switcher + Jobs link), top bar, operator menu,
  asset `<link>`/`<script>`.
- **Content blocks** (`{{define}}`): `dashboard`, `login`, `host-instances`, `instance-detail`,
  `deploy-form`, `logs`, `jobs`, `job-detail`.
- **Dual render path:** a helper renders either the **full page** (layout + content block) on a
  normal navigation, or the **bare fragment** (content block only) when `HX-Request: true` is
  present. One template definition, two outputs — no duplicate markup.

### 4.2 Layout

Persistent **left-sidebar** shell: hosts pinned in the left rail (always-visible switcher) +
main content panel. Selecting a host swaps the instance list into the panel; selecting an
instance swaps in its detail; lifecycle actions and deploy re-render the panel in place via
HTMX. The chrome stays put across swaps.

### 4.3 Deploy form (generic, meta-driven)

Generated from the selected template's `render.Meta`:
- **Host** — fixed (deploy is opened from a host's "+ Deploy").
- **Template** — select from loaded templates.
- **Slug** — text input → `template/slug` instance identity.
- **Parameters** — `Meta.Parameters.Required` (required inputs) and `.Optional` (optional inputs).
- **Secrets — per instance** (`Meta.Secrets.PerInstance`) — write-only password inputs.
- **Secrets — host-referenced** (`Meta.Secrets.PerHostReferenced`) — a **dropdown of the host's
  existing secrets** (with a present/absent check), since these must already exist on the host.

On submit → `instance.Service.Apply` creates the instance; the panel swaps to the new
instance's detail. Validation errors (missing required param, absent host secret) re-render the
form fragment with field-level messages.

### 4.4 Instance detail

Renders the existing `instance.Observed`: pod status, containers (image, status, restarts,
port mappings), volumes (name, size), masked env summary. Lifecycle action buttons
(Start/Stop/Restart/Upgrade/Delete) issue HTMX POSTs that re-render the panel. A "View logs"
control fetches a **static tail** (last N lines).

### 4.5 Assets — vendored

`htmx.min.js` + `pure-min.css` (+ a small `app.css`) checked into the repo and embedded; no
CDN, no build step — the single binary stays self-contained and works offline/air-gapped.
Served with cache headers; filenames content-hashed (or versioned) for cache-busting.

## 5. Error handling

- Service errors map to inline HTML error fragments; status codes reuse the existing
  `internal/api/errors.go` taxonomy, but the body is friendly HTML, not JSON.
- Auth failures on `/ui/*` → redirect to `/ui/login`. API paths keep their existing 401/403
  JSON bodies (unchanged).
- Form validation failures re-render the form fragment with field-level messages and a non-2xx
  status where appropriate for HTMX.

## 6. Testing

- `httptest` over the handlers: login success/failure, session cookie issuance, redirect when
  unauthenticated, CSRF rejection, lifecycle action dispatch.
- HTML render assertions: host list, instance detail, and **deploy-form field generation from a
  fixture template's meta** — lightweight string/structure checks, no JS engine (HTMX is
  progressive enhancement; the server output is the contract).
- Backend via the existing `internal/podman/fake`, so UI tests run with no podman host.
- `Authenticator` / `SessionStore` unit tests (bcrypt verify, token lifecycle, expiry).
- Keep `gofmt -l .` empty and `go vet` clean; build/test under the remote-client tags
  (`make build` / `make test`), per CLAUDE.md.

## 7. Seams (acceptance criteria — roadmap §3.4, §4 risk "seam discipline")

These are requirements, not nice-to-haves:
- `Authenticator` interface — single-operator bcrypt now; RBAC attaches the same interface later.
- `SessionStore` interface — in-process now; persistent/commercial store later.
- `Identity{Subject, Scopes}` carried in context — RBAC-ready from day one.

## 8. Done when

A configured operator opens `/ui`, logs in, sees the host list, selects a host, deploys an
instance from a loaded template via the generic form, watches it come up in the instance detail,
and drives Start/Stop/Restart/Upgrade/Delete + a logs tail — entirely from the browser, no CLI.
