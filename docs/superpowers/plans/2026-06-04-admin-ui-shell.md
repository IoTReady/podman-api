# Admin UI Shell Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single-operator, server-rendered admin UI (login → host list → instance detail → deploy → lifecycle) served from `embed.FS` with HTMX + PureCSS, mounted under `/ui` on the existing listener, talking in-process to `instance.Service`.

**Architecture:** New `internal/ui` package mirroring `internal/api` but rendering HTML. Auth is a pluggable `Authenticator` (single-operator argon2id now) + in-process `SessionStore`, both behind interfaces for future RBAC. Templates use Go `html/template` with one definition rendered either as a full page (layout + block) or a bare HTMX fragment, keyed off the `HX-Request` header. UI handlers call `*instance.Service`/`store.JobStore` directly. The UI mounts only when `-operator-file` is configured.

**Tech Stack:** Go 1.x `html/template`, `embed.FS`, `net/http` (Go 1.22 routing), HTMX (vendored), PureCSS (vendored), `golang.org/x/crypto/argon2` (via existing `config.HashToken`/`VerifyToken`). Build/test under remote-client tags via `make build` / `make test`.

**Spec:** `docs/superpowers/specs/2026-06-04-admin-ui-shell-design.md`

---

## Conventions for every task

- Build & test with the Makefile tags: `make test` runs unit tests; for a single package use
  `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/...`.
  A shell alias for the plan: `export TT='containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper'` then `go test -tags "$TT" ./internal/ui/...`.
- Keep `gofmt -l .` empty and `go vet ./...` (with tags) clean before each commit.
- Commit messages reference `#62` and end with the Co-Authored-By trailer used in this repo.
- Tests use the existing `internal/podman/fake` backend; no podman host required.

---

## File Structure

**New package `internal/ui/`:**
- `ui.go` — `UI` struct (holds `*instance.Service`, `store.JobStore`, `Authenticator`, `SessionStore`, parsed `*template.Template`), `New(...)` constructor, `Handler() http.Handler` returning the `/ui` sub-router.
- `auth.go` — `Authenticator` interface, `Identity` struct, `OperatorAuthenticator` (argon2id, single user), `Operator` credential type + loader.
- `session.go` — `SessionStore` interface, `MemorySessionStore` (mutex map, sliding expiry, random token), cookie helpers, CSRF token derivation.
- `middleware.go` — `requireSession` (redirect to login + inject `Identity`), `requireCSRF` (POST token check).
- `render.go` — `render(w, r, block string, data any)` helper that picks full-page vs fragment off `HX-Request`; error-fragment helper mapping `instance` sentinel errors to status + message.
- `handlers_auth.go` — `GET/POST /ui/login`, `POST /ui/logout`.
- `handlers_hosts.go` — dashboard (`GET /ui`), host instance list (`GET /ui/hosts/{host}`).
- `handlers_instances.go` — instance detail, lifecycle actions, logs tail.
- `handlers_deploy.go` — deploy form (GET) + create (POST).
- `handlers_jobs.go` — jobs list/detail (read).
- `templates/` — `layout.html`, `login.html`, `dashboard.html`, `host-instances.html`, `instance-detail.html`, `deploy-form.html`, `logs.html`, `jobs.html`, `job-detail.html` (each defines a named block).
- `static/` — `htmx.min.js`, `pure-min.css`, `app.css` (vendored).
- `embed.go` — `//go:embed templates/* static/*` declarations.

**Modified:**
- `internal/config/operator.go` (new) — `Operator` parse/load, mirrors `auth.go` patterns.
- `cmd/podman-api/main.go` — `-operator-file` flag, load + SIGHUP reload, mount UI when configured.

---

## Task 1: Operator credential type + loader

**Files:**
- Create: `internal/config/operator.go`
- Test: `internal/config/operator_test.go`

- [ ] **Step 1: Write the failing test**

```go
package config

import "testing"

func TestParseOperatorYAML(t *testing.T) {
	hash, err := HashToken("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte("username: admin\npassword_hash: " + hash + "\n")
	op, err := ParseOperatorYAML(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if op.Username != "admin" {
		t.Errorf("username = %q, want admin", op.Username)
	}
	ok, err := VerifyToken("s3cret", op.PasswordHash)
	if err != nil || !ok {
		t.Errorf("verify = %v, %v; want true, nil", ok, err)
	}
}

func TestParseOperatorYAMLDefaultsUsername(t *testing.T) {
	raw := []byte("password_hash: $argon2id$v=19$m=65536,t=3,p=4$c2FsdHNhbHRzYWx0c2E$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaA\n")
	op, err := ParseOperatorYAML(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if op.Username != "operator" {
		t.Errorf("username = %q, want operator (default)", op.Username)
	}
}

func TestParseOperatorYAMLRequiresHash(t *testing.T) {
	if _, err := ParseOperatorYAML([]byte("username: x\n")); err == nil {
		t.Fatal("expected error when password_hash missing")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/config/ -run TestParseOperatorYAML -v`
Expected: FAIL — `undefined: ParseOperatorYAML`.

- [ ] **Step 3: Write minimal implementation**

```go
package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Operator is the single-operator UI credential, parsed from the -operator-file
// YAML. PasswordHash is an argon2id PHC string (produce one with
// `podman-api hash-token <plaintext>`).
type Operator struct {
	Username     string `yaml:"username"`
	PasswordHash string `yaml:"password_hash"`
}

// ParseOperatorYAML parses an operator credential file. Username defaults to
// "operator" when omitted; password_hash is required.
func ParseOperatorYAML(raw []byte) (Operator, error) {
	var op Operator
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&op); err != nil {
		return Operator{}, fmt.Errorf("parse operator: %w", err)
	}
	if op.PasswordHash == "" {
		return Operator{}, fmt.Errorf("operator: password_hash is required")
	}
	if op.Username == "" {
		op.Username = "operator"
	}
	return op, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags "$TT" ./internal/config/ -run TestParseOperatorYAML -v`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add internal/config/operator.go internal/config/operator_test.go
git commit -m "feat(config): operator credential type + loader (#62)"
```

---

## Task 2: Authenticator interface + single-operator impl

**Files:**
- Create: `internal/ui/auth.go`
- Test: `internal/ui/auth_test.go`

- [ ] **Step 1: Write the failing test**

```go
package ui

import (
	"testing"

	"github.com/iotready/podman-api/internal/config"
)

func TestOperatorAuthenticator(t *testing.T) {
	hash, err := config.HashToken("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	a := NewOperatorAuthenticator(config.Operator{Username: "admin", PasswordHash: hash})

	id, err := a.Authenticate("admin", "hunter2")
	if err != nil {
		t.Fatalf("good creds: %v", err)
	}
	if id.Subject != "admin" {
		t.Errorf("subject = %q, want admin", id.Subject)
	}
	if !id.HasScope("instances:write") {
		t.Errorf("operator identity should hold instances:write")
	}

	if _, err := a.Authenticate("admin", "wrong"); err == nil {
		t.Error("bad password should error")
	}
	if _, err := a.Authenticate("nope", "hunter2"); err == nil {
		t.Error("unknown user should error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/ui/ -run TestOperatorAuthenticator -v`
Expected: FAIL — package/symbols undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// Package ui implements the server-rendered single-operator admin UI.
package ui

import (
	"errors"

	"github.com/iotready/podman-api/internal/config"
)

// ErrAuth is returned for any failed login (unknown user or bad password);
// callers must not distinguish the two (avoids user enumeration).
var ErrAuth = errors.New("invalid credentials")

// Identity is the authenticated subject and its scopes, carried in the request
// context. Single-operator yields one fixed subject with the full scope set;
// future RBAC yields per-user subjects and scopes.
type Identity struct {
	Subject string
	Scopes  []string
}

// HasScope reports whether the identity holds want (supporting the "*"
// wildcard the operator identity is granted).
func (i Identity) HasScope(want string) bool {
	for _, s := range i.Scopes {
		if s == "*" || s == want {
			return true
		}
	}
	return false
}

// Authenticator verifies a login and yields an Identity.
type Authenticator interface {
	Authenticate(user, password string) (Identity, error)
}

// OperatorAuthenticator authenticates the single configured operator against an
// argon2id password hash.
type OperatorAuthenticator struct {
	op config.Operator
}

func NewOperatorAuthenticator(op config.Operator) *OperatorAuthenticator {
	return &OperatorAuthenticator{op: op}
}

func (a *OperatorAuthenticator) Authenticate(user, password string) (Identity, error) {
	if user != a.op.Username {
		return Identity{}, ErrAuth
	}
	ok, err := config.VerifyToken(password, a.op.PasswordHash)
	if err != nil || !ok {
		return Identity{}, ErrAuth
	}
	return Identity{Subject: a.op.Username, Scopes: []string{"*"}}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags "$TT" ./internal/ui/ -run TestOperatorAuthenticator -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ui/auth.go internal/ui/auth_test.go
git commit -m "feat(ui): Authenticator seam + single-operator impl (#62)"
```

---

## Task 3: SessionStore + cookie/CSRF helpers

**Files:**
- Create: `internal/ui/session.go`
- Test: `internal/ui/session_test.go`

- [ ] **Step 1: Write the failing test**

```go
package ui

import (
	"testing"
	"time"
)

func TestMemorySessionStoreLifecycle(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	s := NewMemorySessionStore(time.Hour)
	s.now = func() time.Time { return now }

	id := Identity{Subject: "operator", Scopes: []string{"*"}}
	tok, err := s.Create(id)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("empty token")
	}

	got, ok := s.Lookup(tok)
	if !ok || got.Subject != "operator" {
		t.Fatalf("lookup = %+v, %v", got, ok)
	}

	// Past expiry → gone.
	now = now.Add(2 * time.Hour)
	if _, ok := s.Lookup(tok); ok {
		t.Error("expired session should not resolve")
	}
}

func TestMemorySessionStoreDelete(t *testing.T) {
	s := NewMemorySessionStore(time.Hour)
	tok, _ := s.Create(Identity{Subject: "operator"})
	s.Delete(tok)
	if _, ok := s.Lookup(tok); ok {
		t.Error("deleted session should not resolve")
	}
}

func TestCSRFTokenStablePerSession(t *testing.T) {
	a := csrfToken("session-abc")
	b := csrfToken("session-abc")
	c := csrfToken("session-xyz")
	if a != b {
		t.Error("csrf token must be stable for a session id")
	}
	if a == c {
		t.Error("csrf token must differ across session ids")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestMemorySessionStore|TestCSRF' -v`
Expected: FAIL — symbols undefined.

- [ ] **Step 3: Write minimal implementation**

```go
package ui

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"sync"
	"time"
)

const (
	sessionCookie = "pa_session"
	csrfField     = "csrf_token"
	csrfHeader    = "X-CSRF-Token"
)

// SessionStore persists active sessions. In-process now; swappable later.
type SessionStore interface {
	Create(id Identity) (token string, err error)
	Lookup(token string) (Identity, bool)
	Delete(token string)
}

type sessionEntry struct {
	id      Identity
	expires time.Time
}

// MemorySessionStore is an in-process session store with a sliding TTL.
type MemorySessionStore struct {
	ttl time.Duration
	now func() time.Time

	mu  sync.Mutex
	m   map[string]sessionEntry
}

func NewMemorySessionStore(ttl time.Duration) *MemorySessionStore {
	return &MemorySessionStore{ttl: ttl, now: time.Now, m: map[string]sessionEntry{}}
}

func (s *MemorySessionStore) Create(id Identity) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	tok := base64.RawURLEncoding.EncodeToString(b)
	s.mu.Lock()
	s.m[tok] = sessionEntry{id: id, expires: s.now().Add(s.ttl)}
	s.mu.Unlock()
	return tok, nil
}

func (s *MemorySessionStore) Lookup(tok string) (Identity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[tok]
	if !ok {
		return Identity{}, false
	}
	if s.now().After(e.expires) {
		delete(s.m, tok)
		return Identity{}, false
	}
	// Sliding expiry.
	e.expires = s.now().Add(s.ttl)
	s.m[tok] = e
	return e.id, true
}

func (s *MemorySessionStore) Delete(tok string) {
	s.mu.Lock()
	delete(s.m, tok)
	s.mu.Unlock()
}

// csrfKey is process-random; CSRF tokens are HMAC(sessionID) under it, so they
// are stable per session within a process lifetime and unforgeable without it.
var csrfKey = func() []byte {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return b
}()

func csrfToken(sessionID string) string {
	mac := hmac.New(sha256.New, csrfKey)
	mac.Write([]byte(sessionID))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func csrfEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestMemorySessionStore|TestCSRF' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ui/session.go internal/ui/session_test.go
git commit -m "feat(ui): in-process SessionStore + CSRF token helpers (#62)"
```

---

## Task 4: Vendored assets + embed + template parsing skeleton

**Files:**
- Create: `internal/ui/embed.go`
- Create: `internal/ui/static/htmx.min.js` (download HTMX 2.x minified)
- Create: `internal/ui/static/pure-min.css` (download PureCSS 3.x)
- Create: `internal/ui/static/app.css`
- Create: `internal/ui/templates/layout.html`
- Create: `internal/ui/templates/login.html`
- Create: `internal/ui/ui.go`
- Test: `internal/ui/ui_test.go`

- [ ] **Step 1: Download the vendored assets**

```bash
mkdir -p internal/ui/static internal/ui/templates
curl -fsSL https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js -o internal/ui/static/htmx.min.js
curl -fsSL https://unpkg.com/purecss@3.0.0/build/pure-min.css -o internal/ui/static/pure-min.css
printf 'body{margin:0}.sidebar{min-width:200px}\n' > internal/ui/static/app.css
test -s internal/ui/static/htmx.min.js && test -s internal/ui/static/pure-min.css && echo OK
```
Expected: `OK` (both files non-empty).

- [ ] **Step 2: Write the layout + login templates**

`internal/ui/templates/layout.html`:
```html
{{define "layout"}}<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>podman-api</title>
  <link rel="stylesheet" href="/ui/static/pure-min.css">
  <link rel="stylesheet" href="/ui/static/app.css">
  <script src="/ui/static/htmx.min.js" defer></script>
</head>
<body hx-headers='{"{{.CSRFHeader}}": "{{.CSRF}}"}'>
  {{template "body" .}}
</body>
</html>{{end}}
```

`internal/ui/templates/login.html`:
```html
{{define "login"}}
<div class="login">
  <h1>podman-api</h1>
  {{if .Error}}<p class="error">{{.Error}}</p>{{end}}
  <form method="post" action="/ui/login" class="pure-form pure-form-stacked">
    <input name="username" placeholder="username" autofocus>
    <input name="password" type="password" placeholder="password">
    <button type="submit" class="pure-button pure-button-primary">Sign in</button>
  </form>
</div>
{{end}}
{{define "body"}}{{template "login" .}}{{end}}
```

(Note: `login.html` defines `body` so the login page can render through the layout.)

- [ ] **Step 3: Write the failing test**

`internal/ui/ui_test.go`:
```go
package ui

import "testing"

func TestNewParsesTemplates(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, name := range []string{"layout", "login"} {
		if u.tmpl.Lookup(name) == nil {
			t.Errorf("template %q not parsed", name)
		}
	}
}
```

- [ ] **Step 4: Write the embed + UI skeleton**

`internal/ui/embed.go`:
```go
package ui

import "embed"

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS
```

`internal/ui/ui.go`:
```go
package ui

import (
	"html/template"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// Config wires the UI's dependencies. Auth and Sessions are filled with the
// single-operator defaults by New when left nil.
type Config struct {
	Svc        *instance.Service
	Jobs       store.JobStore
	Auth       Authenticator
	Sessions   SessionStore
	SessionTTL time.Duration
	Secure     bool // set Secure flag on the session cookie (true in production)
}

// UI holds parsed templates and dependencies and produces the /ui sub-router.
type UI struct {
	cfg  Config
	tmpl *template.Template
}

func New(cfg Config) (*UI, error) {
	t, err := template.ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = 12 * time.Hour
	}
	if cfg.Sessions == nil {
		cfg.Sessions = NewMemorySessionStore(cfg.SessionTTL)
	}
	return &UI{cfg: cfg, tmpl: t}, nil
}

// staticHandler serves the embedded /ui/static/* assets.
func (u *UI) staticHandler() http.Handler {
	return http.StripPrefix("/ui/", http.FileServer(http.FS(staticFS)))
}
```

- [ ] **Step 5: Run tests**

Run: `go test -tags "$TT" ./internal/ui/ -run TestNewParsesTemplates -v`
Expected: PASS. Then `go vet -tags "$TT" ./internal/ui/` clean and `gofmt -l internal/ui` empty.

- [ ] **Step 6: Commit**

```bash
git add internal/ui/embed.go internal/ui/ui.go internal/ui/ui_test.go \
        internal/ui/templates/layout.html internal/ui/templates/login.html \
        internal/ui/static/
git commit -m "feat(ui): vendored assets, embed, template parsing skeleton (#62)"
```

---

## Task 5: Render helper (full-page vs HTMX fragment) + error mapping

**Files:**
- Create: `internal/ui/render.go`
- Test: `internal/ui/render_test.go`

- [ ] **Step 1: Write the failing test**

```go
package ui

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/instance"
)

func TestRenderFullPageVsFragment(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	// Full page (no HX-Request) → includes <!DOCTYPE>.
	full := httptest.NewRecorder()
	u.render(full, httptest.NewRequest("GET", "/ui/login", nil), "login", map[string]any{})
	if !strings.Contains(full.Body.String(), "<!DOCTYPE html>") {
		t.Error("full page should include the layout")
	}

	// HX-Request → fragment only, no layout.
	frag := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/ui/login", nil)
	r.Header.Set("HX-Request", "true")
	u.render(frag, r, "login", map[string]any{})
	if strings.Contains(frag.Body.String(), "<!DOCTYPE html>") {
		t.Error("HTMX fragment must not include the layout")
	}
}

func TestErrorStatus(t *testing.T) {
	cases := map[error]int{
		instance.ErrUnknownHost:     http.StatusNotFound,
		instance.ErrInstanceExists:  http.StatusConflict,
		instance.ErrHostDraining:    http.StatusConflict,
		errors.New("boom"):          http.StatusInternalServerError,
	}
	for err, want := range cases {
		if got := errorStatus(err); got != want {
			t.Errorf("errorStatus(%v) = %d, want %d", err, got, want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestRender|TestErrorStatus' -v`
Expected: FAIL — `u.render`/`errorStatus` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
package ui

import (
	"errors"
	"html/template"
	"log"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
)

// render writes block either wrapped in the layout (normal navigation) or bare
// (HTMX fragment, when HX-Request is set). data is shallow-augmented with the
// CSRF token/header for the layout's hx-headers attribute.
func (u *UI) render(w http.ResponseWriter, r *http.Request, block string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["CSRF"] = csrfFromRequest(r)
	data["CSRFHeader"] = csrfHeader

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var err error
	if r.Header.Get("HX-Request") == "true" {
		err = u.tmpl.ExecuteTemplate(w, block, data)
	} else {
		// The page template must (re)define "body" to delegate to block; the
		// layout renders "body". Clone so concurrent requests don't race on
		// redefining "body".
		t, cerr := u.tmpl.Clone()
		if cerr != nil {
			err = cerr
		} else {
			_, derr := t.New("body").Parse(`{{template "` + template.HTMLEscapeString(block) + `" .}}`)
			if derr != nil {
				err = derr
			} else {
				err = t.ExecuteTemplate(w, "layout", data)
			}
		}
	}
	if err != nil {
		log.Printf("ui: render %q: %v", block, err)
	}
}

// csrfFromRequest derives the CSRF token from the request's session cookie, or
// "" when unauthenticated.
func csrfFromRequest(r *http.Request) string {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return ""
	}
	return csrfToken(c.Value)
}

// renderError writes an inline HTML error fragment with the mapped status.
func (u *UI) renderError(w http.ResponseWriter, err error) {
	status := errorStatus(err)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`<div class="error">` + template.HTMLEscapeString(err.Error()) + `</div>`))
}

// errorStatus maps instance sentinel errors to HTTP status codes, mirroring the
// JSON API's taxonomy.
func errorStatus(err error) int {
	switch {
	case errors.Is(err, instance.ErrUnknownHost),
		errors.Is(err, instance.ErrUnknownTemplate),
		errors.Is(err, instance.ErrInstanceNotFound):
		return http.StatusNotFound
	case errors.Is(err, instance.ErrInstanceExists),
		errors.Is(err, instance.ErrHostDraining),
		errors.Is(err, instance.ErrPortConflict):
		return http.StatusConflict
	case errors.Is(err, instance.ErrHostSecretMissing):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestRender|TestErrorStatus' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ui/render.go internal/ui/render_test.go
git commit -m "feat(ui): full-page/fragment render helper + error mapping (#62)"
```

---

## Task 6: Login/logout handlers + session & CSRF middleware

**Files:**
- Create: `internal/ui/middleware.go`
- Create: `internal/ui/handlers_auth.go`
- Modify: `internal/ui/ui.go` (add `Handler()` wiring login/logout/static + middleware)
- Test: `internal/ui/handlers_auth_test.go`

- [ ] **Step 1: Write the failing test**

```go
package ui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
)

func testUI(t *testing.T) *UI {
	t.Helper()
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestLoginSuccessSetsCookie(t *testing.T) {
	u := testUI(t)
	form := url.Values{"username": {"op"}, "password": {"pw"}}
	r := httptest.NewRequest("POST", "/ui/login", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)

	if w.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/ui" {
		t.Errorf("redirect = %q, want /ui", loc)
	}
	if !strings.Contains(w.Header().Get("Set-Cookie"), sessionCookie) {
		t.Error("expected session cookie")
	}
}

func TestLoginFailureRerendersForm(t *testing.T) {
	u := testUI(t)
	form := url.Values{"username": {"op"}, "password": {"WRONG"}}
	r := httptest.NewRequest("POST", "/ui/login", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if !strings.Contains(w.Body.String(), "invalid credentials") {
		t.Error("expected error message in re-rendered form")
	}
}

func TestProtectedRouteRedirectsWhenUnauthenticated(t *testing.T) {
	u := testUI(t)
	r := httptest.NewRequest("GET", "/ui", nil)
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/ui/login" {
		t.Fatalf("got %d %q; want 303 /ui/login", w.Code, w.Header().Get("Location"))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestLogin|TestProtected' -v`
Expected: FAIL — `Handler` not wired / login handlers undefined.

- [ ] **Step 3: Write the middleware**

`internal/ui/middleware.go`:
```go
package ui

import (
	"context"
	"net/http"
)

type ctxKey int

const identityKey ctxKey = 0

// requireSession resolves the session cookie to an Identity and injects it into
// the context, or redirects to /ui/login when absent/invalid.
func (u *UI) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookie)
		if err != nil {
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}
		id, ok := u.cfg.Sessions.Lookup(c.Value)
		if !ok {
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}
		ctx := context.WithValue(r.Context(), identityKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireCSRF rejects unsafe methods whose CSRF token (form field or header)
// does not match the session-derived token.
func (u *UI) requireCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		c, err := r.Cookie(sessionCookie)
		if err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		want := csrfToken(c.Value)
		got := r.Header.Get(csrfHeader)
		if got == "" {
			got = r.FormValue(csrfField)
		}
		if !csrfEqual(got, want) {
			http.Error(w, "bad csrf token", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func identityFrom(r *http.Request) (Identity, bool) {
	id, ok := r.Context().Value(identityKey).(Identity)
	return id, ok
}
```

- [ ] **Step 4: Write login/logout handlers + Handler() wiring**

`internal/ui/handlers_auth.go`:
```go
package ui

import "net/http"

func (u *UI) loginForm(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, "login", map[string]any{})
}

func (u *UI) login(w http.ResponseWriter, r *http.Request) {
	id, err := u.cfg.Auth.Authenticate(r.FormValue("username"), r.FormValue("password"))
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		u.render(w, r, "login", map[string]any{"Error": ErrAuth.Error()})
		return
	}
	tok, err := u.cfg.Sessions.Create(id)
	if err != nil {
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    tok,
		Path:     "/ui",
		HttpOnly: true,
		Secure:   u.cfg.Secure,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (u *UI) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		u.cfg.Sessions.Delete(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/ui", MaxAge: -1})
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}
```

Append to `internal/ui/ui.go`:
```go
// Handler returns the /ui sub-router. Mount it on the main mux with
// mux.Handle("/ui/", ui.Handler()) and mux.Handle("/ui", ui.Handler()).
func (u *UI) Handler() http.Handler {
	mux := http.NewServeMux()

	// Public.
	mux.HandleFunc("GET /ui/login", u.loginForm)
	mux.Handle("POST /ui/login", u.requireCSRF(http.HandlerFunc(u.login)))
	mux.Handle("/ui/static/", u.staticHandler())

	// Guarded (added in later tasks): dashboard, hosts, instances, deploy, jobs.
	guard := func(h http.HandlerFunc) http.Handler { return u.requireSession(h) }
	guardW := func(h http.HandlerFunc) http.Handler { return u.requireSession(u.requireCSRF(h)) }
	mux.Handle("GET /ui", guard(u.dashboard))
	mux.Handle("POST /ui/logout", guardW(u.logout))

	return mux
}
```

NOTE: `POST /ui/login` is intentionally CSRF-exempt from a *session* token (there is no
session yet) — it is protected by `SameSite=Lax` on navigation and is itself the
credential check. The `requireCSRF` wrapper on login will see no cookie; adjust it to allow
the login path. Simplest: do NOT wrap `POST /ui/login` with `requireCSRF`. Replace the login
line with:
```go
	mux.HandleFunc("POST /ui/login", u.login)
```

- [ ] **Step 5: Add a temporary dashboard stub so the package compiles**

In `internal/ui/handlers_hosts.go` (created fully in Task 7, stub now):
```go
package ui

import "net/http"

func (u *UI) dashboard(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, "dashboard", map[string]any{})
}
```
Add a minimal `internal/ui/templates/dashboard.html`:
```html
{{define "dashboard"}}<h1>Hosts</h1>{{end}}
```

- [ ] **Step 6: Run tests**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestLogin|TestProtected' -v`
Expected: PASS. `go vet` + `gofmt -l` clean.

- [ ] **Step 7: Commit**

```bash
git add internal/ui/middleware.go internal/ui/handlers_auth.go internal/ui/ui.go \
        internal/ui/handlers_hosts.go internal/ui/templates/dashboard.html
git commit -m "feat(ui): login/logout + session & CSRF middleware (#62)"
```

---

## Task 7: Dashboard (host list) + host instance list

**Files:**
- Modify: `internal/ui/handlers_hosts.go`
- Create/replace: `internal/ui/templates/dashboard.html`, `internal/ui/templates/host-instances.html`
- Modify: `internal/ui/ui.go` (`Handler()` — add `GET /ui/hosts/{host}`)
- Test: `internal/ui/handlers_hosts_test.go`

- [ ] **Step 1: Write the failing test**

Use the fake podman client to build a real `*instance.Service`. Pattern (verify exact fake constructor against `internal/podman/fake/fake.go` and existing `internal/api/*_test.go` helpers):
```go
package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func uiWithService(t *testing.T) *UI {
	t.Helper()
	fc := fake.New() // confirm constructor name in fake.go; seed one host "edge-1"
	hosts := []config.Host{{ID: "edge-1"}}
	svc := instance.NewService(fc, hosts, nil)
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// authedGet drives a request through a real session by logging in first.
func authedGet(t *testing.T, u *UI, path string) *httptest.ResponseRecorder {
	t.Helper()
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	r := httptest.NewRequest("GET", path, nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	return w
}

func TestDashboardListsHosts(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "edge-1") {
		t.Error("dashboard should list host edge-1")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/ui/ -run TestDashboardListsHosts -v`
Expected: FAIL — dashboard renders empty `<h1>Hosts</h1>` stub, no `edge-1`.

- [ ] **Step 3: Implement handlers**

Replace `internal/ui/handlers_hosts.go`:
```go
package ui

import "net/http"

func (u *UI) dashboard(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, "dashboard", map[string]any{
		"Hosts": u.cfg.Svc.Hosts(),
	})
}

func (u *UI) hostInstances(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	obs, err := u.cfg.Svc.ListAllInstances(r.Context(), host)
	if err != nil {
		u.renderError(w, err)
		return
	}
	u.render(w, r, "host-instances", map[string]any{
		"Host":      host,
		"Instances": obs,
		"Templates": u.cfg.Svc.Templates(),
	})
}
```

`internal/ui/templates/dashboard.html`:
```html
{{define "dashboard"}}
<div class="shell">
  <nav class="sidebar">
    <div class="label">HOSTS</div>
    <ul>
      {{range .Hosts}}
      <li><a href="/ui/hosts/{{.ID}}" hx-get="/ui/hosts/{{.ID}}" hx-target="#main">{{.ID}}</a></li>
      {{end}}
    </ul>
    <a href="/ui/jobs" hx-get="/ui/jobs" hx-target="#main">Jobs</a>
    <form method="post" action="/ui/logout"><input type="hidden" name="{{$.CSRFHeader | printf "%s"}}"><button>Sign out</button></form>
  </nav>
  <main id="main">
    <p>Select a host.</p>
  </main>
</div>
{{end}}
{{define "body"}}{{template "dashboard" .}}{{end}}
```

(Note the logout form needs the CSRF token in a hidden field named `csrf_token`; replace the
hidden input with `<input type="hidden" name="csrf_token" value="{{.CSRF}}">`.)

`internal/ui/templates/host-instances.html`:
```html
{{define "host-instances"}}
<div class="host-head">
  <strong>{{.Host}}</strong> · {{len .Instances}} instances
  <a class="pure-button" href="/ui/hosts/{{.Host}}/deploy" hx-get="/ui/hosts/{{.Host}}/deploy" hx-target="#main">+ Deploy</a>
</div>
<table class="pure-table">
  <tbody>
  {{range .Instances}}
    <tr hx-get="/ui/hosts/{{$.Host}}/instances/{{.Template}}/{{.Slug}}" hx-target="#main" style="cursor:pointer">
      <td>{{.Template}} / {{.Slug}}</td>
      <td>{{.Pod.Status}}</td>
    </tr>
  {{end}}
  </tbody>
</table>
{{end}}
{{define "body"}}{{template "host-instances" .}}{{end}}
```

- [ ] **Step 4: Wire the route in `Handler()`**

Add inside `Handler()` in `internal/ui/ui.go`:
```go
	mux.Handle("GET /ui/hosts/{host}", guard(u.hostInstances))
```

- [ ] **Step 5: Run tests**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestDashboard|TestLogin|TestProtected' -v`
Expected: PASS. Add a `TestHostInstancesList` asserting the instance list renders for a seeded instance if the fake supports seeding; otherwise assert the empty-list page renders 200 with the host name and "+ Deploy".

- [ ] **Step 6: Commit**

```bash
git add internal/ui/handlers_hosts.go internal/ui/templates/dashboard.html \
        internal/ui/templates/host-instances.html internal/ui/ui.go \
        internal/ui/handlers_hosts_test.go
git commit -m "feat(ui): dashboard host list + per-host instance list (#62)"
```

---

## Task 8: Instance detail + lifecycle actions + logs tail

**Files:**
- Create: `internal/ui/handlers_instances.go`
- Create: `internal/ui/templates/instance-detail.html`, `internal/ui/templates/logs.html`
- Modify: `internal/ui/ui.go` (`Handler()` routes)
- Test: `internal/ui/handlers_instances_test.go`

- [ ] **Step 1: Write the failing test**

```go
package ui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// authedAction drives an authenticated POST with a valid CSRF token.
func authedAction(t *testing.T, u *UI, path string) *httptest.ResponseRecorder {
	t.Helper()
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{csrfField: {csrfToken(tok)}}
	r := httptest.NewRequest("POST", path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	return w
}

func TestLifecycleActionRejectsUnknownAction(t *testing.T) {
	u := uiWithService(t)
	w := authedAction(t, u, "/ui/hosts/edge-1/instances/postgres/main/frobnicate")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for unknown action", w.Code)
	}
}

func TestLifecycleActionMissingCSRFForbidden(t *testing.T) {
	u := uiWithService(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op"})
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/instances/postgres/main/stop", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (no csrf token)", w.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/ui/ -run TestLifecycle -v`
Expected: FAIL — routes/handlers undefined.

- [ ] **Step 3: Implement handlers**

`internal/ui/handlers_instances.go`:
```go
package ui

import (
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
)

func (u *UI) instanceDetail(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, err)
		return
	}
	u.render(w, r, "instance-detail", map[string]any{"Host": host, "Inst": obs})
}

// lifecycle dispatches start/stop/restart/upgrade/delete, then re-renders the
// instance detail (or the host list, for delete).
func (u *UI) lifecycle(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	action := r.PathValue("action")
	ctx := r.Context()

	var err error
	switch action {
	case "start":
		err = u.cfg.Svc.Start(ctx, host, tmpl, slug)
	case "stop":
		err = u.cfg.Svc.Stop(ctx, host, tmpl, slug)
	case "restart":
		err = u.cfg.Svc.Restart(ctx, host, tmpl, slug)
	case "upgrade":
		err = u.cfg.Svc.Upgrade(ctx, host, instance.ApplyRequest{Template: tmpl, Slug: slug}, "")
	case "delete":
		err = u.cfg.Svc.Delete(ctx, host, tmpl, slug, instance.DeleteOptions{})
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	if err != nil {
		u.renderError(w, err)
		return
	}
	if action == "delete" {
		// Re-render the host instance list after removal.
		obs, lerr := u.cfg.Svc.ListAllInstances(ctx, host)
		if lerr != nil {
			u.renderError(w, lerr)
			return
		}
		u.render(w, r, "host-instances", map[string]any{
			"Host": host, "Instances": obs, "Templates": u.cfg.Svc.Templates(),
		})
		return
	}
	obs, gerr := u.cfg.Svc.Get(ctx, host, tmpl, slug)
	if gerr != nil {
		u.renderError(w, gerr)
		return
	}
	u.render(w, r, "instance-detail", map[string]any{"Host": host, "Inst": obs})
}

// logsTail renders the last N log lines as static text.
func (u *UI) logsTail(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	ch, err := u.cfg.Svc.Logs(r.Context(), host, tmpl, slug, "", podman.LogOptions{Tail: 200})
	if err != nil {
		u.renderError(w, err)
		return
	}
	var lines []string
	for ln := range ch {
		lines = append(lines, ln.Line)
	}
	u.render(w, r, "logs", map[string]any{"Host": host, "Template": tmpl, "Slug": slug, "Lines": lines})
}
```

VERIFY before writing: `podman.LogOptions` has a `Tail` field (check `internal/podman/types.go`); if
the field is named differently (e.g. `TailLines`), use that. `Upgrade`'s third arg is the image
override — `""` means "re-apply current image"; confirm this matches `service.go:376` semantics, and
if `Upgrade` requires a non-empty image, instead read the current image from `Get` and pass it.

`internal/ui/templates/instance-detail.html`:
```html
{{define "instance-detail"}}
<div class="inst-head">
  <strong>{{.Inst.Template}} / {{.Inst.Slug}}</strong> · {{.Inst.Pod.Status}}
  <span class="actions">
    <button hx-post="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/start"  hx-target="#main">Start</button>
    <button hx-post="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/stop"    hx-target="#main">Stop</button>
    <button hx-post="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/restart" hx-target="#main">Restart</button>
    <button hx-post="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/upgrade" hx-target="#main">Upgrade</button>
    <button hx-post="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/delete"  hx-target="#main"
            hx-confirm="Delete {{.Inst.Template}}/{{.Inst.Slug}}?">Delete</button>
  </span>
</div>
<div class="label">CONTAINERS</div>
{{range .Inst.Containers}}<div>{{.Name}} — {{.Image}} — {{.Status}} (restarts {{.RestartCount}})</div>{{end}}
<div class="label">VOLUMES</div>
{{range .Inst.Volumes}}<div>{{.Name}} {{.SizeBytes}}</div>{{end}}
<div class="label">ENV</div>
{{range $k, $v := .Inst.EnvSummary}}<div>{{$k}}={{$v}}</div>{{end}}
<button hx-get="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/logs" hx-target="#logs">View logs</button>
<pre id="logs"></pre>
{{end}}
{{define "body"}}{{template "instance-detail" .}}{{end}}
```

`internal/ui/templates/logs.html`:
```html
{{define "logs"}}{{range .Lines}}{{.}}
{{end}}{{end}}
```

- [ ] **Step 4: Wire routes in `Handler()`**

```go
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}", guard(u.instanceDetail))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/logs", guard(u.logsTail))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/{action}", guardW(u.lifecycle))
```

- [ ] **Step 5: Run tests**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestLifecycle|TestInstance' -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/ui/handlers_instances.go internal/ui/templates/instance-detail.html \
        internal/ui/templates/logs.html internal/ui/ui.go internal/ui/handlers_instances_test.go
git commit -m "feat(ui): instance detail, lifecycle actions, logs tail (#62)"
```

---

## Task 9: Deploy form (GET) + create (POST)

**Files:**
- Create: `internal/ui/handlers_deploy.go`
- Create: `internal/ui/templates/deploy-form.html`
- Modify: `internal/ui/ui.go` (`Handler()` routes)
- Test: `internal/ui/handlers_deploy_test.go`

- [ ] **Step 1: Write the failing test**

Seed the service with a fixture template carrying meta (one required param, one per-instance
secret) — reuse the parsing already exercised in `internal/config/templates_test.go`. Build a
`config.Template` via `config.LoadTemplates(os.DirFS("testdata"), ".")` with a small fixture, or
construct `config.Template{Meta: render.Meta{ID: "demo", Parameters: render.Parameters{Required: []string{"version"}}, Secrets: render.Secrets{PerInstance: []string{"password"}}}, Body: "..."}` directly.

```go
package ui

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestDeployFormRendersMetaFields(t *testing.T) {
	u := uiWithTemplate(t) // helper: service seeded with the "demo" template above on host edge-1
	w := authedGet(t, u, "/ui/hosts/edge-1/deploy")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `name="param.version"`) {
		t.Error("required param 'version' should render an input")
	}
	if !strings.Contains(body, `name="secret.password"`) {
		t.Error("per-instance secret 'password' should render a password input")
	}
}

func TestDeployCreateMissingRequiredParamRerendersForm(t *testing.T) {
	u := uiWithTemplate(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{
		csrfField:  {csrfToken(tok)},
		"template": {"demo"},
		"slug":     {"main"},
		// no param.version → validation should fail
	}
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/deploy", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code == http.StatusOK || w.Code == http.StatusSeeOther {
		t.Fatalf("expected a non-success status for invalid deploy, got %d", w.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/ui/ -run TestDeploy -v`
Expected: FAIL — routes/handlers undefined.

- [ ] **Step 3: Implement handlers**

`internal/ui/handlers_deploy.go`:
```go
package ui

import (
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
)

func (u *UI) deployForm(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	hostSecrets, _ := u.cfg.Svc.HostSecrets(r.Context(), host) // best-effort for the dropdown
	u.render(w, r, "deploy-form", map[string]any{
		"Host":        host,
		"Templates":   u.cfg.Svc.Templates(),
		"HostSecrets": hostSecrets,
	})
}

func (u *UI) deployCreate(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	req := instance.ApplyRequest{
		Template:   r.FormValue("template"),
		Slug:       r.FormValue("slug"),
		Parameters: map[string]any{},
		Secrets:    map[string]string{},
	}
	for k, vs := range r.Form {
		if v := vs[0]; strings.HasPrefix(k, "param.") {
			req.Parameters[strings.TrimPrefix(k, "param.")] = v
		} else if strings.HasPrefix(k, "secret.") {
			req.Secrets[strings.TrimPrefix(k, "secret.")] = v
		}
	}

	if err := u.cfg.Svc.Apply(r.Context(), host, req, instance.ApplyOptions{Replace: false}); err != nil {
		// Re-render the form with the error and the user's entries preserved.
		w.WriteHeader(errorStatus(err))
		u.render(w, r, "deploy-form", map[string]any{
			"Host":      host,
			"Templates": u.cfg.Svc.Templates(),
			"Error":     err.Error(),
			"Selected":  req.Template,
			"Slug":      req.Slug,
		})
		return
	}
	// Success → swap to the new instance's detail.
	obs, err := u.cfg.Svc.Get(r.Context(), host, req.Template, req.Slug)
	if err != nil {
		u.renderError(w, err)
		return
	}
	u.render(w, r, "instance-detail", map[string]any{"Host": host, "Inst": obs})
}

var _ = config.Operator{} // keep import if unused after edits; remove if not needed
```

(Remove the trailing `var _` line if `config` ends up used elsewhere in the file; it is only a
guard against an unused-import error during incremental editing.)

`internal/ui/templates/deploy-form.html`:
```html
{{define "deploy-form"}}
<form hx-post="/ui/hosts/{{.Host}}/deploy" hx-target="#main" class="pure-form pure-form-stacked">
  <input type="hidden" name="csrf_token" value="{{.CSRF}}">
  {{if .Error}}<p class="error">{{.Error}}</p>{{end}}
  <div class="label">HOST</div><div>{{.Host}}</div>

  <label>Template
    <select name="template" hx-get="/ui/hosts/{{.Host}}/deploy" hx-target="#main" hx-trigger="change" hx-include="closest form">
      {{range .Templates}}<option value="{{.Meta.ID}}" {{if eq .Meta.ID $.Selected}}selected{{end}}>{{.Meta.ID}}</option>{{end}}
    </select>
  </label>
  <label>Slug <input name="slug" value="{{.Slug}}"></label>

  {{range .Templates}}{{if eq .Meta.ID $.Selected}}
    <div class="label">PARAMETERS</div>
    {{range .Meta.Parameters.Required}}<label>{{.}} (required) <input name="param.{{.}}" required></label>{{end}}
    {{range .Meta.Parameters.Optional}}<label>{{.}} <input name="param.{{.}}"></label>{{end}}
    <div class="label">SECRETS</div>
    {{range .Meta.Secrets.PerInstance}}<label>{{.}} <input type="password" name="secret.{{.}}"></label>{{end}}
    {{range .Meta.Secrets.PerHostReferenced}}
      <label>{{.}} (host secret)
        <select name="secret.{{.}}">
          {{range $.HostSecrets}}<option value="{{.Name}}">{{.Name}}</option>{{end}}
        </select>
      </label>
    {{end}}
  {{end}}{{end}}

  <button type="submit" class="pure-button pure-button-primary">Deploy</button>
</form>
{{end}}
{{define "body"}}{{template "deploy-form" .}}{{end}}
```

NOTE on the template-meta fields: when no template is `Selected`, the param/secret block is
empty and selecting a template re-fetches the form (the `<select>` has `hx-get` + `hx-trigger=change`).
The first template should be pre-selected on initial GET so fields show immediately — set
`"Selected"` to the first template's `Meta.ID` in `deployForm` when none was submitted.

- [ ] **Step 4: Pre-select the first template in `deployForm`**

Adjust `deployForm` to set `"Selected"` to the form value `template` (if present) else the first
template ID:
```go
	sel := r.FormValue("template")
	tmpls := u.cfg.Svc.Templates()
	if sel == "" && len(tmpls) > 0 {
		sel = tmpls[0].Meta.ID
	}
	// add "Selected": sel to the render data map
```

- [ ] **Step 5: Wire routes in `Handler()`**

```go
	mux.Handle("GET /ui/hosts/{host}/deploy", guard(u.deployForm))
	mux.Handle("POST /ui/hosts/{host}/deploy", guardW(u.deployCreate))
```

- [ ] **Step 6: Run tests**

Run: `go test -tags "$TT" ./internal/ui/ -run 'TestDeploy' -v`
Expected: PASS. Confirm `render.Validate` is what rejects the missing required param (it is called
inside `Service.Apply`), so the create test exercises real validation.

- [ ] **Step 7: Commit**

```bash
git add internal/ui/handlers_deploy.go internal/ui/templates/deploy-form.html \
        internal/ui/ui.go internal/ui/handlers_deploy_test.go
git commit -m "feat(ui): meta-driven deploy form + create (#62)"
```

---

## Task 10: Jobs list/detail (read)

**Files:**
- Create: `internal/ui/handlers_jobs.go`
- Create: `internal/ui/templates/jobs.html`, `internal/ui/templates/job-detail.html`
- Modify: `internal/ui/ui.go` (`Handler()` routes)
- Test: `internal/ui/handlers_jobs_test.go`

- [ ] **Step 1: Write the failing test**

Verify the `store.JobStore` read methods first (open `internal/store/store.go` / `internal/store/jobs.go`):
confirm the list method name and signature (e.g. `ListJobs(ctx, store.JobFilter) ([]store.Job, error)`)
and the get method (`GetJob(ctx, id) (store.Job, error)`). Use `internal/store/memory.go` as the test backend.

```go
package ui

import (
	"net/http"
	"testing"
)

func TestJobsListRendersWhenStoreNil(t *testing.T) {
	u := uiWithService(t) // Jobs left nil
	w := authedGet(t, u, "/ui/jobs")
	// With no job store, the page should still render (empty / disabled notice), not 500.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./internal/ui/ -run TestJobs -v`
Expected: FAIL — route/handler undefined.

- [ ] **Step 3: Implement handlers**

`internal/ui/handlers_jobs.go` (adjust method names to the verified `store.JobStore` API):
```go
package ui

import "net/http"

func (u *UI) jobsList(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Jobs == nil {
		u.render(w, r, "jobs", map[string]any{"Disabled": true})
		return
	}
	jobs, err := u.cfg.Jobs.ListJobs(r.Context(), store.JobFilter{}) // verify name/signature
	if err != nil {
		u.renderError(w, err)
		return
	}
	u.render(w, r, "jobs", map[string]any{"Jobs": jobs})
}

func (u *UI) jobDetail(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Jobs == nil {
		http.NotFound(w, r)
		return
	}
	j, err := u.cfg.Jobs.GetJob(r.Context(), r.PathValue("id")) // verify name/signature
	if err != nil {
		u.renderError(w, err)
		return
	}
	u.render(w, r, "job-detail", map[string]any{"Job": j})
}
```
Add the `store` import. If the method names differ, fix them and the test together.

`internal/ui/templates/jobs.html`:
```html
{{define "jobs"}}
<h2>Jobs</h2>
{{if .Disabled}}<p>Job store is not enabled.</p>{{else}}
<table class="pure-table"><tbody>
{{range .Jobs}}<tr hx-get="/ui/jobs/{{.ID}}" hx-target="#main"><td>{{.ID}}</td><td>{{.Kind}}</td><td>{{.State}}</td></tr>{{end}}
</tbody></table>
{{end}}
{{end}}
{{define "body"}}{{template "jobs" .}}{{end}}
```

`internal/ui/templates/job-detail.html`:
```html
{{define "job-detail"}}
<h2>Job {{.Job.ID}} — {{.Job.State}}</h2>
<div>kind: {{.Job.Kind}}</div>
{{if .Job.Error}}<div class="error">{{.Job.Error}}</div>{{end}}
<ol>{{range .Job.Steps}}<li>{{.Step}} {{.Detail}}</li>{{end}}</ol>
{{end}}
{{define "body"}}{{template "job-detail" .}}{{end}}
```

- [ ] **Step 4: Wire routes in `Handler()`**

```go
	mux.Handle("GET /ui/jobs", guard(u.jobsList))
	mux.Handle("GET /ui/jobs/{id}", guard(u.jobDetail))
```

- [ ] **Step 5: Run tests**

Run: `go test -tags "$TT" ./internal/ui/ -run TestJobs -v`
Expected: PASS. Add a `TestJobDetail` using `internal/store/memory.go` seeded with one job if time permits.

- [ ] **Step 6: Commit**

```bash
git add internal/ui/handlers_jobs.go internal/ui/templates/jobs.html \
        internal/ui/templates/job-detail.html internal/ui/ui.go internal/ui/handlers_jobs_test.go
git commit -m "feat(ui): jobs list + detail (read) (#62)"
```

---

## Task 11: Wire the UI into the binary (flag, load, SIGHUP reload, mount)

**Files:**
- Modify: `cmd/podman-api/main.go`
- Test: `cmd/podman-api/main_test.go` (extend) and/or `cmd/podman-api/e2e_integration_test.go`

- [ ] **Step 1: Write the failing test**

Add a unit test that builds the router with the UI mounted and asserts an unauthenticated
`GET /ui` redirects to `/ui/login`, and `GET /` redirects to `/ui`. If `main.go`'s wiring is not
unit-testable as-is, extract a small `buildHandler(...)` helper that returns the composed
`http.Handler` and test that. Example (in `cmd/podman-api/main_test.go`):
```go
func TestRootRedirectsToUI(t *testing.T) {
	h := buildHandler(testDeps(t)) // testDeps wires a fake svc + operator + nil jobs
	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/ui" {
		t.Fatalf("got %d %q", w.Code, w.Header().Get("Location"))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags "$TT" ./cmd/podman-api/ -run TestRootRedirectsToUI -v`
Expected: FAIL — no UI mounted / `buildHandler` undefined.

- [ ] **Step 3: Add the flag + loader**

In `main.go`'s flag block:
```go
		operatorFile = flag.String("operator-file", "", "if set, enable the admin UI and authenticate the single operator against this YAML file (username, password_hash)")
```

Add a loader mirroring `loadKeys`:
```go
func loadOperator(path string) (config.Operator, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return config.Operator{}, "", err
	}
	op, err := config.ParseOperatorYAML(raw)
	if err != nil {
		return config.Operator{}, "", err
	}
	sum := sha256.Sum256(raw)
	return op, hex.EncodeToString(sum[:8]), nil
}
```

- [ ] **Step 4: Mount the UI (only when configured) and add `GET /`**

After `router := api.NewRouter(...)`, compose a top-level mux that mounts the API and, when the
operator file is set, the UI + a root redirect. Because the API router is itself an
`http.Handler`, wrap rather than re-register:
```go
	top := http.NewServeMux()
	top.Handle("/", router) // API + existing routes (default)

	if *operatorFile != "" {
		op, fp, err := loadOperator(*operatorFile)
		if err != nil {
			log.Fatalf("operator: %v", err)
		}
		// Store in an atomic holder so SIGHUP can swap it; the Authenticator
		// reads the holder per call.
		var opHolder atomic.Pointer[config.Operator]
		opHolder.Store(&op)
		authr := ui.AuthenticatorFunc(func(user, pass string) (ui.Identity, error) {
			return ui.NewOperatorAuthenticator(*opHolder.Load()).Authenticate(user, pass)
		})
		uiApp, err := ui.New(ui.Config{Svc: svc, Jobs: jobStore, Auth: authr, Secure: false})
		if err != nil {
			log.Fatalf("ui: %v", err)
		}
		top.Handle("/ui", uiApp.Handler())
		top.Handle("/ui/", uiApp.Handler())
		top.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/ui", http.StatusSeeOther)
		})
		log.Printf("admin UI enabled at /ui (operator=%s, fp=%s)", op.Username, fp)
		// Extend the existing SIGHUP goroutine to reload the operator file too
		// (see Step 5).
		_ = opHolder
	}

	srv := &http.Server{Addr: *addr, Handler: top, ReadHeaderTimeout: 10 * time.Second}
```

This requires a small `AuthenticatorFunc` adapter in `internal/ui/auth.go`:
```go
// AuthenticatorFunc adapts a function to the Authenticator interface.
type AuthenticatorFunc func(user, password string) (Identity, error)

func (f AuthenticatorFunc) Authenticate(user, password string) (Identity, error) {
	return f(user, password)
}
```
Add it under Task 2's file (or here) and commit with this task.

NOTE: `GET /{$}` matches only the exact root path under Go 1.22 routing, so it won't shadow the
API routes mounted at `/`. Confirm the API mux still receives `/hosts`, `/jobs`, etc. via the
`top.Handle("/", router)` fallthrough by keeping the redirect pattern as `GET /{$}` (exact match).

- [ ] **Step 5: Extend SIGHUP to reload the operator file**

Inside the existing `for range hup` loop, after the hosts reload block, add:
```go
				if *operatorFile != "" {
					if newOp, fp, err := loadOperator(*operatorFile); err != nil {
						log.Printf("operator reload FAILED, keeping previous: %v", err)
					} else {
						opHolder.Store(&newOp)
						log.Printf("operator reloaded: username=%s, fp=%s", newOp.Username, fp)
					}
				}
```
(`opHolder` must be declared in the outer scope of `main` for the goroutine to capture it; move
its declaration above the `go func()` that handles SIGHUP.)

- [ ] **Step 6: Refactor for testability (`buildHandler`)**

Extract the `top`-mux construction into `func buildHandler(deps) http.Handler` so the Step 1 test
can exercise it without `ListenAndServe`. Keep `main` calling `buildHandler`.

- [ ] **Step 7: Run tests + full build**

Run:
```bash
go test -tags "$TT" ./cmd/podman-api/ -run TestRootRedirectsToUI -v
make build
make test
gofmt -l . ; go vet -tags "$TT" ./...
```
Expected: target test PASS, `make build` produces `bin/podman-api`, `make test` green, `gofmt -l`
empty, `go vet` clean.

- [ ] **Step 8: Commit**

```bash
git add cmd/podman-api/main.go internal/ui/auth.go cmd/podman-api/main_test.go
git commit -m "feat: mount admin UI on -operator-file, SIGHUP reload (#62)"
```

---

## Task 12: Docs + example operator file + finish

**Files:**
- Create: `auth/operator.example.yaml`
- Modify: `README.md` (UI quick reference) and, per CLAUDE.md, draft a wiki "Operating" note
- Modify: `.gitignore` (ignore real `auth/operator.yaml`)

- [ ] **Step 1: Example operator file**

`auth/operator.example.yaml`:
```yaml
# Single-operator admin UI credential.
# Generate the hash with:  ./bin/podman-api hash-token <your-password>
username: operator
password_hash: "$argon2id$v=19$m=65536,t=3,p=4$REPLACE_ME$REPLACE_ME"
```

- [ ] **Step 2: Ignore the real file**

Add to `.gitignore`:
```
/auth/operator.yaml
```

- [ ] **Step 3: README quick reference**

Add a short "Admin UI" section: enable with `-operator-file auth/operator.yaml`; generate the
hash via `hash-token`; UI served at `/ui` on the main `-addr`; note it is disabled unless the
flag is set; link to the design spec and (per CLAUDE.md) the wiki Operating page for the full
walkthrough.

- [ ] **Step 4: Verify end-to-end manually (optional but recommended)**

```bash
./bin/podman-api hash-token devpass            # copy hash into auth/operator.yaml
./bin/podman-api -addr 127.0.0.1:8080 -operator-file auth/operator.yaml -hosts-dir hosts
# browse http://127.0.0.1:8080/ -> /ui/login -> sign in -> host list -> deploy
```
(Use the `verify` skill / manual check; this is not a unit test.)

- [ ] **Step 5: Commit**

```bash
git add auth/operator.example.yaml .gitignore README.md
git commit -m "docs(ui): admin UI quick reference + example operator file (#62)"
```

- [ ] **Step 6: Final verification before opening the PR**

```bash
make build && make test && gofmt -l . && go vet -tags "$TT" ./...
```
Expected: binary builds, all tests pass, `gofmt -l` empty, vet clean. Then open the PR per
CLAUDE.md: `forgejo pr create tej/podman-api --title="Admin UI shell (#62)" --head=<branch> --base=main --body="..."`.

---

## Self-Review notes (author → executor)

- **Spec coverage:** login/session/auth seam (Tasks 1–3,6,11), sidebar shell + host list (Task 7),
  instance detail + lifecycle + logs tail (Task 8), meta-driven deploy form (Task 9), jobs read
  (Task 10), mount/gate/SIGHUP (Task 11), vendored assets + fragment rendering (Tasks 4–5), docs
  (Task 12). All §2 in-scope items map to a task; all §7 seams (`Authenticator`, `SessionStore`,
  `Identity`) are implemented behind interfaces.
- **Verify-before-write flags** are called out where the plan references APIs not fully read during
  planning: `podman.LogOptions.Tail` (Task 8), `Service.Upgrade` image semantics (Task 8),
  `store.JobStore` list/get method names (Task 10), and the `fake` constructor + seeding API
  (Tasks 7–9). Confirm each against the source before implementing that step; adjust names to match.
- **Type consistency:** `Identity{Subject, Scopes}`, `Config{Svc,Jobs,Auth,Sessions,SessionTTL,Secure}`,
  `render(w,r,block,map[string]any)`, cookie/field/header constants (`sessionCookie`, `csrfField`,
  `csrfHeader`) are used consistently across tasks.
- **Out of scope (do not build here):** live log streaming (#64), metrics (#63), catalog/wizard
  (#61), ingress domain entry (#60). The static logs tail is intentional.
