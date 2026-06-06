# Deploy form POST-switch Implementation Plan (#99)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the deploy form's template switch from `hx-get` to a `POST …/deploy/form` re-render endpoint so typed per-instance secrets travel in the request body, never the URL (#99).

**Architecture:** Extract a shared `deployFormData` helper that builds the `deploy-form` data map (used by the GET initial-load handler, the new POST switch handler, and the failed-deploy re-render). Add a dedicated `POST /ui/hosts/{host}/deploy/form` route under `guardW`; CSRF is already carried by the global `X-CSRF-Token` header from `layout.html`. Flip the `<select>` to `hx-post`. Secrets continue to round-trip (preserved across a switch), per #93.

**Tech Stack:** Go 1.22 `net/http.ServeMux`, `html/template`, HTMX 2.0.4, `httptest`. Build/test via `make` (CGO graphdriver tags); `gofmt`/`go vet` clean.

**Spec:** `docs/superpowers/specs/2026-06-06-deploy-form-post-switch-design.md`

**Conventions:**
- Build/test ONLY via `make test` (never bare `go test ./...` — CGO tags). Target one package: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/...`.
- `gofmt -l internal/ui` must be empty. Do NOT `gofmt` `.html` files (they parse as Go and error).
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

### Task 1: Extract `deployFormData`, rewire GET + failed-deploy paths

Pure refactor: lift the duplicated data-map build out of `deployForm` (GET) and `deployCreate`'s error path into one helper. No behaviour change; existing tests stay green and are the regression guard.

**Files:**
- Modify: `internal/ui/handlers_deploy.go` (`deployForm` ~160-192, `deployCreate` error path ~207-231)

- [ ] **Step 1: Run the existing deploy tests to confirm a green baseline**

Run: `cd <repo-root> && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestDeploy ./internal/ui/ -v`
Expected: PASS (all existing `TestDeploy*` tests green).

- [ ] **Step 2: Add the `deployFormData` helper**

Insert above `deployForm` in `internal/ui/handlers_deploy.go`:

```go
// deployFormData resolves the selected template (defaulting to the first
// template when none is given) and assembles the data map the "deploy-form"
// template needs. vals are the raw typed param.*/secret.* values to
// re-populate; mergeParamDefaults is applied here so every caller shares one
// defaults policy. A store error from the template lookup is returned for the
// caller to surface via renderError. The caller adds an "Error" key when
// re-rendering after a failed deploy.
func (u *UI) deployFormData(r *http.Request, host, selected, slug string, vals map[string]string) (map[string]any, error) {
	tmpls, err := u.sortedTemplates(r.Context())
	if err != nil {
		return nil, err
	}
	if selected == "" && len(tmpls) > 0 {
		selected = tmpls[0].Meta.ID
	}
	tmpl, refs, err := u.fieldData(r, host, selected)
	if err != nil {
		return nil, err
	}
	mergeParamDefaults(vals, tmpl)
	return map[string]any{
		"Host":         host,
		"Templates":    tmpls,
		"Selected":     selected,
		"Tmpl":         tmpl,
		"HostRefs":     refs,
		"Slug":         slug,
		"Values":       vals,
		"Placeholders": paramPlaceholders(tmpl),
	}, nil
}
```

- [ ] **Step 3: Rewrite `deployForm` to use the helper**

Replace the body of `deployForm` (keep the signature and the host-existence check):

```go
func (u *UI) deployForm(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	q := r.URL.Query()
	data, err := u.deployFormData(r, host, q.Get("template"), q.Get("slug"), typedValues(q))
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "deploy-form", u.pageData(data))
}
```

- [ ] **Step 4: Rewrite `deployCreate`'s error path to use the helper**

Replace the `if applyErr := …; applyErr != nil { … }` block's body (the part that re-renders the form) with:

```go
	if applyErr := u.cfg.Svc.Apply(r.Context(), host, req, instance.ApplyOptions{Replace: false}); applyErr != nil {
		data, derr := u.deployFormData(r, host, req.Template, req.Slug, typedValues(r.PostForm))
		if derr != nil {
			u.renderError(w, r, derr)
			return
		}
		data["Error"] = applyErr.Error()
		u.render(w, r, errorStatus(applyErr), "deploy-form", u.pageData(data))
		return
	}
```

(The success path below this block — `Get` then render `instance-detail` — is unchanged.)

- [ ] **Step 5: gofmt + vet + run the deploy tests**

Run: `cd <repo-root> && gofmt -l internal/ui && go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/ && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestDeploy ./internal/ui/ -v`
Expected: `gofmt -l` prints nothing; vet clean; all `TestDeploy*` PASS (behaviour preserved).

- [ ] **Step 6: Commit**

```bash
git add internal/ui/handlers_deploy.go
git commit -m "refactor(ui): extract deployFormData shared by GET/POST/error render (#99)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Add the `POST …/deploy/form` re-render endpoint

New handler + route. Drive it with the two switch-behaviour tests migrated from GET-query to POST-body (these also assert the secret round-trips), plus a CSRF-guard test.

**Files:**
- Modify: `internal/ui/ui.go` (route table, near lines 90-91)
- Modify: `internal/ui/handlers_deploy.go` (new handler)
- Modify: `internal/ui/handlers_deploy_test.go` (helper + migrate 2 tests + new CSRF test)

- [ ] **Step 1: Add the `authedPost` test helper**

Add to `internal/ui/handlers_deploy_test.go` (it already imports `net/url`, `strings`, `net/http`, `net/http/httptest`):

```go
// authedPost drives a POST through a real session as x-www-form-urlencoded,
// injecting a valid CSRF token field. The caller supplies the other fields.
func authedPost(t *testing.T, u *UI, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form.Set(csrfField, csrfToken(tok))
	r := httptest.NewRequest("POST", path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	return w
}
```

- [ ] **Step 2: Migrate the two switch tests to POST (write the failing tests)**

Replace `TestDeployFormPreservesTypedValuesOnTemplateSwitch` and `TestDeployFormDropsValuesForFieldsNotInTemplate` in `internal/ui/handlers_deploy_test.go` with POST-driven versions:

```go
// TestDeployFormPreservesTypedValuesOnTemplateSwitch drives the POST re-render
// endpoint (#99): typed params, the typed secret, and the slug all travel in
// the request body and are re-populated into the fragment — no secret in the URL.
func TestDeployFormPreservesTypedValuesOnTemplateSwitch(t *testing.T) {
	u := uiWithTemplate(t)
	w := authedPost(t, u, "/ui/hosts/edge-1/deploy/form", url.Values{
		"template":        {"demo"},
		"slug":            {"web"},
		"param.version":   {"1.2.3"},
		"secret.password": {"hunter2"},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `value="1.2.3"`) {
		t.Error("typed param 'version' should be preserved across template switch")
	}
	if !strings.Contains(body, `value="hunter2"`) {
		t.Error("typed secret 'password' should be preserved across template switch")
	}
	if !strings.Contains(body, `value="web"`) {
		t.Error("typed slug should still round-trip")
	}
}

func TestDeployFormDropsValuesForFieldsNotInTemplate(t *testing.T) {
	u := uiWithTemplate(t)
	w := authedPost(t, u, "/ui/hosts/edge-1/deploy/form", url.Values{
		"template":    {"demo"},
		"param.bogus": {"keepme"},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if strings.Contains(w.Body.String(), "keepme") {
		t.Error("a value for a field not declared by the selected template must not render")
	}
}
```

- [ ] **Step 3: Run the migrated tests to verify they fail**

Run: `cd <repo-root> && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestDeployFormPreservesTypedValuesOnTemplateSwitch|TestDeployFormDropsValuesForFieldsNotInTemplate' ./internal/ui/ -v`
Expected: FAIL — `POST /ui/hosts/edge-1/deploy/form` has no route yet, so the status is 405/404, not 200.

- [ ] **Step 4: Add the `deployFormPost` handler**

Insert after `deployForm` in `internal/ui/handlers_deploy.go`:

```go
// deployFormPost re-renders the deploy form for a newly selected template. The
// template <select> POSTs here (rather than GETs the deploy route) so typed
// per-instance secrets travel in the request body, not the URL (#99). It mirrors
// deployForm but reads the selected template, slug, and typed values from the
// POST body.
func (u *UI) deployFormPost(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	data, err := u.deployFormData(r, host, r.FormValue("template"), r.FormValue("slug"), typedValues(r.PostForm))
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "deploy-form", u.pageData(data))
}
```

- [ ] **Step 5: Register the route**

In `internal/ui/ui.go`, add directly below the existing `POST …/deploy` line (~line 91):

```go
	mux.Handle("POST /ui/hosts/{host}/deploy/form", guardW(u.deployFormPost))
```

- [ ] **Step 6: Run the migrated tests to verify they pass**

Run: `cd <repo-root> && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run 'TestDeployFormPreservesTypedValuesOnTemplateSwitch|TestDeployFormDropsValuesForFieldsNotInTemplate' ./internal/ui/ -v`
Expected: PASS.

- [ ] **Step 7: Add the CSRF-guard test (write failing-then-passing — it passes immediately since the route is `guardW`)**

Add to `internal/ui/handlers_deploy_test.go`:

```go
// TestDeployFormPostRequiresCSRF verifies the POST re-render endpoint is behind
// the write guard: a POST with a valid session but no CSRF token is rejected.
func TestDeployFormPostRequiresCSRF(t *testing.T) {
	u := uiWithTemplate(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{"template": {"demo"}} // no csrf_token field, no header
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/deploy/form", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a POST switch without CSRF", w.Code)
	}
}
```

- [ ] **Step 8: Run the CSRF test**

Run: `cd <repo-root> && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestDeployFormPostRequiresCSRF ./internal/ui/ -v`
Expected: PASS (status 403).

- [ ] **Step 9: gofmt + vet + commit**

Run: `cd <repo-root> && gofmt -l internal/ui && go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/`
Expected: `gofmt -l` prints nothing; vet clean.

```bash
git add internal/ui/ui.go internal/ui/handlers_deploy.go internal/ui/handlers_deploy_test.go
git commit -m "feat(ui): POST template switch to a /deploy/form re-render endpoint (#99)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Flip the `<select>` to `hx-post`

Point the switch at the new endpoint and drop the GET-only `hx-params` exclusion, with a structural test that locks the switch to POST so it can't silently regress to a secret-leaking GET.

**Files:**
- Modify: `internal/ui/templates/deploy-form.html` (the `<select>`)
- Modify: `internal/ui/handlers_deploy_test.go` (structural regression test)

- [ ] **Step 1: Write the failing structural test**

Add to `internal/ui/handlers_deploy_test.go`:

```go
// TestDeployFormSwitchUsesPost locks the template <select> to a POST switch
// (#99): a GET switch would put typed secrets in the URL/logs. The structural
// assertion guards against a silent regression back to hx-get.
func TestDeployFormSwitchUsesPost(t *testing.T) {
	u := uiWithTemplate(t)
	body := authedGet(t, u, "/ui/hosts/edge-1/deploy").Body.String()
	if !strings.Contains(body, `hx-post="/ui/hosts/edge-1/deploy/form"`) {
		t.Error("the template <select> should POST the switch to /deploy/form")
	}
	if strings.Contains(body, `hx-get="/ui/hosts/edge-1/deploy"`) {
		t.Error("the template <select> must not switch via hx-get (secret-in-URL leak)")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd <repo-root> && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestDeployFormSwitchUsesPost ./internal/ui/ -v`
Expected: FAIL — the `<select>` still uses `hx-get="/ui/hosts/edge-1/deploy"`.

- [ ] **Step 3: Flip the `<select>` in `deploy-form.html`**

Replace the `<select …>` line in `internal/ui/templates/deploy-form.html`:

```html
    <select name="template" hx-post="/ui/hosts/{{.Host}}/deploy/form" hx-target="#main" hx-trigger="change" hx-include="closest form">
```

(Changed: `hx-get="/ui/hosts/{{.Host}}/deploy"` → `hx-post="/ui/hosts/{{.Host}}/deploy/form"`; removed `hx-params="not csrf_token"` so the hidden `csrf_token` field rides in the body alongside the global `X-CSRF-Token` header. `hx-target`, `hx-trigger`, `hx-include` unchanged.)

- [ ] **Step 4: Run the structural test to verify it passes**

Run: `cd <repo-root> && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" -run TestDeployFormSwitchUsesPost ./internal/ui/ -v`
Expected: PASS.

- [ ] **Step 5: Full package test + gofmt + vet**

Run: `cd <repo-root> && gofmt -l internal/ui && go vet -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/ && go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/ui/`
Expected: `gofmt -l` prints nothing (note: `deploy-form.html` is NOT a `.go` file, so it is not checked); vet clean; `ok internal/ui`.

- [ ] **Step 6: Commit**

```bash
git add internal/ui/templates/deploy-form.html internal/ui/handlers_deploy_test.go
git commit -m "feat(ui): switch the deploy template <select> to hx-post (#99)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Final verification

- [ ] **Step 1: Full unit suite**

Run: `cd <repo-root> && make test`
Expected: all packages `ok` (or cached).

- [ ] **Step 2: Confirm no secret-bearing GET remains in the deploy template**

Run: `grep -n 'hx-get' internal/ui/templates/deploy-form.html`
Expected: no match (the `<select>` is the only htmx call in this fragment, now POST).
