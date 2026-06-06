# Deploy form: POST the template switch so secrets leave the URL (#99)

**Date:** 2026-06-06
**Issue:** #99 (follow-up from PR #95 review, finding #2 part 2)
**Status:** Design approved; ready for implementation plan.

## Problem

The deploy form's template `<select>` re-fetches the form fragment on `change`
via `hx-get` with `hx-include="closest form"`. Every typed field — **including
`secret.*` per-instance secrets** — is therefore sent as a **GET query string**
on each template switch, landing secret values in:

- server access logs (the request line),
- browser history,
- any intermediary that logs URLs.

`hx-params` only accepts literal field names, so it cannot wildcard `secret.*`
to exclude them from the include. PR #95 added `Cache-Control: no-store` on
rendered pages (closing the response-caching surface), but the query-string
exposure remains.

This is a local single-operator UI, so severity is modest, but it is a real
secret-in-logs leak worth closing.

## Decision

Move the template switch from GET to **POST against a dedicated re-render
endpoint**, so the typed fields travel in the request body rather than the URL.
Secrets continue to be **preserved** across a switch (re-populated into the
re-rendered fragment), consistent with the #93 decision to keep typed values —
including secrets — on a template switch. Secrets still appear in the response
HTML body; that surface is already mitigated by the `Cache-Control: no-store`
header from #95 and is explicitly out of scope here (this issue is about the URL
leak only).

### Approaches considered

- **POST + keep secrets (chosen).** New `POST …/deploy/form` re-render endpoint;
  secrets travel in the body and are echoed back into the fragment. Closes the
  URL/log/history leak; preserves #93 behaviour.
- **POST + blank secrets (rejected).** Same endpoint but secret inputs render
  empty after a switch — never echoes secrets into HTML. Rejected: it reverses
  the #93 "preserve secrets on switch" decision and forces the operator to
  re-type secrets after every template change.
- **Overload `POST …/deploy` (rejected).** Branch the existing deploy handler on
  a switch marker. Rejected: gives one handler two responsibilities and risks an
  accidental deploy if the marker logic regresses.

## Architecture

Add a dedicated endpoint `POST /ui/hosts/{host}/deploy/form` that returns the
`deploy-form` fragment. The `<select>` switches from `hx-get` to `hx-post`
against it, keeping `hx-include="closest form"` so all typed fields (params and
secrets) and the slug travel in the request **body**, never the URL.

The route registers under `guardW` (`requireSession` + `requireCSRF`). CSRF is
already satisfied by the global `X-CSRF-Token` header emitted by
`<body hx-headers='{"X-CSRF-Token": "{{.CSRF}}"}'>` in `layout.html`, so the
POST needs no new token plumbing; dropping the select's `hx-params="not
csrf_token"` additionally lets the hidden `csrf_token` field ride in the body as
a belt-and-suspenders alongside the header.

The existing `GET /ui/hosts/{host}/deploy` (`deployForm`) remains the
initial-load entry point. A freshly loaded form has no typed values, so nothing
sensitive is ever placed in a URL.

## Components / files

### `internal/ui/ui.go`

Register the new route under `guardW`:

```go
mux.Handle("POST /ui/hosts/{host}/deploy/form", guardW(u.deployFormPost))
```

Go 1.22 `ServeMux` distinguishes this from `POST /ui/hosts/{host}/deploy`
(`deployCreate`) by the longer, more specific pattern — no conflict.

### `internal/ui/handlers_deploy.go`

1. **Factor a shared data-builder.** `deployForm` (GET) and `deployCreate`'s
   error path currently duplicate the deploy-form data-map build; the new switch
   handler would be a third copy. Extract:

   ```go
   // deployFormData resolves the selected template and builds the data map the
   // "deploy-form" template needs (templates list, selected id, resolved
   // template, per-host secret refs, slug, typed values, placeholders). vals are
   // the raw typed param.*/secret.* values to re-populate; mergeParamDefaults is
   // applied here so callers share one defaults policy. A store error from the
   // template lookup is propagated for the caller to surface via renderError.
   func (u *UI) deployFormData(r *http.Request, host, selected, slug string, vals map[string]string) (map[string]any, error)
   ```

   Returns the map with keys `Host`, `Templates`, `Selected`, `Tmpl`,
   `HostRefs`, `Slug`, `Values`, `Placeholders`. The caller adds an `Error` key
   when re-rendering after a failed deploy.

2. **New handler `deployFormPost`.** Parses the form, builds `vals` via
   `typedValues(r.PostForm)`, calls `deployFormData(r, host, r.FormValue("template"), r.FormValue("slug"), vals)`, and renders the `deploy-form` fragment. Mirrors `deployForm`'s host-existence and error handling:

   ```go
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
       sel := r.FormValue("template")
       data, err := u.deployFormData(r, host, sel, r.FormValue("slug"), typedValues(r.PostForm))
       if err != nil {
           u.renderError(w, r, err)
           return
       }
       u.render(w, r, http.StatusOK, "deploy-form", u.pageData(data))
   }
   ```

   `deployForm` (GET) and `deployCreate`'s error path are refactored to call
   `deployFormData`, with `deployForm` reading `selected`/`slug`/`vals` from
   `r.URL.Query()` and the default-first-template logic preserved.

### `internal/ui/templates/deploy-form.html`

Switch the `<select>` from `hx-get` to `hx-post` and drop the CSRF exclusion:

```html
<select name="template"
        hx-post="/ui/hosts/{{.Host}}/deploy/form"
        hx-target="#main" hx-trigger="change" hx-include="closest form">
```

Keep `hx-target="#main"`, `hx-trigger="change"`, `hx-include="closest form"`.
Remove `hx-params="not csrf_token"`.

## Data flow

1. **Initial** — `GET …/deploy` → `deployForm` renders a fresh fragment via
   `deployFormData` (selected = `?template=` or first template; no secrets
   anywhere).
2. **Switch** — operator changes `<select>` → htmx POSTs
   `{template, slug, param.*, secret.*, csrf_token}` plus the `X-CSRF-Token`
   header to `…/deploy/form` → `deployFormPost` reads `PostForm`, resolves the
   new template, rebuilds `vals` via `typedValues(r.PostForm)`, and renders the
   fragment into `#main`. Secrets are re-populated from the body.
3. **Deploy** — `POST …/deploy` → `deployCreate` (unchanged); its error path
   re-renders through `deployFormData`.

## Error handling

- Unknown host → `renderError(instance.ErrUnknownHost)` (matches `deployForm`).
- `ParseForm` failure → `400 bad form` (matches `deployCreate`).
- Store errors from `sortedTemplates`/`fieldData` inside `deployFormData`
  propagate as a returned `error` → `renderError` (unchanged behaviour).

## Testing

All tests use `httptest`; no integration host required.

### Migrate existing switch-behaviour tests from GET to POST

- `TestDeployFormPreservesTypedValuesOnTemplateSwitch` (currently
  `authedGet(... ?…&secret.password=hunter2)`) → POST a form body to
  `…/deploy/form`; assert both the param values and the secret are re-populated
  in the fragment.
- `TestDeployFormDropsValuesForFieldsNotInTemplate` → same GET-query → POST-body
  migration.

The pure-render tests that exercise *initial* GET load —
`TestDeployFormTypedValueBeatsParamDefault`,
`TestDeployFormClearedDefaultedFieldStaysEmpty`,
`TestDeployFormDefaultedFieldShowsDefaultPlaceholder`,
`TestDeployFormFalsyDefaultShowsPlaceholder`,
`TestDeployFormExplicitPlaceholderWins`, `TestDeployFormSetsNoStore` — stay on
GET. After factoring `deployFormData`, both the GET and POST paths exercise the
same helper, so the data-build coverage is shared.

### New tests

- **CSRF guard.** POST to `…/deploy/form` with no token → `403`; with the
  `X-CSRF-Token` header (or `csrf_token` field) → `200`. Mirrors the existing
  `guardW` CSRF test pattern.
- **Template-side regression guard.** The rendered `<select>` emits
  `hx-post=".../deploy/form"` and does **not** contain `hx-get` on the select —
  a structural assertion that the switch cannot silently revert to GET and
  re-leak secrets into the URL.
- **Secret round-trip on switch.** POST switch with `secret.password=hunter2` in
  the body → fragment contains `value="hunter2"` (the "keep secrets" decision).
  By construction the secret value is never placed in a URL.

### Verification

- `make test` (unit suite) green.
- `gofmt -l internal/ui` empty (`.go` files only; not the `.html` templates).
- `go vet` clean.

## GET-path hardening (folded in during PR #110 review)

Originally scoped out, then folded in: the GET `deployForm` previously echoed
`secret.*` values from a hand-crafted query string back into the form, round-
tripping a secret through the request line (logs) and the response HTML. The UI
never produces such a URL (the switch is now POST), but the residual is closed
by `queryTypedValues`, which drops `secret.*` keys on the GET path (params still
deep-link). This fully closes the URL-leak surface; issue #111 is closed by this
PR rather than left as a follow-up.

## Out of scope

- Secrets appearing in the response **HTML body** for the legitimate POST-switch
  round-trip (mitigated by `no-store` from #95) — not addressed here.
- The catalog UI rewrite (#61 follow-up) and per-instance secret management
  (#92) remain separate work.
