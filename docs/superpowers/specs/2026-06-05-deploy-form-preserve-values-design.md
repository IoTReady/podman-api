# Deploy-form value preservation on template switch — Design

**Issue:** #93 (follow-up from #62 / PR #89 review finding #6)
**Date:** 2026-06-05
**Status:** Approved

## Problem

In the Admin UI deploy form (`internal/ui/templates/deploy-form.html`), changing
the template `<select>` fires an htmx `GET /ui/hosts/{host}/deploy` that re-renders
the form for the newly-selected template. Any values the operator already typed —
params and per-instance secrets — are discarded, because the inputs re-render
empty. Only `template` (and, since #62, `slug`) survive the switch.

The values are **not lost in transit**: the `<select>` already carries
`hx-include="closest form"`, so every `param.*` and `secret.*` field is sent on
the re-fetch. The gap is entirely server-side — `deployForm` ignores the incoming
field values, and `instance-fields.html` renders inputs with no `value=`.

## Goal

On a template switch, re-populate every input whose name exists in the
newly-selected template with the value the operator already typed — params **and**
secrets (single-operator local session; echoing the just-typed secret back into
the form HTML is acceptable here). Fields that only existed under the previous
template naturally drop, because the new template does not render them.

Apply the same re-population to the **failed-deploy** (POST error) re-render, which
today also loses typed param/secret values. Same mechanism, strictly better.

## Approach

Server-side re-population, extending the existing `slug` round-trip pattern. No
JavaScript is added — consistent with the rest of the server-rendered UI.

### Data flow

1. `deployForm` (GET) and the error path of `deployCreate` (POST) build a single
   `Values map[string]string` keyed by **full field name** (`param.db`,
   `secret.password`, `slug`), collected from the request:
   - GET: from `r.URL.Query()`
   - POST error: from `r.PostForm`
2. The map is passed into the template data under key `Values`.
3. `instance-fields.html` renders each input's `value=` by looking the field up in
   `$.Values`:
   ```
   <input name="param.{{.}}" value="{{index $.Values (printf "param.%s" .)}}" required>
   ```
   `html/template` auto-escapes attribute values, so a typed value containing
   quotes or `<` cannot break out of the attribute or inject markup.
4. A missing key returns the zero value (`""`), so `index` is safe when no value
   was typed for a field.

Because `instance-fields.html` only ranges over the **selected** template's
declared params/secrets, a value carried in `Values` for a field absent from the
new template is simply never read — old-template-only fields drop with no special
handling.

### Slug

`slug` already round-trips via its own `.Slug` data key and works today. It is left
as-is (not folded into `Values`) to keep the change minimal; the two mechanisms
coexist without conflict.

## Components

- `internal/ui/handlers_deploy.go`
  - New helper `typedValues(form map[string][]string) map[string]string` that
    copies `param.*` and `secret.*` entries verbatim (no empty-skipping — an empty
    value re-renders an empty input, which is correct).
  - `deployForm`: build `Values` from `r.URL.Query()`, add to template data.
  - `deployCreate` error path: build `Values` from `r.PostForm`, add to template
    data.
- `internal/ui/templates/instance-fields.html`
  - Emit `value="{{index $.Values (printf "param.%s" .)}}"` on each param input
    and `value="{{index $.Values (printf "secret.%s" .)}}"` on each secret input.
- `internal/ui/templates/deploy-form.html`
  - No change: `{{template "instance-fields" .}}` already passes the root `.`,
    which now carries `Values`.

### Interaction note: `formValues` vs `typedValues`

`formValues` (existing) deliberately **skips empty** fields so a blank required
field surfaces as a validation error rather than an empty submitted value — that
behavior is for the *apply* path and is unchanged. `typedValues` is a separate
concern (what to render back into the form) and does **not** skip empties.

## Testing

New tests in `internal/ui/handlers_deploy_test.go`, using the existing
`uiWithTemplate` fixture (template `demo`, required param `version`, per-instance
secret `password`) and `authedGet` helper:

1. **`TestDeployFormPreservesTypedValuesOnTemplateSwitch`** — GET
   `/ui/hosts/edge-1/deploy?template=demo&slug=web&param.version=1.2.3&secret.password=hunter2`;
   assert the body contains `value="1.2.3"` (param) and `value="hunter2"` (secret),
   and `value="web"` (slug still works).
2. **`TestDeployFormDropsValuesForFieldsNotInTemplate`** — GET with
   `param.bogus=keepme` (a field the `demo` template does not declare); assert the
   body does **not** contain `keepme` (no stray input rendered for it).
3. **`TestDeployCreateErrorPreservesTypedValues`** — POST a deploy that fails
   validation (missing required `version`) while supplying
   `secret.password=hunter2`; assert the re-rendered form contains
   `value="hunter2"` so the operator does not have to re-type it.

## Out of scope

- Client-side field preservation (rejected: adds JS to a deliberately JS-light UI).
- Persisting draft form state across navigation/sessions.
- Any change to the apply/validation semantics of `formValues`.
