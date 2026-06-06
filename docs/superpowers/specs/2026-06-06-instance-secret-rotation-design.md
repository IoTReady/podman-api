# Per-instance secret management (rotation) — Design

**Issue:** #92 (Admin UI: per-instance secret management). Also closes the #96/#54
reconcile residual flagged in #92's comment thread.

**Date:** 2026-06-06

## Problem

A deployed instance carries per-instance secret values (declared by its template's
`secrets.per_instance` names, stored AES-256-GCM-sealed in the spec store). The
Admin UI shell's Upgrade is intentionally image-only — it reuses the stored secrets
and only changes the image. There is no UI to view which per-instance secrets an
instance has, or to rotate a value.

A coupled backend gap (#96/#54): when boot reconciliation reads the destination's
stored spec via `GetSpec`, a **permanent** failure — a secrets blob that no longer
decrypts after key loss/rotation, or a corrupt JSON column — is currently treated
like any transient store error. The reconcile job stays `reconciling` and retries
every 30s forever; only `cancel` ends it. An undecryptable row never becomes
readable, so it is more terminal than the unreachable-host case we already make
terminal. Classifying it cleanly requires `GetSpec` to return a *typed* error that
distinguishes a permanent decode/decrypt failure from `context.*`/`SQLITE_BUSY`.
Both wants are done in lockstep because rotation exercises the same decrypt paths.

## Goals

- Operator can see the per-instance secret names an instance declares, with a
  **set / not set** indicator per name.
- Operator can rotate one or more secret values (write-only — current values are
  never displayed or sent to the browser). Rotation re-applies the instance with
  the new values, restarting the pod.
- `GetSpec` returns a typed `ErrSpecCorrupt` for permanent decode/decrypt failures;
  boot reconciliation maps that to a terminal `failed` job instead of retrying
  forever.

## Non-goals

- Rotation does **not** recover an undecryptable spec. A spec that fails to decrypt
  cannot be rotated (rotation must read the existing params/image); that case is the
  "manual cleanup required" path the typed error now reports.
- No change to per-host (`per_host_referenced`) secret management (#92 is
  per-instance only; host secrets already have their own surface).
- No secret value is ever read back to a client. The view shows presence only.

## Design decision: rotation relaxes the "all required secrets present" rule

Rotation re-applies the stored spec via `Apply(Replace=true)`, which runs
`render.Validate`. `Validate` treats a template's `PerInstance` list as *required*
— every declared secret must be present. A stored spec can legitimately lack one
(a template that gained a per-instance secret after the instance was deployed),
which would block rotating an unrelated secret. Rather than force the operator to
re-supply every declared secret on each rotation, rotation opts into
`render.ValidateAllowMissingSecrets` (a new `ApplyOptions.AllowMissingSecrets`
flag) which skips *only* the "missing required secret" rule; the unknown-secret
check and all parameter validation still run, and the deploy path is unchanged.
This never worsens a pod (a missing required secret was already missing) but does
permit re-applying a pod that lacks a required secret — an accepted trade for not
blocking single-secret rotation. (Decision confirmed during implementation.)

## Architecture

Two layers, one PR.

### Layer 1 — typed corruption error (store + reconcile)

**`internal/store`** — new sentinel:

```go
// ErrSpecCorrupt marks a permanently unreadable spec row: the secrets blob no
// longer decrypts (key loss/rotation) or a JSON column is malformed. Distinct
// from transient store errors (context cancellation, SQLITE_BUSY) and from the
// definitive ErrNotFound.
var ErrSpecCorrupt = errors.New("spec row corrupt or undecryptable")
```

`GetSpec` (`internal/store/sqlite.go`) wraps its four permanent decode/decrypt
points so each is matchable via `errors.Is(err, ErrSpecCorrupt)`:

- `open(key, blob)` decrypt failure
- `json.Unmarshal` of the `secrets` plaintext
- `json.Unmarshal` of the `parameters` column
- `json.Unmarshal` of the `domains` column

Each becomes `fmt.Errorf("%w: %v", store.ErrSpecCorrupt, err)`. Unchanged:
`row.Scan` errors (incl. `sql.ErrNoRows → ErrNotFound`), `ErrSecretsNeedKey`
(no key configured at all — a config state, not a corrupt row), and any
context/BUSY error surfaced by the driver.

**`internal/instance/reconcile.go`** — `destSpecState` returns a 4-state enum
instead of `(persisted, ok bool)`:

```go
type specState int
const (
    specInconclusive specState = iota // store could not be consulted (transient): retry
    specPersisted                     // spec row present and readable: roll forward
    specAbsent                        // definitively not persisted (ErrNotFound): fall through
    specCorrupt                       // permanently unreadable (ErrSpecCorrupt): terminal fail
)

func (s *Service) destSpecState(ctx context.Context, host, tmpl, slug string) specState {
    _, err := s.store.GetSpec(ctx, host, tmpl, slug)
    switch {
    case err == nil:
        return specPersisted
    case errors.Is(err, store.ErrNotFound):
        return specAbsent
    case errors.Is(err, store.ErrSpecCorrupt):
        return specCorrupt
    default:
        return specInconclusive
    }
}
```

The caller (the roll-forward block, currently `reconcile.go:100`) maps:

- `specInconclusive` → `step("reconcile-inconclusive", ...)`, `return false, false, "", nil` (retry — unchanged behaviour)
- `specPersisted` → existing roll-forward path
- `specAbsent` → fall through (dest not committed — unchanged behaviour)
- `specCorrupt` → `step("reconcile-spec-corrupt", ...)`,
  `return true, false, "destination spec unreadable (corrupt/undecryptable); manual cleanup required", nil`
  — terminal `failed`, same return shape as the existing orphan-dest verdict
  (`reconcile.go:184`).

The `destSpecState` doc comment is corrected: it currently lists "a decrypt
failure" as a transient that should retry; that line is removed/reworded, since a
decrypt failure is now classified `specCorrupt` (terminal).

### Layer 2 — rotation service method + UI

**`internal/instance/service.go`** — mirrors `UpgradeImage` (load spec, change one
thing, re-apply with `Replace=true`):

```go
// RotateInstanceSecrets overlays newSecrets onto the instance's stored
// per-instance secrets and re-applies (Replace=true), restarting the pod. Names
// absent from newSecrets keep their existing value (write-only: callers never see
// current values). Returns ErrSpecCorrupt if the stored spec cannot be read.
func (s *Service) RotateInstanceSecrets(ctx context.Context, host, tmpl, slug string, newSecrets map[string]string) error
```

- `GetSpec` first. An `ErrSpecCorrupt` error propagates unchanged (can't rotate an
  unreadable spec).
- Empty `newSecrets` → return an error ("no secrets to rotate") so a blank submit
  does not pointlessly restart the instance.
- Overlay supplied names onto `spec.Secrets`; preserve `spec.Parameters`
  (image etc.) and `spec.Domains`. Build an `ApplyRequest` from the merged spec and
  call `Apply(ctx, host, req, ApplyOptions{Replace: true})`.

**`Svc.InstanceSecretState`** — read method for the view that never returns values:

```go
// InstanceSecretState reports, per declared per-instance secret name, whether a
// value is currently stored — presence only, never the value. Returns
// ErrSpecCorrupt if the spec cannot be read.
func (s *Service) InstanceSecretState(ctx context.Context, host, tmpl, slug string) (set map[string]bool, err error)
```

Derives presence from `spec.Secrets` keys; declared names come from the template
meta at the handler.

**`internal/ui`** — following the `upgrade-form` GET/POST pattern:

- `GET  /ui/hosts/{host}/instances/{template}/{slug}/secrets` → `secretsForm`
  (guard): renders the declared `PerInstance` names (from template meta), each a
  write-only `<input type="password">`, with a set/not-set badge (from
  `InstanceSecretState`) and a warning that saving restarts the instance. On
  `ErrSpecCorrupt`, degrade to "spec unreadable — rotation unavailable, manual
  cleanup required" and render no inputs.
- `POST /ui/hosts/{host}/instances/{template}/{slug}/secrets` → `secretsRotate`
  (guardW): collects filled `secret.*` fields from the request **body** (never the
  URL — per #99), calls `RotateInstanceSecrets`, and renders the instance-detail
  view with a success or error notice.
- `instance-detail.html`: add a **Manage secrets** button, shown only when the
  instance's template declares any `PerInstance` secrets.
- New template `secrets-form.html`.

CSRF: `guardW` carries `requireCSRF`; the `csrf_token` field rides in the POST
body. `Cache-Control: no-store` is already set centrally (#95), covering the form.

## Data flow

View: browser → `GET .../secrets` → handler reads template meta (`Svc.Templates`)
for declared names + `Svc.InstanceSecretState` for presence → render
`secrets-form.html` (names + badges, no values).

Rotate: browser POST (CSRF header + `secret.*` body fields) → `secretsRotate`
parses body-only typed values → `RotateInstanceSecrets` → `GetSpec` → overlay →
`Apply(Replace=true)` → pod restart → render instance-detail with notice.

## Error handling

| Condition | Surface |
|---|---|
| Spec undecryptable/corrupt (view) | Degraded form: "spec unreadable — manual cleanup required"; no inputs |
| Spec undecryptable/corrupt (rotate) | `ErrSpecCorrupt` → error notice; no apply attempted |
| Empty submit (no filled fields) | "no secrets to rotate" notice; no restart |
| Apply failure during rotation | Error notice on the form; instance unchanged where Apply is atomic |
| Unknown host | `renderError(ErrUnknownHost)` → 404 (existing helper) |
| Missing/!valid CSRF on POST | 403 (existing `requireCSRF`) |
| Boot reconcile hits corrupt dest spec | Terminal `failed` job: "destination spec unreadable …; manual cleanup required" (no infinite retry) |

## Testing

**Store** (`internal/store`): each of the four corruption points →
`errors.Is(got, ErrSpecCorrupt)`; `ErrNotFound` and `ErrSecretsNeedKey` paths
unchanged (not classified corrupt).

**Reconcile** (`internal/instance`): corrupt dest spec → terminal `failed` with the
documented message, not inconclusive/retry; transient store error still retries;
`ErrNotFound` still falls through.

**Service** (`internal/instance`): `RotateInstanceSecrets` overlays supplied names
and re-applies; absent names keep existing values; empty map → error; corrupt spec
→ `ErrSpecCorrupt`. `InstanceSecretState` reports presence and never leaks values;
corrupt spec → `ErrSpecCorrupt`.

**UI** (`internal/ui`): `secretsForm` lists declared names with set/not-set;
`secretsRotate` rotates via body fields and requires CSRF; unknown host → 404;
the **Manage secrets** button is hidden when the template declares no per-instance
secrets; secret values never appear in a rendered URL.

## Files

- Create: `internal/ui/handlers_secrets.go`, `internal/ui/templates/secrets-form.html`
- Modify: `internal/store/store.go` (sentinel), `internal/store/sqlite.go` (GetSpec
  wrapping), `internal/instance/reconcile.go` (enum + caller), `internal/instance/service.go`
  (`RotateInstanceSecrets`, `InstanceSecretState`), `internal/ui/ui.go` (routes),
  `internal/ui/templates/instance-detail.html` (button)
- Test: `internal/store/sqlite_test.go`, `internal/instance/reconcile_test.go`,
  `internal/instance/service_test.go`, `internal/ui/handlers_secrets_test.go`
