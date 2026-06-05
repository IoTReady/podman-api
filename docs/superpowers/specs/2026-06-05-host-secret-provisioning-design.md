# Per-host secret provisioning on the destination — Design

> Sub-item of the **#54** migrate/evacuate hardening umbrella.
> Date: 2026-06-05

## Problem

A template can declare `per_host_referenced` secrets — shared, host-scoped
secrets (e.g. a registry credential) that the template references by name but
does not own. They must already exist on whatever host an instance runs on.

When a migrate/evacuate moves an instance to a host that lacks one of these
secrets, the move fails with `ErrHostSecretMissing` and the operator must
hand-seed the secret on the destination first (via `PUT /hosts/{host}/secrets/{name}`)
before retrying. During an evacuate fan-out — typically an incident response —
this manual step is exactly when you least want it.

Per-**instance** secrets do not have this problem: their values are persisted in
the encrypted spec store and re-applied on the destination automatically. Per-host
secrets are referenced by name only; the API never stores their values.

### Hard constraint

**Podman secrets are write-only.** The `podman.Secret` struct exposes only `Name`
and `CreatedAt`, and there is no read-value API. The destination's secret value
therefore **cannot** be copied from the source host (or any other host). To
auto-provision, the API must hold the value itself.

## Goal

When a migrate/evacuate moves an instance to a host missing a `per_host_referenced`
secret, the daemon **auto-provisions that secret on the destination** from a value
persisted in the encrypted state store, instead of failing — provided the value
was persisted. A secret that is absent **and** not persisted stays blocking,
exactly as today.

## Design decisions (settled during brainstorming)

1. **Value source — persist in the state store.** The API holds the value (sealed,
   at rest) so it can re-provision on any host. (Copy-from-source is impossible;
   request-supplied-at-migrate-time was rejected as awkward for an evacuate fan-out.)
2. **Store model — per-host, reuse the existing PUT.** Persisted values are keyed by
   `(host, name)`. On migrate A→B the destination is provisioned with the **source
   host's** stored value ("replicate the source"). No new resource or endpoint.
3. **Persist consent — persist by default (opt-out).** With the store enabled,
   `PUT /hosts/{host}/secrets/{name}` persists the value by default; the caller can
   opt out with `"persist": false`. Consistent with per-instance secrets, which
   always persist when the store is on. Maximizes hands-off evacuate coverage.
4. **Preview signal — non-blocking `provisions` list.** The evacuate plan-preview
   reports per-host secrets that will be auto-provisioned in a new `provisions`
   field per move; `ok` stays `true` (issues still empty). Transparent: the operator
   sees that secret material will be written to a new host before they run it.

## Architecture

End to end:

- **Capture** — `PUT /hosts/{host}/secrets/{name}` pushes the value to the host as
  today and, when the store is enabled and the caller did not opt out, also persists
  it sealed under `(host, name)`.
- **Provision** — on migrate A→B, for each `per_host_referenced` secret absent on B,
  look up host A's persisted value and create it on B before `Apply`. Absent and not
  persisted stays `ErrHostSecretMissing`.
- **Preview** — `POST /evacuate/plan` reports such secrets in `provisions: [name…]`
  per move.

**Invariant (carried from the plan-preview work):** a `200` plan is exactly a
request the real evacuate would accept. Both the preview and the executor consult
the same store via the same shared preflight, so a secret that previews as
`provisions` is one the executor will actually provision, and one that previews as
`host_secret_missing` is one both reject. Preview and executor cannot drift.

When the store is disabled (`s.store == nil`) behaviour is identical to today:
push-to-host-only, no persistence, no auto-provision. (Migrate already requires the
store — `ErrStoreDisabled` — so on the migrate path the store is always present; the
`s.store != nil` guards matter for the PUT/DELETE routes.)

## Components

### 1. Storage layer (`internal/store`)

New table, added to `schemaSQL`, mirroring how `specs.secrets` is sealed:

```sql
CREATE TABLE IF NOT EXISTS host_secrets (
  host    TEXT NOT NULL,
  name    TEXT NOT NULL,
  value   BLOB NOT NULL,          -- seal()-ed, like specs.secrets
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  PRIMARY KEY (host, name)
);
```

Three new methods on the `store.Store` interface, implemented by `SQLite` and the
in-memory `memory.go` double:

```go
// PutHostSecret inserts or replaces the sealed value for (host, name),
// stamping created on first write and updated every write.
PutHostSecret(ctx context.Context, host, name string, value []byte) error
// GetHostSecret returns the decrypted value, or ErrNotFound if absent.
GetHostSecret(ctx context.Context, host, name string) ([]byte, error)
// DeleteHostSecret removes the (host, name) row; no error if absent.
DeleteHostSecret(ctx context.Context, host, name string) error
```

Values are sealed/opened with the existing `seal`/`open` + `KeyStore` path used for
instance secrets — no new crypto. The SQLite implementation reads the key fresh on
every Put/Get (as the specs path does).

### 2. `PUT` / `DELETE` host-secret routes (`internal/instance`, `internal/api`)

`Service.PutHostSecret` gains a `persist bool` parameter and a persist step ordered
**after** the host push:

```go
func (s *Service) PutHostSecret(ctx context.Context, host, name string, value []byte, persist bool) error {
    if _, ok := s.host(host); !ok { return ErrUnknownHost }
    // 1. push to the host (existing remove-then-create rotation)
    if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
        if err := s.client.SecretRemove(ctx, host, name); err != nil { return err }
    }
    if err := s.client.SecretCreate(ctx, host, name, wrapAsKubeSecret(name, value)); err != nil {
        return err
    }
    // 2. persist, only if store enabled AND caller did not opt out
    if s.store != nil && persist {
        if err := s.store.PutHostSecret(ctx, host, name, value); err != nil {
            return fmt.Errorf("persist host secret: %w", err)
        }
    }
    return nil
}
```

- **Push first, persist second** — we never persist a value we failed to apply to
  the host. If persist fails after a successful push, the call errors; the host has
  the new value but the store does not, and the operator retries (idempotent).
- The API handler decodes an optional `"persist"` body field **defaulting to `true`**.
  With the store disabled, `persist` is silently irrelevant.
- `Service.DeleteHostSecret` also calls `store.DeleteHostSecret` (when the store is
  enabled) so the store does not retain a value for a secret the operator removed.

### 3. Migrate executor (`internal/instance/migrate.go`)

**Preflight** — in the shared, no-mutation `preflightIssues`, the per-host secret
loop changes from "missing ⇒ blocking" to "missing ⇒ blocking only if not
provisionable":

```go
for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
    _, err := s.client.SecretInspect(ctx, req.ToHost, name)
    if err == nil { continue }                          // present on dest
    if !errors.Is(err, podman.ErrNotFound) {
        // infra error: short-circuit (unchanged from today)
        return append(issues, fmt.Errorf("inspect host secret %q: %w", name, err))
    }
    // absent on dest: provisionable iff persisted for the SOURCE host
    if s.store != nil {
        if _, gerr := s.store.GetHostSecret(ctx, req.FromHost, name); gerr == nil {
            continue                                     // will be provisioned; not an issue
        } else if !errors.Is(gerr, store.ErrNotFound) {
            return append(issues, fmt.Errorf("lookup persisted host secret %q: %w", name, gerr))
        }
    }
    issues = append(issues, fmt.Errorf("%w: %s", ErrHostSecretMissing, name)) // absent & not persisted
}
```

**Provision** — a new step at the top of `migratePostStop` (the mutating phase),
before volume copy / `Apply`:

```go
// Provision any persisted per-host secrets the dest is missing, from the source
// host's stored value. Idempotent: only creates what is absent.
for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
    if _, err := s.client.SecretInspect(ctx, req.ToHost, name); err == nil { continue }
    val, err := s.store.GetHostSecret(ctx, req.FromHost, name)
    if errors.Is(err, store.ErrNotFound) { continue } // not provisionable; Apply's pre-check rejects
    if err != nil { return fmt.Errorf("load host secret %q: %w", name, err) }
    if err := s.client.SecretCreate(ctx, req.ToHost, name, wrapAsKubeSecret(name, val)); err != nil {
        return fmt.Errorf("provision host secret %q: %w", name, err)
    }
    step("provision-secret", name)
}
```

**Rollback semantics (deliberate):** if the migrate later rolls back, provisioned
host secrets are **left in place**. They are shared, host-scoped, and additive —
other instances already on the destination may depend on them, and the secret
existing is harmless even if nothing uses it. Removing them would risk breaking
unrelated tenants.

### 4. Plan-preview (`internal/instance/evacuate_plan.go`)

`PlannedMove` gains one field:

```go
type PlannedMove struct {
    Slug       string      `json:"slug"`
    Template   string      `json:"template"`
    ToHost     string      `json:"to_host"`
    OK         bool        `json:"ok"`
    Issues     []PlanIssue `json:"issues"`
    Provisions []string    `json:"provisions"` // per-host secrets to be created on dest; [] not null
}
```

`planMove` populates `Provisions` with the names preflight found absent-but-persisted
(same store lookup, same source host). `OK` remains `len(issues) == 0` —
`Provisions` is informational and does not affect `ok`. `Provisions` is initialized
to `[]string{}` so it serializes as `[]`, not `null`.

## Error handling & edge cases

- **Store disabled** — no persistence, no auto-provision; identical to today
  including `ErrHostSecretMissing`.
- **Value diverged on source vs dest** — if the secret already exists on the dest we
  never touch it (`SecretInspect == nil ⇒ continue`). We provision only what is
  absent; we do not reconcile a stale dest secret (out of scope; matches the
  additive, don't-disturb-other-tenants stance).
- **Persisted but absent from source host** — irrelevant; provisioning reads the
  store, not the source host. The source host's live secret is never read (cannot be).
- **Idempotency / concurrent migrates** — two evacuate children targeting the same
  dest+name can both pass the `absent` check, then both `SecretCreate`. The provision
  step treats an already-exists result benignly (re-inspect / ignore already-exists)
  so the race does not fail a move.
- **Security** — the control plane now holds per-host secret material at rest, sealed
  with the existing key (same protection level as instance secrets; no new exposure
  class). Steps/logs record secret **names** only, never values. `secrets:write` still
  gates PUT; the persisted copy inherits that boundary.
- **Wrapping consistency** — provisioned secrets are wrapped with `wrapAsKubeSecret`
  exactly as the original PUT wrote them, so a provisioned dest secret is
  byte-identical to a hand-PUT one.

## Testing

- **store** — `PutHostSecret`/`GetHostSecret`/`DeleteHostSecret` round-trip
  (seal→open), upsert-rotation, `ErrNotFound`, created/updated stamping; for both
  `SQLite` and `memory`.
- **service PUT/DELETE** — persists by default; `persist:false` skips; push-fails ⇒
  no persist; store-disabled ⇒ no-op persist; DELETE removes from store.
- **preflight / migrate** (`fake.Fake`) — absent+persisted ⇒ not blocking and
  provisioned on dest before Apply; absent+not-persisted ⇒ `ErrHostSecretMissing`;
  present-on-dest ⇒ untouched; infra error ⇒ short-circuits; rollback leaves
  provisioned secret in place; concurrent create-race is benign.
- **plan-preview** — `Provisions` lists absent-but-persisted names with `ok=true`;
  `host_secret_missing` only for absent-and-not-persisted; mixed move reports both;
  `openapi_test.go` path/schema check.
- **API** — `evacuate_test.go` happy / blocking / provisions cases; `putSecret`
  handler `persist` decode + default.

## Documentation

- Wiki **Operating** — extend "Previewing an evacuate" (the `provisions` field) and
  the host-secrets section (persist-by-default, `persist:false`, migrate
  auto-provisions persisted per-host secrets, rollback-leaves-in-place caveat,
  store-must-be-enabled).
- `api/openapi.yaml` — `persist` on the PUT body; `provisions` on `PlannedMove`.
- `#54` umbrella annotated on merge (sub-item closed; umbrella stays open for the
  remaining two: separate orchestration pool, auto-resume after restart).

## Out of scope

- Reconciling an existing-but-stale destination secret (we only create absent ones).
- A global "sync all host secrets" operation.
- Provisioning secrets that were seeded out-of-band (SSH/ansible) and never PUT
  through the API — the store has no value for those, so they stay blocking.
