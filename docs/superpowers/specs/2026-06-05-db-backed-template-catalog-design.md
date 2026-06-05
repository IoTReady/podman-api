# DB-Backed Template Catalog Design

**Issue:** #61 (App catalog + one-click deploy) — backend/provider slice.

**Goal:** Make the SQLite state store the backbone of podman-api and move templates
off disk into it as a first-class, CRUD-managed, seeded catalog with structured
parameters and one-click deploy — so the admin UI can render cards and
auto-generate deploy forms from a clean JSON contract.

**Architecture:** Templates become rows in the always-present state store. The
store is opened on every run (`-state-db` selects *where*, not *whether*); the
encryption key (`-spec-key-file`) is now required only for *secret values*, not
for having a store. `render.Meta.Parameters` is upgraded from bare name lists to
typed parameter definitions that carry validation, UI hints, and defaults. The UI
is out of scope beyond keeping `internal/ui` compiling and functional.

**Tech Stack:** Go (remote-client build tags), modernc SQLite, `text/template`,
`go:embed` for seed data, testify.

---

## Background: decisions resolved during brainstorming (2026-06-05)

- **Model:** hybrid — a library of opinionated, wired templates plus a generic
  "basic web" starter. The starter *is* the escape hatch; users **clone-and-edit**
  it (or any template) rather than using a constrained generic form.
- **One store = the DB.** No templates on disk at runtime; embedded files become
  compiled-in **seed data**.
- **DB is the backbone** (`db-is-the-backbone` memory). The "stateless Podman API"
  origin is retired. `-state-db` becomes a location config, default-on. All the
  `s.store != nil` / `db == nil` feature gates are removed.
- **Key-less by default.** The store opens without a key; templates and specs work
  key-less. `-spec-key-file` gates **secret values** only — secret-bearing deploys
  without a key are rejected with a clear error.
- **Edit semantics:** templates are mutable; instances reference by id and
  re-render on their next upgrade/migrate (propagate-on-upgrade). No versioning.
- **Representation:** structured columns/JSON, validated on write; the API serves
  clean JSON the UI consumes directly.
- **Seed policy:** seed-on-empty-table (deletes stick; new shipped seeds need a
  populated-table import, which is out of scope here).
- **No backward compatibility, no vestigial code** (`prerelease-clean-takes`
  memory): there are no released consumers; restructure cleanly and delete the
  old paths rather than shimming.

---

## Scope

**In scope (backend/provider, one PR, staged commits):**

1. `TemplateStore` interface + SQLite + Memory implementations + a `templates` table.
2. Key-less store open; `-spec-key-file` gates secrets only.
3. `render.Meta.Parameters` → typed `[]ParamDef`; `Validate`/`secretEnvs` adapted.
4. Service reads templates from the store; one-click default-fill; delete-protection.
5. Seed-on-empty-table from embedded defaults; add a `basic-web` starter template.
6. Template CRUD/clone API with `templates:read` / `templates:write` scopes,
   validation, and a structured `GET /templates`.
7. `main.go`: store-as-backbone (`-state-db` = location, default path; always open).
8. Update `internal/ui` glue to compile and function against the new model.
9. Wiki docs (Operating/Deploying): DB-managed templates, `-state-db` semantics,
   `templates:write` scope, one-click defaults.

**Out of scope (deferred):** rich catalog UI (cards, form-generation — admin-UI
agent reworks against the structured `GET /templates`); template versioning/pinning;
per-template RBAC beyond the two scopes; catalog import/export; populated-table
re-seed of newly shipped templates.

---

## Component design

### 1. Store: `TemplateStore`, schema, key-less open

New interface, composed into `DB`. A template is the **authored contract**
(`render.Meta`) plus its body and provenance — one source of truth, no field
duplication:

```go
type Template struct {
    Meta    render.Meta // authored contract: ID, Display, Parameters ([]ParamDef),
                        //   Secrets, Volumes, Ingress — see §2
    Body    string      // renderable Pod-YAML (text/template source)
    Origin  string      // "seed" | "user"
    Created time.Time
    Updated time.Time
}
// The template id is Meta.ID.
```

`render.Meta` gains a `Display{ Name, Description, Category, Icon }` field (authored
in the `template-meta` block, so it travels with the template and is copied on
clone), and `Parameters` becomes `[]ParamDef` (§2).

```go
type TemplateStore interface {
    ListTemplates(ctx context.Context) ([]Template, error)        // ordered by id
    GetTemplate(ctx context.Context, id string) (Template, error) // ErrNotFound if absent
    PutTemplate(ctx context.Context, t Template) error            // upsert by id
    DeleteTemplate(ctx context.Context, id string) error          // absent is not an error
    CountTemplates(ctx context.Context) (int, error)              // for seed-on-empty
}

type DB interface {
    Store
    JobStore
    TemplateStore
    io.Closer
}
```

**SQLite:** a new `templates` table at schema **v5** — `id TEXT PRIMARY KEY`,
`body TEXT`, `meta TEXT` (JSON-serialized `render.Meta`), `origin TEXT`,
`created`/`updated`. Plaintext (templates are not secret) so it needs no key.
`schemaSQL` creates it on a fresh DB; `migrateSchema` adds it for existing DBs
guarded by `user_version`.

**Key-less open:** `OpenSQLite(path string, keys *KeyStore)` accepts `keys == nil`.
Spec/template operations work regardless. Secret-sealing operations
(`PutHostSecret`/`GetHostSecret`, and per-instance secret persistence in `PutSpec`)
return a sentinel `ErrSecretsNeedKey` when `keys == nil`. `Memory` gains the same
template methods and the same key-less semantics for parity in tests.

### 2. `render.Meta.Parameters` → typed `[]ParamDef`

```go
type ParamDef struct {
    Name        string   `yaml:"name" json:"name"`
    Type        string   `yaml:"type" json:"type"`             // string|int|bool|select
    Required    bool     `yaml:"required" json:"required"`
    Label       string   `yaml:"label,omitempty" json:"label,omitempty"`
    Description string   `yaml:"description,omitempty" json:"description,omitempty"`
    Default     any      `yaml:"default,omitempty" json:"default,omitempty"`
    Placeholder string   `yaml:"placeholder,omitempty" json:"placeholder,omitempty"`
    Options     []string `yaml:"options,omitempty" json:"options,omitempty"` // for type=select
    Secret      bool     `yaml:"secret,omitempty" json:"secret,omitempty"`   // sensitive input
}
```

`render.Meta.Parameters` changes from `Parameters{Required, Optional []string}` to
`[]ParamDef`. Adapt:
- `render.Validate` — derive required/optional from the defs; type-check supplied
  values against `Type`; enforce `select` membership.
- `secretEnvs` (`service.go`) — derive from the looked-up template at use time
  (drop any boot-time precompute that assumed a static template set).
- The `template-meta` YAML grammar in `ParseMeta` parses the new list form.

This is the one compile-breaking change; it is isolated to a single commit so the
ripple is one reviewable diff.

### 3. Service wiring

- `NewService(client, hosts)` — **drops** the `[]config.Template` argument. The
  service holds a `store.TemplateStore` and resolves templates through it.
- `lookup(host, id)` → `store.GetTemplate`; `Templates()` → `store.ListTemplates`.
  Both return `store.Template` (the `config.Template` type is removed; see Vestigial).
- **One-click default-fill:** in `Apply`, before `render`, fill any omitted
  parameter from its `ParamDef.Default`. The persisted spec records the *effective*
  parameters, so a later default change never silently mutates a deployed instance.
- **Delete-protection** lives at the service layer: `DeleteTemplate(id, force)`
  scans specs across hosts (`ListSpecKeys` per host) for references; if any and not
  `force`, return `ErrTemplateInUse` carrying the referencing instances.
- Remove the `s.store == nil` guards in `migrate.go`, `evacuate.go`, `reconcile.go`,
  `service.go` and `HasStore()` — the store is always present (see Vestigial).

### 4. Seeding + the `basic-web` starter

Embedded `templates/*.yaml` (`go:embed`, already present as `templates.Files`) are
parsed via `config.LoadTemplates` (repurposed as the seed parser) into
`store.Template` rows with `Origin: "seed"`. At boot, **if `CountTemplates() == 0`,
seed them all**; otherwise no-op. Add `templates/basic-web.yaml`: a minimal
image + HTTP-port (+ optional named volume, env) template with ingress wired to the
port — the clone-and-edit escape hatch realizing the hybrid model.

### 5. API: CRUD/clone

| Method/Path | Scope | Notes |
|---|---|---|
| `GET /templates` | `templates:read` | structured list (the UI seam): body + display + typed params + secrets/volumes/ingress |
| `GET /templates/{id}` | `templates:read` | one template, full |
| `POST /templates` | `templates:write` | create; validates |
| `PUT /templates/{id}` | `templates:write` | full-row update |
| `DELETE /templates/{id}` | `templates:write` | delete-protected; `?force=true` overrides |
| `POST /templates/{id}/clone` | `templates:write` | body `{new_id}`; copies row, `origin:"user"` |
| `GET /templates/{id}/render` | `templates:read` | render preview with params (kept) |

**Validation on write** (reject before persist, 400 with a clear message):
`id` is `validName` + unique (dup on create/clone → 409); `body` re-parses via
`ParseMeta` *and* dry-run renders with each param's `Default` (or typed zero);
param names unique; `Default` matches `Type`; `Secret` params are not
`Required`-with-plaintext-default; if `Ingress` set, `container`+`port` valid and
the container exists in the rendered body.

### 6. `main.go`: store-as-backbone

- `-state-db` defaults to `/var/lib/podman-api/state.db`; `openStore` always opens
  (creating the parent dir via `MkdirAll`). No `db == nil` path.
- `-spec-key-file` optional: present → keyed store (secrets enabled); absent →
  key-less store (secrets rejected at use).
- Remove `-templates-dir` (templates are DB-managed; seed source is embedded only).
- Seed-on-empty after open; wire `svc` with the store; job runner / ingress / prune
  no longer gate on a nil store (they may still gate on their own enable flags).

### 7. `internal/ui` (keep compiling + working)

`handlers_deploy.go` / `handlers_instances.go` consume `config.Template`,
`sortedTemplates`, `fieldData`, and `HasStore`/`CanUpgrade`. Update to:
`store.Template` + typed params; drop `HasStore`/`CanUpgrade` gating (store always
present → upgrade always available). Keep the existing deploy form functional
against typed params; the **rich catalog UI is the admin-UI agent's follow-up.**

---

## Vestigial code to remove (explicit)

- `config.Template` type is **removed** in favor of `store.Template`.
  `config.LoadTemplates` is **replaced** by a seed parser that reads the embedded
  files and returns `[]store.Template` (it parses each via `render.ParseMeta`,
  sets `Origin:"seed"`). No runtime disk-loading path remains.
- `-templates-dir` flag and its branch in `main.go`.
- All `s.store == nil` / `s.store != nil` / `db == nil` / `db != nil` feature gates
  in `service.go`, `migrate.go`, `evacuate.go`, `reconcile.go`, `main.go`.
- `Service.HasStore()` and the `CanUpgrade` UI gating.
- `api/templates.go` `templateView` (thin map) and `rebuildSource` (the
  reconstruct-source hack) — replaced by direct structured serialization of
  `store.Template` and a stored `Body`.

---

## Data flow

**Deploy (one-click):** `POST /hosts/{h}/instances {template, slug, params?}` →
`Apply` → `store.GetTemplate` → default-fill omitted params → `render.Validate` →
`render.Render(body, effectiveParams)` → image pull → `kube play` → persist
effective spec (secrets sealed iff key present).

**Author:** `POST /templates {body, display, parameters, ...}` → validate (parse +
dry-run render) → `PutTemplate(origin:"user")`. Edits propagate to existing
instances on their next upgrade/migrate.

**Boot:** open store at `-state-db` (key-less unless `-spec-key-file`) → migrate
schema → if `CountTemplates()==0` seed embedded defaults → start services.

## Error handling

New sentinels: `ErrSecretsNeedKey` (secret op on a key-less store),
`ErrTemplateInUse` (delete-protected, lists referencing instances),
`ErrTemplateExists` (dup id on create/clone). Reused: `ErrUnknownTemplate`,
`ErrInvalidName`. Validation failures are 400 with a specific message; in-use
delete is 409; dup id is 409.

## Testing (TDD)

- **Store:** template CRUD round-trip with SQLite + Memory parity; upsert; `List`
  ordering; `CountTemplates`; schema-v5 migration of an existing DB preserves
  specs/secrets; key-less open works for templates/specs and rejects secret ops.
- **render.Meta:** `ParseMeta` → typed defs; `Validate` required/optional + type
  + select membership; default-fill resolution.
- **Service:** `Templates`/`lookup` from store; `Apply` renders from DB template;
  default-fill before render; delete-protection (referenced → blocked, force →
  deletes); `secretEnvs` from looked-up template.
- **API:** each endpoint happy-path + scope rejection; validation errors; delete
  409 + force; structured `GET /templates` shape; clone.
- **Seed:** boot seeds into an empty store; no-op on a populated store; deletes
  stay deleted across restart.

All under `make test`; `gofmt -l .` empty; `go vet` clean.

## Coordination notes

#54 has fully landed (PR #96). This branch is based on current `main`. The admin-UI
(`internal/ui`) is already merged and is updated here to compile/function; the
admin-UI agent picks up the rich catalog UI against the structured `GET /templates`.

## Staged plan outline (for the implementation plan)

1. Store: `TemplateStore` + table (v5) + key-less open + Memory parity.
2. `render.Meta` → typed `ParamDef` (+ `Validate`/`ParseMeta`).
3. Service wiring: store-backed lookup/list, default-fill, delete-protection;
   remove store-nil guards + `HasStore`.
4. Seed-on-empty + `basic-web` starter.
5. API: CRUD/clone, scopes, validation, structured `GET /templates`.
6. `main.go`: store-as-backbone, flag changes, seed wiring; remove `-templates-dir`.
7. `internal/ui`: compile + function against the new model.
8. Wiki docs.
