# DB-Backed Template Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move templates off disk into the always-present SQLite state store as a CRUD-managed, seeded catalog with structured parameters and one-click deploy.

**Architecture:** A `TemplateStore` (SQLite + Memory) holds templates as rows; the store is opened on every run (`-state-db` = location), key-less unless `-spec-key-file` is given (which now gates secrets only). `render.Meta.Parameters` becomes typed `[]ParamDef` carrying validation + UI hints + defaults. The service resolves templates from the store, fills defaults before render, and protects in-use templates from deletion. The stateless-mode `store==nil` paths are deleted.

**Tech Stack:** Go (build tags `containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper`), modernc SQLite, `text/template`, `go:embed`, testify.

**Spec:** `docs/superpowers/specs/2026-06-05-db-backed-template-catalog-design.md`

**Conventions:**
- Build/test via `make test` (carries the required tags). For a single package: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/...`. Define `TAGS` once: `export TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"`.
- `gofmt -l .` must be empty; `go vet ./...` clean (with tags).
- Commit after each green task. Trailer on every commit: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- This is a clean refactor: **delete** old paths, don't shim. The build will be red across packages mid-stream (the `render.Meta.Parameters` type change is compile-breaking); that's expected — each task lists exactly what to fix to get back to green.

---

## File structure

| File | Responsibility | Action |
|---|---|---|
| `internal/render/meta.go` | `Meta`, typed `ParamDef`, `Display`, `ParseMeta` | Modify |
| `internal/render/validate.go` | `Validate` over typed params | Modify |
| `internal/store/template.go` | `Template`, `TemplateStore` interface | Create |
| `internal/store/store.go` | compose `TemplateStore` into `DB` | Modify |
| `internal/store/sqlite.go` | `templates` table (v5), template methods, key-less | Modify |
| `internal/store/memory.go` | in-memory template methods, key-less | Modify |
| `internal/store/seed.go` | parse embedded files → `[]store.Template` | Create |
| `templates/basic-web.yaml` | seeded escape-hatch starter | Create |
| `internal/instance/service.go` | store-backed lookup/list, default-fill, delete-protection | Modify |
| `internal/instance/{migrate,evacuate,reconcile}.go` | drop `store==nil` guards; `store.Template` params | Modify |
| `internal/instance/errors.go` (or where errors live) | `ErrTemplateInUse`, `ErrTemplateExists` | Modify |
| `internal/api/templates.go` | CRUD/clone handlers, structured views | Rewrite |
| `internal/api/router.go` | template routes + `templates:read/write` scopes | Modify |
| `internal/ui/handlers_deploy.go`, `handlers_instances.go` | compile + function vs typed params; drop `HasStore` | Modify |
| `internal/config/templates.go` | **delete** (replaced by `store/seed.go`) | Delete |
| `cmd/podman-api/main.go` | store-as-backbone; remove `-templates-dir`; seed wiring | Modify |

---

## Stage 1 — Store: typed params foundation in `render`

The compile-breaking type change goes first and lands isolated, so every later task builds on the new shape.

### Task 1: Add `ParamDef` + `Display` and switch `Meta.Parameters`

**Files:**
- Modify: `internal/render/meta.go`
- Test: `internal/render/meta_test.go`

- [ ] **Step 1: Write the failing test** — append to `internal/render/meta_test.go`:

```go
func TestParseMeta_TypedParameters(t *testing.T) {
	src := `# template-meta:
#   id: web
#   display:
#     name: Web
#     category: Apps
#   parameters:
#     - name: image
#       type: string
#       required: true
#       default: "nginx:1"
#     - name: port
#       type: int
#       label: HTTP port
#       default: 8080
---
apiVersion: v1
kind: Pod
`
	m, body, err := ParseMeta(src)
	require.NoError(t, err)
	require.Equal(t, "web", m.ID)
	require.Equal(t, "Web", m.Display.Name)
	require.Equal(t, "Apps", m.Display.Category)
	require.Len(t, m.Parameters, 2)
	require.Equal(t, "image", m.Parameters[0].Name)
	require.True(t, m.Parameters[0].Required)
	require.Equal(t, "string", m.Parameters[0].Type)
	require.Equal(t, "port", m.Parameters[1].Name)
	require.False(t, m.Parameters[1].Required)
	require.Contains(t, body, "kind: Pod")
}
```

- [ ] **Step 2: Run it, expect FAIL** (compile error: `m.Parameters` is a struct, no `.Display`).

Run: `go test -tags "$TAGS" ./internal/render/ -run TestParseMeta_TypedParameters`

- [ ] **Step 3: Implement** — in `internal/render/meta.go` replace the `Parameters` struct and add `ParamDef`/`Display`; add `Display` to `Meta`:

```go
type Meta struct {
	ID         string     `yaml:"id"`
	Display    Display    `yaml:"display"`
	Parameters []ParamDef `yaml:"parameters"`
	Secrets    Secrets    `yaml:"secrets"`
	Volumes    []Volume   `yaml:"volumes"`
	Ingress    *Ingress   `yaml:"ingress"`
}

type Display struct {
	Name        string `yaml:"name,omitempty" json:"name,omitempty"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	Category    string `yaml:"category,omitempty" json:"category,omitempty"`
	Icon        string `yaml:"icon,omitempty" json:"icon,omitempty"`
}

type ParamDef struct {
	Name        string   `yaml:"name" json:"name"`
	Type        string   `yaml:"type" json:"type"`
	Required    bool     `yaml:"required,omitempty" json:"required,omitempty"`
	Label       string   `yaml:"label,omitempty" json:"label,omitempty"`
	Description string   `yaml:"description,omitempty" json:"description,omitempty"`
	Default     any      `yaml:"default,omitempty" json:"default,omitempty"`
	Placeholder string   `yaml:"placeholder,omitempty" json:"placeholder,omitempty"`
	Options     []string `yaml:"options,omitempty" json:"options,omitempty"`
	Secret      bool     `yaml:"secret,omitempty" json:"secret,omitempty"`
}
```

Delete the old `type Parameters struct { Required, Optional []string }`. In `ParseMeta`, after unmarshal, validate each `ParamDef.Type` is one of `string|int|bool|select` (default empty → treat as `string`); return an error for an unknown type. Add helper methods on `Meta` for the call sites that previously read required/optional:

```go
// RequiredParams returns the names of parameters that must be supplied.
func (m Meta) RequiredParams() []string {
	var out []string
	for _, p := range m.Parameters {
		if p.Required {
			out = append(out, p.Name)
		}
	}
	return out
}

// ParamNames returns every declared parameter name.
func (m Meta) ParamNames() []string {
	out := make([]string, 0, len(m.Parameters))
	for _, p := range m.Parameters {
		out = append(out, p.Name)
	}
	return out
}

// Param returns the definition for name, or false.
func (m Meta) Param(name string) (ParamDef, bool) {
	for _, p := range m.Parameters {
		if p.Name == name {
			return p, true
		}
	}
	return ParamDef{}, false
}
```

- [ ] **Step 4: Run it, expect PASS** for the new test. The `render` package's other tests/`Validate` will not compile yet — that's Task 2.

- [ ] **Step 5:** (no commit yet — commit after Task 2, since the package won't build until `Validate` is updated.)

### Task 2: Update `Validate` for typed params + default-fill helper

**Files:**
- Modify: `internal/render/validate.go`
- Test: `internal/render/validate_test.go`

- [ ] **Step 1: Write failing tests** — add to `internal/render/validate_test.go`:

```go
func TestValidate_TypedRequiredAndUnknown(t *testing.T) {
	m := Meta{Parameters: []ParamDef{
		{Name: "image", Type: "string", Required: true},
		{Name: "port", Type: "int"},
	}}
	// missing required
	err := Validate(m, map[string]any{}, nil)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.Contains(t, err.Error(), `missing required parameter "image"`)
	// unknown
	err = Validate(m, map[string]any{"image": "x", "bogus": 1}, nil)
	require.Contains(t, err.Error(), `unknown parameter "bogus"`)
	// ok
	require.NoError(t, Validate(m, map[string]any{"image": "x"}, nil))
}

func TestApplyDefaults_FillsOmitted(t *testing.T) {
	m := Meta{Parameters: []ParamDef{
		{Name: "image", Type: "string", Required: true},
		{Name: "port", Type: "int", Default: 8080},
	}}
	eff := ApplyDefaults(m, map[string]any{"image": "nginx:1"})
	require.Equal(t, "nginx:1", eff["image"])
	require.EqualValues(t, 8080, eff["port"])
	// caller-supplied value wins over default
	eff = ApplyDefaults(m, map[string]any{"image": "x", "port": 9090})
	require.EqualValues(t, 9090, eff["port"])
}
```

- [ ] **Step 2: Run, expect FAIL** (compile: `m.Parameters.Required` gone; `ApplyDefaults` undefined).

Run: `go test -tags "$TAGS" ./internal/render/ -run 'TestValidate_Typed|TestApplyDefaults'`

- [ ] **Step 3: Implement** — rewrite the parameter portion of `Validate` to use the helpers, and add `ApplyDefaults`:

```go
func Validate(m Meta, params map[string]any, secrets map[string]string) error {
	var problems []string

	allowed := map[string]bool{}
	for _, p := range m.Parameters {
		allowed[p.Name] = true
		if p.Required {
			if _, ok := params[p.Name]; !ok {
				problems = append(problems, fmt.Sprintf("missing required parameter %q", p.Name))
			}
		}
	}
	for k := range params {
		if !allowed[k] {
			problems = append(problems, fmt.Sprintf("unknown parameter %q", k))
		}
	}
	// secrets block unchanged ...
```

(Keep the existing secrets block verbatim.) Add at end of file:

```go
// ApplyDefaults returns a copy of params with any omitted parameter filled from
// its ParamDef.Default. Caller-supplied values always win. Parameters without a
// default are left absent (Validate enforces required ones).
func ApplyDefaults(m Meta, params map[string]any) map[string]any {
	out := make(map[string]any, len(params)+len(m.Parameters))
	for k, v := range params {
		out[k] = v
	}
	for _, p := range m.Parameters {
		if _, ok := out[p.Name]; !ok && p.Default != nil {
			out[p.Name] = p.Default
		}
	}
	return out
}
```

- [ ] **Step 4: Run the render package tests, expect PASS:** `go test -tags "$TAGS" ./internal/render/`

- [ ] **Step 5: Commit** (render package now builds + passes):

```bash
git add internal/render/
git commit -m "feat(render): typed ParamDef parameters + ApplyDefaults (#61)"
```

---

## Stage 2 — Store: `TemplateStore`, schema v5, key-less

### Task 3: `Template` type + `TemplateStore` interface

**Files:**
- Create: `internal/store/template.go`
- Modify: `internal/store/store.go` (compose into `DB`)

- [ ] **Step 1: Write the failing test** — create `internal/store/template_memory_test.go`:

```go
func TestMemory_TemplateCRUD(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	n, err := m.CountTemplates(ctx)
	require.NoError(t, err)
	require.Zero(t, n)

	tpl := Template{Meta: render.Meta{ID: "web"}, Body: "kind: Pod", Origin: "user"}
	require.NoError(t, m.PutTemplate(ctx, tpl))

	got, err := m.GetTemplate(ctx, "web")
	require.NoError(t, err)
	require.Equal(t, "web", got.Meta.ID)
	require.Equal(t, "kind: Pod", got.Body)

	list, err := m.ListTemplates(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)

	require.NoError(t, m.DeleteTemplate(ctx, "web"))
	_, err = m.GetTemplate(ctx, "web")
	require.ErrorIs(t, err, ErrNotFound)
}
```

- [ ] **Step 2: Run, expect FAIL** (compile: `Template`, template methods undefined).

Run: `go test -tags "$TAGS" ./internal/store/ -run TestMemory_TemplateCRUD`

- [ ] **Step 3: Implement** — create `internal/store/template.go`:

```go
package store

import (
	"context"
	"time"

	"github.com/iotready/podman-api/internal/render"
)

// Template is an authored contract (render.Meta) plus its renderable body and
// provenance. The template id is Meta.ID.
type Template struct {
	Meta    render.Meta
	Body    string
	Origin  string // "seed" | "user"
	Created time.Time
	Updated time.Time
}

// TemplateStore persists deployable templates.
type TemplateStore interface {
	ListTemplates(ctx context.Context) ([]Template, error)
	GetTemplate(ctx context.Context, id string) (Template, error)
	PutTemplate(ctx context.Context, t Template) error
	DeleteTemplate(ctx context.Context, id string) error
	CountTemplates(ctx context.Context) (int, error)
}
```

In `internal/store/store.go`, add `TemplateStore` to the `DB` interface:

```go
type DB interface {
	Store
	JobStore
	TemplateStore
	io.Closer
}
```

- [ ] **Step 4:** Implement Memory methods (Task 4 has the test; implement here to compile). Add to `internal/store/memory.go`: a `templates map[string]Template` field initialized in `NewMemory`, guarded by the existing mutex, with `ListTemplates` (sorted by id), `GetTemplate` (→ `ErrNotFound`), `PutTemplate` (sets Created/Updated), `DeleteTemplate`, `CountTemplates`. Follow the locking pattern of the existing `PutSpec`/`GetSpec`.

- [ ] **Step 5: Run, expect PASS:** `go test -tags "$TAGS" ./internal/store/ -run TestMemory_TemplateCRUD`. Commit:

```bash
git add internal/store/template.go internal/store/store.go internal/store/memory.go internal/store/template_memory_test.go
git commit -m "feat(store): Template type + TemplateStore + Memory impl (#61)"
```

### Task 4: SQLite `templates` table (schema v5) + methods

**Files:**
- Modify: `internal/store/sqlite.go`
- Test: `internal/store/sqlite_test.go` (or a new `template_sqlite_test.go`)

- [ ] **Step 1: Write the failing test** — `internal/store/template_sqlite_test.go`:

```go
func TestSQLite_TemplateCRUD(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), NewKeyStore(testKey(t)))
	require.NoError(t, err)
	defer db.Close()

	tpl := Template{
		Meta: render.Meta{ID: "web", Display: render.Display{Name: "Web"},
			Parameters: []render.ParamDef{{Name: "image", Type: "string", Required: true}}},
		Body: "kind: Pod", Origin: "seed",
	}
	require.NoError(t, db.PutTemplate(ctx, tpl))

	got, err := db.GetTemplate(ctx, "web")
	require.NoError(t, err)
	require.Equal(t, "Web", got.Meta.Display.Name)
	require.Len(t, got.Meta.Parameters, 1)
	require.Equal(t, "seed", got.Origin)
	require.False(t, got.Created.IsZero())

	n, err := db.CountTemplates(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.NoError(t, db.DeleteTemplate(ctx, "web"))
	_, err = db.GetTemplate(ctx, "web")
	require.ErrorIs(t, err, ErrNotFound)
}
```

(Reuse the existing test key helper; if none, `testKey` returns a 32-byte slice. Check `sqlite_test.go` for the established pattern and match it.)

- [ ] **Step 2: Run, expect FAIL** (no `templates` table / methods).

Run: `go test -tags "$TAGS" ./internal/store/ -run TestSQLite_TemplateCRUD`

- [ ] **Step 3: Implement:**
  1. In `schemaSQL`, append:
     ```sql
     CREATE TABLE IF NOT EXISTS templates (
       id      TEXT PRIMARY KEY,
       body    TEXT NOT NULL,
       meta    TEXT NOT NULL,
       origin  TEXT NOT NULL,
       created INTEGER NOT NULL,
       updated INTEGER NOT NULL
     );
     ```
  2. In `migrateSchema`, after the v4 block, add a v5 step that creates the `templates` table on a pre-v5 DB (use `CREATE TABLE IF NOT EXISTS` so it's idempotent), then `PRAGMA user_version = 5`.
  3. Add methods on `*SQLite` (store/JSON via `encoding/json` for `meta`; times as unix seconds like specs):
     ```go
     func (s *SQLite) PutTemplate(ctx context.Context, t Template) error {
         metaJSON, err := json.Marshal(t.Meta)
         if err != nil { return err }
         now := time.Now().Unix()
         _, err = s.db.ExecContext(ctx,
             `INSERT INTO templates (id, body, meta, origin, created, updated)
              VALUES (?,?,?,?,?,?)
              ON CONFLICT(id) DO UPDATE SET body=excluded.body, meta=excluded.meta,
                origin=excluded.origin, updated=excluded.updated`,
             t.Meta.ID, t.Body, string(metaJSON), t.Origin, now, now)
         return err
     }
     ```
     `GetTemplate` (scan + `json.Unmarshal` meta + `ErrNotFound` on `sql.ErrNoRows` + unix→`time.Time`), `ListTemplates` (`ORDER BY id`), `DeleteTemplate` (exec; absent not an error), `CountTemplates` (`SELECT COUNT(*)`). Templates are **not** sealed — do not touch `s.keys`.

- [ ] **Step 4: Run, expect PASS:** `go test -tags "$TAGS" ./internal/store/ -run TestSQLite_TemplateCRUD`. Also run the whole store package to confirm the v5 migration didn't break existing specs: `go test -tags "$TAGS" ./internal/store/`.

- [ ] **Step 5: Commit:**

```bash
git add internal/store/sqlite.go internal/store/template_sqlite_test.go
git commit -m "feat(store): templates table (schema v5) + SQLite impl (#61)"
```

### Task 5: Key-less store open (key gates secrets only)

**Files:**
- Modify: `internal/store/sqlite.go`, `internal/store/store.go` (sentinel)
- Test: `internal/store/template_sqlite_test.go`

- [ ] **Step 1: Write the failing test:**

```go
func TestSQLite_KeylessRejectsSecretsButAllowsTemplates(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), nil) // no keys
	require.NoError(t, err)
	defer db.Close()

	// templates work key-less
	require.NoError(t, db.PutTemplate(ctx, Template{Meta: render.Meta{ID: "x"}, Body: "k", Origin: "user"}))
	// secret op is rejected
	err = db.PutHostSecret(ctx, "h1", "DB_PASS", []byte("p"))
	require.ErrorIs(t, err, ErrSecretsNeedKey)
}
```

- [ ] **Step 2: Run, expect FAIL** (`OpenSQLite(.., nil)` likely panics/errors; `ErrSecretsNeedKey` undefined).

- [ ] **Step 3: Implement:**
  - Add to `internal/store/store.go`: `var ErrSecretsNeedKey = errors.New("secrets require an encryption key (-spec-key-file)")`.
  - In `OpenSQLite`, accept `keys == nil` (it already just stores `keys`; ensure nothing dereferences it at open). 
  - In every method that seals/unseals — `PutHostSecret`, `GetHostSecret`, and the secret-bearing branch of `PutSpec`/`GetSpec` — guard at the top: `if s.keys == nil { return ErrSecretsNeedKey }` (for getters returning a value, return `nil, ErrSecretsNeedKey`). Find them by grepping `s.keys`. A spec with **no** secrets must still persist key-less: only enter the seal path when `len(s.Secrets) > 0`.
  - Mirror the guard in `Memory` (it has no real key but should return `ErrSecretsNeedKey` when constructed key-less; add a `keyed bool` to `Memory` set by a `NewMemory()`-stays-keyed default — to keep existing tests working, `NewMemory()` remains keyed; add `NewMemoryKeyless()` only if a test needs it). **YAGNI:** if no test needs key-less Memory, skip the Memory change and note it.

- [ ] **Step 4: Run, expect PASS.** Run full store package — existing keyed tests must still pass.

- [ ] **Step 5: Commit:**

```bash
git add internal/store/
git commit -m "feat(store): key-less open; -spec-key-file gates secrets only (#61)"
```

---

## Stage 3 — Seed parser + starter template

### Task 6: `store/seed.go` — embedded files → `[]store.Template`

**Files:**
- Create: `internal/store/seed.go`
- Modify: `templates/templates.go` (keep `go:embed`), **delete** `internal/config/templates.go`
- Test: `internal/store/seed_test.go`

- [ ] **Step 1: Write the failing test:**

```go
func TestParseSeeds_ParsesEmbedded(t *testing.T) {
	seeds, err := ParseSeeds(templates.Files)
	require.NoError(t, err)
	require.NotEmpty(t, seeds)
	for _, s := range seeds {
		require.Equal(t, "seed", s.Origin)
		require.NotEmpty(t, s.Meta.ID)
		require.NotEmpty(t, s.Body)
	}
}
```

(Import `github.com/iotready/podman-api/templates`.)

- [ ] **Step 2: Run, expect FAIL** (`ParseSeeds` undefined).

- [ ] **Step 3: Implement** — `internal/store/seed.go`:

```go
package store

import (
	"io/fs"
	"strings"

	"github.com/iotready/podman-api/internal/render"
)

// ParseSeeds reads every *.yaml in fsys, parses each via render.ParseMeta, and
// returns them as Origin:"seed" templates. Used to seed an empty store at boot.
func ParseSeeds(fsys fs.FS) ([]Template, error) {
	var out []Template
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return err
		}
		b, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}
		meta, body, err := render.ParseMeta(string(b))
		if err != nil {
			return err
		}
		out = append(out, Template{Meta: meta, Body: body, Origin: "seed"})
		return nil
	})
	return out, err
}
```

Delete `internal/config/templates.go` and its test. (If `config` has other exports, leave them; only remove the template-loading file.)

- [ ] **Step 4: Run, expect PASS:** `go test -tags "$TAGS" ./internal/store/ -run TestParseSeeds`

- [ ] **Step 5: Commit:**

```bash
git add internal/store/seed.go internal/store/seed_test.go
git rm internal/config/templates.go internal/config/templates_test.go 2>/dev/null; git add -A internal/config/
git commit -m "feat(store): embedded-template seed parser; drop config.LoadTemplates (#61)"
```

### Task 7: `basic-web` starter template

**Files:**
- Create: `templates/basic-web.yaml`
- Test: `internal/store/seed_test.go`

- [ ] **Step 1: Write the failing test:**

```go
func TestParseSeeds_IncludesBasicWeb(t *testing.T) {
	seeds, err := ParseSeeds(templates.Files)
	require.NoError(t, err)
	var bw *Template
	for i := range seeds {
		if seeds[i].Meta.ID == "basic-web" {
			bw = &seeds[i]
		}
	}
	require.NotNil(t, bw, "basic-web seed must exist")
	require.NotNil(t, bw.Meta.Ingress)
	require.Equal(t, "app", bw.Meta.Ingress.Container)
}
```

- [ ] **Step 2: Run, expect FAIL** (`basic-web` not present).

- [ ] **Step 3: Implement** — create `templates/basic-web.yaml`:

```yaml
# template-meta:
#   id: basic-web
#   display:
#     name: Basic web app
#     description: Run any container image and expose one HTTP port. Clone and edit for your app.
#     category: Generic
#   parameters:
#     - name: image
#       type: string
#       required: true
#       label: Image
#       description: Container image (e.g. docker.io/library/nginx:1)
#     - name: port
#       type: int
#       required: true
#       label: HTTP port
#       description: Port your app serves HTTP on
#       default: 8080
#   ingress:
#     container: app
#     port: 8080
---
apiVersion: v1
kind: Pod
metadata:
  name: basic-web-{{.slug}}
  labels:
    podman-api/template: basic-web
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
      ports:
        - containerPort: {{.port}}
```

Note: the ingress `port` is fixed at 8080 in meta (the container port the pod exposes); the `port` parameter feeds `containerPort`. If you want them coupled, document that editing the port means editing both — acceptable for a starter the user clones.

- [ ] **Step 4: Run, expect PASS.** Also confirm it renders: add a quick assertion that `render.Render` of the body with `{slug:"x", image:"nginx:1", port:8080}` succeeds (reuse `render.Render`).

- [ ] **Step 5: Commit:**

```bash
git add templates/basic-web.yaml internal/store/seed_test.go
git commit -m "feat(templates): basic-web starter (clone-and-edit escape hatch) (#61)"
```

---

## Stage 4 — Service: store-backed templates, default-fill, delete-protection

This stage makes `internal/instance` build against the store and removes `config.Template` + `store==nil` guards. Expect this stage's first task to break the package build until all sub-edits land; do them in one task.

### Task 8: Route `Service` template access through the store

**Files:**
- Modify: `internal/instance/service.go`, `migrate.go`, `evacuate.go`, `reconcile.go`
- Test: `internal/instance/service_test.go` (+ existing tests adapt)

- [ ] **Step 1: Write the failing test** — `internal/instance/templates_store_test.go`:

```go
func TestService_TemplatesFromStore(t *testing.T) {
	ctx := context.Background()
	f := fake.New()
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(ctx, store.Template{
		Meta: render.Meta{ID: "web", Parameters: []render.ParamDef{
			{Name: "slug", Type: "string", Required: true},
			{Name: "image", Type: "string", Required: true},
		}},
		Body: webBody(), Origin: "seed",
	}))
	svc := NewService(f, []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	svc.SetStore(mem)

	got := svc.Templates()
	require.Len(t, got, 1)
	require.Equal(t, "web", got[0].Meta.ID)
}
```

(`webBody()` is a small helper returning the Pod-YAML body string; define it in the test file or reuse `webTemplate().Body` adapted.)

- [ ] **Step 2: Run, expect FAIL** (`NewService` still takes `[]config.Template`; `Templates()` returns `[]config.Template`).

- [ ] **Step 3: Implement:**
  - Change `NewService(client, hosts)` — drop the `tmpls` param and the `templates map[string]config.Template` field. Keep `SetStore`.
  - `Templates() []store.Template` → `s.store.ListTemplates(ctx)`. (Add a `context.Background()` internally or thread ctx — match how other read methods do it; `Templates()` has no ctx today, use `context.Background()` and document.)
  - `lookup(host, id) (store.Template, error)` → verify host exists (as today), then `s.store.GetTemplate(ctx, id)`; map `store.ErrNotFound` → `ErrUnknownTemplate`.
  - Change `validateIngress`, `rawTemplate`, `requiredHostPorts`, `preflightIssues`, `preflightDest`, `migratePostStop` parameter types from `config.Template` to `store.Template`. `rawTemplate(t)` reconstructs source from `t.Meta` + `t.Body` (it already concatenates a meta header + body — update it to serialize `t.Meta` minimally, or better: since `render.Render` needs the full source, change `rawTemplate` to emit a meta block sufficient for `ParseMeta` round-trip — reuse a `render.Meta`→source helper; simplest: store keeps `Body` and render uses `Body` directly if `render.Render` is refactored to take `(meta, body)`).
  - **Decision (lock in):** refactor `render.Render` consumption — add `render.RenderBody(body string, params map[string]any) (string, error)` that skips ParseMeta (the body is already separated). Replace `render.Render(rawTemplate(tmpl), params)` with `render.RenderBody(tmpl.Body, params)` in `service.go` and `migrate.go`. Delete `rawTemplate`. Add the test for `RenderBody` in `render` (trivial: renders `{{.x}}`).
  - Remove `s.store == nil` guards in `service.go:134,321,358,526,601,702,722`, `migrate.go:67,123,238`, `evacuate.go:38`, `reconcile.go:134,197`: the store is always set. Where a guard wrapped persistence (`if s.store != nil { PutSpec }`), make it unconditional. Delete `HasStore()` (`service.go:550-552`).
  - `secretEnvs` (`service.go:86` area): compute from the looked-up template at use time rather than a boot map. Find where `secretEnvs` is read and replace with a call deriving names from `tmpl.Meta` (via `secretEnvNames(tmpl.Body)` as today, or from `tmpl.Meta.Secrets`).

- [ ] **Step 4: Make the package compile + pass.** Update existing `instance` tests that call `NewService(.., tmpls)` or use `config.Template`/`webTemplate()` to instead `PutTemplate` into a Memory store and `SetStore`. This touches `service_ingress_test.go`, `migrate_test.go`, etc. Run `go test -tags "$TAGS" ./internal/instance/` until green.

- [ ] **Step 5: Commit:**

```bash
git add internal/instance/ internal/render/
git commit -m "feat(instance): resolve templates from the store; drop config.Template + store-nil guards (#61)"
```

### Task 9: One-click default-fill in `Apply`

**Files:**
- Modify: `internal/instance/service.go` (`Apply`)
- Test: `internal/instance/templates_store_test.go`

- [ ] **Step 1: Write the failing test:**

```go
func TestApply_FillsParameterDefaults(t *testing.T) {
	ctx := context.Background()
	f := fake.New()
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(ctx, store.Template{
		Meta: render.Meta{ID: "web", Parameters: []render.ParamDef{
			{Name: "slug", Type: "string", Required: true},
			{Name: "image", Type: "string", Required: true},
			{Name: "replicas", Type: "int", Default: 1},
		}},
		Body: webBody(), Origin: "seed",
	}))
	svc := NewService(f, []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	svc.SetStore(mem)

	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template: "web", Slug: "demo",
		Parameters: map[string]any{"slug": "demo", "image": "nginx:1"}, // replicas omitted
	}, ApplyOptions{Replace: true}))

	spec, err := mem.GetSpec(ctx, "h1", "web", "demo")
	require.NoError(t, err)
	require.EqualValues(t, 1, spec.Parameters["replicas"], "default must be persisted")
}
```

- [ ] **Step 2: Run, expect FAIL** (replicas absent from persisted spec).

- [ ] **Step 3: Implement** — in `Apply`, right after `lookup` and before `render.Validate`, replace `req.Parameters` with the defaulted copy:

```go
req.Parameters = render.ApplyDefaults(tmpl.Meta, req.Parameters)
```

(Place it before `Validate` so required-with-default params pass, and before render + persist so the effective params flow everywhere.)

- [ ] **Step 4: Run, expect PASS.** Full instance package green.

- [ ] **Step 5: Commit:**

```bash
git add internal/instance/service.go internal/instance/templates_store_test.go
git commit -m "feat(instance): fill parameter defaults before render+persist (one-click) (#61)"
```

### Task 10: Delete-protection (`DeleteTemplate` with in-use scan)

**Files:**
- Modify: `internal/instance/service.go`, errors file
- Test: `internal/instance/templates_store_test.go`

- [ ] **Step 1: Write the failing test:**

```go
func TestDeleteTemplate_BlockedWhenInUse(t *testing.T) {
	ctx := context.Background()
	f := fake.New()
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(ctx, store.Template{Meta: render.Meta{ID: "web"}, Body: webBody(), Origin: "seed"}))
	require.NoError(t, mem.PutSpec(ctx, store.Spec{Host: "h1", Template: "web", Slug: "demo",
		Parameters: map[string]any{"slug": "demo"}}))
	svc := NewService(f, []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	svc.SetStore(mem)

	err := svc.DeleteTemplate(ctx, "web", false)
	require.ErrorIs(t, err, ErrTemplateInUse)

	require.NoError(t, svc.DeleteTemplate(ctx, "web", true)) // force
	_, err = mem.GetTemplate(ctx, "web")
	require.ErrorIs(t, err, store.ErrNotFound)
}
```

- [ ] **Step 2: Run, expect FAIL** (`DeleteTemplate`/`ErrTemplateInUse` undefined).

- [ ] **Step 3: Implement:**
  - Add `ErrTemplateInUse` and `ErrTemplateExists` to the instance errors file (where `ErrUnknownTemplate` lives — grep for it).
  - Add to `service.go`:
    ```go
    // DeleteTemplate removes a template. Unless force, it is rejected with
    // ErrTemplateInUse when any instance on any host references it.
    func (s *Service) DeleteTemplate(ctx context.Context, id string, force bool) error {
        if !force {
            for _, h := range s.hosts {
                keys, err := s.store.ListSpecKeys(ctx, h.ID)
                if err != nil {
                    return err
                }
                for _, k := range keys {
                    if k.Template == id {
                        return fmt.Errorf("%w: %s/%s on %s", ErrTemplateInUse, id, k.Slug, h.ID)
                    }
                }
            }
        }
        return s.store.DeleteTemplate(ctx, id)
    }
    ```
  - Add thin `CreateTemplate`/`UpdateTemplate`/`GetTemplate`/`CloneTemplate` service methods the API will call (validation lives in Task 11's handler layer or here — put validation here so the UI and API share it). Minimum: `PutTemplate(ctx, t, create bool)` that on `create` errors `ErrTemplateExists` if `GetTemplate` succeeds, and validates via a shared `validateTemplate(t)` (parse body, dry-run render with defaults, check ingress container exists). Define `validateTemplate` now; the API test in Task 12 will exercise it.

- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Commit:**

```bash
git add internal/instance/
git commit -m "feat(instance): template create/update/clone/delete with validation + in-use protection (#61)"
```

---

## Stage 5 — API: CRUD/clone + scopes

### Task 11: Template route table + scopes

**Files:**
- Modify: `internal/api/router.go`
- Test: `internal/api/templates_test.go`

- [ ] **Step 1: Write the failing test** — a route+scope test asserting `GET /templates` requires `templates:read` and `POST /templates` requires `templates:write` (follow the existing scope-guard test pattern in the api package — grep an existing `guard(` test).

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** — in `router.go`, replace the three existing `/templates*` lines with:

```go
mux.Handle("GET /templates", guard("templates:read", http.HandlerFunc(h.listTemplates)))
mux.Handle("POST /templates", guard("templates:write", http.HandlerFunc(h.createTemplate)))
mux.Handle("GET /templates/{id}", guard("templates:read", http.HandlerFunc(h.getTemplate)))
mux.Handle("PUT /templates/{id}", guard("templates:write", http.HandlerFunc(h.updateTemplate)))
mux.Handle("DELETE /templates/{id}", guard("templates:write", http.HandlerFunc(h.deleteTemplate)))
mux.Handle("POST /templates/{id}/clone", guard("templates:write", http.HandlerFunc(h.cloneTemplate)))
mux.Handle("GET /templates/{id}/render", guard("templates:read", http.HandlerFunc(h.renderTemplate)))
```

(Confirm `templates:read`/`templates:write` are valid scope strings — check how scopes are declared in `internal/auth`; add them to any scope registry/enum if one exists.)

- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Commit:**

```bash
git add internal/api/router.go internal/api/templates_test.go
git commit -m "feat(api): template CRUD routes + templates:read/write scopes (#61)"
```

### Task 12: CRUD/clone handlers + structured views

**Files:**
- Rewrite: `internal/api/templates.go`
- Test: `internal/api/templates_test.go`

- [ ] **Step 1: Write failing tests** covering: `GET /templates` returns structured JSON (id, display, parameters with types, body); `POST /templates` creates + 409 on dup; `PUT` updates; `POST /clone` copies to a new id with `origin:"user"`; `DELETE` → 409 when in-use, 200 with `?force=true`; invalid body → 400. Build the handler+store via the api package's existing test harness (grep how `handlers` is constructed in `*_test.go`; it takes the `*instance.Service`). Seed a Memory store + `SetStore`.

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** — rewrite `internal/api/templates.go`:
  - `templateJSON(t store.Template) map[string]any` returning `{id, display, parameters, secrets, volumes, ingress, body, origin, created, updated}` (marshal `t.Meta` fields directly — they already have json tags).
  - `listTemplates` → `svc.Templates()` → `[]templateJSON`.
  - `getTemplate` → `svc.GetTemplate(id)`; `ErrUnknownTemplate` → 404.
  - `createTemplate`/`updateTemplate` → decode `{id?, body, display, parameters, secrets, volumes, ingress}` into a `store.Template` (build `render.Meta`), call the service create/update (validation there); map `ErrTemplateExists`→409, validation→400.
  - `cloneTemplate` → decode `{new_id}`; `svc.CloneTemplate(srcID, newID)`; 409 on dup.
  - `deleteTemplate` → `svc.DeleteTemplate(id, queryBool(r,"force"))`; `ErrTemplateInUse`→409 (list instances in body), else 200.
  - Keep `renderTemplate` but switch it to `svc.GetTemplate` + `render.RenderBody(tmpl.Body, params)`. **Delete** `templateView` and `rebuildSource`.

- [ ] **Step 4: Run, expect PASS.** Full api package: `go test -tags "$TAGS" ./internal/api/`.

- [ ] **Step 5: Commit:**

```bash
git add internal/api/templates.go internal/api/templates_test.go
git commit -m "feat(api): template CRUD/clone handlers + structured views (#61)"
```

---

## Stage 6 — main.go: store-as-backbone + seeding

### Task 13: Always-open store, default path, remove `-templates-dir`, seed-on-empty

**Files:**
- Modify: `cmd/podman-api/main.go`
- Test: `cmd/podman-api/main_test.go`

- [ ] **Step 1: Write the failing test** — a boot/seed test: open a temp SQLite store, run the seed function, assert `CountTemplates` > 0 and that a second seed call is a no-op (count unchanged). Put the seed logic in a testable function:

```go
// in main.go
func seedTemplates(ctx context.Context, db store.TemplateStore, fsys fs.FS) (int, error) {
	n, err := db.CountTemplates(ctx)
	if err != nil || n > 0 {
		return 0, err
	}
	seeds, err := store.ParseSeeds(fsys)
	if err != nil {
		return 0, err
	}
	for _, t := range seeds {
		if err := db.PutTemplate(ctx, t); err != nil {
			return 0, err
		}
	}
	return len(seeds), nil
}
```

Test:

```go
func TestSeedTemplates_OnEmptyOnly(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenSQLite(filepath.Join(t.TempDir(), "s.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	n, err := seedTemplates(ctx, db, templates.Files)
	require.NoError(t, err)
	require.Positive(t, n)

	again, err := seedTemplates(ctx, db, templates.Files)
	require.NoError(t, err)
	require.Zero(t, again, "must not re-seed a populated store")
}
```

- [ ] **Step 2: Run, expect FAIL** (`seedTemplates` undefined).

- [ ] **Step 3: Implement** in `main.go`:
  - `seedTemplates` as above.
  - Change `stateDB` flag default to `/var/lib/podman-api/state.db` (keep the flag for override).
  - Rewrite `openStore`: always open. If `keyFile != ""` load the key and pass a `*KeyStore`; else pass `nil`. `MkdirAll(filepath.Dir(path), 0o750)` before open. Remove the `stateDB == ""` → `(nil,nil)` branch.
  - Remove `-templates-dir` flag, the `var tmpls` block (lines ~108-116), and pass nothing to `NewService(client, hosts)`.
  - After opening `db`: `svc.SetStore(db)`; `seedTemplates(ctx, db, templates.Files)`; log seeded count.
  - Delete the `if db != nil {` wrapper (lines ~141+) — its body becomes unconditional. The ingress `tmplIngress` map (lines 146-154) currently built from `tmpls` must now be built from `db.ListTemplates` (or removed if the ingress controller can read templates from the store — check `ingress.NewCaddyController`'s needs; if it only needs id→{container,port}, build that map from `ListTemplates`).
  - Remove the `*pruneEnabled && db == nil` fatal (line 131) — store always present.

- [ ] **Step 4: Run, expect PASS:** `go test -tags "$TAGS" ./cmd/podman-api/`. Then **full build + test:** `make build && make test`. Fix any remaining `config.Template`/`tmpls` references the compiler flags.

- [ ] **Step 5: Commit:**

```bash
git add cmd/podman-api/
git commit -m "feat(cmd): store-as-backbone; -state-db=location, key-less default, seed-on-empty; drop -templates-dir (#61)"
```

---

## Stage 7 — internal/ui compile + function

### Task 14: Update UI glue to the new model

**Files:**
- Modify: `internal/ui/handlers_deploy.go`, `internal/ui/handlers_instances.go`
- Test: existing `internal/ui/*_test.go`

- [ ] **Step 1: Run the ui package to see the breakage:** `go test -tags "$TAGS" ./internal/ui/` — expect compile errors on `config.Template`, `sortedTemplates`, `fieldData`, `HasStore`, `CanUpgrade`.

- [ ] **Step 2: Implement (red→green driven by the compiler + existing tests):**
  - `sortedTemplates() []store.Template` → `u.cfg.Svc.Templates()` then `slices.SortFunc` by `Meta.ID`.
  - `fieldData(...)` returns `store.Template`; build form fields from `tmpl.Meta.Parameters` ([]ParamDef) — iterate defs for label/required/default/type instead of the old Required/Optional name lists.
  - Remove `HasStore`/`CanUpgrade`: in `handlers_instances.go` set `"CanUpgrade": true` (store always present) or drop the gate and the template conditional that uses it. Grep the HTML templates under `internal/ui` for `CanUpgrade` and simplify.
  - Update any UI template that rendered the old parameter shape.

- [ ] **Step 3: Run, expect PASS:** `go test -tags "$TAGS" ./internal/ui/`.

- [ ] **Step 4: Full suite:** `make test`; `gofmt -l .` (empty); `go vet -tags "$TAGS" ./...`.

- [ ] **Step 5: Commit:**

```bash
git add internal/ui/
git commit -m "fix(ui): compile + function against store-backed typed templates; drop HasStore gating (#61)"
```

---

## Stage 8 — Docs

### Task 15: Wiki updates

**Files:** wiki repo (published directly per CLAUDE.md — no PR flow).

- [ ] **Step 1:** Update **Operating** + **Deploying** + **Provisioning** wiki pages:
  - Templates are DB-managed; CRUD via `/templates` (+ `templates:read`/`templates:write` scopes); clone-and-edit workflow; the `basic-web` starter.
  - `-state-db` is now the DB **location** (default `/var/lib/podman-api/state.db`), always-on; provisioning must create/permission that dir.
  - `-spec-key-file` gates **secrets only**; running key-less disables secret-bearing deploys (clear error).
  - One-click: omitted parameters fall back to declared defaults; persisted spec records effective params.
- [ ] **Step 2:** Update README quick-reference links if any mention on-disk templates or `-templates-dir`.
- [ ] **Step 3:** Publish (git push to the wiki repo or `forgejo api POST /repos/tej/podman-api/wiki/new`).

---

## Final verification (before opening the PR)

- [ ] `make build` succeeds.
- [ ] `make test` all green.
- [ ] `gofmt -l .` empty; `go vet -tags "$TAGS" ./...` clean.
- [ ] Grep confirms vestigial removal: `grep -rn "config.Template\|HasStore\|templates-dir\|store == nil" internal cmd | grep -v _test` returns nothing (the `store==nil` guards and `HasStore` are gone; `config.Template` is gone).
- [ ] Manual smoke (if a podman host is available): boot with no `-spec-key-file`, `GET /templates` lists seeds incl. `basic-web`, deploy `basic-web` with only `image`+`slug`, confirm defaults filled and a secret-bearing template is rejected with the key error.

## PR

Open against `main`: `forgejo pr create tej/podman-api --title="feat: DB-backed template catalog + store-as-backbone (#61)" --head=feat/61-template-catalog --base=main --body="...Closes #61..."`. Body summarizes the architecture shift (store is the backbone, templates in DB, key-less default, typed params, one-click) and the vestigial removals. Flag for the admin-UI agent: rich catalog UI to be built against the structured `GET /templates`; `render.Meta.Parameters` and `NewService` signatures changed.
