# Ingress + Auto-TLS Implementation Plan — Phase 0 (spike) + Phase 1 (foundations)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the spike that de-risks per-host Caddy ingress, then the spike-independent foundations (template ingress metadata, persisted instance domains, a pure Caddyfile renderer, domain validation, and the API surface) — all mergeable on their own and decoupled from the in-flight #54 migrate/evacuate work.

**Architecture:** Per-host managed Caddy (topology B from the design spec, `docs/superpowers/specs/2026-06-04-ingress-auto-tls-design.md`). This plan delivers only the parts that do **not** depend on podman-binding specifics the spike must confirm. The Caddy controller, reconcile loop, network attachment, and `main()` wiring are deliberately deferred to a **Phase 2 plan written after the spike** (see "What comes after this plan").

**Tech Stack:** Go, `podman/v5` bindings, `modernc.org/sqlite`, `gopkg.in/yaml.v3`, `testify/require`, Caddy v2.

**Build/test note (from CLAUDE.md):** all builds/tests need the remote-client tags. Every test command below assumes:
```sh
export TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"
```
Run the full suite with `make test`; targeted runs use `go test -tags "$TAGS" <pkg> -run <name> -v`.

---

## Scope of this plan

**In this plan:** Task 0 (spike, gating) and Tasks 1–5 (foundations).

**Deferred to the Phase 2 plan (after the spike):** the `IngressController`/`CaddyController`, route derivation (computing each route's `Backend` string), the per-host reconcile loop, attaching app pods to the ingress network in the manifest builder, the new `podman.Client` methods (network ensure, container exec, copy-into-container), the `DNSProvider` no-op seam, `main()` flags/wiring, and the integration test. Two design requirements ride with that plan because they only take effect once routing exists: **host-wide domain uniqueness** and the **"template must declare `ingress:` to accept domains"** rejection (route derivation has every route + template meta in hand, the natural place to enforce both).

## File structure

| File | Responsibility | Task |
|---|---|---|
| `internal/render/meta.go` (modify) | Add `Ingress` to template `Meta` + parse-time validation | 1 |
| `internal/render/meta_test.go` (modify) | Cover ingress parsing + validation | 1 |
| `internal/store/store.go` (modify) | Add `Domains []string` to `Spec` | 2 |
| `internal/store/sqlite.go` (modify) | Persist `domains` column + v4 schema migration | 2 |
| `internal/store/sqlite_test.go` (modify) | Round-trip + migration tests | 2 |
| `internal/ingress/caddyfile.go` (create) | `Route` type + pure `RenderCaddyfile` | 3 |
| `internal/ingress/caddyfile_test.go` (create) | Renderer table tests | 3 |
| `internal/ingress/validate.go` (create) | `ValidateDomains` (syntax + intra-slice dups) | 4 |
| `internal/ingress/validate_test.go` (create) | Validation tests | 4 |
| `internal/instance/service.go` (modify) | `ApplyRequest.Domains` → `store.Spec.Domains` | 5 |
| `internal/api/instances.go` (modify) | Reject malformed `domains` with 400 | 5 |
| `internal/api/instances_test.go` (modify) | Cover the 400 path | 5 |

---

## Task 0: De-risking spike (manual, against a real podman host — GATING)

**This task is not TDD and produces no production code.** It runs against the provisioned podman host and proves the three assumptions the rest of the work rests on. **If any assumption fails, stop and revisit the design spec before starting Phase 2.** Tasks 1–5 do not depend on the outcome and may proceed in parallel, but Phase 2 must not start until this passes.

- [ ] **Step 1: Stand up a shared network + Caddy + a dummy backend on the host**

On the podman host (via the same SSH access podman-api uses), by hand:
```sh
podman network create podman-api-ingress
# dummy backend that serves HTTP on 8080 inside the network, named "web-demo"
podman run -d --name web-demo --network podman-api-ingress \
  docker.io/library/nginx:alpine
# caddy on the network, publishing 80/443, config + data on volumes
podman volume create caddy-config && podman volume create caddy-data
printf 'demo.<a-domain-you-control>.com {\n\treverse_proxy web-demo:80\n}\n' > /tmp/Caddyfile
podman run -d --name caddy --network podman-api-ingress \
  -p 80:80 -p 443:443 \
  -v caddy-config:/etc/caddy -v caddy-data:/data \
  docker.io/library/caddy:2 caddy run --config /etc/caddy/Caddyfile
```

- [ ] **Step 2: Prove assumption #1 — network DNS resolves the backend by name**

```sh
podman exec caddy wget -qO- http://web-demo:80 | head -1
```
Expected: nginx's HTML (proves `reverse_proxy web-demo:80` resolves over the network). **Record the exact name a pod's HTTP container is reachable as** — run a *pod* (not a bare container) via `podman kube play` on the network and repeat the lookup; note whether it resolves by container name, pod name, or requires an annotation. This is the open question from spec §11 and Phase 2's `Backend` string depends on it.

- [ ] **Step 3: Prove assumption #2 — HTTP-01 issues and persists across restart**

Point `demo.<domain>` DNS at the host IP, then:
```sh
curl -sI https://demo.<domain>.com        # expect HTTP/2 200 + a valid LE cert
podman restart caddy
curl -sI https://demo.<domain>.com         # still 200, NO re-issue (cert came from caddy-data volume)
podman exec caddy ls /data/caddy/certificates  # confirm persisted cert tree
```
Expected: cert issues on first request and survives the restart with no new ACME order.

- [ ] **Step 4: Prove assumption #3 — cp + reload over the bindings (not the CLI)**

Write a throwaway Go scratch program using `github.com/containers/podman/v5/pkg/bindings/containers` that connects to the host (mirror `internal/podman/real.go`'s `bindings.NewConnectionWithIdentity`) and:
1. copies a new `Caddyfile` into the running caddy container (investigate `containers.CopyFromArchive` / the copy API surface in v5.8.2), then
2. execs `caddy reload --config /etc/caddy/Caddyfile` via `containers.ExecCreate` + `containers.ExecStart`.

Expected: the new route is live after reload, with no container restart. **Record the exact binding function names + signatures** — Phase 2 adds these as `podman.Client` methods and this is the only place they're confirmed before then.

- [ ] **Step 5: Write up findings and commit**

Create `docs/superpowers/specs/2026-06-04-ingress-spike-findings.md` capturing: the resolved backend name format (Step 2), cert persistence path (Step 3), and the exact copy/exec binding signatures (Step 4) — plus any assumption that failed and the design implication.
```sh
git add docs/superpowers/specs/2026-06-04-ingress-spike-findings.md
git commit -m "spike(ingress): validate network DNS, HTTP-01 persistence, cp+reload bindings (#60)"
```

---

## Task 1: Template `ingress:` metadata

**Files:**
- Modify: `internal/render/meta.go`
- Test: `internal/render/meta_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/render/meta_test.go`:
```go
func TestParseMetaIngress(t *testing.T) {
	src := `# template-meta:
#   id: web
#   ingress:
#     container: web
#     port: 8080
---
apiVersion: v1
kind: Pod
`
	meta, _, err := ParseMeta(src)
	require.NoError(t, err)
	require.NotNil(t, meta.Ingress)
	require.Equal(t, "web", meta.Ingress.Container)
	require.Equal(t, 8080, meta.Ingress.Port)
}

func TestParseMetaNoIngress(t *testing.T) {
	src := `# template-meta:
#   id: postgres
---
apiVersion: v1
kind: Pod
`
	meta, _, err := ParseMeta(src)
	require.NoError(t, err)
	require.Nil(t, meta.Ingress)
}

func TestParseMetaIngressInvalid(t *testing.T) {
	src := `# template-meta:
#   id: web
#   ingress:
#     container: web
#     port: 0
---
apiVersion: v1
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/render/ -run TestParseMetaIngress -v`
Expected: FAIL — `meta.Ingress` undefined (compile error).

- [ ] **Step 3: Implement**

In `internal/render/meta.go`, add the field to `Meta` and the type + validation. Add to the `Meta` struct:
```go
	Ingress    *Ingress   `yaml:"ingress"`
```
Add the type below `Volume`:
```go
// Ingress declares which container+port in the rendered pod serves HTTP, so the
// ingress layer can route a domain to it. Absent on non-web templates.
type Ingress struct {
	Container string `yaml:"container"`
	Port      int    `yaml:"port"`
}
```
In `ParseMeta`, immediately before `return wrapper.Meta, body, nil`, add:
```go
	if ing := wrapper.Meta.Ingress; ing != nil {
		if ing.Container == "" {
			return Meta{}, "", errors.New("template-meta: ingress.container is required")
		}
		if ing.Port <= 0 || ing.Port > 65535 {
			return Meta{}, "", fmt.Errorf("template-meta: ingress.port %d out of range", ing.Port)
		}
	}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/render/ -run TestParseMeta -v`
Expected: PASS (all three new tests + existing ones).

- [ ] **Step 5: Commit**

```sh
git add internal/render/meta.go internal/render/meta_test.go
git commit -m "feat(render): parse optional template ingress metadata (#60)"
```

---

## Task 2: Persist instance `Domains`

**Files:**
- Modify: `internal/store/store.go`, `internal/store/sqlite.go`
- Test: `internal/store/sqlite_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/store/sqlite_test.go` (follow the existing test setup there for `OpenSQLite` + a `KeyStore`; reuse whatever helper the file already uses to open a temp DB):
```go
func TestSpecDomainsRoundTrip(t *testing.T) {
	s := openTestSQLite(t) // existing helper in this file
	ctx := context.Background()
	in := Spec{
		Host: "h1", Template: "web", Slug: "a",
		Parameters: map[string]any{},
		Secrets:    map[string]string{},
		Domains:    []string{"a.example.com", "b.example.com"},
	}
	require.NoError(t, s.PutSpec(ctx, in))
	got, err := s.GetSpec(ctx, "h1", "web", "a")
	require.NoError(t, err)
	require.Equal(t, []string{"a.example.com", "b.example.com"}, got.Domains)
}

func TestSpecDomainsDefaultsEmpty(t *testing.T) {
	s := openTestSQLite(t)
	ctx := context.Background()
	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h1", Template: "pg", Slug: "a",
		Parameters: map[string]any{}, Secrets: map[string]string{},
	}))
	got, err := s.GetSpec(ctx, "h1", "pg", "a")
	require.NoError(t, err)
	require.Empty(t, got.Domains)
}
```
> If `openTestSQLite` doesn't exist under that name, use the same open pattern the other tests in the file already use and inline it.

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/store/ -run TestSpecDomains -v`
Expected: FAIL — `Spec` has no field `Domains`.

- [ ] **Step 3: Implement — struct field**

In `internal/store/store.go`, add to `Spec` (after `Secrets`):
```go
	// Domains are the public hostnames the ingress layer routes to this
	// instance. Empty for non-web instances. Non-secret; stored in plaintext.
	Domains []string
```

- [ ] **Step 4: Implement — SQLite column + migration**

In `internal/store/sqlite.go`:

(a) Add the column to the `specs` table in `schemaSQL` (after `secrets BLOB NOT NULL,`):
```sql
  domains    TEXT NOT NULL DEFAULT '[]',
```

(b) Replace the unconditional `PRAGMA user_version = 3` block in `OpenSQLite` with a call to a migration helper:
```go
	if err := migrateSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
```
and add the helper (it brings pre-v4 databases — created before the `domains` column existed — up to date; fresh DBs already have the column from `schemaSQL`, so the ADD COLUMN is tolerated as a duplicate):
```go
// migrateSchema brings an existing specs table up to the current schema. v4
// added specs.domains. The ALTER is guarded by user_version, and the
// duplicate-column case (fresh DB where schemaSQL already created the column)
// is tolerated so OpenSQLite is idempotent.
func migrateSchema(db *sql.DB) error {
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v < 4 {
		if _, err := db.Exec(`ALTER TABLE specs ADD COLUMN domains TEXT NOT NULL DEFAULT '[]'`); err != nil {
			if !strings.Contains(err.Error(), "duplicate column") {
				return err
			}
		}
		if _, err := db.Exec(`PRAGMA user_version = 4`); err != nil {
			return err
		}
	}
	return nil
}
```

(c) `PutSpec` — marshal and write domains. After the `secJSON`/`blob` setup, add:
```go
	domJSON, err := json.Marshal(sp.Domains)
	if err != nil {
		return err
	}
	if sp.Domains == nil {
		domJSON = []byte("[]")
	}
```
and change the INSERT to include the column:
```go
		_, err := s.db.ExecContext(ctx, `
INSERT INTO specs (host, template, slug, parameters, secrets, domains, created, updated)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(host, template, slug) DO UPDATE SET
  parameters = excluded.parameters,
  secrets    = excluded.secrets,
  domains    = excluded.domains,
  updated    = excluded.updated`,
			sp.Host, sp.Template, sp.Slug, string(params), blob, string(domJSON), now, now)
```

(d) `GetSpec` — select and unmarshal domains. Change the SELECT to:
```go
		`SELECT parameters, secrets, domains, created, updated FROM specs WHERE host=? AND template=? AND slug=?`,
```
add a scan target `var domainsJSON string` and include `&domainsJSON` in `row.Scan(...)` (between `&blob` and `&created`), then before the `return`:
```go
	var domains []string
	if err := json.Unmarshal([]byte(domainsJSON), &domains); err != nil {
		return Spec{}, err
	}
```
and add `Domains: domains,` to the returned `Spec{...}`.

> The `Memory` store needs no change — it stores the whole `Spec` value, so `Domains` round-trips for free.

- [ ] **Step 5: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/store/ -run TestSpec -v`
Expected: PASS.

- [ ] **Step 6: Migration test**

Add to `internal/store/sqlite_test.go`:
```go
func TestMigrateAddsDomainsColumn(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/old.db"
	// Simulate a pre-v4 DB: specs table WITHOUT the domains column, user_version=3.
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`CREATE TABLE specs (
  host TEXT NOT NULL, template TEXT NOT NULL, slug TEXT NOT NULL,
  parameters TEXT NOT NULL, secrets BLOB NOT NULL,
  created INTEGER NOT NULL, updated INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug));`)
	require.NoError(t, err)
	_, err = raw.Exec(`PRAGMA user_version = 3`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	// Opening through OpenSQLite must migrate it and then round-trip domains.
	s := openTestSQLiteAt(t, path) // open helper that points at an existing path
	ctx := context.Background()
	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h", Template: "web", Slug: "x",
		Parameters: map[string]any{}, Secrets: map[string]string{},
		Domains: []string{"x.example.com"},
	}))
	got, err := s.GetSpec(ctx, "h", "web", "x")
	require.NoError(t, err)
	require.Equal(t, []string{"x.example.com"}, got.Domains)
}
```
> Use the file's existing key-store helper for `OpenSQLite(path, keys)`. If no `openTestSQLiteAt` helper exists, inline `OpenSQLite(path, keys)` with a freshly created `*KeyStore` the same way the other tests build one.

- [ ] **Step 7: Run to verify pass, then commit**

Run: `go test -tags "$TAGS" ./internal/store/ -run 'TestSpec|TestMigrate' -v`
Expected: PASS.
```sh
git add internal/store/store.go internal/store/sqlite.go internal/store/sqlite_test.go
git commit -m "feat(store): persist instance domains (specs.domains, schema v4) (#60)"
```

---

## Task 3: Pure Caddyfile renderer

**Files:**
- Create: `internal/ingress/caddyfile.go`, `internal/ingress/caddyfile_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/ingress/caddyfile_test.go`:
```go
package ingress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderCaddyfile(t *testing.T) {
	cases := []struct {
		name   string
		email  string
		routes []Route
		want   string
	}{
		{
			name:  "email only, no routes",
			email: "ops@example.com",
			want:  "{\n\temail ops@example.com\n}\n\n",
		},
		{
			name: "routes sorted by domain, no email",
			routes: []Route{
				{Domain: "b.example.com", Backend: "web-b:8080"},
				{Domain: "a.example.com", Backend: "web-a:8080"},
			},
			want: "a.example.com {\n\treverse_proxy web-a:8080\n}\n" +
				"b.example.com {\n\treverse_proxy web-b:8080\n}\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RenderCaddyfile(tc.email, tc.routes)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRenderCaddyfileRejectsEmptyFields(t *testing.T) {
	_, err := RenderCaddyfile("", []Route{{Domain: "a.example.com"}})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestRenderCaddyfile -v`
Expected: FAIL — package/types do not exist.

- [ ] **Step 3: Implement**

Create `internal/ingress/caddyfile.go`:
```go
// Package ingress renders ingress configuration and validates domains. The
// per-host Caddy controller (Phase 2) consumes this package; nothing here
// talks to podman or the network, so it is pure and unit-testable.
package ingress

import (
	"fmt"
	"sort"
	"strings"
)

// Route maps a public domain to the backend address the host's Caddy
// reverse-proxies to. Backend is resolved on the shared ingress network
// (e.g. "web-app1:8080"); its exact name format is confirmed by the spike.
type Route struct {
	Domain  string
	Backend string
}

// RenderCaddyfile produces a deterministic Caddyfile for routes. A non-empty
// acmeEmail sets the global ACME contact. Routes are emitted sorted by domain
// so identical inputs yield byte-identical output — a stable file means a
// `caddy reload` is a no-op when nothing actually changed.
func RenderCaddyfile(acmeEmail string, routes []Route) (string, error) {
	var b strings.Builder
	if acmeEmail != "" {
		fmt.Fprintf(&b, "{\n\temail %s\n}\n\n", acmeEmail)
	}
	sorted := append([]Route(nil), routes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Domain < sorted[j].Domain })
	for _, r := range sorted {
		if r.Domain == "" || r.Backend == "" {
			return "", fmt.Errorf("ingress: route has empty domain or backend: %+v", r)
		}
		fmt.Fprintf(&b, "%s {\n\treverse_proxy %s\n}\n", r.Domain, r.Backend)
	}
	return b.String(), nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestRenderCaddyfile -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```sh
git add internal/ingress/caddyfile.go internal/ingress/caddyfile_test.go
git commit -m "feat(ingress): pure Caddyfile renderer (#60)"
```

---

## Task 4: Domain validation

**Files:**
- Create: `internal/ingress/validate.go`, `internal/ingress/validate_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/ingress/validate_test.go`:
```go
package ingress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDomains(t *testing.T) {
	require.NoError(t, ValidateDomains(nil))
	require.NoError(t, ValidateDomains([]string{"app.example.com", "api.example.com"}))

	require.Error(t, ValidateDomains([]string{"App.Example.com"})) // uppercase
	require.Error(t, ValidateDomains([]string{"not a domain"}))    // syntax
	require.Error(t, ValidateDomains([]string{"-bad.example.com"})) // leading hyphen
	require.Error(t, ValidateDomains([]string{"dup.example.com", "dup.example.com"})) // duplicate
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestValidateDomains -v`
Expected: FAIL — `ValidateDomains` undefined.

- [ ] **Step 3: Implement**

Create `internal/ingress/validate.go`:
```go
package ingress

import (
	"fmt"
	"regexp"
	"strings"
)

// domainRE is a pragmatic FQDN check: ≥2 dot-separated labels, each 1–63 chars
// of [a-z0-9-], not starting/ending with '-', a 2–63 char alpha TLD. Lowercase
// only (ACME/Caddy treat hostnames case-insensitively; we normalize by
// rejecting non-lowercase so stored domains compare byte-for-byte).
var domainRE = regexp.MustCompile(`^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$`)

// ValidateDomains checks that each domain is a syntactically valid lowercase
// FQDN and that the slice has no intra-slice duplicates. A nil/empty slice is
// valid (a non-web instance). Host-wide uniqueness across instances is enforced
// later, at route derivation (Phase 2).
func ValidateDomains(domains []string) error {
	seen := make(map[string]bool, len(domains))
	for _, d := range domains {
		if d != strings.ToLower(d) {
			return fmt.Errorf("ingress: domain %q must be lowercase", d)
		}
		if !domainRE.MatchString(d) {
			return fmt.Errorf("ingress: invalid domain %q", d)
		}
		if seen[d] {
			return fmt.Errorf("ingress: duplicate domain %q", d)
		}
		seen[d] = true
	}
	return nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/ingress/ -run TestValidateDomains -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```sh
git add internal/ingress/validate.go internal/ingress/validate_test.go
git commit -m "feat(ingress): domain syntax + duplicate validation (#60)"
```

---

## Task 5: Accept `domains` on the API and persist them

**Files:**
- Modify: `internal/instance/service.go`, `internal/api/instances.go`
- Test: `internal/api/instances_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/api/instances_test.go` (mirror the existing handler-test setup in that file — build the same `handlers`/router the other tests use):
```go
func TestCreateInstanceRejectsBadDomain(t *testing.T) {
	h := newTestHandlers(t) // use whatever constructor the existing tests use
	body := `{"template":"web","slug":"a","domains":["NOT A DOMAIN"]}`
	req := httptest.NewRequest("POST", "/hosts/h1/instances", strings.NewReader(body))
	req.SetPathValue("host", "h1")
	w := httptest.NewRecorder()
	h.createInstance(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "invalid_domains")
}
```
> If the file routes through the full mux rather than calling `h.createInstance` directly, send the request through that router instead — match the file's existing style.

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags "$TAGS" ./internal/api/ -run TestCreateInstanceRejectsBadDomain -v`
Expected: FAIL — `domains` is dropped (unknown field), handler returns 201, not 400.

- [ ] **Step 3: Implement — request field + persistence**

In `internal/instance/service.go`, add to `ApplyRequest` (after `Secrets`):
```go
	Domains    []string          `json:"domains,omitempty"`
```
In `Service.Apply`, where the `store.Spec{...}` is built (around line 235), add a defensive copy alongside the existing `paramsCopy`/`secretsCopy` and set the field:
```go
	var domainsCopy []string
	if len(req.Domains) > 0 {
		domainsCopy = append([]string(nil), req.Domains...)
	}
```
and add to the struct literal:
```go
			Domains:    domainsCopy,
```

- [ ] **Step 4: Implement — 400 on malformed domains**

In `internal/api/instances.go`, add the import `"github.com/iotready/podman-api/internal/ingress"`. In **both** `createInstance` and `applyInstance`, immediately after the `decodeApply` block (and after the `validInstancePath` check), insert:
```go
	if err := ingress.ValidateDomains(req.Domains); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_domains", Message: err.Error()})
		return
	}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test -tags "$TAGS" ./internal/api/ ./internal/instance/ -run 'Domain|Apply|CreateInstance' -v`
Expected: PASS.

- [ ] **Step 6: Full suite + vet + fmt, then commit**

```sh
make test
go vet -tags "$TAGS" ./...
gofmt -l .   # must print nothing
git add internal/instance/service.go internal/api/instances.go internal/api/instances_test.go
git commit -m "feat(api): accept and persist instance domains; reject malformed (#60)"
```

---

## What comes after this plan (Phase 2 — write its own plan post-spike)

Once Task 0's findings doc lands, run the writing-plans skill again for the controller, using the spike's recorded backend-name format and copy/exec binding signatures. Phase 2 covers:

1. New `podman.Client` methods — `NetworkEnsure`, `ContainerExec`, `CopyToContainer` (signatures from Task 0 Step 4) + their `Real` and `Mock` implementations.
2. `IngressController` interface + `CaddyController` (`EnsureProxy`, `Apply`) — ensures the network + Caddy system pod, renders via `ingress.RenderCaddyfile`, copies the file in, execs `caddy reload` (with `caddy validate` first).
3. Route derivation: build `[]ingress.Route` from a host's instances (`store.Spec.Domains` × the template's `Meta.Ingress`), computing each `Backend` from the spike's name format. **This is where host-wide domain uniqueness and the "template must declare `ingress:`" rejection are enforced.**
4. Attach app pods to the ingress network in the manifest builder (no host port for the HTTP container).
5. Per-host reconcile (inline on create/delete/upgrade + periodic drift correction), serialized per host with a mutex.
6. `DNSProvider` no-op seam.
7. `main()` flags (`-ingress-enabled`, `-ingress-network`, `-ingress-caddy-image`, `-ingress-acme-email`) + wiring; reject `domains` when ingress is disabled.
8. Integration test behind the `integration` build tag (deploy a web template with a domain; assert route live + cert obtained against a local ACME such as Pebble).

## Self-review notes (spec coverage)

- Design §2/§3 (topology, scope) → realized by this plan's foundations + deferred to Phase 2 for the controller; nothing contradicts the spec.
- Design §5 (data model): template `ingress:` → Task 1; `Spec.Domains` → Task 2; `Route` (derived) → renderer Task 3, derivation deferred to Phase 2.
- Design §5 validation: domain syntax/dup → Task 4 + 5. **Host-wide uniqueness** and **"`ingress:` required for domains"** are deferred to Phase 2 route derivation (documented above) because they have no observable effect until routing exists.
- Design §6 (Caddyfile render) → Task 3.
- Design §8 (spike) → Task 0, gating.
- Design §4/§7/§9/§10 (components, seams, error handling, controller tests) → Phase 2.
